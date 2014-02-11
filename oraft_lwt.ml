open Printf
open Lwt
open Oraft
open Oraft.Types
open Oraft_util

module Map    = BatMap
module List   = BatList
module Option = BatOption
module Queue  = BatQueue

let section = Lwt_log.Section.make "oraft_lwt"

module REPID = struct type t = rep_id let compare = String.compare end
module RM = Map.Make(REPID)

module type LWTIO_TYPES =
sig
  type address
  type op
  type connection
  type conn_manager
end

module type LWTIO =
sig
  include LWTIO_TYPES

  val connect : conn_manager -> rep_id -> address -> connection option Lwt.t
  val send    : connection -> (req_id * op) message -> unit Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t

  type snapshot_transfer

  val prepare_snapshot :
    connection -> index -> config -> snapshot_transfer option Lwt.t

  val send_snapshot : snapshot_transfer -> bool Lwt.t
end

module type SERVER_GENERIC =
sig
  open Oraft.Types

  include LWTIO_TYPES

  type 'a server

  type gen_result =
      [ `Error of exn
      | `Redirect of rep_id * address
      | `Redirect_randomized of rep_id * address
      | `Retry_later ]

  type 'a cmd_result   = [ gen_result | `OK of 'a ]
  type ro_op_result = [ gen_result | `OK ]

  val make :
    ('a server -> op -> [`OK of 'a | `Error of exn] Lwt.t) ->
    ?election_period:float -> ?heartbeat_period:float ->
    (req_id * op) Oraft.Core.state -> (rep_id * address) list ->
    conn_manager -> 'a server

  val run     : _ server -> unit Lwt.t
  val abort   : _ server -> unit Lwt.t
  val execute : 'a server -> op -> 'a cmd_result Lwt.t
  val readonly_operation : _ server -> ro_op_result Lwt.t

  val compact_log : _ server -> index -> unit

  module Config :
  sig
    type result =
      [
      | `OK
      | `Redirect of rep_id option
      | `Retry
      | `Cannot_change
      | `Unsafe_change of simple_config * passive_peers
      ]

    val add_failover    : _ server -> rep_id -> address -> result Lwt.t
    val remove_failover : _ server -> rep_id -> result Lwt.t
    val decommission    : _ server -> rep_id -> result Lwt.t
    val demote          : _ server -> rep_id -> result Lwt.t
    val promote         : _ server -> rep_id -> result Lwt.t
    val replace         : _ server -> replacee:rep_id -> failover:rep_id -> result Lwt.t
  end
end

module Make_server(IO : LWTIO) =
struct
  module S    = Set.Make(String)
  module CMDM = Map.Make(struct
                           type t = req_id
                           let compare = compare
                         end)

  exception Stop_node

  type address      = IO.address
  type op           = IO.op
  type connection   = IO.connection
  type conn_manager = IO.conn_manager

  type 'a server =
      {
        execute      : 'a server -> IO.op -> [`OK of 'a | `Error of exn] Lwt.t;
        conn_manager : IO.conn_manager;
        mutable peers            : IO.address RM.t;
        election_period          : float;
        heartbeat_period         : float;
        mutable next_req_id      : Int64.t;
        mutable conns            : IO.connection RM.t;
        mutable state            : (req_id * IO.op) Core.state;
        mutable running          : bool;
        mutable msg_threads      : th_res Lwt.t list;
        mutable election_timeout : th_res Lwt.t;
        mutable heartbeat        : th_res Lwt.t;
        mutable abort            : th_res Lwt.t * th_res Lwt.u;
        mutable get_cmd          : th_res Lwt.t;
        mutable get_ro_op        : th_res Lwt.t;
        push_cmd                 : (req_id * IO.op) -> unit;
        cmd_stream               : (req_id * IO.op) Lwt_stream.t;
        push_ro_op               : ro_op_res Lwt.u -> unit;
        ro_op_stream             : ro_op_res Lwt.u Lwt_stream.t;
        pending_ro_ops           : (Int64.t * ro_op_res Lwt.u) Queue.t;
        mutable pending_cmds     : ('a cmd_res Lwt.t * 'a cmd_res Lwt.u) CMDM.t;
        leader_signal            : unit Lwt_condition.t;
        sent_snapshots           : (rep_id * index) Lwt_stream.t;
        mutable sent_snapshots_th  : th_res Lwt.t;
        snapshot_sent            : ((rep_id * index) -> unit);
        failed_snapshots         : rep_id Lwt_stream.t;
        mutable failed_snapshot_th : th_res Lwt.t;
        snapshot_failed          : rep_id -> unit;
        mutable config_change    : config_change;
      }

  and config_change =
    | No_change
    | New_failover of change_result Lwt.u * rep_id * IO.address
    | Remove_failover of change_result Lwt.u * rep_id
    | Decommission of change_result Lwt.u * rep_id
    | Promote of change_result Lwt.u * rep_id
    | Demote of change_result Lwt.u * rep_id
    | Replace of change_result Lwt.u * rep_id * rep_id

  and change_result = OK | Retry

  and th_res =
      Message of rep_id * (req_id * IO.op) message
    | Client_command of req_id * IO.op
    | Abort
    | Election_timeout
    | Heartbeat_timeout
    | Snapshots_sent of (rep_id * index) list
    | Snapshot_send_failed of rep_id
    | Readonly_op of ro_op_res Lwt.u

  and 'a cmd_res =
      Redirect of rep_id option
    | Executed of [`OK of 'a | `Error of exn]

  and ro_op_res = OK | Retry

  type gen_result =
      [ `Error of exn
      | `Redirect of rep_id * IO.address
      | `Redirect_randomized of rep_id * IO.address
      | `Retry_later ]

  type 'a cmd_result   = [ gen_result | `OK of 'a ]
  type ro_op_result = [ gen_result | `OK ]

  let get_sent_snapshots t =
    match_lwt Lwt_stream.get t.sent_snapshots with
        None -> fst (Lwt.wait ())
      | Some (peer, last_index) ->
          let l = Lwt_stream.get_available t.sent_snapshots in
            Lwt_stream.njunk (List.length l) t.sent_snapshots >>
            return (Snapshots_sent ((peer, last_index) :: l))

  let get_failed_snapshot t =
    match_lwt Lwt_stream.get t.failed_snapshots with
        None -> fst (Lwt.wait ())
      | Some rep_id -> return (Snapshot_send_failed rep_id)

  let make
        execute
        ?(election_period = 2.)
        ?(heartbeat_period = election_period /. 2.) state peers conn_manager =
    let cmd_stream, p     = Lwt_stream.create () in
    let push_cmd x        = p (Some x) in
    let ro_op_stream, p   = Lwt_stream.create () in
    let push_ro_op x      = p (Some x) in
    let election_timeout  = match Core.status state with
                              | Follower | Candidate ->
                                  Lwt_unix.sleep election_period >>
                                  return Election_timeout
                              | Leader -> fst (Lwt.wait ()) in
    let heartbeat         = match Core.status state with
                              | Follower | Candidate -> fst (Lwt.wait ())
                              | Leader ->
                                  Lwt_unix.sleep heartbeat_period >>
                                  return Heartbeat_timeout in
    let sent_snapshots, p = Lwt_stream.create () in
    let snapshot_sent x   = p (Some x) in
    let sent_snapshots_th = fst (Lwt.wait ()) in

    let failed_snapshots, p = Lwt_stream.create () in
    let snapshot_failed x   = p (Some x) in
    let failed_snapshot_th  = fst (Lwt.wait ()) in

    let t =
      {
        execute;
        conn_manager;
        heartbeat_period;
        election_period;
        state;
        election_timeout;
        heartbeat;
        sent_snapshots;
        snapshot_sent;
        sent_snapshots_th;
        failed_snapshots;
        snapshot_failed;
        failed_snapshot_th;
        cmd_stream;
        push_cmd;
        ro_op_stream;
        push_ro_op;
        peers         = List.fold_left
                          (fun m (k, v) -> RM.add k v m) RM.empty
                          (List.filter (fun (id, _) -> id <> Core.id state) peers);
        next_req_id   = 42L;
        conns         = RM.empty;
        running       = true;
        msg_threads   = [];
        abort         = Lwt.task ();
        get_cmd       = (match_lwt Lwt_stream.get cmd_stream with
                           | None -> fst (Lwt.wait ())
                           | Some (req_id, op) ->
                               return (Client_command (req_id, op)));
        get_ro_op     = (match_lwt Lwt_stream.get ro_op_stream with
                           | None -> fst (Lwt.wait ())
                           | Some x -> return (Readonly_op x));
        pending_ro_ops= Queue.create ();
        pending_cmds  = CMDM.empty;
        leader_signal = Lwt_condition.create ();
        config_change = No_change;
      }
    in
      t.sent_snapshots_th   <- get_sent_snapshots  t;
      t.failed_snapshot_th  <- get_failed_snapshot t;
      t

  let abort t =
    if not t.running then
      return ()
    else begin
      t.running <- false;
      begin try (Lwt.wakeup (snd t.abort) Abort) with _ -> () end;
      RM.bindings t.conns |> List.map snd |> Lwt_list.iter_p IO.abort
    end

  let lookup_address t peer_id =
    maybe_nf (RM.find peer_id) t.peers

  let connect_and_get_msgs t (peer, addr) =
    let rec make_thread = function
        0 -> Lwt_unix.sleep 5. >> make_thread 5
      | n ->
          match_lwt IO.connect t.conn_manager peer addr with
            | None -> Lwt_unix.sleep 0.1 >> make_thread (n - 1)
            | Some conn ->
                t.conns <- RM.add peer conn t.conns;
                match_lwt IO.receive conn with
                    None -> Lwt_unix.sleep 0.1 >> make_thread 5
                  | Some msg -> return (Message (peer, msg))
    in
      make_thread 5

  let rec clear_pending_ro_ops t =
    match Queue.Exceptionless.take t.pending_ro_ops with
        None -> ()
      | Some (_, u) -> (try Lwt.wakeup_later u Retry with _ -> ());
                       clear_pending_ro_ops t

  let abort_ongoing_config_change t =
    match t.config_change with
        No_change -> ()
      | New_failover (u, _, _)
      | Remove_failover (u, _)
      | Decommission (u, _)
      | Promote (u, _)
      | Demote (u, _)
      | Replace (u, _, _) ->
          try Lwt.wakeup_later u Retry with _ -> ()

  let notify_config_result t task result =
    t.config_change <- No_change;
    try Lwt.wakeup_later task result with _ -> ()

  let notify_ok_if_mem t u rep_id l =
    notify_config_result t u (if List.mem rep_id l then OK else Retry)

  let notify_ok_if_not_mem t u rep_id l =
    notify_config_result t u (if List.mem rep_id l then Retry else OK)

  let check_config_change_completion t =
    match Core.committed_config t.state with
        Joint_config _ -> (* wait for the final Simple_config *) ()
      | Simple_config (active, passive) ->
          match t.config_change with
            | No_change -> ()
            | New_failover (u, rep_id, addr) ->
                if not (List.mem rep_id active || List.mem rep_id passive) then
                  notify_config_result t u Retry
                else begin
                  t.peers <- RM.add rep_id addr t.peers;
                  notify_config_result t u OK
                end
            | Remove_failover (u, rep_id) -> notify_ok_if_not_mem t u rep_id passive
            | Decommission (u, rep_id) ->
                if List.mem rep_id active || List.mem rep_id passive then
                  notify_config_result t u Retry
                else begin
                  t.peers <- RM.remove rep_id t.peers;
                  notify_config_result t u OK
                end
            | Promote (u, rep_id) -> notify_ok_if_mem t u rep_id active
            | Demote (u, rep_id) -> notify_ok_if_not_mem t u rep_id active
            | Replace (u, replacee, failover) ->
                if not (List.mem failover active) then
                  notify_config_result t u Retry
                else begin
                  t.peers <- RM.remove replacee t.peers;
                  notify_config_result t u OK
                end

  let rec exec_action t : _ action -> unit Lwt.t = function
    | Reset_election_timeout ->
        t.election_timeout <- (Lwt_unix.sleep t.election_period >>
                               return Election_timeout);
        return ()
    | Reset_heartbeat ->
        t.heartbeat <- (Lwt_unix.sleep t.heartbeat_period >>
                        return Heartbeat_timeout);
        return ()
    | Become_candidate
    | Become_follower None ->
        clear_pending_ro_ops t;
        abort_ongoing_config_change t;
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t Reset_election_timeout
    | Become_follower (Some _) ->
        clear_pending_ro_ops t;
        abort_ongoing_config_change t;
        Lwt_condition.broadcast t.leader_signal ();
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t Reset_election_timeout
    | Become_leader ->
        clear_pending_ro_ops t;
        abort_ongoing_config_change t;
        Lwt_condition.broadcast t.leader_signal ();
        exec_action t Reset_election_timeout >>
        exec_action t Reset_heartbeat
    | Changed_config ->
        check_config_change_completion t;
        return_unit
    | Apply l ->
        Lwt_list.iter_s
          (fun (index, (req_id, op), term) ->
             (* TODO: allow to run this in parallel with rest RAFT algorithm.
              * Must make sure that Apply actions are not reordered. *)
             lwt resp = try_lwt t.execute t op
                        with exn -> return (`Error exn)
             in begin
               try_lwt
                 let (_, u), pending_cmds = CMDM.extract req_id t.pending_cmds in
                   Lwt.wakeup_later u (Executed resp);
                   return ()
               with _ -> return ()
             end)
          l
    | Redirect (rep_id, (req_id, _)) -> begin
        try_lwt
          let (_, u), pending_cmds = CMDM.extract req_id t.pending_cmds in
            Lwt.wakeup_later u (Redirect rep_id);
            return ()
        with _ -> return ()
      end
    | Send (rep_id, msg) ->
        (* we allow to run this in parallel with rest RAFT algorithm.
         * It's OK to reorder sends. *)
        (* TODO: limit the number of msgs in outboung queue.
         * Drop after the nth? *)
        ignore begin try_lwt
          IO.send (RM.find rep_id t.conns) msg
        with _ ->
          (* cannot send -- partition *)
          return ()
        end;
        return ()
    | Send_snapshot (rep_id, idx, config) ->
        ignore begin
          match_lwt IO.prepare_snapshot (RM.find rep_id t.conns) idx config with
            | None -> return ()
            | Some transfer ->
                try_lwt
                  match_lwt IO.send_snapshot transfer with
                      true -> t.snapshot_sent (rep_id, idx);
                              return ()
                    | false -> failwith "error"
                with _ ->
                  t.snapshot_failed rep_id;
                  return ()
        end;
        return ()
    | Stop -> raise_lwt Stop_node
    | Exec_readonly n ->
        (* can execute all RO ops whose ID is >= n *)
        let rec notify_ok () =
          match Queue.Exceptionless.peek t.pending_ro_ops with
              None -> return_unit
            | Some (m, _) when m > n -> return_unit
            | Some (_, u) ->
                ignore (Queue.Exceptionless.take t.pending_ro_ops);
                Lwt.wakeup_later u OK;
                notify_ok ()
        in
          notify_ok ()

  let exec_actions t l = Lwt_list.iter_s (exec_action t) l

  let rec run t =
    if not t.running then return ()
    else
      let must_recon = RM.bindings t.peers |>
                       List.filter
                         (fun (peer, _) -> not (RM.mem peer t.conns)) in
      let new_ths    = List.map (connect_and_get_msgs t) must_recon in
        t.msg_threads <- new_ths @ t.msg_threads;

        match_lwt
          Lwt.choose
            ([ t.election_timeout;
               fst t.abort;
               t.get_cmd;
               t.get_ro_op;
               t.heartbeat;
               t.sent_snapshots_th;
             ] @
             t.msg_threads)
        with
          | Abort -> t.running <- false; return ()
          | Readonly_op u -> begin
              match Core.readonly_operation t.state with
                  (s, None) -> Lwt.wakeup_later u Retry;
                               return_unit
                | (s, Some (id, actions)) ->
                    Queue.push (id, u) t.pending_ro_ops;
                    t.state <- s;
                    exec_actions t actions >>
                    run t
            end
          | Client_command (req_id, op) ->
              let state, actions = Core.client_command (req_id, op) t.state in
                t.state <- state;
                exec_actions t actions >>
                run t
          | Message (rep_id, msg) ->
              let state, actions = Core.receive_msg t.state rep_id msg in
                t.state <- state;
                exec_actions t actions >>
                run t
          | Election_timeout ->
              let state, actions = Core.election_timeout t.state in
                t.state <- state;
                exec_actions t actions >>
                run t
          | Heartbeat_timeout ->
              let state, actions = Core.heartbeat_timeout t.state in
                t.state <- state;
                exec_actions t actions >>
                run t
          | Snapshots_sent data ->
              let state, actions =
                List.fold_left
                  (fun (s, actions) (peer, last_index) ->
                     let s, actions' = Core.snapshot_sent peer ~last_index s in
                       (s, actions' @ actions))
                  (t.state, [])
                  data
              in
                t.sent_snapshots_th <- get_sent_snapshots t;
                t.state <- state;
                exec_actions t actions >>
                run t
          | Snapshot_send_failed rep_id ->
              let state, actions = Core.snapshot_send_failed rep_id t.state in
                t.failed_snapshot_th <- get_failed_snapshot t;
                t.state <- state;
                exec_actions t actions >>
                run t

  let run t =
    try_lwt
      run t
    with Stop_node -> t.running <- false; return ()

  let gen_req_id t =
    let id = t.next_req_id in
      t.next_req_id <- Int64.succ id;
      (Core.id t.state, id)

  let rec exec_aux t f =
    match Core.status t.state, Core.leader_id t.state with
      | Follower, Some leader_id -> begin
          match maybe_nf (RM.find leader_id) t.peers with
              Some address -> return (`Redirect (leader_id, address))
            | None ->
                (* redirect to a random server, hoping it knows better *)
                try_lwt
                  let leader_id, address =
                    RM.bindings t.peers |>
                    Array.of_list |>
                    (fun x -> if x = [||] then failwith "empty"; x) |>
                    (fun a -> a.(Random.int (Array.length a)))
                  in
                    return (`Redirect_randomized (leader_id, address))
                with _ ->
                  return `Retry_later
        end
      | Candidate, _ | Follower, _ ->
          (* await leader, retry *)
          Lwt_condition.wait t.leader_signal >>
          exec_aux t f
      | Leader, _ ->
          f t

  let rec execute t cmd =
    exec_aux t
      (fun t ->
          let req_id = gen_req_id t in
          let task   = Lwt.task () in
            t.pending_cmds <- CMDM.add req_id task t.pending_cmds;
            t.push_cmd (req_id, cmd);
            match_lwt fst task with
                Executed res -> return (res :> _ cmd_result)
              | Redirect _ -> execute t cmd)

  let rec readonly_operation t =
    exec_aux t
      (fun t ->
         let th, u = Lwt.task () in
           t.push_ro_op u;
           match_lwt th with
               OK -> return `OK
             | Retry -> readonly_operation t)

  let compact_log t index =
    t.state <- Core.compact_log index t.state

  module Config =
  struct
    type result =
      [
      | `OK
      | `Redirect of rep_id option
      | `Retry
      | `Cannot_change
      | `Unsafe_change of simple_config * passive_peers
      ]

    let retry_delay = 0.05

    let rec perform_change t perform mk_change : result Lwt.t =
      match t.config_change with
          New_failover _ | Remove_failover _ | Decommission _
        | Promote _ | Demote _ | Replace _ ->
            Lwt_unix.sleep retry_delay >>
            perform_change t perform mk_change
        | No_change ->
            match perform t.state with
                `Already_changed -> return `OK
              | `Cannot_change | `Unsafe_change _ | `Redirect _ as x -> return x
              | `Change_in_process ->
                  Lwt_unix.sleep retry_delay >>
                  perform_change t perform mk_change
              | `Start_change state ->
                  t.state <- state;
                  let th, u = Lwt.task () in
                    t.config_change <- mk_change u;
                    match_lwt th with
                        OK -> return `OK
                      | Retry ->
                          Lwt_unix.sleep retry_delay >>
                          perform_change t perform mk_change

    let rec add_failover t rep_id addr =
      perform_change t
        (Core.Config.add_failover rep_id)
        (fun u -> New_failover (u, rep_id, addr))

    let remove_failover t rep_id =
      perform_change t
        (Core.Config.remove_failover rep_id)
        (fun u -> Remove_failover (u, rep_id))

    let decommission t rep_id =
      perform_change t
        (Core.Config.decommission rep_id)
        (fun u -> Decommission (u, rep_id))

    let promote t rep_id =
      perform_change t
        (Core.Config.promote rep_id)
        (fun u -> Promote (u, rep_id))

    let demote t rep_id =
      perform_change t
        (Core.Config.demote rep_id)
        (fun u -> Demote (u, rep_id))

    let replace t ~replacee ~failover =
      perform_change t
        (Core.Config.replace ~replacee ~failover)
        (fun u -> Replace (u, replacee, failover))
  end
end

module type OP =
sig
  type op
  val string_of_op : op -> string
  val op_of_string : string -> op
end

module Simple_IO(OP : OP) =
struct
  module EC = Extprot.Conv

  type address = Unix.sockaddr
  type op = OP.op

  type connection = Lwt_io.input_channel * Lwt_io.output_channel

  module M = Map.Make(String)

  type conn_manager =
      {
        id            : string;
        sock          : Lwt_unix.file_descr;
        mutable conns : connection M.t;
        conn_signal   : unit Lwt_condition.t;
      }

  let make id addr =
    let sock = Lwt_unix.(socket (Unix.domain_of_sockaddr addr) Unix.SOCK_STREAM 0) in
      Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock addr;
      Lwt_unix.listen sock 256;

      let rec accept_loop t =
        lwt (fd, addr) = Lwt_unix.accept sock in
          ignore begin try_lwt
            (* the following are not supported for ADDR_UNIX sockets, so catch
             * possible exceptions *)
            (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
            (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
            let ich = Lwt_io.of_fd Lwt_io.input fd in
            let och = Lwt_io.of_fd Lwt_io.output fd in
            lwt id  = Lwt_io.read_line ich in
              t.conns <- M.add id (ich, och) t.conns;
              Lwt_condition.broadcast t.conn_signal ();
              return ()
          with _ ->
            Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL;
            Lwt_unix.close fd
          end;
          accept_loop t in

      let conn_signal = Lwt_condition.create () in
      let t           = { id; sock; conn_signal; conns = M.empty; } in
        ignore begin
          try_lwt
            accept_loop t
          with
            | Exit -> return ()
            | exn -> Lwt_log.error_f ~exn ~section
                       "Error in connection manager accept loop"
        end;
        t

  let connect t dst_id addr =
    match M.Exceptionless.find dst_id t.conns with
        Some _ as x -> return x
      | None when dst_id < t.id -> (* wait for other end to connect *)
          let rec await_conn () =
            match M.Exceptionless.find dst_id t.conns with
                Some _ as x -> return x
              | None -> Lwt_condition.wait t.conn_signal >>
                       await_conn ()
          in
            await_conn ()
      | None -> (* we must connect ourselves *)
          try_lwt
            lwt ich, och = Lwt_io.open_connection addr in
            Lwt_io.write och (t.id ^ "\n") >>
            Lwt_io.flush och >>
              return (Some (ich, och))
          with _ -> return_none

  open Oraft_proto
  open Raft_message
  open Oraft

  let wrap_msg : _ Oraft.Types.message -> Raft_message.raft_message = function
      Request_vote { term; candidate_id; last_log_index; last_log_term; } ->
        Request_vote { Request_vote.term; candidate_id;
                       last_log_index; last_log_term; }
    | Vote_result { term; vote_granted; } ->
        Vote_result { Vote_result.term; vote_granted; }
    | Ping { term; n } -> Ping { Ping_msg.term; n; }
    | Pong { term; n } -> Pong { Ping_msg.term; n; }
    | Append_result { term; result; } ->
        Append_result { Append_result.term; result }
    | Append_entries { term; leader_id; prev_log_index; prev_log_term;
                       entries; leader_commit; } ->
        let map_entry = function
            (index, (Nop, term)) -> (index, Entry.Nop, term)
          | (index, (Config c, term)) -> (index, Entry.Config c, term)
          | (index, (Op (req_id, x), term)) ->
              (index, Entry.Op (req_id, OP.string_of_op x), term) in

        let entries = List.map map_entry entries in
          Append_entries { Append_entries.term; leader_id; prev_log_index;
                           prev_log_term; entries; leader_commit; }

  let unwrap_msg : Raft_message.raft_message -> _ Oraft.Types.message = function
    | Request_vote { Request_vote.term; candidate_id; last_log_index;
                     last_log_term } ->
        Request_vote { term; candidate_id; last_log_index; last_log_term }
    | Vote_result { Vote_result.term; vote_granted; } ->
        Vote_result { term; vote_granted; }
    | Ping { Ping_msg.term; n } -> Ping { term; n; }
    | Pong { Ping_msg.term; n } -> Pong { term; n; }
    | Append_result { Append_result.term; result; } ->
        Append_result { term; result }
    | Append_entries { Append_entries.term; leader_id;
                       prev_log_index; prev_log_term;
                       entries; leader_commit; } ->
        let map_entry = function
          | (index, Entry.Nop, term) -> (index, (Nop, term))
          | (index, Entry.Config c, term) -> (index, (Config c, term))
          | (index, Entry.Op (req_id, x), term) ->
              let op = OP.op_of_string x in
                (index, (Op (req_id, op), term))
        in
          Append_entries
            { term; leader_id; prev_log_index; prev_log_term;
              entries = List.map map_entry entries;
              leader_commit;
            }

  let send (_, och) msg =
    let payload = EC.serialize Raft_message.write (wrap_msg msg) in
      Lwt_io.atomic
        (fun och ->
           Lwt_io.LE.write_int och (String.length payload) >>
           Lwt_io.write och payload)
        och

  let receive (ich, _) =
    try_lwt
      Lwt_io.atomic
        (fun ich ->
           lwt len = Lwt_io.LE.read_int ich in
           let s   = String.create len in
           lwt ()  = Lwt_io.read_into_exactly ich s 0 len in
             return (Some (EC.deserialize Raft_message.read s |> unwrap_msg)))
        ich
    with _ ->
      Lwt_io.abort ich >>
      return None

  let abort (ich, och) =
    Lwt_io.abort ich

  type snapshot_transfer = unit

  let prepare_snapshot conn index config = return None
  let send_snapshot () = return false
end

module Simple_server(OP : OP) =
  Make_server(Simple_IO(OP))
