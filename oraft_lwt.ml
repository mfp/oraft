open Printf
open Lwt
open Oraft
open Oraft.Types
open Oraft_util

module Map    = BatMap
module List   = BatList
module Option = BatOption

module REPID = struct type t = rep_id let compare = String.compare end
module RM = Map.Make(REPID)

module type LWTIO =
sig
  type address
  type op
  type connection

  val connect : address -> connection option Lwt.t
  val send    : connection -> (req_id * op) message -> bool Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t

  type snapshot_transfer

  val prepare_snapshot :
    connection -> index -> config -> snapshot_transfer option Lwt.t

  val send_snapshot : snapshot_transfer -> unit Lwt.t
end

module type LWTPROC =
sig
  type op
  type resp

  val execute : op -> (resp, exn) result Lwt.t
end

module Make_server
  (PROC : LWTPROC)
  (IO : LWTIO with type op = PROC.op) =
struct
  module S    = Set.Make(String)
  module CMDM = Map.Make(struct
                           type t = req_id
                           let compare = compare
                         end)

  type server =
      {
        peers                    : IO.address RM.t;
        peer_ids                 : rep_id list;
        election_period          : float;
        heartbeat_period         : float;
        mutable next_req_id      : Int64.t;
        mutable conns            : IO.connection RM.t;
        mutable state            : (req_id * PROC.op) Core.state;
        mutable running          : bool;
        mutable msg_threads      : th_res Lwt.t list;
        mutable election_timeout : th_res Lwt.t;
        mutable heartbeat        : th_res Lwt.t;
        mutable abort            : th_res Lwt.t * th_res Lwt.u;
        mutable get_cmd          : th_res Lwt.t;
        push_cmd                 : (req_id * PROC.op) -> unit;
        cmd_stream               : (req_id * PROC.op) Lwt_stream.t;
        mutable pending_cmds     : (cmd_res Lwt.t * cmd_res Lwt.u) CMDM.t;
        leader_signal            : unit Lwt_condition.t;
        snapshot_sent_stream     : rep_id Lwt_stream.t;
        mutable snapshots_sent : th_res Lwt.t;
        snapshot_sent            : (rep_id -> unit);
      }

  and th_res =
      Message of rep_id * (req_id * PROC.op) message
    | Client_command of req_id * PROC.op
    | Abort
    | Election_timeout
    | Heartbeat_timeout
    | Snapshots_sent of rep_id list

  and cmd_res =
      Redirect of rep_id option
    | Executed of (PROC.resp, exn) result

  type result =
      [ `OK of PROC.resp
      | `Error of exn
      | `Redirect of rep_id * IO.address
      | `Redirect_randomized of rep_id * IO.address
      | `Retry_later ]

  let get_sent_snapshots stream =
    match_lwt Lwt_stream.get stream with
        None -> fst (Lwt.wait ())
      | Some peer ->
          let l = Lwt_stream.get_available stream in
            Lwt_stream.njunk (List.length l) stream >>
            return (Snapshots_sent (peer :: l))

  let make
        ?(election_period = 2.)
        ?(heartbeat_period = election_period /. 2.)
        state peers =
    let stream, push      = Lwt_stream.create () in
    let push x            = push (Some x) in
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
    let snapshot_sent_stream,
        push_snapshot_ok  = Lwt_stream.create () in
    let snapshots_sent    = get_sent_snapshots snapshot_sent_stream

    in
      {
        heartbeat_period;
        election_period;
        state;
        election_timeout;
        heartbeat;
        snapshot_sent_stream;
        snapshots_sent;
        peers         = List.fold_left
                          (fun m (k, v) -> RM.add k v m) RM.empty
                          (List.filter (fun (id, _) -> id <> Core.id state) peers);
        peer_ids      = List.filter_map
                          (function
                             | (id, _) when id <> Core.id state -> Some id
                             | _ -> None)
                          peers;
        next_req_id   = 42L;
        conns         = RM.empty;
        running       = true;
        msg_threads   = [];
        abort         = Lwt.task ();
        get_cmd       = (match_lwt Lwt_stream.get stream with
                           | None -> fst (Lwt.wait ())
                           | Some (req_id, op) ->
                               return (Client_command (req_id, op)));
        push_cmd      = push;
        cmd_stream    = stream;
        pending_cmds  = CMDM.empty;
        leader_signal = Lwt_condition.create ();
        snapshot_sent = (fun x -> push_snapshot_ok (Some x));
      }

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
          match_lwt IO.connect addr with
            | None -> Lwt_unix.sleep 0.1 >> make_thread (n - 1)
            | Some conn ->
                t.conns <- RM.add peer conn t.conns;
                match_lwt IO.receive conn with
                    None -> Lwt_unix.sleep 0.1 >> make_thread 5
                  | Some msg -> return (Message (peer, msg))
    in
      make_thread 5

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
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t Reset_election_timeout
    | Become_follower (Some _) ->
        Lwt_condition.broadcast t.leader_signal ();
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t Reset_election_timeout
    | Become_leader ->
        Lwt_condition.broadcast t.leader_signal ();
        t.election_timeout  <- fst (Lwt.wait ());
        exec_action t Reset_heartbeat
    | Apply l ->
        Lwt_list.iter_s
          (fun (index, (req_id, op), term) ->
             (* TODO: allow to run this in parallel with rest RAFT algorithm.
              * Must make sure that Apply actions are not reordered. *)
             lwt resp = try_lwt PROC.execute op
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
          lwt _ = IO.send (RM.find rep_id t.conns) msg in
            return ()
        with _ ->
          (* cannot send -- partition *)
          return ()
        end;
        return ()
    | Send_snapshot (rep_id, idx, config) ->
        ignore begin
          try_lwt
            match_lwt IO.prepare_snapshot (RM.find rep_id t.conns) idx config with
              | None -> return ()
              | Some transfer -> IO.send_snapshot transfer
          finally
            t.snapshot_sent rep_id;
            return ()
        end;
        return ()

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
               t.heartbeat;
               t.snapshots_sent;
             ] @
             t.msg_threads)
        with
          | Abort -> t.running <- false; return ()
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
          | Snapshots_sent peers ->
              let state, actions =
                List.fold_left
                  (fun (s, actions) peer ->
                     let s, actions' = Core.snapshot_sent peer s in
                       (s, actions' @ actions))
                  (t.state, [])
                  peers
              in
                t.state <- state;
                exec_actions t actions >>
                run t

  let gen_req_id t =
    let id = t.next_req_id in
      t.next_req_id <- Int64.succ id;
      (Core.id t.state, id)

  let rec execute t cmd =
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
          execute t cmd
      | Leader, _ ->
          let req_id = gen_req_id t in
          let task   = Lwt.task () in
            t.pending_cmds <- CMDM.add req_id task t.pending_cmds;
            t.push_cmd (req_id, cmd);
            match_lwt fst task with
                Executed res -> return (res :> result)
              | Redirect _ -> execute t cmd
end
