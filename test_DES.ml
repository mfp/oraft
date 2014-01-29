open Printf

module List   = BatList
module Map    = BatMap
module Int64  = BatInt64
module Option = BatOption
module RND    = Random.State
module C      = Oraft.Core

module CLOCK =
struct
  include BatInt64
  type delta = t
end

module type EVENT_QUEUE =
sig
  open Oraft.Types
  type 'a t

  val create     : unit -> 'a t
  val schedule   : 'a t -> CLOCK.delta -> rep_id -> 'a -> CLOCK.t
  val next       : 'a t -> (CLOCK.t * rep_id * 'a) option
  val is_empty   : 'a t -> bool
end

module DES :
sig
  open Oraft.Types

  module Event_queue : EVENT_QUEUE

  type ('op, 'snapshot) event =
    | Election_timeout
    | Heartbeat_timeout
    | Command of 'op
    | Message of rep_id * 'op message
    | Func of (CLOCK.t -> unit)
    | Install_snapshot of rep_id * 'snapshot * term * index * config
    | Snapshot_sent of rep_id * index

  type ('op, 'app_state, 'snapshot) t
  type ('op, 'app_state) node

  val make :
    ?rng:RND.t ->
    ?ev_queue:('op, 'snapshot) event Event_queue.t ->
    num_nodes:int ->
    election_period:CLOCK.t ->
    heartbeat_period:CLOCK.t ->
    rtt:CLOCK.t ->
    (unit -> 'app_state) -> ('op, 'app_state, 'snapshot) t

  val random_node_id : (_, _, _) t -> rep_id
  val node_ids       : (_, _, _) t -> rep_id list
  val nodes          : ('op, 'app_state, _) t -> ('op, 'app_state) node list

  val node_id       : (_, _) node -> rep_id
  val app_state     : (_, 'app_state) node -> 'app_state
  val set_app_state : (_, 'app_state) node -> 'app_state -> unit
  val leader_id     : (_, _) node -> rep_id option

  val simulate :
    ?verbose:bool ->
    ?string_of_cmd:('op -> string) ->
    msg_loss_rate:float ->
    on_apply:(time:Int64.t -> ('op, 'app_state) node ->
              (index * 'op * term) list ->
              [`Snapshot of index * 'app_state |
               `State of 'app_state]) ->
    take_snapshot:(('op, 'app_state) node -> index * 'snapshot * term) ->
    install_snapshot:(('op, 'app_state) node -> 'snapshot -> unit) ->
     ?steps:int -> ('op, 'app_state, 'snapshot) t -> int
end =
struct
  open Oraft.Types

  module Event_queue =
  struct
    type rep_id = string
    module type PACK =
    sig
      type elm
      module M : BatHeap.H with type elem = elm
      val h : M.t ref
      val t : CLOCK.t ref
    end

    type 'a m = (module PACK with type elm = 'a)
    type 'a t = (Int64.t * rep_id * 'a) m

    let create (type a) () : a t =
      let module P =
        struct
          type elm = Int64.t * rep_id * a
          module M = BatHeap.Make(struct
                                    type t = elm
                                    let compare (c1, _, _) (c2, _, _) =
                                      Int64.compare c1 c2
                                  end)
          let h = ref M.empty
          let t = ref 0L
        end
      in
        (module P)

    let schedule (type a) ((module P) : a t) dt node (ev : a) =
      let t = CLOCK.(!P.t + dt) in
        P.h := P.M.add (t, node, ev) !P.h;
        t

    let is_empty (type a) ((module P) : a t) = P.M.size !P.h = 0

    let next (type a) ((module P) : a t) =
      try
        let (t, _, _) as x = P.M.find_min !P.h in
          P.h := P.M.del_min !P.h;
          P.t := t;
          Some x
      with Invalid_argument _ -> None
  end

  type ('op, 'snapshot) event =
    | Election_timeout
    | Heartbeat_timeout
    | Command of 'op
    | Message of rep_id * 'op message
    | Func of (CLOCK.t -> unit)
    | Install_snapshot of rep_id * 'snapshot * term * index * config
    | Snapshot_sent of rep_id * index

  type ('op, 'app_state) node =
      {
        id                     : rep_id;
        mutable state          : 'op C.state;
        mutable next_heartbeat : CLOCK.t option;
        mutable next_election  : CLOCK.t option;
        mutable app_state      : 'app_state;
      }

  type ('op, 'app_state, 'snapshot) t =
      {
        rng              : RND.t;
        ev_queue         : ('op, 'snapshot) event Event_queue.t;
        nodes            : ('op, 'app_state) node array;
        election_period  : CLOCK.t;
        heartbeat_period : CLOCK.t;
        rtt              : CLOCK.t;
      }

  let make_node mk_app_state config id =
    let state     = C.make
                      ~id ~current_term:0L ~voted_for:None
                      ~log:[] ~config () in
    let app_state = mk_app_state () in
      { id; state; next_heartbeat = None; next_election = None; app_state; }

  let node_id n         = n.id
  let leader_id n       = C.leader_id n.state
  let app_state n       = n.app_state
  let set_app_state n x = n.app_state <- x

  let make
        ?(rng = RND.make_self_init ())
        ?(ev_queue = Event_queue.create ())
        ~num_nodes
        ~election_period ~heartbeat_period ~rtt
        mk_app_state =
    let node_ids = Array.init num_nodes (sprintf "n%02d") in
    let nodes    = Array.map (make_node mk_app_state node_ids) node_ids in
      { rng; ev_queue; election_period; heartbeat_period; rtt; nodes; }

  let random_node_id t =
    t.nodes.(RND.int t.rng (Array.length t.nodes)).id

  let node_ids t = Array.(to_list (map (fun n -> n.id) t.nodes))

  let nodes t = Array.to_list t.nodes

  let string_of_msg string_of_cmd = function
      Request_vote { term; candidate_id; last_log_term; last_log_index; _ } ->
        sprintf "Request_vote %S last_term:%Ld last_index:%Ld @ %Ld"
          candidate_id last_log_term last_log_index term
    | Vote_result { term; vote_granted } ->
        sprintf "Vote_result %b @ %Ld" vote_granted term
    | Append_entries { term; prev_log_index; prev_log_term; entries; _ } ->
        let string_of_entry = function
            Nop -> "Nop"
          | Op cmd -> "Op " ^ Option.default (fun _ -> "<cmd>") string_of_cmd cmd in
        let payload_desc =
          entries |>
          List.map
            (fun (index, (entry, term)) ->
               sprintf "(%Ld, %s, %Ld)" index
                 (string_of_entry entry) term) |>
          String.concat ", "
        in
          sprintf "Append_entries (%Ld, %Ld, [%s]) @ %Ld"
            prev_log_index prev_log_term payload_desc term
    | Append_result { term; success; prev_log_index; last_log_index; _ } ->
        sprintf "Append_result %b %Ld -- %Ld @ %Ld"
          success prev_log_index last_log_index term

  let describe_event string_of_cmd = function
      Election_timeout -> "Election_timeout"
    | Heartbeat_timeout -> "Heartbeat_timeout"
    | Command cmd -> sprintf "Command %s"
                       (Option.default  (fun _ -> "<cmd>") string_of_cmd cmd)
    | Message (rep_id, msg) ->
        sprintf "Message (%S, %s)" rep_id (string_of_msg string_of_cmd msg)
    | Func _ -> "Func _"
    | Install_snapshot (src, _, term, index, config) ->
        sprintf "Install_snapshot (%S, _, %Ld, %Ld, _)" src term index
    | Snapshot_sent (dst, idx) -> sprintf "Snapshot_sent %S last_index:%Ld" dst idx

  let schedule_election t node =
    let dt = CLOCK.(t.election_period - t.election_period / 4L +
                    of_int (RND.int t.rng (to_int t.election_period lsr 2))) in
    let t1 = Event_queue.schedule t.ev_queue dt node.id Election_timeout in
      node.next_election <- Some t1

  let schedule_heartbeat t node =
    let t1 = Event_queue.schedule
               t.ev_queue t.heartbeat_period node.id Heartbeat_timeout
    in
      node.next_heartbeat <- Some t1

  let unschedule_election t node =
    node.next_election <- None

  let unschedule_heartbeat t node =
    node.next_heartbeat <- None

  let send_cmd t node_id cmd =
    ignore (Event_queue.schedule t.ev_queue
              200L node_id (Command cmd))

  let must_account time node = function
      Election_timeout -> begin match node.next_election with
          Some t when t = time -> true
        | _ -> false
      end
    | Heartbeat_timeout -> begin match node.next_heartbeat with
          Some t when t = time -> true
        | _ -> false
      end
    | Func _ -> false
    | Command _ | Message _ | Install_snapshot _ | Snapshot_sent _ -> true

  let simulate
        ?(verbose = false) ?string_of_cmd ~msg_loss_rate
        ~on_apply ~take_snapshot ~install_snapshot ?(steps = max_int) t =

    let node_of_id =
      let h = Hashtbl.create 13 in
        Array.iter (fun node -> Hashtbl.add h node.id node) t.nodes;
        (fun node_id -> Hashtbl.find h node_id) in

    let send_cmd ?(dst = random_node_id t) cmd =
      send_cmd t dst cmd in

    let react_to_event time node ev =
      let considered = must_account time node ev in
      let () =
        if considered && verbose then
          printf "%Ld @ %s -> %s\n" time node.id (describe_event string_of_cmd ev) in

      let s, actions = match ev with
          Election_timeout -> begin
            match node.next_election with
                Some t when t = time -> C.election_timeout node.state
              | _ -> (node.state, [])
          end
        | Heartbeat_timeout -> begin
            match node.next_heartbeat with
              | Some t when t = time -> C.heartbeat_timeout node.state
              | _ -> (node.state, [])
          end
        | Command c -> C.client_command c node.state
        | Message (peer, msg) -> C.receive_msg node.state peer msg
        | Func f ->
            f time;
            (node.state, [])
        | Install_snapshot (src, snapshot, last_term, last_index, config) ->
            let s, accepted = C.install_snapshot
                                ~last_term ~last_index ~config node.state
            in
              if accepted then begin
                let _ = Event_queue.schedule t.ev_queue 40L src
                          (Snapshot_sent (node.id, last_index))
                in
                  install_snapshot node snapshot
              end;
              (s, [])
        | Snapshot_sent (peer, last_index) ->
            C.snapshot_sent peer ~last_index node.state
      in

      let rec exec_action = function
          Apply cmds ->
            if verbose then
              printf " Apply %d cmds [%s]\n"
                (List.length cmds)
                (List.map
                   (fun (idx, cmd, term) ->
                      sprintf "(%Ld, %s, %Ld)"
                        idx
                        (Option.default (fun _ -> "<cmd>") string_of_cmd cmd)
                        term)
                   cmds |>
                 String.concat ", ");
            (* simulate current leader being cached by client *)
            begin match on_apply ~time node cmds with
                `Snapshot (last_index, app_state) ->
                  node.app_state <- app_state;
                  node.state <- C.compact_log last_index node.state
              | `State app_state ->
                  node.app_state <- app_state
            end
        | Become_candidate ->
            if verbose then printf " Become_candidate\n";
            unschedule_heartbeat t node;
            exec_action Reset_election_timeout
        | Become_follower None ->
            if verbose then printf " Become_follower\n";
            unschedule_heartbeat t node;
            exec_action Reset_election_timeout
        | Become_follower (Some leader) ->
            if verbose then printf " Become_follower %S\n" leader;
            unschedule_heartbeat t node;
            exec_action Reset_election_timeout
        | Become_leader ->
            if verbose then printf " Become_leader\n";
            unschedule_election t node;
            schedule_heartbeat t node
        | Redirect (Some leader, cmd) ->
            if verbose then printf " Redirect %s\n" leader;
            send_cmd ~dst:leader cmd
        | Redirect (None, cmd) ->
            if verbose then printf " Redirect\n";
            (* send to a random server *)
            send_cmd cmd
        | Reset_election_timeout ->
            if verbose then printf " Reset_election_timeout\n";
            unschedule_election t node;
            schedule_election t node
        | Reset_heartbeat ->
            if verbose then printf " Reset_heartbeat\n";
            unschedule_heartbeat t node;
            schedule_heartbeat t node
        | Send (rep_id, msg) ->
            (* drop message with probability msg_loss_rate *)
            let dropped = RND.float t.rng 1.0 <= msg_loss_rate in
              if verbose then
                printf " Send to %S <- %s%s\n" rep_id
                  (string_of_msg string_of_cmd msg)
                  (if dropped then " DROPPED" else "");
              if not dropped then begin
                let dt = Int64.(t.rtt - t.rtt / 4L +
                                of_int (RND.int t.rng (to_int t.rtt lsr 1)))
                in
                  ignore (Event_queue.schedule t.ev_queue dt rep_id
                            (Message (node.id, msg)))
              end
        | Send_snapshot (dst, idx, config) ->
            if verbose then
              printf " Send_snapshot (%S, %Ld)\n" dst idx;
            let last_index, snapshot, last_term = take_snapshot node in
            let dt = Int64.(t.rtt - t.rtt / 4L +
                       of_int (RND.int t.rng (to_int t.rtt lsr 1)))
            in
              ignore begin
                Event_queue.schedule t.ev_queue dt dst
                  (Install_snapshot
                     (node.id, snapshot, last_term, last_index, config))
              end

      in
        node.state <- s;
        List.iter exec_action actions;
        considered
    in

    let steps = ref 0 in
      (* schedule initial election timeouts *)
      Array.iter (schedule_election t) t.nodes;

      try
        let rec loop () =
          match Event_queue.next t.ev_queue with
              None -> !steps
            | Some (time, rep_id, ev) ->
                if react_to_event time (node_of_id rep_id) ev then incr steps;
                loop ()
        in loop ()
      with Exit -> !steps
end

module FQueue :
sig
  type 'a t

  val empty : 'a t
  val push : 'a -> 'a t -> 'a t
  val length : 'a t -> int
  val to_list : 'a t -> 'a list
end =
struct
  type 'a t = int * 'a list

  let empty          = (0, [])
  let push x (n, l)  = (n + 1, x :: l)
  let length (n, _)  = n
  let to_list (_, l) = List.rev l
end

let run ?(seed = 2) ?(verbose=false) () =
  let module S = Set.Make(String) in

  let completed = ref S.empty in
  let num_nodes = 3 in
  let num_cmds  = 100_000 in
  let init_cmd  = 1 in
  let last_sent = ref init_cmd in
  let ev_queue  = DES.Event_queue.create () in

  let election_period  = 800L in
  let heartbeat_period = 200L in
  let rtt              = 50L in
  let msg_loss_rate    = 0.01 in
  let batch_size       = 20 in

  let retry_period = CLOCK.(4L * election_period) in

  let applied = BatBitSet.create (2 * num_cmds) (* work around BatBitSet bug *) in

  let rng     = Random.State.make [| seed |] in

  let des     = DES.make ~ev_queue ~rng ~num_nodes
                  ~election_period ~heartbeat_period ~rtt
                  (fun () -> (0L, FQueue.empty, 0L)) in

  let rec schedule dt node cmd =
    let _    = DES.Event_queue.schedule ev_queue dt node (DES.Command cmd) in
    (* after the retry_period, check if the cmd has been executed
     * and reschedule if needed *)
    let f _  = if not (BatBitSet.mem applied cmd) then schedule 100L node cmd in
    let _    = DES.Event_queue.schedule ev_queue
                 CLOCK.(dt + retry_period) node (DES.Func f)
    in () in

  let check_if_finished node len =
    if len >= num_cmds then begin
      completed := S.add (DES.node_id node) !completed;
      if S.cardinal !completed >= num_nodes then
        raise Exit
    end in

  let apply_one ~time node acc (index, cmd, term) =
    if cmd mod (if verbose then 1 else 10_000) = 0 then
      printf "XXXXXXXXXXXXX apply %S  cmd:%d index:%Ld term:%Ld @ %Ld\n%!"
        (DES.node_id node) cmd index term time;
    let q    = match acc with | `Snapshot (_, (_, q, _)) | `State (_, q, _) -> q in
    let id   = DES.node_id node in
    let q    = FQueue.push cmd q in
    let len  = FQueue.length q in
      BatBitSet.set applied cmd;
      if cmd >= !last_sent then begin
      (* We schedule the next few commands being sent to the current leader
       * (simulating the client caching the current leader).  *)
        let dt  = CLOCK.(heartbeat_period - 10L) in
        let dst = Option.default id (DES.leader_id node) in
          for i = 1 to batch_size do
            incr last_sent;
            let cmd = !last_sent in
              schedule CLOCK.(of_int i * dt) dst cmd
          done
      end;
      check_if_finished node len;
      if cmd mod 10 = 0 then begin
        if verbose then
          printf "XXXXX snapshot %S %Ld (last cmds: %s)\n" id index
            (FQueue.to_list q |> List.rev |> List.take 5 |> List.rev |>
             List.map string_of_int |> String.concat ", ");
        `Snapshot (index, (index, q, term))
      end else
        `State (index, q, term)
  in

  let on_apply ~time node cmds =
    List.fold_left (apply_one ~time node) (`State (DES.app_state node)) cmds in

  let take_snapshot node =
    if verbose then printf "TAKE SNAPSHOT at %S\n" (DES.node_id node);
    let (index, _, term) as snapshot = DES.app_state node in
      (index, snapshot, term) in

  let install_snapshot node ((last_index, q, last_term) as snapshot) =
    if verbose then
      printf "INSTALL SNAPSHOT at %S last_index:%Ld last_term:%Ld\n"
        (DES.node_id node) last_index last_term;
    DES.set_app_state node snapshot;
    check_if_finished node (FQueue.length q)
  in

  (* schedule init cmd delivery *)
  let ()    = schedule 1000L (DES.random_node_id des) init_cmd in
  let t0    = Unix.gettimeofday () in
  let steps = DES.simulate
                ~verbose ~string_of_cmd:string_of_int
                ~msg_loss_rate
                ~on_apply ~take_snapshot ~install_snapshot
                des in
  let dt    = Unix.gettimeofday () -. t0 in
  let ncmds = DES.nodes des |>
              List.map
                (fun node ->
                   let _, q, _ = DES.app_state node in
                     FQueue.length q) |>
              List.fold_left min max_int in
  let logs  = DES.nodes des |>
              List.map
                (fun node ->
                   let _, q, _ = DES.app_state node in
                     FQueue.to_list q |> List.take ncmds) in
  let ok, _ = List.fold_left
                (fun (ok, l) l' -> match l with
                     None -> (ok, Some l')
                   | Some l -> (ok && l = l', Some l'))
                (true, None)
                logs
    in
      printf "%d commands\n" ncmds;
      printf "Simulated %d steps (%4.2f steps/cmd, %.0f steps/s, %.0f cmds/s).\n"
        steps (float steps /. float ncmds)
        (float steps /. dt) (float ncmds /. dt);
      if ok then
        print_endline "OK"
      else begin
        print_endline "FAILURE: replicated logs differ";
        List.iteri
          (fun n l ->
             let all_eq, _ = List.fold_left
                               (fun (b, prev) x -> match prev with
                                    None -> (true, Some x)
                                  | Some x' -> (b && x = x', Some x))
                               (true, None) l in
             let desc      = if all_eq then "" else " DIFF" in
             let s         = List.map string_of_int l |> String.concat "    " in
               printf "%8d %s%s\n" n s desc)
          (List.transpose logs);
        exit 1;
      end

let () =
  for i = 1 to 100 do
    print_endline "";
    printf "Running with seed %d\n%!" i;
    run ~seed:i ~verbose:false ()
  done
