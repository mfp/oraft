open Printf

module List   = BatList
module Map    = BatMap
module Int64  = BatInt64
module Option = BatOption
module RND    = Random.State

module CLOCK = Int64

module type EVENT_QUEUE =
sig
  open Oraft.Types
  type 'a t

  val create     : unit -> 'a t
  val schedule   : 'a t -> CLOCK.t -> rep_id -> 'a -> unit
  val next       : 'a t -> (CLOCK.t * rep_id * 'a) option
  val is_empty   : 'a t -> bool
end

module DES :
sig
  open Oraft.Types

  module Event_queue : EVENT_QUEUE

  type 'a event =
    | Election_timeout
    | Heartbeat_timeout
    | Command of 'a
    | Message of rep_id * 'a message
    | Func of (CLOCK.t -> unit)

  type 'a t

  val make :
    ?rng:RND.t ->
    ?ev_queue:'a event Event_queue.t ->
    num_nodes:int ->
    election_period:CLOCK.t ->
    heartbeat_period:CLOCK.t ->
    rtt:CLOCK.t ->
    unit -> 'a t

  val random_node_id : 'a t -> rep_id
  val node_ids       : 'a t -> rep_id list

  val simulate :
    ?verbose:bool ->
    msg_loss_rate:float ->
    (time:Int64.t ->
     leader:Oraft.Types.rep_id option ->
     Oraft.Types.rep_id -> 'a -> unit) ->
     ?steps:int -> 'a t -> int
end =
struct
  module C   = Oraft.Core

  open Oraft.Types

  module Event_queue =
  struct
    type rep_id = string
    module type PACK =
    sig
      type elm
      module M : BatHeap.H with type elem = elm
      val h : M.t ref
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
        end
      in
        (module P)

    let schedule (type a) ((module P) : a t) t node (ev : a) =
      P.h := P.M.add (t, node, ev) !P.h

    let is_empty (type a) ((module P) : a t) = P.M.size !P.h = 0

    let next (type a) ((module P) : a t) =
      try
        let x = P.M.find_min !P.h in
          P.h := P.M.del_min !P.h;
          Some x
      with Invalid_argument _ -> None
  end

  type 'a event =
    | Election_timeout
    | Heartbeat_timeout
    | Command of 'a
    | Message of rep_id * 'a message
    | Func of (CLOCK.t -> unit)

  type 'a node =
      {
        id                     : rep_id;
        mutable state          : 'a C.state;
        mutable next_heartbeat : CLOCK.t option;
        mutable next_election  : CLOCK.t option;
      }

  type 'a t =
      {
        rng              : RND.t;
        ev_queue         : 'a event Event_queue.t;
        mutable clock    : CLOCK.t;
        nodes            : 'a node array;
        election_period  : CLOCK.t;
        heartbeat_period : CLOCK.t;
        rtt              : CLOCK.t;
      }

  let make_node peers id =
    let state = C.make
                  ~id ~current_term:0L ~voted_for:None
                  ~log:[] ~peers ()
    in
      { id; state; next_heartbeat = None; next_election = None }

  let make
        ?(rng = RND.make_self_init ())
        ?(ev_queue = Event_queue.create ())
        ~num_nodes
        ~election_period ~heartbeat_period ~rtt
        () =
    let clock    = 0L in
    let node_ids = Array.init num_nodes (sprintf "n%02d") in
    let nodes    = Array.map (make_node node_ids) node_ids in
      { rng; ev_queue; clock; election_period; heartbeat_period; rtt; nodes; }

  let random_node_id t =
    t.nodes.(RND.int t.rng (Array.length t.nodes)).id

  let node_ids t = Array.(to_list (map (fun n -> n.id) t.nodes))

  let string_of_msg = function
      Request_vote { term; candidate_id; last_log_term; last_log_index; _ } ->
        sprintf "Request_vote %S last_term:%Ld last_index:%Ld @ %Ld"
          candidate_id last_log_term last_log_index term
    | Vote_result { term; vote_granted } ->
        sprintf "Vote_result %b @ %Ld" vote_granted term
    | Append_entries { term; prev_log_index; prev_log_term; entries; _ } ->
        sprintf "Append_entries (%Ld, %Ld, [%d]) @ %Ld"
          prev_log_index prev_log_term (List.length entries) term
    | Append_result { term; success; prev_log_index; last_log_index; _ } ->
        sprintf "Append_result %b %Ld -- %Ld @ %Ld"
          success prev_log_index last_log_index term

  let describe_event = function
      Election_timeout -> "Election_timeout"
    | Heartbeat_timeout -> "Heartbeat_timeout"
    | Command cmd -> "Command"
    | Message (rep_id, msg) ->
        sprintf "Message (%S, %s)" rep_id (string_of_msg msg)
    | Func _ -> "Func _"

  let schedule_election t node =
    let dt = Int64.(t.election_period - t.election_period / 4L +
                    of_int (RND.int t.rng (to_int t.election_period lsr 2))) in
    let t1 = Int64.(t.clock + dt) in
      node.next_election <- Some t1;
      Event_queue.schedule t.ev_queue t1 node.id Election_timeout

  let schedule_heartbeat t node =
    let t1 = Int64.(t.clock + t.heartbeat_period) in
      node.next_heartbeat <- Some t1;
      Event_queue.schedule t.ev_queue t1 node.id Heartbeat_timeout

  let unschedule_election t node =
    node.next_election <- None

  let unschedule_heartbeat t node =
    node.next_heartbeat <- None

  let send_cmd t node_id cmd =
    Event_queue.schedule t.ev_queue
      Int64.(t.clock + of_int (RND.int t.rng 100)) node_id (Command cmd)

  let simulate ?(verbose = false) ~msg_loss_rate on_apply
               ?(steps = max_int) t =

    let node_of_id =
      let h = Hashtbl.create 13 in
        Array.iter (fun node -> Hashtbl.add h node.id node) t.nodes;
        (fun node_id -> Hashtbl.find h node_id) in

    let send_cmd ?(dst = random_node_id t) cmd =
      send_cmd t dst cmd in

    let react_to_event time node ev =
      if verbose then
        printf "%Ld @ %s -> %s\n" time node.id (describe_event ev);

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
            (node.state, []) in

      let rec exec_action = function
          `Apply cmd ->
            if verbose then printf " Apply\n";
            (* simulate current leader being cached by client *)
            on_apply ~time ~leader:(C.leader_id node.state) node.id cmd
        | `Become_candidate ->
            if verbose then printf " Become_candidate\n";
            unschedule_heartbeat t node;
            exec_action `Reset_election_timeout
        | `Become_follower None ->
            if verbose then printf " Become_follower\n";
            unschedule_heartbeat t node;
            exec_action `Reset_election_timeout
        | `Become_follower (Some leader) ->
            if verbose then printf " Become_follower %S\n" leader;
            unschedule_heartbeat t node;
            exec_action `Reset_election_timeout
        | `Become_leader ->
            if verbose then printf " Become_leader\n";
            unschedule_election t node;
            schedule_heartbeat t node
        | `Redirect (Some leader, cmd) ->
            if verbose then printf " Redirect %s\n" leader;
            send_cmd ~dst:leader cmd
        | `Redirect (None, cmd) ->
            if verbose then printf " Redirect\n";
            (* send to a random server *)
            send_cmd cmd
        | `Reset_election_timeout ->
            if verbose then printf " Reset_election_timeout\n";
            unschedule_election t node;
            schedule_election t node
        | `Reset_heartbeat ->
            if verbose then printf " Reset_heartbeat\n";
            unschedule_heartbeat t node;
            schedule_heartbeat t node
        | `Send (rep_id, msg) ->
            if verbose then
              printf " Send to %S <- %s\n" rep_id (string_of_msg msg);
            (* drop message with probability msg_loss_rate *)
            if RND.float t.rng 1.0 >= msg_loss_rate then begin
              let t1 = Int64.(t.clock + t.rtt - t.rtt / 4L +
                              of_int (RND.int t.rng (to_int t.rtt lsr 1)))
              in
                Event_queue.schedule t.ev_queue t1 rep_id
                  (Message (node.id, msg))
            end
      in
        node.state <- s;
        List.iter exec_action actions

    in

    let steps = ref 0 in
      (* schedule initial election timeouts *)
      Array.iter (schedule_election t) t.nodes;

      try
        let rec loop () =
          match Event_queue.next t.ev_queue with
              None -> !steps
            | Some (time, rep_id, ev) ->
                incr steps;
                t.clock <- time;
                (* we reverse evs to make sure that two simultaneous events
                 * are executed in the same order they were scheduled *)
                react_to_event time (node_of_id rep_id) ev;
                loop ()
        in loop ()
      with Exit -> !steps
end

let () =
  let get_queue =
    let h = Hashtbl.create 13 in
      (fun id ->
         try Hashtbl.find h id
         with Not_found ->
           let q = Queue.create () in
             Hashtbl.add h id q;
             q) in

  let completed = ref 0 in
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

  let retry_period = 5000L in

  let applied = BatBitSet.create (num_cmds + 20) in

  let on_apply ~time ~leader node_id cmd =
    if cmd mod 10_000 = 0 then printf "XXXXXXXXXXXXX apply %S  %d\n%!" node_id cmd;
    let q    = get_queue node_id in
    let ()   = Queue.push cmd q in
    let len  = Queue.length q in
      BatBitSet.set applied cmd;
      if cmd >= !last_sent then begin
      (* We schedule the next few commands being sent to the current leader
       * (simulating the client caching the current leader). *)
        let dt = Int64.(heartbeat_period - 10L) in
          for i = 1 to batch_size do
            incr last_sent;
            let cmd = !last_sent in
            let rec schedule time =
              DES.Event_queue.schedule ev_queue
                Int64.(time + of_int i * dt)
                (Option.default node_id leader)
                (DES.Command cmd);
              (* after the retry_period, check if the cmd has been executed
               * and reschedule if needed *)
              DES.Event_queue.schedule ev_queue
                Int64.(time + retry_period)
                (Option.default node_id leader)
                (DES.Func
                   (fun time ->
                      if not (BatBitSet.mem applied cmd) then begin
                        schedule time
                      end))
            in
              schedule time
        done
      end;
      if len >= num_cmds then begin
        print_endline "COMPLETED";
        completed := !completed  + 1;
        if !completed >= num_nodes then
          raise Exit
      end in

  let rng = Random.State.make [| 2 |] in

  let des = DES.make ~ev_queue ~rng ~num_nodes
              ~election_period ~heartbeat_period ~rtt ()
  in
    (* schedule init cmd delivery *)
    DES.Event_queue.schedule
      ev_queue 100L (DES.random_node_id des) (DES.Command init_cmd);
    let t0    = Unix.gettimeofday () in
    let steps = DES.simulate ~verbose:false ~msg_loss_rate on_apply des in
    let dt    = Unix.gettimeofday () -. t0 in
    let ncmds = Queue.length (get_queue (DES.random_node_id des)) in
      printf "Replicated log lengths: %s\n"
        (DES.node_ids des |>
         List.map (fun id -> sprintf "%s: %d" id (Queue.length (get_queue id))) |>
         String.concat ", ");
      printf "Simulated %d steps (%4.2f steps/cmd, %.0f steps/s, %.0f cmds/s).\n"
        steps (float steps /. float ncmds)
        (float steps /. dt) (float ncmds /. dt);
