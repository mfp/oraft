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
  val schedule   : 'a t -> rep_id -> CLOCK.t -> 'a -> unit
  val unschedule : 'a t -> rep_id -> CLOCK.t -> 'a -> unit
  val next       : 'a t -> (CLOCK.t * rep_id * 'a list) option
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
    module M = Map.Make(struct
                          type t      = CLOCK.t * rep_id
                          let compare (c1, r1) (c2, r2) =
                            match Int64.compare c1 c2 with
                                0 -> String.compare r1 r2
                              | n -> n
                        end)
    type 'a t = { mutable q : 'a list M.t }

    let create () = { q = M.empty }

    let schedule t rep_id time ev =
      t.q <- M.modify_def [] (time, rep_id) (fun l -> ev :: l) t.q

    let unschedule t rep_id time ev =
      t.q <- M.modify_opt (time, rep_id) (Option.map (List.filter ((<>) ev))) t.q

    let is_empty t = M.is_empty t.q

    let next t =
      try
        let (clock, rep_id), evs = M.min_binding t.q in
          t.q <- M.remove (clock, rep_id) t.q;
          Some (clock, rep_id, evs)
      with Not_found -> None
  end

  type 'a event =
    | Election_timeout
    | Heartbeat_timeout
    | Command of 'a
    | Message of rep_id * 'a message

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

  let string_of_msg = function
      Request_vote { term; candidate_id; _ } ->
        sprintf "Request_vote %S @ %Ld" candidate_id term
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

  let schedule_election t node =
    let dt = Int64.(t.election_period - t.election_period / 4L +
                    of_int (RND.int t.rng (to_int t.election_period lsr 2))) in
    let t1 = Int64.(t.clock + dt) in
      node.next_election <- Some t1;
      Event_queue.schedule t.ev_queue node.id t1 Election_timeout

  let schedule_heartbeat t node =
    let t1 = Int64.(t.clock + t.heartbeat_period) in
      node.next_heartbeat <- Some t1;
      Event_queue.schedule t.ev_queue node.id t1 Heartbeat_timeout

  let unschedule_aux t node time what =
    Option.may
      (fun time ->
         Event_queue.unschedule t.ev_queue node.id time what)
      time

  let unschedule_election t node =
    unschedule_aux t node node.next_election Election_timeout

  let unschedule_heartbeat t node =
    unschedule_aux t node node.next_heartbeat Heartbeat_timeout

  let send_cmd t node_id cmd =
    Event_queue.schedule t.ev_queue
      node_id Int64.(t.clock + of_int (RND.int t.rng 100)) (Command cmd)

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
          Election_timeout -> C.election_timeout node.state
        | Heartbeat_timeout -> C.heartbeat_timeout node.state
        | Command c -> C.client_command c node.state
        | Message (peer, msg) -> C.receive_msg node.state peer msg in

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
                Event_queue.schedule t.ev_queue rep_id t1
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
            | Some (time, rep_id, evs) ->
                incr steps;
                t.clock <- time;
                (* we reverse evs to make sure that two simultaneous events
                 * are executed in the same order they were scheduled *)
                List.(iter (react_to_event time (node_of_id rep_id)) (rev evs));
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
  let num_cmds  = 200_000 in
  let init_cmd  = 1 in
  let last_sent = ref init_cmd in
  let ev_queue  = DES.Event_queue.create () in

  let election_period  = 800L in
  let heartbeat_period = 200L in
  let rtt              = 50L in
  let msg_loss_rate    = 0.01 in
  let batch_size       = 20 in

  let on_apply ~time ~leader node_id cmd =
    if cmd mod 10_000 = 0 then printf "XXXXXXXXXXXXX apply %S  %d\n%!" node_id cmd;
    let q    = get_queue node_id in
    let ()   = Queue.push cmd q in
    let len  = Queue.length q in
      if cmd >= !last_sent then begin
      (* We schedule the next few commands being sent to the current leader
       * (simulating the client caching the current leader). *)
        let dt = Int64.(heartbeat_period - 10L) in
          for i = 1 to batch_size do
            incr last_sent;
            let cmd = !last_sent in
              DES.Event_queue.schedule ev_queue
                (Option.default node_id leader)
                Int64.(time + of_int i * dt) (DES.Command cmd)
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
      ev_queue (DES.random_node_id des) 100L (DES.Command init_cmd);
    let t0    = Unix.gettimeofday () in
    let steps = DES.simulate ~verbose:false ~msg_loss_rate on_apply des in
    let dt    = Unix.gettimeofday () -. t0 in
      printf "Simulated %d steps (%4.2f steps/cmd, %.0f steps/s, %.0f cmds/s).\n"
        steps (float steps /. float !last_sent)
        (float steps /. dt) (float !last_sent /. dt)
