open Printf

module List   = BatList
module Map    = BatMap
module Int64  = BatInt64
module Option = BatOption

module Clock =
struct
  type t = Int64.t
  let compare = compare
end

module DES =
struct
  module RND = Random.State
  module C   = Oraft.Core

  open Oraft.Types

  module Event_queue : sig
    type 'a t

    val create     : unit -> 'a t
    val schedule   : 'a t -> rep_id -> Clock.t -> 'a -> unit
    val unschedule : 'a t -> rep_id -> Clock.t -> 'a -> unit
    val next       : 'a t -> (Clock.t * rep_id * 'a list) option
    val is_empty   : 'a t -> bool
  end =
  struct
    module M = Map.Make(struct
                          type t      = Clock.t * rep_id
                          let compare = compare
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
        mutable next_heartbeat : Clock.t option;
        mutable next_election  : Clock.t option;
      }

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

  let make_node peers id =
    let state = C.make
                  ~id ~current_term:0L ~voted_for:None
                  ~log:[] ~peers ()
    in
      { id; state; next_heartbeat = None; next_election = None }

  let election_period  = 800L
  let heartbeat_period = 200L
  let rtt              = 50L

  let schedule_election rng ev_queue t0 node =
    let dt = Int64.(election_period - election_period / 4L +
                    RND.int64 rng (election_period / 4L)) in
    let t1 = Int64.(t0 + dt) in
      node.next_election <- Some t1;
      Event_queue.schedule ev_queue node.id t1 Election_timeout

  let schedule_heartbeat ev_queue t node =
    let t1 = Int64.(t + heartbeat_period) in
      node.next_heartbeat <- Some t1;
      Event_queue.schedule ev_queue node.id t1 Heartbeat_timeout

  let unschedule_aux ev_queue node t what =
    Option.may
      (fun t ->
         Event_queue.unschedule ev_queue node.id t what)
      t

  let unschedule_election ev_queue node =
    unschedule_aux ev_queue node node.next_election Election_timeout

  let unschedule_heartbeat ev_queue node =
    unschedule_aux ev_queue node node.next_heartbeat Heartbeat_timeout

  let send_cmd rng ev_queue t0 node_id cmd =
    Event_queue.schedule ev_queue
      node_id Int64.(t0 + RND.int64 rng 100L) (Command cmd)

  let simulate
        ?(rng = Random.State.make_self_init ())
        ?(verbose = false)
        ~msg_loss_rate ~num_nodes on_apply init_cmd () =
    let node_ids = Array.init num_nodes (sprintf "n%02d") in
    let nodes    = Array.map (make_node node_ids) node_ids in
    let ev_queue = Event_queue.create () in
    let clock    = ref 0L in

    let random_node_id () =
      node_ids.(RND.int rng (Array.length node_ids)) in

    let node_of_id =
      let h = Hashtbl.create 13 in
        Array.iter (fun node -> Hashtbl.add h node.id node) nodes;
        (fun node_id -> Hashtbl.find h node_id) in

    let send_cmd ?(dst = random_node_id ()) cmd =
      send_cmd rng ev_queue !clock dst cmd in

    let react_to_event t node ev =
      if verbose then
        printf "%Ld @ %s -> %s\n" t node.id (describe_event ev);

      let s, actions = match ev with
          Election_timeout -> C.election_timeout node.state
        | Heartbeat_timeout -> C.heartbeat_timeout node.state
        | Command c -> C.client_command c node.state
        | Message (peer, msg) -> C.receive_msg node.state peer msg in

      let rec exec_action = function
          `Apply cmd ->
            if verbose then printf " Apply\n";
            (* simulate current leader being cached by client *)
            on_apply (send_cmd ?dst:(C.leader_id node.state)) node.id cmd
            (* on_apply (send_cmd ?dst:None) node.id cmd *)
        | `Become_candidate ->
            if verbose then printf " Become_candidate\n";
            unschedule_heartbeat ev_queue node;
            exec_action `Reset_election_timeout
        | `Become_follower None ->
            if verbose then printf " Become_follower\n";
            unschedule_heartbeat ev_queue node;
            exec_action `Reset_election_timeout
        | `Become_follower (Some leader) ->
            if verbose then printf " Become_follower %S\n" leader;
            unschedule_heartbeat ev_queue node;
            exec_action `Reset_election_timeout
        | `Become_leader ->
            if verbose then printf " Become_leader\n";
            unschedule_election ev_queue node;
            schedule_heartbeat ev_queue t node
        | `Redirect (Some leader, cmd) ->
            if verbose then printf " Redirect %s\n" leader;
            send_cmd ~dst:leader cmd
        | `Redirect (None, cmd) ->
            if verbose then printf " Redirect\n";
            (* send to a random server *)
            send_cmd cmd
        | `Reset_election_timeout ->
            if verbose then printf " Reset_election_timeout\n";
            unschedule_election ev_queue node;
            schedule_election rng ev_queue t node
        | `Reset_heartbeat ->
            if verbose then printf " Reset_heartbeat\n";
            unschedule_heartbeat ev_queue node;
            schedule_heartbeat ev_queue t node
        | `Send (rep_id, msg) ->
            if verbose then
              printf " Send to %S <- %s\n" rep_id (string_of_msg msg);
            (* drop message with probability msg_loss_rate *)
            if RND.float rng 1.0 >= msg_loss_rate then begin
              let t1 = Int64.(t + rtt - rtt / 4L + RND.int64 rng (rtt / 2L)) in
                Event_queue.schedule ev_queue rep_id t1
                  (Message (node.id, msg))
            end
      in
        node.state <- s;
        List.iter exec_action actions

    in

    let steps = ref 0 in
      (* schedule initial election timeouts *)
      Array.iter (schedule_election rng ev_queue 0L) nodes;
      (* schedule init cmd delivery *)
      send_cmd init_cmd;

      try
        let rec loop () =
          match Event_queue.next ev_queue with
              None -> ()
            | Some (t, rep_id, evs) ->
                incr steps;
                clock := t;
                (* we reverse evs to make sure that two simultaneous events
                 * are executed in the same order they were scheduled *)
                List.(iter (react_to_event t (node_of_id rep_id)) (rev evs));
                loop ()
        in loop ()
      with Exit ->
        printf "Ran %d steps.\n" !steps;
        ()
end

module IS = Set.Make(struct type t = int let compare = (-) end)

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
  let sent      = ref IS.empty in
  let num_nodes = 3 in
  let num_cmds  = 200_000 in

  let on_apply send node_id cmd =
    (* printf "XXXXXXXXXXXXX apply %S  %d\n" node_id cmd; *)
    let q    = get_queue node_id in
    let cmd' = cmd + 1 in
    let len = Queue.length q + 1 in
      Queue.push cmd q;
      if len < num_cmds && not (IS.mem cmd' !sent) then begin
        sent := IS.add cmd' !sent;
        send cmd'
      end;
      if len >= num_cmds then begin
        print_endline "COMPLETED";
        completed := !completed  + 1;
        if !completed >= num_nodes then
          raise Exit
      end in

  let rng = Random.State.make [| 2 |] in
    DES.simulate ~rng ~verbose:false ~msg_loss_rate:0.01 ~num_nodes on_apply 0 ();
    print_endline "DONE"

