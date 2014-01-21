open Printf
open Lwt

module Map    = BatMap
module List   = BatList
module Option = BatOption

module Kernel =
struct
  type status    = Leader | Follower | Candidate
  type term      = Int64.t
  type index     = Int64.t
  type rep_id    = string
  type client_id = string
  type req_id    = client_id * Int64.t
  type ('a, 'b) result = [`OK of 'a | `Error of 'b]

  module REPID = struct type t = rep_id let compare = String.compare end
  module IM = Map.Make(Int64)
  module RM = Map.Make(REPID)
  module RS = Set.Make(REPID)

  let maybe_nf f x = try Some (f x) with Not_found | Invalid_argument _ -> None

  module LOG : sig
    type 'a t

    val empty       : init_index:index -> init_term:term -> 'a t
    val to_list     : 'a t -> (index * 'a * term) list
    val of_list     : init_index:index -> init_term:term ->
                        (index * 'a * term) list -> 'a t
    val append      : term:term -> 'a -> 'a t -> 'a t
    val last_index  : 'a t -> (term * index)
    val append_many : (index * ('a * term)) list -> 'a t -> 'a t
    val get_range   : from_inclusive:index -> to_inclusive:index -> 'a t ->
                      (index * ('a * term)) list
    val get_term    : index -> 'a t -> term option
  end =
  struct
    type 'a t =
        {
          init_index : index;
          init_term  : term;
          last_index : index;
          last_term  : term;
          entries    : ('a * term) IM.t;
        }

    let empty ~init_index ~init_term =
      { init_index; init_term;
        last_index = init_index;
        last_term  = init_term;
        entries    = IM.empty;
      }

    let to_list t =
      IM.bindings t.entries |> List.map (fun (i, (x, t)) -> (i, x, t))

    let of_list ~init_index ~init_term = function
        [] -> empty ~init_index ~init_term
      | l ->
          let entries =
            List.fold_left
              (fun m (idx, x, term) -> IM.add idx (x, term) m)
              IM.empty l in
          let (init_index, (_, init_term)) = IM.min_binding entries in
          let (last_index, (_, last_term)) = IM.max_binding entries in
            { init_index; init_term; last_index; last_term; entries; }

    let append ~term:last_term x t =
      let last_index = Int64.succ t.last_index in
      let entries    = IM.add last_index (x, last_term) t.entries in
        { t with last_index; last_term; entries; }

    let last_index t = (t.last_term, t.last_index)

    let get idx t =
      try
        Some (IM.find idx t.entries)
      with Not_found -> None

    let append_many l t = match l with
        [] -> t
      | l ->
          let nonconflicting =
            try
              let idx, term =
                List.find
                  (fun (idx, (_, term)) ->
                     match get idx t with
                         Some (_, term') when term <> term' -> true
                       | _ -> false)
                  l in
              let entries, _, _ = IM.split idx t.entries in
                entries
            with Not_found ->
              t.entries
          in
            let last_index, last_term, entries =
              List.fold_left
                (fun (last_index, last_term, m) (idx, (x, term)) ->
                   let m          = IM.add idx (x, term) m in
                   let last_index = max last_index idx in
                   let last_term  = max last_term term in
                     (last_index, last_term, m))
                (t.init_index, t.init_term, nonconflicting) l
            in
              { t with last_index; last_term; entries; }

    let get_range ~from_inclusive ~to_inclusive t =
      let _, _, post = IM.split (Int64.pred from_inclusive) t.entries in
      let pre, _, _  = if to_inclusive = Int64.max_int then (post, None, post)
                       else IM.split (Int64.succ to_inclusive) post
      in
        IM.bindings pre

    let get_term idx t =
      try
        Some (snd (IM.find idx t.entries))
      with Not_found ->
        if idx = t.init_index then Some t.init_term else None
  end

  type 'a state =
      {
        (* persistent *)
        current_term : term;
        voted_for    : rep_id option;
        log          : 'a LOG.t;
        id           : rep_id;
        peers        : rep_id array;

        (* volatile *)
        state        : status;
        commit_index : index;
        last_applied : index;

        leader_id : rep_id option;

        (* volatile on leaders *)
        next_index  : index RM.t;
        match_index : index RM.t;

        votes : RS.t;
      }

  type 'a message =
      Request_vote of request_vote
    | Vote_result of vote_result
    | Append_entries of 'a append_entries
    | Append_result of append_result

  and request_vote =
      {
        term : term;
        candidate_id : rep_id;
        last_log_index : index;
        last_log_term : term;
      }

  and vote_result =
    {
      term : term;
      vote_granted : bool;
    }

  and 'a append_entries =
    {
      term : term;
      leader_id : rep_id;
      prev_log_index : index;
      prev_log_term : term;
      entries : (index * ('a * term)) list;
      leader_commit : index;
    }

  and append_result =
    {
      term : term;
      success : bool;

      (* extra information relative to the fields on the RAFT paper *)

      (* index of log entry immediately preceding new ones in AppendEntries msg
       * this responds to; used for fast next_index rollback by the Leader
       * and allows to receive result of concurrent AppendEntries out-of-order
       * *)
      prev_log_index : index;
      (* index of last log entry included in the msg this responds to;
       * used by the Leader to update match_index, allowing to receive results
       * of concurrent Append_entries request out-of-order *)
      last_log_index : index;
    }

  type 'a action =
      [ `Apply of 'a
      | `Become_candidate
      | `Become_follower of rep_id option
      | `Become_leader
      | `Reset_election_timeout
      | `Reset_heartbeat
      | `Redirect of rep_id option * 'a
      | `Send of rep_id * 'a message
      ]
end

include Kernel

let quorum s = Array.length s.peers / 2 + 1

let vote_result s vote_granted =
  Vote_result { term = s.current_term; vote_granted; }

let append_result s ~prev_log_index ~last_log_index success =
  Append_result
    { term = s.current_term; prev_log_index; last_log_index; success; }

let append_ok ~prev_log_index ~last_log_index s =
  append_result s ~prev_log_index ~last_log_index true

let append_fail ~prev_log_index s =
  append_result s ~prev_log_index ~last_log_index:0L false

let update_commit_index s =
  (* Find last N such that log[N].term = current term AND
   * a majority of peers has got  match_index[peer] >= N. *)
  (* We compute it as follows: get all the match_index, sort them,
   * and get the (quorum - 1)-nth (not quorum-nth because the current leader
   * also counts).*)
  let sorted       = RM.bindings s.match_index |> List.map snd |>
                     List.sort Int64.compare in
  let commit_index' =
    try
      List.nth sorted (quorum s - 1)
    with Not_found ->
      s.commit_index in

  (* increate monotonically *)
  let commit_index = max s.commit_index commit_index' in
    { s with commit_index }

let try_commit s =
  let prev    = s.last_applied in
  let s       = { s with last_applied = s.commit_index } in
  let actions = List.map (fun (_, (x, _)) -> `Apply x)
                  (LOG.get_range
                     ~from_inclusive:(Int64.succ prev)
                     ~to_inclusive:s.commit_index
                     s.log)
  in (s, actions)

let heartbeat s =
  let prev_log_term, prev_log_index = LOG.last_index s.log in
    Append_entries { term = s.current_term; leader_id = s.id;
                     prev_log_term; prev_log_index; entries = [];
                     leader_commit = s.commit_index }

let send_entries s from =
  match LOG.get_term (Int64.pred from) s.log with
      None ->
        None
    | Some prev_log_term ->
        Some
          (Append_entries
            { prev_log_term;
              term           = s.current_term;
              leader_id      = s.id;
              prev_log_index = Int64.pred from;
              entries        = LOG.get_range
                                 ~from_inclusive:from
                                 ~to_inclusive:Int64.max_int
                                 s.log;
              leader_commit  = s.commit_index;
            })

let broadcast s msg =
  Array.to_list s.peers |>
  List.filter_map
    (fun p -> if p = s.id then None else Some (`Send (p, msg)))

let receive_msg s peer = function
  (* " If a server receives a request with a stale term number, it rejects the
   * request." *)
  | Request_vote { term; _ } when term < s.current_term ->
      (s, [`Send (peer, vote_result s false)])

  (* "Current terms are exchanged whenever servers communicate; if one
   * server’s current term is smaller than the other, then it updates its
   * current term to the larger value. If a candidate or leader discovers that
   * its term is out of date, it immediately reverts to follower state."
   * *)
  | Request_vote { term; candidate_id; _ } when term > s.current_term -> begin
      let s = { s with current_term = term;
                       voted_for    = Some candidate_id;
                       state        = Follower; }
      in
        (s, [`Become_follower None; `Send (peer, vote_result s true)])
    end
  | Request_vote { term; candidate_id; last_log_index; last_log_term; } -> begin
      match s.voted_for with
          Some candidate when candidate <> candidate_id ->
            (s, [`Send (peer, vote_result s false)])
        | _ ->
            if (last_log_term, last_log_index) < LOG.last_index s.log then
              (s, [`Send (peer, vote_result s false)])
            else begin
              let s = { s with voted_for = Some candidate_id } in
                (s, [`Reset_election_timeout; `Send (peer, vote_result s true)])
            end
    end
  (* " If a server receives a request with a stale term number, it rejects the
   * request." *)
  | Append_entries { term; prev_log_index; _ } when term < s.current_term ->
      (s, [`Send (peer, append_fail ~prev_log_index s)])
  | Append_entries
      { term; prev_log_index; prev_log_term; entries; leader_commit; } -> begin
        (* "Current terms are exchanged whenever servers communicate; if one
         * server’s current term is smaller than the other, then it updates
         * its current term to the larger value. If a candidate or leader
         * discovers that its term is out of date, it immediately reverts to
         * follower state." *)
        let s, actions =
          if term > s.current_term then
            let s = { s with current_term = term;
                             state        = Follower;
                             leader_id    = Some peer;
                    }
            in
              (s, [`Become_follower (Some peer)])
          else
            (s, [`Reset_election_timeout])
        in
          match LOG.get_term prev_log_index s.log  with
              None ->
                (s, [`Reset_election_timeout;
                     `Send (peer, append_fail ~prev_log_index s)])
            | Some term' when prev_log_term <> term' ->
                (s, [`Reset_election_timeout;
                     `Send (peer, append_fail ~prev_log_index s)])
            | _ ->
                let log          = LOG.append_many entries s.log in
                let last_index   = snd (LOG.last_index log) in
                let commit_index = if leader_commit > s.commit_index then
                                     min leader_commit last_index
                                   else s.commit_index in
                let reply        = append_ok
                                     ~prev_log_index
                                     ~last_log_index:last_index s in
                let s            = { s with commit_index; log;
                                            leader_id = Some peer; } in
                let s, commits   = try_commit s in
                let actions      = List.concat
                                     [ [`Send (peer, reply)];
                                       commits;
                                       actions ]
                in
                  (s, actions)
      end

  | Vote_result { term; _ } when term < s.current_term ->
      (s, [])
  | Vote_result { term; _ } when term > s.current_term ->
      let s = { s with current_term = term; state = Follower } in
        (s, [`Become_follower None])
  | Vote_result { term; vote_granted; } when s.state <> Candidate ->
      (s, [])
  | Vote_result { term; vote_granted; } ->
      if not vote_granted then
        (s, [])
      else
        (* "If votes received from majority of servers: become leader" *)
        let votes = RS.add peer s.votes in
        let s     = { s with votes } in
          if RS.cardinal votes < quorum s - 1 then
            (s, [])
          else
            (* become leader! *)

            (* "When a leader first comes to power, it initializes all
             * nextIndex values to the index just after the last one in its
             * log" *)
            let next_idx    = LOG.last_index s.log |> snd |> Int64.succ in
            let next_index  = Array.fold_left
                                (fun m peer -> RM.add peer next_idx m)
                                RM.empty s.peers in
            let match_index = Array.fold_left
                                (fun m peer -> RM.add peer 0L m)
                                RM.empty s.peers in
            let s     = { s with next_index; match_index;
                                 state     = Leader;
                                 leader_id = Some s.id;
                        } in
            (* "Upon election: send initial empty AppendEntries RPCs
             * (heartbeat) to each server; repeat during idle periods to
             * prevent election timeouts" *)
            let sends = broadcast s (heartbeat s) in
              (s, (`Become_leader :: sends))

  | Append_result { term; _ } when term < s.current_term ->
      (s, [])
  | Append_result { term; _ } when term > s.current_term ->
      (* "If RPC request or response contains term T > currentTerm:
       * set currentTerm = T, convert to follower" *)
      let s = { s with current_term = term; state = Follower; } in
        (s, [`Become_follower None])
  | Append_result { term; success; prev_log_index; last_log_index; } ->
      if success then begin
        let next_index  = RM.modify peer
                            (fun idx -> max idx (Int64.succ last_log_index))
                            s.next_index in
        let match_index = RM.modify_def
                            last_log_index peer
                            (max last_log_index) s.match_index in
        let s           = update_commit_index { s with next_index; match_index } in
        let s, actions  = try_commit s in
          (s, actions)
      end else begin
        (* "After a rejection, the leader decrements nextIndex and retries
         * the AppendEntries RPC. Eventually nextIndex will reach a point
         * where the leader and follower logs match." *)
        let next_index = RM.modify peer
                           (fun idx -> min idx prev_log_index)
                           s.next_index in
        let s          = { s with next_index } in
          match send_entries s (RM.find peer next_index) with
              None ->
                (* Must send snapshot *)
                (* FIXME *)
                (s, [])
            | Some msg ->
                (s, [`Send (peer, msg)])
      end

let election_timeout s = match s.state with
    Leader -> (s, [])
  (* "If election timeout elapses without receiving AppendEntries RPC from
   * current leader or granting vote to candidate: convert to candidate" *)
  | Follower
  (* "If election timeout elapses: start new election" *)
  | Candidate ->
      (* "On conversion to candidate, start election:
       *  * Increment currentTerm
       *  * Vote for self
       *  * Reset election timeout
       *  * Send RequestVote RPCs to all other servers"
       * *)
      let s           = { s with current_term = Int64.succ s.current_term;
                                 state        = Candidate;
                                 votes        = RS.empty;
                                 leader_id    = None;
                                 voted_for    = Some s.id;
                        } in
      let term_, idx_ = LOG.last_index s.log in
      let msg         = Request_vote
                          {
                            term = s.current_term;
                            candidate_id = s.id;
                            last_log_index = idx_;
                            last_log_term = term_;
                          } in
      let sends       = broadcast s msg in
        (s, (`Become_candidate :: sends))

let heartbeat_timeout s = match s.state with
    Follower | Candidate -> (s, [])
  | Leader ->
      (s, (`Reset_heartbeat :: broadcast s (heartbeat s)))

let client_command x s = match s.state with
    Follower | Candidate -> (s, [`Redirect (s.leader_id, x)])
  | Leader ->
      let log     = LOG.append ~term:s.current_term x s.log in
      let s       = { s with log; } in
      let actions = Array.to_list s.peers |>
                    List.filter_map
                      (fun peer ->
                         if peer = s.id then None
                         else
                           match send_entries s (RM.find peer s.next_index) with
                               None ->
                                 (* FIXME: should send snapshot if cannot send
                                  * log *)
                                 None
                             | Some msg -> Some (`Send (peer, msg))) in
      let actions = match actions with
                      | [] -> []
                      | l -> `Reset_heartbeat :: actions
      in
        (s, actions)

module Types = Kernel

module Core =
struct
  include Types

  let make ~id ~current_term ~voted_for ~log ~peers () =
    let log = LOG.of_list ~init_index:0L ~init_term:current_term log in
      {
        current_term; voted_for; log; id; peers;
        state        = Follower;
        commit_index = 0L;
        last_applied = 0L;
        leader_id    = None;
        next_index   = RM.empty;
        match_index  = RM.empty;
        votes        = RS.empty;
      }

  let leader_id (s : _ state) = s.leader_id

  let receive_msg       = receive_msg
  let election_timeout  = election_timeout
  let heartbeat_timeout = heartbeat_timeout
  let client_command    = client_command
end

module type LWTIO =
sig
  type address
  type op
  type connection

  val connect : address -> connection option Lwt.t
  val send    : connection -> (req_id * op) message -> bool Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t
end

module type LWTPROC =
sig
  type op
  type resp

  val execute : op -> (resp, exn) result Lwt.t
end

module Lwt_server
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
        election_period          : float;
        heartbeat_period         : float;
        mutable next_req_id      : Int64.t;
        mutable conns            : IO.connection RM.t;
        mutable state            : (req_id * PROC.op) state;
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
      }

  and th_res =
      Message of rep_id * (req_id * PROC.op) message
    | Client_command of req_id * PROC.op
    | Abort
    | Election_timeout
    | Heartbeat_timeout

  and cmd_res =
      Redirect of rep_id option
    | Executed of (PROC.resp, exn) result

  type result =
      [ `OK of PROC.resp
      | `Error of exn
      | `Redirect of rep_id * IO.address
      | `Redirect_randomized of rep_id * IO.address
      | `Retry_later ]

  let make
        ?(election_period = 2.)
        ?(heartbeat_period = election_period /. 2.)
        state peers =
    let stream, push      = Lwt_stream.create () in
    let push x            = push (Some x) in
    let election_timeout  = match state.Core.state with
                              | Follower | Candidate ->
                                  Lwt_unix.sleep election_period >>
                                  return Election_timeout
                              | Leader -> fst (Lwt.wait ()) in
    let heartbeat         = match state.Core.state with
                              | Follower | Candidate -> fst (Lwt.wait ())
                              | Leader ->
                                  Lwt_unix.sleep heartbeat_period >>
                                  return Heartbeat_timeout
    in
      {
        heartbeat_period;
        election_period;
        state;
        election_timeout;
        heartbeat;
        peers         = List.fold_left
                          (fun m (k, v) -> RM.add k v m) RM.empty peers;
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
    | `Reset_election_timeout ->
        t.election_timeout <- (Lwt_unix.sleep t.election_period >>
                               return Election_timeout);
        return ()
    | `Reset_heartbeat ->
        t.heartbeat <- (Lwt_unix.sleep t.heartbeat_period >>
                        return Heartbeat_timeout);
        return ()
    | `Become_candidate
    | `Become_follower None ->
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t `Reset_election_timeout
    | `Become_follower (Some _) ->
        Lwt_condition.broadcast t.leader_signal ();
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t `Reset_election_timeout
    | `Become_leader ->
        Lwt_condition.broadcast t.leader_signal ();
        t.election_timeout  <- fst (Lwt.wait ());
        exec_action t `Reset_heartbeat
    | `Apply (req_id, op) ->
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
        end
    | `Redirect (rep_id, (req_id, _)) -> begin
        try_lwt
          let (_, u), pending_cmds = CMDM.extract req_id t.pending_cmds in
            Lwt.wakeup_later u (Redirect rep_id);
            return ()
        with _ -> return ()
      end
    | `Send (rep_id, msg) ->
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

  let exec_actions t l = Lwt_list.iter_s (exec_action t) l

  let rec run t =
    if not t.running then return ()
    else
      let must_recon = Array.to_list t.state.peers |>
                       List.filter_map
                         (fun peer -> lookup_address t peer |>
                                      Option.map (fun addr -> (peer, addr))) |>
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

  let gen_req_id t =
    let id = t.next_req_id in
      t.next_req_id <- Int64.succ id;
      (t.state.id, id)

  let rec execute t cmd =
    match t.state.state, t.state.leader_id with
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
