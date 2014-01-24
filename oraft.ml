open Printf
open Lwt

open Oraft_util

module Map    = BatMap
module List   = BatList
module Option = BatOption
module Array  = BatArray

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
      if from_inclusive = t.last_index &&
         to_inclusive >= t.last_index
      then
        [ from_inclusive, IM.find from_inclusive t.entries ]
      else
        let _, _, post = IM.split (Int64.pred from_inclusive) t.entries in
        let pre, _, _  = if to_inclusive = Int64.max_int then (post, None, post)
                         else IM.split (Int64.succ to_inclusive) post
        in
          IM.bindings pre

    let get_term idx t =
      if t.last_index = idx then
        Some t.last_term
      else
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
        log          : 'a entry LOG.t;
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

  and 'a entry = Nop | Op of 'a

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
      entries : (index * ('a entry * term)) list;
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
      Apply of 'a
    | Become_candidate
    | Become_follower of rep_id option
    | Become_leader
    | Redirect of rep_id option * 'a
    | Reset_election_timeout
    | Reset_heartbeat
    | Send of rep_id * 'a message
end

include Kernel

let quorum s = (1 + Array.length s.peers) / 2 + 1

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
   * and get the (M - quorum + 1)-nth as the commit index candidate, where M
   * is the number of peers: this guarantees that a majority of all nodes
   * (including the current leader) have least all entries up to N.
   *
   * Sample index computation and commit index candidates:
   *  [1; 2; 3; 4; 5; 6] -> 4 idx 3   quorum 4    7-4
   *  [1; 2; 3; 4; 5] -> 3    idx 2   quorum 4    6-4
   *  [1; 2; 3; 4] -> 3       idx 2   quorum 3    5-3
   *  [1; 2; 3] -> 2          idx 1   quorum 3    4-3
   *  [1; 2] -> 2             idx 1   quorum 2    3-2
   *  [1] -> 1                idx 0   quorum 2    2-2
   *
   * E.g., M = 3 nodes, quorum = 2, with sorted match_index array like
   *   [ 1; 2 ]
   * we take the  (3 - 2 + 1) = 2nd element (index 1 = 3 - 2 = M - quorum).
   *
   * The new commit index candidate is N = 2: if it fulfills the 2nd criterion
   * ("restriction on commitment"), we can consider entries up to (including)
   * 2 committed, since it's replicated in the leader plus another node.
   *
   * *)
  let sorted = RM.bindings s.match_index |> List.map snd |>
               List.sort Int64.compare in

  let index  =
    try
      List.nth sorted (Array.length s.peers + 1 - quorum s)
    with Invalid_argument _ | Not_found ->
      s.commit_index in

  (* index is the largest N such that at least half of the peers have
   * match_Index[peer] >= N; we have to enforce the other restriction:
   * log[N].term = current *)
  let commit_index' =
    match LOG.get_term index s.log with
      | Some term when term = s.current_term -> index
      | _ -> s.commit_index in

  (* increate monotonically *)
  let commit_index = max s.commit_index commit_index' in
    if commit_index = s.commit_index then s
    else { s with commit_index }

let try_commit s =
  let prev    = s.last_applied in
    if prev = s.commit_index then
      (s, [])
    else
      let s       = { s with last_applied = s.commit_index } in
      let entries = LOG.get_range
                      ~from_inclusive:(Int64.succ prev)
                      ~to_inclusive:s.commit_index
                      s.log in
      let actions = List.filter_map
                      (function
                           (_, (Op x, _)) -> Some (Apply x)
                         | (_, (Nop, _)) -> None)
                      entries
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
    (fun p -> if p = s.id then None else Some (Send (p, msg)))

let receive_msg s peer = function
  (* " If a server receives a request with a stale term number, it rejects the
   * request." *)
  | Request_vote { term; _ } when term < s.current_term ->
      (s, [Send (peer, vote_result s false)])

  (* "Current terms are exchanged whenever servers communicate; if one
   * server’s current term is smaller than the other, then it updates its
   * current term to the larger value. If a candidate or leader discovers that
   * its term is out of date, it immediately reverts to follower state."
   * *)
  | Vote_result { term; _ }
  | Append_result { term; _ } when term > s.current_term ->
      let s = { s with current_term = term;
                       voted_for    = None;
                       state        = Follower }
      in (s, [Become_follower None])

  | Request_vote { term; candidate_id; last_log_index; last_log_term; }
      when term > s.current_term ->
      let s = { s with current_term = term;
                       voted_for    = None;
                       state        = Follower;
              }
      in
        (* "If votedFor is null or candidateId, and candidate's log is at
         * least as up-to-date as receiver’s log, grant vote"
         *
         * [voted_for] is None since it was reset above when we updated
         * [current_term]
         *
         * "Raft determines which of two logs is more up-to-date
         * by comparing the index and term of the last entries in the
         * logs. If the logs have last entries with different terms, then
         * the log with the later term is more up-to-date. If the logs
         * end with the same term, then whichever log is longer is
         * more up-to-date."
         * *)
        if (last_log_term, last_log_index) < LOG.last_index s.log then
          (s, [Become_follower None; Send (peer, vote_result s false)])
        else
          let s = { s with voted_for = Some candidate_id } in
            (s, [Become_follower None; Send (peer, vote_result s true)])

  | Request_vote { term; candidate_id; last_log_index; last_log_term; } -> begin
      (* case term = current_term *)
      match s.state, s.voted_for with
          _, Some candidate when candidate <> candidate_id ->
            (s, [Send (peer, vote_result s false)])
        | (Candidate | Leader), _ ->
            (s, [Send (peer, vote_result s false)])
        | Follower, _ (* None or Some candidate equal to candidate_id *) ->
            (* "If votedFor is null or candidateId, and candidate's log is at
             * least as up-to-date as receiver’s log, grant vote"
             *
             * "Raft determines which of two logs is more up-to-date
             * by comparing the index and term of the last entries in the
             * logs. If the logs have last entries with different terms, then
             * the log with the later term is more up-to-date. If the logs
             * end with the same term, then whichever log is longer is
             * more up-to-date."
             * *)
            if (last_log_term, last_log_index) < LOG.last_index s.log then
              (s, [Send (peer, vote_result s false)])
            else
              let s = { s with voted_for = Some candidate_id } in
                (s, [Send (peer, vote_result s true)])
    end

  (* " If a server receives a request with a stale term number, it rejects the
   * request." *)
  | Append_entries { term; prev_log_index; _ } when term < s.current_term ->
      (s, [Send (peer, append_fail ~prev_log_index s)])
  | Append_entries
      { term; prev_log_index; prev_log_term; entries; leader_commit; } -> begin
        (* "Current terms are exchanged whenever servers communicate; if one
         * server’s current term is smaller than the other, then it updates
         * its current term to the larger value. If a candidate or leader
         * discovers that its term is out of date, it immediately reverts to
         * follower state." *)
        let s, actions =
          if term > s.current_term || s.state = Candidate then
            let s = { s with current_term = term;
                             state        = Follower;
                             leader_id    = Some peer;
                             (* set voted_for so that no other candidates are
                              * accepted during the new term *)
                             voted_for    = Some peer;
                    }
            in
              (s, [Become_follower (Some peer)])
          else (* term = s.current_term && s.state <> Candidate *)
            (s, [Reset_election_timeout])
        in
          match LOG.get_term prev_log_index s.log  with
              None ->
                (s, Send (peer, append_fail ~prev_log_index s) :: actions)
            | Some term' when prev_log_term <> term' ->
                (s, Send (peer, append_fail ~prev_log_index s) :: actions)
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
                                     [ [Send (peer, reply)];
                                       commits;
                                       actions ]
                in
                  (s, actions)
      end

  | Vote_result { term; _ } when term < s.current_term ->
      (s, [])
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

            (* So as to have the leader know which entries are committed
             * (one of the 2 requirements to support read-only operations in
             * the leader, the other being making sure it's still the leader
             * with a hearbeat exchange), we have it commit a blank no-op
             * entry at the start of its term, which also serves as the
             * initial heartbeat *)
            let log         = LOG.append ~term:s.current_term Nop s.log in


            (* With a regular (empty) hearbeat, this applies:
             *   "When a leader first comes to power, it initializes all
             *   nextIndex values to the index just after the last one in its
             *   log"
             * However, in this case we want to send the Nop too.
             * *)
            let next_idx    = LOG.last_index log |> snd in
            let next_index  = Array.fold_left
                                (fun m peer -> RM.add peer next_idx m)
                                RM.empty s.peers in
            let match_index = Array.fold_left
                                (fun m peer -> RM.add peer 0L m)
                                RM.empty s.peers in
            let s     = { s with log; next_index; match_index;
                                 state     = Leader;
                                 leader_id = Some s.id;
                        } in
            (* This heartbeat is replaced by the broadcast of the no-op
             * explained above:
             *
              (* "Upon election: send initial empty AppendEntries RPCs
               * (heartbeat) to each server; repeat during idle periods to
               * prevent election timeouts" *)
              let sends = broadcast s (heartbeat s) in
            *)
            let msg   = send_entries s next_idx in
            let sends = Array.to_list s.peers |>
                        List.filter_map
                          (fun peer -> Option.map (fun m -> Send (peer, m)) msg)
            in
              (s, (Become_leader :: sends))

  | Append_result { term; _ } when term < s.current_term || s.state <> Leader ->
      (s, [])
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
                (s, [Send (peer, msg)])
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
        (s, (Become_candidate :: sends))

let heartbeat_timeout s = match s.state with
    Follower | Candidate -> (s, [])
  | Leader ->
      (s, (Reset_heartbeat :: broadcast s (heartbeat s)))

let client_command x s = match s.state with
    Follower | Candidate -> (s, [Redirect (s.leader_id, x)])
  | Leader ->
      let log     = LOG.append ~term:s.current_term (Op x) s.log in
      let s       = { s with log; } in
      let actions = Array.to_list s.peers |>
                    List.filter_map
                      (fun peer ->
                         match send_entries s (RM.find peer s.next_index) with
                             None ->
                               (* FIXME: should send snapshot if cannot send
                                * log *)
                               None
                           | Some msg -> Some (Send (peer, msg))) in
      let actions = match actions with
                      | [] -> []
                      | l -> Reset_heartbeat :: actions
      in
        (s, actions)

module Types = Kernel

module Core =
struct
  include Types

  let make ~id ~current_term ~voted_for ~log ~peers () =
    let log   = LOG.of_list ~init_index:0L ~init_term:current_term log in
    let peers = Array.filter ((<>) id) peers in
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

  let id s     = s.id
  let status s = s.state

  let receive_msg       = receive_msg
  let election_timeout  = election_timeout
  let heartbeat_timeout = heartbeat_timeout
  let client_command    = client_command
end
