(** Implentation of the RAFT consensus algorithm.
  *
  * Refer to
  * "In Search of an Understandable Consensus Algorithm", Diego Ongaro and John
  * Ousterhout, Stanford University. (Draft of October 7, 2013).
  * [https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf]
  * *)

module Types :
sig
  type status    = Leader | Follower | Candidate
  type term      = Int64.t
  type index     = Int64.t
  type rep_id    = string
  type config    = rep_id array
  type client_id = string
  type req_id    = client_id * Int64.t

  type ('a, 'b) result = [`OK of 'a | `Error of 'b]

  type 'a message =
      Request_vote of request_vote
    | Vote_result of vote_result
    | Append_entries of 'a append_entries
    | Append_result of append_result

  and request_vote = {
    term : term;
    candidate_id : rep_id;
    last_log_index : index;
    last_log_term : term;
  }

  and vote_result = {
    term : term;
    vote_granted : bool;
  }

  and 'a append_entries = {
    term : term;
    leader_id : rep_id;
    prev_log_index : index;
    prev_log_term : term;
    entries : (index * ('a entry * term)) list;
    leader_commit : index;
  }

  and 'a entry = Nop | Op of 'a

  and append_result = {
    term : term;
    success : bool;
    prev_log_index : index;
    last_log_index : index;
  }

  type 'a action =
      Apply of (index * 'a * term) list
    | Become_candidate
    | Become_follower of rep_id option
    | Become_leader
    | Redirect of rep_id option * 'a
    | Reset_election_timeout
    | Reset_heartbeat
    | Send of rep_id * 'a message
    | Send_snapshot of rep_id * index * config
end

module Core :
sig
  open Types

  type 'a state

  val make :
    id:rep_id -> current_term:term -> voted_for:rep_id option ->
    log:(index * 'a entry * term) list ->
    config:config -> unit -> 'a state

  val leader_id : 'a state -> rep_id option
  val id        : 'a state -> rep_id
  val status    : 'a state -> status

  val last_index : 'a state -> index
  val last_term  : 'a state -> term
  val config     : 'a state -> config

  val receive_msg :
    'a state -> rep_id -> 'a message -> 'a state * 'a action list

  val election_timeout  : 'a state -> 'a state * 'a action list
  val heartbeat_timeout : 'a state -> 'a state * 'a action list
  val client_command    : 'a -> 'a state -> 'a state * 'a action list
  val snapshot_sent     : rep_id -> 'a state -> ('a state * 'a action list)

  val install_snapshot :
    last_term:term -> last_index:index -> config:config -> 'a state ->
    'a state * bool

  val compact_log : index -> 'a state -> 'a state
end
