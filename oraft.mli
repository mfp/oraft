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
    entries : (index * ('a * term)) list;
    leader_commit : index;
  }

  and append_result = {
    term : term;
    success : bool;
    prev_log_index : index;
    last_log_index : index;
  }

  type 'a action =
      [ `Apply of 'a
      | `Become_candidate
      | `Become_follower of rep_id option
      | `Become_leader
      | `Redirect of rep_id option * 'a
      | `Reset_election_timeout
      | `Reset_heartbeat
      | `Send of rep_id * 'a message ]
end

module Core :
sig
  open Types

  type 'a state

  val receive_msg :
    'a state -> rep_id -> 'a message -> 'a state * 'a action list

  val election_timeout  : 'a state -> 'a state * 'a action list

  val heartbeat_timeout : 'a state -> 'a state * 'a action list

  val client_command    : 'a -> 'a state -> 'a state * 'a action list
end

module type LWTIO =
sig
  open Types

  type address
  type op
  type connection

  val connect : address -> connection option Lwt.t
  val accept  : unit -> (address * connection) Lwt.t
  val send    : connection -> (req_id * op) message -> bool Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t
end

module type LWTPROC =
sig
  type op
  type resp

  val execute : op -> (resp, exn) Types.result Lwt.t
end

module Lwt_server :
  functor(PROC : LWTPROC) ->
  functor(IO : LWTIO with type op = PROC.op) ->
sig
  open Types

  type server

  type result =
      [ `OK of PROC.resp
      | `Error of exn
      | `Redirect of rep_id * IO.address
      | `Redirect_randomized of rep_id * IO.address
      | `Retry_later
      ]

  val make : ?election_period:float -> ?heartbeat_period:float ->
    (req_id * IO.op) Core.state -> (rep_id * IO.address) list -> server

  val run     : server -> unit Lwt.t
  val abort   : server -> unit
  val execute : server -> PROC.op -> result Lwt.t
end
