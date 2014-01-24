
module type LWTIO =
sig
  open Oraft.Types

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

  val execute : op -> (resp, exn) Oraft.Types.result Lwt.t
end

module Make_server :
  functor(PROC : LWTPROC) ->
  functor(IO : LWTIO with type op = PROC.op) ->
sig
  open Oraft.Types

  type server

  type result =
      [ `OK of PROC.resp
      | `Error of exn
      | `Redirect of rep_id * IO.address
      | `Redirect_randomized of rep_id * IO.address
      | `Retry_later
      ]

  val make : ?election_period:float -> ?heartbeat_period:float ->
    (req_id * IO.op) Oraft.Core.state -> (rep_id * IO.address) list -> server

  val run     : server -> unit Lwt.t
  val abort   : server -> unit Lwt.t
  val execute : server -> PROC.op -> result Lwt.t
end
