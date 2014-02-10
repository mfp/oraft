
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

  type snapshot_transfer

  val prepare_snapshot :
    connection -> index -> config -> snapshot_transfer option Lwt.t

  val send_snapshot : snapshot_transfer -> unit Lwt.t
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

  type gen_result =
      [ `Error of exn
      | `Redirect of rep_id * IO.address
      | `Redirect_randomized of rep_id * IO.address
      | `Retry_later ]

  type cmd_result   = [ gen_result | `OK of PROC.resp ]
  type ro_op_result = [ gen_result | `OK ]

  val make : ?election_period:float -> ?heartbeat_period:float ->
    (req_id * IO.op) Oraft.Core.state -> (rep_id * IO.address) list -> server

  val run     : server -> unit Lwt.t
  val abort   : server -> unit Lwt.t
  val execute : server -> PROC.op -> cmd_result Lwt.t
  val readonly_operation : server -> ro_op_result Lwt.t

  module Config :
  sig
    type result =
      [
      | `OK
      | `Redirect of rep_id option
      | `Retry
      | `Cannot_change
      | `Unsafe_change of simple_config * passive_peers
      ]

    val add_failover    : server -> rep_id -> IO.address -> result Lwt.t
    val remove_failover : server -> rep_id -> result Lwt.t
    val decommission    : server -> rep_id -> result Lwt.t
    val demote          : server -> rep_id -> result Lwt.t
    val promote         : server -> rep_id -> result Lwt.t
    val replace         : server -> replacee:rep_id -> failover:rep_id -> result Lwt.t
  end
end
