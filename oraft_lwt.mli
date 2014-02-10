
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

  val send_snapshot : snapshot_transfer -> bool Lwt.t
end

module Make_server : functor(IO : LWTIO) ->
sig
  open Oraft.Types

  type 'a server

  type gen_result =
      [ `Error of exn
      | `Redirect of rep_id * IO.address
      | `Redirect_randomized of rep_id * IO.address
      | `Retry_later ]

  type 'a cmd_result   = [ gen_result | `OK of 'a ]
  type ro_op_result = [ gen_result | `OK ]

  val make :
    ('a server -> IO.op -> [`OK of 'a | `Error of exn] Lwt.t) ->
    ?election_period:float -> ?heartbeat_period:float ->
    (req_id * IO.op) Oraft.Core.state -> (rep_id * IO.address) list -> 'a server

  val run     : _ server -> unit Lwt.t
  val abort   : _ server -> unit Lwt.t
  val execute : 'a server -> IO.op -> 'a cmd_result Lwt.t
  val readonly_operation : _ server -> ro_op_result Lwt.t

  val compact_log : _ server -> index -> unit

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

    val add_failover    : _ server -> rep_id -> IO.address -> result Lwt.t
    val remove_failover : _ server -> rep_id -> result Lwt.t
    val decommission    : _ server -> rep_id -> result Lwt.t
    val demote          : _ server -> rep_id -> result Lwt.t
    val promote         : _ server -> rep_id -> result Lwt.t
    val replace         : _ server -> replacee:rep_id -> failover:rep_id -> result Lwt.t
  end
end
