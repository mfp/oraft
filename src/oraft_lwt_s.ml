module type LWTIO_TYPES = sig
  type op
  type connection
  type conn_manager
end

module type LWTIO = sig
  open Oraft.Types

  include LWTIO_TYPES

  val make_conn_manager :
    ?conn_wrapper:[ `Incoming | `Outgoing ] Oraft_lwt_conn_wrapper.conn_wrapper ->
    id:address -> Unix.sockaddr -> conn_manager Lwt.t

  val connect : conn_manager -> rep_id -> address -> connection option Lwt.t
  val send    : connection -> (req_id * op) message -> unit Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t

  val is_saturated : connection -> bool

  type snapshot_transfer

  val prepare_snapshot :
    connection -> index -> config -> snapshot_transfer option Lwt.t

  val send_snapshot : snapshot_transfer -> bool Lwt.t
end

module type SERVER_GENERIC = sig
  open Oraft.Types

  include LWTIO_TYPES

  type 'a server

  type cmd_error =
    | Exn of exn
    | Redirect of rep_id * address
    | Retry

  type 'a cmd_result   = ('a, cmd_error) result
  type ro_op_result = (unit, cmd_error) result

  type 'a execution =
    | Sync of 'a Lwt.t
    | Async of 'a Lwt.t

  type 'a apply     = 'a server -> op -> ('a, exn) result execution

  val make :
    'a apply -> ?election_period:float -> ?heartbeat_period:float ->
    (req_id * op) Oraft.Core.state -> conn_manager -> 'a server

  val config  : _ server -> config
  val run     : _ server -> unit Lwt.t
  val abort   : _ server -> unit Lwt.t
  val execute : 'a server -> op -> 'a cmd_result Lwt.t
  val readonly_operation : _ server -> ro_op_result Lwt.t

  val compact_log : _ server -> index -> unit

  module Config : sig
    type error =
      | Redirect of rep_id * address
      | Retry
      | Cannot_change
      | Unsafe_change of simple_config * passive_peers

    val get             : _ server -> config
    val add_failover    : _ server -> rep_id -> address -> (unit, error) result Lwt.t
    val remove_failover : _ server -> rep_id -> (unit, error) result Lwt.t
    val decommission    : _ server -> rep_id -> (unit, error) result Lwt.t
    val demote          : _ server -> rep_id -> (unit, error) result Lwt.t
    val promote         : _ server -> rep_id -> (unit, error) result  Lwt.t
    val replace         : _ server -> replacee:rep_id -> failover:rep_id -> (unit, error) result Lwt.t
  end
end

module type OP = sig
  type op

  val string_of_op : op -> string
  val op_of_string : string -> op
end

module type SERVER_CONF = sig
  open Oraft.Types
  include OP
  val node_sockaddr : address -> Unix.sockaddr
  val string_of_address : address -> string
end
