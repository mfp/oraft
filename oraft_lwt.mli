
module type LWTIO_TYPES =
sig
  type op
  type connection
  type conn_manager
end

module type LWTIO =
sig
  open Oraft.Types

  include LWTIO_TYPES

  val connect : conn_manager -> rep_id -> address -> connection option Lwt.t
  val send    : connection -> (req_id * op) message -> unit Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t

  type snapshot_transfer

  val prepare_snapshot :
    connection -> index -> config -> snapshot_transfer option Lwt.t

  val send_snapshot : snapshot_transfer -> bool Lwt.t
end

module type SERVER_GENERIC =
sig
  open Oraft.Types

  include LWTIO_TYPES

  type 'a server

  type gen_result =
      [ `Error of exn
      | `Redirect of rep_id * address
      | `Redirect_randomized of rep_id * address
      | `Retry_later ]

  type 'a cmd_result   = [ gen_result | `OK of 'a ]
  type ro_op_result = [ gen_result | `OK ]

  val make :
    ('a server -> op -> [`OK of 'a | `Error of exn] Lwt.t) ->
    ?election_period:float -> ?heartbeat_period:float ->
    (req_id * op) Oraft.Core.state -> conn_manager -> 'a server

  val run     : _ server -> unit Lwt.t
  val abort   : _ server -> unit Lwt.t
  val execute : 'a server -> op -> 'a cmd_result Lwt.t
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

    val add_failover    : _ server -> rep_id -> address -> result Lwt.t
    val remove_failover : _ server -> rep_id -> result Lwt.t
    val decommission    : _ server -> rep_id -> result Lwt.t
    val demote          : _ server -> rep_id -> result Lwt.t
    val promote         : _ server -> rep_id -> result Lwt.t
    val replace         : _ server -> replacee:rep_id -> failover:rep_id -> result Lwt.t
  end
end

module Make_server : functor(IO : LWTIO) ->
  SERVER_GENERIC with type op         = IO.op
                  and type connection = IO.connection

module type SERVER_CONF =
sig
  type op
  val string_of_op : op -> string
  val op_of_string : string -> op
  val sockaddr_of_string : string -> Unix.sockaddr
end

val open_connection :
  ?buffer_size : int -> Unix.sockaddr ->
  (Lwt_unix.file_descr * Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

module Simple_server : functor(C : SERVER_CONF) ->
  SERVER_GENERIC with type op = C.op
