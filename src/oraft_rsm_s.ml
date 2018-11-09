open Oraft.Types

type config_change = Oraft_proto.Config_change.config_change =
    Add_failover of rep_id * address
  | Remove_failover of rep_id
  | Decommission of rep_id
  | Demote of rep_id
  | Promote of rep_id
  | Replace of rep_id * rep_id

module type CONF = sig
  include Oraft_lwt_s.SERVER_CONF
  val app_sockaddr : address -> Unix.sockaddr
end

module type CLIENT = sig
  exception Not_connected
  exception Bad_response

  type op
  type t

  val make :
    ?conn_wrapper:[> `Outgoing] Oraft_lwt_conn_wrapper.conn_wrapper -> id:string -> unit -> t

  val connect : t -> addr:address -> unit Lwt.t

  val execute    : t -> op -> (string, string) result Lwt.t
  val execute_ro : t -> op -> (string, string) result Lwt.t
  val get_config : t -> (config, string) result Lwt.t

  type change_config_error =
    | Cannot_change
    | Error of string
    | Unsafe_change of simple_config * passive_peers

  val change_config :
    t -> config_change -> (unit, change_config_error) result Lwt.t
end

module type SERVER = sig
  type op
  type 'a t

  module Core : Oraft_lwt_s.SERVER_GENERIC with type op = op

  type 'a apply = 'a Core.server -> op -> ('a, exn) result Core.execution

  val make :
    'a apply -> address ->
    ?conn_wrapper:[`Outgoing | `Incoming] Oraft_lwt_conn_wrapper.conn_wrapper ->
    ?join:address ->
    ?election_period:float ->
    ?heartbeat_period:float -> rep_id -> 'a t Lwt.t

  val run : string t -> unit Lwt.t
end
