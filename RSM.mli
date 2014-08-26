open Oraft.Types

type config_change =
    Add_failover of rep_id * address
  | Remove_failover of rep_id
  | Decommission of rep_id
  | Demote of rep_id
  | Promote of rep_id
  | Replace of rep_id * rep_id

module type CONF =
sig
  include Oraft_lwt.SERVER_CONF
  val app_sockaddr : address -> Unix.sockaddr
end

module Make_client : functor (C : CONF) ->
sig
  exception Not_connected
  exception Bad_response

  type t

  val make : ?conn_wrapper:Oraft_lwt.conn_wrapper -> id:string -> unit -> t

  val connect : t -> addr:address -> unit Lwt.t

  val execute    : t -> C.op -> [ `Error of string | `OK of string ] Lwt.t
  val execute_ro : t -> C.op -> [ `Error of string | `OK of string ] Lwt.t
  val get_config : t -> [`Error of string | `OK of config ] Lwt.t

  val change_config :
    t -> config_change ->
    [ `Cannot_change
    | `Error of string
    | `OK
    | `Unsafe_change of simple_config * passive_peers ]
    Lwt.t
end

module Make_server : functor (C : CONF) ->
sig
  type 'a t

  module Core : Oraft_lwt.SERVER_GENERIC with type op = C.op

  type 'a execution = [`Sync of 'a Lwt.t | `Async of 'a Lwt.t]
  type 'a apply     = 'a Core.server -> C.op -> [`OK of 'a | `Error of exn] execution

  val make :
    'a apply -> address ->
    ?conn_wrapper:Oraft_lwt.conn_wrapper ->
    ?join:address ->
    ?election_period:float ->
    ?heartbeat_period:float -> rep_id -> 'a t Lwt.t

  val run : string t -> unit Lwt.t
end
