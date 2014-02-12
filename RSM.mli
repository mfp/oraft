open Oraft.Types

module Make_client : functor (C : Oraft_lwt.SERVER_CONF) ->
sig
  exception Not_connected
  exception Bad_response

  type t

  val make : id:string -> unit -> t

  val execute    : t -> C.op -> [ `Error of string | `OK of string ] Lwt.t
  val execute_ro : t -> C.op -> [ `Error of string | `OK of string ] Lwt.t
  val get_config : t -> [`Error of string | `OK of config ] Lwt.t

  val change_config :
    t -> Oraft_proto.Config_change.config_change ->
    [ `Cannot_change
    | `Error of string
    | `OK
    | `Unsafe_change of simple_config * passive_peers ]
    Lwt.t
end

module Make_server : functor (C : Oraft_lwt.SERVER_CONF) ->
sig
  type 'a t

  module SS : Oraft_lwt.SERVER_GENERIC with type op = C.op

  val make :
    ('a SS.server -> SS.op -> [ `Error of exn | `OK of 'a ] Lwt.t) ->
    Unix.sockaddr -> address ->
    ?election_period:float ->
    ?heartbeat_period:float -> rep_id -> 'a t Lwt.t

  val run : string t -> 'a Lwt.t
end
