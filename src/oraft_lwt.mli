open Oraft_lwt_s

val string_of_config :
  (Oraft.Types.address -> string) -> Oraft.Types.config -> string

val pp_exn : Format.formatter -> exn -> unit
val pp_saddr : Format.formatter -> Unix.sockaddr -> unit


module Make_server : functor(IO : LWTIO) ->
  SERVER_GENERIC with type op         = IO.op
                  and type connection = IO.connection

type -'a conn_wrapper

type simple_wrapper =
  Lwt_unix.file_descr -> (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

val make_client_conn_wrapper : simple_wrapper -> [`Outgoing] conn_wrapper

val make_server_conn_wrapper :
  incoming:simple_wrapper -> outgoing:simple_wrapper ->
  [`Incoming | `Outgoing] conn_wrapper

val wrap_outgoing_conn :
  [> `Outgoing] conn_wrapper -> Lwt_unix.file_descr ->
  (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

val wrap_incoming_conn :
  [> `Incoming] conn_wrapper -> Lwt_unix.file_descr ->
  (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

val trivial_conn_wrapper :
  ?buffer_size:int -> unit -> [< `Incoming | `Outgoing] conn_wrapper

module Simple_server : functor(C : SERVER_CONF) ->
sig
  include SERVER_GENERIC with type op = C.op

  val make_conn_manager :
    ?conn_wrapper:[`Incoming | `Outgoing] conn_wrapper ->
    id:string -> Unix.sockaddr -> conn_manager Lwt.t
end
