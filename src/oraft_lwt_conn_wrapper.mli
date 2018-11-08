type -'a conn_wrapper

type simple_wrapper =
  Lwt_unix.file_descr -> (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

val make_client_conn_wrapper :
  simple_wrapper -> [`Outgoing] conn_wrapper

val make_server_conn_wrapper :
  incoming:simple_wrapper ->
  outgoing:simple_wrapper ->
  [`Incoming | `Outgoing] conn_wrapper

val wrap_outgoing_conn :
  [> `Outgoing] conn_wrapper -> Lwt_unix.file_descr ->
  (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

val wrap_incoming_conn :
  [> `Incoming] conn_wrapper -> Lwt_unix.file_descr ->
  (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

val trivial_conn_wrapper :
  ?buffer_size:int -> unit -> [< `Incoming | `Outgoing] conn_wrapper
