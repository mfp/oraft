open Oraft_lwt_s
open Oraft_lwt_conn_wrapper

val string_of_config :
  (Oraft.Types.address -> string) -> Oraft.Types.config -> string

val pp_exn : Format.formatter -> exn -> unit
val pp_saddr : Format.formatter -> Unix.sockaddr -> unit

module Make_server (IO : LWTIO) : SERVER_GENERIC
  with type op           = IO.op
   and type connection   = IO.connection
   and type conn_manager = IO.conn_manager
(** Specialization of [oraft] on top of Lwt IO. A ready-to-use
    implementation of a module of signature [LWTIO] can be found in
    [Oraft_lwt_simple_io]. *)
