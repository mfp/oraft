open Lwt.Infix

let wrap_incoming_conn client_config fd =
  try%lwt
    (try Lwt_unix.set_close_on_exec fd with Invalid_argument _ -> ());
    Tls_lwt.(Unix.client_of_fd ~host:"" client_config fd >|= of_t)
  with exn ->
    Lwt_unix.close fd >>= fun () ->
    Lwt.fail exn

let wrap_outgoing_conn server_config fd =
  Tls_lwt.(Unix.server_of_fd server_config fd >|= of_t)

let make_client_wrapper ~client_config =
  Oraft_lwt.make_client_conn_wrapper (wrap_incoming_conn client_config)

let make_server_wrapper ~client_config ~server_config =
  Oraft_lwt.make_server_conn_wrapper
    ~outgoing:(wrap_incoming_conn client_config)
    ~incoming:(wrap_outgoing_conn server_config)
