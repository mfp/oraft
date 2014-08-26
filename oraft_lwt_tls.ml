open Lwt

let make_conn_wrapper ~client_config ~server_config () =

  let wrap_incoming_conn ?buffer_size fd =
    try_lwt
      (try Lwt_unix.set_close_on_exec fd with Invalid_argument _ -> ());
      Tls_lwt.(Unix.client_of_fd ~host:"" client_config fd >|= of_t)
    with exn ->
      lwt () = Lwt_unix.close fd in
      raise_lwt exn in

  let wrap_outgoing_conn ?buffer_size fd =
    Tls_lwt.(Unix.server_of_fd server_config fd >|= of_t)
  in
    { Oraft_lwt.wrap_incoming_conn; wrap_outgoing_conn }
