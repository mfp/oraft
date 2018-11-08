type 'a conn_wrapper = {
  wrap_incoming_conn :
    Lwt_unix.file_descr ->
    (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t;
  wrap_outgoing_conn :
    Lwt_unix.file_descr ->
    (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t;
}

type simple_wrapper =
  Lwt_unix.file_descr -> (Lwt_io.input_channel * Lwt_io.output_channel) Lwt.t

let make_client_conn_wrapper f = {
  wrap_incoming_conn =
    (fun fd -> Lwt.fail_with "Incoming conn wrapper invoked in client");
  wrap_outgoing_conn = f;
}

let make_server_conn_wrapper ~incoming ~outgoing =
  { wrap_incoming_conn = incoming; wrap_outgoing_conn = outgoing }

let trivial_wrap_outgoing_conn ?buffer_size fd =
  let close =
    lazy begin
      (try%lwt
        Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL;
        Lwt.return_unit
      with Unix.Unix_error(Unix.ENOTCONN, _, _) ->
        (* This may happen if the server closed the connection before us *)
        Lwt.return_unit)
        [%finally
          Lwt_unix.close fd]
    end in
  let buf1 = BatOption.map Lwt_bytes.create buffer_size in
  let buf2 = BatOption.map Lwt_bytes.create buffer_size in
    try%lwt
      (try Lwt_unix.set_close_on_exec fd with Invalid_argument _ -> ());
      Lwt.return (Lwt_io.make ?buffer:buf1
                ~close:(fun _ -> Lazy.force close)
                ~mode:Lwt_io.input (Lwt_bytes.read fd),
              Lwt_io.make ?buffer:buf2
                ~close:(fun _ -> Lazy.force close)
                ~mode:Lwt_io.output (Lwt_bytes.write fd))
    with exn ->
      let%lwt () = Lwt_unix.close fd in
      Lwt.fail exn

let trivial_wrap_incoming_conn ?buffer_size fd =
  let buf1 = BatOption.map Lwt_bytes.create buffer_size in
  let buf2 = BatOption.map Lwt_bytes.create buffer_size in
    Lwt.return
      (Lwt_io.of_fd ?buffer:buf1 ~mode:Lwt_io.input fd,
       Lwt_io.of_fd ?buffer:buf2 ~mode:Lwt_io.output fd)

let trivial_conn_wrapper ?buffer_size () =
  { wrap_incoming_conn = trivial_wrap_incoming_conn ?buffer_size;
    wrap_outgoing_conn = trivial_wrap_outgoing_conn ?buffer_size;
  }

let wrap_outgoing_conn w fd = w.wrap_outgoing_conn fd
let wrap_incoming_conn w fd = w.wrap_incoming_conn fd
