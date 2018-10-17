
(* Trivial distributed key-value service.
 *
 * Usage:
 *
 * (1) Launch of 1st node (will be master with quorum = 1 at first):
 *
 *     ./dict master n1a,n1b
 *         (uses UNIX domain sockets  n1a for Raft communication,
 *          n1b as address for app server -- use   ip1:port1,ip2:port2
 *          to listen at ip1:port1 for Raft, ip1:port2 for app)
 *
 *
 * (2) Launch extra nodes. They will join the cluster and the quorum will be
 *     updated.
 *
 *     ./dict master n2a,n2b --join n1a,n1b
 *
 *     ./dict master n3a,n3b --join n1a,n1b
 *
 *     ...
 *
 * (3) perform client reqs
 *
 *     ./dict get n1a,n1b foo             # retrieve value assoc'ed
 *                                        # to key "foo", block until
 *                                        # available
 *
 *     ./dict set n1a,n1b foo bar         # associate 'bar' to 'foo'
 *
 * *)

open Cmdliner
open Lwt.Infix

module String  = BatString
module Hashtbl = BatHashtbl
module Option  = BatOption

type op =
    Get of string
  | Wait of string
  | Set of string * string

module CONF =
struct
  type nonrec op = op

  let string_of_op = function
      Get v -> "?" ^ v
    | Wait v -> "<" ^ v
    | Set (k, v) -> "!" ^ k ^ "=" ^ v

  let op_of_string s =
    if s = "" then failwith "bad op"
    else
      match s.[0] with
          '?' -> Get (String.slice ~first:1 s)
        | '<' -> Wait (String.slice ~first:1 s)
        | '!' ->
            let k, v = String.slice ~first:1 s |> String.split ~by:"=" in
              Set (k, v)
        | _ -> failwith "bad op"

  let sockaddr s =
    try
      let host, port = String.split ~by:":" s in
        Unix.ADDR_INET (Unix.inet_addr_of_string host, int_of_string port)
    with Not_found ->
      Unix.ADDR_UNIX s

  let node_sockaddr s = String.split ~by:"," s |> fst |> sockaddr
  let app_sockaddr  s =
    Printf.printf "Connecting to %s\n%!" s;
    String.split ~by:"," s |> snd |> sockaddr

  let string_of_address s = s
end

module SERVER = Oraft_rsm.Make_server(CONF)
module CLIENT = Oraft_rsm.Make_client(CONF)

let make_tls_wrapper tls =
  Option.map
    (fun (client_config, server_config) ->
       Oraft_lwt_tls.make_server_wrapper
         ~client_config ~server_config)
    tls

let run_server ?tls ~addr ?join ~id () =
  let h    = Hashtbl.create 13 in
  let cond = Lwt_condition.create () in

  let exec _ op = match op with
      Get s -> `Sync (Lwt.return (try `OK (Hashtbl.find h s) with Not_found -> `OK ""))
    | Wait k ->
        `Async begin
          let rec attempt () =
            match Hashtbl.Exceptionless.find h k with
                Some v -> Lwt.return (`OK v)
              | None ->
                  Lwt_condition.wait cond >>= fun () ->
                  attempt ()
          in
            attempt ()
        end
    | Set (k, v) ->
        if v = "" then
          Hashtbl.remove h k
        else begin
          Hashtbl.add h k v;
          Lwt_condition.broadcast cond ();
        end;
        `Sync (Lwt.return (`OK ""))
  in

  let%lwt server = SERVER.make ?conn_wrapper:(make_tls_wrapper tls) exec addr ?join id in
    SERVER.run server

let client_op ?tls ~addr op =
  let c    = CLIENT.make
               ?conn_wrapper:(make_tls_wrapper tls)
               ~id:(string_of_int (Unix.getpid ())) () in
  let exec = match op with
               | Get _ | Wait _ -> CLIENT.execute_ro
               | Set _ -> CLIENT.execute
  in
    CLIENT.connect c ~addr >>= fun () ->
    match%lwt exec c op with
      | `OK s -> Printf.printf "+OK %s\n" s; Lwt.return_unit
      | `Error s -> Printf.printf "-ERR %s\n" s; Lwt.return_unit

let ro_benchmark ?tls ?(iterations = 10_000) ~addr () =
  let c    = CLIENT.make
               ?conn_wrapper:(make_tls_wrapper tls)
               ~id:(string_of_int (Unix.getpid ())) ()
  in
    CLIENT.connect c ~addr >>= fun () ->
    CLIENT.execute c (Set ("bm", "0")) >>= fun _ ->
    let t0 = Unix.gettimeofday () in
      for%lwt i = 1 to iterations do
        let%lwt _ = CLIENT.execute_ro c (Get "bm") in
          Lwt.return_unit
      done >>= fun () ->
      let dt = Unix.gettimeofday () -. t0 in
        Printf.printf "%.0f RO ops/s\n" (float iterations /. dt);
        Lwt.return_unit

let wr_benchmark ?tls ?(iterations = 10_000) ~addr () =
  let c    = CLIENT.make
               ?conn_wrapper:(make_tls_wrapper tls)
               ~id:(string_of_int (Unix.getpid ())) ()
  in
    CLIENT.connect c ~addr >>= fun () ->
    let t0 = Unix.gettimeofday () in
      for%lwt i = 1 to iterations do
        let%lwt _ = CLIENT.execute c (Set ("bm", "")) in
          Lwt.return_unit
      done >>= fun () ->
      let dt = Unix.gettimeofday () -. t0 in
        Printf.printf "%.0f WR ops/s\n" (float iterations /. dt);
        Lwt.return_unit

let tls_create cert priv_key =
  X509_lwt.private_of_pems ~cert ~priv_key >|= fun cert ->
  (* FIXME: authenticator *)
  Tls.Config.(client ~authenticator:X509.Authenticator.null (),
              server ~certificates:(`Single cert) ())

let lwt_reporter () =
  let buf_fmt ~like =
    let b = Buffer.create 512 in
    Fmt.with_buffer ~like b,
    fun () -> let m = Buffer.contents b in Buffer.reset b; m
  in
  let app, app_flush = buf_fmt ~like:Fmt.stdout in
  let dst, dst_flush = buf_fmt ~like:Fmt.stderr in
  let reporter = Logs_fmt.reporter ~app ~dst () in
  let report src level ~over k msgf =
    let k () =
      let write () = match level with
      | Logs.App -> Lwt_io.write Lwt_io.stdout (app_flush ())
      | _ -> Lwt_io.write Lwt_io.stderr (dst_flush ())
      in
      let unblock () = over (); Lwt.return_unit in
      Lwt.finalize write unblock |> Lwt.ignore_result;
      k ()
    in
    reporter.Logs.report src level ~over:(fun () -> ()) k msgf;
  in
  { Logs.report = report }

let setup_log style_renderer level =
  Fmt_tty.setup_std_outputs ?style_renderer ();
  Logs.set_level level;
  Logs.set_reporter (lwt_reporter ());
  ()

let setup_log =
  Term.(const setup_log $ Fmt_cli.style_renderer () $ Logs_cli.level ())

let tls =
  Arg.(value & opt (t2 string string) ("", "") & info ["tls"] ~doc:"Use TLS")

let tls_create (cert, priv_key) =
  match cert, priv_key with
    | "", "" -> Lwt.return_none
    | _ -> tls_create cert priv_key >>= Lwt.return_some

let master tls addr join () =
  tls_create tls >>= fun tls ->
  run_server ?tls ~addr ?join ~id:addr ()

let master_cmd =
  let listen =
    let doc = "Where to listen." in
      Arg.(required
           & pos 0 (some string) None
           & info [] ~docv:"ADDRESS" ~doc) in
  let join =
    let doc = "Other servers to join." in
      Arg.(value & opt (some string) None &
           info ["join"] ~doc ~docv:"ADDRESS") in
  let doc = "Launch a server" in
    Term.(const master $ tls $ listen $ join $ setup_log),
    Term.info ~doc "master"

let get_key tls addr k () =
  tls_create tls >>= fun tls ->
  client_op ?tls ~addr (Wait k)

let set_key tls addr k v () =
  tls_create tls >>= fun tls ->
  client_op ?tls ~addr (Set (k, v))

let connect_arg =
  let doc = "Where to connect." in
    Arg.(required
         & pos 0 (some string) None
         & info [] ~docv:"ADDRESS" ~doc)

let get_cmd =
  let doc = "Get a key" in
  let k =
    Arg.(required
         & pos 1 (some string) None
         & info [] ~docv:"STRING" ~doc) in
    Term.(const get_key $ tls $ connect_arg $ k $ setup_log),
    Term.info ~doc "get"

let set_cmd =
  let doc = "Set a key" in
  let k =
    Arg.(required
         & pos 1 (some string) None
         & info [] ~docv:"STRING" ~doc) in
  let v =
    Arg.(required
         & pos 2 (some string) None
         & info [] ~docv:"STRING" ~doc) in
    Term.(const set_key $ tls $ connect_arg $ k $ v $ setup_log),
    Term.info ~doc "set"

let ro_bm tls addr iterations () =
  tls_create tls >>= fun tls ->
  ro_benchmark ?tls ~iterations ~addr ()

let ro_bm_cmd =
  let doc = "Run the RO benchmark" in
  let nb_iters =
    Arg.(required
         & pos 1 (some int) None
         & info [] ~docv:"ITERATIONS" ~doc) in
    Term.(const ro_bm $ tls $ connect_arg $ nb_iters $ setup_log),
    Term.info ~doc "ro_bench"

let rw_bm tls addr iterations =
  tls_create tls >>= fun tls ->
  wr_benchmark ?tls ~iterations ~addr ()

let rw_bm_cmd =
  let doc = "Run the RW benchmark" in
  let nb_iters =
    Arg.(required
         & pos 1 (some int) None
         & info [] ~docv:"ITERATIONS" ~doc) in
    Term.(const ro_bm $ tls $ connect_arg $ nb_iters $ setup_log),
    Term.info ~doc "rw_bench"

let lwt_run v =
  Lwt.async_exception_hook := begin fun exn ->
    Logs.err (fun m -> m "%a" Oraft_lwt.pp_exn exn) ;
  end ;
  Lwt_main.run v

let cmds =
  List.map begin fun (term, info) ->
    Term.((const lwt_run) $ term), info
  end [
    master_cmd ;
    get_cmd ;
    set_cmd ;
    ro_bm_cmd ;
    rw_bm_cmd ;
  ]

let default_cmd =
  let doc = "Dict: Trivial distributed key-value service." in
  Term.(ret (const (`Help (`Pager, None)))),
  Term.info ~doc "dict"

let () = match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1
  | #Term.result -> exit 0
