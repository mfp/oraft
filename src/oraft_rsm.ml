open Lwt.Infix

open Oraft.Types
open Oraft_rsm_s

module Map     = BatMap
module Hashtbl = BatHashtbl

module MB = Extprot.Msg_buffer

type 'a conn =
    {
      addr    : 'a;
      ich     : Lwt_io.input_channel;
      och     : Lwt_io.output_channel;
      in_buf  : Bytes.t ref;
      out_buf : MB.t;
    }

let send_msg write conn msg =
  (* Since the buffers are private to the conn AND Lwt_io.atomic prevents
   * concurrent IO operations, it's safe to reuse the buffer for a given
   * channel across send_msg calls.  *)
  Lwt_io.atomic
    (fun och ->
       let buf = conn.out_buf in
         MB.clear buf;
         write buf msg;
         Lwt_io.LE.write_int och (MB.length buf) >>= fun () ->
         Lwt_io.write_from_exactly
           och (MB.unsafe_contents buf) 0  (MB.length buf) >>= fun () ->
         Lwt_io.flush och)
    conn.och

let read_msg read conn =
  (* same as send_msg applies here regarding the buffers *)
  Lwt_io.atomic
    (fun ich ->
       let%lwt len = Lwt_io.LE.read_int ich in
       let buf = conn.in_buf in
         if Bytes.length !buf < len then buf := Bytes.create len;
         Lwt_io.read_into_exactly ich !buf 0 len >>= fun () ->
         Lwt.return (Extprot.Conv.deserialize read (Bytes.unsafe_to_string !buf)))
    conn.ich

module Make_client(C : CONF) = struct
  module M = Map.Make(String)

  open Oraft_proto
  open Client_msg
  open Client_op
  open Server_msg
  open Response

  exception Not_connected
  exception Bad_response

  module H = Hashtbl.Make(struct
      type t = Int64.t
      let hash          = Hashtbl.hash
      let equal i1 i2 = Int64.compare i1 i2 = 0
    end)

  type t =
    {
      id             : string;
      mutable dst    : address conn option;
      mutable conns  : address conn M.t;
      mutable req_id : Int64.t;
      pending_reqs   : response Lwt.u H.t;
      conn_wrapper   : [`Outgoing] Oraft_lwt_conn_wrapper.conn_wrapper;
    }

  and address = string

  let src = Logs.Src.create "oraft_rsm.client"

  let trivial_wrapper () =
    (Oraft_lwt_conn_wrapper.trivial_conn_wrapper () :>
       [`Outgoing] Oraft_lwt_conn_wrapper.conn_wrapper)

  let make ?conn_wrapper ~id () =
    { id; dst = None; conns = M.empty; req_id = 0L;
      pending_reqs = H.create 13;
      conn_wrapper = Option.map_default
                       (fun w -> (w :> [`Outgoing] Oraft_lwt_conn_wrapper.conn_wrapper))
                       (trivial_wrapper ())
                       conn_wrapper;
    }

  let gen_id t =
    t.req_id <- Int64.succ t.req_id;
    t.req_id

  let send_msg conn msg = send_msg Oraft_proto.Client_msg.write conn msg
  let read_msg conn     = read_msg Oraft_proto.Server_msg.read conn

  let connect t peer_id addr =
    let do_connect () =
      let saddr = C.app_sockaddr addr in
      let fd = Lwt_unix.socket (Unix.domain_of_sockaddr saddr) Unix.SOCK_STREAM 0 in
      let%lwt () = Lwt_unix.connect fd saddr in
      let%lwt ich, och     = Oraft_lwt_conn_wrapper.wrap_outgoing_conn t.conn_wrapper fd in
      let out_buf      = MB.create () in
      let in_buf       = ref Bytes.empty in
      let conn         = { addr; ich; och; in_buf; out_buf } in
        (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
        (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
        try%lwt
          send_msg conn { id = 0L; op = (Connect t.id) } >>= fun () ->
          match%lwt read_msg conn with
            | { response = OK id; _ } ->
                t.conns <- M.add id conn t.conns;
                t.dst <- Some conn;
                ignore begin
                  let rec loop_recv () =
                    let%lwt msg = read_msg conn in
                      Logs_lwt.debug ~src begin fun m ->
                        m "Received from server@ %s"
                          (Extprot.Pretty_print.pp Oraft_proto.Server_msg.pp msg)
                      end >>= fun () ->
                      match H.Exceptionless.find t.pending_reqs msg.id with
                          None -> loop_recv ()
                        | Some u ->
                            Lwt.wakeup_later u msg.response;
                            loop_recv ()
                  in
                    (loop_recv ())
                      [%finally
                        t.conns <- M.remove peer_id t.conns;
                        Lwt_io.abort och]
                end;
                Lwt.return_unit
            | _ -> failwith "conn refused"
        with exn ->
          Logs_lwt.err ~src begin fun m ->
            m "Error while connecting (%s)" (Printexc.to_string exn)
          end >>= fun () ->
          t.conns <- M.remove peer_id t.conns;
          Lwt_io.abort och
    in
      match M.Exceptionless.find peer_id t.conns with
          Some conn when conn.addr = addr ->
            t.dst <- Some conn;
            Lwt.return_unit
        | Some conn (* when addr <> address *) ->
            t.conns <- M.remove peer_id t.conns;
            Lwt_io.abort conn.och >>= fun () -> do_connect ()
        | None -> do_connect ()

  let send_and_await_response t op f =
    match t.dst with
        None -> Lwt.fail Not_connected
      | Some c ->
          let th, u = Lwt.task () in
          let id    = gen_id t in
          let msg   = { id; op; } in
            H.add t.pending_reqs id u;
            Logs_lwt.debug ~src begin fun m ->
              m "Sending to server@ %s"
                (Extprot.Pretty_print.pp Oraft_proto.Client_msg.pp msg)
            end >>= fun () ->
            send_msg c msg >>= fun () ->
            let%lwt x = th in
              f c.addr x

  let rec do_execute t op =
    send_and_await_response t op
      (fun dst resp -> match resp with
           OK s -> Lwt.return_ok s
         | Error s -> Lwt.return_error s
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>= fun () ->
             do_execute t op
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>= fun () ->
             do_execute t op
         | Cannot_change | Unsafe_change _ | Config _ ->
             Lwt.fail Bad_response)

  let execute t op =
    do_execute t (Execute (C.string_of_op op))

  let execute_ro t op =
    do_execute t (Execute_RO (C.string_of_op op))

  let rec get_config t =
    send_and_await_response t Get_config
      (fun dst resp -> match resp with
           Config c -> Lwt.return_ok c
         | Error x -> Lwt.return_error x
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>= fun () ->
             get_config t
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>= fun () ->
             get_config t
         | OK _ | Cannot_change | Unsafe_change _ ->
             Lwt.fail Bad_response)

  type change_config_error =
    | Cannot_change
    | Error of string
    | Unsafe_change of simple_config * passive_peers

  let rec change_config t op =
    send_and_await_response t (Change_config op)
      (fun dst resp -> match resp with
           OK _ -> Lwt.return_ok ()
         | Error x -> Lwt.return_error (Error x)
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>= fun () ->
             change_config t op
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>= fun () ->
             change_config t op
         | Cannot_change -> Lwt.return_error Cannot_change
         | Unsafe_change (c, p) -> Lwt.return_error (Unsafe_change (c, p))
         | Config _ -> Lwt.fail Bad_response)

  let connect t ~addr = connect t "" addr
end

module Make_server(C : CONF) = struct
  module IO = Oraft_lwt_extprot_io.Make(C)
  module SS   = Oraft_lwt.Make_server(IO)
  module SSC  = SS.Config
  module CC   = Make_client(C)

  module Core = SS

  open Oraft_proto
  open Client_msg
  open Client_op
  open Server_msg
  open Response
  open Config_change

  type 'a apply =
    'a Core.server -> C.op -> ('a, exn) result Core.execution

  type 'a t =
    {
      id            : rep_id;
      addr          : string;
      c             : CC.t option;
      node_sockaddr : Unix.sockaddr;
      app_sockaddr  : Unix.sockaddr;
      serv          : 'a SS.server;
      exec          : 'a apply;
      conn_wrapper  : [`Incoming | `Outgoing] Oraft_lwt_conn_wrapper.conn_wrapper;
    }

  let src = Logs.Src.create "oraft_rsm.server"

  let raise_if_error = function
    | Ok x -> Lwt.return x
    | Error s -> Lwt.fail_with s

  let check_config_err :
    (unit, CC.change_config_error) result -> unit Lwt.t = function
    | Ok () -> Lwt.return_unit
    | Error (Error s) -> Lwt.fail_with s
    | Error Cannot_change -> Lwt.fail_with "Cannot perform config change"
    | Error (Unsafe_change _) -> Lwt.fail_with "Unsafe config change"

  let make exec addr
        ?(conn_wrapper = Oraft_lwt_conn_wrapper.trivial_conn_wrapper ())
        ?join ?election_period ?heartbeat_period id =
    begin match join with
      | None ->
          Lwt.return (Simple_config ([id, addr], []), None)
      | Some peer_addr ->
          let c = CC.make ~conn_wrapper ~id () in
            Logs_lwt.info ~src begin fun m ->
              m "Connecting to %S" (peer_addr |> C.string_of_address)
            end >>= fun () ->
            CC.connect c ~addr:peer_addr >>= fun () ->
            CC.get_config c >>= raise_if_error >>= fun config ->
            Logs_lwt.info ~src begin fun m ->
              m "Got initial configuration %s"
                (Oraft_lwt.string_of_config C.string_of_address config)
            end >>= fun () ->
            Lwt.return (config, Some c)
    end >>= fun (config, c) ->
    let state         = Oraft.Core.make
                          ~id ~current_term:0L ~voted_for:None
                          ~log:[] ~config () in
    let node_sockaddr = C.node_sockaddr addr in
    let app_sockaddr  = C.app_sockaddr addr in
    let%lwt conn_mgr  = IO.make_conn_manager ~id node_sockaddr in
    let serv          = SS.make exec ?election_period ?heartbeat_period
                          state conn_mgr in
      Lwt.return { id; addr; c ; node_sockaddr;
                   app_sockaddr; serv; exec; conn_wrapper; }

  let send_msg conn msg = send_msg Oraft_proto.Server_msg.write conn msg
  let read_msg conn     = read_msg Oraft_proto.Client_msg.read conn

  let map_op_result : string Core.cmd_result -> response = function
    | Error (Exn exn) -> Error (Printexc.to_string exn)
    | Error (Redirect (peer_id, addr)) -> Redirect (peer_id, addr)
    | Error Retry -> Retry
    | Ok response -> OK response

  let map_apply = function
    | Result.Error exn -> Error (Printexc.to_string exn)
    | Ok _ -> OK ""

  let perform_change t op =
    let map = function
      | Ok () -> OK ""
      | Error SSC.Cannot_change -> Cannot_change
      | Error Unsafe_change (c, p) -> Unsafe_change (c, p)
      | Error Redirect (peer_id, addr) -> Redirect (peer_id, addr)
      | Error Retry -> Retry
    in
      try%lwt
        let%lwt ret =
          match op with
              Add_failover (peer_id, addr) -> SSC.add_failover t.serv peer_id addr
            | Remove_failover peer_id -> SSC.remove_failover t.serv peer_id
            | Decommission peer_id -> SSC.decommission t.serv peer_id
            | Demote peer_id -> SSC.demote t.serv peer_id
            | Promote peer_id -> SSC.promote t.serv peer_id
            | Replace (replacee, failover) -> SSC.replace t.serv ~replacee ~failover
        in
          Lwt.return (map ret)
      with exn ->
        Logs_lwt.debug ~src begin fun m ->
          m "Error while changing cluster configuration@ %s: %s"
            (Extprot.Pretty_print.pp pp_config_change op)
            (Printexc.to_string exn)
        end >>= fun () ->
        Lwt.return (Error (Printexc.to_string exn))

  let process_message t client_id conn = function
      { id; op = Connect _ } ->
        send_msg conn { id; response = Error "Unexpected request" }
    | { id; op = Get_config } ->
        let config = SS.Config.get t.serv in
          send_msg conn { id; response = Config config }
    | { id; op = Change_config x } ->
        Logs_lwt.info ~src begin fun m ->
          m "Config change requested:@ %s"
            (Extprot.Pretty_print.pp pp_config_change x)
        end >>= fun () ->
        let%lwt response = perform_change t x in
          Logs_lwt.info ~src begin fun m ->
            m "Config change result:@ %s"
              (Extprot.Pretty_print.pp pp_response response)
          end >>= fun () ->
          Logs_lwt.info ~src begin fun m ->
            m "New config:@ %s"
              (Oraft_lwt.string_of_config C.string_of_address (SS.config t.serv))
          end >>= fun () ->
          send_msg conn { id; response }
    | { id; op = Execute_RO op; } -> begin
        match%lwt SS.readonly_operation t.serv with
          | Error (Redirect _) | Error Retry | Error _ as x ->
              let response = map_op_result x in
                send_msg conn { id; response; }
          | Ok () ->
              match t.exec t.serv (C.op_of_string op) with
                | Sync resp ->
                    let%lwt resp = resp in
                      send_msg conn { id; response = map_apply resp }
                | Async resp ->
                    ignore begin try%lwt
                        let%lwt resp = try%lwt resp
                          with exn -> Lwt.return_error exn
                        in
                          send_msg conn { id; response = map_apply resp }
                      with exn ->
                        Logs_lwt.debug ~src
                          (fun m -> m "Caught exn: %a" Oraft_lwt.pp_exn exn)
                    end;
                    Lwt.return_unit
      end
    | { id; op = Execute op; } ->
        let%lwt response = SS.execute t.serv (C.op_of_string op) >|= map_op_result in
          send_msg conn { id; response }

  let rec request_loop t client_id conn =
    let%lwt msg = read_msg conn in
      ignore begin
        try%lwt
          process_message t client_id conn msg
        with exn ->
          Logs_lwt.debug ~src begin fun m ->
            m "Error while processing message@ %s: %s"
              (Extprot.Pretty_print.pp Oraft_proto.Client_msg.pp msg)
              (Printexc.to_string exn)
          end >>= fun () ->
          send_msg conn { id = msg.id; response = Error (Printexc.to_string exn) }
      end;
      request_loop t client_id conn

  let is_in_config t config =
    let all = match config with
      | Simple_config (a, p) -> a @ p
      | Joint_config (a1, a2, p) -> a1 @ a2 @ p
    in
      List.mem_assoc t.id all

  let is_active t config =
    let active = match config with
      | Simple_config (a, _) -> a
      | Joint_config (a1, a2, _) -> a1 @ a2
    in
      List.mem_assoc t.id active

  let add_as_failover_if_needed t c config =
    if is_in_config t config then
      Lwt.return_unit
    else begin
      Logs_lwt.info ~src begin fun m ->
        m "Adding failover id:%S addr:%S"
          t.id (C.string_of_address t.addr)
      end >>= fun () ->
      CC.change_config c (Add_failover (t.id, t.addr)) >>= check_config_err
    end

  let promote_if_needed t c config =
    if is_active t config then
      Lwt.return_unit
    else begin
      Logs_lwt.info ~src (fun m -> m "Promoting failover id:%S" t.id) >>= fun () ->
      CC.change_config c (Promote t.id) >>= check_config_err
    end

  let join_cluster t c =
    (* We only try to add as failover/promote if actually needed.
     * Otherwise, we could get blocked in situations were the node is
     * rejoining the cluster (and thus already active in its configuration)
     * and the remaining nodes do not have the quorum to perform a
     * configuration change (even if it'd eventually be a NOP). *)
    let%lwt config = CC.get_config c >>= raise_if_error in
      add_as_failover_if_needed t c config>>= fun () ->
      promote_if_needed t c config>>= fun () ->
      let%lwt config = CC.get_config c >>= raise_if_error in
        Logs_lwt.info ~src begin fun m ->
          m "Final config: %s"
            (Oraft_lwt.string_of_config C.string_of_address config)
        end

  let handle_conn t fd addr =
    (* the following are not supported for ADDR_UNIX sockets, so catch *)
    (* possible exceptions  *)
    (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
    (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
    (try%lwt
       let%lwt ich, och = Oraft_lwt_conn_wrapper.wrap_incoming_conn t.conn_wrapper fd in
       let conn     = { addr; ich; och; in_buf = ref Bytes.empty; out_buf = MB.create () } in
         (match%lwt read_msg conn with
           | { id; op = Connect client_id; _ } ->
               Logs_lwt.info ~src begin fun m ->
                 m "Incoming client connection from %S" client_id
               end >>= fun () ->
               send_msg conn { id; response = OK "" }>>= fun () ->
               request_loop t client_id conn
           | { id; _ } ->
               send_msg conn { id; response = Error "Bad request" })
           [%finally
             try%lwt Lwt_io.close ich with _ -> Lwt.return_unit]
     with
       | End_of_file
       | Unix.Unix_error (Unix.ECONNRESET, _, _) -> Lwt.return_unit
       | exn ->
           Logs_lwt.err ~src
             (fun m -> m "Error in dispatch: %a" Oraft_lwt.pp_exn exn))
      [%finally
        try%lwt Lwt_unix.close fd with _ -> Lwt.return_unit]

  let run t =
    let sock = Lwt_unix.(socket (Unix.domain_of_sockaddr t.app_sockaddr)
                           Unix.SOCK_STREAM 0)
    in
      Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock t.app_sockaddr >>= fun () ->
      Lwt_unix.listen sock 256;

      let rec accept_loop t =
        let%lwt (fd_addrs,_) = Lwt_unix.accept_n sock 50 in
          List.iter
            (fun (fd, addr) -> Lwt.async (fun () -> handle_conn t fd addr))
            fd_addrs;
          accept_loop t
      in
        ignore begin try%lwt
            Logs_lwt.info ~src begin fun m ->
              m "Running app server at %a" Oraft_lwt.pp_saddr t.app_sockaddr
            end >>= fun () ->
            SS.run t.serv
          with exn ->
            Logs_lwt.err ~src begin fun m ->
              m "Error in Oraft_lwt server run(): %a" Oraft_lwt.pp_exn exn
            end
        end;
        (try%lwt
           match t.c with
             | None -> accept_loop t
             | Some c -> join_cluster t c >>= fun () -> accept_loop t
         with exn ->
           Logs_lwt.err ~src (fun m -> m "Exn raised: %a" Oraft_lwt.pp_exn exn))
          [%finally
             (* FIXME: t.c client shutdown *)
            try%lwt Lwt_unix.close sock with _ -> Lwt.return_unit]
end
