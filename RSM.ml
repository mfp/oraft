open Printf
open Lwt

open Oraft.Types

let section = Lwt_log.Section.make "RSM"

module Map     = BatMap
module Hashtbl = BatHashtbl

module MB = Extprot.Msg_buffer

type 'a conn =
    {
      addr    : 'a;
      ich     : Lwt_io.input_channel;
      och     : Lwt_io.output_channel;
      in_buf  : string ref;
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
         Lwt_io.LE.write_int och (MB.length buf) >>
         Lwt_io.write_from_exactly
           och (MB.unsafe_contents buf) 0  (MB.length buf) >>
         Lwt_io.flush och)
    conn.och

let read_msg read conn =
  (* same as send_msg applies here regarding the buffers *)
  Lwt_io.atomic
    (fun ich ->
       lwt len = Lwt_io.LE.read_int ich in
       let buf = conn.in_buf in
         if String.length !buf < len then buf := String.create len;
         Lwt_io.read_into_exactly ich !buf 0 len >>
         return (Extprot.Conv.deserialize read !buf))
    conn.ich

type config_change =
    Oraft_proto.Config_change.config_change =
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

module Make_client(C : CONF) =
struct
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
      }

  and address = string

  let make ~id () =
    { id; dst = None; conns = M.empty; req_id = 0L; pending_reqs = H.create 13; }

  let gen_id t =
    t.req_id <- Int64.succ t.req_id;
    t.req_id

  let send_msg conn msg = send_msg Oraft_proto.Client_msg.write conn msg
  let read_msg conn     = read_msg Oraft_proto.Server_msg.read conn

  let connect t peer_id addr =
    let do_connect () =
      lwt fd, ich, och = Oraft_lwt.open_connection (C.app_sockaddr addr) in
      let out_buf      = MB.create () in
      let in_buf       = ref "" in
      let conn         = { addr; ich; och; in_buf; out_buf } in
        (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
        (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
        try_lwt
          send_msg conn { id = 0L; op = (Connect t.id) } >>
          match_lwt read_msg conn with
            | { response = OK id; _ } ->
                t.conns <- M.add id conn t.conns;
                t.dst <- Some conn;
                ignore begin
                  let rec loop_recv () =
                    lwt msg = read_msg conn in
                      Lwt_log.debug_f ~section
                        "Received from server\n%s"
                        (Extprot.Pretty_print.pp Oraft_proto.Server_msg.pp msg) >>
                      match H.Exceptionless.find t.pending_reqs msg.id with
                          None -> loop_recv ()
                        | Some u ->
                            Lwt.wakeup_later u msg.response;
                            loop_recv ()
                  in
                    try_lwt
                      loop_recv ()
                    finally
                      t.conns <- M.remove peer_id t.conns;
                      Lwt_io.abort och
                end;
                return ()
            | _ -> failwith "conn refused"
        with _ ->
          t.conns <- M.remove peer_id t.conns;
          Lwt_io.abort och
    in
      match M.Exceptionless.find peer_id t.conns with
          Some conn when conn.addr = addr ->
            t.dst <- Some conn;
            return ()
        | Some conn (* when addr <> address *) ->
            t.conns <- M.remove peer_id t.conns;
            Lwt_io.abort conn.och >> do_connect ()
        | None -> do_connect ()

  let send_and_await_response t op f =
    match t.dst with
        None -> raise_lwt Not_connected
      | Some c ->
          let th, u = Lwt.task () in
          let id    = gen_id t in
          let msg   = { id; op; } in
            H.add t.pending_reqs id u;
            Lwt_log.debug_f ~section
              "Sending to server\n%s"
              (Extprot.Pretty_print.pp Oraft_proto.Client_msg.pp msg) >>
            send_msg c msg >>
            lwt x = th in
              f c.addr x

  let rec do_execute t op =
    send_and_await_response t op
      (fun dst resp -> match resp with
           OK s -> return (`OK s)
         | Error s -> return (`Error s)
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>
             do_execute t op
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>
             do_execute t op
         | Cannot_change | Unsafe_change _ | Config _ ->
             raise_lwt Bad_response)

  let execute t op =
    do_execute t (Execute (C.string_of_op op))

  let execute_ro t op =
    do_execute t (Execute_RO (C.string_of_op op))

  let rec get_config t =
    send_and_await_response t Get_config
      (fun dst resp -> match resp with
           Config c -> return (`OK c)
         | Error x -> return (`Error x)
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>
             get_config t
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>
             get_config t
         | OK _ | Cannot_change | Unsafe_change _ ->
             raise_lwt Bad_response)

  let rec change_config t op =
    send_and_await_response t (Change_config op)
      (fun dst resp -> match resp with
           OK _ -> return `OK
         | Error x -> return (`Error x)
         | Redirect (peer_id, address) when peer_id <> dst ->
             connect t peer_id address >>
             change_config t op
         | Redirect _ | Retry ->
             Lwt_unix.sleep 0.050 >>
             change_config t op
         | Cannot_change -> return (`Cannot_change)
         | Unsafe_change (c, p) -> return (`Unsafe_change (c, p))
         | Config _ -> raise_lwt Bad_response)

  let connect t ~addr = connect t "" addr
end

module Make_server(C : CONF) =
struct
  module SS   = Oraft_lwt.Simple_server(C)
  module SSC  = SS.Config
  module CC   = Make_client(C)

  module Core = SS

  open Oraft_proto
  open Client_msg
  open Client_op
  open Server_msg
  open Response
  open Config_change

  type 'a execution = [`Sync of 'a Lwt.t | `Async of 'a Lwt.t]
  type 'a apply     = 'a Core.server -> C.op -> [`OK of 'a | `Error of exn] execution

  type 'a t =
      {
        id            : rep_id;
        addr          : string;
        c             : CC.t option;
        node_sockaddr : Unix.sockaddr;
        app_sockaddr  : Unix.sockaddr;
        serv          : 'a SS.server;
        exec          : 'a SS.apply;
      }

  let raise_if_error = function
      `OK x -> return x
    | `Error s -> raise_lwt (Failure s)

  let check_config_err = function
    | `OK -> return ()
    | `Error s -> raise_lwt (Failure s)
    | `Cannot_change -> raise_lwt (Failure "Cannot perform config change")
    | `Unsafe_change _ -> raise_lwt (Failure "Unsafe config change")

  let make exec addr ?join ?election_period ?heartbeat_period id =
    match join with
        None ->
          let config        = Simple_config ([id, addr], []) in
          let state         = Oraft.Core.make
                                ~id ~current_term:0L ~voted_for:None
                                ~log:[] ~config () in
          let node_sockaddr = C.node_sockaddr addr in
          let app_sockaddr  = C.app_sockaddr addr in
          let conn_mgr      = SS.make_conn_manager ~id node_sockaddr in
          let serv          = SS.make exec ?election_period ?heartbeat_period
                                state conn_mgr
          in
            return { id; addr; c = None; node_sockaddr; app_sockaddr; serv; exec; }
      | Some peer_addr ->
          let c = CC.make ~id () in
            Lwt_log.info_f ~section "Connecting to %S" peer_addr >>
            CC.connect c ~addr:peer_addr >>
            lwt config        = CC.get_config c >>= raise_if_error in
            lwt ()            = Lwt_log.info_f ~section
                                  "Got initial configuration %s"
                                  (Oraft_util.string_of_config config) in
            let state         = Oraft.Core.make
                                  ~id ~current_term:0L ~voted_for:None
                                  ~log:[] ~config () in
            let node_sockaddr = C.node_sockaddr addr in
            let app_sockaddr  = C.app_sockaddr addr in
            let conn_mgr      = SS.make_conn_manager ~id node_sockaddr in
            let serv          = SS.make exec ?election_period
                                  ?heartbeat_period state conn_mgr
            in
              return { id; addr; c = Some c;
                       node_sockaddr; app_sockaddr; serv; exec; }

  let send_msg conn msg = send_msg Oraft_proto.Server_msg.write conn msg
  let read_msg conn     = read_msg Oraft_proto.Client_msg.read conn

  let map_op_result = function
    | `Redirect (peer_id, addr) -> Redirect (peer_id, addr)
    | `Retry -> Retry
    | `Error exn -> Error (Printexc.to_string exn)
    | `OK s -> OK s

  let perform_change t op =
    let map = function
          `OK -> OK ""
        | `Cannot_change -> Cannot_change
        | `Unsafe_change (c, p) -> Unsafe_change (c, p)
        | `Redirect _ | `Retry as x -> map_op_result x
    in
      try_lwt
        lwt ret =
          match op with
              Add_failover (peer_id, addr) -> SSC.add_failover t.serv peer_id addr
            | Remove_failover peer_id -> SSC.remove_failover t.serv peer_id
            | Decommission peer_id -> SSC.decommission t.serv peer_id
            | Demote peer_id -> SSC.demote t.serv peer_id
            | Promote peer_id -> SSC.promote t.serv peer_id
            | Replace (replacee, failover) -> SSC.replace t.serv ~replacee ~failover
        in
          return (map ret)
      with exn ->
        Lwt_log.debug_f ~section ~exn
          "Error while changing cluster configuration\n%s"
          (Extprot.Pretty_print.pp pp_config_change op) >>
        return (Error (Printexc.to_string exn))

  let process_message t client_id conn = function
      { id; op = Connect _ } ->
        send_msg conn { id; response = Error "Unexpected request" }
    | { id; op = Get_config } ->
        let config = SS.Config.get t.serv in
          send_msg conn { id; response = Config config }
    | { id; op = Change_config x } ->
        Lwt_log.info_f ~section
          "Config change requested: %s"
          (Extprot.Pretty_print.pp pp_config_change x) >>
        lwt response = perform_change t x in
          Lwt_log.info_f ~section
            "Config change result: %s"
            (Extprot.Pretty_print.pp pp_response response) >>
          Lwt_log.info_f ~section
            "New config: %s"
            (Oraft_util.string_of_config (SS.config t.serv)) >>
          send_msg conn { id; response }
    | { id; op = Execute_RO op; } -> begin
        match_lwt SS.readonly_operation t.serv with
          | `Redirect _ | `Retry | `Error _ as x ->
              let response = map_op_result x in
                send_msg conn { id; response; }
          | `OK ->
              match t.exec t.serv (C.op_of_string op) with
                | `Sync resp ->
                    lwt resp = resp in
                      send_msg conn { id; response = map_op_result resp }
                | `Async resp ->
                    ignore begin try_lwt
                      lwt resp = try_lwt resp
                                 with exn -> return (`Error exn)
                      in
                        send_msg conn { id; response = map_op_result resp }
                    with _ ->
                      return ()
                    end;
                    return ()
      end
    | { id; op = Execute op; } ->
        lwt response = SS.execute t.serv (C.op_of_string op) >|= map_op_result in
          send_msg conn { id; response }

  let rec request_loop t client_id conn =
    lwt msg = read_msg conn in
      ignore begin
        try_lwt
          process_message t client_id conn msg
        with exn ->
          Lwt_log.debug_f ~section ~exn
            "Error while processing message\n%s"
            (Extprot.Pretty_print.pp Oraft_proto.Client_msg.pp msg) >>
          send_msg conn { id = msg.id; response = Error (Printexc.to_string exn) }
      end;
      request_loop t client_id conn

  let dispatch t fd addr =
    (* the following are not supported for ADDR_UNIX sockets, so catch *)
    (* possible exceptions  *)
    (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
    (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
    let ich  = Lwt_io.of_fd Lwt_io.input fd in
    let och  = Lwt_io.of_fd Lwt_io.output fd in
    let conn = { addr; ich; och; in_buf = ref ""; out_buf = MB.create () } in
      match_lwt read_msg conn with
        | { id; op = Connect client_id; _ } ->
            Lwt_log.info_f ~section "Incoming client connection from %S" client_id >>
            send_msg conn { id; response = OK "" } >>
            request_loop t client_id conn
        | { id; _ } -> send_msg conn { id; response = Error "Bad request" }

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
      return ()
    else begin
      Lwt_log.info_f ~section "Adding failover id:%S addr:%S" t.id t.addr >>
      CC.change_config c (Add_failover (t.id, t.addr)) >>= check_config_err
    end

  let promote_if_needed t c config =
    if is_active t config then
      return ()
    else begin
      Lwt_log.info_f ~section "Promoting failover id:%S" t.id >>
      CC.change_config c (Promote t.id) >>= check_config_err
    end

  let join_cluster t c =
    (* We only try to add as failover/promote if actually needed.
     * Otherwise, we could get blocked in situations were the node is
     * rejoining the cluster (and thus already active in its configuration)
     * and the remaining nodes do not have the quorum to perform a
     * configuration change (even if it'd eventually be a NOP). *)
    lwt config = CC.get_config c >>= raise_if_error in
      add_as_failover_if_needed t c config >>
      promote_if_needed t c config >>
      lwt config = CC.get_config c >>= raise_if_error in
        Lwt_log.info_f ~section "Final config: %s"
          (Oraft_util.string_of_config config)

  let run t =
    let sock = Lwt_unix.(socket (Unix.domain_of_sockaddr t.app_sockaddr)
                           Unix.SOCK_STREAM 0)
    in
      Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock t.app_sockaddr;
      Lwt_unix.listen sock 256;

      let rec accept_loop t =
        lwt (fd, addr) = Lwt_unix.accept sock in
          ignore
            begin try_lwt
              dispatch t fd addr
            with exn ->
              begin match exn with
                  End_of_file
                | Unix.Unix_error (Unix.ECONNRESET, _, _) -> return ()
                | exn -> Lwt_log.error_f ~exn ~section "Error in dispatch"
              end
            finally
              Lwt_log.info_f ~section "Client connection closed" >>
              let () = Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL in
                Lwt_unix.close fd
            end;
          accept_loop t
      in
        ignore begin try_lwt
          SS.run t.serv
        with exn ->
          Lwt_log.error_f ~section ~exn "Error in Oraft_lwt server run()"
        end;
        try_lwt
          match t.c with
            | None -> accept_loop t
            | Some c -> join_cluster t c >> accept_loop t
        finally
          (* FIXME: t.c client shutdown *)
          Lwt_unix.close sock
end
