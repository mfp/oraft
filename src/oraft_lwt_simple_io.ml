open Lwt.Infix

open Oraft.Types
open Oraft_lwt_s
open Oraft_lwt_conn_wrapper

module Map = BatMap

module Make(C : SERVER_CONF) = struct
  module EC = Extprot.Conv

  type op = C.op

  module M  = Map.Make(String)
  module MB = Extprot.Msg_buffer

  let src = Logs.Src.create "oraft_lwt.simple_io"

  type conn_manager =
      {
        id            : string;
        sock          : Lwt_unix.file_descr;
        mutable conns : connection M.t;
        conn_signal   : unit Lwt_condition.t;
        conn_wrapper  : [`Incoming | `Outgoing] conn_wrapper;
      }

  and connection =
    {
      id             : rep_id;
      mgr            : conn_manager;
      ich            : Lwt_io.input_channel;
      och            : Lwt_io.output_channel;
      mutable closed : bool;
      mutable in_buf : Bytes.t;
      out_buf        : MB.t;
      mutable noutgoing : int;
    }

  let make_conn_manager ?(conn_wrapper = trivial_conn_wrapper ()) ~id addr =
    let sock = Lwt_unix.(socket (Unix.domain_of_sockaddr addr) Unix.SOCK_STREAM 0) in
      Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
      Lwt_unix.bind sock addr >>= fun () ->
      Lwt_unix.listen sock 256;

      let rec accept_loop t =
        let%lwt (fd, addr) = Lwt_unix.accept sock in
          Lwt.async begin fun () -> begin try%lwt
                (* the following are not supported for ADDR_UNIX sockets, so catch
                 * possible exceptions *)
                (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
                (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
                let%lwt ich, och = wrap_incoming_conn conn_wrapper fd in
                let%lwt id       = Lwt_io.read_line ich in
                let c        = { id; mgr = t; ich; och; closed = false;
                                 in_buf = Bytes.empty; out_buf = MB.create ();
                                 noutgoing = 0;
                               }
                in
                  t.conns <- M.add id c t.conns;
                  Lwt_condition.broadcast t.conn_signal ();
                  Logs_lwt.info ~src (fun m -> m "Incoming connection from peer %S" id)
              with _ ->
                Lwt_unix.shutdown fd Unix.SHUTDOWN_ALL;
                Lwt_unix.close fd
            end
          end;
          accept_loop t
      in
      let conn_signal = Lwt_condition.create () in
      let t           = { id; sock; conn_signal; conns = M.empty; conn_wrapper; } in
        ignore begin
          try%lwt
            Logs_lwt.info ~src
              (fun m -> m "Running node server at %a" Oraft_lwt.pp_saddr addr) >>= fun () ->
            accept_loop t
          with
            | Exit -> Lwt.return_unit
            | exn -> Logs_lwt.err ~src begin fun m ->
                m "Error in connection manager accept loop: %a" Oraft_lwt.pp_exn exn
              end
        end;
        Lwt.return t

  let connect t dst_id addr =
    match M.Exceptionless.find dst_id t.conns with
        Some _ as x -> Lwt.return x
      | None when dst_id < t.id -> (* wait for other end to connect *)
          let rec await_conn () =
            match M.Exceptionless.find dst_id t.conns with
                Some _ as x -> Lwt.return x
              | None -> Lwt_condition.wait t.conn_signal>>= fun () ->
                       await_conn ()
          in
            await_conn ()
      | None -> (* we must connect ourselves *)
          try%lwt
            Logs_lwt.info ~src begin fun m ->
              m "Connecting to %S" (C.string_of_address addr)
            end >>= fun () ->
            let saddr    = C.node_sockaddr addr in
            let fd       = Lwt_unix.socket (Unix.domain_of_sockaddr saddr)
                             Unix.SOCK_STREAM 0 in
            let%lwt ()       = Lwt_unix.connect fd saddr in
            let%lwt ich, och = wrap_outgoing_conn t.conn_wrapper fd in
              try%lwt
                (try Lwt_unix.setsockopt fd Unix.TCP_NODELAY true with _ -> ());
                (try Lwt_unix.setsockopt fd Unix.SO_KEEPALIVE true with _ -> ());
                Lwt_io.write_line och t.id >>= fun () ->
                Lwt_io.flush och >>= fun () ->
                Lwt.return (Some { id = dst_id; mgr = t; ich; och; closed = false;
                                   in_buf = Bytes.empty; out_buf = MB.create ();
                                   noutgoing = 0; })
              with exn ->
                Logs_lwt.err ~src begin fun m ->
                  m "Failed to write (%s)" (Printexc.to_string exn)
                end >>= fun () ->
                Lwt_unix.close fd >>= fun () ->
                Lwt.fail exn
          with exn ->
            Logs_lwt.err ~src begin fun m ->
              m "Failed to connect (%s)" (Printexc.to_string exn)
            end >>= fun () ->
            Lwt.return_none

  let is_saturated conn = conn.noutgoing > 10

  open Oraft_proto
  open Raft_message
  open Oraft

  let wrap_msg : _ Oraft.Types.message -> Raft_message.raft_message = function
      Request_vote { term; candidate_id; last_log_index; last_log_term; } ->
        Request_vote { Request_vote.term; candidate_id;
                       last_log_index; last_log_term; }
    | Vote_result { term; vote_granted; } ->
        Vote_result { Vote_result.term; vote_granted; }
    | Ping { term; n } -> Ping { Ping_msg.term; n; }
    | Pong { term; n } -> Pong { Ping_msg.term; n; }
    | Append_result { term; result; } ->
        Append_result { Append_result.term; result }
    | Append_entries { term; leader_id; prev_log_index; prev_log_term;
                       entries; leader_commit; } ->
        let map_entry = function
            (index, (Nop, term)) -> (index, Entry.Nop, term)
          | (index, (Config c, term)) -> (index, Entry.Config c, term)
          | (index, (Op (req_id, x), term)) ->
              (index, Entry.Op (req_id, C.string_of_op x), term) in

        let entries = List.map map_entry entries in
          Append_entries { Append_entries.term; leader_id; prev_log_index;
                           prev_log_term; entries; leader_commit; }

  let unwrap_msg : Raft_message.raft_message -> _ Oraft.Types.message = function
    | Request_vote { Request_vote.term; candidate_id; last_log_index;
                     last_log_term } ->
        Request_vote { term; candidate_id; last_log_index; last_log_term }
    | Vote_result { Vote_result.term; vote_granted; } ->
        Vote_result { term; vote_granted; }
    | Ping { Ping_msg.term; n } -> Ping { term; n; }
    | Pong { Ping_msg.term; n } -> Pong { term; n; }
    | Append_result { Append_result.term; result; } ->
        Append_result { term; result }
    | Append_entries { Append_entries.term; leader_id;
                       prev_log_index; prev_log_term;
                       entries; leader_commit; } ->
        let map_entry = function
          | (index, Entry.Nop, term) -> (index, (Nop, term))
          | (index, Entry.Config c, term) -> (index, (Config c, term))
          | (index, Entry.Op (req_id, x), term) ->
              let op = C.op_of_string x in
                (index, (Op (req_id, op), term))
        in
          Append_entries
            { term; leader_id; prev_log_index; prev_log_term;
              entries = List.map map_entry entries;
              leader_commit;
            }

  let abort c =
    if c.closed then Lwt.return_unit
    else begin
      c.mgr.conns <- M.remove c.id c.mgr.conns;
      c.closed    <- true;
      Lwt_io.abort c.och
    end

  let send c msg =
    if c.closed then Lwt.return_unit
    else begin
      let wrapped = wrap_msg msg in
        Logs_lwt.debug ~src begin fun m ->
          m "Sending@ %s" (Extprot.Pretty_print.pp pp_raft_message wrapped)
        end >>= fun () ->
        (try%lwt
          c.noutgoing <- c.noutgoing + 1;
          Lwt_io.atomic
            (fun och ->
               MB.clear c.out_buf;
               Raft_message.write c.out_buf wrapped;
               Lwt_io.LE.write_int och (MB.length c.out_buf)>>= fun () ->
               Lwt_io.write_from_exactly
                 och (MB.unsafe_contents c.out_buf) 0 (MB.length c.out_buf)>>= fun () ->
               Lwt_io.flush och)
            c.och
        with exn ->
          let%lwt () = Logs_lwt.info ~src begin fun m ->
              m "Error on send to %s, closing connection@ %s"
                c.id (Printexc.to_string exn)
            end
          in
            abort c)
          [%finally
            c.noutgoing <- c.noutgoing - 1;
            Lwt.return_unit]
    end

  let receive c =
    if c.closed then
      Lwt.return None
    else
      try%lwt
        Lwt_io.atomic
          (fun ich ->
             let%lwt len = Lwt_io.LE.read_int ich in
               if Bytes.length c.in_buf < len
               then c.in_buf <- Bytes.create len;
               let%lwt ()  = Lwt_io.read_into_exactly ich c.in_buf 0 len in
               let msg = EC.deserialize Raft_message.read (Bytes.unsafe_to_string c.in_buf) in
                 Logs_lwt.debug ~src begin fun m ->
                   m "Received@ %s"
                     (Extprot.Pretty_print.pp pp_raft_message msg)
                 end >>= fun () ->
                 Lwt.return (Some (unwrap_msg msg)))
          c.ich
      with exn ->
        Logs_lwt.info ~src begin fun m ->
          m "Error on receive from %S, closing connection. %a" c.id Oraft_lwt.pp_exn exn
        end >>= fun () ->
        abort c >>= fun () ->
        Lwt.return None

  type snapshot_transfer = unit

  let prepare_snapshot conn index config = Lwt.return None
  let send_snapshot () = Lwt.return false
end
