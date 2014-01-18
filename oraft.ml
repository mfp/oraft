open Printf
open Lwt

module Map    = BatMap
module List   = BatList
module Option = BatOption

module Kernel =
struct
  type status    = Leader | Follower | Candidate
  type term      = Int64.t
  type index     = Int64.t
  type rep_id    = string
  type client_id = string
  type req_id    = client_id * Int64.t
  type 'a result = [`OK of 'a | `Error of exn]

  module IM = Map.Make(struct
                         type t = index
                         let compare = compare
                       end)

  module REPID = struct type t = rep_id let compare = String.compare end
  module RM = Map.Make(REPID)
  module RS = Set.Make(REPID)

  let maybe_nf f x = try Some (f x) with Not_found -> None

  module LOG : sig
    type 'a t

    val empty       : init_index:index -> init_term:term -> 'a t
    val to_list     : 'a t -> (index * 'a * term) list
    val append      : term:term -> 'a -> 'a t -> 'a t
    val last_index  : 'a t -> (term * index)
    val get         : 'a t -> index -> ('a * term) option
    val append_many : (index * ('a * term)) list -> 'a t -> 'a t
    val get_range   : from_inclusive:index -> to_inclusive:index -> 'a t ->
                      (index * ('a * term)) list
    val get_term    : index -> 'a t -> term option
  end =
  struct
    type 'a t =
        {
          idx     : index;
          term    : term;
          entries : ('a * term) IM.t;
        }

    let empty ~init_index:idx ~init_term:term =
      { idx; term; entries = IM.empty; }

    let to_list t =
      IM.bindings t.entries |> List.map (fun (i, (x, t)) -> (i, x, t))

    let append ~term x t =
      let idx = Int64.succ t.idx in
        { t with idx; entries = IM.add idx (x, term) t.entries; }

    let last_index t =
      match maybe_nf IM.max_binding t.entries with
          Some (index, (_, term)) -> (term, index)
        | None -> (t.term, t.idx)

    let get t idx =
      try
        Some (IM.find idx t.entries)
      with Not_found -> None

    let append_many l t = match l with
        [] -> t
      | l ->
          let nonconflicting =
            try
              let idx, term =
                List.find
                  (fun (idx, (_, term)) ->
                     match get t idx with
                         Some (_, term') when term <> term' -> true
                       | _ -> false)
                  l in
              let entries, _, _ = IM.split idx t.entries in
                entries
            with Not_found ->
              t.entries
          in
            let entries = List.fold_left
                            (fun m (idx, (x, term)) -> IM.add idx (x, term) m)
                            nonconflicting l in
            (* entries is not empty since l <> [] *)
            let (idx, (_, term)) = IM.max_binding t.entries in
              { idx; term; entries; }

    let get_range ~from_inclusive ~to_inclusive t =
      let _, _, post = IM.split (Int64.pred from_inclusive) t.entries in
      let pre, _, _  = IM.split (Int64.succ to_inclusive) post in
        IM.bindings pre

    let get_term idx t =
      try
        Some (snd (IM.find idx t.entries))
      with Not_found ->
        if idx = t.idx then Some t.term else None
  end

  type 'a state =
      {
        (* persistent *)
        current_term : term;
        voted_for    : rep_id option;
        log          : 'a LOG.t;
        id           : rep_id;

        (* volatile *)
        state        : status;
        commit_index : index;
        last_applied : index;

        peers     : rep_id array;
        leader_id : rep_id option;

        (* volatile on leaders *)
        next_index  : index RM.t;
        match_index : index RM.t;

        votes : RS.t;
      }

  type 'a message =
      Request_vote of request_vote
    | Vote_result of vote_result
    | Append_entries of 'a append_entries
    | Append_result of append_result

  and request_vote =
      {
        term : term;
        candidate_id : rep_id;
        last_log_index : index;
        last_log_term : term;
      }

  and vote_result =
    {
      term : term;
      vote_granted : bool;
    }

  and 'a append_entries =
    {
      term : term;
      leader_id : rep_id;
      prev_log_index : index;
      prev_log_term : term;
      entries : (index * ('a * term)) list;
      leader_commit : index;
    }

  and append_result =
    {
      term : term;
      success : bool;

      (* extra information relative to the fields on the RAFT paper *)

      (* index of log entry immediately preceding new ones in AppendEntries msg
       * this responds to; used for fast next_index rollback by the Leader
       * and allows to receive result of concurrent AppendEntries out-of-order
       * *)
      prev_log_index : index;
      (* index of last log entry included in the msg this responds to;
       * used by the Leader to update match_index, allowing to receive results
       * of concurrent Append_entries request out-of-order *)
      last_log_index : index;
    }

  type 'a action =
      [ `Apply of 'a
      | `Become_candidate
      | `Become_follower of rep_id option
      | `Become_leader
      | `Reset_election_timeout
      | `Reset_heartbeat
      | `Redirect of rep_id option * 'a
      | `Send of rep_id * 'a message
      ]
end

include Kernel

let quorum s = Array.length s.peers / 2 + 1

let vote_result s vote_granted =
  Vote_result { term = s.current_term; vote_granted; }

let append_result s ~prev_log_index ~last_log_index success =
  Append_result
    { term = s.current_term; prev_log_index; last_log_index; success; }

let update_commit_index s =
  (* Find last N such that log[N].term = current term AND
   * a majority of peers has got  match_index[peer] >= N. *)
  (* We compute it as follows: get all the match_index, sort them,
   * and get the (quorum - 1)-nth (not quorum-nth because the current leader
   * also counts).*)
  let sorted       = RM.bindings s.match_index |> List.map snd |>
                     List.sort Int64.compare in
  let commit_index =
    try
      List.nth sorted (quorum s - 1)
    with Not_found ->
      s.commit_index
  in
    { s with commit_index }

let try_commit s = match s.state with
    Follower | Candidate -> (s, [])
  | Leader ->
      let s       = {s with last_applied = s.commit_index } in
      let actions = List.map (fun (_, (x, _)) -> `Apply x)
                      (LOG.get_range
                         ~from_inclusive:(Int64.succ s.commit_index)
                         ~to_inclusive:s.commit_index
                         s.log)
      in (s, actions)

let heartbeat s =
  let prev_log_term, prev_log_index = LOG.last_index s.log in
    Append_entries { term = s.current_term; leader_id = s.id;
                     prev_log_term; prev_log_index; entries = [];
                     leader_commit = s.commit_index }

let send_entries s from =
  match LOG.get_term from s.log with
      None -> None
    | Some prev_log_term ->
        Some
          (Append_entries
            { prev_log_term;
              term           = s.current_term;
              leader_id      = s.id;
              prev_log_index = Int64.pred from;
              entries        = LOG.get_range
                                 ~from_inclusive:from
                                 ~to_inclusive:Int64.max_int
                                 s.log;
              leader_commit  = s.commit_index;
            })

let broadcast s msg =
  Array.to_list s.peers |>
  List.map (fun p -> `Send (p, msg))

let receive_msg s peer = function
  (* " If a server receives a request with a stale term number, it rejects the
   * request." *)
  | Request_vote { term; _ } when term < s.current_term ->
      (s, [`Send (peer, vote_result s false)])

  (* "Current terms are exchanged whenever servers communicate; if one
   * server’s current term is smaller than the other, then it updates its
   * current term to the larger value. If a candidate or leader discovers that
   * its term is out of date, it immediately reverts to follower state."
   * *)
  | Request_vote { term; candidate_id; _ } when term > s.current_term -> begin
      let s = { s with current_term = term; state = Follower; } in
        match s.voted_for with
            Some candidate when candidate <> candidate_id ->
              (s, [`Become_follower None; `Send (peer, vote_result s false)])
          | Some _ | None ->
              let s = { s with voted_for = Some candidate_id; } in
                (s, [`Become_follower None; `Send (peer, vote_result s true)])
    end
  | Request_vote { term; candidate_id; last_log_index; last_log_term; } -> begin
      match s.voted_for with
          Some candidate when candidate <> candidate_id ->
            (s, [`Send (peer, vote_result s false)])
        | _ ->
            if (last_log_term, last_log_index) < LOG.last_index s.log then
              (s, [`Send (peer, vote_result s false)])
            else begin
              let s = { s with voted_for = Some candidate_id } in
                (s, [`Reset_election_timeout; `Send (peer, vote_result s true)])
            end
    end
  (* " If a server receives a request with a stale term number, it rejects the
   * request." *)
  | Append_entries { term; _ } when term < s.current_term ->
      (s, [`Send (peer, append_result s 0L 0L false)])
  | Append_entries
      { term; prev_log_index; prev_log_term; entries; leader_commit; } -> begin
        (* "Current terms are exchanged whenever servers communicate; if one
         * server’s current term is smaller than the other, then it updates
         * its current term to the larger value. If a candidate or leader
         * discovers that its term is out of date, it immediately reverts to
         * follower state." *)
        let s, actions =
          if term > s.current_term then
            let s = { s with current_term = term;
                             state        = Follower;
                             leader_id    = Some peer;
                    }
            in
              (s, [`Become_follower (Some peer)])
          else
            (s, [`Reset_election_timeout])
        in
          match LOG.get s.log prev_log_index with
              None -> (s, (`Send (peer, append_result s 0L 0L false) :: actions))
            | Some (_, term') when term <> term' ->
                (s, (`Send (peer, append_result s 0L 0L false) :: actions))
            | _ ->
                let log          = LOG.append_many entries s.log in
                let last_index   = snd (LOG.last_index log) in
                let commit_index = if leader_commit > s.commit_index then
                                     min leader_commit last_index
                                   else s.commit_index in
                let reply        = append_result s
                                     ~prev_log_index
                                     ~last_log_index:last_index true in
                let s            = { s with commit_index; log; } in
                  (s, (`Send (peer, reply) :: actions))
      end

  | Vote_result { term; _ } when term < s.current_term ->
      (s, [])
  | Vote_result { term; _ } when term > s.current_term ->
      let s = { s with current_term = term; state = Follower } in
        (s, [`Become_follower None])
  | Vote_result { term; vote_granted; } when s.state <> Candidate ->
      (s, [])
  | Vote_result { term; vote_granted; } ->
      if not vote_granted then
        (s, [])
      else
        let votes = RS.add peer s.votes in
          if RS.cardinal votes < quorum s - 1 then
            (s, [])
          else
            (* become leader! *)
            let next_idx    = LOG.last_index s.log |> snd |> Int64.succ in
            let next_index  = Array.fold_left
                                (fun m peer -> RM.add peer next_idx m)
                                RM.empty s.peers in
            let match_index = Array.fold_left
                                (fun m peer -> RM.add peer 0L m)
                                RM.empty s.peers in
            let s     = { s with next_index; match_index;
                                 state     = Leader;
                                 leader_id = Some s.id;
                        } in
            let sends = broadcast s (heartbeat s) in
              (s, (`Become_leader :: sends))

  | Append_result { term; _ } when term < s.current_term ->
      (s, [])
  | Append_result { term; _ } when term > s.current_term ->
      let s = { s with current_term = term; state = Follower; } in
        (s, [`Become_follower None])
  | Append_result { term; success; prev_log_index; last_log_index; } ->
      if success then begin
        let next_index  = RM.modify peer
                            (fun idx -> max idx (Int64.succ last_log_index))
                            s.next_index in
        let match_index = RM.modify_def
                            last_log_index peer
                            (max last_log_index) s.match_index in
        let s           = update_commit_index { s with next_index; match_index } in
        let s, actions  = try_commit s in
          (s, actions)
      end else begin
        let next_index = RM.modify peer
                           (fun idx -> min idx prev_log_index)
                           s.next_index in
        let s          = { s with next_index } in
          match send_entries s (RM.find peer next_index) with
              None ->
                (* Must send snapshot *)
                (* FIXME *)
                (s, [])
            | Some msg ->
                (s, [`Send (peer, msg)])
      end

let election_timeout s = match s.state with
    Leader -> (s, [])
  | Follower | Candidate ->
      let s           = { s with current_term = Int64.succ s.current_term;
                                 state        = Candidate;
                                 votes        = RS.empty;
                                 leader_id    = None;
                        } in
      let term_, idx_ = LOG.last_index s.log in
      let msg         = Request_vote
                          {
                            term = s.current_term;
                            candidate_id = s.id;
                            last_log_index = idx_;
                            last_log_term = term_;
                          } in
      let sends       = broadcast s msg in
        (s, (`Become_candidate :: `Reset_election_timeout :: sends))

let heartbeat_timeout s = match s.state with
    Follower | Candidate -> (s, [])
  | Leader ->
      (s, (`Reset_heartbeat :: broadcast s (heartbeat s)))

let client_command x s = match s.state with
    Follower | Candidate -> (s, [`Redirect (s.leader_id, x)])
  | Leader ->
      let log     = LOG.append ~term:s.current_term x s.log in
      let actions = Array.to_list s.peers |>
                    List.filter_map
                      (fun peer ->
                         match send_entries s (RM.find peer s.next_index) with
                             None ->
                               (* FIXME: should send snapshot if cannot send log *)
                               None
                           | Some msg -> Some (`Send (peer, msg))) in
      let actions = match actions with
                      | [] -> []
                      | l -> `Reset_heartbeat :: actions in
      let s       = { s with log; } in
        (s, actions)

module Types = Kernel

module Core =
struct
  include Types

  let receive_msg       = receive_msg
  let election_timeout  = election_timeout
  let heartbeat_timeout = heartbeat_timeout
  let client_command    = client_command
end

module type LWTIO =
sig
  type address
  type op
  type connection

  val connect : address -> connection option Lwt.t
  val accept  : unit -> (address * connection) Lwt.t
  val send    : connection -> (req_id * op) message -> bool Lwt.t
  val receive : connection -> (req_id * op) message option Lwt.t
  val abort   : connection -> unit Lwt.t
end

module type PROC =
sig
  type op
  type resp

  val execute : op -> resp result Lwt.t
end

module Lwt_server
  (PROC : PROC)
  (IO : LWTIO with type op = PROC.op) =
struct
  module S    = Set.Make(String)
  module CMDM = Map.Make(struct
                           type t = req_id
                           let compare = compare
                         end)

  type server =
      {
        peers                     : IO.address RM.t;
        election_period           : float;
        heartbeat_period          : float;
        mutable next_req_id       : Int64.t;
        mutable conns             : IO.connection RM.t;
        mutable state             : (req_id * PROC.op) state;
        mutable running           : bool;
        mutable msg_threads       : th_res Lwt.t list;
        mutable election_timeout  : th_res Lwt.t;
        mutable heartbeat         : th_res Lwt.t;
        mutable abort             : th_res Lwt.t * th_res Lwt.u;
        mutable get_client_cmd    : th_res Lwt.t;
        push_client_cmd           : (req_id * PROC.op) -> unit;
        client_cmd_stream         : (req_id * PROC.op) Lwt_stream.t;
        mutable pending_cmds      : (cmd_res Lwt.t * cmd_res Lwt.u) CMDM.t;
        leader_signal             : unit Lwt_condition.t;
      }

  and th_res =
      Message of rep_id * (req_id * PROC.op) message
    | Client_command of req_id * PROC.op
    | Abort
    | Election_timeout
    | Heartbeat_timeout

  and cmd_res =
      Redirect of rep_id option
    | Executed of PROC.resp result

  let make
        ?(election_period = 2.)
        ?(heartbeat_period = election_period /. 2.)
        state peers =
    let stream, push      = Lwt_stream.create () in
    let push x            = push (Some x) in
    let election_timeout  = match state.Core.state with
                              | Follower | Candidate ->
                                  Lwt_unix.sleep election_period >>
                                  return Election_timeout
                              | Leader -> fst (Lwt.wait ()) in
    let heartbeat = match state.Core.state with
                              | Follower | Candidate -> fst (Lwt.wait ())
                              | Leader ->
                                  Lwt_unix.sleep heartbeat_period >>
                                  return Heartbeat_timeout
    in
      { peers;
        heartbeat_period;
        election_period;
        state;
        election_timeout;
        heartbeat;
        next_req_id       = 42L;
        conns             = RM.empty;
        running           = true;
        msg_threads       = [];
        abort             = Lwt.task ();
        get_client_cmd    = (match_lwt Lwt_stream.get stream with
                               | None -> fst (Lwt.wait ())
                               | Some (req_id, op) ->
                                   return (Client_command (req_id, op)));
        push_client_cmd   = push;
        client_cmd_stream = stream;
        pending_cmds      = CMDM.empty;
        leader_signal     = Lwt_condition.create ();
      }

  let abort t =
    t.running <- false;
    try (Lwt.wakeup (snd t.abort) Abort) with _ -> ()

  let lookup_address t peer_id =
    maybe_nf (RM.find peer_id) t.peers

  let connect_and_get_msgs t (peer, addr) =
    let rec make_thread = function
        0 -> Lwt_unix.sleep 5. >> make_thread 5
      | n ->
          match_lwt IO.connect addr with
            | None -> Lwt_unix.sleep 0.1 >> make_thread (n - 1)
            | Some conn ->
                t.conns <- RM.add peer conn t.conns;
                match_lwt IO.receive conn with
                    None -> Lwt_unix.sleep 0.1 >> make_thread 5
                  | Some msg -> return (Message (peer, msg))
    in
      make_thread 5

  let rec exec_action t : _ action -> unit Lwt.t = function
    | `Reset_election_timeout ->
        t.election_timeout <- (Lwt_unix.sleep t.election_period >>
                               return Election_timeout);
        return ()
    | `Reset_heartbeat ->
        t.heartbeat <- (Lwt_unix.sleep t.heartbeat_period >>
                        return Heartbeat_timeout);
        return ()
    | `Become_candidate
    | `Become_follower None ->
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t `Reset_election_timeout
    | `Become_follower (Some _) ->
        Lwt_condition.broadcast t.leader_signal ();
        t.heartbeat <- fst (Lwt.wait ());
        exec_action t `Reset_election_timeout
    | `Become_leader ->
        Lwt_condition.broadcast t.leader_signal ();
        t.election_timeout  <- fst (Lwt.wait ());
        exec_action t `Reset_heartbeat
    | `Apply (req_id, op) ->
        (* TODO: allow to run this in parallel with rest RAFT algorithm.
         * Must make sure that Apply actions are not reordered. *)
        lwt resp = try_lwt PROC.execute op
                   with exn -> return (`Error exn)
        in begin
          try_lwt
            let (_, u), pending_cmds = CMDM.extract req_id t.pending_cmds in
              Lwt.wakeup_later u (Executed resp);
              return ()
          with _ -> return ()
        end
    | `Redirect (rep_id, (req_id, _)) -> begin
        try_lwt
          let (_, u), pending_cmds = CMDM.extract req_id t.pending_cmds in
            Lwt.wakeup_later u (Redirect rep_id);
            return ()
        with _ -> return ()
      end
    | `Send (rep_id, msg) ->
        (* we allow to run this in parallel with rest RAFT algorithm.
         * It's OK to reorder sends. *)
        (* TODO: limit the number of msgs in outboung queue.
         * Drop after the nth? *)
        ignore begin try_lwt
          lwt _ = IO.send (RM.find rep_id t.conns) msg in
            return ()
        with _ ->
          (* cannot send -- partition *)
          return ()
        end;
        return ()

  let exec_actions t l = Lwt_list.iter_s (exec_action t) l

  let rec run t =
    if not t.running then return ()
    else
      let must_recon = Array.to_list t.state.peers |>
                       List.filter_map
                         (fun peer -> lookup_address t peer |>
                                      Option.map (fun addr -> (peer, addr))) |>
                       List.filter
                         (fun (peer, _) -> not (RM.mem peer t.conns)) in
      let new_ths    = List.map (connect_and_get_msgs t) must_recon in
        t.msg_threads <- new_ths @ t.msg_threads;

        match_lwt
          Lwt.choose
            ([ t.election_timeout;
               fst t.abort;
               t.get_client_cmd;
               t.heartbeat;
             ] @
             t.msg_threads)
        with
          | Abort -> t.running <- false; return ()
          | Client_command (req_id, op) ->
              let state, actions = Core.client_command (req_id, op) t.state in
                t.state <- state;
                exec_actions t actions >>
                run t
          | Message (rep_id, msg) ->
              let state, actions = Core.receive_msg t.state rep_id msg in
                t.state <- state;
                exec_actions t actions >>
                run t
          | Election_timeout ->
              let state, actions = Core.election_timeout t.state in
                t.state <- state;
                exec_actions t actions >>
                run t
          | Heartbeat_timeout ->
              let state, actions = Core.heartbeat_timeout t.state in
                t.state <- state;
                exec_actions t actions >>
                run t

  let gen_req_id t =
    let id = t.next_req_id in
      t.next_req_id <- Int64.succ id;
      (t.state.id, id)

  let rec execute t cmd =
    match t.state.state, t.state.leader_id with
      | Follower, Some leader_id -> begin
          match maybe_nf (RM.find leader_id) t.peers with
              Some address -> return (`Redirect (leader_id, address))
            | None ->
                (* redirect to a random server, hoping it knows better *)
                try_lwt
                  let leader_id, address =
                    RM.bindings t.peers |>
                    Array.of_list |>
                    (fun x -> if x = [||] then failwith "empty"; x) |>
                    (fun a -> a.(Random.int (Array.length a)))
                  in
                    return (`Redirect_randomized (leader_id, address))
                with _ ->
                  return `Retry_later
        end
      | Candidate, _ | Follower, _ ->
          (* await leader, retry *)
          Lwt_condition.wait t.leader_signal >>
          execute t cmd
      | Leader, _ ->
          let req_id = gen_req_id t in
          let task   = Lwt.task () in
            t.pending_cmds <- CMDM.add req_id task t.pending_cmds;
            t.push_client_cmd (req_id, cmd);
            match_lwt fst task with
                Executed (`OK _ as res) -> return res
              | Executed (`Error exn) ->
                  return (`Error (Printexc.to_string exn))
              | Redirect _ ->
                  execute t cmd

end
