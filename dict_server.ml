open Printf
open Lwt

module String = BatString

type op =
    Get of string
  | Set of string * string

module CONF =
struct
  type op_ = op
  type op = op_

  let string_of_op = function
      Get v -> "?" ^ v
    | Set (k, v) -> sprintf "!%s=%s" k v

  let op_of_string s =
    if s = "" then failwith "bad op"
    else
      match s.[0] with
          '?' -> Get (String.slice ~first:1 s)
        | '!' -> let k, v = String.slice ~first:1 s |> String.split ~by:"=" in
                   Set (k, v)
        | _ -> failwith "bad op"

  let sockaddr_of_string s =
    try
      let host, port = String.split ~by:":" s in
        printf "host %S   port %s\n" host port;
        Unix.ADDR_INET (Unix.inet_addr_of_string host, int_of_string port)
    with Not_found ->
      Unix.ADDR_UNIX s
end

module SERVER = RSM.Make_server(CONF)
module CLIENT = RSM.Make_client(CONF)

let run_server ~addr ?join ~id () =
  let h = Hashtbl.create 13 in

  let exec _ op = match op with
      Get s -> return (try `OK (Hashtbl.find h s) with Not_found -> `OK "")
    | Set (k, v) ->
        if v = "" then Hashtbl.remove h k
        else Hashtbl.add h k v;
        return (`OK "")
  in

  lwt server = SERVER.make exec addr ?join id in
    SERVER.run server


let client_op ~addr op =
  let c = CLIENT.make ~id:"foo" () in
    CLIENT.connect c ~addr >>
    match_lwt CLIENT.execute c op with
        `OK s -> printf "+OK %s\n" s;
                 return ()
      | `Error s -> printf "-ERR %s\n" s;
                    return ()

let mode         = ref `Help
let cluster_addr = ref None
let k            = ref None
let v            = ref None

let specs =
  Arg.align
    [
      "-master", Arg.String (fun n -> mode := `Master n),
        "ADDR Launch master at given address";
      "-join", Arg.String (fun p -> cluster_addr := Some p),
        "ADDR Join cluster at given address";
      "-client", Arg.String (fun addr -> mode := `Client addr), "ADDR Client mode";
      "-key", Arg.String (fun s -> k := Some s), "STRING Get/set specified key";
      "-value", Arg.String (fun s -> v := Some s),
        "STRING Set key given in -key to STRING";
    ]

let usage () =
  print_endline (Arg.usage_string specs "Usage:");
  exit 1

let () =
  Arg.parse specs ignore "Usage:";
  match !mode with
      `Help -> usage ()
    | `Master addr ->
        Lwt_unix.run (run_server ~addr ?join:!cluster_addr ~id:addr ())
    | `Client addr ->
        match !k, !v with
            None, None | None, _ -> usage ()
          | Some k, Some v ->
              Lwt_unix.run (client_op ~addr (Set (k, v)))
          | Some k, None ->
              Lwt_unix.run (client_op ~addr (Get k))

