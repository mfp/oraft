open Oraft_lwt_s

module Make (C : SERVER_CONF) : LWTIO with type op = C.op
(** Ready-to-use implementation of LWTIO interface using `extprot` for
    message serialization. *)
