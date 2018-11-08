open Oraft_rsm_s

module Make_client (C : CONF) : CLIENT with type op := C.op
module Make_server (C: CONF) : SERVER with type op := C.op
