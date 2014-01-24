
let maybe_nf f x = try Some (f x) with Not_found | Invalid_argument _ -> None
