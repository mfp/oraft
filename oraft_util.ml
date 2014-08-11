open Printf
open Oraft.Types

let s_of_simple_config string_of_address l =
  List.map
    (fun (id, addr) -> sprintf "%S:%S" id (string_of_address addr)) l |>
  String.concat "; "

let string_of_config string_of_address c =
  match c with
      Simple_config (c, passive) ->
        sprintf "Simple ([%s], [%s])"
          (s_of_simple_config string_of_address c)
          (s_of_simple_config string_of_address passive)
    | Joint_config (c1, c2, passive) ->
        sprintf "Joint ([%s], [%s], [%s])"
          (s_of_simple_config string_of_address c1)
          (s_of_simple_config string_of_address c2)
          (s_of_simple_config string_of_address passive)
