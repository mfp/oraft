open Printf
open Oraft.Types

let s_of_simple_config l =
  List.map (fun (id, addr) -> sprintf "%S:%S" id addr) l |> String.concat "; "

let string_of_config c =
  match c with
      Simple_config (c, passive) ->
        sprintf "Simple ([%s], [%s])"
          (s_of_simple_config c) (s_of_simple_config passive)
    | Joint_config (c1, c2, passive) ->
        sprintf "Joint ([%s], [%s], [%s])"
          (s_of_simple_config c1) (s_of_simple_config c2)
          (s_of_simple_config passive)
