open Unix
open Ocamlbuild_plugin

let run_cmd cmd =
  try
    let ch = Unix.open_process_in cmd in
    let line = input_line ch in
    let () = close_in ch in
    line
  with | End_of_file -> "Not available"

let git_revision = run_cmd "git describe --all --long --always --dirty"

let machine = run_cmd "uname -mnrpio"

let time =
  let tm = Unix.gmtime (Unix.time()) in
  Printf.sprintf "%02d/%02d/%04d %02d:%02d:%02d UTC"
    (tm.tm_mday) (tm.tm_mon + 1) (tm.tm_year + 1900)
    tm.tm_hour tm.tm_min tm.tm_sec
  
let make_version _ _ = 
  let cmd = 
    let template = 
      "let git_revision = %S\n" 
      ^^ "let compile_time = %S\n" 
      ^^ "let machine = %S\n\n"
    in
    Printf.sprintf template git_revision time machine
  in
  Cmd (S [A "echo"; Quote(Sh cmd); Sh ">"; P "version.ml"])



let _ = dispatch & function
  | After_rules ->
    rule "version.ml" ~prod: "version.ml" make_version
  | _ -> ()
