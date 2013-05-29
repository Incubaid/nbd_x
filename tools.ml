let log_f x = Lwt_io.printlf x

let create_string n = 
  assert (n < 10 * 1024 * 1024);
  String.create n

let to_hex s = 
  let rec loop acc i = 
    if i < 0 then String.concat " " acc
    else
      let c = s.[i] in
      let ci = Char.code c in
      let acc' = Printf.sprintf "%2x" ci :: acc in
      loop acc' (i-1)
  in
  loop [] (String.length s-1)


let lba2s x = Printf.sprintf "%04x" x 

let lbas2s lbas =
  let b = Buffer.create 128 in  
  let rec loop = function
    | [] -> Buffer.contents b
    | [x] -> begin Buffer.add_string b (lba2s x); Buffer.contents b end
    | x :: t ->
      let () = Buffer.add_string b (lba2s x) in
      let () = Buffer.add_char b ';' in
      loop t
  in
  loop lbas

let lbabs2s lbabs = 
  let buf = Buffer.create 128 in
  let add_one (l,b) = 
    Buffer.add_char buf '(';
    Buffer.add_string buf (lba2s l);
    let b0 = b.[0]
    and bl = b.[String.length b - 1]
    in
    Buffer.add_string buf (Printf.sprintf ", %C...%C)" b0 bl)
  in

  let rec loop = function
    | []     -> Buffer.contents buf
    | [x]    -> add_one x; Buffer.contents buf
    | x :: t -> add_one x; Buffer.add_char buf ';'; loop t
  in
  loop lbabs

