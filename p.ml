type input = {s:string; mutable o:int}
type output = Buffer.t

let make_output size_hint = Buffer.create size_hint

let output_uint64 output i = (* ">Q" *)

  let r = String.make 8 '\x00' in
  let rec loop pos i =
    if i = 0
    then ()
    else
      let b = i land 0xff in
      let () = r.[pos] <- Char.chr b in
      let pos' = pos - 1 in
      let i' = i lsr 8 in
      loop pos' i'
  in
  let () = loop 7 i in
  Buffer.add_string output r

let output_uint32 output i =
  let r = String.create 4 in
  let rec loop p i =
    if p < 0
    then ()
    else
      let b = i land 0xff in
      let () = r.[p] <- Char.chr b in
      let p' = p - 1 in
      let i' = i lsr 8 in
      loop p' i'
  in
  let () = loop 3 i in
  Buffer.add_string output r

let output_raw output s =
  Buffer.add_string output s

let output_char output c = Buffer.add_char output c

let contents output = Buffer.contents output

let make_input s o = {s; o}

let input_uint32 i = (* ">L" *)
  let rec loop acc p pos =
    if p = 4
    then acc
    else
      let b = Char.code i.s.[pos] in
      let acc' = (acc lsl 8 ) lor b
      and p' = p + 1
      and pos' = pos + 1
      in
      loop acc' p' pos'
  in
  let v = loop 0 0 i.o in
  let () = i.o <- i.o + 4 in
  v

let input_uint64 i =
  let rec loop acc p pos =
    if p = 8
    then acc
    else
      let b = Char.code i.s.[pos] in
      let acc' = (acc lsl 8 ) lor b in
      let p' = p+1
      and pos' = pos+ 1
      in
      loop acc' p' pos'
  in
  let v = loop 0 0 i.o in
  let () = i.o <- i.o + 8 in
  v

let input_raw i n =
  let o = i.o in
  let r = String.sub i.s o n in
  let () = i.o <- o + n in
  r

let input_char i =
  let o = i.o in
  let c = i.s.[o] in
  let () = i.o <- o + 1 in
  c

let reset i = i.o <- 0

