let test () = 
  let i = P.make_input "\x00\x12\xd6\x87" 0 in
  let v = P.input_uint32 i in
  let s2 = P.output_uint32 v in
  v,s2


let test2() = 
 (* struct.pack(">Q", 1234567890123)
    '\x00\x00\x01\x1fq\xfb\x04\xcb' *)
  let i = P.make_input "\x00\x00\x01\x1fq\xfb\x04\xcb" 0 in
  let v = P.input_uint64 i in
  v
ff ff ff ff ff ff f8
(* 0x7ffffffffffff800 *)
