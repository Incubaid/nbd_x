let _MAGIC = "NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53"
let _RMAGIC = "gDf\x98"
let _READ = 0
let _WRITE = 1
let _DISCONNECT = 2
let _FLUSH = 3

let magic = 0x25609513

open Lwt


let log_f x = Lwt_io.printlf x

let make_handle () = String.create 8 
  
let write_preamble oc device_size = 
  log_f "device_size:%i%!" device_size >>= fun () ->
  let output = Buffer.create (16 + 8 + 128) in
  let () = P.output_raw output _MAGIC in
  let () = P.output_uint64 output device_size in
  let () = P.output_raw  output (String.make 128  '\x00') in
  let msg = P.contents output in
  Lwt_io.write oc msg 

let write_response oc rc handle = 
  let output = Buffer.create (4 + 4 + 8) in
  let () = P.output_raw output "gDf\x98" in
  let () = P.output_uint32 output rc     in
  let () = P.output_raw output handle    in
  let msg = P.contents output            in
  Lwt_io.write oc msg

let write_request oc (*magic *) req handle offset dlen = 
  let output = Buffer.create 28 in
  let () = P.output_uint32 output magic  in
  let () = P.output_uint32 output req    in
  let () = P.output_raw    output handle in
  let () = P.output_uint64 output offset in
  let () = P.output_uint32 output dlen   in
  let req = P.contents output in
  Lwt_io.write oc req 


type response = {m:string;rc:int;h:string}

let response2s {m;rc;h} = Printf.sprintf "{m=%S;rc=%i;h=%S}" m rc h

let handle r = r.h
let rc r = r.rc

let read_response ic = 
  let res_size = 4 + 4 + 8 in
  let res = String.create res_size in
  Lwt_io.read_into_exactly ic res 0 res_size >>= fun () ->
  let input = P.make_input res 0 in
  let m = P.input_raw     input 4 in
  let rc = P.input_uint32 input   in
  let h  = P.input_raw    input 8 in
  Lwt.return { m;rc;h }
