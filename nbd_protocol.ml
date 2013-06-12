let _OLD_MAGIC = "NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53"
let _NEW_MAGIC = "NBDMAGIC\x49\x48\x41\x56\x45\x4f\x50\x54"
let _RMAGIC = "gDf\x98"
(* follow C naming *)
let _NBD_CMD_READ  = 0
let _NBD_CMD_WRITE = 1
let _NBD_CMD_DISC  = 2
let _NBD_CMD_FLUSH = 3
let _NBD_CMD_TRIM  = 4

let old_magic = 0x25609513
let new_magic = 0x49484156454f5054

open Lwt
open Tools
module Old = struct
  let make_handle () = String.create 8 
  
  type preamble = {device_size: int}  

  let preamble2s p = Printf.sprintf "{device_size = %i}" p.device_size

  let write_preamble oc device_size = 
    log_f "device_size:%i%!" device_size >>= fun () ->
    let flags = String.make 4 '\x00' in
    flags.[3] <- '\x15'; 
  (* 
     0x00000001: HAS_FLAGS    (* X *)
     0x00000002: READ_ONLY    
     0x00000004: SEND_FLUSH   (* X *)
     0x00000010: ROTATIONAL   (* X *)
     0x00000020: SEND_TRIM    (* not supported yet *)
  *)
    let output = P.make_output (16 + 8 + 128) in
    let () = P.output_raw output _OLD_MAGIC in
    let () = P.output_uint64 output device_size in
    let () = P.output_raw output flags in
    let () = P.output_raw  output (String.make 124  '\x00') in
    let msg = P.contents output in
    Lwt_io.write oc msg 

  let read_preamble ic = 
    let s = 16 + 8 + 128 in
    let buf = String.create s in
    Lwt_io.read_into_exactly ic buf 0 s >>= fun () ->
    let input = P.make_input buf 0 in
    let magic = P.input_raw input 16 in
    assert (magic = _OLD_MAGIC);
    let device_size = P.input_uint64 input in
    Lwt.return {device_size}

  let write_response oc rc handle = 
    let output = P.make_output (4 + 4 + 8) in
    let () = P.output_raw output "gDf\x98" in
    let () = P.output_uint32 output rc     in
    let () = P.output_raw output handle    in
    let msg = P.contents output            in
    Lwt_io.write oc msg
      
  let write_request oc (*magic *) req handle offset dlen = 
    let output = P.make_output 28 in
    let () = P.output_uint32 output old_magic  in
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
end
