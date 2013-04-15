let _MAGIC = "NBDMAGIC\x00\x00\x42\x02\x81\x86\x12\x53"

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

let log_f x = Lwt_io.printlf x

open Lwt
open Back


module type NBD = sig
  val nbd : uri -> Lwt_unix.file_descr -> unit Lwt.t
end

module Nbd(B:BACK) = (struct



  let nbd uri socket = 
    let ic = Lwt_io.of_fd Lwt_io.input socket in
    let oc = Lwt_io.of_fd Lwt_io.output socket in
    B.create uri >>= fun back ->
    let device_size = B.device_size back in
    log_f "device_size:%i%!" device_size >>= fun () ->
    let output = Buffer.create (16 + 8 + 128) in
    let () = P.output_raw output _MAGIC in
    let () = P.output_uint64 output device_size in
    let () = P.output_raw  output (String.make 128  '\x00') in
    let msg = P.contents output in
    Lwt_io.write oc msg >>= fun () ->
    let header = String.create 128 in
    let input = P.make_input header 0 in
    let rec loop back = 
      begin
        Lwt_io.read_into_exactly ic header 0 28 >>= fun () ->
        let () = P.reset input in
        let  magic  = P.input_uint32 input in
        let request = P.input_uint32 input in
        let handle  = P.input_raw input 8 in
        let offset  = P.input_uint64 input in
        let dlen    = P.input_uint32 input in
        assert (magic = 0x25609513);
        let write_response rc handle = 
          let output = Buffer.create (4 + 4 + 8) in
          let () = P.output_raw output "gDf\x98" in
          let () = P.output_uint32 output rc in
          let () = P.output_raw output handle in
          let msg = P.contents output in
        (* log_f "msg=%s" (to_hex msg) >>= fun () -> *)
          Lwt_io.write oc msg 
        in
        (* log_f "header:%s\noffset:\t%i\ndlen:\t%i%!" (to_hex header) offset dlen >>= fun ()->  *)
        if offset < 0 || offset +dlen > device_size
        then 
          begin
            log_f "%i < 0 || %i >= %i" offset offset device_size >>= fun () ->
            write_response 1 handle
          end
        else
          begin
            match request with
              | 0 -> (* READ *)
                begin
                (*log_f "read\t0x%016x\t0x%08x\t%f%!" offset dlen (Unix.gettimeofday ()) >>= fun () -> *)
                  B.read back offset dlen >>= fun buf ->
                  write_response 0 handle >>= fun () ->
                  Lwt_io.write oc buf >>= fun () ->
                  loop back
                end
              | 1 -> (* WRITE *)
                begin
                  (*let stamp = Unix.gettimeofday() in
                  log_f "write\t0x%016x\t0x%08x\t%f%!" offset dlen stamp >>= fun () ->
                  *)
                  let buf = String.create dlen in
                  Lwt_io.read_into_exactly ic buf 0 dlen >>= fun () ->
                  B.write back buf 0 dlen offset >>= fun () ->
                  write_response 0 handle >>= fun () ->
                  loop back
                end
              | 2 -> (* DISCONNECT *)
                begin
                  B.disconnect back
                end
              | 3 -> (* FLUSH *)
                begin
                  B.flush back >>= fun () ->
                  write_response 0 handle >>= fun () ->
                  loop back
                end
              | r ->
                begin
                  log_f "r=%i" r >>= fun () ->
                  write_response 0 handle >>= fun () ->
                  loop back
                end
          end
      end
    in
    loop back
      
end : NBD)
open Generic


module NbdF = (Nbd(Fsback.FsBack) : NBD)
module NbdM = (Nbd(Memback.MemBack): NBD) 
module NbdA = (Nbd(GenericBack(Block.ArBlock)) : NBD)
let main () = 
  let modules = 
    [("file"   , (module NbdF : NBD));
     ("mem",     (module NbdM : NBD)); 
     ("arakoon", (module NbdA : NBD));
    ]
  in 
  let uri = Sys.argv.(1) in 
  (* 
     "file:////tmp/my_vol" 
     "arakoon://127.0.0.1:4000/ricky"
     "mem://"
  *)
  let which = Scanf.sscanf uri "%s@://" (fun s -> s) in
  Printf.printf "which:%s\n%!" which;
  let module MyNBD = (val (List.assoc which modules)) in
  let port = 9000 in
  let server () = 
    let ss = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    let sa = Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", port) in
    Lwt_unix.setsockopt ss Unix.SO_REUSEADDR true;
    Lwt_unix.bind ss sa;
    Lwt_unix.listen ss 1024;
    log_f "server started on port %i" port >>= fun () ->
    let rec loop () = 
      begin
        Lwt.catch
          (fun () ->
            Lwt_unix.accept ss >>= fun (fd,_) ->
            Lwt.ignore_result (MyNBD.nbd uri fd);
            Lwt.return ()          
          )
          (function
            | e -> Lwt_io.printl (Printexc.to_string e) >>= fun () -> Lwt.return ()
          )
        >>= fun () ->
        loop ()
      end
    in
    loop ()
  in
  Lwt_main.run (server ())

let () = main()

