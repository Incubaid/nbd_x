
open Lwt
open Back
open Block
open Nbd_protocol

module type NBD = sig
  val nbd : uri -> Lwt_unix.file_descr -> unit Lwt.t
end

module Nbd(B:BACK) = (struct



  let nbd uri socket = 
    let ic = Lwt_io.of_fd ~buffer_size:8192 ~mode:Lwt_io.input socket in
    let oc = Lwt_io.of_fd ~buffer_size:8192 ~mode:Lwt_io.output socket in
    B.create uri >>= fun back ->
    let device_size = B.device_size back in
    Nbd_protocol.write_preamble oc device_size >>= fun () ->
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
        (* log_f "req=%i offset=%016x dlen=%i%!" request offset dlen >>= fun ()->   *)
        if offset < 0 || offset +dlen > device_size
        then 
          begin
            log_f "%i < 0 || %i >= %i" offset offset device_size >>= fun () ->
            Nbd_protocol.write_response oc 1 handle >>= fun () ->
            log_f "ending it here %!"
          end
        else
          begin
            match request with
              | 0 -> (* READ *)
                begin
                (*log_f "read\t0x%016x\t0x%08x\t%f%!" offset dlen (Unix.gettimeofday ()) >>= fun () -> *)
                  B.read back offset dlen >>= fun buf ->
                  Nbd_protocol.write_response oc 0 handle >>= fun () ->
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
                  Nbd_protocol.write_response oc 0 handle >>= fun () ->
                  loop back
                end
              | 2 -> (* DISCONNECT *)
                begin
                  B.disconnect back
                end
              | 3 -> (* FLUSH *)
                begin
                  B.flush back >>= fun () ->
                  Nbd_protocol.write_response oc 0 handle >>= fun () ->
                  loop back
                end
              | r ->
                begin
                  log_f "r=%i" r >>= fun () ->
                  Nbd_protocol.write_response oc 0 handle >>= fun () ->
                  loop back
                end
          end
      end
    in
    Lwt.catch 
      (fun () -> loop back)
      (fun e -> log_f "e: %s" (Printexc.to_string e))
      
end : NBD)
open Generic


module NbdF = (Nbd(GenericBack(Block.CacheBlock(Block.FileBlock))) : NBD)
module NbdM = (Nbd(GenericBack(Mem_block.MemBlock)): NBD) 
module NbdA = (Nbd(GenericBack(Block.ArBlock))   : NBD)
module NbdN = (Nbd(GenericBack(Nbd_block.NBDBlock))  : NBD)

let main () = 
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  let modules = 
    [("file"   , (module NbdF : NBD));
     ("mem",     (module NbdM : NBD)); 
     ("arakoon", (module NbdA : NBD));
     ("nbd"    , (module NbdN : NBD));
    ]
  in 
  let port = ref 9000 in
  (* 
     "file:////tmp/my_vol" 
     "arakoon://127.0.0.1:4000/ricky"
     "mem://"
  *)
  let uri = ref "mem://" in
  let args = [("-p", Arg.Set_int port, "server port");
             ] 
  in Arg.parse args (fun s -> uri := s) "xxxx"; 

  let which = Scanf.sscanf !uri "%s@://" (fun s -> s) in
  Printf.printf "which:%s\n%!" which;
  let module MyNBD = (val (List.assoc which modules)) in
  let server () = 
    let ss = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    let sa = Unix.ADDR_INET (Unix.inet_addr_of_string "127.0.0.1", !port) in
    Lwt_unix.setsockopt ss Unix.SO_REUSEADDR true;
    Lwt_unix.bind ss sa;
    Lwt_unix.listen ss 1024;
    log_f "server started on port %i" !port >>= fun () ->
    let wrap f = 
      Lwt.catch f (fun ex -> log_f "ex:%s" (Printexc.to_string ex))
    in
    let rec loop () = 
      begin
        Lwt.catch
          (fun () ->
            Lwt_unix.accept ss >>= fun (fd,_) ->
            Lwt.ignore_result (wrap (fun () -> MyNBD.nbd !uri fd));
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

