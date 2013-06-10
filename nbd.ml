
open Lwt
open Back
open Block
open Nbd_protocol

module type NBD = sig
  val nbd : uri -> Lwt_unix.file_descr -> unit Lwt.t
end

module Nbd(B:BACK) = (struct
  module Protocol = Nbd_protocol.Old
  let nbd uri socket =
    log_f "nbd: %s" uri >>= fun () ->
    let buffer_size = 131072 in
    let ic = Lwt_io.of_fd ~buffer_size ~mode:Lwt_io.input socket in
    let oc = Lwt_io.of_fd ~buffer_size ~mode:Lwt_io.output socket in
    B.create uri >>= fun back ->
    let device_size = B.device_size back in
    Protocol.write_preamble oc device_size >>= fun () ->
    let header = String.create 128 in
    let input = P.make_input header 0 in
    let rec loop back =
      begin
        Lwt_io.read_into_exactly ic header 0 28 >>= fun () ->
        let () = P.reset input in
        let  magic  = P.input_uint32 input in
        let request = P.input_uint32 input in
        let handle  = P.input_raw input 8  in
        let offset  = P.input_uint64 input in
        let dlen    = P.input_uint32 input in
        assert (magic = 0x25609513);
        let stamp = Unix.gettimeofday() in
        log_f "nbd: req=%i offset=0x%016x dlen=0x%08x\t%f%!" request offset dlen stamp
        >>= fun ()->
        if ((offset < 0) || 
               (offset +dlen > device_size)) && 
          request != 2 (* disconnect sometimes really sends awful offsets *)
        then
          begin
            log_f "nbd: %i < 0 || %i >= %i" offset offset device_size >>= fun () ->
            Protocol.write_response oc 1 handle >>= fun () ->
            log_f "ending it here %!"
          end
        else
          begin
            match request with
              | 0 -> (* READ *)
                begin
                  B.read back offset dlen >>= fun buf ->
                  assert (String.length buf = dlen);
                  Protocol.write_response oc 0 handle >>= fun () ->
                  Lwt_io.write oc buf >>= fun () ->
                  loop back
                end
              | 1 -> (* WRITE *)
                begin
                  let buf = String.create dlen in
                  Lwt_io.read_into_exactly ic buf 0 dlen >>= fun () ->
                  B.write back buf 0 dlen offset >>= fun () ->
                  Protocol.write_response oc 0 handle >>= fun () ->
                  loop back
                end
              | 2 -> (* DISCONNECT *)
                begin
                  B.disconnect back >>= fun () ->
                  Protocol.write_response oc 0 handle 
                  (* end of conversation *)
                end
              | 3 -> (* FLUSH *)
                begin
                  B.flush back >>= fun () ->
                  Protocol.write_response oc 0 handle >>= fun () ->
                  loop back
                end
              | 4 -> (* TRIM *)
                begin
                  B.trim back offset dlen >>= fun () ->
                  Protocol.write_response oc 0 handle >>= fun () ->
                  loop back
                end
              | r ->
                begin
                  Protocol.write_response oc 0 handle >>= fun () ->
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
open Cache

module NbdF = (Nbd(GenericBack(CacheBlock(File_block.FileBlock))) : NBD)
module NbdF' = (Nbd(GenericBack(File_block.FileBlock)) : NBD)
module NbdM = (Nbd(GenericBack(Mem_block.MemBlock)): NBD)
module NbdA = (Nbd(GenericBack(CacheBlock(Ara_block.ArakoonBlock)))   : NBD)
module NbdN = (Nbd(GenericBack(Nbd_block.NBDBlock))  : NBD)

type action = 
| Version
| Server

let show_version () = 
  Printf.printf "git_revision:\t%S\n" Version.git_revision;
  Printf.printf "compiled:\t%S\n" Version.compile_time;
  Printf.printf "machine:\t%S\n" Version.machine


let start_server uri host port=
  Sys.set_signal Sys.sigpipe Sys.Signal_ignore;
  let modules =
    [("file"   , (module NbdF : NBD));
     ("mem",     (module NbdM : NBD));
     ("arakoon", (module NbdA : NBD));
     ("nbd"    , (module NbdN : NBD));
    ]
  in
  let which = Scanf.sscanf uri "%s@://" (fun s -> s) in
  Printf.printf "which:%s\n%!" which;
  let module MyNBD = (val (List.assoc which modules)) in
  let server () =
    let ss = Lwt_unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
    let sa = Unix.ADDR_INET (Unix.inet_addr_of_string host, port) in
    Lwt_unix.setsockopt ss Unix.SO_REUSEADDR true;
    Lwt_unix.bind ss sa;
    Lwt_unix.listen ss 1024;
    log_f "server started on port %i" port >>= fun () ->
    let wrap f =
      Lwt.catch f (fun ex -> log_f "ex:%s" (Printexc.to_string ex))
    in
    let rec loop () =
      begin
        Lwt.catch
          (fun () ->
            Lwt_unix.accept ss >>= fun (fd,_) ->
            Lwt.ignore_result (wrap (fun () -> MyNBD.nbd uri fd));
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

let main () =
  let port = ref 9000 in
  let host = ref "0.0.0.0" in
  let uri = ref "mem://" in
  let action = ref Server in
  let set_action a = Arg.Unit (fun () -> action := a) in
  let args = [("-p", Arg.Set_int port, Printf.sprintf "server port (default is %i)" !port);
              ("-h", Arg.Set_string host, Printf.sprintf "server IP (default is %s)" !host);
              ("--version", set_action Version, "show version info");
             ] in
  let help = (Printf.sprintf "%s [-p port] uri\n" Sys.argv.(0)) ^
    "\npossible URIs are:\n" ^
    "\tarakoon://<cluster_id>/<vol_id>/<node0_id#<host0>#<port0>/node1_id#<host1>#<port1>...\n" ^
    "\tfile:///tmp/my_vol\n" ^
    "\tmem://\n"
  in
  Arg.parse args (fun s -> uri := s) help;
  match !action with
  | Version -> show_version()
  | Server  -> start_server !uri !host !port
  

let () = main()
