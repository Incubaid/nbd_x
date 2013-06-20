
open Lwt
open Back
open Block
open Nbd_protocol
open Lwtq
module type NBD = sig
  val nbd : uri -> Lwt_unix.file_descr -> unit Lwt.t
end

module Nbd(B:BACK) = (struct

  type dlen = int
  type off = int
  type handle = string
  type rc = int
  type request = 
  | READ  of handle * off * dlen
  | WRITE of handle * off * string
  | FLUSH of handle 
  | TRIM  of handle * off * dlen
  | DISCONNECT of handle
  | BAD of handle * rc

  type resp =
  | RRead of handle * rc * string
  | RWRITE of handle * rc
  | RFLUSH of handle * rc
  | RTRIM of handle * rc
  | RDISCONNECT of handle * rc
  | RGENERIC of handle * rc

  let handle_of = function
    | RRead  (h,_,_) | RWRITE (h,_) | RFLUSH(h,_)
    | RTRIM (h,_)    | RDISCONNECT (h,_)| RGENERIC(h,_) -> h

  let rc_of = function
    | RRead (_,rc,_) | RWRITE (_,rc) | RFLUSH  (_,rc)
    | RTRIM (_,rc) | RDISCONNECT (_,rc) | RGENERIC(_,rc) -> rc
                                    
  module Protocol = Nbd_protocol.Old

  let writer_loop oc pull =
    let rec loop () = 
      Lwt_io.flush oc >>= fun () ->
      pull () >>= fun resp ->
      let h = handle_of resp
      and rc = rc_of resp 
      in
      Protocol.write_response oc rc h >>= fun () ->
      begin 
        match resp with
        | RRead (_,_,s) -> 
          begin
            (* log_f "writer_loop writes: %S" s >>= fun () -> *)
            Lwt_io.write oc s >>= fun () -> 
            loop ()
          end
        | RWRITE (_,_)      -> loop ()
        | RFLUSH (_,_)      -> loop ()
        | RTRIM  (_,_)      -> loop ()
        | RDISCONNECT (_,_) -> Lwt.return ()
        | RGENERIC (_,_)    -> loop ()
      end 
      
    in
    loop ()


  let reader_loop device_size ic push =
    begin
      let buffer = String.create 28 in
      let input = P.make_input buffer 0 in
      let rec loop () = 
        begin
          Lwt_io.read_into_exactly ic buffer 0 28 >>= fun () ->
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
          if request = 2 (* TRIM sends bad params *)
          ||
            (dlen >= 0 && 
               (offset >=0 ) && 
               (offset + dlen) <= device_size)
          then
            begin
              begin
                match request with
                | 0 -> return (READ(handle,offset,dlen))
                | 1 -> 
                  begin
                    let buf = String.create dlen in
                    Lwt_io.read_into_exactly ic buf 0 dlen >>= fun () ->
                    let req = WRITE(handle,offset, buf) in
                    return req
                  end
                | 2 -> return (DISCONNECT handle)
                | 3 -> return (FLUSH handle)
                | 4 -> return (TRIM (handle,offset,dlen))
                | _ -> return (BAD (handle,1))
              end 
              >>= fun req -> 
              push req >>= fun () -> 
              loop ()
            end
          else
            begin
              push (BAD (handle,1)) >>= fun () ->
              log_f "bad request; stopping session"
            end
        end
      in
      loop ()
    end

  let rec processor_loop device_size req_q resp_q back = 
    Q.pull req_q >>= fun req ->
    begin
      match req with
      | READ (h, off, dlen) ->
        begin
          B.read back off dlen >>= fun buf ->
          return (RRead(h,0,buf)) 
        end
      | WRITE(h, off, buf) ->
        begin
          let dlen = String.length buf in
          B.write back buf 0 dlen off >>= fun () ->
          return (RWRITE(h,0)) 
        end
      | FLUSH h ->
        begin
          B.flush back >>= fun () ->
          return (RFLUSH (h,0)) 
        end
      | TRIM (h, off, dlen) ->
        begin
          B.trim back off dlen >>= fun () ->
          return (RTRIM (h,0))
        end
      | DISCONNECT h -> 
        begin
          B.disconnect back >>= fun () ->
          return (RDISCONNECT(h,0))
        end
      | BAD (h,rc) -> return (RGENERIC (h,rc))
    end
    >>= fun resp ->
    Q.push resp_q resp >>= fun () ->
    processor_loop device_size req_q resp_q back
    
    

  let nbd uri socket =
    log_f "nbd: %s" uri >>= fun () ->
    let buffer_size = 131072 in
    let ic = Lwt_io.of_fd ~buffer_size ~mode:Lwt_io.input socket in
    let oc = Lwt_io.of_fd ~buffer_size ~mode:Lwt_io.output socket in
    B.create uri >>= fun back ->
    let device_size = B.device_size back in
    Protocol.write_preamble oc device_size >>= fun () ->
    let req_q = Q.create  () in
    let resp_q = Q.create () in
    
    let push req = 
      Q.push req_q req >>=fun () ->
      log_f "req queue size:%i" (Q.length req_q) 
    in
    let pull ()  = 
      log_f "resp queue size:%i" (Q.length resp_q) >>= fun () ->
      Q.pull resp_q 
    in
    Lwt.catch
      (fun () -> Lwt.join [reader_loop device_size ic push; 
                           writer_loop oc pull; 
                           processor_loop device_size req_q resp_q back])
      (fun e -> log_f "e: %s" (Printexc.to_string e))

end : NBD)
open Generic
open Cache

module NbdF = (Nbd(GenericBack(CacheBlock(File_block.FileBlock))) : NBD)
module NbdA = (Nbd(GenericBack(CacheBlock(Ara_block.ArakoonBlock))): NBD)
module NbdM = (Nbd(GenericBack(Mem_block.MemBlock)): NBD)
module NbdN = (Nbd(GenericBack(Nbd_block.NBDBlock))  : NBD)
module NbdF' = (Nbd(GenericBack(File_block.FileBlock)) : NBD)
module NbdA' =(Nbd(GenericBack(Ara_block.ArakoonBlock)) : NBD)


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
    [("file"   , (module NbdF' : NBD));
     ("mem",     (module NbdM  : NBD));
     ("arakoon", (module NbdA  : NBD));
     ("nbd"    , (module NbdN  : NBD));
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
    log_f "git_version = %s " (Version.git_revision) >>= fun () ->
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
