open Back
open Lwt

module FsBack = (struct
  type t = {fd:Lwt_unix.file_descr; 
            device_size:int;
           }

  let log_f x = Lwt_io.printlf x

  let create uri = 
    let fn = Scanf.sscanf uri "file://%s" (fun s -> s) in
    log_f "%s => filename: %S%!" uri fn >>= fun () ->
    Lwt_unix.openfile fn [Unix.O_RDWR;Unix.O_CREAT] 0o644 >>= fun fd ->    
    Lwt_unix.lseek fd 0 Unix.SEEK_END >>= fun device_size ->
    Lwt.return {fd;device_size}
      

  let seek fd pos = 
  Lwt_unix.lseek fd pos Unix.SEEK_SET >>= fun pos' ->
  assert (pos = pos');
  Lwt.return ()

  let rec read_buf fd buf o td = 
    if td = 0
    then Lwt.return () 
    else
      begin
        Lwt_unix.read fd buf o td >>= fun br ->
        let o'  = o  + br in
        let td' = td - br in
        read_buf fd buf o' td'
      end
      
  let rec write_buf fd buf o td = 
    if td = 0 
    then Lwt.return () 
    else
      begin
        Lwt_unix.write fd buf o td >>= fun bw ->
        let o'  = o  + bw in
        let td' = td - bw in
        write_buf fd buf o' td'
      end


  let flush t = Lwt_unix.fsync t.fd

    
  let device_size t = t.device_size

  let read t off dlen = 
    let buf = String.create dlen in 
    seek t.fd off >>= fun () ->
    read_buf t.fd buf 0 dlen >>= fun () ->
    Lwt.return buf

      
  let write t buf boff dlen offset =
    seek t.fd offset >>= fun () ->
    write_buf t.fd buf boff dlen 

  let disconnect t = Lwt_unix.close t.fd
end : BACK)
