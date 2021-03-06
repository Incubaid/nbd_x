open Block
open Lwt
module FileBlock = (struct
  type t = { fd: Lwt_unix.file_descr;
             device_size:int
           }
  
  let create uri = 
    let fn = Scanf.sscanf uri "file://%s" (fun s -> s) in
    log_f "%s => filename: %S%!" uri fn >>= fun () ->
    Lwt_unix.openfile fn [Unix.O_RDWR;Unix.O_CREAT] 0o644 >>= fun fd ->    
    Lwt_unix.lseek fd 0 Unix.SEEK_END >>= fun device_size ->
    Lwt.return {fd;device_size}
      

  let block_size t = 4096

  let device_size t = t.device_size

  let rec _read_buf fd buf o td = 
    if td = 0
    then Lwt.return () 
    else
      begin
        Lwt_unix.read fd buf o td >>= fun br ->
        let o'  = o  + br in
        let td' = td - br in
        _read_buf fd buf o' td'
      end

  let read_block t lba =
    (* log_f "FileBlock : read_block %s" (lba2s lba) >>= fun () -> *)
    let bs = block_size t in
    let pos = lba * bs in
    let fd = t.fd in
    Lwt_unix.lseek fd pos Unix.SEEK_SET >>= fun pos' ->
    assert (pos' = pos);
    let block = String.create bs in
    _read_buf fd block 0 bs >>= fun () ->
    Lwt.return block


  let read_blocks t lbas = 
    Lwt_list.map_s
      (fun lba -> read_block t lba >>= fun block -> 
        Lwt.return (lba,block)) 
      lbas
      
  let rec _write_buf fd buf o td = 
    if td = 0 
    then Lwt.return () 
    else
      begin
        Lwt_unix.write fd buf o td >>= fun bw ->
        let o'  = o  + bw in
        let td' = td - bw in
        _write_buf fd buf o' td'
      end
        
  let write_blocks t writes = 
    let fd = t.fd in
    let bs = block_size t in
    let write_block lba block = 
      let pos = lba * bs in
      (*log_f "write lba:%016x pos:%016x %C %C" lba pos block.[0] block.[bs -1]
      >>= fun () ->
      *)
      Lwt_unix.lseek fd pos Unix.SEEK_SET >>= fun pos' ->
      assert(pos' = pos);
      _write_buf fd block 0 bs
    in
    Lwt_list.iter_s (fun (lba,block) -> write_block lba block) writes


  let flush t = Lwt_unix.fsync t.fd

  let disconnect t = 
    log_f "FileBlock : disconnect" >>= fun () ->
    Lwt_unix.fsync t.fd >>= fun () -> Lwt_unix.close t.fd >>= fun () ->
    log_f "FileBlock : disconnected"

  let trim t off dlen = 
    log_f "FileBlock : trim 0x%0x16x 0x%08x" off dlen >>= fun () ->
    Lwt.return ()
end : BLOCK)


