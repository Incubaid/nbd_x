type lba = int
open Lwt

let log_f x = Lwt_io.printlf x

module type BLOCK = sig

  type t

  val block_size:  t -> int



  val create : string -> t Lwt.t

  val read_blocks  : t -> lba list -> ((lba * string) list) Lwt.t

  val write_blocks : t -> (lba * string) list -> unit Lwt.t

  val flush : t -> unit Lwt.t
end



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
      

  let block_size t = 1024

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
    let bs = block_size t in
    let pos = lba * bs in
    let fd = t.fd in
    Lwt_unix.lseek fd pos Unix.SEEK_SET >>= fun pos' ->
    assert (pos' = pos);
    let block = String.create bs in
    _read_buf fd block 0 bs >>= fun () ->
    Lwt.return block


  let read_blocks t lbas = 
    Lwt_list.map_s(fun lba -> read_block t lba >>= fun block -> Lwt.return (lba,block)) lbas
      
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
      Lwt_unix.lseek fd pos Unix.SEEK_SET >>= fun pos' ->
      assert(pos' = pos);
      _write_buf fd block 0 bs
    in
    Lwt_list.iter_s (fun (lba,block) -> write_block lba block) writes


  let flush t = Lwt_unix.fsync t.fd
end : BLOCK)


