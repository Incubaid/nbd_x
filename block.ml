type lba = int
open Lwt

let log_f x = Lwt_io.printlf x

module type BLOCK = sig

  type t

  val block_size:  t -> int



  val create : string -> t Lwt.t

  val read_blocks  : t -> lba list -> ((lba * string) list) Lwt.t

  val write_blocks : t -> (lba * string) list -> unit Lwt.t

  val trim_blocks : t -> lba list -> unit Lwt.t
  val flush : t -> unit Lwt.t

  val disconnect : t -> unit Lwt.t
  val device_size: t -> int
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


let lba2s x = Printf.sprintf "%04x" x 

let lbas2s lbas =
  let b = Buffer.create 128 in  
  let rec loop = function
    | [] -> Buffer.contents b
    | [x] -> begin Buffer.add_string b (lba2s x); Buffer.contents b end
    | x :: t ->
      let () = Buffer.add_string b (lba2s x) in
      let () = Buffer.add_char b ';' in
      loop t
  in
  loop lbas

let lbabs2s lbabs = 
  let buf = Buffer.create 128 in
  let add_one (l,b) = 
    Buffer.add_char buf '(';
    Buffer.add_string buf (lba2s l);
    let b0 = b.[0]
    and bl = b.[String.length b - 1]
    in
    Buffer.add_string buf (Printf.sprintf ", %C...%C)" b0 bl)
  in

  let rec loop = function
    | []     -> Buffer.contents buf
    | [x]    -> add_one x; Buffer.contents buf
    | x :: t -> add_one x; Buffer.add_char buf ';'; loop t
  in
  loop lbabs



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

  let disconnect t = Lwt_unix.fsync t.fd

  let trim_blocks t lbas = Lwt.return ()
end : BLOCK)


