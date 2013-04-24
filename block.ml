type lba = int
open Lwt

let log_f x = Lwt_io.printlf x

module type BLOCK = sig

  type t

  val block_size:  t -> int



  val create : string -> t Lwt.t

  val read_block  : t -> lba -> string Lwt.t

  val write_blocks : t -> (lba * string) list -> unit Lwt.t

  val flush : t -> unit Lwt.t
end


module CacheBlock (B: BLOCK) = (struct


  module LbaMap = Map.Make(struct type t = lba let compare = compare end)
  module StringMap = Map.Make(String)
  type block = string
  type t = { 
    back : B.t;
    mutable known: block LbaMap.t;
  }
  
  let block_size t = B.block_size t.back

  let create uri = 
    B.create uri >>= fun b ->
    let t = {back = b; 
             known = LbaMap.empty;
            } in
    Lwt.return t
  
  let _learn_block known (lba,block) = 
    let known'= 
      if LbaMap.cardinal known > 3000 
      then
        let k,_ = LbaMap.choose known in
        LbaMap.remove k known
      else 
        known 
    in
    LbaMap.add lba block known'

  let write_blocks t writes = 
    let () = t.known <- List.fold_left _learn_block t.known writes in
    log_f "cache_size: %i%!" (LbaMap.cardinal t.known) >>= fun () ->
    B.write_blocks t.back writes

      
  let read_block t lba = 
    try  let block = LbaMap.find lba t.known in 
         log_f "from cache: %x%!" lba >>= fun () ->
         Lwt.return block
    with Not_found -> B.read_block t.back lba

  let flush t = B.flush t.back 

end: BLOCK)

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
      

  let block_size t = 4096

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


module ArBlock = (struct
  type t = { 
    sa : Unix.sockaddr ; 
    mc : Arakoon_client.client;
  }

  open Lwt
  let create uri = 
    (* arakoon://host:port *) 
    let host,port,cluster_id = Scanf.sscanf uri "arakoon://%s@:%i/%s@" (fun host port cid-> (host,port,cid)) in
    let ia = Unix.inet_addr_of_string host in
    let sa = Unix.ADDR_INET (ia, port) in
    Lwt_io.open_connection sa >>= fun conn ->
    Arakoon_remote_client.make_remote_client cluster_id conn >>= fun client ->
    let t = {sa ; mc = client; } in
    Lwt.return t

  let block_size t = 0x00001000 
  let block_mask  = 0xffffe000

  let make_key lba = Printf.sprintf "%016x" lba 

  let read_block t lba = 
    let key = make_key lba in
    let bs = block_size t in
    Lwt.catch
      (fun () -> 
        t.mc # get key >>=fun block ->
        assert (String.length block = bs);
        Lwt.return block
      )
      (function 
        | Arakoon_exc.Exception(Arakoon_exc.E_NOT_FOUND,_ )->
          let r = String.make bs '\x00' in
          Lwt.return r
        | e -> Lwt.fail e
      )

  let write_blocks t writes = 
    let seq = List.map 
      (fun (lba,block) ->
        let key = make_key lba in
        Arakoon_client.Set (key,block)) writes
    in
    t.mc # sequence seq

  let flush t = Lwt.return ()


end : BLOCK)
