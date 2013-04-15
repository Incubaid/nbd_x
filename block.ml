module type BLOCK = sig

  val block_size : int

  type t

  val create : string -> t Lwt.t

  val read_block  : t -> int -> string Lwt.t

  val write_block : t -> int -> string -> int -> unit Lwt.t

  val flush : t -> unit Lwt.t
end


module MemBlock = (struct
  open Lwt
  let block_size = 0x00001000

  let zeros = String.make block_size '\x00'

  module StringMap = Map.Make(String)
  type t = { mutable blocks : string StringMap.t }

  let make_key lba = Printf.sprintf "%0x16x" lba
     
  let create = 
    (* to be able to reconnect *)
    let ts = ref (Hashtbl.create 5) in
    (fun uri -> 
      let t = 
        try Hashtbl.find !ts uri
        with Not_found -> 
          let t = {blocks = StringMap.empty} in
          let () = Hashtbl.add !ts uri t in
          t
      in
      Lwt.return t)

  let read_block t lba = 
    (* Lwt_io.printlf "read_block %0x" lba >>= fun () -> *)
    let key = make_key lba in
    let block = 
      try 
        StringMap.find key t.blocks
      with
        | Not_found -> zeros
    in
    (* Lwt_io.printlf "returning mem[%i]=%S..." lba (String.sub block 0 4) >>= fun () -> *)
    Lwt.return block

  let write_block t lba buf boff =
    (* Lwt_io.printlf "mem[%i]=%S..." lba (String.sub buf boff 4) >>= fun () -> *)
    let key = make_key lba in
    let block = String.sub buf boff block_size in
    let () = t.blocks <- StringMap.add key block t.blocks in
    Lwt.return ()

  let flush t = Lwt.return ()

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

  let block_size  = 0x00001000 
  let block_mask  = 0xffffe000

  let make_key lba = Printf.sprintf "%016x" lba 

  let read_block t lba = 
    let key = make_key lba in
    Lwt.catch
      (fun () -> 
        t.mc # get key >>=fun block ->
        assert (String.length block = block_size);
        Lwt.return block
      )
      (function 
        | Arakoon_exc.Exception(Arakoon_exc.E_NOT_FOUND,_ )->
          let r = String.make block_size '\x00' in
          Lwt.return r
        | e -> Lwt.fail e
      )

  let write_block t lba buf boff = 
    let key = make_key lba in    
    let value = String.sub buf boff block_size in
    t.mc # set key value 

  let flush t = Lwt.return ()


end : BLOCK)
