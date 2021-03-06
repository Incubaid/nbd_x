open Lwt
open Back
open Block

module MemBlock = (struct
  open Lwt
  module StringMap = Map.Make(String)

  type t = { mutable blocks : string StringMap.t }

  let block_size t = 0x00001000
  let device_size t = 6 * 1024 * 1024 * 1024
  let zeros t = String.make (block_size t) '\x00'




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
    let key = make_key lba in
    let block = 
      try 
        StringMap.find key t.blocks
      with
        | Not_found -> zeros t
    in
    Lwt.return block

  let read_blocks t lbas = 
    Lwt_list.map_s 
      (fun lba -> read_block t lba >>= fun block -> Lwt.return (lba,block)) 
      lbas

  let write_blocks t writes =
    let map = t.blocks in
    let add_one map (lba,block) =         
      let key = make_key lba in
      StringMap.add key block map
    in
    let map' = List.fold_left add_one map writes in
    let () = t.blocks <- map' in
    Lwt.return ()

  let flush t = Lwt.return ()

  let trim t off dlen = 
    log_f "mem : trim 0x%0x16x 0x%08x" off dlen >>= fun () ->
    Lwt.return ()

  let disconnect t = Lwt.return ()
end : BLOCK)

