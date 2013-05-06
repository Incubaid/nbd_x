open Block
open Lwt
module CacheBlock (B: BLOCK) = (struct


  module LbaMap = Map.Make(struct type t = lba let compare = compare end)
  module StringMap = Map.Make(String)
  type block = string
  type t = { 
    back : B.t;
    mutable known: block LbaMap.t;
    mutable outstanding_writes: (lba * block) list;
  }
  
  let block_size t = B.block_size t.back

  let create = 
    let ts = ref (Hashtbl.create 5) in
    (fun uri ->
      try 
        let cb = Hashtbl.find !ts uri in
        Lwt.return cb
      with Not_found ->
        B.create uri >>= fun b ->
        let t = {back = b; 
                 known = LbaMap.empty;
                 outstanding_writes= [];
                } 
        in
        let () = Hashtbl.add !ts uri t in
        Lwt.return t
    )


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

  let _learn_blocks t lbabs =
    t.known <- List.fold_left _learn_block t.known lbabs 


  let write_blocks t writes = 
    let () = _learn_blocks t writes in
    log_f "cache_size: %i%!" (LbaMap.cardinal t.known) >>= fun () ->
    let os = t.outstanding_writes in
    let os' = List.fold_left (fun acc x -> x :: acc) os writes in
    t.outstanding_writes <- os';
    Lwt.return ()

      
  let read_blocks t lbas = 
    let k,u = 
      List.fold_left 
        (fun (known,unknown) lba ->
          try  let block = LbaMap.find lba t.known in 
               let lbab = (lba,block) in
               lbab::known, unknown
          with Not_found -> known,lba :: unknown            
        ) ([],[]) lbas
    in
    let hits = List.length k in
    let misses = List.length u in
    log_f "hit/miss = (%i/%i)" hits misses >>= fun () ->
    match u with
      | [] -> Lwt.return k
      | u ->
        begin
          B.read_blocks t.back u >>= fun lbabs ->
          let () = _learn_blocks t lbabs in
          let r = k @ lbabs in
          Lwt.return r
        end

  let flush t = 
    B.write_blocks t.back t.outstanding_writes >>= fun () ->
    B.flush t.back >>= fun () ->
    t.outstanding_writes <- [];
    Lwt.return ()


end: BLOCK)
