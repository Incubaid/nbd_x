open Block
open Lwt
module CacheBlock (B: BLOCK) = (struct

  module LbaMap = Map.Make(struct 
    type t = lba 
    let compare (t0:lba) (t1:lba) = t1 - t0  
  end)

  type block = string

  type block_map = string LbaMap.t
  type t = { 
    back : B.t;
    mutable outstanding_writes: block_map;
  }
  
  let _max_size = 3000

  let block_size t = B.block_size t.back

  let device_size t = B.device_size t.back

  let create = 
    let ts = ref (Hashtbl.create 5) in
    (fun uri ->
      try 
        let cb = Hashtbl.find !ts uri in
        Lwt.return cb
      with Not_found ->
        B.create uri >>= fun b ->
        let t = {back = b; 
                 outstanding_writes= LbaMap.empty;
                } 
        in
        let () = Hashtbl.add !ts uri t in
        Lwt.return t
    )



  let write_blocks t writes = 
    let os = t.outstanding_writes in
    let os' = List.fold_left (fun os (lba,block) -> LbaMap.add lba block os) os writes in
    t.outstanding_writes <- os';
    Lwt.return ()

      
  let read_blocks t lbas = 
    log_f "Cache : read_blocks lbas=[%s]" (lbas2s lbas) >>= fun ()->
    let ow = t.outstanding_writes in
    let k = List.filter (fun l -> LbaMap.mem l ow) lbas in
    let u = List.filter (fun l -> not (LbaMap.mem l ow)) lbas in
    log_f "Cache : k=[%s] u=[%s]" (lbas2s k) (lbas2s u) >>= fun () ->

    let kbs = List.map (fun l -> (l, LbaMap.find l ow)) k in
    B.read_blocks t.back u >>= fun ubs ->
    let r = kbs @ ubs in
    let sr = List.sort (fun (l0,_) (l1,_) -> l0 - l1) r in
    log_f "Cache : sr=[%s]" (lbabs2s sr) >>= fun () ->
    Lwt.return sr
                                      
  let _writes t = 
    let rw = LbaMap.fold (fun l b rw -> (l,b)::rw) t.outstanding_writes [] in
    List.rev rw 
    
  let flush t = 
    let w = _writes t in
    log_f "Cache: flushing: %i" (List.length w) >>= fun () ->
    let lbas = List.map fst w in
    log_f "Cache: ws: [%s]" (lbas2s lbas) >>= fun () ->
    B.write_blocks t.back w >>= fun () ->
    B.flush t.back >>= fun () ->
    t.outstanding_writes <- LbaMap.empty;
    Lwt.return ()

  let disconnect t = 
    begin
      if t.outstanding_writes = LbaMap.empty 
      then Lwt.return ()
      else
        begin
          let w = _writes t in
          B.write_blocks t.back w >>= fun () ->
          t.outstanding_writes <- LbaMap.empty;
          Lwt.return ()
        end
    end
    >>= fun ()->
    B.disconnect t.back
        
  let trim_blocks t lbas = 
    Lwt.return () 

end: BLOCK)
