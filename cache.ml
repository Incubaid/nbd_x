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
    mutable known : block_map;
  }
  
  let _max_size = 3000

  let block_size t = B.block_size t.back

  let device_size t = B.device_size t.back

  let create uri = 
    B.create uri >>= fun b ->
    let t = {back = b; 
             outstanding_writes= LbaMap.empty;
             known = LbaMap.empty;
            } 
    in
    Lwt.return t


  let _writes t = 
    LbaMap.fold (fun l b rw -> (l,b)::rw) t.outstanding_writes [] 
      
  let flush t = 
    let w = _writes t in
    log_f "Cache: flushing: %i" (List.length w) >>= fun () ->
    let lbas = List.map fst w in
    log_f "Cache: ws: [%s]" (lbas2s lbas) >>= fun () ->
    B.write_blocks t.back w >>= fun () ->
    B.flush t.back >>= fun () ->
    t.outstanding_writes <- LbaMap.empty;
    Lwt.return ()

  let _learn t writes = 
    let known = t.known in
    let known0 = List.fold_left (fun known (lba,block) -> LbaMap.add lba block known) known writes in
    let rec forget known i =
      if i = 0 
      then known
      else 
        let (k,_) = LbaMap.choose known in
        let known' = LbaMap.remove k known in
        forget known' (i-1)
    in
    let known1, size = 
      let size = LbaMap.cardinal known0 in
      if size > _max_size 
      then forget known0 (size - _max_size) , _max_size
      else known0,size
    in
    t.known <- known1;
    size


  let write_blocks t writes = 
    let os = t.outstanding_writes in
    let os' = List.fold_left (fun os (lba,block) -> LbaMap.add lba block os) os writes in
    t.outstanding_writes <- os';
    let _ = _learn t writes in
    
    (* nbd-client sends no flushes *)
    let size = LbaMap.cardinal t.outstanding_writes in
    let max_out = 1024 in (* arbitrary *)
    if size >  max_out
    then
      begin
        log_f "auto-flushing: %i > %i" size max_out >>= fun () ->
        flush t
      end
    else
      Lwt.return () 


      
  let read_blocks t lbas = 
    log_f "Cache : read_blocks lbas=[%s]" (lbas2s lbas) >>= fun ()->
    let k = List.filter (fun l ->      LbaMap.mem l t.known) lbas in
    let u = List.filter (fun l -> not (LbaMap.mem l t.known)) lbas in
    log_f "Cache : k=[%s] u=[%s]" (lbas2s k) (lbas2s u) >>= fun () ->

    let kbs = List.map (fun l -> (l, LbaMap.find l t.known)) k in
    B.read_blocks t.back u >>= fun ubs ->
    let _ = _learn t ubs in
    let r = kbs @ ubs in
    let sr = List.sort (fun (l0,_) (l1,_) -> l0 - l1) r in
    log_f "Cache : sr=[%s]" (lbabs2s sr) >>= fun () ->
    Lwt.return sr
                                      

    

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
