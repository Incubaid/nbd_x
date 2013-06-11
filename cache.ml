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
  
  let _max_size = 4096

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
    let lbas = List.map (fun (lba,_) -> lba) writes in
    log_f "learning: [%s]" (lbas2s lbas) >>= fun () ->
    let known = t.known in
    let bs = block_size t in
    let known0 = List.fold_left 
      (fun known (lba,block) -> 
        assert (String.length block = bs);
        LbaMap.add lba block known) known writes in
    let rec forget dumped known i =
      if i = 0 
      then dumped, known
      else 
        let (k,_) = LbaMap.choose known in
        let dumped' = k :: dumped in
        let known' = LbaMap.remove k known in
        forget dumped' known' (i-1)
    in
    let dumped, known1, size = 
      let size = LbaMap.cardinal known0 in
      if size > _max_size 
      then 
        let dumped, known1 = forget [] known0 (size - _max_size) in 
        dumped, known1, _max_size
      else [], known0,size
    in
    log_f "dumped:[%s]" (lbas2s dumped) >>= fun () ->
    t.known <- known1;
    Lwt.return size


  let write_blocks t writes = 
    let os = t.outstanding_writes in
    let os' = List.fold_left (fun os (lba,block) -> LbaMap.add lba block os) os writes in
    t.outstanding_writes <- os';
    _learn t writes >>= fun _ ->
    
    (* nbd-client sends no flushes *)
    let size = LbaMap.cardinal t.outstanding_writes in
    let max_out = 512 in (* arbitrary, but 1024 seems too high for nbd-verify *)
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
    let kbs, u = List.fold_left (fun (k,u) lba -> 
      if LbaMap.mem lba t.outstanding_writes
      then 
        let block = LbaMap.find lba t.outstanding_writes in
        (lba,block) :: k , u
      else if LbaMap.mem lba t.known 
      then
        let block = LbaMap.find lba t.known in
        (lba,block) :: k , u
      else
        k, lba :: u) ([],[]) lbas
    in
    let k = List.map (fun (lba,_) -> lba) kbs in
    log_f "Cache : k=[%s] u=[%s]" (lbas2s k) (lbas2s u) >>= fun () ->
    B.read_blocks t.back u >>= fun ubs ->
    let _ = _learn t ubs in
    let r = kbs @ ubs in
    let sr = List.sort (fun (l0,_) (l1,_) -> l0 - l1) r in
    log_f "Cache : sr=[%s]" (lbabs2s sr) >>= fun () ->
    Lwt.return sr
                                      

    

  let disconnect t = 
    log_f "Cache : disconnect" >>= fun () ->
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
        
  let trim t off dlen = 
    log_f "Cache : trim 0x%0x16x 0x%08x" off dlen >>= fun () ->
    Lwt.return () 

end: BLOCK)
