open Block
open Lwt


module type CACHE = sig
  type t

  val init : unit -> t Lwt.t
  val find:  t -> lba -> (block option ) Lwt.t
  val learn: t -> (lba*block) list -> t Lwt.t

end

module LbaMap = Map.Make(struct
  type t = lba
  let compare (t0:lba) (t1:lba) = t1 - t0
end)


module LbaCache = (struct
  type t = block LbaMap.t
  type k = lba
  type v = block

  let max_size = 4000
  let init () = Lwt.return LbaMap.empty
  let find t k =
    let vo = try Some (LbaMap.find k t) with Not_found -> None in
    Lwt.return vo

  let learn known writes =
    let lbas = List.map (fun (lba,_) -> lba) writes in
    log_f "LbaCache : learning: [%s]" (lbas2s lbas) >>= fun () ->
    (*
    Lwt_list.fold_left_s (fun () (lba,block) ->         
      log_f "%0x: %i" lba (String.length block)) () writes
    >>= fun ()->
    *)
    log_f "verified" >>= fun ()->
    let known0 = List.fold_left
      (fun known (lba,block) ->
        LbaMap.add lba block known) known writes in

    let rec forget dumped known i =
      if i = 0
      then (List.rev dumped), known
      else
        let (k,_) = LbaMap.choose known in
        let dumped' = k :: dumped in
        let known' = LbaMap.remove k known in
        forget dumped' known' (i-1)
    in
    let dumped, known1, size =
      let size = LbaMap.cardinal known0 in
      if size > max_size
      then
        let dumped, known1 = forget [] known0 (size - max_size) in
        dumped, known1, max_size
      else [], known0,size
    in
    log_f "LbaCache : dumped:[%s]" (lbas2s dumped) >>= fun () ->
    Lwt.return known1

end : CACHE)


module CacheBlock (B: BLOCK) = (struct

  type block_map = string LbaMap.t
  type t = {
    back : B.t;
    mutable outstanding_writes: block_map;
    mutable known : LbaCache.t;
  }

  let _max_size = 4096

  let block_size t = B.block_size t.back

  let device_size t = B.device_size t.back

  let create uri =
    B.create uri >>= fun b ->
    LbaCache.init () >>= fun known ->
    let t = {back = b;
             outstanding_writes= LbaMap.empty;
             known;
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
    let t0 = Unix.gettimeofday() in
    LbaCache.learn t.known writes >>= fun known' ->
    let t1 = Unix.gettimeofday() in
    let d= t1 -. t0 in
    log_f "Cache : learning took: %f" d >>= fun ()->

    t.known <- known';
    Lwt.return ()

  let write_blocks t writes =
    let os = t.outstanding_writes in
    let os' = List.fold_left (fun os (lba,block) -> LbaMap.add lba block os) os writes in
    t.outstanding_writes <- os';
    _learn t writes >>= fun () ->
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
    Lwt_list.fold_left_s 
      (fun (k,u) lba ->
        try
          let block = LbaMap.find lba t.outstanding_writes in
          Lwt.return ((lba,block) :: k , u)
        with Not_found ->
          begin
            LbaCache.find t.known lba >>= fun bo ->
            let r = 
              match bo with
              | None       -> (k, lba :: u)
              | Some block -> (lba,block) :: k, u
            in
            Lwt.return r
          end
      ) 
      ([],[]) lbas 
    >>= fun (kbs,u) ->

    let k = List.map (fun (lba,_) -> lba) kbs in
    log_f "Cache : k=[%s] u=[%s]" (lbas2s k) (lbas2s u) >>= fun () ->
    B.read_blocks t.back u >>= fun ubs ->
    _learn t ubs >>= fun ()->
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
