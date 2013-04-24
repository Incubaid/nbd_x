open Lwt
open Back
open Block
module GenericBack(B:BLOCK) = (struct

  type t = { b: B.t}

  let block_size t = B.block_size t.b

  let create uri = 
    log_f "Generic: create %s" uri >>= fun ()->
    B.create uri >>= fun b ->
    Lwt.return { b }


  let read t off dlen = 
    let bs = block_size t in
    let lba = off / bs in
    match off mod bs with
      | 0 -> 
        begin
          let r = String.create dlen in
          let rec loop lba roff =
            if roff >= dlen
            then Lwt.return r
            else
              begin
                B.read_block t.b lba >>= fun block ->
                String.blit block 0 r roff bs;
                loop (lba+1) (roff + bs) 
              end
          in
          loop lba 0
        end
          
      | r when r + dlen < bs -> 
        begin
          let inner = r in
          B.read_block t.b lba >>= fun block ->
          let part = String.sub block inner dlen in
          Lwt.return part
        end
      | r -> Lwt.fail (Failure "case not supported")
        
  let write t buf boff dlen off =
    let bs = block_size t in
    let lba = off / bs in
    match off mod bs with
      | 0 -> 
        (* still 2 cases. *)
        begin 
          if dlen = bs 
          then 
            let block = String.sub buf boff bs in
            B.write_blocks t.b [(lba,block)]
          else if dlen mod bs = 0 
          then (* a multitude of blocks *)
            let too_far = String.length buf in
            let rec loop lba coff = 
              if coff = too_far 
              then Lwt.return ()
              else 
                let block = String.sub buf boff bs in
                B.write_blocks t.b [(lba, block)]  >>= fun () ->
                loop (lba + 1) (coff + bs) 
            in
            loop lba boff
          else Lwt.fail (Failure "(0) case not supported")
        end            
          
      | r -> (* unaligned write *)
        begin
          let bstart = r in
          let blen = min (bs - r) dlen in
          let rec loop lba bstart blen todo =
            log_f "loop %016x %i %i %i" lba bstart blen todo >>= fun () ->
            if todo <= 0 
            then Lwt.return ()
            else 
              begin
                B.read_block t.b lba >>= fun block ->
                let () = String.blit buf boff block bstart blen in
                B.write_blocks t.b [(lba,block)]  >>= fun () ->
                let lba' = lba + 1 in
                let bstart' = 0 in
                let todo' = todo - blen in
                let blen' = min bs todo' 
                in
                loop lba' bstart' blen' todo'
              end
          in
          loop lba bstart blen dlen
        end

  let flush t = 
    B.flush t.b

  let disconnect t = log_f "disconnect%!" >>= fun () ->Lwt.return ()

  let device_size t = 6 * 1024 * 1024 * 1024
    
end : BACK)
