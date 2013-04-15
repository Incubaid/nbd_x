open Lwt
open Back
open Block
module GenericBack(B:BLOCK) = (struct

  let log_f x = Lwt_io.printlf x
  type t = { b: B.t}

  let block_size = B.block_size

  let create uri = 
    log_f "Generic: create %s" uri >>= fun ()->
    B.create uri >>= fun b ->
    Lwt.return { b } 

  let read t off dlen = 
    (* log_f "read:%016x:%08x%!" off dlen >>=fun () -> *)
    let lba = off / block_size in
    match off mod block_size with
      | 0                            -> 
        begin
          let r = String.create dlen in
          let rec loop lba roff =
            if roff >= dlen
            then Lwt.return r
            else
              begin
                B.read_block t.b lba >>= fun block ->
                String.blit block 0 r roff block_size;
                loop (lba+1) (roff + block_size) 
              end
          in
          loop lba 0
        end
          
      | r when r + dlen < block_size -> 
        begin
          let inner = r in
          B.read_block t.b lba >>= fun block ->
          let part = String.sub block inner dlen in
          Lwt.return part
        end
      | r ->
        Lwt.fail (Failure "case not supported")
        
  let write t buf boff dlen off =
    (* log_f "write:%016x:%08x%!" off dlen >>= fun () -> *)
    let lba = off / block_size in
    match off mod block_size with
      | 0 -> 
        (* still 2 cases. *)
        begin 
          if dlen = block_size 
          then B.write_block t.b lba buf boff  
          else if dlen mod block_size = 0 
          then (* a multitude of blocks *)
            let too_far = String.length buf in
            let rec loop lba coff = 
              if coff = too_far 
              then Lwt.return ()
              else 
                B.write_block t.b lba buf boff  >>= fun () ->
                loop (lba + 1) (coff + block_size) 
            in
            loop lba boff
          else Lwt.fail (Failure "(0) case not supported")
        end            
          
      | r -> (* unaligned write *)
        begin
          let bstart = r in
          let blen = min (block_size - r) dlen in
          let rec loop lba bstart blen todo =
            log_f "loop %i %i %i %i" lba bstart blen todo >>= fun () ->
            if todo <= 0 
            then Lwt.return ()
            else 
              begin
                B.read_block t.b lba >>= fun block ->
                let () = String.blit buf boff block bstart blen in
                B.write_block t.b lba block 0  >>= fun () ->
                let lba' = lba + 1 in
                let bstart' = 0 in
                let todo' = todo - blen in
                let blen' = min block_size todo' 
                in
                loop lba' bstart' blen' todo'
              end
          in
          loop lba bstart blen dlen
        end

  let flush t = B.flush t.b

  let disconnect t = Lwt.return ()

  let device_size t = 6 * 1024 * 1024 * 1024
    
end : BACK)
