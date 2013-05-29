open Lwt
open Back
open Block
module GenericBack(B:BLOCK) = (struct

  type t = { b: B.t}

  let block_size t = B.block_size t.b

  let create uri = 
    log_f "generic: create %s" uri >>= fun ()->
    B.create uri >>= fun b ->
    Lwt.return { b }

  let _lbas_of bs lba0 off dlen = 
    let rec loop rlbas count lba roff = 
      if roff >= dlen 
      then List.rev rlbas, count
      else loop (lba :: rlbas) (count + 1) (lba + 1) (roff + bs)
    in
    loop [] 0 lba0 0

  let read t off dlen = 
    log_f "generic: read off:0x%016x dlen=0x%04x%!" off dlen >>= fun () ->
    if dlen = 0 
    then
      log_f "empty read ?!" >>= fun () ->
      Lwt.return ""
    else
      begin
        let bs = block_size t in
        let lba0 = (off + bs -1) / bs in
        let lbas , c = _lbas_of bs lba0 off dlen in
        log_f "lbas=[%s] c=%i%!" (lbas2s lbas) c >>= fun () ->
        B.read_blocks t.b lbas >>= fun lbabs ->
        let r = String.create dlen in
        let () = 
          List.iter (fun (l, b) ->
            let pos_0 = (l - lba0) * bs in
            let pos_1 = pos_0 + bs in
            let len = 
              if pos_1 <= dlen 
              then bs
              else dlen - pos_0
            in
            String.blit b 0 r pos_0 len) lbabs
        in
        Lwt.return r
      end

  let trim t off dlen = 
    log_f "generic: trim %x %x" off dlen >>= fun () ->
    let bs = block_size t in
    let lba0 = off / bs in
    let lbas, count = _lbas_of bs lba0 off dlen in
    (* This doesn't really work as trim sometimes does things like 
       trim off:0x0 dlen=0x7ffff000
       which is a lot of lbas ...
    *)
    B.trim_blocks t.b lbas >>= fun () ->
    Lwt.return ()

  let write t buf boff dlen off =
    let bs = block_size t in
    let lba = off / bs in
    match off mod bs with
      | 0 -> 
        (* still 2 cases. *)
        begin 
          if dlen mod bs = 0 
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
            log_f "generic: loop %016x bstart:0x%04x blen:0x%04x todo:0x%04x" lba bstart blen todo >>= fun () ->
            if todo <= 0 
            then Lwt.return ()
            else 
              begin
                B.read_blocks t.b [lba] >>= fun lbabs ->
                let (_,block) = List.hd lbabs in
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

  let flush t = B.flush t.b

  let disconnect t = 
    log_f "disconnect%!" >>= fun () ->
    B.disconnect  t.b >>= fun () ->
    Lwt.return ()

  let device_size t = B.device_size t.b
    
end : BACK)
