open Lwt
open Back
open Block
module GenericBack(B:BLOCK) = (struct

  type t = { b: B.t}

  let block_size t = B.block_size t.b

  let create uri = 
    log_f "generic: create %s" uri >>= fun ()->
    B.create uri >>= fun b ->
    log_f "created generic" >>= fun () ->
    Lwt.return { b }

  let _read_block t lba = 
    B.read_blocks t.b [lba] >>= fun lbabs ->
    let _,block = List.hd lbabs in
    Lwt.return block
        

  let read t off dlen = 
    if dlen = 0 
    then
      log_f "empty read ?!" >>= fun () ->
      Lwt.return ""
    else
      begin
        let bs = block_size t in
        let lba0 = off / bs in
        let result = String.create dlen in
        let rec loop result_off lba b_off b_len todo =
          if todo = 0 
          then Lwt.return result
          else
            begin
              _read_block t lba >>= fun block ->
              let () = String.blit block b_off result result_off b_len in
              let result_off' = result_off + b_len in
              let lba' = lba + 1 in
              let b_off' = 0 in
              let todo'  = todo - b_len in
              let b_len' = min todo' bs in
              loop result_off' lba' b_off' b_len' todo'
            end
        in
        let b_off = off mod bs in
        let b_len = min dlen (bs - b_off) in
        loop 0 lba0 b_off b_len dlen 
      end

  let trim t off dlen = 
    log_f "generic: trim %x %x" off dlen >>= fun () ->
    (* This doesn't really work as trim sometimes does things like 
       trim off:0x0 dlen=0x7ffff000
       which is a lot of lbas ...
    *)
    failwith "TODO"

  let rec write t buf boff dlen off =
    let bs = block_size t in
    let lba0 = off / bs in
    let rec loop lbabs lba boff block_off block_len todo = 
      if todo = 0 
      then Lwt.return lbabs
      else
        begin
          begin
            if block_off = 0 && block_len = bs
            then Lwt.return (lba, String.sub buf boff block_len)
            else
              begin
                _read_block t lba >>= fun block ->
                String.blit buf boff block block_off block_len;
                Lwt.return (lba, block)
              end
          end >>= fun (lba, block) ->
          let lbabs' = (lba,block) :: lbabs in
          let lba' = lba + 1 in
          let boff' = boff + block_len in
          let block_off' = 0 in
          let todo' = todo - block_len in
          let block_len' = min bs todo' in
          loop lbabs' lba' boff' block_off' block_len' todo'
        end
    in
    let block_off = off mod bs in
    let block_len = min (bs - block_off) dlen in
    loop [] lba0 boff block_off block_len dlen >>= fun lbabs ->
    B.write_blocks t.b lbabs 

      

  let flush t = B.flush t.b

  let disconnect t = 
    B.disconnect  t.b >>= fun () ->
    Lwt.return ()

  let device_size t = B.device_size t.b
    
end : BACK)
