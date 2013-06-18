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
      
  type instruction = { lba: lba; 
                       b_off : int;
                       result_off: int;
                       b_len : int}

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
        let rec build_ins acc result_off lba b_off b_len todo =
          if todo = 0 
          then List.rev acc
          else
            begin
              let acc' = {lba;result_off;b_off;b_len} :: acc in
              let result_off' = result_off + b_len in
              let lba' = lba + 1 in
              let b_off' = 0 in
              let todo'  = todo - b_len in
              let b_len' = min todo' bs in
              build_ins acc' result_off' lba' b_off' b_len' todo'
            end
        in
        let b_off = off mod bs in
        let b_len = min dlen (bs - b_off) in
        let ins = build_ins [] 0 lba0 b_off b_len dlen in
        let lbas = List.map (fun ins -> ins.lba) ins in
        B.read_blocks t.b lbas >>= fun lbabs ->
        let rec blit ins lbabs =
          match ins, lbabs with
          | [],[] -> ()
          | x :: xs, (lba,block) :: ys -> 
            assert (x.lba = lba);
            let () = String.blit block x.b_off result x.result_off x.b_len in
            blit xs ys
          | _,_ -> failwith "mismatch"
        in
        let () = blit ins lbabs in
        log_f "generic : read dlen=%0x16" dlen >>= fun () ->
        Lwt.return result
      end

  let trim t off dlen = 
    log_f "generic: trim %x %x" off dlen >>= fun () ->
    (* We can't split this into 
       a list of lbas here as we might get
       trim off:0x0 dlen=0x7ffff000
       which is just too many lbas
    *)
    B.trim t.b off dlen


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
    B.write_blocks t.b lbabs >>= fun () ->
    log_f "generic: wrote dlen=%08x" dlen
      

      

  let flush t = B.flush t.b

  let disconnect t = 
    log_f "generic : disconnecting%!" >>= fun () ->
    B.disconnect  t.b >>= fun () ->
    log_f "generic : disconnected%!" >>= fun ()->
    Lwt.return ()

  let device_size t = B.device_size t.b
    
end : BACK)
