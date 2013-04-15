open Lwt
open Back

module ArBack = (struct
  let log_f x = Lwt_io.printlf x

  type t = { 
    sa : Unix.sockaddr ; 
    mc : Arakoon_client.client;
  }

  let create uri = 
    (* arakoon://host:port *) 
    let host,port,cluster_id = Scanf.sscanf uri "arakoon://%s@:%i/%s@" (fun host port cid-> (host,port,cid)) in
    let ia = Unix.inet_addr_of_string host in
    let sa = Unix.ADDR_INET (ia, port) in
    Lwt_io.open_connection sa >>= fun conn ->
    Arakoon_remote_client.make_remote_client cluster_id conn >>= fun client ->
    let t = {sa ; mc = client; } in
    Lwt.return t

  let block_size  = 0x00001000 
  let block_mask  = 0xffffe000

  let make_key lba = Printf.sprintf "%016x" lba 

  let read_block t lba = 
    let key = make_key lba in
    Lwt.catch
      (fun () -> 
        t.mc # get key >>=fun block ->
        assert (String.length block = block_size);
        Lwt.return block
      )
      (function 
        | Arakoon_exc.Exception(Arakoon_exc.E_NOT_FOUND,_ )->
          let r = String.make block_size '\x00' in
          Lwt.return r
        | e -> Lwt.fail e
      )

  let write_block t lba buf boff = 
    let key = make_key lba in    
    let value = String.sub buf boff block_size in
    t.mc # set key value >>= fun () ->
    Lwt.return t


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
                read_block t lba >>= fun block ->
                String.blit block 0 r roff block_size;
                loop (lba+1) (roff + block_size) 
              end
          in
          loop lba 0
        end
          
      | r when r + dlen < block_size -> 
        begin
          let inner = r in
          let key = make_key lba in
          Lwt.catch 
            (fun () -> t.mc # get key)
            (function 
              | Arakoon_exc.Exception(Arakoon_exc.E_NOT_FOUND,_) ->
                let r = String.make block_size '\x00' in
                Lwt.return r
              | e -> Lwt.fail e
            ) >>= fun block ->
          let part = String.sub block inner dlen in
          Lwt.return part
        end
      | r ->
        Lwt.fail (Failure "case not supported")
        
  let write t buf boff dlen off =
    log_f "write:%016x:%08x%!" off dlen >>= fun () ->
    let lba = off / block_size in
    match off mod block_size with
      | 0 -> 
        (* still 2 cases. *)
        begin 
          if dlen = block_size 
          then write_block t lba buf boff  
          else if dlen mod block_size = 0 
          then (* a multitude of blocks *)
            let too_far = String.length buf in
            let rec loop t lba coff = 
              if coff = too_far 
              then Lwt.return t
              else 
                write_block t lba buf boff  >>= fun t ->
                loop t (lba + 1) (coff + block_size) 
            in
            loop t lba boff
          else Lwt.fail (Failure "(0) case not supported")
        end            
          
      | r -> (* unaligned write *)
        begin
          let bstart = r in
          let blen = min (block_size - r) dlen in
          let rec loop t lba bstart blen todo =
            log_f "loop %i %i %i %i" lba bstart blen todo >>= fun () ->
            if todo <= 0 
            then Lwt.return t 
            else 
              begin
                read_block t lba >>= fun block ->
                let () = String.blit buf boff block bstart blen in
                write_block t lba block 0  >>= fun t ->
                let lba' = lba + 1 in
                let bstart' = 0 in
                let todo' = todo - blen in
                let blen' = min block_size todo' 
                in
                loop t lba' bstart' blen' todo'
              end
          in
          loop t lba bstart blen dlen
        end



  let disconnect t = Lwt.return ()

  let device_size t = 6 * 1024 * 1024 * 1024
    
end : BACK)
