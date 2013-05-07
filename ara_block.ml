open Block

module ArakoonBlock = (struct
  type t = { 
    sa : Unix.sockaddr ; 
    mc : Arakoon_client.client;
  }

  open Lwt

  let create uri = 
    log_f "ArakoonBlock:create %s" uri >>= fun () ->
    (* arakoon://host:port/<cid> *) 
    let host,port,cluster_id = Scanf.sscanf uri "arakoon://%s@:%i/%s" (fun host port cid-> (host,port,cid)) in
    log_f "host:%s port:%i cluster_id:%s" host port cluster_id >>= fun () ->
    let ia = Unix.inet_addr_of_string host in
    let sa = Unix.ADDR_INET (ia, port) in
    Lwt_io.open_connection sa >>= fun conn ->
    log_f "ArakoonBlock: got connection" >>= fun () ->
    Arakoon_remote_client.make_remote_client cluster_id conn >>= fun client ->
    log_f "ArakoonBlock: got client " >>= fun () ->
    let t = {sa ; mc = client; } in
    Lwt.return t

  let block_size t = 0x00001000 
  let block_mask  = 0xffffe000

  let make_key lba = Printf.sprintf "%016x" lba 

  let read_blocks t lbas = 
    log_f "ArakoonBlock: read_blocks [%s]" (String.concat ";" (List.map string_of_int lbas)) >>= fun () ->
    let bs = block_size t in
    let keys = List.map make_key lbas in
    t.mc # multi_get_option keys >>= fun bos ->
    let blocks = List.map (function None -> String.make bs '\x00' | Some b -> b) bos in
    let r = List.combine lbas blocks in
    Lwt.return r
      
  let write_blocks t writes = 
    log_f "ArakoonBlock: write_blocks [%s]" (String.concat ";" (List.map (fun (l,b) -> string_of_int l) writes))
      >>= fun () ->
    let seq = List.map 
      (fun (lba,block) ->
        let key = make_key lba in
        Arakoon_client.Set (key,block)) writes
    in
    t.mc # sequence seq

  let flush t = Lwt.return ()
  let trim_blocks t lbas = Lwt.return ()

end : BLOCK)
