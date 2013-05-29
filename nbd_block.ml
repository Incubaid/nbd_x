open Block
open Lwt

module NBDBlock = (struct

  type t = { ic: Lwt_io.input Lwt_io.channel;
             oc: Lwt_io.output Lwt_io.channel;
             device_size: int;
             handle : string;
           }

  let create uri = 
    let host,port = Scanf.sscanf uri "nbd://%s@:%i" (fun host port -> (host,port)) in
    log_f "NBDBlock: forwarding to %s:%i" host port >>= fun () ->
    let handle = Nbd_protocol.make_handle () in
    let ia = Unix.inet_addr_of_string host in
    let sa = Unix.ADDR_INET (ia, port) in
    Lwt_io.open_connection ~buffer_size:8192 sa >>= fun (ic,oc) ->
    let ps = 16 + 8 + 128 in
    let p = String.create ps in
    Lwt_io.read_into_exactly ic p 0 ps >>= fun () ->
    let input = P.make_input p 0 in
    let magic = P.input_raw input 16 in
    assert (magic = Nbd_protocol._MAGIC);
    let device_size = P.input_uint64 input in
    let t = {ic;oc;device_size; handle} in
    Lwt.return t 

  let block_size t = 4096 
  let device_size t = t.device_size

  let flush t = 
    Nbd_protocol.write_request t.oc Nbd_protocol._FLUSH t.handle 0 0 >>= fun () ->
    Nbd_protocol.read_response t.ic >>= fun resp ->
    Lwt.return ()


  let write_blocks t writes = 
    let bs = block_size t in
    let do_one (lba,block) = 
      let off = lba * bs in
      Nbd_protocol.write_request t.oc Nbd_protocol._WRITE t.handle off bs >>= fun () ->
      Lwt_io.write t.oc block >>= fun () ->
      Nbd_protocol.read_response t.ic >>= fun response ->
      Lwt.return () 
    in
    Lwt_list.iter_s do_one writes 


  let read_blocks t lbas = 
    let bs = block_size t in
    let dlen = bs in
    let read_block lba = 
      let off = lba * bs in
      Nbd_protocol.write_request t.oc Nbd_protocol._READ t.handle off dlen >>= fun () ->
      Nbd_protocol.read_response t.ic >>= fun response ->
      assert (Nbd_protocol.handle response = t.handle);
      let rc = Nbd_protocol.rc response in
      if rc = 0
      then
        let block = String.create dlen in
        Lwt_io.read_into_exactly t.ic block 0 dlen >>= fun () ->
        Lwt.return block
      else
        Lwt.fail (Failure (Printf.sprintf "rc=%i" rc))
    in
    Lwt_list.map_s (fun lba -> read_block lba >>= fun block -> Lwt.return (lba,block)) lbas


  let trim_blocks t lbas = 
    log_f "trim_blocks [%s]" (lbas2s lbas) >>= fun () ->
    (* ... *)
    Lwt.return ()
    
  let disconnect t = 
    Nbd_protocol.write_request t.oc Nbd_protocol._DISCONNECT t.handle 0 0>>= fun () ->
    Lwt_io.close t.oc >>= fun () ->
    Lwt_io.close t.ic 

    
end :BLOCK)
