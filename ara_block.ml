open Block
open Lwt

let sa2s = function
  | Unix.ADDR_INET (addr, port) ->
    Printf.sprintf "%s:%04i" (Unix.string_of_inet_addr addr) port
  | Unix.ADDR_UNIX s -> Printf.sprintf "ADDR_UNIX %s" s

let lwt_failfmt x = Printf.ksprintf (fun s -> Lwt.fail (Failure s)) x

module C = struct

  type t = {
    cid: string;
    nodes : (string * Unix.sockaddr) list;
     mutable client : Arakoon_client.client option;
   }

  let create cid nodes = {cid;nodes; client = None}

  let connect cid (nid,sa) =
    Lwt_io.open_connection sa ~buffer_size:65536 >>= fun conn ->
    log_f "C: got connection" >>= fun () ->
    Arakoon_remote_client.make_remote_client cid conn >>= fun client ->
    log_f "C: got client " >>= fun () ->
    Lwt.return client

  let with_client t f =
    begin
      match t.client with
      | None ->
        let rec find = function
          | [] -> Lwt.fail (Failure "no client")
          | (nid,sa) :: rest ->
            Lwt.catch
              (fun () ->
                connect t.cid (nid,sa) >>= fun client ->
                client # who_master () >>= fun mo ->
                match mo with
                | None   -> lwt_failfmt "%s says None" nid
                | Some m -> let sa = List.assoc m t.nodes in
                            connect t.cid (m,sa)
              )
              (fun e ->
                let s = sa2s sa in
                log_f "e:%s => %s" s (Printexc.to_string e) >>= fun () ->
                find rest)
        in
        find t.nodes >>= fun c ->
        t.client <- Some c;
        Lwt.return c
    | Some c -> Lwt.return c
    end >>= fun c ->
    f c

end

module ArakoonBlock = (struct
  type t = {c :C.t;
            vol_id: string;
            device_size: int;
            block_size: int;
            zeros : string;
           }


  let make_sa h p =
    let ia = Unix.inet_addr_of_string h in
    Unix.ADDR_INET (ia, p)


  let _fetch c (vol_id:string) (k:string) (def:string) =
    C.with_client c
      (fun client ->
        let key = Printf.sprintf "%s/_%s" vol_id k in
        Lwt.catch
          (fun () -> client # get key)
          (function
          | e -> 
            log_f "couldn't get %s => returning %s" key def >>= fun () ->
            Lwt.return def)
      )
 
  let create uri =
    log_f "ArakoonBlock:create %s" uri >>= fun () ->
    (* arakoon://<cid>/<volid>/node0#127.0.0.1#8080/node1#127.0.0.1#8081/node2#127.0.0.1#8082 *)
    (* This is the part where letting an arbitrary node deliver the whole config would be handy *)
    let cluster_id,vol_id, hpp = Scanf.sscanf uri "arakoon://%s@/%s@/%s%!" 
      (fun cid vid hp -> cid,vid,hp) in
    log_f "vol_id = %s hpp=%s" vol_id hpp >>= fun () ->
    let hpl = Str.split (Str.regexp "/") hpp in
    let hps = List.map (fun hp -> Scanf.sscanf hp "%s@#%s@#%i" (fun n h p -> n,h,p)) hpl in
    Lwt_list.iter_s (fun (n,h,p) ->  log_f "nid:%s host:%s port:%i" n h p) hps >>= fun () ->
    let hpsas = List.map (fun (n,h,p) -> n, make_sa h p) hps in
    let c = C.create cluster_id hpsas in
    let def_device_size = 16 * 1024 * 1024 * 1024 in
    _fetch c vol_id "device_size" (Printf.sprintf "%i" def_device_size) >>= fun dss ->
    let device_size = Scanf.sscanf dss "%i" (fun i -> i) in
    let block_size = 0x00000400 in
    let zeros = String.make block_size '\x00' in
    let t = { c;vol_id;device_size; block_size; zeros } in
    Lwt.return t

  let block_size t = t.block_size 
  let device_size t = t.device_size
  let block_mask  = 0xffffe000

  let make_key t lba = Printf.sprintf "%s/%016x" t.vol_id lba
  let make_hkey t h = Printf.sprintf "%s/_hash/%s" t.vol_id h

  let _write_blocks_direct t writes = 
    log_f "ArakoonBlock: _write_blocks_direct (%i blocks)" (List.length writes) >>= fun () ->
    let seq = List.map
      (fun (lba,block) ->
        let key = make_key t lba in
        Arakoon_client.Set (key,block)) writes
    in
    let f mc = mc # sequence seq in
    C.with_client t.c f

  let _write_blocks_dedupe t writes = 
    log_f "ArakoonBlock : _write_blocks_dedupe (%i blocks)" (List.length writes) >>= fun () ->
    let seq = 
      List.fold_left 
        (fun acc (lba,block) ->
          let key = make_key t lba in
          let hash = Digest.string block in
          let kh = Arakoon_client.Set(key, hash) in
          let hb = Arakoon_client.Set(make_hkey t hash, block)in
          kh :: hb :: acc)
        [] writes
    in
    let f mc = mc # sequence seq in
    C.with_client t.c f


  let _read_blocks_dedupe t lbas = 
    let lbas_size = List.length lbas in
    log_f "ArakoonBlock : _wriate_blocks_dedupe (%i blocks)" lbas_size >>= fun () ->
    let f (mc : Arakoon_client.client) = 
      let keys = List.map (make_key t) lbas in
      mc # multi_get_option keys >>= fun hos ->
      let lh = Hashtbl.create lbas_size in
      let to_fetch = ref [] in
      List.iter2 
        (fun lba ho -> 
          match ho with
          | None -> () 
          | Some h -> 
            let () = Hashtbl.add lh lba h in
            let () = to_fetch := (lba,h) :: !to_fetch in
            ())
        lbas hos;
      let hb = Hashtbl.create lbas_size in
      let hkeys = List.map (fun (_,h) -> make_hkey t h) !to_fetch in
      mc # multi_get_option hkeys >>= fun bos ->
      
      List.iter2 (fun (_,h) bo -> 
        match bo with
        | None   -> () 
        | Some b -> Hashtbl.add hb h b
      ) !to_fetch bos;
      
      let r = List.map (fun lba -> 
        try let h = Hashtbl.find lh lba in
            let b = Hashtbl.find hb h in
            (lba,b)
        with
          Not_found -> (lba, t.zeros)
      ) lbas 
      in
      Lwt.return r
    in
    C.with_client t.c f

  let _read_blocks_direct t lbas = 
    log_f "ArakoonBlock: read_blocks (%i blocks)" (List.length lbas) >>= fun () ->
    let bs = block_size t in
    let keys = List.map (make_key t) lbas in
    let f (mc: Arakoon_client.client) =
      mc # multi_get_option keys >>= fun bos ->
      let blocks = List.map (function 
        | None -> t.zeros 
        | Some b -> assert (String.length b = bs);b) 
        bos 
      in
      let r = List.combine lbas blocks in
      Lwt.return r
    in
    let t0 = Unix.gettimeofday() in
    C.with_client t.c f >>= fun r ->
    let t1 = Unix.gettimeofday () in
    log_f "ArakoonBlock: read_direct took %f" (t1 -. t0) >>= fun () ->
    Lwt.return r

  
          
  let write_blocks = _write_blocks_direct

  let read_blocks = _read_blocks_direct

  let flush t = Lwt.return ()
  let disconnect t = Lwt.return () 
  let trim t off dlen = 
    log_f "ArakoonBlock: trim 0x%0x16x 0x%08x" off dlen >>= fun () ->
    Lwt.return ()

end : BLOCK)
