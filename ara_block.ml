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
    Lwt_io.open_connection sa >>= fun conn ->
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
  type t = C.t


  let make_sa h p =
    let ia = Unix.inet_addr_of_string h in
    Unix.ADDR_INET (ia, p)


  let create uri =
    log_f "ArakoonBlock:create %s" uri >>= fun () ->
    (* arakoon://<cid>/node0#127.0.0.1#8080/node1#127.0.0.1#8081/node2#127.0.0.1#8082 *)
    (* This is the part where letting an arbitrary node deliver the whole config would be handy *)
    let cluster_id,hpp = Scanf.sscanf uri "arakoon://%s@/%s%!" (fun cid hp -> cid,hp) in
    log_f "hpp=%s" hpp >>= fun () ->
    let hpl = Str.split (Str.regexp "/") hpp in
    let hps = List.map (fun hp -> Scanf.sscanf hp "%s@#%s@#%i" (fun n h p -> n,h,p)) hpl in
    Lwt_list.iter_s (fun (n,h,p) ->  log_f "nid:%s host:%s port:%i" n h p) hps >>= fun () ->
    let hpsas = List.map (fun (n,h,p) -> n, make_sa h p) hps in
    let t = C.create cluster_id hpsas in
    Lwt.return t

  let block_size t = 0x00001000
  let device_size t = (6 * 1024 * 1024)
  let block_mask  = 0xffffe000

  let make_key lba = Printf.sprintf "%016x" lba

  let read_blocks t lbas =
    log_f "ArakoonBlock: read_blocks [%s]" (String.concat ";" (List.map string_of_int lbas)) >>= fun () ->
    let bs = block_size t in
    let keys = List.map make_key lbas in
    let f (mc: Arakoon_client.client) =
      mc # multi_get_option keys >>= fun bos ->
      let blocks = List.map (function None -> String.make bs '\x00' | Some b -> b) bos in
      let r = List.combine lbas blocks in
      Lwt.return r
    in
    C.with_client t f

  let write_blocks t writes =
    log_f "ArakoonBlock: write_blocks [%s]" (String.concat ";" (List.map (fun (l,b) -> string_of_int l) writes))
    >>= fun () ->
    let seq = List.map
      (fun (lba,block) ->
        let key = make_key lba in
        Arakoon_client.Set (key,block)) writes
    in
    let f mc = mc # sequence seq in
    C.with_client t f

  let flush t = Lwt.return ()
  let trim_blocks t lbas = Lwt.return ()

end : BLOCK)
