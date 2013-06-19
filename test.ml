open Nbd_block
open Nbd_protocol
open Lwt

module N = Old

let read (ic,oc) handle off size = 
  N.write_request oc _NBD_CMD_READ handle off size >>= fun () ->
  N.read_response ic >>= fun response ->
  (* Lwt_io.printlf "response=%s%!" (N.response2s response) >>= fun () -> *)
  let buf = String.create size in
  Lwt_io.read_into_exactly ic buf 0 size >>= fun () ->
  (* Lwt_io.printlf "read %i bytes: %S %!" size buf >>= fun ()-> *)
  Lwt.return buf

let write (ic,oc) handle off s = 
  let size = String.length s in
  N.write_request oc _NBD_CMD_WRITE handle off size >>= fun () ->
  Lwt_io.write oc s >>= fun () ->
  N.read_response ic >>= fun response ->
  (* Lwt_io.printlf "response=%s%!" (N.response2s response) >>= fun () ->  *)
  Lwt.return ()

let disconnect (ic,oc) handle = 
  let size = 0
  and off = 0
  in
  N.write_request oc _NBD_CMD_DISC handle off size >>= fun () ->
  N.read_response ic >>= fun response ->
  Lwt.return ()

let test_superblock (ic,oc) handle = 
  let off = 0 in
  let size = 512 in
  read (ic,oc) handle off size >>= fun buf->
  Lwt.return true

let test_overlap_first_last (ic,oc) handle = 
  let off = 200 in
  let size = 10000 in
  read (ic,oc) handle off size >>= fun buf->
  let r = String.length buf = size in
  Lwt.return r

let test_fragment (ic,oc) handle = 
  let off = 201
  and size = 7 in
  let block = 
    let r = String.create size in
    let rec loop i = 
      if i = size
      then r
      else
        let c = Char.chr (i mod 255 + 65) in
        r.[i] <- c;
        loop (i+1) 
    in
    loop 0
  in
  (* Lwt_io.printlf "block=\n%S\n" block >>= fun () -> *)
  write(ic,oc) handle off block >>= fun () ->
  read(ic,oc) handle off size >>= fun buf ->
  let r = (String.length buf = size) in
  let r' = (buf = block) in
  Lwt.return (r && r')


let test_bad_blocks (ic,oc) handle = 
   (* n = 2^16 + 1 = 65,537 (a Fermat prime F4) and g = 75 *)
  let n = 65537 
  and g = 75 in
  let bs = 1023 in
  let rec fill s i =
    if i = 0 
    then Lwt.return () 
    else
      begin
        begin
          if i mod 10 = 0 
          then Lwt_io.printlf "%i" i 
          else Lwt.return () 
        end
        >>= fun  () ->
        let c0 = Char.chr (s mod 256) in
        let string = String.make bs c0 in
        let off = s * bs in
        write (ic,oc) handle off string >>= fun () ->
        let s' = s * g mod n in
        let i' = i - 1 in
        fill s' i' 
      end
  in
  let rec compare s i = 
    if i = 0 
    then Lwt.return ()
    else  
      let c0 = Char.chr (s mod 256) in
      let off = s * bs in
      read (ic,oc) handle off bs >>= fun string ->
      let r0 = string.[0] in
      let rl = string.[bs -1] in
      Lwt_io.printlf "%8i: %C <-> %C <-> %C %!" i r0 c0 rl >>= fun () ->
      begin
        if r0 <> c0 
        then Lwt.fail (Failure "different") 
        else Lwt.return () 
      end
      >>= fun () ->
      let s' = s * g mod n in
      let i' = i - 1 in
      compare s' i'
  in
  let limit = 15000 in
  fill 1 limit    >>= fun () ->
  compare 1 limit >>= fun () ->
  Lwt.return true
    
let test_disconnect sa handle = 
  Lwt_io.with_connection sa 
    (fun (ic,oc) ->
      N.read_preamble ic >>= fun p ->
      write (ic,oc) handle 0 (String.make 4096 'z') >>= fun () ->
      write (ic,oc) handle 1 (String.make 19 'y') >>= fun () ->
      N.write_request oc _NBD_CMD_DISC handle 0 0 >>= fun () ->
      N.read_response ic >>= fun resp ->
      Lwt_io.printlf "response:%s" (N.response2s resp) >>= fun ()->
      Lwt.return ()
    )
  >>= fun () ->
  Lwt_io.printlf "wrote & disconnected, now part2" >>= fun () ->
  Lwt_io.with_connection sa 
    (fun (ic,oc) ->
      N.read_preamble ic >>= fun p ->
      read(ic,oc) handle 0 4096 >>= fun buf ->
      Lwt_io.printlf "buf:\n%S" buf >>= fun () ->
      Lwt.return ()
    )
    
let run_tests sa handle =
  let tests = 
    [     
      ("test_superblock", test_superblock);
      ("test_overlap_first_last", test_overlap_first_last);
      ("test_fragment", test_fragment);
      ("test_bad_blocks", test_bad_blocks);
    ]
  in
  Lwt_list.iter_s (fun (n,test) ->
    Lwt_io.eprintf "%-40s:%!" n >>= fun () ->
    Lwt_io.with_connection sa 
      (fun (ic,oc) ->
        N.read_preamble ic >>= fun p ->
        test (ic,oc) handle >>= fun r ->
        Lwt_io.eprintlf "\t%b%!" r >>= fun () ->
        disconnect (ic,oc) handle
      ) >>= fun () ->
      Lwt.return ()
    )
    tests

let () =
  let cmd = ref "help" in
  (*let offset = ref 0 in
  let dlen = ref 0 in
  *)
  let args = [
    ("-t", Arg.Unit (fun () -> cmd := "test"), "run tests");
    (* 
       ("-r", Arg.Unit (fun () -> cmd := "read"), "read");
       ("-o", Arg.Set_int offset, "offset");
       ("-l", Arg.Set_int dlen, "length");
    *)
  ]
  in
  let host = "127.0.0.1"
  and port = 9000 
  in
  let usage_msg = 
    Printf.sprintf 
    ("simple test runner,\n" ^^
      "make sure you have an nbd server running on (%s,%i)\n" )
      host port
  in
  let () = Arg.parse args (fun s -> ()) usage_msg in
  match !cmd with
  | "help" -> begin () end
  | "test" -> 
    begin 
      let ia = Unix.inet_addr_of_string host in
      let sa = Unix.ADDR_INET(ia,port) in
      let handle = N.make_handle () in
      Lwt_main.run (run_tests sa handle)
    end

  | _ -> Arg.usage args usage_msg
