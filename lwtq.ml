open Lwt
module Q = struct
  type 'a t = { empty_m: Lwt_mutex.t;
                full_m : Lwt_mutex.t;
                empty: unit Lwt_condition.t;
                full : unit Lwt_condition.t;
                q : 'a Queue.t;
              }

  let is_full t = Queue.length t.q >= 256

  let create () = 
    { empty_m = Lwt_mutex.create();
      full_m = Lwt_mutex.create ();
      empty = Lwt_condition.create () ;
      full = Lwt_condition.create ();
      q = Queue.create ();
    }

  let length t = Queue.length t.q

  let push t e =
    let _add e =
      let () = Queue.add e t.q in
      let () = Lwt_condition.signal t.empty () in
      Lwt.return () 
    in 
    Lwt_mutex.with_lock t.full_m
      (fun () ->
        begin
          if is_full t 
          then 
            Lwt_condition.wait t.full 
          else
            Lwt.return () 
        end >>= fun () ->
        _add e
      )
      
  let pull t = 
    Lwt_mutex.with_lock t.empty_m
      (fun () ->
        (if Queue.is_empty t.q
        then Lwt_condition.wait t.empty
        else Lwt.return () )
        >>= fun () ->
        let e = Queue.take t.q in
        let () = Lwt_condition.signal t.full () in
        Lwt.return e
      )
end
