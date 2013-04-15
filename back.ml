type uri = string

module type BACK = sig
  type t

  val create : uri -> t Lwt.t
  val device_size: t -> int

  val read : t -> int -> int -> string Lwt.t

  val write : t -> string -> int -> int -> int -> unit Lwt.t

  val flush : t -> unit Lwt.t
  val disconnect : t -> unit Lwt.t
end
