open Lwt
include Tools

type lba = int
type block = string

module type BLOCK = sig

  type t

  val block_size:  t -> int

  val create : string -> t Lwt.t

  val read_blocks  : t -> lba list -> (lba * block) list Lwt.t

  val write_blocks : t -> (lba * string) list -> unit Lwt.t

  val trim : t -> int -> int -> unit Lwt.t
  val flush : t -> unit Lwt.t

  val disconnect : t -> unit Lwt.t
  val device_size: t -> int
end

