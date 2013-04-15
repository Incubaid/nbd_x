nbd_x
=====

experimental nbd server:

* building it:
  ocamlbuild -use-ocamlfind nbd.native


* running the file backend:
  ./nbd.native file:///media/xfs/my_vol

* running the mem backend:
  ./nbd.native mem:///whatever

* running the arakoon backend:
  ./nbd.native "arakoon://127.0.0.1:4000/ricky"

  beware, for the moment this one only supports single node arakoon clusters.


