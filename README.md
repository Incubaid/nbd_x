nbd_x
=====

experimental nbd server:

* building it:
  make


* running the file backend:
  ./nbd.native file:///media/xfs/my_vol

* running the mem backend:
  ./nbd.native mem:///whatever

* running the arakoon backend:
  ./nbd.native -p 9000 arakoon://ricky/arakoon_0#127.0.0.1#4000/arakoon_1#127.0.0.1#4001

  beware, for the moment this one only supports single node arakoon clusters.


