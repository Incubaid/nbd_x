# nbd_x #

experimental nbd server:

* building it:
  make


* running the file backend:
  ./nbd.native file:///media/xfs/my_vol

* running the mem backend:
  ./nbd.native mem:///whatever

* running the arakoon backend:
  ./nbd.native -p 9000 arakoon://ricky/vol0/arakoon_0#127.0.0.1#4000/arakoon_1#127.0.0.1#4001



# a complete scenario: #

## server side: ##

### create a volume file ###

    $> dd if=/dev/zero of=./my_vol bs=1024 count=6MB
    6000000+0 records in
    6000000+0 records out
    6144000000 bytes (6,1 GB) copied, 23,3209 s, 263 MB/s

### put a file system on it ###
    $> mke2fs ./my_vol
    mke2fs 1.42.8 (20-Jun-2013)
    ./my_vol is not a block special device.
    Proceed anyway? (y,n) y
    Discarding device blocks: done
    Filesystem label=
    OS type: Linux
    Block size=4096 (log=2)
    Fragment size=4096 (log=2)
    Stride=0 blocks, Stripe width=0 blocks
    375360 inodes, 1500000 blocks
    75000 blocks (5.00%) reserved for the super user
    First data block=0
    Maximum filesystem blocks=1539309568
    46 block groups
    32768 blocks per group, 32768 fragments per group
    8160 inodes per group
    Superblock backups stored on blocks:
    32768, 98304, 163840, 229376, 294912, 819200, 884736

    Allocating group tables: done
    Writing inode tables: done
    Writing superblocks and filesystem accounting information: done

### run our nbd server ###
    $> ./nbd.native -p 9000 file://.my_vol


## client side: ##

### connect the block device ###

    $> nbd-client 127.0.0.1 9000 /dev/nbd0 -persist
    Negotiation: ..size = 5859MB
    bs=1024, sz=6144000000 bytes

### mount the volume ###

    $> mount /dev/nbd0 /mnt/my_vol/
    $> ...

### finish up ###

    $> umount /mnt/my_vol
    $> nbd-client -d /dev/nbd0
    Disconnecting: que, disconnect, sock, done
