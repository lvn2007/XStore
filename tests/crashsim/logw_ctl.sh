#!/bin/bash
# Mystic-side controller for crash-replay runs (dm-log-writes). Needs root (called through sudo).
#   logw_ctl.sh setup            6 GB ext4 image -> loop -> dm-log-writes (log on a 40 GB image) mounted at /mnt/xsfault;
#                                base.img = the image before any logged write
#   logw_ctl.sh finish           sync, umount, remove the dm device and both loops (the log is complete afterwards)
#   logw_ctl.sh mountimg <img>   loop-attach a replayed image, fsck -fy, mount at /mnt/xsfault
#   logw_ctl.sh umountimg        umount and detach that loop
#   logw_ctl.sh teardown         idempotent: umount, remove dm + loops (does not delete the images)
# Everything lives under /NVME_RAID-0/faultimg and /mnt/xsfault. Never kills anything.
D=/NVME_RAID-0/faultimg/logw; MNT=/mnt/xsfault; DM=xslog
unmount() { mountpoint -q $MNT || return 0; umount $MNT 2>/dev/null && return 0; sleep 3; umount $MNT 2>/dev/null; mountpoint -q $MNT && { echo "unmount-failed (busy)"; return 1; }; return 0; }
detach() { for f in "$@"; do [ -f $D/$f ] && { losetup -d "$(cat $D/$f)" 2>/dev/null; rm -f $D/$f; }; done; }
case "$1" in
setup)
  modprobe dm_log_writes || exit 1
  mkdir -p $D $MNT; rm -f $D/data.img $D/log.img $D/base.img
  truncate -s 6G $D/data.img; truncate -s 40G $D/log.img
  mkfs.ext4 -q -F $D/data.img || exit 1
  cp --sparse=always $D/data.img $D/base.img
  LD=$(losetup -f --show $D/data.img); echo $LD > $D/loop_data
  LL=$(losetup -f --show $D/log.img); echo $LL > $D/loop_log
  SECT=$(blockdev --getsz $LD)
  dmsetup create $DM --table "0 $SECT log-writes $LD $LL" || exit 1
  mount /dev/mapper/$DM $MNT || exit 1
  chown ubuntu:ubuntu $MNT; echo setup-ok ;;
finish)
  sync; unmount || exit 1
  dmsetup info $DM >/dev/null 2>&1 && { dmsetup remove $DM 2>/dev/null || { sleep 2; dmsetup remove $DM; }; }
  detach loop_data loop_log; echo finish-ok ;;
mountimg)
  unmount || exit 1
  L=$(losetup -f --show "$2"); echo $L > $D/loop_img
  fsck.ext4 -fy $L 2>&1 | tail -3; echo "fsck_rc=${PIPESTATUS[0]}"
  mount $L $MNT && chown ubuntu:ubuntu $MNT && echo mount-ok || { echo mount-failed; exit 1; } ;;
umountimg)
  unmount || exit 1
  detach loop_img; echo umountimg-ok ;;
teardown)
  unmount || { echo "teardown-aborted: still mounted"; exit 1; }
  dmsetup info $DM >/dev/null 2>&1 && { dmsetup remove $DM 2>/dev/null || { sleep 2; dmsetup remove $DM; }; }
  detach loop_img loop_data loop_log; echo teardown-ok ;;
*) echo "usage: $0 setup|finish|mountimg <img>|umountimg|teardown"; exit 2 ;;
esac
