#!/bin/bash
# Mystic-side controller for the I/O-error injection runs. Needs root (called through sudo).
#   fault_ctl.sh setup "<mount opts>"   8 GB loop file -> dm-linear -> ext4 mounted at /mnt/xsfault
#   fault_ctl.sh error                  every I/O on the device fails (dm error target)
#   fault_ctl.sh linear                 device healthy again
#   fault_ctl.sh fsck "<mount opts>"    umount, fsck -fy, mount again
#   fault_ctl.sh teardown               idempotent: umount, remove the dm device and the loop device and the image
# Everything lives under /NVME_RAID-0/faultimg and /mnt/xsfault; nothing else on the host is touched.
D=/NVME_RAID-0/faultimg; IMG=$D/disk.img; MNT=/mnt/xsfault; DM=xsfault
opts() { [ -n "${1:-}" ] && echo "-o $1"; }
# Never kills anything: the caller stops the server first. Returns non-zero (and prints) if the fs stays busy.
# Table swap without flushing queued I/O into a failing target (the plain load+resume fails with EIO then).
swap() { dmsetup suspend --noflush --nolockfs $DM && dmsetup load $DM --table "$1" && dmsetup resume $DM || echo "swap-failed"; }
unmount() { mountpoint -q $MNT || return 0; umount $MNT 2>/dev/null && return 0; sleep 3; umount $MNT 2>/dev/null; mountpoint -q $MNT && { echo "unmount-failed (busy)"; return 1; }; return 0; }
case "$1" in
setup)
  mkdir -p $D $MNT; rm -f $IMG; truncate -s 8G $IMG
  LOOP=$(losetup -f --show $IMG); echo $LOOP > $D/loop
  SECT=$(blockdev --getsz $LOOP); echo $SECT > $D/sect
  dmsetup create $DM --table "0 $SECT linear $LOOP 0" || exit 1
  mkfs.ext4 -q -F /dev/mapper/$DM || exit 1
  mount $(opts "${2:-}") /dev/mapper/$DM $MNT || exit 1
  chown ubuntu:ubuntu $MNT; echo setup-ok ;;
error)
  swap "0 $(cat $D/sect) error"; date +%s%3N ;;
linear)
  swap "0 $(cat $D/sect) linear $(cat $D/loop) 0"; date +%s%3N ;;
fsck)
  unmount || { echo "fsck-skipped fsck_rc=99"; exit 1; }
  fsck.ext4 -fy /dev/mapper/$DM 2>&1 | tail -6; echo "fsck_rc=${PIPESTATUS[0]}"
  mount $(opts "${2:-}") /dev/mapper/$DM $MNT && chown ubuntu:ubuntu $MNT && echo remount-ok || { echo remount-failed; exit 1; } ;;
teardown)
  unmount || { echo "teardown-aborted: still mounted"; exit 1; }
  dmsetup info $DM >/dev/null 2>&1 && { dmsetup remove $DM 2>/dev/null || { sleep 2; dmsetup remove $DM; }; }
  [ -f $D/loop ] && losetup -d "$(cat $D/loop)" 2>/dev/null
  rm -f $IMG $D/loop $D/sect; rmdir $MNT 2>/dev/null; echo teardown-ok ;;
*) echo "usage: $0 setup|error|linear|fsck|teardown"; exit 2 ;;
esac
