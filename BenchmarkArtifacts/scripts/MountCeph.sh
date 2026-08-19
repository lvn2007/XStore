#!/usr/bin/env bash
set -euo pipefail

MOUNT_POINT="/ceph"
MONS="172.16.6.77,172.16.6.79,172.16.6.81"
FSNAME="mysticfs"
SECRET_FILE="/etc/ceph/MysticCeph.secret"

mkdir -p /etc/ceph/

# Sanity checks
if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must be run as root (use sudo)" >&2
  exit 1
fi

if [[ ! -f "$SECRET_FILE" ]]; then
  echo "ERROR: secret file not found: $SECRET_FILE" >&2
  exit 1
fi

mkdir -p "$MOUNT_POINT"

# Skip if already mounted
if findmnt -T "$MOUNT_POINT" >/dev/null 2>&1; then
 echo "CephFS already mounted at $MOUNT_POINT"
 exit 0
fi

echo "Mounting CephFS ($FSNAME) at $MOUNT_POINT..."

mount -t ceph \
  "$MONS:/" \
  "$MOUNT_POINT" \
  -o "fs=$FSNAME,name=$FSNAME,secretfile=$SECRET_FILE"

echo "Mount successful."

# Verification
findmnt -T "$MOUNT_POINT"
df -h "$MOUNT_POINT"