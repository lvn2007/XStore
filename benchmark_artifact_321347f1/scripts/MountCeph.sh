#!/usr/bin/env bash
#
# mount-cephfs.sh — bootstrap and mount a CephFS filesystem (kernel client).
#
#   - installs ceph-common if the mount helper is missing (apt/dnf/yum/zypper)
#   - validates the secret key, mon reachability, and kernel client support
#   - mounts idempotently with retries, then verifies it is actually usable
#   - optional persistence across reboots, optional ceph-fuse fallback
#
set -euo pipefail

# ── configuration (env-overridable) ─────────────────────────────────────────
MOUNT_POINT="${CEPH_MOUNT_POINT:-/ceph}"
MONS="${CEPH_MONS:-172.16.6.77,172.16.6.79,172.16.6.81}"
FSNAME="${CEPH_FSNAME:-mysticfs}"
CLIENT_NAME="${CEPH_CLIENT_NAME:-$FSNAME}"          # client.<name>, no "client." prefix
SECRET_FILE="${CEPH_SECRET_FILE:-/etc/ceph/MysticCeph.secret}"
EXTRA_OPTS="${CEPH_MOUNT_OPTS:-noatime}"
MON_PORT="${CEPH_MON_PORT:-3300}"                   # 3300 = msgr2, 6789 = msgr1
CEPH_RELEASE="${CEPH_RELEASE:-}"                    # quincy | reef | squid ...
CEPH_CONF="/etc/ceph/ceph.conf"
LOCK_FILE="/run/lock/mount-cephfs.lock"

DRY_RUN=0; PERSIST=0; UNMOUNT=0; ADD_REPO=0; USE_FUSE=0; PKG=""

# ── logging ─────────────────────────────────────────────────────────────────
log()  { printf '%s %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*" >&2; }
info() { log "INFO  $*"; }
warn() { log "WARN  $*"; }
die()  { log "ERROR $*"; exit 1; }
run()  { if [[ $DRY_RUN == 1 ]]; then printf '[dry-run] %s\n' "$*" >&2; return 0; fi
         "$@" || return $?; }

usage() {
  cat >&2 <<'EOF'
Usage: sudo mount-cephfs.sh [options]
  --persist          add an fstab entry so the mount survives reboot
  --unmount          unmount MOUNT_POINT and exit
  --dry-run          print actions without executing them
  --add-repo REL     add the upstream Ceph repo (quincy|reef|squid) before install
  --fuse             use ceph-fuse instead of the kernel client
  --mon-port N       3300 (msgr2, default) or 6789 (msgr1)
  --help             this message
Env overrides: CEPH_MOUNT_POINT CEPH_MONS CEPH_FSNAME CEPH_CLIENT_NAME
               CEPH_SECRET_FILE CEPH_MOUNT_OPTS CEPH_MON_PORT CEPH_RELEASE
EOF
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --persist)   PERSIST=1 ;;
    --unmount)   UNMOUNT=1 ;;
    --dry-run)   DRY_RUN=1 ;;
    --add-repo)  ADD_REPO=1; CEPH_RELEASE="${2:?missing release}"; shift ;;
    --fuse)      USE_FUSE=1 ;;
    --mon-port)  MON_PORT="${2:?missing port}"; shift ;;
    -h|--help)   usage ;;
    *) die "unknown option: $1 (try --help)" ;;
  esac
  shift
done

# ── preflight: root + single instance ───────────────────────────────────────
[[ $EUID -eq 0 ]] || die "must be run as root (use sudo)"
mkdir -p /run/lock /etc/ceph
exec 9>"$LOCK_FILE"
flock -n 9 || die "another instance is already running"

# ── package manager detection ───────────────────────────────────────────────
detect_pkg_mgr() {
  local id="" like=""
  if [[ -r /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release; id="${ID:-}"; like="${ID_LIKE:-}"
  fi
  case "$id $like" in
    *debian*|*ubuntu*) PKG=apt ;;
    *rhel*|*fedora*|*centos*)
      if command -v dnf >/dev/null 2>&1; then PKG=dnf; else PKG=yum; fi ;;
    *suse*) PKG=zypper ;;
    *)
      if   command -v apt-get >/dev/null 2>&1; then PKG=apt
      elif command -v dnf     >/dev/null 2>&1; then PKG=dnf
      elif command -v yum     >/dev/null 2>&1; then PKG=yum
      elif command -v zypper  >/dev/null 2>&1; then PKG=zypper
      else PKG="" ; fi ;;
  esac
}

find_mount_helper() {
  local c
  for c in /usr/sbin/mount.ceph /sbin/mount.ceph; do
    [[ -x $c ]] && { printf '%s\n' "$c"; return 0; }
  done
  command -v mount.ceph >/dev/null 2>&1 && { command -v mount.ceph; return 0; }
  return 1
}

add_ceph_repo() {
  [[ -n $CEPH_RELEASE ]] || die "--add-repo needs a release codename (quincy|reef|squid)"
  info "Adding upstream Ceph repo for '$CEPH_RELEASE'"
  case $PKG in
    apt)
      command -v curl >/dev/null 2>&1 || run apt-get install -y curl
      install -d -m 0755 /etc/apt/keyrings
      run bash -c "curl -fsSL https://download.ceph.com/debian-$CEPH_RELEASE/keys/automated-sign.key \
                   | gpg --dearmor -o /etc/apt/keyrings/ceph.gpg"
      local codename; codename=$(. /etc/os-release; echo "${VERSION_CODENAME:-focal}")
      run tee /etc/apt/sources.list.d/ceph.list <<EOF
deb [signed-by=/etc/apt/keyrings/ceph.gpg] https://download.ceph.com/debian-$CEPH_RELEASE $codename main
EOF
      ;;
    dnf|yum)
      local rel="el${VERSION_ID%%.*}"
      run tee /etc/yum.repos.d/ceph.repo <<EOF
[ceph]
name=Ceph \$releasever \$basearch
baseurl=https://download.ceph.com/rpm-$CEPH_RELEASE/$rel/\$basearch
enabled=1
gpgcheck=1
gpgkey=https://download.ceph.com/keys/release.asc
EOF
      ;;
    *) warn "no repo bootstrap for '$PKG'; expecting ceph-common to be available" ;;
  esac
}

ensure_packages() {
  local want=(ceph-common)
  if [[ $USE_FUSE == 1 ]]; then want=(ceph-fuse); fi

  if [[ $USE_FUSE == 0 ]] && find_mount_helper >/dev/null 2>&1; then
    info "Ceph mount helper found: $(find_mount_helper)"
    return 0
  fi
  if [[ $USE_FUSE == 1 ]] && command -v ceph-fuse >/dev/null 2>&1; then
    info "ceph-fuse already installed"; return 0
  fi

  detect_pkg_mgr
  [[ -n $PKG ]] || die "unsupported distro — install '${want[*]}' manually"
  [[ $ADD_REPO == 1 ]] && add_ceph_repo

  info "Installing ${want[*]} via $PKG ..."
  case $PKG in
    apt)  run env DEBIAN_FRONTEND=noninteractive apt-get update -y
          run env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${want[@]}" ;;
    dnf)  run dnf -y install "${want[@]}" ;;
    yum)  run yum -y install "${want[@]}" ;;
    zypper) run zypper --non-interactive install "${want[@]}" ;;
  esac

  if [[ $USE_FUSE == 0 ]] && ! find_mount_helper >/dev/null 2>&1; then
    die "install completed but mount.ceph is still missing — check repo/version"
  fi
  info "Installed: $(ceph --version 2>/dev/null | head -1 || echo 'version unknown')"
}

# ── validation ──────────────────────────────────────────────────────────────
validate_secret() {
  [[ -f $SECRET_FILE ]] || die "secret file not found: $SECRET_FILE
  export it from the cluster: ceph auth get client.$CLIENT_NAME -o $SECRET_FILE
  then strip to the bare key:  sed -i 's/^key = //' $SECRET_FILE"

  local perms; perms=$(stat -c '%a' "$SECRET_FILE" 2>/dev/null || echo 644)
  if (( 8#$perms > 8#600 )); then
    warn "secret file mode is $perms; tightening to 600"
    run chmod 600 "$SECRET_FILE"
  fi

  local key; key=$(tr -d '[:space:]' < "$SECRET_FILE")
  if [[ $key == key=* ]]; then
    warn "normalizing '$SECRET_FILE' (kernel secretfile= needs a bare base64 key)"
    run cp -a "$SECRET_FILE" "$SECRET_FILE.bak"
    run sed -i 's/^[[:space:]]*key[[:space:]]*=[[:space:]]*//' "$SECRET_FILE"
    key=$(tr -d '[:space:]' < "$SECRET_FILE")
  fi
  [[ $key =~ ^[A-Za-z0-9+/]{20,}={0,2}$ ]] || die "secret file does not look like a Ceph key"
}

check_mons() {
  local -a mons; IFS=',' read -ra mons <<< "$MONS"
  local mon reachable=0
  for mon in "${mons[@]}"; do
    mon="${mon%%:*}"
    if timeout 5 bash -c "exec 3<>/dev/tcp/$mon/$MON_PORT" 2>/dev/null; then
      info "mon $mon:$MON_PORT reachable"; reachable=1
    else
      warn "mon $mon:$MON_PORT unreachable"
    fi
  done
  (( reachable )) || die "no mon reachable on port $MON_PORT — try --mon-port 6789 (msgr1)"
}

ensure_ceph_conf() {
  if [[ -s $CEPH_CONF ]]; then info "keeping existing $CEPH_CONF"; return 0; fi
  info "writing minimal $CEPH_CONF"
  if [[ $DRY_RUN == 0 ]]; then
    cat > "$CEPH_CONF" <<EOF
[global]
mon_host = ${MONS//,/ }
EOF
    chmod 644 "$CEPH_CONF"
  else
    printf '[dry-run] write %s\n' "$CEPH_CONF" >&2
  fi
}

kernel_client_ok() {
  grep -qE '^[[:space:]]*(ceph|libceph)$' /proc/filesystems && return 0
  modprobe ceph 2>/dev/null || modprobe libceph 2>/dev/null || true
  grep -qE '^[[:space:]]*(ceph|libceph)$' /proc/filesystems
}

# NOTE: exact mount-point match — findmnt -T would resolve to '/' and lie.
is_mounted() {
  findmnt -rn -o TARGET "$1" >/dev/null 2>&1 && return 0
  awk -v m="$1" '$2 == m { found = 1 } END { exit !found }' /proc/mounts
}

# ── mount / unmount ─────────────────────────────────────────────────────────
do_mount() {
  local opts="fs=$FSNAME,name=$CLIENT_NAME,secretfile=$SECRET_FILE,$EXTRA_OPTS"
  local attempt
  for attempt in 1 2 3; do
    if run mount -t ceph "$MONS:/" "$MOUNT_POINT" -o "$opts"; then
      return 0
    fi
    warn "mount attempt $attempt/3 failed"
    [[ $attempt -lt 3 ]] && sleep $((attempt * 5))
  done
  return 1
}

do_fuse_mount() {
  run ceph-fuse -m "$MONS:$MON_PORT" -n "client.$CLIENT_NAME" \
      --keyfile="$SECRET_FILE" --client_fs="$FSNAME" "$MOUNT_POINT"
}

persist_fstab() {
  local entry="$MONS:/ $MOUNT_POINT ceph fs=$FSNAME,name=$CLIENT_NAME,secretfile=$SECRET_FILE,_netdev,$EXTRA_OPTS,x-systemd.automount,x-systemd.mount-timeout=60 0 0"
  if grep -qs "[[:space:]]$MOUNT_POINT[[:space:]]" /etc/fstab; then
    info "fstab already has an entry for $MOUNT_POINT — not touching it"; return 0
  fi
  info "adding fstab entry (backup: /etc/fstab.pre-ceph)"
  if [[ $DRY_RUN == 0 ]]; then
    cp -a /etc/fstab "/etc/fstab.pre-ceph"
    printf '%s\n' "$entry" >> /etc/fstab
    systemctl daemon-reload 2>/dev/null || true
  else
    printf '[dry-run] append to /etc/fstab: %s\n' "$entry" >&2
  fi
}

verify_mount() {
  findmnt -T "$MOUNT_POINT" || true
  df -h "$MOUNT_POINT" 2>/dev/null || true
  local probe="$MOUNT_POINT/.ceph-mount-probe.$$"
  if { echo ok > "$probe" && [[ "$(cat "$probe")" == ok ]] && rm -f "$probe"; } 2>/dev/null; then
    info "read/write verification passed"
  else
    warn "mount exists but read/write test failed — check caps/permissions for client.$CLIENT_NAME"
    return 1
  fi
}

selinux_hint() {
  command -v getenforce >/dev/null 2>&1 || return 0
  [[ "$(getenforce 2>/dev/null)" == "Enforcing" ]] || return 0
  if command -v semanage >/dev/null 2>&1; then
    semanage fcontext -l 2>/dev/null | grep -q "^$MOUNT_POINT(/.*)\?" \
      || warn "SELinux enforcing: consider 'semanage fcontext -a -t mnt_t \"$MOUNT_POINT(/.*)?\" && restorecon -Rv $MOUNT_POINT'"
  fi
}

# ── main ────────────────────────────────────────────────────────────────────
if [[ $UNMOUNT == 1 ]]; then
  is_mounted "$MOUNT_POINT" || { info "nothing mounted at $MOUNT_POINT"; exit 0; }
  run umount "$MOUNT_POINT" && info "unmounted $MOUNT_POINT"
  exit 0
fi

ensure_packages
validate_secret
ensure_ceph_conf
check_mons

if is_mounted "$MOUNT_POINT"; then
  info "already mounted at $MOUNT_POINT"
  verify_mount
  [[ $PERSIST == 1 ]] && persist_fstab
  exit 0
fi

mkdir -p "$MOUNT_POINT"

if [[ $USE_FUSE == 1 ]]; then
  info "mounting via ceph-fuse"
  do_fuse_mount || die "ceph-fuse mount failed"
else
  if ! kernel_client_ok; then
    warn "kernel 'ceph' filesystem unavailable — re-run with --fuse"
    die "cannot use kernel client on this kernel"
  fi
  info "mounting CephFS '$FSNAME' (client.$CLIENT_NAME) at $MOUNT_POINT ..."
  do_mount || die "mount failed after 3 attempts — see 'dmesg | tail -40' for the kernel-side reason"
fi

verify_mount || die "mounted but not usable"
[[ $PERSIST == 1 ]] && persist_fstab
selinux_hint
info "done."