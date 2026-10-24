#!/usr/bin/env python3
"""Replay a dm-log-writes log onto copies of base.img and snapshot crash states.
usage: replay.py <base.img> <log.img> <outdir> <target_ms>...
For each target_ms the first timestamp mark ("t<epoch_ms>") with time >= target_ms gives two states:
  A<k>.img  every logged write before the mark
  C<k>_0.img, C<k>_1.img  B plus a random half (seeded) of the writes between the last flush and 24 entries after the mark: a power
            cut that persisted some unflushed writes and not others (the writes of acks older than the mark are all flushed)
  B<k>.img  only the writes up to the last FLUSH/FUA entry before the mark (the unflushed writes are lost, as in a power cut
            with a volatile write cache)
Prints one JSON line per target: {"k", "mark_ms", "A", "B", "writes_a", "writes_b"}; 'info' prints the log summary."""
import json, mmap, os, random, shutil, struct, subprocess, sys
FLUSH, FUA, DISCARD, MARK = 1, 2, 4, 8
def parse(path):
    # entry = 32 B header at a sector boundary. MARK: the name follows the header inside the same sector(s) (data_len bytes).
    # write: nr_sectors*512 data bytes follow the header sector (data_len is 0 for them). discard: header sector only.
    f = open(path, "rb"); m = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    magic, ver, nent, ssz = struct.unpack_from("<QQQI", m, 0)
    ents, pos = [], ssz
    while pos + 32 <= len(m):
        sector, nsec, flags, dlen = struct.unpack_from("<QQQQ", m, pos)
        if flags > 0xff or (flags == 0 and nsec == 0): break
        if flags & MARK:
            ents.append((sector, nsec, flags, dlen, pos + 32)); size = -(-(32 + dlen) // ssz) * ssz
        elif flags & DISCARD:
            ents.append((sector, nsec, flags, 0, pos + ssz)); size = ssz
        else:
            ents.append((sector, nsec, flags, nsec * 512, pos + ssz)); size = ssz + -(-(nsec * 512) // ssz) * ssz
        pos += size
    return m, ssz, nent, ents
def mark_name(m, e):
    return m[e[4]:e[4] + e[3]].decode(errors="replace")
def main():
    if sys.argv[1] == "info":
        m, ssz, nent, ents = parse(sys.argv[2])
        kinds = {"write": 0, "flush": 0, "fua": 0, "discard": 0, "mark": 0}
        for e in ents:
            if e[2] & MARK: kinds["mark"] += 1
            elif e[2] & DISCARD: kinds["discard"] += 1
            else: kinds["write"] += 1
            if e[2] & FLUSH: kinds["flush"] += 1
            if e[2] & FUA: kinds["fua"] += 1
        print(json.dumps({"super_entries": nent, "parsed": len(ents), "sectorsize": ssz, **kinds}))
        return
    base, logp, out = sys.argv[1:4]; targets = sorted(int(x) for x in sys.argv[4:])
    m, ssz, nent, ents = parse(logp)
    marks = []  # (entry index, ms)
    for i, e in enumerate(ents):
        if e[2] & MARK:
            nm = mark_name(m, e)
            if nm.startswith("t") and nm[1:].isdigit(): marks.append((i, int(nm[1:])))
    os.makedirs(out, exist_ok=True)
    work = os.path.join(out, "work.img")
    subprocess.check_call(["cp", "--sparse=always", base, work])
    fd = os.open(work, os.O_RDWR)
    applied = 0
    def apply_to(idx):  # apply entries [applied, idx)
        nonlocal applied
        for e in ents[applied:idx]:
            if e[2] & MARK: continue
            if e[2] & DISCARD:
                if e[1]: os.pwrite(fd, b"\0" * (e[1] * 512), e[0] * 512)
                continue
            if e[1] and e[3]: os.pwrite(fd, m[e[4]:e[4] + e[3]], e[0] * 512)
        applied = max(applied, idx)
    def snap(name):
        os.fsync(fd)
        subprocess.check_call(["cp", "--sparse=always", work, os.path.join(out, name)])
    for k, tgt in enumerate(targets):
        mi = next(((i, ms) for i, ms in marks if ms >= tgt and i >= applied), None)
        if mi is None: print(json.dumps({"k": k, "error": "no mark at or after %d" % tgt})); continue
        idx, ms = mi
        def boundary(lo, hi):  # prefix length that a power cut at entry hi still keeps: a FUA entry is durable itself, a
            for j in range(hi - 1, lo - 1, -1):  # PREFLUSH entry only makes the entries before it durable
                if ents[j][2] & FUA: return j + 1
                if ents[j][2] & FLUSH: return j
            return None
        bnd = boundary(applied, idx)
        if bnd is not None and bnd > applied: apply_to(bnd)
        wb = sum(1 for e in ents[:applied] if not e[2] & (MARK | DISCARD))
        snap("B%d.img" % k)
        cs = []
        for v in range(2):
            window = [e for e in ents[bnd if bnd is not None else applied:idx + 24] if not e[2] & (MARK | DISCARD) and e[1]]
            rng = random.Random(k * 7 + v)
            keep = [e for e in window if rng.random() < 0.5]
            subprocess.check_call(["cp", "--sparse=always", os.path.join(out, "B%d.img" % k), os.path.join(out, "C%d_%d.img" % (k, v))])
            cfd = os.open(os.path.join(out, "C%d_%d.img" % (k, v)), os.O_RDWR)
            for e in keep: os.pwrite(cfd, m[e[4]:e[4] + e[3]], e[0] * 512)
            os.fsync(cfd); os.close(cfd); cs.append("%d/%d" % (len(keep), len(window)))
        apply_to(idx)
        wa = sum(1 for e in ents[:applied] if not e[2] & (MARK | DISCARD))
        snap("A%d.img" % k)
        print(json.dumps({"k": k, "mark_ms": ms, "A": os.path.join(out, "A%d.img" % k), "B": os.path.join(out, "B%d.img" % k), "C0": os.path.join(out, "C%d_0.img" % k), "C1": os.path.join(out, "C%d_1.img" % k), "kept": cs,
                          "writes_a": wa, "writes_b": wb}), flush=True)
    os.close(fd); os.remove(work)
main()
