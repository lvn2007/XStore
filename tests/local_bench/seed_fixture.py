#!/usr/bin/env python3
"""Populate the local XSTORE DB with exactly the rows the QUERY/AGG workload
files in 10Y_128-Cols_Experiments actually reference (union of every
timestamp/[startTime,endTime] window across UNARY_QUERY_*, BATCH_QUERY_*,
AGG_*_QUERY), instead of restoring the full pre-baked 10-year dense
DataStore snapshot used on Mystic (that snapshot isn't in this checkout --
only XBench's per-client *parameter* files are, per docs/XStore_Technical_
Reference.md section 10.4). Span stays the true 10-year range; footprint is
scoped to what's actually queried.

Run once per (dataset_dir, pool size) combination; re-running is a cheap
no-op if the on-disk marker matches.
"""
import argparse
import glob
import hashlib
import json
import math
import multiprocessing
import os
import subprocess
import sys
import time

import numpy as np
import pyarrow.parquet as pq

sys.path.insert(0, os.path.dirname(__file__))
import bench_common as bc

sys.path.insert(0, "/home/ubuntu/XStore/client_src/Python")
import XStore as xstore_mod
import XStore_pb2 as xstore_proto

DB_NAME = "BENCH_DB"  # hardcoded inside every XStore_Adapter.cpp function -- the
# CLI's -d/--db_name flag only selects which adapter class (XSTORE/MONGODB/...)
# to dispatch through, not the logical DB name used on the wire
ROW_LENGTH = 1408  # 128 cols * (8B double + 3B protobuf field overhead), already 64B-aligned
SEED = 42

UNARY_WORKLOADS = ["UNARY_QUERY_SEQ", "UNARY_QUERY_RAND"]
RANGE_WORKLOADS = ["BATCH_QUERY_SEQ", "AGG_MIN", "AGG_MAX", "AGG_AVG", "AGG_SUM"]
ARBITRARY_SET_WORKLOADS = ["BATCH_QUERY_RAND"]


def collect_timestamps(dataset_dir, pool_size):
    ts_set = set()

    for name in UNARY_WORKLOADS:
        for f in bc.dataset_pool(dataset_dir, name, limit=pool_size):
            col = pq.read_table(f, columns=["timestamp"])["timestamp"].to_numpy()
            ts_set.update(col.tolist())
        print(f"[seed] after {name}: {len(ts_set):,} distinct timestamps")

    for name in RANGE_WORKLOADS:
        for f in bc.dataset_pool(dataset_dir, name, limit=pool_size):
            t = pq.read_table(f, columns=["startTime", "endTime"])
            starts = t["startTime"].to_numpy()
            ends = t["endTime"].to_numpy()
            for s, e in zip(starts, ends):
                ts_set.update(range(int(s), int(e) + 1))
        print(f"[seed] after {name}: {len(ts_set):,} distinct timestamps")

    for name in ARBITRARY_SET_WORKLOADS:
        for f in bc.dataset_pool(dataset_dir, name, limit=pool_size):
            flat = pq.read_table(f).to_pandas().to_numpy().flatten()
            ts_set.update(flat.tolist())
        print(f"[seed] after {name}: {len(ts_set):,} distinct timestamps")

    return ts_set


def marker_path(cache_dir):
    return os.path.join(cache_dir, "fixture_marker.json")


def compute_marker(dataset_dir, pool_size):
    # Hash the *set of source file paths*, not their content -- the parquet
    # files are static, checked-in fixtures; path+pool_size fully determines
    # what collect_timestamps() would produce.
    names = []
    for name in UNARY_WORKLOADS + RANGE_WORKLOADS + ARBITRARY_SET_WORKLOADS:
        names.extend(bc.dataset_pool(dataset_dir, name, limit=pool_size))
    digest = hashlib.sha256("\n".join(sorted(names)).encode()).hexdigest()
    return {"dataset_dir": dataset_dir, "pool_size": pool_size, "digest": digest}


def _load_shard(args):
    # Top-level (picklable) worker body: each process opens its OWN ZMQ
    # connection -- sockets aren't fork-safe/shareable across processes.
    # This is CPU-bound work (building a RowData protobuf message field by
    # field in pure Python, ~128 attribute-typed .add() calls per row, is
    # slow per the pure-Python protobuf implementation's per-field descriptor
    # lookups), not I/O-bound, so threads wouldn't help here -- they'd still
    # serialize on the GIL for the part that's actually slow. Separate
    # processes are the only way to get real parallelism on it.
    host, port, shard_idx, ts_shard, seed, chunk_size = args
    client = xstore_mod.XStoreClient(host, str(port))
    rng = np.random.default_rng(seed)
    total = len(ts_shard)
    for start in range(0, total, chunk_size):
        chunk_ts = ts_shard[start:start + chunk_size]
        chunk_vals = rng.uniform(-1_000_000.0, 1_000_000.0, size=(len(chunk_ts), 128))
        data_list = [
            {"timestamp": int(ts), "data": chunk_vals[i].tolist()}
            for i, ts in enumerate(chunk_ts)
        ]
        client.batchInsert(data_list, DB_NAME)
    return shard_idx, total


def load_fixture(host, port, sorted_ts, workers=1, chunk_size=5000):
    if workers <= 1:
        _load_shard((host, port, 0, sorted_ts, SEED, chunk_size))
        return

    shard_size = math.ceil(len(sorted_ts) / workers)
    shards = [sorted_ts[i:i + shard_size] for i in range(0, len(sorted_ts), shard_size)]
    job_args = [(host, port, idx, shard, SEED + idx, chunk_size) for idx, shard in enumerate(shards)]

    print(f"[seed] loading {len(sorted_ts):,} rows across {len(shards)} worker processes")
    with multiprocessing.Pool(processes=len(shards)) as pool:
        for shard_idx, n in pool.imap_unordered(_load_shard, job_args):
            print(f"[seed] worker {shard_idx} done ({n:,} rows)")


def _fixture_present_on_server(host, port):
    client = xstore_mod.XStoreClient(host, str(port))
    info = client.infoQuery()
    return any(db.dbName == DB_NAME for db in info.dbList)


def ensure_seeded(host, port, dataset_dir, pool_size, cache_dir, force=False, workers=1, chunk_size=5000):
    """Idempotent: no-ops (fast marker check + a live BENCH_DB presence
    check) if this exact (dataset_dir, pool_size) combination was already
    seeded AND is still actually on the server. Safe to call on every
    run_local_bench.py invocation -- the common case is a sub-second no-op.

    The marker file lives in the source tree (cache_dir), not in the build
    dir -- a `rm -rf build && cmake .. && make` (or any other DataStore
    reset -- an INSERT-only run purges it too) wipes BENCH_DB but leaves the
    marker untouched. Trusting the marker alone would then skip reseeding
    against an empty DB, producing "No valid data" on every query. Always
    cross-check the live server, not just the marker."""
    os.makedirs(cache_dir, exist_ok=True)
    marker = compute_marker(dataset_dir, pool_size)
    mpath = marker_path(cache_dir)
    if not force and os.path.exists(mpath):
        with open(mpath) as fh:
            existing = json.load(fh)
        if existing == marker:
            if _fixture_present_on_server(host, port):
                print(f"[seed] marker matches ({mpath}) and {DB_NAME} is present -- already seeded, nothing to do (pass force=True to redo)")
                return False
            print(f"[seed] marker matches ({mpath}) but {DB_NAME} is missing on the server "
                  f"(DataStore reset since the last seed?) -- reseeding")

    t0 = time.time()
    ts_set = collect_timestamps(dataset_dir, pool_size)
    sorted_ts = sorted(ts_set)
    print(f"[seed] {len(sorted_ts):,} total distinct timestamps to load (scan took {time.time()-t0:.1f}s)")

    client = xstore_mod.XStoreClient(host, str(port))
    client.createDB(DB_NAME, xstore_proto.SECOND, ROW_LENGTH)

    t1 = time.time()
    load_fixture(host, port, sorted_ts, workers=workers, chunk_size=chunk_size)
    print(f"[seed] load done in {time.time()-t1:.1f}s ({workers} worker process(es))")

    with open(mpath, "w") as fh:
        json.dump(marker, fh)
    return True


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset-dir", default="/home/ubuntu/XStore/10Y_128-Cols_Experiments")
    ap.add_argument("--server-build-dir", default="/home/ubuntu/XStore/build")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=19497)
    ap.add_argument("--pool-size", type=int, default=bc.CPU_COUNT,
                     help="max tier this fixture supports (files pooled per workload); defaults to this "
                          "machine's core count since benchmarkClient can't run a tier above that anyway")
    ap.add_argument("--cache-dir", default=os.path.join(os.path.dirname(__file__), "cache"))
    ap.add_argument("--force", action="store_true", help="reseed even if the marker matches")
    ap.add_argument("--workers", type=int, default=os.cpu_count(),
                     help="parallel worker processes for the load step (CPU-bound protobuf construction, not I/O -- threads wouldn't help, see _load_shard)")
    ap.add_argument("--start-server", action="store_true", help="start+stop an XStore server for this run instead of assuming one is already up")
    args = ap.parse_args()

    server_proc = None
    if args.start_server:
        server_bin = os.path.join(args.server_build_dir, "XStore")
        server_proc = subprocess.Popen(
            [server_bin, "--host", args.host, "--port", str(args.port), "--threads", "0"],
            cwd=args.server_build_dir,
        )
        bc.wait_for_server(args.host, args.port)

    try:
        ensure_seeded(args.host, args.port, args.dataset_dir, args.pool_size, args.cache_dir,
                      force=args.force, workers=args.workers)
    finally:
        if server_proc is not None:
            server_proc.terminate()
            server_proc.wait(timeout=15)


if __name__ == "__main__":
    main()
