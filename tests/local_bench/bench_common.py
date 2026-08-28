"""Shared constants/helpers for the single-machine XStore benchmark harness.

Mirrors the 12-workload matrix XBench's notebook drives on Mystic
(benchmark_artifact/XStore/XStore_NVMe_Mystic.ipynb), but runs everything on
one box: one XStore server process + one benchmarkClient process per tier,
with thread-count standing in for client-count (the notebook only fanned
workloads out across real machines once tier >= 8; the underlying unit of
concurrency was always "-t N threads in one benchmarkClient process").
"""
import glob
import os
import re
import shutil
import subprocess
import time

# name -> (dataset subdir under 10Y_128-Cols_Experiments, on-disk file prefix
# benchmarkClient's DataManager matches by, CLI subcommand, -e experiment_type,
# "kind" bucket -- also the CSV kind-word benchmarkClient's WriteCSV uses)
WORKLOADS = {
    "UNARY_INSERT_SEQ":  ("UNARY_INSERT_SEQ",  "BASIC-INSERT-UNARY-SEQ_Client-",  "insertBench", "UNARY_SEQ",  "INSERT"),
    "UNARY_INSERT_RAND": ("UNARY_INSERT_RAND", "BASIC-INSERT-UNARY-RAND_Client-", "insertBench", "UNARY_RAND", "INSERT"),
    "BATCH_INSERT_SEQ":  ("BATCH_INSERT_SEQ",  "BASIC-INSERT-BATCH-SEQ_Client-",  "insertBench", "BATCH_SEQ",  "INSERT"),
    "BATCH_INSERT_RAND": ("BATCH_INSERT_RAND", "BASIC-INSERT-BATCH-RAND_Client-", "insertBench", "BATCH_RAND", "INSERT"),
    "UNARY_QUERY_SEQ":   ("UNARY_QUERY_SEQ",   "BASIC-QUERY-UNARY-SEQ_Client-",   "queryBench",  "UNARY_SEQ",  "QUERY"),
    "UNARY_QUERY_RAND":  ("UNARY_QUERY_RAND",  "BASIC-QUERY-UNARY-RAND_Client-",  "queryBench",  "UNARY_RAND", "QUERY"),
    "BATCH_QUERY_SEQ":   ("BATCH_QUERY_SEQ",   "BASIC-QUERY-BATCH-SEQ_Client-",   "queryBench",  "BATCH_SEQ",  "QUERY"),
    "BATCH_QUERY_RAND":  ("BATCH_QUERY_RAND",  "BASIC-QUERY-BATCH-RAND_Client-",  "queryBench",  "BATCH_RAND", "QUERY"),
    "AGG_MIN":           ("AGG_MIN_QUERY",     "AGG-MIN-QUERY_Client-",           "aggBench",    "MIN",        "AGG"),
    "AGG_MAX":           ("AGG_MAX_QUERY",     "AGG-MAX-QUERY_Client-",           "aggBench",    "MAX",        "AGG"),
    "AGG_AVG":           ("AGG_AVG_QUERY",     "AGG-AVG-QUERY_Client-",           "aggBench",    "AVG",        "AGG"),
    "AGG_SUM":           ("AGG_SUM_QUERY",     "AGG-SUM-QUERY_Client-",           "aggBench",    "SUM",        "AGG"),
}

# Workloads whose "kind" purges/recreates its DB each tier (destructive) vs.
# ones that only read against a pre-seeded fixture (see seed_fixture.py).
# Both kinds target the same hardcoded DB name inside XStore_Adapter.cpp
# (BENCH_DB, see FIXTURE_DB_NAME) -- run_local_bench.py keeps them from
# fighting over it by renaming the fixture out of the way (stash_fixture)
# for the whole INSERT phase and renaming it back (restore_fixture)
# afterward, rather than by ordering alone.
DESTRUCTIVE_KINDS = {"INSERT"}

# The DB name every XStore_Adapter.cpp Insert/Query/Agg function hardcodes
# (not configurable without recompiling XBench -- -d/--db_name XSTORE only
# selects which adapter class to dispatch through, not the DB name on the
# wire). INSERT and QUERY/AGG workloads are both stuck sharing this name.
FIXTURE_DB_NAME = "BENCH_DB"
FIXTURE_STASH_SUFFIX = ".fixture_stash"

# The default `--workloads all` set/order: the 4 INSERT + 4 QUERY workloads,
# then just AGG_MIN as a representative aggregate (AGG_MAX/AVG/SUM exercise
# the same WalkYearSpan machinery with a different reduction -- pass them
# explicitly via --workloads if you want all four every run).
DEFAULT_WORKLOADS = [
    "UNARY_INSERT_SEQ", "UNARY_INSERT_RAND", "BATCH_INSERT_SEQ", "BATCH_INSERT_RAND",
    "UNARY_QUERY_SEQ", "UNARY_QUERY_RAND", "BATCH_QUERY_SEQ", "BATCH_QUERY_RAND",
    "AGG_MIN",
]

# benchmarkClient itself refuses -t above its own hardware_concurrency() (a
# hard exit(1) on the client side, observed in practice at tier=32 on a
# 16-core box) -- so the max useful tier IS this machine's core count, and
# fine-grained steps in between (2,4,8...) tell you little that 1 vs.
# "as many threads as this box has" doesn't already show for a single-node
# harness. {1, half, all} give a floor/mid/ceiling read in 3 tiers instead of 7.
CPU_COUNT = os.cpu_count() or 1
DEFAULT_TIERS = sorted({1, max(1, CPU_COUNT // 2), CPU_COUNT})
POOL_NODE_GLOB = "*_Node-*"


def dataset_pool(dataset_dir, workload_name, limit=None):
    """All parquet files for one workload, pooled across every Node-* dir
    (flattened -- these were only split by machine for the original
    distributed run; a single local benchmarkClient process can use any of
    them as one of its -t threads), sorted for reproducible selection."""
    subdir, prefix, *_ = WORKLOADS[workload_name]
    pattern = os.path.join(dataset_dir, subdir, f"{subdir}{POOL_NODE_GLOB}", f"{prefix}*.parquet")
    files = sorted(glob.glob(pattern))
    if limit is not None:
        files = files[:limit]
    return files


def parse_n_b(path):
    """Pull the -n/-b values benchmarkClient's DataManager validates against
    out of a staged file's own name, instead of hand-tracking what the
    original notebook happened to pass. The embedded token is the 3rd
    underscore-delimited segment; DataManager reads a bare digit run as
    n_iteration (unary), or a "X-Y" run as -b X -n Y (batch/agg) -- verified
    against DataManager::ReadQueryWorkload/ReadAggQueryWorkload, not against
    what the numbers "should" mean semantically.

    What they actually drive at runtime (confirmed by reading
    XStore_Adapter.cpp's *_SingleThread bodies, e.g. RangeQuery_SingleThread/
    AggMin_SingleThread: `for (i = 0; i < N_ITERATION; i++)` where the
    dispatcher passes B_ITERATIONS as N_ITERATION for batch/agg types):
    for UNARY, `n` is both the file's row count and the actual per-thread
    timed-loop count (1 row moved per call). For BATCH/AGG, the timed loop
    runs `b` times (one call per row in the file); `n` is never passed to
    the loop at all -- it's the rows-moved-per-call width baked into each
    row's own start/end time when the corpus was generated (confirmed
    against the raw files: BATCH_QUERY_SEQ's endTime-startTime == n == 100;
    AGG_*'s == n == 60)."""
    base = os.path.basename(path)
    token = base.split("_")[2]
    if "-" in token:
        b_str, n_str = token.split("-", 1)
        return int(n_str), int(b_str)
    return int(token), None


def stage_files(pool, tier, scratch_dir):
    """Symlink exactly `tier` files from `pool` into `scratch_dir`, renaming
    only the "Client-<n>" component to avoid collisions when the pool spans
    multiple original Node-* dirs (each of which independently numbers its
    files Client-0..31) -- everything after that token (the n/b-encoded
    segment DataManager validates, plus the offset/timestamp tail) is left
    untouched."""
    if len(pool) < tier:
        raise RuntimeError(f"pool has {len(pool)} files, need {tier}")
    os.makedirs(scratch_dir, exist_ok=True)
    clear_scratch(scratch_dir)
    for i, src in enumerate(pool[:tier]):
        base = re.sub(r"Client-\d+", f"Client-{i}", os.path.basename(src))
        os.symlink(src, os.path.join(scratch_dir, base))


def clear_scratch(scratch_dir):
    # benchmarkClient also drops its own logs/ dir (spdlog file sink) into cwd
    # alongside the staged parquet symlinks and its per-thread CSVs.
    if not os.path.isdir(scratch_dir):
        return
    for name in os.listdir(scratch_dir):
        path = os.path.join(scratch_dir, name)
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)


def drop_caches(log_fh=None):
    # Same exact sequence as the original runXStoreServer.sh, for parity
    # with the Mystic runs: free (before, informational) -> sync -> drop
    # caches -> free (after, informational).
    cmd = "free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null"
    try:
        subprocess.run(cmd, shell=True, check=True, timeout=15, stdout=log_fh, stderr=subprocess.STDOUT)
    except Exception as e:
        print(f"[drop_caches] skipped ({e}) -- continuing without a clean page cache")


def _fixture_dir(build_dir):
    return os.path.join(build_dir, "DataStore", FIXTURE_DB_NAME)


def _fixture_stash_dir(build_dir):
    return _fixture_dir(build_dir) + FIXTURE_STASH_SUFFIX


def stash_fixture(build_dir):
    """Rename DataStore/BENCH_DB out of the way (if present) before running
    INSERT tiers, so INSERT's own hardcoded BENCH_DB purge/recreate never
    touches the real query fixture -- it just creates a fresh BENCH_DB of
    its own next to the stashed one. No-op if there's nothing to protect
    (fixture not seeded yet) or it's already stashed (e.g. a prior run
    crashed mid-INSERT and never got to restore_fixture)."""
    live, stash = _fixture_dir(build_dir), _fixture_stash_dir(build_dir)
    if os.path.exists(stash):
        print(f"[stash] {stash} already exists (interrupted earlier run?) -- leaving it in place")
        return
    if os.path.exists(live):
        os.rename(live, stash)


def restore_fixture(build_dir):
    """Undo stash_fixture: drop INSERT's own scratch BENCH_DB and rename the
    stashed real fixture back to BENCH_DB. Only touches `live` at all when
    there's actually a `stash` to restore from -- otherwise (stash_fixture
    was never called this run, or this is a second, redundant call after an
    earlier call in the same run already consumed the stash) `live` is left
    exactly as-is, whether that's an already-restored fixture or nothing.
    Getting this wrong (unconditionally dropping `live`) would delete the
    fixture on every *successful* run, since run_local_bench.py calls this
    again as a safety net in `finally` after already calling it for real
    mid-run -- confirmed live, not hypothetical."""
    live, stash = _fixture_dir(build_dir), _fixture_stash_dir(build_dir)
    if os.path.exists(stash):
        if os.path.exists(live):
            shutil.rmtree(live)
        os.rename(stash, live)


def purge_insert_db(build_dir):
    """Fresh empty BENCH_DB for one INSERT tier. Only ever called between a
    stash_fixture()/restore_fixture() pair, so this never touches the real
    fixture -- it's purging INSERT's own scratch copy of the DB name."""
    ds = os.path.join(build_dir, "DataStore")
    os.makedirs(ds, exist_ok=True)
    live = _fixture_dir(build_dir)
    subprocess.run(["rm", "-rf", live], check=True)


def start_server(server_bin, host, port, cwd, log_fh=None):
    proc = subprocess.Popen(
        [server_bin, "--host", host, "--port", str(port), "--threads", "0"],
        cwd=cwd, stdout=log_fh, stderr=subprocess.STDOUT,
    )
    wait_for_server(host, port)
    return proc


def stop_server(proc):
    proc.terminate()
    try:
        proc.wait(timeout=15)
    except subprocess.TimeoutExpired:
        proc.kill()


def restart_server(proc, server_bin, host, port, cwd, log_fh=None):
    """MUST be called after any filesystem mutation under DataStore
    (stash_fixture/restore_fixture/purge_insert_db) and before the next
    client operation that touches it. The server caches open file mappings
    (mmap/fd, keyed by path) in memory for its whole lifetime and has no way
    to notice a file was renamed or deleted out from under it -- POSIX keeps
    an unlinked-but-still-mapped inode alive, so a stale cache entry keeps
    serving (or writing into) that orphaned copy instead of what's actually
    on disk at that path now. This is exactly why the reference
    runXStoreServer.sh restarts the server before every single tier; this
    harness had dropped that (one persistent server for the whole run, for
    speed) which is what let this class of bug back in."""
    if proc is not None:
        stop_server(proc)
    return start_server(server_bin, host, port, cwd, log_fh=log_fh)


def is_unary_workload(name):
    return name.startswith("UNARY")


def summarize_rows(name, rows):
    """rows: iterable of (tier, client_id, batch_size, time_taken_ms), where
    batch_size is rows-MOVED-per-call (parse_n_b's `n`; 1 for UNARY, since
    one call moves one row) -- NOT the iteration count (`b`); that's already
    implicit in how many rows landed in this tier's bucket. UNARY workloads
    -> {tier: mean_latency_ms}; BATCH/AGG workloads -> {tier: throughput_
    rows_per_sec} = batch_size (rows/call) * calls/sec/thread * tier
    (threads) = 1000 * batch_size / mean_latency_ms * tier."""
    by_tier = {}
    for tier, _client_id, batch_size, latency_ms in rows:
        by_tier.setdefault(tier, ([], batch_size))[0].append(latency_ms)

    out = {}
    for tier, (latencies, batch_size) in by_tier.items():
        mean_latency = sum(latencies) / len(latencies)
        if is_unary_workload(name):
            out[tier] = mean_latency
        else:
            n = batch_size or 1
            out[tier] = 0 if mean_latency == 0 else round(1000 * n / mean_latency * tier)
    return out


def wait_for_server(host, port, timeout=30):
    import sys
    sys.path.insert(0, "/home/ubuntu/XStore/client_src/Python")
    import XStore as xstore_mod
    deadline = time.time() + timeout
    last_err = None
    while time.time() < deadline:
        try:
            client = xstore_mod.XStoreClient(host, str(port))
            client.ping()
            return
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise TimeoutError(f"server never answered ping on {host}:{port}: {last_err}")
