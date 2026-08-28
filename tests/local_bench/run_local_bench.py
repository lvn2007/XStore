#!/usr/bin/env python3
"""Opt-in, single-machine stand-in for benchmark_artifact/XStore/
XStore_NVMe_Mystic.ipynb -- NOT run as part of the normal test suite or
build (no ctest/CMake wiring); invoke this script directly when you want a
local read on whether a code change to XStore or XStore_Client helped.

Same 9 workload families (unary/batch insert+query x SEQ/RAND, plus the 4
aggregates) and the same client tiers, but no SSH/Fabric/tmux and no second
machine: one local XStore server + one benchmarkClient process per tier,
using -t <tier> threads in that one process as the concurrency unit (this is
exactly what the notebook did anyway for every tier below 8; see
runWorkload()'s useSingleMachine branch in benchmark_artifact/scripts/
utilities.py, which this harness otherwise leaves untouched). Point --tag at
two different builds/binaries (baseline vs. a code change) and compare the
resulting summaries to tell whether the change helped.

Console output is a progress bar (tqdm if installed, else a plain fallback)
plus one summary table at the end (tier columns x workload rows -- mean
latency for UNARY_* workloads, throughput for BATCH_*/AGG_* ones).
Everything verbose goes to --log-dir (default logs/<tag>/ next to this
script) instead, split one file per actual measured run rather than one
giant blob: logs/<tag>/<WORKLOAD>_<tier>-Clients.log combines that tier's
XStore server output AND benchmarkClient output together (both target the
same file's fd, so their lines interleave in real time, not concatenated
after the fact) -- e.g. UNARY_INSERT_SEQ_1-Clients.log. Setup/teardown
(server boot before the first tier, fixture stash/restore, seeding) goes to
logs/<tag>/setup.log. Per-run CSVs land under results/<tag>/<WORKLOAD>.csv
(columns tier,client_id,batch_size,time_taken_ms) -- use
summarize_local_bench.py to compare multiple tags side by side after the
fact.

INSERT workloads always run first (reordered automatically regardless of
--workloads order), then QUERY/AGG. INSERT and QUERY/AGG both target the
same hardcoded BENCH_DB name inside XStore_Adapter.cpp (not configurable
without recompiling XBench), so before any INSERT tier runs, the real query
fixture (if seeded) gets renamed out of the way (bc.stash_fixture) --
INSERT's own purge/recreate then only ever touches its own fresh, separate
BENCH_DB. Once every INSERT tier is done, the fixture gets renamed back
(bc.restore_fixture) and this script seeds it itself on demand (see
seed_fixture.ensure_seeded) before the first QUERY/AGG workload -- a no-op
after the first run against a given (--dataset-dir, tiers) combination, so a
single `bench_local` covers insert+seed+query/agg+summarize end to end.

Per-tier sequence (matches the reference runXStoreServer.sh): cache clear ->
server restart -> wait 5s (let the fresh server settle) -> benchmarkClient
dispatch, scheduled via -ia 10s from the moment it's launched.

-t is capped at this machine's core count: benchmarkClient itself refuses a
thread count above its own hardware_concurrency() (hard exit(1) on the
client side), so requesting a tier above that fails fast here instead of
partway through a run.
"""
import argparse
import contextlib
import csv
import glob
import os
import subprocess
import sys
import time

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

sys.path.insert(0, os.path.dirname(__file__))
import bench_common as bc
import seed_fixture


class _FallbackProgress:
    """Used only if tqdm isn't installed -- same .update()/.set_description()/
    .close() surface, plain-text so it still shows live progress in a
    CMake/ninja build log."""
    def __init__(self, total, desc):
        self.total, self.desc, self.n = total, desc, 0

    def set_description(self, desc):
        self.desc = desc

    def update(self, k=1):
        self.n += k
        sys.stderr.write(f"\r[{self.desc}] {self.n}/{self.total}")
        sys.stderr.flush()

    def close(self):
        sys.stderr.write("\n")
        sys.stderr.flush()


def make_progress(total, desc):
    if tqdm is not None:
        return tqdm(total=total, desc=desc, file=sys.stderr)
    return _FallbackProgress(total, desc)


@contextlib.contextmanager
def logged(path, mode="w"):
    """Open `path` and redirect print()/subprocess stdout (when passed this
    context's yielded fh explicitly) into it for the duration of the block."""
    with open(path, mode) as fh:
        with contextlib.redirect_stdout(fh):
            yield fh


def order_workloads(names):
    destructive = [n for n in names if bc.WORKLOADS[n][4] in bc.DESTRUCTIVE_KINDS]
    non_destructive = [n for n in names if bc.WORKLOADS[n][4] not in bc.DESTRUCTIVE_KINDS]
    if non_destructive and destructive:
        print("[order] running INSERT workloads first, then QUERY/AGG (the fixture "
              "is stashed under a different name for the INSERT phase, then restored)")
    return destructive + non_destructive


def run_tier(client_bin, host, port, workload_name, tier, pool, scratch_dir, log_fh):
    subdir, prefix, subcmd, exp_type, kind = bc.WORKLOADS[workload_name]
    bc.stage_files(pool, tier, scratch_dir)
    staged = sorted(glob.glob(os.path.join(scratch_dir, "*.parquet")))
    n, b = bc.parse_n_b(staged[0])

    invoke_at = int(time.time()) + 10
    cmd = [
        client_bin, subcmd,
        "-i", host, "-p", str(port), "-d", "XSTORE",
        "-t", str(tier), "-e", exp_type,
        "-n", str(n), "-b", str(b if b is not None else 10),
        "-ia", str(invoke_at),
    ]
    subprocess.run(cmd, cwd=scratch_dir, check=True, stdout=log_fh, stderr=subprocess.STDOUT)

    pattern = os.path.join(scratch_dir, f"{kind}-{exp_type}_Client-*.csv")
    rows = []
    # Rows moved per call: for UNARY, 1 (one row per op); for BATCH/AGG, `n`
    # (the width baked into each file row -- see bc.parse_n_b's docstring).
    # NOT `b` -- `b` is the iteration count, already implicit in how many
    # rows land in this tier's bucket below.
    batch_size = n if b is not None else 1
    for i, csv_path in enumerate(sorted(glob.glob(pattern))):
        with open(csv_path) as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                rows.append((tier, i, batch_size, float(row["Time taken (ms)"])))
    if not rows:
        raise RuntimeError(f"no per-client CSVs matched {pattern} after running {workload_name} tier={tier}")
    return rows


def print_summary_table(tag, tiers, results):
    """results: {workload_name: (unit, {tier: value})}, in the order the
    workloads actually ran. One row per workload, one column per tier."""
    name_w = max(len(n) for n in results) + 2
    unit_w = 10
    col_w = 14
    header = f"{'Workload':<{name_w}}{'Unit':<{unit_w}}" + "".join(f"{'t=' + str(t):>{col_w}}" for t in tiers)
    print(f"\n=== local_bench summary (tag={tag}) ===")
    print(header)
    print("-" * len(header))
    for name, (unit, per_tier) in results.items():
        cells = "".join(f"{per_tier[t]:>{col_w},.2f}" if t in per_tier else f"{'--':>{col_w}}" for t in tiers)
        print(f"{name:<{name_w}}{unit:<{unit_w}}{cells}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", required=True, help="label for this build/binary, e.g. 'baseline' or 'cache-v2'")
    ap.add_argument("--server-build-dir", default="/home/ubuntu/XStore/build")
    ap.add_argument("--client-bin", default="/home/ubuntu/XBench/build/benchmarkClient")
    ap.add_argument("--dataset-dir", default="/home/ubuntu/XStore/10Y_128-Cols_Experiments")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=19497)
    ap.add_argument("--tiers", default=",".join(str(t) for t in bc.DEFAULT_TIERS))
    ap.add_argument("--workloads", default=",".join(bc.DEFAULT_WORKLOADS),
                     help="comma list of workload names, or 'all' for all 12 (default: the 4 INSERT + 4 QUERY + AGG_MIN)")
    ap.add_argument("--out-dir", default=os.path.join(os.path.dirname(__file__), "results"))
    ap.add_argument("--scratch-dir", default=os.path.join(os.path.dirname(__file__), "scratch"))
    ap.add_argument("--log-dir", default=os.path.join(os.path.dirname(__file__), "logs"),
                     help="verbose per-run logs land under <log-dir>/<tag>/ -- one <WORKLOAD>_<tier>-Clients.log "
                          "per (workload, tier), combining that run's server + benchmarkClient output, plus setup.log")
    ap.add_argument("--skip-cache-drop", action="store_true", help="for environments without passwordless sudo")
    ap.add_argument("--seed-cache-dir", default=os.path.join(os.path.dirname(__file__), "cache"))
    ap.add_argument("--seed-workers", type=int, default=os.cpu_count(),
                     help="parallel worker processes for one-time fixture seeding (see seed_fixture.py)")
    ap.add_argument("--force-reseed", action="store_true", help="reseed the QUERY/AGG fixture even if already up to date")
    args = ap.parse_args()

    tiers = [int(t) for t in args.tiers.split(",")]
    over_cap = [t for t in tiers if t > bc.CPU_COUNT]
    if over_cap:
        raise SystemExit(
            f"--tiers {over_cap} exceed this machine's core count ({bc.CPU_COUNT}) -- "
            f"benchmarkClient refuses -t above its own hardware_concurrency(), it would "
            f"fail partway through the run. Pass --tiers within 1..{bc.CPU_COUNT}."
        )
    names = list(bc.WORKLOADS.keys()) if args.workloads == "all" else args.workloads.split(",")
    for n in names:
        if n not in bc.WORKLOADS:
            raise SystemExit(f"unknown workload {n!r}; choices: {sorted(bc.WORKLOADS)}")

    out_dir = os.path.join(args.out_dir, args.tag)
    os.makedirs(out_dir, exist_ok=True)
    log_dir = os.path.join(args.log_dir, args.tag)
    os.makedirs(log_dir, exist_ok=True)
    setup_log_path = os.path.join(log_dir, "setup.log")

    results = {}  # name -> (unit, {tier: value}), populated in run order
    progress = make_progress(len(names) * len(tiers), f"bench_local[{args.tag}]")

    with logged(setup_log_path) as fh:
        names = order_workloads(names)
        server_bin = os.path.join(args.server_build_dir, "XStore")
        server_proc = bc.start_server(server_bin, args.host, args.port, args.server_build_dir, log_fh=fh)

    def restart(fh):
        # See bc.restart_server's docstring: mandatory after any filesystem
        # mutation under DataStore, so the server's in-memory mmap/fd cache
        # can't serve a stale (orphaned/renamed-away) file instead of what's
        # actually on disk at that path now.
        nonlocal server_proc
        server_proc = bc.restart_server(server_proc, server_bin, args.host, args.port, args.server_build_dir, log_fh=fh)

    try:
        if any(bc.WORKLOADS[n][4] in bc.DESTRUCTIVE_KINDS for n in names):
            with logged(setup_log_path, mode="a") as fh:
                bc.stash_fixture(args.server_build_dir)
                restart(fh)

        seeded = False
        for name in names:
            _, _, _, _, kind = bc.WORKLOADS[name]
            pool = bc.dataset_pool(args.dataset_dir, name)
            max_tier = max(tiers)
            if len(pool) < max_tier:
                raise SystemExit(f"{name}: pool has {len(pool)} files, need {max_tier} for the largest requested tier")

            if kind not in bc.DESTRUCTIVE_KINDS and not seeded:
                # First non-destructive workload in the (now INSERT-first)
                # order -- restore the real fixture (stashed above, if any
                # INSERT workload ran) before trusting/seeding it, so
                # ensure_seeded's marker+liveness check reflects reality.
                with logged(setup_log_path, mode="a") as fh:
                    bc.restore_fixture(args.server_build_dir)
                    restart(fh)
                    seed_fixture.ensure_seeded(
                        args.host, args.port, args.dataset_dir, max(tiers), args.seed_cache_dir,
                        force=args.force_reseed, workers=args.seed_workers,
                    )
                    restart(fh)
                seeded = True

            all_rows = []
            for tier in tiers:
                progress.set_description(f"{args.tag} | {name} t={tier}")
                tier_log_path = os.path.join(log_dir, f"{name}_{tier}-Clients.log")
                with logged(tier_log_path) as fh:
                    print(f"[{args.tag}] {name} tier={tier}")
                    sys.stdout.flush()  # otherwise this can land after the subprocess's own direct-fd writes below
                    if not args.skip_cache_drop:
                        bc.drop_caches(log_fh=fh)
                    if kind in bc.DESTRUCTIVE_KINDS:
                        bc.purge_insert_db(args.server_build_dir)
                    restart(fh)
                    time.sleep(5)
                    all_rows.extend(run_tier(args.client_bin, args.host, args.port, name, tier, pool, args.scratch_dir, fh))
                progress.update(1)

            out_path = os.path.join(out_dir, f"{name}.csv")
            with open(out_path, "w", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(["tier", "client_id", "batch_size", "time_taken_ms"])
                writer.writerows(all_rows)
            with logged(setup_log_path, mode="a") as fh:
                print(f"[{args.tag}] {name}: wrote {len(all_rows)} rows -> {out_path}")

            unit = "ms" if bc.is_unary_workload(name) else "rows/sec"
            results[name] = (unit, bc.summarize_rows(name, all_rows))

        bc.clear_scratch(args.scratch_dir)
    finally:
        progress.close()
        # Safety net: guarantees the real fixture ends up named BENCH_DB
        # again even on a run that only had INSERT workloads (so nothing
        # ever triggered the restore above) or that crashed mid-INSERT.
        # Idempotent -- a no-op if there was nothing stashed.
        with logged(setup_log_path, mode="a") as fh:
            bc.restore_fixture(args.server_build_dir)
            bc.stop_server(server_proc)

    if results:
        print_summary_table(args.tag, tiers, results)
    print(f"(verbose logs: {log_dir}/)")


if __name__ == "__main__":
    main()
