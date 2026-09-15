# Local XStore benchmark harness

Opt-in, single-machine stand-in for `benchmark_artifact/XStore/XStore_NVMe_Mystic.ipynb`.
Same 9 workload families (unary/batch insert+query x SEQ/RAND, plus MIN/MAX/AVG/SUM
aggregates), but no SSH/Fabric/tmux and no second machine required --
everything runs as local subprocesses. Point it at two different builds and
compare the printed summaries to tell whether a code change to XStore or
XStore_Client helped or hurt.

Client tiers default to `{1, cores/2, cores}` (e.g. `1,8,16` on a 16-core
box), not a fine-grained `1,2,4,8,...` sweep -- `XBench` refuses `-t`
above its own `hardware_concurrency()` (hard client-side error), so the max
useful tier already *is* this machine's core count, and a single-node
harness has nothing new to show between the floor and the ceiling.

`--workloads all` runs all 12; the default (no `--workloads` flag) instead
runs a fixed 9-workload sequence -- `UNARY_INSERT_SEQ`, `UNARY_INSERT_RAND`,
`BATCH_INSERT_SEQ`, `BATCH_INSERT_RAND`, `UNARY_QUERY_SEQ`,
`UNARY_QUERY_RAND`, `BATCH_QUERY_SEQ`, `BATCH_QUERY_RAND`, `AGG_MIN` -- all 4
INSERT patterns, all 4 QUERY patterns, and just `AGG_MIN` as a representative
aggregate (`AGG_MAX`/`AGG_AVG`/`AGG_SUM` exercise the same `WalkYearSpan`
machinery with a different reduction; pass them explicitly if you want all
four every run).

This is wired into `tests/CMakeLists.txt` as opt-in custom targets, never
part of `ALL` -- same precedent as `tests/perf/XStorePerfTest.cpp`
(`xstore_perf_tests`, deliberately left out of the POST_BUILD ctest hook that
runs the correctness suites automatically). Nothing here builds or runs
unless you invoke a script or `cmake --build . --target bench_local*`
directly.

## Why this is a fair stand-in

The notebook only fanned a tier out across real machines once client count
reached 8 (`useSingleMachine = numClient < 8` in the original `runWorkload()`
in `benchmark_artifact/scripts/utilities.py`). Below that, and always as the
actual unit of concurrency, it was one `XBench` process given `-t N`
threads. This harness always uses that path, for every tier -- real
concurrency, same per-tier discipline as the reference `runXStoreServer.sh`:
**cache clear -> server restart -> wait 5s -> XBench dispatch**
(scheduled via `-ia`, 10s from the moment it's launched), for every single
(workload, tier) pair. The server restart isn't just for fairness -- it's
load-bearing: the server caches open file mappings (mmap/fd) by path for its
whole lifetime, so a fresh restart after any DataStore mutation (INSERT's
purge, or the fixture stash/restore, see below) is what keeps it from
serving/writing through a stale, orphaned mapping instead of what's actually
on disk. What's lost vs. Mystic: cross-machine dispatch skew/network latency.

## What each experiment actually runs

Confirmed by reading `XStore_Adapter.cpp`'s `*_SingleThread` bodies (the
timed loop runs `-b` times for BATCH/AGG, `-n` times for UNARY) and the raw
corpus files' own `startTime`/`endTime` columns:

| Experiment | Op/iteration | Iterations/thread | Batch size / range width |
|---|---|---|---|
| UNARY_INSERT_SEQ/RAND | 1 row insert | 1000 | n/a |
| BATCH_INSERT_SEQ/RAND | 1 RangeInsert/BatchInsert call | 10 | 1000 rows/call |
| UNARY_QUERY_SEQ/RAND | 1 row query | 1000 | n/a |
| BATCH_QUERY_SEQ/RAND | 1 range/batch query | 10 | 100 rows/call |
| AGG_MIN/MAX/AVG/SUM | 1 aggregate RPC | 1000 | 60s range width, random target column |

Both `-n`/`-b` are derived per staged file (`bc.parse_n_b`), never
hardcoded, so these numbers hold for the real corpus regardless of which
files get pooled for a given tier.

## Files

- `bench_common.py` -- the 12-workload table (dataset dir, file-name prefix,
  CLI subcommand/experiment type) and shared helpers (server lifecycle,
  cache-drop, DataStore purge, client-file staging, summary math).
- `seed_fixture.py` -- one-time setup for QUERY/AGG workloads (see below).
- `run_local_bench.py` -- the driver. Runs every tier of every requested
  workload, writes one CSV per workload under
  `results/<tag>/<WORKLOAD>.csv` (columns `tier,client_id,batch_size,
  time_taken_ms` -- `batch_size` is rows moved per call: 1 for UNARY, the
  range/batch width for BATCH/AGG, see "What each experiment actually runs"
  below). Console output is a progress bar (`tqdm` if installed, else a
  plain fallback -- `pip install tqdm` for the nicer one, entirely optional)
  plus one summary table at the end (tier columns x workload rows).
  Everything verbose -- server boot, XBench's own per-op logging,
  seed progress, per-tier housekeeping -- goes to `--log-dir` (default
  `logs/<tag>/` next to this script) instead, one file per actual measured
  run rather than one giant blob: `<WORKLOAD>_<tier>-Clients.log` combines
  that tier's XStore server output *and* XBench output together
  (both write to the same fd, so lines interleave in real time -- e.g.
  `UNARY_INSERT_SEQ_1-Clients.log`). Setup/teardown (initial server boot,
  fixture stash/restore, seeding) goes to `logs/<tag>/setup.log`. Re-running
  the same `--tag` overwrites that tag's own log files, not a sibling
  workload/tier's.
- `summarize_local_bench.py` -- re-prints that same per-tier summary for one
  or more already-run tags side by side, with %change vs. the first tag --
  no plotting, just a table (this is a local dev loop, not the paper's
  multi-machine artifact).

## The query/agg dataset

INSERT workloads reuse `benchmark_artifact/10Y_128-Cols_Experiments/*_INSERT_*`
as-is -- those files already carry real row data (timestamp + 128 double
columns), so no generation step is needed; `run_local_bench.py` purges the
DataStore and lets each insert tier populate it fresh, same as the original.

QUERY/AGG workloads only carry *parameters* in that corpus (which timestamps /
ranges to query) -- the actual populated 10-year DataStore the notebook
queries against is a separate pre-baked snapshot restored from an object
store on Mystic (see `docs/XStore_Technical_Reference.md` §10.4), which isn't
checked into this repo and is far too large to be "local-scale" anyway.

Instead, `seed_fixture.py` scans every `UNARY_QUERY_*`, `BATCH_QUERY_*`, and
`AGG_*_QUERY` parameter file (up to `--pool-size` files per workload, default
this machine's core count -- matching the tier cap above, since a bigger
pool would only support tiers this box can never actually run), takes the
union of every timestamp/range they actually reference, and inserts
deterministic synthetic rows (seeded RNG, same 128-double-column schema) at
exactly those points. The span still covers the full 2000-2009 10-year
range (whatever the corpus's random windows land on); only the *footprint*
is scoped down to what's actually queried -- on a 16-core box, on the order
of 1M rows (~1GB on disk); scale up with a larger explicit `--pool-size` if
you want to test more (rented/bigger) hardware's worth of tiers later. You
never need to run it by hand -- `run_local_bench.py` (and therefore
`bench_local`, see below) calls `seed_fixture.ensure_seeded()` itself right
before the first QUERY/AGG workload and skips straight through (a sub-second
marker check) once it's already done. The parquet-scan + row-generation +
insert is parallelized across `--seed-workers` processes (default: all
cores) precisely because it's CPU-bound, not I/O-bound -- see "Why seeding
is slow" below.

`seed_fixture.py` still exists standalone if you want to pre-warm a fixture
ahead of time, independent of any particular run:

```
python3 seed_fixture.py --start-server
```

Re-running (standalone or via run_local_bench.py) is a cheap no-op (marker
file keyed on the source file list, cross-checked against a live `BENCH_DB`
presence query -- not just the marker) unless you pass
`--force`/`--force-reseed` or point at a different `--pool-size`/
`--dataset-dir`. The live check matters because the marker lives in the
source tree, not in `build/`: a `rm -rf build && cmake .. && make` (or any
other DataStore reset, e.g. running INSERT workloads alone) wipes `BENCH_DB`
but leaves the marker file untouched, so it alone would have said "already
seeded" against an empty store.

### Why seeding is slow

The per-row cost is building a `RowData` protobuf message in pure Python --
128 typed `.add()` calls per row, each paying the Python protobuf
implementation's per-field descriptor lookup. That's CPU-bound work held
under the GIL, so it doesn't overlap across Python *threads*; `seed_fixture.py`
instead shards the timestamp list across `--seed-workers` separate
*processes* (default `os.cpu_count()`), each with its own ZMQ connection --
real parallelism, roughly linear in core count. Network round-trips
(one `BatchInsert` per 5000-row chunk) are a minor fraction of the total by
comparison.

## Running an A/B comparison

### Via CMake (a few commands)

`tests/CMakeLists.txt` wires this harness in as opt-in custom targets --
never part of `ALL`, so plain `cmake .. && make -j` is completely unaffected.
`bench_local` alone does everything -- seeds the fixture if needed, runs, and
summarizes:

```
cmake .. -DXSTORE_LOCAL_BENCH_TAG=baseline   # reconfigure to change the tag
cmake --build . --target bench_local
```

Rebuild with your code change, reconfigure with a new tag, run again, then
compare both:

```
cmake --build . --target XStore
cmake .. -DXSTORE_LOCAL_BENCH_TAG=changed
cmake --build . --target bench_local
python3 tests/local_bench/summarize_local_bench.py --tags baseline,changed
```

(`bench_local`'s built-in summarize step only ever sees its own current
`XSTORE_LOCAL_BENCH_TAG`, so comparing *two* tags together is one direct
`summarize_local_bench.py --tags a,b` call, same as above.)

`bench_local_seed` still exists as its own target too, for pre-warming a
larger fixture ahead of time instead of paying that cost inside the first
`bench_local` run.

Cache variables (`cmake -D<VAR>=<value> ..` or `ccmake .`):

| Variable | Default | Meaning |
|---|---|---|
| `XSTORE_LOCAL_BENCH_TAG` | `default` | label for `bench_local_run`/`bench_local` |
| `XSTORE_LOCAL_BENCH_TIERS` | *(empty = auto)* | client tiers to run, e.g. `1,8,16`; empty defers to the Python script's own `{1, cores/2, cores}` default |
| `XSTORE_LOCAL_BENCH_WORKLOADS` | *(empty = auto)* | comma list of workload names, or `all` for all 12; empty defers to the script's own default (see below) |
| `XSTORE_LOCAL_BENCH_POOL_SIZE` | *(empty = auto)* | max tier `bench_local_seed` prepares a fixture for; empty defers to this machine's core count |
| `XSTORE_LOCAL_BENCH_CLIENT_BIN` | `/home/ubuntu/XBench/build/XBench` | path to `XBench` |

Targets: `bench_local_seed`, `bench_local_run`, `bench_local_summarize`
(re-prints the current tag's summary on its own), and `bench_local` (run +
summarize in one command).

### Directly (finer-grained, e.g. comparing tags without reconfiguring)

```
# 1. Build baseline, point the harness at it, run and tag it
cmake --build build-baseline -j$(nproc)
python3 run_local_bench.py --tag baseline \
    --server-build-dir /path/to/build-baseline \
    --client-bin /path/to/XBench/build/XBench

# 2. Build your change, same dataset/fixture, different tag
cmake --build build-changed -j$(nproc)
python3 run_local_bench.py --tag changed \
    --server-build-dir /path/to/build-changed \
    --client-bin /path/to/XBench/build/XBench

# 3. Compare
python3 summarize_local_bench.py --tags baseline,changed
```

Use `--workloads UNARY_INSERT_SEQ,UNARY_QUERY_RAND` (etc.) and/or `--tiers
1,4,16` to iterate on a subset instead of the full matrix while developing.

Changing only `client_src` (not the server)? Point `--server-build-dir` at the
same server build for both tags and only swap `--client-bin`.

## Requirements

- Passwordless `sudo` for the cache-drop step (`--skip-cache-drop` to run
  without it, at the cost of comparability with runs that do drop caches).
- `-t`/`--tiers` values must be `<=` this machine's core count --
  `XBench` refuses a thread count above its own
  `hardware_concurrency()` (hard client-side error), so `run_local_bench.py`
  validates this itself upfront and refuses to start rather than fail
  partway through a run.
- `BENCH_DB` (the logical DB name hardcoded inside every `XStore_Adapter.cpp`
  function -- `-d/--db_name XSTORE` only selects *which adapter*
  XBench dispatches through, it isn't the DB name on the wire) gets
  seeded automatically; you don't need to run `seed_fixture.py` yourself.
- INSERT and QUERY/AGG both target that same hardcoded `BENCH_DB` name, and
  INSERT purges+recreates it every tier -- left alone, that would destroy
  the QUERY/AGG fixture the moment any INSERT workload ran in the same
  invocation. A run that mixes both kinds always executes INSERT first
  (regardless of `--workloads` order) and, right before any INSERT tier
  runs, renames `DataStore/BENCH_DB` to `DataStore/BENCH_DB.fixture_stash`
  if a fixture is already there (`bc.stash_fixture`) -- INSERT's own
  purge/recreate then only ever touches a fresh, separate `BENCH_DB` of its
  own. Once every INSERT tier is done, the stashed fixture gets renamed
  back (`bc.restore_fixture`) before QUERY/AGG starts, and again
  unconditionally in a `finally` block (covers INSERT-only runs, or a
  crash mid-INSERT) -- so the real fixture is never actually destroyed,
  just relocated for the duration of the INSERT phase.
- The Python `XStoreClient`'s per-connection "Connecting to tcp://..." print
  is off by default (`verbose=False`); this harness creates a lot of
  connections (one per seed worker process, one per driver run) and doesn't
  pass `verbose=True` anywhere, so that noise stays suppressed.
