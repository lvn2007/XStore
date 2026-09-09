#pragma once
#include <cstddef>
#include <cstdint>

// Single source of truth for every tunable cap/shard-count/timing knob in
// XStore. XStore.h re-exposes the cache-related ones as XStore::<NAME>
// static constexpr aliases so existing call sites (XStore::CFG_SHARD_COUNT,
// XStore::MMAP_CACHE_CAP, etc., including tests/integration/XStoreTest.cpp)
// are unaffected -- this header exists so tuning any of these values means
// editing one file, not hunting through class internals.
namespace XStoreTunables {

inline constexpr size_t CFG_SHARD_COUNT  = 16;

inline constexpr size_t MMAP_SHARD_COUNT = 128;
inline constexpr size_t MMAP_CACHE_CAP   = 512;
inline constexpr size_t MMAP_SHARD_CAP   = MMAP_CACHE_CAP / MMAP_SHARD_COUNT;

inline constexpr size_t FD_SHARD_COUNT   = 16;
inline constexpr size_t FD_CACHE_CAP     = 64;
inline constexpr size_t FD_SHARD_CAP     = FD_CACHE_CAP / FD_SHARD_COUNT;

// WAL -- resource sizing kept independent of CFG_SHARD_COUNT on purpose:
// these size a pool of ALWAYS-RESIDENT writer threads (a real cost), not a
// lock-striping factor (near-zero cost to have many of). Default left
// conservative; raise only if a benchmark shows a specific shard's writer
// thread is the throughput bottleneck.
inline constexpr size_t      WAL_SHARD_COUNT            = 8;
// This is an idle-poll ceiling, not a batching delay: wait_dequeue_bulk_timed
// (moodycamel) grabs whatever's already queued the instant it arrives via a
// semaphore signal on enqueue -- it only actually sleeps out this window
// when the queue is genuinely empty. Every wake (including a "still empty,
// go back to sleep" one) pays a fixed ~10k-iteration CAS-spin before the
// real OS block (LightweightSemaphore's maxSpins), so a SHORTER window
// means MORE wakes/sec means MORE wasted idle CPU -- measured 1us -> ~203%
// CPU across the 8 writer threads while completely idle, 500us -> ~42%,
// 50ms -> ~0.6% (single idle server, zero traffic, /proc/<pid>/task
// utime+stime deltas). 10ms was chosen as the production value: keeps the
// bulk of that idle-CPU win while staying an order of magnitude below the
// range where anything else could plausibly notice it. No hot-path latency
// cost at any window size (a real enqueue always wakes the writer
// immediately, never gated by this value) -- confirmed via matched local
// benchmark runs (tier-1 UNARY_INSERT_SEQ/RAND, 500us vs 50us, no
// consistent delta beyond run-to-run noise). Only other place this value
// matters at all: how long StartWalWriters' atexit-registered shutdown
// waits for an idle shard's writer thread to notice stop_flag and join --
// irrelevant for the server binary itself (zmq::proxy() never returns
// under normal operation, and there's no SIGTERM handler, so the server is
// always killed abruptly and this atexit path never runs in practice for
// it); does apply once per process for the GoogleTest binaries (each test
// case is its own subprocess under ctest, and each exits normally,
// triggering it) -- measured negligible in practice: two controlled
// back-to-back full xstore_tests suite runs (102 tests, 102 subprocesses)
// came out at 223.55s (500us) vs 225.63s (10ms), a ~1% difference within
// normal run-to-run noise for a process-spawn-heavy suite.
inline constexpr unsigned    WAL_GROUP_COMMIT_WINDOW_US  = 10000;
inline constexpr size_t      WAL_BATCH_MAX_RECORDS       = 256;
// Linux's IOV_MAX/UIO_MAXIOV (confirmed via getconf IOV_MAX on the
// deployment target) -- pwritev fails with EINVAL past this many iovecs in
// one call. WalWriterLoop's per-file write step chunks into multiple
// pwritev calls (same file, advancing offset) rather than assuming a
// single call always fits, now that Batch/Range Insert can hand a single
// WalRecord hundreds of thousands of rows (2 iovecs/row: an epoch-time
// buffer + the row bytes for every row past the first).
inline constexpr size_t      WAL_PWRITEV_MAX_IOVECS      = 1024;
inline constexpr const char* WAL_DIR_NAME                = "WAL";

// WAL_SHARD_COUNT above is the compile-time ceiling (array size); the
// *active* shard/writer-thread count is computed at startup as
// hardware_concurrency() / WAL_ACTIVE_SHARDS_CORE_DIVISOR, clamped to
// [WAL_MIN_ACTIVE_SHARDS, WAL_SHARD_COUNT] -- see StartWalWriters. Writer
// threads are mostly blocked in fsync()/on the queue semaphore (near-zero
// idle CPU cost, see the WAL_GROUP_COMMIT_WINDOW_US comment above), so the
// real ceiling on "how many is too many" is contention at the storage
// device, not CPU -- this stays a conservative fraction of core count
// rather than 1:1. Named out (not an inline literal in StartWalWriters)
// specifically so testing "does more active-shard capacity help" is a
// one-line edit here + rebuild, not a hunt for a magic number.
inline constexpr size_t      WAL_ACTIVE_SHARDS_CORE_DIVISOR = 4;
inline constexpr size_t      WAL_MIN_ACTIVE_SHARDS       = 2;

// Experiment knob, default was 1 (= today's exact per-DB-hash behavior,
// unchanged): how many of the active shards ONE DB's writes are allowed to
// spread across (see EnqueueWalRecord). Raising this does NOT create new
// writer-thread capacity -- active shard count is fixed by hardware
// (above), independent of DB count or this value; `min(active, this)` is
// the real cap. Helps only when there are few hot DBs relative to active
// shard count (a value of 1 funnels each DB onto exactly 1 of possibly
// many idle shards). Actively HURTS when DB count already meets/exceeds
// active shard count: those DBs already share shards by pigeonhole even
// at 1, and spreading each one across MORE of that same small pool just
// makes assignment less sticky -- more batches end up mixing records
// from different DBs, which pays a separate pwrite+fdatasync per
// distinct file within one batch anyway, not a shared one, for no
// benefit. Whether raising this helps AT ALL is also storage-device-
// dependent (deep-queue NVMe vs. a device that can't actually flush
// multiple streams in parallel) -- benchmark before committing to a
// non-default value, don't assume. Compile-time, not runtime-mutable: a
// mid-run change would need to reconcile with the process-wide-static
// wal_shards array and any in-flight records already hashed under the
// old span.
//
// Set to 4 (from 1) to address a measured INSERT_BATCH_RAND regression at
// high client concurrency (+8.3% at 32 clients, +25.4% at 64 clients vs.
// main): BatchInsert's per-call-thread salting means many concurrent
// client threads funneling one hot DB's writes onto a single shard was
// the contention point, not per-record imbalance within a call. 4 (not
// WAL_SHARD_COUNT=8) is a deliberate middle ground -- spreads enough to
// relieve that single-shard contention while still keeping each DB's
// writes concentrated on a handful of shards, preserving most of the
// fsync-amortization benefit described above. See async-batch-range-wal
// task-10 report for the benchmarked comparison across shard counts.
//
// NOTE: This value applies only to the current single-node system. When
// the distributed design (docs/XStore_Distributed_Architecture.md) is
// implemented, this MUST revert to 1 -- per-slot LSN ordering depends on
// all records for a slot passing through a single WAL writer thread.
inline constexpr size_t      WAL_SHARDS_PER_DB           = 4;

// Adaptive WAL writer polling. After a batch is dequeued, the writer stays
// in a tight try_dequeue_bulk spin (bypasses wait_dequeue_bulk_timed's
// LightweightSemaphore entirely -- no CAS-spin-then-park, just a raw
// non-blocking poll) for up to this many microseconds of continued silence
// before falling back to the normal blocking wait. Real client traffic's
// inter-request gap (~1.1-1.2ms, measured) exceeds the semaphore's own
// ~10k-iteration spin window (~5us, measured via spin_measure.cpp), which
// is what pays the ~350-800us OS park/wake tax on the NEXT commit after any
// gap -- this window is sized to outlast that gap so a shard mid-burst
// never pays it, while a genuinely idle shard still falls back to near-zero
// CPU (see WAL_GROUP_COMMIT_WINDOW_US) once the window lapses. Unlike a
// permanent poll-always mode (spin forever = ~100% of one core per active
// shard even at total idle, rejected -- see fsync_floor/spin_measure
// investigation notes), this only spends CPU right after real activity,
// not on shards nobody is using. Default 0 disables adaptive polling
// entirely (falls straight to wait_dequeue_bulk_timed every iteration,
// today's exact behavior) -- opt in after benchmarking, same as
// WAL_SHARDS_PER_DB.
//
// Measured on torusnode15 (real client machines, real network, SYNC --
// GROUP_COMMIT at the time of this measurement, since merged into SYNC):
// with the SMALL (~8-byte) row payload used in early testing, showed no
// latency change at 1500us or 50000us, and live per-thread CPU sampling
// (/proc utime+stime deltas, not ps's decaying %cpu average) showed the
// writer thread almost never actually spinning at that payload size.
// RE-TESTED with a realistic ~1.5KB row payload at t=1/16/64 real
// concurrent clients (properly spread across multiple client machines):
// this DOES show a real, consistent win at every concurrency level --
// p50 latency dropped ~8% at t=1 and t=16, ~3.4% at t=64 (never a
// regression at any tested point). Payload size matters here: a bigger
// row means more per-batch serialize/CRC/write work, which changes the
// writer thread's real idle-vs-busy timing enough to make the semaphore
// park/wake path get hit meaningfully more often than the small-payload
// test suggested. Conclusion from the earlier (small-payload) measurement
// -- "not the bottleneck" -- does not hold at realistic row sizes.
// Shipped ON (50ms): real, measured win at every concurrency level tested
// (1/16/64 real clients, ~3.4-8%), never a regression -- decided default-on
// given this project's priority is latency, not idle-CPU minimization on
// this deployment. Costs ~1 core per active shard (4-8, not all
// WAL_SHARD_COUNT) near 100% even at total idle -- re-evaluate if deployed
// somewhere that tradeoff doesn't hold (e.g. a cost-constrained box with no
// spare cores).
inline constexpr unsigned    WAL_ADAPTIVE_POLL_WINDOW_US = 50000;

// Same idea as WAL_ADAPTIVE_POLL_WINDOW_US above, applied to each ZMQ
// worker thread's socket.recv() instead of the WAL writer's queue wait --
// see WorkerRoutine (ZMQ_Server.cpp). Shipped ON (50ms): measured a real
// ~37% p50 latency win at t=1, and no regression at t=16 (noise-level, all
// 16 workers were already ~100% spin-serviced at both levels -- confirmed
// via direct per-worker instrumentation, not assumed). Costs more than the
// WAL writer's own poll above: ALL num_threads workers spin, not just
// active shards, and -- per real measurement -- that's true even under
// single-client traffic (ZMQ's DEALER round-robins fairly across every
// registered worker regardless of actual concurrent client count, so a
// worker only needs to be revisited within its own window to stay hot,
// which happens for all of them even at t=1). Re-evaluate on a deployment
// that's usually busy with no idle stretches, or one without spare
// cores/SMT lanes for this to share with other work.
inline constexpr unsigned    ZMQ_ADAPTIVE_POLL_WINDOW_US = 50000;

// Debug/benchmark-only (see the XSTORE_WAL_STATS CMake option): how often
// WalStatsReporterLoop logs each shard's average batch size. Only compiled
// in / used when that flag is on -- harmless as a plain constant otherwise.
inline constexpr unsigned    WAL_STATS_LOG_INTERVAL_S    = 10;

// Group-commit gather window: a real, bounded LINGER after the first
// record arrives -- not just a couple of free non-blocking peeks (that
// version measured out to ~zero real batching under load: a peek only
// catches something already sitting in the queue at that exact instant).
// The writer actively waits (short poll loop, not the full window as one
// sleep) until EITHER the window has elapsed OR the batch hits
// WAL_BATCH_MAX_RECORDS, whichever comes first -- same dual-trigger shape
// as Kafka producer batching (linger.ms + batch.size). This DOES cost a
// lone writer up to the window's added latency every commit, even with
// nothing else to batch with.
//
// The window itself is now ADAPTIVE, not a single flat constant (an
// earlier fixed 500us was measured on real hardware/real clients to cost
// ~500-800us of latency for no real batching benefit whenever a shard's
// traffic was sparse -- confirmed via avg_batch_size staying close to 1.0
// under low concurrency). Each shard's writer thread tracks its own
// exponential moving average of recent batch sizes (thread-local, no
// locks/atomics -- only that shard's own writer thread ever reads or
// writes it) and derives its NEXT window from it: close to
// WAL_GATHER_WINDOW_MIN_US when the EMA is near 1.0 (no real grouping
// happening, don't make this shard pay for nothing), scaling up toward
// WAL_GATHER_WINDOW_MAX_US as the EMA climbs (real concurrent batching is
// happening on this shard, worth paying more to catch more of it). A
// sparse/lone-writer shard self-tunes down near MIN (matching the real
// measured floor); a genuinely concurrent shard self-tunes up toward MAX
// (preserving the original amortization benefit) -- automatically, per
// shard, without assuming the traffic shape up front.
//
// MAX_US swept on torusnode15 against {250, 500, 750, 1000} x ALPHA
// {0.1, 0.2, 0.4}, then the winner re-tested across client counts
// {1, 4, 16, 32, 64} (each spread across up to 16 threads/machine over
// multiple real client machines, not piled onto one -- piling threads past
// a client machine's own core count confounds the measurement with
// client-side NUMA/overload, not server behavior). 250us won cleanly at
// EVERY concurrency level tested, including 64 clients against this
// server's 16 worker threads (deliberate over-provisioning): a higher
// MAX_US measurably raised avg_batch_size (more records/batch under real
// concurrency) but that batching NEVER translated into lower latency on
// this hardware -- e.g. at 64 clients, MAX_US=1000 reached avg_batch_size
// 15.9 vs 250's 8.3, yet still measured slower (11.4ms mean vs 8.7ms).
// This cluster's actual bottleneck isn't fsync-call-count amortization, so
// paying extra linger for more batching is pure latency cost here with no
// offsetting throughput win. Re-benchmark before raising this on different
// storage/hardware -- this conclusion is a measured fact about this
// device, not a universal one.
inline constexpr unsigned    WAL_GATHER_WINDOW_MIN_US    = 50;
inline constexpr unsigned    WAL_GATHER_WINDOW_MAX_US    = 250;
// EMA smoothing factor for the batch-size average above: new_ema =
// ALPHA*batch_size + (1-ALPHA)*old_ema. Higher = adapts faster to recent
// traffic changes but noisier off a single outlier batch; lower = smoother
// but slower to react. Swept alongside MAX_US above (0.1/0.2/0.4): effect
// was noise-level at low concurrency, but 0.1 was part of the combo that
// won cleanly at every concurrency level in the follow-up sweep, and never
// lost to 0.2/0.4 in any single measurement -- no evidence favors a faster
// reaction here on this hardware.
inline constexpr double      WAL_GATHER_ADAPT_ALPHA      = 0.1;
// EMA at/below this maps to WAL_GATHER_WINDOW_MIN_US; EMA at/above this
// maps to WAL_GATHER_WINDOW_MAX_US, linearly interpolated between. 4.0
// chosen as "a real, multi-record batch is consistently forming" --
// higher than the bare >1.0 threshold for "any grouping at all" so a
// single occasional 2-record batch doesn't swing the window to MAX.
inline constexpr double      WAL_GATHER_ADAPT_EMA_SATURATE = 4.0;
// Poll interval while waiting out the gather window above.
inline constexpr unsigned    WAL_GATHER_POLL_US          = 50;

// Real block allocation (fallocate, not a sparse ftruncate) reserved once
// per WAL shard file so ordinary commits never pay a filesystem metadata
// journal update (block alloc + extent-map growth) on top of the data
// fsync -- see the removed per-commit ftruncate(fd,0) this replaces.
//
// Sized to make growth-past-preallocation structurally IMPOSSIBLE for
// today's only WAL producer, not just unlikely: the bound below is the
// true worst case allowed by the wire format's field widths (dbNameLen/
// rowLength are each a wire u16, so 65535 is a real reachable max, not a
// practical guess), times WAL_BATCH_MAX_RECORDS. This is what lets the
// writer loop use a plain fdatasync() instead of fsync() unconditionally
// (XStore_Wal.cpp) -- fdatasync() alone is only correct when the pwrite
// it syncs never changes file size/metadata, which requires the write to
// always land inside the preallocated region. The static_assert below
// enforces that invariant so it can't silently break if WAL_BATCH_MAX_
// RECORDS is ever raised.
//
// NOTE: this bound was originally sized assuming every WalRecord carries
// exactly 1 row -- true only for UnaryInsert. BatchInsert/RangeInsert now
// route through this same WAL path too (one WalRecord per year-bucket
// group, see XStore_BatchOps.cpp/XStore_RangeOps.cpp) and can legitimately
// hand WalWriterLoop a single record with hundreds of thousands of rows,
// so WAL_MAX_BATCH_BYTES below is NOT a real structural ceiling on a
// batch's total wire bytes for multi-row producers -- the static_assert
// below only guarantees the single-row-per-record case stays inside the
// preallocated region. Rather than bound row count per record (would need
// threading a cap through both BatchInsert's and RangeInsert's grouping
// logic), WalWriterLoop carries a runtime size check instead: it sums each
// file's total bytes for the current batch pass before writing, and if
// that would grow the file past WAL_FILE_PREALLOC_BYTES, it falls back to
// a real fsync() (not just fdatasync()) for that one write -- correct
// either way, since fsync() also flushes the size-changing metadata-journal
// update fdatasync() alone would miss -- and logs a warning so an
// oversized batch remains diagnosable instead of silently defeating the
// preallocation's whole point. See the WalWriterLoop per-file write block
// (XStore_Wal.cpp) for the actual check. ReplayWal (XStore_Management.cpp)
// carries a matching sanity bound (WAL_REPLAY_MAX_FILE_BYTES below) so a
// shard file grown past this bound can't turn crash-recovery startup into
// an unbounded single-shot allocation.
inline constexpr size_t WAL_RECORD_HEADER_BYTES = 20; // recordLen(4)+crc32c(4)+dbNameLen(2)+rowLength(2)+timeResolution(4)+rowCount(4) -- see SerializeWalRecord
inline constexpr size_t WAL_MAX_DB_NAME_BYTES   = 65535; // dbNameLen wire field is a u16
inline constexpr size_t WAL_MAX_ROW_BYTES       = 65535; // rowLength wire field is a u16
inline constexpr size_t WAL_MAX_RECORD_BYTES    =
    WAL_RECORD_HEADER_BYTES + WAL_MAX_DB_NAME_BYTES + sizeof(uint64_t) /* epoch_time */ + WAL_MAX_ROW_BYTES;
inline constexpr size_t WAL_MAX_BATCH_BYTES     = WAL_BATCH_MAX_RECORDS * WAL_MAX_RECORD_BYTES;

inline constexpr size_t      WAL_FILE_PREALLOC_BYTES     = 40u * 1024 * 1024;
// Guarantees the single-row-per-record case (UnaryInsert, WAL_BATCH_MAX_
// RECORDS records each carrying its own single row) always stays inside
// the preallocated region -- fdatasync() alone is provably correct for
// that producer, no runtime check needed. Does NOT bound a multi-row
// record (BatchInsert/RangeInsert, unlimited rows per record) -- see the
// NOTE above and WalWriterLoop's runtime oversized_write check, which is
// what makes the multi-row case correct now that this assert alone can't.
static_assert(WAL_FILE_PREALLOC_BYTES >= WAL_MAX_BATCH_BYTES,
              "WAL_FILE_PREALLOC_BYTES must cover the worst-case single-row-per-record batch "
              "(see WalWriterLoop's runtime oversized_write check for the multi-row case).");

// Sanity ceiling for ReplayWal's single-shot std::string read
// (XStore_Management.cpp): a shard file is normally exactly
// WAL_FILE_PREALLOC_BYTES, but the oversized_write fallback above now lets
// a single huge Batch/Range record legitimately grow it past that. This
// multiple gives generous headroom for a real oversized batch while still
// bounding the crash-recovery-path allocation against a corrupted or
// maliciously huge WAL file turning into an unbounded-memory startup
// crash. A shard file over this bound is logged and skipped (its replay
// is lost) rather than read into memory -- a deliberate, narrow tradeoff:
// only a single batch whose own on-wire size exceeds this bound is at
// risk, and only if the process also crashes before that batch's rows
// were applied to the real data file.
inline constexpr size_t      WAL_REPLAY_MAX_FILE_BYTES   = 8 * WAL_FILE_PREALLOC_BYTES;

// Enqueue-side backpressure: once a shard's queue depth reaches this many
// pending records, EnqueueWalRecord blocks the calling (client-handling)
// thread -- polling every WAL_BACKPRESSURE_POLL_US -- until the writer
// thread drains it back down, instead of accepting unboundedly and
// risking unbounded memory growth (and, for ASYNC, an ever-growing
// crash-loss window while still telling the client "success"). Applies
// uniformly to every durability mode; deliberately makes the client WAIT
// rather than rejecting the write.
inline constexpr size_t      WAL_QUEUE_HIGH_WATERMARK    = WAL_BATCH_MAX_RECORDS * 4;
inline constexpr unsigned    WAL_BACKPRESSURE_POLL_US    = 200;

// RangeQuery and BatchQuery (XStore_RangeOps.cpp,
// XStore_BatchOps.cpp): target size of each emitted wire-format chunk. A
// query whose total output stays under this is sent as a single ZMQ frame
// (chunking is not a separate path -- it's just what a small query
// naturally never triggers); a bigger query streams as multiple frames of
// roughly this size each, bounding server peak memory to ~this value
// regardless of total row count. Shared by both ops since they emit the
// identical wire shape (repeated UnaryQuery_REP) via the same encoder --
// see XStore_WireEncode.h.
//
// Swept 8/16/32/64/128/256/512 MB on torusnode15 against a real 15M-row/
// 975MB-wire RangeQuery (BACKTESTER, rowLength=64) over a loopback client:
// latency stayed flat (5.7-6.3s) across every chunk size -- ZMQ's send()
// buffers locally rather than blocking on frame count, so per-frame
// overhead doesn't show up on a low-latency link the way it would over a
// real WAN to a desktop client. Peak RSS climbed with chunk size (1.91GB at
// 8MB -> 2.49GB at 512MB), though most of that floor is unavoidable mmap'd
// source-data residency from the madvise(WILLNEED) prefetch (same for every
// chunk size), not the chunk buffer itself -- 64MB (this default) sits
// right where RSS is still close to the 32MB/8MB end (1.99GB, vs 1.96GB and
// 1.91GB) while keeping frame count low (15 frames for this query, vs 117
// at 8MB) -- fewer frames matters once a real network's per-message
// overhead isn't zero, which this loopback benchmark can't measure. Same
// query against the pre-chunking object-tree RangeQuery peaked at 6.6GB
// RSS for comparison -- see the RangeQuery OOM handoff doc's history.
inline constexpr size_t      STREAMED_QUERY_CHUNK_SIZE_BYTES = 64ull * 1024 * 1024;

// SysStatsCollector (XStore_SysStats.*): how often the background host-metrics
// thread samples /proc + statvfs and re-publishes on its dedicated PUB
// socket. 2s matches the desktop viewer's poll/render cadence -- a shorter
// interval buys nothing (the viewer can't render faster than it renders)
// while raising wakeup count on a thread that's deliberately SCHED_IDLE, and
// a longer one would make CPU%/IOPS/net-rate deltas laggy relative to what
// the panel implies it's showing. This thread never touches the WAL/ZMQ
// hot path, so this knob has zero effect on insert/query latency either way.
inline constexpr unsigned    SYS_STATS_COLLECT_INTERVAL_MS = 2000;

// BatchQuery (XStore_BatchOps.cpp) + XStorePreadPool
// (XStore_PreadPool.*): pread() is a syscall -- on BatchQuery's scattered
// access pattern (the whole reason it uses pread-per-row instead of mmap,
// see there) a large batch pays hundreds of thousands of these, serially,
// on one thread. Two separate costs hide in that: (a) fixed per-syscall CPU
// overhead (mode switch, page-cache lookup), paid whether the row is cached
// or not; (b) real device wait when a row is NOT in page cache -- a single
// blocking thread can only ever have one read in flight, so a device's own
// concurrency (NVMe/SSD: dozens of outstanding requests; a spinning disk
// with NCQ: a much smaller but still real handful) goes unused. Fanning
// pread() out across threads attacks both: divides the fixed overhead
// across cores, and lets multiple reads be in flight at once.
//
// BATCH_QUERY_PREAD_POOL_SIZE_CAP is how many worker threads actually
// exist to do this, system-wide -- not "how many threads this one query
// spawns". XStorePreadPool starts this many threads ONCE at process
// startup (first use) and every BatchQuery call, from every
// concurrent client, submits its pread work to that SAME fixed pool. That's
// the difference between a real device-concurrency cap and a per-call fan-
// out limit: with the latter, two concurrent queries each capped at K
// threads still put 2K readers on the disk at once; with a shared pool
// capped at K, the whole server never exceeds K concurrent preads no matter
// how many clients are querying. This is what makes the cap trustworthy on
// a spinning disk under real concurrent load, not just in a single-query
// benchmark.
//
// Tune by device: NVMe/SSD (deep hardware queue) wants this reasonably
// high. A spinning disk with NCQ still benefits from a SMALL amount of
// concurrency (2-4 -- lets the drive's scheduler reorder/merge nearby
// seeks) but degrades past that as the head thrashes between unrelated
// requests; a disk with no queuing at all wants 1 (= fully serial, single
// reader at a time -- never worse than the pre-optimization pread-per-row
// loop, since a 1-worker pool degenerates to exactly that). Benchmark
// before trusting the default on unfamiliar storage -- don't assume, same
// rule as WAL_SHARDS_PER_DB above.
//
// THE ACTUAL POOL SIZE IS NOT THIS CONSTANT -- it's computed at runtime
// (XStorePreadPool::PoolSize, XStore_PreadPool.cpp) as hardware_concurrency()
// minus however many ZMQ worker threads THIS server instance was actually
// started with (XStorePreadPool::Init, called once from main.cpp right
// after -t/--threads resolves to its final value), clamped to
// [BATCH_QUERY_PREAD_POOL_SIZE_MIN, this cap]. Measured need for this: a
// flat hardware_concurrency()-sized pool regressed BatchQuery throughput at
// high client concurrency (32/64 clients) on a 16-core/32-thread box --
// the pool's own worker threads were competing with the ZMQ worker threads
// (also sized from hardware_concurrency()) for the same physical cores,
// two independently-sized consumers oversubscribing one budget. Sizing off
// "cores minus what ZMQ already claimed" fixes this on ANY machine
// automatically -- a small box naturally gets a small pool, a big box a
// big one, without a manual per-deployment multiplier. This cap remains a
// hard ceiling on top of that (mainly relevant on a very large box, where
// "cores minus ZMQ threads" alone could still exceed how many concurrent
// preads are actually useful against one storage device).
//
// BATCH_QUERY_PARALLEL_PREAD_MIN_ROWS is a separate, smaller guard: below
// this many rows in one year-bucket, skip the pool entirely and pread
// serially inline -- avoids paying a job-submit + semaphore-wait round trip
// for a handful of rows where the overhead could plausibly exceed the work.
//
// BATCH_QUERY_PREAD_WAVE_ROWS bounds the scratch buffer used for one
// dispatch to a fixed size independent of the batch's total row count -- a
// 500k-row batch still only ever holds this many rows' worth of scratch
// memory live at once, preserving the same server-memory-bounded design
// BatchQuery's chunking already gives on the encode side. Decoupled
// from pool size on purpose: this bounds MEMORY per dispatch, the pool cap
// above bounds DEVICE CONCURRENCY -- unrelated resources, no reason one
// number should have to serve both.
inline constexpr size_t      BATCH_QUERY_PREAD_POOL_SIZE_CAP      = 32;
// Floor under the "cores minus ZMQ threads" computation -- a heavily
// loaded/small box (e.g. ZMQ threads already >= hardware_concurrency())
// still gets at least this many pread workers rather than the pool
// disappearing to 0. 1 is always safe (degenerates to the pre-optimization
// serial pread-per-row loop, never worse); raised slightly above 1 by
// default so a tight box still gets a little real concurrency rather than
// none.
inline constexpr size_t      BATCH_QUERY_PREAD_POOL_SIZE_MIN      = 2;
inline constexpr size_t      BATCH_QUERY_PREAD_WAVE_ROWS          = 4096;
inline constexpr size_t      BATCH_QUERY_PARALLEL_PREAD_MIN_ROWS  = 256;

// TRIED AND REVERTED: fanning RangeQuery's per-row wire-encode
// transform out across XStorePreadPool, the same way BatchQuery's pread
// fan-out uses it. Measured on Mystic (real 8-machine multi-client sweep,
// 500k-row RangeQuery, BACKTESTER's 64-byte rows): 41-77% THROUGHPUT
// REGRESSION at every concurrency level (1/8/16/32/64 clients), not a
// small one. Root cause: BatchQuery's dispatched unit of work is a real
// blocking pread() syscall (microseconds of actual I/O wait), so a wave's
// job-submit-and-wait round trip through the pool is negligible next to
// it. RangeQuery's read side is already free (mmap + madvise(WILLNEED) --
// page-cache-resident, no syscalls at all), so its dispatched unit of work
// is just a few dozen nanoseconds of in-memory transform (tag byte,
// varint, memcpy) -- cheaper than the dispatch overhead itself. A 500k-row
// query paying ~122 such wave round trips (at a 4096-row wave size) lost
// badly to a single core just running the plain loop. Larger wave sizes
// might not fix this either -- the per-row work may simply be too cheap to
// ever amortize a thread-pool hop, even a lock-free one. Don't re-attempt
// this without a cheap local microbenchmark FIRST (encode throughput
// with/without the fan-out, no network/Mystic round trip needed) -- that
// would have caught this before ever reaching real infrastructure. Might
// be worth revisiting once rows are large enough that the per-row
// transform itself is expensive (this measurement used 64-byte rows;
// unproven at, say, a 128-column/1KB-row dataset).

} // namespace XStoreTunables
