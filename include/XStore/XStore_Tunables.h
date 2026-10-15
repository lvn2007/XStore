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

// There is no "shards per DB" knob any more. EnqueueWalRecord picks the shard from the record's (db, year) alone
// (XStore::WalShardFor), never from the calling thread, so every write to one row goes through one shard and replay
// keeps acknowledgment order for SYNC inserts (ASYNC writes to one row from two threads have no defined order: the
// caller is acknowledged before the record is queued).
// History: this used to be WAL_SHARDS_PER_DB, which was 1, then set to 4 (salting the shard by calling thread) to
// fix a measured INSERT_BATCH_RAND regression at 32/64 clients. Now different DBs and different years spread
// across shards; a hot single-DB single-year workload funnels through ONE shard, like WAL_SHARDS_PER_DB=1 did.
// That is the accepted trade-off; measure it (INSERT_BATCH_RAND/SEQ at 32/64 clients).

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
// today's exact behavior) -- opt in after benchmarking, like any knob here.
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

// Graceful shutdown (ZmqServer::StartServer): after the ZMQ context is shut
// down, how long to wait for the worker threads to finish their in-flight
// request and exit before giving up. On timeout the process must NOT run
// XStore::Shutdown() (a worker may still be inserting) -- it exits with the
// indexes left dirty instead, which the next start rebuilds.
inline constexpr unsigned    ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S = 10;

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

// recordLen+crc (8) + fixed fields (12) = 20; the hdr.reserve() in WalWriterLoop adds the 8-byte LSN, the name and the
// 8-byte first epoch, so the reserve equals the built header size exactly.
inline constexpr size_t WAL_RECORD_HEADER_BYTES = 20; // recordLen(4)+crc32c(4)+dbNameLen(2)+rowLength(2)+timeResolution(4)+rowCount(4)

// Sanity ceiling for the legacy-format ReplayWal streaming read
// (XStore_Management.cpp), applied PER RECORD. Legacy shard files are read in
// bounded WAL_REPLAY_READ_CHUNK_BYTES-sized chunks, growing the buffer only as
// far as one record's own declared length needs, checked against this bound
// *before* trying to satisfy it -- so a corrupt record_len can never force an
// allocation anywhere near that size. A record declaring more than this is
// treated like any other unparseable/torn record: replay stops there for this
// shard, and every valid record before it is still applied.
//
// Deliberately well under 4 GiB (UINT32_MAX): the on-wire record_len field is
// a uint32_t, so any value at or above 2^32 would make the
// `declared_record_len > WAL_REPLAY_MAX_RECORD_BYTES` check impossible to ever
// trigger. 2 GiB stays comfortably below that while far exceeding any real
// single-record size.
inline constexpr size_t      WAL_REPLAY_MAX_RECORD_BYTES  = 2u * 1024 * 1024 * 1024;

// Read granularity for ReplayWal's streaming parse above -- how many more
// bytes to pull from disk at a time when the current record isn't fully
// buffered yet. Purely a throughput/memory-footprint tradeoff (more
// syscalls vs. more resident memory per shard during startup replay only);
// correctness doesn't depend on this value.
inline constexpr size_t      WAL_REPLAY_READ_CHUNK_BYTES  = 1u * 1024 * 1024;

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

// Zone-map secondary index (see docs/XStore_Technical_Reference.md section
// 4.14). Target BYTE size per block, not a fixed row count --
// XStore's fixed-width rowLength varies 10-100x across differently-shaped
// DBs, so a constant row count (e.g. ClickHouse's index_granularity=8192)
// would produce wildly different block byte sizes per DB. 2MB lands near a
// single efficient NVMe read chunk; re-sweep on Mystic before trusting this
// default in production (see Task 8's benchmark gate).
inline constexpr uint64_t ZONE_MAP_TARGET_BLOCK_BYTES = 2ull * 1024 * 1024;

// XStore has no declared column schema (CreateDB takes only rowLength, a
// byte budget -- see proto/XStore.proto's CreateDB_REQ). Tracking "one zone-
// map entry per column that could possibly fit in rowLength" was checked
// and rejected: it inflates zone-map file size past the feature's own
// near-zero-memory premise for wide-row DBs (a 1536-byte row would derive
// ~768 phantom tracked columns). This keeps tracked columns at a small,
// realistic count; column indices >= the width actually in force always
// fall back to a full scan for that column -- correct, just not accelerated.
//
// This is a DEFAULT AND A FLOOR, not a hard cap -- it stopped being a cap
// when ConfigStruct.columnCount landed. XStore::EffectiveTrackedColumnCount
// returns max(cfg.columncount(), this), so a DB whose rows are genuinely
// wider tracks more than this, and a DB that has only ever seen narrow rows
// still tracks exactly this many (the floor is load-bearing for
// correctness, not just for acceleration -- see that function's comment in
// XStore.h). The real upper bound on tracked columns is
// MAX_TRACKED_COLUMN_COUNT below, which is what bounds the allocations this
// number used to bound.
inline constexpr uint32_t ZONE_MAP_MAX_TRACKED_COLUMNS = 64;

// Hard upper bound on a DB's ConfigStruct.columnCount -- both the seed a
// client may pass to CreateDB and the high-water mark MaybeGrowColumnCount
// will grow to from observed rows. columnCount directly sizes
// std::vector<ZoneMapEntry> allocations (ZoneMapFile::EnsureBlockLoaded /
// TryGetBlockStats) and BuildZoneMapObservations' reserve(), and it is
// persisted to .CFG, so an unvalidated value straight off the wire is a
// durable remote denial of service: CreateDB(columnCount=0xFFFFFFFF) makes
// every later insert on that DB throw std::length_error out of a ZMQ worker
// thread, and the poisoned value survives on disk to do it again on every
// retry. 4096 is far past any realistic row (a 65535-byte row cannot hold
// 4096 meaningfully-populated protobuf fields) while capping one block's
// entry vector at 4096 * 48 = 192KB.
//
// Enforced on the READ side too, by XStore::ClampPersistedColumnCount --
// guarding only the two write paths above leaves a .CFG written by an older
// binary (or restored/corrupted) free to reintroduce the same allocation,
// this time from InitDB(), i.e. at ZmqServer worker construction.
inline constexpr uint32_t MAX_TRACKED_COLUMN_COUNT = 4096;

// How many blocks' stats a ZoneMapFile keeps resident at once, per (db,
// year) file, purely to avoid a repeat pread() when consecutive updates hit
// the same block (the common sequential-append case) -- every entry here is
// always disk-coherent (write-through, see ZoneMapFile::UpdateRow), so this
// is a pure performance cache, not a durability buffer. 4 blocks * 64
// tracked columns * 80 bytes/entry (ZoneMapEntry, v3 -- 40 bytes originally,
// 48 after sum_sq_value for VARIANCE/STDDEV, 80 after first_value/last_value/
// first_row_index/last_row_index for OHLC) = 20,480 bytes resident
// per (db, year) file with this default -- negligible on its own; see
// ZONE_MAP_REGISTRY_CAP below for the bound on how many (db, year) files can
// be resident at once, which is what actually keeps total memory bounded.
inline constexpr size_t ZONE_MAP_HOT_CACHE_BLOCKS = 4;

// How many ZoneMapFile instances (one per (db, year) pair ever touched) the
// registry keeps resident at once (see XStore::GetOrOpenZoneMap). Same
// discipline as MMAP_CACHE_CAP/FD_CACHE_CAP above: every other per-key cache
// in this codebase is capped, and a zone-map registry with no cap would grow
// for the lifetime of a long-running server across every distinct (db,
// year) combination it ever sees -- slow, but genuinely unbounded, which
// directly conflicts with XStore's minimal-memory-footprint goal. Eviction
// here is trivially safe and cheap (unlike the mmap/fd caches): every
// ZoneMapFile update is already write-through, so dropping an instance
// loses nothing -- the next touch just reopens the file and pays one fresh
// pread() for whatever block it needs. 256 instances * ~12KB each (default
// ZONE_MAP_HOT_CACHE_BLOCKS) = ~3MB worst-case resident for this cache --
// re-sweep alongside the Task 8 benchmark gate before trusting this default,
// same rule as every other capacity constant in this file.
inline constexpr size_t ZONE_MAP_REGISTRY_CAP = 256;

// ## CatIdx (categorical/high-cardinality equality-lookup acceleration)
//
// Separate sidecar file from .zmap (see docs/superpowers/specs/
// 2026-09-21-categorical-high-cardinality-acceleration-design.md for why:
// independent version churn, no cache-footprint regression to existing
// zone-map-only queries, isolated corruption blast radius).
inline constexpr uint32_t CATIDX_VERSION = 2;

// k=7 hash functions, 160 bytes (1280 bits) per tracked column per block.
//
// KNOWN UNDERSIZED -- these numbers were derived for n=128 distinct values
// per block, which is NOT the real block geometry: RowsPerBlock(row_length)
// is ZONE_MAP_TARGET_BLOCK_BYTES / row_length (2MiB / row_length), i.e.
// 32,768 rows/block at row_length=64 and 128 only at row_length=16384. At a
// realistic few hundred to few thousand distinct values per block, 1280 bits
// with k=7 saturates and the filter's effective selectivity degrades toward
// zero pruning -- well before the high cardinalities this feature is named
// for. This costs performance ONLY: a bloom filter can only ever
// over-approximate, so a saturated filter just stops skipping blocks and
// every query falls through to the real-scan path, which is always correct.
// Retuning these two constants needs real cardinality data and benchmarking
// (bigger entries cost disk on every block x every tracked column); tracked
// as a follow-up in TODO.md, deliberately not guessed at here.
inline constexpr size_t   CATIDX_BLOOM_HASH_COUNT = 7;
inline constexpr size_t   CATIDX_BLOOM_BYTES_PER_ENTRY = 160;

// FindByPattern (XStore_PatternMatch.*): a request's pattern longer than this
// is rejected before compiling, so a hostile request can't force a huge LIKE
// segment table or RE2 program.
inline constexpr size_t   FIND_BY_PATTERN_MAX_PATTERN_BYTES = 4096;

// RE2::Options::max_mem for one FindByPattern REGEX query (compiled program +
// DFA cache). RE2's own default, made explicit so it's a visible bound on
// per-query memory; a regex that needs more fails to compile, it never OOMs.
inline constexpr int64_t  FIND_BY_PATTERN_REGEX_MAX_MEM_BYTES = 8ll << 20;

// Same per-DB high-water-mark tracked-column-count concept zone-map uses
// (ConfigStruct.columncount) -- reuse the existing cap rather than a second one.
inline constexpr uint32_t CATIDX_MAX_TRACKED_COLUMNS = ZONE_MAP_MAX_TRACKED_COLUMNS;

// Max resident CatIdxFile instances across all (db,year) pairs -- same
// bound/reasoning as ZONE_MAP_REGISTRY_CAP.
inline constexpr size_t   CATIDX_REGISTRY_CAP = 256;

// Per-(db,year) bloom-block LRU cache size -- same reasoning as ZONE_MAP_HOT_CACHE_BLOCKS.
inline constexpr size_t   CATIDX_HOT_CACHE_BLOCKS = 4;

// Startup index rebuild (XStore::RebuildYearIndexes): a year file is scanned
// once, in chunks of whole zone-map blocks of about this many bytes (at least
// one block). Each chunk's rows are parsed once into reusable observation
// batches, then folded into each stale index with one mutex hold and one
// write per block (ZoneMapFile/CatIdxFile::RebuildFoldRows). The batches cost
// about 48 bytes per present column per row, i.e. roughly 3-4x this value for
// a dense numeric row (worst case 24x: every column a 2-byte empty field), per
// year rebuilt concurrently, plus the 1 MiB scan buffer. Smaller only adds
// per-chunk overhead (three lseeks and two mutex rounds per chunk).
inline constexpr uint64_t REBUILD_CHUNK_BYTES = 4ull * 1024 * 1024;

// Startup index rebuild across cores (XStore::RebuildYearsIndexes): every year
// is split into groups of consecutive chunks of about this many bytes (rounded
// UP to whole chunks, at least one), and dedicated threads fold different
// groups at once, so one big year uses every core. A group is the unit of work
// claimed; smaller groups balance better at the end of a year, larger ones
// keep the frontier further behind a slow group (see the contiguous watermark
// in Tech Reference 4.14).
inline constexpr uint64_t REBUILD_PARALLEL_GROUP_BYTES = 64ull * 1024 * 1024;
// Startup rebuild progress (XStore_RebuildProgress.h): on a terminal one progress bar, redrawn every
// REBUILD_PROGRESS_BAR_INTERVAL_MS; anywhere else (nohup, a file, TERM=dumb) one spdlog info line every
// REBUILD_PROGRESS_LOG_INTERVAL_SEC seconds. Both only read one relaxed atomic per rebuilt chunk.
inline constexpr unsigned REBUILD_PROGRESS_LOG_INTERVAL_SEC = 10;
inline constexpr unsigned REBUILD_PROGRESS_BAR_INTERVAL_MS = 500;
// Upper bound on the startup rebuild threads (each holds a 1 MiB read buffer plus fold batches of about 15-18 MB, so
// 64 threads is about 1.2 GB); the old shared pool was capped at 32.
inline constexpr unsigned REBUILD_MAX_THREADS = 64;

// Live index widening (XStore::LiveWidenOneYear, Tech Reference 4.18): the
// builder rebuilds a year's .zmap/.catidx in place while the server serves,
// one chunk of rows at a time under the data file's READ range lock (writers
// of those rows wait while it is held). Owner-approved defaults: about 5 ms
// of lock hold per chunk, about 1 ms pause between chunks. The chunk size
// adapts: after each chunk that read rows, rows *= target / observed hold
// (the factor clamped to [0.5, 2] so one slow chunk cannot swing it).
inline constexpr uint64_t LIVE_WIDEN_LOCK_HOLD_TARGET_US = 5000;
inline constexpr uint64_t LIVE_WIDEN_PAUSE_US = 1000;
// The lock is taken with a timed wait (FileRangeLock::TryLockRangeFor), so a
// hot writer never makes the builder queue behind it indefinitely. On a
// timeout the chunk is halved (down to the minimum) and the SAME chunk is
// retried after a pause that doubles on each consecutive timeout, up to the
// max pause (F is one contiguous frontier: a busy chunk cannot be skipped).
inline constexpr uint64_t LIVE_WIDEN_LOCK_WAIT_MS = 50;
inline constexpr uint64_t LIVE_WIDEN_MAX_PAUSE_US = 50000;
// Writers first (FileRangeLock::TryReadLockYieldingFor): a chunk's READ lock
// also waits while an overlapping WRITE is waiting, so back-to-back builder
// READs cannot starve a wide writer. Capped per lock attempt (from its start)
// so a steady stream of waiting writers cannot stall the widen forever; past
// the cap the attempt waits for held locks only. Yield time does not count
// against LIVE_WIDEN_LOCK_WAIT_MS.
inline constexpr uint64_t LIVE_WIDEN_YIELD_MAX_MS = 200;
// A blocked WRITE in FileRangeLock that waited at least this long also counts as a long wait (WriterWaits()).
inline constexpr uint64_t LIVE_WIDEN_WRITER_LONG_WAIT_US = 25000;
// After this many consecutive timeouts at the minimum chunk the builder logs
// a warning once (per stretch of timeouts) and keeps retrying.
inline constexpr uint64_t LIVE_WIDEN_TIMEOUT_WARN_AFTER = 100;
// Chunk bounds in rows. The minimum is ONE row: under a hot writer the
// builder must be able to lock a range that ends between two busy rows, so
// it never needs neighbouring hot rows free at the same instant. The first
// chunk is small; the maximum is LIVE_WIDEN_MAX_CHUNK_BYTES worth of rows.
inline constexpr uint64_t LIVE_WIDEN_MIN_CHUNK_ROWS = 1;
inline constexpr uint64_t LIVE_WIDEN_INITIAL_CHUNK_ROWS = 256;
// Cap on the rows read under one lock hold: 1 MiB worth of rows (512 rows of
// 2 KiB, about 6-7 ms of decode and fold at the ~13 us/row measured for 100
// double columns), so the row count alone cannot stretch a hold to tens of
// ms (the startup chunk, REBUILD_CHUNK_BYTES, is 4x that). A single hold can
// still run long on a stall (preemption, a cold page-cache read, waiting for
// an index file's mutex); see Tech Reference 4.18.
inline constexpr uint64_t LIVE_WIDEN_MAX_CHUNK_BYTES = 1ull * 1024 * 1024;
// A locked range with no data rows (a sparse hole) costs one lseek under the
// lock, microseconds: each consecutive hole-only range is 4x the previous one,
// up to this many rows. When no data is left from F to the end of the year,
// the next attempt locks [F, end) once and finishes.
inline constexpr uint64_t LIVE_WIDEN_MAX_HOLE_SPAN_ROWS = 1ull << 20;
// Workers folding ONE year of a live widen at once (Tech Reference 4.18, "Parallel live widen"). 1 = the single
// sequential builder, unchanged. With N > 1 the year is cut into groups of about LIVE_WIDEN_GROUP_BYTES (rounded up
// to whole zone-map blocks; 8 maximum-size chunks), claimed in order by N dedicated threads, each chunk still under
// its own timed READ range lock. The load governor (S3) is meant to raise N when the machine is idle.
inline constexpr unsigned LIVE_WIDEN_WORKERS = 1;
inline constexpr uint64_t LIVE_WIDEN_GROUP_BYTES = 8 * LIVE_WIDEN_MAX_CHUNK_BYTES;
// Live-widen load governor (XStore_LiveWidenGovernor.h, Tech Reference 4.18 "Governor"). These are the defaults of
// GovernorParams. LIVE_WIDEN_GOVERNOR true: RunLiveWidenPass gives every DB's years one governor (cores - headroom
// workers, parked between groups while the governor denies them) instead of the fixed LIVE_WIDEN_WORKERS.
inline constexpr bool LIVE_WIDEN_GOVERNOR = false;                   // off: the fixed LIVE_WIDEN_WORKERS, as before
inline constexpr unsigned LIVE_WIDEN_FLOOR_WORKERS = 1;              // workers always allowed (0: the starvation guard)
inline constexpr unsigned LIVE_WIDEN_HEADROOM_CORES = 2;             // logical CPUs kept free for foreground work
inline constexpr unsigned LIVE_WIDEN_MAX_WORKERS_CAP = 16;           // max workers = min(cores - headroom, this)
inline constexpr uint64_t LIVE_WIDEN_GOVERNOR_BRAKE_HOLD_SEC = 10;   // no raise for this long after a writer brake
// Summed writer wait, in microseconds PER SECOND of the interval since the last evaluation (intervals vary:
// evaluations happen when a worker calls Permit), that brakes. G3 calibration (local 16-vCPU VM only, 4 SYNC writers,
// per-second samples): the 1-worker floor alone reaches p95 103-116k, p99 116-120k, max 120k (up to ~130k while
// governed), so the earlier 100000 braked the floor itself and kept the governor at 1 worker; 4 workers p50
// 107-145k, p95 166-175k. 150000 is above every 1-worker second seen and below the 4-worker p95; verified in a
// governed run (floor never braked, ramps to 3-8 then brakes). Recheck on Mystic.
inline constexpr uint64_t LIVE_WIDEN_WRITER_WAIT_BRAKE_US = 150000;
// Long waits (>= LIVE_WIDEN_WRITER_LONG_WAIT_US) since the last evaluation that brake (a plain count, not a rate: one
// long wait is one too many whatever the interval). G3 local VM: no wait >= 25 ms in any arm (1, 4 or governed
// workers, ~550 builder seconds); kept at 1, never the trigger there.
inline constexpr uint64_t LIVE_WIDEN_LONG_WAITS_BRAKE = 1;
inline constexpr uint64_t LIVE_WIDEN_STARVATION_GUARD_SEC = 600;     // floor 0 only: one-group grant after this
inline constexpr uint64_t LIVE_WIDEN_AT_FLOOR_WARN_SEC = 60;         // WARNING once per at-floor episode this long
inline constexpr unsigned LIVE_WIDEN_GOVERNOR_SMOOTHING_TICKS = 3;   // window for raises (drops use the last tick)

// Live widening trigger (M4 stage 3, XStore::RunLiveWidenPass). When an insert
// raises a DB's effective tracked width, the DB is recorded; once its width
// has not grown for this long (every further growth restarts the wait), the
// background index thread widens all of its year files, newest year first.
// Owner-approved: 30 s.
inline constexpr uint64_t LIVE_WIDEN_QUIET_PERIOD_SEC = 30;
// How often the background index thread looks for due widening work (and
// checks whether compaction is due). One second: a recorded DB is picked up
// at most about a second after its quiet period ends.
inline constexpr uint64_t LIVE_WIDEN_TICK_SEC = 1;
// A year whose rebuild could not even begin (BeginRebuild failed: the old,
// narrower index is intact and still used) is retried: at most this many
// attempts in all for one growth, the n-th retry waiting
// LIVE_WIDEN_RETRY_BASE_SEC * 2^(n-1) after the failed attempt. Then the DB is
// given up until its width grows again or the next start rebuilds it.
inline constexpr uint32_t LIVE_WIDEN_MAX_ATTEMPTS = 3;
inline constexpr uint64_t LIVE_WIDEN_RETRY_BASE_SEC = 60;
// Compaction yields to live widening: while a widen is recorded, the
// background index thread does not start compaction, and a running pass
// abandons its year at its next 64K-row check. Once no widen is recorded any
// more, the next compaction after any live widen runs this long after
// that tick instead of waiting out CATIDX_COMPACTION_POLL_INTERVAL_SEC.
inline constexpr uint64_t LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC = 30;
// RemoveDB / RemoveDBFiles of a year file stop the background index thread's
// work on that DB/year (widening or compaction) and wait at most this long for
// it to let go, then remove anyway (with an error in the log).
inline constexpr uint64_t BACKGROUND_REMOVE_WAIT_MS = 10000;

// Cardinality cap for the compaction-built bitmap section: a column whose
// distinct-value count in a given year crosses this threshold never gets a
// bitmap for that year (stays bloom+real-scan forever) -- this is what lets
// "automatic for every column" coexist with continuous (near-unique-value)
// columns without exploding disk usage. Chosen well above realistic
// categorical cardinalities (symbol/venue/status IDs) and well below where
// a per-value roaring bitmap directory stops paying for itself.
inline constexpr uint64_t CATIDX_MAX_BITMAP_DISTINCT_VALUES = 10000;

// How often the compaction background thread checks for newly-cold year
// files (year rolled over from current calendar year). Compaction is a
// rare, low-frequency background job -- no need for WAL-writer-loop-style
// sub-millisecond polling.
inline constexpr uint64_t CATIDX_COMPACTION_POLL_INTERVAL_SEC = 3600;

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

// FilesystemKeyStore (XStore_Curve.*): minimum gap between two mtime checks
// on the CURVE allowlist directory. Refresh() is invoked synchronously on
// every ZAP request (not on a background timer -- the ZAP handler thread
// already blocks on recv(), so there's no independent timer to run one
// against) -- this is a debounce so a burst of concurrent connection
// attempts pays one stat() call, not one per request.
inline constexpr unsigned CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS = 1000;

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
// rule as the other benchmark-first knobs above.
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

// --- WAL ring + paced checkpoint (Durability C) ---
inline constexpr uint64_t    WAL_RING_BYTES                = 64u << 20;   // per-shard ring size (bytes of log retained between checkpoints)
inline constexpr size_t      WAL_DIRECT_MIN_BYTES          = 1u << 20;    // batch/range inserts at least this big skip the WAL (direct apply)
inline constexpr size_t      WAL_DIRECT_PRESSURE_MIN_BYTES = 64u << 10;   // same bypass threshold while the ring is under pressure
inline constexpr double      WAL_CKPT_SOFT_FILL            = 0.25;        // ring fill fraction that starts an opportunistic checkpoint
inline constexpr double      WAL_CKPT_CYCLE_FILL           = 0.50;        // fill fraction that forces a full checkpoint cycle
inline constexpr double      WAL_STALL_FILL                = 0.90;        // fill fraction at which writers stall for the checkpoint
inline constexpr unsigned    WAL_STALL_TIMEOUT_MS          = 5000;        // max writer stall before the insert is failed
inline constexpr unsigned    WAL_CKPT_MAX_AGE_MS           = 5000;        // full checkpoint cycle once the oldest not-yet-flushed dirty-tracker entry is this old
inline constexpr unsigned    WAL_CKPT_IDLE_MS              = 50;          // idle time with no WAL traffic that triggers a checkpoint
inline constexpr size_t      WAL_FLUSH_CHUNK_BYTES         = 4u << 20;    // paced data-file flush granularity during a checkpoint

} // namespace XStoreTunables
