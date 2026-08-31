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
inline constexpr const char* WAL_DIR_NAME                = "WAL";

// WAL_SHARD_COUNT above is the compile-time ceiling (array size); the
// *active* shard/writer-thread count is computed at startup from
// hardware_concurrency(), clamped to [WAL_MIN_ACTIVE_SHARDS,
// WAL_SHARD_COUNT] -- see StartWalWriters. Writer threads are mostly
// blocked in fsync()/on the queue semaphore (near-zero idle CPU cost, see
// the WAL_GROUP_COMMIT_WINDOW_US comment above), so the real ceiling on
// "how many is too many" is contention at the storage device, not CPU --
// this stays a conservative fraction of core count rather than 1:1.
inline constexpr size_t      WAL_MIN_ACTIVE_SHARDS       = 2;

// Group-commit gather window: a real, bounded LINGER after the first
// record arrives -- not just a couple of free non-blocking peeks (that
// version measured out to ~zero real batching under load: a peek only
// catches something already sitting in the queue at that exact instant).
// The writer actively waits (short poll loop, not the full window as one
// sleep) until EITHER this many microseconds have elapsed OR the batch
// hits WAL_BATCH_MAX_RECORDS, whichever comes first -- same dual-trigger
// shape as Kafka producer batching (linger.ms + batch.size). Unlike the
// non-blocking version, this DOES cost a lone writer up to this much
// added latency every commit, even with nothing else to batch with --
// deliberate trade: kept short (500us, two orders of magnitude below
// WAL_GROUP_COMMIT_WINDOW_US) so that cost stays small, while still being
// long enough for concurrent writers on the SAME shard (which per-DB
// hashing now funnels together -- see EnqueueWalRecord) to land in the
// same batch and share its fsync.
inline constexpr unsigned    WAL_GATHER_WINDOW_US        = 500;
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
// NOTE: this bound assumes every WalRecord carries exactly 1 row (true
// for UnaryInsert, the only current WAL producer -- see EnqueueWalRecord
// call sites). A future multi-row Batch/Range WAL path would need to
// either bound row count per record too, or bring back a runtime size
// check before the live-commit fsync/fdatasync call.
inline constexpr size_t WAL_RECORD_HEADER_BYTES = 20; // recordLen(4)+crc32(4)+dbNameLen(2)+rowLength(2)+timeResolution(4)+rowCount(4) -- see SerializeWalRecord
inline constexpr size_t WAL_MAX_DB_NAME_BYTES   = 65535; // dbNameLen wire field is a u16
inline constexpr size_t WAL_MAX_ROW_BYTES       = 65535; // rowLength wire field is a u16
inline constexpr size_t WAL_MAX_RECORD_BYTES    =
    WAL_RECORD_HEADER_BYTES + WAL_MAX_DB_NAME_BYTES + sizeof(uint64_t) /* epoch_time */ + WAL_MAX_ROW_BYTES;
inline constexpr size_t WAL_MAX_BATCH_BYTES     = WAL_BATCH_MAX_RECORDS * WAL_MAX_RECORD_BYTES;

inline constexpr size_t      WAL_FILE_PREALLOC_BYTES     = 40u * 1024 * 1024;
static_assert(WAL_FILE_PREALLOC_BYTES >= WAL_MAX_BATCH_BYTES,
              "WAL_FILE_PREALLOC_BYTES must cover the worst-case batch so the live-commit "
              "fdatasync() (not fsync()) stays correct -- a batch write must never be able "
              "to grow the file past its preallocated region.");

// Enqueue-side backpressure: once a shard's queue depth reaches this many
// pending records, EnqueueWalRecord blocks the calling (client-handling)
// thread -- polling every WAL_BACKPRESSURE_POLL_US -- until the writer
// thread drains it back down, instead of accepting unboundedly and
// risking unbounded memory growth (and, for ASYNC_ONLY, an ever-growing
// crash-loss window while still telling the client "success"). Applies
// uniformly to every durability mode; deliberately makes the client WAIT
// rather than rejecting the write.
inline constexpr size_t      WAL_QUEUE_HIGH_WATERMARK    = WAL_BATCH_MAX_RECORDS * 4;
inline constexpr unsigned    WAL_BACKPRESSURE_POLL_US    = 200;

} // namespace XStoreTunables
