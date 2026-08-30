#pragma once
#include <cstddef>

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

} // namespace XStoreTunables
