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
inline constexpr unsigned    WAL_GROUP_COMMIT_WINDOW_US  = 500;
inline constexpr size_t      WAL_BATCH_MAX_RECORDS       = 256;
inline constexpr const char* WAL_DIR_NAME                = "WAL";

} // namespace XStoreTunables
