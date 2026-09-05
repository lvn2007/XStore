#pragma once
#include <cstddef>
#include <functional>

// Shared, process-lifetime worker pool for fanning pread() calls out across
// threads -- see XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_CAP for the
// full rationale (XStore_Tunables.h). Deliberately its own free-standing
// pair of files (not a method on XStore, and not folded into
// XStore_BatchOps.cpp) since this is generic bounded-concurrency-fan-out
// infrastructure, not BatchQuery-specific -- any future caller with the
// same "many independent blocking-syscall-shaped units of work, bounded
// device concurrency" shape can reuse it as-is. NOT a fit for CPU-bound
// work whose per-unit cost is cheap relative to a dispatch round trip --
// see XStore_Tunables.h's "TRIED AND REVERTED" note on RangeQuery's
// wire-encode transform for a measured example of exactly that mistake.
namespace XStorePreadPool {

// Tells the pool how many threads are already spoken for by other work on
// this box (in practice: the ZMQ server's own worker thread count, known
// once -t/--threads resolves in main.cpp) so its own worker count can be
// sized as "cores left over" instead of independently claiming up to
// hardware_concurrency() threads on top of whatever else is already
// running -- see XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_CAP's comment
// in XStore_Tunables.h for why that oversubscription was a real, measured
// problem. Only has an effect if called BEFORE the pool's worker threads
// have started (i.e. before the first ParallelFor call anywhere in the
// process) -- the pool's thread count is fixed once it starts, same as
// XStore's other background thread pools. Safe to never call at all
// (defaults to reserving 0, i.e. the pool sizes off hardware_concurrency()
// alone) -- this is what every test that calls ParallelFor without going
// through main.cpp gets, unchanged from before this existed.
void Init(size_t reserved_threads);

// Runs fn(i) for every i in [0, count), blocking the calling thread until
// all `count` calls have completed. Safe to call concurrently from
// multiple threads -- every concurrent caller's work lands on the SAME
// fixed-size worker pool, so XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_CAP
// is a real cap on total concurrent execution across all callers combined,
// not a per-call fan-out limit. `fn` must be safe to invoke concurrently
// from multiple threads for different `i` values (pread() itself is: it
// takes its offset as a parameter rather than touching shared file-position
// state).
void ParallelFor(size_t count, const std::function<void(size_t)> &fn);

}  // namespace XStorePreadPool
