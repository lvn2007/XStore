#include "include/XStore/XStore_PreadPool.h"
#include "include/XStore/XStore_Tunables.h"
#include <blockingconcurrentqueue.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>

namespace XStorePreadPool {

namespace {

// One in-flight ParallelFor call's shared state. Workers claim work by
// fetch_add'ing NextIndex rather than each being handed a pre-partitioned
// slice, so a worker that finishes its share early (e.g. its rows happened
// to be page-cache-resident) naturally picks up more of the remaining
// range instead of sitting idle while a slower worker's fixed slice still
// has rows left -- matters more here than in SortBatch's par_unseq sort
// since per-row cost (a real pread, maybe a real disk wait) is far less
// uniform than a comparison.
//
// OutstandingTickets, not a plain "index units remaining" counter, is what
// this Job's lifetime is tied to -- see WorkerLoop's comment for why: a
// job.count-based completion counter let the submitter return (and destroy
// this stack-local Job) while OTHER already-pushed tickets for the SAME job
// were still sitting unconsumed in the queue, which then dereferenced a
// dangling Job* whenever they finally got dequeued. That bug only showed up
// under real multi-client concurrent load (many jobs racing for the same
// worker pool), not any single-threaded test, since it needs a fast worker
// to drain a whole job before its sibling tickets even get a turn -- rare
// with only one caller in flight, routine with many.
struct Job {
    const std::function<void(size_t)> *fn;
    size_t count;
    std::atomic<size_t> next_index{0};
    std::atomic<size_t> outstanding_tickets;
    moodycamel::LightweightSemaphore done_sem;

    Job(const std::function<void(size_t)> *f, size_t c, size_t tickets)
        : fn(f), count(c), outstanding_tickets(tickets) {}
};

// The pool's dispatch queue: one Job* "ticket" per worker allowed to touch
// that job. A ticket isn't "process exactly one index" -- a worker that
// dequeues one keeps pulling indices from the job's NextIndex counter until
// the job is exhausted, then goes back to waiting on the queue. Pushing
// min(PoolSize, job.count) tickets is what bounds a single job's own
// concurrency to the pool size; tickets from DIFFERENT concurrent Jobs
// naturally interleave in the same queue, which is what makes the pool
// size a real cross-call concurrency cap (see XStore_Tunables.h) rather
// than a per-call one.
moodycamel::BlockingConcurrentQueue<Job *> &TicketQueue() {
    static moodycamel::BlockingConcurrentQueue<Job *> queue;
    return queue;
}

// Set via Init(), read once by PoolSize() at first-use. Relaxed: Init() is
// only ever meant to be called once, before any ParallelFor call, by a
// single thread (main.cpp's startup sequence) -- no concurrent writer to
// race against, this is just a plain cross-thread handoff to whichever
// thread later calls EnsureStarted().
std::atomic<size_t> &ReservedThreads() {
    static std::atomic<size_t> reserved{0};
    return reserved;
}

size_t PoolSize() {
    size_t hw = static_cast<size_t>(std::thread::hardware_concurrency());
    if (hw == 0) hw = XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_CAP;  // hardware_concurrency() is allowed to report 0 when undetermined
    size_t reserved = ReservedThreads().load(std::memory_order_relaxed);
    size_t leftover = reserved < hw ? hw - reserved : 0;
    size_t n = std::min(leftover, XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_CAP);
    return std::max(n, XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_MIN);
}

void WorkerLoop() {
    moodycamel::BlockingConcurrentQueue<Job *> &queue = TicketQueue();
    Job *job = nullptr;
    while (true) {
        queue.wait_dequeue(job);
        size_t idx;
        while ((idx = job->next_index.fetch_add(1, std::memory_order_relaxed)) < job->count) {
            (*job->fn)(idx);
        }
        // This ticket's turn is over -- whether it claimed and ran real
        // work above, or arrived after next_index was already exhausted
        // and did nothing. Either way, this worker is done touching `job`,
        // so it retires its ticket. The Job must stay alive until EVERY
        // pushed ticket retires, not just until all `count` work units are
        // done -- those aren't the same moment when more tickets were
        // pushed than workers happened to be needed to finish the work
        // (the common case: a few fast workers drain the whole job while
        // other tickets are still queued behind unrelated work). The
        // worker whose decrement sees this reach exactly zero is, by
        // construction, always the last ticket to retire -- exactly one
        // such worker, so exactly one signal() per job, matching
        // ParallelFor's single wait() below.
        if (job->outstanding_tickets.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            job->done_sem.signal();
        }
    }
}

// Lazily starts the pool's worker threads on first use, once, for the
// life of the process -- same shape as XStore's other background thread
// pools (WAL writers, ZMQ workers). No shutdown path: like those, the
// server process is always killed abruptly rather than gracefully
// stopped, so there's nothing that ever needs these threads to join.
void EnsureStarted() {
    static std::once_flag started;
    std::call_once(started, [] {
        size_t n = PoolSize();
        for (size_t i = 0; i < n; i++) {
            std::thread(WorkerLoop).detach();
        }
    });
}

}  // namespace

void Init(size_t reserved_threads) {
    ReservedThreads().store(reserved_threads, std::memory_order_relaxed);
}

void ParallelFor(size_t count, const std::function<void(size_t)> &fn) {
    if (count == 0) return;
    EnsureStarted();

    size_t tickets = std::min(count, PoolSize());
    Job job(&fn, count, tickets);
    moodycamel::BlockingConcurrentQueue<Job *> &queue = TicketQueue();
    for (size_t i = 0; i < tickets; i++) {
        queue.enqueue(&job);
    }
    job.done_sem.wait();
}

}  // namespace XStorePreadPool
