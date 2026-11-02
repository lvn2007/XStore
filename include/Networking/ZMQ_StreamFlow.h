#pragma once
// Flow control of the chunks of a streamed reply (Request.streamOk), so a client that reads slowly cannot make the server hold its whole reply in
// memory. The numbers and the reasons are in XStoreTunables (STREAM_*). Header-only, no network: ZMQ_Server.cpp's ReplyWriter uses it, and the
// unit tests drive it directly.
//
//   ReplyWindow window;                       // one per reply
//   Verdict v = window.Admit(bytes);          // before a chunk is queued; may wait (see below)
//   if (v == Verdict::kSend) ... queue the chunk, and call window.Done(bytes) when the network layer has written it (or dropped it)
//   else the reply is stopped (the caller sends an error status as the last frame and drops the rest of the chunks)
//
// Admit waits only when this reply already has queued bytes and the chunk would pass the window, and only while fewer than `max_waiting` workers are
// waiting in all (a worker that does not wait queues the chunk at once). A wait ends when the reply's queued bytes are down to half the window (one
// wake-up per half window, not per chunk) or after `stall` without that (kStalled).
//
// One memory budget: the bytes of a chunk are charged to the process-wide MemoryPool while they are queued (Admit charges, Done returns them), so
// operations see the memory that slow readers hold and the pool's limit (host memory minus its reserve) is the only limit. A reply never waits or is
// refused for that: a producer that parks asks ShouldPark(), which is true when its window is full, or when the pool is full while it has bytes
// queued (they will drain, which wakes it).
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <mutex>

#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_Tunables.h"

namespace xstore::streamflow {

struct Config {
    uint64_t window_bytes = XStoreTunables::STREAM_WINDOW_BYTES;   // 0 = no window
    uint64_t min_chunk_bytes = XStoreTunables::STREAM_MIN_ACCOUNTED_CHUNK_BYTES;
    uint32_t max_waiting = 0;                                      // 0 = not set yet: the server sets it from its worker count
    bool park_replies = true;                                      // a reply that would wait parks instead (ZMQ_ParkedReplies.h); XSTORE_PARK_REPLIES=0: wait in the worker
    std::chrono::milliseconds stall{std::chrono::seconds(XStoreTunables::STREAM_STALL_SECONDS)};
};

namespace detail {
inline std::mutex &ConfigMutex() { static std::mutex m; return m; }
inline uint64_t EnvMegabytes(const char *name, uint64_t fallback) {
    const char *env = std::getenv(name);
    if (env == nullptr) return fallback;
    char *end = nullptr;
    const unsigned long long value = std::strtoull(env, &end, 10);
    return (end != env && *end == '\0') ? static_cast<uint64_t>(value) << 20 : fallback;
}
inline Config FromEnv();
// ONE static, initialized from the environment on first use by whichever of Get / Set / SetDefaultMaxWaiting comes first (three separate initializers each
// overwrote what the others had set: the server's cap on waiting workers was lost at the first reply).
inline Config &Stored() { static Config c = FromEnv(); return c; }
inline Config FromEnv() {
    Config c;
    c.window_bytes = EnvMegabytes("XSTORE_STREAM_WINDOW_MB", c.window_bytes);
    if (const char *env = std::getenv("XSTORE_STREAM_STALL_SECONDS")) {
        char *end = nullptr;
        const unsigned long long value = std::strtoull(env, &end, 10);
        if (end != env && *end == '\0' && value > 0) c.stall = std::chrono::seconds(value);
    }
    if (const char *env = std::getenv("XSTORE_STREAM_MAX_WAITING")) {
        char *end = nullptr;
        const unsigned long long value = std::strtoull(env, &end, 10);
        if (end != env && *end == '\0') c.max_waiting = static_cast<uint32_t>(value);
    }
    if (const char *env = std::getenv("XSTORE_PARK_REPLIES")) c.park_replies = env[0] != '0';
    return c;
}
inline std::atomic<uint64_t> &Queued() { static std::atomic<uint64_t> q{0}; return q; }
inline std::atomic<int> &Waiting() { static std::atomic<int> w{0}; return w; }
inline std::atomic<uint64_t> &Wakeups() { static std::atomic<uint64_t> w{0}; return w; }
inline std::atomic<uint64_t> &Parks() { static std::atomic<uint64_t> p{0}; return p; }
inline std::atomic<uint64_t> &Resumes() { static std::atomic<uint64_t> r{0}; return r; }
inline std::atomic<uint64_t> &Cancels() { static std::atomic<uint64_t> c{0}; return c; }
inline std::atomic<uint64_t> &GoneDrops() { static std::atomic<uint64_t> d{0}; return d; }
}  // namespace detail

// The configuration in force: the environment (read once) unless Set() was called. Copied under a mutex (a few ns per chunk of 512 KiB).
inline Config Get() {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    return detail::Stored();
}
// Tests: replace the configuration (from here on Get() returns it, whatever the environment says).
inline void Set(const Config &config) {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    detail::Stored() = config;
}
// The server, once it knows its workers: at most XStoreTunables::STREAM_WAITING_WORKERS_PERCENT of them may wait on a window, unless the environment
// (XSTORE_STREAM_MAX_WAITING) or Set() already chose a number.
inline void SetDefaultMaxWaiting(int workers) {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    if (detail::Stored().max_waiting == 0) {
        detail::Stored().max_waiting = static_cast<uint32_t>(std::max<int>(1, workers * static_cast<int>(XStoreTunables::STREAM_WAITING_WORKERS_PERCENT) / 100));
    }
}
// Bytes queued for all replies now / the workers waiting on a window now (tests, metrics).
inline uint64_t QueuedBytes() { return detail::Queued().load(); }
inline int WaitingWorkers() { return detail::Waiting().load(); }
// Times a waiting worker was woken since the start (tests).
inline uint64_t Wakeups() { return detail::Wakeups().load(); }
// Replies parked / resumed since the start (tests, metrics).
inline void NoteParked() { detail::Parks().fetch_add(1); }
inline void NoteResumed() { detail::Resumes().fetch_add(1); }
inline uint64_t ParkCount() { return detail::Parks().load(); }
inline uint64_t ResumeCount() { return detail::Resumes().load(); }
// Replies cancelled since the start because their client was gone (ReplyWindow::Cancel).
inline uint64_t CancelCount() { return detail::Cancels().load(); }
// Reply messages the broker could not relay because their client was gone (counted only when that happens): after a cancel the producer stops, so a
// reply that ends this way adds a few, a scan that goes on for nobody adds one per chunk.
inline void NoteGoneDrop() { detail::GoneDrops().fetch_add(1, std::memory_order_relaxed); }
inline uint64_t GoneDropCount() { return detail::GoneDrops().load(); }

// The pool is full (leases plus charged reply bytes have reached its limit). Tests can force the answer: 1 = full, 0 = not, -1 = ask the pool.
namespace detail {
inline std::atomic<int> &PoolFullOverride() { static std::atomic<int> o{-1}; return o; }
}  // namespace detail
inline void SetPoolFullForTests(int forced) { detail::PoolFullOverride().store(forced); }
inline bool PoolFull() {
    const int forced = detail::PoolFullOverride().load(std::memory_order_relaxed);
    return forced >= 0 ? forced == 1 : MemoryPool::Instance().Full();
}

enum class Verdict { kSend, kStalled };

class ReplyWindow {
public:
    ReplyWindow() = default;
    ReplyWindow(const ReplyWindow &) = delete;
    ReplyWindow &operator=(const ReplyWindow &) = delete;

    // Before queueing a chunk of `bytes`. kSend: it counts as queued from now on (call Done(bytes) when it has been written out). Otherwise nothing
    // was counted and the reply must be stopped.
    // may_wait = false: never wait for the window (a reply that parks instead of waiting; the window only decides when it parks).
    Verdict Admit(uint64_t bytes, bool may_wait = true) {
        const Config config = Get();
        if (may_wait && config.window_bytes != 0 && queued_.load() > 0 && queued_.load() + bytes > config.window_bytes) {
            const int already_waiting = detail::Waiting().fetch_add(1);
            if (static_cast<uint32_t>(already_waiting) < config.max_waiting) {
                const uint64_t half = config.window_bytes / 2;
                wake_at_.store(half);
                bool drained;
                {
                    std::unique_lock<std::mutex> lock(mutex_);
                    waiters_.fetch_add(1);
                    drained = cv_.wait_for(lock, config.stall, [&] { return queued_.load() <= half; });
                    waiters_.fetch_sub(1);
                }
                detail::Waiting().fetch_sub(1);
                if (!drained) return Verdict::kStalled;
            } else {
                detail::Waiting().fetch_sub(1);
            }
        }
        queued_.fetch_add(bytes);
        detail::Queued().fetch_add(bytes);
        MemoryPool::Instance().Charge(bytes);
        return Verdict::kSend;
    }

    // A chunk counted by Admit has been written out (or dropped with its connection). Called from the network layer's thread: never throws.
    void Done(uint64_t bytes) noexcept {
        const uint64_t left = queued_.fetch_sub(bytes) - bytes;
        detail::Queued().fetch_sub(bytes);
        MemoryPool::Instance().Uncharge(bytes);
        // Wake only when the queue is down to what the waiter wants (a wake-up per written chunk costs a context switch each, 64 per half window)
        if (drain_armed_.load() && left <= drain_below_.load()) FireDrain();
        if (waiters_.load() > 0 && left <= wake_at_.load()) {
            { std::lock_guard<std::mutex> lock(mutex_); }   // a waiter between its test and its sleep has the lock: this orders us after it
            detail::Wakeups().fetch_add(1);
            cv_.notify_all();
        }
    }

    uint64_t QueuedBytesOfThisReply() const { return queued_.load(); }

    // The reply's client is gone (GoneClients::Mark): nobody reads what is produced from now on. The producer checks Cancelled() where it asks
    // whether to park; a cancelled reply is only ended (relaxed: the producer sees it at its next boundary, no ordering with other data is needed).
    void Cancel() noexcept {
        if (!cancelled_.exchange(true, std::memory_order_relaxed)) detail::Cancels().fetch_add(1, std::memory_order_relaxed);
    }
    bool Cancelled() const noexcept { return cancelled_.load(std::memory_order_relaxed); }

    // Whether the producer should stop and park: its window is full, or the pool is full and this reply has bytes queued whose draining will wake it.
    // With nothing queued it never parks (nothing would ever wake it): the run goes on for at least one more boundary.
    bool ShouldPark(uint64_t window_bytes) const {
        const uint64_t queued = queued_.load();
        return queued >= window_bytes || (queued > 0 && PoolFull());
    }

    // Instead of a worker waiting in Admit (park and resume): `fn` runs once, on whichever thread gets there first (the caller, or the network layer's
    // thread in Done), when this reply's queued bytes are at most `below`. Runs at once if they already are. fn must be quick and must not throw.
    void ArmDrain(uint64_t below, std::function<void()> fn) {
        {
            std::lock_guard<std::mutex> lock(drain_mutex_);
            drain_fn_ = std::move(fn);
            drain_below_.store(below);
            drain_armed_.store(true);
        }
        if (queued_.load() <= below) FireDrain();
    }

private:
    void FireDrain() noexcept {
        std::function<void()> fn;
        {
            std::lock_guard<std::mutex> lock(drain_mutex_);
            if (!drain_armed_.exchange(false)) return;
            fn = std::move(drain_fn_);
        }
        fn();
    }

    std::atomic<uint64_t> queued_{0};
    std::atomic<bool> cancelled_{false};
    std::atomic<bool> drain_armed_{false};
    std::atomic<uint64_t> drain_below_{0};
    std::mutex drain_mutex_;
    std::function<void()> drain_fn_;
    std::atomic<int> waiters_{0};
    std::atomic<uint64_t> wake_at_{0};   // the queued bytes a waiter waits for (set by Admit before it waits)
    std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace xstore::streamflow
