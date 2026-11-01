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
// waiting in all (a worker that does not wait queues the chunk at once: the queue then grows past the window, which the global limit bounds).
// A wait ends when the reply's queued bytes are down to half the window (one wake-up per half window, not per chunk) or after `stall` without that
// (kStalled). A reply that holds more than `share_bytes` is refused (kQueueFull) when the bytes queued for all replies would pass `queue_limit_bytes`.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <mutex>

#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_Tunables.h"

namespace xstore::streamflow {

struct Config {
    uint64_t window_bytes = XStoreTunables::STREAM_WINDOW_BYTES;   // 0 = no window
    uint64_t queue_limit_bytes = 0;                                // 0 = a quarter of the memory pool's limit
    uint64_t share_bytes = XStoreTunables::STREAM_SHARE_BYTES;
    uint64_t min_chunk_bytes = XStoreTunables::STREAM_MIN_ACCOUNTED_CHUNK_BYTES;
    uint32_t max_waiting = 0;                                      // 0 = not set yet: the server sets it from its worker count
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
    c.queue_limit_bytes = EnvMegabytes("XSTORE_STREAM_QUEUE_LIMIT_MB", c.queue_limit_bytes);
    c.share_bytes = EnvMegabytes("XSTORE_STREAM_SHARE_MB", c.share_bytes);
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
    return c;
}
inline std::atomic<uint64_t> &Queued() { static std::atomic<uint64_t> q{0}; return q; }
inline std::atomic<int> &Waiting() { static std::atomic<int> w{0}; return w; }
inline std::atomic<uint64_t> &Wakeups() { static std::atomic<uint64_t> w{0}; return w; }
}  // namespace detail

// The configuration in force: the environment (read once) unless Set() was called. Copied under a mutex (a few ns per chunk of 512 KiB).
inline Config Get() {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    Config c = detail::Stored();
    if (c.queue_limit_bytes == 0) c.queue_limit_bytes = MemoryPool::Instance().Limit() / 4;
    return c;
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

enum class Verdict { kSend, kStalled, kQueueFull };

class ReplyWindow {
public:
    ReplyWindow() = default;
    ReplyWindow(const ReplyWindow &) = delete;
    ReplyWindow &operator=(const ReplyWindow &) = delete;

    // Before queueing a chunk of `bytes`. kSend: it counts as queued from now on (call Done(bytes) when it has been written out). Otherwise nothing
    // was counted and the reply must be stopped.
    Verdict Admit(uint64_t bytes) {
        const Config config = Get();
        if (config.window_bytes != 0 && queued_.load() > 0 && queued_.load() + bytes > config.window_bytes) {
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
        if (config.queue_limit_bytes != 0 && queued_.load() > config.share_bytes && detail::Queued().load() + bytes > config.queue_limit_bytes) {
            return Verdict::kQueueFull;
        }
        queued_.fetch_add(bytes);
        detail::Queued().fetch_add(bytes);
        return Verdict::kSend;
    }

    // A chunk counted by Admit has been written out (or dropped with its connection). Called from the network layer's thread: never throws.
    void Done(uint64_t bytes) noexcept {
        const uint64_t left = queued_.fetch_sub(bytes) - bytes;
        detail::Queued().fetch_sub(bytes);
        // Wake only when the queue is down to what the waiter wants (a wake-up per written chunk costs a context switch each, 64 per half window)
        if (waiters_.load() > 0 && left <= wake_at_.load()) {
            { std::lock_guard<std::mutex> lock(mutex_); }   // a waiter between its test and its sleep has the lock: this orders us after it
            detail::Wakeups().fetch_add(1);
            cv_.notify_all();
        }
    }

    uint64_t QueuedBytesOfThisReply() const { return queued_.load(); }

private:
    std::atomic<uint64_t> queued_{0};
    std::atomic<int> waiters_{0};
    std::atomic<uint64_t> wake_at_{0};   // the queued bytes a waiter waits for (set by Admit before it waits)
    std::mutex mutex_;
    std::condition_variable cv_;
};

}  // namespace xstore::streamflow
