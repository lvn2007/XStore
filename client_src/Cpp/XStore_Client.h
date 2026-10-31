#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY
#endif
#include "fmt/format.h"

#include <google/protobuf/arena.h>
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include "XStore.pb.h"

// ======================================================================================================================================
// Part 1 of 3: a load-aware job runner (namespace xstore_client). Knows nothing about XStore, ZMQ or protobuf: lift this part as is for
// any batch of independent jobs.
//
//   RunJobs(n, policy, fn)   runs fn(0) .. fn(n-1) and returns when all are done. The calling thread always works. With a fixed policy
//                            the team is exactly that big. Otherwise it is adaptive: the team starts at the caller alone, a short-lived
//                            controller thread looks at how many threads are runnable on the whole machine every `tick`, and
//                            - while there is room it doubles the team (slow start), up to the cap;
//                            - when the machine is over-subscribed the helpers above the surplus leave, each after its current job.
//                            Independent processes never talk to each other: the machine's run queue is what they share, so a second
//                            client that starts parsing makes the first one's helpers retreat within one job, and both settle.
//
// Jobs should be short (milliseconds to tens of milliseconds): that is the latency of the team reacting to the load.
// ======================================================================================================================================
namespace xstore_client {

[[nodiscard]] inline size_t CoreCount() { return std::max<size_t>(1, std::thread::hardware_concurrency()); }

// "0.20 0.18 0.15 3/1234 5678": the fourth field of /proc/loadavg is runnable/total scheduling entities right now.
[[nodiscard]] inline std::optional<size_t> ParseRunnableFromLoadAvg(std::string_view text) {
    size_t pos = 0;
    for (int field = 0; field < 3; ++field) {
        pos = text.find(' ', pos);
        if (pos == std::string_view::npos) return std::nullopt;
        ++pos;
    }
    size_t runnable = 0;
    size_t digits = 0;
    for (; pos < text.size() && text[pos] >= '0' && text[pos] <= '9'; ++pos, ++digits) runnable = runnable * 10 + static_cast<size_t>(text[pos] - '0');
    if (digits == 0 || pos >= text.size() || text[pos] != '/') return std::nullopt;
    return runnable;
}

// Threads runnable on this machine right now, this one included; nullopt where /proc/loadavg does not exist.
[[nodiscard]] inline std::optional<size_t> ReadRunnableThreads() {
    std::ifstream loadavg("/proc/loadavg");
    char buffer[128];
    loadavg.read(buffer, sizeof buffer);
    const std::streamsize bytes = loadavg.gcount();
    return bytes > 0 ? ParseRunnableFromLoadAvg(std::string_view(buffer, static_cast<size_t>(bytes))) : std::nullopt;
}

struct ParallelismPolicy {
    size_t fixed_threads = 0;                 // > 0: exactly this many threads (at most one per job), the load is ignored
    size_t max_threads = CoreCount();         // adaptive: the team never grows past this
    size_t cores = CoreCount();               // what the runnable threads are compared with
    size_t headroom = 1;                      // cores left free for everyone else
    std::chrono::milliseconds tick{10};       // how often the controller looks at the load
    std::function<std::optional<size_t>()> runnable = ReadRunnableThreads;
};

// The team size the controller wants next. `runnable` counts every runnable thread of the machine, this team's included.
// Room (cores - runnable - headroom > 0) doubles the team, but never beyond that room, the cap, or the jobs left; more runnable
// threads than cores sends the surplus home; in between the team holds, so it does not flap. Never below the calling thread.
[[nodiscard]] inline size_t NextWorkerTarget(size_t target, size_t runnable, size_t cores, size_t headroom, size_t cap, size_t jobs_left) {
    if (runnable > cores) {
        const size_t surplus = runnable - cores;
        return target > surplus ? target - surplus : 1;
    }
    const size_t room = cores > runnable + headroom ? cores - runnable - headroom : 0;
    if (room == 0 || target >= jobs_left || target >= cap) return target;
    return std::min({target + std::min(room, target), cap, jobs_left});
}

namespace detail {

template <class Fn>
class Team {
 public:
    Team(size_t jobs, size_t cap, const ParallelismPolicy &policy, Fn &fn)
        : jobs_(jobs), cap_(cap), policy_(policy), fn_(fn), helpers_(cap), alive_(cap) {}

    void Run() {
        const bool adaptive = policy_.fixed_threads == 0;
        target_ = adaptive ? 1 : cap_;
        std::thread controller;
        if (adaptive) {
            try {
                controller = std::thread([this] { Control(); });
            } catch (const std::system_error &) {   // no controller: the caller alone
            }
        } else {
            Grow();
        }
        Work(0);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finished_ = true;
        }
        wake_.notify_all();
        if (controller.joinable()) controller.join();
        for (std::thread &helper : helpers_) {
            if (helper.joinable()) helper.join();
        }
        if (error_) std::rethrow_exception(error_);
    }

 private:
    // Slot 0 is the calling thread and never leaves. A helper works only while its slot is below the target.
    void Work(size_t slot) {
        try {
            while (!stop_.load(std::memory_order_relaxed) && (slot == 0 || slot < target_.load(std::memory_order_relaxed))) {
                const size_t job = next_.fetch_add(1, std::memory_order_relaxed);
                if (job >= jobs_) return;
                fn_(job);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!error_) error_ = std::current_exception();
            stop_.store(true, std::memory_order_relaxed);
        }
    }

    // Starts (or restarts, after a retreat) the helper of every slot below the target. Only one thread calls this at a time.
    void Grow() {
        for (size_t slot = 1; slot < target_.load(); ++slot) {
            if (alive_[slot].load()) continue;
            if (helpers_[slot].joinable()) helpers_[slot].join();   // exited earlier
            alive_[slot].store(true);
            try {
                helpers_[slot] = std::thread([this, slot] {
                    Work(slot);
                    alive_[slot].store(false);
                });
            } catch (const std::system_error &) {   // cannot start more threads: keep what runs
                alive_[slot].store(false);
                target_.store(slot);
                return;
            }
        }
    }

    void Tick() {
        const std::optional<size_t> runnable = policy_.runnable();
        if (!runnable) return;
        const size_t target = target_.load();
        const size_t jobs_left = jobs_ - std::min(jobs_, next_.load(std::memory_order_relaxed));
        const size_t wanted = NextWorkerTarget(target, *runnable, policy_.cores, policy_.headroom, cap_, jobs_left);
        if (wanted == target) return;
        target_.store(wanted);
        if (wanted > target) Grow();
    }

    void Control() {
        std::unique_lock<std::mutex> lock(mutex_);
        do {
            lock.unlock();
            Tick();
            lock.lock();
        } while (!wake_.wait_for(lock, policy_.tick, [this] { return finished_; }));
    }

    const size_t jobs_;
    const size_t cap_;
    const ParallelismPolicy &policy_;
    Fn &fn_;
    std::vector<std::thread> helpers_;          // by slot; slot 0 is unused
    std::vector<std::atomic<bool>> alive_;
    std::atomic<size_t> next_{0};
    std::atomic<size_t> target_{1};
    std::atomic<bool> stop_{false};
    std::mutex mutex_;
    std::condition_variable wake_;
    bool finished_ = false;
    std::exception_ptr error_;
};

}  // namespace detail

// Runs fn(0) .. fn(jobs - 1) on a team the policy sizes, and returns when all are done. fn must be safe to call concurrently for
// different jobs. The first exception fn throws stops the team taking jobs and is rethrown here once every thread has stopped.
template <class Fn>
void RunJobs(size_t jobs, ParallelismPolicy policy, Fn &&fn) {
    if (jobs == 0) return;
    // Where the load cannot be read (not Linux) an adaptive team has nothing to adapt to: a modest fixed one.
    if (policy.fixed_threads == 0 && !(policy.runnable && policy.runnable())) policy.fixed_threads = std::max<size_t>(1, policy.cores / 2);
    const size_t cap = std::max<size_t>(1, std::min(jobs, policy.fixed_threads != 0 ? policy.fixed_threads : policy.max_threads));
    if (cap == 1) {
        for (size_t job = 0; job < jobs; ++job) fn(job);
        return;
    }
    detail::Team<std::remove_reference_t<Fn>> team(jobs, cap, policy, fn);
    team.Run();
}

// JobStream: the same load-aware team for jobs that arrive one at a time from ONE producer thread (a reply being received, say) while
// earlier ones are already being worked on.
//
//   Submit(job)   hands a job to the team; never blocks. Jobs are taken in submission order.
//   Finish()      no more jobs: the calling thread joins the team for what is left and returns when every job is done. Rethrows the first
//                 exception a job threw (after which jobs were no longer taken).
//
// The producer is usually busy (blocked in a recv), so the team is helpers only until Finish(): a fixed policy of N means N - 1 helpers
// plus the caller at the end; an adaptive team starts with one helper and doubles while there is a backlog and the machine has room, and
// retreats when it does not (the controller works exactly as in RunJobs, with the queue length as "jobs left"). A JobStream dropped
// without Finish() (the producer failed) stops its helpers and discards the queued jobs.
template <class Job, class Fn>
class JobStream {
 public:
    JobStream(const ParallelismPolicy &policy, Fn &fn) : policy_(policy), fn_(fn) {
        size_t fixed = policy_.fixed_threads;
        if (fixed == 0 && !(policy_.runnable && policy_.runnable())) fixed = std::max<size_t>(1, policy_.cores / 2);   // load unreadable
        adaptive_ = fixed == 0;
        cap_ = std::max<size_t>(1, adaptive_ ? policy_.max_threads : fixed);
        helpers_ = std::vector<std::thread>(cap_);
        alive_ = std::vector<std::atomic<bool>>(cap_);
        target_.store(cap_ > 1 ? (adaptive_ ? 2 : cap_) : 1);
        Grow();
        if (adaptive_ && cap_ > 1) {
            try {
                controller_ = std::thread([this] { Control(); });
            } catch (const std::system_error &) {   // no controller: the helper it started and the caller
            }
        }
    }

    JobStream(const JobStream &) = delete;
    JobStream &operator=(const JobStream &) = delete;

    ~JobStream() {
        if (shut_down_) return;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        wake_.notify_all();
        Shutdown();
    }

    void Submit(Job job) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) return;
            queue_.push_back(std::move(job));
        }
        wake_.notify_one();
    }

    void Finish() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }
        wake_.notify_all();
        Work(0);
        Shutdown();
        if (error_) std::rethrow_exception(error_);
    }

 private:
    // Slot 0 is the calling thread (only in Finish) and never leaves. A helper works only while its slot is below the target.
    void Work(size_t slot) {
        while (true) {
            Job job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                wake_.wait(lock, [&] { return stop_ || closed_ || !queue_.empty() || (slot != 0 && slot >= target_.load()); });
                if (stop_ || (slot != 0 && slot >= target_.load())) return;
                if (queue_.empty()) {
                    if (closed_) return;
                    continue;
                }
                job = std::move(queue_.front());
                queue_.pop_front();
            }
            try {
                fn_(job);
            } catch (...) {
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    if (!error_) error_ = std::current_exception();
                    stop_ = true;
                }
                wake_.notify_all();
                return;
            }
        }
    }

    // Starts (or restarts, after a retreat) the helper of every slot below the target. Called by the constructor, then only by the controller.
    void Grow() {
        for (size_t slot = 1; slot < target_.load(); ++slot) {
            if (alive_[slot].load()) continue;
            if (helpers_[slot].joinable()) helpers_[slot].join();   // exited earlier
            alive_[slot].store(true);
            try {
                helpers_[slot] = std::thread([this, slot] {
                    Work(slot);
                    alive_[slot].store(false);
                });
            } catch (const std::system_error &) {   // cannot start more threads: keep what runs
                alive_[slot].store(false);
                target_.store(slot);
                return;
            }
        }
    }

    void Tick() {
        const std::optional<size_t> runnable = policy_.runnable();
        if (!runnable) return;
        size_t pending = 0;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending = queue_.size();
        }
        const size_t target = target_.load();
        const size_t wanted = NextWorkerTarget(target, *runnable, policy_.cores, policy_.headroom, cap_, pending);
        if (wanted == target) return;
        target_.store(wanted);
        if (wanted > target) Grow();
        else wake_.notify_all();   // helpers above the new target see it and leave
    }

    void Control() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!control_.wait_for(lock, policy_.tick, [this] { return finished_; })) {
            lock.unlock();
            Tick();
            lock.lock();
        }
    }

    void Shutdown() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finished_ = true;
        }
        control_.notify_all();
        if (controller_.joinable()) controller_.join();
        for (std::thread &helper : helpers_) {
            if (helper.joinable()) helper.join();
        }
        shut_down_ = true;
    }

    const ParallelismPolicy policy_;
    Fn &fn_;
    bool adaptive_ = false;
    size_t cap_ = 1;                            // team size, the calling thread included
    std::vector<std::thread> helpers_;          // by slot; slot 0 is unused
    std::vector<std::atomic<bool>> alive_;
    std::thread controller_;
    std::atomic<size_t> target_{1};
    std::mutex mutex_;
    std::condition_variable wake_;              // work arrived, the stream closed or stopped, the target dropped
    std::condition_variable control_;           // the controller's tick
    std::deque<Job> queue_;
    bool closed_ = false;
    bool stop_ = false;
    bool finished_ = false;
    bool shut_down_ = false;
    std::exception_ptr error_;
};

// ======================================================================================================================================
// Part 2 of 3: parallel parse of a query reply (RangeQuery, BatchQuery, FindByValue, FindByPattern); uses part 1.
//
// The server cuts a reply into frames only between whole rows, and a reply is just its rows (field 1) plus an optional status (field 3),
// so any run of whole top-level fields is a reply of its own: the reply is cut into pieces of a few MB at row boundaries, the pieces are
// parsed by a team that sizes itself to the machine's load (part 1), and the rows are moved into the result in order. A reply that cannot
// be split or parsed this way makes ParseFragmentsParallel return false with the result untouched; the caller parses it serially.
// ======================================================================================================================================

// Below this many reply bytes the serial parse is quicker than starting a team.
inline constexpr size_t MIN_PARALLEL_PARSE_BYTES = 4ull * 1024 * 1024;
inline constexpr size_t PARSE_PIECE_BYTES = 4ull * 1024 * 1024;

[[nodiscard]] inline size_t PositiveFromEnv(const char *name) {
    const char *env = std::getenv(name);
    if (env == nullptr) return 0;
    char *end = nullptr;
    const unsigned long long n = std::strtoull(env, &end, 10);
    return (end != env && *end == '\0' && n > 0) ? static_cast<size_t>(n) : 0;
}

// The policy for one parse. fixed_threads > 0 forces that team size; 0 reads XSTORE_CLIENT_PARSE_THREADS (a number forces it,
// anything else is adaptive). XSTORE_CLIENT_PARSE_MAX caps an adaptive team (default the machine's hardware threads).
[[nodiscard]] inline ParallelismPolicy ParsePolicy(size_t fixed_threads) {
    ParallelismPolicy policy;
    policy.fixed_threads = fixed_threads != 0 ? fixed_threads : PositiveFromEnv("XSTORE_CLIENT_PARSE_THREADS");
    if (const size_t max_threads = PositiveFromEnv("XSTORE_CLIENT_PARSE_MAX"); max_threads != 0) policy.max_threads = max_threads;
    return policy;
}

struct Fragment {
    const void *data;
    size_t size;
};

namespace detail {

[[nodiscard]] inline bool ReadVarint(const uint8_t *&p, const uint8_t *end, uint64_t &value) {
    value = 0;
    for (int shift = 0; shift < 64 && p < end; shift += 7) {
        const uint8_t byte = *p++;
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return true;
    }
    return false;
}

// The end of the top-level field that starts at p, or nullptr if it is malformed (or a group, which a reply never has).
[[nodiscard]] inline const uint8_t *FieldEnd(const uint8_t *p, const uint8_t *end) {
    uint64_t tag = 0;
    if (!ReadVarint(p, end, tag)) return nullptr;
    uint64_t length = 0;
    switch (tag & 7) {
        case 0: return ReadVarint(p, end, length) ? p : nullptr;   // varint
        case 1: length = 8; break;                                  // fixed64
        case 2: if (!ReadVarint(p, end, length)) return nullptr; break;   // length-delimited
        case 5: length = 4; break;                                  // fixed32
        default: return nullptr;
    }
    return length <= static_cast<uint64_t>(end - p) ? p + length : nullptr;
}

}  // namespace detail

namespace detail {

// Appends the pieces of one fragment. False if the fragment is not well framed.
[[nodiscard]] inline bool SplitFragment(const Fragment &fragment, size_t piece_bytes, std::vector<Fragment> &pieces) {
    const auto *begin = static_cast<const uint8_t *>(fragment.data);
    const uint8_t *end = begin + fragment.size;
    const uint8_t *piece = begin;
    for (const uint8_t *p = begin; p < end;) {
        p = FieldEnd(p, end);
        if (p == nullptr) return false;
        if (static_cast<size_t>(p - piece) >= piece_bytes) {
            pieces.push_back({piece, static_cast<size_t>(p - piece)});
            piece = p;
        }
    }
    if (piece < end) pieces.push_back({piece, static_cast<size_t>(end - piece)});
    return true;
}

}  // namespace detail

// Cuts every fragment into pieces of about piece_bytes (never shorter than one field) between top-level fields; a piece never spans
// fragments. False if a fragment is not well framed.
[[nodiscard]] inline bool SplitIntoPieces(const std::vector<Fragment> &fragments, size_t piece_bytes, std::vector<Fragment> &pieces) {
    pieces.clear();
    for (const Fragment &fragment : fragments) {
        if (!detail::SplitFragment(fragment, piece_bytes, pieces)) return false;
    }
    return true;
}

// The same pieces in the same order, with the fragments split on the policy's team: walking the row headers of a big reply touches every
// row's cache line, and a fragment start is always a row boundary, so the fragments are independent jobs.
[[nodiscard]] inline bool SplitIntoPiecesParallel(const std::vector<Fragment> &fragments, size_t piece_bytes, const ParallelismPolicy &policy,
                                                  std::vector<Fragment> &pieces) {
    if (fragments.size() < 2) return SplitIntoPieces(fragments, piece_bytes, pieces);
    std::vector<std::vector<Fragment>> by_fragment(fragments.size());
    std::atomic<bool> well_framed{true};
    RunJobs(fragments.size(), policy, [&](size_t k) {
        if (!detail::SplitFragment(fragments[k], piece_bytes, by_fragment[k])) well_framed.store(false, std::memory_order_relaxed);
    });
    if (!well_framed.load()) return false;
    pieces.clear();
    for (const std::vector<Fragment> &one : by_fragment) pieces.insert(pieces.end(), one.begin(), one.end());
    return true;
}

// True for the reply messages made of repeated queryResponse rows plus an optional queryStatus.
template <class RepT, class = void>
struct IsFragmentMergeable : std::false_type {};
template <class RepT>
struct IsFragmentMergeable<RepT, std::void_t<decltype(std::declval<RepT &>().mutable_queryresponse()),
                                              decltype(std::declval<const RepT &>().has_querystatus())>> : std::true_type {};

// Appends the rows of `parts` to `out` in order, moving the row objects (not copying them), and takes the last queryStatus. `out` and
// every part must be on the same arena (or all on the heap): a row moved between two different owners would be freed twice.
template <class RepT>
void MergeParts(RepT &out, const std::vector<RepT *> &parts) {
    using Row = typename std::remove_reference_t<decltype(*out.mutable_queryresponse())>::value_type;
    int total_rows = out.queryresponse_size();
    for (const RepT *part : parts) total_rows += part->queryresponse_size();
    out.mutable_queryresponse()->Reserve(total_rows);
    std::vector<Row *> moved;
    for (RepT *part : parts) {
        const int rows = part->queryresponse_size();
        moved.resize(static_cast<size_t>(rows));
        part->mutable_queryresponse()->UnsafeArenaExtractSubrange(0, rows, moved.data());   // same arena on both sides: no copy
        for (Row *row : moved) out.mutable_queryresponse()->UnsafeArenaAddAllocated(row);
        if (part->has_querystatus()) out.set_querystatus(*part->mutable_querystatus());
    }
}

// Parses the fragments piece by piece on a team the policy sizes and appends the rows to `out` in order (moving the row objects, not
// copying them), taking the last queryStatus. Returns false, with `out` untouched, when the reply is not worth splitting (fewer than two
// pieces), not well framed, or a piece does not parse.
// `arena` is the arena `out` was created on (nullptr when `out` lives on the heap): the pieces are created on it too, so the rows move
// between messages of ONE arena. A row moved between two different owners would be freed twice or read after its owner is gone.
template <class RepT>
[[nodiscard]] bool ParseFragmentsParallel(const std::vector<Fragment> &fragments, RepT &out, const ParallelismPolicy &policy,
                                          size_t piece_bytes = PARSE_PIECE_BYTES, google::protobuf::Arena *arena = nullptr) {
    static_assert(IsFragmentMergeable<RepT>::value, "RepT needs repeated queryResponse and optional queryStatus");
    std::vector<Fragment> pieces;
    std::vector<std::unique_ptr<RepT>> heap_parts;   // used when there is no arena; an arena owns its pieces
    std::vector<RepT *> parts;
    std::atomic<bool> failed{false};
    try {
        if (!SplitIntoPiecesParallel(fragments, piece_bytes, policy, pieces) || pieces.size() < 2) return false;
        for (const Fragment &piece : pieces) {
            if (piece.size > static_cast<size_t>(INT_MAX)) return false;
        }
        parts.reserve(pieces.size());
        for (size_t k = 0; k < pieces.size(); ++k) {
            if (arena != nullptr) {
                parts.push_back(google::protobuf::Arena::Create<RepT>(arena));
            } else {
                heap_parts.push_back(std::make_unique<RepT>());
                parts.push_back(heap_parts.back().get());
            }
        }
        RunJobs(parts.size(), policy, [&](size_t k) {
            if (failed.load(std::memory_order_relaxed)) return;
            if (!parts[k]->ParseFromArray(pieces[k].data, static_cast<int>(pieces[k].size))) failed.store(true, std::memory_order_relaxed);
        });
    } catch (...) {   // out of memory in a worker: let the caller try the serial parse
        return false;
    }
    if (failed.load()) return false;

    MergeParts(out, parts);
    return true;
}

// A reply of rows together with the arena it lives on. Freeing a parsed reply message by message was the biggest cost left in a 1M-row
// RangeQuery (4-7 s of 10-14 s: 128 million small objects, one free each); an arena hands its memory back in a few large blocks (about
// 0.5 s) and also makes the parse itself cheaper. Move-only; use it like a pointer to the reply (`reply->queryresponse()`, `*reply`).
// Everything read from it (rows, strings) stays valid exactly as long as the ArenaReply that produced it, and no longer.
template <class RepT>
class ArenaReply {
 public:
    ArenaReply() : arena_(std::make_unique<google::protobuf::Arena>()), rep_(google::protobuf::Arena::Create<RepT>(arena_.get())) {}
    ArenaReply(std::unique_ptr<google::protobuf::Arena> arena, RepT *rep) : arena_(std::move(arena)), rep_(rep) {}
    ArenaReply(ArenaReply &&other) noexcept : arena_(std::move(other.arena_)), rep_(std::exchange(other.rep_, nullptr)) {}
    ArenaReply &operator=(ArenaReply &&other) noexcept {
        if (this != &other) {
            arena_ = std::move(other.arena_);   // frees the reply this one held
            rep_ = std::exchange(other.rep_, nullptr);
        }
        return *this;
    }
    ArenaReply(const ArenaReply &) = delete;
    ArenaReply &operator=(const ArenaReply &) = delete;

    RepT *get() const noexcept { return rep_; }
    RepT *operator->() const noexcept { return rep_; }
    RepT &operator*() const noexcept { return *rep_; }

 private:
    std::unique_ptr<google::protobuf::Arena> arena_;
    RepT *rep_ = nullptr;
};

// The reply's frames parsed as ONE message: a single frame as it is, several joined in order first. For every reply that is not worth (or not
// fit for) the piece-by-piece parse. Protobuf cannot parse a message of 2 GB or more as one (its sizes are int): such a reply is refused
// with std::length_error rather than parsed wrongly, and `into` is left untouched.
template <class RepT>
void ParseFragmentsSerially(const std::vector<Fragment> &fragments, RepT &into) {
    size_t total = 0;
    for (const Fragment &fragment : fragments) total += fragment.size;
    if (total > static_cast<size_t>(INT_MAX)) {
        throw std::length_error("XStore_Client: a reply of " + std::to_string(total) +
                                " bytes cannot be parsed as one protobuf message (2 GB limit) and its frames could not be parsed piece by piece");
    }
    if (fragments.size() == 1) {
        (void)into.ParseFromArray(fragments.front().data, static_cast<int>(fragments.front().size));
    } else if (!fragments.empty()) {
        std::string joined;
        joined.reserve(total);
        for (const Fragment &fragment : fragments) joined.append(static_cast<const char *>(fragment.data), fragment.size);
        (void)into.ParseFromArray(joined.data(), static_cast<int>(joined.size()));
    }
}

// ---- memory budget ----
// A received reply costs the client about 5 times its size on the wire (every value is an object), so a reply that is only a few GB on the
// wire can take the machine's memory. A call that receives its reply as messages counts the memory it holds as it goes (the received frames
// plus the arena of parsed objects) and stops with MemoryBudgetExceeded once that passes its budget, instead of running the machine out.
// Default budget: 95% of the memory available when the reply turns out to be big (the smaller of MemAvailable and the cgroup headroom).

inline constexpr size_t NO_MEMORY_BUDGET = std::numeric_limits<size_t>::max();   // XStoreClient::SetMemoryBudget(NO_MEMORY_BUDGET): no limit

class MemoryBudgetExceeded : public std::runtime_error {
 public:
    using std::runtime_error::runtime_error;
};

// "MemAvailable:   61000000 kB" of /proc/meminfo, in bytes.
[[nodiscard]] inline std::optional<size_t> ParseMemAvailableBytes(std::string_view meminfo) {
    constexpr std::string_view KEY = "MemAvailable:";
    const size_t at = meminfo.find(KEY);
    if (at == std::string_view::npos) return std::nullopt;
    size_t pos = at + KEY.size();
    while (pos < meminfo.size() && meminfo[pos] == ' ') ++pos;
    size_t kb = 0, digits = 0;
    for (; pos < meminfo.size() && meminfo[pos] >= '0' && meminfo[pos] <= '9'; ++pos, ++digits) kb = kb * 10 + static_cast<size_t>(meminfo[pos] - '0');
    return digits > 0 ? std::optional<size_t>(kb * 1024) : std::nullopt;
}

// The content of a cgroup v2 memory.max / memory.current file: a number of bytes, or "max" (no limit).
[[nodiscard]] inline std::optional<size_t> ParseCgroupBytes(std::string_view text) {
    size_t value = 0, digits = 0;
    for (size_t pos = 0; pos < text.size() && text[pos] >= '0' && text[pos] <= '9'; ++pos, ++digits) value = value * 10 + static_cast<size_t>(text[pos] - '0');
    return digits > 0 ? std::optional<size_t>(value) : std::nullopt;
}

namespace detail {
[[nodiscard]] inline std::string ReadSmallFile(const char *path) {
    std::ifstream in(path);
    char buffer[8192];
    in.read(buffer, sizeof buffer);
    return std::string(buffer, static_cast<size_t>(in.gcount()));
}
}  // namespace detail

// The memory this process can still use: the smaller of the machine's MemAvailable and, in a cgroup with a limit, what is left of it.
[[nodiscard]] inline std::optional<size_t> AvailableMemoryBytes() {
    std::optional<size_t> available = ParseMemAvailableBytes(detail::ReadSmallFile("/proc/meminfo"));
    const auto limit = ParseCgroupBytes(detail::ReadSmallFile("/sys/fs/cgroup/memory.max"));
    const auto current = ParseCgroupBytes(detail::ReadSmallFile("/sys/fs/cgroup/memory.current"));
    if (limit && current) {
        const size_t headroom = *limit > *current ? *limit - *current : 0;
        available = available ? std::min(*available, headroom) : headroom;
    }
    return available;
}

// configured: 0 = automatic (`fraction` of `available`; no limit where that is unknown), NO_MEMORY_BUDGET = no limit, anything else = that many bytes.
[[nodiscard]] inline std::optional<size_t> MemoryBudgetBytes(size_t configured, std::optional<size_t> available, double fraction) {
    if (configured == NO_MEMORY_BUDGET) return std::nullopt;
    if (configured != 0) return configured;
    if (!available) return std::nullopt;
    return static_cast<size_t>(static_cast<double>(*available) * fraction);
}

// ---- windows ----
// A result bigger than memory is read as windows: successive ordinary (streamed) calls over slices of the range, each handed to the caller and
// then dropped, the next one fetched on a second client while the caller works on the current one. A window of W bytes holds at most
// W / (row length + 16) timestamps (an entry of a reply is never longer than a row plus its 16 bytes of framing), so its reply is bounded
// whatever the data, and the memory it needs is about 6 times that on the client. Mystic, 1M rows of 1.42 GB: windows of 71-284 MB took
// the time of one call (prefetched ones 8-10% less) in 4-11 times less memory; 28 MB windows were 26-35% slower.
struct WindowOptions {
    size_t window_bytes = 0;     // 0 = DefaultWindowBytes of the memory budget
    bool prefetch = true;        // fetch the next window while the current one is consumed (two windows in memory)
    int32_t row_length = 0;      // the database's row length; 0 = ask the server (one INFO call)
};

[[nodiscard]] inline uint64_t WindowTimestamps(size_t window_bytes, int32_t row_length) {
    const uint64_t entry = static_cast<uint64_t>(std::max<int32_t>(row_length, 0)) + 16;
    return std::max<uint64_t>(1, window_bytes / entry);
}

// Two windows in flight, each about 6 times its size in objects: a twelfth of the budget, between 4 and 256 MiB.
[[nodiscard]] inline size_t DefaultWindowBytes(std::optional<size_t> budget) {
    constexpr size_t FLOOR = 4ull << 20, CAP = 256ull << 20;
    if (!budget) return CAP;
    return std::clamp<size_t>(*budget / 12, FLOOR, CAP);
}

// ---- requests sent without waiting for each answer ----
inline constexpr size_t PIPELINE_IN_FLIGHT = 4;                  // unanswered requests at most
inline constexpr size_t PIPELINED_PART_BYTES = 64ull << 20;      // the size of a part of a pipelined BatchInsert (23-360 MB all measured alike)

// Adds up the parts of a split call whose success status reads "<prefix> - <marker><count><suffix>" (Inserted N elements successfully,
// Deleted N items): the first part that did not succeed is what the caller gets back instead.
class SplitStatusTally {
 public:
    SplitStatusTally(std::string_view marker, std::string_view suffix) : marker_(marker), suffix_(suffix) {}

    // false if this part did not succeed (its status is then `failure()`, the first one only)
    bool Add(const std::string &status) {
        const size_t at = status.find(marker_);
        if (status.compare(0, 6, "[INFO]") != 0 || at == std::string::npos) {
            if (failure_.empty()) failure_ = status.empty() ? std::string("(empty reply)") : status;
            return false;
        }
        if (parts_ == 0) prefix_ = status.substr(0, at);
        count_ += std::strtoull(status.c_str() + at + marker_.size(), nullptr, 10);
        ++parts_;
        return true;
    }
    [[nodiscard]] bool failed() const { return !failure_.empty(); }
    [[nodiscard]] const std::string &failure() const { return failure_; }
    [[nodiscard]] std::string Combined() const {
        return prefix_ + std::string(marker_) + std::to_string(count_) + std::string(suffix_) + " (" + std::to_string(parts_) + " requests)";
    }

 private:
    std::string marker_, suffix_, prefix_, failure_;
    uint64_t count_ = 0;
    size_t parts_ = 0;
};

// The same parse for a reply that is still arriving: Add() takes the frames in order as they are received, cuts each into pieces and hands
// them to a team (JobStream) that parses while the next frames are on the wire; Finish() joins the team and merges the rows into `out`.
// Finish() returns false, with `out` untouched, when any frame is not well framed or a piece does not parse (or memory ran out): the caller
// then parses the frames it kept serially. The frames' bytes must stay valid until Finish() returns.
template <class RepT>
class ReplyStreamParser {
 public:
    static_assert(IsFragmentMergeable<RepT>::value, "RepT needs repeated queryResponse and optional queryStatus");

    // `arena`: the arena `out` was created on (nullptr when it lives on the heap); the pieces are created on it too.
    ReplyStreamParser(RepT &out, google::protobuf::Arena *arena, const ParallelismPolicy &policy)
        : out_(out), arena_(arena), parse_([this](Piece &piece) { Parse(piece); }), stream_(policy, parse_) {}

    ReplyStreamParser(const ReplyStreamParser &) = delete;
    ReplyStreamParser &operator=(const ReplyStreamParser &) = delete;

    void Add(Fragment fragment) {
        if (bad_) return;   // already going to be parsed serially: no point in more work
        try {
            std::vector<Fragment> cut;
            if (!detail::SplitFragment(fragment, PARSE_PIECE_BYTES, cut)) {
                bad_ = true;
                return;
            }
            for (const Fragment &piece : cut) {
                if (piece.size > static_cast<size_t>(INT_MAX)) {
                    bad_ = true;
                    return;
                }
                RepT *part = nullptr;
                if (arena_ != nullptr) {
                    part = google::protobuf::Arena::Create<RepT>(arena_);
                } else {
                    heap_parts_.push_back(std::make_unique<RepT>());
                    part = heap_parts_.back().get();
                }
                parts_.push_back(part);
                stream_.Submit(Piece{piece, part});
            }
        } catch (...) {   // out of memory: the serial parse will do
            bad_ = true;
        }
    }

    [[nodiscard]] bool Finish() {
        try {
            stream_.Finish();
            if (bad_ || failed_.load()) return false;
            MergeParts(out_, parts_);
            return true;
        } catch (...) {
            return false;
        }
    }

    // Pieces parsed so far (for tests: parsing must not wait for Finish()).
    [[nodiscard]] size_t PiecesParsed() const { return parsed_.load(); }

 private:
    struct Piece {
        Fragment fragment;
        RepT *part;
    };

    void Parse(Piece &piece) {
        if (failed_.load(std::memory_order_relaxed)) return;
        if (!piece.part->ParseFromArray(piece.fragment.data, static_cast<int>(piece.fragment.size))) failed_.store(true);
        parsed_.fetch_add(1, std::memory_order_relaxed);
    }

    RepT &out_;
    google::protobuf::Arena *arena_;
    std::vector<std::unique_ptr<RepT>> heap_parts_;   // used when there is no arena; an arena owns its pieces
    std::vector<RepT *> parts_;                        // in order, for the merge
    bool bad_ = false;
    std::atomic<bool> failed_{false};
    std::atomic<size_t> parsed_{0};
    std::function<void(Piece &)> parse_;
    JobStream<Piece, std::function<void(Piece &)>> stream_;   // last: its helpers start working at once and use everything above
};

}  // namespace xstore_client

// ======================================================================================================================================
// Part 3 of 3: the XStore client.
// ======================================================================================================================================

namespace xstore_client {

// The request as a ZMQ message. Protobuf cannot serialize a message of 2 GB or more: SerializeToArray fails and leaves its buffer zeroed, and
// sending that would make the server drop the request and answer with an empty reply the caller would take for a status. So a request over
// `limit` (default: the most protobuf can serialize, 2^31 - 1) is refused with std::length_error before anything is sent. Only the calls that carry
// many rows or timestamps (BatchInsert, BatchDelete, BatchQuery) can get that big: send them in several calls.
[[nodiscard]] inline zmq::message_t SerializeRequest(const XStore_Proto::Request &request, size_t limit = static_cast<size_t>(INT_MAX)) {
    const size_t byte_size = request.ByteSizeLong();
    if (byte_size > limit) {
        throw std::length_error("XStore_Client: the request is " + std::to_string(byte_size) + " bytes; protobuf cannot serialize a message larger than " +
                                std::to_string(limit) + " bytes -- send the rows in several calls");
    }
    zmq::message_t message(byte_size);
    if (!request.SerializeToArray(message.data(), static_cast<int>(byte_size))) {
        throw std::runtime_error("XStore_Client: the request could not be serialized");
    }
    return message;
}


namespace detail {
[[nodiscard]] inline size_t VarintLength(uint64_t value) {
    size_t length = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++length;
    }
    return length;
}
inline uint8_t *PutVarint(uint8_t *out, uint64_t value) {
    while (value >= 0x80) {
        *out++ = static_cast<uint8_t>(value | 0x80);
        value >>= 7;
    }
    *out++ = static_cast<uint8_t>(value);
    return out;
}
}  // namespace detail

// The BatchInsert request for rows [begin, end), written straight from the caller's rows into one buffer of exactly the right size: the bytes
// protobuf would make of the same Request (batchInsert = field 8 of the envelope, timeoutMs = field 50), without copying every row into a
// Request message (a heap object per value) and walking that twice. `sizes[i - begin]` must be rows[i].second.ByteSizeLong(), computed just
// before by the caller (the row is then serialized with its cached size, as the server does). Refused with std::length_error over `limit`,
// like SerializeRequest. Fields at their proto3 default (a zero timestamp, a zero durability mode, an empty database name, timeout_ms 0) are
// left out, as protobuf leaves them out; a negative timeout_ms leaves the field out too.
[[nodiscard]] inline zmq::message_t EncodeBatchInsert(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows,
                                                      size_t begin, size_t end, const std::vector<uint32_t> &sizes,
                                                      XStore_Proto::WalDurabilityMode durability_mode, int timeout_ms,
                                                      size_t limit = static_cast<size_t>(INT_MAX)) {
    using detail::PutVarint;
    using detail::VarintLength;
    const uint64_t durability = static_cast<uint64_t>(durability_mode);
    const uint64_t rtype = static_cast<uint64_t>(XStore_Proto::BATCHINSERT);
    size_t body = 0;   // the BatchInsert_REQ
    if (rtype != 0) body += 1 + VarintLength(rtype);
    if (!db_name.empty()) body += 1 + VarintLength(db_name.size()) + db_name.size();
    for (size_t i = begin; i < end; ++i) {
        const size_t row_bytes = sizes[i - begin];
        const size_t entry = (rows[i].first != 0 ? 1 + VarintLength(rows[i].first) : 0) + 1 + VarintLength(row_bytes) + row_bytes;
        body += 1 + VarintLength(entry) + entry;
    }
    if (durability != 0) body += 1 + VarintLength(durability);
    const uint64_t timeout = timeout_ms > 0 ? static_cast<uint64_t>(timeout_ms) : 0;
    const size_t total = 1 + VarintLength(body) + body + (timeout != 0 ? 2 + VarintLength(timeout) : 0);
    if (total > limit) {
        throw std::length_error("XStore_Client: the request is " + std::to_string(total) + " bytes; protobuf cannot serialize a message larger than " +
                                std::to_string(limit) + " bytes -- send the rows in several calls");
    }
    zmq::message_t message(total);
    uint8_t *out = static_cast<uint8_t *>(message.data());
    *out++ = 0x42;   // Request.batchInsert (8), length-delimited
    out = PutVarint(out, body);
    if (rtype != 0) {
        *out++ = 0x08;
        out = PutVarint(out, rtype);
    }
    if (!db_name.empty()) {
        *out++ = 0x12;
        out = PutVarint(out, db_name.size());
        std::memcpy(out, db_name.data(), db_name.size());
        out += db_name.size();
    }
    for (size_t i = begin; i < end; ++i) {   // BatchInsert_DATA (3): timestamp (1), data (2)
        const size_t row_bytes = sizes[i - begin];
        const size_t entry = (rows[i].first != 0 ? 1 + VarintLength(rows[i].first) : 0) + 1 + VarintLength(row_bytes) + row_bytes;
        *out++ = 0x1a;
        out = PutVarint(out, entry);
        if (rows[i].first != 0) {
            *out++ = 0x08;
            out = PutVarint(out, rows[i].first);
        }
        *out++ = 0x12;
        out = PutVarint(out, row_bytes);
        out = rows[i].second.SerializeWithCachedSizesToArray(out);
    }
    if (durability != 0) {
        *out++ = 0x20;   // durabilityMode (4)
        out = PutVarint(out, durability);
    }
    if (timeout != 0) {
        *out++ = 0x90;   // Request.timeoutMs (50), varint
        *out++ = 0x03;
        out = PutVarint(out, timeout);
    }
    return message;
}

}  // namespace xstore_client

// Client-side CURVE identity: this client's own keypair plus the server's
// public key (which the client must already know out-of-band -- see the
// design doc). Deliberately duplicates the tiny amount of Z85-file-reading
// logic XStore_Curve.h has server-side rather than including that header,
// so this file keeps compiling standalone if client_src/Cpp is copied out
// on its own without the rest of the repo.
struct XStoreCurveClientAuth {
  std::string client_public_key_z85;
  std::string client_secret_key_z85;
  std::string server_public_key_z85;

  // client_secret_key_file's sibling ".pub" (same directory/stem) supplies
  // the client's own public key -- the same xstore-keys gen convention
  // used server-side.
  static XStoreCurveClientAuth LoadFromFiles(std::string_view client_secret_key_file,
                                              std::string_view server_public_key_file) {
      auto read_z85 = [](const std::filesystem::path &p) {
          std::ifstream f(p);
          if (!f.is_open()) {
              throw std::runtime_error("XStoreCurveClientAuth: cannot open " + p.string());
          }
          std::string key;
          f >> key;
          if (key.size() != 40) {
              throw std::runtime_error("XStoreCurveClientAuth: " + p.string() +
                                        " does not contain a 40-character Z85 key");
          }
          return key;
      };
      std::filesystem::path secret_path(client_secret_key_file);
      std::filesystem::path public_path = secret_path;
      public_path.replace_extension(".pub");

      XStoreCurveClientAuth auth;
      auth.client_secret_key_z85 = read_z85(secret_path);
      auth.client_public_key_z85 = read_z85(public_path);
      auth.server_public_key_z85 = read_z85(std::filesystem::path(server_public_key_file));
      return auth;
  }
};

struct FileInfo {
  std::string file_name;
  // actual = real bytes on disk (st_blocks*512, sparse-hole-aware);
  // apparent = logical/reported size (st_size). apparent can exceed actual
  // for a preallocated file that still has unwritten holes.
  uint64_t actual_size_bytes;
  uint64_t apparent_size_bytes;
};

struct DbInfo {
  std::string db_name;
  XStore_Proto::TimeUnit db_resolution;
  // Matches XStore_Proto::DbStats.totalSizeInByte (uint64) -- sum of every
  // file's actual_size_bytes, i.e. real disk usage of this DB. A float here
  // silently loses precision past 2^24 bytes (16MB) -- easy to hit for any
  // real time-series file.
  uint64_t total_size_in_byte;
  std::vector<FileInfo> files;
  // Matches XStore_Proto::DbStats.rowLength -- the configured per-row slot
  // size (bytes) every insert's serialized RowData is zero-padded to.
  // Compare against a real row's actual serialized size to tell whether
  // this was sized too large (wasted padding/row) or too small.
  int32_t row_length;
  // Matches XStore_Proto::DbStats.columnNames -- optional per-index column
  // names set at CreateDB time (see ResolveColumnIndex). Empty if the DB
  // was created without names.
  std::vector<std::string> column_names;
};

struct SysInfo {
  std::string server_addr;
  int num_threads;
  std::vector<DbInfo> db_info_list;
};

// Factory helpers for XStore_Proto::FData's oneof. proto3 picks a different
// wire encoding for sint32/fixed32/sfixed32 even though they all take a
// plain int32_t in C++, so overloading on parameter type alone can't tell
// them apart -- name the intent instead. Purely additive: existing callers
// that build FData by hand (field->set_stringdata(...)) still work.
namespace xstore_fdata {
  inline XStore_Proto::FData Double(double v)        { XStore_Proto::FData f; f.set_doubledata(v);   return f; }
  inline XStore_Proto::FData Float(float v)           { XStore_Proto::FData f; f.set_floatdata(v);    return f; }
  inline XStore_Proto::FData Int32(int32_t v)         { XStore_Proto::FData f; f.set_int32data(v);    return f; }
  inline XStore_Proto::FData Int64(int64_t v)         { XStore_Proto::FData f; f.set_int64data(v);    return f; }
  inline XStore_Proto::FData UInt32(uint32_t v)       { XStore_Proto::FData f; f.set_uint32data(v);   return f; }
  inline XStore_Proto::FData UInt64(uint64_t v)       { XStore_Proto::FData f; f.set_uint64data(v);   return f; }
  inline XStore_Proto::FData SInt32(int32_t v)        { XStore_Proto::FData f; f.set_sint32data(v);   return f; }
  inline XStore_Proto::FData SInt64(int64_t v)        { XStore_Proto::FData f; f.set_sint64data(v);   return f; }
  inline XStore_Proto::FData Fixed32(uint32_t v)      { XStore_Proto::FData f; f.set_fixed32data(v);  return f; }
  inline XStore_Proto::FData Fixed64(uint64_t v)      { XStore_Proto::FData f; f.set_fixed64data(v);  return f; }
  inline XStore_Proto::FData SFixed32(int32_t v)      { XStore_Proto::FData f; f.set_sfixed32data(v); return f; }
  inline XStore_Proto::FData SFixed64(int64_t v)      { XStore_Proto::FData f; f.set_sfixed64data(v); return f; }
  inline XStore_Proto::FData Bool(bool v)             { XStore_Proto::FData f; f.set_booldata(v);     return f; }
  inline XStore_Proto::FData String(std::string v)    { XStore_Proto::FData f; f.set_stringdata(std::move(v)); return f; }
  inline XStore_Proto::FData Bytes(std::string v)     { XStore_Proto::FData f; f.set_bytesdata(std::move(v));  return f; }
}

class XStoreClient {
 private:
  zmq::context_t ctx;
  zmq::socket_t sock { ctx, zmq::socket_type::req };

  // Reused across calls instead of a fresh std::vector<zmq::message_t>
  // constructed on every SendRecv -- .clear() keeps the already-grown
  // backing storage. Safe as a plain member (not thread_local): a single
  // XStore_Client instance's REQ socket is strict send-then-recv lockstep
  // already, so it was never safe to call concurrently from multiple
  // threads to begin with.
  std::vector<zmq::message_t> recv_buf;

  // This client's own send/recv timeout (ctor/Reconnect's timeout_ms); -1 = wait forever. Filled into every
  // Request's timeoutMs field (clamped to uint32, 0 = not sent) so the server can abandon a request this socket
  // would already have given up waiting for, instead of a worker blocking on it (or the server's memory pool)
  // forever. A plain member, not thread_local: this client's REQ socket is already single-threaded use only.
  int timeout_ms_ = -1;

  // Threads used to parse a big reply of rows; 0 = adaptive. See SetParseThreads.
  size_t parse_threads_ = 0;

  // Memory a reply received as messages may hold: 0 = automatic. See SetMemoryBudget.
  size_t memory_budget_ = 0;

  // The size above which a BatchInsert is sent as several requests. See BatchInsert and SetMaxRequestBytes.
  size_t max_request_bytes_ = 512ull << 20;

  // What a stream socket needs to be (re)created, and the socket itself: a DEALER used by the calls that return rows, made on the first such
  // call. A REQ socket delivers one multipart reply, whole, so it cannot show a chunk before the last is in; a DEALER receives a reply sent
  // as separate messages (Request.streamOk) one at a time. It is lockstep by convention only, so any call that fails between its send and
  // the last message of its reply drops the socket (a late message must never be taken for the next call's reply).
  std::string endpoint_, server_addr_, server_port_;
  std::optional<XStoreCurveClientAuth> curve_auth_;
  std::optional<zmq::socket_t> stream_sock_;
  std::deque<zmq::message_t> stream_buf_;   // the frames of the reply being received; a deque never moves its elements

  using FragmentList = std::vector<xstore_client::Fragment>;

  static size_t BytesOf(const FragmentList &fragments) {
      size_t total = 0;
      for (const auto &fragment : fragments) total += fragment.size;
      return total;
  }

  static void ApplySocketOptions(zmq::socket_t &socket, int timeout_ms, const std::optional<XStoreCurveClientAuth> &curve_auth) {
      socket.set(zmq::sockopt::rcvtimeo, timeout_ms);
      socket.set(zmq::sockopt::sndtimeo, timeout_ms);
      socket.set(zmq::sockopt::linger, 0);
      if (curve_auth) {
          socket.set(zmq::sockopt::curve_serverkey, curve_auth->server_public_key_z85);
          socket.set(zmq::sockopt::curve_publickey, curve_auth->client_public_key_z85);
          socket.set(zmq::sockopt::curve_secretkey, curve_auth->client_secret_key_z85);
      }
  }

  // Sends the request on the REQ socket and leaves the reply's frames in recv_buf.
  void Exchange(XStore_Proto::Request &to_server) {
      if (timeout_ms_ >= 0) to_server.set_timeoutms(static_cast<uint32_t>(timeout_ms_));
      ExchangeMessage(xstore_client::SerializeRequest(to_server));
  }

  // The same for a request already serialized (a BatchInsert encoded straight from the caller's rows).
  void ExchangeMessage(zmq::message_t &&z_out) {
      if (!sock.send(z_out, zmq::send_flags::none)) {
          throw std::runtime_error("XStore_Client: send failed/timed out");
      }

      recv_buf.clear();
      if (!zmq::recv_multipart(sock, std::back_inserter(recv_buf)) || recv_buf.empty()) {
          throw std::runtime_error("XStore_Client: recv failed/timed out");
      }
  }

  FragmentList FragmentsOfRecvBuf() const {
      FragmentList fragments;
      fragments.reserve(recv_buf.size());
      for (const auto &part : recv_buf) fragments.push_back({part.data(), part.size()});
      return fragments;
  }

  // A big reply of rows (RangeQuery, BatchQuery, FindByValue, FindByPattern) is cut at row boundaries and parsed on a team of threads
  // that sizes itself to the machine's load (part 2 above) -- the single ParseFromArray was most of a big RangeQuery's time.
  // Returns false (`into` untouched) for every other reply and for any reply that cannot be handled that way.
  template <typename RepT>
  bool TryParseInParallel(const FragmentList &fragments, RepT &into, google::protobuf::Arena *arena) {
      if constexpr (xstore_client::IsFragmentMergeable<RepT>::value) {
          if (BytesOf(fragments) < xstore_client::MIN_PARALLEL_PARSE_BYTES) return false;
          return xstore_client::ParseFragmentsParallel(fragments, into, xstore_client::ParsePolicy(parse_threads_),
                                                        xstore_client::PARSE_PIECE_BYTES, arena);
      } else {
          (void)fragments;
          (void)into;
          (void)arena;
          return false;
      }
  }

  // rows [begin, end) as ONE BatchInsert request, encoded straight from the rows. `sizes` (their serialized sizes) may be given by a caller that
  // has just measured them; otherwise they are measured here.
  std::string BatchInsertPart(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows, size_t begin,
                              size_t end, XStore_Proto::WalDurabilityMode durability_mode, const std::vector<uint32_t> *sizes = nullptr) {
      std::vector<uint32_t> measured;
      if (sizes == nullptr) {
          measured.reserve(end - begin);
          for (size_t i = begin; i < end; ++i) measured.push_back(static_cast<uint32_t>(rows[i].second.ByteSizeLong()));
          sizes = &measured;
      }
      return SendRecvMessage<XStore_Proto::BatchInsert_REP>(xstore_client::EncodeBatchInsert(db_name, rows, begin, end, *sizes, durability_mode, timeout_ms_))
          .insertstatus();
  }

  // The next part of `rows` from `begin`: its end and the sizes of its rows, cut once the part would pass `part_bytes` (a row bigger than that
  // still goes, alone).
  size_t NextInsertPart(const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows, size_t begin, size_t part_bytes,
                        std::vector<uint32_t> &sizes) const {
      sizes.clear();
      size_t end = begin, bytes = 0;
      while (end < rows.size()) {
          const size_t row_bytes = rows[end].second.ByteSizeLong();
          if (end > begin && bytes + row_bytes + 16 > part_bytes) break;
          bytes += row_bytes + 16;
          sizes.push_back(static_cast<uint32_t>(row_bytes));
          ++end;
      }
      return end;
  }

  // Could these rows come to a quarter of max_request_bytes_ or more? Judged from up to 32 evenly spaced rows, so a normal batch costs 32
  // size computations (none under 16 rows); only a batch that might be big is measured exactly (and a request that still turns out too big is refused by
  // xstore_client::SerializeRequest rather than sent).
  bool MayNeedSplitting(const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows, size_t limit) const {
      if (rows.size() < 16) return false;   // a request of fewer rows than this is never judged: it would need rows of tens of MB each
      const size_t samples = std::min<size_t>(rows.size(), 32);
      size_t sampled_bytes = 0;
      for (size_t k = 0; k < samples; ++k) sampled_bytes += rows[k * rows.size() / samples].second.ByteSizeLong() + 16;
      return sampled_bytes / samples * rows.size() >= limit / 4;
  }

  // Sends the requests `next_request` makes, already serialized (one at a time, until it returns nullopt), on the stream socket keeping at most `in_flight` unanswered,
  // and gives each reply to `on_reply` as it arrives (they may come in any order; `on_reply` returns false to stop sending: the replies still
  // owed are read before returning). A failure drops the socket, as for any call, since answers are still on their way.
  template <typename RepT, typename Next, typename OnReply>
  void SendPipelined(size_t in_flight, Next &&next_request, OnReply &&on_reply) {
      try {
          zmq::socket_t &socket = StreamSocket();
          size_t outstanding = 0;
          auto read_one = [&]() {
              stream_buf_.clear();
              FragmentList fragments;
              (void)RecvStreamMessage(socket, fragments);
              RepT reply;
              xstore_client::ParseFragmentsSerially(fragments, reply);
              stream_buf_.clear();
              --outstanding;
              return on_reply(reply);
          };
          bool more = true;
          while (more) {
              while (more && outstanding < in_flight) {
                  std::optional<zmq::message_t> body = next_request();
                  if (!body) {
                      more = false;
                      break;
                  }
                  if (!socket.send(zmq::message_t(), zmq::send_flags::sndmore) || !socket.send(*body, zmq::send_flags::none)) {
                      throw std::runtime_error("XStore_Client: send failed/timed out");
                  }
                  ++outstanding;
              }
              if (outstanding == 0) break;
              if (!read_one()) more = false;
          }
          while (outstanding > 0) (void)read_one();
      } catch (...) {
          stream_sock_.reset();
          stream_buf_.clear();
          throw;
      }
  }

  // The row length of a database (one INFO call).
  int32_t RowLengthOf(std::string_view db_name) {
      auto upper = [](std::string_view name) {
          std::string out(name);
          std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
          return out;
      };
      const std::string wanted = upper(db_name);
      for (const DbInfo &db : InfoRequest().db_info_list) {
          if (upper(db.db_name) == wanted) return db.row_length;
      }
      throw std::runtime_error("XStore_Client: no database named '" + std::string(db_name) + "' on the server");
  }

  // Reads `total_units` units (timestamps, or the entries of a list) as windows of `per_window`: fetch(client, first_unit, count) makes one
  // streamed call, on_window gets its reply and the reply is dropped. With prefetch the next window is fetched on a second client meanwhile;
  // the memory budget is then shared by the two. Returns the rows delivered.
  template <typename RepT, typename Fetch>
  uint64_t ReadInWindows(uint64_t total_units, uint64_t per_window, bool prefetch, Fetch &&fetch,
                         const std::function<void(const RepT &)> &on_window) {
      const uint64_t windows = total_units / per_window + (total_units % per_window != 0 ? 1 : 0);
      auto count_of = [&](uint64_t w) { return std::min(per_window, total_units - w * per_window); };
      uint64_t rows = 0;
      if (!prefetch || windows <= 1) {
          for (uint64_t w = 0; w < windows; ++w) {
              const xstore_client::ArenaReply<RepT> reply = fetch(*this, w * per_window, count_of(w));
              rows += static_cast<uint64_t>(reply->queryresponse_size());
              on_window(*reply);
          }
          return rows;
      }
      XStoreClient helper(server_addr_, server_port_, timeout_ms_, /*warmup_on_init=*/false, curve_auth_);
      helper.parse_threads_ = parse_threads_;
      const size_t saved_budget = memory_budget_;
      const std::optional<size_t> budget =
          xstore_client::MemoryBudgetBytes(memory_budget_, memory_budget_ == 0 ? xstore_client::AvailableMemoryBytes() : std::nullopt, 0.95);
      if (budget) {
          memory_budget_ = std::max<size_t>(1, *budget / 2);   // two windows in memory at once
          helper.memory_budget_ = memory_budget_;
      }
      struct Restore {
          size_t &target;
          size_t value;
          ~Restore() { target = value; }
      } restore{memory_budget_, saved_budget};
      XStoreClient *clients[2] = {this, &helper};
      auto launch = [&](uint64_t w) {
          return std::async(std::launch::async, [&, w] { return fetch(*clients[w % 2], w * per_window, count_of(w)); });
      };
      std::future<xstore_client::ArenaReply<RepT>> next = launch(0);
      for (uint64_t w = 0; w < windows; ++w) {
          const xstore_client::ArenaReply<RepT> current = next.get();
          if (w + 1 < windows) next = launch(w + 1);
          rows += static_cast<uint64_t>(current->queryresponse_size());
          on_window(*current);
      }
      return rows;
  }

  // A range [start_time, end_time] read as windows of timestamps; fetch(client, from, to) makes one call for [from, to].
  template <typename RepT, typename Fetch>
  uint64_t ReadInTimeWindows(uint64_t start_time, uint64_t end_time, std::string_view db_name, const xstore_client::WindowOptions &options,
                             Fetch &&fetch, const std::function<void(const RepT &)> &on_window) {
      if (end_time < start_time) throw std::invalid_argument("XStore_Client: the end of the range is before its start");
      const uint64_t span = end_time - start_time;
      const uint64_t total = span == std::numeric_limits<uint64_t>::max() ? span : span + 1;
      return ReadInWindows<RepT>(
          total, WindowLength(db_name, options), options.prefetch,
          [&](XStoreClient &c, uint64_t first, uint64_t count) { return fetch(c, start_time + first, start_time + first + count - 1); }, on_window);
  }

  // The window length in timestamps for these options (looks the row length up when it is not given).
  uint64_t WindowLength(std::string_view db_name, const xstore_client::WindowOptions &options) {
      const int32_t row_length = options.row_length > 0 ? options.row_length : RowLengthOf(db_name);
      size_t window_bytes = options.window_bytes;
      if (window_bytes == 0) {
          const std::optional<size_t> budget =
              xstore_client::MemoryBudgetBytes(memory_budget_, memory_budget_ == 0 ? xstore_client::AvailableMemoryBytes() : std::nullopt, 0.95);
          window_bytes = xstore_client::DefaultWindowBytes(budget);
      }
      return xstore_client::WindowTimestamps(window_bytes, row_length);
  }

  // rows [begin, end) of a BatchDelete's timestamps as ONE request.
  std::string BatchDeletePart(std::string_view db_name, const std::vector<uint64_t> &timestamps, size_t begin, size_t end) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchdelete();
      payload->set_rtype(XStore_Proto::BATCHDELETE);
      payload->set_dbname(std::string(db_name));
      payload->mutable_timestamp()->Reserve(static_cast<int>(end - begin));
      for (size_t i = begin; i < end; ++i) payload->add_timestamp(timestamps[i]);
      return SendRecv<XStore_Proto::BatchDelete_REP>(to_server).deletestatus();
  }

  // Every RPC is: serialize request, blocking send, blocking recv, parse
  // reply. Sharing that here means the 17 request types can't drift out of
  // sync with each other the way copy-pasted send/recv blocks do. ToServer
  // is always the Request envelope -- the oneof case tells the server which
  // payload it's holding, so it only has to parse the wire bytes once.
  template <typename RepT>
  RepT SendRecv(XStore_Proto::Request &to_server) {
      Exchange(to_server);
      return ParseReply<RepT>();
  }

  template <typename RepT>
  RepT SendRecvMessage(zmq::message_t &&request) {
      ExchangeMessage(std::move(request));
      return ParseReply<RepT>();
  }

  // The reply now in recv_buf.
  template <typename RepT>
  RepT ParseReply() {
      const FragmentList fragments = FragmentsOfRecvBuf();
      RepT from_server;
      if (!TryParseInParallel(fragments, from_server, nullptr)) xstore_client::ParseFragmentsSerially(fragments, from_server);
      return from_server;
  }

  // A reply that is complete in `fragments`, parsed onto a new arena. A big reply gets 1-8 MiB blocks (a few allocations for the whole
  // reply); a small one keeps the default small blocks.
  template <typename RepT>
  xstore_client::ArenaReply<RepT> ParseOnArena(const FragmentList &fragments) {
      google::protobuf::ArenaOptions options;
      if (BytesOf(fragments) >= xstore_client::MIN_PARALLEL_PARSE_BYTES) {
          options.start_block_size = size_t{1} << 20;
          options.max_block_size = size_t{8} << 20;
      }
      auto arena = std::make_unique<google::protobuf::Arena>(options);
      RepT *from_server = google::protobuf::Arena::Create<RepT>(arena.get());
      if (!TryParseInParallel(fragments, *from_server, arena.get())) xstore_client::ParseFragmentsSerially(fragments, *from_server);
      return xstore_client::ArenaReply<RepT>(std::move(arena), from_server);
  }

  zmq::socket_t &StreamSocket() {
      if (!stream_sock_) {
          stream_sock_.emplace(ctx, zmq::socket_type::dealer);
          ApplySocketOptions(*stream_sock_, timeout_ms_, curve_auth_);
          stream_sock_->connect(endpoint_);
      }
      return *stream_sock_;
  }

  // One message of a reply: [delimiter][frame...]. Appends the frames to stream_buf_ and `fragments`; true when this was the reply's last
  // message (empty delimiter, like a REQ socket's -- which is also what a server that does not stream sends, the whole reply in one message).
  bool RecvStreamMessage(zmq::socket_t &socket, FragmentList &fragments) {
      zmq::message_t delimiter;
      if (!socket.recv(delimiter, zmq::recv_flags::none)) throw std::runtime_error("XStore_Client: recv failed/timed out");
      for (bool more = delimiter.more(); more;) {
          stream_buf_.emplace_back();
          if (!socket.recv(stream_buf_.back(), zmq::recv_flags::none)) throw std::runtime_error("XStore_Client: recv failed/timed out");
          more = stream_buf_.back().more();
          fragments.push_back({stream_buf_.back().data(), stream_buf_.back().size()});
      }
      return delimiter.size() == 0;
  }

  // The calls that return rows. The request asks the server to send a reply of several chunks as separate messages, and each chunk is cut
  // into pieces and handed to the parse team as soon as it is received, so the parse overlaps the rest of the transfer (and of the server's
  // scan). A server that does not stream (older, or a small reply) answers with one message and this is the same as SendRecv.
  template <typename RepT>
  xstore_client::ArenaReply<RepT> SendRecvOnArena(XStore_Proto::Request &to_server) {
      to_server.set_streamok(true);
      if (timeout_ms_ >= 0) to_server.set_timeoutms(static_cast<uint32_t>(timeout_ms_));
      try {
          zmq::socket_t &socket = StreamSocket();
          zmq::message_t z_out = xstore_client::SerializeRequest(to_server);
          if (!socket.send(zmq::message_t(), zmq::send_flags::sndmore) || !socket.send(z_out, zmq::send_flags::none)) {
              throw std::runtime_error("XStore_Client: send failed/timed out");
          }

          stream_buf_.clear();
          FragmentList fragments;
          bool last = RecvStreamMessage(socket, fragments);
          if (last) {
              auto reply = ParseOnArena<RepT>(fragments);
              stream_buf_.clear();
              return reply;
          }
          // More than one message: a reply of many chunks, so the arena gets big blocks at once.
          google::protobuf::ArenaOptions options;
          options.start_block_size = size_t{1} << 20;
          options.max_block_size = size_t{8} << 20;
          auto arena = std::make_unique<google::protobuf::Arena>(options);
          RepT *from_server = google::protobuf::Arena::Create<RepT>(arena.get());
          xstore_client::ReplyStreamParser<RepT> parser(*from_server, arena.get(), xstore_client::ParsePolicy(parse_threads_));
          const std::optional<size_t> budget =
              xstore_client::MemoryBudgetBytes(memory_budget_, memory_budget_ == 0 ? xstore_client::AvailableMemoryBytes() : std::nullopt, 0.95);
          size_t fed = 0, received_bytes = 0;
          while (true) {
              for (; fed < fragments.size(); ++fed) {
                  received_bytes += fragments[fed].size;
                  parser.Add(fragments[fed]);
              }
              if (budget && received_bytes + arena->SpaceAllocated() > *budget) {
                  throw xstore_client::MemoryBudgetExceeded(
                      "XStore_Client: the reply holds " + std::to_string(received_bytes + arena->SpaceAllocated()) + " bytes of its " +
                      std::to_string(*budget) + "-byte memory budget and was stopped; ask for a narrower range, or raise or lift the limit with SetMemoryBudget");
              }
              if (last) break;
              last = RecvStreamMessage(socket, fragments);
          }
          if (!parser.Finish()) xstore_client::ParseFragmentsSerially(fragments, *from_server);
          stream_buf_.clear();
          return xstore_client::ArenaReply<RepT>(std::move(arena), from_server);
      } catch (...) {
          stream_sock_.reset();   // whatever is still on its way belongs to this call, not to the next
          stream_buf_.clear();
          throw;
      }
  }

 public:
  // Threads used to parse a big reply of rows -- RangeQuery, BatchQuery, FindByValue, FindByPattern. 0 (the default) is adaptive: the
  // parse starts on the calling thread and adds helpers while the machine has idle cores, and they leave when other work needs the cores
  // (cap: XSTORE_CLIENT_PARSE_MAX, default the machine's hardware threads). N > 0 forces N threads (XSTORE_CLIENT_PARSE_THREADS=N does the
  // same for every client of a process); 1 parses on the calling thread only.
  void SetParseThreads(size_t threads) { parse_threads_ = threads; }

  // The memory a big reply may hold (received frames + parsed objects) before the call stops with xstore_client::MemoryBudgetExceeded rather
  // than run the machine out of memory: a number of bytes, 0 (the default) for 95% of the memory available when the reply turns out to be big,
  // or xstore_client::NO_MEMORY_BUDGET for no limit. Counted only for a reply that arrives as several messages (every big one from this
  // server); a reply of one message is parsed whatever its size.
  void SetMemoryBudget(size_t bytes) { memory_budget_ = bytes; }

  // The most bytes one BatchInsert request may hold before the batch is split into several (default 512 MiB; protobuf's own limit is 2 GB).
  // Smaller parts cost a round trip each; bigger ones need more client and server memory at once.
  void SetMaxRequestBytes(size_t bytes) { max_request_bytes_ = std::max<size_t>(1, bytes); }


  // timeoutMs < 0 (default) blocks forever, matching the previous behavior.
  // Pass a positive value so an unreachable/dead server raises instead of
  // hanging this REQ socket indefinitely. warmupOnInit lets callers who
  // don't care about paying the handshake cost upfront (e.g. short-lived
  // clients that may never send a request) skip it.
  XStoreClient(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
               bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      timeout_ms_ = timeout_ms;
      curve_auth_ = curve_auth;
      ApplySocketOptions(sock, timeout_ms, curve_auth_);
      server_addr_ = std::string(server_addr);
      server_port_ = std::string(server_port);
      endpoint_ = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(endpoint_);
      if (warmup_on_init) {
          Warmup();
      }
  }

  // ZMQ connect() is lazy -- TCP + ZMTP handshake actually fires on first
  // send(). Call this right after construction to eat that cost now instead
  // of on the first real RPC. PING is a bare round trip with no server-side
  // XStore work (unlike INFO, which scans the data store directory), so it
  // forces the handshake without the extra I/O. Throws same as any RPC on
  // timeout/unreachable server -- caller must Reconnect() before retrying,
  // same as elsewhere.
  void Warmup() {
      Ping();
  }

  // A REQ socket that hits a send/recv timeout has its strict
  // send-then-recv state machine left in an undefined spot per ZMQ's own
  // docs -- the fix is a fresh socket, not another send. Call this after
  // catching a timeout exception from any RPC below before retrying.
  void Reconnect(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
                 bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      timeout_ms_ = timeout_ms;
      curve_auth_ = curve_auth;
      sock = zmq::socket_t(ctx, zmq::socket_type::req);
      stream_sock_.reset();
      ApplySocketOptions(sock, timeout_ms, curve_auth_);
      server_addr_ = std::string(server_addr);
      server_port_ = std::string(server_port);
      endpoint_ = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(endpoint_);
      if (warmup_on_init) {
          Warmup();
      }
  }

  std::string GetCurrDT() {
      std::stringstream formatted_time;
      std::time_t time_now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      formatted_time << std::put_time(std::localtime(&time_now), "%b %d, %Y %r");
      std::string to_return = formatted_time.str();
      return to_return;
  }

  // ==== PING ====
  // Bare round trip, no server-side XStore work -- cheaper than InfoRequest()
  // for warmup/liveness checks that don't need SysInfo's contents.
  void Ping() {
      XStore_Proto::Request to_server;
      to_server.mutable_ping()->set_rtype(XStore_Proto::PING);
      SendRecv<XStore_Proto::Ping_REP>(to_server);
  }

  // ==== INFO ====
  SysInfo InfoRequest() {
      XStore_Proto::Request to_server;
      to_server.mutable_info()->set_rtype(XStore_Proto::INFO);
      XStore_Proto::SysInfo_REP from_server = SendRecv<XStore_Proto::SysInfo_REP>(to_server);

      SysInfo to_return;
      to_return.server_addr = from_server.serveraddr();
      to_return.num_threads = from_server.threadcount();

      for (int i = 0; i < from_server.dbinfo_size(); i++) {
          DbInfo curr_db;
          curr_db.db_name = from_server.dbinfo(i).dbname();
          curr_db.db_resolution = from_server.dbinfo(i).dbresolution();
          curr_db.total_size_in_byte = from_server.dbinfo(i).totalsizeinbyte();
          curr_db.row_length = from_server.dbinfo(i).rowlength();

          for (int j = 0; j < from_server.dbinfo(i).columnnames_size(); j++) {
              curr_db.column_names.push_back(from_server.dbinfo(i).columnnames(j));
          }

          for (int j = 0; j < from_server.dbinfo(i).files_size(); j++) {
              const XStore_Proto::FileStats &f = from_server.dbinfo(i).files(j);
              curr_db.files.push_back(FileInfo{f.filename(), f.actualsizebytes(), f.apparentsizebytes()});
          }
          to_return.db_info_list.push_back(curr_db);
      }

      return to_return;
  }

  // ==== DB management ====
  [[nodiscard]] std::string CreateDB(std::string_view db_name, XStore_Proto::TimeUnit time_resolution, short int row_length,
                                      const std::vector<std::string> &column_names = {}) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_createdb();
      payload->set_rtype(XStore_Proto::CREATEDB);
      payload->set_dbname(std::string(db_name));
      payload->set_timeresolution(time_resolution);
      payload->set_rowlength(row_length);
      for (const std::string &name : column_names) {
          payload->add_columnnames(name);
      }
      return SendRecv<XStore_Proto::CreateDB_REP>(to_server).createdbstatus();
  }

  // Pure name->index lookup, no network call -- callers fetch column_names
  // once (InfoRequest()'s DbInfo.column_names, or the same list they just
  // passed to CreateDB) and pass it in here before serializing a query/
  // aggregate request with the resolved index. Throws rather than silently
  // guessing on an empty list or an unknown name.
  [[nodiscard]] static int ResolveColumnIndex(const std::vector<std::string> &column_names, std::string_view name) {
      for (size_t i = 0; i < column_names.size(); i++) {
          if (column_names[i] == name) return static_cast<int>(i);
      }
      throw std::invalid_argument(fmt::format("XStoreClient::ResolveColumnIndex: no column named '{}' (DB has {} named column(s))",
                                                name, column_names.size()));
  }

  [[nodiscard]] std::string RemoveDB(std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_removedb();
      payload->set_rtype(XStore_Proto::REMOVEDB);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RemoveDB_REP>(to_server).removestatus();
  }

  // Per-file status, one entry per requested fileName -- can't be flattened
  // to a single string without losing which file(s) failed.
  [[nodiscard]] XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(std::string_view db_name, const std::vector<std::string> &file_names) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_removedbfiles();
      payload->set_rtype(XStore_Proto::REMOVEDBFILES);
      payload->set_dbname(std::string(db_name));
      for (const std::string &name : file_names) {
          payload->add_filename(name);
      }
      return SendRecv<XStore_Proto::RemoveDBFiles_REP>(to_server);
  }

  // ==== Row sizing ====
  // Suggests a CreateDB rowLength/slot size from one sample RowData, so
  // callers don't have to hand-guess a value (and over/under-provision --
  // see DbInfo::row_length above). Mirrors XBench's
  // XStore_Adapter::ComputeRowLength, ported here so client_src callers get
  // it natively instead of reimplementing it.
  //
  // PROTOBUF_FIELD_OVERHEAD=3 is exact for every fixed-width/varint oneof
  // case (double/int32/int64/etc.): 1B outer repeated-field tag (RowData's
  // fieldData=1) + 1B outer length-varint (assumes the encoded FData stays
  // under 128B, true for any scalar or short string) + 1B inner oneof tag.
  // It undercounts stringData/bytesData by 1B: those are themselves
  // length-delimited, so they need a second inner length-varint the
  // fixed/varint cases don't. That 1B/string-field gap already existed in
  // the XBench original this mirrors -- flagged here, not silently fixed,
  // since this helper is meant to match, not diverge from, the logic it's
  // porting.
  template <uint32_t Alignment = 8>
  [[nodiscard]] static uint32_t SuggestRowLength(const XStore_Proto::RowData &sample_row) {
      static_assert((Alignment & (Alignment - 1)) == 0, "Alignment must be power of 2");
      constexpr uint32_t PROTOBUF_FIELD_OVERHEAD = 3;

      uint32_t row_size = 0;
      for (int i = 0; i < sample_row.fielddata_size(); i++) {
          const XStore_Proto::FData &field = sample_row.fielddata(i);
          switch (field.oneOfData_case()) {
              case XStore_Proto::FData::kStringData:
                  row_size += static_cast<uint32_t>(field.stringdata().size()) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kBytesData:
                  row_size += static_cast<uint32_t>(field.bytesdata().size()) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kDoubleData:
                  row_size += sizeof(double) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kFloatData:
                  row_size += sizeof(float) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kBoolData:
                  row_size += sizeof(bool) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kInt32Data:
              case XStore_Proto::FData::kUint32Data:
              case XStore_Proto::FData::kSint32Data:
              case XStore_Proto::FData::kFixed32Data:
              case XStore_Proto::FData::kSfixed32Data:
                  row_size += sizeof(int32_t) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kInt64Data:
              case XStore_Proto::FData::kUint64Data:
              case XStore_Proto::FData::kSint64Data:
              case XStore_Proto::FData::kFixed64Data:
              case XStore_Proto::FData::kSfixed64Data:
                  row_size += sizeof(int64_t) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::ONEOFDATA_NOT_SET:
                  break;
          }
      }

      return (row_size + Alignment - 1) & ~(Alignment - 1);
  }

  // ==== Unary ====
  // durability_mode defaults to ASYNC -- no fsync wait, lowest latency. A
  // row is only in-memory/queued until the writer thread flushes it, so a
  // crash before that flush loses it. Pass SYNC explicitly for writes that
  // must survive a crash.
  [[nodiscard]] std::string UnaryInsert(std::time_t insert_unix_time, const XStore_Proto::RowData &data, std::string_view db_name,
                                         XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unaryinsert();
      payload->set_rtype(XStore_Proto::UNARYINSERT);
      payload->set_timestamp(insert_unix_time);
      *payload->mutable_data() = data;
      payload->set_dbname(std::string(db_name));
      payload->set_durabilitymode(durability_mode);
      return SendRecv<XStore_Proto::UnaryInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] XStore_Proto::UnaryQuery_REP UnaryQuery(std::time_t target_unix_time, std::string_view target_db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unaryquery();
      payload->set_rtype(XStore_Proto::UNARYQUERY);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryQuery_REP>(to_server);
  }

  [[nodiscard]] std::string UnaryDelete(std::time_t target_unix_time, std::string_view target_db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unarydelete();
      payload->set_rtype(XStore_Proto::UNARYDELETE);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryDelete_REP>(to_server).deletestatus();
  }

  // ==== Batch (arbitrary timestamp set) ====
  // durability_mode defaults to ASYNC, same WAL-routed semantics as
  // UnaryInsert's: see BatchInsert_REQ's durabilityMode comment in
  // XStore.proto. SYNC blocks until the write is durably fsync'd and
  // applied; ASYNC enqueues and returns immediately -- note that for the
  // default ASYNC mode, both the fsync AND the data-file write itself are
  // deferred to the server's WAL writer thread, so (unlike this client's
  // pre-WAL behavior) a read immediately after an ASYNC BatchInsert is not
  // guaranteed to see it yet.
  // A batch too big for one request (protobuf cannot serialize 2 GB or more as one message) is sent as several, in order, each acknowledged
  // before the next, each of at most about max_request_bytes_ (see SetMaxRequestBytes); the returned status is then one combined line, or the
  // first part's that did not succeed (the rows of the parts before it are in, none after it were sent). A batch that fits is ONE request as
  // always, atomic as the server makes it; a split batch is not one atomic batch any more. The size is estimated from a few rows first, so a
  // normal batch pays for nothing but that.
  [[nodiscard]] std::string BatchInsert(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows,
                                         XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      if (!MayNeedSplitting(rows, max_request_bytes_)) return BatchInsertPart(db_name, rows, 0, rows.size(), durability_mode);

      std::string first_status;
      uint64_t inserted = 0;
      size_t parts = 0;
      std::vector<uint32_t> sizes;
      for (size_t begin = 0; begin < rows.size();) {
          const size_t end = NextInsertPart(rows, begin, max_request_bytes_, sizes);
          const std::string status = BatchInsertPart(db_name, rows, begin, end, durability_mode, &sizes);
          const size_t at = status.find(" - Inserted ");
          if (status.compare(0, 6, "[INFO]") != 0 || at == std::string::npos) return status;   // this part failed: stop, say why
          if (parts == 0) first_status = status.substr(0, at);
          inserted += std::strtoull(status.c_str() + at + std::string_view(" - Inserted ").size(), nullptr, 10);
          ++parts;
          begin = end;
      }
      return fmt::format("{} - Inserted {} elements successfully ({} requests)", first_status, inserted, parts);
  }

  // BatchInsert for a bulk load: the parts (about 64 MiB each) are sent without waiting for each answer (at most PIPELINE_IN_FLIGHT unanswered),
  // so the client builds the next part while the server applies the previous ones: on Mystic 1M rows (1.42 GB) took 15-18 s against 31 s. The
  // price: different server workers apply the parts, so they may land in any order (the last of two rows with the same timestamp may not win
  // across parts) and after a failure an arbitrary subset of the parts is in. Use it for distinct timestamps; BatchInsert keeps the order.
  // A batch that is small (a quarter of a part or less, judged as in BatchInsert) is one request as always.
  [[nodiscard]] std::string BatchInsertPipelined(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows,
                                                  XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      const size_t part_bytes = std::min(max_request_bytes_, xstore_client::PIPELINED_PART_BYTES);
      if (!MayNeedSplitting(rows, part_bytes)) return BatchInsertPart(db_name, rows, 0, rows.size(), durability_mode);
      xstore_client::SplitStatusTally tally(" - Inserted ", " elements successfully");
      size_t begin = 0;
      std::vector<uint32_t> sizes;
      SendPipelined<XStore_Proto::BatchInsert_REP>(
          xstore_client::PIPELINE_IN_FLIGHT,
          [&]() -> std::optional<zmq::message_t> {
              if (begin >= rows.size() || tally.failed()) return std::nullopt;
              const size_t end = NextInsertPart(rows, begin, part_bytes, sizes);
              zmq::message_t body = xstore_client::EncodeBatchInsert(db_name, rows, begin, end, sizes, durability_mode, timeout_ms_);
              begin = end;
              return body;
          },
          [&](const XStore_Proto::BatchInsert_REP &reply) { return tally.Add(reply.insertstatus()); });
      return tally.failed() ? tally.failure() : tally.Combined();
  }

  // Advanced/perf overload: build the request directly (e.g. reuse a
  // populated message across calls) instead of via the (timestamp, RowData)
  // vector above. Takes by value so an rvalue caller moves in for free.
  [[nodiscard]] std::string BatchInsert(XStore_Proto::BatchInsert_REQ insert_data) {
      XStore_Proto::Request to_server;
      *to_server.mutable_batchinsert() = std::move(insert_data);
      return SendRecv<XStore_Proto::BatchInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::BatchQuery_REP> BatchQuery(std::string_view db_name, const std::vector<uint64_t> &timestamps, bool sort_time = true) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchquery();
      payload->set_rtype(XStore_Proto::BATCHQUERY);
      payload->set_dbname(std::string(db_name));
      payload->set_sorttime(sort_time);
      for (uint64_t ts : timestamps) {
          payload->add_timestamp(ts);
      }
      return SendRecvOnArena<XStore_Proto::BatchQuery_REP>(to_server);
  }

  // A list of timestamps too long for one request (10 bytes each at most; max_request_bytes_ per request) is sent as several, pipelined: deleting
  // is idempotent and the order of different timestamps does not matter, so the parts are sent without waiting for each answer (at most
  // PIPELINE_IN_FLIGHT unanswered) and their counts are added up. A list that fits is ONE request as always.
  [[nodiscard]] std::string BatchDelete(std::string_view db_name, const std::vector<uint64_t> &timestamps) {
      const size_t per_part = std::max<size_t>(1, max_request_bytes_ / 10);
      if (timestamps.size() <= per_part) return BatchDeletePart(db_name, timestamps, 0, timestamps.size());
      xstore_client::SplitStatusTally tally(" - Deleted ", " items");
      size_t begin = 0;
      SendPipelined<XStore_Proto::BatchDelete_REP>(
          xstore_client::PIPELINE_IN_FLIGHT,
          [&]() -> std::optional<zmq::message_t> {
              if (begin >= timestamps.size() || tally.failed()) return std::nullopt;
              const size_t end = std::min(timestamps.size(), begin + per_part);
              XStore_Proto::Request request;
              auto *payload = request.mutable_batchdelete();
              payload->set_rtype(XStore_Proto::BATCHDELETE);
              payload->set_dbname(std::string(db_name));
              payload->mutable_timestamp()->Reserve(static_cast<int>(end - begin));
              for (size_t i = begin; i < end; ++i) payload->add_timestamp(timestamps[i]);
              begin = end;
              if (timeout_ms_ >= 0) request.set_timeoutms(static_cast<uint32_t>(timeout_ms_));
              return xstore_client::SerializeRequest(request);
          },
          [&](const XStore_Proto::BatchDelete_REP &reply) { return tally.Add(reply.deletestatus()); });
      return tally.failed() ? tally.failure() : tally.Combined();
  }

  // ==== Range (contiguous timestamp span) ====
  // There is no RangeInsert here: BatchInsert takes timestamp-ordered, gapped or unordered rows alike (the server
  // detects order itself; the last duplicate wins). The RANGEINSERT wire message stays server-side for old clients.

  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::RangeQuery_REP> RangeQuery(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_rangequery();
      payload->set_rtype(XStore_Proto::RANGEQUERY);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecvOnArena<XStore_Proto::RangeQuery_REP>(to_server);
  }

  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::FindByValue_REP> FindByValue(uint64_t start_time, uint64_t end_time,
                                                                                  std::string_view db_name, int32_t column_idx,
                                                                                  const XStore_Proto::FData &value) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_findbyvalue();
      payload->set_rtype(XStore_Proto::FINDBYVALUE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      *payload->mutable_value() = value;
      return SendRecvOnArena<XStore_Proto::FindByValue_REP>(to_server);
  }

  // Server side is a TEMPORARY IMPLEMENTATION (option A, brute-force scan);
  // this call's shape is stable. LIKE: % any run, _ one byte, \ escapes.
  // REGEX: RE2, unanchored. case_insensitive: ASCII fold for LIKE.
  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::FindByPattern_REP> FindByPattern(
      uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, std::string_view pattern,
      XStore_Proto::PatternMode mode = XStore_Proto::PATTERN_LIKE, bool case_insensitive = false) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_findbypattern();
      payload->set_rtype(XStore_Proto::FINDBYPATTERN);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      payload->set_pattern(std::string(pattern));
      payload->set_mode(mode);
      payload->set_caseinsensitive(case_insensitive);
      return SendRecvOnArena<XStore_Proto::FindByPattern_REP>(to_server);
  }

  // ==== reads bigger than memory: the same calls as windows ====
  // Each reads its range (or list) as successive windows, in order, each an ordinary streamed call whose reply is handed to `on_window` (with
  // its status, if any) and then dropped; with options.prefetch (the default) the next window arrives while on_window works on the current
  // one, so memory stays at about two windows whatever the size of the result, and the time stays that of one call (see WindowOptions).
  // Return the rows delivered. An exception from on_window or from a window's call stops the read and propagates.
  uint64_t RangeQueryEach(uint64_t start_time, uint64_t end_time, std::string_view db_name,
                          const std::function<void(const XStore_Proto::RangeQuery_REP &)> &on_window,
                          const xstore_client::WindowOptions &options = {}) {
      return ReadInTimeWindows<XStore_Proto::RangeQuery_REP>(
          start_time, end_time, db_name, options,
          [&](XStoreClient &c, uint64_t from, uint64_t to) { return c.RangeQuery(from, to, db_name); }, on_window);
  }

  uint64_t FindByValueEach(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, const XStore_Proto::FData &value,
                           const std::function<void(const XStore_Proto::FindByValue_REP &)> &on_window,
                           const xstore_client::WindowOptions &options = {}) {
      return ReadInTimeWindows<XStore_Proto::FindByValue_REP>(
          start_time, end_time, db_name, options,
          [&](XStoreClient &c, uint64_t from, uint64_t to) { return c.FindByValue(from, to, db_name, column_idx, value); }, on_window);
  }

  uint64_t FindByPatternEach(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, std::string_view pattern,
                             XStore_Proto::PatternMode mode, bool case_insensitive,
                             const std::function<void(const XStore_Proto::FindByPattern_REP &)> &on_window,
                             const xstore_client::WindowOptions &options = {}) {
      return ReadInTimeWindows<XStore_Proto::FindByPattern_REP>(
          start_time, end_time, db_name, options,
          [&](XStoreClient &c, uint64_t from, uint64_t to) { return c.FindByPattern(from, to, db_name, column_idx, pattern, mode, case_insensitive); },
          on_window);
  }

  // The windows are consecutive slices of `timestamps`; with sort_time they are sliced from the sorted list, so the order holds across windows.
  uint64_t BatchQueryEach(std::string_view db_name, const std::vector<uint64_t> &timestamps, bool sort_time,
                          const std::function<void(const XStore_Proto::BatchQuery_REP &)> &on_window,
                          const xstore_client::WindowOptions &options = {}) {
      if (timestamps.empty()) return 0;
      std::vector<uint64_t> sorted;
      if (sort_time) {
          sorted = timestamps;
          std::sort(sorted.begin(), sorted.end());
      }
      const std::vector<uint64_t> &list = sort_time ? sorted : timestamps;
      return ReadInWindows<XStore_Proto::BatchQuery_REP>(
          list.size(), WindowLength(db_name, options), options.prefetch,
          [&](XStoreClient &c, uint64_t first, uint64_t count) {
              const std::vector<uint64_t> slice(list.begin() + static_cast<std::ptrdiff_t>(first), list.begin() + static_cast<std::ptrdiff_t>(first + count));
              return c.BatchQuery(db_name, slice, sort_time);
          },
          on_window);
  }

  [[nodiscard]] std::string RangeDelete(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_rangedelete();
      payload->set_rtype(XStore_Proto::RANGEDELETE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RangeDelete_REP>(to_server).deletestatus();
  }

  // ==== Aggregates ====
  [[nodiscard]] XStore_Proto::Min_REP Min(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_min();
      payload->set_rtype(XStore_Proto::MIN);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Min_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Max_REP Max(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_max();
      payload->set_rtype(XStore_Proto::MAX);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Max_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Sum_REP Sum(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_sum();
      payload->set_rtype(XStore_Proto::SUM);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Sum_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Avg_REP Avg(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_avg();
      payload->set_rtype(XStore_Proto::AVG);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Avg_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Variance_REP Variance(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_variance();
      payload->set_rtype(XStore_Proto::VARIANCE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Variance_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Stddev_REP Stddev(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_stddev();
      payload->set_rtype(XStore_Proto::STDDEV);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Stddev_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Exists_REP Exists(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_exists();
      payload->set_rtype(XStore_Proto::EXISTS);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Exists_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Ohlc_REP Ohlc(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_ohlc();
      payload->set_rtype(XStore_Proto::OHLC);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Ohlc_REP>(to_server);
  }

  // Exact percentile (PERCENTILE_CONT/linear-interpolation semantics),
  // percentile in [0.0, 1.0] (0.5 = median). Not zone-map-accelerated --
  // see Percentile_REQ's comment in XStore.proto.
  [[nodiscard]] XStore_Proto::Percentile_REP Percentile(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, double percentile) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_percentile();
      payload->set_rtype(XStore_Proto::PERCENTILE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      payload->set_percentile(percentile);
      return SendRecv<XStore_Proto::Percentile_REP>(to_server);
  }

  // Exact distinct-value count. Not zone-map-accelerated -- see
  // DistinctCount_REQ's comment in XStore.proto.
  [[nodiscard]] XStore_Proto::DistinctCount_REP DistinctCount(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_distinctcount();
      payload->set_rtype(XStore_Proto::DISTINCTCOUNT);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::DistinctCount_REP>(to_server);
  }

  // Exact top-k most-frequent values (k=1 gives the mode). Not zone-map-
  // accelerated -- see TopK_REQ's comment in XStore.proto.
  [[nodiscard]] XStore_Proto::TopK_REP TopK(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, uint32_t k) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_topk();
      payload->set_rtype(XStore_Proto::TOPK);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      payload->set_k(k);
      return SendRecv<XStore_Proto::TopK_REP>(to_server);
  }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
