#pragma once
// Registry of streamed replies parked by their worker (docs/superpowers/specs/2026-11-01-park-resume-replies-design.md). A worker that has queued
// a window of a reply and would have to wait for the client parks the rest of the reply here as a bookmark (`payload`) and goes back to the broker;
// a free worker resumes it when it is ready.
//
//   token = Park(bookmark)        worker, BEFORE it sends the release message
//   OnReleased(token)             broker, after it relayed the release message to the client's socket
//   OnDrained(token)              network layer's thread, when the reply's queue is down (ReplyWindow::ArmDrain)
//   NextReady() -> token          broker: a reply that is ready to resume, oldest first
//   Take(token, &expired) -> bookmark   the worker the broker dispatched the resume to
//
// A reply is ready only after BOTH OnReleased and OnDrained, in either order, and exactly once. Both are needed: if it were ready as soon as the
// queue drained, a second worker could send chunks before the first worker's last chunk reached the client's socket (two workers' messages are not
// ordered), and the client would parse rows out of order. Tokens are random 64-bit numbers: a client cannot resume (or hijack) a reply by sending a
// request, and Take refuses a token whose reply is not ready.
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

namespace xstore::parked {

// The wire marks (see the design note): the delimiter of a streamed message is 0x01 (more follows, the worker stays busy), 0x02 + the token (more
// follows, the worker is RELEASED: its reply is parked), or empty (the last message). A resume goes to a worker as [token]['R'][empty body].
inline constexpr char RELEASED_MARK = '\x02';
inline constexpr char RESUME_MARK = 'R';
inline std::string ReleaseDelimiter(uint64_t token) {
    std::string delimiter(1 + sizeof(token), RELEASED_MARK);
    std::memcpy(&delimiter[1], &token, sizeof(token));
    return delimiter;
}
inline bool ParseReleaseDelimiter(const void *data, size_t size, uint64_t &token) {
    if (size != 1 + sizeof(token) || static_cast<const char *>(data)[0] != RELEASED_MARK) return false;
    std::memcpy(&token, static_cast<const char *>(data) + 1, sizeof(token));
    return true;
}

class ParkedReplies {
public:
    using Clock = std::chrono::steady_clock;

    ParkedReplies() : wake_fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)), rng_(std::random_device{}()) {
        if (wake_fd_ < 0) throw std::runtime_error("ParkedReplies: eventfd failed");
    }
    ~ParkedReplies() { ::close(wake_fd_); }
    ParkedReplies(const ParkedReplies &) = delete;
    ParkedReplies &operator=(const ParkedReplies &) = delete;

    // Readable while at least one reply is ready to resume: for the broker's poll set.
    int WakeFd() const { return wake_fd_; }

    [[nodiscard]] uint64_t Park(std::shared_ptr<void> payload, Clock::time_point parked_at = Clock::now()) {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t token;
        do { token = rng_(); } while (token == 0 || entries_.count(token) != 0);
        entries_.emplace(token, Entry{std::move(payload), parked_at});
        return token;
    }

    // false: no such reply (taken, or a token nobody parked).
    bool OnReleased(uint64_t token) { return Event(token, [](Entry &e) { e.released = true; }); }
    bool OnDrained(uint64_t token) { return Event(token, [](Entry &e) { e.drained = true; }); }

    [[nodiscard]] std::optional<uint64_t> NextReady() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (ready_.empty()) return std::nullopt;
        const uint64_t token = ready_.front();
        ready_.pop_front();
        // Everything that raised the fd is under this lock too: drained only when nothing is left, so the fd stays readable while a reply is ready.
        if (ready_.empty()) {
            uint64_t counter;
            (void)!::read(wake_fd_, &counter, sizeof(counter));
        }
        return token;
    }

    // The bookmark of a reply NextReady returned, once. nullptr for anything else (unknown, not ready, already taken). *expired: the reply was
    // resumed because it waited too long (ExpireOlderThan), so it should be stopped, not continued.
    [[nodiscard]] std::shared_ptr<void> Take(uint64_t token, bool *expired) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = entries_.find(token);
        if (it == entries_.end() || !it->second.ready) return nullptr;
        if (expired != nullptr) *expired = it->second.expired;
        std::shared_ptr<void> payload = std::move(it->second.payload);
        entries_.erase(it);
        return payload;
    }

    // Replies released before `cutoff` that are still not drained become ready as expired (a client that does not read): returns how many.
    size_t ExpireOlderThan(Clock::time_point cutoff) {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t count = 0;
        for (auto &[token, entry] : entries_) {
            if (entry.released && !entry.ready && entry.parked_at < cutoff) {
                entry.expired = true;
                MakeReady(token, entry);
                ++count;
            }
        }
        return count;
    }

    // Replies parked or ready and not yet taken.
    size_t ParkedCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return entries_.size();
    }

private:
    struct Entry {
        std::shared_ptr<void> payload;
        Clock::time_point parked_at;
        bool released = false;
        bool drained = false;
        bool ready = false;
        bool expired = false;
    };

    template <typename Mark>
    bool Event(uint64_t token, Mark mark) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = entries_.find(token);
        if (it == entries_.end()) return false;
        mark(it->second);
        if (it->second.released && it->second.drained && !it->second.ready) MakeReady(token, it->second);
        return true;
    }

    void MakeReady(uint64_t token, Entry &entry) {
        entry.ready = true;
        ready_.push_back(token);
        const uint64_t one = 1;
        (void)!::write(wake_fd_, &one, sizeof(one));
    }

    const int wake_fd_;
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, Entry> entries_;
    std::deque<uint64_t> ready_;
    std::mt19937_64 rng_;
};

}  // namespace xstore::parked
