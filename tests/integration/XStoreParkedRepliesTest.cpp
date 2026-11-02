// The registry of parked streamed replies (include/Networking/ZMQ_ParkedReplies.h): a reply becomes ready to resume only when BOTH the broker has
// relayed its release message AND the network layer has written the queue down (otherwise a second worker could send chunks before the first
// worker's last chunk reached the client). No network here: the test plays the broker and the network thread.
#include <gtest/gtest.h>

#include <poll.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "include/Networking/ZMQ_ParkedReplies.h"

namespace {
using xstore::parked::ParkedReplies;
using Clock = std::chrono::steady_clock;

std::shared_ptr<void> Payload(int value) { return std::make_shared<int>(value); }
int Value(const std::shared_ptr<void> &p) { return *static_cast<int *>(p.get()); }
bool Readable(int fd) {
    pollfd item{fd, POLLIN, 0};
    return ::poll(&item, 1, 0) == 1 && (item.revents & POLLIN) != 0;
}

TEST(ParkedRepliesTest, AReplyIsReadyOnlyAfterTheReleaseWasRelayedAndTheQueueDrained_ReleaseFirst) {
    ParkedReplies parked;
    const uint64_t token = parked.Park(Payload(7));
    parked.OnReleased(token);
    EXPECT_FALSE(parked.NextReady().has_value());
    parked.OnDrained(token);
    const auto ready = parked.NextReady();
    ASSERT_TRUE(ready.has_value());
    EXPECT_EQ(*ready, token);
    EXPECT_FALSE(parked.NextReady().has_value()) << "ready exactly once";
}

TEST(ParkedRepliesTest, AReplyIsReadyOnlyAfterTheReleaseWasRelayedAndTheQueueDrained_DrainFirst) {
    ParkedReplies parked;
    const uint64_t token = parked.Park(Payload(7));
    parked.OnDrained(token);
    EXPECT_FALSE(parked.NextReady().has_value()) << "drained but the release message is not relayed yet: the next chunks could overtake it";
    parked.OnReleased(token);
    EXPECT_EQ(parked.NextReady().value_or(0), token);
    EXPECT_FALSE(parked.NextReady().has_value());
}

TEST(ParkedRepliesTest, RepeatedEventsDoNotMakeAReplyReadyTwice) {
    ParkedReplies parked;
    const uint64_t token = parked.Park(Payload(1));
    parked.OnReleased(token);
    parked.OnReleased(token);
    parked.OnDrained(token);
    parked.OnDrained(token);
    EXPECT_EQ(parked.NextReady().value_or(0), token);
    EXPECT_FALSE(parked.NextReady().has_value());
}

TEST(ParkedRepliesTest, TakeGivesThePayloadOnceAndAnUnknownTokenGivesNothing) {
    ParkedReplies parked;
    const uint64_t token = parked.Park(Payload(42));
    parked.OnReleased(token);
    parked.OnDrained(token);
    ASSERT_EQ(parked.NextReady().value_or(0), token);
    bool expired = true;
    const auto payload = parked.Take(token, &expired);
    ASSERT_NE(payload, nullptr);
    EXPECT_EQ(Value(payload), 42);
    EXPECT_FALSE(expired);
    EXPECT_EQ(parked.Take(token, &expired), nullptr) << "a token resumes one reply once";
    EXPECT_EQ(parked.Take(token ^ 0x9e3779b97f4a7c15ull, &expired), nullptr) << "a guessed token";
    parked.OnReleased(token);   // events of a token that is gone are ignored
    parked.OnDrained(token);
    EXPECT_FALSE(parked.NextReady().has_value());
}

TEST(ParkedRepliesTest, ATokenOfAReplyThatIsNotReadyCannotBeTaken) {
    ParkedReplies parked;
    const uint64_t token = parked.Park(Payload(5));
    EXPECT_EQ(parked.Take(token, nullptr), nullptr) << "a forged resume of a reply that still runs or waits";
    parked.OnReleased(token);
    EXPECT_EQ(parked.Take(token, nullptr), nullptr);
    parked.OnDrained(token);
    EXPECT_NE(parked.Take(token, nullptr), nullptr);
}

TEST(ParkedRepliesTest, TokensAreUniqueAndNeverZero) {
    ParkedReplies parked;
    std::set<uint64_t> seen;
    for (int i = 0; i < 20000; ++i) {
        const uint64_t token = parked.Park(Payload(i));
        ASSERT_NE(token, 0u);
        ASSERT_TRUE(seen.insert(token).second);
    }
    EXPECT_EQ(parked.ParkedCount(), 20000u);
}

TEST(ParkedRepliesTest, ReadyRepliesComeOutInTheOrderTheyBecameReady) {
    ParkedReplies parked;
    const uint64_t a = parked.Park(Payload(1)), b = parked.Park(Payload(2)), c = parked.Park(Payload(3));
    for (uint64_t t : {a, b, c}) parked.OnReleased(t);
    parked.OnDrained(c);
    parked.OnDrained(a);
    parked.OnDrained(b);
    EXPECT_EQ(parked.NextReady().value_or(0), c);
    EXPECT_EQ(parked.NextReady().value_or(0), a);
    EXPECT_EQ(parked.NextReady().value_or(0), b);
}

TEST(ParkedRepliesTest, TheWakeFdIsReadableWhileSomethingIsReadyAndOnlyThen) {
    ParkedReplies parked;
    EXPECT_FALSE(Readable(parked.WakeFd()));
    const uint64_t a = parked.Park(Payload(1)), b = parked.Park(Payload(2));
    parked.OnReleased(a);
    parked.OnReleased(b);
    EXPECT_FALSE(Readable(parked.WakeFd())) << "released but still queued: nothing to resume";
    parked.OnDrained(a);
    parked.OnDrained(b);
    EXPECT_TRUE(Readable(parked.WakeFd()));
    ASSERT_TRUE(parked.NextReady().has_value());
    EXPECT_TRUE(Readable(parked.WakeFd())) << "one more is ready";
    ASSERT_TRUE(parked.NextReady().has_value());
    EXPECT_FALSE(Readable(parked.WakeFd()));
}

TEST(ParkedRepliesTest, AReleasedReplyThatNeverDrainsIsMadeReadyAsExpiredAfterTheAge) {
    ParkedReplies parked;
    const auto t0 = Clock::now();
    const uint64_t old_released = parked.Park(Payload(1), t0);
    const uint64_t new_released = parked.Park(Payload(2), t0 + std::chrono::seconds(100));
    const uint64_t old_running = parked.Park(Payload(3), t0);   // its release message is not relayed yet: still being parked
    parked.OnReleased(old_released);
    parked.OnReleased(new_released);
    EXPECT_EQ(parked.ExpireOlderThan(t0 + std::chrono::seconds(50)), 1u);
    ASSERT_EQ(parked.NextReady().value_or(0), old_released);
    bool expired = false;
    ASSERT_NE(parked.Take(old_released, &expired), nullptr);
    EXPECT_TRUE(expired);
    EXPECT_FALSE(parked.NextReady().has_value());
    EXPECT_EQ(parked.Take(old_running, nullptr), nullptr);
    EXPECT_EQ(parked.ExpireOlderThan(t0 + std::chrono::seconds(50)), 0u) << "expires once";
}

TEST(ParkedRepliesTest, ManyThreadsReleasingAndDrainingInAnyOrderMakeEveryReplyReadyExactlyOnce) {
    constexpr int kReplies = 4000;
    ParkedReplies parked;
    std::vector<uint64_t> tokens;
    for (int i = 0; i < kReplies; ++i) tokens.push_back(parked.Park(Payload(i)));
    struct Event { uint64_t token; bool release; };
    std::vector<Event> events;   // one release and one drain per reply, shuffled: every order of the two, across 4 threads
    for (const uint64_t token : tokens) { events.push_back({token, true}); events.push_back({token, false}); }
    std::mt19937 rng(12345);
    std::shuffle(events.begin(), events.end(), rng);
    std::atomic<size_t> next{0};
    auto work = [&] {
        for (size_t i = next.fetch_add(1); i < events.size(); i = next.fetch_add(1)) {
            if (events[i].release) parked.OnReleased(events[i].token); else parked.OnDrained(events[i].token);
        }
    };
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) threads.emplace_back(work);
    std::multiset<uint64_t> got;
    std::atomic<bool> done{false};
    std::thread consumer([&] {
        while (!done.load() || got.size() < static_cast<size_t>(kReplies)) {
            if (const auto t = parked.NextReady()) got.insert(*t);
            else if (done.load() && got.size() >= static_cast<size_t>(kReplies)) break;
            else std::this_thread::yield();
        }
    });
    for (auto &t : threads) t.join();
    done.store(true);
    consumer.join();
    EXPECT_EQ(got.size(), static_cast<size_t>(kReplies));
    EXPECT_EQ(std::set<uint64_t>(got.begin(), got.end()).size(), static_cast<size_t>(kReplies));
    EXPECT_FALSE(parked.NextReady().has_value());
}
}  // namespace
