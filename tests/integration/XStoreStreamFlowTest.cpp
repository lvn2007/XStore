// Flow control of the chunks of a streamed reply (include/Networking/ZMQ_StreamFlow.h): the window of one reply, the cap on waiting workers, the limit
// on the bytes queued for all replies, and the stall timeout. No network: the "network layer" is the test calling Done().
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include "include/Networking/ZMQ_StreamFlow.h"

namespace {
using namespace std::chrono_literals;
using xstore::streamflow::Config;
using xstore::streamflow::ReplyWindow;
using xstore::streamflow::Verdict;

Config TestConfig() {
    Config config;
    config.window_bytes = 100;
    config.queue_limit_bytes = 1'000'000;   // not in the way unless a test lowers it
    config.share_bytes = 10;
    config.min_chunk_bytes = 1;
    config.max_waiting = 4;
    config.stall = 30s;
    return config;
}

class StreamFlowTest : public ::testing::Test {
protected:
    void SetUp() override { xstore::streamflow::Set(TestConfig()); }
    void TearDown() override { xstore::streamflow::Set(Config()); }
    // true if `future` is still not ready after a moment (the call is blocked)
    template <typename F>
    static bool StillBlocked(F &future) { return future.wait_for(150ms) == std::future_status::timeout; }
};

TEST_F(StreamFlowTest, ChunksWithinTheWindowAreQueuedAtOnceAndCounted) {
    ReplyWindow window;
    EXPECT_EQ(window.Admit(60), Verdict::kSend);
    EXPECT_EQ(window.Admit(40), Verdict::kSend);   // exactly the window
    EXPECT_EQ(window.QueuedBytesOfThisReply(), 100u);
    EXPECT_EQ(xstore::streamflow::QueuedBytes(), 100u);
    window.Done(60);
    window.Done(40);
    EXPECT_EQ(window.QueuedBytesOfThisReply(), 0u);
    EXPECT_EQ(xstore::streamflow::QueuedBytes(), 0u);
}

TEST_F(StreamFlowTest, TheFirstChunkIsNeverHeldBackEvenWhenItIsBiggerThanTheWindow) {
    ReplyWindow window;
    EXPECT_EQ(window.Admit(500), Verdict::kSend);
    window.Done(500);
}

TEST_F(StreamFlowTest, APastTheWindowChunkWaitsUntilTheQueueIsDownToHalfAndNotBefore) {
    ReplyWindow window;
    ASSERT_EQ(window.Admit(60), Verdict::kSend);
    ASSERT_EQ(window.Admit(40), Verdict::kSend);
    auto blocked = std::async(std::launch::async, [&] { return window.Admit(30); });
    EXPECT_TRUE(StillBlocked(blocked));
    EXPECT_EQ(xstore::streamflow::WaitingWorkers(), 1);
    window.Done(40);   // 60 queued: above half (50)
    EXPECT_TRUE(StillBlocked(blocked)) << "woken before the queue was down to half the window";
    window.Done(10);   // 50: half
    ASSERT_EQ(blocked.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(blocked.get(), Verdict::kSend);
    EXPECT_EQ(xstore::streamflow::WaitingWorkers(), 0);
    EXPECT_EQ(window.QueuedBytesOfThisReply(), 80u);   // 50 left + the 30 admitted
    window.Done(80);
}

// Each wake-up of a waiting worker costs CPU (Mystic, 8 nodes x 100k-row RangeQuery: +12% server CPU when every written chunk woke it, 64 wake-ups per half window).
TEST_F(StreamFlowTest, AWaitingWorkerIsWokenOnceWhenTheQueueReachesHalfAndNotByEveryChunkBefore) {
    ReplyWindow window;
    for (int i = 0; i < 10; ++i) ASSERT_EQ(window.Admit(10), Verdict::kSend);   // the whole window, in ten chunks
    const uint64_t wakeups_before = xstore::streamflow::Wakeups();
    auto blocked = std::async(std::launch::async, [&] { return window.Admit(30); });
    ASSERT_TRUE(StillBlocked(blocked));
    for (int i = 0; i < 4; ++i) window.Done(10);   // 60 queued: above half
    EXPECT_EQ(xstore::streamflow::Wakeups(), wakeups_before) << "a chunk written above half the window woke the worker";
    EXPECT_TRUE(StillBlocked(blocked));
    window.Done(10);   // 50: half
    ASSERT_EQ(blocked.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(blocked.get(), Verdict::kSend);
    EXPECT_EQ(xstore::streamflow::Wakeups(), wakeups_before + 1);
    window.Done(80);
}

TEST_F(StreamFlowTest, OnlyMaxWaitingWorkersWaitAndTheOthersQueueAtOnce) {
    Config config = TestConfig();
    config.max_waiting = 1;
    xstore::streamflow::Set(config);
    ReplyWindow a, b;
    ASSERT_EQ(a.Admit(100), Verdict::kSend);
    ASSERT_EQ(b.Admit(100), Verdict::kSend);
    auto first = std::async(std::launch::async, [&] { return a.Admit(50); });
    EXPECT_TRUE(StillBlocked(first));
    // the one waiting slot is taken: b goes on without waiting, past its window
    EXPECT_EQ(b.Admit(50), Verdict::kSend);
    EXPECT_EQ(b.QueuedBytesOfThisReply(), 150u);
    EXPECT_EQ(xstore::streamflow::WaitingWorkers(), 1);
    a.Done(100);
    ASSERT_EQ(first.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(first.get(), Verdict::kSend);
    a.Done(50);
    b.Done(150);
}

TEST_F(StreamFlowTest, WithoutAWindowNothingEverWaits) {
    Config config = TestConfig();
    config.window_bytes = 0;
    xstore::streamflow::Set(config);
    ReplyWindow window;
    EXPECT_EQ(window.Admit(100000), Verdict::kSend);
    EXPECT_EQ(window.Admit(100000), Verdict::kSend);
    window.Done(200000);
}

TEST_F(StreamFlowTest, AReplyHoldingMoreThanItsShareIsRefusedWhenTheTotalPassesTheLimitAndASmallOneIsNot) {
    Config config = TestConfig();
    config.window_bytes = 0;
    config.queue_limit_bytes = 1000;
    config.share_bytes = 100;
    xstore::streamflow::Set(config);
    ReplyWindow stalled, healthy;
    ASSERT_EQ(stalled.Admit(900), Verdict::kSend);   // first chunk: nothing queued yet
    ASSERT_EQ(stalled.Admit(50), Verdict::kSend);    // 950 in all: under the limit
    EXPECT_EQ(stalled.Admit(100), Verdict::kQueueFull) << "950 + 100 passes 1000 and this reply holds more than its share";
    EXPECT_EQ(stalled.QueuedBytesOfThisReply(), 950u) << "a refused chunk is not counted";
    EXPECT_EQ(xstore::streamflow::QueuedBytes(), 950u);
    EXPECT_EQ(healthy.Admit(200), Verdict::kSend) << "a reply that holds nothing is not the one cut, whatever the total";
    EXPECT_EQ(healthy.Admit(50), Verdict::kQueueFull) << "...but once it holds more than its share it is";
    healthy.Done(200);
    stalled.Done(950);
    EXPECT_EQ(xstore::streamflow::QueuedBytes(), 0u);
}

TEST_F(StreamFlowTest, AWaitThatNothingEndsIsStoppedAfterTheStallTimeAndCountsNothing) {
    Config config = TestConfig();
    config.stall = 300ms;
    xstore::streamflow::Set(config);
    ReplyWindow window;
    ASSERT_EQ(window.Admit(100), Verdict::kSend);
    const auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(window.Admit(50), Verdict::kStalled);
    EXPECT_GE(std::chrono::steady_clock::now() - started, 280ms);
    EXPECT_EQ(window.QueuedBytesOfThisReply(), 100u);
    EXPECT_EQ(xstore::streamflow::WaitingWorkers(), 0);
    window.Done(100);
}

// The server sets the cap from its worker count once it starts: 75% of them (at least one), unless a number was chosen (environment or Set), and a
// later read of the configuration must keep it.
TEST_F(StreamFlowTest, TheServerSetsTheCapOnWaitingWorkersFromItsWorkerCountUnlessOneWasChosen) {
    Config config = TestConfig();
    config.max_waiting = 0;
    xstore::streamflow::Set(config);
    xstore::streamflow::SetDefaultMaxWaiting(32);
    EXPECT_EQ(xstore::streamflow::Get().max_waiting, 24u);
    EXPECT_EQ(xstore::streamflow::Get().max_waiting, 24u) << "a read does not undo it";
    xstore::streamflow::SetDefaultMaxWaiting(8);
    EXPECT_EQ(xstore::streamflow::Get().max_waiting, 24u) << "set once, kept";

    config.max_waiting = 5;   // chosen
    xstore::streamflow::Set(config);
    xstore::streamflow::SetDefaultMaxWaiting(32);
    EXPECT_EQ(xstore::streamflow::Get().max_waiting, 5u);

    config.max_waiting = 0;
    xstore::streamflow::Set(config);
    xstore::streamflow::SetDefaultMaxWaiting(1);
    EXPECT_EQ(xstore::streamflow::Get().max_waiting, 1u) << "never below one worker";
}

TEST_F(StreamFlowTest, ManyThreadsQueueingAndCompletingEndWithNothingCounted) {
    Config config = TestConfig();
    config.window_bytes = 1000;
    config.max_waiting = 8;
    xstore::streamflow::Set(config);
    std::vector<std::thread> threads;
    std::atomic<int> refused{0};
    for (int t = 0; t < 6; ++t) {
        threads.emplace_back([&] {
            ReplyWindow window;
            std::vector<uint64_t> in_flight;
            for (int i = 0; i < 400; ++i) {
                if (window.Admit(100) == Verdict::kSend) in_flight.push_back(100); else ++refused;
                while (in_flight.size() > 3) { window.Done(in_flight.front()); in_flight.erase(in_flight.begin()); }   // the "network" takes the oldest
            }
            for (uint64_t bytes : in_flight) window.Done(bytes);
        });
    }
    for (auto &thread : threads) thread.join();
    EXPECT_EQ(refused.load(), 0);
    EXPECT_EQ(xstore::streamflow::QueuedBytes(), 0u);
    EXPECT_EQ(xstore::streamflow::WaitingWorkers(), 0);
}
}  // namespace
