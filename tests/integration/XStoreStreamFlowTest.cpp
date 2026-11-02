// Flow control of the chunks of a streamed reply (include/Networking/ZMQ_StreamFlow.h): the window of one reply, the cap on waiting workers, the limit
// on the bytes queued for all replies, and the stall timeout. No network: the "network layer" is the test calling Done().
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "include/Networking/ZMQ_GoneClients.h"
#include "include/Networking/ZMQ_StreamFlow.h"

namespace {
using namespace std::chrono_literals;
using xstore::streamflow::Config;
using xstore::streamflow::ReplyWindow;
using xstore::streamflow::Verdict;

Config TestConfig() {
    Config config;
    config.window_bytes = 100;
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

// Park and resume (docs/superpowers/specs/2026-11-01-park-resume-replies-design.md): instead of a worker waiting, the reply arms a callback that fires
// once when the queue is down to the level it wants.
TEST_F(StreamFlowTest, ADrainCallbackFiresOnceWhenTheQueueReachesTheLevelAndNotBefore) {
    ReplyWindow window;
    for (int i = 0; i < 10; ++i) ASSERT_EQ(window.Admit(10), Verdict::kSend);
    int fired = 0;
    window.ArmDrain(50, [&] { ++fired; });
    for (int i = 0; i < 4; ++i) window.Done(10);   // 60 queued
    EXPECT_EQ(fired, 0);
    window.Done(10);   // 50
    EXPECT_EQ(fired, 1);
    window.Done(10);
    window.Done(10);
    EXPECT_EQ(fired, 1) << "fires once per arming";
    window.Done(10);
    window.Done(10);
    window.Done(10);
}

TEST_F(StreamFlowTest, ADrainCallbackArmedAfterTheQueueIsAlreadyLowFiresAtOnce) {
    ReplyWindow window;
    ASSERT_EQ(window.Admit(10), Verdict::kSend);
    int fired = 0;
    window.ArmDrain(50, [&] { ++fired; });
    EXPECT_EQ(fired, 1);
    window.Done(10);
    EXPECT_EQ(fired, 1);
}

TEST_F(StreamFlowTest, ADrainCallbackFiresExactlyOnceWhenTheLastChunkIsWrittenWhileItIsBeingArmed) {
    for (int round = 0; round < 2000; ++round) {
        ReplyWindow window;
        ASSERT_EQ(window.Admit(60), Verdict::kSend);
        std::atomic<int> fired{0};
        std::thread network([&] { window.Done(60); });   // the write completes on another thread
        window.ArmDrain(50, [&] { fired.fetch_add(1); });
        network.join();
        ASSERT_EQ(fired.load(), 1) << "round " << round;
    }
}

// Parking is the default; XSTORE_PARK_REPLIES=0 goes back to a worker waiting for its window.
TEST(StreamFlowDefaultsTest, ParkingIsOnUnlessTheEnvironmentTurnsItOff) {
    unsetenv("XSTORE_PARK_REPLIES");
    EXPECT_TRUE(Config().park_replies);
    EXPECT_TRUE(xstore::streamflow::detail::FromEnv().park_replies);
    setenv("XSTORE_PARK_REPLIES", "0", 1);
    EXPECT_FALSE(xstore::streamflow::detail::FromEnv().park_replies);
    setenv("XSTORE_PARK_REPLIES", "1", 1);
    EXPECT_TRUE(xstore::streamflow::detail::FromEnv().park_replies);
    setenv("XSTORE_PARK_REPLIES", "true", 1);
    EXPECT_TRUE(xstore::streamflow::detail::FromEnv().park_replies) << "only 0 turns it off";
    unsetenv("XSTORE_PARK_REPLIES");
}

// ---- One memory budget: the bytes queued for a slow client are charged to the process-wide MemoryPool ----
TEST_F(StreamFlowTest, AdmitChargesTheMemoryPoolAndDoneReturnsIt) {
    xstore::MemoryPool &pool = xstore::MemoryPool::Instance();
    const uint64_t base = pool.UsedBytes();
    ReplyWindow window;
    ASSERT_EQ(window.Admit(60), Verdict::kSend);
    ASSERT_EQ(window.Admit(40), Verdict::kSend);
    EXPECT_EQ(pool.UsedBytes(), base + 100);
    window.Done(60);
    EXPECT_EQ(pool.UsedBytes(), base + 40);
    window.Done(40);
    EXPECT_EQ(pool.UsedBytes(), base);
}

TEST_F(StreamFlowTest, AChunkIsNeverRefusedBecauseThePoolIsFull) {
    xstore::MemoryPool &pool = xstore::MemoryPool::Instance();
    const uint64_t base = pool.UsedBytes();
    ReplyWindow window;
    xstore::streamflow::SetPoolFullForTests(1);
    EXPECT_EQ(window.Admit(10), Verdict::kSend) << "a chunk is never refused for memory: the producer parks instead";
    window.Done(10);
    xstore::streamflow::SetPoolFullForTests(-1);
    EXPECT_EQ(pool.UsedBytes(), base);
}

// When a producer should stop and park: its window is full, or the pool is full while it has something queued that will drain. A reply with
// nothing queued has nothing to wait for (it would never be woken).
class ParkDecisionTest : public StreamFlowTest {
protected:
    void TearDown() override {
        xstore::streamflow::SetPoolFullForTests(-1);
        StreamFlowTest::TearDown();
    }
};

TEST_F(ParkDecisionTest, ParksWhenItsWindowIsFull) {
    ReplyWindow window;
    ASSERT_EQ(window.Admit(100), Verdict::kSend);
    EXPECT_TRUE(window.ShouldPark(100));
    window.Done(100);
}

TEST_F(ParkDecisionTest, DoesNotParkBelowItsWindowWhileThePoolHasRoom) {
    ReplyWindow window;
    ASSERT_EQ(window.Admit(99), Verdict::kSend);
    xstore::streamflow::SetPoolFullForTests(0);
    EXPECT_FALSE(window.ShouldPark(100));
    window.Done(99);
}

TEST_F(ParkDecisionTest, ParksBelowItsWindowWhenThePoolIsFullAndSomethingIsQueued) {
    ReplyWindow window;
    ASSERT_EQ(window.Admit(10), Verdict::kSend);
    xstore::streamflow::SetPoolFullForTests(1);
    EXPECT_TRUE(window.ShouldPark(100));
    window.Done(10);
}

TEST_F(ParkDecisionTest, NeverParksWithNothingQueuedWhateverThePoolSays) {
    ReplyWindow window;
    xstore::streamflow::SetPoolFullForTests(1);
    EXPECT_FALSE(window.ShouldPark(100)) << "nothing will drain, so nothing would ever wake it";
    ASSERT_EQ(window.Admit(10), Verdict::kSend);
    window.Done(10);
    EXPECT_FALSE(window.ShouldPark(100));
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

// ---- A client that is gone (include/Networking/ZMQ_GoneClients.h) ----

namespace {
using xstore::streamflow::GoneClients;

TEST(GoneClientsTest, AWindowIsNotCancelledUntilItsClientIsMarkedGone) {
    GoneClients gone;
    const auto window = std::make_shared<ReplyWindow>();
    gone.Register("client-a", window);
    EXPECT_FALSE(window->Cancelled());
    EXPECT_EQ(gone.Mark("client-a"), 1u);
    EXPECT_TRUE(window->Cancelled());
}

TEST(GoneClientsTest, OnlyTheRepliesOfThatClientAreCancelled) {
    GoneClients gone;
    const auto a1 = std::make_shared<ReplyWindow>(), a2 = std::make_shared<ReplyWindow>(), b = std::make_shared<ReplyWindow>();
    gone.Register("client-a", a1);
    gone.Register("client-b", b);
    gone.Register("client-a", a2);
    EXPECT_EQ(gone.Mark("client-a"), 2u);
    EXPECT_TRUE(a1->Cancelled());
    EXPECT_TRUE(a2->Cancelled());
    EXPECT_FALSE(b->Cancelled());
}

TEST(GoneClientsTest, MarkingAnUnknownClientDoesNothing) {
    GoneClients gone;
    const auto window = std::make_shared<ReplyWindow>();
    gone.Register("client-a", window);
    EXPECT_EQ(gone.Mark("nobody"), 0u);
    EXPECT_FALSE(window->Cancelled());
}

TEST(GoneClientsTest, RepliesThatEndedAreNotCountedAndNothingIsKeptAlive) {
    GoneClients gone;
    std::weak_ptr<ReplyWindow> watch;
    {
        const auto window = std::make_shared<ReplyWindow>();
        watch = window;
        gone.Register("client-a", window);
    }
    EXPECT_TRUE(watch.expired()) << "the registry must not keep a finished reply's window alive";
    EXPECT_EQ(gone.Mark("client-a"), 0u);
}

TEST(GoneClientsTest, AClientMarkedGoneIsForgottenSoANewConnectionWithTheSameIdIsNotCancelled) {
    GoneClients gone;
    const auto old_reply = std::make_shared<ReplyWindow>();
    gone.Register("client-a", old_reply);
    EXPECT_EQ(gone.Mark("client-a"), 1u);
    const auto new_reply = std::make_shared<ReplyWindow>();
    gone.Register("client-a", new_reply);
    EXPECT_FALSE(new_reply->Cancelled());
    EXPECT_EQ(gone.Mark("client-a"), 1u);
    EXPECT_TRUE(new_reply->Cancelled());
}

TEST(GoneClientsTest, ClientsThatCameAndWentDoNotAccumulate) {
    GoneClients gone;
    for (int i = 0; i < 5000; ++i) gone.Register("client-" + std::to_string(i), std::make_shared<ReplyWindow>());   // each window ends at once
    EXPECT_LT(gone.EntryCountForTests(), 1000u);
}

TEST(GoneClientsTest, RegisteringAndMarkingFromManyThreadsCancelsEveryLiveReply) {
    GoneClients gone;
    std::vector<std::shared_ptr<ReplyWindow>> windows;
    for (int i = 0; i < 400; ++i) windows.push_back(std::make_shared<ReplyWindow>());
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            for (int i = t; i < 400; i += 4) gone.Register("client-" + std::to_string(i % 20), windows[static_cast<size_t>(i)]);
        });
    }
    for (auto &t : threads) t.join();
    size_t cancelled = 0;
    for (int c = 0; c < 20; ++c) cancelled += gone.Mark("client-" + std::to_string(c));
    EXPECT_EQ(cancelled, 400u);
    for (const auto &w : windows) EXPECT_TRUE(w->Cancelled());
}

TEST(GoneClientsTest, TheCancelCountGrowsByTheRepliesCancelled) {
    GoneClients gone;
    const uint64_t before = xstore::streamflow::CancelCount();
    const auto window = std::make_shared<ReplyWindow>();
    gone.Register("client-a", window);
    gone.Mark("client-a");
    EXPECT_EQ(xstore::streamflow::CancelCount(), before + 1);
}
}  // namespace
