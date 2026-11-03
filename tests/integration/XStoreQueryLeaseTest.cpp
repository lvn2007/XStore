// A streamed RangeQuery/BatchQuery reply is held in server memory until its last chunk is sent (ZMQ delivers a multipart reply
// atomically), and an exact aggregate (Percentile/DistinctCount/TopK) holds every value in its range, so such an operation
// estimated at XStoreTunables::OPERATION_LEASE_MIN_BYTES or more reserves that estimate from the process-wide MemoryPool for as
// long as it runs: it waits its turn when the pool is full, is rejected when its deadline passes, and returns the reservation
// afterwards. Smaller ones never touch the pool.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_Tunables.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <thread>

namespace {

using xstore::MemoryLease;
using xstore::MemoryPool;
using namespace std::chrono_literals;

constexpr short kRowLength = 100;
constexpr uint64_t kBoundarySeconds = 1704067200;   // 2024-01-01T00:00:00Z
constexpr uint64_t kBytesPerRow = kRowLength + 16;   // the estimate's per-row figure
const std::string kDb = "QLEASE";
// 8 MB / 116 B = 72,315 rows; this many is comfortably over the threshold, and a tiny range comfortably under it.
constexpr uint64_t kBigRows = 100'000;
constexpr uint64_t kSmallRows = 100;

class QueryLeaseTest : public ::testing::Test {
protected:
    std::unique_ptr<xstore_test::ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    std::shared_ptr<MemoryLease> hog;   // the whole pool, so any further reservation has to wait

    void SetUp() override {
        scratch = std::make_unique<xstore_test::ScratchDir>("xstore_query_lease_test");
        store = std::make_unique<XStore>();
        xstore_test::CreateTestDb(*store, kDb, XStore_Proto::SECOND, kRowLength);
    }
    void TearDown() override {
        hog.reset();
        store.reset();
        scratch.reset();
    }
    void HogThePool() {
        MemoryPool &pool = MemoryPool::Instance();
        ASSERT_EQ(pool.Acquire(pool.Limit(), MemoryPool::Clock::now() + 10s, hog), MemoryPool::Result::kGranted);
    }
    // The status text of a RangeQuery over `rows` seconds (no data stored: only the reply's status matters here).
    std::string RangeStatus(uint64_t rows, MemoryPool::Clock::time_point deadline) {
        std::string buffer;
        store->RangeQuery(kBoundarySeconds, kBoundarySeconds + rows - 1, kDb, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [&](std::string &&chunk, bool) { buffer += chunk; }, deadline);
        XStore_Proto::RangeQuery_REP rep;
        rep.ParseFromString(buffer);
        return rep.querystatus();
    }
    // The opstatus of an exact aggregate over `rows` seconds (no data stored: only the status matters here).
    std::string PercentileStatus(uint64_t rows, MemoryPool::Clock::time_point deadline) {
        XStore_Proto::Percentile_REP rep;
        store->PercentileAggregate(kBoundarySeconds, kBoundarySeconds + rows - 1, kDb, 0, 0.5, &rep, deadline);
        return rep.opstatus();
    }
    std::string DistinctStatus(uint64_t rows, MemoryPool::Clock::time_point deadline) {
        XStore_Proto::DistinctCount_REP rep;
        store->DistinctCountAggregate(kBoundarySeconds, kBoundarySeconds + rows - 1, kDb, 0, &rep, deadline);
        return rep.opstatus();
    }
    std::string TopKStatus(uint64_t rows, MemoryPool::Clock::time_point deadline, uint32_t k = 3) {
        XStore_Proto::TopK_REP rep;
        store->TopKAggregate(kBoundarySeconds, kBoundarySeconds + rows - 1, kDb, 0, k, &rep, deadline);
        return rep.opstatus();
    }
    std::string BatchStatus(uint64_t rows, MemoryPool::Clock::time_point deadline) {
        XStore_Proto::BatchQuery_REQ req;
        req.set_dbname(kDb);
        for (uint64_t i = 0; i < rows; ++i) req.add_timestamp(kBoundarySeconds + i);
        std::string buffer;
        store->BatchQuery(&req, true, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [&](std::string &&chunk, bool) { buffer += chunk; }, deadline);
        XStore_Proto::BatchQuery_REP rep;
        rep.ParseFromString(buffer);
        return rep.querystatus();
    }
};

TEST_F(QueryLeaseTest, ASmallReplyNeverWaitsForThePool) {
    HogThePool();
    const auto start = MemoryPool::Clock::now();
    const auto deadline = start + 1s;   // only so a failing run ends instead of waiting forever
    EXPECT_EQ(RangeStatus(kSmallRows, deadline).find("rejected"), std::string::npos);
    EXPECT_EQ(BatchStatus(kSmallRows, deadline).find("rejected"), std::string::npos);
    EXPECT_LT(MemoryPool::Clock::now() - start, 500ms) << "a small reply waited for the full pool";
}

TEST_F(QueryLeaseTest, ABigRangeReplyIsRejectedWhenItsDeadlinePassesWhileThePoolIsFull) {
    HogThePool();
    const auto start = MemoryPool::Clock::now();
    const std::string status = RangeStatus(kBigRows, start + 300ms);
    EXPECT_NE(status.find("rejected"), std::string::npos) << status;
    EXPECT_NE(status.find("deadline"), std::string::npos) << status;
    EXPECT_GE(MemoryPool::Clock::now() - start, 250ms) << "it did not wait for its turn";
}

TEST_F(QueryLeaseTest, ABigBatchReplyIsRejectedWhenItsDeadlinePassesWhileThePoolIsFull) {
    HogThePool();
    const std::string status = BatchStatus(kBigRows, MemoryPool::Clock::now() + 300ms);
    EXPECT_NE(status.find("rejected"), std::string::npos) << status;
    EXPECT_NE(status.find("deadline"), std::string::npos) << status;
}

TEST_F(QueryLeaseTest, ABigReplyWaitsForMemoryAndRunsOnceItIsReturned) {
    HogThePool();
    std::atomic<bool> finished{false};
    std::string status = "not run";
    std::thread query([&] {
        status = RangeStatus(kBigRows, MemoryPool::Clock::time_point::max());
        finished = true;
    });
    std::this_thread::sleep_for(300ms);
    EXPECT_FALSE(finished) << "the reply ran although the pool was full";
    hog.reset();
    for (int i = 0; i < 5000 && !finished; ++i) std::this_thread::sleep_for(1ms);
    query.join();
    EXPECT_TRUE(finished);
    EXPECT_EQ(status.find("rejected"), std::string::npos) << status;
}

TEST_F(QueryLeaseTest, TheLeaseIsHeldWhileChunksStreamAndFullyReturnedAfterwards) {
    MemoryPool &pool = MemoryPool::Instance();
    const uint64_t used_before = pool.UsedBytes();
    uint64_t used_while_streaming = 0;
    store->RangeQuery(kBoundarySeconds, kBoundarySeconds + kBigRows - 1, kDb, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&, bool) { used_while_streaming = pool.UsedBytes(); });
    EXPECT_EQ(used_while_streaming, used_before + kBigRows * kBytesPerRow);
    EXPECT_EQ(pool.UsedBytes(), used_before);
    EXPECT_EQ(pool.PendingBytes(), 0u);
}

// ---- exact aggregates: 16 bytes per value of the range (plus 16 per TopK entry kept) ----
constexpr uint64_t kAggRows = 1'000'000;   // over the threshold for all three (16 MB each)

TEST_F(QueryLeaseTest, ASmallAggregateNeverWaitsForThePool) {
    HogThePool();
    const auto start = MemoryPool::Clock::now();
    const auto deadline = start + 1s;   // only so a failing run ends instead of waiting forever
    EXPECT_EQ(PercentileStatus(kSmallRows, deadline).find("rejected"), std::string::npos);
    EXPECT_EQ(DistinctStatus(kSmallRows, deadline).find("rejected"), std::string::npos);
    EXPECT_EQ(TopKStatus(kSmallRows, deadline).find("rejected"), std::string::npos);
    EXPECT_LT(MemoryPool::Clock::now() - start, 500ms) << "a small aggregate waited for the full pool";
}

TEST_F(QueryLeaseTest, ABigAggregateIsRejectedWhenItsDeadlinePassesWhileThePoolIsFull) {
    HogThePool();
    for (const auto &run : std::vector<std::pair<const char *, std::function<std::string(MemoryPool::Clock::time_point)>>>{
             {"percentile", [&](auto d) { return PercentileStatus(kAggRows, d); }},
             {"distinctcount", [&](auto d) { return DistinctStatus(kAggRows, d); }},
             {"topk", [&](auto d) { return TopKStatus(kAggRows, d); }}}) {
        const auto start = MemoryPool::Clock::now();
        const std::string status = run.second(start + 300ms);
        EXPECT_NE(status.find("rejected"), std::string::npos) << run.first << ": " << status;
        EXPECT_NE(status.find("deadline"), std::string::npos) << run.first << ": " << status;
        EXPECT_GE(MemoryPool::Clock::now() - start, 250ms) << run.first << " did not wait for its turn";
    }
}

TEST_F(QueryLeaseTest, ABigAggregateWaitsForMemoryAndRunsOnceItIsReturned) {
    HogThePool();
    std::atomic<bool> finished{false};
    std::string status = "not run";
    std::thread aggregate([&] {
        status = DistinctStatus(kAggRows, MemoryPool::Clock::time_point::max());
        finished = true;
    });
    std::this_thread::sleep_for(300ms);
    EXPECT_FALSE(finished) << "the aggregate ran although the pool was full";
    hog.reset();
    for (int i = 0; i < 5000 && !finished; ++i) std::this_thread::sleep_for(1ms);
    aggregate.join();
    EXPECT_TRUE(finished);
    EXPECT_EQ(status.find("rejected"), std::string::npos) << status;
}

TEST_F(QueryLeaseTest, AnAggregateReservesPerValueBytesAndReturnsThemWhenDone) {
    MemoryPool &pool = MemoryPool::Instance();
    const uint64_t used_before = pool.UsedBytes();
    const auto fits = [&](const std::function<std::string(MemoryPool::Clock::time_point)> &run) {
        return run(MemoryPool::Clock::now() + 300ms).find("rejected") == std::string::npos;
    };
    const std::function<std::string(MemoryPool::Clock::time_point)> percentile = [&](auto d) { return PercentileStatus(kAggRows, d); };
    const std::function<std::string(MemoryPool::Clock::time_point)> distinct = [&](auto d) { return DistinctStatus(kAggRows, d); };
    const std::function<std::string(MemoryPool::Clock::time_point)> topk = [&](auto d) { return TopKStatus(kAggRows, d); };
    const std::function<std::string(MemoryPool::Clock::time_point)> topk_many = [&](auto d) { return TopKStatus(kAggRows, d, 1'000'000'000); };

    // 12 MB of room: none of the three 16 MB estimates fits, so each has to wait and is rejected at its short deadline.
    pool.Acquire(pool.Limit() - used_before - 12'000'000, MemoryPool::Clock::now() + 5s, hog);
    ASSERT_NE(hog, nullptr);
    hog->MarkResident();   // otherwise the host check subtracts it as memory not allocated yet and nothing fits
    EXPECT_FALSE(fits(percentile)) << "16 MB should not fit in 12 MB";
    EXPECT_FALSE(fits(distinct)) << "16 MB should not fit in 12 MB";
    EXPECT_FALSE(fits(topk)) << "16 MB should not fit in 12 MB";
    hog.reset();

    // 20 MB of room: all three are admitted at once, but not a TopK that may keep a million entries (16 MB + 16 MB).
    pool.Acquire(pool.Limit() - used_before - 20'000'000, MemoryPool::Clock::now() + 5s, hog);
    ASSERT_NE(hog, nullptr);
    hog->MarkResident();
    EXPECT_TRUE(fits(percentile)) << "16 MB should fit in 20 MB";
    EXPECT_TRUE(fits(distinct)) << "16 MB should fit in 20 MB";
    EXPECT_TRUE(fits(topk)) << "16 MB should fit in 20 MB";
    EXPECT_FALSE(fits(topk_many)) << "a TopK that may keep a million entries should not fit";
    hog.reset();
    EXPECT_EQ(pool.UsedBytes(), used_before);
    EXPECT_EQ(pool.PendingBytes(), 0u);
}

}  // namespace
