// A streamed RangeQuery/BatchQuery reply is held in server memory until its last chunk is sent (ZMQ delivers a multipart reply
// atomically), so a reply estimated at XStoreTunables::QUERY_REPLY_LEASE_MIN_BYTES or more reserves that estimate from the
// process-wide MemoryPool for as long as it streams: it waits its turn when the pool is full, is rejected when its deadline passes,
// and returns the reservation afterwards. Smaller replies never touch the pool.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_Tunables.h"
#include <atomic>
#include <chrono>
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

}  // namespace
