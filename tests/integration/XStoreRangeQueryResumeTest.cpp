// A RangeQuery that can stop at a scan-chunk boundary and continue later (park and resume, docs/superpowers/specs/2026-11-01-park-resume-replies-design.md):
// XStore::RangeQueryRun(job, emit, should_park, slice_lease_bytes). Parking anywhere must not change a byte of the reply, must release the memory lease
// while parked, and a DB removed meanwhile must end the reply with an error.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_ScanLock.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/DataStructs/TimeStruct.h"
#include <string>
#include <vector>

using namespace xstore_test;

namespace {
constexpr short kRowLength = 512;   // a zone-map block (the smallest scan chunk) is 2 MiB / 512 = 4096 rows
constexpr uint64_t kRows = 20000;   // 10,000 either side of a year boundary: two year files, 3 blocks each
const std::string kDb = "RQRESUME";
constexpr uint64_t kScanChunkBytes = 4096ull * kRowLength;   // one block per scan chunk: 6 chunk boundaries here
constexpr size_t kReplyChunkBytes = 16 * 1024;

class RangeQueryResumeTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    unsigned long long first = 0, last = 0;   // the query range, over the year boundary

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_rq_resume_test");
        store = std::make_unique<XStore>();
        CreateTestDb(*store, kDb, XStore_Proto::SECOND, kRowLength);
        const unsigned long long boundary = TimeStruct(static_cast<std::time_t>(1704067200ULL), XStore_Proto::SECOND).first_epoch;   // 2024-01-01
        first = boundary - kRows / 2;
        last = boundary + kRows / 2 - 1;
        for (uint64_t from = 0; from < kRows; from += 2000) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(kDb);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (uint64_t i = from; i < from + 2000; ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(first + i);
                entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(i));
            }
            XStore_Proto::BatchInsert_REP rep;
            store->BatchInsert(&req, &rep);
        }
        xstore::scanlock::Config config;
        config.chunk_bytes = kScanChunkBytes;
        config.parallel_threads = 1;
        xstore::scanlock::Set(config);
    }
    void TearDown() override {
        xstore::scanlock::Set(xstore::scanlock::Config{});
        store.reset();
        scratch.reset();
    }

    // The reply of the one-shot RangeQuery, byte for byte.
    std::string OneShot() {
        std::string buffer;
        store->RangeQuery(first, last, kDb, kReplyChunkBytes, [&](std::string &&chunk, bool) { buffer += chunk; });
        return buffer;
    }

    XStore::RangeQueryJob NewJob() {
        XStore::RangeQueryJob job;
        job.start_epoch = first;
        job.end_epoch = last;
        job.db_name = kDb;
        job.chunk_size_bytes = kReplyChunkBytes;
        return job;
    }
};
}  // namespace

TEST_F(RangeQueryResumeTest, ARunThatNeverParksIsTheOneShotReplyAndEmitsOneLastChunk) {
    XStore::RangeQueryJob job = NewJob();
    std::string buffer;
    int last_flags = 0;
    const auto state = store->RangeQueryRun(job, [&](std::string &&chunk, bool is_last) { buffer += chunk; last_flags += is_last; }, [] { return false; }, 0);
    EXPECT_EQ(state, XStore::RangeQueryState::kDone);
    EXPECT_EQ(last_flags, 1);
    EXPECT_EQ(buffer, OneShot());
    EXPECT_GT(buffer.size(), kRows * 10) << "every timestamp has an entry";
}

TEST_F(RangeQueryResumeTest, ParkingAtEveryScanChunkAndResumingGivesTheSameBytes) {
    const std::string expected = OneShot();
    XStore::RangeQueryJob job = NewJob();
    std::string buffer;
    int last_flags = 0, parks = 0;
    // Park every time it is asked: one scan chunk per run.
    for (int guard = 0; guard < 1000; ++guard) {
        const auto state = store->RangeQueryRun(job, [&](std::string &&chunk, bool is_last) { buffer += chunk; last_flags += is_last; }, [] { return true; }, 0);
        if (state == XStore::RangeQueryState::kDone) break;
        ++parks;
        EXPECT_EQ(last_flags, 0) << "a parked reply has not ended";
    }
    EXPECT_EQ(buffer, expected);
    EXPECT_EQ(last_flags, 1);
    EXPECT_GE(parks, 4) << "20,000 rows in scan chunks of one block, over two year files";
}

TEST_F(RangeQueryResumeTest, ParkingEveryThirdScanChunkGivesTheSameBytes) {
    const std::string expected = OneShot();
    XStore::RangeQueryJob job = NewJob();
    std::string buffer;
    int asked = 0;
    for (int guard = 0; guard < 1000; ++guard) {
        if (store->RangeQueryRun(job, [&](std::string &&chunk, bool) { buffer += chunk; }, [&] { return ++asked % 3 == 0; }, 0) == XStore::RangeQueryState::kDone) break;
    }
    EXPECT_EQ(buffer, expected);
}

TEST_F(RangeQueryResumeTest, ARunAlwaysDoesAtLeastOneScanChunkBeforeItParksSoItMakesProgress) {
    XStore::RangeQueryJob job = NewJob();
    std::string buffer;
    // should_park is true from the first question on; a run that parked before scanning anything would never finish.
    const auto state = store->RangeQueryRun(job, [&](std::string &&chunk, bool) { buffer += chunk; }, [] { return true; }, 0);
    EXPECT_EQ(state, XStore::RangeQueryState::kParked);
    EXPECT_GT(job.next_epoch, first);
}

TEST_F(RangeQueryResumeTest, TheSliceLeaseIsHeldOnlyWhileARunIsInProgress) {
    // A range whose estimate is over the lease threshold (no data needed: the reply still has an entry per timestamp).
    constexpr uint64_t kBigRows = 200'000;   // 200,000 x 528 B = 105 MB estimate (threshold 8 MB)
    XStore::RangeQueryJob job;
    job.start_epoch = first;
    job.end_epoch = first + kBigRows - 1;
    job.db_name = kDb;
    job.chunk_size_bytes = kReplyChunkBytes;
    xstore::MemoryPool &pool = xstore::MemoryPool::Instance();
    const uint64_t base = pool.UsedBytes();
    const uint64_t slice = 9u << 20;   // over the threshold, far under the whole estimate
    uint64_t held_during = 0;
    int asked = 0;
    const auto state = store->RangeQueryRun(job, [](std::string &&, bool) {}, [&] {
        held_during = std::max(held_during, pool.UsedBytes() - base);
        return ++asked >= 3;
    }, slice);
    ASSERT_EQ(state, XStore::RangeQueryState::kParked);
    EXPECT_GE(held_during, slice) << "the run held a lease of its slice";
    EXPECT_LT(held_during, slice + (4u << 20)) << "not the whole-reply estimate";
    EXPECT_EQ(pool.UsedBytes(), base) << "a parked reply holds no pool memory";
}

TEST_F(RangeQueryResumeTest, ADbRemovedWhileParkedEndsTheReplyWithAnError) {
    XStore::RangeQueryJob job = NewJob();
    std::string buffer;
    ASSERT_EQ(store->RangeQueryRun(job, [&](std::string &&chunk, bool) { buffer += chunk; }, [] { return true; }, 0), XStore::RangeQueryState::kParked);
    (void)store->RemoveDB(kDb);
    int last_flags = 0;
    std::string tail;
    EXPECT_EQ(store->RangeQueryRun(job, [&](std::string &&chunk, bool is_last) { tail += chunk; last_flags += is_last; }, [] { return true; }, 0), XStore::RangeQueryState::kDone);
    EXPECT_EQ(last_flags, 1);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer + tail));
    EXPECT_NE(rep.querystatus().find("removed"), std::string::npos) << rep.querystatus();
}

TEST_F(RangeQueryResumeTest, ARefusedRequestFinishesAtOnceLikeTheOneShot) {
    XStore::RangeQueryJob job = NewJob();
    job.db_name = "NOSUCHDB";
    std::string buffer;
    int last_flags = 0;
    EXPECT_EQ(store->RangeQueryRun(job, [&](std::string &&chunk, bool is_last) { buffer += chunk; last_flags += is_last; }, [] { return true; }, 0), XStore::RangeQueryState::kDone);
    EXPECT_EQ(last_flags, 1);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer));
    EXPECT_NE(rep.querystatus().find("does not exist"), std::string::npos) << rep.querystatus();
}
