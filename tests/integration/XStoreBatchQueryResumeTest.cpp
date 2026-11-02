// BatchQuery that can stop between waves of preads and continue later (docs/superpowers/specs/2026-11-01-park-resume-replies-design.md):
// XStore::BatchQueryRun. Parking anywhere must not change a byte of the reply (rows in the order of the request, holes left out), must hold no lock and no
// pool lease while parked, and a DB removed meanwhile must end the reply with an error.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/DataStructs/TimeStruct.h"
#include <algorithm>
#include <random>
#include <string>
#include <vector>

using namespace xstore_test;

namespace {
constexpr short kRowLength = 512;
constexpr uint64_t kRows = 20000;    // 10,000 either side of a year boundary: two year files, each a group of waves of 4096 preads
constexpr uint64_t kHoles = 1000;    // requested but never inserted: left out of the reply
constexpr size_t kReplyChunkBytes = 16 * 1024;
const std::string kDb = "BQRESUME";

class BatchQueryResumeTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    std::vector<unsigned long long> requested;   // in the order the client asked

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_bq_resume_test");
        store = std::make_unique<XStore>();
        CreateTestDb(*store, kDb, XStore_Proto::SECOND, kRowLength);
        const unsigned long long boundary = TimeStruct(static_cast<std::time_t>(1704067200ULL), XStore_Proto::SECOND).first_epoch;   // 2024-01-01
        const unsigned long long first = boundary - kRows / 2;
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
        for (uint64_t i = 0; i < kRows; ++i) requested.push_back(first + i);
        for (uint64_t j = 0; j < kHoles; ++j) requested.push_back(first + kRows + 5 + j);   // rows after the data, same year as its end
        std::mt19937 rng(7);
        std::shuffle(requested.begin(), requested.end(), rng);
    }
    void TearDown() override {
        store.reset();
        scratch.reset();
    }

    std::string OneShot(bool sort_time) {
        XStore_Proto::BatchQuery_REQ req;
        req.set_dbname(kDb);
        for (unsigned long long t : requested) req.add_timestamp(t);
        std::string buffer;
        store->BatchQuery(&req, sort_time, kReplyChunkBytes, [&](std::string &&c, bool) { buffer += c; });
        return buffer;
    }
    XStore::BatchQueryJob NewJob(bool sort_time) {
        XStore::BatchQueryJob job;
        job.db_name = kDb;
        job.timestamps = requested;
        job.sort_time = sort_time;
        job.chunk_size_bytes = kReplyChunkBytes;
        return job;
    }
};
}  // namespace

TEST_F(BatchQueryResumeTest, ARunThatNeverParksIsTheOneShotReplyAndEmitsOneLastChunk) {
    const std::string expected = OneShot(true);
    XStore::BatchQueryJob job = NewJob(true);
    std::string buffer;
    int last_flags = 0;
    EXPECT_EQ(store->BatchQueryRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return false; }, 0), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    EXPECT_TRUE(buffer == expected);
    XStore_Proto::BatchQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer));
    EXPECT_EQ(static_cast<uint64_t>(rep.queryresponse_size()), kRows) << "the holes are left out";
}

TEST_F(BatchQueryResumeTest, ParkingAtEveryWaveAndResumingGivesTheSameBytes) {
    for (const bool sort_time : {true, false}) {
        const std::string expected = OneShot(sort_time);
        XStore::BatchQueryJob job = NewJob(sort_time);
        std::string buffer;
        int parks = 0, last_flags = 0;
        for (int guard = 0; guard < 1000; ++guard) {
            if (store->BatchQueryRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return true; }, 0) == XStore::RunState::kDone) break;
            ++parks;
            EXPECT_EQ(last_flags, 0);
        }
        EXPECT_TRUE(buffer == expected) << (sort_time ? "sorted" : "in request order");
        EXPECT_EQ(last_flags, 1);
        EXPECT_GE(parks, 4) << "21,000 timestamps in waves of 4096, over two year files";
    }
}

TEST_F(BatchQueryResumeTest, ParkingEveryThirdWaveGivesTheSameBytes) {
    const std::string expected = OneShot(true);
    XStore::BatchQueryJob job = NewJob(true);
    std::string buffer;
    int asked = 0;
    for (int guard = 0; guard < 1000; ++guard) {
        if (store->BatchQueryRun(job, [&](std::string &&c, bool) { buffer += c; }, [&] { return ++asked % 3 == 0; }, 0) == XStore::RunState::kDone) break;
    }
    EXPECT_TRUE(buffer == expected);
}

TEST_F(BatchQueryResumeTest, ARunAlwaysDoesAWaveBeforeItParks) {
    XStore::BatchQueryJob job = NewJob(true);
    std::string buffer;
    EXPECT_EQ(store->BatchQueryRun(job, [&](std::string &&c, bool) { buffer += c; }, [] { return true; }, 0), XStore::RunState::kParked);
    EXPECT_GT(job.position + job.group, 0u);
    EXPECT_FALSE(buffer.empty() && job.chunk_buf.empty()) << "something was read";
}

TEST_F(BatchQueryResumeTest, TheSliceLeaseIsHeldOnlyWhileARunIsInProgress) {
    xstore::MemoryPool &pool = xstore::MemoryPool::Instance();
    const uint64_t base = pool.UsedBytes();
    // 21,000 x 528 B = 11 MB estimate: over the lease threshold (8 MB). A slice of 9 MB is the most one run reserves.
    XStore::BatchQueryJob job = NewJob(true);
    uint64_t held_during = 0;
    int asked = 0;
    const auto state = store->BatchQueryRun(job, [](std::string &&, bool) {}, [&] {
        held_during = std::max(held_during, pool.UsedBytes() - base);
        return ++asked >= 2;
    }, 9u << 20);
    ASSERT_EQ(state, XStore::RunState::kParked);
    EXPECT_GE(held_during, 9u << 20);
    EXPECT_LT(held_during, (9u << 20) + (1u << 20)) << "the slice, not the 11 MB estimate of the whole reply";
    EXPECT_EQ(pool.UsedBytes(), base) << "a parked reply holds no pool memory";
}

TEST_F(BatchQueryResumeTest, ADbRemovedWhileParkedEndsTheReplyWithAnError) {
    XStore::BatchQueryJob job = NewJob(true);
    std::string buffer;
    ASSERT_EQ(store->BatchQueryRun(job, [&](std::string &&c, bool) { buffer += c; }, [] { return true; }, 0), XStore::RunState::kParked);
    (void)store->RemoveDB(kDb);
    int last_flags = 0;
    std::string tail;
    EXPECT_EQ(store->BatchQueryRun(job, [&](std::string &&c, bool is_last) { tail += c; last_flags += is_last; }, [] { return true; }, 0), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    XStore_Proto::BatchQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer + tail));
    EXPECT_NE(rep.querystatus().find("removed"), std::string::npos) << rep.querystatus();
}

TEST_F(BatchQueryResumeTest, ARefusedRequestFinishesAtOnce) {
    XStore::BatchQueryJob job = NewJob(true);
    job.db_name = "NOSUCHDB";
    std::string buffer;
    int last_flags = 0;
    EXPECT_EQ(store->BatchQueryRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return true; }, 0), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    XStore_Proto::BatchQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer));
    EXPECT_NE(rep.querystatus().find("does not exist"), std::string::npos) << rep.querystatus();
}
