// The catalog-index bitmap build job keeps, per tracked column, every row index of every value until it has scanned the whole
// year (a column is dropped only past CATIDX_MAX_BITMAP_DISTINCT_VALUES distinct values). Once those accumulators pass
// XStoreTunables::OPERATION_LEASE_MIN_BYTES they are reserved from the process-wide MemoryPool in
// CATIDX_BITMAP_BUILD_LEASE_STEP_BYTES steps. Bitmap build is background work, so it never waits in the pool's line: when a
// step cannot be had at once the year is abandoned for this pass (nothing appended) and the function reports it.
#include <gtest/gtest.h>
#include <functional>
#include "XStore/XStore.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_MemoryPool.h"
#include "XStore/XStore_Tunables.h"
#include "include/DataStructs/TimeStruct.h"
#include "XStore.pb.h"
#include "XStoreTestSupport.h"
#include <algorithm>
#include <filesystem>
#include <memory>
#include <string>

namespace {

using xstore::MemoryLease;
using xstore::MemoryPool;
using namespace xstore_test;

constexpr uint16_t kRowLength = 16;
const std::string kDb = "COMPLEASE";
// One row index is XStoreTunables::CATIDX_BITMAP_BUILD_BYTES_PER_ROW (16) bytes: this many rows is over the 8 MB threshold
// (524,288 rows) and under one 64 MB step (4,194,304 rows); the second figure needs a second step.
constexpr uint64_t kOneStepRows = 600'000;
constexpr uint64_t kTwoStepRows = 4'300'000;
constexpr uint64_t kSmallRows = 1000;

class BitmapBuildLeaseTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    std::shared_ptr<MemoryLease> hog;
    unsigned short year = 0;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_bitmap_build_lease_test");
        store = std::make_unique<XStore>();
        CreateTestDb(*store, kDb, XStore_Proto::SECOND, kRowLength);
    }
    void TearDown() override {
        hog.reset();
        store.reset();
        scratch.reset();
    }
    std::string DbPath() const { return (scratch->test_root / "DataStore" / kDb).string(); }

    // `rows` rows of a one-column, seven-valued year (so no column is dropped for its cardinality), then the sparse tail of
    // the year file cut off so the bitmap build scan reads only these rows.
    void FillYear(uint64_t rows) {
        TimeStruct ref(static_cast<std::time_t>(1700000000ULL), XStore_Proto::SECOND);
        year = ref.year;
        const auto base = static_cast<unsigned long long>(ref.first_epoch);
        for (uint64_t first = 0; first < rows; first += 50'000) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(kDb);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (uint64_t i = first; i < std::min(rows, first + 50'000); ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(base + i);
                entry->mutable_data()->add_fielddata()->set_int64data(static_cast<int64_t>(i % 7));
            }
            XStore_Proto::BatchInsert_REP rep;
            store->BatchInsert(&req, &rep);
            ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
        }
        std::error_code ec;
        std::filesystem::resize_file(DbPath() + "/" + std::to_string(year) + ".XSTORE", rows * kRowLength, ec);
        ASSERT_FALSE(ec) << ec.message();
    }
    // Everything but `room` bytes of the pool, counted as in use and resident.
    void HogAllBut(uint64_t room) {
        MemoryPool &pool = MemoryPool::Instance();
        ASSERT_EQ(pool.Acquire(pool.Limit() - room, MemoryPool::Clock::now() + std::chrono::seconds(10), hog),
                  MemoryPool::Result::kGranted);
        hog->MarkResident();
    }
    bool Build() { return store->BuildOneYearBitmapSection(DbPath(), year, kRowLength, 1); }
    bool HasBitmaps() { return XStore::GetOrOpenCatIdx(DbPath(), year, kRowLength, 1).HasBitmapSection(); }
};

TEST_F(BitmapBuildLeaseTest, AYearThatNeedsMemoryIsDeferredWhilePoolIsFullAndBuildsOnceItIsFree) {
    FillYear(kOneStepRows);
    const uint64_t used_before = MemoryPool::Instance().UsedBytes();
    HogAllBut(0);
    EXPECT_FALSE(Build()) << "reported done although the pool had no room";
    EXPECT_FALSE(HasBitmaps()) << "a deferred year must have nothing appended";
    hog.reset();
    EXPECT_TRUE(Build());
    EXPECT_TRUE(HasBitmaps());
    EXPECT_EQ(MemoryPool::Instance().UsedBytes(), used_before) << "the lease was not returned";
}

TEST_F(BitmapBuildLeaseTest, ASecondStepIsReservedWhenTheAccumulatorsGrowPastTheFirst) {
    FillYear(kTwoStepRows);
    const uint64_t step = XStoreTunables::CATIDX_BITMAP_BUILD_LEASE_STEP_BYTES;
    HogAllBut(step + step / 2);   // room for one step, not for two
    EXPECT_FALSE(Build());
    EXPECT_FALSE(HasBitmaps());
    hog.reset();
    HogAllBut(3 * step);
    const uint64_t used_before = MemoryPool::Instance().UsedBytes();
    EXPECT_TRUE(Build());
    EXPECT_TRUE(HasBitmaps());
    EXPECT_EQ(MemoryPool::Instance().UsedBytes(), used_before) << "the leases were not returned";
}

TEST_F(BitmapBuildLeaseTest, ASmallYearNeverTouchesThePool) {
    FillYear(kSmallRows);
    HogAllBut(0);
    EXPECT_TRUE(Build());
    EXPECT_TRUE(HasBitmaps());
}


// ---- A year that does not fit is not rescanned every 30 s (TODO: bitmap build retries from scratch) ----
// The per-year function still reports a deferral at once (the tests above); the pass over all years remembers it: a backoff that doubles from
// LIVE_WIDEN_BITMAP_BUILD_RESUME_DELAY_SEC up to the poll interval, and for a year that needs more than the pool's whole limit, no retry until restart.
class BitmapBuildBackoffTest : public BitmapBuildLeaseTest {
protected:
    using Clock = std::chrono::steady_clock;
    Clock::time_point fake_now = Clock::time_point{} + std::chrono::hours(1);
    std::function<Clock::time_point()> Now() { return [this] { return fake_now; }; }
    std::string DataStoreDir() const { return (scratch->test_root / "DataStore").string(); }
    bool Pass() { return store->BuildColdBitmapSectionsForAllDbs(DataStoreDir(), Now()); }
    const XStore::BitmapDeferral *Deferral() const { return store->BitmapDeferralFor(DbPath(), year); }
    static std::chrono::seconds Delay(uint32_t attempt) {
        return std::chrono::seconds(std::min<uint64_t>(XStoreTunables::LIVE_WIDEN_BITMAP_BUILD_RESUME_DELAY_SEC << (attempt - 1),
                                                       XStoreTunables::CATIDX_BITMAP_BUILD_POLL_INTERVAL_SEC));
    }
};

TEST_F(BitmapBuildBackoffTest, ADeferredYearIsNotScannedAgainBeforeItsBackoffEnds) {
    FillYear(kOneStepRows);
    HogAllBut(0);
    const uint64_t scans = XStore::BitmapYearScansStarted();
    EXPECT_TRUE(Pass());   // a deferred year is tracked, not reported as an interrupted run
    EXPECT_EQ(XStore::BitmapYearScansStarted(), scans + 1);
    ASSERT_NE(Deferral(), nullptr);
    EXPECT_EQ(Deferral()->attempts, 1u);
    EXPECT_FALSE(Deferral()->permanent);
    EXPECT_EQ(Deferral()->retry_at, fake_now + Delay(1));

    fake_now += std::chrono::seconds(XStoreTunables::LIVE_WIDEN_BITMAP_BUILD_RESUME_DELAY_SEC - 1);
    EXPECT_TRUE(Pass());
    EXPECT_EQ(XStore::BitmapYearScansStarted(), scans + 1) << "scanned again inside the backoff";

    fake_now += std::chrono::seconds(2);
    EXPECT_TRUE(Pass());
    EXPECT_EQ(XStore::BitmapYearScansStarted(), scans + 2) << "not retried once the backoff ended";
    EXPECT_EQ(Deferral()->attempts, 2u);
}

TEST_F(BitmapBuildBackoffTest, TheBackoffDoublesAndStopsAtThePollInterval) {
    FillYear(kOneStepRows);
    HogAllBut(0);
    for (uint32_t attempt = 1; attempt <= 12; ++attempt) {
        EXPECT_TRUE(Pass());
        ASSERT_NE(Deferral(), nullptr);
        EXPECT_EQ(Deferral()->attempts, attempt);
        EXPECT_EQ(Deferral()->retry_at, fake_now + Delay(attempt)) << "attempt " << attempt;
        fake_now = Deferral()->retry_at;
    }
    EXPECT_EQ(Deferral()->retry_at - fake_now, std::chrono::seconds(0));
    EXPECT_EQ(Delay(12), std::chrono::seconds(XStoreTunables::CATIDX_BITMAP_BUILD_POLL_INTERVAL_SEC));
}

TEST_F(BitmapBuildBackoffTest, AYearThatNeverFitsThePoolIsNotRetriedUntilRestart) {
    FillYear(kOneStepRows);
    HogAllBut(0);
    store->SetBitmapPoolLimitForTests(XStoreTunables::CATIDX_BITMAP_BUILD_LEASE_STEP_BYTES / 2);   // the first step alone is over the limit
    const uint64_t scans = XStore::BitmapYearScansStarted();
    EXPECT_TRUE(Pass());
    ASSERT_NE(Deferral(), nullptr);
    EXPECT_TRUE(Deferral()->permanent);
    EXPECT_GT(Deferral()->need_bytes, XStoreTunables::CATIDX_BITMAP_BUILD_LEASE_STEP_BYTES / 2);

    hog.reset();   // even with the pool free, and a day later: the limit did not change, nothing is scanned
    fake_now += std::chrono::hours(24);
    EXPECT_TRUE(Pass());
    EXPECT_EQ(XStore::BitmapYearScansStarted(), scans + 1);
    EXPECT_FALSE(HasBitmaps());
}

TEST_F(BitmapBuildBackoffTest, ASuccessfulRetryBuildsTheSectionAndForgetsTheDeferral) {
    FillYear(kOneStepRows);
    HogAllBut(0);
    EXPECT_TRUE(Pass());
    ASSERT_NE(Deferral(), nullptr);
    hog.reset();
    fake_now = Deferral()->retry_at;
    EXPECT_TRUE(Pass());
    EXPECT_EQ(Deferral(), nullptr);
    EXPECT_TRUE(HasBitmaps());
}

TEST_F(BitmapBuildBackoffTest, TheBackgroundTickWakesAtTheRetryNotAtThePollInterval) {
    FillYear(kOneStepRows);
    HogAllBut(0);
    XStore::BackgroundIndexSchedule schedule;   // the first tick builds
    store->BackgroundIndexTick(DataStoreDir(), schedule, Now());
    ASSERT_NE(Deferral(), nullptr);
    EXPECT_EQ(schedule.next_bitmap_build, fake_now + Delay(1)) << "the tick would sleep the whole poll interval";
    fake_now = schedule.next_bitmap_build;
    store->BackgroundIndexTick(DataStoreDir(), schedule, Now());
    EXPECT_EQ(schedule.next_bitmap_build, fake_now + Delay(2));

    hog.reset();
    fake_now = schedule.next_bitmap_build;
    store->BackgroundIndexTick(DataStoreDir(), schedule, Now());
    EXPECT_TRUE(HasBitmaps());
    EXPECT_EQ(schedule.next_bitmap_build, fake_now + std::chrono::seconds(XStoreTunables::CATIDX_BITMAP_BUILD_POLL_INTERVAL_SEC));
}

TEST_F(BitmapBuildBackoffTest, ASmallYearIsBuiltByThePassAndLeavesNoDeferral) {
    FillYear(kSmallRows);
    HogAllBut(0);
    EXPECT_TRUE(Pass());
    EXPECT_EQ(Deferral(), nullptr);
    EXPECT_TRUE(HasBitmaps());
}

}  // namespace
