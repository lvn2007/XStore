// The catalog-index compaction job keeps, per tracked column, every row index of every value until it has scanned the whole
// year (a column is dropped only past CATIDX_MAX_BITMAP_DISTINCT_VALUES distinct values). Once those accumulators pass
// XStoreTunables::OPERATION_LEASE_MIN_BYTES they are reserved from the process-wide MemoryPool in
// CATIDX_COMPACTION_LEASE_STEP_BYTES steps. Compaction is background work, so it never waits in the pool's line: when a
// step cannot be had at once the year is abandoned for this pass (nothing appended) and the function reports it.
#include <gtest/gtest.h>
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
// One row index is XStoreTunables::CATIDX_COMPACTION_BYTES_PER_ROW (16) bytes: this many rows is over the 8 MB threshold
// (524,288 rows) and under one 64 MB step (4,194,304 rows); the second figure needs a second step.
constexpr uint64_t kOneStepRows = 600'000;
constexpr uint64_t kTwoStepRows = 4'300'000;
constexpr uint64_t kSmallRows = 1000;

class CompactionLeaseTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    std::shared_ptr<MemoryLease> hog;
    unsigned short year = 0;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_compaction_lease_test");
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
    // the year file cut off so the compaction scan reads only these rows.
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
    bool Compact() { return store->CompactOneYearCatIdx(DbPath(), year, kRowLength, 1); }
    bool HasBitmaps() { return XStore::GetOrOpenCatIdx(DbPath(), year, kRowLength, 1).HasBitmapSection(); }
};

TEST_F(CompactionLeaseTest, AYearThatNeedsMemoryIsDeferredWhilePoolIsFullAndCompactsOnceItIsFree) {
    FillYear(kOneStepRows);
    const uint64_t used_before = MemoryPool::Instance().UsedBytes();
    HogAllBut(0);
    EXPECT_FALSE(Compact()) << "reported done although the pool had no room";
    EXPECT_FALSE(HasBitmaps()) << "a deferred year must have nothing appended";
    hog.reset();
    EXPECT_TRUE(Compact());
    EXPECT_TRUE(HasBitmaps());
    EXPECT_EQ(MemoryPool::Instance().UsedBytes(), used_before) << "the lease was not returned";
}

TEST_F(CompactionLeaseTest, ASecondStepIsReservedWhenTheAccumulatorsGrowPastTheFirst) {
    FillYear(kTwoStepRows);
    const uint64_t step = XStoreTunables::CATIDX_COMPACTION_LEASE_STEP_BYTES;
    HogAllBut(step + step / 2);   // room for one step, not for two
    EXPECT_FALSE(Compact());
    EXPECT_FALSE(HasBitmaps());
    hog.reset();
    HogAllBut(3 * step);
    const uint64_t used_before = MemoryPool::Instance().UsedBytes();
    EXPECT_TRUE(Compact());
    EXPECT_TRUE(HasBitmaps());
    EXPECT_EQ(MemoryPool::Instance().UsedBytes(), used_before) << "the leases were not returned";
}

TEST_F(CompactionLeaseTest, ASmallYearNeverTouchesThePool) {
    FillYear(kSmallRows);
    HogAllBut(0);
    EXPECT_TRUE(Compact());
    EXPECT_TRUE(HasBitmaps());
}

}  // namespace
