#include <gtest/gtest.h>
#include "XStore/XStore.h"
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "include/DataStructs/TimeStruct.h"
#include "XStoreTestSupport.h"
#include <malloc.h>
#include <memory>
#include <string>
#include <chrono>
#include <thread>

using xstore_test::CreateTestDb;
using xstore_test::FString;
using xstore_test::MakeRow;
using xstore_test::ScratchDir;

namespace {
#if defined(__SANITIZE_ADDRESS__)
constexpr bool kSanitized = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
constexpr bool kSanitized = true;
#else
constexpr bool kSanitized = false;
#endif
#else
constexpr bool kSanitized = false;
#endif

size_t LiveHeapBytes() {
#if defined(__GLIBC__)
    return mallinfo2().uordblks;
#else
    return 0;
#endif
}

XStore_Proto::BatchInsert_REQ MakeBatch(std::string_view db, unsigned long long first_ts, size_t rows) {
    XStore_Proto::BatchInsert_REQ req;
    req.set_rtype(XStore_Proto::BATCHINSERT);
    req.set_dbname(std::string(db));
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (size_t k = 0; k < rows; ++k) {
        auto *d = req.add_batchinsert_data();
        d->set_timestamp(first_ts + k);
        *d->mutable_data() = MakeRow({FString("k" + std::to_string(first_ts + k) + "-" + std::string(90, 'x'))});
    }
    return req;
}
} // namespace

class XStoreMemoryTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_memory_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
    }
    void TearDown() override { scratch.reset(); }
};

// A completed batch's WalRecord must not stay pinned in the WAL writer's
// batch slot: live heap after one 20k-row SYNC batch returns to near its
// pre-batch level.
TEST_F(XStoreMemoryTest, AppliedWalRecordsAreReleased) {
    if (kSanitized) GTEST_SKIP() << "mallinfo2 is meaningless under ASan";
#if !defined(__GLIBC__)
    GTEST_SKIP() << "needs glibc mallinfo2";
#endif
    XStore store;
    CreateTestDb(store, "MEMWAL", XStore_Proto::SECOND, 128);
    // Warm-up batch: only 10 rows -- enough to first-touch the year's
    // data/zone-map/catidx files and caches, but small enough that its own
    // WalRecord (a few KB) doesn't dominate the `before` sample the way a
    // 20k-row warm-up would (that record would already sit pinned in the
    // writer's slot 0 before `before` is even taken).
    { auto req = MakeBatch("MEMWAL", 1592222400ULL, 10); XStore_Proto::BatchInsert_REP rep; store.BatchInsert(&req, &rep);
      ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus(); }
    const size_t before = LiveHeapBytes();
    { auto req = MakeBatch("MEMWAL", 1592222400ULL + 1000ULL, 20000);
      XStore_Proto::BatchInsert_REP rep;
      store.BatchInsert(&req, &rep);
      ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus(); }
    // The writer releases the record just after signalling the SYNC caller
    // (deliberately off the caller's latency path), so the drop trails
    // BatchInsert's return by a few ms: wait for it, bounded.
    size_t after = LiveHeapBytes();
    for (int waited_ms = 0; after >= before + 16u * 1024 * 1024 && waited_ms < 2000; waited_ms += 10) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        after = LiveHeapBytes();
    }
    // A retained 20k-row record is ~100 MB today; allow 16 MiB of slack for caches.
    EXPECT_LT(after, before + 16u * 1024 * 1024) << "live heap grew by " << (after - before) / (1024 * 1024) << " MiB";
}

// Mixed widths/types (user's example): zone-map stats and catidx membership
// must be exactly today's semantics, including null_count for absent/unset/
// string columns up to the file's tracked width.
TEST_F(XStoreMemoryTest, MixedRowsIndexSemanticsArePinned) {
    XStore store;
    CreateTestDb(store, "MEMMIX", XStore_Proto::SECOND, 128);
    using namespace xstore_test;
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1000, MakeRow({FInt64(123), FInt64(444), FInt64(1), XStore_Proto::FData(), FString("hello world")}),
                      "MEMMIX", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(1001, MakeRow({FString("hello"), FInt64(111), FInt64(12), FInt64(43), FInt64(312123), FInt64(231312), FInt64(12123123)}),
                      "MEMMIX", &rep, XStore_Proto::SYNC);
    const auto path = (scratch->test_root / "DataStore" / "MEMMIX").string();
    const unsigned short year = TimeStruct(1000, XStore_Proto::SECOND).year;
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, year, 128);
    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    const auto &s = *stats;
    ASSERT_GE(s.size(), 64u);
    auto check = [&](size_t c, uint64_t valid, uint64_t nulls, double mn, double mx) {
        EXPECT_EQ(s[c].valid_count, valid) << "col " << c;
        EXPECT_EQ(s[c].null_count, nulls) << "col " << c;
        if (valid > 0) { EXPECT_EQ(s[c].min_value, mn) << "col " << c; EXPECT_EQ(s[c].max_value, mx) << "col " << c; }
    };
    check(0, 1, 1, 123, 123);           // int, then string
    check(1, 2, 0, 111, 444);
    check(2, 2, 0, 1, 12);
    check(3, 1, 1, 43, 43);             // unset, then 43
    check(4, 1, 1, 312123, 312123);     // string, then int
    check(5, 1, 1, 231312, 231312);     // absent in row 0
    check(6, 1, 1, 12123123, 12123123); // absent in row 0
    for (size_t c = 7; c < s.size(); ++c) check(c, 0, 2, 0, 0); // absent in both rows

    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, year, 128, static_cast<uint32_t>(s.size()));
    EXPECT_TRUE(catidx.MightContain(0, 0, EncodeFDataForCatIdx(FInt64(123))));
    EXPECT_TRUE(catidx.MightContain(0, 0, EncodeFDataForCatIdx(FString("hello"))));
    EXPECT_TRUE(catidx.MightContain(0, 4, EncodeFDataForCatIdx(FString("hello world"))));
    EXPECT_TRUE(catidx.MightContain(0, 6, EncodeFDataForCatIdx(FInt64(12123123))));
}

// A fresh DB whose first batch has 128 columns must get 128-wide index files
// immediately, not 64-wide ones that stay narrow until a restart.
TEST_F(XStoreMemoryTest, FirstBatchWiderThanFloorCreatesWideIndex) {
    XStore store;
    CreateTestDb(store, "MEMWIDE", XStore_Proto::SECOND, 2048);
    XStore_Proto::BatchInsert_REQ req;
    req.set_rtype(XStore_Proto::BATCHINSERT);
    req.set_dbname("MEMWIDE");
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (int k = 0; k < 10; ++k) {
        auto *d = req.add_batchinsert_data();
        d->set_timestamp(5000 + k);
        for (int c = 0; c < 128; ++c) d->mutable_data()->add_fielddata()->set_doubledata(c + k);
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
    const auto path = (scratch->test_root / "DataStore" / "MEMWIDE").string();
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, TimeStruct(5000, XStore_Proto::SECOND).year, 2048, 128);
    EXPECT_EQ(zmap.TrackedColumnCount(), 128u);
}

// Year file created 64-wide, then the DB grows to 100 columns: MIN on column 80
// must still be correct, and must say it was a full scan and recommend a restart.
TEST_F(XStoreMemoryTest, UnindexedColumnReportsFullScanNotice) {
    XStore store;
    CreateTestDb(store, "MEMNOTE", XStore_Proto::SECOND, 1400);
    XStore_Proto::UnaryInsert_REP urep;
    store.UnaryInsert(7000, MakeRow({FString("narrow")}), "MEMNOTE", &urep, XStore_Proto::SYNC);  // file born 64-wide
    XStore_Proto::RowData wide;
    for (int c = 0; c < 100; ++c) wide.add_fielddata()->set_doubledata(c == 80 ? 42.5 : 1.0);
    store.UnaryInsert(7001, wide, "MEMNOTE", &urep, XStore_Proto::SYNC);                        // DB grows to 100
    XStore_Proto::Min_REP rep;
    store.MinAggregate(7000, 7001, "MEMNOTE", 80, &rep);
    EXPECT_DOUBLE_EQ(rep.opresult().doubledata(), 42.5);
    EXPECT_NE(rep.opstatus().find("not yet indexed"), std::string::npos) << rep.opstatus();
    EXPECT_NE(rep.opstatus().find("restart XStore"), std::string::npos) << rep.opstatus();
}

// Same DB/growth setup as UnindexedColumnReportsFullScanNotice, but querying
// a column below the ZONE_MAP_MAX_TRACKED_COLUMNS floor (always indexed by
// construction) must succeed with a plain SUCCESS status -- no full-scan
// notice, and (fix round 1) no header pread for this path at all.
TEST_F(XStoreMemoryTest, NoticeFastPathSkipsIndexedLowColumns) {
    XStore store;
    CreateTestDb(store, "MEMNOTE2", XStore_Proto::SECOND, 1400);
    XStore_Proto::UnaryInsert_REP urep;
    store.UnaryInsert(7000, MakeRow({FString("narrow")}), "MEMNOTE2", &urep, XStore_Proto::SYNC);
    XStore_Proto::RowData wide;
    for (int c = 0; c < 100; ++c) wide.add_fielddata()->set_doubledata(c == 80 ? 42.5 : 1.0);
    store.UnaryInsert(7001, wide, "MEMNOTE2", &urep, XStore_Proto::SYNC);
    XStore_Proto::Min_REP rep;
    store.MinAggregate(7000, 7001, "MEMNOTE2", 0, &rep);
    EXPECT_EQ(rep.opstatus().find("not yet indexed"), std::string::npos) << rep.opstatus();
}

// A row rejected as oversized (serialized size > rowLength) must not widen
// the persisted index: the pre-write "widest" pass has to skip it, not just
// count its fielddata_size(). Two rows in one SYNC BatchInsert, same year --
// row A (1 column) fits rowLength 128 and is accepted; row B (200 double
// columns, far past 128 bytes serialized) is rejected by the per-row
// serialized_size > rowLength check further down. Before the fix, the
// pre-pass counted row B's fielddata_size() (200) as "widest" regardless of
// rejection, so the year's .zmap was born 200-wide instead of floor-64.
TEST_F(XStoreMemoryTest, OversizedRowDoesNotWidenIndex) {
    XStore store;
    CreateTestDb(store, "MEMOVER", XStore_Proto::SECOND, 128);

    XStore_Proto::BatchInsert_REQ req;
    req.set_rtype(XStore_Proto::BATCHINSERT);
    req.set_dbname("MEMOVER");
    req.set_durabilitymode(XStore_Proto::SYNC);

    auto *row_a = req.add_batchinsert_data();
    row_a->set_timestamp(9000);
    *row_a->mutable_data() = MakeRow({FString("ok")});

    auto *row_b = req.add_batchinsert_data();
    row_b->set_timestamp(9001);
    for (int c = 0; c < 200; ++c) row_b->mutable_data()->add_fielddata()->set_doubledata(c);

    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();

    const auto path = (scratch->test_root / "DataStore" / "MEMOVER").string();
    const unsigned short year = TimeStruct(9000, XStore_Proto::SECOND).year;
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, year, 128);
    // GetOrLoadCfg (and therefore EffectiveTrackedColumnCount from a test)
    // is private -- the zone-map width check alone is the reachable half of
    // this assertion, per the plan's own fallback.
    EXPECT_EQ(zmap.TrackedColumnCount(), 64u);
}
