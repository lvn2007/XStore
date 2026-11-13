#include <gtest/gtest.h>
#include "include/XStore/XStore_Indexer.h"
#include "include/XStore/XStore_Internal.h"   // WalRecord::AppendRow / ReserveRows
#include "include/DataStructs/FileRangeLock.h"
#include "XStoreSyscallFault.h"
#include "XStoreTestSupport.h"
#include "XStoreRebuildTestSupport.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <csignal>
#include <fcntl.h>
#include <fstream>
#include <sys/wait.h>
#include <unistd.h>

extern char **environ;

namespace {

class ScopedEnv {
public:
    ScopedEnv(const char *name, const char *value) : name_(name) {
        if (const char *old = std::getenv(name)) { had_old_ = true; old_ = old; }
        if (value) setenv(name, value, 1); else unsetenv(name);
    }
    ~ScopedEnv() {
        if (had_old_) setenv(name_.c_str(), old_.c_str(), 1); else unsetenv(name_.c_str());
    }
private:
    std::string name_;
    std::string old_;
    bool had_old_ = false;
};

TEST(IndexerConfigTest, DefaultsWithoutEnv) {
    ScopedEnv t("XSTORE_INDEXER_THREADS", nullptr);
    ScopedEnv b("XSTORE_INDEXER_BACKLOG_MB", nullptr);
    const xstore::IndexerConfig c = xstore::IndexerConfigFromEnv();
    const uint32_t hw = std::thread::hardware_concurrency();
    EXPECT_EQ(c.threads, std::max<uint32_t>(1, hw / 4));
    EXPECT_EQ(c.backlog_bytes, XStoreTunables::INDEXER_BACKLOG_MB_DEV_DEFAULT << 20);
}

TEST(IndexerConfigTest, EnvOverrides) {
    ScopedEnv t("XSTORE_INDEXER_THREADS", "3");
    ScopedEnv b("XSTORE_INDEXER_BACKLOG_MB", "2");
    const xstore::IndexerConfig c = xstore::IndexerConfigFromEnv();
    EXPECT_EQ(c.threads, 3u);
    EXPECT_EQ(c.backlog_bytes, uint64_t{2} << 20);
}

TEST(IndexerConfigTest, ZeroThreadsClampsToOne) {
    ScopedEnv t("XSTORE_INDEXER_THREADS", "0");
    EXPECT_EQ(xstore::IndexerConfigFromEnv().threads, 1u);
}

TEST(IndexerConfigTest, BacklogZeroMeansNoPool) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.Start({2, 0});
    EXPECT_FALSE(ix.Active());
    EXPECT_FALSE(ix.TryReserve(1));
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    ix.StopAndDrain(std::chrono::milliseconds(100));
}

TEST(IndexerConfigTest, ReservationAccounting) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.Start({2, 100});
    EXPECT_TRUE(ix.Active());
    EXPECT_TRUE(ix.TryReserve(60));
    EXPECT_FALSE(ix.TryReserve(60));
    ix.Release(60);
    EXPECT_TRUE(ix.TryReserve(60));
    EXPECT_EQ(ix.BacklogBytes(), 60u);
    EXPECT_TRUE(ix.TryReserve(40));  // exactly at the bound is allowed
    EXPECT_FALSE(ix.TryReserve(1));
    ix.Release(100);
    const auto t0 = std::chrono::steady_clock::now();
    ix.StopAndDrain(std::chrono::seconds(10));
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2));
    EXPECT_FALSE(ix.Active());
    EXPECT_FALSE(ix.TryReserve(1));
    ix.StopAndDrain(std::chrono::milliseconds(10));  // twice is safe
}

TEST(IndexerConfigTest, StopWithoutStartAndRestart) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.StopAndDrain(std::chrono::milliseconds(10));
    ix.Start({1, 10});
    EXPECT_TRUE(ix.TryReserve(10));
    xstore::Indexer::SetConfigForTests({3, 20});  // restarts: counter reset, new bound
    EXPECT_TRUE(ix.Active());
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    EXPECT_TRUE(ix.TryReserve(20));
    EXPECT_FALSE(ix.TryReserve(1));
    ix.Release(20);
    ix.StopAndDrain(std::chrono::seconds(10));
}

TEST(IndexerConfigTest, StartWithZeroThreadsStillRunsOneWorker) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.Start({0, 10});
    EXPECT_TRUE(ix.Active());
    ix.StopAndDrain(std::chrono::seconds(10));
    EXPECT_FALSE(ix.Active());
}

TEST(IndexerConfigTest, HugeOrNegativeThreadCountsClampTo256) {
    for (const char *v : {"100000", "-1", "18446744073709551615"}) {
        ScopedEnv t("XSTORE_INDEXER_THREADS", v);
        EXPECT_EQ(xstore::IndexerConfigFromEnv().threads, 256u) << v;
    }
    ScopedEnv t("XSTORE_INDEXER_THREADS", "256");
    EXPECT_EQ(xstore::IndexerConfigFromEnv().threads, 256u);
}

TEST(IndexerConfigTest, HugeOrNegativeBacklogSaturates) {
    const uint64_t saturated = (UINT64_MAX >> 20) << 20;
    for (const char *v : {"18446744073709551615", "-1", "17592186044416"}) {   // the last one is 2^44: << 20 overflows
        ScopedEnv b("XSTORE_INDEXER_BACKLOG_MB", v);
        EXPECT_EQ(xstore::IndexerConfigFromEnv().backlog_bytes, saturated) << v;
    }
    ScopedEnv b("XSTORE_INDEXER_BACKLOG_MB", "17592186044415");   // 2^44 - 1: still fits
    EXPECT_EQ(xstore::IndexerConfigFromEnv().backlog_bytes, uint64_t{17592186044415} << 20);
}

TEST(IndexerConfigTest, TryReserveDoesNotOverflowNearTheLimit) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.Start({1, UINT64_MAX});
    EXPECT_TRUE(ix.TryReserve(UINT64_MAX - 1));
    EXPECT_FALSE(ix.TryReserve(5)) << "used + bytes wraps around";
    EXPECT_TRUE(ix.TryReserve(1));
    ix.Release(UINT64_MAX);
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    ix.StopAndDrain(std::chrono::seconds(10));
}

TEST(IndexerConfigTest, HintsAreDroppedAndDrained) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.Start({2, 1000});
    std::vector<std::weak_ptr<xstore::PendingMap>> maps;
    for (int i = 0; i < 100; ++i) {
        xstore::IndexHint h;
        h.map = std::make_shared<xstore::PendingMap>(16);
        h.block = static_cast<uint64_t>(i);
        maps.push_back(h.map);
        ix.Enqueue(std::move(h));
    }
    ix.StopAndDrain(std::chrono::seconds(10));
    EXPECT_FALSE(ix.Active());
    for (const auto &m : maps) EXPECT_TRUE(m.expired()) << "a hint (and its map) outlived the drain";
}

TEST(IndexerConfigTest, EnqueueOnAnInactivePoolDropsTheHintAndItsReservation) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.Start({1, 0});
    xstore::IndexHint h;
    h.map = std::make_shared<xstore::PendingMap>(16);
    const std::weak_ptr<xstore::PendingMap> weak = h.map;
    ix.Enqueue(std::move(h));
    EXPECT_TRUE(weak.expired());
    // A hint whose reservation outlived the pool (taken just before a stop) gives it back.
    ix.Start({1, 100});
    ASSERT_TRUE(ix.TryReserve(40));
    ix.StopAndDrain(std::chrono::seconds(10));
    xstore::IndexHint late;
    late.map = std::make_shared<xstore::PendingMap>(16);
    late.reserved_bytes = 40;
    ix.Enqueue(std::move(late));
    EXPECT_EQ(ix.BacklogBytes(), 0u);
}

TEST(IndexerConfigTest, AHintWithAnEmptyMapDoesNotStopAWorker) {
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.Start({1, 1000});
    ASSERT_TRUE(ix.TryReserve(10));
    xstore::IndexHint empty;
    empty.reserved_bytes = 10;
    ix.Enqueue(std::move(empty));
    EXPECT_EQ(ix.BacklogBytes(), 0u) << "an empty hint is dropped and its reservation released";
    ASSERT_TRUE(ix.TryReserve(10));
    xstore::IndexHint h;
    h.map = std::make_shared<xstore::PendingMap>(16);
    h.reserved_bytes = 10;
    const std::weak_ptr<xstore::PendingMap> weak = h.map;
    ix.Enqueue(std::move(h));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while ((!weak.expired() || ix.BacklogBytes() != 0) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(weak.expired()) << "the only worker stopped at the empty hint";
    EXPECT_EQ(ix.BacklogBytes(), 0u) << "a stale hint releases its reservation";
    ix.StopAndDrain(std::chrono::seconds(10));
}

// ---------------------------------------------------------------------------------------------------------------------
// IndexerPassTest: one pass folds the pending rows of one block exactly once (stage 3, Task 3).

using namespace xstore_test;
using namespace xstore_rebuild_test;
using Rows = std::vector<std::pair<unsigned long long, XStore_Proto::RowData>>;

class IndexerPassTest : public ::testing::Test {
protected:
    static constexpr uint16_t kRowLength = 1024;   // 2048 rows per block, 1024 rows per 1 MiB chunk
    static constexpr unsigned short kYear = 1971;
    static constexpr uint64_t kBacklog = uint64_t{64} << 20;
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_indexer_pass_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        XStore::ClearLiveWidenRequestsForTests();
        xstore::Indexer::SetConfigForTests({1, kBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    }
    void TearDown() override {
        xstore::Indexer::SetPassDelayForTests(nullptr);
        xstore::Indexer::Instance().StopAndDrain(std::chrono::seconds(10));
        XStore::ResetShutdownForTests();
        // An ASYNC record is acked before its apply, and a test that waited only for its marks can end while the writer
        // still holds the record's pins (dropped as ApplyWalRecord returns, before PublishApplied): wait (bounded) for
        // every ring's applied LSN to reach its head before the clears free the files (TSan saw that free race).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        for (const auto &ring : walring::AllRings()) {
            while (ring->AppliedLsn() < ring->HeadLsn() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            if (ring->AppliedLsn() < ring->HeadLsn()) {
                ADD_FAILURE() << "WAL ring " << ring->path() << " did not drain: applied " << ring->AppliedLsn() << " < head "
                              << ring->HeadLsn();
            }
        }
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        scratch.reset();
    }

    std::string DbPath(const std::string &db) const { return (scratch->test_root / "DataStore" / db).string(); }
    std::string YearPath(const std::string &db, const char *ext) const {
        return DbPath(db) + "/" + std::to_string(kYear) + ext;
    }
    std::string Key(const std::string &db) const { return DbPath(db) + "/" + std::to_string(kYear); }

    static void Insert(XStore &store, const std::string &db, const Rows &rows) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (const auto &[ts, row] : rows) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(ts);
            *entry->mutable_data() = row;
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
    }
    // Every hint consumed (each releases its reservation once, after its pass let go of its pins).
    static bool Quiesce() {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (xstore::Indexer::Instance().BacklogBytes() != 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return xstore::Indexer::Instance().BacklogBytes() == 0;
    }
    // The pool is on in this fixture, so an insert hands its runs off: wait until they are folded, before a test
    // resets the sidecars (clearing the registries under a pass's pins would free the files it folds into).
    static void InsertAndIndex(XStore &store, const std::string &db, const Rows &rows) {
        Insert(store, db, rows);
        ASSERT_TRUE(Quiesce()) << "the indexer did not catch up";
    }
    static Rows Dense(uint64_t first, uint64_t count, int columns, uint64_t skip = UINT64_MAX) {
        Rows rows;
        for (uint64_t i = first; i < first + count; ++i) {
            if (i != skip) rows.emplace_back(Jan1(kYear) + i, MixedRow(i, columns));
        }
        return rows;
    }
    uint32_t Width(const std::string &db) const { return ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).tracked_column_count; }

    // Both sidecars marked clean (so their headers compare), then their bytes.
    std::pair<std::string, std::string> CleanBytes(const std::string &db, uint32_t width) const {
        {
            ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
            CatIdxFile::Pin c = XStore::PinCatIdx(DbPath(db), kYear, kRowLength, width);
            EXPECT_TRUE(z->MarkClean());
            EXPECT_TRUE(c->MarkClean());
        }
        return {ReadFileBytes(YearPath(db, ".zmap")), ReadFileBytes(YearPath(db, ".catidx"))};
    }
    // Fresh, empty sidecars for the year (the rows stay in the data file); returns the year's (new) pending map.
    std::shared_ptr<xstore::PendingMap> ResetSidecars(const std::string &db, uint32_t width) const {
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        std::filesystem::remove(YearPath(db, ".zmap"));
        std::filesystem::remove(YearPath(db, ".catidx"));
        ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
        CatIdxFile::Pin c = XStore::PinCatIdx(DbPath(db), kYear, kRowLength, width);
        EXPECT_EQ(z->Pending(), c->Pending());
        return z->Pending();
    }
    // A hint for `block` holding a reservation of `bytes` (taken here).
    xstore::IndexHint Hint(const std::string &db, const std::shared_ptr<xstore::PendingMap> &map, uint32_t width,
                           uint64_t block, uint64_t bytes) const {
        EXPECT_TRUE(xstore::Indexer::Instance().TryReserve(bytes));
        xstore::IndexHint h;
        h.map = map;
        h.db_base_path = DbPath(db);
        h.db_name = db;
        h.year = kYear;
        h.row_length = kRowLength;
        h.tracked_column_count = width;
        h.block = block;
        h.reserved_bytes = bytes;
        return h;
    }
};

// (a) Every row of a year marked pending by hand, one pass per block: the sidecars equal the synchronous build byte for
// byte, nothing is left pending and every reservation is back.
TEST_F(IndexerPassTest, OnePassPerBlockEqualsTheSynchronousBuild) {
    const std::string db = "IDXPASSA";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 3000, 40));
    const uint32_t width = Width(db);
    const auto ref = CleanBytes(db, width);
    ASSERT_NE(ref.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos);
    const auto map = ResetSidecars(db, width);
    ASSERT_NE(CleanBytes(db, width), ref) << "the reset sidecars must differ, or the comparison proves nothing";
    map->MarkRows(0, 3000);
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.RunPassForTests(Hint(db, map, width, 0, 2048 * kRowLength));
    // A writer that saw a narrower file: the pass folds at the file's current width, not the hint's.
    xstore::IndexHint narrow = Hint(db, map, width, 1, 952 * kRowLength);
    narrow.tracked_column_count = 8;
    ix.RunPassForTests(narrow);
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    EXPECT_EQ(map->AllPendingRuns().size(), 0u);
    EXPECT_EQ(ix.BacklogBytes(), 0u) << "each hint releases its reservation once";
    const auto got = CleanBytes(db, width);
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the synchronous build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the synchronous build";
}

// (b) A hint for a block without pending rows opens nothing and changes nothing.
TEST_F(IndexerPassTest, AStaleHintTouchesNothing) {
    const std::string db = "IDXPASSB";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    const auto ref = CleanBytes(db, width);
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    const auto map = xstore::PendingMaps::For(Key(db), RowsPerBlock(kRowLength));
    xstore::Indexer::Instance().RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
    EXPECT_EQ(XStore::ZoneMapRegistrySize(), 0u) << "a stale hint opened the sidecars";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".zmap")) == ref.first);
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".catidx")) == ref.second);
}

// (c) Two passes on the same block at once (the first held inside its lock): the rows are folded once.
TEST_F(IndexerPassTest, TwoPassesOnOneBlockFoldItOnce) {
    const std::string db = "IDXPASSC";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 3000, 40));
    const uint32_t width = Width(db);
    const auto ref = CleanBytes(db, width);
    const auto map = ResetSidecars(db, width);
    map->MarkRows(0, 3000);
    std::atomic<int> calls{0};
    xstore::Indexer::SetPassDelayForTests([&] {
        if (calls.fetch_add(1) == 0) std::this_thread::sleep_for(std::chrono::milliseconds(300));
    });
    xstore::Indexer &ix = xstore::Indexer::Instance();
    const xstore::IndexHint a = Hint(db, map, width, 0, 2048 * kRowLength);
    const xstore::IndexHint b = Hint(db, map, width, 0, 2048 * kRowLength);
    std::thread first([&] { ix.RunPassForTests(a); });
    while (calls.load() == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::thread second([&] { ix.RunPassForTests(b); });
    first.join();
    second.join();
    xstore::Indexer::SetPassDelayForTests(nullptr);
    ix.RunPassForTests(Hint(db, map, width, 1, 952 * kRowLength));
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    const auto got = CleanBytes(db, width);
    EXPECT_TRUE(got.first == ref.first) << ".zmap: a row was folded twice";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs";
}

// (d) While a pass holds its READ lock on the first chunk of a block: a writer of a row in that chunk waits for it, a
// writer of a row in the second chunk (not locked yet) goes through and hands its row to the pool (whose pass of the
// block waits for this one). The pass re-reads the bits under each chunk's lock, so each row is folded once whoever
// gets to it; the bits of the held chunk stay set until its fold.
// 5 columns: exact sums, so the fold order does not matter for the comparison.
TEST_F(IndexerPassTest, AWriterOfTheLockedChunkWaitsAndNoRowIsFoldedTwice) {
    const std::string db = "IDXPASSD", ref_db = "IDXPASSDREF";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, ref_db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, ref_db, Dense(0, 3000, 5));
    InsertAndIndex(store, db, Dense(0, 3000, 5, /*skip=*/100));
    const uint32_t width = Width(db);
    ASSERT_EQ(width, Width(ref_db));
    const auto ref = CleanBytes(ref_db, width);
    const auto map = ResetSidecars(db, width);
    map->MarkRows(0, 100);
    map->MarkRows(101, 2899);
    std::atomic<int> calls{0};
    std::atomic<bool> writer_done{false};
    std::thread writer;
    xstore::Indexer::SetPassDelayForTests([&] {
        if (calls.fetch_add(1) != 0) return;
        const std::vector<xstore::IndexRun> held = map->PendingRunsOfBlock(0);
        EXPECT_TRUE(!held.empty() && held.front().first_row == 0) << "the chunk's bits were cleared before its fold";
        Insert(store, db, Dense(1500, 1, 5));   // second chunk: same bytes again, handed off (no wait: the pool's pass
                                                // of block 0 waits for this one)
        writer = std::thread([&] {
            Insert(store, db, Dense(100, 1, 5));   // first chunk: held by the pass's READ lock
            writer_done.store(true);
        });
        FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (lock.WaitingWritersForTests() == 0 && !writer_done.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        EXPECT_GT(lock.WaitingWritersForTests(), 0u) << "the writer of the locked chunk is not waiting on the pass's lock";
        EXPECT_FALSE(writer_done.load()) << "a writer of the locked chunk did not wait for the pass";
    });
    xstore::Indexer &ix = xstore::Indexer::Instance();
    ix.RunPassForTests(Hint(db, map, width, 0, 2047 * kRowLength));
    ASSERT_TRUE(writer.joinable());
    writer.join();
    EXPECT_TRUE(writer_done.load());
    xstore::Indexer::SetPassDelayForTests(nullptr);
    ix.RunPassForTests(Hint(db, map, width, 0, kRowLength));   // what is left of block 0 (row 100 unless the pool got it)
    ix.RunPassForTests(Hint(db, map, width, 1, 952 * kRowLength));
    ASSERT_TRUE(Quiesce()) << "the writers' hints were not consumed";
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    const auto got = CleanBytes(db, width);
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the synchronous build of all rows";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the synchronous build of all rows";
}

// (e) A read failure: the bits stay set, both sidecars are marked dirty on disk, the failure is logged once per file
// (a second failing pass says nothing) and the reservation is released.
TEST_F(IndexerPassTest, AReadFailureLeavesTheRowsPendingAndTheSidecarsDirty) {
    const std::string db = "IDXPASSE";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    static_cast<void>(CleanBytes(db, width));
    ASSERT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ASSERT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 10);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 10); });   // also on an early ASSERT return
    xstore::Indexer &ix = xstore::Indexer::Instance();
    kLogCapture->Clear();
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 0, 1000, EIO);
        ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
        ASSERT_GT(fail.Failures(), 0) << "the pass did not read the data file";
        EXPECT_EQ(kLogCapture->Count("[INDEXER]"), 1u);
        ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
    }
    EXPECT_EQ(kLogCapture->Count("[INDEXER]"), 1u) << "logged again for the same file";
    const std::vector<xstore::IndexRun> runs = map->PendingRunsOfBlock(0);
    ASSERT_EQ(runs.size(), 1u);
    EXPECT_EQ(runs[0].first_row, 0u);
    EXPECT_EQ(runs[0].row_count, 10u);
    EXPECT_EQ(ix.BacklogBytes(), 0u) << "a failed hint must still give its reservation back";
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
}

// A read that fails part way through a chunk (its first run already folded, the second not): the bits stay set so a
// retry folds both runs, and the block is marked inexact, because the retry folds the first run a second time
// (count and sum doubled). Aggregates scan an inexact block, so they stay exact.
TEST_F(IndexerPassTest, AReadFailureAfterAFoldedRunMarksTheBlockInexact) {
    const std::string db = "IDXPASSI";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows = Dense(0, 10, 40);
    const Rows second = Dense(20, 10, 40);
    rows.insert(rows.end(), second.begin(), second.end());
    InsertAndIndex(store, db, rows);
    const uint32_t width = Width(db);
    const auto map = ResetSidecars(db, width);
    map->MarkRows(0, 10);
    map->MarkRows(20, 10);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRuns(map->AllPendingRuns()); });
    xstore::Indexer &ix = xstore::Indexer::Instance();
    {
        // An earlier failure on this file (nothing folded): the partial failure below is not the file's first.
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 0, 1000, EIO);
        ix.RunPassForTests(Hint(db, map, width, 0, 20 * kRowLength));
        ASSERT_GT(fail.Failures(), 0);
    }
    {
        // FoldStoredRows reads one run per pread: the first passes, the second fails.
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 1, 1, EIO);
        ix.RunPassForTests(Hint(db, map, width, 0, 20 * kRowLength));
        ASSERT_EQ(fail.Failures(), 1) << "the second run's read was not the one that failed";
    }
    const std::vector<xstore::IndexRun> left = map->PendingRunsOfBlock(0);
    ASSERT_EQ(left.size(), 2u);
    EXPECT_EQ(left[0].first_row, 0u);
    EXPECT_EQ(left[1].first_row, 20u);
    ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
    EXPECT_FALSE(z->TryGetBlockStats(0).has_value()) << "a pending block is trusted";
    ix.RunPassForTests(Hint(db, map, width, 0, 20 * kRowLength));   // the retry: folds run 1 a second time
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    EXPECT_TRUE(z->BlockInexact(0)) << "the block holding a twice-folded run is trusted";
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    double expected = 0;
    for (const auto &[ts, row] : rows) {
        if (row.fielddata(0).has_doubledata()) expected += row.fielddata(0).doubledata();
    }
    XStore_Proto::Sum_REP sum;
    store.SumAggregate(Jan1(kYear), Jan1(kYear) + RowsPerBlock(kRowLength) - 1, db, 0, &sum);
    EXPECT_EQ(sum.opresult(), expected) << sum.opstatus();
}

// The data file cannot be opened (EACCES here; EMFILE alike): handled like a failed read: the bits stay set, both
// sidecars are marked dirty, logged once per file, the reservation is released.
TEST_F(IndexerPassTest, ADataFileThatCannotBeOpenedIsHandledLikeAFailedRead) {
    const std::string db = "IDXPASSJ";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    static_cast<void>(CleanBytes(db, width));
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 10);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 10); });
    store.EvictFdForTests(YearPath(db, ".XSTORE"));
    xstore::Indexer &ix = xstore::Indexer::Instance();
    kLogCapture->Clear();
    {
        xstore_fault::ScopedPathFault fail(YearPath(db, ".XSTORE"), false, EACCES,
                                           xstore_fault::kOpenRead | xstore_fault::kOpenWrite);
        ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
        ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
        ASSERT_GT(fail.OpenHits(), 0u) << "the pass did not try to open the data file";
    }
    EXPECT_EQ(kLogCapture->Count("[INDEXER]"), 1u);
    EXPECT_EQ(map->PendingRunsOfBlock(0).size(), 1u);
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
}

// (f) A hint for a year that was removed (and its DB re-created) is dropped: no file of the old year comes back.
TEST_F(IndexerPassTest, AHintForARemovedYearIsDropped) {
    const std::string db = "IDXPASSF";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 10);
    ASSERT_NE(store.RemoveDB(db).removestatus().find("Removed DB"), std::string::npos);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    xstore::Indexer::Instance().RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".XSTORE")));
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".zmap")));
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".catidx")));
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    map->ClearRows(0, 10);
}

// A lock that does not come in time re-queues the hint (the pool folds it once the writer is gone).
TEST_F(IndexerPassTest, ATimedOutLockRequeuesTheHint) {
    const std::string db = "IDXPASSG";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    const auto ref = CleanBytes(db, width);
    const auto map = ResetSidecars(db, width);
    map->MarkRows(0, 10);
    xstore::Indexer &ix = xstore::Indexer::Instance();
    {
        FileRangeLockGuard writer(YearPath(db, ".XSTORE"), 0, 10 * kRowLength, FileRangeLock::LockType::WRITE);
        ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
        EXPECT_EQ(map->PendingBlockCount(), 1u) << "folded under a writer's lock";
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while ((map->PendingBlockCount() != 0 || ix.BacklogBytes() != 0) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "the timed-out hint was lost";
    EXPECT_EQ(ix.BacklogBytes(), 0u);
    const auto got = CleanBytes(db, width);
    EXPECT_TRUE(got.first == ref.first);
    EXPECT_TRUE(got.second == ref.second);
}

// StopAndDrain is bounded while workers run real passes: once the bound passes, the queued hints are dropped (their
// reservations released) instead of being worked through.
TEST_F(IndexerPassTest, StopAndDrainIsBoundedWhilePassesRun) {
    const std::string db = "IDXPASSH";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 1, 40));
    const uint32_t width = Width(db);
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    constexpr uint64_t kBlocks = 200;
    for (uint64_t b = 0; b < kBlocks; ++b) map->MarkRows(b * RowsPerBlock(kRowLength), 1);
    xstore::Indexer::SetPassDelayForTests([] { std::this_thread::sleep_for(std::chrono::milliseconds(50)); });
    xstore::Indexer &ix = xstore::Indexer::Instance();
    for (uint64_t b = 0; b < kBlocks; ++b) ix.Enqueue(Hint(db, map, width, b, kRowLength));
    const auto t0 = std::chrono::steady_clock::now();
    ix.StopAndDrain(std::chrono::milliseconds(100));
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2)) << "the drain worked through every hint";
    EXPECT_EQ(ix.BacklogBytes(), 0u) << "dropped hints keep their reservations";
    EXPECT_GT(map->PendingBlockCount(), 0u);
    map->ClearRuns(map->AllPendingRuns());
}

// ---------------------------------------------------------------------------------------------------------------------
// IndexerWriteTest: ApplyWalRecord hands its runs to the indexer, or folds inline when the pool is off or full (Task 4).

class IndexerWriteTest : public IndexerPassTest {
protected:
    static constexpr uint64_t kPoolBacklog = uint64_t{64} << 20;
    std::atomic<bool> released_{true};

    void TearDown() override {
        released_.store(true);   // a held pass must not outlive the test (the hook reads this member)
        IndexerPassTest::TearDown();
    }
    // Every pass waits inside its READ lock until ReleasePasses().
    void HoldPasses() {
        released_.store(false);
        xstore::Indexer::SetPassDelayForTests([this] {
            while (!released_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }
    void ReleasePasses() {
        released_.store(true);
        xstore::Indexer::SetPassDelayForTests(nullptr);
    }
    std::shared_ptr<xstore::PendingMap> Map(const std::string &db) const { return xstore::PendingMaps::Find(Key(db)); }
    // The reference: the same rows inserted with the pool off (the synchronous build).
    std::pair<std::string, std::string> ReferenceBytes(XStore &store, const std::string &ref_db, const Rows &rows) {
        xstore::Indexer::SetConfigForTests({2, 0});
        CreateTestDb(store, ref_db, XStore_Proto::SECOND, kRowLength);
        for (size_t at = 0; at < rows.size(); at += 4000) {
            Insert(store, ref_db, Rows(rows.begin() + at, rows.begin() + std::min(rows.size(), at + 4000)));
        }
        return CleanBytes(ref_db, Width(ref_db));
    }
    // An oracle independent of the has_obs fold: a 1-byte backlog, smaller than any run (a run holds at least one
    // 1024-byte row), so every run takes the rows-only inline re-read fold, never the request thread's observations.
    // expect_content: the reference sidecars must hold data, so an equivalence test cannot pass by comparing two empty
    // files; false only for a test whose reference is legitimately (almost) empty.
    std::pair<std::string, std::string> RowsOnlyReferenceBytes(XStore &store, const std::string &ref_db, const Rows &rows,
                                                               bool expect_content = true) {
        xstore::Indexer::SetConfigForTests({2, 1, /*min_run_rows=*/1, /*hand_off_sync=*/true});
        CreateTestDb(store, ref_db, XStore_Proto::SECOND, kRowLength);
        const uint64_t before = XStore::InlineRereadFoldsForTests();
        for (size_t at = 0; at < rows.size(); at += 4000) {
            Insert(store, ref_db, Rows(rows.begin() + at, rows.begin() + std::min(rows.size(), at + 4000)));
        }
        EXPECT_GT(XStore::InlineRereadFoldsForTests(), before) << "the reference did not take the rows-only fold";
        auto bytes = CleanBytes(ref_db, Width(ref_db));
        if (expect_content) {
            EXPECT_NE(bytes.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos)
                << "the reference .zmap is empty";
            EXPECT_NE(bytes.second.find_first_not_of('\0', sizeof(CatIdxFileHeader)), std::string::npos)
                << "the reference .catidx is empty";
        }
        return bytes;
    }
    // ASYNC (the only mode the production predicate hands off): acked before the writer applies; see WaitFor. Returns
    // false (with a failure recorded) when the call was not acked, so a caller stops before a long wait.
    [[nodiscard]] static bool InsertAsync(XStore &store, const std::string &db, const Rows &rows) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::ASYNC);
        for (const auto &[ts, row] : rows) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(ts);
            *entry->mutable_data() = row;
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        const bool ok = rep.insertstatus().find("successfully") != std::string::npos;
        EXPECT_TRUE(ok) << rep.insertstatus();
        return ok;
    }
    // Bounded wait for `done` (an ASYNC record's apply).
    template <class Done>
    static bool WaitFor(Done done, std::chrono::seconds bound = std::chrono::seconds(10)) {
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while (!done() && std::chrono::steady_clock::now() < deadline) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        return done();
    }
    // The year's pending map once the writer has applied (marked) at least `rows` rows of it, or null after the bound.
    std::shared_ptr<xstore::PendingMap> AppliedMap(const std::string &key, uint64_t rows) const {
        std::shared_ptr<xstore::PendingMap> map;
        static_cast<void>(WaitFor([&] {
            map = xstore::PendingMaps::Find(key);
            return map != nullptr && map->TotalMarkedRowsForTests() >= rows;
        }));
        return map != nullptr && map->TotalMarkedRowsForTests() >= rows ? map : nullptr;
    }
    // With every pass held: true once the writer has reserved backlog for at least `rows` rows (marks come before the
    // reservation, so AppliedMap alone does not mean an ASYNC hand-off finished). A has_obs record reserves nothing.
    static bool HandedOff(uint64_t rows) {
        return WaitFor([rows] { return xstore::Indexer::Instance().BacklogBytes() >= rows * kRowLength; });
    }
};

// (a) With the pool on and every pass held: the rows stay pending after the ack and the sidecars hold nothing; after
// the release the indexer folds them to the synchronous build's bytes.
TEST_F(IndexerWriteTest, HandedOffRowsStayPendingUntilTheIndexerFoldsThem) {
    const std::string db = "IDXWRA", ref_db = "IDXWRAREF";
    XStore store;
    const Rows rows = Dense(0, 3000, 40);
    const auto ref = ReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    Insert(store, db, rows);
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    EXPECT_GT(map->PendingBlockCount(), 0u) << "the rows were folded inline";
    EXPECT_EQ(map->TotalMarkedRowsForTests(), 3000u);
    EXPECT_GT(xstore::Indexer::Instance().BacklogBytes(), 0u);
    // Unfolded: the bodies differ from the folded reference (which has content, or the comparison proves nothing).
    ASSERT_NE(ref.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos);
    ASSERT_NE(ref.second.find_first_not_of('\0', sizeof(CatIdxFileHeader)), std::string::npos);
    const std::string zmap = ReadFileBytes(YearPath(db, ".zmap"));
    const std::string catidx = ReadFileBytes(YearPath(db, ".catidx"));
    EXPECT_EQ(zmap.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos) << "the zone map was folded";
    EXPECT_NE(zmap.substr(sizeof(ZoneMapFileHeader)), ref.first.substr(sizeof(ZoneMapFileHeader)));
    EXPECT_NE(catidx.substr(sizeof(CatIdxFileHeader)), ref.second.substr(sizeof(CatIdxFileHeader)))
        << "the catidx was folded";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the synchronous build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the synchronous build";
}

// Clean sidecars stay trusted after a restart, so a handed-off run (folded only after the ack) must mark both dirty on
// disk before the ack, as the inline fold does: a kill before the indexer's pass then leaves them dirty (rebuilt).
TEST_F(IndexerWriteTest, AHandOffMarksCleanSidecarsDirtyOnDiskBeforeTheAck) {
    const std::string db = "IDXWRDIRTY";
    XStore store;
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    static_cast<void>(CleanBytes(db, Width(db)));
    ASSERT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ASSERT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    HoldPasses();
    Insert(store, db, Dense(3000, 1000, 40));   // a fresh block: no overwrite check touches the zone map
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    ASSERT_GT(map->PendingBlockCount(), 0u) << "the run was not handed off";
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u) << ".zmap clean on disk after the ack";
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u) << ".catidx clean on disk after the ack";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
}

// (b) A backlog smaller than any run: every run is folded inline, nothing stays pending, no reservation is kept.
TEST_F(IndexerWriteTest, AFullBacklogFoldsInline) {
    const std::string db = "IDXWRB", ref_db = "IDXWRBREF";
    XStore store;
    const Rows rows = Dense(0, 3000, 40);
    const auto ref = ReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, 1024, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    Insert(store, db, rows);
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    EXPECT_EQ(map->TotalMarkedRowsForTests(), 3000u);
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "a run was handed off past the backlog bound";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    ReleasePasses();
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first);
    EXPECT_TRUE(got.second == ref.second);
}

// (c) Backlog 0 is the stage 2 path: nothing pending, the synchronous bytes.
TEST_F(IndexerWriteTest, BacklogZeroIsTheSynchronousPath) {
    const std::string db = "IDXWRC", ref_db = "IDXWRCREF";
    XStore store;
    const Rows rows = Dense(0, 3000, 40);
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, rows);
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first);
    EXPECT_TRUE(got.second == ref.second);
}

// Backlog 0 is the base build's path: the request thread builds the observations, the writer only applies them. The
// writer must never read rows back to fold them (that serial stage was the -18.5 % of the 2026-11-06 G1 run).
TEST_F(IndexerWriteTest, BacklogZeroBuildsObservationsOnTheRequestThread) {
    const std::string db = "IDXOFF", ref_db = "IDXOFFREF";
    XStore store;
    const Rows rows = Dense(0, 3000, 40);
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    Insert(store, db, rows);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before)
        << "the writer re-read rows to fold them with the pool off";
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the synchronous build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the synchronous build";
}

// The same for UNARY inserts: one has_obs record per row, folded from its observations, never re-read.
TEST_F(IndexerWriteTest, BacklogZeroUnaryInsertsBuildObservationsOnTheRequestThread) {
    const std::string db = "IDXOFFU", ref_db = "IDXOFFUREF";
    XStore store;
    const Rows rows = Dense(0, 200, 40);
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    ASSERT_NE(ref.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos) << "an empty zone map proves nothing";
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    for (const auto &[ts, row] : rows) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(ts, row, db, &rep, XStore_Proto::SYNC);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    }
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before)
        << "the writer re-read unary rows to fold them with the pool off";
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// A DB written first with the pool off (observation records) and then with it on (rows-only, handed off) ends in the
// rows-only build's bytes: the two record kinds coexist on one year. Each half holds an empty row (a hole to both folds).
TEST_F(IndexerWriteTest, ObservationAndRowsOnlyRecordsMixOnOneYear) {
    const std::string db = "IDXMIX", ref_db = "IDXMIXREF";
    XStore store;
    Rows rows = Dense(0, 6000, 40);
    rows[100].second = XStore_Proto::RowData();
    rows[4000].second = XStore_Proto::RowData();
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    Insert(store, db, Rows(rows.begin(), rows.begin() + 3000));
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before) << "the pool-off half was not folded from its observations";
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    HoldPasses();
    Insert(store, db, Rows(rows.begin() + 3000, rows.end()));
    ASSERT_GT(Map(db)->PendingBlockCount(), 0u) << "the pool-on half was not handed off";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(Map(db)->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// The same with the order reversed: rows-only records handed off first, then observation records on the same blocks.
TEST_F(IndexerWriteTest, RowsOnlyThenObservationRecordsMixOnOneYear) {
    const std::string db = "IDXMIXR", ref_db = "IDXMIXRREF";
    XStore store;
    Rows rows = Dense(0, 6000, 40);
    rows[100].second = XStore_Proto::RowData();
    rows[4000].second = XStore_Proto::RowData();
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    Insert(store, db, Rows(rows.begin(), rows.begin() + 3000));
    ASSERT_GT(Map(db)->PendingBlockCount(), 0u) << "the pool-on half was not handed off";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());   // before the restart below, which would drop hints still queued
    xstore::Indexer::SetConfigForTests({2, 0});
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    Insert(store, db, Rows(rows.begin() + 3000, rows.end()));
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before) << "the pool-off half was not folded from its observations";
    EXPECT_EQ(Map(db)->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// A pool-off batch with empty rows among normal ones (first row, block edges, last row): the observation fold skips
// them as holes, as the rows-only fold does.
TEST_F(IndexerWriteTest, AnObservationBatchWithEmptyRowsEqualsTheRowsOnlyBuild) {
    const std::string db = "IDXOBSEMPTY", ref_db = "IDXOBSEMPTYREF";
    XStore store;
    Rows rows = Dense(0, 3000, 40);
    for (const size_t i : {size_t{0}, size_t{5}, size_t{2047}, size_t{2048}, size_t{2999}}) rows[i].second = XStore_Proto::RowData();
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    Insert(store, db, rows);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before) << "the writer re-read rows to fold them with the pool off";
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// A pool-off record of ONLY empty rows folds nothing: no null is counted for them, so the sidecars equal the rows-only
// build of the other rows alone.
TEST_F(IndexerWriteTest, AnObservationRunOfOnlyEmptyRowsCountsNoNull) {
    const std::string db = "IDXOBSHOLES", ref_db = "IDXOBSHOLESREF";
    XStore store;
    const Rows rows = Dense(0, 10, 40);
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows, false);
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, rows);
    Rows holes;
    for (uint64_t i = 20; i < 25; ++i) holes.emplace_back(Jan1(kYear) + i, XStore_Proto::RowData());
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    Insert(store, db, holes);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before) << "the writer re-read rows to fold them with the pool off";
    EXPECT_EQ(Map(db)->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap counted the empty rows";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// A record built with the pool off (has_obs) but applied after the pool came on: the writer is held on a WRITE lock of
// the rows between the two, so the flip falls exactly there. The record is not handed off (passes are held: a hint
// would stay pending), it is folded from its observations (no re-read), its marks are set and cleared, and the bytes
// equal the rows-only build's.
TEST_F(IndexerWriteTest, AnObservationRecordAppliedAfterThePoolCameOnIsFoldedInline) {
    const std::string db = "IDXOBSFLIP", ref_db = "IDXOBSFLIPREF";
    XStore store;
    const Rows rows = Dense(0, 3000, 40);
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(0, rows.size() * kRowLength, FileRangeLock::LockType::WRITE);
    std::atomic<bool> writer_done{false};
    std::thread writer([&] {
        Insert(store, db, rows);   // records built here, with the pool off
        writer_done.store(true);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (lock.WaitingWritersForTests() == 0 && !writer_done.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const bool waiting = lock.WaitingWritersForTests() > 0 && !writer_done.load();
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    HoldPasses();
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    lock.UnlockRange(0, rows.size() * kRowLength, FileRangeLock::LockType::WRITE);
    writer.join();
    ASSERT_TRUE(waiting) << "the record's apply did not wait on the test's lock: the flip did not fall between build and apply";
    ASSERT_TRUE(xstore::Indexer::Instance().Active());
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    EXPECT_EQ(map->TotalMarkedRowsForTests(), rows.size()) << "the runs were not marked";
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "a has_obs record was handed off";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u) << "a has_obs record took a reservation";
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before) << "the has_obs record was re-read instead of folded";
    ReleasePasses();
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// (d) A failed data pwrite with the pool on: the run is not handed off (nothing pending) and no reservation is kept.
TEST_F(IndexerWriteTest, AFailedWriteIsNotHandedOff) {
    const std::string db = "IDXWRD";
    XStore store;
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, Dense(0, 1, 40));   // creates the year file and its pending map
    ASSERT_TRUE(Quiesce());
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    ASSERT_EQ(map->PendingBlockCount(), 0u);
    const std::string zmap_before = ReadFileBytes(YearPath(db, ".zmap")).substr(sizeof(ZoneMapFileHeader));
    HoldPasses();   // a hint that slipped through would keep its block pending and its reservation
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname(db);
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (uint64_t i = 1; i < 3000; ++i) {   // spans two blocks
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(Jan1(kYear) + i);
        *entry->mutable_data() = MixedRow(i, 40);
    }
    XStore_Proto::BatchInsert_REP rep;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, ".XSTORE"), 0, 1000, EIO);
        store.BatchInsert(&req, &rep);
        ASSERT_GT(fail.Failures(), 0) << "the data pwrite was not reached: " << rep.insertstatus();
    }
    EXPECT_EQ(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
    EXPECT_EQ(map->TotalMarkedRowsForTests(), 3000u) << "the failed run was marked";
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "a failed run stayed pending";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u) << "a failed run kept its reservation";
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".zmap")).substr(sizeof(ZoneMapFileHeader)) == zmap_before)
        << "a run whose write failed was folded";
    ReleasePasses();
}

// (e) 8 writers x 20,000 scattered rows with the pool on: after quiesce the sidecars equal the synchronous build of the same rows. Small integral values: the block
// sums and sums of squares are exact in any order (MixedRow's large values round differently when folded out of order).
TEST_F(IndexerWriteTest, ConcurrentScatteredWritersEqualTheSynchronousBuild) {
    const std::string db = "IDXWRE", ref_db = "IDXWREREF";
    constexpr uint64_t kWriters = 8, kPerWriter = 20000, kBatch = 200;
    XStore store;
    Rows all;
    for (uint64_t i = 0; i < kWriters * kPerWriter; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(static_cast<double>((i * 7) % 1000) * 0.5);
        row.add_fielddata()->set_int64data(static_cast<int64_t>(i % 100000));
        if (i % 13 != 0) row.add_fielddata()->set_uint32data(static_cast<uint32_t>(i));
        all.emplace_back(Jan1(kYear) + i, std::move(row));
    }
    const auto ref = ReferenceBytes(store, ref_db, all);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    // 8 SYNC writers fill the ring: it must drain. WalCheckpointSyncTest leaves the checkpointer paused (its SetUp
    // pauses, its TearDown does not resume), so a run of both suites in one process would hang here.
    walring::Checkpointer::Instance().SetPausedForTests(false);
    walring::Checkpointer::Instance().Start();
    std::atomic<bool> writing{true};
    std::atomic<uint64_t> peak{0};
    std::thread sampler([&] {
        while (writing.load()) {
            peak.store(std::max(peak.load(), xstore::Indexer::Instance().BacklogBytes()));
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });
    std::vector<std::thread> writers;
    for (uint64_t w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            std::vector<uint64_t> mine;
            for (uint64_t i = 0; i < kPerWriter; ++i) mine.push_back(i * kWriters + w);
            std::mt19937_64 rng(w + 1);
            std::shuffle(mine.begin(), mine.end(), rng);
            for (size_t at = 0; at < mine.size(); at += kBatch) {
                std::vector<uint64_t> batch(mine.begin() + at, mine.begin() + std::min(mine.size(), at + kBatch));
                std::sort(batch.begin(), batch.end());
                XStore_Proto::BatchInsert_REQ req;
                req.set_dbname(db);
                req.set_durabilitymode(XStore_Proto::SYNC);
                for (uint64_t i : batch) {
                    XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                    entry->set_timestamp(all[i].first);
                    *entry->mutable_data() = all[i].second;
                }
                // 8 SYNC writers can fill the WAL ring: an overloaded batch (one year = one group) wrote nothing and
                // is retried, as a client would.
                std::string status;
                for (int attempt = 0; attempt < 10000; ++attempt) {
                    XStore_Proto::BatchInsert_REP rep;
                    store.BatchInsert(&req, &rep);
                    status = rep.insertstatus();
                    if (status.find(XStore::kWalOverloadedText) == std::string::npos) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                ASSERT_NE(status.find("successfully"), std::string::npos) << status;
            }
        });
    }
    for (std::thread &t : writers) t.join();
    writing.store(false);
    sampler.join();
    EXPECT_GT(peak.load(), 0u) << "no run was handed to the indexer";
    EXPECT_LE(peak.load(), kPoolBacklog) << "the backlog went past its bound (a hint's share is wrong)";
    ASSERT_TRUE(Quiesce());
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    const uint32_t width = Width(db);
    ASSERT_EQ(width, Width(ref_db));
    const auto got = CleanBytes(db, width);
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the synchronous build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the synchronous build";
}

// ---------------------------------------------------------------------------------------------------------------------
// Adaptive hand-off (plan 2026-11-12): a record goes to the pool only when it is ASYNC and its average contiguous run is
// at least min_run_rows; every other record is has_obs (the request thread builds its observations). The tests below use
// the production semantics (hand_off_sync false), so they insert ASYNC where the pool may take the record and wait for the
// writer's apply (AppliedMap / WaitFor).

// Pure function: a request goes to the pool only when its average contiguous run is long enough.
TEST(IndexerPredicateTest, HandOffNeedsAnAverageRunOfMinRunRows) {
    const auto async_mode = XStore_Proto::ASYNC;
    EXPECT_FALSE(HandOffToPool(/*rows=*/40, /*runs=*/40, /*min_run_rows=*/64, async_mode, false));   // sparse: 1 row per run
    EXPECT_FALSE(HandOffToPool(1000, 20, 64, async_mode, false));                           // 50 per run
    EXPECT_TRUE(HandOffToPool(1280, 20, 64, async_mode, false));                            // exactly 64 per run
    EXPECT_TRUE(HandOffToPool(1000, 1, 64, async_mode, false));                             // dense
    EXPECT_FALSE(HandOffToPool(1000, 1, 64, XStore_Proto::SYNC, false));                    // dense but SYNC: the client waits anyway
    EXPECT_FALSE(HandOffToPool(1000, 1, 64, XStore_Proto::BYPASS, false));
    EXPECT_TRUE(HandOffToPool(1000, 1, 64, XStore_Proto::SYNC, true));                      // the tests-only switch
    EXPECT_FALSE(HandOffToPool(0, 0, 64, async_mode, false));                               // empty: nothing to hand off
    EXPECT_TRUE(HandOffToPool(5, 5, 1, async_mode, false));                                 // threshold 1 = always (the old behaviour)
}

TEST(IndexerConfigTest, MinRunRowsFromEnvAndClamped) {
    {
        ScopedEnv m("XSTORE_INDEXER_MIN_RUN_ROWS", nullptr);
        const xstore::IndexerConfig c = xstore::IndexerConfigFromEnv();
        EXPECT_EQ(c.min_run_rows, XStoreTunables::INDEXER_MIN_RUN_ROWS_DEFAULT);
        EXPECT_FALSE(c.hand_off_sync) << "the test-only switch has no env variable";
    }
    // 0, a negative or an unparsable value keeps the default: clamping it to 1 would hand off every record (the measured
    // sparse collapse).
    constexpr uint32_t kDefault = XStoreTunables::INDEXER_MIN_RUN_ROWS_DEFAULT;
    for (const auto &[v, want] : std::vector<std::pair<const char *, uint32_t>>{
             {"64", 64u}, {"1", 1u}, {"99999999", 1u << 20}, {"0", kDefault}, {"abc", kDefault}, {"", kDefault},
             {"-5", kDefault}, {"12abc", kDefault}}) {
        ScopedEnv m("XSTORE_INDEXER_MIN_RUN_ROWS", v);
        EXPECT_EQ(xstore::IndexerConfigFromEnv().min_run_rows, want) << '"' << v << '"';
    }
}

// The run count the predicate divides by: duplicates start a new run (conservative).
TEST(IndexerPredicateTest, CountContiguousRunsCountsGapsAndDuplicates) {
    struct TsRows {
        std::vector<unsigned long long> ts;
        uint64_t Timestamp(int i) const { return ts[static_cast<size_t>(i)]; }
    };
    const auto runs = [](std::vector<unsigned long long> ts) {
        std::vector<unsigned long long int> indices(ts.size());
        for (size_t i = 0; i < indices.size(); ++i) indices[i] = i;
        return CountContiguousRuns(TsRows{std::move(ts)}, indices);
    };
    EXPECT_EQ(runs({}), 0u);
    EXPECT_EQ(runs({7}), 1u);
    EXPECT_EQ(runs({7, 8, 9, 10}), 1u) << "ascending contiguous";
    EXPECT_EQ(runs({7, 8, 10, 11}), 2u) << "one gap";
    EXPECT_EQ(runs({7, 8, 8, 9}), 2u) << "a duplicate timestamp starts a new run";
    // Only the rows the indices name count (a year group of a larger request).
    const TsRows rows{{1, 2, 3, 100, 101, 4}};
    EXPECT_EQ(CountContiguousRuns(rows, std::vector<unsigned long long int>{0, 1, 2, 5}), 1u);
}

// With the pool on, a sparse batch is indexed by the request thread (base path): the writer never re-reads and nothing is
// pending; a dense batch still goes through the pool. Both end in the independent reference's bytes.
TEST_F(IndexerWriteTest, SparseRecordsTakeTheRequestThreadPathAndDenseRecordsTheHandOff) {
    const std::string db = "IDXADAPT", ref_db = "IDXADAPTREF";
    XStore store;
    Rows sparse;   // 40 rows, every second timestamp: 40 runs of 1 row
    for (uint64_t i = 0; i < 40; ++i) sparse.emplace_back(Jan1(kYear) + 2 * i, MixedRow(2 * i, 40));
    const Rows dense = Dense(10000, 3000, 40);   // 3000 contiguous rows: one run
    Rows all = sparse;
    all.insert(all.end(), dense.begin(), dense.end());
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, all);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/16});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    const uint64_t rereads = XStore::InlineRereadFoldsForTests();
    ASSERT_TRUE(InsertAsync(store, db, sparse));
    const auto map = AppliedMap(Key(db), sparse.size());
    ASSERT_NE(map, nullptr) << "the sparse record was not applied";
    EXPECT_TRUE(WaitFor([&] { return map->PendingBlockCount() == 0; }));
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "a sparse record was handed to the pool";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), rereads) << "the writer re-read a sparse record";
    ASSERT_TRUE(InsertAsync(store, db, dense));
    EXPECT_TRUE(HandedOff(dense.size())) << "a dense record was not handed to the pool";
    EXPECT_GT(map->PendingBlockCount(), 0u) << "a dense record was not handed to the pool";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// One request, two year groups: a sparse one (request thread) and a dense one (handed off) = one record of each kind.
// Both years end in the independent reference's bytes.
TEST_F(IndexerWriteTest, ARequestWithASparseAndADenseYearMakesOneRecordOfEachKind) {
    const std::string db = "IDXADAPTY", ref_db = "IDXADAPTYREF";
    constexpr unsigned short kYear2 = 1972;
    XStore store;
    Rows rows;
    for (uint64_t i = 0; i < 40; ++i) rows.emplace_back(Jan1(kYear) + 3 * i, MixedRow(3 * i, 40));   // 40 runs of 1
    for (uint64_t i = 0; i < 3000; ++i) rows.emplace_back(Jan1(kYear2) + i, MixedRow(i, 40));         // one run
    const auto year2_bytes = [&](const std::string &name) {
        const std::string base = DbPath(name) + "/" + std::to_string(kYear2);
        const uint32_t width = ReadHeader<ZoneMapFileHeader>(base + ".zmap").tracked_column_count;
        {
            ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(name), kYear2, kRowLength, width);
            CatIdxFile::Pin c = XStore::PinCatIdx(DbPath(name), kYear2, kRowLength, width);
            EXPECT_TRUE(z->MarkClean());
            EXPECT_TRUE(c->MarkClean());
        }
        return std::make_pair(ReadFileBytes(base + ".zmap"), ReadFileBytes(base + ".catidx"));
    };
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    const auto ref2 = year2_bytes(ref_db);
    ASSERT_NE(ref2.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/16});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    ASSERT_TRUE(InsertAsync(store, db, rows));
    const auto sparse_map = AppliedMap(Key(db), 40);
    const auto dense_map = AppliedMap(DbPath(db) + "/" + std::to_string(kYear2), 3000);
    ASSERT_NE(sparse_map, nullptr) << "the sparse year's record was not applied";
    ASSERT_NE(dense_map, nullptr) << "the dense year's record was not applied";
    EXPECT_TRUE(WaitFor([&] { return sparse_map->PendingBlockCount() == 0; }));
    EXPECT_EQ(sparse_map->PendingBlockCount(), 0u) << "the sparse year's record was handed to the pool";
    EXPECT_TRUE(HandedOff(3000)) << "the dense year's record was not handed to the pool";
    EXPECT_GT(dense_map->PendingBlockCount(), 0u) << "the dense year's record was not handed to the pool";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(dense_map->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << kYear << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << kYear << ".catidx differs from the rows-only build";
    const auto got2 = year2_bytes(db);
    EXPECT_TRUE(got2.first == ref2.first) << kYear2 << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got2.second == ref2.second) << kYear2 << ".catidx differs from the rows-only build";
}

// The production config (default min_run_rows, no hand_off_sync): a dense SYNC record is indexed by the request thread
// (nothing pending, nothing re-read), while the same rows ASYNC are handed off. Both end in the reference's bytes.
TEST_F(IndexerWriteTest, ADenseSyncRecordTakesTheRequestThreadPathAndTheSameRowsAsyncAreHandedOff) {
    const std::string db = "IDXADAPTS", async_db = "IDXADAPTA", ref_db = "IDXADAPTSREF";
    XStore store;
    const Rows rows = Dense(0, 3000, 40);
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog});
    ASSERT_EQ(xstore::Indexer::Instance().MinRunRows(), XStoreTunables::INDEXER_MIN_RUN_ROWS_DEFAULT);
    ASSERT_FALSE(xstore::Indexer::Instance().HandOffSync());
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, async_db, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    const uint64_t rereads = XStore::InlineRereadFoldsForTests();
    Insert(store, db, rows);   // SYNC
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    EXPECT_EQ(map->TotalMarkedRowsForTests(), rows.size());
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "a SYNC record was handed to the pool";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), rereads) << "a SYNC record was re-read instead of folded from its observations";
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap of the SYNC record differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx of the SYNC record differs from the rows-only build";
    ASSERT_TRUE(InsertAsync(store, async_db, rows));
    const auto async_map = AppliedMap(Key(async_db), rows.size());
    ASSERT_NE(async_map, nullptr) << "the ASYNC record was not applied";
    EXPECT_TRUE(HandedOff(rows.size())) << "the same rows ASYNC were not handed to the pool";
    EXPECT_GT(async_map->PendingBlockCount(), 0u) << "the same rows ASYNC were not handed to the pool";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(async_map->PendingBlockCount(), 0u);
    const auto got_async = CleanBytes(async_db, Width(async_db));
    EXPECT_TRUE(got_async.first == ref.first) << ".zmap of the ASYNC record differs from the rows-only build";
    EXPECT_TRUE(got_async.second == ref.second) << ".catidx of the ASYNC record differs from the rows-only build";
}

// A unary insert is one row (a run of 1): it takes the request-thread path even with the pool on, also ASYNC (the only
// mode the pool would take).
TEST_F(IndexerWriteTest, AUnaryInsertTakesTheRequestThreadPathWithThePoolOn) {
    const std::string db = "IDXADAPTU", ref_db = "IDXADAPTUREF";
    XStore store;
    Rows rows;
    for (uint64_t i = 0; i < 20; ++i) rows.emplace_back(Jan1(kYear) + 3 * i, MixedRow(3 * i, 40));
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/16});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    const uint64_t rereads = XStore::InlineRereadFoldsForTests();
    for (const auto &[ts, row] : rows) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(ts, row, db, &rep, XStore_Proto::ASYNC);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    }
    const auto map = AppliedMap(Key(db), rows.size());
    ASSERT_NE(map, nullptr) << "the unary records were not applied";
    EXPECT_TRUE(WaitFor([&] { return map->PendingBlockCount() == 0; }));
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "a unary record was handed to the pool";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), rereads) << "a unary record was re-read instead of folded from its observations";
    ReleasePasses();
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
}

// The exact boundary through the real insert path (production predicate, ASYNC, min_run_rows 16): 10 runs of exactly 16
// rows are handed off, 10 runs of 15 are not. Both end in the independent reference's bytes.
TEST_F(IndexerWriteTest, AnAverageRunOfExactlyMinRunRowsIsHandedOffAndOneLessIsNot) {
    const std::string db16 = "IDXADAPT16", db15 = "IDXADAPT15", ref16 = "IDXADAPT16REF", ref15 = "IDXADAPT15REF";
    XStore store;
    const auto runs_of = [](uint64_t run) {   // 10 runs of `run` rows, each followed by a one-row gap
        Rows rows;
        for (uint64_t k = 0; k < 10; ++k) {
            for (uint64_t j = 0; j < run; ++j) rows.emplace_back(Jan1(kYear) + k * (run + 1) + j, MixedRow(k * (run + 1) + j, 40));
        }
        return rows;
    };
    const Rows rows16 = runs_of(16), rows15 = runs_of(15);
    const auto want16 = RowsOnlyReferenceBytes(store, ref16, rows16);
    const auto want15 = RowsOnlyReferenceBytes(store, ref15, rows15);
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/16});
    CreateTestDb(store, db16, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, db15, XStore_Proto::SECOND, kRowLength);
    HoldPasses();
    const uint64_t rereads = XStore::InlineRereadFoldsForTests();
    ASSERT_TRUE(InsertAsync(store, db15, rows15));
    const auto map15 = AppliedMap(Key(db15), rows15.size());
    ASSERT_NE(map15, nullptr) << "the 15-row-run record was not applied";
    EXPECT_TRUE(WaitFor([&] { return map15->PendingBlockCount() == 0; }));
    EXPECT_EQ(map15->PendingBlockCount(), 0u) << "runs of 15 (< min_run_rows 16) were handed to the pool";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), rereads) << "the 15-row-run record was re-read";
    ASSERT_TRUE(InsertAsync(store, db16, rows16));
    EXPECT_TRUE(HandedOff(rows16.size())) << "runs of exactly min_run_rows were not handed to the pool";
    const auto map16 = Map(db16);
    ASSERT_NE(map16, nullptr);
    EXPECT_GT(map16->PendingBlockCount(), 0u) << "runs of exactly min_run_rows were not handed to the pool";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(map16->PendingBlockCount(), 0u);
    const auto got16 = CleanBytes(db16, Width(db16));
    const auto got15 = CleanBytes(db15, Width(db15));
    EXPECT_TRUE(got16.first == want16.first) << "runs of 16: .zmap differs from the rows-only build";
    EXPECT_TRUE(got16.second == want16.second) << "runs of 16: .catidx differs from the rows-only build";
    EXPECT_TRUE(got15.first == want15.first) << "runs of 15: .zmap differs from the rows-only build";
    EXPECT_TRUE(got15.second == want15.second) << "runs of 15: .catidx differs from the rows-only build";
}

// ---------------------------------------------------------------------------------------------------------------------
// IndexerQueryTest: queries while the indexer is behind (Task 5: items A, B, C, H of the stage 2 carry-forward list).

constexpr const char *kPendingNotice = "not indexed yet";

size_t CountOf(const std::string &s, const std::string &what) {
    size_t n = 0;
    for (size_t at = s.find(what); at != std::string::npos; at = s.find(what, at + 1)) ++n;
    return n;
}

class IndexerQueryTest : public IndexerWriteTest {
protected:
    static constexpr double kTarget = 7.0;   // the FindByValue value of column 1
    std::atomic<int> held_{0};

    // One worker, held inside a pass over a row of another DB: every later hint waits in the queue, so the rows stay
    // pending and no pass holds a READ lock over them (a held pass over a row blocks a writer that overwrites it).
    void PlugThePool(XStore &store, const std::string &plug_db) {
        xstore::Indexer::SetConfigForTests({1, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
        released_.store(false);
        xstore::Indexer::SetPassDelayForTests([this] {
            ++held_;
            while (!released_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
        CreateTestDb(store, plug_db, XStore_Proto::SECOND, kRowLength);
        Insert(store, plug_db, Dense(0, 1, 40));
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (held_.load() == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_EQ(held_.load(), 1) << "the worker did not take the plug's hint";
    }

    static unsigned long long Ts(unsigned short year, uint64_t row) { return Jan1(year) + row; }
    // Small integral values: sums and sums of squares are exact in any fold order.
    static XStore_Proto::RowData Row(double a, double b) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(a);
        row.add_fielddata()->set_doubledata(b);
        return row;
    }
    static double AOf(uint64_t i) { return static_cast<double>((i * 37) % 1000); }
    static double BOf(uint64_t i) { return static_cast<double>(i % 50); }
    static XStore_Proto::RowData WideRow(uint64_t r, int columns) {
        XStore_Proto::RowData row;
        for (int c = 0; c < columns; ++c) row.add_fielddata()->set_doubledata(static_cast<double>((r + c) % 1000));
        return row;
    }
    static void InsertInChunks(XStore &store, const std::string &db, const Rows &rows) {
        for (size_t at = 0; at < rows.size(); at += 4000) {
            Insert(store, db, Rows(rows.begin() + at, rows.begin() + std::min(rows.size(), at + 4000)));
        }
    }

    struct Found {
        std::string status;
        std::vector<std::pair<unsigned long long, std::string>> rows;   // (timestamp, serialized row), ascending
    };
    static Found Find(XStore &store, const std::string &db, unsigned long long lo, unsigned long long hi, int32_t column,
                      double value) {
        std::string buf;
        store.FindByValue(lo, hi, db, column, FDouble(value), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [&](std::string &&chunk, bool) { buf += chunk; });
        XStore_Proto::FindByValue_REP rep;
        EXPECT_TRUE(rep.ParseFromString(buf));
        Found f;
        f.status = rep.querystatus();
        for (const auto &e : rep.queryresponse()) f.rows.emplace_back(e.timestamp(), e.data().SerializeAsString());
        std::sort(f.rows.begin(), f.rows.end());
        return f;
    }
    static std::vector<unsigned long long> Timestamps(const Found &f) {
        std::vector<unsigned long long> ts;
        for (const auto &r : f.rows) ts.push_back(r.first);
        return ts;
    }

    // `got` (from the DB under test) equals `want` (the same final rows in a DB indexed synchronously, nothing pending)
    // except for the pending notices `got` appends to its status.
    template <class Rep>
    static void ExpectSame(const char *op, const Rep &got, const Rep &want, size_t notices) {
        EXPECT_EQ(CountOf(got.opstatus(), kPendingNotice), notices) << op << ": " << got.opstatus();
        EXPECT_EQ(got.opstatus().rfind(want.opstatus(), 0), 0u) << op << ": " << got.opstatus() << " vs " << want.opstatus();
        Rep a = got, b = want;
        a.clear_opstatus();
        b.clear_opstatus();
        EXPECT_EQ(a.SerializeAsString(), b.SerializeAsString()) << op;
    }
    // Variance and stddev: a scanned block and a block read from the zone map may round differently.
    template <class Rep>
    static void ExpectClose(const char *op, const Rep &got, const Rep &want, size_t notices) {
        EXPECT_EQ(CountOf(got.opstatus(), kPendingNotice), notices) << op << ": " << got.opstatus();
        EXPECT_EQ(got.opstatus().rfind(want.opstatus(), 0), 0u) << op << ": " << got.opstatus() << " vs " << want.opstatus();
        EXPECT_NEAR(got.opresult(), want.opresult(), 1e-9 * std::max(1.0, std::fabs(want.opresult()))) << op;
    }
    // Every aggregate that reads the zone map, and FindByValue, over [lo, hi] of `db` against `ref_db`; `notices` is the
    // number of pending years the range touches (FindByValue says it once per call).
    static void ExpectSameAnswers(XStore &store, const std::string &db, const std::string &ref_db, unsigned long long lo,
                                  unsigned long long hi, size_t notices) {
        for (unsigned short c : {0, 1}) {
            SCOPED_TRACE("column " + std::to_string(c) + " [" + std::to_string(lo) + ", " + std::to_string(hi) + "]");
            {
                XStore_Proto::Min_REP g, w;
                store.MinAggregate(lo, hi, db, c, &g);
                store.MinAggregate(lo, hi, ref_db, c, &w);
                ExpectSame("MIN", g, w, notices);
            }
            {
                XStore_Proto::Max_REP g, w;
                store.MaxAggregate(lo, hi, db, c, &g);
                store.MaxAggregate(lo, hi, ref_db, c, &w);
                ExpectSame("MAX", g, w, notices);
            }
            {
                XStore_Proto::Sum_REP g, w;
                store.SumAggregate(lo, hi, db, c, &g);
                store.SumAggregate(lo, hi, ref_db, c, &w);
                ExpectSame("SUM", g, w, notices);
            }
            {
                XStore_Proto::Avg_REP g, w;
                store.AvgAggregate(lo, hi, db, c, &g);
                store.AvgAggregate(lo, hi, ref_db, c, &w);
                ExpectSame("AVG", g, w, notices);
            }
            {
                XStore_Proto::Exists_REP g, w;
                store.ExistsAggregate(lo, hi, db, c, &g);
                store.ExistsAggregate(lo, hi, ref_db, c, &w);
                ExpectSame("EXISTS", g, w, notices);
            }
            {
                XStore_Proto::Ohlc_REP g, w;
                store.OhlcAggregate(lo, hi, db, c, &g);
                store.OhlcAggregate(lo, hi, ref_db, c, &w);
                ExpectSame("OHLC", g, w, notices);
            }
            {
                XStore_Proto::Variance_REP g, w;
                store.VarianceAggregate(lo, hi, db, c, &g);
                store.VarianceAggregate(lo, hi, ref_db, c, &w);
                ExpectClose("VARIANCE", g, w, notices);
            }
            {
                XStore_Proto::Stddev_REP g, w;
                store.StddevAggregate(lo, hi, db, c, &g);
                store.StddevAggregate(lo, hi, ref_db, c, &w);
                ExpectClose("STDDEV", g, w, notices);
            }
        }
        const Found got = Find(store, db, lo, hi, 1, kTarget);
        const Found want = Find(store, ref_db, lo, hi, 1, kTarget);
        EXPECT_EQ(got.rows, want.rows) << "FINDBYVALUE";
        EXPECT_EQ(CountOf(got.status, kPendingNotice), notices == 0 ? 0u : 1u) << got.status;
    }
};

// (B) A block with pending rows reads as inexact; it reads as before once they are cleared.
TEST_F(IndexerQueryTest, ABlockWithPendingRowsReadsAsInexact) {
    const std::string db = "IDXQINEXACT";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 3000, 40));
    const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, Width(db));
    ASSERT_NE(z->Pending(), nullptr);
    EXPECT_FALSE(z->BlockInexact(1));
    z->Pending()->MarkRows(2100, 1);
    EXPECT_TRUE(z->BlockInexact(1)) << "a pending block's entries describe rows not folded yet";
    EXPECT_FALSE(z->BlockInexact(0));
    z->Pending()->ClearRows(2100, 1);
    EXPECT_FALSE(z->BlockInexact(1));
}

// (A) A bitmapped year with a pending row holding the value: FindByValue must not answer from the bitmap directory (a
// build-time snapshot a pending row is not in, and which only a fold invalidates).
TEST_F(IndexerQueryTest, TheBitmapDirectoryIsNotUsedWhileARowIsPending) {
    const std::string db = "IDXQBITMAP";
    const uint64_t rpb = RowsPerBlock(kRowLength);
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t i = 0; i < 1000; ++i) rows.emplace_back(Ts(kYear, i), Row(AOf(i), BOf(i)));
    InsertAndIndex(store, db, rows);
    // The bitmap build reads every row of the year file: cut it to two blocks (all rows and queries stay below the cut).
    std::filesystem::resize_file(YearPath(db, ".XSTORE"), 2 * rpb * kRowLength);
    const uint32_t width = Width(db);
    ASSERT_TRUE(store.BuildOneYearBitmapSection(DbPath(db), kYear, kRowLength, width));
    const CatIdxFile::Pin catidx = XStore::PinCatIdx(DbPath(db), kYear, kRowLength, width);
    ASSERT_TRUE(catidx->HasBitmapSection());
    ASSERT_TRUE(catidx->LookupExact(1, EncodeFDataForCatIdx(FDouble(kTarget))).has_value());
    {
        // Only this year's pending rows count: a pending row of another year leaves the directory in use.
        const auto other = xstore::PendingMaps::For(DbPath(db) + "/1999", RowsPerBlock(kRowLength));
        other->MarkRows(0, 1);
        EXPECT_TRUE(catidx->LookupExact(1, EncodeFDataForCatIdx(FDouble(kTarget))).has_value());
        other->ClearRows(0, 1);
    }

    HoldPasses();
    // A new row in block 1 and an overwrite in block 0, both holding the target value and both pending.
    Insert(store, db, {{Ts(kYear, 20), Row(1.0, kTarget)}, {Ts(kYear, rpb + 5), Row(1.0, kTarget)}});
    ASSERT_GT(Map(db)->PendingBlockCount(), 0u);
    ASSERT_TRUE(catidx->HasBitmapSection()) << "nothing was folded, so nothing invalidated the directory";
    EXPECT_FALSE(catidx->LookupExact(1, EncodeFDataForCatIdx(FDouble(kTarget))).has_value())
        << "the directory answered while the year has a pending row";

    std::vector<unsigned long long> expected;   // the scan oracle: the rows holding the value now
    for (uint64_t i = 0; i < 1000; ++i) {
        if (i == 20 || BOf(i) == kTarget) expected.push_back(Ts(kYear, i));
    }
    expected.push_back(Ts(kYear, rpb + 5));
    EXPECT_EQ(Timestamps(Find(store, db, Ts(kYear, 0), Ts(kYear, 2 * rpb - 1), 1, kTarget)), expected);
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(Timestamps(Find(store, db, Ts(kYear, 0), Ts(kYear, 2 * rpb - 1), 1, kTarget)), expected);
}

// (C, H) Rows the indexer has not folded yet are overwritten and deleted while its one worker is held (PlugThePool). Every aggregate and
// FindByValue equals the same final rows indexed synchronously, during the hold (one notice per pending year) and after
// the indexer caught up (no notice). Afterwards: block 0 (a FOLDED row overwritten) is inexact; blocks 1 and 2 (only
// PENDING rows overwritten or deleted, at or above the block's fold_end) hold each final row folded once, i.e. the zone
// map of the final data; the bloom bits are a superset of the final data's (block 0 keeps the overwritten values).
TEST_F(IndexerQueryTest, PendingRowsOverwrittenOrDeletedAreAnsweredExactly) {
    const std::string db = "IDXQOVR", ref_db = "IDXQOVRREF";
    constexpr unsigned short kYear2 = 1972;
    XStore store;
    std::map<unsigned long long, std::pair<double, double>> model;   // the final rows
    Rows seed, behind, overwrites;
    for (uint64_t i = 0; i < 3000; ++i) seed.emplace_back(Ts(kYear, i), Row(AOf(i), BOf(i)));     // folded first
    for (uint64_t i = 3000; i < 5000; ++i) behind.emplace_back(Ts(kYear, i), Row(AOf(i), BOf(i)));
    for (uint64_t i = 0; i < 500; ++i) behind.emplace_back(Ts(kYear2, i), Row(AOf(i), BOf(i)));
    for (uint64_t i = 100; i < 110; ++i) overwrites.emplace_back(Ts(kYear, i), Row(-5.0, kTarget));            // folded
    for (uint64_t i = 3500; i < 3510; ++i) overwrites.emplace_back(Ts(kYear, i), Row(2000.0 + i % 10, kTarget));   // pending
    for (const Rows *part : {&seed, &behind, &overwrites}) {
        for (const auto &[ts, row] : *part) model[ts] = {row.fielddata(0).doubledata(), row.fielddata(1).doubledata()};
    }
    const unsigned long long del_lo = Ts(kYear, 4200), del_hi = Ts(kYear, 4209), del_one = Ts(kYear, 4500);
    for (unsigned long long ts = del_lo; ts <= del_hi; ++ts) model.erase(ts);
    model.erase(del_one);
    Rows final_rows;
    for (const auto &[ts, v] : model) final_rows.emplace_back(ts, Row(v.first, v.second));
    static_cast<void>(ReferenceBytes(store, ref_db, final_rows));

    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, seed);
    ASSERT_NO_FATAL_FAILURE(PlugThePool(store, "IDXQOVRPLUG"));
    Insert(store, db, behind);
    Insert(store, db, overwrites);
    const XStore_Proto::RangeDelete_REP range_del = store.RangeDelete(del_lo, del_hi, db);
    ASSERT_NE(range_del.deletestatus().find("[INFO]"), std::string::npos) << range_del.deletestatus();
    const XStore_Proto::UnaryDelete_REP unary_del = store.UnaryDelete(del_one, db);
    ASSERT_NE(unary_del.deletestatus().find("[INFO]"), std::string::npos) << unary_del.deletestatus();
    const auto map = Map(db);
    const auto map2 = xstore::PendingMaps::Find(DbPath(db) + "/" + std::to_string(kYear2));
    ASSERT_NE(map, nullptr);
    ASSERT_NE(map2, nullptr);
    ASSERT_EQ(map->PendingBlockCount(), 3u) << "every block of the year must be pending during the hold";
    ASSERT_GT(map2->PendingBlockCount(), 0u);

    {
        SCOPED_TRACE("during the hold");
        ExpectSameAnswers(store, db, ref_db, Ts(kYear, 0), Ts(kYear2, 499), 2);   // two pending years: two notices
        ExpectSameAnswers(store, db, ref_db, Ts(kYear, 0), Ts(kYear, 4999), 1);
        ExpectSameAnswers(store, db, ref_db, Ts(kYear, 3400), Ts(kYear, 4600), 1);
        // The model, independently of the reference: the overwritten pending rows hold the maximum.
        double max_a = -std::numeric_limits<double>::infinity();
        std::vector<unsigned long long> target_rows;
        for (const auto &[ts, v] : model) {
            if (ts > Ts(kYear, 4999)) continue;
            max_a = std::max(max_a, v.first);
            if (v.second == kTarget) target_rows.push_back(ts);
        }
        XStore_Proto::Max_REP max_rep;
        store.MaxAggregate(Ts(kYear, 0), Ts(kYear, 4999), db, 0, &max_rep);
        EXPECT_EQ(max_rep.opresult().doubledata(), max_a);
        EXPECT_EQ(Timestamps(Find(store, db, Ts(kYear, 0), Ts(kYear, 4999), 1, kTarget)), target_rows);
    }
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    EXPECT_EQ(map2->PendingBlockCount(), 0u);
    {
        SCOPED_TRACE("after the indexer caught up");
        ExpectSameAnswers(store, db, ref_db, Ts(kYear, 0), Ts(kYear2, 499), 0);
        ExpectSameAnswers(store, db, ref_db, Ts(kYear, 3400), Ts(kYear, 4600), 0);
    }

    const uint32_t width = Width(db);
    ASSERT_EQ(width, Width(ref_db));
    {
        const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
        const ZoneMapFile::Pin zr = XStore::PinZoneMap(DbPath(ref_db), kYear, kRowLength, width);
        EXPECT_TRUE(z->BlockInexact(0)) << "a folded row was overwritten";
        for (uint64_t b : {1, 2}) {
            SCOPED_TRACE("block " + std::to_string(b));
            EXPECT_FALSE(z->BlockInexact(b));
            const auto got = z->TryGetBlockStats(b);
            const auto want = zr->TryGetBlockStats(b);
            ASSERT_TRUE(got.has_value() && want.has_value());
            ASSERT_EQ(got->size(), want->size());
            EXPECT_EQ(std::memcmp(got->data(), want->data(), got->size() * sizeof(ZoneMapEntry)), 0)
                << "not the zone map of the final data";
        }
    }
    const std::string got_c = ReadFileBytes(YearPath(db, ".catidx"));
    const std::string ref_c = ReadFileBytes(YearPath(ref_db, ".catidx"));
    ASSERT_EQ(got_c.size(), ref_c.size());
    size_t missing = 0;
    for (size_t i = sizeof(CatIdxFileHeader); i < ref_c.size(); ++i) {
        if ((got_c[i] & ref_c[i]) != ref_c[i]) ++missing;
    }
    EXPECT_EQ(missing, 0u) << "a bloom bit of the final data is missing";
}

// Pool off: a batch, an overwrite of 500 of its rows with other values, then 20 UNARY inserts, all observation records
// (none read back). The oracle is the same calls through the rows-only inline fold (1-byte backlog): an overwrite marks
// its block inexact on both paths, so a build of the final rows alone would not match. Same bytes, same answers.
TEST_F(IndexerQueryTest, ObservationRecordsOverwriteAndUnaryInsertMatchTheSynchronousBuild) {
    const std::string db = "IDXQOBSOVR", ref_db = "IDXQOBSOVRREF";
    XStore store;
    const auto ops = [&](const std::string &name) {
        CreateTestDb(store, name, XStore_Proto::SECOND, kRowLength);
        Rows seed, overwrites;
        for (uint64_t i = 0; i < 3000; ++i) seed.emplace_back(Ts(kYear, i), Row(AOf(i), BOf(i)));
        for (uint64_t i = 1000; i < 1500; ++i) overwrites.emplace_back(Ts(kYear, i), Row(-5.0 - static_cast<double>(i % 7), kTarget));
        Insert(store, name, seed);
        Insert(store, name, overwrites);
        for (uint64_t i = 3000; i < 3020; ++i) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(Ts(kYear, i), Row(AOf(i), BOf(i)), name, &rep, XStore_Proto::SYNC);
            ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
        }
    };
    xstore::Indexer::SetConfigForTests({2, 1, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    const uint64_t ref_before = XStore::InlineRereadFoldsForTests();
    ops(ref_db);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_GT(XStore::InlineRereadFoldsForTests(), ref_before) << "the reference did not take the rows-only fold";
    xstore::Indexer::SetConfigForTests({2, 0});
    const uint64_t before = XStore::InlineRereadFoldsForTests();
    ops(db);
    ASSERT_FALSE(HasFatalFailure());
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), before) << "the writer re-read rows to fold them with the pool off";
    const uint32_t width = Width(db);
    ASSERT_EQ(width, Width(ref_db));
    const auto ref = CleanBytes(ref_db, width);
    const auto got = CleanBytes(db, width);
    ASSERT_NE(ref.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos) << "an empty zone map proves nothing";
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build";
    {
        const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
        EXPECT_TRUE(z->BlockInexact(0)) << "the overwrite of folded rows was not seen";
    }
    ExpectSameAnswers(store, db, ref_db, Ts(kYear, 0), Ts(kYear, 3019), 0);
}

// Both record kinds on one block while its handed-off rows are still pending (production predicate): an ASYNC dense run
// (rows 0..999 of block 0, handed off, its hints queued behind a plugged pool, so no pass holds a READ lock on them),
// then SYNC (has_obs) records: an overwrite of the still-pending row 500 and new rows 1500..1599 of the same block. The
// has_obs apply folds its rows and clears their bits, so the pass folds every other pending row once. The block had
// nothing folded when row 500 was overwritten (no overwrite check, not inexact), and 1500.. lie above everything folded,
// so the result is the zone map of the final data: equal byte for byte to the rows-only build of the final rows (small
// integral values: exact in any fold order).
TEST_F(IndexerQueryTest, HasObsRecordsOnABlockWithPendingHandedOffRowsEqualTheRowsOnlyBuild) {
    const std::string db = "IDXQMIXPEND", ref_db = "IDXQMIXPENDREF", plug_db = "IDXQMIXPENDPLUG";
    XStore store;
    Rows dense, overwrite, more;
    for (uint64_t i = 0; i < 1000; ++i) dense.emplace_back(Ts(kYear, i), Row(AOf(i), BOf(i)));
    overwrite.emplace_back(Ts(kYear, 500), Row(-7.0, kTarget));
    for (uint64_t i = 1500; i < 1600; ++i) more.emplace_back(Ts(kYear, i), Row(AOf(i), BOf(i)));
    Rows final_rows = dense;
    final_rows[500] = overwrite[0];
    final_rows.insert(final_rows.end(), more.begin(), more.end());
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, final_rows);

    // A plug with the production predicate (PlugThePool's config hands SYNC records off): one worker, held in the
    // plug's pass, so the hints below wait in the queue.
    xstore::Indexer::SetConfigForTests({1, kPoolBacklog, /*min_run_rows=*/16});
    released_.store(false);
    xstore::Indexer::SetPassDelayForTests([this] {
        ++held_;
        while (!released_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    CreateTestDb(store, plug_db, XStore_Proto::SECOND, kRowLength);
    ASSERT_TRUE(InsertAsync(store, plug_db, Dense(0, 100, 40)));
    ASSERT_TRUE(WaitFor([this] { return held_.load() == 1; })) << "the worker did not take the plug's hint";

    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    ASSERT_TRUE(InsertAsync(store, db, dense));
    ASSERT_TRUE(HandedOff(100 + dense.size())) << "the dense ASYNC run was not handed to the pool";
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    const uint64_t rereads = XStore::InlineRereadFoldsForTests();
    Insert(store, db, overwrite);   // SYNC: has_obs, applied when the call returns
    Insert(store, db, more);
    EXPECT_EQ(XStore::InlineRereadFoldsForTests(), rereads) << "a SYNC record was re-read instead of folded from its observations";
    EXPECT_EQ(map->TotalMarkedRowsForTests(), dense.size() + 1 + more.size());
    // The has_obs writes happened while the handed-off rows were pending: exactly rows 0..499 and 501..999 still are.
    ASSERT_GT(map->PendingBlockCount(), 0u) << "the handed-off rows were folded before the has_obs writes";
    const std::vector<xstore::IndexRun> pending = map->PendingRunsOfBlock(0);
    ASSERT_EQ(pending.size(), 2u);
    EXPECT_EQ(pending[0].first_row, 0u);
    EXPECT_EQ(pending[0].row_count, 500u);
    EXPECT_EQ(pending[1].first_row, 501u);
    EXPECT_EQ(pending[1].row_count, 499u);
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the rows-only build of the final rows";
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the rows-only build of the final rows";
    {
        const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, Width(db));
        EXPECT_FALSE(z->BlockInexact(0)) << "nothing folded was overwritten";
    }
    ExpectSameAnswers(store, db, ref_db, Ts(kYear, 0), Ts(kYear, 1599), 0);
}

// (H) With the pool off no reply carries the notice, even with a block marked; Start turns it on, StopAndDrain off.
TEST_F(IndexerQueryTest, TheNoticeFollowsThePool) {
    const std::string db = "IDXQPOOL";
    XStore store;
    xstore::Indexer::SetConfigForTests({2, 0});
    EXPECT_FALSE(xstore::PendingMaps::NoticesEnabled());
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, Dense(0, 100, 40));
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    map->MarkRows(5, 1);
    const unsigned long long lo = Ts(kYear, 0), hi = Ts(kYear, 99);
    auto notices = [&] {
        XStore_Proto::Max_REP max_rep;
        store.MaxAggregate(lo, hi, db, 0, &max_rep);
        return std::make_pair(CountOf(max_rep.opstatus(), kPendingNotice),
                              CountOf(Find(store, db, lo, hi, 0, 1.0).status, kPendingNotice));
    };
    EXPECT_EQ(notices(), std::make_pair(size_t{0}, size_t{0})) << "pool off";
    xstore::Indexer::SetConfigForTests({1, kPoolBacklog});
    EXPECT_TRUE(xstore::PendingMaps::NoticesEnabled());
    EXPECT_EQ(notices(), std::make_pair(size_t{1}, size_t{1})) << "pool on";
    xstore::Indexer::Instance().StopAndDrain(std::chrono::seconds(10));
    EXPECT_FALSE(xstore::PendingMaps::NoticesEnabled());
    EXPECT_EQ(notices(), std::make_pair(size_t{0}, size_t{0})) << "pool stopped";
    map->ClearRows(5, 1);
}

// (H) The pending notice joins the unindexed-column notice and the live-widen notice instead of replacing them.
TEST_F(IndexerQueryTest, ThePendingNoticeJoinsTheColumnNotices) {
    const std::string db = "IDXQJOIN";
    constexpr uint64_t kRows = 20000;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, {{Ts(kYear, 0), WideRow(0, 10)}});   // the index files are born 64 columns wide
    Rows rows;
    for (uint64_t r = 1; r < kRows; ++r) rows.emplace_back(Ts(kYear, r), WideRow(r, 70));
    InsertInChunks(store, db, rows);
    ASSERT_TRUE(Quiesce());
    const std::string path = DbPath(db);
    ASSERT_EQ(Width(db), 64u);
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    const unsigned long long lo = Ts(kYear, 0), hi = Ts(kYear, kRows - 1);

    XStore_Proto::Max_REP alone;   // nothing pending: the column notice alone
    store.MaxAggregate(lo, hi, db, 65, &alone);
    EXPECT_EQ(CountOf(alone.opstatus(), kPendingNotice), 0u) << alone.opstatus();
    EXPECT_EQ(CountOf(alone.opstatus(), "is not yet indexed in"), 1u) << alone.opstatus();

    map->MarkRows(5, 1);   // as a handed-off run leaves it
    XStore_Proto::Max_REP unindexed;
    store.MaxAggregate(lo, hi, db, 65, &unindexed);
    EXPECT_EQ(CountOf(unindexed.opstatus(), kPendingNotice), 1u) << unindexed.opstatus();
    EXPECT_EQ(CountOf(unindexed.opstatus(), "is not yet indexed in"), 1u) << unindexed.opstatus();
    map->ClearRows(5, 1);

    // A live widen held mid-way (as XStoreLiveWidenTest does: a WRITE lock on the row past the data).
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    const auto growth = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, FileRangeLock::LockType::WRITE);
    auto pass = std::async(std::launch::async, [&] {
        XStore builder;
        return builder.RunLiveWidenPass(growth + std::chrono::seconds(30));
    });
    bool widening = false;
    for (const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
         !widening && std::chrono::steady_clock::now() < deadline;) {
        XStore::LiveRebuildProgress p;
        widening = XStore::FindRebuildProgress(path, kYear, /*catidx=*/false, p) && p.done_rows >= kRows;
        if (!widening) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(widening) << "the live widen never reached the held row";   // no ASSERT while the lock is held
    map->MarkRows(5, 1);
    XStore_Proto::Max_REP widen;
    store.MaxAggregate(lo, hi, db, 65, &widen);
    map->ClearRows(5, 1);
    lock.UnlockRange(kRows * kRowLength, kRowLength, FileRangeLock::LockType::WRITE);
    ASSERT_EQ(pass.wait_for(std::chrono::seconds(60)), std::future_status::ready);
    static_cast<void>(pass.get());
    XStore::ClearLiveWidenRequestsForTests();
    EXPECT_EQ(CountOf(widen.opstatus(), kPendingNotice), 1u) << widen.opstatus();
    EXPECT_EQ(CountOf(widen.opstatus(), "is not fully indexed"), 1u) << widen.opstatus();
}

// (H) 4 writers insert while 4 readers loop MaxAggregate, SumAggregate and FindByValue, the pool running normally.
// Oracle: a row is written once and its values are a function of its index, so the visible rows only grow; a sound
// answer contains every row acked before the query began and only rows submitted before it ended. MAX: at least the
// largest value of the acked rows, and the value of a submitted row; SUM (values >= 0): between the sums of those two
// sets; FINDBYVALUE: a set between those two sets. Each check reads the writers' per-row states before (acked) and
// after (submitted) the query; rows are read under the scan's READ locks, so no row is torn. After the writers stop
// and the indexer catches up both answers are exact and carry no notice.
TEST_F(IndexerQueryTest, QueriesDuringInsertsMatchAScanOracle) {
    const std::string db = "IDXQLIVE";
    constexpr uint64_t kWriters = 4, kReaders = 4, kPerWriter = 100000, kBatch = 100;
    constexpr uint64_t kRows = kWriters * kPerWriter;
    constexpr double kFindValue = 5.0;
    const auto a_of = [](uint64_t i) { return static_cast<double>((i * 2654435761ULL) % 1000003); };
    const auto b_of = [](uint64_t i) { return static_cast<double>(i % 64); };
    XStore store;
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    walring::Checkpointer::Instance().SetPausedForTests(false);   // see ConcurrentScatteredWritersEqualTheSynchronousBuild
    walring::Checkpointer::Instance().Start();
    std::vector<std::atomic<uint8_t>> state(kRows);   // 0 not sent, 1 sent, 2 acked
    std::atomic<bool> writing{true};
    std::atomic<uint64_t> queries{0}, noticed{0}, violations{0};
    std::mutex first_mutex;
    std::string first_violation;
    auto violate = [&](const std::string &what) {
        if (violations.fetch_add(1) == 0) {
            std::lock_guard<std::mutex> lock(first_mutex);
            first_violation = what;
        }
    };
    const auto stop_at = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    // An indexer that lags (as under load): 3 ms per pass. Unslowed, it folds a run before its SYNC ack returns, and the
    // oracle would never see an acked row in a pending block.
    xstore::Indexer::SetPassDelayForTests([] { std::this_thread::sleep_for(std::chrono::milliseconds(3)); });

    std::vector<std::thread> writers;
    for (uint64_t w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            std::vector<uint64_t> batches;   // first rows of this writer's batches, in a shuffled order
            for (uint64_t at = w * kPerWriter; at < (w + 1) * kPerWriter; at += kBatch) batches.push_back(at);
            std::mt19937_64 rng(w + 1);
            std::shuffle(batches.begin(), batches.end(), rng);
            for (uint64_t first : batches) {
                if (std::chrono::steady_clock::now() >= stop_at) break;
                XStore_Proto::BatchInsert_REQ req;
                req.set_dbname(db);
                req.set_durabilitymode(XStore_Proto::SYNC);
                for (uint64_t i = first; i < first + kBatch; ++i) {
                    XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                    entry->set_timestamp(Ts(kYear, i));
                    *entry->mutable_data() = Row(a_of(i), b_of(i));
                    state[i].store(1, std::memory_order_release);
                }
                std::string status;
                for (int attempt = 0; attempt < 10000; ++attempt) {
                    XStore_Proto::BatchInsert_REP rep;
                    store.BatchInsert(&req, &rep);
                    status = rep.insertstatus();
                    if (status.find(XStore::kWalOverloadedText) == std::string::npos) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (status.find("successfully") == std::string::npos) {
                    violate("insert failed: " + status);
                    return;
                }
                for (uint64_t i = first; i < first + kBatch; ++i) state[i].store(2, std::memory_order_release);
            }
        });
    }
    std::vector<std::thread> readers;
    for (uint64_t r = 0; r < kReaders; ++r) {
        readers.emplace_back([&, r] {
            std::mt19937_64 rng(100 + r);
            std::vector<char> acked, sent;
            for (uint64_t n = 0; writing.load(); ++n) {
                uint64_t lo = rng() % kRows, hi = rng() % kRows;
                if (n % 8 == 0) { lo = 0; hi = kRows - 1; }
                if (lo > hi) std::swap(lo, hi);
                acked.assign(hi - lo + 1, 0);
                sent.assign(hi - lo + 1, 0);
                for (uint64_t i = lo; i <= hi; ++i) acked[i - lo] = state[i].load(std::memory_order_acquire) == 2;
                std::string status;
                std::vector<unsigned long long> found;
                double value = 0;
                bool value_ok = false;
                const uint64_t kind = (n + r) % 3;   // 0 MAX, 1 SUM, 2 FINDBYVALUE
                const bool use_max = kind == 0;
                if (kind == 0) {
                    XStore_Proto::Max_REP rep;
                    store.MaxAggregate(Ts(kYear, lo), Ts(kYear, hi), db, 0, &rep);
                    status = rep.opstatus();
                    value_ok = status.rfind("SUCCESS", 0) == 0;
                    value = rep.opresult().doubledata();
                } else if (kind == 1) {
                    XStore_Proto::Sum_REP rep;
                    store.SumAggregate(Ts(kYear, lo), Ts(kYear, hi), db, 0, &rep);
                    status = rep.opstatus();
                    value_ok = status.rfind("SUCCESS", 0) == 0;
                    value = rep.opresult();
                } else {
                    const Found f = Find(store, db, Ts(kYear, lo), Ts(kYear, hi), 1, kFindValue);
                    status = f.status;
                    found = Timestamps(f);
                }
                for (uint64_t i = lo; i <= hi; ++i) sent[i - lo] = state[i].load(std::memory_order_acquire) >= 1;
                queries.fetch_add(1);
                if (CountOf(status, kPendingNotice) != 0) noticed.fetch_add(1);
                const std::string where = std::string(kind == 0 ? "MAX [" : kind == 1 ? "SUM [" : "FINDBYVALUE [") +
                                          std::to_string(lo) + ", " + std::to_string(hi) + "]: ";
                if (use_max) {
                    bool any_acked = false, is_sent_value = false;
                    double lower = -std::numeric_limits<double>::infinity();
                    for (uint64_t i = lo; i <= hi; ++i) {
                        if (acked[i - lo]) { any_acked = true; lower = std::max(lower, a_of(i)); }
                        if (sent[i - lo] && a_of(i) == value) is_sent_value = true;
                    }
                    if (!value_ok) {
                        if (any_acked) violate(where + "no data although rows were acked: " + status);
                    } else if (value < lower) {
                        violate(where + std::to_string(value) + " < acked max " + std::to_string(lower));
                    } else if (!is_sent_value) {
                        violate(where + std::to_string(value) + " is no submitted row's value");
                    }
                } else if (kind == 1) {
                    // Values are non-negative integers below 2^20 and at most 400,000 rows: every sum is exact.
                    bool any_acked = false;
                    double lower = 0, upper = 0;
                    for (uint64_t i = lo; i <= hi; ++i) {
                        if (acked[i - lo]) { any_acked = true; lower += a_of(i); }
                        if (sent[i - lo]) upper += a_of(i);
                    }
                    if (!value_ok) {
                        if (any_acked) violate(where + "no data although rows were acked: " + status);
                    } else if (value < lower || value > upper) {
                        violate(where + std::to_string(value) + " outside [" + std::to_string(lower) + ", " +
                                std::to_string(upper) + "]");
                    }
                } else {
                    std::set<unsigned long long> got(found.begin(), found.end());
                    if (got.size() != found.size()) violate(where + "a row returned twice");
                    for (unsigned long long ts : got) {
                        const uint64_t i = ts - Ts(kYear, 0);
                        if (i < lo || i > hi || !sent[i - lo] || b_of(i) != kFindValue) {
                            violate(where + "row " + std::to_string(i) + " is not a submitted match");
                        }
                    }
                    for (uint64_t i = lo; i <= hi; ++i) {
                        if (acked[i - lo] && b_of(i) == kFindValue && got.count(Ts(kYear, i)) == 0) {
                            violate(where + "acked row " + std::to_string(i) + " missing");
                        }
                    }
                }
            }
        });
    }
    for (std::thread &t : writers) t.join();
    writing.store(false);
    for (std::thread &t : readers) t.join();
    xstore::Indexer::SetPassDelayForTests(nullptr);
    std::printf("[ INFO     ] %llu queries, %llu with the pending notice\n",
                static_cast<unsigned long long>(queries.load()), static_cast<unsigned long long>(noticed.load()));
    EXPECT_EQ(violations.load(), 0u) << first_violation;
    EXPECT_GT(queries.load(), 0u);

    ASSERT_TRUE(Quiesce());
    double max_a = -std::numeric_limits<double>::infinity(), sum_a = 0;
    std::vector<unsigned long long> expected;
    for (uint64_t i = 0; i < kRows; ++i) {
        if (state[i].load() != 2) continue;
        max_a = std::max(max_a, a_of(i));
        sum_a += a_of(i);
        if (b_of(i) == kFindValue) expected.push_back(Ts(kYear, i));
    }
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Ts(kYear, 0), Ts(kYear, kRows - 1), db, 0, &max_rep);
    EXPECT_EQ(max_rep.opstatus(), "SUCCESS");
    EXPECT_EQ(max_rep.opresult().doubledata(), max_a);
    // Exact (integers, sum below 2^53): a row folded twice stays inside the [acked, sent] window above, not here.
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(Ts(kYear, 0), Ts(kYear, kRows - 1), db, 0, &sum_rep);
    EXPECT_EQ(sum_rep.opstatus(), "SUCCESS");
    EXPECT_EQ(sum_rep.opresult(), sum_a);
    const Found f = Find(store, db, Ts(kYear, 0), Ts(kYear, kRows - 1), 1, kFindValue);
    EXPECT_EQ(Timestamps(f), expected);
    EXPECT_EQ(CountOf(f.status, kPendingNotice), 0u) << f.status;
}

// ---------------------------------------------------------------------------------------------------------------------
// IndexerLifecycleTest: removal vs the indexer (Task 6 part A: the RemoveDB handshake, DropHints, pinned readers).

class IndexerLifecycleTest : public IndexerQueryTest {
protected:
    std::atomic<int> open_held_{0};
    std::atomic<bool> open_released_{true};

    void TearDown() override {
        open_released_.store(true);
        xstore::Indexer::SetPassOpenDelayForTests(nullptr);
        XStore::SetIndexerDrainBoundForTests(std::chrono::milliseconds(XStoreTunables::INDEXER_DRAIN_BOUND_MS));
        IndexerQueryTest::TearDown();
    }
    // The first pass to reach its opens waits there (key taken, removed-year check done) until ReleaseOpen().
    void HoldFirstOpen() {
        open_released_.store(false);
        xstore::Indexer::SetPassOpenDelayForTests([this] {
            if (open_held_.fetch_add(1) != 0) return;
            while (!open_released_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }
    void ReleaseOpen() { open_released_.store(true); }
    void WaitHeld(const std::atomic<int> &counter) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (counter.load() == 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_GT(counter.load(), 0) << "no pass reached the hook";
    }
    static std::string Removed(XStore &store, const std::string &db) { return store.RemoveDB(db).removestatus(); }

    // Every pass waits inside its READ lock, counted in held_, until ReleasePasses().
    void HoldCountedPasses() {
        released_.store(false);
        xstore::Indexer::SetPassDelayForTests([this] {
            ++held_;
            while (!released_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }
    // Shutdown's MarkClean needs a final checkpoint cycle (WalCheckpointSyncTest leaves the checkpointer paused).
    static void RunCheckpointer() {
        walring::Checkpointer::Instance().SetPausedForTests(false);
        walring::Checkpointer::Instance().Start();
    }
    std::string YearFile(const std::string &db, unsigned short year, const char *ext) const {
        return DbPath(db) + "/" + std::to_string(year) + ext;
    }
    uint32_t DirtyOf(const std::string &db, unsigned short year, const char *ext) const {
        return std::string(ext) == ".zmap" ? ReadHeader<ZoneMapFileHeader>(YearFile(db, year, ext)).dirty
                                           : ReadHeader<CatIdxFileHeader>(YearFile(db, year, ext)).dirty;
    }
    // Rewrites the on-disk zone map header's block size (a stale or corrupt header).
    static void SetRowsPerBlock(const std::string &zmap_path, uint64_t rows_per_block) {
        ZoneMapFileHeader h = ReadHeader<ZoneMapFileHeader>(zmap_path);
        h.rows_per_block = rows_per_block;
        const int fd = open(zmap_path.c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h)));
        close(fd);
    }
    template <class Header>
    static Header HeaderOf(const std::string &bytes) {
        Header h{};
        if (bytes.size() >= sizeof(h)) std::memcpy(&h, bytes.data(), sizeof(h));
        return h;
    }
    // Runs the DISABLED_ test `child_test` in a new process (it writes its scratch root to XSTORE_KILL_CHILD_OUT when
    // ready, then waits), SIGKILLs it once ready, and makes the child's directory this test's.
    void RunChildAndKillItWhenReady(const std::string &child_test) {
        const std::filesystem::path mine = scratch->test_root;
        const std::string out = (mine / "child_ready").string(), log = (mine / "child.log").string();
        std::vector<std::string> args = {"/proc/self/exe", "--gtest_filter=" + child_test, "--gtest_also_run_disabled_tests"};
        std::vector<std::string> env;
        for (char **e = environ; *e != nullptr; ++e) env.emplace_back(*e);
        env.push_back("XSTORE_KILL_CHILD_OUT=" + out);
        std::vector<char *> argv, envp;
        for (std::string &a : args) argv.push_back(a.data());
        for (std::string &e : env) envp.push_back(e.data());
        argv.push_back(nullptr);
        envp.push_back(nullptr);
        pid_t pid = fork();
        if (pid == 0) {
            const int fd = open(log.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd >= 0) {
                dup2(fd, 1);
                dup2(fd, 2);
            }
            execve(argv[0], argv.data(), envp.data());
            _exit(127);
        }
        ASSERT_GT(pid, 0);
        struct Reaper {
            pid_t &pid;
            ~Reaper() {
                if (pid > 0) {
                    kill(pid, SIGKILL);
                    waitpid(pid, nullptr, 0);
                }
            }
        } reaper{pid};
        bool ready = false;
        for (const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(120);
             !ready && std::chrono::steady_clock::now() < until;) {
            ready = std::filesystem::exists(out);
            if (!ready && waitpid(pid, nullptr, WNOHANG) == pid) {
                pid = -1;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_TRUE(ready) << "the child did not get ready; its log:\n" << ReadFileBytes(log);
        const std::string child_root = ReadFileBytes(out);
        ASSERT_EQ(kill(pid, SIGKILL), 0);
        int status = 0;
        ASSERT_EQ(waitpid(pid, &status, 0), pid);
        pid = -1;
        ASSERT_TRUE(WIFSIGNALED(status));
        // The child's directory becomes this test's (its ScratchDir removes it).
        scratch->test_root = child_root;
        std::filesystem::current_path(child_root);
        std::filesystem::remove_all(mine);
    }
    // The number of ring records the last start replayed, from its info lines (kLogCapture at info level).
    static uint64_t ReplayedRecords() {
        uint64_t replayed = 0;
        const std::string tag = "[WAL REPLAY] - Replayed ";
        for (const std::string &line : kLogCapture->Lines()) {
            const size_t at = line.find(tag);
            if (at != std::string::npos) replayed += std::strtoull(line.c_str() + at + tag.size(), nullptr, 10);
        }
        return replayed;
    }
};

// (a) RemoveDB while a pass of the DB runs (held) and more of its hints are queued: the removal waits for the pass, then
// returns; nothing stays reserved or registered, and the DB re-created under the name starts clean and indexes to the
// synchronous build's bytes.
TEST_F(IndexerLifecycleTest, RemoveDbWaitsForARunningPassAndTheRecreatedDbStartsClean) {
    const std::string db = "IDXLCA", ref_db = "IDXLCAREF";
    XStore store;
    const Rows rows = Dense(0, 6000, 40);   // three blocks: one pass held, two hints queued
    const auto ref = ReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({1, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    released_.store(false);
    xstore::Indexer::SetPassDelayForTests([this] {
        ++held_;
        while (!released_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    Insert(store, db, rows);
    ASSERT_NO_FATAL_FAILURE(WaitHeld(held_));
    ASSERT_GT(xstore::Indexer::Instance().BacklogBytes(), 0u);
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        return Removed(r, db);
    });
    EXPECT_EQ(remove.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout)
        << "the removal did not wait for the running pass";
    ReleasePasses();
    ASSERT_EQ(remove.wait_for(std::chrono::seconds(15)), std::future_status::ready) << "the removal did not return";
    EXPECT_NE(remove.get().find("Removed DB"), std::string::npos);
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), nullptr);
    EXPECT_FALSE(std::filesystem::exists(DbPath(db)));

    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), nullptr) << "the re-created DB inherited a map";
    Insert(store, db, rows);
    ASSERT_TRUE(Quiesce());
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap of the re-created DB differs from the synchronous build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx of the re-created DB differs from the synchronous build";
}

// (a) The queued hints of a removed DB are dropped by the removal (the pool is plugged by another DB, so no worker could
// have consumed them) and give their reservations back at once; the plug's stays.
TEST_F(IndexerLifecycleTest, RemoveDbDropsTheQueuedHintsOfTheDb) {
    const std::string db = "IDXLCA2";
    XStore store;
    ASSERT_NO_FATAL_FAILURE(PlugThePool(store, "IDXLCA2PLUG"));
    const uint64_t plug = xstore::Indexer::Instance().BacklogBytes();
    ASSERT_EQ(plug, uint64_t{kRowLength});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, Dense(0, 5000, 40));
    ASSERT_EQ(xstore::Indexer::Instance().BacklogBytes(), plug + 5000 * uint64_t{kRowLength});
    EXPECT_NE(Removed(store, db).find("Removed DB"), std::string::npos);
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), plug) << "the removed DB's queued hints kept their reservations";
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), nullptr);
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
}

// (b) Item K: a reader pinning the zone map across RemoveDB keeps the old map; the registry forgets it and a reopen
// after a re-create gets a different one.
TEST_F(IndexerLifecycleTest, APinHeldAcrossRemoveDbKeepsItsMapAndAReopenGetsANewOne) {
    const std::string db = "IDXLCB";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    std::optional<ZoneMapFile::Pin> pin(XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width));
    const std::shared_ptr<xstore::PendingMap> old_map = (*pin)->Pending();
    ASSERT_NE(old_map, nullptr);
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        return Removed(r, db);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::filesystem::exists(DbPath(db)) && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_FALSE(std::filesystem::exists(DbPath(db))) << "the removal never got to the files";
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), nullptr);
    EXPECT_EQ((*pin)->Pending(), old_map) << "the pinned file lost its map";
    pin.reset();
    ASSERT_EQ(remove.wait_for(std::chrono::seconds(15)), std::future_status::ready);
    EXPECT_NE(remove.get().find("Removed DB"), std::string::npos);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const ZoneMapFile::Pin fresh = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, Width(db));
    ASSERT_NE(fresh->Pending(), nullptr);
    EXPECT_NE(fresh->Pending(), old_map) << "a reopen after the re-create got the removed year's map";
    EXPECT_EQ(fresh->Pending(), xstore::PendingMaps::Find(Key(db)));
}

// (b) Keep-old widen with marks set: the outer files keep the registry's map; the kept old index has none.
TEST_F(IndexerLifecycleTest, AKeepOldWidenKeepsTheRegistrysMapOnTheOuterFiles) {
    const std::string db = "IDXLCB2";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    const ZoneMapFile::Pin zmap = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
    const CatIdxFile::Pin catidx = XStore::PinCatIdx(DbPath(db), kYear, kRowLength, width);
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 3);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 3); });
    ASSERT_EQ(zmap->BeginLiveRebuild(width + 16), BeginRebuildResult::kStarted);
    ASSERT_EQ(catidx->BeginLiveRebuild(width + 16), BeginRebuildResult::kStarted);
    ASSERT_NE(zmap->OldWidth(), 0u);
    ASSERT_NE(catidx->OldWidth(), 0u);
    EXPECT_EQ(zmap->Pending(), map);
    EXPECT_EQ(catidx->Pending(), map);
    EXPECT_FALSE(zmap->OldHasPendingForTests()) << "the kept old zone map has a map of its own";
    EXPECT_FALSE(catidx->OldHasPendingForTests()) << "the kept old catidx has a map of its own";
    EXPECT_EQ(map->PendingBlockCount(), 1u);
}

// (c) The retire_late window: a request already past the .CFG check hands a run off while a removal of its DB is in
// progress. Enqueue refuses it and gives the reservation back at once (the plugged pool could not), registers nothing,
// and a hint of a DB the removal does not cover is still queued.
TEST_F(IndexerLifecycleTest, AHintEnqueuedDuringARemovalOfItsDbReleasesItsReservation) {
    const std::string db = "IDXLCC", other = "IDXLCCOTHER";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    ASSERT_NO_FATAL_FAILURE(PlugThePool(store, "IDXLCCPLUG"));
    xstore::Indexer &ix = xstore::Indexer::Instance();
    const uint64_t plug = ix.BacklogBytes();
    const std::string scope = DbPath(db) + "/";   // RemoveDB's scope
    const bool cancelled = XStore::StopBackgroundWorkForTests(scope);
    ix.Enqueue(Hint(db, map, 64, 0, 7 * kRowLength));
    EXPECT_EQ(ix.BacklogBytes(), plug) << "a hint of a DB being removed kept its reservation";
    ix.Enqueue(Hint(other, map, 64, 0, 3 * kRowLength));
    EXPECT_EQ(ix.BacklogBytes(), plug + 3 * kRowLength) << "a hint of another DB was refused";
    XStore::ResumeBackgroundWorkForTests(scope, cancelled);
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), map) << "a refused hint replaced or forgot the year's map";
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
}

// (d) DropHints: only the hints whose year key matches go, each releasing its reservation exactly once; the others
// stay queued and are still processed.
TEST_F(IndexerLifecycleTest, DropHintsDropsTheMatchingHintsAndReleasesEachOnce) {
    XStore store;
    ASSERT_NO_FATAL_FAILURE(PlugThePool(store, "IDXLCDPLUG"));
    xstore::Indexer &ix = xstore::Indexer::Instance();
    const uint64_t plug = ix.BacklogBytes();
    std::vector<std::weak_ptr<xstore::PendingMap>> dropped, kept;
    for (int i = 0; i < 4; ++i) {
        auto m = std::make_shared<xstore::PendingMap>(16);
        dropped.push_back(m);
        ix.Enqueue(Hint("IDXLCDA", m, 64, static_cast<uint64_t>(i), 100));
    }
    for (int i = 0; i < 3; ++i) {
        auto m = std::make_shared<xstore::PendingMap>(16);
        kept.push_back(m);
        ix.Enqueue(Hint("IDXLCDB", m, 64, static_cast<uint64_t>(i), 100));
    }
    ASSERT_EQ(ix.BacklogBytes(), plug + 700);
    const std::string drop_key = Key("IDXLCDA");
    ix.DropHints([&](const std::string &key) { return key == drop_key; });
    EXPECT_EQ(ix.BacklogBytes(), plug + 300);
    for (const auto &w : dropped) EXPECT_TRUE(w.expired()) << "a dropped hint is still queued";
    for (const auto &w : kept) EXPECT_FALSE(w.expired()) << "a hint that does not match was dropped";
    ReleasePasses();
    ASSERT_TRUE(Quiesce()) << "the survivors were not processed, or a reservation was released twice (wrapped)";
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::any_of(kept.begin(), kept.end(), [](const auto &w) { return !w.expired(); }) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    for (const auto &w : kept) EXPECT_TRUE(w.expired()) << "a surviving hint was never processed";
    EXPECT_EQ(ix.BacklogBytes(), 0u);
}

// (e) A pass that has checked its year (still registered) and is about to open it when RemoveDBFiles of that year starts:
// the removal waits for the pass's key, so the pass works on the existing files and the removal deletes them after;
// no file of the removed year comes back.
TEST_F(IndexerLifecycleTest, ARemovalBetweenAPasssCheckAndItsOpensDoesNotBringTheYearBack) {
    const std::string db = "IDXLCE";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldFirstOpen();
    Insert(store, db, Dense(0, 10, 40));
    ASSERT_NO_FATAL_FAILURE(WaitHeld(open_held_));
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        XStore_Proto::RemoveDBFiles_REQ req;
        req.add_filename(std::to_string(kYear) + ".XSTORE");
        return r.RemoveDBFiles(&req, db).removestatus_size();
    });
    EXPECT_EQ(remove.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout)
        << "the removal ran while a pass that had checked the year was about to open it";
    ReleaseOpen();
    ASSERT_EQ(remove.wait_for(std::chrono::seconds(15)), std::future_status::ready);
    ASSERT_TRUE(Quiesce());
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".XSTORE"))) << "the pass re-created the removed year file";
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".zmap")));
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".catidx")));
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), nullptr);
}

// Task 3 review M4: the data file's open fails because the year was removed meanwhile (unlinked between the open and the
// fd cache install, its map forgotten): the hint is dropped quietly, no error is logged and no sidecar is marked dirty.
TEST_F(IndexerLifecycleTest, AFailedOpenOfAYearRemovedMeanwhileIsDroppedQuietly) {
    const std::string db = "IDXLCF";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    static_cast<void>(CleanBytes(db, width));
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 10);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 10); });
    store.EvictFdForTests(YearPath(db, ".XSTORE"));
    xstore::Indexer &ix = xstore::Indexer::Instance();
    const std::string data_path = YearPath(db, ".XSTORE"), key = Key(db);
    kLogCapture->Clear();
    {
        xstore_fault::ScopedOpenHook unlink_after_open(data_path, [&] {
            ::unlink(data_path.c_str());
            xstore::PendingMaps::ForgetMatching([&](const std::string &k) { return k == key; });
        });
        ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
        ASSERT_TRUE(unlink_after_open.Fired()) << "the pass did not open the data file";
    }
    EXPECT_EQ(kLogCapture->Count("[INDEXER]"), 0u) << "a removed year was logged as an open failure";
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u) << "a removed year's .zmap marked dirty";
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u) << "a removed year's .catidx marked dirty";
    EXPECT_EQ(ix.BacklogBytes(), 0u);
}

// Review 1 (Important): RemoveDBFiles whose unlink of the data file fails keeps the year's pending map registered: the
// rows stay queryable, their marked (unindexed) rows stay untrusted, and a later cold open shares the old map instead of
// getting an empty one (which would trust blocks that miss them).
TEST_F(IndexerLifecycleTest, AFailedUnlinkOfTheYearKeepsItsPendingMap) {
    const std::string db = "IDXLCG";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 10);   // handed off and not folded yet
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 10); });
    XStore_Proto::RemoveDBFiles_REP rep;
    {
        xstore_fault::ScopedPathFault fail(YearPath(db, ".XSTORE"), false, EBUSY, xstore_fault::kRemove);
        XStore_Proto::RemoveDBFiles_REQ req;
        req.add_filename(std::to_string(kYear) + ".XSTORE");
        rep = store.RemoveDBFiles(&req, db);
        ASSERT_GT(fail.OpenHits(), 0u) << "the unlink was not attempted";
    }
    ASSERT_GE(rep.removestatus_size(), 1);
    ASSERT_EQ(rep.removestatus(0).rfind("[ERROR]", 0), 0u) << rep.removestatus(0);
    ASSERT_TRUE(std::filesystem::exists(YearPath(db, ".XSTORE")));
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), map) << "the map of a year whose data file stayed was forgotten";
    const ZoneMapFile::Pin reopened = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
    EXPECT_EQ(reopened->Pending(), map) << "a cold open after the failed removal got another map";
    EXPECT_FALSE(reopened->TryGetBlockStats(0).has_value()) << "a block with unindexed rows is trusted";
}

// Review 1, minor 2: a pass that took its key before a removal registered its scope, then misses the registry (the
// cold open is refused: "being removed"), drops the hint without an error log.
TEST_F(IndexerLifecycleTest, AColdOpenRefusedByARemovalIsDroppedQuietly) {
    const std::string db = "IDXLCH";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 10);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 10); });
    const std::string scope = DbPath(db) + "/";
    xstore::Indexer &ix = xstore::Indexer::Instance();
    kLogCapture->Clear();
    // At the open hook the pass holds its key and has seen the map registered. Now the registry entries go and a
    // removal registers its scope from another thread (StopBackgroundWork then waits for this pass's key), so the
    // pass's cold opens are refused ("being removed").
    std::atomic<bool> registered{false};
    std::thread remover;
    xstore::Indexer::SetPassOpenDelayForTests([&] {
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        remover = std::thread([&] {
            const bool cancelled = XStore::StopBackgroundWorkForTests(scope);
            registered.store(true);
            XStore::ResumeBackgroundWorkForTests(scope, cancelled);
        });
        // The scope is in once a background year under it is refused.
        while (XStore::EnterBackgroundYearForTests(Key(db))) {
            XStore::LeaveBackgroundYearForTests();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
    xstore::Indexer::SetPassOpenDelayForTests(nullptr);
    remover.join();
    EXPECT_TRUE(registered.load());
    EXPECT_EQ(kLogCapture->Count("[INDEXER]"), 0u) << "a cold open refused by a removal was logged as a failure";
    EXPECT_EQ(ix.BacklogBytes(), 0u);
}

// Review 1, minor 6: several passes of one year run in parallel (pool of 4) when RemoveDB starts: the removal waits for
// every one of them, then returns with nothing reserved or registered.
TEST_F(IndexerLifecycleTest, RemoveDbWaitsForEveryParallelPassOfTheYear) {
    const std::string db = "IDXLCI";
    XStore store;
    xstore::Indexer::SetConfigForTests({4, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    released_.store(false);
    xstore::Indexer::SetPassDelayForTests([this] {
        ++held_;
        while (!released_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    Insert(store, db, Dense(0, 4 * RowsPerBlock(kRowLength), 40));   // four blocks: four hints of one key
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (held_.load() < 2 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_GE(held_.load(), 2) << "no two passes of the year ran at once";
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        return Removed(r, db);
    });
    EXPECT_EQ(remove.wait_for(std::chrono::milliseconds(200)), std::future_status::timeout)
        << "the removal did not wait for the running passes";
    ReleasePasses();
    ASSERT_EQ(remove.wait_for(std::chrono::seconds(15)), std::future_status::ready) << "the removal did not return";
    EXPECT_NE(remove.get().find("Removed DB"), std::string::npos);
    ASSERT_TRUE(Quiesce());
    EXPECT_EQ(xstore::PendingMaps::Find(Key(db)), nullptr);
    EXPECT_FALSE(std::filesystem::exists(DbPath(db)));
    EXPECT_TRUE(XStore::EnterBackgroundYearForTests(Key(db))) << "a removal scope or key was left behind";
    XStore::LeaveBackgroundYearForTests();
}

// The test-only registry clears wait for the pool to go idle: clearing under a running pass's pins would free the files
// it folds into (heap-use-after-free at the pass's end, seen under ASan).
TEST_F(IndexerLifecycleTest, ClearingTheRegistriesWaitsForARunningPass) {
    const std::string db = "IDXLCCLR";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    HoldCountedPasses();
    Insert(store, db, Dense(0, 10, 40));
    ASSERT_NO_FATAL_FAILURE(WaitHeld(held_));   // the pass holds its pins, inside its READ lock
    std::atomic<int> stage{0};                  // 1: clearing, 2: both clears returned
    std::thread clearer([&] {
        stage.store(1);
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        stage.store(2);
    });
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (stage.load() == 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto window = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
    while (stage.load() != 2 && std::chrono::steady_clock::now() < window) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(stage.load(), 1) << "the registries were cleared under a running pass's pins";
    ReleasePasses();
    clearer.join();
    EXPECT_EQ(stage.load(), 2);
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u) << "the clear returned before the pass let go";
}

// Pool off (the default): the registry clears do not wait at all.
TEST_F(IndexerLifecycleTest, ClearingTheRegistriesWithThePoolOffReturnsAtOnce) {
    xstore::Indexer::SetConfigForTests({1, 0});
    ASSERT_FALSE(xstore::Indexer::Instance().Active());
    const auto t0 = std::chrono::steady_clock::now();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(1));
}

// ---------------------------------------------------------------------------------------------------------------------
// Task 6 part B: the shutdown drain, the MarkClean gates (shutdown and live widen), the startup order, the Open holes.

// (d) Shutdown with a backlog (passes held, released while Shutdown drains): the indexer folds everything before
// MarkClean, so the sidecars end clean and byte-equal to the synchronous build.
TEST_F(IndexerLifecycleTest, ShutdownDrainsTheBacklogBeforeMarkingTheSidecarsClean) {
    const std::string db = "IDXLCSD", ref_db = "IDXLCSDREF";
    XStore store;
    const Rows rows = Dense(0, 6000, 40);
    const auto ref = ReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({1, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    RunCheckpointer();
    HoldCountedPasses();
    Insert(store, db, rows);
    ASSERT_NO_FATAL_FAILURE(WaitHeld(held_));
    ASSERT_GT(xstore::Indexer::Instance().BacklogBytes(), 0u);
    std::thread releaser([this] {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        ReleasePasses();
    });
    XStore::Shutdown();
    const std::string zmap = ReadFileBytes(YearPath(db, ".zmap"));   // before a late pass could change anything
    const std::string catidx = ReadFileBytes(YearPath(db, ".catidx"));
    releaser.join();
    EXPECT_EQ(HeaderOf<ZoneMapFileHeader>(zmap).dirty, 0u);
    EXPECT_EQ(HeaderOf<CatIdxFileHeader>(catidx).dirty, 0u);
    EXPECT_TRUE(zmap == ref.first) << ".zmap after Shutdown differs from the synchronous build";
    EXPECT_TRUE(catidx == ref.second) << ".catidx after Shutdown differs from the synchronous build";
    EXPECT_EQ(Map(db)->PendingBlockCount(), 0u);
    ASSERT_TRUE(Quiesce());
}

// (e) The drain runs out (a pass held past the bound): Shutdown returns, the year with rows still pending stays dirty,
// the other year is marked clean, and the next start rebuilds the dirty year to the synchronous build's bytes.
TEST_F(IndexerLifecycleTest, ADrainThatRunsOutLeavesOnlyThePendingYearDirtyAndTheRestartRebuildsIt) {
    const std::string db = "IDXLCSE", ref_db = "IDXLCSEREF";
    constexpr unsigned short kOther = 1972;
    XStore store;
    const Rows rows = Dense(0, 6000, 40);   // three blocks: one pass held, two hints queued
    const auto ref = ReferenceBytes(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({1, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows other;
    for (uint64_t i = 0; i < 100; ++i) other.emplace_back(Jan1(kOther) + i, MixedRow(i, 40));
    InsertAndIndex(store, db, other);
    RunCheckpointer();
    HoldCountedPasses();
    Insert(store, db, rows);
    ASSERT_NO_FATAL_FAILURE(WaitHeld(held_));
    constexpr std::chrono::milliseconds kBound(200);
    XStore::SetIndexerDrainBoundForTests(kBound);
    std::thread releaser([this, kBound] {
        // Shutdown's StopAndDrain turns Active() off, then waits up to the bound for the held pass: release it only
        // after that wait gave up (no fixed guess at when Shutdown gets there).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
        while (xstore::Indexer::Instance().Active() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        // Active() turns off just before StopAndDrain computes its deadline (now + bound): bound + 300 ms is past it
        // with 300 ms for the scheduler. Releasing early only makes the drain finish (the test then fails, not passes).
        std::this_thread::sleep_for(kBound + std::chrono::milliseconds(300));
        ReleasePasses();
    });
    XStore::Shutdown();
    const uint32_t zmap_dirty = DirtyOf(db, kYear, ".zmap"), catidx_dirty = DirtyOf(db, kYear, ".catidx");
    releaser.join();
    EXPECT_EQ(zmap_dirty, 1u) << "a year with rows the indexer never folded was marked clean";
    EXPECT_EQ(catidx_dirty, 1u) << "a year with rows the indexer never folded was marked clean";
    EXPECT_EQ(DirtyOf(db, kOther, ".zmap"), 0u) << "a fully indexed year was left dirty";
    EXPECT_EQ(DirtyOf(db, kOther, ".catidx"), 0u) << "a fully indexed year was left dirty";
    EXPECT_GT(Map(db)->PendingBlockCount(), 0u) << "the drain did not run out (the test proves nothing)";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u) << "a dropped hint kept its reservation";

    // The next start (the indexer is stopped, as in a new process): the dirty year is rebuilt.
    const uint32_t width = Width(db);
    xstore::Indexer::Instance().StopAndDrain(std::chrono::seconds(10));   // no pass may outlive the registries below
    XStore::WalSimulateRestartForTests();
    for (auto &shard : XStore::cfg_shards) shard.map.clear();
    store.InitDB();
    EXPECT_EQ(DirtyOf(db, kYear, ".zmap"), 0u);
    const auto got = CleanBytes(db, width);
    EXPECT_TRUE(got.first == ref.first) << ".zmap rebuilt after the timed-out drain differs from the synchronous build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx rebuilt after the timed-out drain differs from the synchronous build";
}

// (g) Task 3's failed pass (a read error) leaves rows pending with no reservation (BacklogBytes() == 0): Shutdown must
// still leave the sidecars dirty.
TEST_F(IndexerLifecycleTest, AFailedPassFollowedByShutdownLeavesTheSidecarsDirty) {
    const std::string db = "IDXLCSG";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    static_cast<void>(CleanBytes(db, width));
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    map->MarkRows(0, 10);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 10); });
    xstore::Indexer &ix = xstore::Indexer::Instance();
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 0, 1000, EIO);
        ix.RunPassForTests(Hint(db, map, width, 0, 10 * kRowLength));
        ASSERT_GT(fail.Failures(), 0) << "the pass did not read";
    }
    ASSERT_GT(map->PendingBlockCount(), 0u);
    ASSERT_EQ(ix.BacklogBytes(), 0u);
    RunCheckpointer();
    XStore::Shutdown();
    EXPECT_EQ(DirtyOf(db, kYear, ".zmap"), 1u) << "Shutdown marked a year with unfolded rows clean";
    EXPECT_EQ(DirtyOf(db, kYear, ".catidx"), 1u) << "Shutdown marked a year with unfolded rows clean";
}

// (j) Rows handed off whose hints were dropped (as by a removal whose unlink then failed): pending, no hint, nothing
// reserved. Shutdown skips that year's MarkClean; the plug's year, fully folded, is marked clean.
TEST_F(IndexerLifecycleTest, AYearWithPendingRowsAndNoHintIsNotMarkedCleanAtShutdown) {
    const std::string db = "IDXLCSJ", plug_db = "IDXLCSJPLUG";
    XStore store;
    ASSERT_NO_FATAL_FAILURE(PlugThePool(store, plug_db));
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, Dense(0, 10, 40));
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    ASSERT_GT(map->PendingBlockCount(), 0u);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(0, 10); });
    const std::string key = Key(db);
    xstore::Indexer::Instance().DropHints([&](const std::string &k) { return k == key; });
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
    ASSERT_GT(map->PendingBlockCount(), 0u);
    RunCheckpointer();
    XStore::Shutdown();
    EXPECT_EQ(DirtyOf(db, kYear, ".zmap"), 1u) << "a year with hint-less pending rows was marked clean";
    EXPECT_EQ(DirtyOf(db, kYear, ".catidx"), 1u) << "a year with hint-less pending rows was marked clean";
    EXPECT_EQ(DirtyOf(plug_db, kYear, ".zmap"), 0u);
    EXPECT_EQ(DirtyOf(plug_db, kYear, ".catidx"), 0u);
}

// (h) A live widen of a year whose rows are pending (one pass held, one queued): the widened files are never clean
// while a row is pending, and the rows are folded once (the widen scan and the passes do not both fold them).
TEST_F(IndexerLifecycleTest, ALiveWidenOfAYearWithPendingRowsFoldsThemOnceAndIsNotCleanWhilePending) {
    const std::string db = "IDXLCSH", ref_db = "IDXLCSHREF";
    XStore store;
    const Rows rows = Dense(0, 6000, 40);
    xstore::Indexer::SetConfigForTests({2, 0});
    CreateTestDb(store, ref_db, XStore_Proto::SECOND, kRowLength);
    InsertInChunks(store, ref_db, rows);
    xstore::Indexer::SetConfigForTests({1, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Rows(rows.begin(), rows.begin() + 3000));
    const uint32_t width = Width(db);
    HoldCountedPasses();
    Insert(store, db, Rows(rows.begin() + 3000, rows.end()));
    ASSERT_NO_FATAL_FAILURE(WaitHeld(held_));
    const auto map = Map(db);
    ASSERT_GT(map->PendingBlockCount(), 0u);
    auto widen = std::async(std::launch::async, [&] {
        XStore w;
        return w.LiveWidenOneYear(DbPath(db), kYear, kRowLength, width + 16, true, true, nullptr, nullptr);
    });
    bool clean_while_pending = false;
    for (const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
         std::chrono::steady_clock::now() < until;) {
        // Pending before and after the header reads: clean at a moment a row was pending.
        const bool pending_before = map->PendingBlockCount() != 0;
        const bool clean = DirtyOf(db, kYear, ".zmap") == 0 || DirtyOf(db, kYear, ".catidx") == 0;
        clean_while_pending = clean_while_pending || (pending_before && clean && map->PendingBlockCount() != 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ReleasePasses();
    ASSERT_EQ(widen.wait_for(std::chrono::seconds(60)), std::future_status::ready) << "the widen did not end";
    EXPECT_EQ(widen.get(), XStore::LiveWidenResult::kDone);
    ASSERT_TRUE(Quiesce());
    EXPECT_FALSE(clean_while_pending) << "a widened index was marked clean while rows of its year were pending";
    EXPECT_EQ(map->PendingBlockCount(), 0u);
    EXPECT_EQ(Width(db), width + 16);
    EXPECT_EQ(DirtyOf(db, kYear, ".zmap"), 0u) << "the widen did not end clean";
    ExpectSameAnswers(store, db, ref_db, Jan1(kYear), Jan1(kYear) + 5999, 0);
}

// (h) A row pending when the widen ends (after its drain wait: here marked while the scan waits on a writer's lock) keeps
// the widened files dirty: the widen does not mark an index clean while a row of its year is not folded.
TEST_F(IndexerLifecycleTest, ALiveWidenEndingWithARowPendingLeavesTheIndexDirty) {
    const std::string db = "IDXLCSH3";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 3000, 40));
    const uint32_t width = Width(db);
    const auto map = Map(db);
    ASSERT_NE(map, nullptr);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(0, 0, FileRangeLock::LockType::WRITE);   // the scan waits here, past its drain wait (nothing pending)
    auto widen = std::async(std::launch::async, [&] {
        XStore w;
        return w.LiveWidenOneYear(DbPath(db), kYear, kRowLength, width + 16, true, true, nullptr, nullptr);
    });
    // The widen publishes the year's row count only after its wait for pending rows: from then on it is scanning (held by
    // the lock above), and a row marked now is one the wait did not see. (A fresh status: no earlier widen of this DB.)
    bool scanning = false;
    for (const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
         !scanning && std::chrono::steady_clock::now() < deadline;) {
        XStore::LiveRebuildProgress p;
        scanning = XStore::FindRebuildProgress(DbPath(db), kYear, /*catidx=*/false, p) && p.total_rows != 0;
        if (!scanning) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_TRUE(scanning) << "the widen never got past its wait for pending rows";   // no ASSERT while the lock is held
    map->MarkRows(10, 1);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(10, 1); });
    lock.UnlockRange(0, 0, FileRangeLock::LockType::WRITE);
    ASSERT_EQ(widen.wait_for(std::chrono::seconds(60)), std::future_status::ready) << "the widen did not end";
    EXPECT_EQ(widen.get(), XStore::LiveWidenResult::kDone);
    EXPECT_EQ(Width(db), width + 16);
    EXPECT_EQ(DirtyOf(db, kYear, ".zmap"), 1u) << "a widened index was marked clean while a row of its year was pending";
    EXPECT_EQ(DirtyOf(db, kYear, ".catidx"), 1u) << "a widened index was marked clean while a row of its year was pending";
}

// (h) While a sidecar of the year is being rebuilt the writer folds inline: a run handed off then could be folded by
// both the builder's scan and its pass.
TEST_F(IndexerLifecycleTest, ARunWrittenWhileItsYearIsRebuildingIsFoldedInline) {
    const std::string db = "IDXLCSH2";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    const ZoneMapFile::Pin zmap = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
    ASSERT_EQ(zmap->BeginLiveRebuild(width + 16), BeginRebuildResult::kStarted);
    HoldPasses();
    Insert(store, db, Dense(3000, 1000, 40));
    EXPECT_EQ(Map(db)->PendingBlockCount(), 0u) << "a run was handed off while its year was rebuilding";
    EXPECT_EQ(xstore::Indexer::Instance().BacklogBytes(), 0u);
    ReleasePasses();
    ASSERT_TRUE(Quiesce());
}

// (i) A current-version zone map whose block size is not the one the row length gives (a stale header the startup check
// did not see, e.g. a .zmap with no .XSTORE): Open empties it and marks it dirty, as for a version or width mismatch.
TEST_F(IndexerLifecycleTest, OpenEmptiesAZoneMapOfAnotherBlockSizeAndMarksItDirty) {
    const std::string db = "IDXLCSI";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    static_cast<void>(CleanBytes(db, width));
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    ASSERT_NO_FATAL_FAILURE(SetRowsPerBlock(YearPath(db, ".zmap"), RowsPerBlock(kRowLength) / 2));
    {
        const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
        EXPECT_EQ(z->RowsPerBlockValue(), RowsPerBlock(kRowLength));
    }
    const ZoneMapFileHeader h = ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap"));
    EXPECT_EQ(h.rows_per_block, RowsPerBlock(kRowLength));
    EXPECT_EQ(h.dirty, 1u);
    EXPECT_EQ(std::filesystem::file_size(YearPath(db, ".zmap")), sizeof(ZoneMapFileHeader))
        << "the stats of the other block size were kept";
}

// (i) The startup rebuild cannot empty an unusable zone map (its ftruncate fails): the start fails loudly instead of
// rebuilding into the old block size; without the fault the next start rebuilds it.
TEST_F(IndexerLifecycleTest, AZoneMapThatCannotBeEmptiedFailsTheStart) {
    const std::string db = "IDXLCSI2";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    // A clean stop first: the ring is checkpointed, so the start below replays nothing (a replay would open the zone
    // map before the rebuild does and meet the fault there).
    RunCheckpointer();
    XStore::Shutdown();   // also stops the pool: a start runs with none
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    ASSERT_NO_FATAL_FAILURE(SetRowsPerBlock(YearPath(db, ".zmap"), RowsPerBlock(kRowLength) / 2));
    for (auto &shard : XStore::cfg_shards) shard.map.clear();
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kFtruncate, YearPath(db, ".zmap"), 0, 1, EIO);
        EXPECT_THROW(store.InitDB(), std::exception);
        EXPECT_EQ(fail.Failures(), 1);
    }
    for (auto &shard : XStore::cfg_shards) shard.map.clear();
    store.InitDB();
    const ZoneMapFileHeader h = ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap"));
    EXPECT_EQ(h.rows_per_block, RowsPerBlock(kRowLength));
    EXPECT_EQ(h.dirty, 0u);
}

// (i) A pending map left registered for the year (here with another block size and marks, PendingMaps::For keeps it)
// does not survive the startup rebuild: the rebuilt files share a fresh map and their blocks are trusted.
TEST_F(IndexerLifecycleTest, AStartupRebuildReplacesAStalePendingMap) {
    const std::string db = "IDXLCSI3";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    xstore::Indexer::Instance().StopAndDrain(std::chrono::seconds(10));
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    const auto stale = xstore::PendingMaps::For(Key(db), RowsPerBlock(kRowLength) / 2);
    stale->MarkRows(100, 10);   // rows the replay of the ring does not rewrite (its inline apply clears what it writes)
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { stale->ClearRows(100, 10); });
    XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width)->MarkDirty();
    for (auto &shard : XStore::cfg_shards) shard.map.clear();
    store.InitDB();
    const auto now = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(now, nullptr);
    EXPECT_NE(now, stale) << "the rebuild kept the stale map";
    EXPECT_EQ(now->PendingBlockCount(), 0u);
    EXPECT_EQ(now->RowsPerBlock(), RowsPerBlock(kRowLength));
    const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
    EXPECT_EQ(z->Pending(), now);
    EXPECT_TRUE(z->TryGetBlockStats(0).has_value()) << "a rebuilt block is not trusted";
}

// (f) The child of AKillWhileTheIndexerLags... (run only by it: XSTORE_KILL_CHILD_OUT names the file to report to).
// Clean sidecars first, then rows handed off with every pass held and acked; then it waits to be SIGKILLed.
constexpr const char *kKillDb = "IDXLCSF";
TEST_F(IndexerLifecycleTest, DISABLED_KillDuringLagChild) {
    const char *out = std::getenv("XSTORE_KILL_CHILD_OUT");
    if (out == nullptr) GTEST_SKIP() << "run by AKillWhileTheIndexerLagsLeavesDirtySidecarsAndTheRestartRebuildsThem";
    XStore store;
    walring::Checkpointer::Instance().SetPausedForTests(true);   // every record stays in the ring: the restart replays
    CreateTestDb(store, kKillDb, XStore_Proto::SECOND, kRowLength);
    InsertAndIndex(store, kKillDb, Dense(0, 3000, 40));
    static_cast<void>(CleanBytes(kKillDb, Width(kKillDb)));
    ASSERT_EQ(DirtyOf(kKillDb, kYear, ".zmap"), 0u);
    ASSERT_EQ(DirtyOf(kKillDb, kYear, ".catidx"), 0u);
    HoldCountedPasses();
    // Small calls: ring records (a large one takes the direct path, which leaves nothing to replay).
    const Rows lagging = Dense(3000, 3000, 40);
    for (size_t at = 0; at < lagging.size(); at += 40) {
        Insert(store, kKillDb, Rows(lagging.begin() + at, lagging.begin() + std::min(lagging.size(), at + 40)));
    }
    ASSERT_GT(Map(kKillDb)->PendingBlockCount(), 0u);
    {
        std::ofstream report(std::string(out) + ".tmp");
        report << scratch->test_root.string();
    }
    std::filesystem::rename(std::string(out) + ".tmp", out);
    for (;;) std::this_thread::sleep_for(std::chrono::seconds(1));
}

// (f) Kill during lag: a process with clean sidecars hands rows off, acks them and is SIGKILLed while every pass is held.
// The next start finds the sidecars dirty, replays and rebuilds with the indexer stopped (it starts after InitDB, from
// the environment), and its indexes and answers equal a from-scratch synchronous build of the same rows.
TEST_F(IndexerLifecycleTest, AKillWhileTheIndexerLagsLeavesDirtySidecarsAndTheRestartRebuildsThem) {
    ASSERT_NO_FATAL_FAILURE(RunChildAndKillItWhenReady("IndexerLifecycleTest.DISABLED_KillDuringLagChild"));

    EXPECT_EQ(DirtyOf(kKillDb, kYear, ".zmap"), 1u) << "acked handed-off rows left a clean .zmap behind";
    EXPECT_EQ(DirtyOf(kKillDb, kYear, ".catidx"), 1u) << "acked handed-off rows left a clean .catidx behind";

    // A new process: no pool until the constructor starts one from the environment, after InitDB.
    xstore::Indexer::Instance().StopAndDrain(std::chrono::seconds(10));
    {
        ScopedEnv backlog("XSTORE_INDEXER_BACKLOG_MB", "64");
        ScopedEnv threads("XSTORE_INDEXER_THREADS", "1");
        std::atomic<int> passes_during_start{0};
        xstore::Indexer::SetPassOpenDelayForTests([&] { ++passes_during_start; });
        kLogCapture->Clear();
        kLogCapture->set_level(spdlog::level::info);   // the replay count is an info line
        XStore restarted;
        kLogCapture->set_level(spdlog::level::warn);
        const int passes = passes_during_start.load();
        xstore::Indexer::SetPassOpenDelayForTests(nullptr);
        EXPECT_EQ(passes, 0) << "the indexer ran during InitDB's replay and rebuild";
        EXPECT_TRUE(xstore::Indexer::Instance().Active()) << "the indexer was not started after InitDB";
        // The "indexer ran during the replay" half needs records to replay (a direct-path write leaves none).
        EXPECT_GT(ReplayedRecords(), 0u) << "the restart replayed no ring record: the check above proves nothing";
    }
    EXPECT_EQ(DirtyOf(kKillDb, kYear, ".zmap"), 0u) << "the restart did not rebuild the dirty .zmap";
    EXPECT_EQ(DirtyOf(kKillDb, kYear, ".catidx"), 0u) << "the restart did not rebuild the dirty .catidx";

    const std::string ref_db = "IDXLCSFREF";
    XStore store;
    const auto ref = ReferenceBytes(store, ref_db, Dense(0, 6000, 40));
    const auto got = CleanBytes(kKillDb, Width(kKillDb));
    EXPECT_TRUE(got.first == ref.first) << ".zmap rebuilt after the kill differs from the synchronous build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx rebuilt after the kill differs from the synchronous build";
    ExpectSameAnswers(store, kKillDb, ref_db, Jan1(kYear), Jan1(kYear) + 5999, 0);
}

// The child of AKillWithThePoolOff... (run only by it). Clean sidecars first, then rows written with the pool off in
// small calls (observation records, kept in the ring by the paused checkpointer) and acked; then it waits to be killed.
constexpr const char *kKillOffDb = "IDXLCOFF";
TEST_F(IndexerLifecycleTest, DISABLED_KillWithThePoolOffChild) {
    const char *out = std::getenv("XSTORE_KILL_CHILD_OUT");
    if (out == nullptr) GTEST_SKIP() << "run by AKillWithThePoolOffReplaysAndTheRestartRebuildsTheSidecars";
    XStore store;
    xstore::Indexer::SetConfigForTests({2, 0});
    walring::Checkpointer::Instance().SetPausedForTests(true);   // every record stays in the ring: the restart replays
    CreateTestDb(store, kKillOffDb, XStore_Proto::SECOND, kRowLength);
    Insert(store, kKillOffDb, Dense(0, 3000, 40));
    static_cast<void>(CleanBytes(kKillOffDb, Width(kKillOffDb)));
    ASSERT_EQ(DirtyOf(kKillOffDb, kYear, ".zmap"), 0u);
    ASSERT_EQ(DirtyOf(kKillOffDb, kYear, ".catidx"), 0u);
    const Rows more = Dense(3000, 3000, 40);
    for (size_t at = 0; at < more.size(); at += 40) {
        Insert(store, kKillOffDb, Rows(more.begin() + at, more.begin() + std::min(more.size(), at + 40)));
    }
    {
        std::ofstream report(std::string(out) + ".tmp");
        report << scratch->test_root.string();
    }
    std::filesystem::rename(std::string(out) + ".tmp", out);
    for (;;) std::this_thread::sleep_for(std::chrono::seconds(1));
}

// A kill after pool-off (observation) records were acked: their fold marked the sidecars dirty on disk, the restart
// (pool off) replays the ring records, which carry no observations (the parsers leave has_obs false), and rebuilds
// the sidecars to the rows-only build's bytes.
TEST_F(IndexerLifecycleTest, AKillWithThePoolOffReplaysAndTheRestartRebuildsTheSidecars) {
    ASSERT_NO_FATAL_FAILURE(RunChildAndKillItWhenReady("IndexerLifecycleTest.DISABLED_KillWithThePoolOffChild"));
    EXPECT_EQ(DirtyOf(kKillOffDb, kYear, ".zmap"), 1u) << "acked observation records left a clean .zmap behind";
    EXPECT_EQ(DirtyOf(kKillOffDb, kYear, ".catidx"), 1u) << "acked observation records left a clean .catidx behind";
    xstore::Indexer::Instance().StopAndDrain(std::chrono::seconds(10));
    {
        ScopedEnv backlog("XSTORE_INDEXER_BACKLOG_MB", "0");
        kLogCapture->Clear();
        kLogCapture->set_level(spdlog::level::info);   // the replay count is an info line
        XStore restarted;
        kLogCapture->set_level(spdlog::level::warn);
        EXPECT_FALSE(xstore::Indexer::Instance().Active());
        EXPECT_GT(ReplayedRecords(), 0u) << "the restart replayed no ring record";
    }
    EXPECT_EQ(DirtyOf(kKillOffDb, kYear, ".zmap"), 0u) << "the restart did not rebuild the dirty .zmap";
    EXPECT_EQ(DirtyOf(kKillOffDb, kYear, ".catidx"), 0u) << "the restart did not rebuild the dirty .catidx";

    const std::string ref_db = "IDXLCOFFREF";
    XStore store;
    const auto ref = RowsOnlyReferenceBytes(store, ref_db, Dense(0, 6000, 40));
    const auto got = CleanBytes(kKillOffDb, Width(kKillOffDb));
    EXPECT_TRUE(got.first == ref.first) << ".zmap rebuilt after the kill differs from the rows-only build";
    EXPECT_TRUE(got.second == ref.second) << ".catidx rebuilt after the kill differs from the rows-only build";
    ExpectSameAnswers(store, kKillOffDb, ref_db, Jan1(kYear), Jan1(kYear) + 5999, 0);
}

// ---------------------------------------------------------------------------------------------------------------------
// IndexerOracleTest: the byte-identical and query oracles at scale (Task 7, spec section 9).

// XSTORE_ORACLE_SEED reruns a printed seed.
uint64_t OracleSeed() {
    if (const char *v = std::getenv("XSTORE_ORACLE_SEED"); v != nullptr && *v != '\0') return std::strtoull(v, nullptr, 10);
    return 20261106;
}

class IndexerOracleTest : public IndexerLifecycleTest {
protected:
    using Ops = std::function<void(const std::string &db, bool pooled)>;
    std::atomic<uint64_t> passes_{0};
    std::atomic<bool> gate_open_{true};
    std::mutex delay_mutex_;
    std::mt19937_64 delay_rng_;

    void TearDown() override {
        gate_open_.store(true);   // no pass may stay held past the test (the hook reads this member)
        IndexerLifecycleTest::TearDown();
    }
    // Every pass sleeps a seeded random 0..max_us inside its READ lock, is counted, then waits while the gate is shut.
    void SeededPassDelays(uint64_t seed, int max_us) {
        {
            std::lock_guard<std::mutex> lock(delay_mutex_);
            delay_rng_.seed(seed);
        }
        xstore::Indexer::SetPassDelayForTests([this, max_us] {
            ++passes_;
            int us = 0;
            {
                std::lock_guard<std::mutex> lock(delay_mutex_);
                us = std::uniform_int_distribution<int>(0, max_us)(delay_rng_);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(us));
            while (!gate_open_.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }

    // Every column type with small values, so sums and sums of squares are exact in any fold order (a pass folds a
    // block's rows in row order, the inline path in arrival order; MixedRow's large values would round differently).
    // Columns 0 and 1 are doubles (ExpectSameAnswers' columns; column 1 holds kTarget every 50 rows); `salt` gives a
    // rewrite new values.
    static XStore_Proto::RowData ExactRow(uint64_t i, int columns, uint64_t salt = 0) {
        XStore_Proto::RowData row;
        const uint64_t v = i * 37 + salt * 101;
        for (int c = 0; c < columns; ++c) {
            XStore_Proto::FData *f = row.add_fielddata();
            const uint64_t x = v + static_cast<uint64_t>(c);
            if (c == 0) {
                f->set_doubledata(static_cast<double>(x % 2000) * 0.5);
                continue;
            }
            if (c == 1) {
                f->set_doubledata(static_cast<double>((i + salt) % 50));
                continue;
            }
            if (x % 11 == 0) continue;   // null
            if (columns > 100) {   // 130 small ints fit a 1 KiB row
                f->set_int32data(static_cast<int32_t>(x % 100));
                continue;
            }
            switch (c % 8) {
                case 0: f->set_doubledata(-static_cast<double>(x % 1000) * 0.25); break;
                case 1: f->set_floatdata(static_cast<float>(x % 64) * 0.5f); break;
                case 2: f->set_int64data(static_cast<int64_t>(x % 100000)); break;
                case 3: f->set_sint32data(-static_cast<int32_t>(x % 777)); break;
                case 4: f->set_uint32data(static_cast<uint32_t>(x % 5000)); break;
                case 5: f->set_stringdata("s" + std::to_string(x % 23)); break;
                case 6: f->set_booldata(x % 2 == 0); break;
                default: f->set_bytesdata(std::string("\x01\x00", 2) + std::to_string(x % 5)); break;
            }
        }
        return row;
    }
    static Rows RowsOf(unsigned short year, uint64_t first, uint64_t count, int columns, uint64_t salt = 0) {
        Rows rows;
        for (uint64_t i = first; i < first + count; ++i) rows.emplace_back(Ts(year, i), ExactRow(i, columns, salt));
        return rows;
    }
    // The rows in a fixed shuffled order, `batch` per SYNC call.
    static void InsertShuffled(XStore &store, const std::string &db, Rows rows, size_t batch, uint64_t order_seed) {
        std::mt19937_64 rng(order_seed);
        std::shuffle(rows.begin(), rows.end(), rng);
        for (size_t at = 0; at < rows.size() && !HasFatalFailure(); at += batch) {
            Insert(store, db, Rows(rows.begin() + at, rows.begin() + std::min(rows.size(), at + batch)));
        }
    }

    std::string YearKeyOf(const std::string &db, unsigned short year) const { return DbPath(db) + "/" + std::to_string(year); }
    uint32_t WidthOf(const std::string &db, unsigned short year) const {
        return ReadHeader<ZoneMapFileHeader>(YearFile(db, year, ".zmap")).tracked_column_count;
    }
    // CleanBytes for any year.
    std::pair<std::string, std::string> YearBytes(const std::string &db, unsigned short year) const {
        const uint32_t width = WidthOf(db, year);
        {
            ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), year, kRowLength, width);
            CatIdxFile::Pin c = XStore::PinCatIdx(DbPath(db), year, kRowLength, width);
            EXPECT_TRUE(z->MarkClean());
            EXPECT_TRUE(c->MarkClean());
        }
        return {ReadFileBytes(YearFile(db, year, ".zmap")), ReadFileBytes(YearFile(db, year, ".catidx"))};
    }
    // Where two images first differ; for a zone map (width != 0) also the block, column and entry byte.
    static std::string FirstDiff(const std::string &a, const std::string &b, uint32_t width) {
        const size_t n = std::min(a.size(), b.size());
        size_t at = 0;
        while (at < n && a[at] == b[at]) ++at;
        std::string s = " (sizes " + std::to_string(a.size()) + " vs " + std::to_string(b.size()) + "; first difference at byte " +
                        std::to_string(at);
        if (width != 0 && at >= sizeof(ZoneMapFileHeader) && at < n) {
            const size_t off = at - sizeof(ZoneMapFileHeader), entry = off / sizeof(ZoneMapEntry);
            s += ": block " + std::to_string(entry / width) + ", column " + std::to_string(entry % width) + ", entry byte " +
                 std::to_string(off % sizeof(ZoneMapEntry));
        }
        return s + ")";
    }

    // `ops` builds `ref` with the pool off, then `db` with it on (seeded pass delays); waits for the indexer.
    void BuildBoth(XStore &store, const std::string &db, const std::string &ref, const Ops &ops, uint64_t seed) {
        xstore::Indexer::SetConfigForTests({2, 0});
        CreateTestDb(store, ref, XStore_Proto::SECOND, kRowLength);
        ops(ref, false);
        ASSERT_FALSE(HasFatalFailure());
        xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
        passes_ = 0;
        SeededPassDelays(seed, 1000);
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        ops(db, true);
        ASSERT_FALSE(HasFatalFailure());
        ASSERT_TRUE(Quiesce());
        xstore::Indexer::SetPassDelayForTests(nullptr);
        EXPECT_GT(passes_.load(), 0u) << "no run was handed to the indexer";
    }
    // I3, strict: after quiesce every year's .zmap and .catidx equal the synchronous build's byte for byte, nothing is
    // left pending, and every aggregate and FindByValue answer equals the synchronous DB's.
    // `extent`: the rows of each year the answers are compared on (a query over a whole year reads its sparse file).
    // `min_passes`: a quarter of the load's insert calls. A call's runs are handed off as at least one hint per block,
    // and a pass folds whatever is pending in its block, so passes can only fall below the calls when records of one
    // block arrive while an earlier pass of it waits (0-1 ms delays): observed passes are 1-5x the calls on Mystic, and
    // a quarter is a floor that only a mostly-inline (not handed off) build misses.
    void RunStrict(XStore &store, const char *name, const std::vector<unsigned short> &years, uint64_t extent, const Ops &ops,
                   uint64_t seed, uint64_t min_passes) {
        SCOPED_TRACE(std::string("load ") + name + ", seed " + std::to_string(seed));
        const auto t0 = std::chrono::steady_clock::now();
        const std::string db = std::string("IDXOR") + name, ref = db + "REF";
        ASSERT_NO_FATAL_FAILURE(BuildBoth(store, db, ref, ops, seed));
        EXPECT_GE(passes_.load(), min_passes) << "fewer passes than a quarter of the insert calls: runs were not handed off";
        for (unsigned short year : years) {
            SCOPED_TRACE("year " + std::to_string(year));
            const auto map = xstore::PendingMaps::Find(YearKeyOf(db, year));
            ASSERT_NE(map, nullptr);
            EXPECT_EQ(map->PendingBlockCount(), 0u) << "rows left pending after the indexer quiesced";
            const uint32_t width = WidthOf(db, year);
            ASSERT_EQ(width, WidthOf(ref, year));
            const auto got = YearBytes(db, year), want = YearBytes(ref, year);
            ASSERT_NE(want.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos)
                << "an empty reference proves nothing";
            EXPECT_TRUE(got.first == want.first) << ".zmap differs from the synchronous build" << FirstDiff(got.first, want.first, width);
            EXPECT_TRUE(got.second == want.second) << ".catidx differs from the synchronous build" << FirstDiff(got.second, want.second, 0);
        }
        for (unsigned short year : years) ExpectSameAnswers(store, db, ref, Ts(year, 0), Ts(year, extent - 1), 0);
        ExpectSameAnswers(store, db, ref, Ts(years.front(), 1000), Ts(years.front(), 5000), 0);
        std::printf("[ INFO     ] load %s: %.1f s, %llu passes\n", name,
                    std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(),
                    static_cast<unsigned long long>(passes_.load()));
    }

    // A live widen of kYear from 64 to 70 columns held mid-scan by a WRITE lock on one row: rows of block 0 pending when
    // it begins (its wait for them), rows below and above its frontier while it runs (folded inline: the year is
    // rebuilding), rows of another year meanwhile (handed off), more after it.
    void WidenOps(XStore &store, const std::string &db, bool pooled, uint64_t seed) {
        constexpr unsigned short kOther = 1972;
        constexpr uint64_t kHeld = 5000;
        Insert(store, db, {{Ts(kYear, 0), ExactRow(0, 10)}});   // the index files are born 64 columns wide
        // No call's rows lie on both sides of the held row: a record locks its runs as one set, so one holding rows
        // around it would wait for the test's lock (a test deadlock, not a product one).
        Rows before, pre, below, above, other, after;
        for (uint64_t i = 2; i < 6000; i += 2) before.emplace_back(Ts(kYear, i), ExactRow(i, 70));
        for (uint64_t i = 1001; i < 2000; i += 2) pre.emplace_back(Ts(kYear, i), ExactRow(i, 70));
        for (uint64_t i = 1; i < 4000; i += 2) {
            if (i < 1001 || i >= 2000) below.emplace_back(Ts(kYear, i), ExactRow(i, 70));
        }
        for (uint64_t i = 7000; i < 8000; ++i) above.emplace_back(Ts(kYear, i), ExactRow(i, 70));
        for (uint64_t i = 0; i < 1000; ++i) other.emplace_back(Ts(kOther, i), ExactRow(i, 70));
        for (uint64_t i = 4001; i < 6000; i += 2) after.emplace_back(Ts(kYear, i), ExactRow(i, 70));
        InsertInChunks(store, db, before);
        ASSERT_FALSE(HasFatalFailure());
        ASSERT_EQ(WidthOf(db, kYear), 64u);
        // Only block 0 is pending when the widen begins (a pass over the held row's chunk could not fold, and the
        // widen would wait for it in vain), and stays so (its pass held) until the widen waits for it.
        if (pooled) {
            ASSERT_TRUE(Quiesce());
            HoldPasses();
        }
        Insert(store, db, pre);
        if (pooled) {
            const auto map = xstore::PendingMaps::Find(Key(db));
            ASSERT_NE(map, nullptr);
            EXPECT_EQ(map->PendingBlockCount(), 1u) << "block 0 must be pending when the widen begins";
        }
        FileRangeLock &lock = FileLockRegistry::Get(YearFile(db, kYear, ".XSTORE"));
        std::future<XStore::LiveWidenResult> widen;   // destroyed after the guard below lets the row go
        lock.LockRange(kHeld * kRowLength, kRowLength, FileRangeLock::LockType::WRITE);
        bool locked = true;
        const auto unlock = [&] {
            if (locked) lock.UnlockRange(kHeld * kRowLength, kRowLength, FileRangeLock::LockType::WRITE);
            locked = false;
        };
        const std::shared_ptr<void> unlock_on_exit(nullptr, [&](void *) { unlock(); });
        widen = std::async(std::launch::async, [&] {
            XStore w;
            return w.LiveWidenOneYear(DbPath(db), kYear, kRowLength, 70, true, true, nullptr, nullptr);
        });
        if (pooled) {
            const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, 64);
            for (const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
                 !z->Rebuilding() && std::chrono::steady_clock::now() < deadline;) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            EXPECT_TRUE(z->Rebuilding()) << "the widen did not begin";
            ReleasePasses();
            SCOPED_TRACE("widen pass-delay seed " + std::to_string(seed + 1));
            SeededPassDelays(seed + 1, 1000);
        }
        bool scanning = false;   // past its wait for pending rows (see ALiveWidenEndingWithARowPendingLeavesTheIndexDirty)
        for (const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
             !scanning && std::chrono::steady_clock::now() < deadline;) {
            XStore::LiveRebuildProgress p;
            scanning = XStore::FindRebuildProgress(DbPath(db), kYear, /*catidx=*/false, p) && p.total_rows != 0;
            if (!scanning) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_TRUE(scanning) << "the widen never started its scan";
        for (const Rows *part : {&below, &above, &other}) InsertInChunks(store, db, *part);
        ASSERT_FALSE(HasFatalFailure());
        XStore::LiveRebuildProgress p;
        EXPECT_TRUE(XStore::FindRebuildProgress(DbPath(db), kYear, false, p)) << "the widen ended before the lock let go";
        unlock();
        ASSERT_EQ(widen.wait_for(std::chrono::seconds(60)), std::future_status::ready) << "the widen did not end";
        ASSERT_EQ(widen.get(), XStore::LiveWidenResult::kDone);
        ASSERT_EQ(WidthOf(db, kYear), 70u);
        InsertInChunks(store, db, after);
        XStore::ClearLiveWidenRequestsForTests();
    }

    // Deletes of folded rows only (unary, batch, a whole block, part of one), then new rows, some into the emptied block.
    void DeleteOps(XStore &store, const std::string &db, bool pooled) {
        InsertInChunks(store, db, RowsOf(kYear, 0, 4 * 2048, 40));
        if (pooled) {
            ASSERT_TRUE(Quiesce()) << "the deletes must find every row folded, as the synchronous build does";
        }
        for (uint64_t i = 0; i < 2048; i += 7) {
            ASSERT_NE(store.UnaryDelete(Ts(kYear, i), db).deletestatus().find("[INFO]"), std::string::npos);
        }
        XStore_Proto::BatchDelete_REQ req;
        req.set_dbname(db);
        for (uint64_t i = 2048; i < 4096; i += 3) req.add_timestamp(Ts(kYear, i));
        ASSERT_NE(store.BatchDelete(&req).deletestatus().find("[INFO]"), std::string::npos);
        ASSERT_NE(store.RangeDelete(Ts(kYear, 4096), Ts(kYear, 6143), db).deletestatus().find("[INFO]"), std::string::npos);
        ASSERT_NE(store.RangeDelete(Ts(kYear, 6200), Ts(kYear, 6399), db).deletestatus().find("[INFO]"), std::string::npos);
        Insert(store, db, RowsOf(kYear, 4096, 500, 40, 9));
        InsertInChunks(store, db, RowsOf(kYear, 8192, 1800, 40));
    }

    // Folded rows rewritten (block 0); then, with the one worker plugged, rows handed off and rewritten or deleted while
    // pending: at/above fold_end (blocks 1 and 4), below it (block 2), next to folded rows (block 3). 4,750 rewrites of
    // 8,308 rows.
    void OverwriteOps(XStore &store, const std::string &db, bool pooled) {
        Insert(store, db, RowsOf(kYear, 0, 2000, 40));
        Insert(store, db, RowsOf(kYear, 2048, 952, 40));   // block 1 folded to 3000
        Insert(store, db, RowsOf(kYear, 5000, 1000, 40));   // block 2 folded to 6000
        Insert(store, db, RowsOf(kYear, 6144, 856, 40));    // block 3 folded to 7000
        if (pooled) {
            ASSERT_TRUE(Quiesce());
        }
        Insert(store, db, RowsOf(kYear, 0, 2000, 40, 1));   // every folded row of block 0 rewritten
        if (pooled) {
            ASSERT_TRUE(Quiesce());
            ASSERT_NO_FATAL_FAILURE(PlugThePool(store, db + "P"));
        }
        Insert(store, db, RowsOf(kYear, 3000, 1000, 40));
        Insert(store, db, RowsOf(kYear, 3000, 500, 40, 2));   // pending, at/above fold_end
        Insert(store, db, RowsOf(kYear, 4100, 500, 40));      // new rows below fold_end (zeros: no mark)
        Insert(store, db, RowsOf(kYear, 4100, 250, 40, 3));   // pending, below fold_end
        Insert(store, db, RowsOf(kYear, 7000, 1000, 40));
        ASSERT_NE(store.UnaryDelete(Ts(kYear, 7500), db).deletestatus().find("[INFO]"), std::string::npos);
        XStore_Proto::BatchDelete_REQ req;
        req.set_dbname(db);
        for (uint64_t i = 7600; i < 7610; ++i) req.add_timestamp(Ts(kYear, i));
        ASSERT_NE(store.BatchDelete(&req).deletestatus().find("[INFO]"), std::string::npos);
        Insert(store, db, RowsOf(kYear, 8192, 1000, 40));
        Insert(store, db, RowsOf(kYear, 8192, 1000, 40, 4));
        Insert(store, db, RowsOf(kYear, 8192, 1000, 40, 5));
        // Deletes of pending rows in a block with nothing folded: no inexact mark, the pass folds the rest once (R).
        ASSERT_NE(store.UnaryDelete(Ts(kYear, 8500), db).deletestatus().find("[INFO]"), std::string::npos);
        XStore_Proto::BatchDelete_REQ req4;
        req4.set_dbname(db);
        for (uint64_t i = 8600; i < 8605; ++i) req4.add_timestamp(Ts(kYear, i));
        ASSERT_NE(store.BatchDelete(&req4).deletestatus().find("[INFO]"), std::string::npos);
        if (pooled) {
            const auto map = xstore::PendingMaps::Find(Key(db));
            ASSERT_NE(map, nullptr);
            EXPECT_EQ(map->PendingBlockCount(), 4u) << "blocks 1-4 must be pending during the rewrites";
            ReleasePasses();
            ASSERT_TRUE(Quiesce());
        }
    }
    // I3 with the overwrite exception (ledger "Task 4: concern-1 verdict"): each .zmap block equals the synchronous
    // build's; or, not inexact, equals a rebuild of the final data (a row rewritten while pending at or above fold_end is
    // folded once, with its final bytes); or is inexact, and only the query oracle speaks for it. Catidx bits: rebuild
    // subset of async subset of synchronous.
    void RunOverwriteHeavy(XStore &store) {
        SCOPED_TRACE("load OVERWRITE");
        const auto t0 = std::chrono::steady_clock::now();
        const std::string db = "IDXOROVR", ref = "IDXOROVRREF";
        xstore::Indexer::SetConfigForTests({2, 0});
        CreateTestDb(store, ref, XStore_Proto::SECOND, kRowLength);
        ASSERT_NO_FATAL_FAILURE(OverwriteOps(store, ref, false));
        xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        ASSERT_NO_FATAL_FAILURE(OverwriteOps(store, db, true));
        EXPECT_EQ(Map(db)->PendingBlockCount(), 0u);
        const uint32_t width = WidthOf(db, kYear);
        ASSERT_EQ(width, WidthOf(ref, kYear));
        const auto got = YearBytes(db, kYear), sync = YearBytes(ref, kYear);
        const std::string zpath = (scratch->test_root / "final.zmap").string();
        const std::string cpath = (scratch->test_root / "final.catidx").string();
        ASSERT_NO_FATAL_FAILURE(BuildReferenceIndexes(store, YearFile(db, kYear, ".XSTORE"), zpath, cpath, kRowLength, width));
        const std::string rebuilt = ReadFileBytes(zpath), rebuilt_c = ReadFileBytes(cpath);
        EXPECT_TRUE(got.first.substr(0, sizeof(ZoneMapFileHeader)) == sync.first.substr(0, sizeof(ZoneMapFileHeader)));
        const size_t block_bytes = size_t{width} * sizeof(ZoneMapEntry);
        const auto block_of = [&](const std::string &file, uint64_t b) {
            const size_t off = sizeof(ZoneMapFileHeader) + b * block_bytes;
            std::string s = off < file.size() ? file.substr(off, block_bytes) : std::string();
            s.resize(block_bytes, '\0');
            return s;
        };
        const auto inexact = [](const std::string &block) {
            ZoneMapEntry e{};
            std::memcpy(&e, block.data(), sizeof(e));
            return ZoneMapEntryInexact(e);
        };
        std::string kinds;   // per block: S = the synchronous bytes, R = the rebuild's, I = inexact
        for (uint64_t b = 0; b < 5; ++b) {
            const std::string a = block_of(got.first, b), s = block_of(sync.first, b), r = block_of(rebuilt, b);
            if (a == s) {
                kinds += 'S';
            } else if (!inexact(a)) {
                kinds += 'R';
                EXPECT_TRUE(a == r) << "block " << b << ": neither the synchronous build nor the zone map of the final data"
                                    << FirstDiff(a, r, 0);
            } else {
                kinds += 'I';
            }
            if (b == 0 || b == 2 || b == 3) {
                EXPECT_TRUE(inexact(a)) << "block " << b << " must be inexact";
            }
        }
        // 0: folded rows rewritten, as the synchronous build; 1, 4: rewritten only while pending at/above fold_end (4
        // also has pending rows deleted, nothing folded beside them); 2: a pending row below fold_end rewritten; 3: a
        // pending row deleted next to folded rows.
        EXPECT_EQ(kinds, "SRIIR");
        const size_t n = std::max({got.second.size(), sync.second.size(), rebuilt_c.size()});
        size_t missing = 0, extra = 0, narrower = 0;
        const auto at = [](const std::string &s, size_t i) { return i < s.size() ? static_cast<unsigned char>(s[i]) : 0u; };
        for (size_t i = sizeof(CatIdxFileHeader); i < n; ++i) {
            if ((at(rebuilt_c, i) & ~at(got.second, i)) != 0) ++missing;
            if ((at(got.second, i) & ~at(sync.second, i)) != 0) ++extra;
            if (at(got.second, i) != at(sync.second, i)) ++narrower;
        }
        EXPECT_EQ(missing, 0u) << "a bloom bit of the final data is missing";
        EXPECT_EQ(extra, 0u) << "a bloom bit no synchronous build sets";
        EXPECT_GT(narrower, 0u) << "the async catidx equals the synchronous one: the rewrites proved nothing";
        ExpectSameAnswers(store, db, ref, Ts(kYear, 0), Ts(kYear, 5 * 2048 - 1), 0);
        for (uint64_t b = 0; b < 5; ++b) ExpectSameAnswers(store, db, ref, Ts(kYear, b * 2048 + 10), Ts(kYear, b * 2048 + 2047), 0);
        std::printf("[ INFO     ] load OVERWRITE: %.1f s, blocks %s\n",
                    std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count(), kinds.c_str());
    }

    // The columns the lag oracle checks: three double columns of ExactRow(_, 12) (8 has nulls).
    static constexpr int kScanColumns[3] = {0, 1, 8};
    struct ScannedRow {
        bool present = false;
        std::array<bool, 3> set{};
        std::array<double, 3> value{};
    };
    // From scratch: rows [0, rows) of the year file read and parsed, holes skipped; no index involved.
    std::vector<ScannedRow> ScanYear(const std::string &db, uint64_t rows) const {
        std::vector<ScannedRow> out(rows);
        std::vector<char> buf(rows * kRowLength);
        const int fd = open(YearFile(db, kYear, ".XSTORE").c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        if (fd < 0) return out;
        const ssize_t got = pread(fd, buf.data(), buf.size(), 0);
        close(fd);
        EXPECT_EQ(got, static_cast<ssize_t>(buf.size()));
        for (uint64_t r = 0; r < rows; ++r) {
            const char *p = buf.data() + r * kRowLength;
            if (p[0] == 0) continue;
            XStore_Proto::RowData row;
            const size_t len = XStoreWireEncode::RealRowDataLength(p, kRowLength);
            if (len > 0) static_cast<void>(row.ParseFromArray(p, static_cast<int>(len)));
            out[r].present = true;
            for (size_t k = 0; k < 3; ++k) {
                if (kScanColumns[k] < row.fielddata_size() &&
                    row.fielddata(kScanColumns[k]).oneOfData_case() == XStore_Proto::FData::kDoubleData) {
                    out[r].set[k] = true;
                    out[r].value[k] = row.fielddata(kScanColumns[k]).doubledata();
                }
            }
        }
        return out;
    }
    // MIN, MAX, SUM, AVG and EXISTS on the three columns and FindByValue(column 1, kTarget) over rows [lo, hi] against
    // the scan; the first mismatch, or "". Counts the replies that carried the pending notice.
    static std::string CompareWithScan(XStore &store, const std::string &db, const std::vector<ScannedRow> &rows,
                                       uint64_t lo, uint64_t hi, uint64_t &replies, uint64_t &noticed) {
        const std::string where = "[" + std::to_string(lo) + ", " + std::to_string(hi) + "] ";
        const auto note = [&](const std::string &status) {
            ++replies;
            if (CountOf(status, kPendingNotice) != 0) ++noticed;
            return status.rfind("SUCCESS", 0) == 0;
        };
        for (size_t k = 0; k < 3; ++k) {
            const auto column = static_cast<unsigned short>(kScanColumns[k]);
            uint64_t count = 0;
            double lowest = 0, highest = 0, sum = 0;
            for (uint64_t r = lo; r <= hi; ++r) {
                if (!rows[r].present || !rows[r].set[k]) continue;
                const double v = rows[r].value[k];
                lowest = count == 0 ? v : std::min(lowest, v);
                highest = count == 0 ? v : std::max(highest, v);
                sum += v;
                ++count;
            }
            const std::string col = where + "column " + std::to_string(column) + " ";
            XStore_Proto::Min_REP min_rep;
            store.MinAggregate(Ts(kYear, lo), Ts(kYear, hi), db, column, &min_rep);
            if (note(min_rep.opstatus()) != (count != 0) || (count != 0 && min_rep.opresult().doubledata() != lowest)) {
                return col + "MIN " + std::to_string(min_rep.opresult().doubledata()) + " (" + min_rep.opstatus() +
                       "), scan " + std::to_string(lowest) + " over " + std::to_string(count) + " values";
            }
            XStore_Proto::Max_REP max_rep;
            store.MaxAggregate(Ts(kYear, lo), Ts(kYear, hi), db, column, &max_rep);
            if (note(max_rep.opstatus()) != (count != 0) || (count != 0 && max_rep.opresult().doubledata() != highest)) {
                return col + "MAX " + std::to_string(max_rep.opresult().doubledata()) + " (" + max_rep.opstatus() +
                       "), scan " + std::to_string(highest);
            }
            XStore_Proto::Sum_REP sum_rep;
            store.SumAggregate(Ts(kYear, lo), Ts(kYear, hi), db, column, &sum_rep);
            if (note(sum_rep.opstatus()) != (count != 0) || (count != 0 && sum_rep.opresult() != sum)) {
                return col + "SUM " + std::to_string(sum_rep.opresult()) + " (" + sum_rep.opstatus() + "), scan " +
                       std::to_string(sum);
            }
            XStore_Proto::Avg_REP avg_rep;
            store.AvgAggregate(Ts(kYear, lo), Ts(kYear, hi), db, column, &avg_rep);
            if (note(avg_rep.opstatus()) != (count != 0) ||
                (count != 0 && avg_rep.opresult() != sum / static_cast<double>(count))) {
                return col + "AVG " + std::to_string(avg_rep.opresult()) + " (" + avg_rep.opstatus() + "), scan " +
                       std::to_string(sum / static_cast<double>(count));
            }
            XStore_Proto::Exists_REP exists_rep;
            store.ExistsAggregate(Ts(kYear, lo), Ts(kYear, hi), db, column, &exists_rep);
            static_cast<void>(note(exists_rep.opstatus()));
            if (exists_rep.exists() != (count != 0)) {
                return col + "EXISTS " + std::to_string(exists_rep.exists()) + " (" + exists_rep.opstatus() + "), scan " +
                       std::to_string(count) + " values";
            }
        }
        std::vector<unsigned long long> want;
        for (uint64_t r = lo; r <= hi; ++r) {
            if (rows[r].present && rows[r].set[1] && rows[r].value[1] == kTarget) want.push_back(Ts(kYear, r));
        }
        const Found f = Find(store, db, Ts(kYear, lo), Ts(kYear, hi), 1, kTarget);
        static_cast<void>(note(f.status));
        if (Timestamps(f) != want) {
            return where + "FINDBYVALUE " + std::to_string(f.rows.size()) + " rows, scan " + std::to_string(want.size());
        }
        return std::string();
    }
};

// Spec section 9's byte-identical oracle over a table of loads; the overwrite-heavy load last, with the I3 exception.
TEST_F(IndexerOracleTest, ByteIdenticalAfterQuiesce) {
    const uint64_t seed = OracleSeed();
    constexpr unsigned short kOther = 1972;
    XStore store;
    const auto sparse = [&](const std::string &db, bool) {
        std::mt19937_64 rng(11);
        std::set<uint64_t> picked;
        while (picked.size() < 6000) picked.insert(rng() % (40 * 2048));
        Rows rows;
        for (uint64_t r : picked) rows.emplace_back(Ts(kYear, r), ExactRow(r, 12));
        for (uint64_t r : {90000ULL, 100001ULL, 119999ULL}) rows.emplace_back(Ts(kYear, r), ExactRow(r, 12));
        InsertShuffled(store, db, std::move(rows), 100, 12);
    };
    const auto empty = [&](const std::string &db, bool) {
        Rows rows;
        for (uint64_t i = 0; i < 6000; ++i) {
            XStore_Proto::RowData row;   // i % 3 == 0: no columns at all
            if (i % 3 == 1) {
                for (int c = 0; c < 40; ++c) row.add_fielddata();   // every column unset
            } else if (i % 3 == 2) {
                row = ExactRow(i, 40);
            }
            rows.emplace_back(Ts(kYear, i), std::move(row));
        }
        InsertShuffled(store, db, std::move(rows), 100, 13);
    };
    const auto two_years = [&](const std::string &db, bool) {
        Rows rows = RowsOf(kYear, 0, 3000, 40);
        const Rows other = RowsOf(kOther, 0, 3000, 40);
        rows.insert(rows.end(), other.begin(), other.end());
        InsertShuffled(store, db, std::move(rows), 150, 14);
    };
    // min_passes = insert calls / 4 (see RunStrict).
    RunStrict(store, "DENSE", {kYear}, 8000, [&](const std::string &db, bool) {
        const Rows ring = Dense(0, 4000, 40);
        for (size_t at = 0; at < ring.size() && !HasFatalFailure(); at += 40) {   // ring records
            Insert(store, db, Rows(ring.begin() + at, ring.begin() + static_cast<std::ptrdiff_t>(at + 40)));
        }
        Insert(store, db, Dense(4000, 4000, 40));   // one direct record
    }, seed, 101 / 4);
    RunStrict(store, "SPARSE", {kYear}, 120000, sparse, seed, 61 / 4);
    RunStrict(store, "MIXED", {kYear}, 6144, [&](const std::string &db, bool) {
        InsertShuffled(store, db, RowsOf(kYear, 0, 6144, 64), 200, 15);
    }, seed, 31 / 4);
    RunStrict(store, "WIDE", {kYear}, 4096, [&](const std::string &db, bool) {
        InsertShuffled(store, db, RowsOf(kYear, 0, 4096, 130), 256, 16);
        EXPECT_EQ(WidthOf(db, kYear), 130u);
    }, seed, 16 / 4);
    RunStrict(store, "EMPTY", {kYear}, 6000, empty, seed, 60 / 4);
    RunStrict(store, "TWOYEARS", {kYear, kOther}, 3000, two_years, seed, 40 / 4);
    RunStrict(store, "DELETE", {kYear}, 10000, [&](const std::string &db, bool pooled) { DeleteOps(store, db, pooled); }, seed, 1);
    RunStrict(store, "WIDEN", {kYear, kOther}, 8000, [&](const std::string &db, bool pooled) { WidenOps(store, db, pooled, seed); }, seed, 1);
    RunOverwriteHeavy(store);
}

// An empty row (RowData{}, all zero bytes on disk) is a hole on every path since the rows-only records: the inline fold,
// a pass and the startup rebuild all fold nothing for it (Task 7 found the inline path counted it as a null in every
// column; that I3 exception is gone). Byte for byte, and every answer equals.
TEST_F(IndexerOracleTest, AnEmptyRowIsAHoleOnEveryPath) {
    XStore store;
    const Ops ops = [&](const std::string &db, bool) {
        Insert(store, db, {{Ts(kYear, 0), ExactRow(0, 40)}});
        Insert(store, db, {{Ts(kYear, 5), XStore_Proto::RowData()}});
    };
    ASSERT_NO_FATAL_FAILURE(BuildBoth(store, "IDXOREMPTY1", "IDXOREMPTY1REF", ops, OracleSeed()));
    const uint32_t width = WidthOf("IDXOREMPTY1", kYear);
    const auto got = YearBytes("IDXOREMPTY1", kYear), want = YearBytes("IDXOREMPTY1REF", kYear);
    EXPECT_TRUE(got.first == want.first) << ".zmap differs from the synchronous build" << FirstDiff(got.first, want.first, width);
    EXPECT_TRUE(got.second == want.second) << ".catidx differs from the synchronous build";
    ExpectSameAnswers(store, "IDXOREMPTY1", "IDXOREMPTY1REF", Ts(kYear, 0), Ts(kYear, 99), 0);
}

// Spec section 9's query oracle during lag: seeded pass delays (0-5 ms) while one writer inserts, rewrites and deletes
// rows of 5 blocks; after every acked call every aggregate on three columns and FindByValue equal a from-scratch scan of
// the year file (no index read), on the whole span and a random range. Every 5th step is an insert made with the passes
// held (from an idle indexer, so no held pass covers a row the writer needs): its rows are surely pending when queried
// (the random delays alone do not guarantee it, e.g. under TSan). Then the same after the indexer caught up.
TEST_F(IndexerOracleTest, QueriesEqualScansDuringLag) {
    const uint64_t seed = OracleSeed();
    SCOPED_TRACE("seed " + std::to_string(seed) + " (rerun with XSTORE_ORACLE_SEED)");
    const std::string db = "IDXORLAG";
    const uint64_t span = 5 * RowsPerBlock(kRowLength);
    XStore store;
    xstore::Indexer::SetConfigForTests({2, kPoolBacklog, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    SeededPassDelays(seed, 5000);
    std::mt19937_64 rng(seed);
    uint64_t replies = 0, noticed = 0;
    std::string first;
    const auto t0 = std::chrono::steady_clock::now();
    uint64_t step = 0, gated_steps = 0, gated_noticed = 0;
    for (; step < 120 && first.empty(); ++step) {
        const bool gated = step % 5 == 4;
        if (gated) {
            ASSERT_TRUE(Quiesce());
            gate_open_.store(false);
        }
        const uint64_t kind = gated ? 0 : rng() % 10;
        if (kind < 8) {   // 60 rows, contiguous (one run) or scattered; a row written before gets new values
            std::set<uint64_t> picked;
            const bool contiguous = rng() % 2 == 0;
            const uint64_t start = rng() % (span - 60);
            while (picked.size() < 60) picked.insert(contiguous ? start + picked.size() : rng() % span);
            Rows batch;
            for (uint64_t r : picked) batch.emplace_back(Ts(kYear, r), ExactRow(r, 12, step));
            Insert(store, db, batch);
            ASSERT_FALSE(HasFatalFailure()) << "step " << step;
        } else if (kind == 8) {
            static_cast<void>(store.UnaryDelete(Ts(kYear, rng() % span), db));
        } else {
            const uint64_t lo = rng() % (span - 30);
            static_cast<void>(store.RangeDelete(Ts(kYear, lo), Ts(kYear, lo + 29), db));
        }
        const std::vector<ScannedRow> rows = ScanYear(db, span);
        uint64_t lo = rng() % span, hi = rng() % span;
        if (lo > hi) std::swap(lo, hi);
        first = CompareWithScan(store, db, rows, 0, span - 1, replies, noticed);
        if (first.empty()) first = CompareWithScan(store, db, rows, lo, hi, replies, noticed);
        if (gated) {
            ++gated_steps;
            XStore_Proto::Sum_REP sum_rep;   // a sum reads every block: it met the held rows
            store.SumAggregate(Ts(kYear, 0), Ts(kYear, span - 1), db, 0, &sum_rep);
            if (CountOf(sum_rep.opstatus(), kPendingNotice) == 1) ++gated_noticed;
            gate_open_.store(true);
        }
    }
    EXPECT_TRUE(first.empty()) << "after step " << step << ": " << first;
    EXPECT_EQ(gated_noticed, gated_steps) << "a sum over rows held pending did not say so";
    EXPECT_GT(noticed, 0u) << "no reply met a pending block: the lag was never queried";
    std::printf("[ INFO     ] %llu steps (%llu held), %llu replies, %llu with the pending notice, %.1f s\n",
                static_cast<unsigned long long>(step), static_cast<unsigned long long>(gated_steps),
                static_cast<unsigned long long>(replies), static_cast<unsigned long long>(noticed),
                std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count());
    ASSERT_TRUE(Quiesce());
    xstore::Indexer::SetPassDelayForTests(nullptr);
    EXPECT_EQ(Map(db)->PendingBlockCount(), 0u);
    const std::vector<ScannedRow> rows = ScanYear(db, span);
    uint64_t after_noticed = 0;
    EXPECT_EQ(CompareWithScan(store, db, rows, 0, span - 1, replies, after_noticed), "");
    EXPECT_EQ(CompareWithScan(store, db, rows, 100, span - 100, replies, after_noticed), "");
    EXPECT_EQ(after_noticed, 0u) << "a notice after the indexer caught up";
}

// Inline fallback, backlog = 2 runs of 50 rows. First, deterministic: with the passes held by the gate two runs fill the
// backlog exactly; a third run is acked at once (no waiting for room), reserves nothing, marks nothing pending and is
// already in the zone map (folded inline). Then the stress half: 8 SYNC writers of 50-row runs, passes slowed (seeded
// 0-2 ms): the backlog is often full, never past its bound; at the end nothing is pending and the sidecars equal the
// synchronous build's bytes.
TEST_F(IndexerOracleTest, BacklogFullFallsBackInline) {
    const uint64_t seed = OracleSeed();
    SCOPED_TRACE("seed " + std::to_string(seed));
    constexpr uint64_t kWriters = 8, kRunRows = 50, kRunsPerWriter = 60;
    constexpr uint64_t kRunBytes = kRunRows * kRowLength, kLimit = 2 * kRunBytes;
    const std::string db = "IDXORFULL", ref_db = "IDXORFULLREF";
    XStore store;
    const Rows all = RowsOf(kYear, 0, kWriters * kRunsPerWriter * kRunRows, 20);
    const auto ref = ReferenceBytes(store, ref_db, all);
    xstore::Indexer::SetConfigForTests({2, kLimit, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    RunCheckpointer();   // 8 SYNC writers can fill the ring
    passes_ = 0;
    SeededPassDelays(seed, 2000);
    const auto run_rows = [&](uint64_t r) { return Rows(all.begin() + r * kRunRows, all.begin() + (r + 1) * kRunRows); };
    constexpr uint64_t kInlineRun = 100;   // rows 5000-5049, block 2: no lock in common with the held passes of block 0
    {
        gate_open_.store(false);
        Insert(store, db, run_rows(0));
        Insert(store, db, run_rows(1));
        ASSERT_EQ(xstore::Indexer::Instance().BacklogBytes(), kLimit) << "two handed-off runs must fill the backlog";
        const auto map = Map(db);
        ASSERT_NE(map, nullptr);
        ASSERT_EQ(map->PendingBlockCount(), 1u);
        auto third = std::async(std::launch::async, [&] { Insert(store, db, run_rows(kInlineRun)); });
        if (third.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
            gate_open_.store(true);   // the run waiting for room gets it once the held passes go on
            third.get();
            FAIL() << "a run met a full backlog and waited for room instead of folding inline";
        }
        third.get();
        // Checked with the passes still held: the backlog is as the two runs left it.
        const uint64_t backlog = xstore::Indexer::Instance().BacklogBytes();
        const bool third_pending = map->AnyPendingInRows(kInlineRun * kRunRows, (kInlineRun + 1) * kRunRows - 1);
        const size_t pending_blocks = map->PendingBlockCount();
        std::optional<std::vector<ZoneMapEntry>> stats;
        {
            const ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, Width(db));
            stats = z->TryGetBlockStats(2);
        }
        gate_open_.store(true);
        EXPECT_EQ(backlog, kLimit) << "the third run reserved backlog";
        EXPECT_FALSE(third_pending) << "the third run is pending";
        EXPECT_EQ(pending_blocks, 1u);
        ASSERT_TRUE(stats.has_value()) << "the inline run's block is not trusted";
        EXPECT_EQ((*stats)[0].valid_count, kRunRows) << "the inline run is not in the zone map";
        ASSERT_TRUE(Quiesce());
    }
    std::atomic<bool> writing{true};
    std::atomic<uint64_t> peak{0}, full{0}, samples{0};
    std::thread sampler([&] {
        while (writing.load()) {
            const uint64_t used = xstore::Indexer::Instance().BacklogBytes();
            peak.store(std::max(peak.load(), used));
            if (used + kRunBytes > kLimit) ++full;
            ++samples;
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });
    std::atomic<uint64_t> failures{0};
    std::vector<std::thread> writers;
    for (uint64_t w = 0; w < kWriters; ++w) {
        writers.emplace_back([&, w] {
            std::vector<uint64_t> runs;
            for (uint64_t r = w; r < kWriters * kRunsPerWriter; r += kWriters) {
                if (r != 0 && r != 1 && r != kInlineRun) runs.push_back(r);
            }
            std::mt19937_64 rng(seed + w);
            std::shuffle(runs.begin(), runs.end(), rng);
            for (uint64_t r : runs) {
                XStore_Proto::BatchInsert_REQ req;
                req.set_dbname(db);
                req.set_durabilitymode(XStore_Proto::SYNC);
                for (uint64_t i = r * kRunRows; i < (r + 1) * kRunRows; ++i) {
                    XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                    entry->set_timestamp(all[i].first);
                    *entry->mutable_data() = all[i].second;
                }
                std::string status;
                for (int attempt = 0; attempt < 10000; ++attempt) {
                    XStore_Proto::BatchInsert_REP rep;
                    store.BatchInsert(&req, &rep);
                    status = rep.insertstatus();
                    if (status.find(XStore::kWalOverloadedText) == std::string::npos) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
                if (status.find("successfully") == std::string::npos) ++failures;
            }
        });
    }
    for (std::thread &t : writers) t.join();
    writing.store(false);
    sampler.join();
    EXPECT_EQ(failures.load(), 0u);
    std::printf("[ INFO     ] backlog peak %llu of %llu bytes, full in %llu of %llu samples, %llu passes\n",
                static_cast<unsigned long long>(peak.load()), static_cast<unsigned long long>(kLimit),
                static_cast<unsigned long long>(full.load()), static_cast<unsigned long long>(samples.load()),
                static_cast<unsigned long long>(passes_.load()));
    EXPECT_GT(passes_.load(), 0u) << "no run was handed to the indexer";
    EXPECT_GT(full.load(), 0u) << "the backlog was never full: no run had to fold inline";
    EXPECT_LE(peak.load(), kLimit) << "the backlog went past its bound";
    ASSERT_TRUE(Quiesce());
    xstore::Indexer::SetPassDelayForTests(nullptr);
    EXPECT_EQ(Map(db)->PendingBlockCount(), 0u) << "rows left pending";
    const auto got = CleanBytes(db, Width(db));
    EXPECT_TRUE(got.first == ref.first) << ".zmap differs from the synchronous build" << FirstDiff(got.first, ref.first, Width(db));
    EXPECT_TRUE(got.second == ref.second) << ".catidx differs from the synchronous build" << FirstDiff(got.second, ref.second, 0);
}

// ---------------------------------------------------------------------------------------------------------------------
// RowsOnlyRecordTest (stage 3-4, Task 8): records created with the pool off carry observations (has_obs, folded by the
// writer from them); records the pool may take (HandOffToPool: ASYNC with long runs, or any with the tests' hand_off_sync)
// carry rows only (handed off, or folded by FoldStoredRows when the backlog is full).

#if defined(__GLIBCXX__) && defined(__x86_64__) && (!defined(_GLIBCXX_USE_CXX11_ABI) || _GLIBCXX_USE_CXX11_ABI)
// The four observation arrays (4 x 24 B) and zone_map_pad_to are back for has_obs records (pool off); a rows-only
// record leaves them empty. `replayed` still does the pad_to's job on replay.
static_assert(sizeof(XStore::WalRecord) == 248, "WalRecord layout changed: update this check deliberately");
#endif

using RowsOnlyRecordTest = IndexerPassTest;

TEST_F(RowsOnlyRecordTest, AppendRowAddsOneRowSlotAndNothingElse) {
    XStore::WalRecord record;
    record.row_length = kRowLength;
    const std::vector<XStore_Proto::RowData> rows = {MixedRow(1, 40), XStore_Proto::RowData(), MixedRow(2, 3)};
    record.ReserveRows(rows.size());
    EXPECT_GE(record.wire.capacity(), rows.size() * record.RowStride());
    for (size_t i = 0; i < rows.size(); ++i) {
        ASSERT_LE(rows[i].ByteSizeLong(), kRowLength);   // also caches the sizes AppendRow serializes with
        record.AppendRow(100 + i, rows[i]);
        EXPECT_EQ(record.wire.size(), (i + 1) * record.RowStride()) << "row " << i;
    }
    ASSERT_EQ(record.row_count, rows.size());
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(record.EpochAt(i), 100 + i);
        std::string slot = rows[i].SerializeAsString();
        slot.resize(kRowLength, '\0');
        EXPECT_TRUE(std::string(record.RowAt(i), kRowLength) == slot) << "row " << i;
    }
    EXPECT_FALSE(record.replayed);
}

// ClearObservations also clears has_obs. The replay loop does not call it: the WAL parsers set has_obs = false
// themselves next to replayed = true and leave the (always empty) arrays alone.
TEST_F(RowsOnlyRecordTest, ClearObservationsClearsHasObs) {
    XStore::WalRecord record;
    record.row_length = kRowLength;
    record.tracked_column_count = 40;
    record.has_obs = true;
    record.zone_map_pad_to = 40;
    const XStore_Proto::RowData row = MixedRow(1, 40);
    ASSERT_LE(row.ByteSizeLong(), kRowLength);   // also caches the sizes AppendRowWithObs serializes with
    record.ReserveRowsWithObs(1, 40);
    record.AppendRowWithObs(100, row);
    ASSERT_EQ(record.obs_end.size(), 1u);
    record.ClearObservations();
    EXPECT_FALSE(record.has_obs);
    EXPECT_TRUE(record.obs_end.empty());
    EXPECT_TRUE(record.zmap_values.empty());
    EXPECT_TRUE(record.catidx_vals.empty());
    EXPECT_EQ(record.zone_map_pad_to, 0u);
}

// A 1000-row MultiRowInsert (with an empty row and a row with an unset column) and two UNARY inserts, pool off (so the
// writer folds the request thread's observations, has_obs): the sidecars equal fresh ones filled by FoldStoredRows over
// the same stored rows, byte for byte (the empty row is a hole to both).
TEST_F(RowsOnlyRecordTest, TheWritersInlineFoldEqualsFoldStoredRowsOverTheSameRows) {
    xstore::Indexer::SetConfigForTests({1, 0});
    const std::string db = "ROWSONLY";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows = Dense(0, 1000, 40);
    rows[7].second = XStore_Proto::RowData();   // all-zero bytes on disk: a hole to every fold
    rows[8].second.mutable_fielddata(0)->Clear();   // an unset column: a null
    Insert(store, db, rows);
    for (const uint64_t r : {1000ULL, 2050ULL}) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(Jan1(kYear) + r, MixedRow(r, 33), db, &rep, XStore_Proto::SYNC);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    }
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    EXPECT_EQ(map->PendingBlockCount(), 0u) << "pool off: every run is folded before the ack";
    const uint32_t width = Width(db);
    const auto got = CleanBytes(db, width);
    ASSERT_NE(got.first.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos) << "an empty zone map proves nothing";
    static_cast<void>(ResetSidecars(db, width));
    ASSERT_NE(CleanBytes(db, width), got) << "the reset sidecars must differ, or the comparison proves nothing";
    {
        const int fd = open(YearPath(db, ".XSTORE").c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
        CatIdxFile::Pin c = XStore::PinCatIdx(DbPath(db), kYear, kRowLength, width);
        const bool folded = XStore::FoldStoredRows(fd, kRowLength, width, c->TrackedColumnCount(), {{0, 1001}, {2050, 1}}, *z, *c);
        close(fd);
        ASSERT_TRUE(folded);
    }
    const auto want = CleanBytes(db, width);
    EXPECT_TRUE(got.first == want.first) << ".zmap of the inline fold differs from FoldStoredRows";
    EXPECT_TRUE(got.second == want.second) << ".catidx of the inline fold differs from FoldStoredRows";
}

// The inline fold reads the run back; if that read fails the rows are written (the insert succeeds) but not indexed:
// the run stays pending (no query trusts its block), the block is inexact, both sidecars are dirty on disk, and every
// answer is still exact (scanned). The inline re-read fold is reached through a backlog smaller than any run (with the
// pool off the request thread builds observations and nothing is read back).
TEST_F(RowsOnlyRecordTest, AFailedReadBackKeepsTheRunUntrustedAndTheSidecarsDirty) {
    xstore::Indexer::SetConfigForTests({1, 1024, /*min_run_rows=*/1, /*hand_off_sync=*/true});
    const std::string db = "ROWSONLYF";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, Dense(0, 10, 40));
    const uint32_t width = Width(db);
    static_cast<void>(CleanBytes(db, width));
    ASSERT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ASSERT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    const auto map = xstore::PendingMaps::Find(Key(db));
    ASSERT_NE(map, nullptr);
    const std::shared_ptr<void> unmark(nullptr, [&](void *) { map->ClearRows(20, 10); });   // also on an early ASSERT return
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 0, 1000, EIO);
        Insert(store, db, Dense(20, 10, 40));
        ASSERT_GT(fail.Failures(), 0) << "the inline fold did not read the run back";
    }
    const std::vector<xstore::IndexRun> runs = map->PendingRunsOfBlock(0);
    ASSERT_EQ(runs.size(), 1u);
    EXPECT_EQ(runs[0].first_row, 20u);
    EXPECT_EQ(runs[0].row_count, 10u);
    {
        ZoneMapFile::Pin z = XStore::PinZoneMap(DbPath(db), kYear, kRowLength, width);
        EXPECT_FALSE(z->TryGetBlockStats(0).has_value()) << "a block with an unindexed run is trusted";
        EXPECT_TRUE(z->BlockInexact(0));
    }
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    double expected = 0;
    for (const Rows &part : {Dense(0, 10, 40), Dense(20, 10, 40)}) {
        for (const auto &[ts, row] : part) {
            if (row.fielddata(0).has_doubledata()) expected += row.fielddata(0).doubledata();
        }
    }
    XStore_Proto::Sum_REP sum;
    store.SumAggregate(Jan1(kYear), Jan1(kYear) + 29, db, 0, &sum);
    EXPECT_EQ(sum.opresult(), expected) << sum.opstatus();
}

}  // namespace
