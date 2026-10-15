// Live index widening, M4 stage 2b (Tech Reference 4.18 "Live widening"): XStore::LiveWidenOneYear rebuilds a year's
// .zmap and .catidx in place while requests run, chunk by chunk under the data file's READ range lock (timed:
// FileRangeLock::TryLockRangeFor). Nothing in production calls it yet (stage 3 adds the trigger), so these tests drive
// it directly. References: the startup rebuild (InitDB -> RebuildYearIndexes) of a sparse copy of the same year file,
// and full scans of the data file taken under a READ lock.
#include <gtest/gtest.h>
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_RowScan.h"
#include "XStore/XStore_WireEncode.h"
#include "XStore/XStore_WalRing.h"
#include "include/DataStructs/FileRangeLock.h"
#include "XStoreTestSupport.h"
#include "XStoreSyscallFault.h"
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <future>
#include <filesystem>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace {
using namespace xstore_test;
using Result = XStore::LiveWidenResult;
using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;
constexpr auto kRead = FileRangeLock::LockType::READ;
constexpr auto kWrite = FileRangeLock::LockType::WRITE;

// ---- FileRangeLock::TryLockRangeFor ---------------------------------------------------------------------------------

int64_t MsSince(Clock::time_point t0) {
    return std::chrono::duration_cast<milliseconds>(Clock::now() - t0).count();
}

TEST(FileRangeLockTimedTest, SucceedsAtOnceWhenTheRangeIsFree) {
    FileRangeLock lock;
    const Clock::time_point t0 = Clock::now();
    ASSERT_TRUE(lock.TryLockRangeFor(0, 100, kWrite, milliseconds(2000)));
    EXPECT_LT(MsSince(t0), 500);
    lock.UnlockRange(0, 100, kWrite);
}

// The waiter runs on its own thread with a watchdog: a TryLockRangeFor that ignores its timeout must fail the test,
// not hang it.
TEST(FileRangeLockTimedTest, TimesOutWhileAConflictingWriteIsHeld) {
    FileRangeLock lock;
    lock.LockRange(50, 10, kWrite);
    auto waiter = std::async(std::launch::async, [&] {
        const Clock::time_point t0 = Clock::now();
        const bool got = lock.TryLockRangeFor(0, 100, kRead, milliseconds(100));
        return std::make_pair(got, MsSince(t0));
    });
    if (waiter.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        ADD_FAILURE() << "TryLockRangeFor ignored its timeout";
        lock.UnlockRange(50, 10, kWrite);
        if (waiter.get().first) lock.UnlockRange(0, 100, kRead);
        return;
    }
    const auto [got, elapsed_ms] = waiter.get();
    EXPECT_FALSE(got);
    EXPECT_GE(elapsed_ms, 90);
    lock.UnlockRange(50, 10, kWrite);
}

TEST(FileRangeLockTimedTest, SucceedsWhenTheConflictIsReleasedWithinTheTimeout) {
    FileRangeLock lock;
    lock.LockRange(0, 10, kWrite);
    std::thread releaser([&] {
        std::this_thread::sleep_for(milliseconds(50));
        lock.UnlockRange(0, 10, kWrite);
    });
    const Clock::time_point t0 = Clock::now();
    EXPECT_TRUE(lock.TryLockRangeFor(5, 10, kWrite, milliseconds(3000)));
    const int64_t elapsed_ms = MsSince(t0);
    releaser.join();
    EXPECT_GE(elapsed_ms, 40);
    EXPECT_LT(elapsed_ms, 2500);
    lock.UnlockRange(5, 10, kWrite);
}

TEST(FileRangeLockTimedTest, ReadsShareAndDisjointRangesDoNotBlock) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kRead);
    EXPECT_TRUE(lock.TryLockRangeFor(50, 100, kRead, milliseconds(0)));
    EXPECT_TRUE(lock.TryLockRangeFor(150, 10, kWrite, milliseconds(0)));
    EXPECT_FALSE(lock.TryLockRangeFor(149, 1, kWrite, milliseconds(0))) << "overlaps the second READ";
    lock.UnlockRange(0, 100, kRead);
    lock.UnlockRange(50, 100, kRead);
    lock.UnlockRange(150, 10, kWrite);
}

// A timed-out attempt registers nothing: once the conflict is gone, a WRITE over the whole file ("rest of file",
// length 0) is granted at once. Same for the RAII guard.
TEST(FileRangeLockTimedTest, ATimeoutLeavesNoRegistrationBehind) {
    FileRangeLock lock;
    lock.LockRange(10, 10, kWrite);
    auto waiter = std::async(std::launch::async, [&] {
        return std::make_pair(lock.TryLockRangeFor(0, 100, kRead, milliseconds(20)), lock.TryLockRangeFor(0, 0, kRead, milliseconds(0)));
    });
    if (waiter.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        ADD_FAILURE() << "TryLockRangeFor ignored its timeout";
        lock.UnlockRange(10, 10, kWrite);
        const auto got = waiter.get();
        if (got.first) lock.UnlockRange(0, 100, kRead);
        if (got.second) lock.UnlockRange(0, 0, kRead);
        return;
    }
    const auto [ranged, rest_of_file] = waiter.get();
    EXPECT_FALSE(ranged);
    EXPECT_FALSE(rest_of_file);
    lock.UnlockRange(10, 10, kWrite);
    ASSERT_TRUE(lock.TryLockRangeFor(0, 0, kWrite, milliseconds(0)));
    lock.UnlockRange(0, 0, kWrite);

    const std::string key = "/live_widen_test/guard_key";
    FileLockRegistry::Get(key).LockRange(0, 10, kWrite);
    {
        TimedFileRangeLockGuard guard(key, 5, 10, kRead, milliseconds(10));
        EXPECT_FALSE(guard.owns_lock());
    }
    FileLockRegistry::Get(key).UnlockRange(0, 10, kWrite);
    {
        TimedFileRangeLockGuard guard(key, 5, 10, kRead, milliseconds(0));
        EXPECT_TRUE(guard.owns_lock());
    }
    TimedFileRangeLockGuard whole(key, 0, 0, kWrite, milliseconds(0));
    EXPECT_TRUE(whole.owns_lock()) << "a guard leaked its range";
}

// ---- Helpers --------------------------------------------------------------------------------------------------------

unsigned long long Jan1(unsigned short year) {
    unsigned long long days = 0;
    for (unsigned y = 1970; y < year; ++y) days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    return days * 86400ULL;
}

std::string ReadFileBytes(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

template <typename Header>
Header ReadHeader(const std::string &path) {
    Header h{};
    const int fd = open(path.c_str(), O_RDONLY);
    EXPECT_GE(fd, 0) << path;
    EXPECT_EQ(pread(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h))) << path;
    close(fd);
    return h;
}

// Copies only the data extents of a sparse year file, keeping its size.
void SparseCopy(const std::string &from, const std::string &to) {
    const int in = open(from.c_str(), O_RDONLY);
    ASSERT_GE(in, 0) << from;
    const int out = open(to.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(out, 0) << to;
    const off_t size = lseek(in, 0, SEEK_END);
    ASSERT_EQ(ftruncate(out, size), 0);
    std::vector<char> buf(1 << 20);
    off_t pos = 0;
    while (pos < size) {
        const off_t data = lseek(in, pos, SEEK_DATA);
        if (data < 0) break;
        const off_t hole = lseek(in, data, SEEK_HOLE);
        for (off_t p = data; p < hole;) {
            const ssize_t n = pread(in, buf.data(), static_cast<size_t>(std::min<off_t>(hole - p, static_cast<off_t>(buf.size()))), p);
            ASSERT_GT(n, 0);
            ASSERT_EQ(pwrite(out, buf.data(), static_cast<size_t>(n), p), n);
            p += n;
        }
        pos = hole;
    }
    close(in);
    close(out);
}

// Numeric coercion of a column, the zone map's rule (FData cases 1..12; bool, string and bytes are not numeric).
bool NumericOf(const XStore_Proto::FData &f, double &out) {
    long double v = 0;
    switch (f.oneOfData_case()) {
        case XStore_Proto::FData::kDoubleData: v = f.doubledata(); break;
        case XStore_Proto::FData::kFloatData: v = f.floatdata(); break;
        case XStore_Proto::FData::kInt32Data: v = f.int32data(); break;
        case XStore_Proto::FData::kInt64Data: v = f.int64data(); break;
        case XStore_Proto::FData::kUint32Data: v = f.uint32data(); break;
        case XStore_Proto::FData::kUint64Data: v = f.uint64data(); break;
        case XStore_Proto::FData::kSint32Data: v = f.sint32data(); break;
        case XStore_Proto::FData::kSint64Data: v = f.sint64data(); break;
        case XStore_Proto::FData::kFixed32Data: v = f.fixed32data(); break;
        case XStore_Proto::FData::kFixed64Data: v = f.fixed64data(); break;
        case XStore_Proto::FData::kSfixed32Data: v = f.sfixed32data(); break;
        case XStore_Proto::FData::kSfixed64Data: v = f.sfixed64data(); break;
        default: return false;
    }
    out = static_cast<double>(v);
    return true;
}

// Every present row of [lo, hi] of an open year file, parsed.
std::vector<std::pair<uint64_t, XStore_Proto::RowData>> ReadRows(int fd, uint64_t lo, uint64_t hi, uint16_t row_length) {
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    const bool complete = ForEachDataRow(fd, hi + 1, row_length, [&](uint64_t r, const char *buf) {
        if (buf[0] == 0) return;
        XStore_Proto::RowData row;
        const size_t real_len = XStoreWireEncode::RealRowDataLength(buf, row_length);
        if (real_len > 0) static_cast<void>(row.ParseFromArray(buf, static_cast<int>(real_len)));
        rows.emplace_back(r, std::move(row));
    }, lo);
    EXPECT_TRUE(complete);
    return rows;
}

// A row of `columns` doubles (base + c), with a null column wherever (base + c) % 13 == 0. Integer values keep every
// sum exact whatever the summation order.
XStore_Proto::RowData ValueRow(uint64_t base, int columns) {
    XStore_Proto::RowData row;
    for (int c = 0; c < columns; ++c) {
        XStore_Proto::FData *f = row.add_fielddata();
        const uint64_t v = base + static_cast<uint64_t>(c);
        if (v % 13 != 0) f->set_doubledata(static_cast<double>(v));
    }
    return row;
}

// A row cycling through every FData type, with nulls (the stage-2a reference test's shape).
XStore_Proto::RowData MixedRow(uint64_t i, int columns) {
    XStore_Proto::RowData row;
    for (int c = 0; c < columns; ++c) {
        XStore_Proto::FData *f = row.add_fielddata();
        const uint64_t v = i * 31 + static_cast<uint64_t>(c);
        if ((i + static_cast<uint64_t>(c)) % 17 == 0) continue;
        switch (c % 16) {
            case 0: f->set_doubledata(static_cast<double>(v) * 0.5); break;
            case 1: f->set_floatdata(static_cast<float>(v) * 0.25f); break;
            case 2: f->set_int32data(-static_cast<int32_t>(v % 100000)); break;
            case 3: f->set_int64data(static_cast<int64_t>(v) * 1000003); break;
            case 4: f->set_uint32data(static_cast<uint32_t>(v)); break;
            case 5: f->set_uint64data(0xFFFFFFFFFFFF0000ULL + v); break;
            case 6: f->set_sint32data(-static_cast<int32_t>(v % 777)); break;
            case 7: f->set_sint64data(-static_cast<int64_t>(v)); break;
            case 8: f->set_fixed32data(static_cast<uint32_t>(v * 3)); break;
            case 9: f->set_fixed64data(v * 5); break;
            case 10: f->set_sfixed32data(-static_cast<int32_t>(v % 1000)); break;
            case 11: f->set_sfixed64data(-static_cast<int64_t>(v * 7)); break;
            case 12: f->set_booldata(v % 2 == 0); break;
            case 13: f->set_stringdata("sym" + std::to_string(v % 23)); break;
            case 14: f->set_bytesdata(std::string("\x01\x00\x02", 3) + std::to_string(v % 5)); break;
            default: f->set_doubledata(-static_cast<double>(v)); break;
        }
    }
    return row;
}

using Rows = std::vector<std::pair<unsigned long long, XStore_Proto::RowData>>;

class XStoreLiveWidenTest : public ::testing::Test {
protected:
    static constexpr uint16_t kRowLength = 2048; // 1024 rows per zone-map block; room for 100 double columns
    static constexpr unsigned short kYear = 1980;
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_live_widen_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        XStore::ClearLiveWidenRequestsForTests(); // other tests' growths are recorded too (nothing runs a pass there)
    }
    void TearDown() override {
        XStore::bg_cancel_.store(false);
        XStore::ClearLiveWidenRequestsForTests();
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        scratch.reset();
    }

    static uint64_t Rpb() { return RowsPerBlock(kRowLength); }
    static unsigned long long Epoch(uint64_t row) { return Jan1(kYear) + row; }
    std::string DbPath(const std::string &db) const { return (scratch->test_root / "DataStore" / db).string(); }
    std::string YearPath(const std::string &db, const char *ext, unsigned short year = kYear) const {
        return DbPath(db) + "/" + std::to_string(year) + ext;
    }

    static void Insert(XStore &store, const std::string &db, const Rows &rows,
                       XStore_Proto::WalDurabilityMode mode = XStore_Proto::SYNC) {
        for (size_t from = 0; from < rows.size(); from += 4000) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(db);
            req.set_durabilitymode(mode);
            for (size_t i = from; i < std::min(rows.size(), from + 4000); ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(rows[i].first);
                *entry->mutable_data() = rows[i].second;
            }
            XStore_Proto::BatchInsert_REP rep;
            store.BatchInsert(&req, &rep);
            ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
        }
    }

    // A DB whose kYear index files are 64 columns wide (created by a 10-column first row) while its rows, and so its
    // configured width, reach `width` columns: rows 1..rows-1 are ValueRow(row * 7 % 100000), every 5th one only 40
    // columns wide.
    void SeedNarrowIndexWideRows(XStore &store, const std::string &db, uint64_t rows, int width) {
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
        Rows batch;
        for (uint64_t r = 1; r < rows; ++r) batch.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, r % 5 == 0 ? 40 : width));
        Insert(store, db, batch);
        ASSERT_EQ(XStore::GetOrOpenZoneMap(DbPath(db), kYear, kRowLength).TrackedColumnCount(), 64u);
        ASSERT_EQ(XStore::GetOrOpenCatIdx(DbPath(db), kYear, kRowLength).TrackedColumnCount(), 64u);
    }

    // Clean stop (indexes clean on disk), then a "new process": empty registries.
    static void CleanRestartState() {
        XStore::Shutdown();
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
    }

    // The startup rebuild of db's kYear file at `width`: a second DB (configured `width` wide by one row in 1975) gets
    // a sparse copy of the year file and no sidecars, and InitDB rebuilds it (RebuildYearIndexes). InitDB also
    // rebuilds whatever else is stale, db's own dirty indexes included.
    void BuildStartupReference(XStore &store, const std::string &db, const std::string &ref_db, int width) {
        CreateTestDb(store, ref_db, XStore_Proto::SECOND, kRowLength);
        Insert(store, ref_db, {{Jan1(1975), ValueRow(1, width)}});
        SparseCopy(YearPath(db, ".XSTORE"), YearPath(ref_db, ".XSTORE"));
        ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle()); // nothing left to replay
        store.InitDB();
        EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(ref_db, ".zmap")).tracked_column_count, static_cast<uint32_t>(width));
    }

    void ExpectSameFilesAs(const std::string &db, const std::string &ref_db) {
        for (const char *ext : {".zmap", ".catidx"}) {
            SCOPED_TRACE(ext);
            const std::string got = ReadFileBytes(YearPath(db, ext));
            const std::string want = ReadFileBytes(YearPath(ref_db, ext));
            EXPECT_EQ(got.size(), want.size());
            EXPECT_TRUE(got == want) << "differs from the startup rebuild";
        }
    }

    void RunIdleWiden(XStore &store, int width, const std::string &db, const std::string &ref_db);
    // The concurrent oracle of section 2, with the builder's worker count and group size.
    void RunConcurrentOracle(const std::string &db, unsigned workers, uint64_t group_bytes);

    // Answers on [lo, hi] for every listed column, against a scan of the data file (no concurrent writers here).
    void ExpectAnswersMatchTheData(XStore &store, const std::string &db, uint64_t lo, uint64_t hi,
                                   const std::vector<int> &columns, unsigned short year = kYear) {
        const auto at = [year](uint64_t row) { return Jan1(year) + row; };
        const int fd = open(YearPath(db, ".XSTORE", year).c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        const auto rows = ReadRows(fd, lo, hi, kRowLength);
        close(fd);
        for (int c : columns) {
            SCOPED_TRACE("column " + std::to_string(c));
            double max = -INFINITY, sum = 0;
            uint64_t count = 0;
            const XStore_Proto::FData *some_value = nullptr;
            uint64_t some_value_rows = 0;
            for (const auto &[r, row] : rows) {
                double v = 0;
                if (c < row.fielddata_size() && NumericOf(row.fielddata(c), v)) {
                    max = std::max(max, v);
                    sum += v;
                    ++count;
                    if (some_value == nullptr) some_value = &row.fielddata(c);
                }
            }
            ASSERT_GT(count, 0u);
            for (const auto &[r, row] : rows) {
                if (c < row.fielddata_size() && row.fielddata(c).SerializeAsString() == some_value->SerializeAsString()) ++some_value_rows;
            }
            XStore_Proto::Max_REP max_rep;
            store.MaxAggregate(at(lo), at(hi), db, static_cast<unsigned short>(c), &max_rep);
            double got_max = 0;
            ASSERT_TRUE(NumericOf(max_rep.opresult(), got_max)) << max_rep.opstatus();
            EXPECT_EQ(got_max, max);
            XStore_Proto::Sum_REP sum_rep;
            store.SumAggregate(at(lo), at(hi), db, static_cast<unsigned short>(c), &sum_rep);
            EXPECT_NEAR(sum_rep.opresult(), sum, 1e-9 * std::max(1.0, std::fabs(sum))) << sum_rep.opstatus();
            std::string buf;
            store.FindByValue(at(lo), at(hi), db, c, *some_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                              [&](std::string &&chunk, bool) { buf += chunk; });
            XStore_Proto::RangeQuery_REP rep;
            ASSERT_TRUE(rep.ParseFromString(buf));
            EXPECT_EQ(static_cast<uint64_t>(rep.queryresponse_size()), some_value_rows);
        }
    }
};

constexpr const char *kNotIndexed = "not yet indexed";

// ---- 1. Idle correctness: byte-identical to the startup rebuild -----------------------------------------------------

// Dense mixed-type rows (every 10th one `width` columns wide), an overwrite and a delete (the old index carries an
// inexact block; a rebuild starts exact), far-apart sparse rows, a partial last block of data.
void XStoreLiveWidenTest::RunIdleWiden(XStore &store, int width, const std::string &db, const std::string &ref_db) {
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), MixedRow(0, 10)}}); // index files born 64 wide
    Rows rows;
    for (uint64_t i = 1; i < 5500; ++i) rows.emplace_back(Epoch(i), MixedRow(i, i % 10 == 0 ? width : 40 + static_cast<int>(i % 9)));
    for (uint64_t r : {20000ULL, 20001ULL, 50000ULL, 1000000ULL, 3000000ULL}) rows.emplace_back(Epoch(r), MixedRow(r, width));
    Insert(store, db, rows);
    Insert(store, db, {{Epoch(18), MixedRow(999, 12)}});
    ASSERT_NE(store.UnaryDelete(Epoch(17), db).deletestatus().find("[INFO]"), std::string::npos);

    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    ASSERT_EQ(zmap.TrackedColumnCount(), 64u);
    ASSERT_EQ(catidx.TrackedColumnCount(), 64u);
    const std::vector<int> columns = {0, 2, 64, 66, width - 4};

    // Before: right answers; a column past 64 is answered by a scan and says so.
    ExpectAnswersMatchTheData(store, db, 0, 5499, columns);
    XStore_Proto::Max_REP before;
    store.MaxAggregate(Epoch(0), Epoch(5499), db, 64, &before);
    EXPECT_NE(before.opstatus().find(kNotIndexed), std::string::npos) << before.opstatus();

    XStore::LiveWidenStats stats;
    ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, static_cast<uint32_t>(width), true, true, nullptr, &stats),
              Result::kDone);
    EXPECT_TRUE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_TRUE(stats.zmap_began && stats.catidx_began);
    EXPECT_FALSE(stats.zmap_busy || stats.catidx_busy);
    EXPECT_EQ(stats.rows_folded, 5500u + 5u - 1u) << "every present row folded exactly once";
    EXPECT_GT(stats.chunks, 2u);
    EXPECT_EQ(stats.lock_timeouts, 0u);
    // Same objects (registry entries are never replaced), now wide, clean on disk.
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(path, kYear, kRowLength), &zmap);
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(path, kYear, kRowLength), &catidx);
    EXPECT_FALSE(zmap.Rebuilding());
    EXPECT_FALSE(catidx.Rebuilding());
    EXPECT_EQ(zmap.TrackedColumnCount(), static_cast<uint32_t>(width));
    EXPECT_EQ(catidx.TrackedColumnCount(), static_cast<uint32_t>(width));
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    auto block0 = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(block0.has_value());
    EXPECT_EQ(block0->size(), static_cast<size_t>(width));
    EXPECT_GT((*block0)[64].valid_count, 0u) << "a new column is summarized";

    // After: right answers, and the new columns are indexed (no scan notice).
    ExpectAnswersMatchTheData(store, db, 0, 5499, columns);
    XStore_Proto::Max_REP after;
    store.MaxAggregate(Epoch(0), Epoch(5499), db, 64, &after);
    EXPECT_EQ(after.opstatus(), "SUCCESS");

    BuildStartupReference(store, db, ref_db, width);
    ExpectSameFilesAs(db, ref_db);
}

TEST_F(XStoreLiveWidenTest, IdleWidenTo70IsByteIdenticalToTheStartupRebuild) {
    XStore store;
    RunIdleWiden(store, 70, "LWIDLE70", "LWIDLE70REF");
}

TEST_F(XStoreLiveWidenTest, IdleWidenTo100IsByteIdenticalToTheStartupRebuild) {
    XStore store;
    RunIdleWiden(store, 100, "LWIDLE100", "LWIDLE100REF");
}

// ---- 2. Concurrent oracle -------------------------------------------------------------------------------------------

// Answers checked while 4 writers (Unary/Batch/Range inserts in SYNC/ASYNC/BYPASS, overwrites, duplicate timestamps,
// all three deletes) race the builder below, inside and above its moving frontier. A query thread holds a READ lock on
// the data-file range it asks about (writers of those rows wait; the builder, also a reader, does not), so its scan of
// the data file is the exact answer the store must give at that instant: before, during and after the widen. At the
// end every tracked column of every exact block equals a startup rebuild of the final data, and the bloom holds every
// present value.
void XStoreLiveWidenTest::RunConcurrentOracle(const std::string &db, unsigned workers, uint64_t group_bytes) {
    XStore store;
    constexpr int kWidth = 70;
    const uint64_t rpb = Rpb();
    constexpr uint64_t kBlocks = 96;
    const uint64_t rows = kBlocks * rpb;
    // Even rows only: most writer traffic is fresh rows in the odd holes, which keeps blocks exact (an overwrite or a
    // delete flags its block and takes it out of the final block comparison).
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
    {
        Rows seed;
        for (uint64_t r = 2; r < rows; r += 2) seed.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, r % 10 == 0 ? 40 : kWidth));
        Insert(store, db, seed);
    }
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    ASSERT_EQ(zmap.TrackedColumnCount(), 64u);

    std::atomic<bool> running{true};
    std::atomic<int> writes{0};
    std::atomic<int> queries{0};
    std::atomic<int> mismatches{0};
    std::mutex fresh_mutex;
    std::set<uint64_t> fresh_used;
    auto frontier = [&]() -> uint64_t {
        const uint64_t f = zmap.FrontierForTests();
        return f == ZoneMapFile::kNoRebuild ? (zmap.TrackedColumnCount() == 64 ? 0 : rows) : std::min(f, rows);
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            XStore w;
            std::mt19937_64 rng(4242 + t);
            auto value = [&] { return rng() % 90000; };
            while (running.load()) {
                const int64_t f = static_cast<int64_t>(frontier());
                const uint64_t near = static_cast<uint64_t>(std::clamp<int64_t>(f + static_cast<int64_t>(rng() % 3000) - 1000, 1,
                                                                                 static_cast<int64_t>(rows) - 1));
                const uint64_t anywhere = 1 + rng() % (rows - 1);
                const uint64_t target = rng() % 2 ? near : anywhere;
                const XStore_Proto::WalDurabilityMode mode =
                    std::array<XStore_Proto::WalDurabilityMode, 3>{XStore_Proto::SYNC, XStore_Proto::ASYNC, XStore_Proto::BYPASS}[rng() % 3];
                const unsigned op = static_cast<unsigned>(rng() % 100);
                if (op < 55) { // fresh odd rows, one Unary or a scattered Batch
                    std::vector<uint64_t> picked;
                    {
                        std::lock_guard<std::mutex> lock(fresh_mutex);
                        for (uint64_t i = 0; i < 1 + (op % 4) && (target | 1) + i * 64 < rows; ++i) {
                            if (fresh_used.insert((target | 1) + i * 64).second) picked.push_back((target | 1) + i * 64);
                        }
                    }
                    if (picked.empty()) continue;
                    if (picked.size() == 1) {
                        XStore_Proto::UnaryInsert_REP rep;
                        w.UnaryInsert(Epoch(picked[0]), ValueRow(value(), rng() % 4 == 0 ? 40 : kWidth), db, &rep, mode);
                    } else {
                        XStore_Proto::BatchInsert_REQ req;
                        req.set_dbname(db);
                        req.set_durabilitymode(mode);
                        for (uint64_t r : picked) {
                            XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                            e->set_timestamp(Epoch(r));
                            *e->mutable_data() = ValueRow(value(), kWidth);
                        }
                        XStore_Proto::BatchInsert_REP rep;
                        w.BatchInsert(&req, &rep);
                    }
                } else if (op < 63) { // overwrite one row
                    XStore_Proto::UnaryInsert_REP rep;
                    w.UnaryInsert(Epoch(target), ValueRow(value(), kWidth), db, &rep, mode);
                } else if (op < 68) { // a run of consecutive rows (one record, may span blocks)
                    XStore_Proto::RangeInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(mode);
                    for (uint64_t i = 0; i < 1 + rng() % 30 && target + i < rows; ++i) {
                        XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
                        e->set_timestamp(Epoch(target + i));
                        *e->mutable_data() = ValueRow(value(), kWidth);
                    }
                    XStore_Proto::RangeInsert_REP rep;
                    w.RangeInsert(&req, &rep);
                } else if (op < 71) { // duplicate timestamps inside one request
                    XStore_Proto::BatchInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(mode);
                    for (int i = 0; i < 2; ++i) {
                        XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                        e->set_timestamp(Epoch(target));
                        *e->mutable_data() = ValueRow(value(), kWidth);
                    }
                    XStore_Proto::BatchInsert_REP rep;
                    w.BatchInsert(&req, &rep);
                } else if (op < 73) {
                    static_cast<void>(w.UnaryDelete(Epoch(target), db));
                } else if (op < 75) {
                    XStore_Proto::BatchDelete_REQ req;
                    req.set_dbname(db);
                    for (uint64_t i = 0; i < 3 && target + i * 50 < rows; ++i) req.add_timestamp(Epoch(target + i * 50));
                    static_cast<void>(w.BatchDelete(&req));
                } else if (op < 77) { // a range: one whole block (the reset path) or a cut range
                    uint64_t lo = target / rpb * rpb, hi = lo + rpb - 1;
                    if (op == 76) {
                        lo = target;
                        hi = std::min(rows - 1, target + rng() % 300);
                    }
                    static_cast<void>(w.RangeDelete(Epoch(std::max<uint64_t>(lo, 1)), Epoch(hi), db));
                } else {
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                    continue;
                }
                ++writes;
            }
            // Everything this thread enqueued (ASYNC included) is applied before the final comparison.
            WaitForWalDrained(w, db, Epoch(rows + 10 + static_cast<uint64_t>(t)));
        });
    }

    for (int q = 0; q < 2; ++q) {
        threads.emplace_back([&, q] {
            XStore r;
            std::mt19937_64 rng(777 + q);
            const int qfd = open(data_path.c_str(), O_RDONLY);
            ASSERT_GE(qfd, 0);
            const std::vector<int> columns = {0, 7, 39, 63, 64, 65, 69};
            while (running.load()) {
                // Whole blocks (zone-map pushdown) or a cut range, up to 12 blocks.
                const uint64_t first_block = rng() % kBlocks;
                const uint64_t nblocks = 1 + rng() % std::min<uint64_t>(12, kBlocks - first_block);
                uint64_t lo = first_block * rpb, hi = (first_block + nblocks) * rpb - 1;
                if (rng() % 3 == 0) {
                    lo += rng() % rpb;
                    hi = std::max(lo, hi - rng() % rpb);
                }
                const int c = columns[rng() % columns.size()];
                const unsigned op = static_cast<unsigned>(rng() % 9);
                FileRangeLockGuard hold(data_path, lo * kRowLength, (hi - lo + 1) * kRowLength, kRead);
                const auto data = ReadRows(qfd, lo, hi, kRowLength);
                std::vector<std::pair<uint64_t, double>> values;
                for (const auto &[row_index, row] : data) {
                    double v = 0;
                    if (c < row.fielddata_size() && NumericOf(row.fielddata(c), v)) values.emplace_back(row_index, v);
                }
                double sum = 0, sum_sq = 0, mn = INFINITY, mx = -INFINITY;
                for (const auto &[row_index, v] : values) {
                    sum += v;
                    sum_sq += v * v;
                    mn = std::min(mn, v);
                    mx = std::max(mx, v);
                }
                const double n = static_cast<double>(values.size());
                const double variance = values.size() > 1 ? std::max(0.0, (sum_sq - sum * sum / n) / (n - 1.0)) : 0.0;
                const auto bad = [&](const std::string &what) {
                    if (mismatches.fetch_add(1) < 10) {
                        ADD_FAILURE() << what << " op " << op << " column " << c << " rows [" << lo << ", " << hi
                                      << "] frontier " << frontier() << " valid " << values.size();
                    }
                };
                const auto near = [](double a, double b) { return std::fabs(a - b) <= 1e-9 * std::max(1.0, std::fabs(b)); };
                const bool success_expected = !values.empty();
                const unsigned short col = static_cast<unsigned short>(c);
                switch (op) {
                    case 0: {
                        XStore_Proto::Min_REP rep;
                        r.MinAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && rep.opresult().doubledata() != mn)) bad("MIN " + rep.opstatus());
                        break;
                    }
                    case 1: {
                        XStore_Proto::Max_REP rep;
                        r.MaxAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && rep.opresult().doubledata() != mx)) bad("MAX " + rep.opstatus());
                        break;
                    }
                    case 2: {
                        XStore_Proto::Sum_REP rep;
                        r.SumAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && rep.opresult() != sum)) bad("SUM " + rep.opstatus());
                        break;
                    }
                    case 3: {
                        XStore_Proto::Avg_REP rep;
                        r.AvgAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && !near(rep.opresult(), sum / n))) bad("AVG " + rep.opstatus());
                        break;
                    }
                    case 4: {
                        XStore_Proto::Variance_REP rep;
                        r.VarianceAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != (values.size() > 1) ||
                            (values.size() > 1 && !near(rep.opresult(), variance))) bad("VARIANCE " + rep.opstatus());
                        break;
                    }
                    case 5: {
                        XStore_Proto::Stddev_REP rep;
                        r.StddevAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != (values.size() > 1) ||
                            (values.size() > 1 && !near(rep.opresult(), std::sqrt(variance)))) bad("STDDEV " + rep.opstatus());
                        break;
                    }
                    case 6: {
                        XStore_Proto::Ohlc_REP rep;
                        r.OhlcAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && (rep.open() != values.front().second || rep.close() != values.back().second ||
                                                  rep.high() != mx || rep.low() != mn))) {
                            bad("OHLC " + rep.opstatus());
                        }
                        break;
                    }
                    case 7: {
                        XStore_Proto::Exists_REP rep;
                        r.ExistsAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if (rep.opstatus().rfind("SUCCESS", 0) != 0 || rep.exists() != success_expected) bad("EXISTS " + rep.opstatus());
                        break;
                    }
                    default: {
                        if (!success_expected) break;
                        const double target = values[rng() % values.size()].second;
                        std::set<unsigned long long> want;
                        for (const auto &[row_index, v] : values) {
                            if (v == target) want.insert(Epoch(row_index));
                        }
                        std::string buf;
                        r.FindByValue(Epoch(lo), Epoch(hi), db, c, FDouble(target), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                                      [&](std::string &&chunk, bool) { buf += chunk; });
                        XStore_Proto::RangeQuery_REP rep;
                        std::set<unsigned long long> got;
                        if (rep.ParseFromString(buf)) {
                            for (const auto &e : rep.queryresponse()) got.insert(e.timestamp());
                        }
                        if (got != want) bad("FINDBYVALUE got " + std::to_string(got.size()) + " want " + std::to_string(want.size()));
                        break;
                    }
                }
                ++queries;
            }
            close(qfd);
        });
    }

    std::this_thread::sleep_for(milliseconds(400)); // before: the 64-wide index under traffic
    const int queries_before = queries.load();
    XStore::LiveWidenStats stats;
    const Clock::time_point widen_start = Clock::now();
    const Result result = store.LiveWidenOneYear(path, kYear, kRowLength, kWidth, true, true, nullptr, &stats, workers,
                                                 group_bytes);
    const int64_t widen_ms = MsSince(widen_start);
    const int queries_during = queries.load() - queries_before;
    const int writes_at_end_of_widen = writes.load();
    std::this_thread::sleep_for(milliseconds(400)); // after: the widened index under traffic
    running.store(false);
    for (std::thread &t : threads) t.join();
    EXPECT_EQ(result, Result::kDone);
    EXPECT_EQ(mismatches.load(), 0);
    EXPECT_GT(writes_at_end_of_widen, 200) << "too little concurrent traffic to mean anything";
    EXPECT_GT(queries_during, 10) << "too few queries during the widen";
    std::printf("[live widen oracle] %u workers: widen %lld ms, %llu chunks, %llu rows, %llu timeouts, max hold %llu us; %d writes, "
                "%d queries (%d during)\n", workers, static_cast<long long>(widen_ms), static_cast<unsigned long long>(stats.chunks),
                static_cast<unsigned long long>(stats.rows_folded), static_cast<unsigned long long>(stats.lock_timeouts),
                static_cast<unsigned long long>(stats.max_lock_hold_us), writes.load(), queries.load(), queries_during);

    // Capture the live index before InitDB (which rebuilds this DB's index again: it is dirty after the writes).
    std::vector<std::vector<ZoneMapEntry>> live(kBlocks);
    for (uint64_t b = 0; b < kBlocks; ++b) {
        auto st = zmap.TryGetBlockStats(b);
        ASSERT_TRUE(st.has_value()) << "block " << b;
        ASSERT_EQ(st->size(), static_cast<size_t>(kWidth));
        live[b] = *st;
    }
    {   // Bloom: every present value of every tracked column.
        const int fd = open(data_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        uint64_t checked = 0, misses = 0;
        for (const auto &[r, row] : ReadRows(fd, 0, rows - 1, kRowLength)) {
            for (int c = 0; c < std::min(kWidth, row.fielddata_size()); ++c) {
                if (row.fielddata(c).oneOfData_case() == XStore_Proto::FData::ONEOFDATA_NOT_SET) continue;
                ++checked;
                if (!catidx.MightContain(BlockIndexForRow(r, rpb), static_cast<uint32_t>(c), EncodeFDataForCatIdx(row.fielddata(c)))) {
                    if (++misses < 5) ADD_FAILURE() << "bloom misses row " << r << " column " << c;
                }
            }
        }
        close(fd);
        EXPECT_EQ(misses, 0u);
        EXPECT_GT(checked, 1000000u);
    }

    BuildStartupReference(store, db, db + "REF", kWidth);
    ZoneMapFile &oracle = XStore::GetOrOpenZoneMap(DbPath(db + "REF"), kYear, kRowLength);
    uint64_t exact = 0, inexact = 0;
    for (uint64_t b = 0; b < kBlocks; ++b) {
        auto o = oracle.TryGetBlockStats(b);
        ASSERT_TRUE(o.has_value());
        ASSERT_EQ(o->size(), static_cast<size_t>(kWidth));
        for (int c = 0; c < kWidth; ++c) {
            SCOPED_TRACE("block " + std::to_string(b) + " column " + std::to_string(c));
            const ZoneMapEntry &oe = (*o)[static_cast<size_t>(c)];
            const ZoneMapEntry &le = live[b][static_cast<size_t>(c)];
            ASSERT_FALSE(ZoneMapEntryInexact(oe));
            if (ZoneMapEntryInexact(le)) {
                ++inexact;
                if (oe.valid_count > 0) { // pruning still uses min/max: too wide is fine, too narrow is not
                    EXPECT_LE(le.min_value, oe.min_value);
                    EXPECT_GE(le.max_value, oe.max_value);
                }
                continue;
            }
            ++exact;
            EXPECT_EQ(le.valid_count, oe.valid_count);
            if (oe.valid_count == 0) continue;
            EXPECT_EQ(le.min_value, oe.min_value);
            EXPECT_EQ(le.max_value, oe.max_value);
            EXPECT_NEAR(le.sum_value, oe.sum_value, 1e-6 * std::max(1.0, std::fabs(oe.sum_value)));
            EXPECT_EQ(le.first_value, oe.first_value);
            EXPECT_EQ(le.last_value, oe.last_value);
        }
    }
    EXPECT_GT(exact, static_cast<uint64_t>(kWidth) * 10) << "test too weak: almost every block ended inexact";
    std::printf("[live widen oracle] %llu exact and %llu inexact (block, column) entries compared\n",
                static_cast<unsigned long long>(exact), static_cast<unsigned long long>(inexact));
}

TEST_F(XStoreLiveWidenTest, AnswersStayExactUnderConcurrentWritesAndTheResultMatchesAStartupRebuild) {
    RunConcurrentOracle("LWLIVE", 1, XStoreTunables::LIVE_WIDEN_GROUP_BYTES);
}

// ---- 3. Starvation: a hot head must not stop the builder ------------------------------------------------------------

// Writers hammer the head rows (the newest rows of the year) through the store, and a relay holds WRITE locks on the
// two rows right after the data so that at EVERY instant one of them is locked (each is free part of the time). A
// builder that needs a range covering both at once (a blocking lock) can never pass; the timed lock shrinks the chunk
// to one row and gets through. Bound: 30 s (the expected time is about 1-2 s); a watchdog stops the hammering at the
// bound so a failing builder ends instead of hanging.
TEST_F(XStoreLiveWidenTest, AHotHeadDoesNotStopTheBuilder) {
    XStore store;
    const std::string db = "LWHOT";
    constexpr uint64_t kHead = 20000;
    SeedNarrowIndexWideRows(store, db, kHead, 70);
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    std::atomic<bool> hammer{true};
    std::vector<std::thread> threads;
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&, t] {
            XStore w;
            uint64_t i = 0;
            while (hammer.load()) {
                XStore_Proto::UnaryInsert_REP rep;
                ++i;
                w.UnaryInsert(Epoch(kHead - 1 - (i % 64) - 64 * static_cast<uint64_t>(t)), ValueRow(i % 1000, 70), db, &rep,
                              XStore_Proto::BYPASS);
            }
        });
    }
    std::atomic<uint64_t> relay_rounds{0};
    threads.emplace_back([&] {
        FileRangeLock &lock = FileLockRegistry::Get(data_path);
        const uint64_t a = kHead * kRowLength, b = (kHead + 1) * kRowLength;
        lock.LockRange(a, kRowLength, kWrite);
        while (hammer.load()) {
            lock.LockRange(b, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(1));
            lock.UnlockRange(a, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(2));
            lock.LockRange(a, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(1));
            lock.UnlockRange(b, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(2));
            ++relay_rounds;
        }
        lock.UnlockRange(a, kRowLength, kWrite);
    });
    std::this_thread::sleep_for(milliseconds(50));

    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    const Clock::time_point t0 = Clock::now();
    const bool in_time = builder.wait_for(std::chrono::seconds(30)) == std::future_status::ready;
    const int64_t elapsed_ms = MsSince(t0);
    hammer.store(false);
    const Result result = builder.get();
    for (std::thread &t : threads) t.join();
    EXPECT_TRUE(in_time) << "the builder did not finish within 30 s under a hot head";
    EXPECT_EQ(result, Result::kDone);
    EXPECT_GT(stats.lock_timeouts, 0u) << "the hot head never blocked the builder: the test proves nothing";
    EXPECT_GT(relay_rounds.load(), 0u);
    std::printf("[live widen hot head] %lld ms, %llu chunks, %llu timeouts (max %llu in a row)\n",
                static_cast<long long>(elapsed_ms), static_cast<unsigned long long>(stats.chunks),
                static_cast<unsigned long long>(stats.lock_timeouts), static_cast<unsigned long long>(stats.max_consecutive_timeouts));
    ExpectAnswersMatchTheData(store, db, 0, kHead - 1, {0, 65});
}

// Waits until the builder has folded some rows (its zone map's frontier passed `rows`), or fails after 20 s.
void WaitForFrontierPast(ZoneMapFile &zmap, uint64_t rows) {
    const Clock::time_point deadline = Clock::now() + std::chrono::seconds(20);
    for (;;) {
        const uint64_t f = zmap.FrontierForTests();
        if (f != ZoneMapFile::kNoRebuild && f > rows) return;
        ASSERT_LT(Clock::now(), deadline) << "the builder never got past row " << rows;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

// ---- 4. Stop flag ---------------------------------------------------------------------------------------------------

TEST_F(XStoreLiveWidenTest, StopMidBuildLeavesADirtyHalfTrustedIndexThatTheNextStartRebuilds) {
    XStore store;
    const std::string db = "LWSTOP";
    constexpr uint64_t kRows = 60000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    std::atomic<bool> stop{false};
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, &stop, &stats);
    });
    WaitForFrontierPast(zmap, 3 * Rpb());
    stop.store(true);
    ASSERT_EQ(builder.get(), Result::kStopped);

    const uint64_t f = zmap.FrontierForTests();
    ASSERT_NE(f, ZoneMapFile::kNoRebuild);
    EXPECT_LT(f, kRows) << "stopped too late to mean anything";
    EXPECT_EQ(catidx.FrontierForTests(), f) << "both files advance together";
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    const uint64_t complete_blocks = f / Rpb();
    ASSERT_GT(complete_blocks, 0u);
    auto trusted = zmap.TryGetBlockStats(complete_blocks - 1);
    ASSERT_TRUE(trusted.has_value());
    EXPECT_EQ(trusted->size(), 70u);
    EXPECT_FALSE(zmap.TryGetBlockStats(complete_blocks).has_value()) << "a block past F must not be trusted";
    EXPECT_TRUE(catidx.MightContain(complete_blocks, 0, EncodeFDataForCatIdx(FDouble(-1.0))));
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    EXPECT_FALSE(zmap.MarkClean());
    EXPECT_FALSE(catidx.MarkClean());

    XStore::Shutdown(); // clean stop: the half-built index stays dirty
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    BuildStartupReference(store, db, "LWSTOPREF", 70); // its InitDB also rebuilds LWSTOP's dirty index
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ExpectSameFilesAs(db, "LWSTOPREF");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// ---- 5. Failure injection -------------------------------------------------------------------------------------------

// The third chunk's pread fails: that chunk is not folded (F stays at its start), both files stay in rebuild, dirty,
// answers stay right (scan), and the next start rebuilds them.
TEST_F(XStoreLiveWidenTest, AReadFailureMidScanLeavesTheIndexInRebuildBehindTheFailedChunk) {
    XStore store;
    const std::string db = "LWPREAD";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 2, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    EXPECT_EQ(stats.chunks, 2u) << "two chunks folded before the failed read";
    const uint64_t f = zmap.FrontierForTests();
    EXPECT_EQ(f, stats.rows_folded) << "F is the end of the second chunk (rows 0..F-1 are all present)";
    EXPECT_EQ(catidx.FrontierForTests(), f);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState(); // MarkClean refuses: both stay dirty
    BuildStartupReference(store, db, "LWPREADREF", 70);
    ExpectSameFilesAs(db, "LWPREADREF");
}

// A .zmap block write fails mid-build: the zone map stops behind its frontier (dirty, in rebuild), the catidx carries
// on and finishes clean. The result is kFailed.
TEST_F(XStoreLiveWidenTest, AZoneMapWriteFailureStopsOnlyTheZoneMap) {
    XStore store;
    const std::string db = "LWPWRITE";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    CleanRestartState(); // both index files clean on disk: BeginRebuild's dirty mark is one pwrite
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        // .zmap pwrites: the dirty mark, the fresh header, then blocks: fail the fourth block write.
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, ".zmap"), 5, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    EXPECT_FALSE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_LT(zmap.FrontierForTests(), kRows);
    EXPECT_FALSE(catidx.Rebuilding());
    EXPECT_EQ(catidx.TrackedColumnCount(), 70u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, "LWPWRITEREF", 70);
    ExpectSameFilesAs(db, "LWPWRITEREF");
}

// The final fdatasync of the data file fails: the build is complete and trusted in this session (pruned at the new
// width) but stays dirty on disk, so the next start rebuilds it.
TEST_F(XStoreLiveWidenTest, ADataSyncFailureAtTheEndLeavesTheWidenedIndexDirty) {
    XStore store;
    const std::string db = "LWSYNC";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        ScopedCheckpointerPause no_background_sync; // the fault must hit the builder's fdatasync, not a checkpoint's
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, ".XSTORE"), 0, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    EXPECT_TRUE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_FALSE(zmap.Rebuilding());
    EXPECT_EQ(zmap.TrackedColumnCount(), 70u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    XStore_Proto::Max_REP rep;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &rep);
    EXPECT_EQ(rep.opstatus(), "SUCCESS") << "indexed at the new width in this session";
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// BeginRebuild fails on the zone map (its dirty-mark fdatasync): nothing changed there -- the old 64-column index
// stays in place, trusted and pruning -- while the catidx widens: kPartial. If both refuse: kFailed, both old.
TEST_F(XStoreLiveWidenTest, ABeginRebuildFailureKeepsTheOldIndexInUse) {
    XStore store;
    const std::string db = "LWBEGIN";
    constexpr uint64_t kRows = 10000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    CleanRestartState(); // clean on disk: BeginRebuild must make the dirty mark durable first
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, ".zmap"), 0, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kPartial);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    EXPECT_FALSE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_FALSE(stats.zmap_began) << "begin failed: neither began nor busy (a retry is reasonable)";
    EXPECT_FALSE(stats.zmap_busy);
    EXPECT_TRUE(stats.catidx_began);
    EXPECT_FALSE(zmap.Rebuilding());
    EXPECT_EQ(zmap.TrackedColumnCount(), 64u);
    auto block = zmap.TryGetBlockStats(1);
    ASSERT_TRUE(block.has_value()) << "the old index is still trusted";
    EXPECT_EQ(block->size(), 64u);
    EXPECT_GT((*block)[0].valid_count, 0u);
    EXPECT_EQ(catidx.TrackedColumnCount(), 70u);
    EXPECT_FALSE(catidx.Rebuilding());
    XStore_Proto::Max_REP narrow;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &narrow);
    EXPECT_NE(narrow.opstatus().find(kNotIndexed), std::string::npos) << "column 65 is still scanned";
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

    // Both refuse (the catidx is now clean and 70 wide; ask for 72 so both must rebuild).
    CleanRestartState();
    {
        xstore_fault::ScopedFdFault zfail(xstore_fault::FdOp::kSync, YearPath(db, ".zmap"), 0, 1, EIO);
        xstore_fault::ScopedFdFault cfail(xstore_fault::FdOp::kPwrite, YearPath(db, ".catidx"), 0, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 72, true, true, nullptr, &stats), Result::kFailed);
    }
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, kYear, kRowLength).TrackedColumnCount(), 64u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 70u);
    EXPECT_FALSE(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).Rebuilding());
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// A file already in rebuild state at entry is not touched (resuming is the trigger's business).
TEST_F(XStoreLiveWidenTest, AFileAlreadyRebuildingIsLeftAlone) {
    XStore store;
    const std::string db = "LWBUSY";
    SeedNarrowIndexWideRows(store, db, 3000, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    ASSERT_TRUE(zmap.BeginRebuild(64));
    XStore::LiveWidenStats stats;
    EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kAlreadyRebuilding);
    EXPECT_TRUE(stats.zmap_busy);
    EXPECT_FALSE(stats.zmap_began || stats.catidx_began || stats.catidx_busy) << "the catidx must not even be tried";
    EXPECT_EQ(zmap.FrontierForTests(), 0u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 64u) << "the catidx was touched";
    EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, false, false, nullptr, nullptr), Result::kNothingToDo);
    EXPECT_EQ(store.LiveWidenOneYear(path, 1999, kRowLength, 70, true, true, nullptr, nullptr), Result::kNothingToDo);
}

// ---- 6. Registry pressure -------------------------------------------------------------------------------------------

// More than the registry cap of other year files opened while a widen runs: the pinned, rebuilding entries survive.
TEST_F(XStoreLiveWidenTest, RegistryPressureDuringAWidenDoesNotEvictTheRebuildingFiles) {
    XStore store;
    const std::string db = "LWREG";
    constexpr uint64_t kRows = 100000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile *zmap = &XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile *catidx = &XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    WaitForFrontierPast(*zmap, Rpb());
    for (unsigned short year = 2001; year < 2001 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 20; ++year) {
        XStore::GetOrOpenZoneMap(path, year, kRowLength);
        XStore::GetOrOpenCatIdx(path, year, kRowLength);
    }
    const bool overlapped = zmap->Rebuilding();
    EXPECT_EQ(builder.get(), Result::kDone);
    ASSERT_TRUE(overlapped) << "the widen finished before the registry filled up: the test proves nothing";
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(path, kYear, kRowLength), zmap) << "the rebuilding zone map was evicted";
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(path, kYear, kRowLength), catidx) << "the rebuilding catidx was evicted";
    EXPECT_EQ(zmap->TrackedColumnCount(), 70u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// ---- 7. Everything about rows >= F happens under the lock -----------------------------------------------------------

// Every data-file read and every SEEK_DATA probe the builder makes must fall inside a range it holds READ on, under the
// writers' lock key (CachedYearFilePath): at that moment a WRITE try-lock on those bytes must fail. Nothing else runs,
// so a WRITE that succeeds means the builder read or probed rows it had not locked (a fold or a hole skip outside the
// lock, or a lock under another spelling of the path).
TEST_F(XStoreLiveWidenTest, EveryReadAndHoleProbeOfTheBuilderIsUnderItsReadLock) {
    XStore store;
    const std::string db = "LWLOCKED";
    SeedNarrowIndexWideRows(store, db, 6000, 70);
    {   // holes between data runs, so the probes and the scan both skip some
        Rows sparse;
        for (uint64_t r : {9000ULL, 9001ULL, 30000ULL, 200000ULL, 5000000ULL}) sparse.emplace_back(Epoch(r), ValueRow(r % 1000, 70));
        Insert(store, db, sparse);
    }
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    ASSERT_EQ(path, store.data_store_dir + "/" + db) << "must be the spelling the writers use (CachedDbBasePath)";
    FileRangeLock &writers_lock = FileLockRegistry::Get(path + "/" + std::to_string(kYear) + ".XSTORE"); // CachedYearFilePath's format
    std::atomic<uint64_t> reads{0}, probes{0}, unlocked{0};
    Result result = Result::kFailed;
    std::thread builder([&] {
        xstore_fault::ScopedFdObserver observe(data_path, [&](xstore_fault::ObservedOp op, uint64_t offset, uint64_t length) {
            (op == xstore_fault::ObservedOp::kPread ? reads : probes).fetch_add(1);
            const uint64_t bytes = std::max<uint64_t>(length, 1);
            if (writers_lock.TryLockRangeFor(offset, bytes, kWrite, milliseconds(0))) {
                writers_lock.UnlockRange(offset, bytes, kWrite);
                unlocked.fetch_add(1);
            }
        });
        XStore b;
        result = b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, nullptr);
    });
    builder.join();
    EXPECT_EQ(result, Result::kDone);
    EXPECT_GT(reads.load(), 0u);
    EXPECT_GT(probes.load(), 2u);
    EXPECT_EQ(unlocked.load(), 0u) << "the builder read or probed data-file bytes it did not hold a READ lock on";
    ExpectAnswersMatchTheData(store, db, 0, 5999, {0, 65});
}

// A WRITE lock held on one hole row R ahead of the data: while it is held the frontier never passes R (passing it would
// mean deciding about a row nobody read under a lock: the hole could be written the next instant); once released the
// build completes. Catches an in-lock probe whose finding (the next data row, past the locked range) is used to move F.
TEST_F(XStoreLiveWidenTest, TheFrontierNeverPassesAHoleRowAWriterHolds) {
    XStore store;
    const std::string db = "LWHOLE";
    SeedNarrowIndexWideRows(store, db, 6000, 70);
    Insert(store, db, {{Epoch(9000), ValueRow(9, 70)}, {Epoch(9001), ValueRow(10, 70)}});
    const std::string path = DbPath(db);
    constexpr uint64_t kHoleRow = 7000;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kHoleRow * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    uint64_t highest = 0;
    bool passed = false;
    const Clock::time_point until = Clock::now() + milliseconds(1500);
    while (Clock::now() < until) {
        const uint64_t f = zmap.FrontierForTests();
        if (f != ZoneMapFile::kNoRebuild) {
            highest = std::max(highest, f);
            passed = passed || f > kHoleRow;
        }
        if (f == ZoneMapFile::kNoRebuild && highest > 0) passed = true; // finished: it went past R
        std::this_thread::sleep_for(milliseconds(1));
    }
    const bool still_running = builder.wait_for(milliseconds(0)) != std::future_status::ready;
    lock.UnlockRange(kHoleRow * kRowLength, kRowLength, kWrite);
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_FALSE(passed) << "the frontier passed a hole row a writer held (highest F seen " << highest << ")";
    EXPECT_TRUE(still_running) << "the builder finished while R was locked";
    EXPECT_GE(highest, 6000u) << "the builder never reached the held row: the test proves nothing";
    EXPECT_GT(stats.lock_timeouts, 0u);
    ExpectAnswersMatchTheData(store, db, 0, 9001, {0, 65});
}

// The zone map begins but the catidx is busy (a catidx-only builder owns it, here: a rebuild begun by hand): the zone
// map widens (kPartial) and the catidx is left exactly as its owner has it -- never folded into, advanced, finished
// or cleaned by this builder.
TEST_F(XStoreLiveWidenTest, ABusyCatIdxIsLeftToItsOwnerWhileTheZoneMapWidens) {
    XStore store;
    const std::string db = "LWCATBUSY";
    SeedNarrowIndexWideRows(store, db, 8000, 70);
    const std::string path = DbPath(db);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    ASSERT_TRUE(catidx.BeginRebuild(64));
    XStore::LiveWidenStats stats;
    EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kPartial);
    EXPECT_TRUE(stats.zmap_began);
    EXPECT_TRUE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_busy);
    EXPECT_FALSE(stats.catidx_began);
    EXPECT_FALSE(stats.catidx_widened);
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(catidx.FrontierForTests(), 0u) << "the builder advanced a catidx it did not begin";
    EXPECT_EQ(catidx.TrackedColumnCount(), 64u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, 7999, {0, 65});
    BuildStartupReference(store, db, "LWCATBUSYREF", 70); // InitDB also rebuilds LWCATBUSY's (dirty) catidx; not compared
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".zmap")) == ReadFileBytes(YearPath("LWCATBUSYREF", ".zmap")))
        << "the zone map differs from the startup rebuild";
}

// S2 (review): two builders on one year at the same instant (released together from a spin barrier, 5 rounds): exactly
// one rebuilds (kDone) and the other touches nothing (kAlreadyRebuilding), so the loser can never truncate the
// winner's half-built files while the winner keeps advancing F. The result equals a startup rebuild.
TEST_F(XStoreLiveWidenTest, TwoBuildersOnOneYearNeverBothBegin) {
    XStore store;
    const std::string db = "LWTWICE";
    SeedNarrowIndexWideRows(store, db, 20000, 70);
    const std::string path = DbPath(db);
    for (int round = 0; round < 5; ++round) {
        SCOPED_TRACE("round " + std::to_string(round));
        std::atomic<int> ready{0};
        auto run = [&] {
            XStore b;
            ready.fetch_add(1);
            while (ready.load() < 2) {
            }
            return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, nullptr);
        };
        auto first = std::async(std::launch::async, run);
        auto second = std::async(std::launch::async, run);
        const Result a = first.get();
        const Result b = second.get();
        EXPECT_TRUE((a == Result::kDone && b == Result::kAlreadyRebuilding) || (a == Result::kAlreadyRebuilding && b == Result::kDone))
            << "results " << static_cast<int>(a) << " and " << static_cast<int>(b);
    }
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    BuildStartupReference(store, db, "LWTWICEREF", 70);
    ExpectSameFilesAs(db, "LWTWICEREF");
}

// ---- 8. The DB's year file removed mid-widen ------------------------------------------------------------------------

TEST_F(XStoreLiveWidenTest, RemovingTheYearFileMidWidenFailsTheBuildWithoutHarm) {
    XStore store;
    const std::string db = "LWGONE";
    constexpr uint64_t kRows = 60000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    // Held through a pin: the removal retires the year's entries, and a retired object lives only while pinned.
    const ZoneMapFile::Pin zmap = XStore::PinZoneMap(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    WaitForFrontierPast(*zmap, Rpb());
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    const XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, db);
    ASSERT_GT(rep.removestatus_size(), 0);
    EXPECT_NE(rep.removestatus(0).find("Removed"), std::string::npos) << rep.removestatus(0);
    EXPECT_EQ(builder.get(), Result::kFailed);
    EXPECT_TRUE(zmap->Rebuilding()) << "stays untrusted past F, dirty";
    EXPECT_LT(zmap->FrontierForTests(), kRows);
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".zmap"))) << "the year's index files go with its data";
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".catidx")));
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(path, kYear).has_value()) << "the year's entry was not retired";
}

// ---- 9. Registry lifetime: RemoveDB, pins on the request paths (Tech Reference 4.14) -------------------------------

// S1: RemoveDB must forget the DB's warm index objects. Otherwise a DB re-created under the same name gets the old
// objects back (fd on the unlinked files, stats and blooms of the deleted rows) and fully covered blocks answer with
// the deleted values.
TEST_F(XStoreLiveWidenTest, RemoveDbForgetsTheIndexesOfTheRemovedDb) {
    XStore store;
    const std::string db = "REGREMOVE";
    const uint64_t last = 2 * Rpb() - 1;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r <= last; ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r, 2)); // 1000 % 13 != 0
    Insert(store, db, rows);
    XStore_Proto::Min_REP old_min;
    store.MinAggregate(Epoch(0), Epoch(last), db, 0, &old_min);
    ASSERT_EQ(old_min.opresult().doubledata(), 1000.0);

    ASSERT_NE(store.RemoveDB(db).removestatus().find("[INFO]"), std::string::npos);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}, {Epoch(Rpb() + 7), ValueRow(5001, 2)}});

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(last), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(last), db, 0, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5001.0);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(Epoch(0), Epoch(last), db, 0, &sum_rep);
    EXPECT_EQ(sum_rep.opresult(), 10001.0);

    XStore_Proto::FData old_value;
    old_value.set_doubledata(1000.0);
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(last), db, 0, old_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 0);
    // FindByValue rescans a "maybe" block, so only the bloom itself shows a stale entry.
    CatIdxFile::Pin catidx = XStore::PinCatIdx(DbPath(db), kYear, kRowLength);
    EXPECT_FALSE(catidx->MightContain(0, 0, EncodeFDataForCatIdx(old_value))) << "the old DB's bloom came back";
    XStore_Proto::FData new_value;
    new_value.set_doubledata(5000.0);
    EXPECT_TRUE(catidx->MightContain(0, 0, EncodeFDataForCatIdx(new_value)));
}

// A pinned entry under a removed DB is retired, not destroyed: its holder keeps a valid object (writing into the unlinked
// file), lookups never return it, and a re-created DB gets a fresh one. Once the pin drops a later sweep frees it.
TEST_F(XStoreLiveWidenTest, RemoveDbRetiresAPinnedIndexAndLookupsNeverReturnIt) {
    XStore store;
    const std::string db = "REGRETIRE";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(3), ValueRow(700, 2)}});
    std::optional<ZoneMapFile::Pin> zmap(XStore::PinZoneMap(DbPath(db), kYear, kRowLength));
    std::optional<CatIdxFile::Pin> catidx(XStore::PinCatIdx(DbPath(db), kYear, kRowLength));
    ZoneMapFile *old_zmap = zmap->get();
    CatIdxFile *old_catidx = catidx->get();

    ASSERT_NE(store.RemoveDB(db).removestatus().find("[INFO]"), std::string::npos);
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value()) << "a lookup returned the retired entry";
    old_zmap->UpdateRow(9, {{0, 1.0, true}}, 1); // the holder may still use it
    EXPECT_TRUE(old_catidx->MightContain(0, 0, EncodeFDataForCatIdx(ValueRow(700, 2).fielddata(0))));

    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(4), ValueRow(800, 2)}});
    ZoneMapFile::Pin fresh_zmap = XStore::PinZoneMap(DbPath(db), kYear, kRowLength);
    CatIdxFile::Pin fresh_catidx = XStore::PinCatIdx(DbPath(db), kYear, kRowLength);
    EXPECT_NE(fresh_zmap.get(), old_zmap);
    EXPECT_NE(fresh_catidx.get(), old_catidx);
    auto stats = fresh_zmap->TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 1u);
    EXPECT_EQ((*stats)[0].min_value, 800.0);
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 2u);

    zmap.reset();
    catidx.reset();
    XStore::GetOrOpenZoneMap(DbPath(db), 1999, kRowLength); // a cold open sweeps the retired zone maps
    XStore::GetOrOpenCatIdx(DbPath(db), 1999, kRowLength);
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u);
}

// The DB-prefix match is a whole directory: removing DB1 leaves DB10's warm entries alone.
TEST_F(XStoreLiveWidenTest, RemoveDbLeavesADbWhoseNameExtendsItAlone) {
    XStore store;
    CreateTestDb(store, "REGP1", XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGP10", XStore_Proto::SECOND, kRowLength);
    Insert(store, "REGP1", {{Epoch(1), ValueRow(1, 2)}});
    Insert(store, "REGP10", {{Epoch(1), ValueRow(1, 2)}});
    ZoneMapFile *kept = &XStore::GetOrOpenZoneMap(DbPath("REGP10"), kYear, kRowLength);
    CatIdxFile *kept_catidx = &XStore::GetOrOpenCatIdx(DbPath("REGP10"), kYear, kRowLength);
    // The warm objects synced their dirty mark once; a cold reopen of the (dirty on disk) files would count 0. That
    // tells "the same object" from "a new one at a reused address".
    ASSERT_EQ(kept->DirtyMarkWritesForTests(), 1u);
    ASSERT_EQ(kept_catidx->DirtyMarkWritesForTests(), 1u);
    ASSERT_NE(store.RemoveDB("REGP1").removestatus().find("[INFO]"), std::string::npos);
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath("REGP1"), kYear).has_value());
    EXPECT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath("REGP10"), kYear).has_value());
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath("REGP10"), kYear, kRowLength), kept);
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(DbPath("REGP10"), kYear, kRowLength), kept_catidx);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(DbPath("REGP10"), kYear, kRowLength).DirtyMarkWritesForTests(), 1u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(DbPath("REGP10"), kYear, kRowLength).DirtyMarkWritesForTests(), 1u);
}

// ---- RemoveDBFiles of one year (F1): only that year's files and registry entry go ----------------------------------

constexpr unsigned short kNextYear = 1981; // XStoreLiveWidenTest::kYear + 1

// Removing YYYY.XSTORE also removes YYYY.zmap/.catidx and retires only the registry key of that year. A re-inserted
// year answers from the new rows only; the other year's warm objects stay the live entries.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesOfOneYearForgetsOnlyThatYearsIndexes) {
    XStore store;
    const std::string db = "REGRMYEAR";
    const uint64_t last = 2 * Rpb() - 1;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r <= last; ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r, 2));
    Insert(store, db, rows);
    Insert(store, db, {{Jan1(kNextYear) + 3, ValueRow(300, 2)}, {Jan1(kNextYear) + 4, ValueRow(301, 2)}});
    ZoneMapFile *next_zmap = &XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength);
    CatIdxFile *next_catidx = &XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength);
    ASSERT_EQ(next_zmap->DirtyMarkWritesForTests(), 1u);
    ASSERT_EQ(next_catidx->DirtyMarkWritesForTests(), 1u);

    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    const XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, db);
    ASSERT_EQ(rep.removestatus_size(), 3);
    EXPECT_EQ(rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: REGRMYEAR] - Removed 1980.XSTORE");
    EXPECT_EQ(rep.removestatus(1), "[INFO] [REMOVE DB FILES] [DB: REGRMYEAR] - Removed 1980.zmap");
    EXPECT_EQ(rep.removestatus(2), "[INFO] [REMOVE DB FILES] [DB: REGRMYEAR] - Removed 1980.catidx");
    for (const char *ext : {".XSTORE", ".zmap", ".catidx"}) EXPECT_FALSE(std::filesystem::exists(YearPath(db, ext))) << ext;

    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}, {Epoch(Rpb() + 7), ValueRow(5001, 2)}});
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(last), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(last), db, 0, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5001.0);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(Epoch(0), Epoch(last), db, 0, &sum_rep);
    EXPECT_EQ(sum_rep.opresult(), 10001.0);
    XStore_Proto::FData old_value;
    old_value.set_doubledata(1000.0);
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(last), db, 0, old_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 0);
    EXPECT_FALSE(XStore::PinCatIdx(DbPath(db), kYear, kRowLength)->MightContain(0, 0, EncodeFDataForCatIdx(old_value)));

    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength), next_zmap) << "the other year was retired";
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength), next_catidx);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength).DirtyMarkWritesForTests(), 1u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength).DirtyMarkWritesForTests(), 1u);
    XStore_Proto::Max_REP next_max;
    store.MaxAggregate(Jan1(kNextYear), Jan1(kNextYear) + 10, db, 0, &next_max);
    EXPECT_EQ(next_max.opresult().doubledata(), 301.0);
}

// A pin held on another year across the removal: that year's entry stays the live one (a retire would let the next
// cold open create a second object on the same, still existing file, and the two would overwrite each other's blocks).
TEST_F(XStoreLiveWidenTest, RemoveDbFilesKeepsAPinnedEntryOfAnotherYearLive) {
    XStore store;
    const std::string db = "REGRMPIN";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}, {Jan1(kNextYear) + 1, ValueRow(2, 2)}});
    const ZoneMapFile::Pin zmap = XStore::PinZoneMap(DbPath(db), kNextYear, kRowLength);
    const CatIdxFile::Pin catidx = XStore::PinCatIdx(DbPath(db), kNextYear, kRowLength);
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    ASSERT_NE(store.RemoveDBFiles(&req, db).removestatus(0).find("[INFO]"), std::string::npos);
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u);
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength), zmap.get()) << "a second object was opened";
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength), catidx.get());
}

// An untrusted entry of another year (EnterUntrustedRebuildState, no pin) survives the removal and stays untrusted:
// erasing it would let a cold reopen trust its stale file.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesKeepsAnUntrustedEntryOfAnotherYear) {
    XStore store;
    const std::string db = "REGRMUNTRUST";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}, {Jan1(kNextYear) + 1, ValueRow(2, 2)}});
    XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength).EnterUntrustedRebuildState();
    XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength).EnterUntrustedRebuildState();
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    ASSERT_NE(store.RemoveDBFiles(&req, db).removestatus(0).find("[INFO]"), std::string::npos);
    EXPECT_TRUE(XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength).Rebuilding());
    EXPECT_TRUE(XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength).Rebuilding());
}

// An index file alone is refused: a fresh, empty index for a year whose data still exists would be trusted.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesRefusesAnIndexFileOnItsOwn) {
    XStore store;
    const std::string db = "REGRMIDX";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}});
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename("1980.zmap");
    req.add_filename("1980.catidx");
    const XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, db);
    ASSERT_EQ(rep.removestatus_size(), 2);
    EXPECT_EQ(rep.removestatus(0).rfind("[WARNING] ", 0), 0u) << rep.removestatus(0);
    EXPECT_EQ(rep.removestatus(1).rfind("[WARNING] ", 0), 0u) << rep.removestatus(1);
    EXPECT_TRUE(std::filesystem::exists(YearPath(db, ".zmap")));
    EXPECT_TRUE(std::filesystem::exists(YearPath(db, ".catidx")));
    EXPECT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value());
}

// The year key matches exactly: removing 1980 of DB1 leaves year 19801 of DB1 and year 1980 of DB10 alone.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesMatchesTheYearKeyExactly) {
    XStore store;
    CreateTestDb(store, "REGK1", XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGK10", XStore_Proto::SECOND, kRowLength);
    Insert(store, "REGK1", {{Epoch(1), ValueRow(1, 2)}});
    Insert(store, "REGK10", {{Epoch(1), ValueRow(1, 2)}});
    const ZoneMapFile::Pin long_year = XStore::PinZoneMap(DbPath("REGK1"), 19801, kRowLength);
    ZoneMapFile *other_db = &XStore::GetOrOpenZoneMap(DbPath("REGK10"), kYear, kRowLength);
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename("1980.XSTORE");
    ASSERT_NE(store.RemoveDBFiles(&req, "REGK1").removestatus(0).find("[INFO]"), std::string::npos);
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath("REGK1"), kYear).has_value());
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath("REGK1"), 19801, kRowLength), long_year.get());
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u) << "year 19801 matched the 1980 key";
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath("REGK10"), kYear, kRowLength), other_db);
    EXPECT_EQ(other_db->DirtyMarkWritesForTests(), 1u);
}

// Deterministic: an insert stopped inside ApplyWalRecord, after both registry lookups (it waits for the WRITE range
// lock the test holds), keeps both of its index files through more than the cap of other cold opens. Without the pins
// the registry evicts the zone map the writer still holds, and the writer then updates a destroyed object.
TEST_F(XStoreLiveWidenTest, AnInsertHoldsItsIndexesThroughRegistryPressure) {
    XStore store;
    const std::string db = "REGHOLD";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGFILL", XStore_Proto::SECOND, kRowLength);
    auto hold = std::make_unique<FileRangeLockGuard>(YearPath(db, ".XSTORE"), 0, 0, kWrite);
    auto writer = std::async(std::launch::async, [&] {
        XStore w;
        XStore_Proto::UnaryInsert_REP rep;
        w.UnaryInsert(Epoch(5), ValueRow(100, 2), db, &rep, XStore_Proto::BYPASS);
        return rep.insertstatus();
    });
    // The catidx file appears inside the second lookup, under the registry mutex; the storm below takes that mutex,
    // so it starts only after the writer holds both files.
    const Clock::time_point t0 = Clock::now();
    while (!std::filesystem::exists(YearPath(db, ".catidx")) && MsSince(t0) < 10000) {
        std::this_thread::sleep_for(milliseconds(1));
    }
    ASSERT_TRUE(std::filesystem::exists(YearPath(db, ".catidx"))) << "the writer never reached its index lookups";
    for (unsigned short year = 2001; year < 2001 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 10; ++year) {
        XStore::GetOrOpenZoneMap(DbPath("REGFILL"), year, kRowLength);
        XStore::GetOrOpenCatIdx(DbPath("REGFILL"), year, kRowLength);
    }
    EXPECT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value()) << "evicted while the writer holds it";
    hold.reset();
    ASSERT_EQ(writer.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    EXPECT_NE(writer.get().find("[INFO]"), std::string::npos);

    const XStore_Proto::FData value = ValueRow(100, 2).fielddata(0);
    EXPECT_TRUE(XStore::PinCatIdx(DbPath(db), kYear, kRowLength)->MightContain(0, 0, EncodeFDataForCatIdx(value)));
    auto stats = XStore::PinZoneMap(DbPath(db), kYear, kRowLength)->TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 1u);
    EXPECT_EQ((*stats)[0].min_value, 100.0);
}

// SMOKE TEST, NOT a regression guard for the pins (it passed 3 of 3 with ApplyWalRecord's pins removed; the guard is
// AnInsertHoldsItsIndexesThroughRegistryPressure above). Writers (BYPASS on their own thread, SYNC through the WAL
// writers) and readers on a year file while a third thread keeps cold-opening more than the cap of other year files:
// no crash, and the final indexes are byte-identical to a startup rebuild of the final data.
TEST_F(XStoreLiveWidenTest, RegistryPressureUnderWritersAndReadersMatchesAStartupRebuild) {
    XStore store;
    const std::string db = "REGSTRESS";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGSFILL", XStore_Proto::SECOND, kRowLength);
    constexpr int kWriters = 4;
    const uint64_t rows = 8 * Rpb();
    std::atomic<bool> running{true};
    std::atomic<int> writers_left{kWriters};
    std::atomic<uint64_t> fill_passes{0};
    std::vector<std::thread> threads;
    // No null columns: the startup rebuild folds an unset column into the bloom (its tag byte) and a live insert does
    // not, so with nulls the two files differ by a few superset bits that have nothing to do with the registry.
    const auto FullRow = [](uint64_t v) {
        XStore_Proto::RowData row;
        for (uint64_t c = 0; c < 4; ++c) row.add_fielddata()->set_doubledata(static_cast<double>(v + c));
        return row;
    };
    for (int t = 0; t < kWriters; ++t) {
        threads.emplace_back([&, t] {
            XStore w;
            std::mt19937_64 rng(4242 + t);
            std::vector<uint64_t> mine;
            for (uint64_t r = static_cast<uint64_t>(t); r < rows; r += kWriters) mine.push_back(r);
            std::shuffle(mine.begin(), mine.end(), rng);
            for (size_t from = 0; from < mine.size();) {
                const size_t n = std::min<size_t>(mine.size() - from, 1 + rng() % 16);
                const XStore_Proto::WalDurabilityMode mode = rng() % 2 ? XStore_Proto::BYPASS : XStore_Proto::SYNC;
                if (n == 1) {
                    XStore_Proto::UnaryInsert_REP rep;
                    w.UnaryInsert(Epoch(mine[from]), FullRow(mine[from] * 7), db, &rep, mode);
                    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
                } else {
                    XStore_Proto::BatchInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(mode);
                    for (size_t i = from; i < from + n; ++i) {
                        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                        entry->set_timestamp(Epoch(mine[i]));
                        *entry->mutable_data() = FullRow(mine[i] * 7);
                    }
                    XStore_Proto::BatchInsert_REP rep;
                    w.BatchInsert(&req, &rep);
                    ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
                }
                from += n;
            }
            writers_left.fetch_sub(1);
        });
    }
    threads.emplace_back([&] { // readers: the aggregate and FindByValue paths under the same pressure
        XStore r;
        std::mt19937_64 rng(99);
        while (running.load()) {
            const uint64_t lo = rng() % rows, hi = std::min(rows - 1, lo + rng() % (3 * Rpb()));
            XStore_Proto::Max_REP max_rep;
            r.MaxAggregate(Epoch(lo), Epoch(hi), db, 0, &max_rep);
            XStore_Proto::Sum_REP sum_rep;
            r.SumAggregate(Epoch(lo), Epoch(hi), db, 1, &sum_rep);
            XStore_Proto::FData v;
            v.set_doubledata(static_cast<double>(lo * 7));
            r.FindByValue(Epoch(lo), Epoch(hi), db, 0, v, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [](std::string &&, bool) {});
        }
    });
    threads.emplace_back([&] { // the evictor
        while (running.load()) {
            for (unsigned short year = 2001; year < 2001 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 20; ++year) {
                XStore::GetOrOpenZoneMap(DbPath("REGSFILL"), year, kRowLength);
                XStore::GetOrOpenCatIdx(DbPath("REGSFILL"), year, kRowLength);
            }
            fill_passes.fetch_add(1);
        }
    });
    while (writers_left.load() > 0) std::this_thread::sleep_for(milliseconds(5));
    running.store(false);
    for (std::thread &t : threads) t.join();
    EXPECT_GT(fill_passes.load(), 0u);

    // Back into the registry (an evicted file is dirty on disk and only registry entries are marked clean at stop).
    XStore::GetOrOpenZoneMap(DbPath(db), kYear, kRowLength);
    XStore::GetOrOpenCatIdx(DbPath(db), kYear, kRowLength);
    CleanRestartState(); // drains the WAL and marks the live indexes clean, so the startup below keeps them as they are
    BuildStartupReference(store, db, "REGSTRESSREF", 64);
    ExpectSameFilesAs(db, "REGSTRESSREF");
}

// Step 0 of stage 3 (registry fix-round review, finding 1): a sidecar whose removal fails (here a non-empty directory
// in place of 1980.zmap; the warm entry keeps its fd on the old file) is reported as an [ERROR] status, no exception
// escapes, and the year's entries are retired anyway (a live entry would let a later insert re-create the year file
// through it with the deleted rows' stats). The other year is untouched.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesRetiresTheYearEvenWhenASidecarCannotBeRemoved) {
    XStore store;
    const std::string db = "REGRMFAIL";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}, {Jan1(kNextYear) + 1, ValueRow(2, 2)}});
    ASSERT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value());
    ZoneMapFile *next_zmap = &XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength);
    const std::string zmap_path = YearPath(db, ".zmap");
    ASSERT_TRUE(std::filesystem::remove(zmap_path));
    ASSERT_TRUE(std::filesystem::create_directory(zmap_path));
    std::ofstream(zmap_path + "/keep") << "x";

    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    XStore_Proto::RemoveDBFiles_REP rep;
    EXPECT_NO_THROW(rep = store.RemoveDBFiles(&req, db));
    ASSERT_EQ(rep.removestatus_size(), 3);
    EXPECT_EQ(rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: REGRMFAIL] - Removed 1980.XSTORE");
    EXPECT_EQ(rep.removestatus(1).rfind("[ERROR] [REMOVE DB FILES] [DB: REGRMFAIL] - Unable to remove 1980.zmap", 0), 0u)
        << rep.removestatus(1);
    EXPECT_EQ(rep.removestatus(2), "[INFO] [REMOVE DB FILES] [DB: REGRMFAIL] - Removed 1980.catidx");
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value()) << "the year's entry was not retired";
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength), next_zmap) << "another year was touched";
    std::filesystem::remove_all(zmap_path);
}

// ---- 10. The trigger (M4 stage 3): RunLiveWidenPass driven with a fake clock ----------------------------------------

using PassResult = std::vector<std::pair<unsigned short, Result>>;
PassResult YearsAndResults(const std::vector<XStore::LiveWidenPassEntry> &pass) {
    PassResult out;
    for (const XStore::LiveWidenPassEntry &e : pass) out.emplace_back(e.year, e.result);
    return out;
}
constexpr const char *kBeingIndexed = "being indexed in the background";
using std::chrono::seconds;

// Real growth through MaybeGrowColumnCount, debounce, then one pass widens every year of the DB, newest first, to the
// exact width, byte-identical to a startup rebuild of each year. Queries are right before, during (a query thread runs
// while the pass works) and after.
TEST_F(XStoreLiveWidenTest, TheTriggerWidensEveryYearNewestFirstAfterTheQuietPeriod) {
    XStore store;
    const std::string db = "LWTRIG";
    const std::string path = DbPath(db);
    const std::vector<unsigned short> years = {1979, 1980, 1981};
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    for (unsigned short y : years) Insert(store, db, {{Jan1(y), ValueRow(1, 10)}}); // index files born 64 wide
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "a width <= 64 needs no widen";
    Rows rows;
    for (unsigned short y : years) {
        for (uint64_t r = 1; r < 4000; ++r) rows.emplace_back(Jan1(y) + r, ValueRow((r * 7 + y) % 100000, r % 5 == 0 ? 40 : 70));
    }
    Insert(store, db, rows); // growth 64 -> 70
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    const Clock::time_point g1 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_TRUE(store.RunLiveWidenPass(g1 + seconds(29)).empty()) << "inside the quiet period";

    Insert(store, db, {{Jan1(1980) + 5000, ValueRow(5, 80)}}); // growth 70 -> 80: restarts the quiet period
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_GT(g2, g1);
    EXPECT_TRUE(store.RunLiveWidenPass(g1 + seconds(30)).empty()) << "the second growth did not restart the quiet period";
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    // A year file created after the growth is born at the new width; it is dirty (as every file written at runtime) and
    // must not be rebuilt: the decision is made on widths only.
    Insert(store, db, {{Jan1(1982) + 1, ValueRow(9, 80)}});
    ASSERT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap", 1982)).dirty, 1u);
    ASSERT_EQ(XStore::GetOrOpenZoneMap(path, 1982, kRowLength).TrackedColumnCount(), 80u);

    // Before: right answers; column 65 is scanned and says so.
    for (unsigned short y : years) ExpectAnswersMatchTheData(store, db, 0, 3999, {0, 65}, y);
    XStore_Proto::Max_REP before;
    store.MaxAggregate(Jan1(1980), Jan1(1980) + 3999, db, 65, &before);
    EXPECT_NE(before.opstatus().find(kNotIndexed), std::string::npos) << before.opstatus();

    // During: a query thread checks MAX/SUM of column 65 per year against values computed from the data up front.
    std::map<unsigned short, std::pair<double, double>> oracle; // year -> (max, sum)
    for (unsigned short y : years) {
        const int fd = open(YearPath(db, ".XSTORE", y).c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        double mx = -INFINITY, sum = 0;
        for (const auto &[r, row] : ReadRows(fd, 0, 3999, kRowLength)) {
            double v = 0;
            if (row.fielddata_size() > 65 && NumericOf(row.fielddata(65), v)) {
                mx = std::max(mx, v);
                sum += v;
            }
        }
        close(fd);
        oracle[y] = {mx, sum};
    }
    std::atomic<bool> running{true};
    std::atomic<int> queries{0}, mismatches{0};
    std::thread querier([&] {
        XStore q;
        while (running.load()) {
            for (unsigned short y : years) {
                XStore_Proto::Max_REP max_rep;
                q.MaxAggregate(Jan1(y), Jan1(y) + 3999, db, 65, &max_rep);
                XStore_Proto::Sum_REP sum_rep;
                q.SumAggregate(Jan1(y), Jan1(y) + 3999, db, 65, &sum_rep);
                if (max_rep.opresult().doubledata() != oracle[y].first || sum_rep.opresult() != oracle[y].second) {
                    if (mismatches.fetch_add(1) < 5) ADD_FAILURE() << "year " << y << ": " << max_rep.opstatus();
                }
                ++queries;
            }
        }
    });
    const auto pass = store.RunLiveWidenPass(g2 + seconds(30));
    running.store(false);
    querier.join();
    EXPECT_EQ(mismatches.load(), 0);
    EXPECT_GT(queries.load(), 0);
    EXPECT_EQ(YearsAndResults(pass), (PassResult{{1982, Result::kNothingToDo}, {1981, Result::kDone}, {1980, Result::kDone}, {1979, Result::kDone}}))
        << "every year, newest first; the year already at the new width untouched";
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "the record must go once the DB is widened";
    EXPECT_TRUE(store.RunLiveWidenPass(g2 + seconds(3600)).empty());

    for (unsigned short y : years) {
        SCOPED_TRACE("year " + std::to_string(y));
        EXPECT_EQ(XStore::GetOrOpenZoneMap(path, y, kRowLength).TrackedColumnCount(), 80u) << "the exact width";
        EXPECT_EQ(XStore::GetOrOpenCatIdx(path, y, kRowLength).TrackedColumnCount(), 80u);
        EXPECT_FALSE(XStore::GetOrOpenZoneMap(path, y, kRowLength).Rebuilding());
        ExpectAnswersMatchTheData(store, db, 0, 3999, {0, 65}, y);
    }
    XStore_Proto::Max_REP after;
    store.MaxAggregate(Jan1(1980), Jan1(1980) + 3999, db, 65, &after);
    EXPECT_EQ(after.opstatus(), "SUCCESS");

    // Byte-identical to a startup rebuild of each year (sparse copies in a reference DB 80 wide; its InitDB keeps the
    // live DB's clean, right-width files as they are, and they are read before anyway).
    std::map<std::pair<unsigned short, std::string>, std::string> live;
    for (unsigned short y : years) {
        for (const char *ext : {".zmap", ".catidx"}) live[{y, ext}] = ReadFileBytes(YearPath(db, ext, y));
    }
    const std::string ref = "LWTRIGREF";
    CreateTestDb(store, ref, XStore_Proto::SECOND, kRowLength);
    Insert(store, ref, {{Jan1(1975), ValueRow(1, 80)}});
    for (unsigned short y : years) SparseCopy(YearPath(db, ".XSTORE", y), YearPath(ref, ".XSTORE", y));
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    store.InitDB();
    for (unsigned short y : years) {
        for (const char *ext : {".zmap", ".catidx"}) {
            EXPECT_TRUE((live[{y, ext}] == ReadFileBytes(YearPath(ref, ext, y)))) << y << ext << " differs from the startup rebuild";
        }
    }
}

// A growth while a pass is widening restarts the DB's quiet period and its record survives the pass; the next pass
// re-reads the widths and widens again, to the larger width. A WRITE lock on the hole row after the data keeps the
// builder from finishing until the test has grown the DB (and checked the notices of a rebuilding year).
TEST_F(XStoreLiveWidenTest, AGrowthDuringAPassKeepsTheRecordAndTheNextPassWidensFurther) {
    XStore store;
    const std::string db = "LWTRIGMID";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    const Clock::time_point g1 = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g1 + seconds(30));
    });
    WaitForFrontierPast(zmap, Rpb());
    EXPECT_TRUE(zmap.Rebuilding()); // (no ASSERT while the lock is held: the pass would never end)

    // Notices on a rebuilding year: a column >= 64 says "being indexed in the background", a lower one says nothing.
    XStore_Proto::Max_REP high;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &high);
    EXPECT_NE(high.opstatus().find(kBeingIndexed), std::string::npos) << high.opstatus();
    XStore_Proto::Max_REP low;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 5, &low);
    EXPECT_EQ(low.opstatus(), "SUCCESS");
    std::string find_buf;
    store.FindByValue(Epoch(0), Epoch(kRows - 1), db, 65, FDouble(1.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { find_buf += chunk; });
    XStore_Proto::FindByValue_REP find_rep;
    EXPECT_TRUE(find_rep.ParseFromString(find_buf));
    EXPECT_NE(find_rep.querystatus().find(kBeingIndexed), std::string::npos) << find_rep.querystatus();
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

    XStore_Proto::UnaryInsert_REP grow;
    store.UnaryInsert(Epoch(kRows + 500), ValueRow(3, 90), db, &grow); // 70 -> 90 while the pass runs
    EXPECT_NE(grow.insertstatus().find("[INFO]"), std::string::npos) << grow.insertstatus();
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_GT(g2, g1);
    EXPECT_TRUE(zmap.Rebuilding()) << "the growth did not land during the pass";
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_EQ(pass.wait_for(seconds(60)), std::future_status::ready);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kDone}}));
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u) << "a growth arrived mid-pass: the record must stay";
    EXPECT_EQ(zmap.TrackedColumnCount(), 70u) << "that pass widened to the width it started with";
    XStore_Proto::Max_REP done;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &done);
    EXPECT_EQ(done.opstatus(), "SUCCESS") << "no notice once the rebuild is done";
    find_buf.clear();
    store.FindByValue(Epoch(0), Epoch(kRows - 1), db, 65, FDouble(1.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { find_buf += chunk; });
    ASSERT_TRUE(find_rep.ParseFromString(find_buf));
    EXPECT_EQ(find_rep.querystatus().find(kBeingIndexed), std::string::npos) << find_rep.querystatus();
    EXPECT_EQ(find_rep.querystatus().find(kNotIndexed), std::string::npos) << find_rep.querystatus();

    EXPECT_TRUE(store.RunLiveWidenPass(g2 + seconds(29)).empty()) << "the new quiet period counts from the growth";
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g2 + seconds(30))), (PassResult{{kYear, Result::kDone}}));
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    EXPECT_EQ(zmap.TrackedColumnCount(), 90u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 90u);
    ExpectAnswersMatchTheData(store, db, 0, kRows + 500, {0, 65, 85});
}

// A year whose index is already in rebuild state (a stopped builder; here a rebuild begun by hand) is skipped with one
// log line per year file, never retried in a loop; the DB's other years still widen.
TEST_F(XStoreLiveWidenTest, AYearAlreadyRebuildingIsSkippedAndTheOtherYearsWiden) {
    XStore store;
    const std::string db = "LWTRIGSKIP";
    const std::string path = DbPath(db);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (unsigned short y : {1979, 1980, 1981}) {
        Insert(store, db, {{Jan1(y), ValueRow(1, 10)}});
        for (uint64_t r = 1; r < 2000; ++r) rows.emplace_back(Jan1(y) + r, ValueRow(r, 70));
    }
    Insert(store, db, rows);
    ZoneMapFile &stuck = XStore::GetOrOpenZoneMap(path, 1980, kRowLength);
    ASSERT_TRUE(stuck.BeginRebuild(64));
    const Clock::time_point g1 = *XStore::LiveWidenLastGrowthForTests(path);
    kLogCapture->Clear();
    const auto pass = store.RunLiveWidenPass(g1 + seconds(30));
    EXPECT_EQ(YearsAndResults(pass),
              (PassResult{{1981, Result::kDone}, {1980, Result::kAlreadyRebuilding}, {1979, Result::kDone}}));
    ASSERT_EQ(pass.size(), 3u);
    EXPECT_TRUE(pass[1].skipped_rebuilding);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "a skipped year is not retried";
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, 1981, kRowLength).TrackedColumnCount(), 70u);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, 1979, kRowLength).TrackedColumnCount(), 70u);
    EXPECT_TRUE(stuck.Rebuilding());
    EXPECT_EQ(stuck.FrontierForTests(), 0u) << "the stuck year was touched";
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, 1980, kRowLength).TrackedColumnCount(), 64u) << "its catidx was touched";

    Insert(store, db, {{Jan1(1981) + 3000, ValueRow(2, 75)}}); // another growth: the stuck year is met again
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g2 + seconds(30))),
              (PassResult{{1981, Result::kDone}, {1980, Result::kAlreadyRebuilding}, {1979, Result::kDone}}));
    EXPECT_EQ(kLogCapture->Count("is already being rebuilt"), 1u) << "one line per year file, not one per pass";
    ExpectAnswersMatchTheData(store, db, 0, 1999, {0, 65}, 1980);
}

// BeginRebuild fails (the zone map's dirty-mark fdatasync, every time): the old 64-wide zone map stays in use and
// trusted, and the year is retried after 60 s and 120 s more (fake time), then given up until the next growth.
TEST_F(XStoreLiveWidenTest, AFailedBeginIsRetriedWithBackoffThenGivenUp) {
    XStore store;
    const std::string db = "LWTRIGRETRY";
    constexpr uint64_t kRows = 10000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    CleanRestartState(); // clean on disk: a begin must make the dirty mark durable first
    kLogCapture->Clear();
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, ".zmap"), 0, 1000000, EIO);
        Clock::time_point t = g + seconds(30);
        EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(t)), (PassResult{{kYear, Result::kPartial}})) << "the catidx widens";
        EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
        ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
        EXPECT_FALSE(zmap.Rebuilding());
        EXPECT_EQ(zmap.TrackedColumnCount(), 64u);
        auto block = zmap.TryGetBlockStats(1);
        ASSERT_TRUE(block.has_value()) << "the old index must stay trusted";
        EXPECT_EQ(block->size(), 64u);
        EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 70u);
        ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

        EXPECT_TRUE(store.RunLiveWidenPass(t + seconds(59)).empty()) << "first retry waits 60 s";
        t += seconds(60);
        EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(t)), (PassResult{{kYear, Result::kFailed}})) << "only the zone map left";
        EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
        EXPECT_TRUE(store.RunLiveWidenPass(t + seconds(119)).empty()) << "second retry waits 120 s";
        t += seconds(120);
        EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(t)), (PassResult{{kYear, Result::kFailed}}));
        EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "given up after 3 attempts";
        EXPECT_TRUE(store.RunLiveWidenPass(t + seconds(100000)).empty());
        EXPECT_GT(fail.Failures(), 2);
    }
    EXPECT_EQ(kLogCapture->Count("giving up"), 1u);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, kYear, kRowLength).TrackedColumnCount(), 64u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// The cancel flag (shutdown) mid-pass: kStopped, the pass ends, the year's files stay in rebuild (dirty, untrusted past
// F), answers stay right, Shutdown leaves them dirty with the clear message, and a restart rebuilds them correctly.
TEST_F(XStoreLiveWidenTest, ACancelMidPassStopsItAndTheNextStartRebuilds) {
    XStore store;
    const std::string db = "LWTRIGSTOP";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    WaitForFrontierPast(zmap, Rpb());
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(ended) << "the pass ignored the cancel flag";
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    XStore::bg_cancel_.store(false);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u) << "a stopped pass keeps the record";
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

    kLogCapture->Clear();
    XStore::Shutdown();
    EXPECT_EQ(kLogCapture->Count("an index was still being rebuilt; left dirty, rebuilt at the next start"), 2u);
    EXPECT_FALSE(kLogCapture->Contains("MarkClean failed"));
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    BuildStartupReference(store, db, "LWTRIGSTOPREF", 70); // its InitDB also rebuilds LWTRIGSTOP's dirty files
    ExpectSameFilesAs(db, "LWTRIGSTOPREF");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g + seconds(31))), (PassResult{{kYear, Result::kNothingToDo}}));
    EXPECT_FALSE(XStore::LiveWidenLastGrowthForTests(path).has_value()); // (the reference DB's own growth is recorded)
}

// RemoveDB while a pass widens one of the DB's years: it cancels the widen, waits for the thread to let go, then
// removes. No crash, no leaked rebuilding entry, the record is dropped, and a DB re-created under the name sees only
// its own values. A watchdog bounds every wait (a RemoveDB that neither cancels nor waits must fail, not hang).
TEST_F(XStoreLiveWidenTest, RemoveDbDuringAWidenCancelsWaitsThenRemoves) {
    XStore store;
    const std::string db = "LWTRIGRMDB";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(path, kYear, kRowLength);
        WaitForFrontierPast(*zmap, Rpb());
    }
    EXPECT_EQ(XStore::BackgroundActiveKeyForTests(), path + "/" + std::to_string(kYear));
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        return r.RemoveDB(db).removestatus();
    });
    const bool removed_in_time = remove.wait_for(seconds(30)) == std::future_status::ready;
    const bool pass_ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(removed_in_time) << "RemoveDB hung";
    ASSERT_TRUE(pass_ended) << "the pass hung";
    EXPECT_NE(remove.get().find("[INFO]"), std::string::npos);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}})) << "RemoveDB must cancel the widen";
    EXPECT_FALSE(XStore::bg_cancel_.load()) << "the cancel was not reset";
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "the removed DB's record must go";
    EXPECT_EQ(XStore::BackgroundActiveKeyForTests(), "");
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(path, kYear).has_value());
    EXPECT_FALSE(std::filesystem::exists(path));

    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    XStore::GetOrOpenZoneMap(path, 1999, kRowLength); // a cold open sweeps unpinned retired entries
    XStore::GetOrOpenCatIdx(path, 1999, kRowLength);
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u) << "a rebuilding entry leaked";
    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}, {Epoch(Rpb() + 7), ValueRow(5001, 2)}});
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5001.0);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &sum_rep);
    EXPECT_EQ(sum_rep.opresult(), 10001.0);
    const XStore_Proto::FData old_value = FDouble(70.0); // row 10's column 0 in the removed DB
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(kRows - 1), db, 0, old_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 0);
    EXPECT_FALSE(XStore::PinCatIdx(path, kYear, kRowLength)->MightContain(0, 0, EncodeFDataForCatIdx(old_value)));
    EXPECT_EQ(XStore::PinZoneMap(path, kYear, kRowLength)->TrackedColumnCount(), 64u);
    EXPECT_FALSE(XStore::PinZoneMap(path, kYear, kRowLength)->Rebuilding());
}

// RemoveDBFiles of the year a pass is widening: cancelled and waited for the same way; the year's files (half-built
// indexes included) are gone, its entries retired; a re-inserted year answers from its new rows only.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesOfTheYearBeingWidenedCancelsWaitsThenRemoves) {
    XStore store;
    const std::string db = "LWTRIGRMYEAR";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(path, kYear, kRowLength);
        WaitForFrontierPast(*zmap, Rpb());
    }
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        XStore_Proto::RemoveDBFiles_REQ req;
        req.add_filename(std::to_string(kYear) + ".XSTORE");
        return r.RemoveDBFiles(&req, db);
    });
    const bool removed_in_time = remove.wait_for(seconds(30)) == std::future_status::ready;
    const bool pass_ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(removed_in_time) << "RemoveDBFiles hung";
    ASSERT_TRUE(pass_ended) << "the pass hung";
    const XStore_Proto::RemoveDBFiles_REP rep = remove.get();
    ASSERT_EQ(rep.removestatus_size(), 3);
    for (int i = 0; i < 3; ++i) EXPECT_EQ(rep.removestatus(i).rfind("[INFO]", 0), 0u) << rep.removestatus(i);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    EXPECT_FALSE(XStore::bg_cancel_.load());
    for (const char *ext : {".XSTORE", ".zmap", ".catidx"}) EXPECT_FALSE(std::filesystem::exists(YearPath(db, ext))) << ext;
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(path, kYear).has_value());
    EXPECT_TRUE(store.RunLiveWidenPass(g + seconds(31)).empty()) << "no year file left to widen";
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);

    Insert(store, db, {{Epoch(5), ValueRow(5000, 70)}, {Epoch(Rpb() + 7), ValueRow(5001, 70)}});
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5066.0);
    EXPECT_EQ(XStore::PinZoneMap(path, kYear, kRowLength)->TrackedColumnCount(), 70u) << "a new year file at the DB's width";
}

// The TODO "compaction vs RemoveDB" race: a RemoveDB + CreateDB + insert during a compaction scan of a cold year. The
// scan is long (the year file is cut to 2M rows, about seconds of scanning); the test waits until the background
// handshake shows the compaction working on the year, then removes and re-creates. The old year's bitmap section must
// never land on the new DB's catidx, and FindByValue must find the new row.
TEST_F(XStoreLiveWidenTest, RemoveDbDuringACompactionScanNeverPutsTheOldBitmapOnTheNewCatIdx) {
    XStore store;
    const std::string db = "LWCOMPRM";
    const std::string path = DbPath(db);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r < 3000; ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r % 7, 2)); // few distinct values
    Insert(store, db, rows);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    ASSERT_EQ(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(2000000) * kRowLength), 0);
    auto compaction = std::async(std::launch::async, [&] {
        XStore c;
        c.CompactColdCatIdxForAllDbs(c.data_store_dir);
    });
    const std::string year_key = path + "/" + std::to_string(kYear);
    const Clock::time_point t0 = Clock::now();
    while (XStore::BackgroundActiveKeyForTests() != year_key && MsSince(t0) < 20000) std::this_thread::yield();
    ASSERT_EQ(XStore::BackgroundActiveKeyForTests(), year_key) << "the compaction never started on the year";

    ASSERT_NE(store.RemoveDB(db).removestatus().find("[INFO]"), std::string::npos);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(5), ValueRow(777, 2)}});
    // Bounds a compaction of the NEW year, should the scan of the data directory reach the re-created DB.
    static_cast<void>(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(4000) * kRowLength));
    ASSERT_EQ(compaction.wait_for(seconds(120)), std::future_status::ready);
    compaction.get();

    CatIdxFile::Pin catidx = XStore::PinCatIdx(path, kYear, kRowLength);
    const auto old_rows = catidx->LookupExact(0, EncodeFDataForCatIdx(FDouble(1000.0)));
    EXPECT_FALSE(old_rows.has_value() && !old_rows->empty()) << "the removed DB's bitmap section landed on the new catidx";
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(2999), db, 0, FDouble(777.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 1) << "the new row was not found";
}

// Widening and compaction on the same thread, one after the other: compaction skips a year left rebuilding by a
// stopped widen (no bitmap section written), and after a completed widen it builds the section at the new width.
TEST_F(XStoreLiveWidenTest, CompactionAfterAPassSkipsARebuildingYearAndCompactsAWidenedOne) {
    XStore store;
    const std::string db = "LWTRIGCOMP";
    const std::string path = DbPath(db);
    constexpr uint64_t kRows = 3000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    // Bound the compaction scans (the year file is preallocated to the whole year).
    ASSERT_EQ(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(kRows + 1000) * kRowLength), 0);

    // A stopped widen leaves the year rebuilding: compaction must not write a section into it.
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    WaitForFrontierPast(zmap, Rpb());
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(ended);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    XStore::bg_cancel_.store(false);
    store.CompactColdCatIdxForAllDbs(store.data_store_dir);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).bitmap_section_offset, 0u)
        << "compaction wrote into a rebuilding catidx";

    // Next start rebuilds it; then a fresh growth, a pass and a compaction, in that order.
    CleanRestartState();
    store.InitDB();
    Insert(store, db, {{Epoch(kRows + 10), ValueRow(4, 72)}}); // 70 -> 72
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g2 + seconds(30))), (PassResult{{kYear, Result::kDone}}));
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    store.CompactColdCatIdxForAllDbs(store.data_store_dir);
    CatIdxFile::Pin catidx = XStore::PinCatIdx(path, kYear, kRowLength, 72);
    EXPECT_EQ(catidx->TrackedColumnCount(), 72u);
    EXPECT_TRUE(catidx->HasBitmapSection()) << "the widened year was not compacted";
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// The fast path stays as it was: inserts at an unchanged width (or growth that stays under the 64-column floor) record
// nothing.
TEST_F(XStoreLiveWidenTest, InsertsAtAnUnchangedWidthRecordNothing) {
    XStore store;
    const std::string db = "LWTRIGFAST";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows narrow;
    for (uint64_t r = 0; r < 500; ++r) narrow.emplace_back(Epoch(r), ValueRow(r, 1 + static_cast<int>(r % 60)));
    Insert(store, db, narrow); // the column count grows 1 -> 60: still under the floor
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    Insert(store, db, {{Epoch(600), ValueRow(1, 70)}});
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    XStore::ClearLiveWidenRequestsForTests();
    Rows same;
    for (uint64_t r = 700; r < 1700; ++r) same.emplace_back(Epoch(r), ValueRow(r, r % 2 ? 70 : 30));
    Insert(store, db, same);
    for (uint64_t r = 2000; r < 2050; ++r) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(Epoch(r), ValueRow(r, 70), db, &rep);
    }
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
}

// ---- 11. Parallel live widen (S2): N workers inside one year ---------------------------------------------------------

// Waits (polling, 20 s bound) until pred() holds.
template <class Pred>
void WaitUntil(Pred pred, const char *what) {
    const Clock::time_point deadline = Clock::now() + std::chrono::seconds(20);
    while (!pred()) {
        ASSERT_LT(Clock::now(), deadline) << "timed out waiting: " << what;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

// Releases a WRITE lock the test holds on data-file bytes, on every exit path.
struct HeldWrite {
    FileRangeLock &lock;
    uint64_t offset, length;
    bool held = true;
    HeldWrite(FileRangeLock &l, uint64_t off, uint64_t len) : lock(l), offset(off), length(len) { lock.LockRange(offset, length, kWrite); }
    void Release() {
        if (held) lock.UnlockRange(offset, length, kWrite);
        held = false;
    }
    ~HeldWrite() { Release(); }
};

// MIN/MAX/SUM/AVG/EXISTS and FindByValue (of the largest value) of column c on rows [lo, hi], against a scan of the data
// file; an empty range must answer "no data" (EXISTS false).
void ExpectExactAnswers(XStore &store, const std::string &db, const std::string &data_path, uint64_t lo, uint64_t hi, int c,
                        unsigned long long (*at)(uint64_t)) {
    SCOPED_TRACE("rows [" + std::to_string(lo) + ", " + std::to_string(hi) + "] column " + std::to_string(c));
    const int fd = open(data_path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<std::pair<uint64_t, double>> values;
    for (const auto &[r, row] : ReadRows(fd, lo, hi, 2048)) {
        double v = 0;
        if (c < row.fielddata_size() && NumericOf(row.fielddata(c), v)) values.emplace_back(r, v);
    }
    close(fd);
    double mn = INFINITY, mx = -INFINITY, sum = 0;
    for (const auto &[r, v] : values) {
        mn = std::min(mn, v);
        mx = std::max(mx, v);
        sum += v;
    }
    const bool any = !values.empty();
    const unsigned short col = static_cast<unsigned short>(c);
    const auto success = [](const std::string &status) { return status.rfind("SUCCESS", 0) == 0; };
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(at(lo), at(hi), db, col, &min_rep);
    EXPECT_EQ(success(min_rep.opstatus()), any) << min_rep.opstatus();
    if (any) EXPECT_EQ(min_rep.opresult().doubledata(), mn);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(at(lo), at(hi), db, col, &max_rep);
    EXPECT_EQ(success(max_rep.opstatus()), any) << max_rep.opstatus();
    if (any) EXPECT_EQ(max_rep.opresult().doubledata(), mx);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(at(lo), at(hi), db, col, &sum_rep);
    EXPECT_EQ(success(sum_rep.opstatus()), any) << sum_rep.opstatus();
    if (any) EXPECT_EQ(sum_rep.opresult(), sum);
    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(at(lo), at(hi), db, col, &avg_rep);
    EXPECT_EQ(success(avg_rep.opstatus()), any) << avg_rep.opstatus();
    if (any) EXPECT_NEAR(avg_rep.opresult(), sum / static_cast<double>(values.size()), 1e-9 * std::max(1.0, std::fabs(sum)));
    XStore_Proto::Exists_REP exists_rep;
    store.ExistsAggregate(at(lo), at(hi), db, col, &exists_rep);
    EXPECT_TRUE(success(exists_rep.opstatus())) << exists_rep.opstatus();
    EXPECT_EQ(exists_rep.exists(), any);
    if (!any) return;
    std::set<unsigned long long> want;
    for (const auto &[r, v] : values) {
        if (v == mx) want.insert(at(r));
    }
    std::string buf;
    store.FindByValue(at(lo), at(hi), db, c, FDouble(mx), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP rep;
    std::set<unsigned long long> got;
    if (rep.ParseFromString(buf)) {
        for (const auto &e : rep.queryresponse()) got.insert(e.timestamp());
    }
    EXPECT_EQ(got, want) << "FindByValue " << mx;
}

unsigned long long EpochOfRow(uint64_t row) { return Jan1(1980) + row; }

// Byte-identical: the widened .zmap/.catidx equal the startup rebuild of a copy of the same year for 1, 2, 4 and 8
// workers, with one zone-map block per group (so dense data has many groups and the year's empty tail thousands).
class XStoreParallelLiveWidenTest : public XStoreLiveWidenTest {
protected:
    void ExpectEveryWorkerCountMatchesTheStartupRebuild(const std::string &db, int width, const Rows &rows) {
        XStore store;
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        Insert(store, db, {{Epoch(0), MixedRow(0, 10)}}); // index files born 64 wide
        Insert(store, db, rows);
        BuildStartupReference(store, db, db + "REF", width);
        const std::string path = DbPath(db);
        uint64_t rows_folded = 0;
        for (unsigned workers : {1u, 2u, 4u, 8u}) {
            SCOPED_TRACE(std::to_string(workers) + " workers");
            XStore::LiveWidenStats stats;
            ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, static_cast<uint32_t>(width), true, true, nullptr,
                                             &stats, workers, Rpb() * kRowLength), Result::kDone);
            EXPECT_TRUE(stats.zmap_widened && stats.catidx_widened);
            if (workers == 1) rows_folded = stats.rows_folded;
            EXPECT_EQ(stats.rows_folded, rows_folded) << "every present row folded exactly once";
            EXPECT_EQ(stats.lock_hold_us.size(), stats.chunks);
            ExpectSameFilesAs(db, db + "REF");
            EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
        }
        ExpectAnswersMatchTheData(store, db, 0, 2 * Rpb() - 1, {0, 2, 64, width - 4});
    }
};

TEST_F(XStoreParallelLiveWidenTest, DenseRowsAreByteIdenticalForEveryWorkerCount) {
    Rows rows;
    for (uint64_t i = 1; i < 20 * Rpb() + 300; ++i) rows.emplace_back(Epoch(i), MixedRow(i, i % 10 == 0 ? 70 : 40 + static_cast<int>(i % 9)));
    ExpectEveryWorkerCountMatchesTheStartupRebuild("LWPDENSE", 70, rows);
}

TEST_F(XStoreParallelLiveWidenTest, SparseRowsAreByteIdenticalForEveryWorkerCount) {
    Rows rows;
    for (uint64_t i = 37; i < 30 * Rpb(); i += 37) rows.emplace_back(Epoch(i), MixedRow(i, 70));
    for (uint64_t r : {200000ULL, 200001ULL, 1000000ULL, 3000000ULL, 30000000ULL}) rows.emplace_back(Epoch(r), MixedRow(r, 70));
    ExpectEveryWorkerCountMatchesTheStartupRebuild("LWPSPARSE", 70, rows);
}

TEST_F(XStoreParallelLiveWidenTest, WideRowsAreByteIdenticalForEveryWorkerCount) {
    Rows rows;
    for (uint64_t i = 1; i < 12 * Rpb(); ++i) rows.emplace_back(Epoch(i), MixedRow(i, 100));
    ExpectEveryWorkerCountMatchesTheStartupRebuild("LWPWIDE", 100, rows);
}

// THE lost update. The worker that claims group 0 stalls on row 0 (the test holds a WRITE lock on it), so the frontier
// stays at 0 while the other workers fold groups 1..15. Then writes land in those folded groups ABOVE the frontier:
// fresh rows (Unary, Batch), a Range insert and a Unary overwrite, all three deletes (one resets a whole block). Each
// must update the index (or flag/reset its block): the fold already read the old rows. With the old check (skip every
// row >= F) the index keeps the old rows and answers wrong. Then row 0 is released and the widen finishes.
TEST_F(XStoreParallelLiveWidenTest, WritesIntoAFoldedGroupAboveTheFrontierAreNotLost) {
    XStore store;
    const std::string db = "LWPFOLDED";
    constexpr int kWidth = 70;
    const uint64_t rpb = Rpb();
    constexpr uint64_t kBlocks = 16;
    const uint64_t rows = kBlocks * rpb;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
    {
        Rows seed; // even rows: the odd ones are holes for fresh inserts
        for (uint64_t r = 2; r < rows; r += 2) seed.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, kWidth));
        Insert(store, db, seed);
    }
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    std::future<Result> builder; // destroyed (waited for) after the held lock below is released
    HeldWrite row0(FileLockRegistry::Get(data_path), 0, kRowLength);
    builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, kWidth, true, true, nullptr, &stats, 4, rpb * kRowLength);
    });
    WaitUntil([&] {
        if (!zmap.Rebuilding()) return false;
        for (uint64_t b = 1; b < kBlocks; ++b) {
            if (!zmap.WriterUpdatesRowForTests((b + 1) * rpb - 1)) return false;
        }
        return true;
    }, "groups 1..15 folded");
    ASSERT_EQ(zmap.FrontierForTests(), 0u) << "the frontier passed the stalled group 0";
    ASSERT_EQ(catidx.FrontierForTests(), 0u);
    ASSERT_FALSE(zmap.WriterUpdatesRowForTests(5)) << "group 0 is not folded: its writers must leave it to the builder";

    const auto ok = [](const std::string &status) { return status.rfind("[INFO]", 0) == 0; };
    {
        XStore_Proto::UnaryInsert_REP rep; // unfolded group 0: left to the builder
        store.UnaryInsert(Epoch(5), ValueRow(400000, kWidth), db, &rep, XStore_Proto::SYNC);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::RangeInsert_REQ req; // crosses from the unfolded group 0 into the folded group 1 (not row 0: it is held)
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (uint64_t r = rpb - rpb / 4; r < rpb + rpb / 4; ++r) {
            XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
            e->set_timestamp(Epoch(r));
            *e->mutable_data() = ValueRow(1000000 + r, kWidth);
        }
        XStore_Proto::RangeInsert_REP rep;
        store.RangeInsert(&req, &rep);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    ASSERT_NE(store.RangeDelete(Epoch(rpb / 2), Epoch(rpb + rpb / 2 - 1), db).deletestatus().find("[INFO]"),
              std::string::npos) << "a range delete crossing from the unfolded group 0 into the folded group 1";
    {
        XStore_Proto::UnaryInsert_REP rep; // fresh row, block 3: the new maximum of its block
        store.UnaryInsert(Epoch(3 * rpb + 1), ValueRow(500000, kWidth), db, &rep, XStore_Proto::SYNC);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::BatchInsert_REQ req; // fresh rows, blocks 4 and 5
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (const auto &[r, v] : std::vector<std::pair<uint64_t, uint64_t>>{{4 * rpb + 3, 600000}, {5 * rpb + 5, 700000}}) {
            XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
            e->set_timestamp(Epoch(r));
            *e->mutable_data() = ValueRow(v, kWidth);
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::RangeInsert_REQ req; // 20 consecutive rows of block 6: fresh odd rows and overwritten even rows
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (uint64_t i = 0; i < 20; ++i) {
            XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
            e->set_timestamp(Epoch(6 * rpb + 10 + i));
            *e->mutable_data() = ValueRow(800000 + i, kWidth);
        }
        XStore_Proto::RangeInsert_REP rep;
        store.RangeInsert(&req, &rep);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::UnaryInsert_REP rep; // overwrite, block 7
        store.UnaryInsert(Epoch(7 * rpb + 2), ValueRow(900000, kWidth), db, &rep, XStore_Proto::SYNC);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    // Deletes of the largest values of blocks 8, 9 (and a cut range in 11), and all of block 10 (the reset path).
    const auto max_row_of_block = [&](uint64_t b) {
        uint64_t best = b * rpb + 2;
        for (uint64_t r = b * rpb + 2; r < (b + 1) * rpb; r += 2) {
            if (r * 7 % 100000 > best * 7 % 100000) best = r;
        }
        return best;
    };
    ASSERT_NE(store.UnaryDelete(Epoch(max_row_of_block(8)), db).deletestatus().find("[INFO]"), std::string::npos);
    {
        XStore_Proto::BatchDelete_REQ req;
        req.set_dbname(db);
        req.add_timestamp(Epoch(9 * rpb + 6));
        req.add_timestamp(Epoch(max_row_of_block(9)));
        ASSERT_NE(store.BatchDelete(&req).deletestatus().find("[INFO]"), std::string::npos);
    }
    ASSERT_NE(store.RangeDelete(Epoch(10 * rpb), Epoch(11 * rpb - 1), db).deletestatus().find("[INFO]"), std::string::npos);
    ASSERT_NE(store.RangeDelete(Epoch(11 * rpb + 100), Epoch(11 * rpb + 300), db).deletestatus().find("[INFO]"),
              std::string::npos);
    EXPECT_EQ(zmap.FrontierForTests(), 0u) << "the writes must have landed above the frontier";

    row0.Release();
    ASSERT_EQ(builder.wait_for(std::chrono::seconds(30)), std::future_status::ready) << "the widen did not finish";
    ASSERT_EQ(builder.get(), Result::kDone);
    EXPECT_FALSE(zmap.Rebuilding());

    for (uint64_t b = 0; b < kBlocks; ++b) {
        for (int c : {0, 33, 65, 69}) ExpectExactAnswers(store, db, data_path, b * rpb, (b + 1) * rpb - 1, c, EpochOfRow);
    }
    ExpectExactAnswers(store, db, data_path, 0, rows - 1, 65, EpochOfRow);

    // The live index against a startup rebuild of the final data: blocks the writes kept exact (fresh rows, the reset)
    // are identical entry for entry; flagged ones only bound min/max.
    std::vector<std::vector<ZoneMapEntry>> live(kBlocks);
    for (uint64_t b = 0; b < kBlocks; ++b) {
        auto st = zmap.TryGetBlockStats(b);
        ASSERT_TRUE(st.has_value()) << "block " << b;
        live[b] = *st;
    }
    {   // Bloom: every present value of every tracked column.
        const int fd = open(data_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        uint64_t misses = 0;
        for (const auto &[r, row] : ReadRows(fd, 0, rows - 1, kRowLength)) {
            for (int c = 0; c < std::min(kWidth, row.fielddata_size()); ++c) {
                if (row.fielddata(c).oneOfData_case() == XStore_Proto::FData::ONEOFDATA_NOT_SET) continue;
                if (!catidx.MightContain(BlockIndexForRow(r, rpb), static_cast<uint32_t>(c), EncodeFDataForCatIdx(row.fielddata(c)))) {
                    if (++misses < 5) ADD_FAILURE() << "bloom misses row " << r << " column " << c;
                }
            }
        }
        close(fd);
        EXPECT_EQ(misses, 0u);
    }
    BuildStartupReference(store, db, db + "REF", kWidth);
    ZoneMapFile &oracle = XStore::GetOrOpenZoneMap(DbPath(db + "REF"), kYear, kRowLength);
    for (uint64_t b = 0; b < kBlocks; ++b) {
        SCOPED_TRACE("block " + std::to_string(b));
        auto o = oracle.TryGetBlockStats(b);
        ASSERT_TRUE(o.has_value());
        ASSERT_EQ(o->size(), live[b].size());
        const bool flagged = ZoneMapEntryInexact(live[b][0]);
        const bool expect_flagged = (b >= 6 && b <= 11 && b != 10) || b == 1; // block 1: the crossing range overwrite and delete
        EXPECT_EQ(flagged, expect_flagged) << "overwrites and deletes flag their block; fresh rows and the reset do not";
        if (!flagged) {
            EXPECT_EQ(std::memcmp(live[b].data(), o->data(), o->size() * sizeof(ZoneMapEntry)), 0) << "differs from the startup rebuild";
            continue;
        }
        for (size_t c = 0; c < o->size(); ++c) {
            if ((*o)[c].valid_count == 0) continue;
            EXPECT_LE(live[b][c].min_value, (*o)[c].min_value) << "column " << c;
            EXPECT_GE(live[b][c].max_value, (*o)[c].max_value) << "column " << c;
        }
    }
}

// Section 2's oracle with four workers and one block per group: writers race many folded groups above the frontier.
TEST_F(XStoreParallelLiveWidenTest, AnswersStayExactUnderConcurrentWritesWithFourWorkers) {
    RunConcurrentOracle("LWPLIVE", 4, Rpb() * kRowLength);
}

// Stop with four workers: the files stay dirty at the contiguous watermark (a held row in the last data block keeps
// the build from finishing first), never trusted past it, and the next start rebuilds them.
TEST_F(XStoreParallelLiveWidenTest, StopMidBuildLeavesTheContiguousWatermarkAndTheNextStartRebuilds) {
    XStore store;
    const std::string db = "LWPSTOP";
    constexpr uint64_t kRows = 60000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    std::atomic<bool> stop{false};
    XStore::LiveWidenStats stats;
    std::future<Result> builder;
    HeldWrite last(FileLockRegistry::Get(YearPath(db, ".XSTORE")), (kRows - 1) * kRowLength, kRowLength);
    builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, &stop, &stats, 4, Rpb() * kRowLength);
    });
    WaitForFrontierPast(zmap, 3 * Rpb());
    stop.store(true);
    ASSERT_EQ(builder.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "the stop was not honoured";
    ASSERT_EQ(builder.get(), Result::kStopped);
    last.Release();

    const uint64_t f = zmap.FrontierForTests();
    ASSERT_NE(f, ZoneMapFile::kNoRebuild);
    EXPECT_LT(f, kRows);
    EXPECT_EQ(catidx.FrontierForTests(), f) << "both files mark the same chunks";
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    const uint64_t complete_blocks = f / Rpb();
    ASSERT_GT(complete_blocks, 0u);
    EXPECT_TRUE(zmap.TryGetBlockStats(complete_blocks - 1).has_value());
    EXPECT_FALSE(zmap.TryGetBlockStats(complete_blocks).has_value()) << "a block past F must not be trusted";
    EXPECT_TRUE(catidx.MightContain(complete_blocks, 0, EncodeFDataForCatIdx(FDouble(-1.0))));
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    EXPECT_FALSE(zmap.MarkClean());
    EXPECT_FALSE(catidx.MarkClean());

    XStore::Shutdown();
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    BuildStartupReference(store, db, db + "REF", 70); // its InitDB also rebuilds LWPSTOP's dirty index
    ExpectSameFilesAs(db, db + "REF");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// A read failure with four workers: kFailed, both files in rebuild and dirty behind the failed chunk, answers right,
// the next start rebuilds.
TEST_F(XStoreParallelLiveWidenTest, AReadFailureLeavesTheIndexInRebuildBehindTheFailedChunk) {
    XStore store;
    const std::string db = "LWPPREAD";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 2, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, nullptr, 4, Rpb() * kRowLength),
                  Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    const uint64_t f = zmap.FrontierForTests();
    EXPECT_LT(f, kRows) << "the frontier passed the failed chunk";
    EXPECT_EQ(catidx.FrontierForTests(), f);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}

// A .zmap block write fails with four workers: the zone map stops behind its frontier (dirty, in rebuild), the catidx
// carries on and finishes clean; kFailed.
TEST_F(XStoreParallelLiveWidenTest, AZoneMapWriteFailureStopsOnlyTheZoneMap) {
    XStore store;
    const std::string db = "LWPPWRITE";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    CleanRestartState();
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, ".zmap"), 5, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 4, Rpb() * kRowLength),
                  Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    EXPECT_FALSE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_LT(zmap.FrontierForTests(), kRows);
    EXPECT_FALSE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}

// A WRITE lock held on one data row of group 5: that worker retries (lock timeouts) while the others fold every later
// group; the frontier never passes the held row; once it is released the widen completes within a bounded wait.
TEST_F(XStoreParallelLiveWidenTest, AHeldRowStallsOnlyItsGroup) {
    XStore store;
    const std::string db = "LWPHELD";
    const uint64_t rpb = Rpb();
    const uint64_t rows = 16 * rpb;
    SeedNarrowIndexWideRows(store, db, rows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    const uint64_t held_row = 5 * rpb + 3;
    XStore::LiveWidenStats stats;
    std::future<Result> builder;
    HeldWrite held(FileLockRegistry::Get(YearPath(db, ".XSTORE")), held_row * kRowLength, kRowLength);
    builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 4, rpb * kRowLength);
    });
    WaitUntil([&] {
        if (!zmap.Rebuilding()) return false;
        for (uint64_t b = 6; b < 16; ++b) {
            if (!zmap.WriterUpdatesRowForTests((b + 1) * rpb - 1)) return false;
        }
        return true;
    }, "the groups after the held one folded");
    // Longer than one lock wait (LIVE_WIDEN_LOCK_WAIT_MS): the stalled worker has timed out at least once.
    std::this_thread::sleep_for(milliseconds(3 * XStoreTunables::LIVE_WIDEN_LOCK_WAIT_MS));
    const uint64_t f = zmap.FrontierForTests();
    EXPECT_LE(f, held_row) << "the frontier passed a row a writer holds";
    EXPECT_GE(f, 5 * rpb) << "groups 0..4 should be folded";
    EXPECT_NE(builder.wait_for(milliseconds(0)), std::future_status::ready) << "finished while the row was held";
    held.Release();
    ASSERT_EQ(builder.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_GT(stats.lock_timeouts, 0u);
    ExpectAnswersMatchTheData(store, db, 0, rows - 1, {0, 65});
}

// The trigger with four workers: bg_cancel_ (shutdown, RemoveDB) is honoured within about one chunk (a lock wait plus
// a pause, each at most 50 ms), not after the year; the files stay dirty and the next start rebuilds them.
TEST_F(XStoreParallelLiveWidenTest, ACancelStopsEveryWorkerWithinAChunk) {
    XStore store;
    const std::string db = "LWPCANCEL";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    std::future<std::vector<XStore::LiveWidenPassEntry>> pass;
    HeldWrite after_data(FileLockRegistry::Get(YearPath(db, ".XSTORE")), kRows * kRowLength, kRowLength);
    pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30), 4);
    });
    WaitForFrontierPast(zmap, Rpb());
    const Clock::time_point cancelled = Clock::now();
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    const int64_t stop_ms = MsSince(cancelled);
    after_data.Release();
    ASSERT_TRUE(ended) << "the pass ignored the cancel flag";
    EXPECT_LT(stop_ms, 1000) << "the workers took " << stop_ms << " ms to stop";
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    XStore::bg_cancel_.store(false);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_LE(zmap.FrontierForTests(), kRows);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}


} // namespace
