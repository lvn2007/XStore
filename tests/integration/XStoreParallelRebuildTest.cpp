// Parallel startup index rebuild (Tech Reference 4.14, S1 of docs/superpowers/specs/2026-10-14-parallel-index-rebuild-
// design.md): XStore::RebuildYearsIndexes splits every year into groups of chunks that several threads fold at once.
// Checks: the bytes do not depend on the thread count; a group that finishes before a lower one never makes the lower
// one's rows count as folded (the contiguous watermark); a read failure or an index write failure in the middle of a
// year leaves the frontier at a chunk boundary below which every block is exactly the reference's, the blocks above it
// untrusted, the file dirty, and the next start rebuilds it to the reference.
#include <gtest/gtest.h>
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_WalRing.h"
#include "XStoreTestSupport.h"
#include "XStoreRebuildTestSupport.h"
#include "XStoreSyscallFault.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <map>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

namespace {
using namespace xstore_test;
using namespace xstore_rebuild_test;

constexpr uint16_t kRowLength = 1024; // 2048 rows per block, 4096 per chunk
constexpr uint64_t kGroupBytes = 2 * XStoreTunables::REBUILD_CHUNK_BYTES; // two chunks: 8192 rows per group

uint64_t ChunkRows() {
    const uint64_t rpb = RowsPerBlock(kRowLength);
    return std::max<uint64_t>(1, XStoreTunables::REBUILD_CHUNK_BYTES / (rpb * kRowLength)) * rpb;
}

class XStoreParallelRebuildTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_parallel_rebuild_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
    }
    void TearDown() override {
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        scratch.reset();
    }

    std::string DbPath(const std::string &db) const { return (scratch->test_root / "DataStore" / db).string(); }
    std::string YearPath(const std::string &db, unsigned short year, const char *ext) const {
        return DbPath(db) + "/" + std::to_string(year) + ext;
    }
    std::string RefPath(unsigned short year, const char *ext) const {
        return (scratch->test_root / ("ref_" + std::to_string(year) + ext)).string();
    }

    // Writes `rows` consecutive rows starting at `first_row` straight into the year file (one pwrite).
    static void WriteRows(const std::string &path, uint64_t first_row, const std::vector<std::string> &rows) {
        std::string buf(rows.size() * kRowLength, '\0');
        for (size_t i = 0; i < rows.size(); ++i) {
            ASSERT_LE(rows[i].size(), kRowLength);
            std::memcpy(&buf[i * kRowLength], rows[i].data(), rows[i].size());
        }
        const int fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
        ASSERT_GE(fd, 0) << path;
        ASSERT_EQ(pwrite(fd, buf.data(), buf.size(), static_cast<off_t>(first_row * kRowLength)),
                  static_cast<ssize_t>(buf.size()));
        close(fd);
    }
    static void ExtendTo(const std::string &path, uint64_t rows) {
        const int fd = open(path.c_str(), O_WRONLY | O_CREAT, 0644);
        ASSERT_GE(fd, 0) << path;
        ASSERT_EQ(ftruncate(fd, static_cast<off_t>(rows * kRowLength)), 0);
        close(fd);
    }
    static std::vector<std::string> Mixed(uint64_t first, uint64_t count, int columns) {
        std::vector<std::string> out;
        out.reserve(count);
        for (uint64_t i = first; i < first + count; ++i) {
            out.push_back(MixedRow(i, columns > 0 ? columns : (i % 50 == 0 ? 80 : 40 + static_cast<int>(i % 9))).SerializeAsString());
        }
        return out;
    }

    // Runs the engine over `years` of `db` (both indexes) and returns each year's (.zmap, .catidx) bytes.
    std::map<unsigned short, std::pair<std::string, std::string>> Rebuild(XStore &store, const std::string &db,
                                                                          const std::vector<unsigned short> &years,
                                                                          uint32_t width, unsigned threads,
                                                                          const rebuildprogress::Options &progress = {}) {
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        std::vector<XStore::RebuildYearTask> tasks;
        for (unsigned short y : years) tasks.push_back({y, true, true});
        const std::vector<std::exception_ptr> failures =
            store.RebuildYearsIndexes(DbPath(db), tasks, kRowLength, width, threads, kGroupBytes, progress);
        EXPECT_EQ(failures.size(), years.size());
        for (const std::exception_ptr &f : failures) EXPECT_FALSE(f) << "a year threw";
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        std::map<unsigned short, std::pair<std::string, std::string>> out;
        for (unsigned short y : years) {
            EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, y, ".zmap")).dirty, 0u) << y;
            EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, y, ".catidx")).dirty, 0u) << y;
            out[y] = {ReadFileBytes(YearPath(db, y, ".zmap")), ReadFileBytes(YearPath(db, y, ".catidx"))};
        }
        return out;
    }

    void ExpectMatchesReference(XStore &store, const std::string &db, unsigned short year, uint32_t width,
                                const std::pair<std::string, std::string> &got) {
        BuildReferenceIndexes(store, YearPath(db, year, ".XSTORE"), RefPath(year, ".zmap"), RefPath(year, ".catidx"),
                              kRowLength, width);
        EXPECT_TRUE(got.first == ReadFileBytes(RefPath(year, ".zmap"))) << year << ": .zmap differs from the reference";
        EXPECT_TRUE(got.second == ReadFileBytes(RefPath(year, ".catidx"))) << year << ": .catidx differs from the reference";
    }

    static void Insert(XStore &store, const std::string &db, unsigned short year, uint64_t rows, int columns) {
        for (uint64_t first = 0; first < rows; first += 5000) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(db);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (uint64_t i = first; i < std::min(rows, first + 5000); ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(Jan1(year) + i);
                *entry->mutable_data() = MixedRow(i, columns);
            }
            XStore_Proto::BatchInsert_REP rep;
            store.BatchInsert(&req, &rep);
            ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
        }
    }
};

constexpr uint32_t kShapeWidth = 80;
constexpr unsigned short kDense = 1971, kSparse = 1972, kHoles = 1973, kEmpty = 1974;

// Every data shape, several years at once, many groups per year: 1, 2, 5 and 16 threads give the same bytes, and those
// are the per-row reference's.
TEST_F(XStoreParallelRebuildTest, EveryThreadCountGivesTheReferenceBytesOnEveryDataShape) {
    const std::string db = "PARREBUILD";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    // Dense: 30000 rows (4 groups, the last one partial), widths 40..48 with every 50th row 80 wide, and damaged rows
    // (the one-pass decoder declines them) inside the first group and on a group edge.
    WriteRows(YearPath(db, kDense, ".XSTORE"), 0, Mixed(0, 30000, 0));
    PwriteRow(YearPath(db, kDense, ".XSTORE"), 10, kRowLength, DoubleColumn(1.5) + std::string("\x12\x01\x41", 3));
    PwriteRow(YearPath(db, kDense, ".XSTORE"), 8191, kRowLength, DoubleColumn(3.5) + std::string("\x0a\xff\x0f", 3));
    PwriteRow(YearPath(db, kDense, ".XSTORE"), 8192, kRowLength, std::string("\x08\x05", 2) + DoubleColumn(2.5));
    // Sparse: rows on block, chunk and group edges and far apart, over the whole 31.5M-row year (about 3850 groups).
    for (uint64_t r : {0ULL, 1ULL, 2047ULL, 2048ULL, 4095ULL, 4096ULL, 8191ULL, 8192ULL, 100000ULL, 100001ULL, 3000000ULL, 31535999ULL}) {
        WriteRows(YearPath(db, kSparse, ".XSTORE"), r, Mixed(r, 1, 1 + static_cast<int>(r % 7)));
    }
    // Holes: two dense runs with a long hole between them (whole empty groups) and a trailing hole.
    WriteRows(YearPath(db, kHoles, ".XSTORE"), 0, Mixed(0, 6000, 30));
    WriteRows(YearPath(db, kHoles, ".XSTORE"), 30000, Mixed(30000, 6000, 12));
    ExtendTo(YearPath(db, kHoles, ".XSTORE"), 60000);
    // Empty: a year file with no row at all.
    ExtendTo(YearPath(db, kEmpty, ".XSTORE"), 50000);
    if (HasFatalFailure()) return;

    const std::vector<unsigned short> years = {kDense, kSparse, kHoles, kEmpty};
    const auto one = Rebuild(store, db, years, kShapeWidth, 1);
    for (unsigned short y : years) {
        SCOPED_TRACE(y);
        ExpectMatchesReference(store, db, y, kShapeWidth, one.at(y));
    }
    for (unsigned threads : {2u, 5u, 16u}) {
        SCOPED_TRACE(threads);
        const auto many = Rebuild(store, db, years, kShapeWidth, threads);
        for (unsigned short y : years) {
            EXPECT_TRUE(many.at(y).first == one.at(y).first) << y << ": .zmap depends on the thread count";
            EXPECT_TRUE(many.at(y).second == one.at(y).second) << y << ": .catidx depends on the thread count";
        }
    }
}

// The progress display on: 1 and 5 threads over dense, holey and empty years give the reference bytes, the final line
// reads 100% of every row (holes included) and every year, lines are logged while it runs, and nothing is logged after
// the engine returned (the reporter thread is joined).
TEST_F(XStoreParallelRebuildTest, ProgressDisplayLeavesTheBytesAloneAndEndsAtOneHundredPercent) {
    const std::string db = "PARREBUILDPROG";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    WriteRows(YearPath(db, kDense, ".XSTORE"), 0, Mixed(0, 30000, 0));
    WriteRows(YearPath(db, kHoles, ".XSTORE"), 0, Mixed(0, 6000, 30));
    WriteRows(YearPath(db, kHoles, ".XSTORE"), 30000, Mixed(30000, 6000, 12));
    ExtendTo(YearPath(db, kHoles, ".XSTORE"), 60000);
    ExtendTo(YearPath(db, kEmpty, ".XSTORE"), 50000);
    if (HasFatalFailure()) return;
    const std::vector<unsigned short> years = {kDense, kHoles, kEmpty};
    const auto plain = Rebuild(store, db, years, kShapeWidth, 1);
    for (unsigned short y : years) {
        SCOPED_TRACE(y);
        ExpectMatchesReference(store, db, y, kShapeWidth, plain.at(y));
    }
    for (unsigned threads : {1u, 5u}) {
        SCOPED_TRACE(threads);
        ProgressCapture capture;
        const auto shown = Rebuild(store, db, years, kShapeWidth, threads, capture.Options());
        for (unsigned short y : years) {
            EXPECT_TRUE(shown.at(y) == plain.at(y)) << y << ": the bytes changed with the progress display on";
        }
        const size_t at_return = capture.Lines().size();
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        const std::vector<std::string> lines = capture.Lines();
        EXPECT_EQ(lines.size(), at_return) << "the reporter outlived the engine";
        ASSERT_GE(lines.size(), 1u);
        EXPECT_NE(lines.back().find("100% (140.0K of 140.0K rows), 3/3 years done"), std::string::npos) << lines.back();
        EXPECT_NE(lines.back().find("[DB: PARREBUILDPROG]"), std::string::npos) << lines.back();
        EXPECT_NE(lines.back().find(", done"), std::string::npos) << lines.back();
    }
}

// One year, more threads than groups (4 groups, 16 threads), and the default group size (one group for this year).
TEST_F(XStoreParallelRebuildTest, OneYearWithMoreThreadsThanGroups) {
    const std::string db = "PARREBUILD1";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    WriteRows(YearPath(db, kDense, ".XSTORE"), 0, Mixed(0, 30000, 0));
    if (HasFatalFailure()) return;
    const auto one = Rebuild(store, db, {kDense}, kShapeWidth, 1);
    ExpectMatchesReference(store, db, kDense, kShapeWidth, one.at(kDense));
    const auto many = Rebuild(store, db, {kDense}, kShapeWidth, 16);
    EXPECT_TRUE(many.at(kDense) == one.at(kDense));

    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    const std::vector<std::exception_ptr> failures =
        store.RebuildYearsIndexes(DbPath(db), {{kDense, true, true}}, kRowLength, kShapeWidth, 16);
    ASSERT_EQ(failures.size(), 1u);
    EXPECT_FALSE(failures[0]);
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    EXPECT_TRUE(ReadFileBytes(YearPath(db, kDense, ".zmap")) == one.at(kDense).first);
    EXPECT_TRUE(ReadFileBytes(YearPath(db, kDense, ".catidx")) == one.at(kDense).second);
}

// Out-of-order completion: group 0 holds 8192 wide rows, each of the other 15 groups one narrow row, and there are as
// many threads as groups, so the light groups finish long before group 0. A frontier moved to the end of a finished
// group without waiting for the groups below it would make group 0's rows count as already folded (RebuildFoldRows
// skips rows below the frontier): its blocks would stay empty and the bytes would differ from the 1-thread build.
TEST_F(XStoreParallelRebuildTest, GroupsFinishingBeforeALowerGroupDoNotSkipItsRows) {
    const std::string db = "PARREBUILDOOO";
    const unsigned short year = 1975;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const uint64_t group_rows = 2 * ChunkRows();
    WriteRows(YearPath(db, year, ".XSTORE"), 0, Mixed(0, group_rows, 80));
    for (uint64_t g = 1; g < 16; ++g) WriteRows(YearPath(db, year, ".XSTORE"), g * group_rows + 7, Mixed(g, 1, 3));
    ExtendTo(YearPath(db, year, ".XSTORE"), 16 * group_rows);
    if (HasFatalFailure()) return;
    const auto one = Rebuild(store, db, {year}, kShapeWidth, 1);
    ExpectMatchesReference(store, db, year, kShapeWidth, one.at(year));
    for (int round = 0; round < 3; ++round) {
        const auto many = Rebuild(store, db, {year}, kShapeWidth, 16);
        EXPECT_TRUE(many.at(year).first == one.at(year).first) << "round " << round << ": .zmap lost rows";
        EXPECT_TRUE(many.at(year).second == one.at(year).second) << "round " << round << ": .catidx lost rows";
    }
}

// The failure tests: one year of 30000 16-column rows (4 groups of 2 chunks), inserted through the API so InitDB, the
// "next start", rebuilds at the same width.
class XStoreParallelRebuildFaultTest : public XStoreParallelRebuildTest {
protected:
    const std::string db = "PARREBUILDFAULT";
    const unsigned short year = 1981;
    static constexpr uint64_t kRows = 30000;
    uint32_t width = 0;

    void Seed(XStore &store) {
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        Insert(store, db, year, kRows, 16);
        XStore::Shutdown();
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        width = ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).tracked_column_count;
        ASSERT_GT(width, 0u);
        BuildReferenceIndexes(store, YearPath(db, year, ".XSTORE"), RefPath(year, ".zmap"), RefPath(year, ".catidx"),
                              kRowLength, width);
    }
    void ResetIndexes() {
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        std::filesystem::remove(YearPath(db, year, ".zmap"));
        std::filesystem::remove(YearPath(db, year, ".catidx"));
    }
    std::exception_ptr RunEngine(XStore &store, unsigned threads, const rebuildprogress::Options &progress = {}) {
        const std::vector<std::exception_ptr> failures =
            store.RebuildYearsIndexes(DbPath(db), {{year, true, true}}, kRowLength, width, threads, kGroupBytes, progress);
        EXPECT_EQ(failures.size(), 1u);
        return failures.empty() ? nullptr : failures[0];
    }

    // The zone map stopped at its frontier F: F is a chunk boundary below the end, every block below it equals the
    // reference's block, every block at or above it is untrusted; the file is dirty and still in rebuild.
    uint64_t ExpectZoneMapStoppedAtAnHonestFrontier() {
        ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(DbPath(db), year, kRowLength);
        EXPECT_TRUE(zmap.Rebuilding());
        const uint64_t f = zmap.FrontierForTests();
        EXPECT_LT(f, kRows);
        EXPECT_EQ(f % ChunkRows(), 0u) << "the frontier must sit on a chunk boundary";
        EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).dirty, 1u);
        ZoneMapFile ref;
        EXPECT_TRUE(ref.Open(RefPath(year, ".zmap"), kRowLength, width));
        const uint64_t rpb = RowsPerBlock(kRowLength);
        const uint64_t blocks = (kRows + rpb - 1) / rpb;
        for (uint64_t b = 0; b < blocks; ++b) {
            const auto got = zmap.TryGetBlockStats(b);
            if (b < f / rpb) {
                const auto want = ref.TryGetBlockStats(b);
                EXPECT_TRUE(got.has_value() && want.has_value()) << "block " << b << " below F must be trusted";
                if (got && want) {
                    EXPECT_TRUE(got->size() == want->size() &&
                                std::memcmp(got->data(), want->data(), got->size() * sizeof(ZoneMapEntry)) == 0)
                        << "block " << b << " below F differs from the reference: it was not fully folded";
                }
            } else {
                EXPECT_FALSE(got.has_value()) << "block " << b << " at or above F must not be trusted";
            }
        }
        return f;
    }
    // A query over the whole year answers from the data wherever the index is untrusted.
    void ExpectMaxIsExact(XStore &store) {
        XStore_Proto::Max_REP max_rep;
        store.MaxAggregate(Jan1(year), Jan1(year) + kRows - 1, db, 0, &max_rep);
        EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), static_cast<double>((kRows - 1) * 31) * 0.5);
    }
    void ExpectNextStartRebuildsToTheReference(XStore &store) {
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        store.InitDB();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).dirty, 0u);
        EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 0u);
        EXPECT_TRUE(ReadFileBytes(YearPath(db, year, ".zmap")) == ReadFileBytes(RefPath(year, ".zmap")));
        EXPECT_TRUE(ReadFileBytes(YearPath(db, year, ".catidx")) == ReadFileBytes(RefPath(year, ".catidx")));
    }
};

// A data-file read fails in the middle of the year (every read after the first 12 fails: 4 reads per chunk) with 1, 4
// and 16 threads: both indexes stay dirty and in rebuild at the same honest frontier, nothing is marked clean, queries
// stay exact, and the next start rebuilds both to the reference. With one thread the frontier is exact: 3 chunks.
TEST_F(XStoreParallelRebuildFaultTest, ReadFailureMidYearStopsBothIndexesAtTheContiguousWatermark) {
    XStore store;
    Seed(store);
    if (HasFatalFailure()) return;
    for (unsigned threads : {1u, 4u, 16u}) {
        SCOPED_TRACE(threads);
        ResetIndexes();
        {
            xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, year, ".XSTORE"), 12, 1000, EIO);
            EXPECT_FALSE(RunEngine(store, threads));
            EXPECT_GT(fail.Failures(), 0);
        }
        const uint64_t f = ExpectZoneMapStoppedAtAnHonestFrontier();
        if (threads == 1) {
            EXPECT_EQ(f, 3 * ChunkRows());
        }
        CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath(db), year, kRowLength);
        EXPECT_TRUE(catidx.Rebuilding());
        EXPECT_EQ(catidx.FrontierForTests(), f) << "no write failed: both files stop at the same watermark";
        EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 1u);
        const uint64_t rpb = RowsPerBlock(kRowLength);
        for (uint64_t b = f / rpb; b < (kRows + rpb - 1) / rpb; ++b) {
            EXPECT_TRUE(catidx.MightContain(b, 0, EncodeFDataForCatIdx(FString("absent"))))
                << "block " << b << " at or above F must not be trusted";
        }
        ExpectMaxIsExact(store);
    }
    ExpectNextStartRebuildsToTheReference(store);
}

// The progress display on a failing rebuild: the reporter is joined (no line after the engine returned), the engine
// returns exactly as without it (no exception, the same honest frontier), and the last line says it stopped early.
TEST_F(XStoreParallelRebuildFaultTest, ReadFailureWithProgressStillJoinsTheReporterAndStopsAtTheSameWatermark) {
    XStore store;
    Seed(store);
    if (HasFatalFailure()) return;
    for (unsigned threads : {1u, 4u}) {
        SCOPED_TRACE(threads);
        ResetIndexes();
        ProgressCapture capture;
        {
            xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, year, ".XSTORE"), 12, 1000, EIO);
            EXPECT_FALSE(RunEngine(store, threads, capture.Options()));
            EXPECT_GT(fail.Failures(), 0);
        }
        const size_t at_return = capture.Lines().size();
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        EXPECT_EQ(capture.Lines().size(), at_return) << "the reporter outlived the engine";
        ASSERT_GE(at_return, 1u);
        EXPECT_NE(capture.Lines().back().find("stopped before the end"), std::string::npos) << capture.Lines().back();
        const uint64_t f = ExpectZoneMapStoppedAtAnHonestFrontier();
        if (threads == 1) EXPECT_EQ(f, 3 * ChunkRows());
    }
    ExpectNextStartRebuildsToTheReference(store);
}

// A zone-map block write fails (the 6th block written) with 1, 4 and 16 threads: only the zone map stops (dirty, honest
// frontier); the catidx completes, is clean and equals the reference; the next start rebuilds the zone map.
TEST_F(XStoreParallelRebuildFaultTest, ZoneMapWriteFailureStopsOnlyTheZoneMap) {
    XStore store;
    Seed(store);
    if (HasFatalFailure()) return;
    for (unsigned threads : {1u, 4u, 16u}) {
        SCOPED_TRACE(threads);
        ResetIndexes();
        {
            // pwrites on the new .zmap: Open's header, the dirty mark, BeginRebuild's header, then one per block.
            xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, year, ".zmap"), 8, 1, EIO);
            EXPECT_FALSE(RunEngine(store, threads));
            EXPECT_EQ(fail.Failures(), 1);
        }
        const uint64_t f = ExpectZoneMapStoppedAtAnHonestFrontier();
        if (threads == 1) {
            EXPECT_EQ(f, 2 * ChunkRows()) << "block 5 is in chunk 2";
        }
        EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 0u) << "the catidx is unaffected";
        EXPECT_TRUE(ReadFileBytes(YearPath(db, year, ".catidx")) == ReadFileBytes(RefPath(year, ".catidx")));
        ExpectMaxIsExact(store);
    }
    ExpectNextStartRebuildsToTheReference(store);
}

} // namespace
