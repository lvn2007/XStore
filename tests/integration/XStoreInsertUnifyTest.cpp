// BatchInsert and RangeInsert share one implementation (XStore::MultiRowInsert): the same rows sent through either RPC
// must give the same status text (modulo the "[BATCH INSERT]"/"[RANGE INSERT]" tag) and byte-identical DB files
// (<year>.XSTORE, .zmap, .catidx; the .CFG compared with its dbName cleared). Duplicate timestamps inside one request:
// the LAST occurrence wins on both RPCs.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_WalRing.h"
#include <spdlog/fmt/fmt.h>
#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

namespace fs = std::filesystem;
using xstore_test::FDouble;
using xstore_test::FString;
using xstore_test::MakeRow;

constexpr short kRowLength = 48;
constexpr uint64_t kBoundarySeconds = 1704067200;   // 2024-01-01T00:00:00Z
constexpr uint64_t kSecondsPerYear = 31'536'000;    // spacing for the multi-year case (365 days)
const std::string kBatchDb = "EQBATCH";
const std::string kRangeDb = "EQRANGE";

struct Row {
    uint64_t ts;
    XStore_Proto::RowData data;
};

// Two columns (numeric + string) so both the .zmap and the .catidx get real content; value = request index, so a
// duplicate that lost reads back a visibly wrong value.
Row MakeIndexedRow(uint64_t ts, size_t request_index) {
    return {ts, MakeRow({FDouble(static_cast<double>(request_index)), FString("v" + std::to_string(request_index))})};
}

std::vector<Row> RowsFor(const std::vector<uint64_t> &timestamps) {
    std::vector<Row> rows;
    for (size_t i = 0; i < timestamps.size(); ++i) rows.push_back(MakeIndexedRow(timestamps[i], i));
    return rows;
}

std::string Replace(std::string s, const std::string &from, const std::string &to) {
    for (size_t pos = s.find(from); pos != std::string::npos; pos = s.find(from, pos + to.size())) s.replace(pos, from.size(), to);
    return s;
}

// [start, end) data extents of a (sparse: data files are ftruncated to a whole year) file; a filesystem without
// SEEK_DATA support reports the whole file as data.
std::vector<std::pair<off_t, off_t>> DataExtents(int fd, off_t size) {
    std::vector<std::pair<off_t, off_t>> extents;
    off_t pos = 0;
    while (pos < size) {
        const off_t start = lseek(fd, pos, SEEK_DATA);
        if (start < 0) {
            if (errno == EINVAL && extents.empty()) extents.emplace_back(0, size);
            break;   // ENXIO: no data past pos
        }
        off_t end = lseek(fd, start, SEEK_HOLE);
        if (end < 0) end = size;
        extents.emplace_back(start, end);
        pos = end;
    }
    return extents;
}

// Byte-compares two files without reading their holes: every data extent of either file is compared against the
// same range of the other (a hole reads as zeros, so a hole vs written zeros is equal, as it is to a reader).
void ExpectSameBytes(const std::string &path_a, const std::string &path_b) {
    const int fd_a = open(path_a.c_str(), O_RDONLY);
    const int fd_b = open(path_b.c_str(), O_RDONLY);
    ASSERT_GE(fd_a, 0) << path_a;
    ASSERT_GE(fd_b, 0) << path_b;
    struct stat st_a{}, st_b{};
    ASSERT_EQ(fstat(fd_a, &st_a), 0);
    ASSERT_EQ(fstat(fd_b, &st_b), 0);
    EXPECT_EQ(st_a.st_size, st_b.st_size) << path_a << " vs " << path_b;
    if (st_a.st_size == st_b.st_size) {
        std::vector<std::pair<off_t, off_t>> extents = DataExtents(fd_a, st_a.st_size);
        const std::vector<std::pair<off_t, off_t>> extents_b = DataExtents(fd_b, st_b.st_size);
        if (st_a.st_blocks > 0) EXPECT_FALSE(extents.empty()) << path_a << ": allocated blocks but no data extent -- the comparison would be vacuous";
        extents.insert(extents.end(), extents_b.begin(), extents_b.end());
        constexpr size_t kChunk = 1 << 20;
        std::string buf_a(kChunk, '\0'), buf_b(kChunk, '\0');
        bool same = true;
        for (const auto &[start, end] : extents) {
            for (off_t off = start; same && off < end; off += static_cast<off_t>(kChunk)) {
                const size_t len = static_cast<size_t>(std::min<off_t>(static_cast<off_t>(kChunk), end - off));
                ASSERT_EQ(pread(fd_a, buf_a.data(), len, off), static_cast<ssize_t>(len)) << path_a;
                ASSERT_EQ(pread(fd_b, buf_b.data(), len, off), static_cast<ssize_t>(len)) << path_b;
                if (buf_a.compare(0, len, buf_b, 0, len) != 0) {
                    same = false;
                    ADD_FAILURE() << path_a << " and " << path_b << " differ in [" << off << ", " << off + static_cast<off_t>(len) << ")";
                }
            }
        }
    }
    close(fd_a);
    close(fd_b);
}

std::set<std::string> FileNames(const std::string &dir) {
    std::set<std::string> names;
    std::error_code ec;
    for (const auto &entry : fs::directory_iterator(dir, ec)) names.insert(entry.path().filename().string());
    return names;
}

XStore_Proto::ConfigStruct ReadCfgWithoutName(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    XStore_Proto::ConfigStruct cfg;
    EXPECT_TRUE(cfg.ParseFromIstream(&in)) << path;
    cfg.clear_dbname();
    return cfg;
}

}  // namespace

class InsertUnifyTest : public ::testing::Test {
protected:
    // No background checkpoint between the two requests, so nothing but the inserts themselves touches the files.
    xstore_test::ScopedCheckpointerPause checkpointer_paused_;
    std::unique_ptr<xstore_test::ScratchDir> scratch;

    void ClearRegistries() {
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        walring::ClearDirtyForTests();
    }
    void SetUp() override {
        scratch = std::make_unique<xstore_test::ScratchDir>("xstore_insert_unify_test");
        ClearRegistries();
    }
    void TearDown() override {
        ClearRegistries();
        scratch.reset();
    }

    static std::string Batch(XStore &store, const std::string &db, const std::vector<Row> &rows, XStore_Proto::WalDurabilityMode mode) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(mode);
        for (const Row &row : rows) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(row.ts);
            *entry->mutable_data() = row.data;
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        return rep.insertstatus();
    }
    static std::string Range(XStore &store, const std::string &db, const std::vector<Row> &rows, XStore_Proto::WalDurabilityMode mode) {
        XStore_Proto::RangeInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(mode);
        for (const Row &row : rows) {
            XStore_Proto::RangeInsert_DATA *entry = req.add_rangeinsert_data();
            entry->set_timestamp(row.ts);
            *entry->mutable_data() = row.data;
        }
        XStore_Proto::RangeInsert_REP rep;
        store.RangeInsert(&req, &rep);
        return rep.insertstatus();
    }

    // BatchInsert on one fresh DB, RangeInsert on another (same cfg, same rows, same mode): the statuses differ only
    // by the tag (and the DB name), and every file of the two DB directories is byte-identical. Returns the batch status.
    std::string RunEquivalence(XStore &store, XStore_Proto::TimeUnit unit, const std::vector<Row> &rows,
                               XStore_Proto::WalDurabilityMode mode = XStore_Proto::SYNC) {
        xstore_test::CreateTestDb(store, kBatchDb, unit, kRowLength);
        xstore_test::CreateTestDb(store, kRangeDb, unit, kRowLength);
        const std::string batch_status = Batch(store, kBatchDb, rows, mode);
        const std::string range_status = Range(store, kRangeDb, rows, mode);
        EXPECT_EQ(batch_status, fmt::format("[INFO] [BATCH INSERT] [DB: {}] - Inserted {} elements successfully", kBatchDb, rows.size()));
        EXPECT_EQ(Replace(Replace(batch_status, "[BATCH INSERT]", "[RANGE INSERT]"), "[DB: " + kBatchDb + "]", "[DB: " + kRangeDb + "]"),
                  range_status);

        const std::string dir_batch = store.data_store_dir + "/" + kBatchDb;
        const std::string dir_range = store.data_store_dir + "/" + kRangeDb;
        std::set<std::string> names_batch = FileNames(dir_batch), names_range = FileNames(dir_range);
        // The .CFG is named after its DB.
        EXPECT_EQ(names_batch.erase(kBatchDb + ".CFG"), 1u);
        EXPECT_EQ(names_range.erase(kRangeDb + ".CFG"), 1u);
        EXPECT_EQ(names_batch, names_range);
        EXPECT_EQ(ReadCfgWithoutName(dir_batch + "/" + kBatchDb + ".CFG").SerializeAsString(),
                  ReadCfgWithoutName(dir_range + "/" + kRangeDb + ".CFG").SerializeAsString());
        for (const std::string &name : names_batch) {
            if (names_range.count(name) != 0) ExpectSameBytes(dir_batch + "/" + name, dir_range + "/" + name);
        }
        return batch_status;
    }

    // On both DBs, every timestamp reads back the value of its LAST occurrence in `rows` (absent when that occurrence
    // was rejected as oversized).
    static void ExpectLastOccurrenceWins(XStore &store, const std::vector<Row> &rows, const std::set<uint64_t> &rejected = {}) {
        std::map<uint64_t, const Row *> last;
        for (const Row &row : rows) last[row.ts] = &row;
        for (const std::string &db : {kBatchDb, kRangeDb}) {
            for (const auto &[ts, row] : last) {
                XStore_Proto::UnaryQuery_REP rep;
                store.UnaryQuery(ts, db, &rep);
                if (rejected.count(ts) != 0) {
                    EXPECT_EQ(rep.data().fielddata_size(), 0) << db << " " << ts << ": the oversized row must be dropped";
                    continue;
                }
                ASSERT_TRUE(rep.has_data()) << db << " " << ts;
                EXPECT_EQ(rep.data().SerializeAsString(), row->data.SerializeAsString()) << db << " timestamp " << ts << " must hold its last occurrence";
            }
        }
    }
};

TEST_F(InsertUnifyTest, SortedRequestIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 300; ++i) ts.push_back(kBoundarySeconds + 1000 + i);
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, FullyReversedRequestIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 300; i-- > 0;) ts.push_back(kBoundarySeconds + 1000 + i);
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, SortedWithOneLateInversionIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 300; ++i) ts.push_back(kBoundarySeconds + 1000 + i);
    std::swap(ts[290], ts[291]);
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, FiveYearsInterleavedIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    // Years 2022..2026 round-robin (10 days into each, clear of any time-zone offset).
    for (uint64_t i = 0; i < 200; ++i) ts.push_back(kBoundarySeconds - 2 * kSecondsPerYear + (i % 5) * kSecondsPerYear + 10 * 86400 + i);
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
    const std::set<std::string> names = FileNames(store.data_store_dir + "/" + kBatchDb);
    EXPECT_EQ(std::count_if(names.begin(), names.end(),
                            [](const std::string &n) { return n.size() > 7 && n.compare(n.size() - 7, 7, ".XSTORE") == 0; }), 5)
        << "one data file per year";
}

// Last second of 2023 and first second of 2024, sorted and with the crossing inverted.
TEST_F(InsertUnifyTest, ExactYearBoundarySortedIsIdentical) {
    XStore store;
    const std::vector<Row> rows = RowsFor({kBoundarySeconds - 2, kBoundarySeconds - 1, kBoundarySeconds, kBoundarySeconds + 1});
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, ExactYearBoundaryInvertedIsIdentical) {
    XStore store;
    const std::vector<Row> rows = RowsFor({kBoundarySeconds, kBoundarySeconds - 1, kBoundarySeconds + 1, kBoundarySeconds - 2});
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

// 200 rows over 20 timestamps across the year boundary, shuffled: the stable sort on the cold path must keep each
// timestamp's request order (an unstable std::sort picks a winner at random past 16 rows). Same rows sorted (stably,
// so request order among duplicates is kept) take the never-sort path and must give the same result.
TEST_F(InsertUnifyTest, DuplicateTimestampsUnsortedLastOccurrenceWinsOnBoth) {
    XStore store;
    std::vector<uint64_t> ts;
    for (size_t i = 0; i < 200; ++i) ts.push_back(kBoundarySeconds - 10 + i % 20);
    std::mt19937 rng(12345);
    std::shuffle(ts.begin(), ts.end(), rng);
    ASSERT_FALSE(std::is_sorted(ts.begin(), ts.end()));
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, DuplicateTimestampsSortedLastOccurrenceWinsOnBoth) {
    XStore store;
    std::vector<uint64_t> ts;
    for (size_t i = 0; i < 200; ++i) ts.push_back(kBoundarySeconds - 10 + i % 20);
    std::mt19937 rng(12345);
    std::shuffle(ts.begin(), ts.end(), rng);
    std::stable_sort(ts.begin(), ts.end());
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

// An oversized row in the middle (unsorted request) is dropped, not truncated; every other row is intact.
TEST_F(InsertUnifyTest, OversizedRowInTheMiddleIsDroppedIdentically) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 100; ++i) ts.push_back(kBoundarySeconds + 5000 - i);
    std::vector<Row> rows = RowsFor(ts);
    rows[50].data = MakeRow({FDouble(50.0), FString(std::string(200, 'x'))});
    ASSERT_GT(rows[50].data.ByteSizeLong(), static_cast<size_t>(kRowLength));
    RunEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows, {rows[50].ts});
}

TEST_F(InsertUnifyTest, EmptyRequestIsIdentical) {
    XStore store;
    const std::string status = RunEquivalence(store, XStore_Proto::SECOND, {});
    EXPECT_EQ(status, "[INFO] [BATCH INSERT] [DB: " + kBatchDb + "] - Inserted 0 elements successfully");
}

TEST_F(InsertUnifyTest, MillisecondResolutionIsIdentical) {
    XStore store;
    const uint64_t boundary_ms = kBoundarySeconds * 1000;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 100; ++i) ts.push_back(boundary_ms - 50 + (i * 37) % 100);   // shuffled across the boundary
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::MILLISECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, BypassDurabilityIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 200; ++i) ts.push_back(kBoundarySeconds - 100 + (i * 7) % 200);
    const std::vector<Row> rows = RowsFor(ts);
    RunEquivalence(store, XStore_Proto::SECOND, rows, XStore_Proto::BYPASS);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, NonexistentDbStatusIsIdenticalModuloTag) {
    XStore store;
    const std::vector<Row> rows = RowsFor({kBoundarySeconds});
    EXPECT_EQ(Batch(store, "NOPE", rows, XStore_Proto::SYNC), "[WARNING] [BATCH INSERT] [DB: NOPE] - DB does not exist");
    EXPECT_EQ(Range(store, "NOPE", rows, XStore_Proto::SYNC), "[WARNING] [RANGE INSERT] [DB: NOPE] - DB does not exist");
    EXPECT_FALSE(fs::exists(store.data_store_dir + "/NOPE"));
}
