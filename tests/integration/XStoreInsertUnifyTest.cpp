// BatchInsert and RangeInsert share one implementation (XStore::MultiRowInsert): the same rows sent through either RPC
// must give the same status text (modulo the "[BATCH INSERT]"/"[RANGE INSERT]" tag) and byte-identical DB files
// (<year>.XSTORE, .zmap, .catidx; the .CFG compared with its dbName cleared). Duplicate timestamps inside one request:
// the LAST occurrence wins on both RPCs.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_WalRing.h"
#include <spdlog/fmt/fmt.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
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
                               XStore_Proto::WalDurabilityMode mode = XStore_Proto::SYNC, short row_length = kRowLength) {
        xstore_test::CreateTestDb(store, kBatchDb, unit, row_length);
        xstore_test::CreateTestDb(store, kRangeDb, unit, row_length);
        const std::string batch_status = Batch(store, kBatchDb, rows, mode);
        const std::string range_status = Range(store, kRangeDb, rows, mode);
        EXPECT_EQ(batch_status, fmt::format("[INFO] [BATCH INSERT] [DB: {}] - Inserted {} elements successfully", kBatchDb, rows.size()));
        EXPECT_EQ(Replace(Replace(batch_status, "[BATCH INSERT]", "[RANGE INSERT]"), "[DB: " + kBatchDb + "]", "[DB: " + kRangeDb + "]"),
                  range_status);

        ExpectSameDirectories(store);
        return batch_status;
    }

    // Every file of the two DB directories is byte-identical (the .CFG compared with its dbName cleared).
    static void ExpectSameDirectories(XStore &store) {
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
    }

    static XStore_Proto::BatchInsert_REQ BatchReq(const std::string &db, const std::vector<Row> &rows, XStore_Proto::WalDurabilityMode mode) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(mode);
        for (const Row &row : rows) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(row.ts);
            *entry->mutable_data() = row.data;
        }
        return req;
    }
    static XStore_Proto::RangeInsert_REQ RangeReq(const std::string &db, const std::vector<Row> &rows, XStore_Proto::WalDurabilityMode mode) {
        XStore_Proto::RangeInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(mode);
        for (const Row &row : rows) {
            XStore_Proto::RangeInsert_DATA *entry = req.add_rangeinsert_data();
            entry->set_timestamp(row.ts);
            *entry->mutable_data() = row.data;
        }
        return req;
    }
    // The same rows through BatchInsertFromWire / RangeInsertFromWire: the request is serialized into the Request envelope
    // exactly as a client sends it. Returns whether the wire path handled it; `status` is its reply text.
    static bool BatchViaWire(XStore &store, const std::string &db, const std::vector<Row> &rows, XStore_Proto::WalDurabilityMode mode, std::string &status) {
        XStore_Proto::Request envelope;
        *envelope.mutable_batchinsert() = BatchReq(db, rows, mode);
        const std::string bytes = envelope.SerializeAsString();
        XStore_Proto::BatchInsert_REP rep;
        const bool handled = store.BatchInsertFromWire(bytes.data(), bytes.size(), &rep);
        status = rep.insertstatus();
        return handled;
    }
    static bool RangeViaWire(XStore &store, const std::string &db, const std::vector<Row> &rows, XStore_Proto::WalDurabilityMode mode, std::string &status) {
        XStore_Proto::Request envelope;
        *envelope.mutable_rangeinsert() = RangeReq(db, rows, mode);
        const std::string bytes = envelope.SerializeAsString();
        XStore_Proto::RangeInsert_REP rep;
        const bool handled = store.RangeInsertFromWire(bytes.data(), bytes.size(), &rep);
        status = rep.insertstatus();
        return handled;
    }

    // The parsed path on kBatchDb, the wire path on kRangeDb (same cfg, rows and mode; BatchInsert on both, or RangeInsert on
    // both): the wire path must handle the request, give the same reply (modulo the DB name) and leave byte-identical files.
    std::string RunWireEquivalence(XStore &store, XStore_Proto::TimeUnit unit, const std::vector<Row> &rows,
                                   XStore_Proto::WalDurabilityMode mode = XStore_Proto::SYNC, short row_length = kRowLength, bool range_rpc = false) {
        xstore_test::CreateTestDb(store, kBatchDb, unit, row_length);
        xstore_test::CreateTestDb(store, kRangeDb, unit, row_length);
        const std::string parsed_status = range_rpc ? Range(store, kBatchDb, rows, mode) : Batch(store, kBatchDb, rows, mode);
        std::string wire_status;
        const bool handled = range_rpc ? RangeViaWire(store, kRangeDb, rows, mode, wire_status) : BatchViaWire(store, kRangeDb, rows, mode, wire_status);
        EXPECT_TRUE(handled) << "the wire path declined a plain well-formed request";
        EXPECT_EQ(Replace(parsed_status, "[DB: " + kBatchDb + "]", "[DB: " + kRangeDb + "]"), wire_status);
        ExpectSameDirectories(store);
        return wire_status;
    }

    // ASYNC and BYPASS do not wait for the apply the way SYNC does: the rows are compared through reads, after the writers drained.
    void RunWireModeCase(XStore_Proto::WalDurabilityMode mode) {
        XStore store;
        std::vector<uint64_t> ts;
        for (uint64_t i = 0; i < 100; ++i) ts.push_back(kBoundarySeconds + 5000 + i);
        const std::vector<Row> rows = RowsFor(ts);
        xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
        xstore_test::CreateTestDb(store, kRangeDb, XStore_Proto::SECOND, kRowLength);
        EXPECT_FALSE(Batch(store, kBatchDb, rows, mode).empty());
        std::string status;
        EXPECT_TRUE(BatchViaWire(store, kRangeDb, rows, mode, status));
        EXPECT_NE(status.find("Inserted 100 elements successfully"), std::string::npos) << status;
        if (mode == XStore_Proto::ASYNC) {   // the sentinel is a timestamp no row uses
            xstore_test::WaitForWalDrained(store, kBatchDb, kBoundarySeconds + 9000);
            xstore_test::WaitForWalDrained(store, kRangeDb, kBoundarySeconds + 9000);
        }
        ExpectLastOccurrenceWins(store, rows);
    }

    // A request the wire path must decline: nothing handled, no reply text, and nothing written (no year file, no row).
    static void ExpectDeclinedAndUntouched(XStore &store, const std::string &db, const std::string &request_bytes) {
        XStore_Proto::BatchInsert_REP rep;
        EXPECT_FALSE(store.BatchInsertFromWire(request_bytes.data(), request_bytes.size(), &rep));
        EXPECT_TRUE(rep.insertstatus().empty()) << rep.insertstatus();
        for (const std::string &name : FileNames(store.data_store_dir + "/" + db)) {
            EXPECT_TRUE(name.size() < 7 || name.compare(name.size() - 7, 7, ".XSTORE") != 0) << "a declined request created " << name;
        }
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

// ---------------------------------------------------------------------------------------------------------------------
// BatchInsertFromWire / RangeInsertFromWire: the same request, handled from the received bytes, must give the same reply and
// byte-identical files as the parsed path (data, .zmap, .catidx, .CFG), and anything that is not a plain well-formed insert
// must be declined with nothing done (the caller then takes the parsed path).
// ---------------------------------------------------------------------------------------------------------------------
namespace {
std::string Varint(uint64_t v) {
    std::string out;
    for (; v >= 128; v >>= 7) out.push_back(static_cast<char>(v | 128));
    out.push_back(static_cast<char>(v));
    return out;
}
std::string LenField(uint32_t number, const std::string &payload) { return Varint((number << 3) | 2) + Varint(payload.size()) + payload; }
std::string VarintField(uint32_t number, uint64_t v) { return Varint(number << 3) + Varint(v); }
// One BatchInsert_DATA (field 3 of BatchInsert_REQ): timestamp (1) + the raw RowData bytes (2).
std::string RawData(uint64_t ts, const std::string &row_bytes) { return LenField(3, VarintField(1, ts) + LenField(2, row_bytes)); }
// A whole Request holding a BatchInsert_REQ (field 8) built from raw pieces.
std::string RawBatchRequest(const std::string &body) { return LenField(8, body); }
std::string RawDoubleEntry() { return std::string("\x0A\x09\x09", 3) + std::string(8, '\x11'); }   // RowData.fielddata{doubleData}: tag, len 9, tag, 8 bytes
}  // namespace

TEST_F(InsertUnifyTest, WireSortedRequestIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 300; ++i) ts.push_back(kBoundarySeconds + 1000 + i);
    const std::vector<Row> rows = RowsFor(ts);
    RunWireEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, WireFullyReversedRequestIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 300; i-- > 0;) ts.push_back(kBoundarySeconds + 1000 + i);
    const std::vector<Row> rows = RowsFor(ts);
    RunWireEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, WireDuplicateTimestampsKeepTheLastOccurrence) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 200; ++i) ts.push_back(kBoundarySeconds + 1000 + (i * 7) % 40);   // 40 timestamps, each ~5 times, unsorted
    const std::vector<Row> rows = RowsFor(ts);
    RunWireEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, WireFiveYearsInterleavedIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 200; ++i) ts.push_back(kBoundarySeconds - 2 * kSecondsPerYear + (i % 5) * kSecondsPerYear + 10 * 86400 + i);
    const std::vector<Row> rows = RowsFor(ts);
    RunWireEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows);
}

TEST_F(InsertUnifyTest, WireMillisecondDbIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 150; ++i) ts.push_back((kBoundarySeconds - kSecondsPerYear + 86400) * 1000 + (i % 3) * kSecondsPerYear * 1000 + i);
    const std::vector<Row> rows = RowsFor(ts);
    RunWireEquivalence(store, XStore_Proto::MILLISECOND, rows);
}

TEST_F(InsertUnifyTest, WireOversizedRowsAreRejectedLikeTheParsedPath) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 60; ++i) ts.push_back(kBoundarySeconds + 1000 + i);
    std::vector<Row> rows = RowsFor(ts);
    std::set<uint64_t> rejected;
    for (size_t i : {3u, 17u, 59u}) {   // a row that does not fit its 48-byte slot is dropped, never truncated
        rows[i].data = MakeRow({FDouble(1.0), FString(std::string(100, 'x'))});
        rejected.insert(rows[i].ts);
    }
    RunWireEquivalence(store, XStore_Proto::SECOND, rows);
    ExpectLastOccurrenceWins(store, rows, rejected);
}

TEST_F(InsertUnifyTest, WireEveryColumnTypeIsIdentical) {
    XStore store;
    std::vector<Row> rows;
    for (uint64_t i = 0; i < 80; ++i) {
        rows.push_back({kBoundarySeconds + 2000 + i,
                        MakeRow({xstore_test::FDouble(i * 0.25), xstore_test::FFloat(static_cast<float>(i)), xstore_test::FInt32(-static_cast<int32_t>(i)),
                                 xstore_test::FInt64(static_cast<int64_t>(i) << 33), xstore_test::FFixed32(static_cast<uint32_t>(i)),
                                 xstore_test::FFixed64(i << 40), xstore_test::FBool(i % 2 == 0), xstore_test::FString("é" + std::to_string(i)),
                                 xstore_test::FBytes(std::string(1, static_cast<char>(i)) + "\xff")})});
    }
    RunWireEquivalence(store, XStore_Proto::SECOND, rows, XStore_Proto::SYNC, /*row_length=*/200);
}

TEST_F(InsertUnifyTest, WireWideRowsGrowTheIndexLikeTheParsedPath) {
    XStore store;
    std::vector<Row> rows;
    for (uint64_t i = 0; i < 40; ++i) {
        std::vector<XStore_Proto::FData> fields;
        for (int c = 0; c < 45 + static_cast<int>(i % 5); ++c) fields.push_back(FDouble(static_cast<double>(i) * 0.5 + c));   // 45..49 columns
        rows.push_back({kBoundarySeconds + 3000 + i, MakeRow(fields)});
    }
    RunWireEquivalence(store, XStore_Proto::SECOND, rows, XStore_Proto::SYNC, /*row_length=*/700);
}

TEST_F(InsertUnifyTest, WireEmptyRowsAndAnEmptyRequestAreIdentical) {
    XStore store;
    std::vector<Row> rows;
    for (uint64_t i = 0; i < 10; ++i) rows.push_back({kBoundarySeconds + 4000 + i, i % 2 == 0 ? MakeRow({}) : MakeRow({FDouble(1.0)})});
    RunWireEquivalence(store, XStore_Proto::SECOND, rows);
}

TEST_F(InsertUnifyTest, WireAnEmptyRequestIsHandledLikeTheParsedPath) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    std::string status;
    EXPECT_TRUE(BatchViaWire(store, kBatchDb, {}, XStore_Proto::SYNC, status));
    EXPECT_EQ(status, Batch(store, kBatchDb, {}, XStore_Proto::SYNC));
}

TEST_F(InsertUnifyTest, WireAMissingDbGivesTheSameReplyAsTheParsedPath) {
    XStore store;
    const std::vector<Row> rows = RowsFor({kBoundarySeconds + 1, kBoundarySeconds + 2});
    std::string wire_status;
    EXPECT_TRUE(BatchViaWire(store, "NOSUCHDB", rows, XStore_Proto::SYNC, wire_status));
    EXPECT_EQ(wire_status, Batch(store, "NOSUCHDB", rows, XStore_Proto::SYNC));
    EXPECT_NE(wire_status.find("DB does not exist"), std::string::npos) << wire_status;
}

TEST_F(InsertUnifyTest, WireRangeInsertIsIdentical) {
    XStore store;
    std::vector<uint64_t> ts;
    for (uint64_t i = 0; i < 200; ++i) ts.push_back(kBoundarySeconds - kSecondsPerYear + (i % 3) * kSecondsPerYear + 10 * 86400 + (199 - i));
    const std::vector<Row> rows = RowsFor(ts);
    RunWireEquivalence(store, XStore_Proto::SECOND, rows, XStore_Proto::SYNC, kRowLength, /*range_rpc=*/true);
}

TEST_F(InsertUnifyTest, WireAsyncModeGivesTheSameRowsAsTheParsedPath) { RunWireModeCase(XStore_Proto::ASYNC); }
TEST_F(InsertUnifyTest, WireBypassModeGivesTheSameRowsAsTheParsedPath) { RunWireModeCase(XStore_Proto::BYPASS); }

// --- requests the wire path must decline, with nothing done ---------------------------------------------------------

TEST_F(InsertUnifyTest, WireDeclinesAnExtraFieldAfterTheEnvelope) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, envelope.SerializeAsString() + VarintField(100, 1));
}

TEST_F(InsertUnifyTest, WireDeclinesTwoEnvelopesInOneMessage) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    const std::string one = envelope.SerializeAsString();
    ExpectDeclinedAndUntouched(store, kBatchDb, one + one);   // protobuf would merge the two; the scan does not guess
}

TEST_F(InsertUnifyTest, WireDeclinesAnUnknownFieldInsideTheRequest) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::BatchInsert_REQ req = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    req.GetReflection()->MutableUnknownFields(&req)->AddVarint(100, 1);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = req;
    ExpectDeclinedAndUntouched(store, kBatchDb, envelope.SerializeAsString());
}

TEST_F(InsertUnifyTest, WireDeclinesARepeatedDbNameField) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    const std::string body = LenField(2, kBatchDb) + LenField(2, kBatchDb) + RawData(kBoundarySeconds + 1, "") + VarintField(4, XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, RawBatchRequest(body));
}

TEST_F(InsertUnifyTest, WireDeclinesANonAsciiDbName) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    const std::string body = LenField(2, "\xC3\x89QBATCH") + RawData(kBoundarySeconds + 1, "") + VarintField(4, XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, RawBatchRequest(body));
}

TEST_F(InsertUnifyTest, WireDeclinesATruncatedRequest) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1, kBoundarySeconds + 2, kBoundarySeconds + 3}), XStore_Proto::SYNC);
    const std::string bytes = envelope.SerializeAsString();
    ExpectDeclinedAndUntouched(store, kBatchDb, bytes.substr(0, bytes.size() / 2));
    ExpectDeclinedAndUntouched(store, kBatchDb, bytes.substr(0, bytes.size() - 1));
}

TEST_F(InsertUnifyTest, WireDeclinesARowWithAnUnknownField) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    const std::string body = LenField(2, kBatchDb) + RawData(kBoundarySeconds + 1, RawDoubleEntry() + VarintField(2, 7)) + VarintField(4, XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, RawBatchRequest(body));
}

TEST_F(InsertUnifyTest, WireDeclinesARowInNonCanonicalEncoding) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    // The FData length written with a redundant second byte (0x89 0x00 = 9): protobuf accepts it, but re-serializing would give 1 byte
    // less than the slice, so the stored bytes would not be the received ones.
    const std::string non_canonical = std::string("\x0A\x89\x00\x09", 4) + std::string(8, '\x11');
    const std::string body = LenField(2, kBatchDb) + RawData(kBoundarySeconds + 1, non_canonical) + VarintField(4, XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, RawBatchRequest(body));
}

// --- the Request envelope's timeoutMs field (50): a trailing field after the one insert submessage is otherwise
// declined (WireDeclinesAnExtraFieldAfterTheEnvelope above, which deliberately uses field 100, not 50), but this
// one specific field is read, not declined -- see docs/superpowers/specs/2026-10-28-memory-guard-handoff.md item 4.

TEST_F(InsertUnifyTest, WireAcceptsATrailingTimeoutMsField) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    const std::string bytes = envelope.SerializeAsString() + VarintField(50, 5000);
    XStore_Proto::BatchInsert_REP rep;
    EXPECT_TRUE(store.BatchInsertFromWire(bytes.data(), bytes.size(), &rep));
    EXPECT_NE(rep.insertstatus().find("Inserted 1 elements successfully"), std::string::npos) << rep.insertstatus();
}

TEST_F(InsertUnifyTest, WireDeclinesATimeoutMsFieldWithTheWrongWireType) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, envelope.SerializeAsString() + LenField(50, "x"));
}

TEST_F(InsertUnifyTest, WireDeclinesAnOversizedTimeoutMsValue) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, envelope.SerializeAsString() + VarintField(50, 1ULL << 33));
}

TEST_F(InsertUnifyTest, WireDeclinesTrailingBytesAfterATimeoutMsField) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    ExpectDeclinedAndUntouched(store, kBatchDb, envelope.SerializeAsString() + VarintField(50, 10) + VarintField(50, 20));
}

// A request whose client had already given up by the time the server picked it up (an explicit `pickup` far in the
// past, paired with a real timeoutMs): handled (a reply is still produced) but ABANDONED -- nothing enqueued, no
// year file, same effect as a declined request.
TEST_F(InsertUnifyTest, WireAbandonsARequestWhoseDeadlineHasAlreadyPassed) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::Request envelope;
    *envelope.mutable_batchinsert() = BatchReq(kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC);
    const std::string bytes = envelope.SerializeAsString() + VarintField(50, 10);   // 10 ms of patience
    XStore_Proto::BatchInsert_REP rep;
    const auto long_ago = xstore::MemoryPool::Clock::now() - std::chrono::hours(1);
    EXPECT_TRUE(store.BatchInsertFromWire(bytes.data(), bytes.size(), &rep, long_ago));
    EXPECT_NE(rep.insertstatus().find("abandoned"), std::string::npos) << rep.insertstatus();
    for (const std::string &name : FileNames(store.data_store_dir + "/" + kBatchDb)) {
        EXPECT_TRUE(name.size() < 7 || name.compare(name.size() - 7, 7, ".XSTORE") != 0) << "an abandoned request created " << name;
    }
}

// The handler-scope memory lease is released (SYNC completes synchronously, so this covers MarkResident too) by the
// time the call returns: an ordinary insert leaves the process-wide pool exactly as it found it.
TEST_F(InsertUnifyTest, OrdinaryInsertLeavesTheMemoryPoolFullyReleased) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    xstore::MemoryPool &pool = xstore::MemoryPool::Instance();
    const uint64_t used_before = pool.UsedBytes(), pending_before = pool.PendingBytes();
    EXPECT_FALSE(Batch(store, kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::SYNC).empty());
    EXPECT_EQ(pool.UsedBytes(), used_before);
    EXPECT_EQ(pool.PendingBytes(), pending_before);
}

// An ASYNC insert is acknowledged before its record is written, so its memory reservation must follow the record into the WAL
// writer: still counted while the writer holds the record (parked after taking it), fully released once the batch is done.
TEST_F(InsertUnifyTest, AsyncInsertKeepsItsMemoryLeaseUntilTheWriterIsDone) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    xstore::MemoryPool &pool = xstore::MemoryPool::Instance();
    const uint64_t used_before = pool.UsedBytes();
    walring::PauseWriterForTests(1);
    EXPECT_FALSE(Batch(store, kBatchDb, RowsFor({kBoundarySeconds + 1}), XStore_Proto::ASYNC).empty());
    for (int i = 0; i < 5000 && !walring::WriterParkedForTests(); i++) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    ASSERT_TRUE(walring::WriterParkedForTests());
    EXPECT_GT(pool.UsedBytes(), used_before) << "the reservation was returned while the writer still held the record";
    walring::PauseWriterForTests(0);
    for (int i = 0; i < 5000 && pool.UsedBytes() != used_before; i++) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    EXPECT_EQ(pool.UsedBytes(), used_before);
    EXPECT_EQ(pool.PendingBytes(), 0u);
}

// Nothing is enqueued until every row has been read: a row that fails late (here an invalid UTF-8 string in the LAST year group) must not
// leave the earlier year groups applied. The parsed path drops such a request when parsing it, before any effect.
TEST_F(InsertUnifyTest, WireAMalformedLateRowLeavesNothingApplied) {
    XStore store;
    xstore_test::CreateTestDb(store, kBatchDb, XStore_Proto::SECOND, kRowLength);
    const std::string good_row = RawDoubleEntry() + RawDoubleEntry() + RawDoubleEntry() + RawDoubleEntry();   // wider than the DB's columns: growing the width for it would be visible
    const std::string cfg_path = store.data_store_dir + "/" + kBatchDb + "/" + kBatchDb + ".CFG";
    const uint32_t columns_before = ReadCfgWithoutName(cfg_path).columncount();
    const std::string bad_row = LenField(1, std::string("\x72\x01\xFF", 3));   // FData.stringData = 0xFF: not valid UTF-8
    const std::string body = LenField(2, kBatchDb) + RawData(kBoundarySeconds - 3 * kSecondsPerYear, good_row) + RawData(kBoundarySeconds - 2 * kSecondsPerYear, good_row) +
                             RawData(kBoundarySeconds + 10, bad_row) + VarintField(4, XStore_Proto::SYNC);
    const std::string bytes = RawBatchRequest(body);
    XStore_Proto::Request parsed;
    if (!parsed.ParseFromString(bytes)) {
        ExpectDeclinedAndUntouched(store, kBatchDb, bytes);   // the parsed path fails the whole request; so must the wire path, before any effect
        EXPECT_EQ(ReadCfgWithoutName(cfg_path).columncount(), columns_before) << "a declined request grew the index width";
    } else {
        GTEST_SKIP() << "this protobuf build accepts the invalid string, nothing to compare with";
    }
}
