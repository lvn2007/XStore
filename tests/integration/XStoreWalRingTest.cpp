// WAL ring + paced checkpoint (Durability C), Task 1: ring file header
// slots, append planning, PAD records and the v2 (LSN-carrying) record
// parser. Pure format tests -- nothing here touches the running WAL path.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_Crc32c.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/DataStructs/TimeStruct.h"
#include <spdlog/fmt/fmt.h>
#include <chrono>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

// mkstemp file in the system temp dir, unlinked on destruction.
struct TempFile {
    int fd = -1;
    std::string path;
    TempFile() {
        path = (std::filesystem::temp_directory_path() / "xstore_walring_XXXXXX").string();
        fd = mkstemp(path.data());
    }
    ~TempFile() {
        if (fd >= 0) close(fd);
        if (!path.empty()) unlink(path.c_str());
    }
    TempFile(const TempFile &) = delete;
    TempFile &operator=(const TempFile &) = delete;
};

template <typename T>
void Put(std::string &s, T v) { s.append(reinterpret_cast<const char *>(&v), sizeof v); }

// Hand-assembled v2 record so the test (not the library) defines the layout:
// [u32 len][u32 crc][u64 lsn][u16 dbLen][u16 rowLen][i32 res][u32 rows][db][u64 epoch + row]*
std::string BuildRecordV2ForTest(uint64_t lsn, const std::string &db, uint16_t row_length,
                                 const std::vector<std::pair<uint64_t, std::string>> &rows) {
    std::string body;  // everything after the CRC field
    Put<uint64_t>(body, lsn);
    Put<uint16_t>(body, static_cast<uint16_t>(db.size()));
    Put<uint16_t>(body, row_length);
    Put<int32_t>(body, 0);
    Put<uint32_t>(body, static_cast<uint32_t>(rows.size()));
    body += db;
    for (const auto &r : rows) {
        Put<uint64_t>(body, r.first);
        body += r.second;
    }
    std::string rec;
    Put<uint32_t>(rec, static_cast<uint32_t>(4 + body.size()));  // counts crc + body
    Put<uint32_t>(rec, Crc32c(body.data(), body.size()));
    return rec + body;
}

TEST(WalRingFormatTest, HeaderSlotsPickHighestValidSequence) {
    TempFile f; walring::RingHeader h; h.ring_bytes = 1 << 20; h.shard_id = 3;
    h.slot_seq = 1; h.checkpoint_lsn = 100; ASSERT_TRUE(walring::WriteHeaderSlot(f.fd, h));
    h.slot_seq = 2; h.checkpoint_lsn = 200; ASSERT_TRUE(walring::WriteHeaderSlot(f.fd, h));
    walring::RingHeader got; ASSERT_TRUE(walring::ReadBestHeader(f.fd, got));
    EXPECT_EQ(got.checkpoint_lsn, 200u);
    // Tear slot 0 (seq 2 -> 2 % 2 == 0): flip a byte; reader must fall back to slot 1 (seq 1).
    char b; ASSERT_EQ(pread(f.fd, &b, 1, 8), 1); b ^= 0x5A; ASSERT_EQ(pwrite(f.fd, &b, 1, 8), 1);
    ASSERT_TRUE(walring::ReadBestHeader(f.fd, got));
    EXPECT_EQ(got.checkpoint_lsn, 100u);
}

TEST(WalRingFormatTest, NeitherSlotValidMeansLegacyOrNew) {
    TempFile f; char junk[4096]; memset(junk, 0x11, sizeof junk); ASSERT_EQ(pwrite(f.fd, junk, sizeof junk, 0), 4096);
    walring::RingHeader got; EXPECT_FALSE(walring::ReadBestHeader(f.fd, got));
}

TEST(WalRingFormatTest, PlanAppendNeverStraddlesAndPadsOrSkips) {
    size_t pad = 99;
    EXPECT_EQ(walring::PlanAppend(1000, 4096, 100, pad), 1000u); EXPECT_EQ(pad, 0u);          // fits
    EXPECT_EQ(walring::PlanAppend(4096 - 200, 4096, 300, pad), 4096u); EXPECT_EQ(pad, 200u);   // pad 200, record at next lap
    EXPECT_EQ(walring::PlanAppend(4096 - 10, 4096, 300, pad), 4096u); EXPECT_EQ(pad, 0u);      // < 28 left: skip silently
}

TEST(WalRingFormatTest, RecordV2RoundTripRejectsStaleLapAndBadCrc) {
    std::string rec = BuildRecordV2ForTest(/*lsn=*/5000, "DB1", /*row_length=*/8, {{10, "AAAAAAAA"}, {11, "BBBBBBBB"}});
    XStore::WalRecord out; size_t used = 0;
    EXPECT_EQ(XStore::ParseWalRecordV2(rec.data(), rec.size(), 5000, used, out), XStore::RingParse::kOk);
    EXPECT_EQ(used, rec.size()); EXPECT_EQ(out.rows.size(), 2u); EXPECT_EQ(out.db_name, "DB1");
    EXPECT_EQ(out.row_length, 8u);
    EXPECT_EQ(out.rows[1].epoch_time, 11u); EXPECT_EQ(out.rows[1].row_bytes, "BBBBBBBB");
    EXPECT_EQ(XStore::ParseWalRecordV2(rec.data(), rec.size(), 5000 + 4096, used, out), XStore::RingParse::kEnd);  // stale lap
    EXPECT_EQ(XStore::ParseWalRecordV2(rec.data(), rec.size() - 1, 5000, used, out), XStore::RingParse::kEnd);     // torn tail
    rec[rec.size() - 1] ^= 1;
    EXPECT_EQ(XStore::ParseWalRecordV2(rec.data(), rec.size(), 5000, used, out), XStore::RingParse::kEnd);         // CRC
}

TEST(WalRingFormatTest, PadRecordParsesAsPad) {
    std::string pad(200, '\1'); walring::BuildPad(pad.data(), pad.size(), 7000);
    XStore::WalRecord out; size_t used = 0;
    EXPECT_EQ(XStore::ParseWalRecordV2(pad.data(), pad.size(), 7000, used, out), XStore::RingParse::kPad);
    EXPECT_EQ(used, 200u);
}

TEST(WalRingFormatTest, InitRingFileZeroFillsAndWritesCleanHeader) {
    TempFile f; ASSERT_TRUE(walring::InitRingFile(f.fd, 2, 8 << 20));
    walring::RingHeader h; ASSERT_TRUE(walring::ReadBestHeader(f.fd, h));
    EXPECT_EQ(h.ring_bytes, 8u << 20); EXPECT_EQ(h.checkpoint_lsn, 0u); EXPECT_EQ(h.shard_id, 2u);
    struct stat st; ASSERT_EQ(fstat(f.fd, &st), 0); EXPECT_GE(static_cast<size_t>(st.st_blocks) * 512, (8u << 20));  // really allocated
}


// ---- Task 1 review gaps ---------------------------------------------------

TEST(WalRingFormatTest, PlanAppendBoundaryCases) {
    size_t pad = 99;
    // Exactly RECORD_FIXED_BYTES (28) left and the record needs more: 28-byte PAD, record next lap.
    EXPECT_EQ(walring::PlanAppend(4096 - 28, 4096, 29, pad), 4096u); EXPECT_EQ(pad, 28u);
    // 27 left: too small for a PAD header -> skipped silently.
    EXPECT_EQ(walring::PlanAppend(4096 - 27, 4096, 29, pad), 4096u); EXPECT_EQ(pad, 0u);
    // Record exactly fills the remainder: no wrap, no pad.
    EXPECT_EQ(walring::PlanAppend(4096 - 300, 4096, 300, pad), 4096u - 300); EXPECT_EQ(pad, 0u);
    // One byte more than the remainder: pad the whole remainder.
    EXPECT_EQ(walring::PlanAppend(4096 - 300, 4096, 301, pad), 4096u); EXPECT_EQ(pad, 300u);
    // Head exactly at a ring end (second lap boundary): a fresh lap, whole ring free.
    EXPECT_EQ(walring::PlanAppend(8192, 4096, 4096, pad), 8192u); EXPECT_EQ(pad, 0u);
    EXPECT_EQ(walring::PlanAppend(8192, 4096, 100, pad), 8192u); EXPECT_EQ(pad, 0u);
}

TEST(WalRingFormatTest, PadWithWrongLsnIsEnd) {
    std::string pad(200, '\1'); walring::BuildPad(pad.data(), pad.size(), 7000);
    XStore::WalRecord out; size_t used = 0;
    EXPECT_EQ(XStore::ParseWalRecordV2(pad.data(), pad.size(), 7000 + 4096, used, out), XStore::RingParse::kEnd);
}

// Same layout as BuildRecordV2ForTest but with independently chosen (possibly lying) length fields and a VALID CRC,
// so only the structural checks can reject it.
std::string BuildLyingRecordV2(uint64_t lsn, uint16_t db_name_len_field, uint16_t row_length_field,
                               uint32_t row_count_field, const std::string &tail) {
    std::string body;
    Put<uint64_t>(body, lsn);
    Put<uint16_t>(body, db_name_len_field);
    Put<uint16_t>(body, row_length_field);
    Put<int32_t>(body, 0);
    Put<uint32_t>(body, row_count_field);
    body += tail;
    std::string rec;
    Put<uint32_t>(rec, static_cast<uint32_t>(4 + body.size()));
    Put<uint32_t>(rec, Crc32c(body.data(), body.size()));
    return rec + body;
}

TEST(WalRingFormatTest, StructuralRejectsWithValidCrc) {
    XStore::WalRecord out; size_t used = 0;
    // rowCount says 3 but only 2 rows (8 + 8 bytes each) follow the 3-byte name.
    std::string tail = "DB1" + std::string(2 * 16, 'x');
    std::string rec = BuildLyingRecordV2(100, 3, 8, 3, tail);
    EXPECT_EQ(XStore::ParseWalRecordV2(rec.data(), rec.size(), 100, used, out), XStore::RingParse::kEnd);
    // Control: the same bytes with the true rowCount parse.
    rec = BuildLyingRecordV2(100, 3, 8, 2, tail);
    EXPECT_EQ(XStore::ParseWalRecordV2(rec.data(), rec.size(), 100, used, out), XStore::RingParse::kOk);
    // dbNameLen reaches past the payload.
    rec = BuildLyingRecordV2(100, 1000, 8, 0, "DB1");
    EXPECT_EQ(XStore::ParseWalRecordV2(rec.data(), rec.size(), 100, used, out), XStore::RingParse::kEnd);
}

TEST(WalRingFormatTest, RingBytesBelowOneRecordHeaderIsRejected) {
    TempFile f;
    EXPECT_FALSE(walring::InitRingFile(f.fd, 0, walring::RECORD_FIXED_BYTES - 1));
    walring::RingHeader h; h.ring_bytes = 0; h.slot_seq = 1;
    ASSERT_TRUE(walring::WriteHeaderSlot(f.fd, h));   // CRC-valid header with a zero ring size
    walring::RingHeader got;
    EXPECT_FALSE(walring::ReadBestHeader(f.fd, got)) << "ring_bytes == 0 would divide by zero in RingOffset/PlanAppend";
}

// ---- Task 2: writer appends to the ring -----------------------------------

// Replay-rule walk over a ring file: from `from` until `to`, skipping tails < 28 bytes,
// treating PAD as a skip, stopping early on anything unparseable.
struct RingWalk { uint64_t end_lsn = 0; size_t records = 0; size_t pads = 0; size_t rows = 0; };
RingWalk WalkRing(const walring::Ring &ring, uint64_t from, uint64_t to) {
    std::string bytes(ring.ring_bytes(), '\0');
    EXPECT_EQ(pread(ring.fd(), bytes.data(), bytes.size(), walring::HEADER_REGION_BYTES), static_cast<ssize_t>(bytes.size()));
    RingWalk w; w.end_lsn = from;
    while (w.end_lsn < to) {
        const uint64_t off = w.end_lsn % ring.ring_bytes();
        const uint64_t rem = ring.ring_bytes() - off;
        if (rem < walring::RECORD_FIXED_BYTES) { w.end_lsn += rem; continue; }
        XStore::WalRecord rec; size_t used = 0;
        const XStore::RingParse r = XStore::ParseWalRecordV2(bytes.data() + off, rem, w.end_lsn, used, rec);
        if (r == XStore::RingParse::kEnd) break;
        if (r == XStore::RingParse::kPad) ++w.pads; else { ++w.records; w.rows += rec.rows.size(); }
        w.end_lsn += used;
    }
    return w;
}

class WalRingWriterTest : public ::testing::Test {
protected:
    static constexpr uint64_t kBaseEpoch = 1700000000;
    const std::string db_name = "WALRINGW";
    std::unique_ptr<xstore_test::ScratchDir> scratch;

    void ClearRegistries() {
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
    }
    void SetUp() override {
        scratch = std::make_unique<xstore_test::ScratchDir>("xstore_walring_writer_test");
        ClearRegistries();
    }
    void TearDown() override {
        walring::SetRingBytesForTests(0);
        walring::SetStallTimeoutMsForTests(0);
        walring::FailNextWalSyncsForTests(0);
        ClearRegistries();
        scratch.reset();
    }

    bool InsertSync(XStore &store, uint64_t i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(static_cast<double>(i));
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(kBaseEpoch + i, row, db_name, &rep, XStore_Proto::SYNC);
        return rep.insertstatus().rfind("[ERROR]", 0) != 0;
    }

    // Rings whose file lives under this test's scratch dir.
    std::vector<std::shared_ptr<walring::Ring>> MyRings() const {
        std::vector<std::shared_ptr<walring::Ring>> mine;
        for (auto &r : walring::AllRings()) {
            if (r->path().rfind(scratch->test_root.string(), 0) == 0) mine.push_back(r);
        }
        return mine;
    }
    // The (single) ring that received this thread's records.
    std::shared_ptr<walring::Ring> BusyRing() const {
        std::shared_ptr<walring::Ring> busy;
        for (auto &r : MyRings()) {
            if (r->HeadLsn() > 0) { EXPECT_EQ(busy, nullptr) << "records landed on more than one ring"; busy = r; }
        }
        return busy;
    }
};

TEST_F(WalRingWriterTest, RecordsAppendAtAdvancingLsn) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertSync(store, i));

    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    EXPECT_GT(ring->HeadLsn(), 0u);
    EXPECT_EQ(ring->HeadLsn(), ring->AppliedLsn());
    EXPECT_EQ(ring->CheckpointLsn(), 0u) << "no checkpointer yet";
    EXPECT_EQ(ring->ring_bytes(), xstore_test::kTestDefaultRingBytes) << "XStoreTestSupport.h's suite-wide default must be installed";

    std::string bytes(ring->HeadLsn(), '\0');
    ASSERT_EQ(pread(ring->fd(), bytes.data(), bytes.size(), walring::RingOffset(0, ring->ring_bytes())),
              static_cast<ssize_t>(bytes.size()));
    XStore::WalRecord rec1, rec2; size_t used1 = 0, used2 = 0;
    ASSERT_EQ(XStore::ParseWalRecordV2(bytes.data(), bytes.size(), 0, used1, rec1), XStore::RingParse::kOk);
    EXPECT_EQ(rec1.db_name, db_name);
    ASSERT_EQ(rec1.rows.size(), 1u);
    EXPECT_EQ(rec1.rows[0].epoch_time, kBaseEpoch);
    ASSERT_EQ(XStore::ParseWalRecordV2(bytes.data() + used1, bytes.size() - used1, used1, used2, rec2), XStore::RingParse::kOk);
    EXPECT_EQ(rec2.rows[0].epoch_time, kBaseEpoch + 1);
    // All three records, contiguous, ending exactly at head.
    const RingWalk w = WalkRing(*ring, 0, ring->HeadLsn());
    EXPECT_EQ(w.end_lsn, ring->HeadLsn());
    EXPECT_EQ(w.records, 3u);
}

TEST_F(WalRingWriterTest, WrapPadsInsteadOfStraddling) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 4096);

    std::shared_ptr<walring::Ring> ring;
    std::vector<uint64_t> heads;   // head after each insert
    for (uint64_t i = 0; i < 160; ++i) {   // ~4.1 KiB each: 160 records > 2 laps of 256 KiB
        ASSERT_TRUE(InsertSync(store, i)) << "insert " << i;
        if (!ring) ring = BusyRing();
        ASSERT_NE(ring, nullptr);
        heads.push_back(ring->HeadLsn());
        // No checkpointer yet: free everything but the last few records by hand.
        if (heads.size() > 4) ring->AdvanceCheckpoint(heads[heads.size() - 5]);
    }
    ASSERT_EQ(ring->ring_bytes(), kRing);
    const uint64_t head = ring->HeadLsn();
    ASSERT_GT(head, 2 * kRing) << "must wrap at least twice";

    // Walk from the last record boundary before the final lap boundary, across it, to head.
    const uint64_t boundary = head / kRing * kRing;
    uint64_t from = 0;
    for (uint64_t h : heads) if (h < boundary) from = h;
    ASSERT_GT(from, head - kRing) << "walk start must still be inside the live ring";
    const RingWalk w = WalkRing(*ring, from, head);
    EXPECT_EQ(w.end_lsn, head) << "walk must reach HeadLsn exactly";
    EXPECT_GE(w.pads, 1u) << "a 4 KiB-record stream cannot land exactly on a lap end; a PAD must fill the tail";
    EXPECT_GE(w.records, 2u);
}

TEST_F(WalRingWriterTest, FullRingStallsThenFailsWithoutOverwriting) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    walring::SetStallTimeoutMsForTests(200);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 4096);
    const uint64_t stalls_before = XStore::wal_ring_stalls.load();

    size_t accepted = 0;
    bool failed = false;
    std::chrono::steady_clock::duration fail_took{};
    for (uint64_t i = 0; i < 200 && !failed; ++i) {
        const auto t0 = std::chrono::steady_clock::now();
        if (InsertSync(store, i)) { ++accepted; continue; }
        fail_took = std::chrono::steady_clock::now() - t0;
        failed = true;
    }
    ASSERT_TRUE(failed) << "200 x 4 KiB records must not fit a 256 KiB ring with no checkpoint";
    EXPECT_GE(fail_took, std::chrono::milliseconds(150));
    EXPECT_LT(fail_took, std::chrono::seconds(5));
    EXPECT_GT(XStore::wal_ring_stalls.load(), stalls_before);
    EXPECT_GT(accepted, 40u);

    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    EXPECT_LE(ring->HeadLsn() - ring->CheckpointLsn(), kRing) << "never beyond one lap";
    const RingWalk w = WalkRing(*ring, ring->CheckpointLsn(), ring->HeadLsn());
    EXPECT_EQ(w.end_lsn, ring->HeadLsn()) << "every unflushed acknowledged record must still parse";
    EXPECT_EQ(w.records, accepted);

    // Freeing space unblocks the writer.
    ring->AdvanceCheckpoint(ring->HeadLsn());
    EXPECT_TRUE(InsertSync(store, 1000));
}

TEST_F(WalRingWriterTest, OversizedRecordTakesInterimDirectPath) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    ASSERT_TRUE(InsertSync(store, 0));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head_before = ring->HeadLsn();

    constexpr uint64_t kRows = 3000;   // 3000 x (8 + 64) = 216000 B > kRing / 2
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname(db_name);
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (uint64_t i = 0; i < kRows; ++i) {
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(kBaseEpoch + 10 + i);
        entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(i) + 0.5);
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().rfind("[ERROR]", 0), 0u) << rep.insertstatus();

    EXPECT_EQ(ring->HeadLsn(), head_before) << "an oversized record bypasses the ring";
    for (uint64_t i : {uint64_t{0}, kRows / 2, kRows - 1}) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(kBaseEpoch + 10 + i, db_name, &q);
        ASSERT_TRUE(q.has_data()) << i;
        EXPECT_DOUBLE_EQ(q.data().fielddata(0).doubledata(), static_cast<double>(i) + 0.5);
    }
}


// A sub-batch whose WAL write/fdatasync fails has no durable ring record, so its
// rows must not reach the data file or the index sidecars (spec invariant: every
// data-file write has its WAL record inside [checkpoint, head)), and the caller
// must be told.
TEST_F(WalRingWriterTest, FailedWalSyncIsNeitherAppliedNorAcknowledged) {
    constexpr uint16_t kRowLength = 64;
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    ASSERT_TRUE(InsertSync(store, 5));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head_before = ring->HeadLsn();

    const TimeStruct ts(kBaseEpoch + 6, XStore_Proto::SECOND);
    const std::string db_base = (scratch->test_root / "DataStore" / db_name).string();
    const std::string data_path = fmt::format("{}/{}.XSTORE", db_base, ts.year);
    auto row_bytes_at = [&](uint64_t i) {
        std::string bytes(kRowLength, 'x');
        int fd = open(data_path.c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        EXPECT_EQ(pread(fd, bytes.data(), bytes.size(), static_cast<off_t>((kBaseEpoch + i - ts.first_epoch) * kRowLength)),
                  static_cast<ssize_t>(kRowLength));
        close(fd);
        return bytes;
    };
    auto zmap_valid_count = [&](uint64_t i) {
        ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(db_base, ts.year, kRowLength);
        auto stats = zmap.TryGetBlockStats(BlockIndexForRow(kBaseEpoch + i - ts.first_epoch, zmap.RowsPerBlockValue()));
        return stats && !stats->empty() ? (*stats)[0].valid_count : 0u;
    };
    const std::string zeros(kRowLength, '\0');
    ASSERT_NE(row_bytes_at(5), zeros);
    ASSERT_EQ(zmap_valid_count(5), 1u);

    walring::FailNextWalSyncsForTests(1);
    EXPECT_FALSE(InsertSync(store, 6)) << "a failed WAL fdatasync must fail the SYNC insert";
    EXPECT_EQ(ring->HeadLsn(), head_before) << "head must not advance over a record that is not durable";
    EXPECT_EQ(row_bytes_at(6), zeros) << "row reached the data file without a durable WAL record";
    EXPECT_EQ(zmap_valid_count(6), 1u) << "zone map folded a row that was never applied";

    EXPECT_TRUE(InsertSync(store, 7)) << "hook exhausted: the next insert is normal";
    EXPECT_GT(ring->HeadLsn(), head_before);
    EXPECT_EQ(ring->HeadLsn(), ring->AppliedLsn());
    EXPECT_NE(row_bytes_at(7), zeros);
    EXPECT_EQ(row_bytes_at(6), zeros);
    EXPECT_EQ(zmap_valid_count(7), 2u);
    // The failed record's bytes were overwritten by the next one: the log parses intact.
    const RingWalk w = WalkRing(*ring, 0, ring->HeadLsn());
    EXPECT_EQ(w.end_lsn, ring->HeadLsn());
    EXPECT_EQ(w.records, 2u);
}


// ClearRingRegistryForTests must reach the writer threads' per-path ring caches;
// otherwise the writer keeps a pinned Ring (and fd) while the registry hands out a
// second Ring over the same file.
TEST_F(WalRingWriterTest, RegistryClearIsSeenByWriterThreads) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    ASSERT_TRUE(InsertSync(store, 1));
    auto first = BusyRing();
    ASSERT_NE(first, nullptr);
    const uint64_t epoch_before = walring::RegistryEpoch();

    walring::ClearRingRegistryForTests();
    EXPECT_GT(walring::RegistryEpoch(), epoch_before);
    EXPECT_TRUE(MyRings().empty());
    ASSERT_TRUE(InsertSync(store, 2));
    auto second = BusyRing();
    ASSERT_NE(second, nullptr) << "writer kept appending through a Ring the registry no longer knows";
    EXPECT_NE(second.get(), first.get());
    EXPECT_GT(second->HeadLsn(), 0u);
}

TEST(WalRingFormatTest, DefaultRingBytesOverrideLayers) {
    const uint64_t tunable = XStoreTunables::WAL_RING_BYTES;
    walring::SetDefaultRingBytesForTests(0);
    walring::SetRingBytesForTests(0);
    EXPECT_EQ(walring::DefaultRingBytes(), tunable);
    walring::SetDefaultRingBytesForTests(1 << 20);
    EXPECT_EQ(walring::DefaultRingBytes(), 1u << 20);
    walring::SetRingBytesForTests(2 << 20);
    EXPECT_EQ(walring::DefaultRingBytes(), 2u << 20);
    walring::SetRingBytesForTests(0);   // restores the DEFAULT override, not the tunable
    EXPECT_EQ(walring::DefaultRingBytes(), 1u << 20);
    walring::SetDefaultRingBytesForTests(0);
    EXPECT_EQ(walring::DefaultRingBytes(), tunable);
    walring::SetDefaultRingBytesForTests(xstore_test::kTestDefaultRingBytes);   // back to the suite-wide value
}

}  // namespace
