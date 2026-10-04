// WAL ring + paced checkpoint (Durability C), Task 1: ring file header
// slots, append planning, PAD records and the v2 (LSN-carrying) record
// parser. Pure format tests -- nothing here touches the running WAL path.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_Crc32c.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_WalCheckpoint.h"
#include "include/DataStructs/TimeStruct.h"
#include <spdlog/fmt/fmt.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fcntl.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <thread>
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
    // These tests hand-drive checkpoints and inspect ring heads: no background checkpointer.
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
        scratch = std::make_unique<xstore_test::ScratchDir>("xstore_walring_writer_test");
        ClearRegistries();
    }
    void TearDown() override {
        walring::SetRingBytesForTests(0);
        walring::SetStallTimeoutMsForTests(0);
        walring::FailNextWalSyncsForTests(0);
        walring::FailNextHeaderWritesForTests(0);
        walring::FailNextHeaderWritesStickyForTests(0);
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

// ---- Task 3: dirty-range tracking at the apply choke point ------------------

namespace {
// The merged ranges `ranges` holds for `path` cover [begin, end) in full.
bool Covers(const std::vector<walring::DirtyRange> &ranges, uint64_t begin, uint64_t end) {
    for (const auto &r : ranges) if (r.begin <= begin && end <= r.end) return true;
    return false;
}
}  // namespace

TEST_F(WalRingWriterTest, DirtyTrackerMergesAdjacentRanges) {
    walring::DirtyTracker t;
    t.Note(nullptr, "/f", 0, 10, 0);
    t.Note(nullptr, "/f", 10, 20, 0);
    t.Note(nullptr, "/f", 40, 50, 0);
    t.Note(nullptr, "/f", 45, 60, 0);    // overlaps the previous one
    t.Note(nullptr, "/f", 5, 12, 0);     // inside an existing range
    EXPECT_EQ(t.UnflushedBytes(), 20u + 20u);
    const auto snap = t.Take();
    ASSERT_EQ(snap.ranges.size(), 1u);
    const auto &r = snap.ranges.at("/f");
    ASSERT_EQ(r.size(), 2u);
    EXPECT_EQ(r[0].begin, 0u);  EXPECT_EQ(r[0].end, 20u);
    EXPECT_EQ(r[1].begin, 40u); EXPECT_EQ(r[1].end, 60u);
}

TEST_F(WalRingWriterTest, TakeSwapsAndClears) {
    walring::DirtyTracker t;
    EXPECT_EQ(t.OldestUnsyncedAgeMs(), 0u);
    EXPECT_EQ(t.UnflushedBytes(), 0u);
    t.Note(nullptr, "/a", 0, 8, 0);
    t.Note(nullptr, "/b", 100, 164, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(15));
    EXPECT_GE(t.OldestUnsyncedAgeMs(), 10u);
    EXPECT_EQ(t.UnflushedBytes(), 72u);

    const auto first = t.Take();
    EXPECT_EQ(first.ranges.size(), 2u);
    EXPECT_EQ(t.UnflushedBytes(), 0u);
    EXPECT_EQ(t.OldestUnsyncedAgeMs(), 0u);
    EXPECT_TRUE(t.Take().ranges.empty()) << "a second Take sees nothing";

    t.Note(nullptr, "/a", 8, 16, 0);
    const auto again = t.Take();
    ASSERT_EQ(again.ranges.size(), 1u);
    EXPECT_EQ(again.ranges.at("/a")[0].begin, 8u);
}

TEST_F(WalRingWriterTest, RingClearReflectsCheckpoint) {
    auto ring = walring::GetOrOpenRing((scratch->test_root / "dirty_ring.WAL").string(), 0);
    ASSERT_NE(ring, nullptr);
    ring->CommitAppended(1000);

    walring::DirtyTracker t;
    EXPECT_TRUE(t.RingClear("/f")) << "a file nothing ever touched is clear";
    t.Note(nullptr, "/f", 0, 8, 0);
    EXPECT_TRUE(t.RingClear("/f")) << "a direct write carries no ring record";
    t.Note(ring, "/f", 8, 16, 500);
    EXPECT_FALSE(t.RingClear("/f"));
    EXPECT_TRUE(t.RingClear("/other"));
    (void)t.Take();
    EXPECT_FALSE(t.RingClear("/f")) << "Take must not forget un-checkpointed ring records";

    ring->AdvanceCheckpoint(499);
    EXPECT_FALSE(t.RingClear("/f"));
    ring->AdvanceCheckpoint(500);
    EXPECT_TRUE(t.RingClear("/f"));
    t.NoteCheckpointed(ring.get(), 500);   // pruning is hygiene only
    EXPECT_TRUE(t.RingClear("/f"));

    // A destroyed ring counts as clear.
    auto ring2 = walring::GetOrOpenRing((scratch->test_root / "dirty_ring2.WAL").string(), 1);
    ASSERT_NE(ring2, nullptr);
    ring2->CommitAppended(1000);
    t.Note(ring2, "/g", 0, 8, 900);
    EXPECT_FALSE(t.RingClear("/g"));
    ring2.reset();
    walring::ClearRingRegistryForTests();
    EXPECT_TRUE(t.RingClear("/g"));
}

TEST_F(WalRingWriterTest, EveryDataWriteIsTracked) {
    constexpr uint16_t kRowLength = 64;
    constexpr uint64_t kYear2Epoch = 1740000000;   // a different year than kBaseEpoch
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    (void)walring::Dirty().Take();                 // CreateDB's own writes are not under test

    ASSERT_TRUE(InsertSync(store, 5));
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname(db_name);
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (uint64_t i = 0; i < 3; ++i) {
        for (uint64_t base : {kBaseEpoch + 100, kYear2Epoch}) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(base + i);
            entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(i));
        }
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().rfind("[ERROR]", 0), 0u) << rep.insertstatus();

    const std::string db_base = (scratch->test_root / "DataStore" / db_name).string();
    const TimeStruct ts1(kBaseEpoch, XStore_Proto::SECOND);
    const TimeStruct ts2(kYear2Epoch, XStore_Proto::SECOND);
    ASSERT_NE(ts1.year, ts2.year);
    const std::string path1 = fmt::format("{}/{}.XSTORE", db_base, ts1.year);
    const std::string path2 = fmt::format("{}/{}.XSTORE", db_base, ts2.year);

    const auto snap = walring::Dirty().Take();
    ASSERT_TRUE(snap.ranges.count(path1)) << "unary + batch rows of year 1";
    ASSERT_TRUE(snap.ranges.count(path2)) << "batch rows of year 2";
    for (uint64_t i : {uint64_t{5}, uint64_t{100}, uint64_t{101}, uint64_t{102}}) {
        const uint64_t off = (kBaseEpoch + i - ts1.first_epoch) * kRowLength;
        EXPECT_TRUE(Covers(snap.ranges.at(path1), off, off + kRowLength)) << "year-1 row " << i;
    }
    for (uint64_t i = 0; i < 3; ++i) {
        const uint64_t off = (kYear2Epoch + i - ts2.first_epoch) * kRowLength;
        EXPECT_TRUE(Covers(snap.ranges.at(path2), off, off + kRowLength)) << "year-2 row " << i;
    }
    // Contiguous rows 100..102 are ONE run, not three.
    const uint64_t off100 = (kBaseEpoch + 100 - ts1.first_epoch) * kRowLength;
    EXPECT_TRUE(Covers(snap.ranges.at(path1), off100, off100 + 3 * kRowLength));
}

TEST_F(WalRingWriterTest, RingClearIsFalseUntilCheckpointPassesTheRecord) {
    constexpr uint16_t kRowLength = 64;
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    ASSERT_TRUE(InsertSync(store, 1));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const TimeStruct ts(kBaseEpoch, XStore_Proto::SECOND);
    const std::string data_path = fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(), ts.year);

    EXPECT_FALSE(walring::Dirty().RingClear(data_path)) << "the ring record is still un-checkpointed";
    (void)walring::Dirty().Take();
    EXPECT_FALSE(walring::Dirty().RingClear(data_path)) << "Take must not clear the ring bookkeeping";
    ring->AdvanceCheckpoint(ring->HeadLsn());
    EXPECT_TRUE(walring::Dirty().RingClear(data_path));
}

TEST_F(WalRingWriterTest, BypassWriteIsTrackedButDoesNotBlockRingClear) {
    constexpr uint16_t kRowLength = 64;
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    (void)walring::Dirty().Take();

    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(1.0);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(kBaseEpoch + 9, row, db_name, &rep, XStore_Proto::BYPASS);
    ASSERT_NE(rep.insertstatus().rfind("[ERROR]", 0), 0u) << rep.insertstatus();

    const TimeStruct ts(kBaseEpoch, XStore_Proto::SECOND);
    const std::string data_path = fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(), ts.year);
    EXPECT_TRUE(walring::Dirty().RingClear(data_path)) << "no ring record covers a BYPASS write";
    const auto snap = walring::Dirty().Take();
    ASSERT_TRUE(snap.ranges.count(data_path));
    const uint64_t off = (kBaseEpoch + 9 - ts.first_epoch) * kRowLength;
    EXPECT_TRUE(Covers(snap.ranges.at(data_path), off, off + kRowLength));
}

// ---- Task 3 polish --------------------------------------------------------

TEST_F(WalRingWriterTest, SingleNoteBridgesSeveralRanges) {
    walring::DirtyTracker t;
    t.Note(nullptr, "/f", 0, 10, 0);
    t.Note(nullptr, "/f", 20, 30, 0);
    t.Note(nullptr, "/f", 40, 50, 0);
    t.Note(nullptr, "/f", 60, 70, 0);
    EXPECT_EQ(t.UnflushedBytes(), 40u);
    t.Note(nullptr, "/f", 5, 45, 0);      // bridges [0,10) [20,30) [40,50) in one Note
    EXPECT_EQ(t.UnflushedBytes(), 50u + 10u);
    const auto snap = t.Take();
    const auto &r = snap.ranges.at("/f");
    ASSERT_EQ(r.size(), 2u);
    EXPECT_EQ(r[0].begin, 0u);  EXPECT_EQ(r[0].end, 50u);
    EXPECT_EQ(r[1].begin, 60u); EXPECT_EQ(r[1].end, 70u);
}

TEST_F(WalRingWriterTest, WriterRecordsTheExactEndLsn) {
    constexpr uint16_t kRowLength = 64;
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    ASSERT_TRUE(InsertSync(store, 1));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_GT(ring->HeadLsn(), 1u);
    const TimeStruct ts(kBaseEpoch, XStore_Proto::SECOND);
    const std::string data_path = fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(), ts.year);

    ring->AdvanceCheckpoint(ring->HeadLsn() - 1);
    EXPECT_FALSE(walring::Dirty().RingClear(data_path)) << "the mark is the record's END lsn: head-1 must not clear it";
    ring->AdvanceCheckpoint(ring->HeadLsn());
    EXPECT_TRUE(walring::Dirty().RingClear(data_path)) << "head clears it exactly";
}

// ---- Task 4: checkpointer ---------------------------------------------------

TEST_F(WalRingWriterTest, PeekRangesDoesNotReset) {
    walring::DirtyTracker t;
    t.Note(nullptr, "/a", 0, 8, 0);
    t.Note(nullptr, "/a", 16, 24, 0);
    const auto peek = t.PeekRanges();
    ASSERT_EQ(peek.ranges.at("/a").size(), 2u);
    EXPECT_EQ(t.UnflushedBytes(), 16u) << "Peek must not clear the bytes";
    EXPECT_EQ(t.PeekRanges().ranges.at("/a").size(), 2u);
    EXPECT_EQ(t.Take().ranges.at("/a").size(), 2u) << "Take still sees everything after a Peek";
    EXPECT_TRUE(t.PeekRanges().ranges.empty());
}

// M6: a hole-punch is a metadata change nothing else marks dirty; without a Note a clean stop would never
// fdatasync it (the final cycle only syncs noted files).
TEST_F(WalRingWriterTest, EveryDeleteMarksItsFileDirty) {
    constexpr uint16_t kRowLength = 64;
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    for (uint64_t i = 0; i < 12; ++i) ASSERT_TRUE(InsertSync(store, i));
    const TimeStruct ts(kBaseEpoch, XStore_Proto::SECOND);
    const std::string path = fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(), ts.year);
    const auto offset_of = [&](uint64_t i) { return (kBaseEpoch + i - ts.first_epoch) * kRowLength; };
    // Dirty ranges noted since the last Take for `path`, after the delete under test.
    const auto expect_marked = [&](const char *what, uint64_t first_row, uint64_t row_count) {
        const auto snap = walring::Dirty().Take();
        const auto it = snap.ranges.find(path);
        ASSERT_NE(it, snap.ranges.end()) << what << ": the punched file was not marked dirty";
        EXPECT_TRUE(Covers(it->second, offset_of(first_row), offset_of(first_row) + row_count * kRowLength)) << what;
    };

    (void)walring::Dirty().Take();
    EXPECT_NE(store.UnaryDelete(kBaseEpoch + 1, db_name).deletestatus().find("[INFO]"), std::string::npos);
    expect_marked("UnaryDelete", 1, 1);

    XStore_Proto::BatchDelete_REQ req;
    req.set_dbname(db_name);
    for (uint64_t i : {uint64_t{3}, uint64_t{5}, uint64_t{9}}) req.add_timestamp(kBaseEpoch + i);
    EXPECT_NE(store.BatchDelete(&req).deletestatus().find("Deleted 3 items"), std::string::npos);
    {
        const auto snap = walring::Dirty().Take();
        const auto it = snap.ranges.find(path);
        ASSERT_NE(it, snap.ranges.end()) << "BatchDelete: the punched file was not marked dirty";
        for (uint64_t i : {uint64_t{3}, uint64_t{5}, uint64_t{9}}) {
            EXPECT_TRUE(Covers(it->second, offset_of(i), offset_of(i) + kRowLength)) << "BatchDelete row " << i;
        }
    }

    EXPECT_NE(store.RangeDelete(kBaseEpoch + 6, kBaseEpoch + 8, db_name).deletestatus().find("Deleted 3 items"), std::string::npos);
    expect_marked("RangeDelete", 6, 3);
}

namespace {
struct ShutdownReset { ~ShutdownReset() { XStore::ResetShutdownForTests(); } };
}  // namespace

class WalCheckpointTest : public WalRingWriterTest {
protected:
    static walring::Checkpointer &Ckpt() { return walring::Checkpointer::Instance(); }

    void TearDown() override {
        Ckpt().SetPausedForTests(true);
        Ckpt().FailNextDataSyncsForTests(0);
        Ckpt().FailNextDirSyncsForTests(0);
        Ckpt().ClearPoisonForTests();
        Ckpt().SetIdleMsForTests(0);
        Ckpt().SetMaxAgeMsForTests(0);
        Ckpt().SetReplaySyncDelayMsForTests(0);
        Ckpt().Start();   // a test may have stopped it
        WalRingWriterTest::TearDown();
    }

    std::string DataPath() const {
        const TimeStruct ts(kBaseEpoch, XStore_Proto::SECOND);
        return fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(), ts.year);
    }
    // `dirty` flag of this test's year index (".zmap" / ".catidx"), read from disk by path.
    template <typename Header>
    uint32_t OnDiskDirty(const char *ext) const {
        const std::string path = std::filesystem::path(DataPath()).replace_extension(ext).string();
        Header h{};
        const int fd = open(path.c_str(), O_RDONLY);
        EXPECT_GE(fd, 0) << path;
        EXPECT_EQ(pread(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h)));
        close(fd);
        return h.dirty;
    }
    uint32_t ZmapDirty() const { return OnDiskDirty<ZoneMapFileHeader>(".zmap"); }
    uint32_t CatIdxDirty() const { return OnDiskDirty<CatIdxFileHeader>(".catidx"); }
    uint64_t OnDiskCheckpoint(const walring::Ring &ring) const {
        const int fd = open(ring.path().c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        walring::RingHeader h;
        EXPECT_TRUE(walring::ReadBestHeader(fd, h));
        close(fd);
        return h.checkpoint_lsn;
    }
    template <typename Pred>
    static bool WaitFor(Pred pred, std::chrono::milliseconds limit) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (!pred()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        return true;
    }
    // Inserts rows until the busy ring's fill reaches `fill` (checkpointer must not be cycling).
    std::shared_ptr<walring::Ring> FillTo(XStore &store, double fill, uint64_t &next) {
        std::shared_ptr<walring::Ring> ring;
        while (!ring || ring->Fill() < fill) {
            EXPECT_TRUE(InsertSync(store, next++));
            if (!ring) ring = BusyRing();
            if (next > 1000) break;
        }
        return ring;
    }
};

TEST_F(WalCheckpointTest, CycleAdvancesCheckpointToAppliedAndPersistsIt) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 50; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_GT(ring->HeadLsn(), 0u);
    EXPECT_EQ(ring->CheckpointLsn(), 0u);

    ASSERT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), ring->AppliedLsn());
    EXPECT_EQ(ring->AppliedLsn(), ring->HeadLsn());
    EXPECT_EQ(Ckpt().LastCycleSyncedPaths().count(DataPath()), 1u);

    const int fd = open(ring->path().c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    walring::RingHeader h;
    ASSERT_TRUE(walring::ReadBestHeader(fd, h));
    close(fd);
    EXPECT_EQ(h.checkpoint_lsn, ring->HeadLsn()) << "the checkpoint must be persisted in the header";
}

TEST_F(WalCheckpointTest, CheckpointNeverAdvancesWhenDataSyncFails) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);

    Ckpt().FailNextDataSyncsForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), 0u) << "a failed data fsync must not release WAL space";

    // The failed cycle already Took the dirty set; the retry must still sync that file.
    EXPECT_EQ(walring::Dirty().UnflushedBytes(), 0u);
    ASSERT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
    EXPECT_EQ(Ckpt().LastCycleSyncedPaths().count(DataPath()), 1u) << "retry synced the carried file";
}

TEST_F(WalCheckpointTest, CheckpointNeverAdvancesWhenDirSyncFails) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);

    Ckpt().FailNextDirSyncsForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    ASSERT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
}

TEST_F(WalCheckpointTest, NewYearFileParentDirIsSyncedBeforeAdvance) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    ASSERT_TRUE(InsertSync(store, 0));   // creates this year's file
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);

    const uint64_t before = Ckpt().DirSyncsForTests();
    Ckpt().FailNextDirSyncsForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), 0u) << "no advance without the directory entry being durable";
    EXPECT_EQ(Ckpt().DirSyncsForTests(), before) << "the failed dir sync was not counted";
    ASSERT_TRUE(Ckpt().RunCycle());
    EXPECT_GE(Ckpt().DirSyncsForTests(), before + 1) << "the directory was fsync'd in the cycle that advanced the checkpoint";
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
    const std::string year_dir = std::filesystem::path(DataPath()).parent_path().string();
    EXPECT_EQ(Ckpt().LastCycleSyncedDirsForTests().count(year_dir), 1u) << "the year file's parent directory must be among the synced dirs";
}

TEST_F(WalCheckpointTest, RetryableInjectedFailureStillRetries) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    Ckpt().FailNextDataSyncsForTests(1);
    Ckpt().FailNextDirSyncsForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_FALSE(Ckpt().Poisoned()) << "a plain injected failure stays retryable";
    EXPECT_FALSE(Ckpt().RunCycle()) << "second cycle consumes the dir-sync failure";
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    ASSERT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
}

TEST_F(WalCheckpointTest, StickySyncErrorPoisonsAndNeverAdvancesAgain) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    Ckpt().FailNextDataSyncsStickyForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    // No hooks left: a real retried fdatasync would now return 0 -- it must still not advance.
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    EXPECT_TRUE(Ckpt().Poisoned());
    // The thread does not retry either, even with every trigger satisfied.
    Ckpt().SetIdleMsForTests(1);
    Ckpt().SetPausedForTests(false);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
}

TEST_F(WalCheckpointTest, StickyDirSyncErrorAlsoPoisons) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    Ckpt().FailNextDirSyncsStickyForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
}

// ---- Final fix round: separate header file description (I-1), sticky header/WAL errors ----

namespace {
// "ino:" line of /proc/self/fdinfo/<fd>; 0 if unavailable.
uint64_t FdInode(int fd) {
    std::FILE *f = std::fopen(fmt::format("/proc/self/fdinfo/{}", fd).c_str(), "r");
    if (!f) return 0;
    char line[256];
    uint64_t ino = 0;
    while (std::fgets(line, sizeof line, f)) {
        if (std::strncmp(line, "ino:", 4) == 0) ino = std::strtoull(line + 4, nullptr, 10);
    }
    std::fclose(f);
    return ino;
}
}  // namespace

// Linux reports a writeback error once per open FILE DESCRIPTION. If the checkpointer's header fdatasync and the
// writer's ring fdatasync shared one, either could swallow the other's error. The Ring therefore opens the file twice.
TEST_F(WalCheckpointTest, RingHeaderWritesUseAnIndependentFileDescription) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    ASSERT_TRUE(InsertSync(store, 0));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const int fd = ring->fd(), hdr = ring->HeaderFdForTests();
    ASSERT_GE(fd, 0);
    ASSERT_GE(hdr, 0);
    EXPECT_NE(hdr, fd);
    // Same file...
    const uint64_t ino = FdInode(fd);
    ASSERT_NE(ino, 0u) << "/proc/self/fdinfo unavailable";
    EXPECT_EQ(FdInode(hdr), ino);
    // ...but two open file descriptions: a dup()'d descriptor would share this offset (and the error cursor).
    const off_t w0 = lseek(fd, 0, SEEK_CUR), h0 = lseek(hdr, 0, SEEK_CUR);
    ASSERT_GE(w0, 0);
    ASSERT_GE(h0, 0);
    ASSERT_EQ(lseek(hdr, 12345, SEEK_SET), 12345);
    EXPECT_EQ(lseek(fd, 0, SEEK_CUR), w0) << "the header fd shares an open file description with the writer fd";
    ASSERT_EQ(lseek(hdr, h0, SEEK_SET), h0);
    // Where the kernel allows it, ask directly (0 = same description).
    const long same = syscall(SYS_kcmp, getpid(), getpid(), 0 /*KCMP_FILE*/, fd, hdr);
    if (same != -1) EXPECT_NE(same, 0L) << "kcmp says both fds are the same open file description";
}

// Proves the header slots really are written through hdr_fd_ (not fd_): make hdr_fd_ read-only; the next header
// write must fail with a REAL error (EBADF) and poison.
TEST_F(WalCheckpointTest, HeaderWritesGoThroughTheHeaderFd) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const int ro = open(ring->path().c_str(), O_RDONLY);
    ASSERT_GE(ro, 0);
    ASSERT_EQ(dup2(ro, ring->HeaderFdForTests()), ring->HeaderFdForTests());
    close(ro);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_TRUE(Ckpt().Poisoned()) << "a genuine header pwrite error is sticky";
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    EXPECT_EQ(OnDiskCheckpoint(*ring), 0u);
}

TEST_F(WalCheckpointTest, HeaderWriteRealErrorPoisonsCheckpointer) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    walring::FailNextHeaderWritesStickyForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    EXPECT_EQ(OnDiskCheckpoint(*ring), 0u);
    // No hook left: a retried header fdatasync could now "succeed" -- the checkpointer must still never advance.
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    EXPECT_TRUE(Ckpt().Poisoned());
}

TEST_F(WalCheckpointTest, HeaderWriteInjectedErrorStillRetries) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    walring::FailNextHeaderWritesForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_FALSE(Ckpt().Poisoned()) << "a plain injected header failure stays retryable";
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    EXPECT_EQ(OnDiskCheckpoint(*ring), 0u);
    ASSERT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
    EXPECT_EQ(OnDiskCheckpoint(*ring), ring->HeadLsn());
}

// The failed batch is reported to its callers, AND the checkpointer is poisoned: a device that is dropping the
// writer's ring pages must not have the checkpoint advanced over data files either.
TEST_F(WalCheckpointTest, WriterRingSyncRealErrorPoisons) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    ASSERT_TRUE(InsertSync(store, 0));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    walring::FailNextWalSyncsStickyForTests(1);
    EXPECT_FALSE(InsertSync(store, 1)) << "the caller of the failed batch must be told";
    EXPECT_EQ(ring->HeadLsn(), head);
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_FALSE(Ckpt().RunCycle()) << "poisoned: nothing advances";
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
}

TEST_F(WalCheckpointTest, WriterRingSyncInjectedErrorDoesNotPoison) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    ASSERT_TRUE(InsertSync(store, 0));
    walring::FailNextWalSyncsForTests(1);
    EXPECT_FALSE(InsertSync(store, 1));
    EXPECT_FALSE(Ckpt().Poisoned()) << "the plain injected WAL sync failure stays retryable";
    EXPECT_TRUE(InsertSync(store, 2));
    EXPECT_TRUE(Ckpt().RunCycle());
}

// M9: sync_file_range consumes ANY writeback error, so every errno poisons except the retried EINTR/EAGAIN and
// the "unsupported here" usage errors.
TEST_F(WalCheckpointTest, PacedFlushPoisonsOnEveryErrnoExceptUsageErrors) {
    constexpr uint64_t kRing = 512 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 4096);
    Ckpt().SetIdleMsForTests(1000000);
    Ckpt().SetMaxAgeMsForTests(1000000);
    uint64_t next = 0;
    auto ring = FillTo(store, 0.27, next);   // above the 25% soft threshold, far below the 50% cycle threshold
    ASSERT_NE(ring, nullptr);
    Ckpt().SetPausedForTests(false);
    ASSERT_TRUE(WaitFor([&] { return Ckpt().PacedFlushBytesForTests() > 0; }, std::chrono::milliseconds(2000)));

    // Benign: skipped as best effort, the flush carries on, nothing poisons.
    for (const int err : {EINTR, EAGAIN, EINVAL, ESPIPE, ENOSYS, EBADF}) {
        const uint64_t flushed = Ckpt().PacedFlushBytesForTests();
        Ckpt().SimulateSyncFileRangeErrnoForTests(err);
        ASSERT_TRUE(InsertSync(store, next++));   // a fresh dirty range for the loop to flush
        EXPECT_TRUE(WaitFor([&] { return Ckpt().PacedFlushBytesForTests() > flushed; }, std::chrono::milliseconds(2000)))
            << "errno " << err << " must be skipped, not stop the flush";
        EXPECT_FALSE(Ckpt().Poisoned()) << "errno " << err << " is a usage/transient error";
    }
    // Everything else poisons (EIO/ENOSPC/EDQUOT as before; ENOMEM, EROFS, EBADMSG, ... newly).
    for (const int err : {EIO, ENOSPC, EDQUOT, ENOMEM, EROFS, EBADMSG}) {
        Ckpt().ClearPoisonForTests();
        Ckpt().SimulateSyncFileRangeErrnoForTests(err);
        ASSERT_TRUE(InsertSync(store, next++));
        EXPECT_TRUE(WaitFor([&] { return Ckpt().Poisoned(); }, std::chrono::milliseconds(2000))) << "errno " << err << " must poison";
        EXPECT_EQ(ring->CheckpointLsn(), 0u);
    }
}

TEST_F(WalCheckpointTest, PoisonedCheckpointerMakesWritersStallThenFail) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    walring::SetStallTimeoutMsForTests(200);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 4096);
    ASSERT_TRUE(InsertSync(store, 0));
    size_t accepted = 1;
    Ckpt().FailNextDataSyncsStickyForTests(1);
    ASSERT_FALSE(Ckpt().RunCycle());
    ASSERT_TRUE(Ckpt().Poisoned());
    Ckpt().SetPausedForTests(false);   // the thread must not rescue the ring
    const uint64_t stalls_before = XStore::wal_ring_stalls.load();

    bool failed = false;
    for (uint64_t i = 1; i < 200 && !failed; ++i) {
        if (InsertSync(store, i)) ++accepted; else failed = true;
    }
    ASSERT_TRUE(failed) << "a poisoned checkpointer can never free the ring";
    EXPECT_GT(XStore::wal_ring_stalls.load(), stalls_before);
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
    EXPECT_LE(ring->HeadLsn(), kRing) << "never beyond one lap";
    const RingWalk w = WalkRing(*ring, 0, ring->HeadLsn());
    EXPECT_EQ(w.end_lsn, ring->HeadLsn()) << "every acknowledged record must still parse (nothing overwritten)";
    EXPECT_EQ(w.records, accepted);
}

TEST_F(WalCheckpointTest, DeletedDataFileDoesNotBlockTheCheckpoint) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    ASSERT_TRUE(InsertSync(store, 0));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_EQ(rename(DataPath().c_str(), (DataPath() + ".gone").c_str()), 0);   // as if the DB were deleted

    ASSERT_TRUE(Ckpt().RunCycle()) << "ENOENT = deleted DB: nothing left to make durable, never a permanent stall";
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
    EXPECT_EQ(Ckpt().LastCycleSyncedPaths().count(DataPath()), 0u) << "a file that was not synced must not be claimed synced";
}

TEST_F(WalCheckpointTest, IdleTriggersACycleWithinBounds) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    Ckpt().SetIdleMsForTests(50);
    Ckpt().SetPausedForTests(false);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    EXPECT_TRUE(WaitFor([&] { return ring->CheckpointLsn() == ring->HeadLsn(); }, std::chrono::milliseconds(1000)))
        << "checkpoint=" << ring->CheckpointLsn() << " head=" << ring->HeadLsn();
}

TEST_F(WalCheckpointTest, LazyBelowSoftFillNoCycle) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    Ckpt().SetIdleMsForTests(1000000);
    Ckpt().SetMaxAgeMsForTests(1000000);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    Ckpt().SetPausedForTests(false);
    const uint64_t cycles = Ckpt().CyclesForTests();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    EXPECT_EQ(Ckpt().CyclesForTests(), cycles);
    EXPECT_EQ(ring->CheckpointLsn(), 0u);
}

TEST_F(WalCheckpointTest, SoftFillStartsPacedFlushWithoutAdvancingCheckpoint) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 4096);
    Ckpt().SetIdleMsForTests(1000000);
    Ckpt().SetMaxAgeMsForTests(1000000);
    const uint64_t cycles = Ckpt().CyclesForTests();
    const uint64_t flushed = Ckpt().PacedFlushBytesForTests();
    uint64_t next = 0;
    auto ring = FillTo(store, 0.27, next);   // ~70 KiB of a 256 KiB ring; the cycle threshold is 50%
    ASSERT_NE(ring, nullptr);
    ASSERT_GE(ring->Fill(), 0.25);
    ASSERT_LT(ring->Fill(), 0.5);

    Ckpt().SetPausedForTests(false);
    EXPECT_TRUE(WaitFor([&] { return Ckpt().PacedFlushBytesForTests() > flushed; }, std::chrono::milliseconds(2000)))
        << "fill >= SOFT must start flushing dirty ranges";
    EXPECT_EQ(ring->CheckpointLsn(), 0u) << "a paced flush never advances the checkpoint";
    EXPECT_EQ(Ckpt().CyclesForTests(), cycles);
}

TEST_F(WalCheckpointTest, FullRingUnblocksWhenCheckpointerRuns) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 4096);
    Ckpt().SetPausedForTests(false);
    const uint64_t total = 4 * kRing / 4096 + 8;   // > 4 laps of ~4 KiB records
    for (uint64_t i = 0; i < total; ++i) ASSERT_TRUE(InsertSync(store, i)) << "insert " << i;
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    EXPECT_GT(ring->HeadLsn(), 4 * kRing);
    EXPECT_GT(ring->CheckpointLsn(), 0u);
}

TEST_F(WalCheckpointTest, ShutdownRunsFinalCycle) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 20; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_GT(ring->HeadLsn(), ring->CheckpointLsn());
    {
        ShutdownReset reset;
        XStore::Shutdown();
        for (auto &r : MyRings()) {
            EXPECT_EQ(r->CheckpointLsn(), r->HeadLsn()) << r->path();
            const int fd = open(r->path().c_str(), O_RDONLY);
            ASSERT_GE(fd, 0);
            walring::RingHeader h;
            ASSERT_TRUE(walring::ReadBestHeader(fd, h));
            close(fd);
            EXPECT_EQ(h.checkpoint_lsn, r->HeadLsn());
        }
    }
}

TEST_F(WalCheckpointTest, StopAfterFinalCycleIsIdempotent) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    Ckpt().StopAfterFinalCycle();
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn()) << "the final cycle ran";
    Ckpt().StopAfterFinalCycle();   // second call: no hang, no crash

    // Restartable: the thread comes back and the idle trigger works again.
    Ckpt().Start();
    Ckpt().Start();   // idempotent
    Ckpt().SetPausedForTests(false);
    for (uint64_t i = 5; i < 8; ++i) ASSERT_TRUE(InsertSync(store, i));
    EXPECT_TRUE(WaitFor([&] { return ring->CheckpointLsn() == ring->HeadLsn(); }, std::chrono::milliseconds(1500)));
}

// ---- Task 6: Shutdown relies on the final cycle; index MarkClean is gated on it ----

TEST_F(WalCheckpointTest, ShutdownMarksIndexesCleanAfterFinalCycle) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 20; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_EQ(ZmapDirty(), 1u) << "precondition: inserts dirty the zone map";
    ASSERT_EQ(CatIdxDirty(), 1u) << "precondition: inserts dirty the catidx";
    {
        ShutdownReset reset;
        XStore::Shutdown();
        EXPECT_TRUE(Ckpt().FinalCycleOk());
        EXPECT_EQ(ZmapDirty(), 0u);
        EXPECT_EQ(CatIdxDirty(), 0u);
        EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
        EXPECT_EQ(OnDiskCheckpoint(*ring), ring->HeadLsn());
    }
}

TEST_F(WalCheckpointTest, ShutdownDoesNotMarkIndexesCleanWhenPoisoned) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    ASSERT_GT(head, 0u);
    ASSERT_EQ(ZmapDirty(), 1u);
    ASSERT_EQ(CatIdxDirty(), 1u);

    Ckpt().FailNextDataSyncsStickyForTests(1);   // the final cycle itself hits the real-style error
    {
        ShutdownReset reset;
        XStore::Shutdown();   // returns normally: no throw, no abort
        EXPECT_TRUE(Ckpt().Poisoned());
        EXPECT_FALSE(Ckpt().FinalCycleOk());
        EXPECT_EQ(ZmapDirty(), 1u) << "an index must not be marked clean over unsynced data";
        EXPECT_EQ(CatIdxDirty(), 1u);
        EXPECT_EQ(ring->CheckpointLsn(), 0u);
        EXPECT_EQ(ring->HeadLsn(), head);
        EXPECT_EQ(OnDiskCheckpoint(*ring), 0u);
    }
}

TEST_F(WalCheckpointTest, ShutdownDoesNotMarkIndexesCleanWhenPoisonedBeforeShutdown) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    Ckpt().FailNextDataSyncsStickyForTests(1);
    ASSERT_FALSE(Ckpt().RunCycle());
    ASSERT_TRUE(Ckpt().Poisoned());
    {
        ShutdownReset reset;
        XStore::Shutdown();   // the final cycle refuses (poisoned)
        EXPECT_EQ(ring->CheckpointLsn(), 0u);
        EXPECT_GT(ring->HeadLsn(), 0u);
        EXPECT_EQ(ZmapDirty(), 1u);
        EXPECT_EQ(CatIdxDirty(), 1u);
    }
}

TEST_F(WalCheckpointTest, ShutdownDoesNotMarkIndexesCleanWhenFinalCycleFails) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    Ckpt().FailNextDataSyncsForTests(1);   // retryable: consumed by the final cycle (the thread is paused)
    {
        ShutdownReset reset;
        XStore::Shutdown();
        EXPECT_FALSE(Ckpt().Poisoned()) << "a retryable failure does not poison";
        EXPECT_FALSE(Ckpt().FinalCycleOk());
        EXPECT_EQ(ZmapDirty(), 1u);
        EXPECT_EQ(CatIdxDirty(), 1u);
        EXPECT_EQ(ring->CheckpointLsn(), 0u) << "the WAL must stay replayable";
    }
}

// The final cycle only syncs files dirtied since the previous cycle, so a file synced by an
// earlier cycle is absent from LastCycleSyncedPaths() although its data is durable. Gating
// MarkClean on that set would leave its indexes dirty for no reason.
TEST_F(WalCheckpointTest, ShutdownMarksCleanEvenForFilesSyncedInEarlierCycles) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_TRUE(Ckpt().RunCycle());
    ASSERT_EQ(ring->CheckpointLsn(), ring->HeadLsn());
    ASSERT_EQ(ZmapDirty(), 1u) << "a cycle alone never marks an index clean";
    {
        ShutdownReset reset;
        XStore::Shutdown();   // no new writes: nothing dirty for the final cycle
        EXPECT_EQ(Ckpt().LastCycleSyncedPaths().count(DataPath()), 0u) << "the case an old per-path gate would have broken";
        EXPECT_TRUE(Ckpt().FinalCycleOk());
        EXPECT_EQ(ZmapDirty(), 0u);
        EXPECT_EQ(CatIdxDirty(), 0u);
    }
}

// A filesystem that cannot fsync directories (EINVAL ...) is not a writeback error.
TEST_F(WalCheckpointTest, DirectoryFsyncUnsupportedDoesNotPoison) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    Ckpt().SimulateDirSyncErrnoForTests(EINVAL);
    const uint64_t warnings = Ckpt().DirSyncUnsupportedWarningsForTests();
    const uint64_t dir_syncs = Ckpt().DirSyncsForTests();

    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertSync(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_TRUE(Ckpt().RunCycle()) << "an unsupported directory fsync must not fail the cycle";
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());

    for (uint64_t i = 3; i < 6; ++i) ASSERT_TRUE(InsertSync(store, i));   // dirties the file again => a second dir sync attempt
    ASSERT_TRUE(Ckpt().RunCycle());
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn());

    EXPECT_EQ(Ckpt().DirSyncUnsupportedWarningsForTests(), warnings + 1) << "one warning per process, not per cycle";
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_syncs) << "no directory was actually made durable";

    // A genuine writeback error on the same path still poisons.
    Ckpt().SimulateDirSyncErrnoForTests(EIO);
    for (uint64_t i = 6; i < 8; ++i) ASSERT_TRUE(InsertSync(store, i));
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_TRUE(Ckpt().Poisoned());
}

// Informational: prints the time; the bound is deliberately generous.
TEST_F(WalCheckpointTest, ShutdownTimeIsSmallAfterBulkInsert) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    constexpr int kBatches = 20, kRowsPerBatch = 1000;
    for (int b = 0; b < kBatches; ++b) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db_name);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (int i = 0; i < kRowsPerBatch; ++i) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(kBaseEpoch + static_cast<uint64_t>(b) * kRowsPerBatch + static_cast<uint64_t>(i));
            entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(i));
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    }
    {
        ShutdownReset reset;
        const auto t0 = std::chrono::steady_clock::now();
        XStore::Shutdown();
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
        std::printf("[ INFO ] Shutdown() after %d SYNC rows took %lld ms\n", kBatches * kRowsPerBatch, static_cast<long long>(ms));
        EXPECT_LT(ms, 5000);
        EXPECT_EQ(ZmapDirty(), 0u);
    }
}

// ---- Task 5: replay from checkpoint_lsn; legacy upgrade ----------------------

// Crash = WalSimulateRestartForTests() (no final cycle, no MarkClean, registries dropped),
// restart = InitDB(). Rows live in 1970 (epoch 1000+) so the forced zone-map rebuild after a
// replay scans a few KiB, not a calendar year; the year file is truncated to the written rows
// for the same reason (the fd cache stays warm across the simulated restart, so replay does
// not re-grow it).
class WalReplayTest : public WalCheckpointTest {
protected:
    static constexpr uint64_t kEpoch0 = 1000;
    uint16_t row_length = 64;

    std::string YearFile() const {
        return fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(),
                           TimeStruct(kEpoch0, XStore_Proto::SECOND).year);
    }
    bool InsertAt(XStore &store, uint64_t i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(static_cast<double>(i) + 1.0);
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(kEpoch0 + i, row, db_name, &rep, XStore_Proto::SYNC);
        return rep.insertstatus().rfind("[ERROR]", 0) != 0;
    }
    void ShrinkDataFile(uint64_t rows) const {
        const int fd = open(YearFile().c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, static_cast<off_t>((kEpoch0 + rows + 1) * row_length)), 0);
        close(fd);
    }
    std::string RowBytes(uint64_t i) const {
        std::string bytes(row_length, 'x');
        const int fd = open(YearFile().c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        EXPECT_EQ(pread(fd, bytes.data(), bytes.size(), static_cast<off_t>((kEpoch0 + i) * row_length)), static_cast<ssize_t>(row_length));
        close(fd);
        return bytes;
    }
    bool RowPresent(uint64_t i) const { return RowBytes(i) != std::string(row_length, '\0'); }
    void ZeroRows(uint64_t from, uint64_t to) const {   // [from, to)
        const std::string zeros((to - from) * row_length, '\0');
        const int fd = open(YearFile().c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, zeros.data(), zeros.size(), static_cast<off_t>((kEpoch0 + from) * row_length)), static_cast<ssize_t>(zeros.size()));
        close(fd);
    }
    static walring::RingHeader ReadHeader(const std::string &wal_path) {
        walring::RingHeader h{};
        const int fd = open(wal_path.c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        EXPECT_TRUE(walring::ReadBestHeader(fd, h)) << wal_path;
        close(fd);
        return h;
    }
    static void Crash() { XStore::WalSimulateRestartForTests(); }
    void TearDown() override {
        walring::FailNextReplayReadsForTests(0);
        walring::FailNextReplayZeroingsForTests(0);
        WalCheckpointTest::TearDown();
    }
};

TEST_F(WalReplayTest, ReplayAppliesRecordsAfterCheckpointOnly) {
    walring::SetRingBytesForTests(256 << 10);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 10; ++i) ASSERT_TRUE(InsertAt(store, i));          // A
    ASSERT_TRUE(Ckpt().RunCycle());
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t ckpt_a = ring->CheckpointLsn();
    ASSERT_EQ(ckpt_a, ring->HeadLsn());
    for (uint64_t i = 10; i < 20; ++i) ASSERT_TRUE(InsertAt(store, i));         // B
    const uint64_t head = ring->HeadLsn();
    ASSERT_GT(head, ckpt_a);
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(20);

    Crash();
    ZeroRows(0, 20);                  // lost page cache
    const uint64_t zeroed_before = walring::ReplayZeroedBytesForTests();
    store.InitDB();
    EXPECT_EQ(walring::ReplayZeroedBytesForTests(), zeroed_before) << "a plain zeros end is a clean end: no ring write";

    for (uint64_t i = 0; i < 10; ++i) EXPECT_FALSE(RowPresent(i)) << "A row " << i << " is behind the checkpoint: must not be replayed";
    for (uint64_t i = 10; i < 20; ++i) EXPECT_TRUE(RowPresent(i)) << "B row " << i;
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head) << "replay checkpoints at the end of the log";
}

// Natural stale-lap leftovers: the records of the previous lap that survive after the head of a wrapped
// ring (contrast ReplayDoesNotTrustBytesPastTheLastLsnMatchingRecord, which plants a CRC-valid foreign-LSN record).
TEST_F(WalReplayTest, ReplayStopsAtStaleLapRecord) {
    constexpr uint64_t kRing = 16 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    constexpr uint64_t kN = 400;   // ~108 B each: > 2 laps of 16 KiB
    std::shared_ptr<walring::Ring> ring;
    std::vector<uint64_t> heads;
    for (uint64_t i = 0; i < kN; ++i) {
        ASSERT_TRUE(InsertAt(store, i)) << i;
        if (!ring) ring = BusyRing();
        ASSERT_NE(ring, nullptr);
        heads.push_back(ring->HeadLsn());
        if (heads.size() > 4) ring->AdvanceCheckpoint(heads[heads.size() - 5]);   // keep the last 4 records live
    }
    const uint64_t head = ring->HeadLsn();
    ASSERT_GT(head, 2 * kRing);
    const uint64_t ckpt = ring->CheckpointLsn();
    ASSERT_EQ(ckpt, heads[kN - 5]);
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(kN);

    Crash();
    ZeroRows(0, kN);
    store.InitDB();

    for (uint64_t i = 0; i < kN - 4; ++i) ASSERT_FALSE(RowPresent(i)) << "row " << i << ": replay must start at the checkpoint and not run into stale-lap records";
    for (uint64_t i = kN - 4; i < kN; ++i) EXPECT_TRUE(RowPresent(i)) << "row " << i;
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head) << "replay must end exactly at the head, not at stale bytes past it";
}

TEST_F(WalReplayTest, ReplayAdvancesCheckpointOnlyAfterDataSync) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(5);
    Crash();

    Ckpt().FailNextDataSyncsForTests(1);
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u) << "the data sync failed: the checkpoint must not move";
    EXPECT_FALSE(Ckpt().Poisoned()) << "an injected failure stays retryable";

    store.InitDB();   // hook consumed: the same records replay again and now checkpoint
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
}

TEST_F(WalReplayTest, ReplayRealSyncErrorPoisons) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(5);
    Crash();

    Ckpt().FailNextDataSyncsStickyForTests(1);
    store.InitDB();
    EXPECT_TRUE(Ckpt().Poisoned()) << "a real sync error during replay is fsyncgate too";
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u);
    store.InitDB();   // no hooks left, but poisoned: still no checkpoint
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u);
}

// A real header error while committing a replay is fsyncgate too: the shard is fenced (as for any failed commit)
// AND the checkpointer is poisoned. A plain injected failure only fences.
TEST_F(WalReplayTest, ReplayCommitReplayRealErrorFencesAndPoisons) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();

    walring::FailNextHeaderWritesStickyForTests(1);
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u) << "the header write failed: the checkpoint must not move";
    EXPECT_TRUE(Ckpt().Poisoned()) << "a real header error during replay poisons";
    EXPECT_FALSE(InsertAt(store, 5)) << "the ring is fenced: appends would land on top of un-checkpointed records";

    Ckpt().ClearPoisonForTests();
    store.InitDB();   // unfence + retry
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
    EXPECT_TRUE(InsertAt(store, 5));
}

TEST_F(WalReplayTest, ReplayCommitReplayInjectedErrorFencesWithoutPoison) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();

    walring::FailNextHeaderWritesForTests(1);
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u);
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_FALSE(InsertAt(store, 5));
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
}

// M3: replay Notes files while last_writer_activity_ns is still 0 ("idle since forever"); without the boot hold the
// loop would cycle on the idle trigger during the replay.
TEST_F(WalReplayTest, CheckpointerDoesNotCycleOnIdleDuringReplay) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ring.reset();
    ShrinkDataFile(5);
    Crash();

    Ckpt().SetIdleMsForTests(1);
    Ckpt().SetMaxAgeMsForTests(1000000);
    Ckpt().SetReplaySyncDelayMsForTests(300);   // the replayed rows sit dirty for 300 ms before their sync
    Ckpt().SetPausedForTests(false);
    store.InitDB();
    EXPECT_EQ(Ckpt().ReplaySyncWindowCyclesForTests(), 0u) << "the idle trigger must not fire while ReplayWal runs";

    // The hold is released afterwards: the idle trigger works again.
    Ckpt().SetReplaySyncDelayMsForTests(0);
    ASSERT_TRUE(InsertAt(store, 5));
    auto live = BusyRing();
    ASSERT_NE(live, nullptr);
    EXPECT_TRUE(WaitFor([&] { return live->CheckpointLsn() == live->HeadLsn(); }, std::chrono::milliseconds(2000)));
}

// M5: un-checkpointed ring records of a removed DB must not replay into a DB re-created under the same name.
TEST_F(WalReplayTest, RemoveDbCheckpointsFirstSoReplayCannotCorruptARecreatedDb) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 10; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    ASSERT_EQ(ring->CheckpointLsn(), 0u) << "precondition: the DB's records are not checkpointed";

    (void)store.RemoveDB(db_name);
    EXPECT_EQ(ring->CheckpointLsn(), ring->HeadLsn()) << "RemoveDB must checkpoint before unlinking";

    row_length = 128;   // a different row length: replaying the old 64-byte rows would scribble over the new file
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 3));
    ring.reset();
    ShrinkDataFile(10);
    Crash();
    store.InitDB();

    std::string bytes((kEpoch0 + 11) * row_length, '\0');
    const int fd = open(YearFile().c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pread(fd, bytes.data(), bytes.size(), 0), static_cast<ssize_t>(bytes.size()));
    close(fd);
    const size_t keep_begin = (kEpoch0 + 3) * row_length;
    for (size_t i = 0; i < bytes.size(); ++i) {
        if (i >= keep_begin && i < keep_begin + row_length) continue;
        ASSERT_EQ(bytes[i], '\0') << "byte " << i << ": a removed DB's row was replayed into the re-created DB";
    }
    EXPECT_TRUE(RowPresent(3)) << "the new DB's own row survives";
}

// RemoveDB must not hang or throw when the checkpoint cannot be taken (poisoned checkpointer).
TEST_F(WalReplayTest, RemoveDbProceedsWhenTheCheckpointerIsPoisoned) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    Ckpt().FailNextDataSyncsStickyForTests(1);
    ASSERT_FALSE(Ckpt().RunCycle());
    ASSERT_TRUE(Ckpt().Poisoned());
    const auto rep = store.RemoveDB(db_name);
    EXPECT_NE(rep.removestatus().find("Removed DB"), std::string::npos) << rep.removestatus();
    EXPECT_FALSE(std::filesystem::exists(scratch->test_root / "DataStore" / db_name));
}

namespace {
// Legacy (pre-ring) record: [u32 recordLen][u32 crc][u16 dbLen][u16 rowLen][i32 res][u32 rows][db][u64 epoch + row]
std::string BuildLegacyRecordForTest(const std::string &db, uint16_t row_length, uint64_t epoch, double value) {
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(value);
    std::string row_bytes(row_length, '\0');
    row.SerializeToArray(row_bytes.data(), static_cast<int>(row.ByteSizeLong()));
    std::string payload;
    Put<uint16_t>(payload, static_cast<uint16_t>(db.size()));
    Put<uint16_t>(payload, row_length);
    Put<int32_t>(payload, static_cast<int32_t>(XStore_Proto::SECOND));
    Put<uint32_t>(payload, 1);
    payload += db;
    Put<uint64_t>(payload, epoch);
    payload += row_bytes;
    std::string rec;
    Put<uint32_t>(rec, static_cast<uint32_t>(4 + payload.size()));
    Put<uint32_t>(rec, Crc32c(payload.data(), payload.size()));
    return rec + payload;
}
std::string ReadWholeFile(const std::string &path) {
    std::string out;
    const int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return out;
    struct stat st{};
    if (fstat(fd, &st) == 0) {
        out.resize(static_cast<size_t>(st.st_size));
        EXPECT_EQ(pread(fd, out.data(), out.size(), 0), static_cast<ssize_t>(out.size()));
    }
    close(fd);
    return out;
}
}  // namespace

// Replay of a legacy record into a COLD fd cache preallocates the whole calendar year and the forced
// rebuild rescans it (~25 s). So the legacy record lives in 1970 and an ordinary insert into the same
// year warms the fd cache first; the year file is then cut down to the rows that matter.
class WalLegacyUpgradeTest : public WalReplayTest {
protected:
    static constexpr uint64_t kLegacyEpoch = 2000;
    std::string wal_path;
    uint32_t legacy_shard = 0;
    void SetUp() override {
        WalReplayTest::SetUp();
        row_length = 16;
    }
    void WriteLegacyFile(XStore &store) {
        xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
        // The first XStore of a process eagerly creates a ring file per active shard; start from a
        // clean registry and fresh writers so no Ring object survives over the file replaced below.
        Crash();
        ASSERT_TRUE(InsertAt(store, 500));   // warms the fd cache for 1970.XSTORE (and creates one ring file)
        ShrinkDataFile(kLegacyEpoch - kEpoch0);
        auto busy = BusyRing();
        ASSERT_NE(busy, nullptr);
        const std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
        const size_t shard = busy->ShardId() == 0 ? 1 : 0;   // any shard but the one holding the warm-up record
        busy.reset();
        wal_path = (wal_dir / ("shard_" + std::to_string(shard) + ".WAL")).string();
        std::filesystem::remove(wal_path);   // an eagerly created, unreferenced ring file
        legacy_shard = static_cast<uint32_t>(shard);
        const std::string rec = BuildLegacyRecordForTest(db_name, row_length, kLegacyEpoch, 77.5);
        const int fd = open(wal_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(write(fd, rec.data(), rec.size()), static_cast<ssize_t>(rec.size()));
        ASSERT_EQ(fsync(fd), 0);
        close(fd);
    }
    bool LegacyRowPresent(XStore &store) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(kLegacyEpoch, db_name, &q);
        return q.has_data() && q.data().fielddata_size() == 1 && q.data().fielddata(0).doubledata() == 77.5;
    }
    void ZeroLegacyRow() {
        const TimeStruct ts(kLegacyEpoch, XStore_Proto::SECOND);
        const std::string zeros(row_length, '\0');
        const int fd = open(fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(), ts.year).c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, zeros.data(), zeros.size(), static_cast<off_t>((kLegacyEpoch - ts.first_epoch) * row_length)), static_cast<ssize_t>(row_length));
        close(fd);
    }
};

TEST_F(WalLegacyUpgradeTest, LegacyWalIsReplayedOnceThenBecomesARing) {
    XStore store;
    ASSERT_NO_FATAL_FAILURE(WriteLegacyFile(store));
    {
        walring::RingHeader h;
        const int fd = open(wal_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        ASSERT_FALSE(walring::ReadBestHeader(fd, h)) << "precondition: legacy file has no ring header";
        close(fd);
    }
    store.InitDB();
    EXPECT_TRUE(LegacyRowPresent(store));
    const walring::RingHeader h = ReadHeader(wal_path);
    EXPECT_EQ(h.checkpoint_lsn, 0u);
    EXPECT_EQ(h.ring_bytes, walring::DefaultRingBytes());
    EXPECT_EQ(h.shard_id, legacy_shard);
    struct stat st{};
    ASSERT_EQ(stat(wal_path.c_str(), &st), 0);
    EXPECT_EQ(static_cast<uint64_t>(st.st_size), walring::HEADER_REGION_BYTES + h.ring_bytes);

    // Second start: nothing left to replay.
    ASSERT_NO_FATAL_FAILURE(ZeroLegacyRow());
    ASSERT_FALSE(LegacyRowPresent(store));
    store.InitDB();
    EXPECT_FALSE(LegacyRowPresent(store)) << "the legacy record must not be replayed twice";
}

TEST_F(WalLegacyUpgradeTest, LegacyWalNotConvertedWhenSyncFails) {
    XStore store;
    ASSERT_NO_FATAL_FAILURE(WriteLegacyFile(store));
    const std::string original = ReadWholeFile(wal_path);
    ASSERT_FALSE(original.empty());

    // Two failures: the ring file the warm-up insert created is replayed too (before or after the legacy
    // file, by shard index) and consumes one of them.
    Ckpt().FailNextDataSyncsForTests(2);
    store.InitDB();
    EXPECT_EQ(ReadWholeFile(wal_path), original) << "a legacy WAL whose replay is not durable must stay untouched";
    EXPECT_EQ(walring::GetOrOpenRing(wal_path, legacy_shard, /*reinit_legacy=*/true), nullptr)
        << "the writer's last resort must not wipe a legacy WAL that still has to be replayed";
    EXPECT_EQ(ReadWholeFile(wal_path), original);

    ASSERT_NO_FATAL_FAILURE(ZeroLegacyRow());
    store.InitDB();   // hook consumed: replays again and converts
    EXPECT_TRUE(LegacyRowPresent(store));
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u);
}

TEST_F(WalReplayTest, TornTailIsIgnored) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    std::shared_ptr<walring::Ring> ring;
    std::vector<uint64_t> heads;
    for (uint64_t i = 0; i < 5; ++i) {
        ASSERT_TRUE(InsertAt(store, i));
        if (!ring) ring = BusyRing();
        ASSERT_NE(ring, nullptr);
        heads.push_back(ring->HeadLsn());
    }
    const std::string wal_path = ring->path();
    const uint64_t ring_bytes = ring->ring_bytes();
    ring.reset();
    ShrinkDataFile(5);
    Crash();

    const int fd = open(wal_path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    char b;
    const off_t last_byte = static_cast<off_t>(walring::RingOffset(heads[4] - 1, ring_bytes));
    ASSERT_EQ(pread(fd, &b, 1, last_byte), 1);
    b ^= 0x5A;
    ASSERT_EQ(pwrite(fd, &b, 1, last_byte), 1);
    close(fd);
    ZeroRows(0, 5);

    store.InitDB();
    for (uint64_t i = 0; i < 4; ++i) EXPECT_TRUE(RowPresent(i)) << i;
    EXPECT_FALSE(RowPresent(4)) << "the torn final record must not be applied";
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, heads[3]) << "replay ends where the valid log ends";
}

TEST_F(WalReplayTest, ReplayRingUsesHeaderRingBytesNotProcessDefault) {
    constexpr uint64_t kRing = 256 << 10;
    row_length = 4096;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    constexpr uint64_t kN = 90;   // ~4.1 KiB each: wraps the 256 KiB ring
    std::shared_ptr<walring::Ring> ring;
    std::vector<uint64_t> heads;
    for (uint64_t i = 0; i < kN; ++i) {
        ASSERT_TRUE(InsertAt(store, i)) << i;
        if (!ring) ring = BusyRing();
        ASSERT_NE(ring, nullptr);
        heads.push_back(ring->HeadLsn());
        if (heads.size() > 4) ring->AdvanceCheckpoint(heads[heads.size() - 5]);
    }
    ASSERT_EQ(ring->ring_bytes(), kRing);
    const uint64_t head = ring->HeadLsn();
    ASSERT_GT(head, kRing) << "must wrap";
    ASSERT_GT(heads[kN - 5], kRing - 1) << "the checkpoint itself must already be in lap 2";
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(kN);
    Crash();
    walring::SetRingBytesForTests(0);   // the process default (8 MiB) differs from the file's 256 KiB
    ASSERT_NE(walring::DefaultRingBytes(), kRing);
    ZeroRows(0, kN);

    store.InitDB();
    for (uint64_t i = kN - 4; i < kN; ++i) EXPECT_TRUE(RowPresent(i)) << "row " << i;
    const walring::RingHeader h = ReadHeader(wal_path);
    EXPECT_EQ(h.ring_bytes, kRing);
    EXPECT_EQ(h.checkpoint_lsn, head);
}

TEST_F(WalReplayTest, RingContinuesAtEndLsnAfterReplay) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t end_lsn = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();
    ZeroRows(0, 5);
    store.InitDB();
    for (uint64_t i = 0; i < 5; ++i) ASSERT_TRUE(RowPresent(i)) << i;

    ASSERT_TRUE(InsertAt(store, 5));
    ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    EXPECT_EQ(ring->CheckpointLsn(), end_lsn);
    const RingWalk w = WalkRing(*ring, end_lsn, ring->HeadLsn());
    EXPECT_EQ(w.records, 1u) << "the next record must land exactly at end_lsn";
    EXPECT_EQ(w.end_lsn, ring->HeadLsn());
    ring.reset();

    Crash();
    ZeroRows(0, 6);
    store.InitDB();
    EXPECT_TRUE(RowPresent(5)) << "the new record replays";
    for (uint64_t i = 0; i < 5; ++i) EXPECT_FALSE(RowPresent(i)) << "row " << i << " is behind the checkpoint the first replay wrote";
}

TEST_F(WalReplayTest, ReplayDoesNotTrustBytesPastTheLastLsnMatchingRecord) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const uint64_t ring_bytes = ring->ring_bytes();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(60);
    Crash();

    // CRC-valid records that belong to another lap, right after the head.
    const std::string other_lap = BuildRecordV2ForTest(head + ring_bytes, db_name, row_length, {{kEpoch0 + 50, std::string(row_length, 'Z')}});
    const int fd = open(wal_path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, other_lap.data(), other_lap.size(), static_cast<off_t>(walring::RingOffset(head, ring_bytes))), static_cast<ssize_t>(other_lap.size()));
    close(fd);
    XStore::WalRecord sanity; size_t used = 0;
    ASSERT_EQ(XStore::ParseWalRecordV2(other_lap.data(), other_lap.size(), head + ring_bytes, used, sanity), XStore::RingParse::kOk);
    ZeroRows(0, 60);

    store.InitDB();
    for (uint64_t i = 0; i < 3; ++i) EXPECT_TRUE(RowPresent(i)) << i;
    EXPECT_FALSE(RowPresent(50)) << "a CRC-valid record with the wrong LSN must end replay";
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
}

// ---- Task 5 fix round 1 -------------------------------------------------------

namespace {
// Reads `len` ring bytes starting at `lsn` (no wrap handling: callers stay inside one lap).
std::string ReadRingBytes(const std::string &wal_path, uint64_t lsn, size_t len, uint64_t ring_bytes) {
    std::string out(len, 'x');
    const int fd = open(wal_path.c_str(), O_RDONLY);
    EXPECT_GE(fd, 0);
    EXPECT_EQ(pread(fd, out.data(), out.size(), static_cast<off_t>(walring::RingOffset(lsn, ring_bytes))), static_cast<ssize_t>(len));
    close(fd);
    return out;
}
}  // namespace

// A torn/damaged record N with an INTACT N+1 behind it (both unacknowledged): replay stops at N and
// checkpoints there. A new same-size record A then lands at N's position and the checkpoint reaches
// A's end -- exactly where the old N+1 starts with an LSN that matches. Replay must have zeroed it.
TEST_F(WalReplayTest, ReplayZeroesBytesPastTornRecordSoStaleSameLapRecordCannotReturn) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    std::vector<uint64_t> heads{0};
    std::shared_ptr<walring::Ring> ring;
    for (uint64_t i = 0; i < 3; ++i) {            // r0 = N-1, r1 = N, r2 = N+1 (identical sizes)
        ASSERT_TRUE(InsertAt(store, i));
        if (!ring) ring = BusyRing();
        ASSERT_NE(ring, nullptr);
        heads.push_back(ring->HeadLsn());
    }
    ASSERT_EQ(heads[2] - heads[1], heads[3] - heads[2]);
    ASSERT_EQ(heads[1] - heads[0], heads[2] - heads[1]);
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(5);
    Crash();

    // Tear N (flip a payload byte; CRC now fails) and leave N+1 intact.
    {
        const int fd = open(wal_path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        char b;
        const off_t at = static_cast<off_t>(walring::RingOffset(heads[1] + 20, kRing));
        ASSERT_EQ(pread(fd, &b, 1, at), 1);
        b ^= 0x5A;
        ASSERT_EQ(pwrite(fd, &b, 1, at), 1);
        close(fd);
    }
    ZeroRows(0, 5);
    const uint64_t zeroed_before = walring::ReplayZeroedBytesForTests();
    store.InitDB();
    EXPECT_TRUE(RowPresent(0));
    EXPECT_FALSE(RowPresent(1));
    EXPECT_FALSE(RowPresent(2));
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, heads[1]);
    EXPECT_EQ(walring::ReplayZeroedBytesForTests() - zeroed_before, kRing - heads[1]) << "everything past the damaged record, up to one lap";
    const std::string tail = ReadRingBytes(wal_path, heads[2], heads[3] - heads[2], kRing);
    EXPECT_EQ(tail, std::string(tail.size(), '\0')) << "the intact N+1 record must be gone";

    // A same-size record lands where N was; the checkpoint passes it (that also covers the old N+1's slot start).
    ASSERT_TRUE(InsertAt(store, 3));
    auto live = BusyRing();
    ASSERT_NE(live, nullptr);
    EXPECT_EQ(live->HeadLsn(), heads[2]);
    ASSERT_TRUE(Ckpt().RunCycle());
    ASSERT_EQ(live->CheckpointLsn(), heads[2]);
    live.reset();
    Crash();
    store.InitDB();
    EXPECT_FALSE(RowPresent(2)) << "N+1 was never acknowledged: it must not come back";
    EXPECT_TRUE(RowPresent(3)) << "A was acknowledged";
}

TEST_F(WalReplayTest, CleanEndDoesNotZeroTheRing) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    std::shared_ptr<walring::Ring> ring;
    for (uint64_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(InsertAt(store, i));
        if (!ring) ring = BusyRing();
    }
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(3);
    Crash();

    // A CRC-valid record of another lap right after the head is a CLEAN end (stale bytes): untouched.
    const std::string stale = BuildRecordV2ForTest(head + kRing, db_name, row_length, {{kEpoch0 + 2, std::string(row_length, 'Z')}});
    {
        const int fd = open(wal_path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, stale.data(), stale.size(), static_cast<off_t>(walring::RingOffset(head, kRing))), static_cast<ssize_t>(stale.size()));
        close(fd);
    }
    const uint64_t zeroed_before = walring::ReplayZeroedBytesForTests();
    store.InitDB();
    EXPECT_EQ(walring::ReplayZeroedBytesForTests(), zeroed_before) << "a clean end must not cost a ring write";
    EXPECT_EQ(ReadRingBytes(wal_path, head, stale.size(), kRing), stale) << "bytes past a clean end stay as they were";
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
}

TEST_F(WalReplayTest, FailedWalWriteZeroesItsRange) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 0));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();

    walring::FailNextWalSyncsForTests(1);
    EXPECT_FALSE(InsertAt(store, 1)) << "the failed WAL sync fails the insert";
    EXPECT_EQ(ring->HeadLsn(), head);
    const std::string failed_range = ReadRingBytes(wal_path, head, head, kRing);   // same size as record 0
    EXPECT_EQ(failed_range, std::string(failed_range.size(), '\0')) << "the unacknowledged record's bytes must be overwritten";

    ASSERT_TRUE(InsertAt(store, 2));
    ring.reset();
    ShrinkDataFile(3);
    Crash();
    ZeroRows(0, 3);
    store.InitDB();
    EXPECT_TRUE(RowPresent(0));
    EXPECT_FALSE(RowPresent(1)) << "the failed insert's record must not replay";
    EXPECT_TRUE(RowPresent(2));
}

TEST_F(WalReplayTest, ReplayApplyFailureDoesNotAdvanceCheckpointAndFencesTheShard) {
    if (geteuid() == 0) GTEST_SKIP() << "root bypasses file modes";
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();

    store.EvictFdForTests(YearFile());       // a new process has no cached fd
    ASSERT_EQ(chmod(YearFile().c_str(), 0), 0);
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u) << "a record that failed to apply must stay in the log";
    EXPECT_FALSE(InsertAt(store, 5)) << "the fenced shard must refuse appends until the next start";

    ASSERT_EQ(chmod(YearFile().c_str(), 0644), 0);
    Crash();                                  // restart: fence cleared
    store.WarmFdForTests(YearFile(), row_length, kEpoch0 + 4);   // keep the file small: a cold open regrows the year
    ZeroRows(0, 3);
    store.InitDB();
    for (uint64_t i = 0; i < 3; ++i) EXPECT_TRUE(RowPresent(i)) << i;
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
    EXPECT_TRUE(InsertAt(store, 5)) << "no fence after a successful replay";
}

TEST_F(WalReplayTest, ReplaySkipsRecordsForDeletedDbAndStillAdvances) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    Crash();

    store.EvictFdForTests(YearFile());
    std::filesystem::remove_all(scratch->test_root / "DataStore" / db_name);   // the DB was deleted before this start
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head) << "records of a deleted DB are skipped, not retried forever";
    EXPECT_NE(walring::GetOrOpenRing(wal_path, 0), nullptr) << "and the shard is not fenced";
}

TEST_F(WalReplayTest, ReplayAdvancesCheckpointOnlyAfterDirectorySync) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(3);
    Crash();

    Ckpt().FailNextDirSyncsForTests(1);
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u) << "the data file's directory entry is not durable yet";
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
}

// A non-empty file with no valid ring header whose first 4 KiB is not blank and which holds no parseable
// legacy record may be a ring with BOTH header slots damaged: it must never be wiped automatically.
TEST_F(WalReplayTest, BothHeaderSlotsCorruptRefusesToWipe) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    const uint32_t shard = ring->ShardId();
    ring.reset();
    ShrinkDataFile(3);
    Crash();

    const std::string original_header = [&] {
        std::string h(walring::HEADER_REGION_BYTES, '\0');
        const int fd = open(wal_path.c_str(), O_RDWR);
        EXPECT_GE(fd, 0);
        EXPECT_EQ(pread(fd, h.data(), h.size(), 0), static_cast<ssize_t>(h.size()));
        for (size_t slot = 0; slot < 2; ++slot) {   // flip a byte inside each slot's checksummed fields
            char b = h[slot * walring::HEADER_SLOT_BYTES + 20];
            b ^= 0x5A;
            EXPECT_EQ(pwrite(fd, &b, 1, static_cast<off_t>(slot * walring::HEADER_SLOT_BYTES + 20)), 1);
        }
        close(fd);
        return h;
    }();
    const std::string damaged = ReadWholeFile(wal_path);
    ZeroRows(0, 3);

    store.InitDB();
    EXPECT_EQ(ReadWholeFile(wal_path), damaged) << "a damaged-header ring must not be converted/wiped";
    EXPECT_EQ(walring::GetOrOpenRing(wal_path, shard, /*reinit_legacy=*/true), nullptr) << "and the writer must not wipe it either";

    // Operator repairs the header; the next start replays the records.
    {
        const int fd = open(wal_path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, original_header.data(), original_header.size(), 0), static_cast<ssize_t>(original_header.size()));
        close(fd);
    }
    Crash();
    store.InitDB();
    for (uint64_t i = 0; i < 3; ++i) EXPECT_TRUE(RowPresent(i)) << i;
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
}

TEST_F(WalLegacyUpgradeTest, LegacyWalNotConvertedWhenCheckpointerIsPoisoned) {
    XStore store;
    ASSERT_NO_FATAL_FAILURE(WriteLegacyFile(store));
    const std::string original = ReadWholeFile(wal_path);
    Ckpt().FailNextDataSyncsStickyForTests(1);
    store.InitDB();
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_EQ(ReadWholeFile(wal_path), original) << "a legacy WAL must stay legacy while replay cannot be made durable";
    store.InitDB();   // still poisoned: never converts
    EXPECT_EQ(ReadWholeFile(wal_path), original);
}

TEST_F(WalLegacyUpgradeTest, LegacyApplyFailureDoesNotConvertAndFences) {
    if (geteuid() == 0) GTEST_SKIP() << "root bypasses file modes";
    XStore store;
    ASSERT_NO_FATAL_FAILURE(WriteLegacyFile(store));
    const std::string original = ReadWholeFile(wal_path);
    store.EvictFdForTests(YearFile());
    ASSERT_EQ(chmod(YearFile().c_str(), 0), 0);
    store.InitDB();
    EXPECT_EQ(ReadWholeFile(wal_path), original) << "a legacy record that failed to apply must stay in the file";
    EXPECT_EQ(walring::GetOrOpenRing(wal_path, legacy_shard, /*reinit_legacy=*/true), nullptr);

    ASSERT_EQ(chmod(YearFile().c_str(), 0644), 0);
    store.WarmFdForTests(YearFile(), row_length, kLegacyEpoch + 4);   // keep the file small: a cold open regrows the year
    store.InitDB();   // fence retried from scratch: now it applies and converts
    EXPECT_TRUE(LegacyRowPresent(store));
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u);
}

// ---- Task 5 fix round 2 -------------------------------------------------------

namespace {
// Restores a directory's/file's mode on scope exit so a failing test cannot leave an unremovable scratch dir.
struct ModeGuard {
    std::string path;
    mode_t restore;
    ~ModeGuard() { chmod(path.c_str(), restore); }
};
}  // namespace

// Every replay exit that does not commit must fence the ring: the writer's Ring (opened at startup with
// head == checkpoint) would otherwise append on top of un-checkpointed records.
TEST_F(WalReplayTest, ReplayDataSyncFailureFencesTheShard) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();

    Ckpt().FailNextDataSyncsForTests(1);
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u);
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(InsertAt(store, 5)) << "appends would land on top of un-checkpointed records";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2)) << "must fail promptly, not stall";

    store.InitDB();   // unfence + retry
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
    EXPECT_TRUE(InsertAt(store, 5));
}

TEST_F(WalReplayTest, ReplayZeroingFailureFencesTheShard) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    std::vector<uint64_t> heads{0};
    std::shared_ptr<walring::Ring> ring;
    for (uint64_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(InsertAt(store, i));
        if (!ring) ring = BusyRing();
        heads.push_back(ring->HeadLsn());
    }
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();
    {   // tear record 2 (LSN field intact => a torn record of this lap)
        const int fd = open(wal_path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        char b;
        const off_t at = static_cast<off_t>(walring::RingOffset(heads[2] + 20, kRing));
        ASSERT_EQ(pread(fd, &b, 1, at), 1);
        b ^= 0x5A;
        ASSERT_EQ(pwrite(fd, &b, 1, at), 1);
        close(fd);
    }

    walring::FailNextReplayZeroingsForTests(1);
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u) << "the stale tail was not zeroed: the checkpoint must not move";
    EXPECT_FALSE(InsertAt(store, 5)) << "a stale same-lap record could come back if appends resumed";

    store.InitDB();   // unfence + retry: now zeroes and commits
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, heads[2]);
    EXPECT_TRUE(InsertAt(store, 5));
}

TEST_F(WalReplayTest, ReplayReadErrorFencesTheShard) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();

    // Every ring file's first read fails (the empty rings of the other shards are read first), so no
    // single-shot count can be aimed at this shard.
    walring::FailNextReplayReadsForTests(1000);
    store.InitDB();
    walring::FailNextReplayReadsForTests(0);
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u) << "a read error is not the end of the log";
    EXPECT_FALSE(InsertAt(store, 5));

    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
    EXPECT_TRUE(InsertAt(store, 5));
}

// EACCES on the DB directory is "cannot tell", never "deleted": the record must not be skipped.
TEST_F(WalReplayTest, ReplayApplyFailureOnUnsearchableDbDirFencesNotSkips) {
    if (geteuid() == 0) GTEST_SKIP() << "root bypasses file modes";
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    for (uint64_t i = 0; i < 3; ++i) ASSERT_TRUE(InsertAt(store, i));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head = ring->HeadLsn();
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(6);
    Crash();

    const std::string db_dir = (scratch->test_root / "DataStore" / db_name).string();
    ModeGuard restore{db_dir, 0755};
    store.EvictFdForTests(YearFile());
    ASSERT_EQ(chmod(db_dir.c_str(), 0), 0);
    store.InitDB();
    ASSERT_EQ(chmod(db_dir.c_str(), 0755), 0);
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u) << "an unsearchable DB directory is not a deleted DB";
    EXPECT_FALSE(InsertAt(store, 5));

    store.WarmFdForTests(YearFile(), row_length, kEpoch0 + 4);
    ZeroRows(0, 3);
    store.InitDB();
    for (uint64_t i = 0; i < 3; ++i) EXPECT_TRUE(RowPresent(i)) << i;
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
}

// After the ring has lapped, the bytes at the end of the log are mid-record leftovers of an older lap:
// a clean end, never zeroed (that would cost a ring write per shard on every start).
TEST_F(WalReplayTest, WrappedRingStaleBytesDoNotTriggerZeroing) {
    constexpr uint64_t kRing = 16 << 10;   // 16384 % 108 != 0: lap 2 records sit at shifted offsets
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    constexpr uint64_t kN = 400;
    std::shared_ptr<walring::Ring> ring;
    std::vector<uint64_t> heads;
    for (uint64_t i = 0; i < kN; ++i) {
        ASSERT_TRUE(InsertAt(store, i)) << i;
        if (!ring) ring = BusyRing();
        heads.push_back(ring->HeadLsn());
        if (heads.size() > 4) ring->AdvanceCheckpoint(heads[heads.size() - 5]);
    }
    const uint64_t head = ring->HeadLsn();
    ASSERT_GT(head, 2 * kRing);
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(kN);
    Crash();
    {   // precondition: stale non-zero bytes really sit at the end of the log
        uint32_t stale_len = 0;
        const std::string b = ReadRingBytes(wal_path, head, 4, kRing);
        std::memcpy(&stale_len, b.data(), 4);
        ASSERT_NE(stale_len, 0u);
    }
    const uint64_t zeroed_before = walring::ReplayZeroedBytesForTests();
    store.InitDB();
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, head);
    store.InitDB();
    store.InitDB();
    EXPECT_EQ(walring::ReplayZeroedBytesForTests(), zeroed_before) << "stale older-lap bytes are a clean end";
}

// Accepted trade-off, documented at the classification: a damaged record whose unchecked LSN field
// does not equal the expected LSN looks like stale bytes, not a torn record of this lap.
TEST_F(WalReplayTest, DamagedRecordWithForeignLsnFieldIsTreatedAsCleanEnd) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    std::vector<uint64_t> heads{0};
    std::shared_ptr<walring::Ring> ring;
    for (uint64_t i = 0; i < 3; ++i) {
        ASSERT_TRUE(InsertAt(store, i));
        if (!ring) ring = BusyRing();
        heads.push_back(ring->HeadLsn());
    }
    const std::string wal_path = ring->path();
    ring.reset();
    ShrinkDataFile(3);
    Crash();
    {
        const int fd = open(wal_path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        char b;
        const off_t at = static_cast<off_t>(walring::RingOffset(heads[2] + 9, kRing));   // inside the LSN field
        ASSERT_EQ(pread(fd, &b, 1, at), 1);
        b ^= 0x5A;
        ASSERT_EQ(pwrite(fd, &b, 1, at), 1);
        close(fd);
    }
    ZeroRows(0, 3);
    const uint64_t zeroed_before = walring::ReplayZeroedBytesForTests();
    store.InitDB();
    EXPECT_TRUE(RowPresent(0));
    EXPECT_TRUE(RowPresent(1));
    EXPECT_FALSE(RowPresent(2));
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, heads[2]);
    EXPECT_EQ(walring::ReplayZeroedBytesForTests(), zeroed_before);
}

// The failed append is a pad (ring tail) plus a record at the start of the next lap: both ranges are zeroed.
TEST_F(WalReplayTest, FailedWalWriteZeroesPadAndWrappedRange) {
    constexpr uint64_t kRing = 16 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 0));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t record_size = ring->HeadLsn();
    const std::string wal_path = ring->path();
    uint64_t i = 1;
    for (;; ++i) {   // fill to just short of the lap end, freeing space as we go
        ring->AdvanceCheckpoint(ring->HeadLsn());
        const uint64_t rem = kRing - ring->HeadLsn() % kRing;
        if (rem >= walring::RECORD_FIXED_BYTES && rem < record_size) break;
        ASSERT_LT(i, 400u);
        ASSERT_TRUE(InsertAt(store, i));
    }
    const uint64_t head = ring->HeadLsn();
    const uint64_t rem = kRing - head % kRing;
    ASSERT_NE(ReadRingBytes(wal_path, head + rem, record_size, kRing), std::string(record_size, '\0')) << "lap-1 record 0 sits where the wrapped record goes";

    walring::FailNextWalSyncsForTests(1);
    EXPECT_FALSE(InsertAt(store, i + 1));
    EXPECT_EQ(ring->HeadLsn(), head);
    EXPECT_EQ(ReadRingBytes(wal_path, head, rem, kRing), std::string(rem, '\0')) << "pad range";
    EXPECT_EQ(ReadRingBytes(wal_path, head + rem, record_size, kRing), std::string(record_size, '\0')) << "wrapped record range";
}

// ---- Task 5 fix round 3 -------------------------------------------------------

// A read error (not EOF) while scanning a legacy WAL must not end the scan as a "torn tail" and convert:
// the records behind the failed read would be wiped.
TEST_F(WalLegacyUpgradeTest, LegacyReadErrorFencesAndDoesNotConvert) {
    XStore store;
    ASSERT_NO_FATAL_FAILURE(WriteLegacyFile(store));
    const std::string original = ReadWholeFile(wal_path);

    walring::FailNextReplayReadsForTests(1000);   // every replay read fails (ring files and the legacy file)
    store.InitDB();
    walring::FailNextReplayReadsForTests(0);
    EXPECT_EQ(ReadWholeFile(wal_path), original) << "the legacy file must stay byte-identical";
    EXPECT_EQ(walring::GetOrOpenRing(wal_path, legacy_shard, /*reinit_legacy=*/true), nullptr) << "fenced: the writer must not wipe it";
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_FALSE(InsertAt(store, 5)) << "the DB's ring is fenced too (its read failed as well)";
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(2));

    store.InitDB();   // no hook: unfence, replay, convert
    EXPECT_TRUE(LegacyRowPresent(store));
    EXPECT_EQ(ReadHeader(wal_path).checkpoint_lsn, 0u);
    EXPECT_TRUE(InsertAt(store, 5));
}

// A fence placed by an earlier attempt must not outlive a replay that finds the file missing.
TEST_F(WalReplayTest, ReplayUnfencesShardFilesThatAreMissingThisTime) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    Crash();
    const std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::create_directories(wal_dir);
    const size_t shard = XStoreTunables::WAL_SHARD_COUNT - 1;
    const std::string path = (wal_dir / ("shard_" + std::to_string(shard) + ".WAL")).string();
    std::filesystem::remove(path);
    walring::FenceWalFile(path);
    ASSERT_EQ(walring::GetOrOpenRing(path, static_cast<uint32_t>(shard)), nullptr);
    store.InitDB();
    EXPECT_NE(walring::GetOrOpenRing(path, static_cast<uint32_t>(shard)), nullptr) << "the fence must not survive a replay that skips the file";
}

TEST(WalRingFormatTest, ParseMsKnobAcceptsOnlyPlainUnsignedDecimals) {
    unsigned v = 777;
    EXPECT_TRUE(walring::ParseMsKnob("0", v));          EXPECT_EQ(v, 0u);
    EXPECT_TRUE(walring::ParseMsKnob("3600000", v));    EXPECT_EQ(v, 3600000u);
    EXPECT_TRUE(walring::ParseMsKnob("4294967295", v)); EXPECT_EQ(v, 4294967295u);
    v = 777;
    for (const char *bad : {"", " 5", "5 ", "-0", "-5", "+5", "0x10", "5ms", "abc", "4294967296", "99999999999999999999999"}) {
        EXPECT_FALSE(walring::ParseMsKnob(bad, v)) << "'" << bad << "'";
    }
    EXPECT_FALSE(walring::ParseMsKnob(nullptr, v));
    EXPECT_EQ(v, 777u) << "a rejected value must leave the output alone";
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

// ---- Direct path for large records (spec §3.3) -------------------------------------------------

class WalDirectPathTest : public WalReplayTest {
protected:
    static constexpr uint64_t kBatchRows = 1000;                  // 1000 x (8 + 64) = 72000 B
    static constexpr uint64_t kOtherYearEpoch = 31536000 + 1000;   // 1971: a different data file

    void SetUp() override {
        WalReplayTest::SetUp();
        walring::SetDirectMinBytesForTests(32 << 10);
        walring::SetDirectPressureMinBytesForTests(16 << 10);
    }
    void TearDown() override {
        walring::SetDirectMinBytesForTests(0);
        walring::SetDirectPressureMinBytesForTests(0);
        walring::FailNextDirectSyncsForTests(0);
        WalReplayTest::TearDown();
    }

    static uint64_t DirectRecords() { return XStore::wal_direct_records.load(); }
    static uint64_t RingRecords() { return XStore::wal_ring_records.load(); }
    uint64_t TotalHead() const {
        uint64_t sum = 0;
        for (auto &r : MyRings()) sum += r->HeadLsn();
        return sum;
    }
    // One SYNC BatchInsert of `rows` rows at epoch first_epoch.. ; row i holds the double base + i.
    static std::string Batch(XStore &store, const std::string &db, uint64_t first_epoch, uint64_t rows, double base) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (uint64_t i = 0; i < rows; ++i) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(first_epoch + i);
            entry->mutable_data()->add_fielddata()->set_doubledata(base + static_cast<double>(i));
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        return rep.insertstatus();
    }
    static bool Failed(const std::string &status) { return status.rfind("[ERROR]", 0) == 0; }
    std::string BigBatch(XStore &store, double base) { return Batch(store, db_name, kEpoch0, kBatchRows, base); }
    double Read(XStore &store, uint64_t epoch) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(epoch, db_name, &q);
        EXPECT_TRUE(q.has_data()) << epoch;
        return q.has_data() ? q.data().fielddata(0).doubledata() : -1.0;
    }
};

TEST_F(WalDirectPathTest, ChooseRouteTable) {
    using walring::ChooseRoute;
    using walring::Route;
    walring::SetDirectMinBytesForTests(0);
    walring::SetDirectPressureMinBytesForTests(0);
    constexpr size_t kRing = 64u << 20;
    constexpr size_t k64K = 64u << 10, k1M = 1u << 20;
    //                                payload  bypass unary  pressure clear
    EXPECT_EQ(ChooseRoute(k64K,       false, false, false, true,  kRing), Route::kRing);
    EXPECT_EQ(ChooseRoute(k1M,        false, false, false, true,  kRing), Route::kDirect);
    EXPECT_EQ(ChooseRoute(k1M,        false, false, false, false, kRing), Route::kRing) << "ordering rule: not clear => ring";
    EXPECT_EQ(ChooseRoute(k64K,       false, false, true,  true,  kRing), Route::kDirect) << "pressure lowers the threshold";
    EXPECT_EQ(ChooseRoute(k64K,       false, false, true,  false, kRing), Route::kRing);
    EXPECT_EQ(ChooseRoute(8u << 20,   false, true,  false, true,  kRing), Route::kRing) << "unary never direct";
    EXPECT_EQ(ChooseRoute(8u << 20,   false, true,  true,  true,  8u << 20), Route::kRing) << "unary, even over half the ring";
    EXPECT_EQ(ChooseRoute(8u << 20,   true,  false, false, true,  kRing), Route::kRing) << "BYPASS never reaches the writer";
    EXPECT_EQ(ChooseRoute(kRing / 2 + 1, false, false, false, true,  kRing), Route::kDirect) << "over half the ring, clear";
    EXPECT_EQ(ChooseRoute(kRing / 2 + 1, false, false, false, false, kRing), Route::kRing) << "over half, not clear: the writer forces a checkpoint";
    EXPECT_EQ(ChooseRoute(k1M - 1,    false, false, false, true,  kRing), Route::kRing);
    EXPECT_EQ(ChooseRoute(k64K - 1,   false, false, true,  true,  kRing), Route::kRing);
    EXPECT_EQ(ChooseRoute(k64K,       false, false, true,  true,  kRing), Route::kDirect) << "exactly at the pressure threshold";
    EXPECT_EQ(ChooseRoute(k1M,        false, false, false, true,  kRing), Route::kDirect) << "exactly at the threshold";
    // The override setters feed the same decision (0 restores the tunable).
    walring::SetDirectMinBytesForTests(1000);
    EXPECT_EQ(ChooseRoute(1000, false, false, false, true, kRing), Route::kDirect);
    EXPECT_EQ(ChooseRoute(999,  false, false, false, true, kRing), Route::kRing);
    walring::SetDirectMinBytesForTests(0);
    EXPECT_EQ(walring::DirectMinBytes(), XStoreTunables::WAL_DIRECT_MIN_BYTES);
    EXPECT_EQ(walring::DirectPressureMinBytes(), XStoreTunables::WAL_DIRECT_PRESSURE_MIN_BYTES);
}

TEST_F(WalDirectPathTest, LargeBatchOnCleanFileGoesDirectAndIsSyncedBeforeAck) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    const uint64_t direct0 = DirectRecords(), head0 = TotalHead();
    const uint64_t data_syncs0 = Ckpt().DataSyncsForTests(), dir_syncs0 = Ckpt().DirSyncsForTests(), cycles0 = Ckpt().CyclesForTests();

    const std::string status = BigBatch(store, 100.0);
    ASSERT_FALSE(Failed(status)) << status;

    // Everything below is read right after the call returned and the checkpointer is paused, so the
    // syncs can only have come from the direct path, before the ack.
    EXPECT_EQ(DirectRecords(), direct0 + 1);
    EXPECT_EQ(TotalHead(), head0) << "no ring record for a direct write";
    EXPECT_GE(Ckpt().DataSyncsForTests(), data_syncs0 + 1);
    EXPECT_GE(Ckpt().DirSyncsForTests(), dir_syncs0 + 1);
    EXPECT_EQ(Ckpt().CyclesForTests(), cycles0);
    for (uint64_t i : {uint64_t{0}, kBatchRows / 2, kBatchRows - 1}) EXPECT_DOUBLE_EQ(Read(store, kEpoch0 + i), 100.0 + static_cast<double>(i));
    EXPECT_TRUE(RowPresent(0));
    EXPECT_TRUE(RowPresent(kBatchRows - 1));
}

TEST_F(WalDirectPathTest, DirectPathAlsoFsyncsParentDirectory) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    const uint64_t dir_syncs0 = Ckpt().DirSyncsForTests();
    const std::string status = BigBatch(store, 1.0);   // creates the year file
    ASSERT_FALSE(Failed(status)) << status;
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_syncs0 + 1) << "one distinct parent directory => one directory fsync";

    // And the directory fsync is on the ack path: failing it fails the caller.
    Ckpt().FailNextDirSyncsForTests(1);
    EXPECT_TRUE(Failed(BigBatch(store, 2.0)));
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_FALSE(Failed(BigBatch(store, 3.0)));
}

TEST_F(WalDirectPathTest, LargeBatchGoesThroughRingWhenSmallRecordPending) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 0));   // un-checkpointed ring record on this year's file
    const uint64_t direct0 = DirectRecords(), ring0 = RingRecords(), head0 = TotalHead();

    ASSERT_FALSE(Failed(Batch(store, db_name, kEpoch0 + 10, kBatchRows, 5.0)));
    EXPECT_EQ(DirectRecords(), direct0) << "ordering rule: the file has a pending ring record";
    EXPECT_EQ(RingRecords(), ring0 + 1);
    EXPECT_GT(TotalHead(), head0);

    ASSERT_TRUE(Ckpt().RunCycle());
    const uint64_t head1 = TotalHead();
    ASSERT_FALSE(Failed(Batch(store, db_name, kEpoch0 + 2000, kBatchRows, 7.0)));
    EXPECT_EQ(DirectRecords(), direct0 + 1) << "after the checkpoint passed it, the file is clear";
    EXPECT_EQ(TotalHead(), head1);
}

// The hazard: SYNC insert T=v1 (ring), then a large batch overwriting T=v2. If the batch went direct, a
// crash that loses the page cache replays the OLD ring record over the (synced) v2.
TEST_F(WalDirectPathTest, OverwriteOrderingSurvivesLostPageCache) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    constexpr uint64_t kT = 5;
    ASSERT_TRUE(InsertAt(store, kT));   // T = 6.0 (ring record, checkpointer paused)
    const std::string v1 = RowBytes(kT);
    const uint64_t direct0 = DirectRecords();

    ASSERT_FALSE(Failed(BigBatch(store, 100.0)));   // T = 105.0
    EXPECT_EQ(DirectRecords(), direct0) << "must take the ring path: the file has an un-checkpointed record";
    const std::string v2 = RowBytes(kT), v2_other = RowBytes(500);
    ASSERT_NE(v1, v2);

    BusyRing().reset();
    // Lost page cache: the data file reverts to its last checkpointed (synced) state. The file did not
    // exist at the last checkpoint, so that state is "all zeros"; the truncation keeps the sparse
    // year-sized file small so InitDB's index rebuild is quick (same as the other replay fixtures).
    ShrinkDataFile(kBatchRows);
    Crash();
    ZeroRows(0, kBatchRows);
    store.InitDB();

    EXPECT_EQ(RowBytes(kT), v2) << "ring replay applies v1 then v2 in log order";
    EXPECT_EQ(RowBytes(500), v2_other);
}

// A fenced shard's ring still holds un-checkpointed records that replay applied with no ring/dirty mark,
// so RingClear would say "clear" for their file: a direct write elsewhere would be undone by the next
// start's replay of the fenced ring. While anything is fenced, large records must take the ring.
TEST_F(WalDirectPathTest, FencedShardForcesLargeBatchesOntoTheRing) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 0));   // un-checkpointed ring record on this thread's shard
    BusyRing().reset();
    ShrinkDataFile(kBatchRows);
    Crash();
    Ckpt().FailNextDataSyncsForTests(1);
    store.InitDB();   // replay applies the record, its data sync fails => the shard is fenced
    ASSERT_TRUE(walring::AnyFenced());

    // A DB whose writes land on a healthy shard. (The shard is the DB's name hash plus a per-thread salt, and
    // thread ids hash alike here, so vary the DB.) Probe with a tiny batch; the fence is process-wide, so
    // the large batch to that DB must still not go direct.
    const uint64_t direct0 = DirectRecords();
    std::string healthy;
    for (int n = 0; n < 32 && healthy.empty(); ++n) {
        const std::string candidate = fmt::format("WALDIRHEALTHY{}", n);
        xstore_test::CreateTestDb(store, candidate, XStore_Proto::SECOND, row_length);
        if (!Failed(Batch(store, candidate, kOtherYearEpoch, 2, 1.0))) healthy = candidate;   // other year: 1970 stays clear
    }
    ASSERT_FALSE(healthy.empty()) << "no DB landed on a healthy shard";
    const std::string big = Batch(store, healthy, kEpoch0 + 100, kBatchRows, 200.0);
    EXPECT_FALSE(Failed(big)) << big;
    EXPECT_EQ(DirectRecords(), direct0) << "a fenced WAL file anywhere disables the direct path";
    XStore_Proto::UnaryQuery_REP q;
    store.UnaryQuery(kEpoch0 + 105, healthy, &q);
    ASSERT_TRUE(q.has_data());
    EXPECT_DOUBLE_EQ(q.data().fielddata(0).doubledata(), 205.0);

    // InitDB rebuilds dirty indexes by scanning each year file, which is year-sized (sparse): truncate the
    // healthy DB's files first, as ShrinkDataFile does for the primary one.
    for (const auto &[year, rows] : {std::pair<const char *, uint64_t>{"1970", kEpoch0 + kBatchRows + 200},
                                     std::pair<const char *, uint64_t>{"1971", 1100}}) {
        const std::string file = fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / healthy).string(), year);
        ASSERT_EQ(truncate(file.c_str(), static_cast<off_t>(rows * row_length)), 0) << file;
    }
    store.InitDB();   // unfence + successful replay
    ASSERT_FALSE(walring::AnyFenced());
    ASSERT_TRUE(Ckpt().RunCycle());
    ASSERT_FALSE(Failed(Batch(store, healthy, kEpoch0 + 2000, kBatchRows, 300.0)));
    EXPECT_EQ(DirectRecords(), direct0 + 1) << "unfenced and checkpointed clear: direct again";
}

TEST_F(WalDirectPathTest, OversizedRecordForcesCheckpointThenGoesDirect) {
    constexpr uint64_t kRing = 256 << 10;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 0));
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t head0 = ring->HeadLsn(), direct0 = DirectRecords(), cycles0 = Ckpt().CyclesForTests();
    ASSERT_EQ(ring->CheckpointLsn(), 0u);

    constexpr uint64_t kRows = 3000;   // 3000 x (8 + 64) = 216000 B > kRing / 2
    ASSERT_FALSE(Failed(Batch(store, db_name, kEpoch0 + 10, kRows, 0.5)));
    EXPECT_EQ(DirectRecords(), direct0 + 1);
    EXPECT_EQ(ring->HeadLsn(), head0) << "an oversized record never enters the ring";
    EXPECT_EQ(ring->CheckpointLsn(), head0) << "the pending record's checkpoint passed before the direct write";
    EXPECT_GT(Ckpt().CyclesForTests(), cycles0);
    for (uint64_t i : {uint64_t{0}, kRows / 2, kRows - 1}) EXPECT_DOUBLE_EQ(Read(store, kEpoch0 + 10 + i), 0.5 + static_cast<double>(i));
}

TEST_F(WalDirectPathTest, OversizedRecordFailsWhenTheFileCannotBeCheckpointedClear) {
    walring::SetRingBytesForTests(256 << 10);
    walring::SetStallTimeoutMsForTests(150);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 0));
    const uint64_t direct0 = DirectRecords();
    Ckpt().FailNextDataSyncsForTests(1000000);   // every forced cycle fails retryably

    EXPECT_TRUE(Failed(Batch(store, db_name, kEpoch0 + 10, 3000, 0.5)));
    EXPECT_EQ(DirectRecords(), direct0);
    EXPECT_FALSE(Ckpt().Poisoned());
    Ckpt().FailNextDataSyncsForTests(0);
    EXPECT_FALSE(Failed(Batch(store, db_name, kEpoch0 + 10, 3000, 0.5))) << "recovers once checkpoints can progress";
}

TEST_F(WalDirectPathTest, OversizedRecordWaitingForClearLeavesOnShutdown) {
    walring::SetRingBytesForTests(256 << 10);
    walring::SetStallTimeoutMsForTests(60000);   // the wait would otherwise outlast the test
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    ASSERT_TRUE(InsertAt(store, 0));
    Ckpt().FailNextDataSyncsForTests(1000000);   // forced cycles never succeed: the file never becomes clear

    std::string status;
    const auto t0 = std::chrono::steady_clock::now();
    std::thread worker([&] { status = Batch(store, db_name, kEpoch0 + 10, 3000, 0.5); });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    Crash();   // stops the writers
    worker.join();
    EXPECT_LT(std::chrono::steady_clock::now() - t0, std::chrono::seconds(20)) << "the writer must leave its forced-cycle loop on shutdown";
    EXPECT_TRUE(Failed(status));
}

TEST_F(WalDirectPathTest, DirectSyncFailureFailsTheCaller) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);

    walring::FailNextDirectSyncsForTests(1);
    EXPECT_TRUE(Failed(BigBatch(store, 10.0)));
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_FALSE(Failed(BigBatch(store, 10.0))) << "the failure was retryable";

    Ckpt().FailNextDataSyncsForTests(1);
    EXPECT_TRUE(Failed(BigBatch(store, 20.0)));
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_FALSE(Failed(BigBatch(store, 20.0)));
    EXPECT_DOUBLE_EQ(Read(store, kEpoch0 + 3), 23.0);
}

TEST_F(WalDirectPathTest, DirectPathRefusesWhenPoisoned) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    Ckpt().FailNextDataSyncsStickyForTests(1);   // like a real fdatasync error
    EXPECT_TRUE(Failed(BigBatch(store, 10.0))) << "a real sync error is never acknowledged";
    ASSERT_TRUE(Ckpt().Poisoned());

    const uint64_t direct0 = DirectRecords();
    EXPECT_TRUE(Failed(BigBatch(store, 20.0))) << "a fresh fd after a writeback error cannot be trusted";
    EXPECT_EQ(DirectRecords(), direct0) << "refused at decision time, nothing applied";
}

TEST_F(WalDirectPathTest, UnaryNeverGoesDirect) {
    walring::SetDirectMinBytesForTests(1);
    walring::SetDirectPressureMinBytesForTests(1);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    const uint64_t direct0 = DirectRecords(), ring0 = RingRecords(), head0 = TotalHead();
    ASSERT_TRUE(InsertAt(store, 0));
    EXPECT_EQ(DirectRecords(), direct0);
    EXPECT_EQ(RingRecords(), ring0 + 1);
    EXPECT_GT(TotalHead(), head0);
    // A one-row batch is unary-sized for the writer: also the ring.
    ASSERT_TRUE(Ckpt().RunCycle());
    ASSERT_FALSE(Failed(Batch(store, db_name, kEpoch0 + 50, 1, 1.0)));
    EXPECT_EQ(DirectRecords(), direct0);
}

TEST_F(WalDirectPathTest, RingPressureLowersTheThresholdEndToEnd) {
    walring::SetRingBytesForTests(256 << 10);
    walring::SetDirectMinBytesForTests(1u << 30);   // only pressure can make a batch direct
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    const uint64_t direct0 = DirectRecords();
    // Fill the ring past WAL_CKPT_CYCLE_FILL with records on ANOTHER year's file (this year stays clear).
    ASSERT_FALSE(Failed(Batch(store, db_name, kOtherYearEpoch, kBatchRows, 1.0)));
    ASSERT_FALSE(Failed(Batch(store, db_name, kOtherYearEpoch + 2000, kBatchRows, 1.0)));
    ASSERT_TRUE(walring::RingPressure());
    EXPECT_EQ(DirectRecords(), direct0) << "below the (huge) normal threshold: ring";

    ASSERT_FALSE(Failed(Batch(store, db_name, kEpoch0, 300, 9.0)));   // 21600 B >= pressure threshold (16 KiB)
    EXPECT_EQ(DirectRecords(), direct0 + 1);
}

// ---- Task 8: crash simulation (lost page cache) and concurrency ----------------------------------
//
// PAGE-CACHE-LOSS MODEL. A power loss keeps everything the kernel was told to make durable (fdatasync/fsync
// returned) and loses the rest of the page cache. Process death alone (what WalSimulateRestartForTests does) keeps
// the whole page cache, so on its own it cannot expose a missing data fdatasync. This fixture adds the loss:
//   * images_[file] = the first ImageBytes() bytes of a data file "as of the last moment the SYSTEM guaranteed them
//     durable". It is refreshed at exactly two kinds of moment:
//       (a) right after a successful Checkpointer::RunCycle() (the cycle fdatasyncs every dirty data file BEFORE it
//           advances any checkpoint), for every tracked file; and
//       (b) right after an acknowledged DIRECT-path write returned (it fdatasyncs the file before the ack), for that
//           file only. The ring-clear ordering rule guarantees the file had no pending ring record then, so its whole
//           content is durable (fdatasync flushes the whole file, not just the new range).
//       (c) after InitDB for every file: replay fdatasyncs the files it touched before it checkpoints.
//     Bytes pwritten by ring-path records since the last refresh are NOT in the image.
//   * LosePageCache(): WalSimulateRestartForTests() (writers/checkpointer stopped without a final cycle, in-memory
//     state dropped, the poison flag cleared like a fresh process), then each tracked data file is truncated/extended
//     to ImageBytes() and overwritten with its image (never-imaged file = all zeros). The WAL ring files are left
//     exactly as they are: records were fdatasynced before the ack, so they survive a power loss.
//   * Recover(): InitDB (replay from the persisted checkpoint_lsn; the stale, dirty-flagged .zmap/.catidx rebuild).
// What it cannot model: partial/torn page writeback (a real crash may persist SOME unsynced pages -- strictly easier
// than "none", but not exercised), reordering of unsynced pages among themselves, loss of a freshly created file's
// directory entry (file existence is treated as durable), durability of the index sidecars (they are always rebuilt
// after a crash here), and a crash in the middle of replay or of a checkpoint cycle. It trusts that fdatasync really
// reached stable storage; the tests tie the (a)/(b) moments to the production syncs via DataSyncsForTests().
class WalCrashSimTest : public WalDirectPathTest {
protected:
    // Rows live at epochs [kEpoch0, kEpoch0 + kSpanRows) of the 1970 file and at kOtherYearEpoch + [0, kSpanRows) of the 1971 file.
    static constexpr uint64_t kSpanRows = 4000;
    static constexpr uint64_t kYear1971Start = 31536000;
    static constexpr uint64_t kSmallBatchRows = 500;   // 500 x 72 = 36000 B > the 32 KiB direct threshold set by the base fixture

    std::map<std::string, std::string> images_;
    std::vector<std::string> files_;

    void TearDown() override {
        images_.clear();
        files_.clear();
        WalDirectPathTest::TearDown();
    }

    uint64_t ImageBytes() const { return (kEpoch0 + kSpanRows + 1) * row_length; }
    std::string Year1971File() const {
        return fmt::format("{}/{}.XSTORE", (scratch->test_root / "DataStore" / db_name).string(),
                           TimeStruct(kOtherYearEpoch, XStore_Proto::SECOND).year);
    }

    std::string ReadPrefix(const std::string &path) const {
        std::string bytes(ImageBytes(), '\0');
        const int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) return bytes;   // not created yet: durable image is "all zeros"
        size_t done = 0;
        while (done < bytes.size()) {
            const ssize_t n = pread(fd, bytes.data() + done, bytes.size() - done, static_cast<off_t>(done));
            if (n <= 0) break;       // past EOF reads as zero
            done += static_cast<size_t>(n);
        }
        close(fd);
        return bytes;
    }
    void Snapshot(const std::string &path) { images_[path] = ReadPrefix(path); }
    void SnapshotAll() { for (const auto &f : files_) Snapshot(f); }

    void LosePageCache() {
        Crash();
        Ckpt().ClearPoisonForTests();   // a restart is a fresh process: no sticky sync error
        for (const auto &f : files_) {
            const int fd = open(f.c_str(), O_WRONLY);
            if (fd < 0) continue;       // never created
            const auto it = images_.find(f);
            const std::string zeros(it == images_.end() ? ImageBytes() : 0, '\0');
            const std::string &img = it == images_.end() ? zeros : it->second;
            ASSERT_EQ(ftruncate(fd, static_cast<off_t>(ImageBytes())), 0);
            ASSERT_EQ(pwrite(fd, img.data(), img.size(), 0), static_cast<ssize_t>(img.size()));
            close(fd);
        }
    }
    void Recover(XStore &store) {
        store.InitDB();
        SnapshotAll();   // (c): replay fdatasynced what it touched before checkpointing
    }
    void PowerLoss(XStore &store) {
        LosePageCache();
        Recover(store);
    }

    bool Put(XStore &store, uint64_t epoch, double v) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(v);
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(epoch, row, db_name, &rep, XStore_Proto::SYNC);
        return !Failed(rep.insertstatus());
    }
    // Returns false if the row is absent.
    bool Lookup(XStore &store, const std::string &db, uint64_t epoch, double &v) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(epoch, db, &q);
        if (!q.has_data() || q.data().fielddata_size() != 1) return false;
        v = q.data().fielddata(0).doubledata();
        return true;
    }
    // Every acked (epoch -> value) must read back exactly. Reports the first few mismatches.
    void VerifyAll(XStore &store, const std::map<uint64_t, double> &acked, const std::string &when) {
        size_t bad = 0;
        for (const auto &kv : acked) {
            double v = 0;
            if (!Lookup(store, db_name, kv.first, v) || v != kv.second) {
                if (++bad <= 5) ADD_FAILURE() << when << ": acked row " << kv.first << " expected " << kv.second << " got " << (Lookup(store, db_name, kv.first, v) ? std::to_string(v) : std::string("<absent>"));
            }
        }
        EXPECT_EQ(bad, 0u) << when << ": " << bad << " of " << acked.size() << " acknowledged rows lost or stale";
    }

    // 5 rounds of: 200 acked rows, a successful checkpoint cycle (durable image), 200 more acked rows, power loss.
    void RunAckedRounds(uint64_t ring_bytes) {
        walring::SetRingBytesForTests(ring_bytes);
        XStore store;
        xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
        files_ = {YearFile()};

        std::map<uint64_t, double> acked;
        uint64_t next = 0;
        auto put_batch = [&](int round, uint64_t n) {
            for (uint64_t i = 0; i < n; ++i, ++next) {
                const double v = static_cast<double>(next) * 1.5 + static_cast<double>(round);
                ASSERT_TRUE(Put(store, kEpoch0 + next, v)) << "insert " << next;
                acked[kEpoch0 + next] = v;
            }
        };
        for (int round = 1; round <= 5; ++round) {
            put_batch(round, 200);
            ASSERT_FALSE(HasFailure());
            const uint64_t syncs0 = Ckpt().DataSyncsForTests();
            ASSERT_TRUE(Ckpt().RunCycle());
            ASSERT_GT(Ckpt().DataSyncsForTests(), syncs0) << "the cycle must have fdatasynced the dirty year file";
            SnapshotAll();                        // (a) durable image
            put_batch(round, 200);                // acked, but only in the WAL + page cache
            ASSERT_FALSE(HasFailure());
            LosePageCache();
            ASSERT_FALSE(RowPresent(next - 1)) << "the model must bite: the last acked row exists only in the WAL until replay";
            ASSERT_TRUE(RowPresent(next - 201)) << "...while the checkpointed batch is in the durable image";
            Recover(store);
            VerifyAll(store, acked, "after round " + std::to_string(round));
            if (HasFailure()) return;
        }
        if (ring_bytes < (1u << 20)) {
            EXPECT_GT(TotalHead(), 2 * ring_bytes) << "the small ring must have wrapped during the rounds";
        }
    }
};

TEST_F(WalCrashSimTest, AckedSyncRowsSurviveLostPageCache) { RunAckedRounds(8u << 20); }

TEST_F(WalCrashSimTest, AckedSyncRowsSurviveLostPageCacheWithWrappingRing) { RunAckedRounds(64u << 10); }

TEST_F(WalCrashSimTest, LargeDirectBatchSurvivesLostPageCache) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    files_ = {YearFile(), Year1971File()};
    std::map<uint64_t, double> acked;

    // A small ring record on the 1971 file: acked, never checkpointed, so only the WAL has it.
    ASSERT_TRUE(Put(store, kOtherYearEpoch, 4242.0));
    acked[kOtherYearEpoch] = 4242.0;
    const uint64_t head_small = TotalHead();
    ASSERT_GT(head_small, 0u);

    // A large batch on the (clear) 1970 file takes the direct path: fdatasync'd before the ack, no ring record.
    const uint64_t direct0 = DirectRecords(), data_syncs0 = Ckpt().DataSyncsForTests();
    ASSERT_FALSE(Failed(BigBatch(store, 500.0)));
    ASSERT_EQ(DirectRecords(), direct0 + 1) << "must be the direct path";
    ASSERT_GT(Ckpt().DataSyncsForTests(), data_syncs0) << "the ack came after a data fdatasync (checkpointer is paused)";
    ASSERT_EQ(TotalHead(), head_small) << "a direct write appends nothing to the ring";
    Snapshot(YearFile());                         // (b) durable at the ack; the 1971 file is NOT imaged
    for (uint64_t i = 0; i < kBatchRows; ++i) acked[kEpoch0 + i] = 500.0 + static_cast<double>(i);

    LosePageCache();
    for (uint64_t i = 0; i < kBatchRows; ++i) ASSERT_TRUE(RowPresent(i)) << "direct batch row " << i << " must survive WITHOUT replay";
    {   // The small record's data lived only in the page cache of the 1971 file: gone until replay.
        const int fd = open(Year1971File().c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        std::string row(row_length, 'x');
        ASSERT_EQ(pread(fd, row.data(), row.size(), static_cast<off_t>(kEpoch0 * row_length)), static_cast<ssize_t>(row.size()));
        close(fd);
        EXPECT_EQ(row, std::string(row_length, '\0')) << "the un-checkpointed ring row must have been lost with the page cache";
    }
    Recover(store);
    VerifyAll(store, acked, "after recovery");
    EXPECT_EQ(DirectRecords(), direct0 + 1);
}

TEST_F(WalCrashSimTest, RepeatedOverwritesOfOneRowKeepTheNewestValueAfterLostPageCache) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    files_ = {YearFile()};
    constexpr uint64_t kT = kEpoch0 + 7;
    double last = 0;
    for (int k = 1; k <= 50; ++k) {
        last = 100.0 + k;
        ASSERT_TRUE(Put(store, kT, last)) << k;
        if (k % 10 == 0 && k < 50) {   // durable images hold 110, 120, 130, 140; 141..150 exist only in the WAL
            ASSERT_TRUE(Ckpt().RunCycle());
            SnapshotAll();
        }
    }
    PowerLoss(store);
    double v = 0;
    ASSERT_TRUE(Lookup(store, db_name, kT, v));
    EXPECT_EQ(v, last) << "replay of records 141..150 in log order must leave the LAST acked value";
}

TEST_F(WalCrashSimTest, CrashStormNeverLosesAnAckedRow) {
    walring::SetRingBytesForTests(2u << 20);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    files_ = {YearFile(), Year1971File()};
    std::mt19937_64 rng(0xC0FFEE);
    std::map<uint64_t, double> acked;
    double counter = 1.0;
    size_t direct_batches = 0, ring_batches = 0, cycles = 0, small = 0;
    const auto epoch_of = [](bool y71, uint64_t idx) { return (y71 ? kYear1971Start : uint64_t{0}) + kEpoch0 + idx; };
    for (int round = 1; round <= 30; ++round) {
        const unsigned ops = 5 + static_cast<unsigned>(rng() % 20);
        for (unsigned op = 0; op < ops; ++op) {
            const unsigned dice = static_cast<unsigned>(rng() % 100);
            const bool y71 = rng() & 1;
            const uint64_t idx = rng() % (kSpanRows - kSmallBatchRows);
            if (dice < 75) {
                const double v = counter++;
                ASSERT_TRUE(Put(store, epoch_of(y71, idx), v));
                acked[epoch_of(y71, idx)] = v;
                ++small;
            } else if (dice < 83) {
                ASSERT_TRUE(Ckpt().RunCycle());
                SnapshotAll();
                ++cycles;
            } else {
                const uint64_t direct0 = DirectRecords();
                const double base = counter;
                counter += static_cast<double>(kSmallBatchRows);
                const std::string status = Batch(store, db_name, epoch_of(y71, idx), kSmallBatchRows, base);
                ASSERT_FALSE(Failed(status)) << status;
                for (uint64_t i = 0; i < kSmallBatchRows; ++i) acked[epoch_of(y71, idx) + i] = base + static_cast<double>(i);
                if (DirectRecords() > direct0) {
                    Snapshot(y71 ? Year1971File() : YearFile());   // (b)
                    ++direct_batches;
                } else {
                    ++ring_batches;
                }
            }
        }
        PowerLoss(store);
        VerifyAll(store, acked, "storm round " + std::to_string(round));
        if (HasFailure()) return;
    }
    EXPECT_GT(direct_batches, 0u);
    EXPECT_GT(ring_batches, 0u);
    EXPECT_GT(cycles, 0u);
    EXPECT_GT(small, 0u);
}

TEST_F(WalCrashSimTest, PowerLossWithPoisonedCheckpointerReplaysFromLastGoodCheckpoint) {
    walring::SetRingBytesForTests(256 << 10);
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    files_ = {YearFile()};
    std::map<uint64_t, double> acked;
    uint64_t next = 0;
    const auto put = [&](uint64_t n) {
        for (uint64_t i = 0; i < n; ++i, ++next) {
            ASSERT_TRUE(Put(store, kEpoch0 + next, static_cast<double>(next) + 0.25)) << next;
            acked[kEpoch0 + next] = static_cast<double>(next) + 0.25;
        }
    };
    put(100);
    ASSERT_TRUE(Ckpt().RunCycle());
    SnapshotAll();                                 // the last GOOD checkpoint
    auto ring = BusyRing();
    ASSERT_NE(ring, nullptr);
    const uint64_t good_ckpt = ring->CheckpointLsn();
    ASSERT_EQ(good_ckpt, ring->HeadLsn());

    put(50);
    Ckpt().FailNextDataSyncsStickyForTests(1);     // a real fdatasync error: dirty pages may be dropped by the kernel
    ASSERT_FALSE(Ckpt().RunCycle());
    ASSERT_TRUE(Ckpt().Poisoned());
    put(50);                                       // still acked through the ring (far below the stall limit)
    EXPECT_EQ(ring->CheckpointLsn(), good_ckpt) << "a failed sync must never let the checkpoint pass unsynced data";
    EXPECT_GT(ring->HeadLsn(), good_ckpt);
    ring.reset();

    LosePageCache();
    ASSERT_TRUE(RowPresent(99));
    ASSERT_FALSE(RowPresent(100)) << "rows acked after the last good checkpoint exist only in the WAL until replay";
    ASSERT_FALSE(RowPresent(199));
    Recover(store);
    VerifyAll(store, acked, "after poisoned power loss");
}

TEST_F(WalCrashSimTest, ConcurrentWritersAndCheckpointerStayConsistent) {
    constexpr uint64_t kRing = 256 << 10;
    constexpr int kThreads = 8, kOps = 500;
    walring::SetRingBytesForTests(kRing);
    XStore store;
    const std::string dbs[2] = {"CRASHCONCA", "CRASHCONCB"};
    constexpr short kWideRow = 256;                // ~300 B ring records so 4800 rows wrap a 256 KiB ring
    for (const auto &db : dbs) xstore_test::CreateTestDb(store, db, XStore_Proto::SECOND, kWideRow);
    Ckpt().SetPausedForTests(false);               // the REAL checkpointer

    const auto value_of = [](uint64_t epoch) { return static_cast<double>(epoch) + 0.5; };
    std::atomic<int> failures{0};
    std::atomic<bool> done{false};
    std::atomic<uint64_t> violations{0}, max_gap{0}, samples{0};
    std::thread monitor([&] {
        while (!done.load(std::memory_order_acquire)) {
            for (const auto &ring : walring::AllRings()) {
                const uint64_t ckpt = ring->CheckpointLsn();   // read first: head only grows, so the gap can only be over-estimated
                const uint64_t head = ring->HeadLsn();
                const uint64_t gap = head - ckpt;
                if (head < ckpt || gap > ring->ring_bytes()) violations.fetch_add(1, std::memory_order_relaxed);
                uint64_t prev = max_gap.load(std::memory_order_relaxed);
                while (gap > prev && !max_gap.compare_exchange_weak(prev, gap, std::memory_order_relaxed)) {}
                samples.fetch_add(1, std::memory_order_relaxed);
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    std::vector<std::thread> writers;
    for (int t = 0; t < kThreads; ++t) {
        writers.emplace_back([&, t] {
            const std::string &db = dbs[t % 2];
            for (int i = 0; i < kOps; ++i) {
                const uint64_t ts = kEpoch0 + static_cast<uint64_t>(t) * 5000 + static_cast<uint64_t>(i) * 4;
                if (i % 10 == 9) {   // a 3-row SYNC batch
                    if (Failed(Batch(store, db, ts, 3, value_of(ts)))) failures.fetch_add(1);
                } else {
                    XStore_Proto::RowData row;
                    row.add_fielddata()->set_doubledata(value_of(ts));
                    XStore_Proto::UnaryInsert_REP rep;
                    store.UnaryInsert(ts, row, db, &rep, XStore_Proto::SYNC);
                    if (Failed(rep.insertstatus())) failures.fetch_add(1);
                }
            }
        });
    }
    for (auto &w : writers) w.join();
    done.store(true, std::memory_order_release);
    monitor.join();

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(violations.load(), 0u) << "HeadLsn - CheckpointLsn exceeded the ring size (max observed gap " << max_gap.load() << ")";
    EXPECT_GT(samples.load(), 0u);
    size_t rows = 0, missing = 0;
    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kOps; ++i) {
            const uint64_t ts = kEpoch0 + static_cast<uint64_t>(t) * 5000 + static_cast<uint64_t>(i) * 4;
            for (uint64_t j = 0; j < (i % 10 == 9 ? 3u : 1u); ++j, ++rows) {
                double v = 0;
                if (!Lookup(store, dbs[t % 2], ts + j, v) || v != value_of(ts + j)) ++missing;
            }
        }
    }
    EXPECT_EQ(missing, 0u) << "of " << rows << " acknowledged rows";
    uint64_t max_head = 0;
    for (const auto &r : walring::AllRings()) max_head = std::max(max_head, r->HeadLsn());
    EXPECT_GT(max_head, kRing) << "at least one ring must have wrapped, so the checkpointer really ran concurrently";

    {
        ShutdownReset reset;
        XStore::Shutdown();
        for (const auto &r : walring::AllRings()) {
            EXPECT_EQ(r->CheckpointLsn(), r->HeadLsn()) << r->path();
            EXPECT_EQ(OnDiskCheckpoint(*r), r->HeadLsn()) << r->path();
        }
    }
}

TEST_F(WalCrashSimTest, EveryInsertApiMarksItsFileDirty) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, row_length);
    const std::string path = YearFile();
    enum class Api { kUnary, kBatch, kRange };
    int slot = 0;
    for (const Api api : {Api::kUnary, Api::kBatch, Api::kRange}) {
        for (const auto mode : {XStore_Proto::SYNC, XStore_Proto::ASYNC}) {
            const uint64_t first = kEpoch0 + 100 * static_cast<uint64_t>(++slot);
            const uint64_t rows = api == Api::kUnary ? 1 : 3;
            (void)walring::Dirty().Take();
            if (api == Api::kUnary) {
                XStore_Proto::RowData row;
                row.add_fielddata()->set_doubledata(1.0);
                XStore_Proto::UnaryInsert_REP rep;
                store.UnaryInsert(first, row, db_name, &rep, mode);
                ASSERT_FALSE(Failed(rep.insertstatus())) << rep.insertstatus();
            } else if (api == Api::kBatch) {
                XStore_Proto::BatchInsert_REQ req;
                req.set_dbname(db_name);
                req.set_durabilitymode(mode);
                for (uint64_t i = 0; i < rows; ++i) {
                    auto *e = req.add_batchinsert_data();
                    e->set_timestamp(first + i);
                    e->mutable_data()->add_fielddata()->set_doubledata(2.0);
                }
                XStore_Proto::BatchInsert_REP rep;
                store.BatchInsert(&req, &rep);
                ASSERT_FALSE(Failed(rep.insertstatus())) << rep.insertstatus();
            } else {
                XStore_Proto::RangeInsert_REQ req;
                req.set_dbname(db_name);
                req.set_durabilitymode(mode);
                for (uint64_t i = 0; i < rows; ++i) {
                    auto *e = req.add_rangeinsert_data();
                    e->set_timestamp(first + i);
                    e->mutable_data()->add_fielddata()->set_doubledata(3.0);
                }
                XStore_Proto::RangeInsert_REP rep;
                store.RangeInsert(&req, &rep);
                ASSERT_FALSE(Failed(rep.insertstatus())) << rep.insertstatus();
            }
            const auto covered = [&] {
                const auto snap = walring::Dirty().PeekRanges();
                const auto it = snap.ranges.find(path);
                if (it == snap.ranges.end()) return false;
                for (uint64_t i = 0; i < rows; ++i) {
                    const uint64_t off = (first + i) * row_length;   // 1970: first_epoch == 0
                    if (!Covers(it->second, off, off + row_length)) return false;
                }
                return true;
            };
            const char *name = api == Api::kUnary ? "Unary" : api == Api::kBatch ? "Batch" : "Range";
            if (mode == XStore_Proto::SYNC) {
                EXPECT_TRUE(covered()) << name << " SYNC: every written row must be in the dirty ranges once the call returned";
            } else {
                EXPECT_TRUE(WaitFor(covered, std::chrono::milliseconds(2000))) << name << " ASYNC: rows never appeared in the dirty ranges";
                // The dirty note precedes the writer's zone-map/catidx update: wait for the whole record to be applied,
                // or TearDown's registry clear would destroy those indexes under the still-running writer (TSAN-visible).
                EXPECT_TRUE(WaitFor([&] {
                    for (const auto &r : MyRings()) if (r->AppliedLsn() != r->HeadLsn()) return false;
                    return true;
                }, std::chrono::milliseconds(2000))) << name << " ASYNC: record never finished applying";
            }
        }
    }
}

}  // namespace
