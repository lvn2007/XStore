// WAL ring + paced checkpoint (Durability C), Task 1: ring file header
// slots, append planning, PAD records and the v2 (LSN-carrying) record
// parser. Pure format tests -- nothing here touches the running WAL path.
#include <gtest/gtest.h>
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_Crc32c.h"
#include "include/XStore/XStore_WalRing.h"
#include <cstdlib>
#include <cstring>
#include <filesystem>
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

}  // namespace
