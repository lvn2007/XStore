// How a range scan holds its READ range lock (include/XStore/XStore_ScanLock.h): one chunk at a time, not the whole requested
// range for the whole scan. Every scan op (the exact aggregates, RangeQuery, FindByValue, FindByPattern) must give the same
// answer chunked and whole, let a WRITE to a part of the range it is not in get in while it is under way, and send its output
// to the client with no lock held.
#include <gtest/gtest.h>
#include "XStore/XStore.h"
#include "XStore/XStore_ScanLock.h"
#include "DataStructs/FileRangeLock.h"
#include "include/DataStructs/TimeStruct.h"
#include "XStore.pb.h"
#include "XStoreTestSupport.h"
#include <chrono>
#include <filesystem>
#include <functional>
#include <future>
#include <string>

namespace {

using namespace xstore_test;
using namespace std::chrono_literals;

constexpr uint16_t kRowLength = 512;              // a zone-map block is 2 MiB / 512 = 4096 rows: the 20,000 rows are 5 blocks
constexpr uint64_t kRows = 20'000;
constexpr uint64_t kBlockBytes = 4096ull * kRowLength;

// What each op returns for the data below, as a number the tests can compare: a count of rows or distinct values.
struct Op {
    const char *name;
    std::function<uint64_t(XStore &, const std::string &db, unsigned long long base)> run;
    uint64_t expected;
};

std::string StringOf(uint64_t i) { return "row-" + std::to_string(i); }

class ScanLockTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    const std::string db = "SCANLOCK";
    unsigned long long base = 0;
    std::string year_path;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_scan_lock_test");
        store = std::make_unique<XStore>();
        CreateTestDb(*store, db, XStore_Proto::SECOND, kRowLength);
        TimeStruct ref(static_cast<std::time_t>(1700000000ULL), XStore_Proto::SECOND);
        base = static_cast<unsigned long long>(ref.first_epoch);
        for (uint64_t first = 0; first < kRows; first += 5000) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(db);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (uint64_t i = first; i < first + 5000; ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(base + i);
                entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(i));
                entry->mutable_data()->add_fielddata()->set_stringdata(StringOf(i));
            }
            XStore_Proto::BatchInsert_REP rep;
            store->BatchInsert(&req, &rep);
        }
        year_path = (scratch->test_root / "DataStore" / db / (std::to_string(TimeStruct(base, XStore_Proto::SECOND).year) + ".XSTORE")).string();
    }
    void TearDown() override {
        xstore::scanlock::Set(xstore::scanlock::Config{});
        xstore::scanlock::SetChunkHookForTests(nullptr);
        store.reset();
        scratch.reset();
    }

    static std::vector<Op> Ops() {
        return {
            {"DistinctCount", [](XStore &s, const std::string &db, unsigned long long base) {
                 XStore_Proto::DistinctCount_REP rep;
                 s.DistinctCountAggregate(base, base + kRows - 1, db, 0, &rep);   // an exact aggregate reads every value
                 return rep.count();
             }, kRows},
            {"RangeQuery", [](XStore &s, const std::string &db, unsigned long long base) {
                 return static_cast<uint64_t>(RangeQuery(s, base, base + kRows - 1, db).queryresponse_size());
             }, kRows},
            {"FindByValue", [](XStore &s, const std::string &db, unsigned long long base) {
                 std::string buf;
                 s.FindByValue(base, base + kRows - 1, db, 0, FDouble(12345.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                               [&](std::string &&chunk, bool) { buf += chunk; });
                 XStore_Proto::RangeQuery_REP rep;
                 rep.ParseFromString(buf);
                 return static_cast<uint64_t>(rep.queryresponse_size());
             }, 1},
            {"FindByPattern", [](XStore &s, const std::string &db, unsigned long long base) {
                 std::string buf;
                 // row-1... : the rows 1, 10-19, 100-199, 1000-1999, 10000-19999 = 1 + 10 + 100 + 1000 + 10000
                 s.FindByPattern(base, base + kRows - 1, db, 1, "row-1%", XStore_Proto::PATTERN_LIKE, false,
                                 XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES, [&](std::string &&chunk, bool) { buf += chunk; });
                 XStore_Proto::RangeQuery_REP rep;
                 rep.ParseFromString(buf);
                 return static_cast<uint64_t>(rep.queryresponse_size());
             }, 11'111},
        };
    }
};

}  // namespace

TEST_F(ScanLockTest, EveryScanOpGivesTheSameAnswerWholeAndChunked) {
    for (const uint64_t chunk_bytes : {uint64_t{0}, kBlockBytes, 3 * kBlockBytes, uint64_t{4} << 20}) {
        xstore::scanlock::Set({chunk_bytes});
        for (const Op &op : Ops()) {
            EXPECT_EQ(op.run(*store, db, base), op.expected) << op.name << " with chunks of " << chunk_bytes << " bytes";
        }
    }
}

TEST_F(ScanLockTest, AWriterToAnUnscannedChunkGetsInMidScanForEveryScanOp) {
    FileRangeLock &lock = FileLockRegistry::Get(year_path);
    xstore::scanlock::Set({kBlockBytes});   // one block per chunk: 5 chunks
    for (const Op &op : Ops()) {
        SCOPED_TRACE(op.name);
        std::future<void> writer;
        bool writer_done_by_chunk_2 = false;
        uint64_t last_chunk = 0;
        xstore::scanlock::SetChunkHookForTests([&](uint64_t chunk) {
            last_chunk = chunk;
            if (chunk == 0) {
                // A WRITE to the last rows of the range, which the scan has not reached.
                writer = std::async(std::launch::async, [&] {
                    lock.LockRange((kRows - 10) * kRowLength, 10 * kRowLength, FileRangeLock::LockType::WRITE);
                    lock.UnlockRange((kRows - 10) * kRowLength, 10 * kRowLength, FileRangeLock::LockType::WRITE);
                });
                writer.wait_for(2s);
            }
            if (chunk == 2) writer_done_by_chunk_2 = writer.wait_for(0ms) == std::future_status::ready;
        });
        EXPECT_EQ(op.run(*store, db, base), op.expected);
        xstore::scanlock::SetChunkHookForTests(nullptr);
        ASSERT_TRUE(writer.valid());
        writer.get();
        EXPECT_GE(last_chunk, 3u) << "the scan was not chunked";
        EXPECT_TRUE(writer_done_by_chunk_2) << "the writer was still held off two chunks after it asked";
    }
}

// A RangeQuery sends its output to the client while it scans. The client may be slow, so the send must not hold a lock: from the
// output callback, a WRITE to the rows already scanned (and to the ones not yet reached) must be granted at once.
TEST_F(ScanLockTest, RangeQuerySendsItsOutputWithNoLockHeld) {
    FileRangeLock &lock = FileLockRegistry::Get(year_path);
    xstore::scanlock::Set({kBlockBytes});
    int sends = 0;
    int writes_granted = 0;
    std::string buf;
    store->RangeQuery(base, base + kRows - 1, db, 1,   // a one-byte target: every completed row is sent
                      [&](std::string &&chunk, bool) {
                          buf += chunk;
                          if (++sends % 3000 != 1) return;   // sample the callback a few times across the scan
                          for (const unsigned long long row : {uint64_t{5}, kRows - 5}) {
                              if (lock.TryLockRangeFor(row * kRowLength, kRowLength, FileRangeLock::LockType::WRITE, 1s)) {
                                  ++writes_granted;
                                  lock.UnlockRange(row * kRowLength, kRowLength, FileRangeLock::LockType::WRITE);
                              }
                          }
                      });
    EXPECT_GE(sends, 3000);
    EXPECT_EQ(writes_granted, 2 * ((sends + 2999) / 3000)) << "a write was refused while the client was being sent output";
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buf));
    EXPECT_EQ(rep.queryresponse_size(), static_cast<int>(kRows));
}
