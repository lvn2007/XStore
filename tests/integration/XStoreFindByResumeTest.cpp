// FindByValue and FindByPattern that can stop at a scan-chunk (sequential) or piece (parallel) boundary and continue later
// (docs/superpowers/specs/2026-11-01-park-resume-replies-design.md): XStore::FindByValueRun / FindByPatternRun. Parking anywhere must not change a byte of
// the reply, including the status text (the notices a run adds are not repeated by the next one), and a DB removed meanwhile ends the reply with an error.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_ScanLock.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/DataStructs/TimeStruct.h"
#include <functional>
#include <string>

using namespace xstore_test;

namespace {
constexpr short kRowLength = 512;   // a zone-map block (the smallest scan chunk or piece) is 2 MiB / 512 = 4096 rows
constexpr uint64_t kRows = 20000;   // 5 blocks, a fifth of the rows match
constexpr size_t kReplyChunkBytes = 16 * 1024;
const std::string kDb = "FBRESUME";
// The rows FindByValue finds in column 2: the first row of every zone-map block (where a scan chunk or piece starts, so a resume that loses its first
// row is noticed) and a fifth of the others.
bool IsMatch(uint64_t i) { return i % 4096 == 0 || i % 5 == 2; }
uint64_t MatchCount() {
    uint64_t count = 0;
    for (uint64_t i = 0; i < kRows; ++i) count += IsMatch(i) ? 1 : 0;
    return count;
}

class FindByResumeTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    unsigned long long first = 0, begin = 0, last = 0;   // the rows start at first; the query range starts three years earlier (years that have no file: three notices)

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_findby_resume_test");
        store = std::make_unique<XStore>();
        CreateTestDb(*store, kDb, XStore_Proto::SECOND, kRowLength);
        first = TimeStruct(static_cast<std::time_t>(1672531200ULL), XStore_Proto::SECOND).first_epoch;   // 2023-01-01
        begin = first - 3 * 366 * 86400ULL;
        last = first + kRows - 1;
        for (uint64_t from = 0; from < kRows; from += 2000) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(kDb);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (uint64_t i = from; i < from + 2000; ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(first + i);
                entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(i));
                entry->mutable_data()->add_fielddata()->set_stringdata("grp-" + std::to_string(i % 5) + "-" + std::to_string(i));
                entry->mutable_data()->add_fielddata()->set_int32data(IsMatch(i) ? 1 : 0);
            }
            XStore_Proto::BatchInsert_REP rep;
            store->BatchInsert(&req, &rep);
        }
        // Category column: its value is the 'grp' string; FindByValue compares the whole value, so use a second DB column of plain categories.
        UseSequential();
    }
    void TearDown() override {
        xstore::scanlock::Set(xstore::scanlock::Config{});
        store.reset();
        scratch.reset();
    }
    void UseSequential() {
        xstore::scanlock::Config config;
        config.chunk_bytes = 4096ull * kRowLength;
        config.parallel_threads = 1;
        xstore::scanlock::Set(config);
    }
    void UseParallel() {
        xstore::scanlock::Config config;
        config.chunk_bytes = 4096ull * kRowLength;
        config.parallel_threads = 2;
        config.ordered_piece_bytes = 4096ull * kRowLength;   // one piece per block
        xstore::scanlock::Set(config);
    }

    // FindByValue needs a column that repeats: the doubles repeat nowhere, so ask for one row value (a single match) and for a whole-string match on the
    // category through FindByPattern (a fifth of the rows). FindByValue on column 0 with a double: exactly one row.
    static XStore_Proto::FData TheDouble() { return FInt32(1); }

    XStore::FindByValueJob ValueJob() {
        XStore::FindByValueJob job;
        job.start_epoch = begin;
        job.end_epoch = last;
        job.db_name = kDb;
        job.column_idx = 2;
        job.value = TheDouble();
        job.chunk_size_bytes = 1;   // every match is a chunk of its own: the parking points are not limited by chunk size
        return job;
    }
    XStore::FindByPatternJob PatternJob() {
        XStore::FindByPatternJob job;
        job.start_epoch = begin;
        job.end_epoch = last;
        job.db_name = kDb;
        job.column_idx = 1;
        job.pattern = "grp-2%";
        job.mode = XStore_Proto::PATTERN_LIKE;
        job.chunk_size_bytes = kReplyChunkBytes;
        return job;
    }
};
}  // namespace

// ---- FindByPattern: a fifth of the rows match, so every scan chunk has output ----

TEST_F(FindByResumeTest, PatternRunThatNeverParksIsTheOneShotReply) {
    std::string expected;
    store->FindByPattern(begin, last, kDb, 1, "grp-2%", XStore_Proto::PATTERN_LIKE, false, kReplyChunkBytes, [&](std::string &&c, bool) { expected += c; });
    XStore::FindByPatternJob job = PatternJob();
    std::string buffer;
    int last_flags = 0;
    EXPECT_EQ(store->FindByPatternRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return false; }), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    EXPECT_TRUE(buffer == expected);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer));
    EXPECT_EQ(rep.queryresponse_size(), static_cast<int>(kRows / 5));
    EXPECT_NE(rep.querystatus().find("does not exist"), std::string::npos) << "the missing years' notices are in the status";
}

TEST_F(FindByResumeTest, PatternParkingAtEveryScanChunkGivesTheSameBytesSequential) {
    std::string expected;
    store->FindByPattern(begin, last, kDb, 1, "grp-2%", XStore_Proto::PATTERN_LIKE, false, kReplyChunkBytes, [&](std::string &&c, bool) { expected += c; });
    XStore::FindByPatternJob job = PatternJob();
    std::string buffer;
    int parks = 0, last_flags = 0;
    for (int guard = 0; guard < 1000; ++guard) {
        if (store->FindByPatternRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return true; }) == XStore::RunState::kDone) break;
        ++parks;
        EXPECT_EQ(last_flags, 0);
    }
    EXPECT_TRUE(buffer == expected);
    EXPECT_EQ(last_flags, 1);
    EXPECT_GE(parks, 3) << "5 blocks, one scan chunk each";
}

TEST_F(FindByResumeTest, PatternParkingAtEveryPieceGivesTheSameBytesParallel) {
    std::string expected;
    store->FindByPattern(begin, last, kDb, 1, "grp-2%", XStore_Proto::PATTERN_LIKE, false, kReplyChunkBytes, [&](std::string &&c, bool) { expected += c; });
    UseParallel();
    XStore::FindByPatternJob job = PatternJob();
    std::string buffer;
    int parks = 0;
    for (int guard = 0; guard < 1000; ++guard) {
        if (store->FindByPatternRun(job, [&](std::string &&c, bool) { buffer += c; }, [] { return true; }) == XStore::RunState::kDone) break;
        ++parks;
    }
    EXPECT_TRUE(buffer == expected);
    EXPECT_GE(parks, 3);
}

TEST_F(FindByResumeTest, PatternParkingEveryThirdBoundaryGivesTheSameBytes) {
    std::string expected;
    store->FindByPattern(begin, last, kDb, 1, "grp-2%", XStore_Proto::PATTERN_LIKE, false, kReplyChunkBytes, [&](std::string &&c, bool) { expected += c; });
    for (const bool parallel : {false, true}) {
        if (parallel) UseParallel();
        XStore::FindByPatternJob job = PatternJob();
        std::string buffer;
        int asked = 0;
        for (int guard = 0; guard < 1000; ++guard) {
            if (store->FindByPatternRun(job, [&](std::string &&c, bool) { buffer += c; }, [&] { return ++asked % 3 == 0; }) == XStore::RunState::kDone) break;
        }
        EXPECT_TRUE(buffer == expected) << (parallel ? "parallel" : "sequential");
    }
}

TEST_F(FindByResumeTest, PatternARunAlwaysConsumesSomethingBeforeItParks) {
    for (const bool parallel : {false, true}) {
        if (parallel) UseParallel();
        XStore::FindByPatternJob job = PatternJob();
        std::string buffer;
        EXPECT_EQ(store->FindByPatternRun(job, [&](std::string &&c, bool) { buffer += c; }, [] { return true; }), XStore::RunState::kParked);
        EXPECT_GT(job.next_epoch, begin) << (parallel ? "parallel" : "sequential");
    }
}

TEST_F(FindByResumeTest, PatternADbRemovedWhileParkedEndsTheReplyWithAnError) {
    XStore::FindByPatternJob job = PatternJob();
    std::string buffer;
    ASSERT_EQ(store->FindByPatternRun(job, [&](std::string &&c, bool) { buffer += c; }, [] { return true; }), XStore::RunState::kParked);
    (void)store->RemoveDB(kDb);
    int last_flags = 0;
    std::string tail;
    EXPECT_EQ(store->FindByPatternRun(job, [&](std::string &&c, bool is_last) { tail += c; last_flags += is_last; }, [] { return true; }), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer + tail));
    EXPECT_NE(rep.querystatus().find("removed"), std::string::npos) << rep.querystatus();
}

TEST_F(FindByResumeTest, PatternARefusedRequestFinishesAtOnce) {
    XStore::FindByPatternJob job = PatternJob();
    job.pattern = "[";
    job.mode = XStore_Proto::PATTERN_REGEX;
    std::string buffer;
    int last_flags = 0;
    EXPECT_EQ(store->FindByPatternRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return true; }), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer));
    EXPECT_NE(rep.querystatus().find("Invalid pattern"), std::string::npos) << rep.querystatus();
}

// ---- FindByValue: one row matches, but every scan chunk is still a parking point ----

TEST_F(FindByResumeTest, ValueRunThatNeverParksIsTheOneShotReply) {
    std::string expected;
    store->FindByValue(begin, last, kDb, 2, TheDouble(), 1, [&](std::string &&c, bool) { expected += c; });
    XStore::FindByValueJob job = ValueJob();
    std::string buffer;
    int last_flags = 0;
    EXPECT_EQ(store->FindByValueRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return false; }), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    EXPECT_TRUE(buffer == expected);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer));
    EXPECT_EQ(static_cast<uint64_t>(rep.queryresponse_size()), MatchCount());
}

TEST_F(FindByResumeTest, ValueParkingAtEveryBoundaryGivesTheSameBytesSequentialAndParallel) {
    std::string expected;
    store->FindByValue(begin, last, kDb, 2, TheDouble(), 1, [&](std::string &&c, bool) { expected += c; });
    for (const bool parallel : {false, true}) {
        if (parallel) UseParallel();
        XStore::FindByValueJob job = ValueJob();
        std::string buffer;
        int parks = 0, last_flags = 0;
        for (int guard = 0; guard < 1000; ++guard) {
            if (store->FindByValueRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return true; }) == XStore::RunState::kDone) break;
            ++parks;
        }
        EXPECT_TRUE(buffer == expected) << (parallel ? "parallel" : "sequential");
        EXPECT_EQ(last_flags, 1);
        EXPECT_GE(parks, 3) << (parallel ? "parallel" : "sequential");
    }
}

TEST_F(FindByResumeTest, ValueADbRemovedWhileParkedEndsTheReplyWithAnError) {
    XStore::FindByValueJob job = ValueJob();
    std::string buffer;
    ASSERT_EQ(store->FindByValueRun(job, [&](std::string &&c, bool) { buffer += c; }, [] { return true; }), XStore::RunState::kParked);
    (void)store->RemoveDB(kDb);
    std::string tail;
    EXPECT_EQ(store->FindByValueRun(job, [&](std::string &&c, bool) { tail += c; }, [] { return true; }), XStore::RunState::kDone);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer + tail));
    EXPECT_NE(rep.querystatus().find("removed"), std::string::npos) << rep.querystatus();
}

TEST_F(FindByResumeTest, ValueANullTargetIsRefusedAtOnce) {
    XStore::FindByValueJob job = ValueJob();
    job.value = XStore_Proto::FData();
    std::string buffer;
    int last_flags = 0;
    EXPECT_EQ(store->FindByValueRun(job, [&](std::string &&c, bool is_last) { buffer += c; last_flags += is_last; }, [] { return true; }), XStore::RunState::kDone);
    EXPECT_EQ(last_flags, 1);
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buffer));
    EXPECT_NE(rep.querystatus().find("null"), std::string::npos) << rep.querystatus();
}
