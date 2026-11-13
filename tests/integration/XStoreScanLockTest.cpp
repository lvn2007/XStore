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
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <atomic>
#include <cstdlib>
#include <new>

// Counts the allocations of 24 KiB up to 256 KiB (a value vector of the test rows is 32 KiB per zone-map block; Percentile's histograms are
// 256 KiB and 512 KiB) while armed. Replaces the global operator new of this test binary only to count.
namespace {
std::atomic<bool> g_count_big_allocs{false};
std::atomic<uint64_t> g_big_allocs{0};
}  // namespace
void *operator new(std::size_t size) {
    if (g_count_big_allocs.load(std::memory_order_relaxed) && size >= 24576 && size < 262144) {
        g_big_allocs.fetch_add(1, std::memory_order_relaxed);
    }
    if (void *p = std::malloc(size == 0 ? 1 : size)) return p;
    throw std::bad_alloc();
}
void operator delete(void *p) noexcept { std::free(p); }
void operator delete(void *p, std::size_t) noexcept { std::free(p); }
// The nothrow pair too (libstdc++'s temporary buffers, e.g. std::stable_sort in gtest): replaced deletes free() what
// the default nothrow new allocated, which AddressSanitizer reports as alloc-dealloc-mismatch.
void *operator new(std::size_t size, const std::nothrow_t &) noexcept { return std::malloc(size == 0 ? 1 : size); }
void operator delete(void *p, const std::nothrow_t &) noexcept { std::free(p); }

namespace {

using namespace xstore_test;
using namespace std::chrono_literals;

constexpr uint16_t kRowLength = 512;              // a zone-map block is 2 MiB / 512 = 4096 rows: the 20,000 rows are 5 blocks
constexpr uint64_t kRows = 20'000;
constexpr uint64_t kBlockBytes = 4096ull * kRowLength;

// The scans are adaptive and parallel by default. These tests need the plain sequential scan (a chunk hook that sees chunk 0, 1, 2 in
// order, or the sequential answer to compare the parallel ones with), so they ask for it.
xstore::scanlock::Config Sequential(uint64_t chunk_bytes) {
    xstore::scanlock::Config config;
    config.chunk_bytes = chunk_bytes;
    config.parallel_threads = 1;
    return config;
}

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
        xstore::scanlock::Set(Sequential(chunk_bytes));
        for (const Op &op : Ops()) {
            EXPECT_EQ(op.run(*store, db, base), op.expected) << op.name << " with chunks of " << chunk_bytes << " bytes";
        }
    }
}

// An exact aggregate collects its values in one vector per piece, and the scan lock hands the kernel one chunk at a time: the vector must
// be sized once for the piece, not regrown (reallocated and copied) at every chunk, which made a 12M-row call spend ~90 % of its time in
// memmove (Mystic, 2026-11-03).
TEST_F(ScanLockTest, TheExactAggregatesAllocateEachValueVectorOnce) {
    const auto call = [&](int op) {
        if (op == 0) {
            XStore_Proto::Percentile_REP rep;
            store->PercentileAggregate(base, base + kRows - 1, db, 0, 0.5, &rep);
            EXPECT_NEAR(rep.value(), (kRows - 1) / 2.0, 1.0);
        } else if (op == 1) {
            XStore_Proto::DistinctCount_REP rep;
            store->DistinctCountAggregate(base, base + kRows - 1, db, 0, &rep);
            EXPECT_EQ(rep.count(), kRows);
        } else {
            XStore_Proto::TopK_REP rep;
            store->TopKAggregate(base, base + kRows - 1, db, 0, 3, &rep);
            EXPECT_EQ(rep.entries_size(), 3);
        }
    };
    const auto big_allocations = [&](const xstore::scanlock::Config &config, int op) {
        xstore::scanlock::Set(config);
        uint64_t fewest = UINT64_MAX;
        for (int run = 0; run < 3; ++run) {   // the least of three: another thread's allocation of that size cannot make it fail
            g_big_allocs = 0;
            g_count_big_allocs = true;
            call(op);
            g_count_big_allocs = false;
            fewest = std::min<uint64_t>(fewest, g_big_allocs.load());
        }
        return fewest;
    };
    // Each op's own other allocations (DistinctCount's table, the merge) are the same whether the scan lock hands the kernel one chunk or
    // many, so the count with the whole range under one lock (nothing to regrow) is the baseline the chunked count must not exceed.
    // Sequential: one piece of five chunks. Pieces: 3 blocks each (12,288 and 7,712 rows), three and two chunks, two threads.
    xstore::scanlock::Config pieces;
    pieces.parallel_threads = 2;
    pieces.piece_bytes = 3 * kBlockBytes;
    for (int op = 0; op < 3; ++op) {
        pieces.chunk_bytes = 0;
        const uint64_t pieces_baseline = big_allocations(pieces, op);
        pieces.chunk_bytes = kBlockBytes;
        EXPECT_LE(big_allocations(pieces, op), pieces_baseline) << "op " << op << ": a piece's value vector grew chunk by chunk";
        if (op == 0) {   // Percentile allocates nothing else of that size: one vector per piece (98,304 and 61,696 bytes), no merged copy
            EXPECT_LE(big_allocations(pieces, op), 2u) << "a piece's vector was sized too small and regrown";
        }
        const uint64_t sequential_baseline = big_allocations(Sequential(0), op);
        EXPECT_LE(big_allocations(Sequential(kBlockBytes), op), sequential_baseline) << "op " << op << ": the value vector grew chunk by chunk";
        if (op == 0) {
            EXPECT_LE(big_allocations(Sequential(kBlockBytes), op), 1u) << "the value vector was sized too small and regrown";
        }
    }
}

// A year is cut into pieces that run in parallel (xstore::scanlock::Config::parallel_threads): the aggregates must give the same
// answer for every number of threads, with pieces of one block (5 pieces here) and with one piece.
TEST_F(ScanLockTest, TheExactAggregatesGiveTheSameAnswerForEveryParallelThreads) {
    for (const uint32_t threads : {1u, 2u, 4u, 8u, 0u}) {   // 0 = adaptive
        for (const uint64_t piece_bytes : {kBlockBytes, 2 * kBlockBytes, uint64_t{64} << 20}) {
            xstore::scanlock::Config config;
            config.chunk_bytes = kBlockBytes;
            config.parallel_threads = threads;
            config.piece_bytes = piece_bytes;
            xstore::scanlock::Set(config);
            std::mutex threads_mutex;
            std::set<std::thread::id> threads_seen;
            xstore::scanlock::SetChunkHookForTests([&](uint64_t) {
                std::this_thread::sleep_for(20ms);   // long enough for the other pieces of the wave to be under way too
                std::lock_guard<std::mutex> lock(threads_mutex);
                threads_seen.insert(std::this_thread::get_id());
            });
            XStore_Proto::DistinctCount_REP distinct;
            store->DistinctCountAggregate(base, base + kRows - 1, db, 0, &distinct);
            xstore::scanlock::SetChunkHookForTests(nullptr);
            EXPECT_EQ(distinct.count(), kRows) << "threads " << threads << " piece " << piece_bytes;
            if (piece_bytes == kBlockBytes) {   // 5 pieces
                if (threads == 1) EXPECT_EQ(threads_seen.size(), 1u) << "pieces ran on several threads with one thread";
                else if (threads != 0) EXPECT_GE(threads_seen.size(), 2u) << "threads " << threads << ": the pieces did not run in parallel";
            }
            XStore_Proto::Percentile_REP median;
            store->PercentileAggregate(base, base + kRows - 1, db, 0, 0.5, &median);
            EXPECT_NEAR(median.value(), (kRows - 1) / 2.0, 1.0) << "threads " << threads << " piece " << piece_bytes;
            XStore_Proto::TopK_REP top;
            store->TopKAggregate(base, base + kRows - 1, db, 0, 3, &top);
            EXPECT_EQ(top.entries_size(), 3) << "threads " << threads << " piece " << piece_bytes;
        }
    }
}

// FindByValue and FindByPattern scanned in parallel send their rows in time order: the reply bytes must be identical
// to the sequential scan's for every number of threads and piece size (a match in every piece, and across the piece boundaries), and the
// pieces must run on several threads.
TEST_F(ScanLockTest, TheRowEmittingScansReturnTheSameBytesInTheSameOrderForEveryParallelThreads) {
    const auto reply_bytes = [&](const char *which) {
        std::string buf;
        const auto sink = [&](std::string &&chunk, bool) { buf += chunk; };
        if (std::string(which) == "value") store->FindByValue(base + 7, base + kRows - 9, db, 0, FDouble(12345.0), 1 << 20, sink);
        else if (std::string(which) == "valuemany") store->FindByValue(base, base + kRows - 1, db, 1, FString("nope"), 1 << 20, sink);
        else store->FindByPattern(base, base + kRows - 1, db, 1, "row-1%", XStore_Proto::PATTERN_LIKE, false, 1 << 20, sink);
        return buf;
    };
    xstore::scanlock::Set(Sequential(kBlockBytes));
    std::map<std::string, std::string> sequential;
    for (const char *which : {"value", "valuemany", "pattern"}) sequential[which] = reply_bytes(which);
    ASSERT_FALSE(sequential["value"].empty());
    ASSERT_FALSE(sequential["pattern"].empty());
    for (const uint32_t threads : {2u, 4u, 8u, 0u}) {   // 0 = adaptive
        for (const uint64_t piece_bytes : {kBlockBytes, 2 * kBlockBytes, uint64_t{64} << 20}) {
            xstore::scanlock::Config config;
            config.chunk_bytes = kBlockBytes;
            config.parallel_threads = threads;
            config.ordered_piece_bytes = piece_bytes;
            xstore::scanlock::Set(config);
            std::mutex threads_mutex;
            std::set<std::thread::id> threads_seen;
            xstore::scanlock::SetChunkHookForTests([&](uint64_t) {
                std::this_thread::sleep_for(10ms);
                std::lock_guard<std::mutex> lock(threads_mutex);
                threads_seen.insert(std::this_thread::get_id());
            });
            for (const char *which : {"value", "valuemany", "pattern"}) {
                threads_seen.clear();
                EXPECT_TRUE(reply_bytes(which) == sequential[which]) << which << " threads " << threads << " piece " << piece_bytes;
                if (threads != 0 && piece_bytes == kBlockBytes && std::string(which) == "pattern") {
                    EXPECT_GE(threads_seen.size(), 2u) << which << " threads " << threads << ": the pieces did not run in parallel";
                }
            }
            xstore::scanlock::SetChunkHookForTests(nullptr);
        }
    }
}

// A client that is slow to take its reply must not park the shared pool: producers that find no room for 100 ms give their
// thread back, the consumer starts new ones, and the output is still complete and in order.
TEST_F(ScanLockTest, ASlowConsumerMakesTheProducersRetireAndTheScanStillFinishesInOrder) {
    const auto run = [&](bool slow) {
        std::string buf;
        int sends = 0;
        store->FindByPattern(base, base + kRows - 1, db, 1, "row-1%", XStore_Proto::PATTERN_LIKE, false, 1,   // every piece is sent
                             [&](std::string &&chunk, bool) {
                                 buf += chunk;
                                 if (slow && ++sends <= 3) std::this_thread::sleep_for(250ms);
                             });
        return buf;
    };
    xstore::scanlock::Set(Sequential(kBlockBytes));
    const std::string sequential = run(false);
    ASSERT_FALSE(sequential.empty());
    xstore::scanlock::Config config;
    config.chunk_bytes = kBlockBytes;
    config.parallel_threads = 2;
    config.ordered_piece_bytes = kBlockBytes;   // 5 pieces, a window of 4
    xstore::scanlock::Set(config);
    const uint64_t relaunches_before = xstore::scanlock::ProducerRelaunchesForTests();
    EXPECT_TRUE(run(true) == sequential);
    EXPECT_GE(xstore::scanlock::ProducerRelaunchesForTests(), relaunches_before + 1) << "no producer retired and was started again";
}

// A parallel row-emitting scan that is stopped by its consumer (a reply that parks) reports how many rows of the region were consumed, so the
// scan can be started again from there (docs/superpowers/specs/2026-11-01-park-resume-replies-design.md).
class OrderedScanStopTest : public ::testing::Test {
protected:
    void SetUp() override {
        xstore::scanlock::Config config;
        config.chunk_bytes = kBlockBytes;
        config.parallel_threads = 2;
        config.ordered_piece_bytes = kBlockBytes;   // one piece per zone-map block
        xstore::scanlock::Set(config);
    }
    void TearDown() override { xstore::scanlock::Set(xstore::scanlock::Config{}); }
    std::vector<char> region = std::vector<char>(5 * 4096 * kRowLength);   // 5 blocks

    xstore::scanlock::OrderedScan Run(unsigned long long first_row_in_year, int stop_after_pieces, int *consumed_pieces) {
        return xstore::scanlock::ScanRegionOrdered(
            "ordered-scan-stop-test", region.data(), 0, region.size(), kRowLength, first_row_in_year,
            [](const char *, unsigned long long, unsigned long long, std::string &out) { out.assign(1, 'x'); },
            [&](std::string &) { return ++*consumed_pieces != stop_after_pieces; });
    }
};

TEST_F(OrderedScanStopTest, AScanThatRunsToTheEndReportsTheWholeRegionConsumed) {
    int pieces = 0;
    const xstore::scanlock::OrderedScan result = Run(0, -1, &pieces);
    EXPECT_TRUE(result.ran);
    EXPECT_EQ(pieces, 5);
    EXPECT_EQ(result.rows_consumed, 5u * 4096u);
}

TEST_F(OrderedScanStopTest, AScanStoppedAfterTwoPiecesReportsTheirRows) {
    int pieces = 0;
    const xstore::scanlock::OrderedScan result = Run(0, 2, &pieces);
    EXPECT_TRUE(result.ran);
    EXPECT_EQ(pieces, 2) << "nothing is consumed after the stop";
    EXPECT_EQ(result.rows_consumed, 2u * 4096u);
}

TEST_F(OrderedScanStopTest, PiecesStartAtTheBlockBoundariesOfTheYearNotOfTheRegion) {
    int pieces = 0;
    const xstore::scanlock::OrderedScan result = Run(1000, 2, &pieces);   // the region starts at row 1000 of the year: the first piece is 3096 rows
    EXPECT_EQ(result.rows_consumed, 3096u + 4096u);
}

TEST_F(OrderedScanStopTest, ANotParallelScanReportsThatItDidNotRun) {
    xstore::scanlock::Config config;
    config.parallel_threads = 1;
    xstore::scanlock::Set(config);
    int pieces = 0;
    const xstore::scanlock::OrderedScan result = Run(0, -1, &pieces);
    EXPECT_FALSE(result.ran);
    EXPECT_FALSE(static_cast<bool>(result));
    EXPECT_EQ(pieces, 0);
    EXPECT_EQ(result.rows_consumed, 0u);
}

// Out of the box a scan is adaptive (parallel_threads 0) and may use half of the machine's hardware threads at most; the environment
// still forces a number or goes back to adaptive.
TEST(ScanThreadsTest, TheDefaultIsAdaptiveCappedAtHalfTheCores) {
    unsetenv("XSTORE_SCAN_PARALLEL_THREADS");
    unsetenv("XSTORE_SCAN_ADAPTIVE_MAX");
    const unsigned half = std::max(1u, std::thread::hardware_concurrency() / 2);
    const xstore::scanlock::Config plain;
    EXPECT_EQ(plain.parallel_threads, 0u);
    EXPECT_EQ(plain.adaptive_max, half);
    const xstore::scanlock::Config from_env = xstore::scanlock::detail::FromEnv();
    EXPECT_EQ(from_env.parallel_threads, 0u);
    EXPECT_EQ(from_env.adaptive_max, half);
}

// The pieces of a row-emitting scan are 2 MiB of the file out of the box (8 MiB left windows of up to ~6k rows of 1.4 KB in one serial piece and made
// the cost per timestamp rise with the window up to ~8k rows; see the Technical Reference, section 8.1); the environment still sets it.
TEST(ScanThreadsTest, TheOrderedPieceIsTwoMiBAndTheEnvironmentChangesIt) {
    unsetenv("XSTORE_SCAN_ORDERED_PIECE_MB");
    EXPECT_EQ(xstore::scanlock::Config().ordered_piece_bytes, uint64_t{2} << 20);
    EXPECT_EQ(xstore::scanlock::detail::FromEnv().ordered_piece_bytes, uint64_t{2} << 20);
    setenv("XSTORE_SCAN_ORDERED_PIECE_MB", "8", 1);
    EXPECT_EQ(xstore::scanlock::detail::FromEnv().ordered_piece_bytes, uint64_t{8} << 20);
    unsetenv("XSTORE_SCAN_ORDERED_PIECE_MB");
}

TEST(ScanThreadsTest, TheEnvironmentForcesANumberOrTheCap) {
    setenv("XSTORE_SCAN_PARALLEL_THREADS", "3", 1);
    setenv("XSTORE_SCAN_ADAPTIVE_MAX", "5", 1);
    EXPECT_EQ(xstore::scanlock::detail::FromEnv().parallel_threads, 3u);
    EXPECT_EQ(xstore::scanlock::detail::FromEnv().adaptive_max, 5u);
    setenv("XSTORE_SCAN_PARALLEL_THREADS", "1", 1);
    EXPECT_EQ(xstore::scanlock::detail::FromEnv().parallel_threads, 1u);   // one thread = the sequential scan
    setenv("XSTORE_SCAN_PARALLEL_THREADS", "adaptive", 1);
    EXPECT_EQ(xstore::scanlock::detail::FromEnv().parallel_threads, 0u);
    unsetenv("XSTORE_SCAN_PARALLEL_THREADS");
    unsetenv("XSTORE_SCAN_ADAPTIVE_MAX");
}

// Adaptive threads (parallel_threads 0): the idle cores of the machine less a headroom, between 1 and the cap.
TEST(ScanThreadsTest, AdaptiveThreadsFollowTheIdleCoresLessTheHeadroomWithinTheLimits) {
    using xstore::scanlock::ThreadsFor;
    EXPECT_EQ(ThreadsFor(0.0, 32, 2, 16), 16u) << "an idle 32-core machine: capped";
    EXPECT_EQ(ThreadsFor(0.25, 32, 2, 16), 16u) << "24 idle cores less 2: still over the cap";
    EXPECT_EQ(ThreadsFor(0.5, 32, 2, 16), 14u) << "16 idle cores less 2";
    EXPECT_EQ(ThreadsFor(0.75, 32, 2, 16), 6u) << "8 idle cores less 2";
    EXPECT_EQ(ThreadsFor(0.95, 32, 2, 16), 1u) << "1.6 idle cores: no room, sequential";
    EXPECT_EQ(ThreadsFor(1.0, 32, 2, 16), 1u);
    EXPECT_EQ(ThreadsFor(0.0, 4, 2, 16), 2u) << "a small machine";
    EXPECT_EQ(ThreadsFor(0.0, 2, 2, 16), 1u) << "headroom takes it all: still 1";
    EXPECT_EQ(ThreadsFor(-0.3, 8, 2, 16), 6u) << "a bad sample is clamped";
    EXPECT_EQ(ThreadsFor(1.7, 8, 2, 16), 1u);
}

TEST_F(ScanLockTest, AWriterToAnUnscannedChunkGetsInMidScanForEveryScanOp) {
    FileRangeLock &lock = FileLockRegistry::Get(year_path);
    xstore::scanlock::Set(Sequential(kBlockBytes));   // one block per chunk: 5 chunks
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
    xstore::scanlock::Set(Sequential(kBlockBytes));
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
