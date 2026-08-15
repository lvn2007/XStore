// Throughput/latency micro-benchmarks for the four hot paths that get
// exercised in production the most: Unary Insert/Query (one row per call)
// and Batch Insert/Query (many rows per call). Deliberately not gtest
// benchmark-library-based (no google/benchmark dependency in this repo) --
// each case is a plain timed loop reported via std::cout, with only soft
// correctness assertions (no crash / no data loss). No hard latency/
// throughput thresholds are asserted: absolute numbers vary too much by
// machine to make a good CI gate, so this suite exists to be read by a
// human doing perf work (e.g. comparing before/after a change), not to fail
// a build. See tests/integration/XStoreTest.cpp for correctness coverage of
// these same APIs.
//
// Row/operation count defaults to 200,000 and can be overridden via the
// XSTORE_PERF_N environment variable for quicker local iteration or a
// heavier run.
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <unistd.h>

#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include "tests/integration/XStoreTestSupport.h"

using namespace xstore_test;

namespace {

unsigned long long PerfOpCount(unsigned long long DefaultCount) {
    if (const char *Env = std::getenv("XSTORE_PERF_N")) {
        char *End = nullptr;
        unsigned long long Parsed = std::strtoull(Env, &End, 10);
        if (End != Env && Parsed > 0) return Parsed;
    }
    return DefaultCount;
}

struct PerfStats {
    std::string Name;
    unsigned long long Count;
    double ElapsedSeconds;

    double OpsPerSec() const { return ElapsedSeconds > 0 ? (double)Count / ElapsedSeconds : 0.0; }
    double AvgLatencyUs() const { return Count > 0 ? (ElapsedSeconds * 1'000'000.0) / (double)Count : 0.0; }
};

void ReportPerf(const PerfStats &Stats) {
    std::cout << std::fixed << std::setprecision(2)
              << "[PERF] " << Stats.Name
              << " | rows=" << Stats.Count
              << " | elapsed=" << Stats.ElapsedSeconds << "s"
              << " | throughput=" << Stats.OpsPerSec() << " rows/sec"
              << " | avg_latency=" << Stats.AvgLatencyUs() << "us/row"
              << std::endl;
}

// A representative payload (not a single trivial column) so the timed loop
// reflects realistic per-row serialize/copy cost, not a best case.
XStore_Proto::RowData PerfRow(unsigned long long i) {
    return MakeRow({FDouble((double)i), FInt64((int64_t)i), FString("payload")});
}

}  // namespace

class XStorePerfTest : public ::testing::Test {
 protected:
  std::unique_ptr<ScratchDir> Scratch;
  // Mid-year anchor (2024-05-30 12:00:00 UTC) so N sequential seconds from
  // here stay within a single year file for any N used by this suite --
  // isolates steady-state per-row cost from one-time year-file creation.
  static constexpr unsigned long long BaseTs = 1717070400ULL;

  void SetUp() override {
      // XStore logs an [INFO] line per insert/query call via the default
      // spdlog logger; at 200k+ calls that stdout I/O would dominate the
      // timed loops below and measure logging cost instead of XStore's.
      spdlog::set_level(spdlog::level::warn);
      Scratch = std::make_unique<ScratchDir>("xstore_perf_test");
      for (auto &Shard : XStore::CfgShards) Shard.Map.clear();
  }

  void TearDown() override {
      Scratch.reset();
  }
};

// ---------------------------------------------------------------------
// Unary Insert / Query -- one row per call, the shape of a single live
// write/read in production.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, UnaryInsertThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(200'000);

    XStore_Proto::UnaryInsert_REP Rep;
    auto Start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < N; i++) {
        Store.UnaryInsert(BaseTs + i, PerfRow(i), "PERFDB", &Rep);
    }
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_NE(Rep.insertstatus().find("[INFO]"), std::string::npos) << "last insert failed: " << Rep.insertstatus();
    ReportPerf({"UnaryInsert", N, Elapsed});
}

TEST_F(XStorePerfTest, UnaryQueryThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(200'000);

    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::UnaryInsert_REP SeedRep;
        Store.UnaryInsert(BaseTs + i, PerfRow(i), "PERFDB", &SeedRep);
    }

    XStore_Proto::UnaryQuery_REP QueryRep;
    auto Start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < N; i++) {
        Store.UnaryQuery(BaseTs + i, "PERFDB", &QueryRep);
    }
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_EQ(QueryRep.data().fielddata_size(), 3) << "last query returned unexpected column count";
    ReportPerf({"UnaryQuery", N, Elapsed});
}

// ---------------------------------------------------------------------
// Batch Insert / Query -- many rows in a single call, the shape of a bulk
// load/export in production.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, BatchInsertThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(200'000);

    XStore_Proto::BatchInsert_REQ Req;
    Req.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::BatchInsert_DATA *Entry = Req.add_batchinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }

    XStore_Proto::BatchInsert_REP Rep;
    auto Start = std::chrono::steady_clock::now();
    Store.BatchInsert(&Req, &Rep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    EXPECT_NE(Rep.insertstatus().find(fmt::format("Inserted {} elements", N)), std::string::npos) << Rep.insertstatus();
    ReportPerf({"BatchInsert", N, Elapsed});
}

TEST_F(XStorePerfTest, BatchQueryThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(200'000);

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) QueryReq.add_timestamp(BaseTs + i);

    XStore_Proto::BatchQuery_REP QueryRep;
    auto Start = std::chrono::steady_clock::now();
    Store.BatchQuery(&QueryReq, &QueryRep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_EQ(QueryRep.queryresponse_size(), (int)N) << "batch query lost/gained rows";
    ReportPerf({"BatchQuery", N, Elapsed});
}

// ---------------------------------------------------------------------
// Range Insert / Query -- many rows in a single call like Batch above, but
// addressed by a contiguous [start, end] span instead of an explicit
// timestamp list (and holes come back as empty entries instead of being
// omitted) -- a distinct code path worth its own throughput number.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, RangeInsertThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(200'000);

    XStore_Proto::RangeInsert_REQ Req;
    Req.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::RangeInsert_DATA *Entry = Req.add_rangeinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }

    XStore_Proto::RangeInsert_REP Rep;
    auto Start = std::chrono::steady_clock::now();
    Store.RangeInsert(&Req, &Rep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    EXPECT_NE(Rep.insertstatus().find(fmt::format("Inserted {} elements", N)), std::string::npos) << Rep.insertstatus();
    ReportPerf({"RangeInsert", N, Elapsed});
}

TEST_F(XStorePerfTest, RangeQueryThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(200'000);

    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::RangeQuery_REP QueryRep;
    auto Start = std::chrono::steady_clock::now();
    Store.RangeQuery(BaseTs, BaseTs + N - 1, "PERFDB", &QueryRep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_EQ(QueryRep.queryresponse_size(), (int)N) << "range query lost/gained rows";
    ReportPerf({"RangeQuery", N, Elapsed});
}

// ---------------------------------------------------------------------
// Aggregates -- Min/Max/Sum/Avg each do a single mmap'd linear scan over
// the requested range (ProcessFileRangeSelect for Min/Max,
// ProcessFileRangeAccumulate for Sum/Avg -- Avg is Sum's kernel plus one
// division, so its number is a proxy for Sum's), independent of the
// GetOrMapFile cache Unary/Batch/Range insert+query share. Default row
// count is heavier than the insert/query cases above (1,000,000) since a
// single scan call is being timed, not N individual calls.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, MinAggregateThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::Min_REP MinRep;
    auto Start = std::chrono::steady_clock::now();
    Store.MinAggregate(BaseTs, BaseTs + N - 1, "PERFDB", 0, &MinRep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_EQ(MinRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(MinRep.opresult().doubledata(), 0.0);
    ReportPerf({"MinAggregate", N, Elapsed});
}

TEST_F(XStorePerfTest, MaxAggregateThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::Max_REP MaxRep;
    auto Start = std::chrono::steady_clock::now();
    Store.MaxAggregate(BaseTs, BaseTs + N - 1, "PERFDB", 0, &MaxRep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_EQ(MaxRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(MaxRep.opresult().doubledata(), (double)(N - 1));
    ReportPerf({"MaxAggregate", N, Elapsed});
}

TEST_F(XStorePerfTest, SumAggregateThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::Sum_REP SumRep;
    auto Start = std::chrono::steady_clock::now();
    Store.SumAggregate(BaseTs, BaseTs + N - 1, "PERFDB", 0, &SumRep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_EQ(SumRep.opstatus(), "SUCCESS");
    ReportPerf({"SumAggregate", N, Elapsed});
}

// ---------------------------------------------------------------------
// MinAggregate: generic (ValueStruct/long double via GetFDataValue) vs a
// type-specialized prototype (ProcessFileRangeSelectTypedDouble, compares
// native double directly -- see XStore.h/.cpp). Same mmap/parse plumbing in
// both; only the post-parse compare step differs, so any gap here isolates
// that cost. Swept across small/medium/large N to see whether the delta's
// share of total time changes with scan size.
// ---------------------------------------------------------------------

namespace {

// Both kernels read the exact same mmap'd bytes and count of rows, so a run
// order where one goes first would let the second look faster purely from
// inheriting hot page-cache/TLB state, not real kernel cost. This runs both
// once (untimed, to fault every page in for both) then times each kernel
// kRepeats times, alternating order each repeat, and reports the min of each
// (standard microbench practice: min filters OS jitter better than mean).
constexpr int kRepeats = 5;

// No root on this box (drop_caches is root-only), but /tmp -- where
// ScratchDir puts its files -- is real ext4, not tmpfs, so per-file
// fsync+POSIX_FADV_DONTNEED evicts exactly this DB's year files from page
// cache without touching the rest of the machine's cache. fsync first:
// DONTNEED on a still-dirty page is not guaranteed to evict it.
void DropPageCacheForDb(const std::string &dbName) {
    fs::path DbDir = fs::current_path() / "DataStore" / dbName;
    if (!fs::exists(DbDir)) return;
    for (const auto &Entry : fs::directory_iterator(DbDir)) {
        if (Entry.path().extension() != ".XSTORE") continue;
        int Fd = open(Entry.path().c_str(), O_RDONLY);
        if (Fd < 0) continue;
        fsync(Fd);
        posix_fadvise(Fd, 0, 0, POSIX_FADV_DONTNEED);
        close(Fd);
    }
}

double TimedRunGeneric(XStore &Store, unsigned long long BaseTs, unsigned long long N,
                        XStore_Proto::Min_REP &Rep) {
    auto Start = std::chrono::steady_clock::now();
    Store.MinAggregate(BaseTs, BaseTs + N - 1, "PERFDB", 0, &Rep);
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();
}

double TimedRunTyped(XStore &Store, unsigned long long BaseTs, unsigned long long N,
                      XStore_Proto::Min_REP &Rep) {
    auto Start = std::chrono::steady_clock::now();
    Store.MinAggregateTypedDouble(BaseTs, BaseTs + N - 1, "PERFDB", 0, &Rep);
    return std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();
}

void RunMinAggregateCompare(const std::string &Label, unsigned long long N) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("PERFDB");
    InsertReq.mutable_batchinsert_data()->Reserve((int)N);
    // Same anchor as XStorePerfTest::BaseTs (mid-year, so N sequential
    // seconds stay within one year file for any N used here).
    constexpr unsigned long long BaseTs = 1717070400ULL;
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    // --- WARM: page cache hot, both kernels reading RAM -------------------
    XStore_Proto::Min_REP WarmRep;
    TimedRunGeneric(Store, BaseTs, N, WarmRep);
    TimedRunTyped(Store, BaseTs, N, WarmRep);

    double GenWarmBest = std::numeric_limits<double>::max();
    double TypedWarmBest = std::numeric_limits<double>::max();
    XStore_Proto::Min_REP GenericRep, TypedRep;

    for (int Rep = 0; Rep < kRepeats; Rep++) {
        if (Rep % 2 == 0) {
            GenWarmBest = std::min(GenWarmBest, TimedRunGeneric(Store, BaseTs, N, GenericRep));
            TypedWarmBest = std::min(TypedWarmBest, TimedRunTyped(Store, BaseTs, N, TypedRep));
        } else {
            TypedWarmBest = std::min(TypedWarmBest, TimedRunTyped(Store, BaseTs, N, TypedRep));
            GenWarmBest = std::min(GenWarmBest, TimedRunGeneric(Store, BaseTs, N, GenericRep));
        }
    }

    ASSERT_EQ(GenericRep.opstatus(), "SUCCESS");
    ASSERT_EQ(TypedRep.opstatus(), "SUCCESS");
    ASSERT_DOUBLE_EQ(GenericRep.opresult().doubledata(), TypedRep.opresult().doubledata())
        << "generic and typed kernels disagree on the min -- correctness regression";

    ReportPerf({"MinAggregate[generic][warm]/" + Label, N, GenWarmBest});
    ReportPerf({"MinAggregate[typed]  [warm]/" + Label, N, TypedWarmBest});

    double WarmDeltaPct = GenWarmBest > 0 ? 100.0 * (GenWarmBest - TypedWarmBest) / GenWarmBest : 0.0;
    std::cout << "[PERF] MinAggregate typed vs generic, WARM (" << Label << ", N=" << N
              << ", best-of-" << kRepeats << " order-alternated): "
              << std::fixed << std::setprecision(1) << WarmDeltaPct << "% "
              << (WarmDeltaPct >= 0 ? "faster" : "slower") << std::endl;

    // --- COLD: page cache for this DB's files dropped before every single
    // timed call, so each one pays a real disk read, not a RAM copy --------
    constexpr int kColdRepeats = 3;
    double GenColdBest = std::numeric_limits<double>::max();
    double TypedColdBest = std::numeric_limits<double>::max();
    XStore_Proto::Min_REP GenericColdRep, TypedColdRep;

    for (int Rep = 0; Rep < kColdRepeats; Rep++) {
        if (Rep % 2 == 0) {
            DropPageCacheForDb("PERFDB");
            GenColdBest = std::min(GenColdBest, TimedRunGeneric(Store, BaseTs, N, GenericColdRep));
            DropPageCacheForDb("PERFDB");
            TypedColdBest = std::min(TypedColdBest, TimedRunTyped(Store, BaseTs, N, TypedColdRep));
        } else {
            DropPageCacheForDb("PERFDB");
            TypedColdBest = std::min(TypedColdBest, TimedRunTyped(Store, BaseTs, N, TypedColdRep));
            DropPageCacheForDb("PERFDB");
            GenColdBest = std::min(GenColdBest, TimedRunGeneric(Store, BaseTs, N, GenericColdRep));
        }
    }

    ASSERT_EQ(GenericColdRep.opstatus(), "SUCCESS");
    ASSERT_EQ(TypedColdRep.opstatus(), "SUCCESS");
    ASSERT_DOUBLE_EQ(GenericColdRep.opresult().doubledata(), TypedColdRep.opresult().doubledata())
        << "generic and typed kernels disagree on the min (cold) -- correctness regression";

    ReportPerf({"MinAggregate[generic][cold]/" + Label, N, GenColdBest});
    ReportPerf({"MinAggregate[typed]  [cold]/" + Label, N, TypedColdBest});

    double ColdDeltaPct = GenColdBest > 0 ? 100.0 * (GenColdBest - TypedColdBest) / GenColdBest : 0.0;
    std::cout << "[PERF] MinAggregate typed vs generic, COLD (" << Label << ", N=" << N
              << ", best-of-" << kColdRepeats << " order-alternated, cache dropped before every call): "
              << std::fixed << std::setprecision(1) << ColdDeltaPct << "% "
              << (ColdDeltaPct >= 0 ? "faster" : "slower") << std::endl;

    double ColdVsWarmSlowdown = GenWarmBest > 0 ? GenColdBest / GenWarmBest : 0.0;
    std::cout << "[PERF] cold/warm slowdown for generic kernel (" << Label << "): "
              << std::setprecision(2) << ColdVsWarmSlowdown << "x" << std::endl;
}

}  // namespace

TEST_F(XStorePerfTest, MinAggregateTypedVsGeneric_Small) {
    RunMinAggregateCompare("small", 10'000);
}

TEST_F(XStorePerfTest, MinAggregateTypedVsGeneric_Medium) {
    RunMinAggregateCompare("medium", 1'000'000);
}

TEST_F(XStorePerfTest, MinAggregateTypedVsGeneric_Large) {
    RunMinAggregateCompare("large", 10'000'000);
}

TEST_F(XStorePerfTest, AvgAggregateThroughput) {
    XStore Store;
    Store.CreateDB("PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long N = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < N; i++) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(BaseTs + i);
        *Entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::Avg_REP AvgRep;
    auto Start = std::chrono::steady_clock::now();
    Store.AvgAggregate(BaseTs, BaseTs + N - 1, "PERFDB", 0, &AvgRep);
    double Elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - Start).count();

    ASSERT_EQ(AvgRep.opstatus(), "SUCCESS");
    ReportPerf({"AvgAggregate", N, Elapsed});
}
