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
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <unistd.h>
#include <unordered_set>

#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include "tests/integration/XStoreTestSupport.h"

using namespace xstore_test;

namespace {

unsigned long long PerfOpCount(unsigned long long default_count) {
    if (const char *env = std::getenv("XSTORE_PERF_N")) {
        char *end = nullptr;
        unsigned long long parsed = std::strtoull(env, &end, 10);
        if (end != env && parsed > 0) return parsed;
    }
    return default_count;
}

struct PerfStats {
    std::string name;
    unsigned long long count;
    double elapsed_seconds;

    double OpsPerSec() const { return elapsed_seconds > 0 ? (double)count / elapsed_seconds : 0.0; }
    double AvgLatencyUs() const { return count > 0 ? (elapsed_seconds * 1'000'000.0) / (double)count : 0.0; }
};

void ReportPerf(const PerfStats &stats) {
    std::cout << std::fixed << std::setprecision(2)
              << "[PERF] " << stats.name
              << " | rows=" << stats.count
              << " | elapsed=" << stats.elapsed_seconds << "s"
              << " | throughput=" << stats.OpsPerSec() << " rows/sec"
              << " | avg_latency=" << stats.AvgLatencyUs() << "us/row"
              << std::endl;
}

// A representative payload (not a single trivial column) so the timed loop
// reflects realistic per-row serialize/copy cost, not a best case.
XStore_Proto::RowData PerfRow(unsigned long long i) {
    return MakeRow({FDouble((double)i), FInt64((int64_t)i), FString("payload")});
}

// No root on this box (drop_caches is root-only), but /tmp -- where
// ScratchDir puts its files -- is real ext4, not tmpfs, so per-file
// fsync+POSIX_FADV_DONTNEED evicts exactly this DB's year files from page
// cache without touching the rest of the machine's cache. fsync first:
// DONTNEED on a still-dirty page is not guaranteed to evict it.
void DropPageCacheForDb(const std::string &db_name) {
    fs::path db_dir = fs::current_path() / "DataStore" / db_name;
    if (!fs::exists(db_dir)) return;
    for (const auto &entry : fs::directory_iterator(db_dir)) {
        if (entry.path().extension() != ".XSTORE") continue;
        int fd = open(entry.path().c_str(), O_RDONLY);
        if (fd < 0) continue;
        fsync(fd);
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
        close(fd);
    }
}

}  // namespace

class XStorePerfTest : public ::testing::Test {
 protected:
  std::unique_ptr<ScratchDir> scratch;
  // Mid-year anchor (2024-05-30 12:00:00 UTC) so N sequential seconds from
  // here stay within a single year file for any N used by this suite --
  // isolates steady-state per-row cost from one-time year-file creation.
  static constexpr unsigned long long BASE_TS = 1717070400ULL;

  void SetUp() override {
      // XStore logs an [INFO] line per insert/query call via the default
      // spdlog logger; at 200k+ calls that stdout I/O would dominate the
      // timed loops below and measure logging cost instead of XStore's.
      spdlog::set_level(spdlog::level::warn);
      scratch = std::make_unique<ScratchDir>("xstore_perf_test");
      for (auto &shard : XStore::cfg_shards) shard.map.clear();
  }

  void TearDown() override {
      scratch.reset();
  }
};

// ---------------------------------------------------------------------
// Unary Insert / Query -- one row per call, the shape of a single live
// write/read in production.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, UnaryInsertThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::UnaryInsert_REP rep;
    auto start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < n; i++) {
        store.UnaryInsert(BASE_TS + i, PerfRow(i), "PERFDB", &rep);
    }
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << "last insert failed: " << rep.insertstatus();
    ReportPerf({"UnaryInsert", n, elapsed});
}

// Same shape as UnaryInsertThroughput above, explicit SYNC -- pins the mode
// instead of relying on UnaryInsert's implicit default, so this stays a
// valid SYNC baseline even if that default ever changes. Single-threaded/
// single-client, so this is NOT expected to show SYNC's amortized-fsync
// benefit (nothing concurrent to batch with) -- it exists to confirm SYNC's
// own per-commit gather-window cost in isolation (previously split across
// GROUP_COMMIT/SYNC_PER_COMMIT before those were merged -- see
// WalDurabilityMode in XStore.proto), and as a baseline to compare against
// UnaryInsertRandomConcurrentThroughput-style concurrent runs where the
// batching payoff should actually show up.
TEST_F(XStorePerfTest, UnaryInsertThroughput_Sync) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    // Smaller default than the other tests: one fdatasync per call (~320 ops/s single-client) would make the usual
    // 200k take ~10 minutes. XSTORE_PERF_N still overrides.
    const unsigned long long n = PerfOpCount(5'000);

    XStore_Proto::UnaryInsert_REP rep;
    auto start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < n; i++) {
        store.UnaryInsert(BASE_TS + i, PerfRow(i), "PERFDB", &rep, XStore_Proto::SYNC);
    }
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << "last insert failed: " << rep.insertstatus();
    ReportPerf({"UnaryInsert[SYNC]", n, elapsed});
}

// Same shape, explicit ASYNC -- the "doesn't wait for durability" mode.
// Existing implementation still routes this through the full WAL pipeline
// (enqueue/gather/fsync/apply), just without the caller waiting on the
// promise; this number is what a future WAL-bypass-for-ASYNC decision
// should be compared against.
TEST_F(XStorePerfTest, UnaryInsertThroughput_Async) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::UnaryInsert_REP rep;
    auto start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < n; i++) {
        store.UnaryInsert(BASE_TS + i, PerfRow(i), "PERFDB", &rep, XStore_Proto::ASYNC);
    }
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << "last insert failed: " << rep.insertstatus();
    ReportPerf({"UnaryInsert[ASYNC]", n, elapsed});
    // Outside the timed region: the writer is still applying; do not let ScratchDir teardown race it.
    WaitForWalDrained(store, "PERFDB", BASE_TS - 1);
}

TEST_F(XStorePerfTest, UnaryQueryThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::UnaryInsert_REP seed_rep;
        store.UnaryInsert(BASE_TS + i, PerfRow(i), "PERFDB", &seed_rep);
    }

    XStore_Proto::UnaryQuery_REP query_rep;
    auto start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < n; i++) {
        store.UnaryQuery(BASE_TS + i, "PERFDB", &query_rep);
    }
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(query_rep.data().fielddata_size(), 3) << "last query returned unexpected column count";
    ReportPerf({"UnaryQuery", n, elapsed});
}

// ---------------------------------------------------------------------
// Unary Insert RND -- same one-row-per-call shape as UnaryInsertThroughput
// above, but timestamps are drawn uniformly at random (no replacement) from
// a multi-year span instead of a sequential run -- mirrors XBench's
// UNARY_RAND workload (Parquet_OPS.py: random.sample(range(0, numRows), N)
// over a ~10 year span), which is what the reported real-world regression
// was measured against. Every insert here (after the first ~SpanYears of
// them) has a real chance of landing in a different year file than the
// previous call, unlike the sequential test where every call after the
// first shares the same file -- so this is the shape that actually
// exercises the per-call TargetFile path build/cache and FileLockRegistry
// lookup, not just the pwrite() cost.
// ---------------------------------------------------------------------

namespace {

// Unique random timestamps across [BaseTs, BaseTs + SpanSeconds), fixed seed
// for reproducibility across variant runs (comparing optimization variants
// requires the exact same insert order/spread each time). Collision retry
// via unordered_set is fine here: N is always a small fraction of
// SpanSeconds, so collision odds are negligible.
std::vector<unsigned long long> UniqueRandomTimestamps(unsigned long long base_ts, unsigned long long span_seconds, unsigned long long n) {
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<unsigned long long> dist(0, span_seconds - 1);
    std::unordered_set<unsigned long long> seen;
    std::vector<unsigned long long> out;
    out.reserve(n);
    while (out.size() < n) {
        unsigned long long candidate = base_ts + dist(rng);
        if (seen.insert(candidate).second) {
            out.push_back(candidate);
        }
    }
    return out;
}

}  // namespace

TEST_F(XStorePerfTest, UnaryInsertRandomThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    // 2020-01-01T00:00:00Z, spanning 5 years -- several distinct year files
    // get touched, same as the real UNARY_RAND workload's multi-year spread.
    constexpr unsigned long long rand_base_ts = 1577836800ULL;
    constexpr unsigned long long span_seconds = 5ULL * 365 * 86'400;
    std::vector<unsigned long long> timestamps = UniqueRandomTimestamps(rand_base_ts, span_seconds, n);

    // Best-of-3, OS page cache for PERFDB's files dropped before every timed
    // pass -- isolates steady-state per-row cost (path build, lock registry
    // lookup, pwrite) from one-shot OS readahead/writeback state, same
    // methodology as RunMinAggregateCompare's cold runs below.
    constexpr int k_rnd_repeats = 3;
    double best = std::numeric_limits<double>::max();
    XStore_Proto::UnaryInsert_REP rep;
    for (int iter = 0; iter < k_rnd_repeats; iter++) {
        DropPageCacheForDb("PERFDB");
        auto start = std::chrono::steady_clock::now();
        for (unsigned long long i = 0; i < n; i++) {
            store.UnaryInsert(timestamps[i], PerfRow(i), "PERFDB", &rep);
        }
        double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
        best = std::min(best, elapsed);
    }

    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << "last insert failed: " << rep.insertstatus();

    // Correctness: spot-check every Nth row round-trips through UnaryQuery
    // exactly as inserted -- catches a broken path-cache/lock-registry
    // variant silently writing to (or reading back from) the wrong file.
    const unsigned long long check_stride = std::max<unsigned long long>(1, n / 500);
    for (unsigned long long i = 0; i < n; i += check_stride) {
        XStore_Proto::UnaryQuery_REP query_rep;
        store.UnaryQuery(timestamps[i], "PERFDB", &query_rep);
        ASSERT_EQ(query_rep.data().fielddata_size(), 3) << "row " << i << " (ts=" << timestamps[i] << ") missing/corrupt after RND insert";
        ASSERT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), (double)i) << "row " << i << " (ts=" << timestamps[i] << ") wrong data -- wrote to/read from wrong file";
    }

    ReportPerf({"UnaryInsertRND", n, best});
}

// Concurrency variant: T worker threads, each its own XStore instance (own
// thread_local caches, matching ZMQ_Server::WorkerRoutine's one-instance-
// per-thread model), all inserting into the SAME db concurrently with
// non-overlapping random timestamps. Isolates FileLockRegistry/fd-shard
// contention specifically -- the single-threaded test above can't surface
// that, since a lone thread never contends with itself.
TEST_F(XStorePerfTest, UnaryInsertRandomConcurrentThroughput) {
    XStore setup;
    CreateTestDb(setup, "PERFDB", XStore_Proto::SECOND, 64);

    const unsigned long long n = PerfOpCount(200'000);
    constexpr unsigned int num_threads = 8;
    constexpr unsigned long long rand_base_ts = 1577836800ULL;
    // 30 distinct year files, well past FdCacheCap (8, process-wide) --
    // guarantees the fd cache actually evicts under this test's concurrency
    // instead of every touched file coincidentally fitting in the cache, so
    // this reliably exercises (and regresses against) the eviction-vs.
    // in-flight-use race GetOrOpenFile/GetOrMapFile guard against.
    constexpr unsigned long long span_seconds = 30ULL * 365 * 86'400;
    std::vector<unsigned long long> timestamps = UniqueRandomTimestamps(rand_base_ts, span_seconds, n);

    DropPageCacheForDb("PERFDB");

    std::vector<std::thread> workers;
    std::vector<XStore_Proto::UnaryInsert_REP> reps(num_threads);
    auto start = std::chrono::steady_clock::now();
    for (unsigned int t = 0; t < num_threads; t++) {
        workers.emplace_back([&, t]() {
            XStore thread_store;
            for (unsigned long long i = t; i < n; i += num_threads) {
                thread_store.UnaryInsert(timestamps[i], PerfRow(i), "PERFDB", &reps[t]);
            }
        });
    }
    for (auto &w : workers) w.join();
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    for (unsigned int t = 0; t < num_threads; t++) {
        ASSERT_NE(reps[t].insertstatus().find("[INFO]"), std::string::npos) << "thread " << t << " last insert failed: " << reps[t].insertstatus();
    }

    // Correctness: every row round-trips, and (critically for a concurrency
    // bug) no thread's writes clobbered another's -- each timestamp is
    // unique across threads by construction (interleaved partition of
    // Timestamps), so any cross-talk would show up as a wrong value here.
    XStore verify;
    const unsigned long long check_stride = std::max<unsigned long long>(1, n / 500);
    for (unsigned long long i = 0; i < n; i += check_stride) {
        XStore_Proto::UnaryQuery_REP query_rep;
        verify.UnaryQuery(timestamps[i], "PERFDB", &query_rep);
        ASSERT_EQ(query_rep.data().fielddata_size(), 3) << "row " << i << " (ts=" << timestamps[i] << ") missing/corrupt after concurrent RND insert";
        ASSERT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), (double)i) << "row " << i << " (ts=" << timestamps[i] << ") wrong data -- cross-thread clobber";
    }

    ReportPerf({"UnaryInsertRND[concurrent T=" + std::to_string(num_threads) + "]", n, elapsed});
}

// ---------------------------------------------------------------------
// CPU-only microbenchmark for CachedDbBasePath/CachedYearFilePath (see
// XStore.h/XStore_Management.cpp) -- isolates their allocation/lookup cost
// from the disk pwrite()/pread() that dominates (and adds heavy run-to-run
// variance to, per this box's ~30-50% swings observed in the end-to-end RND
// tests above) every real Unary/Batch/Range op. No XStore instance, no
// disk I/O, nothing but the exact caching pattern those private methods use
// (reproduced here rather than exposing them via a friend declaration just
// for a benchmark) versus the raw JoinPath/fmt::format they replace.
// ---------------------------------------------------------------------

namespace {

const std::string& BenchCachedDbBasePath(const std::string &db_name) {
    thread_local std::unordered_map<std::string, std::string> tls_base_paths;
    auto it = tls_base_paths.find(db_name);
    if (it != tls_base_paths.end()) return it->second;
    return tls_base_paths.emplace(db_name, "/fake/DataStore/" + db_name).first->second;
}

const std::string& BenchCachedYearFilePath(const std::string &db_base_path, unsigned short year) {
    thread_local std::unordered_map<std::string, std::unordered_map<unsigned short, std::string>> tls_year_paths;
    std::unordered_map<unsigned short, std::string> &year_map = tls_year_paths[db_base_path];
    auto it = year_map.find(year);
    if (it != year_map.end()) return it->second;
    return year_map.emplace(year, fmt::format("{}/{}.XSTORE", db_base_path, year)).first->second;
}

}  // namespace

TEST(YearPathCacheMicrobench, HashmapCacheBeatsRawRebuildUnderBoundedRandomYearAccess) {
    const unsigned long long n = PerfOpCount(2'000'000);
    constexpr int k_years = 30;  // matches the RND tests' 30-year span above
    const std::string db_name = "PERFDB";

    std::mt19937 rng(7);
    std::uniform_int_distribution<int> year_dist(0, k_years - 1);
    std::vector<unsigned short> years(n);
    for (auto &y : years) y = static_cast<unsigned short>(2000 + year_dist(rng));

    volatile size_t sink = 0;  // keeps the optimizer from eliding the calls

    // Warm the cache once (untimed) so the timed pass measures steady-state
    // hit cost, not KYears' one-time misses -- this IS the behavior a real
    // worker thread gets after its first pass over a bounded year range.
    for (int y = 0; y < k_years; y++) {
        sink += BenchCachedYearFilePath(BenchCachedDbBasePath(db_name), static_cast<unsigned short>(2000 + y)).size();
    }

    auto cached_start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < n; i++) {
        sink += BenchCachedYearFilePath(BenchCachedDbBasePath(db_name), years[i]).size();
    }
    double cached_elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - cached_start).count();

    // Raw equivalent: what every call paid before these caches existed --
    // JoinPath(data_store_dir, dbName) reproduced inline (JoinPath itself is
    // private) followed by fmt::format for the year file, same as
    // DbBasePath()+CachedYearFilePath's miss path.
    auto raw_start = std::chrono::steady_clock::now();
    for (unsigned long long i = 0; i < n; i++) {
        std::string raw_base_path = "/fake/DataStore/" + db_name;
        sink += fmt::format("{}/{}.XSTORE", raw_base_path, years[i]).size();
    }
    double raw_elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - raw_start).count();

    ASSERT_GT(sink, 0u);

    double speedup_pct = raw_elapsed > 0 ? 100.0 * (raw_elapsed - cached_elapsed) / raw_elapsed : 0.0;
    ReportPerf({"YearPathCache[hashmap-cached]", n, cached_elapsed});
    ReportPerf({"YearPathCache[raw-rebuild]", n, raw_elapsed});
    std::cout << "[PERF] YearPathCache: hashmap-cached vs raw rebuild, " << k_years << "-year bounded RND access ("
              << n << " calls): " << std::fixed << std::setprecision(1) << speedup_pct << "% "
              << (speedup_pct >= 0 ? "faster" : "slower") << std::endl;
}

// ---------------------------------------------------------------------
// Batch Insert / Query -- many rows in a single call, the shape of a bulk
// load/export in production.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, BatchInsertThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }

    XStore_Proto::BatchInsert_REP rep;
    auto start = std::chrono::steady_clock::now();
    store.BatchInsert(&req, &rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    EXPECT_NE(rep.insertstatus().find(fmt::format("Inserted {} elements", n)), std::string::npos) << rep.insertstatus();
    ReportPerf({"BatchInsert", n, elapsed});
    WaitForWalDrained(store, "PERFDB", BASE_TS - 1);   // ASYNC times the enqueue only; see UnaryInsertThroughput_Async
}

// Same as BatchInsertThroughput but SYNC: the call returns only once every row is WAL-durable AND applied, so this
// (not the ASYNC one, which times enqueue only) is the meaningful durable-write number.
TEST_F(XStorePerfTest, BatchInsertThroughput_Sync) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("PERFDB");
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }

    XStore_Proto::BatchInsert_REP rep;
    auto start = std::chrono::steady_clock::now();
    store.BatchInsert(&req, &rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    EXPECT_NE(rep.insertstatus().find(fmt::format("Inserted {} elements", n)), std::string::npos) << rep.insertstatus();
    ReportPerf({"BatchInsert[SYNC]", n, elapsed});
}

TEST_F(XStorePerfTest, BatchQueryThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("PERFDB");
    // SYNC: the default ASYNC returns before the rows are applied, so the query below would see none of them
    // (and the late apply would race ScratchDir teardown).
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < n; i++) query_req.add_timestamp(BASE_TS + i);

    XStore_Proto::BatchQuery_REP query_rep;
    auto start = std::chrono::steady_clock::now();
    query_rep = BatchQuery(store, &query_req);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(query_rep.queryresponse_size(), (int)n) << "batch query lost/gained rows";
    ReportPerf({"BatchQuery", n, elapsed});
}

// ---------------------------------------------------------------------
// Range Insert / Query -- many rows in a single call like Batch above, but
// addressed by a contiguous [start, end] span instead of an explicit
// timestamp list (and holes come back as empty entries instead of being
// omitted) -- a distinct code path worth its own throughput number.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, RangeInsertThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::RangeInsert_REQ req;
    req.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::RangeInsert_DATA *entry = req.add_rangeinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }

    XStore_Proto::RangeInsert_REP rep;
    auto start = std::chrono::steady_clock::now();
    store.RangeInsert(&req, &rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    EXPECT_NE(rep.insertstatus().find(fmt::format("Inserted {} elements", n)), std::string::npos) << rep.insertstatus();
    ReportPerf({"RangeInsert", n, elapsed});
    WaitForWalDrained(store, "PERFDB", BASE_TS - 1);   // ASYNC times the enqueue only; see UnaryInsertThroughput_Async
}

// Same as RangeInsertThroughput but SYNC (see BatchInsertThroughput_Sync).
TEST_F(XStorePerfTest, RangeInsertThroughput_Sync) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::RangeInsert_REQ req;
    req.set_dbname("PERFDB");
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::RangeInsert_DATA *entry = req.add_rangeinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }

    XStore_Proto::RangeInsert_REP rep;
    auto start = std::chrono::steady_clock::now();
    store.RangeInsert(&req, &rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    EXPECT_NE(rep.insertstatus().find(fmt::format("Inserted {} elements", n)), std::string::npos) << rep.insertstatus();
    ReportPerf({"RangeInsert[SYNC]", n, elapsed});
}

TEST_F(XStorePerfTest, RangeQueryThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_dbname("PERFDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);   // see BatchQueryThroughput
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::RangeQuery_REP query_rep;
    auto start = std::chrono::steady_clock::now();
    query_rep = RangeQuery(store, BASE_TS, BASE_TS + n - 1, "PERFDB");
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(query_rep.queryresponse_size(), (int)n) << "range query lost/gained rows";
    ReportPerf({"RangeQuery", n, elapsed});
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
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("PERFDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);   // setup only: rows must be applied before the timed scan (see BatchQueryThroughput)
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::Min_REP min_rep;
    auto start = std::chrono::steady_clock::now();
    store.MinAggregate(BASE_TS, BASE_TS + n - 1, "PERFDB", 0, &min_rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 0.0);
    ReportPerf({"MinAggregate", n, elapsed});
}

TEST_F(XStorePerfTest, MaxAggregateThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("PERFDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);   // setup only: rows must be applied before the timed scan (see BatchQueryThroughput)
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::Max_REP max_rep;
    auto start = std::chrono::steady_clock::now();
    store.MaxAggregate(BASE_TS, BASE_TS + n - 1, "PERFDB", 0, &max_rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(max_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), (double)(n - 1));
    ReportPerf({"MaxAggregate", n, elapsed});
}

TEST_F(XStorePerfTest, SumAggregateThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("PERFDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);   // setup only: rows must be applied before the timed scan (see BatchQueryThroughput)
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::Sum_REP sum_rep;
    auto start = std::chrono::steady_clock::now();
    store.SumAggregate(BASE_TS, BASE_TS + n - 1, "PERFDB", 0, &sum_rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(sum_rep.opstatus(), "SUCCESS");
    ReportPerf({"SumAggregate", n, elapsed});
}

TEST_F(XStorePerfTest, AvgAggregateThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(1'000'000);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("PERFDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);   // setup only: rows must be applied before the timed scan (see BatchQueryThroughput)
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::Avg_REP avg_rep;
    auto start = std::chrono::steady_clock::now();
    store.AvgAggregate(BASE_TS, BASE_TS + n - 1, "PERFDB", 0, &avg_rep);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_EQ(avg_rep.opstatus(), "SUCCESS");
    ReportPerf({"AvgAggregate", n, elapsed});
}

// ---------------------------------------------------------------------
// FindByPattern -- TEMPORARY IMPLEMENTATION (option A, brute-force scan).
// Baseline for the A vs. B (per-block trigram bloom) vs. C (inverted index)
// decision (TODO.md): rows scanned/sec per pattern shape, warm and cold
// page cache. "rows" here = rows in the scanned range, not matches.
// ---------------------------------------------------------------------

TEST_F(XStorePerfTest, FindByPatternScanThroughput) {
    XStore store;
    CreateTestDb(store, "PERFPATTERN", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    static const char *const SYMBOLS[] = {"AAPL", "MSFT", "GOOG", "AMZN", "META", "NVDA", "TSLA", "ORCL"};

    // Load: BatchInsert in ~10k-row batches (SYNC durability) instead of one
    // UnaryInsert call per row -- a calibration run at XSTORE_PERF_N=200000
    // showed the per-row UnaryInsert loop (as in the original brief) would
    // extrapolate to well over 10 minutes for the full 1,000,000-row load,
    // which would dominate this test's wall time before the timed query
    // cases even start. BatchInsert with explicit SYNC keeps the same
    // durability guarantee (rows applied before the timed queries run) while
    // batching the WAL append cost; row content/timestamps are unchanged
    // from the brief (BASE_TS + i, "<SYMBOL>-order-<i>").
    constexpr unsigned long long kBatchSize = 10'000;
    for (unsigned long long batch_start = 0; batch_start < n; batch_start += kBatchSize) {
        unsigned long long batch_end = std::min(batch_start + kBatchSize, n);
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname("PERFPATTERN");
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (unsigned long long i = batch_start; i < batch_end; i++) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(BASE_TS + i);
            *entry->mutable_data() = MakeRow({FDouble(static_cast<double>(i)), FString(fmt::format("{}-order-{}", SYMBOLS[i % 8], i))});
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        // Match the success form specifically ("...elements successfully") --
        // the failure message also contains "Inserted {N} elements" as a
        // prefix ("...elements, but the write failed for at least one
        // group", see XStore_BatchOps.cpp), so a substring match on just
        // that much would pass on failure too.
        ASSERT_NE(rep.insertstatus().find(fmt::format("Inserted {} elements successfully", batch_end - batch_start)), std::string::npos) << rep.insertstatus();
    }

    // Correctness guard -- so the perf numbers below can't be silently
    // meaningless: confirm the loaded data actually has the shape the perf
    // cases assume (1/8 of rows are AAPL, a known no-match pattern matches
    // nothing) before trusting any timing.
    {
        std::string buffer;
        store.FindByPattern(BASE_TS, BASE_TS + n - 1, "PERFPATTERN", 1, "AAPL%", XStore_Proto::PATTERN_LIKE, false,
                            XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                            [&](std::string &&chunk, bool) { buffer += chunk; });
        XStore_Proto::FindByPattern_REP rep;
        ASSERT_TRUE(rep.ParseFromString(buffer)) << "FindByPattern reply failed to parse";
        if (n % 8 == 0) {
            ASSERT_EQ(rep.queryresponse_size(), (int)(n / 8)) << "AAPL% match count wrong -- data load or scan is broken";
        }

        std::string no_match_buffer;
        store.FindByPattern(BASE_TS, BASE_TS + n - 1, "PERFPATTERN", 1, "%ZZZZ%", XStore_Proto::PATTERN_LIKE, false,
                            XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                            [&](std::string &&chunk, bool) { no_match_buffer += chunk; });
        XStore_Proto::FindByPattern_REP no_match_rep;
        ASSERT_TRUE(no_match_rep.ParseFromString(no_match_buffer)) << "FindByPattern no-match reply failed to parse";
        ASSERT_EQ(no_match_rep.queryresponse_size(), 0) << "no-match pattern returned rows -- scan is broken";
    }

    struct Case {
        const char *name;
        const char *pattern;
        XStore_Proto::PatternMode mode;
        bool case_insensitive;
    };
    std::vector<Case> cases = {
        {"LIKE prefix 'AAPL%'", "AAPL%", XStore_Proto::PATTERN_LIKE, false},
        {"LIKE suffix '%99'", "%99", XStore_Proto::PATTERN_LIKE, false},
        {"LIKE substring '%order-1234%'", "%order-1234%", XStore_Proto::PATTERN_LIKE, false},
        {"LIKE common literal '%order%'", "%order%", XStore_Proto::PATTERN_LIKE, false},
        {"LIKE no-match '%ZZZZ%'", "%ZZZZ%", XStore_Proto::PATTERN_LIKE, false},
        {"ILIKE '%aapl%'", "%aapl%", XStore_Proto::PATTERN_LIKE, true},
    };
#ifdef XSTORE_HAVE_RE2
    cases.push_back({"REGEX '^(AAPL|MSFT)-order-[0-9]+7$'", "^(AAPL|MSFT)-order-[0-9]+7$", XStore_Proto::PATTERN_REGEX, false});
#endif

    auto run = [&](const Case &c) {
        size_t bytes = 0;
        store.FindByPattern(BASE_TS, BASE_TS + n - 1, "PERFPATTERN", 1, c.pattern, c.mode, c.case_insensitive,
                            XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                            [&](std::string &&chunk, bool) { bytes += chunk.size(); });
        return bytes;
    };

    // Same prefix RemoveDB passes to the private EraseMappedFilesUnderPrefix
    // (db_loc + "/", where db_loc = JoinPath(data_store_dir, dbName)) --
    // db_name is already upper-cased ("PERFPATTERN" has no lowercase chars
    // to begin with).
    const std::string mmap_prefix = store.data_store_dir + "/PERFPATTERN/";

    for (const auto &c : cases) {
        // Drop this process's own warm mapping FIRST, then the page cache:
        // posix_fadvise(DONTNEED) (what DropPageCacheForDb uses) cannot
        // evict a page that's still backing a live mapping in this process
        // -- and by this point the untimed correctness guard above has
        // already faulted the whole file's pages in via GetOrMapFile's
        // process-wide cache. Without evicting the mapping first, every
        // "cold" run below would actually run warm.
        store.EvictMappedFilesUnderPrefixForTests(mmap_prefix);
        DropPageCacheForDb("PERFPATTERN");
        auto start = std::chrono::steady_clock::now();
        run(c);
        ReportPerf({fmt::format("FindByPattern cold {}", c.name), n,
                    std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count()});

        start = std::chrono::steady_clock::now();
        size_t reply_bytes = run(c);
        ReportPerf({fmt::format("FindByPattern warm {}", c.name), n,
                    std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count()});
        std::cout << "[PERF]   reply_bytes=" << reply_bytes << std::endl;
    }

    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cout << "[PERF] FindByPattern process peak RSS=" << usage.ru_maxrss << " KiB "
              << "(includes the fixture's inserts and the 64 MiB streamed-chunk reserve per query)" << std::endl;
}
