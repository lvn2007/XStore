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
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <unistd.h>
#include <unordered_set>
#include <vector>

#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_LiveWidenGovernor.h"
#include "include/XStore/XStore_LiveWidenLoadSource.h"
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

int EnvInt(const char *name, int fallback) {
    const char *v = std::getenv(name);
    return v != nullptr && std::atoi(v) > 0 ? std::atoi(v) : fallback;
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
    // UnaryInsert's implicit durability default is SYNC (one fdatasync per call, ~330 ops/s single-client), so
    // 200k would take ~10 minutes: same smaller default as UnaryInsertThroughput_Sync. XSTORE_PERF_N overrides.
    const unsigned long long n = PerfOpCount(5'000);

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

// Same as BatchInsertThroughput_Sync but scattered, the XBench BATCH_RAND shape: SYNC calls of `rows_per_call` random
// distinct rows each (fixed seed, no duplicates), all within one year, so every call is one WAL record of that many
// isolated runs.
constexpr unsigned long long kScatterYearStart = 1704067200ULL;   // 2024-01-01 00:00:00 UTC
constexpr unsigned long long kScatterYearSeconds = 366ULL * 86400;

// The SYNC BatchInsert requests of the scattered tests, `n` rows in all.
static std::vector<XStore_Proto::BatchInsert_REQ> ScatteredSyncRequests(unsigned long long n, unsigned long long rows_per_call) {
    std::mt19937_64 rng(20261018);
    std::unordered_set<unsigned long long> used;
    std::vector<unsigned long long> rows;
    rows.reserve(n);
    while (rows.size() < n) {
        const unsigned long long r = rng() % kScatterYearSeconds;
        if (used.insert(r).second) rows.push_back(r);
    }
    std::vector<XStore_Proto::BatchInsert_REQ> reqs;
    for (unsigned long long from = 0; from < n; from += rows_per_call) {
        XStore_Proto::BatchInsert_REQ &req = reqs.emplace_back();
        req.set_dbname("PERFDB");
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (unsigned long long i = from; i < std::min(n, from + rows_per_call); i++) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(kScatterYearStart + rows[i]);
            *entry->mutable_data() = PerfRow(i);
        }
    }
    return reqs;
}

static void RunScatteredBatchInsertSync(const char *label, unsigned long long rows_per_call) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    // Distinct rows of one year: more than the year holds would never be found.
    const unsigned long long n = std::min(PerfOpCount(200'000), kScatterYearSeconds);
    std::vector<XStore_Proto::BatchInsert_REQ> reqs = ScatteredSyncRequests(n, rows_per_call);

    unsigned long long failed = 0;
    auto start = std::chrono::steady_clock::now();
    for (XStore_Proto::BatchInsert_REQ &req : reqs) {
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        if (rep.insertstatus().find("successfully") == std::string::npos) ++failed;
    }
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    EXPECT_EQ(failed, 0u);
    ReportPerf({label, n, elapsed});
}

// 110 isolated runs per record: under WAL_APPLY_MAX_RUN_LOCKS, so every run is locked on its own.
TEST_F(XStorePerfTest, BatchInsertScatteredThroughput_Sync) {
    RunScatteredBatchInsertSync("BatchInsertScattered[SYNC]", 110);
}

// 1000 isolated runs per record (the XBench BATCH_RAND record size): under the cap (4096) too, so per-run locks.
TEST_F(XStorePerfTest, BatchInsertScatteredLargeThroughput_Sync) {
    RunScatteredBatchInsertSync("BatchInsertScatteredLarge[SYNC]", 1000);
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

TEST_F(XStorePerfTest, BatchDeleteThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = PerfOpCount(200'000);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("PERFDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);   // see BatchQueryThroughput
    for (unsigned long long i = 0; i < n; i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(BASE_TS + i);
        *entry->mutable_data() = PerfRow(i);
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);
    ASSERT_NE(insert_rep.insertstatus().find("successfully"), std::string::npos) << insert_rep.insertstatus();

    XStore_Proto::BatchDelete_REQ delete_req;
    delete_req.set_dbname("PERFDB");
    for (unsigned long long i = 0; i < n; i++) delete_req.add_timestamp(BASE_TS + i);   // ascending, as a client sends it

    auto start = std::chrono::steady_clock::now();
    XStore_Proto::BatchDelete_REP delete_rep = store.BatchDelete(&delete_req);
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    ASSERT_NE(delete_rep.deletestatus().find(fmt::format("Deleted {} items", n)), std::string::npos) << delete_rep.deletestatus();
    ReportPerf({"BatchDelete", n, elapsed});
}

// BatchDelete of scattered rows: the n rows of the XBench BATCH_RAND shape (ScatteredSyncRequests) are inserted, then
// deleted in disjoint random subsets of XSTORE_PERF_BATCH_DELETE_ROWS (default 300) rows (fixed seed, ascending within a
// call), so every call is that many isolated runs: BATCH_DELETE_MAX_RUN_LOCKS is 1 today (the span lock); a build with a
// per-run BatchDelete (cap >= 300) locks run by run. Request building stays outside the timed window.
TEST_F(XStorePerfTest, BatchDeleteScatteredThroughput) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    const unsigned long long n = std::min(PerfOpCount(200'000), kScatterYearSeconds);
    const unsigned long long kRowsPerCall = static_cast<unsigned long long>(EnvInt("XSTORE_PERF_BATCH_DELETE_ROWS", 300));
    std::vector<unsigned long long> rows;
    rows.reserve(n);
    for (XStore_Proto::BatchInsert_REQ &req : ScatteredSyncRequests(n, kRowsPerCall)) {
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
        for (const XStore_Proto::BatchInsert_DATA &entry : req.batchinsert_data()) rows.push_back(entry.timestamp());
    }
    std::mt19937_64 rng(20261019);
    std::shuffle(rows.begin(), rows.end(), rng);

    std::vector<XStore_Proto::BatchDelete_REQ> reqs;
    for (unsigned long long from = 0; from < n; from += kRowsPerCall) {
        const unsigned long long to = std::min(n, from + kRowsPerCall);
        std::sort(rows.begin() + static_cast<std::ptrdiff_t>(from), rows.begin() + static_cast<std::ptrdiff_t>(to));
        XStore_Proto::BatchDelete_REQ &req = reqs.emplace_back();
        req.set_dbname("PERFDB");
        for (unsigned long long i = from; i < to; i++) req.add_timestamp(rows[i]);
    }

    std::vector<std::string> wants;
    for (const XStore_Proto::BatchDelete_REQ &req : reqs) wants.push_back(fmt::format("Deleted {} items", req.timestamp_size()));

    unsigned long long failed = 0;
    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < reqs.size(); i++) {
        XStore_Proto::BatchDelete_REP rep = store.BatchDelete(&reqs[i]);
        if (rep.deletestatus().find(wants[i]) == std::string::npos) ++failed;
    }
    double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    EXPECT_EQ(failed, 0u);
    ReportPerf({"BatchDeleteScattered", n, elapsed});
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
        {"ILIKE selective '%OrDeR-1234%'", "%OrDeR-1234%", XStore_Proto::PATTERN_LIKE, true},
        {"ILIKE no-match '%zzzz%'", "%zzzz%", XStore_Proto::PATTERN_LIKE, true},
        // Short and ubiquitous literals: where a prefilter can only add its per-row cost.
        {"LIKE 1-byte literal in every row '%e%'", "%e%", XStore_Proto::PATTERN_LIKE, false},
        {"LIKE 2-byte literal '%-1%'", "%-1%", XStore_Proto::PATTERN_LIKE, false},
        {"ILIKE 1-byte literal in every row '%E%'", "%E%", XStore_Proto::PATTERN_LIKE, true},
        {"ILIKE 2-byte literal in every row '%R-%'", "%R-%", XStore_Proto::PATTERN_LIKE, true},
    };
#ifdef XSTORE_HAVE_RE2
    cases.push_back({"REGEX '^(AAPL|MSFT)-order-[0-9]+7$'", "^(AAPL|MSFT)-order-[0-9]+7$", XStore_Proto::PATTERN_REGEX, false});
    // Selective regexes with a required literal ("order-1234" / "ZZZZ") for the prefilter.
    cases.push_back({"REGEX selective 'order-1234[0-9]'", "order-1234[0-9]", XStore_Proto::PATTERN_REGEX, false});
    cases.push_back({"REGEX selective 'order-1234\\d+$'", "order-1234\\d+$", XStore_Proto::PATTERN_REGEX, false});
    cases.push_back({"REGEX no-match 'ZZZZ.*'", "ZZZZ.*", XStore_Proto::PATTERN_REGEX, false});
    cases.push_back({"REGEX 2-byte literal in every row 'r-[0-9]'", "r-[0-9]", XStore_Proto::PATTERN_REGEX, false});
    cases.push_back({"REGEX 1-byte literal in every row 'e.*[0-9]'", "e.*[0-9]", XStore_Proto::PATTERN_REGEX, false});
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

// ---------------------------------------------------------------------
// Startup index rebuild throughput (Tech Reference 4.14 / 4.19): a cold
// InitDB() over a DB whose .zmap/.catidx sidecars were deleted rebuilds
// both from the year files. Fixture: a 72-column row (60 doubles, 6
// int64, 6 short categorical strings; the DB's tracked width is 72),
// inserted dense, then a clean Shutdown(), then per mode the sidecars are
// deleted and InitDB() is timed ("cold"), followed by a second InitDB()
// over the now-clean indexes ("clean"). Modes: both indexes stale, only
// the zone map, only the catidx -- so the two halves can be told apart.
// Per phase it also reports process CPU time and /proc/self/io syscall
// counts (no perf needed). Page cache is WARM (the data was just
// written). XSTORE_PERF_N overrides the total row count (default 500k for
// one year, 600k for four years); XSTORE_REBUILD_MODES (e.g. "both")
// limits the modes.
// ---------------------------------------------------------------------

namespace {

constexpr short kRebuildRowLength = 1024;
constexpr int kRebuildColumns = 72;

XStore_Proto::RowData RebuildWideRow(unsigned long long i) {
    XStore_Proto::RowData row;
    for (int c = 0; c < kRebuildColumns; ++c) {
        XStore_Proto::FData *f = row.add_fielddata();
        if (c % 12 == 11) {
            f->set_stringdata("cat" + std::to_string((i + static_cast<unsigned long long>(c)) % 40));
        } else if (c % 12 == 10) {
            f->set_int64data(static_cast<int64_t>(i * 7 + static_cast<unsigned long long>(c)));
        } else {
            f->set_doubledata(static_cast<double>(i) * 0.25 + c);
        }
    }
    return row;
}

struct ProcCounters {
    double user_s = 0, sys_s = 0;
    unsigned long long syscr = 0, syscw = 0, rchar = 0, wchar = 0;
};

ProcCounters ReadProcCounters() {
    ProcCounters out;
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    out.user_s = static_cast<double>(usage.ru_utime.tv_sec) + static_cast<double>(usage.ru_utime.tv_usec) / 1e6;
    out.sys_s = static_cast<double>(usage.ru_stime.tv_sec) + static_cast<double>(usage.ru_stime.tv_usec) / 1e6;
    std::ifstream io("/proc/self/io");
    std::string key;
    unsigned long long value = 0;
    while (io >> key >> value) {
        if (key == "rchar:") out.rchar = value;
        else if (key == "wchar:") out.wchar = value;
        else if (key == "syscr:") out.syscr = value;
        else if (key == "syscw:") out.syscw = value;
    }
    return out;
}

bool RebuildModeEnabled(const std::string &mode) {
    const char *env = std::getenv("XSTORE_REBUILD_MODES");
    if (env == nullptr || *env == '\0') return true;
    return std::string(env).find(mode) != std::string::npos;
}

// Inserts rows_per_year rows into each year starting at its epoch (SYNC batches), then a clean Shutdown.
void SeedRebuildFixture(XStore &store, const std::string &db, const std::vector<unsigned long long> &year_epochs,
                        unsigned long long rows_per_year) {
    CreateTestDb(store, db, XStore_Proto::SECOND, kRebuildRowLength);
    constexpr unsigned long long kBatch = 20'000;
    for (unsigned long long year_epoch : year_epochs) {
        for (unsigned long long start = 0; start < rows_per_year; start += kBatch) {
            const unsigned long long end = std::min(start + kBatch, rows_per_year);
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(db);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (unsigned long long i = start; i < end; ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(year_epoch + i);
                *entry->mutable_data() = RebuildWideRow(i);
            }
            XStore_Proto::BatchInsert_REP rep;
            store.BatchInsert(&req, &rep);
            ASSERT_NE(rep.insertstatus().find(fmt::format("Inserted {} elements successfully", end - start)), std::string::npos)
                << rep.insertstatus();
        }
    }
    XStore::Shutdown();            // final checkpoint + MarkClean: the next InitDB replays nothing
    XStore::ResetShutdownForTests();
}

void RunRebuildThroughput(const std::string &label, const std::vector<unsigned long long> &year_epochs,
                          unsigned long long rows_per_year) {
    XStore store;
    const std::string db = "REBUILDPERF";
    SeedRebuildFixture(store, db, year_epochs, rows_per_year);
    if (::testing::Test::HasFatalFailure()) return;
    const fs::path db_dir = fs::current_path() / "DataStore" / db;
    const unsigned long long total_rows = rows_per_year * year_epochs.size();

    for (const std::string mode : {"both", "zmap", "catidx"}) {
        if (!RebuildModeEnabled(mode)) continue;
        // A fresh process has empty registries; the sidecars are then gone from disk.
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        for (const auto &entry : fs::directory_iterator(db_dir)) {
            const std::string ext = entry.path().extension().string();
            if ((ext == ".zmap" && mode != "catidx") || (ext == ".catidx" && mode != "zmap")) fs::remove(entry.path());
        }
        for (const char *phase : {"cold", "clean"}) {
            const ProcCounters before = ReadProcCounters();
            const auto start = std::chrono::steady_clock::now();
            store.InitDB();
            const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
            const ProcCounters after = ReadProcCounters();
            ReportPerf({fmt::format("RebuildThroughput[{} {} {}]", label, mode, phase), total_rows, elapsed});
            std::cout << std::fixed << std::setprecision(2) << "[PERF]   cpu_user=" << after.user_s - before.user_s
                      << "s cpu_sys=" << after.sys_s - before.sys_s << "s syscr=" << after.syscr - before.syscr
                      << " syscw=" << after.syscw - before.syscw
                      << " rchar_MiB=" << static_cast<double>(after.rchar - before.rchar) / (1 << 20)
                      << " wchar_MiB=" << static_cast<double>(after.wchar - before.wchar) / (1 << 20) << std::endl;
        }
        // Sanity: the rebuilt indexes answer like the data (MIN of column 0 over the first year is 0).
        XStore_Proto::Min_REP min_rep;
        store.MinAggregate(year_epochs[0], year_epochs[0] + rows_per_year - 1, db, 0, &min_rep);
        ASSERT_EQ(min_rep.opstatus(), "SUCCESS");
        EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 0.0);
    }
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
}

}  // namespace

TEST_F(XStorePerfTest, RebuildThroughput_OneYear) {
    RunRebuildThroughput("1 year", {BASE_TS}, PerfOpCount(500'000));
}

TEST_F(XStorePerfTest, RebuildThroughput_FourYears) {
    // 2020..2023, one year file each, rows starting at Jan 1.
    const std::vector<unsigned long long> years = {1577836800ULL, 1609459200ULL, 1640995200ULL, 1672531200ULL};
    RunRebuildThroughput("4 years", years, PerfOpCount(600'000) / years.size());
}

// ---------------------------------------------------------------------
// LiveWidenDuringInserts (M4 stage 2b): insert latency/throughput with and
// without XStore::LiveWidenOneYear running on the same year, in interleaved
// rounds (A B C, A B C, ...) so slow drift on the VM hits every arm alike.
//   A (no builder):   the year's index is already 100 columns wide.
//   B (widen):        the builder widens the index 64 -> 100, back to back
//                     (an untimed 100 -> 64 rebuild before each widen; calls
//                     made during those resets are not counted).
//   C (control):      the builder rebuilds 100 -> 100, back to back.
// C vs A is the builder's own overhead. B and C are nearly the same workload
// by construction (BeginRebuild sets the new width at once, and in both rows
// >= F skip the index), so "B ~ C" is NOT independent evidence that widening
// is cheap. The builder runs back to back, so the throughput drop is a
// conservative upper bound, not the cost of one widen. The loop is closed (4
// threads, each waits for its call): a slower arm gets fewer samples, and p99
// differences of a few tens of ms are not distinguishable from noise here.
// For p99, prefer 5 rounds x 60 s pooled, with each round's min/max beside
// the median, over longer phases.
// The year holds XSTORE_PERF_N (default 300k) existing 100-column rows;
// 4 writer threads make SYNC calls of 100 rows, alternating a BatchInsert of
// random existing rows (overwrites, which contend with the builder's chunk
// locks) and a "head RangeInsert" of 100 consecutive rows in a 100k-row window
// past the existing rows, rewritten after its first pass so the year does not
// keep growing (round 0 arm A also contains the window's first-pass appends).
// The two kinds are reported separately.
// OPT-IN: skipped unless XSTORE_LW_PHASE_S is set; each phase then lasts that
// many seconds (60 recommended; B and C count only builder time), over
// XSTORE_LW_ROUNDS rounds (default 3: about 12-14 minutes at 60 s).
// XSTORE_LW_WORKERS (default 1) sets the builder's worker count (parallel live widen).
// XSTORE_LW_GOVERNOR=1 runs every timed builder run of B and C through a production load governor (real clock, real
// /proc/stat, the year lock's writer counters; one governor per arm phase, as a pass has one per DB) instead of the
// fixed XSTORE_LW_WORKERS (which the untimed B resets still use; their CPU, all on the calling thread with 1 worker,
// is reported to the governor as builder CPU so it is not taken for foreground load). Every arm also reports the
// year file lock's writer waits (FileRangeLock::WriterWaits deltas over the counted time) and, sampled once per
// second of builder time, their per-second distribution: the governor's writer-brake calibration input. Governed
// arms also print a per-second timeline of the governor (allowed, CPU target, foreground, brake, reason).
// XSTORE_LW_WAIT_BRAKE_US overrides the governed arms' writer-wait brake (calibration only).
// XSTORE_LW_NO_WRITERS=1 starts no writer thread (the builder alone: widen speed with an idle machine).
// ---------------------------------------------------------------------

namespace {

constexpr short kLiveWidenRowLength = 2048;
constexpr int kLiveWidenColumns = 100;

XStore_Proto::RowData LiveWidenRow(unsigned long long i, int columns) {
    XStore_Proto::RowData row;
    for (int c = 0; c < columns; ++c) {
        XStore_Proto::FData *f = row.add_fielddata();
        if (c % 12 == 11) {
            f->set_stringdata("cat" + std::to_string((i + static_cast<unsigned long long>(c)) % 40));
        } else {
            f->set_doubledata(static_cast<double>(i) * 0.25 + c);
        }
    }
    return row;
}

double Quantile(std::vector<double> v, double q) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    return v[std::min(v.size() - 1, static_cast<size_t>(q * static_cast<double>(v.size())))];
}

struct ArmResult {
    double seconds = 0;
    unsigned long long rows = 0;
    std::vector<double> overwrite_us, append_us;
    std::vector<double> holds_us;
    unsigned long long builder_runs = 0, timeouts = 0, yields = 0, yield_us = 0;
    unsigned long long wait_us = 0, waits = 0, long_waits = 0;   // writer waits on the year lock, counted time only
    unsigned long long chunks = 0, hole_chunks = 0;
    // Every builder arm, once per second of builder time: the year lock's writer wait (us/s) and long waits.
    std::vector<double> sec_wait_rate, sec_long;
    // Governed arms: allowed sampled with them, the governor's brakes and grants, parked time, a per-second timeline.
    std::vector<std::string> timeline;
    unsigned allowed_min = 0, allowed_max = 0;
    double allowed_sum = 0;
    unsigned long long allowed_samples = 0, brakes = 0, grants = 0;
    double parked_s = 0;
    unsigned max_workers = 0;
};

std::string LatencyText(const std::vector<double> &v) {
    return fmt::format("n={} p50={:.1f} p99={:.1f} p99.9={:.1f} max={:.1f} ms", v.size(), Quantile(v, 0.5) / 1000,
                       Quantile(v, 0.99) / 1000, Quantile(v, 0.999) / 1000,
                       v.empty() ? 0.0 : *std::max_element(v.begin(), v.end()) / 1000);
}

std::string LoadAvg() {
    std::ifstream in("/proc/loadavg");
    std::string a, b, c;
    in >> a >> b >> c;
    return a + " " + b + " " + c;
}

}  // namespace

TEST_F(XStorePerfTest, LiveWidenDuringInserts) {
    XStore store;
    const std::string db = "LWPERF";
    const unsigned long long year_start = 1704067200ULL; // 2024-01-01
    const unsigned long long existing = PerfOpCount(300'000);
    if (std::getenv("XSTORE_LW_PHASE_S") == nullptr) GTEST_SKIP() << "opt-in: set XSTORE_LW_PHASE_S (e.g. 60)";
    const int phase_s = EnvInt("XSTORE_LW_PHASE_S", 60);
    const int rounds = EnvInt("XSTORE_LW_ROUNDS", 3);
    CreateTestDb(store, db, XStore_Proto::SECOND, kLiveWidenRowLength);
    {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(year_start, LiveWidenRow(0, 10), db, &rep, XStore_Proto::SYNC); // index files born 64 wide
        constexpr unsigned long long kBatch = 20'000;
        for (unsigned long long start = 1; start < existing; start += kBatch) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(db);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (unsigned long long i = start; i < std::min(existing, start + kBatch); ++i) {
                XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                e->set_timestamp(year_start + i);
                *e->mutable_data() = LiveWidenRow(i, kLiveWidenColumns);
            }
            XStore_Proto::BatchInsert_REP brep;
            store.BatchInsert(&req, &brep);
            ASSERT_NE(brep.insertstatus().find("successfully"), std::string::npos) << brep.insertstatus();
        }
    }
    const std::string path = (fs::current_path() / "DataStore" / db).string();
    const unsigned workers = static_cast<unsigned>(EnvInt("XSTORE_LW_WORKERS", 1)); // live widen workers (default 1)
    const bool governed = EnvInt("XSTORE_LW_GOVERNOR", 0) == 1;
    std::cout << "[PERF] LiveWidenDuringInserts workers " << (governed ? std::string("governed") : std::to_string(workers))
              << std::endl;
    FileRangeLock &year_lock = FileLockRegistry::Get(fmt::format("{}/2024.XSTORE", path)); // the writers' lock key
    std::unique_ptr<livewiden::LiveWidenLoadSource> gov_load;
    std::unique_ptr<livewiden::LiveWidenGovernor> gov;
    const auto widen = [&](uint32_t width, XStore::LiveWidenStats *stats, bool use_governor = false) {
        XStore b;
        return b.LiveWidenOneYear(path, 2024, kLiveWidenRowLength, width, true, true, nullptr, stats, workers,
                                  XStoreTunables::LIVE_WIDEN_GROUP_BYTES, use_governor ? gov.get() : nullptr,
                                  use_governor ? gov_load.get() : nullptr);
    };
    const auto add_waits = [&](ArmResult &r, const FileRangeLock::WriterWaitStats &w0) {
        const FileRangeLock::WriterWaitStats w1 = year_lock.WriterWaits();
        r.wait_us += w1.total_us - w0.total_us;
        r.waits += w1.waits - w0.waits;
        r.long_waits += w1.long_waits - w0.long_waits;
    };
    ASSERT_EQ(widen(kLiveWidenColumns, nullptr), XStore::LiveWidenResult::kDone); // arm A starts 100 wide

    // Writers record into the arm `recording` names (-1: not counted).
    std::atomic<int> recording{-1};
    std::atomic<bool> stop{false};
    std::atomic<unsigned long long> head{0};
    std::mutex sink_mutex;
    ArmResult *sink[3] = {nullptr, nullptr, nullptr};
    std::vector<std::thread> writers;
    const int writer_threads = EnvInt("XSTORE_LW_NO_WRITERS", 0) == 1 ? 0 : 4;
    std::cout << "[PERF] LiveWidenDuringInserts writer threads " << writer_threads << std::endl;
    for (int t = 0; t < writer_threads; ++t) {
        writers.emplace_back([&, t] {
            XStore w;
            std::mt19937_64 rng(99 + t);
            unsigned long long n = 0;
            while (!stop.load()) {
                const int arm = recording.load();
                const bool overwrite = n++ % 2 == 0;
                const auto t0 = std::chrono::steady_clock::now();
                if (overwrite) {
                    XStore_Proto::BatchInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(XStore_Proto::SYNC);
                    for (int i = 0; i < 100; ++i) {
                        XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                        const unsigned long long r = 1 + rng() % (existing - 1);
                        e->set_timestamp(year_start + r);
                        *e->mutable_data() = LiveWidenRow(r + n, kLiveWidenColumns);
                    }
                    XStore_Proto::BatchInsert_REP rep;
                    w.BatchInsert(&req, &rep);
                } else {
                    // A head window of 100k rows, rewritten after the first pass, so the year (and every builder run) does
                    // not grow for the whole test.
                    const unsigned long long first = existing + head.fetch_add(100) % 100'000;
                    XStore_Proto::RangeInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(XStore_Proto::SYNC);
                    for (unsigned long long i = first; i < first + 100; ++i) {
                        XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
                        e->set_timestamp(year_start + i);
                        *e->mutable_data() = LiveWidenRow(i, kLiveWidenColumns);
                    }
                    XStore_Proto::RangeInsert_REP rep;
                    w.RangeInsert(&req, &rep);
                }
                const double us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - t0).count();
                if (arm >= 0 && recording.load() == arm) { // a call that straddles a switch is not counted
                    std::lock_guard<std::mutex> lock(sink_mutex);
                    ArmResult &r = *sink[arm];
                    r.rows += 100;
                    (overwrite ? r.overwrite_us : r.append_us).push_back(us);
                }
            }
        });
    }

    const char *kArmNames[3] = {"A no builder (100 wide)", "B widen 64->100", "C control rebuild 100->100"};
    std::vector<std::array<ArmResult, 3>> results(static_cast<size_t>(rounds));
    std::vector<std::string> loads;
    bool all_done = true;
    for (int round = 0; round < rounds; ++round) {
        loads.push_back(LoadAvg());
        for (int arm = 0; arm < 3; ++arm) {
            ArmResult &r = results[static_cast<size_t>(round)][static_cast<size_t>(arm)];
            {
                std::lock_guard<std::mutex> lock(sink_mutex);
                sink[arm] = &r;
            }
            if (arm == 0) {
                const FileRangeLock::WriterWaitStats w0 = year_lock.WriterWaits();
                recording.store(0);
                const auto t0 = std::chrono::steady_clock::now();
                std::this_thread::sleep_for(std::chrono::seconds(phase_s));
                recording.store(-1);
                r.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
                add_waits(r, w0);
                continue;
            }
            std::atomic<bool> phase_over{false};
            if (governed) {
                gov_load = std::make_unique<livewiden::LiveWidenLoadSource>();
                // XSTORE_LW_WAIT_BRAKE_US: try another writer-wait brake (us/s) without changing the tunable.
                livewiden::GovernorParams params;
                params.cores = gov_load->Cores();
                params.label = fmt::format("perf r{} {}", round, arm == 1 ? "B" : "C");
                params.wait_brake_us = static_cast<uint64_t>(EnvInt("XSTORE_LW_WAIT_BRAKE_US",
                                                                    static_cast<int>(params.wait_brake_us)));
                livewiden::LiveWidenLoadSource *src = gov_load.get();
                gov = std::make_unique<livewiden::LiveWidenGovernor>(
                    std::move(params), [src] { return src->Sample(); },
                    [] { return livewiden::LiveWidenGovernor::Clock::now(); }, [src] { return src->Progress(); });
                r.max_workers = gov->MaxWorkers();
                r.allowed_min = std::numeric_limits<unsigned>::max();
            }
            // Once a second; a second counts only if the builder was timed (recording == arm) at both ends.
            std::thread monitor([&, arm] {
                FileRangeLock::WriterWaitStats w0 = year_lock.WriterWaits();
                auto t0 = std::chrono::steady_clock::now();
                bool was_recording = recording.load() == arm;
                double builder_s = 0;
                while (!phase_over.load()) {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                    const FileRangeLock::WriterWaitStats w1 = year_lock.WriterWaits();
                    const auto t1 = std::chrono::steady_clock::now();
                    const bool is_recording = recording.load() == arm;
                    const double dt = std::chrono::duration<double>(t1 - t0).count();
                    if (was_recording && is_recording && dt > 0) {
                        builder_s += dt;
                        const double rate = static_cast<double>(w1.total_us - w0.total_us) / dt;
                        const double lw = static_cast<double>(w1.long_waits - w0.long_waits);
                        std::string line;
                        if (governed) {
                            const auto snap = gov->GetSnapshot();
                            static constexpr const char *kReason[] = {"start", "none", "ramp", "fgcpu", "WAIT", "LONG"};
                            line = fmt::format("t{:.0f} a{} tgt{} fg{:.1f}/{:.1f} lock{:.0f}k lw{:.0f} gov{:.0f}k glw{} {}{}",
                                               builder_s, snap.allowed, snap.cpu_target, snap.fg_tick, snap.fg_cores,
                                               rate / 1000, lw, snap.wait_us_per_s / 1000, snap.long_waits_delta,
                                               kReason[static_cast<int>(snap.reason)], snap.brake_active ? " BRAKE" : "");
                        }
                        std::lock_guard<std::mutex> lock(sink_mutex);
                        r.sec_wait_rate.push_back(rate);
                        r.sec_long.push_back(lw);
                        if (governed) {
                            const unsigned a = gov->GetSnapshot().allowed;
                            r.allowed_min = std::min(r.allowed_min, a);
                            r.allowed_max = std::max(r.allowed_max, a);
                            r.allowed_sum += a;
                            ++r.allowed_samples;
                            r.timeline.push_back(std::move(line));
                        }
                    }
                    w0 = w1;
                    t0 = t1;
                    was_recording = is_recording;
                }
            });
            while (r.seconds < phase_s) {
                if (arm == 1) { // untimed reset; with a governor its CPU is builder CPU, not foreground
                    const auto cpu_ns = [] {
                        timespec ts{};
                        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);
                        return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
                    };
                    const uint64_t c0 = cpu_ns();
                    all_done = widen(64, nullptr) == XStore::LiveWidenResult::kDone && all_done;
                    if (gov != nullptr) gov->AddBuilderCpu(cpu_ns() - c0);
                }
                XStore::LiveWidenStats stats;
                const FileRangeLock::WriterWaitStats w0 = year_lock.WriterWaits();
                recording.store(arm);
                const auto t0 = std::chrono::steady_clock::now();
                all_done = widen(kLiveWidenColumns, &stats, governed) == XStore::LiveWidenResult::kDone && all_done;
                recording.store(-1);
                r.seconds += std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
                add_waits(r, w0);
                r.parked_s += stats.governor_parked_s;
                r.chunks += stats.chunks;
                r.hole_chunks += stats.hole_chunks;
                r.holds_us.insert(r.holds_us.end(), stats.lock_hold_us.begin(), stats.lock_hold_us.end());
                ++r.builder_runs;
                r.timeouts += stats.lock_timeouts;
                r.yields += stats.writer_yields;
                r.yield_us += stats.writer_yield_us;
            }
            phase_over.store(true);
            monitor.join();
            if (governed) {
                const auto snap = gov->GetSnapshot();
                r.brakes = snap.brakes;
                r.grants = snap.grants;
                gov.reset();
                gov_load.reset();
            }
        }
    }
    stop.store(true);
    for (std::thread &t : writers) t.join();
    EXPECT_TRUE(all_done);

    for (int round = 0; round < rounds; ++round) {
        std::cout << "[PERF] LiveWidenDuringInserts round " << round << " | loadavg at start " << loads[static_cast<size_t>(round)] << std::endl;
        for (int arm = 0; arm < 3; ++arm) {
            const ArmResult &r = results[static_cast<size_t>(round)][static_cast<size_t>(arm)];
            std::cout << fmt::format("[PERF] LiveWidenDuringInserts round {} {} | {:.0f} s | {:.0f} rows/s | overwrite {} | "
                                     "head RangeInsert {}", round, kArmNames[arm], r.seconds,
                                     r.seconds > 0 ? static_cast<double>(r.rows) / r.seconds : 0.0,
                                     LatencyText(r.overwrite_us), LatencyText(r.append_us))
                      << (arm == 0 ? std::string() : fmt::format(" | builder runs {} hold {} timeouts {} writer yields {} ({} ms)",
                                                                 r.builder_runs, LatencyText(r.holds_us), r.timeouts,
                                                                 r.yields, r.yield_us / 1000))
                      << (arm == 0 ? std::string()
                                   : fmt::format(" | run avg {:.2f} s, chunks {} (holes {}) | per-second wait us/s p50 {:.0f} "
                                                 "p95 {:.0f} p99 {:.0f} max {:.0f} ({} s), seconds with a long wait {}",
                                                 r.builder_runs ? r.seconds / static_cast<double>(r.builder_runs) : 0.0,
                                                 r.chunks, r.hole_chunks, Quantile(r.sec_wait_rate, 0.5),
                                                 Quantile(r.sec_wait_rate, 0.95), Quantile(r.sec_wait_rate, 0.99),
                                                 r.sec_wait_rate.empty() ? 0.0 : *std::max_element(r.sec_wait_rate.begin(), r.sec_wait_rate.end()),
                                                 r.sec_wait_rate.size(),
                                                 std::count_if(r.sec_long.begin(), r.sec_long.end(), [](double v) { return v > 0; })))
                      << fmt::format(" | writer waits {} us ({:.0f} us/s) {} waits ({:.1f}/s) {} long ({:.2f}/s)", r.wait_us,
                                     r.seconds > 0 ? static_cast<double>(r.wait_us) / r.seconds : 0.0, r.waits,
                                     r.seconds > 0 ? static_cast<double>(r.waits) / r.seconds : 0.0, r.long_waits,
                                     r.seconds > 0 ? static_cast<double>(r.long_waits) / r.seconds : 0.0)
                      << (r.allowed_samples == 0 ? std::string()
                                                 : fmt::format(" | governor max {} allowed min {} max {} avg {:.2f} ({} samples) "
                                                               "brakes {} grants {} parked {:.1f} s", r.max_workers,
                                                               r.allowed_min, r.allowed_max,
                                                               r.allowed_sum / static_cast<double>(r.allowed_samples),
                                                               r.allowed_samples, r.brakes, r.grants, r.parked_s))
                      << std::endl;
            for (const std::string &line : r.timeline)
                std::cout << "[PERF] LiveWidenDuringInserts timeline r" << round << " " << (arm == 1 ? "B" : "C") << " " << line
                          << std::endl;
        }
    }
    for (int arm = 0; arm < 3; ++arm) {
        std::vector<double> rate, ow50, ow99, ow999, ap50, ap99, ap999, wait_rate, waits_rate, long_rate, runs;
        std::vector<double> holds, per_sec;
        for (const auto &round : results) {
            const ArmResult &r = round[static_cast<size_t>(arm)];
            rate.push_back(r.seconds > 0 ? static_cast<double>(r.rows) / r.seconds : 0.0);
            ow50.push_back(Quantile(r.overwrite_us, 0.5));
            ow99.push_back(Quantile(r.overwrite_us, 0.99));
            ow999.push_back(Quantile(r.overwrite_us, 0.999));
            ap50.push_back(Quantile(r.append_us, 0.5));
            ap99.push_back(Quantile(r.append_us, 0.99));
            ap999.push_back(Quantile(r.append_us, 0.999));
            holds.insert(holds.end(), r.holds_us.begin(), r.holds_us.end());
            const double secs = r.seconds > 0 ? r.seconds : 1.0;
            wait_rate.push_back(static_cast<double>(r.wait_us) / secs);
            waits_rate.push_back(static_cast<double>(r.waits) / secs);
            long_rate.push_back(static_cast<double>(r.long_waits) / secs);
            runs.push_back(static_cast<double>(r.builder_runs));
            per_sec.insert(per_sec.end(), r.sec_wait_rate.begin(), r.sec_wait_rate.end());
        }
        std::cout << fmt::format("[PERF] LiveWidenDuringInserts MEDIAN over {} rounds | {} | {:.0f} rows/s | overwrite p50 {:.1f} "
                                 "p99 {:.1f} p99.9 {:.1f} ms | head RangeInsert p50 {:.1f} p99 {:.1f} p99.9 {:.1f} ms", rounds,
                                 kArmNames[arm], Quantile(rate, 0.5), Quantile(ow50, 0.5) / 1000, Quantile(ow99, 0.5) / 1000,
                                 Quantile(ow999, 0.5) / 1000, Quantile(ap50, 0.5) / 1000, Quantile(ap99, 0.5) / 1000,
                                 Quantile(ap999, 0.5) / 1000)
                  << (arm == 0 ? std::string() : " | lock hold over all rounds " + LatencyText(holds))
                  << fmt::format(" | writer waits {:.0f} us/s {:.1f} waits/s {:.2f} long/s", Quantile(wait_rate, 0.5),
                                 Quantile(waits_rate, 0.5), Quantile(long_rate, 0.5))
                  << (arm == 0 ? std::string()
                               : fmt::format(" | builder runs per phase {:.0f} | per-second wait us/s pooled p50 {:.0f} p95 {:.0f} "
                                             "p99 {:.0f} max {:.0f} ({} s)", Quantile(runs, 0.5), Quantile(per_sec, 0.5),
                                             Quantile(per_sec, 0.95), Quantile(per_sec, 0.99),
                                             per_sec.empty() ? 0.0 : *std::max_element(per_sec.begin(), per_sec.end()),
                                             per_sec.size()))
                  << std::endl;
    }
}

// One writer thread makes the SYNC scattered BatchInsert calls of BatchInsertScatteredLargeThroughput_Sync (1000
// random distinct rows each, one year, fixed seed) while XSTORE_PERF_READERS (default 4) reader threads, each with its
// own XStore and fixed seed, UnaryQuery random rows of the same year (misses are fine) until the writer is done. The
// year file exists before the readers start (one seed row), and they start on the writer's 'go', set right before its
// start timestamp; reader ops/s counts only reads that completed inside the writer's window. Latencies are kept per
// thread and merged after the join. Reports only; no performance assertion.
TEST_F(XStorePerfTest, ScatteredInsertWithConcurrentReads) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    {   // the year file must exist, or the readers time the missing-file path (a scattered write may overwrite it)
        XStore_Proto::UnaryInsert_REP seed;
        store.UnaryInsert(kScatterYearStart, PerfRow(0), "PERFDB", &seed, XStore_Proto::SYNC);
        ASSERT_NE(seed.insertstatus().find("[INFO]"), std::string::npos) << seed.insertstatus();
    }
    const unsigned long long n = std::min(PerfOpCount(200'000), kScatterYearSeconds);
    std::vector<XStore_Proto::BatchInsert_REQ> reqs = ScatteredSyncRequests(n, 1000);
    const unsigned readers = static_cast<unsigned>(EnvInt("XSTORE_PERF_READERS", 4));

    using SteadyClock = std::chrono::steady_clock;
    std::atomic<bool> go{false}, writer_done{false};
    // Written by the writer before it releases go / sets writer_done; read by the readers after the join only.
    SteadyClock::time_point window_start, window_end;
    struct Read { SteadyClock::time_point done; double us; };
    std::vector<std::vector<Read>> reads(readers);
    std::vector<std::thread> reader_threads;
    for (unsigned t = 0; t < readers; ++t) {
        reader_threads.emplace_back([&, t] {
            XStore r;
            std::mt19937_64 rng(7000 + t);
            std::vector<Read> &mine = reads[t];
            XStore_Proto::UnaryQuery_REP rep;
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            while (!writer_done.load(std::memory_order_relaxed)) {
                const unsigned long long ts = kScatterYearStart + rng() % kScatterYearSeconds;
                const SteadyClock::time_point t0 = SteadyClock::now();
                r.UnaryQuery(ts, "PERFDB", &rep);
                const SteadyClock::time_point t1 = SteadyClock::now();
                mine.push_back({t1, std::chrono::duration<double, std::micro>(t1 - t0).count()});
            }
        });
    }

    unsigned long long failed = 0;
    std::thread writer([&] {
        XStore w;
        window_start = SteadyClock::now();
        go.store(true, std::memory_order_release);
        for (XStore_Proto::BatchInsert_REQ &req : reqs) {
            XStore_Proto::BatchInsert_REP rep;
            w.BatchInsert(&req, &rep);
            if (rep.insertstatus().find("successfully") == std::string::npos) ++failed;
        }
        window_end = SteadyClock::now();
        writer_done.store(true, std::memory_order_relaxed);
    });
    writer.join();
    for (std::thread &t : reader_threads) t.join();
    const double elapsed = std::chrono::duration<double>(window_end - window_start).count();

    std::vector<double> in_window;   // latencies of reads completed inside [window_start, window_end]
    for (const std::vector<Read> &v : reads) {
        for (const Read &e : v) {
            if (e.done <= window_end) in_window.push_back(e.us);
        }
    }
    std::sort(in_window.begin(), in_window.end());
    const auto at = [&](double q) {
        return in_window.empty() ? 0.0 : in_window[std::min(in_window.size() - 1, static_cast<size_t>(q * static_cast<double>(in_window.size())))];
    };
    EXPECT_EQ(failed, 0u);
    ReportPerf({"ScatteredInsertWithConcurrentReads[writer SYNC]", n, elapsed});
    const double secs = elapsed > 0 ? elapsed : 1.0;
    std::cout << fmt::format("[PERF] ScatteredInsertWithConcurrentReads | readers={} | writer {:.0f} rows/s | readers {} ops, "
                             "{:.0f} ops/s total | read latency p50 {:.1f} p99 {:.1f} max {:.1f} us",
                             readers, static_cast<double>(n) / secs, in_window.size(),
                             static_cast<double>(in_window.size()) / secs, at(0.5), at(0.99),
                             in_window.empty() ? 0.0 : in_window.back())
              << std::endl;
}

// ScatteredInsertWithConcurrentReads with BatchQuery readers: the same SYNC writer (1000 random distinct rows per
// BatchInsert, one year, fixed seed) while XSTORE_PERF_READERS (default 4) threads each BatchQuery
// XSTORE_PERF_BATCH_QUERY_ROWS (default 300) random rows of the same year (misses are fine, request order) until the
// writer is done. With per-run READ locks a query conflicts with a writer record only where their rows coincide or
// sit next to each other, not over their spans. Same window rule and output style; reports only, no performance
// assertion. Run it with XSTORE_TEST_RING_BYTES=67108864 like the other perf tests: on the default 8 MiB test ring the
// checkpointer falls behind and a writer batch fails with "ring full" (seen locally with the span lock too).
TEST_F(XStorePerfTest, ScatteredInsertWithConcurrentBatchQueries) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    {   // the year file must exist, or the readers time the missing-file path (a scattered write may overwrite it)
        XStore_Proto::UnaryInsert_REP seed;
        store.UnaryInsert(kScatterYearStart, PerfRow(0), "PERFDB", &seed, XStore_Proto::SYNC);
        ASSERT_NE(seed.insertstatus().find("[INFO]"), std::string::npos) << seed.insertstatus();
    }
    const unsigned long long n = std::min(PerfOpCount(200'000), kScatterYearSeconds);
    std::vector<XStore_Proto::BatchInsert_REQ> reqs = ScatteredSyncRequests(n, 1000);
    const unsigned readers = static_cast<unsigned>(EnvInt("XSTORE_PERF_READERS", 4));
    const int query_rows = EnvInt("XSTORE_PERF_BATCH_QUERY_ROWS", 300);

    using SteadyClock = std::chrono::steady_clock;
    std::atomic<bool> go{false}, writer_done{false};
    SteadyClock::time_point window_start, window_end;
    struct Read { SteadyClock::time_point done; double us; };
    std::vector<std::vector<Read>> reads(readers);
    std::vector<std::thread> reader_threads;
    for (unsigned t = 0; t < readers; ++t) {
        reader_threads.emplace_back([&, t] {
            XStore r;
            std::mt19937_64 rng(7000 + t);
            std::vector<Read> &mine = reads[t];
            XStore_Proto::BatchQuery_REQ req;
            req.set_dbname("PERFDB");
            std::string sink;
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            while (!writer_done.load(std::memory_order_relaxed)) {
                req.clear_timestamp();
                for (int i = 0; i < query_rows; ++i) req.add_timestamp(kScatterYearStart + rng() % kScatterYearSeconds);
                const SteadyClock::time_point t0 = SteadyClock::now();
                r.BatchQuery(&req, /*sort_time=*/false, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                             [&](std::string &&chunk, bool) { sink = std::move(chunk); });
                const SteadyClock::time_point t1 = SteadyClock::now();
                mine.push_back({t1, std::chrono::duration<double, std::micro>(t1 - t0).count()});
            }
        });
    }

    unsigned long long failed = 0;
    std::thread writer([&] {
        XStore w;
        window_start = SteadyClock::now();
        go.store(true, std::memory_order_release);
        for (XStore_Proto::BatchInsert_REQ &req : reqs) {
            XStore_Proto::BatchInsert_REP rep;
            w.BatchInsert(&req, &rep);
            if (rep.insertstatus().find("successfully") == std::string::npos) ++failed;
        }
        window_end = SteadyClock::now();
        writer_done.store(true, std::memory_order_relaxed);
    });
    writer.join();
    for (std::thread &t : reader_threads) t.join();
    const double elapsed = std::chrono::duration<double>(window_end - window_start).count();

    std::vector<double> in_window;
    for (const std::vector<Read> &v : reads) {
        for (const Read &e : v) {
            if (e.done <= window_end) in_window.push_back(e.us);
        }
    }
    std::sort(in_window.begin(), in_window.end());
    const auto at = [&](double q) {
        return in_window.empty() ? 0.0 : in_window[std::min(in_window.size() - 1, static_cast<size_t>(q * static_cast<double>(in_window.size())))];
    };
    EXPECT_EQ(failed, 0u);
    ReportPerf({"ScatteredInsertWithConcurrentBatchQueries[writer SYNC]", n, elapsed});
    const double secs = elapsed > 0 ? elapsed : 1.0;
    std::cout << fmt::format("[PERF] ScatteredInsertWithConcurrentBatchQueries | readers={} x {} rows | writer {:.0f} rows/s | "
                             "readers {} ops, {:.0f} ops/s total | read latency p50 {:.1f} p99 {:.1f} max {:.1f} us",
                             readers, query_rows, static_cast<double>(n) / secs, in_window.size(),
                             static_cast<double>(in_window.size()) / secs, at(0.5), at(0.99),
                             in_window.empty() ? 0.0 : in_window.back())
              << std::endl;
}

// ScatteredInsertWithConcurrentBatchQueries with BatchDelete threads: the same SYNC writer (1000 random distinct rows
// per BatchInsert, one year, fixed seed) while XSTORE_PERF_DELETERS (default 4) threads each BatchDelete
// XSTORE_PERF_BATCH_DELETE_ROWS (default 300) rows per call until the writer is done. The
// deleted rows are real hits: a row set D (random, fixed seed, disjoint from the writer's rows, so interleaved with them
// across the year) is inserted SYNC before the window, and each deleter works through its own disjoint, pre-built,
// ascending chunks of D in order, wrapping around when it reaches the end. With the per-run WRITE set a delete conflicts with a writer record only where their
// rows coincide or sit next to each other, not over their spans. D is capped (kMaxDeleteSetRows, 500 chunks per deleter),
// so only the first pass over the chunks hits rows; later passes delete already-deleted rows (same locks, same status
// text). The calls made per deleter are printed with the first-pass chunk count. Same window rule and output style
// as the BatchQueries test. Run it with XSTORE_TEST_RING_BYTES=67108864 like the other perf tests: on the default
// 8 MiB test ring the checkpointer falls behind and a writer batch fails with "ring full".
TEST_F(XStorePerfTest, ScatteredInsertWithConcurrentBatchDeletes) {
    XStore store;
    CreateTestDb(store, "PERFDB", XStore_Proto::SECOND, 64);
    {   // the year file must exist, or the deleters time the missing-file path (a scattered write may overwrite it)
        XStore_Proto::UnaryInsert_REP seed;
        store.UnaryInsert(kScatterYearStart, PerfRow(0), "PERFDB", &seed, XStore_Proto::SYNC);
        ASSERT_NE(seed.insertstatus().find("[INFO]"), std::string::npos) << seed.insertstatus();
    }
    const unsigned long long n = std::min(PerfOpCount(200'000), kScatterYearSeconds);
    std::vector<XStore_Proto::BatchInsert_REQ> reqs = ScatteredSyncRequests(n, 1000);
    const unsigned deleters = static_cast<unsigned>(EnvInt("XSTORE_PERF_DELETERS", 4));
    const unsigned long long delete_rows = static_cast<unsigned long long>(EnvInt("XSTORE_PERF_BATCH_DELETE_ROWS", 300));

    // D: about deleters x chunks x delete_rows rows, capped so the SYNC setup insert stays short.
    constexpr unsigned long long kMaxDeleteSetRows = 150'000;
    const unsigned long long chunks = std::clamp<unsigned long long>(kMaxDeleteSetRows / (deleters * delete_rows), 1, 500);
    const unsigned long long d_rows = deleters * chunks * delete_rows;
    std::unordered_set<unsigned long long> used = {kScatterYearStart};
    for (const XStore_Proto::BatchInsert_REQ &req : reqs) {
        for (const XStore_Proto::BatchInsert_DATA &entry : req.batchinsert_data()) used.insert(entry.timestamp());
    }
    std::vector<unsigned long long> d;
    d.reserve(d_rows);
    std::mt19937_64 d_rng(20261020);
    while (d.size() < d_rows) {
        const unsigned long long ts = kScatterYearStart + d_rng() % kScatterYearSeconds;
        if (used.insert(ts).second) d.push_back(ts);
    }
    for (unsigned long long from = 0; from < d_rows; from += 1000) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname("PERFDB");
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (unsigned long long i = from; i < std::min(d_rows, from + 1000); i++) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(d[i]);
            *entry->mutable_data() = PerfRow(i);
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
    }
    // deleter t owns d[t * chunks * delete_rows, (t + 1) * chunks * delete_rows), cut into ascending chunks
    std::vector<std::vector<XStore_Proto::BatchDelete_REQ>> del_reqs(deleters);
    for (unsigned t = 0; t < deleters; ++t) {
        for (unsigned long long c = 0; c < chunks; ++c) {
            const auto first = d.begin() + static_cast<std::ptrdiff_t>((t * chunks + c) * delete_rows);
            std::sort(first, first + static_cast<std::ptrdiff_t>(delete_rows));
            XStore_Proto::BatchDelete_REQ &req = del_reqs[t].emplace_back();
            req.set_dbname("PERFDB");
            for (auto it = first; it != first + static_cast<std::ptrdiff_t>(delete_rows); ++it) req.add_timestamp(*it);
        }
    }
    const std::string want = fmt::format("Deleted {} items", delete_rows);

    using SteadyClock = std::chrono::steady_clock;
    std::atomic<bool> go{false}, writer_done{false};
    SteadyClock::time_point window_start, window_end;
    struct Del { SteadyClock::time_point done; double us; };
    std::vector<std::vector<Del>> dels(deleters);
    std::vector<unsigned long long> del_failed(deleters, 0);
    std::vector<std::thread> deleter_threads;
    for (unsigned t = 0; t < deleters; ++t) {
        deleter_threads.emplace_back([&, t] {
            XStore dl;
            std::vector<Del> &mine = dels[t];
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            // Wraps around until the writer is done: after the first pass the rows are gone, so later passes punch
            // holes on empty rows (same locks, same status text).
            for (size_t i = 0; !writer_done.load(std::memory_order_relaxed); i = (i + 1) % del_reqs[t].size()) {
                XStore_Proto::BatchDelete_REQ &req = del_reqs[t][i];
                const SteadyClock::time_point t0 = SteadyClock::now();
                XStore_Proto::BatchDelete_REP rep = dl.BatchDelete(&req);
                const SteadyClock::time_point t1 = SteadyClock::now();
                mine.push_back({t1, std::chrono::duration<double, std::micro>(t1 - t0).count()});
                if (rep.deletestatus().find(want) == std::string::npos) ++del_failed[t];
            }
        });
    }

    unsigned long long failed = 0;
    std::thread writer([&] {
        XStore w;
        window_start = SteadyClock::now();
        go.store(true, std::memory_order_release);
        for (XStore_Proto::BatchInsert_REQ &req : reqs) {
            XStore_Proto::BatchInsert_REP rep;
            w.BatchInsert(&req, &rep);
            if (rep.insertstatus().find("successfully") == std::string::npos) ++failed;
        }
        window_end = SteadyClock::now();
        writer_done.store(true, std::memory_order_relaxed);
    });
    writer.join();
    for (std::thread &t : deleter_threads) t.join();
    const double elapsed = std::chrono::duration<double>(window_end - window_start).count();

    std::vector<double> in_window;
    unsigned long long delete_failed = 0;
    for (unsigned t = 0; t < deleters; ++t) {
        delete_failed += del_failed[t];
        for (const Del &e : dels[t]) {
            if (e.done <= window_end) in_window.push_back(e.us);
        }
    }
    std::sort(in_window.begin(), in_window.end());
    const auto at = [&](double q) {
        return in_window.empty() ? 0.0 : in_window[std::min(in_window.size() - 1, static_cast<size_t>(q * static_cast<double>(in_window.size())))];
    };
    EXPECT_EQ(failed, 0u);
    EXPECT_EQ(delete_failed, 0u);
    ReportPerf({"ScatteredInsertWithConcurrentBatchDeletes[writer SYNC]", n, elapsed});
    const double secs = elapsed > 0 ? elapsed : 1.0;
    std::cout << fmt::format("[PERF] ScatteredInsertWithConcurrentBatchDeletes | deleters={} x {} rows | writer {:.0f} rows/s | "
                             "deleters {} ops, {:.0f} ops/s total | delete latency p50 {:.1f} p99 {:.1f} max {:.1f} us",
                             deleters, delete_rows, static_cast<double>(n) / secs, in_window.size(),
                             static_cast<double>(in_window.size()) / secs, at(0.5), at(0.99),
                             in_window.empty() ? 0.0 : in_window.back())
              << std::endl;
    std::cout << "ScatteredInsertWithConcurrentBatchDeletes calls per deleter (first pass = " << chunks << " chunks):";
    for (const std::vector<Del> &v : dels) std::cout << " " << v.size();
    std::cout << std::endl;
}
