// tests/integration/XStoreWalTest.cpp
#include <gtest/gtest.h>
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_Crc32c.h"
#include "XStoreTestSupport.h"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>
#include <vector>

namespace {

using xstore_test::ScratchDir;
using xstore_test::CreateTestDb;

// Reuses the same scratch-directory helper as tests/integration/XStoreTest.cpp
// (pid + monotonic counter, not this-pointer-based -- gtest fixture objects
// are heap-allocated per test and their address is commonly reused across
// sequential tests, which would otherwise collide two tests onto the same
// scratch path and trip XStore's constructor-level
// once-per-distinct-data_store_dir guard on the second test).
class XStoreWalTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_wal_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
    }

    void TearDown() override {
        scratch.reset();
    }
};

XStore_Proto::RowData MakeRow(double value) {
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(value);
    return row;
}

} // namespace

// SYNC (the default) must return only after the row is durable AND applied
// -- a query for the same key immediately after UnaryInsert returns must
// see it, with no wait/retry needed on the test's side.
TEST_F(XStoreWalTest, SyncAppliesBeforeReturning) {
    XStore store;
    XStore_Proto::CreateDB_REP create_rep = store.CreateDB("WALTEST", XStore_Proto::SECOND, 64);
    ASSERT_NE(create_rep.createdbstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1700000000ULL, MakeRow(42.5), "WALTEST", &insert_rep, XStore_Proto::SYNC);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(1700000000ULL, "WALTEST", &query_rep);
    ASSERT_TRUE(query_rep.has_data());
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 42.5);
}

// ASYNC must still return successfully (enqueue succeeded) and the row must
// become visible eventually -- this test polls with a bounded timeout
// instead of asserting immediate visibility, since ASYNC explicitly does
// not wait for apply.
TEST_F(XStoreWalTest, AsyncEventuallyApplies) {
    XStore store;
    CreateTestDb(store, "WALASYNC", XStore_Proto::SECOND, 64);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1700000001ULL, MakeRow(7.0), "WALASYNC", &insert_rep, XStore_Proto::ASYNC);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    bool became_visible = false;
    for (int attempt = 0; attempt < 200; ++attempt) {
        XStore_Proto::UnaryQuery_REP query_rep;
        store.UnaryQuery(1700000001ULL, "WALASYNC", &query_rep);
        if (query_rep.has_data() && query_rep.data().fielddata_size() == 1) {
            became_visible = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(became_visible);
}

// BYPASS applies straight to the data file on the calling thread, same
// synchronous-before-returning guarantee as SYNC -- no polling needed,
// unlike ASYNC.
TEST_F(XStoreWalTest, BypassAppliesBeforeReturning) {
    XStore store;
    CreateTestDb(store, "WALBYPASS", XStore_Proto::SECOND, 64);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1700000003ULL, MakeRow(13.5), "WALBYPASS", &insert_rep, XStore_Proto::BYPASS);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(1700000003ULL, "WALBYPASS", &query_rep);
    ASSERT_TRUE(query_rep.has_data());
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 13.5);
}

// BYPASS's whole point is skipping WAL entirely -- confirms no WAL record is
// ever written for it, unlike every other durability mode (which all funnel
// through EnqueueWalRecord). A shard's WAL file is only created lazily, on
// its data_store_dir's first REAL (non-BYPASS) WAL write (StartWalWriters
// itself only eagerly preallocates for the one data_store_dir the whole
// test binary happened to construct first -- every other scratch dir's WAL
// file comes from WalWriterLoop's fd-cache-miss path instead) -- so this
// primes the file with a SYNC insert first, snapshots its bytes
// (offset-0-overwrite format: one record always lives at the same offset),
// then does a BYPASS insert on the SAME db/shard and confirms the file is
// byte-for-byte unchanged, while the BYPASS row is independently visible
// via query (proving it landed in the data file, not lost).
TEST_F(XStoreWalTest, BypassWritesNoWalRecord) {
    XStore store;
    CreateTestDb(store, "WALBYPASSNOWAL", XStore_Proto::SECOND, 64);

    XStore_Proto::UnaryInsert_REP priming_rep;
    store.UnaryInsert(1700000004ULL, MakeRow(5.0), "WALBYPASSNOWAL", &priming_rep, XStore_Proto::SYNC);
    ASSERT_NE(priming_rep.insertstatus().find("[INFO]"), std::string::npos);

    // active_wal_shard_count, not the WAL_SHARD_COUNT compile-time ceiling:
    // matches EnqueueWalRecord's own modulo for which shard file this DB
    // actually landed on.
    std::filesystem::path db_base_path = scratch->test_root / "DataStore" / "WALBYPASSNOWAL";
    size_t active = XStore::active_wal_shard_count.load();
    size_t shard_idx = std::hash<std::string>{}(db_base_path.string()) % active;
    std::filesystem::path wal_file = scratch->test_root / "DataStore" / "WAL" / ("shard_" + std::to_string(shard_idx) + ".WAL");

    auto read_file = [&]() {
        std::ifstream in(wal_file, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    };
    std::string wal_bytes_before = read_file();
    ASSERT_FALSE(wal_bytes_before.empty());

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1700000005ULL, MakeRow(21.0), "WALBYPASSNOWAL", &insert_rep, XStore_Proto::BYPASS);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    std::string wal_bytes_after = read_file();
    EXPECT_EQ(wal_bytes_after, wal_bytes_before) << "BYPASS must never write a WAL record";

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(1700000005ULL, "WALBYPASSNOWAL", &query_rep);
    ASSERT_TRUE(query_rep.has_data());
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 21.0);
}

// BatchInsert now routes through the same WAL EnqueueWalRecord/ApplyWalRecord
// path UnaryInsert uses (see Task 1 of the WAL-safety-net plan) -- SYNC/BYPASS
// apply before returning (immediately queryable, same as before), but ASYNC
// now defers the actual data-file write itself (not just its fsync) to the
// WAL writer thread's gather window, matching UnaryInsert's own ASYNC
// behavior exactly. Poll for ASYNC instead of asserting immediate visibility.
TEST_F(XStoreWalTest, BatchInsertDurabilityModesAllApplyCorrectly) {
    XStore store;
    CreateTestDb(store, "WALBATCHDUR", XStore_Proto::SECOND, 64);

    XStore_Proto::BatchInsert_REQ async_req;
    async_req.set_dbname("WALBATCHDUR");
    async_req.set_durabilitymode(XStore_Proto::ASYNC);
    XStore_Proto::BatchInsert_DATA *async_entry = async_req.add_batchinsert_data();
    async_entry->set_timestamp(1700600000ULL);
    *async_entry->mutable_data() = MakeRow(1.0);
    XStore_Proto::BatchInsert_REP async_rep;
    store.BatchInsert(&async_req, &async_rep);
    EXPECT_NE(async_rep.insertstatus().find("[INFO]"), std::string::npos) << async_rep.insertstatus();

    XStore_Proto::BatchInsert_REQ sync_req;
    sync_req.set_dbname("WALBATCHDUR");
    sync_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::BatchInsert_DATA *sync_entry = sync_req.add_batchinsert_data();
    sync_entry->set_timestamp(1700600001ULL);
    *sync_entry->mutable_data() = MakeRow(2.0);
    XStore_Proto::BatchInsert_REP sync_rep;
    store.BatchInsert(&sync_req, &sync_rep);
    EXPECT_NE(sync_rep.insertstatus().find("[INFO]"), std::string::npos) << sync_rep.insertstatus();

    XStore_Proto::BatchInsert_REQ bypass_req;
    bypass_req.set_dbname("WALBATCHDUR");
    bypass_req.set_durabilitymode(XStore_Proto::BYPASS);
    XStore_Proto::BatchInsert_DATA *bypass_entry = bypass_req.add_batchinsert_data();
    bypass_entry->set_timestamp(1700600002ULL);
    *bypass_entry->mutable_data() = MakeRow(3.0);
    XStore_Proto::BatchInsert_REP bypass_rep;
    store.BatchInsert(&bypass_req, &bypass_rep);
    EXPECT_NE(bypass_rep.insertstatus().find("[INFO]"), std::string::npos) << bypass_rep.insertstatus();

    // SYNC and BYPASS apply before returning -- immediately queryable.
    XStore_Proto::UnaryQuery_REP q2, q3;
    store.UnaryQuery(1700600001ULL, "WALBATCHDUR", &q2);
    store.UnaryQuery(1700600002ULL, "WALBATCHDUR", &q3);
    ASSERT_TRUE(q2.has_data());
    ASSERT_TRUE(q3.has_data());
    EXPECT_DOUBLE_EQ(q2.data().fielddata(0).doubledata(), 2.0);
    EXPECT_DOUBLE_EQ(q3.data().fielddata(0).doubledata(), 3.0);

    // ASYNC: poll with a bounded timeout, same pattern as
    // XStoreWalTest.AsyncEventuallyApplies for UnaryInsert.
    bool became_visible = false;
    for (int attempt = 0; attempt < 200; ++attempt) {
        XStore_Proto::UnaryQuery_REP q1;
        store.UnaryQuery(1700600000ULL, "WALBATCHDUR", &q1);
        if (q1.has_data() && q1.data().fielddata_size() == 1) {
            EXPECT_DOUBLE_EQ(q1.data().fielddata(0).doubledata(), 1.0);
            became_visible = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(became_visible);
}

// A single BatchInsert call spanning multiple rows in one year-group must
// build exactly one WalRecord for that group and apply every row -- proves
// the year-bucket -> WalRecord mapping introduced in Task 1 doesn't drop or
// misorder rows within a group.
TEST_F(XStoreWalTest, BatchInsertMultiRowSingleYearGroupAppliesAllRows) {
    XStore store;
    CreateTestDb(store, "WALBATCHMULTI", XStore_Proto::SECOND, 64);

    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("WALBATCHMULTI");
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (int i = 0; i < 5; ++i) {
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(1700610000ULL + static_cast<unsigned long long>(i));
        *entry->mutable_data() = MakeRow(10.0 + i);
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();

    for (int i = 0; i < 5; ++i) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(1700610000ULL + static_cast<unsigned long long>(i), "WALBATCHMULTI", &q);
        ASSERT_TRUE(q.has_data()) << "row " << i;
        EXPECT_DOUBLE_EQ(q.data().fielddata(0).doubledata(), 10.0 + i) << "row " << i;
    }
}

TEST_F(XStoreWalTest, RangeInsertDurabilityModesAllApplyCorrectly) {
    XStore store;
    CreateTestDb(store, "WALRANGEDUR", XStore_Proto::SECOND, 64);

    XStore_Proto::RangeInsert_REQ async_req;
    async_req.set_dbname("WALRANGEDUR");
    async_req.set_durabilitymode(XStore_Proto::ASYNC);
    XStore_Proto::RangeInsert_DATA *async_entry = async_req.add_rangeinsert_data();
    async_entry->set_timestamp(1700700000ULL);
    *async_entry->mutable_data() = MakeRow(4.0);
    XStore_Proto::RangeInsert_REP async_rep;
    store.RangeInsert(&async_req, &async_rep);
    EXPECT_NE(async_rep.insertstatus().find("[INFO]"), std::string::npos) << async_rep.insertstatus();

    XStore_Proto::RangeInsert_REQ sync_req;
    sync_req.set_dbname("WALRANGEDUR");
    sync_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::RangeInsert_DATA *sync_entry = sync_req.add_rangeinsert_data();
    sync_entry->set_timestamp(1700700001ULL);
    *sync_entry->mutable_data() = MakeRow(5.0);
    XStore_Proto::RangeInsert_REP sync_rep;
    store.RangeInsert(&sync_req, &sync_rep);
    EXPECT_NE(sync_rep.insertstatus().find("[INFO]"), std::string::npos) << sync_rep.insertstatus();

    XStore_Proto::RangeInsert_REQ bypass_req;
    bypass_req.set_dbname("WALRANGEDUR");
    bypass_req.set_durabilitymode(XStore_Proto::BYPASS);
    XStore_Proto::RangeInsert_DATA *bypass_entry = bypass_req.add_rangeinsert_data();
    bypass_entry->set_timestamp(1700700002ULL);
    *bypass_entry->mutable_data() = MakeRow(6.0);
    XStore_Proto::RangeInsert_REP bypass_rep;
    store.RangeInsert(&bypass_req, &bypass_rep);
    EXPECT_NE(bypass_rep.insertstatus().find("[INFO]"), std::string::npos) << bypass_rep.insertstatus();

    XStore_Proto::UnaryQuery_REP q1, q2, q3;
    store.UnaryQuery(1700700000ULL, "WALRANGEDUR", &q1);
    store.UnaryQuery(1700700001ULL, "WALRANGEDUR", &q2);
    store.UnaryQuery(1700700002ULL, "WALRANGEDUR", &q3);
    ASSERT_TRUE(q1.has_data());
    ASSERT_TRUE(q2.has_data());
    ASSERT_TRUE(q3.has_data());
    EXPECT_DOUBLE_EQ(q1.data().fielddata(0).doubledata(), 4.0);
    EXPECT_DOUBLE_EQ(q2.data().fielddata(0).doubledata(), 5.0);
    EXPECT_DOUBLE_EQ(q3.data().fielddata(0).doubledata(), 6.0);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
}

// Crash-recovery replay: writes directly into a shard's WAL file the same
// way the writer thread would (bypassing UnaryInsert entirely, simulating
// "process crashed after fsync, before apply"), then constructs a fresh
// XStore against the same directory and confirms InitDB's ReplayWal
// reconstructed the row.
TEST_F(XStoreWalTest, ReplayRecoversUnappliedRecord) {
    {
        XStore store;
        CreateTestDb(store, "WALCRASH", XStore_Proto::SECOND, 64);
    }

    // Locate this DB's WAL shard file the same way EnqueueWalRecord would
    // (hash(dbBasePath) % WAL_SHARD_COUNT), then hand-write one record into
    // it directly -- standing in for "the writer thread fsync'd this batch
    // but the process crashed before the apply-to-data-file step ran".
    std::filesystem::path db_base_path = scratch->test_root / "DataStore" / "WALCRASH";
    size_t shard_idx = std::hash<std::string>{}(db_base_path.string()) % XStoreTunables::WAL_SHARD_COUNT;
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::create_directories(wal_dir);
    std::filesystem::path wal_file = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");

    XStore_Proto::RowData row = MakeRow(99.25);
    std::string row_bytes(64, '\0');
    row.SerializeToArray(row_bytes.data(), static_cast<int>(row.ByteSizeLong()));

    // Wire format is per-REQUEST (rowCount + repeated rows), even though
    // this test hand-writes a single-row request -- matches
    // XStore_Wal.cpp's SerializeWalRecord/ParseWalRecord framing (Task 5).
    std::string db_name = "WALCRASH";
    std::string payload;
    auto append_pod = [&](auto value) {
        payload.append(reinterpret_cast<const char *>(&value), sizeof(value));
    };
    uint16_t db_name_len = static_cast<uint16_t>(db_name.size());
    uint16_t row_length = 64;
    int32_t time_res = static_cast<int32_t>(XStore_Proto::SECOND);
    uint32_t row_count = 1;
    uint64_t epoch_time = 1700000002ULL;
    append_pod(db_name_len);
    append_pod(row_length);
    append_pod(time_res);
    append_pod(row_count);
    payload.append(db_name);
    append_pod(epoch_time);
    payload.append(row_bytes);

    uint32_t crc = Crc32c(payload.data(), payload.size());

    std::string record;
    auto append_pod_record = [&](auto value) {
        record.append(reinterpret_cast<const char *>(&value), sizeof(value));
    };
    uint32_t record_len = static_cast<uint32_t>(4 + payload.size());
    append_pod_record(record_len);
    append_pod_record(crc);
    record.append(payload);

    std::ofstream out(wal_file, std::ios::binary | std::ios::trunc);
    out.write(record.data(), static_cast<std::streamsize>(record.size()));
    out.close();

    // Fresh XStore instance against the same scratch directory. Its own
    // constructor won't re-run InitDB -- XStore::XStore()'s
    // initialized_dirs guard runs InitDB (and therefore ReplayWal) at most
    // once per distinct data_store_dir *per process*, by design (so many
    // worker threads sharing one process/data_store_dir don't all pay the
    // scan cost -- see CfgCachedByOneXStoreInstanceIsVisibleToAnotherInstance
    // in XStoreTest.cpp for the same guard exercised from the CFG-cache
    // side). A real crash-restart is a new process, so the guard's static
    // state starts empty there; within one test binary, force the same
    // effect by calling the public InitDB() directly instead of relying on
    // construction to trigger it.
    XStore store2;
    store2.InitDB();
    XStore_Proto::UnaryQuery_REP query_rep;
    store2.UnaryQuery(1700000002ULL, "WALCRASH", &query_rep);
    ASSERT_TRUE(query_rep.has_data());
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 99.25);
}

// Many threads inserting distinct keys through the WAL concurrently -- all
// rows must be present afterward, none lost to a race in EnqueueWalRecord's
// shard hashing or the writer thread's batching.
TEST_F(XStoreWalTest, ConcurrentInsertsAllPersist) {
    XStore store;
    CreateTestDb(store, "WALCONC", XStore_Proto::SECOND, 64);

    constexpr int kThreads = 8;
    constexpr int kPerThread = 50;
    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&store, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                XStore_Proto::UnaryInsert_REP rep;
                unsigned long long ts = 1700100000ULL + static_cast<unsigned long long>(t * kPerThread + i);
                store.UnaryInsert(ts, MakeRow(static_cast<double>(t * kPerThread + i)), "WALCONC", &rep, XStore_Proto::SYNC);
            }
        });
    }
    for (auto &w : workers) w.join();

    for (int t = 0; t < kThreads; ++t) {
        for (int i = 0; i < kPerThread; ++i) {
            unsigned long long ts = 1700100000ULL + static_cast<unsigned long long>(t * kPerThread + i);
            XStore_Proto::UnaryQuery_REP rep;
            store.UnaryQuery(ts, "WALCONC", &rep);
            ASSERT_TRUE(rep.has_data()) << "missing row at ts=" << ts;
            EXPECT_DOUBLE_EQ(rep.data().fielddata(0).doubledata(), static_cast<double>(t * kPerThread + i));
        }
    }
}

#ifdef XSTORE_WAL_STATS_ENABLED
// Debug/benchmark-only (XSTORE_WAL_STATS): confirms WalShard's batching
// counters actually move, and that real batching (avg batch size > 1)
// happens under concurrent SYNC load on one DB -- the counters' whole
// purpose is answering "is it actually grouping," so this is the direct
// check that they do.
TEST_F(XStoreWalTest, WalStatsCountersReflectRealBatching) {
    XStore store;
    CreateTestDb(store, "WALSTATS", XStore_Proto::SECOND, 64);

    std::filesystem::path db_base_path = scratch->test_root / "DataStore" / "WALSTATS";
    size_t active = XStore::active_wal_shard_count.load(std::memory_order_relaxed);
    size_t shard_idx = std::hash<std::string>{}(db_base_path.string()) % active;
    XStore::WalShard &shard = XStore::wal_shards[shard_idx];

    uint64_t batches_before = shard.total_batches_processed.load(std::memory_order_relaxed);
    uint64_t records_before = shard.total_records_processed.load(std::memory_order_relaxed);

    constexpr int kThreads = 8;
    constexpr int kPerThread = 100;
    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&store, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                XStore_Proto::UnaryInsert_REP rep;
                unsigned long long ts = 1700500000ULL + static_cast<unsigned long long>(t * kPerThread + i);
                store.UnaryInsert(ts, MakeRow(1.0), "WALSTATS", &rep, XStore_Proto::SYNC);
            }
        });
    }
    for (auto &w : workers) w.join();

    uint64_t d_batches = shard.total_batches_processed.load(std::memory_order_relaxed) - batches_before;
    uint64_t d_records = shard.total_records_processed.load(std::memory_order_relaxed) - records_before;

    ASSERT_GT(d_batches, 0u) << "no batches observed -- counters aren't moving";
    EXPECT_EQ(d_records, static_cast<uint64_t>(kThreads * kPerThread));
    EXPECT_GT(d_records, d_batches) << "avg batch size was 1 (" << d_records << " records / " << d_batches
                                     << " batches) -- no real batching occurred under concurrent load";
}
#endif

// A prior version of this file had a regression guard here
// (SyncPerCommitLatencyUnaffectedByConcurrentGroupCommit) asserting that a
// SYNC_PER_COMMIT insert didn't inherit GROUP_COMMIT's gather-window linger
// when the two modes coexisted on one shard. That distinction no longer
// exists: SYNC_PER_COMMIT and GROUP_COMMIT were collapsed into one SYNC mode
// (see WalDurabilityMode in XStore.proto) that always rides the adaptive
// gather window -- a lone SYNC writer under concurrent SYNC load riding that
// same window with everyone else is now the INTENDED behavior, not a
// regression, so the old assertion no longer describes a real invariant.
// Removed rather than mechanically renamed. Re-add a real regression guard
// here once the Mystic-cluster benchmark (see the WAL_GATHER_WINDOW_MIN_US
// comment in XStore_Tunables.h) has established what a lone SYNC writer's
// latency SHOULD look like under the merged mode.

// Regression test for a real bug found via the XBench benchmark harness:
// its reset script does `rm -rf DataStore/BENCH_DB` between runs but never
// touches DataStore/WAL/ -- so a WAL record durably written in a PRIOR run,
// for a DB deleted before THIS run's InitDB ever gets a chance to replay
// it, would fail ApplyWalRecord ("Unable to open <year>.XSTORE" -- the DB's
// directory doesn't exist yet, since ReplayWal always runs before any
// client can reach CreateDB). Under the original truncate-per-commit
// design this self-healed within one run (the file was ~always empty by
// the time of the next crash); the preallocated/never-truncated redesign
// needed an explicit checkpoint (InvalidateWalFileHeader) to avoid
// replaying -- and failing on -- the same orphaned record on every single
// future restart forever.
TEST_F(XStoreWalTest, ReplayOfOrphanedRecordIsNotRetriedForever) {
    // Hand-write a WAL record for a DB that was never created in this
    // scratch dir at all -- standing in for "existed in a prior run, its
    // directory was since deleted out from under this one".
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::create_directories(wal_dir);
    std::filesystem::path wal_file = wal_dir / "shard_0.WAL";

    XStore_Proto::RowData row = MakeRow(13.0);
    std::string row_bytes(64, '\0');
    row.SerializeToArray(row_bytes.data(), static_cast<int>(row.ByteSizeLong()));

    std::string db_name = "GONE_DB";
    std::string payload;
    auto append_pod = [&](auto value) {
        payload.append(reinterpret_cast<const char *>(&value), sizeof(value));
    };
    uint16_t db_name_len = static_cast<uint16_t>(db_name.size());
    uint16_t row_length = 64;
    int32_t time_res = static_cast<int32_t>(XStore_Proto::SECOND);
    uint32_t row_count = 1;
    uint64_t epoch_time = 1700000003ULL;
    append_pod(db_name_len);
    append_pod(row_length);
    append_pod(time_res);
    append_pod(row_count);
    payload.append(db_name);
    append_pod(epoch_time);
    payload.append(row_bytes);

    uint32_t crc = Crc32c(payload.data(), payload.size());

    std::string record;
    auto append_pod_record = [&](auto value) {
        record.append(reinterpret_cast<const char *>(&value), sizeof(value));
    };
    uint32_t record_len = static_cast<uint32_t>(4 + payload.size());
    append_pod_record(record_len);
    append_pod_record(crc);
    record.append(payload);

    std::ofstream out(wal_file, std::ios::binary | std::ios::trunc);
    out.write(record.data(), static_cast<std::streamsize>(record.size()));
    out.close();

    // Constructing XStore runs InitDB -> ReplayWal once, automatically
    // (the "first restart after the crash"). GONE_DB's directory was
    // never created, so ApplyWalRecord must fail (logged, not fatal) --
    // the real assertion is that the checkpoint still ran anyway.
    XStore store;

    std::ifstream in(wal_file, std::ios::binary);
    uint32_t header_after_first_replay = 0xFFFFFFFFu;
    in.read(reinterpret_cast<char *>(&header_after_first_replay), 4);
    in.close();
    EXPECT_EQ(header_after_first_replay, 0u)
        << "orphaned record's WAL header must be invalidated after replay, "
           "even though ApplyWalRecord failed -- otherwise it replays (and "
           "fails) again on every future restart";

    // A second explicit "restart" against the same file must find nothing
    // left to replay (and therefore log no further apply failure) -- calls
    // InitDB directly since the constructor-level once-per-directory guard
    // already ran it once above (same pattern as ReplayRecoversUnappliedRecord).
    store.InitDB();
    std::ifstream in2(wal_file, std::ios::binary);
    uint32_t header_after_second_replay = 0xFFFFFFFFu;
    in2.read(reinterpret_cast<char *>(&header_after_second_replay), 4);
    in2.close();
    EXPECT_EQ(header_after_second_replay, 0u);
}
