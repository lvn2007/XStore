// tests/integration/XStoreWalTest.cpp
#include <gtest/gtest.h>
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_Crc32c.h"
#include "include/XStore/XStore_WalRing.h"
#include "XStoreTestSupport.h"
#include <chrono>
#include <fcntl.h>
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

// Hand-builds one raw WAL record's on-wire bytes (recordLen+crc32c header
// followed by the same payload SerializeWalRecord/ParseWalRecord use --
// dbNameLen+rowLength+timeResolution+rowCount, dbName, epoch_time, row
// bytes), matching XStore_Wal.cpp's framing. Factored out of
// ReplayRecoversUnappliedRecord (which keeps its own inline copy
// untouched) so the two size-reclaim tests below don't each duplicate it.
std::string BuildRawWalRecord(const std::string &db_name, XStore_Proto::TimeUnit time_res,
                               uint64_t epoch_time, const std::string &row_bytes) {
    std::string payload;
    auto append_pod = [&](auto value) {
        payload.append(reinterpret_cast<const char *>(&value), sizeof(value));
    };
    uint16_t db_name_len = static_cast<uint16_t>(db_name.size());
    uint16_t row_length = static_cast<uint16_t>(row_bytes.size());
    int32_t time_res_val = static_cast<int32_t>(time_res);
    uint32_t row_count = 1;
    append_pod(db_name_len);
    append_pod(row_length);
    append_pod(time_res_val);
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
    return record;
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
    // Compares whole ring-file bytes (header slots included): no background checkpoint may rewrite them.
    xstore_test::ScopedCheckpointerPause checkpointer_paused;
    XStore store;
    CreateTestDb(store, "WALBYPASSNOWAL", XStore_Proto::SECOND, 64);

    XStore_Proto::UnaryInsert_REP priming_rep;
    store.UnaryInsert(1700000004ULL, MakeRow(5.0), "WALBYPASSNOWAL", &priming_rep, XStore_Proto::SYNC);
    ASSERT_NE(priming_rep.insertstatus().find("[INFO]"), std::string::npos);

    // Which shard the priming record actually landed on can't be predicted
    // by hash(db_base_path) % active alone anymore: EnqueueWalRecord salts
    // by calling-thread-id whenever WAL_SHARDS_PER_DB > 1 (see its own
    // comment), so the same DB can land on any of up to WAL_SHARDS_PER_DB
    // shards depending on which thread happens to call it -- this test's
    // calling thread's salt isn't known ahead of time. Discover the real
    // shard by scanning every active shard's file for the one that's
    // non-empty after the priming write, rather than recomputing (and
    // duplicating) EnqueueWalRecord's own salting formula here. Both calls
    // below are on this same test thread, so they're guaranteed to land on
    // the same shard as each other regardless of which one that is.
    auto read_file = [](const std::filesystem::path &path) {
        std::ifstream in(path, std::ios::binary);
        return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    };
    size_t active = XStore::active_wal_shard_count.load();
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::path wal_file;
    std::string wal_bytes_before;
    for (size_t i = 0; i < active; ++i) {
        std::filesystem::path candidate = wal_dir / ("shard_" + std::to_string(i) + ".WAL");
        std::string contents = read_file(candidate);
        if (!contents.empty()) {
            wal_file = candidate;
            wal_bytes_before = std::move(contents);
            break;
        }
    }
    ASSERT_FALSE(wal_bytes_before.empty()) << "no active shard's WAL file received the priming record";

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1700000005ULL, MakeRow(21.0), "WALBYPASSNOWAL", &insert_rep, XStore_Proto::BYPASS);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    std::string wal_bytes_after = read_file(wal_file);
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

// RangeInsert now routes through the same WAL EnqueueWalRecord/ApplyWalRecord
// path UnaryInsert/BatchInsert use (Task 4 of the WAL-safety-net plan) --
// SYNC/BYPASS apply before returning (immediately queryable, same as
// before), but ASYNC now defers the actual data-file write itself (not just
// its fsync) to the WAL writer thread's gather window, matching
// UnaryInsert/BatchInsert's own ASYNC behavior exactly. Poll for ASYNC
// instead of asserting immediate visibility -- same structure as
// XStoreWalTest.BatchInsertDurabilityModesAllApplyCorrectly.
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

    // SYNC and BYPASS apply before returning -- immediately queryable.
    XStore_Proto::UnaryQuery_REP q2, q3;
    store.UnaryQuery(1700700001ULL, "WALRANGEDUR", &q2);
    store.UnaryQuery(1700700002ULL, "WALRANGEDUR", &q3);
    ASSERT_TRUE(q2.has_data());
    ASSERT_TRUE(q3.has_data());
    EXPECT_DOUBLE_EQ(q2.data().fielddata(0).doubledata(), 5.0);
    EXPECT_DOUBLE_EQ(q3.data().fielddata(0).doubledata(), 6.0);

    // ASYNC: poll with a bounded timeout, same pattern as
    // XStoreWalTest.AsyncEventuallyApplies for UnaryInsert.
    bool became_visible = false;
    for (int attempt = 0; attempt < 200; ++attempt) {
        XStore_Proto::UnaryQuery_REP q1;
        store.UnaryQuery(1700700000ULL, "WALRANGEDUR", &q1);
        if (q1.has_data() && q1.data().fielddata_size() == 1) {
            EXPECT_DOUBLE_EQ(q1.data().fielddata(0).doubledata(), 4.0);
            became_visible = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(became_visible);
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

// A shard WAL file that's grown past WAL_FILE_PREALLOC_BYTES (a prior
// oversized BatchInsert/RangeInsert pass, see WAL_FILE_PREALLOC_BYTES's own
// comment) must: (1) still have its real record correctly replayed --
// ReplayWal's streaming parser no longer gates on the file's overall size at
// all, only on each individual record's own declared length, see
// WAL_REPLAY_MAX_RECORD_BYTES -- and (2) get shrunk back down to default
// afterward by ReclaimWalFileSize, so it doesn't stay oversized forever
// across every future restart. Grows the file via a cheap sparse
// resize_file rather than writing that much real data -- ReplayWal reads in
// bounded chunks regardless of the file's reported size, so this exercises
// the exact same code path a real oversized write would, without the test
// itself needing to write hundreds of MB.
TEST_F(XStoreWalTest, ReplayReclaimsOversizedFileAfterRecoveringRecord) {
    {
        XStore store;
        CreateTestDb(store, "WALSHRINK", XStore_Proto::SECOND, 64);
    }

    std::filesystem::path db_base_path = scratch->test_root / "DataStore" / "WALSHRINK";
    size_t shard_idx = std::hash<std::string>{}(db_base_path.string()) % XStoreTunables::WAL_SHARD_COUNT;
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::create_directories(wal_dir);
    std::filesystem::path wal_file = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");

    XStore_Proto::RowData row = MakeRow(7.5);
    std::string row_bytes(64, '\0');
    row.SerializeToArray(row_bytes.data(), static_cast<int>(row.ByteSizeLong()));
    std::string record = BuildRawWalRecord("WALSHRINK", XStore_Proto::SECOND, 1700000005ULL, row_bytes);

    std::ofstream out(wal_file, std::ios::binary | std::ios::trunc);
    out.write(record.data(), static_cast<std::streamsize>(record.size()));
    out.close();

    std::error_code resize_ec;
    std::filesystem::resize_file(wal_file, XStoreTunables::WAL_FILE_PREALLOC_BYTES + 4096, resize_ec);
    ASSERT_FALSE(resize_ec) << resize_ec.message();
    ASSERT_GT(std::filesystem::file_size(wal_file), XStoreTunables::WAL_FILE_PREALLOC_BYTES);

    XStore store2;
    store2.InitDB();

    // Replay still recovered the record even though the file was oversized.
    XStore_Proto::UnaryQuery_REP query_rep;
    store2.UnaryQuery(1700000005ULL, "WALSHRINK", &query_rep);
    ASSERT_TRUE(query_rep.has_data());
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 7.5);

    // And the oversized legacy file was recreated as a fixed-size ring (header + one
    // default ring): it neither stays oversized nor keeps the legacy layout.
    EXPECT_EQ(std::filesystem::file_size(wal_file), walring::HEADER_REGION_BYTES + walring::DefaultRingBytes());
    {
        int fd = open(wal_file.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        walring::RingHeader header;
        EXPECT_TRUE(walring::ReadBestHeader(fd, header));
        close(fd);
    }
}

// A shard WAL file whose reported size is enormous -- far past even the old
// (now-removed) per-file WAL_REPLAY_MAX_FILE_BYTES ceiling -- must still
// have its real, valid record correctly replayed. This is the direct fix
// for the data-loss gap the old per-file ceiling had: a file's on-disk
// *size* alone is no longer a reason to skip its content; ReplayWal streams
// it in bounded chunks and only stops at a record it genuinely can't parse
// (or one declaring an implausible length, see the sibling test below).
// Grows the file via a cheap sparse resize_file (not real writes) purely so
// the test doesn't need to allocate real disk for a multi-GiB file.
TEST_F(XStoreWalTest, ReplayRecoversRecordEvenWhenFileFarExceedsOldFileCeiling) {
    {
        XStore store;
        CreateTestDb(store, "WALHUGEFILE", XStore_Proto::SECOND, 64);
    }

    std::filesystem::path db_base_path = scratch->test_root / "DataStore" / "WALHUGEFILE";
    size_t shard_idx = std::hash<std::string>{}(db_base_path.string()) % XStoreTunables::WAL_SHARD_COUNT;
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::create_directories(wal_dir);
    std::filesystem::path wal_file = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");

    XStore_Proto::RowData row = MakeRow(3.25);
    std::string row_bytes(64, '\0');
    row.SerializeToArray(row_bytes.data(), static_cast<int>(row.ByteSizeLong()));
    std::string record = BuildRawWalRecord("WALHUGEFILE", XStore_Proto::SECOND, 1700000006ULL, row_bytes);

    std::ofstream out(wal_file, std::ios::binary | std::ios::trunc);
    out.write(record.data(), static_cast<std::streamsize>(record.size()));
    out.close();

    // Sized well past what the old file-level ceiling (8x default) would
    // have allowed, to prove size alone no longer matters.
    uintmax_t huge_size = 2 * (8 * XStoreTunables::WAL_FILE_PREALLOC_BYTES);
    std::error_code resize_ec;
    std::filesystem::resize_file(wal_file, huge_size, resize_ec);
    ASSERT_FALSE(resize_ec) << resize_ec.message();
    ASSERT_GT(std::filesystem::file_size(wal_file), 8 * XStoreTunables::WAL_FILE_PREALLOC_BYTES);

    XStore store2;
    store2.InitDB();

    // Recovered, even though the file's reported size was enormous.
    XStore_Proto::UnaryQuery_REP query_rep;
    store2.UnaryQuery(1700000006ULL, "WALHUGEFILE", &query_rep);
    ASSERT_TRUE(query_rep.has_data());
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 3.25);

    // And recreated as a fixed-size ring afterward, same as any oversized legacy file.
    EXPECT_EQ(std::filesystem::file_size(wal_file), walring::HEADER_REGION_BYTES + walring::DefaultRingBytes());
}

// The one thing that must still stop replay of a record: the record itself
// declaring an implausible length in its own header, past
// WAL_REPLAY_MAX_RECORD_BYTES -- this is what actually guards against a
// corrupted or malicious record_len field forcing a huge allocation, now
// that file size alone no longer gates anything (see the sibling test
// above). Hand-writes just a bogus 4-byte record_len header (no real
// payload needed -- ReplayWal must reject it before ever trying to read
// that many more bytes) followed by one real, otherwise-valid record, to
// also confirm the bad header stops replay before reaching it.
TEST_F(XStoreWalTest, ReplayStopsOnRecordDeclaringLengthOverSanityCeiling) {
    {
        XStore store;
        CreateTestDb(store, "WALBADLEN", XStore_Proto::SECOND, 64);
    }

    std::filesystem::path db_base_path = scratch->test_root / "DataStore" / "WALBADLEN";
    size_t shard_idx = std::hash<std::string>{}(db_base_path.string()) % XStoreTunables::WAL_SHARD_COUNT;
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::create_directories(wal_dir);
    std::filesystem::path wal_file = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");

    uint32_t bogus_record_len = static_cast<uint32_t>(XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES) + 1;
    std::string bogus_header;
    bogus_header.append(reinterpret_cast<const char *>(&bogus_record_len), sizeof(bogus_record_len));
    bogus_header.append(4, '\0'); // fake crc32c field -- never reached, length check rejects first

    XStore_Proto::RowData row = MakeRow(6.5);
    std::string row_bytes(64, '\0');
    row.SerializeToArray(row_bytes.data(), static_cast<int>(row.ByteSizeLong()));
    std::string real_record = BuildRawWalRecord("WALBADLEN", XStore_Proto::SECOND, 1700000007ULL, row_bytes);

    std::ofstream out(wal_file, std::ios::binary | std::ios::trunc);
    out.write(bogus_header.data(), static_cast<std::streamsize>(bogus_header.size()));
    out.write(real_record.data(), static_cast<std::streamsize>(real_record.size()));
    out.close();

    XStore store2;
    store2.InitDB();

    // Never applied -- replay stopped at the bogus header before it ever
    // reached the real record sitting right after it.
    XStore_Proto::UnaryQuery_REP query_rep;
    store2.UnaryQuery(1700000007ULL, "WALBADLEN", &query_rep);
    EXPECT_FALSE(query_rep.has_data());
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

// Exercises ApplyWalRecord's run-coalescing rewrite (Task 2): a batch with
// two contiguous runs separated by a gap (a timestamp never inserted) must
// still land every row at its correct offset -- a run-detection off-by-one
// would misalign the second run onto the first run's tail.
TEST_F(XStoreWalTest, BatchInsertGappedRunsLandAtCorrectOffsets) {
    XStore store;
    CreateTestDb(store, "WALBATCHGAP", XStore_Proto::SECOND, 64);

    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("WALBATCHGAP");
    req.set_durabilitymode(XStore_Proto::SYNC);
    // Run 1: timestamps 1700620000..1700620002 (3 contiguous rows)
    // Gap: 1700620003 never inserted
    // Run 2: timestamps 1700620004..1700620005 (2 contiguous rows)
    const std::vector<unsigned long long> timestamps = {
        1700620000ULL, 1700620001ULL, 1700620002ULL, 1700620004ULL, 1700620005ULL
    };
    for (size_t i = 0; i < timestamps.size(); ++i) {
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(timestamps[i]);
        *entry->mutable_data() = MakeRow(100.0 + static_cast<double>(i));
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();

    for (size_t i = 0; i < timestamps.size(); ++i) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(timestamps[i], "WALBATCHGAP", &q);
        ASSERT_TRUE(q.has_data()) << "timestamp " << timestamps[i];
        EXPECT_DOUBLE_EQ(q.data().fielddata(0).doubledata(), 100.0 + static_cast<double>(i)) << "timestamp " << timestamps[i];
    }

    // The gap itself must read back empty (never written), not leaked data
    // from a misaligned run.
    XStore_Proto::UnaryQuery_REP gap_q;
    store.UnaryQuery(1700620003ULL, "WALBATCHGAP", &gap_q);
    EXPECT_FALSE(gap_q.has_data() && gap_q.data().fielddata_size() > 0 && gap_q.data().fielddata(0).has_doubledata());
}

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

    // The legacy file was consumed by that replay and recreated as an (empty) ring:
    // a valid ring header at checkpoint 0 and a zeroed ring, so the orphaned record
    // is gone for good instead of replaying (and failing) again on every restart.
    auto expect_empty_ring = [&](const char *when) {
        int fd = open(wal_file.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0) << when;
        walring::RingHeader header;
        ASSERT_TRUE(walring::ReadBestHeader(fd, header)) << when << ": not a ring after replay";
        EXPECT_EQ(header.checkpoint_lsn, 0u) << when;
        uint64_t first_ring_bytes = 0xFFFFFFFFFFFFFFFFull;
        EXPECT_EQ(pread(fd, &first_ring_bytes, sizeof(first_ring_bytes), walring::HEADER_REGION_BYTES),
                  static_cast<ssize_t>(sizeof(first_ring_bytes))) << when;
        EXPECT_EQ(first_ring_bytes, 0u) << when << ": the orphaned record's bytes must not survive the upgrade";
        close(fd);
    };
    expect_empty_ring("after the first replay");

    // A second explicit "restart" against the same file must find nothing
    // left to replay (and therefore log no further apply failure) -- calls
    // InitDB directly since the constructor-level once-per-directory guard
    // already ran it once above (same pattern as ReplayRecoversUnappliedRecord).
    store.InitDB();
    expect_empty_ring("after the second replay");
}

// Before Task 3's fix, a single WalRecord with enough rows blows past
// pwritev's IOV_MAX (2 iovecs/row: an epoch-time buffer + the row bytes) in
// ONE syscall and fails with EINVAL, silently losing the whole batch (the
// WalWriterLoop's per-file write step treated any pwritev failure as
// "not durable" and moved on, so this manifested as every row in the batch
// staying invisible forever, not a crash). 2000 contiguous rows in one
// year-group needs ~4000 iovecs, well past the previously-assumed
// 512-iovec ceiling.
TEST_F(XStoreWalTest, BatchInsertLargeContiguousBatchAllRowsPersist) {
    XStore store;
    CreateTestDb(store, "WALBATCHBIG", XStore_Proto::SECOND, 64);

    constexpr int kRowCount = 2000;
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("WALBATCHBIG");
    req.set_durabilitymode(XStore_Proto::SYNC);
    const unsigned long long base_ts = 1700630000ULL;
    for (int i = 0; i < kRowCount; ++i) {
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(base_ts + static_cast<unsigned long long>(i));
        *entry->mutable_data() = MakeRow(static_cast<double>(i));
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();

    for (int i = 0; i < kRowCount; i += 137) {  // sample, not every row -- keeps the test fast
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(base_ts + static_cast<unsigned long long>(i), "WALBATCHBIG", &q);
        ASSERT_TRUE(q.has_data()) << "row " << i;
        EXPECT_DOUBLE_EQ(q.data().fielddata(0).doubledata(), static_cast<double>(i)) << "row " << i;
    }
    XStore_Proto::UnaryQuery_REP last_q;
    store.UnaryQuery(base_ts + kRowCount - 1, "WALBATCHBIG", &last_q);
    ASSERT_TRUE(last_q.has_data()) << "last row";
    EXPECT_DOUBLE_EQ(last_q.data().fielddata(0).doubledata(), static_cast<double>(kRowCount - 1));
}

// Mirrors BatchInsertLargeContiguousBatchAllRowsPersist, using RangeInsert
// instead of BatchInsert -- locks in correctness once RangeInsert is wired
// through the WAL (Task 4) and Task 3's pwritev chunking below it: a
// 2000-row single-year group must not silently drop rows past the iovec
// chunk boundary.
TEST_F(XStoreWalTest, RangeInsertLargeContiguousBatchAllRowsPersist) {
    XStore store;
    CreateTestDb(store, "WALRANGEBIG", XStore_Proto::SECOND, 64);

    constexpr int kRowCount = 2000;
    XStore_Proto::RangeInsert_REQ req;
    req.set_dbname("WALRANGEBIG");
    req.set_durabilitymode(XStore_Proto::SYNC);
    const unsigned long long base_ts = 1700710000ULL;
    for (int i = 0; i < kRowCount; ++i) {
        XStore_Proto::RangeInsert_DATA *entry = req.add_rangeinsert_data();
        entry->set_timestamp(base_ts + static_cast<unsigned long long>(i));
        *entry->mutable_data() = MakeRow(static_cast<double>(i));
    }
    XStore_Proto::RangeInsert_REP rep;
    store.RangeInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();

    for (int i = 0; i < kRowCount; i += 137) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(base_ts + static_cast<unsigned long long>(i), "WALRANGEBIG", &q);
        ASSERT_TRUE(q.has_data()) << "row " << i;
        EXPECT_DOUBLE_EQ(q.data().fielddata(0).doubledata(), static_cast<double>(i)) << "row " << i;
    }
}

// Crash-recovery replay for a MULTI-row record -- the actual capability
// this plan adds for Batch/Range Insert. ParseWalRecord/ReplayWal already
// handled row_count > 1 generically (the wire format always carried it),
// but no real producer had ever emitted one before Tasks 1/4 wired
// Batch/Range through the WAL -- this test proves the multi-row path
// actually round-trips, standing in for "the process crashed after the WAL
// fsync'd a 3-row Batch/Range record but before any of the 3 rows were
// applied to the data file".
TEST_F(XStoreWalTest, ReplayRecoversMultiRowUnappliedRecord) {
    {
        XStore store;
        CreateTestDb(store, "WALCRASHMULTI", XStore_Proto::SECOND, 64);
    }

    std::filesystem::path db_base_path = scratch->test_root / "DataStore" / "WALCRASHMULTI";
    size_t shard_idx = std::hash<std::string>{}(db_base_path.string()) % XStoreTunables::WAL_SHARD_COUNT;
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    std::filesystem::create_directories(wal_dir);
    std::filesystem::path wal_file = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");

    struct RowFixture { uint64_t epoch_time; double value; };
    const std::vector<RowFixture> rows = {
        {1700000010ULL, 11.0},
        {1700000011ULL, 22.0},
        {1700000012ULL, 33.0},
    };

    std::string db_name = "WALCRASHMULTI";
    std::string payload;
    auto append_pod = [&](auto value) {
        payload.append(reinterpret_cast<const char *>(&value), sizeof(value));
    };
    uint16_t db_name_len = static_cast<uint16_t>(db_name.size());
    uint16_t row_length = 64;
    int32_t time_res = static_cast<int32_t>(XStore_Proto::SECOND);
    uint32_t row_count = static_cast<uint32_t>(rows.size());
    append_pod(db_name_len);
    append_pod(row_length);
    append_pod(time_res);
    append_pod(row_count);
    payload.append(db_name);
    for (const RowFixture &r : rows) {
        XStore_Proto::RowData row_data = MakeRow(r.value);
        std::string row_bytes(row_length, '\0');
        row_data.SerializeToArray(row_bytes.data(), static_cast<int>(row_data.ByteSizeLong()));
        uint64_t epoch_time = r.epoch_time;
        append_pod(epoch_time);
        payload.append(row_bytes);
    }

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

    XStore store2;
    store2.InitDB();
    for (const RowFixture &r : rows) {
        XStore_Proto::UnaryQuery_REP query_rep;
        store2.UnaryQuery(r.epoch_time, "WALCRASHMULTI", &query_rep);
        ASSERT_TRUE(query_rep.has_data()) << "epoch_time " << r.epoch_time;
        ASSERT_EQ(query_rep.data().fielddata_size(), 1) << "epoch_time " << r.epoch_time;
        EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), r.value) << "epoch_time " << r.epoch_time;
    }
}

// Task 11 fix 1 regression test: WAL_FILE_PREALLOC_BYTES (40MB) was sized
// assuming exactly 1 row per WalRecord (true only for UnaryInsert -- see
// the NOTE on WAL_FILE_PREALLOC_BYTES in XStore_Tunables.h). BatchInsert
// builds one WalRecord per year-bucket group, so a single call with enough
// rows in one year can legitimately exceed that bound. This drives a
// single-year batch past WAL_FILE_PREALLOC_BYTES and confirms: (1) the
// write still durably lands and every row reads back correctly (WAL ring
// layout: the record exceeds ring_bytes/2, so it takes the interim direct
// path -- apply + data-file fdatasync -- instead of the ring), (2) no shard
// file grew past its fixed ring and no ring head moved, and (3) a subsequent
// InitDB doesn't crash or corrupt the data.
TEST_F(XStoreWalTest, BatchInsertRecordExceedingWalPreallocBoundPersistsAndReplays) {
    XStore store;
    CreateTestDb(store, "WALOVERSIZEBATCH", XStore_Proto::SECOND, 1024);

    // Row count is derived from the current WAL_FILE_PREALLOC_BYTES rather
    // than a fixed literal, so this test keeps proving what it claims to
    // (the batch genuinely exceeds the preallocated bound) regardless of
    // how that tunable gets retuned later -- a hardcoded row count here
    // already went stale once (a prior default of 40 MiB vs. today's much
    // larger one) without the test itself failing to compile or obviously
    // signal it had quietly stopped exercising the oversized_write path.
    // kRowWireBytes mirrors the per-row wire cost every row actually pays
    // (epoch_time(8) + row_length), see SerializeWalRecord/ParseWalRecord.
    constexpr size_t kRowWireBytes = 8 + 1024;
    const int kRowCount = static_cast<int>(XStoreTunables::WAL_FILE_PREALLOC_BYTES / kRowWireBytes) + 5000;
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("WALOVERSIZEBATCH");
    req.set_durabilitymode(XStore_Proto::SYNC);
    const unsigned long long base_ts = 1700800000ULL;
    for (int i = 0; i < kRowCount; ++i) {
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(base_ts + static_cast<unsigned long long>(i));
        *entry->mutable_data() = MakeRow(static_cast<double>(i));
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();

    // Confirm the write is real and durable, not silently dropped or
    // truncated -- sample across the whole range, plus the very last row
    // (most likely to be lost if the oversized-write fallback mishandled
    // the tail of the record).
    for (int i = 0; i < kRowCount; i += 3001) {
        XStore_Proto::UnaryQuery_REP q;
        store.UnaryQuery(base_ts + static_cast<unsigned long long>(i), "WALOVERSIZEBATCH", &q);
        ASSERT_TRUE(q.has_data()) << "row " << i;
        EXPECT_DOUBLE_EQ(q.data().fielddata(0).doubledata(), static_cast<double>(i)) << "row " << i;
    }
    XStore_Proto::UnaryQuery_REP last_q;
    store.UnaryQuery(base_ts + kRowCount - 1, "WALOVERSIZEBATCH", &last_q);
    ASSERT_TRUE(last_q.has_data()) << "last row";
    EXPECT_DOUBLE_EQ(last_q.data().fielddata(0).doubledata(), static_cast<double>(kRowCount - 1));

    // WAL ring layout: the shard files are fixed-size rings (header + ring_bytes)
    // and never grow. A record this large (> ring_bytes/2) cannot be logged at
    // all; it takes the interim direct path (apply + data-file fdatasync), so no
    // shard file holds it and none exceeds header + one ring.
    std::filesystem::path wal_dir = scratch->test_root / "DataStore" / "WAL";
    for (size_t shard_idx = 0; shard_idx < XStoreTunables::WAL_SHARD_COUNT; ++shard_idx) {
        std::filesystem::path shard_file = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");
        if (!std::filesystem::exists(shard_file)) continue;
        EXPECT_LE(std::filesystem::file_size(shard_file), walring::HEADER_REGION_BYTES + XStoreTunables::WAL_RING_BYTES)
            << shard_file << " grew past its ring";
    }
    for (auto &ring : walring::AllRings()) {
        if (ring->path().rfind(scratch->test_root.string(), 0) != 0) continue;
        EXPECT_EQ(ring->HeadLsn(), 0u) << ring->path() << ": the oversized record must not be appended to the ring";
    }

    // Re-running InitDB (same trick ReplayRecoversMultiRowUnappliedRecord
    // above uses to simulate a restart within one test process) must leave the
    // already-applied data intact: the direct-path record is in no ring, and
    // ring replay only touches records at or after the checkpoint.
    store.InitDB();
    XStore_Proto::UnaryQuery_REP replay_q;
    store.UnaryQuery(base_ts, "WALOVERSIZEBATCH", &replay_q);
    ASSERT_TRUE(replay_q.has_data());
    EXPECT_DOUBLE_EQ(replay_q.data().fielddata(0).doubledata(), 0.0);
}
