// tests/integration/XStoreWalTest.cpp
#include <gtest/gtest.h>
#include "include/XStore/XStore.h"
#include "XStoreTestSupport.h"
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>
#include <vector>

namespace {

using xstore_test::ScratchDir;

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

// SYNC_PER_COMMIT (the default) must return only after the row is durable
// AND applied -- a query for the same key immediately after UnaryInsert
// returns must see it, with no wait/retry needed on the test's side.
TEST_F(XStoreWalTest, SyncPerCommitAppliesBeforeReturning) {
    XStore store;
    XStore_Proto::CreateDB_REP create_rep = store.CreateDB("WALTEST", XStore_Proto::SECOND, 64);
    ASSERT_NE(create_rep.createdbstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1700000000ULL, MakeRow(42.5), "WALTEST", &insert_rep, XStore_Proto::SYNC_PER_COMMIT);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(1700000000ULL, "WALTEST", &query_rep);
    ASSERT_TRUE(query_rep.has_data());
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 42.5);
}

// ASYNC_ONLY must still return successfully (enqueue succeeded) and the row
// must become visible eventually -- this test polls with a bounded timeout
// instead of asserting immediate visibility, since ASYNC_ONLY explicitly
// does not wait for apply.
TEST_F(XStoreWalTest, AsyncOnlyEventuallyApplies) {
    XStore store;
    store.CreateDB("WALASYNC", XStore_Proto::SECOND, 64);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1700000001ULL, MakeRow(7.0), "WALASYNC", &insert_rep, XStore_Proto::ASYNC_ONLY);
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

// Crash-recovery replay: writes directly into a shard's WAL file the same
// way the writer thread would (bypassing UnaryInsert entirely, simulating
// "process crashed after fsync, before apply"), then constructs a fresh
// XStore against the same directory and confirms InitDB's ReplayWal
// reconstructed the row.
TEST_F(XStoreWalTest, ReplayRecoversUnappliedRecord) {
    {
        XStore store;
        store.CreateDB("WALCRASH", XStore_Proto::SECOND, 64);
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

    // Same CRC-32 as XStore_Wal.cpp's Crc32 -- duplicated here for the same
    // reason ReplayWal's own copy is duplicated (small, single-caller).
    auto crc32 = [](const std::string &data) {
        static const std::array<uint32_t, 256> table = [] {
            std::array<uint32_t, 256> t{};
            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t c = i;
                for (int k = 0; k < 8; ++k) {
                    c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
                }
                t[i] = c;
            }
            return t;
        }();
        uint32_t crc = 0xFFFFFFFFu;
        for (unsigned char byte : data) {
            crc = table[(crc ^ byte) & 0xFFu] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFFu;
    };
    uint32_t crc = crc32(payload);

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
    store.CreateDB("WALCONC", XStore_Proto::SECOND, 64);

    constexpr int kThreads = 8;
    constexpr int kPerThread = 50;
    std::vector<std::thread> workers;
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&store, t]() {
            for (int i = 0; i < kPerThread; ++i) {
                XStore_Proto::UnaryInsert_REP rep;
                unsigned long long ts = 1700100000ULL + static_cast<unsigned long long>(t * kPerThread + i);
                store.UnaryInsert(ts, MakeRow(static_cast<double>(t * kPerThread + i)), "WALCONC", &rep, XStore_Proto::GROUP_COMMIT);
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

    auto crc32 = [](const std::string &data) {
        static const std::array<uint32_t, 256> table = [] {
            std::array<uint32_t, 256> t{};
            for (uint32_t i = 0; i < 256; ++i) {
                uint32_t c = i;
                for (int k = 0; k < 8; ++k) {
                    c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
                }
                t[i] = c;
            }
            return t;
        }();
        uint32_t crc = 0xFFFFFFFFu;
        for (unsigned char byte : data) {
            crc = table[(crc ^ byte) & 0xFFu] ^ (crc >> 8);
        }
        return crc ^ 0xFFFFFFFFu;
    };
    uint32_t crc = crc32(payload);

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
