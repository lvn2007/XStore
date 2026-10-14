// Failure branches of UnaryDelete/BatchDelete/RangeDelete when fallocate(PUNCH_HOLE) fails.
//
// Production carries no fault-injection hook. Instead this TU defines fallocate64() itself: the linker binds the
// xstore_core static library's references to the executable's own definition ahead of libc's, and the definition
// forwards to the real libc function (dlsym RTLD_NEXT) unless a ScopedFallocatePunchFailure is alive. Only calls with
// FALLOC_FL_PUNCH_HOLE are ever intercepted; the default state is pure forwarding.
#include <gtest/gtest.h>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <memory>
#include <string>

#include "XStoreTestSupport.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/XStore/XStore_ZoneMap.h"
#include <unistd.h>

using namespace xstore_test;

namespace {

std::atomic<int> g_punch_errno{0};          // 0 = not armed
std::atomic<int> g_fail_only_nth{0};        // 0 = fail every punch, N = fail only the Nth armed punch (1-based)
std::atomic<int> g_punch_calls{0};

constexpr unsigned long long TS_BASE = 1717070400ULL;  // 2024-05-30 12:00:00 UTC

}  // namespace

extern "C" int fallocate64(int fd, int mode, off64_t offset, off64_t len) {
    using Fn = int (*)(int, int, off64_t, off64_t);
    static const Fn real = reinterpret_cast<Fn>(dlsym(RTLD_NEXT, "fallocate64"));
    const int err = g_punch_errno.load(std::memory_order_relaxed);
    if (err != 0 && (mode & FALLOC_FL_PUNCH_HOLE) != 0) {
        const int call = g_punch_calls.fetch_add(1, std::memory_order_relaxed) + 1;
        const int nth = g_fail_only_nth.load(std::memory_order_relaxed);
        if (nth == 0 || call == nth) {
            errno = err;
            return -1;
        }
    }
    return real(fd, mode, offset, len);
}

namespace {

class ScopedFallocatePunchFailure {
public:
    explicit ScopedFallocatePunchFailure(int err, int fail_only_nth = 0) {
        g_punch_calls.store(0);
        g_fail_only_nth.store(fail_only_nth);
        g_punch_errno.store(err);
    }
    ~ScopedFallocatePunchFailure() {
        g_punch_errno.store(0);
        g_fail_only_nth.store(0);
    }
    ScopedFallocatePunchFailure(const ScopedFallocatePunchFailure &) = delete;
    ScopedFallocatePunchFailure &operator=(const ScopedFallocatePunchFailure &) = delete;
};

class XStoreDeleteFaultTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;
    std::unique_ptr<XStore> store;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_delfault");
        // cfg_shards is a process-wide cache keyed by DB name; see XStoreTest::SetUp.
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        store = std::make_unique<XStore>();
        CreateTestDb(*store, "TESTDB", XStore_Proto::SECOND, 128);
    }
    void TearDown() override {
        store.reset();
        scratch.reset();
    }

    void InsertRows(int count) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname("TESTDB");
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (int i = 0; i < count; i++) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(TS_BASE + static_cast<unsigned long long>(i));
            *entry->mutable_data() = MakeRow({FInt64(i + 1)});
        }
        XStore_Proto::BatchInsert_REP rep;
        store->BatchInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    }

    // true iff the row at TS_BASE + i still holds its inserted value.
    bool RowPresent(int i) {
        XStore_Proto::UnaryQuery_REP rep;
        store->UnaryQuery(TS_BASE + static_cast<unsigned long long>(i), "TESTDB", &rep);
        return rep.data().fielddata_size() == 1 && rep.data().fielddata(0).int64data() == i + 1;
    }

    XStore_Proto::BatchDelete_REP BatchDeleteRows(int count) {
        XStore_Proto::BatchDelete_REQ req;
        req.set_dbname("TESTDB");
        for (int i = 0; i < count; i++) req.add_timestamp(TS_BASE + static_cast<unsigned long long>(i));
        return store->BatchDelete(&req);
    }
};

TEST_F(XStoreDeleteFaultTest, BatchDeleteSuccessMessageUnchangedWhenUnarmed) {
    InsertRows(3);
    const XStore_Proto::BatchDelete_REP rep = BatchDeleteRows(3);
    EXPECT_EQ(rep.deletestatus(), "[INFO] [BATCH DELETE] [DB: TESTDB] - Deleted 3 items");
    for (int i = 0; i < 3; i++) EXPECT_FALSE(RowPresent(i)) << i;
}

TEST_F(XStoreDeleteFaultTest, BatchDeleteAllPunchesFailingReportsWarningAndKeepsRows) {
    InsertRows(3);
    XStore_Proto::BatchDelete_REP rep;
    {
        ScopedFallocatePunchFailure fail(EOPNOTSUPP);
        rep = BatchDeleteRows(3);
        EXPECT_EQ(g_punch_calls.load(), 3);
    }
    const std::string &status = rep.deletestatus();
    EXPECT_EQ(status.rfind("[WARNING] ", 0), 0U) << status;
    EXPECT_NE(status.find("Deleted 0 items"), std::string::npos) << status;
    EXPECT_NE(status.find("3 item(s) could not be deleted"), std::string::npos) << status;
    EXPECT_NE(status.find(std::strerror(EOPNOTSUPP)), std::string::npos) << status;
    for (int i = 0; i < 3; i++) EXPECT_TRUE(RowPresent(i)) << i;
}

TEST_F(XStoreDeleteFaultTest, BatchDeleteSecondOfThreePunchesFailingDeletesOnlyOtherTwo) {
    InsertRows(3);
    XStore_Proto::BatchDelete_REP rep;
    {
        ScopedFallocatePunchFailure fail(EOPNOTSUPP, 2);
        rep = BatchDeleteRows(3);  // the batch is sorted ascending, so punch #2 is row 1
    }
    const std::string &status = rep.deletestatus();
    EXPECT_EQ(status.rfind("[WARNING] ", 0), 0U) << status;
    EXPECT_NE(status.find("Deleted 2 items; 1 item(s) could not be deleted"), std::string::npos) << status;
    EXPECT_NE(status.find(std::strerror(EOPNOTSUPP)), std::string::npos) << status;
    EXPECT_FALSE(RowPresent(0));
    EXPECT_TRUE(RowPresent(1));
    EXPECT_FALSE(RowPresent(2));
}

TEST_F(XStoreDeleteFaultTest, RangeDeleteSuccessMessageUnchangedWhenUnarmed) {
    InsertRows(5);
    const XStore_Proto::RangeDelete_REP rep = store->RangeDelete(TS_BASE, TS_BASE + 4, "TESTDB");
    EXPECT_EQ(rep.deletestatus(), "[INFO] [RANGE DELETE] [DB: TESTDB] - Deleted 5 items");
    for (int i = 0; i < 5; i++) EXPECT_FALSE(RowPresent(i)) << i;
}

TEST_F(XStoreDeleteFaultTest, RangeDeleteFailingPunchReportsWarningAndKeepsRows) {
    InsertRows(5);
    XStore_Proto::RangeDelete_REP rep;
    {
        ScopedFallocatePunchFailure fail(EOPNOTSUPP);
        rep = store->RangeDelete(TS_BASE, TS_BASE + 4, "TESTDB");
    }
    const std::string &status = rep.deletestatus();
    EXPECT_EQ(status.rfind("[WARNING] ", 0), 0U) << status;
    EXPECT_NE(status.find("Deleted 0 items; 5 item(s) could not be deleted"), std::string::npos) << status;
    EXPECT_NE(status.find(std::strerror(EOPNOTSUPP)), std::string::npos) << status;
    for (int i = 0; i < 5; i++) EXPECT_TRUE(RowPresent(i)) << i;
}

TEST_F(XStoreDeleteFaultTest, UnaryDeleteSuccessMessageUnchangedWhenUnarmed) {
    InsertRows(1);
    const XStore_Proto::UnaryDelete_REP rep = store->UnaryDelete(TS_BASE, "TESTDB");
    EXPECT_EQ(rep.deletestatus().rfind("[INFO] ", 0), 0U) << rep.deletestatus();
    EXPECT_EQ(rep.deletestatus().find("Unable to delete"), std::string::npos) << rep.deletestatus();
    EXPECT_FALSE(RowPresent(0));
}

TEST_F(XStoreDeleteFaultTest, UnaryDeleteFailingPunchReportsErrorAndKeepsRow) {
    InsertRows(1);
    XStore_Proto::UnaryDelete_REP rep;
    {
        ScopedFallocatePunchFailure fail(EOPNOTSUPP);
        rep = store->UnaryDelete(TS_BASE, "TESTDB");
    }
    const std::string &status = rep.deletestatus();
    EXPECT_EQ(status.rfind("[ERROR] ", 0), 0U) << status;
    EXPECT_NE(status.find("Unable to delete"), std::string::npos) << status;
    EXPECT_NE(status.find(std::strerror(EOPNOTSUPP)), std::string::npos) << status;
    EXPECT_TRUE(RowPresent(0));
}

// RangeDelete over a whole zone-map block: the data punch succeeds, the zone map's own punch (which resets the block
// to empty) fails. The block must then be flagged instead, so it is never trusted with the deleted row's value.
TEST_F(XStoreDeleteFaultTest, RangeDeleteFallsBackToFlagWhenZoneMapResetFails) {
    InsertRows(1);
    const TimeStruct ts(static_cast<time_t>(TS_BASE), XStore_Proto::SECOND);
    const uint64_t rpb = RowsPerBlock(128);
    const uint64_t block = BlockIndexForRow(TS_BASE - ts.first_epoch, rpb);
    const unsigned long long block_start = ts.first_epoch + block * rpb;
    XStore_Proto::RangeDelete_REP rep;
    {
        ScopedFallocatePunchFailure fail(EOPNOTSUPP, 2); // punch 1 = data file, punch 2 = zone map reset
        rep = store->RangeDelete(block_start, block_start + rpb - 1, "TESTDB");
        EXPECT_EQ(g_punch_calls.load(), 2);
    }
    EXPECT_EQ(rep.deletestatus().rfind("[INFO] ", 0), 0U) << rep.deletestatus();
    EXPECT_FALSE(RowPresent(0));
    const std::string db_path = (scratch->test_root / "DataStore" / "TESTDB").string();
    EXPECT_TRUE(XStore::GetOrOpenZoneMap(db_path, ts.year, 128).BlockInexact(block));
}

// A zone map that cannot be opened refuses the delete, and the status says why (not "fallocate failed").
TEST_F(XStoreDeleteFaultTest, DeleteRefusedWhenZoneMapCannotBeOpenedSaysSo) {
    InsertRows(3);
    const TimeStruct ts(static_cast<time_t>(TS_BASE), XStore_Proto::SECOND);
    XStore::ClearZoneMapRegistryForTests();
    const std::string zmap_path = (scratch->test_root / "DataStore" / "TESTDB" / (std::to_string(ts.year) + ".zmap")).string();
    {
        int fd = open(zmap_path.c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        const uint32_t bad_magic = 0;
        ASSERT_EQ(pwrite(fd, &bad_magic, sizeof(bad_magic), 0), static_cast<ssize_t>(sizeof(bad_magic)));
        close(fd);
    }
    const std::string unary = store->UnaryDelete(TS_BASE, "TESTDB").deletestatus();
    EXPECT_EQ(unary.rfind("[ERROR] ", 0), 0U) << unary;
    EXPECT_NE(unary.find("zone map could not be opened"), std::string::npos) << unary;

    XStore_Proto::BatchDelete_REQ req;
    req.set_dbname("TESTDB");
    req.add_timestamp(TS_BASE + 1);
    const std::string batch = store->BatchDelete(&req).deletestatus();
    EXPECT_NE(batch.find("Deleted 0 items; 1 item(s) could not be deleted (zone map could not be opened)"), std::string::npos)
        << batch;

    const std::string range = store->RangeDelete(TS_BASE + 2, TS_BASE + 2, "TESTDB").deletestatus();
    EXPECT_NE(range.find("Deleted 0 items; 1 item(s) could not be deleted (zone map could not be opened)"), std::string::npos)
        << range;
    for (int i = 0; i < 3; i++) EXPECT_TRUE(RowPresent(i)) << i;
}

}  // namespace
