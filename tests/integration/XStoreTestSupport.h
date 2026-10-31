#ifndef XSTORE_TEST_SUPPORT_H
#define XSTORE_TEST_SUPPORT_H

#include <arpa/inet.h>
#include <atomic>
#include <cstdlib>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/spdlog.h>

#include "include/XStore/XStore.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_WalCheckpoint.h"

namespace xstore_test {

namespace fs = std::filesystem;

// Suite-wide default WAL ring size for in-process XStore instances. A production
// ring (64 MiB) is zero-filled and fsynced on creation -- about a second per test
// process for the eagerly created shard rings. Tests that depend on a specific
// size call walring::SetRingBytesForTests(explicit). The forked real server in
// XStoreGracefulShutdownTest is a separate process and keeps the production size.
inline constexpr uint64_t kTestDefaultRingBytes = 8u << 20;
// C++17 inline variable: initialized once per process, in every TU that includes this header.
// XSTORE_TEST_RING_BYTES (decimal bytes) overrides the 8 MiB test default -- the perf gate sets it to the
// production 64 MiB so benchmarks do not run against an undersized ring that checkpoints constantly.
inline uint64_t TestDefaultRingBytes() {
    if (const char *v = std::getenv("XSTORE_TEST_RING_BYTES"); v != nullptr && *v >= '0' && *v <= '9') {
        char *end = nullptr;
        const unsigned long long parsed = std::strtoull(v, &end, 10);
        if (end != v && *end == '\0' && parsed >= (1u << 20)) return static_cast<uint64_t>(parsed);
    }
    return kTestDefaultRingBytes;
}
inline const bool kTestDefaultRingInstalled = (walring::SetDefaultRingBytesForTests(TestDefaultRingBytes()), true);

// Process-wide log capture for tests that assert on warnings/errors. It is attached to the default logger ONCE,
// at static-init time (single-threaded, before any WAL-writer/checkpointer thread exists), so tests never swap or
// mutate the logger -- spdlog::default_logger_raw() is unsynchronised, so swapping it while those long-lived
// threads log is a data race. Tests Clear() it, act, then read it. warn+ only, so other tests pay almost nothing.
class LogCaptureSink final : public spdlog::sinks::base_sink<std::mutex> {
public:
    void Clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        lines_.clear();
    }
    std::vector<std::string> Lines() {
        std::lock_guard<std::mutex> lock(mutex_);
        return lines_;
    }
    // Number of captured messages containing `needle`.
    size_t Count(std::string_view needle) {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t n = 0;
        for (const std::string &line : lines_) {
            if (line.find(needle) != std::string::npos) ++n;
        }
        return n;
    }
    bool Contains(std::string_view needle) { return Count(needle) != 0; }

protected:
    void sink_it_(const spdlog::details::log_msg &msg) override {
        spdlog::memory_buf_t formatted;
        formatter_->format(msg, formatted);
        lines_.emplace_back(formatted.data(), formatted.size());
    }
    void flush_() override {}

private:
    std::vector<std::string> lines_;
};

inline std::shared_ptr<LogCaptureSink> InstallLogCapture() {
    auto sink = std::make_shared<LogCaptureSink>();
    sink->set_level(spdlog::level::warn);
    spdlog::default_logger()->sinks().push_back(sink);   // creates the default logger first if it does not exist yet
    return sink;
}
// C++17 inline variable: runs once per process during static init (single-threaded, before any WAL thread).
inline const std::shared_ptr<LogCaptureSink> kLogCapture = InstallLogCapture();

// RAII: the background checkpointer takes no action while one of these is alive,
// for tests that inspect or hand-drive ring heads/checkpoints. (RunCycle() still
// works explicitly; pausing waits for an in-flight cycle to finish.)
struct ScopedCheckpointerPause {
    ScopedCheckpointerPause() { walring::Checkpointer::Instance().SetPausedForTests(true); }
    ~ScopedCheckpointerPause() { walring::Checkpointer::Instance().SetPausedForTests(false); }
    ScopedCheckpointerPause(const ScopedCheckpointerPause &) = delete;
    ScopedCheckpointerPause &operator=(const ScopedCheckpointerPause &) = delete;
};

// One factory per `oneof oneOfData` branch in FData (see proto/XStore.proto)
// so tests can build rows out of every type the wire format actually allows,
// not just double/string/bool.
inline XStore_Proto::FData FDouble(double v)                { XStore_Proto::FData f; f.set_doubledata(v); return f; }
inline XStore_Proto::FData FFloat(float v)                  { XStore_Proto::FData f; f.set_floatdata(v); return f; }
inline XStore_Proto::FData FInt32(int32_t v)                { XStore_Proto::FData f; f.set_int32data(v); return f; }
inline XStore_Proto::FData FInt64(int64_t v)                { XStore_Proto::FData f; f.set_int64data(v); return f; }
inline XStore_Proto::FData FUInt32(uint32_t v)               { XStore_Proto::FData f; f.set_uint32data(v); return f; }
inline XStore_Proto::FData FUInt64(uint64_t v)               { XStore_Proto::FData f; f.set_uint64data(v); return f; }
inline XStore_Proto::FData FSInt32(int32_t v)                { XStore_Proto::FData f; f.set_sint32data(v); return f; }
inline XStore_Proto::FData FSInt64(int64_t v)                { XStore_Proto::FData f; f.set_sint64data(v); return f; }
inline XStore_Proto::FData FFixed32(uint32_t v)              { XStore_Proto::FData f; f.set_fixed32data(v); return f; }
inline XStore_Proto::FData FFixed64(uint64_t v)              { XStore_Proto::FData f; f.set_fixed64data(v); return f; }
inline XStore_Proto::FData FSFixed32(int32_t v)              { XStore_Proto::FData f; f.set_sfixed32data(v); return f; }
inline XStore_Proto::FData FSFixed64(int64_t v)              { XStore_Proto::FData f; f.set_sfixed64data(v); return f; }
inline XStore_Proto::FData FBool(bool v)                     { XStore_Proto::FData f; f.set_booldata(v); return f; }
inline XStore_Proto::FData FString(const std::string &v)     { XStore_Proto::FData f; f.set_stringdata(v); return f; }
inline XStore_Proto::FData FBytes(const std::string &v)      { XStore_Proto::FData f; f.set_bytesdata(v); return f; }

// Builds a RowData out of a mix of column types/counts so tests can express
// "1 column here, 3 columns there" without hand-rolling FData boilerplate.
inline XStore_Proto::RowData MakeRow(const std::vector<XStore_Proto::FData> &fields) {
    XStore_Proto::RowData row;
    for (const auto &f : fields) {
        *row.add_fielddata() = f;
    }
    return row;
}

// Creates a DB and asserts it succeeded (fatal -- a failure here means the test's own premise is broken).
inline void CreateTestDb(XStore &store, std::string_view name, XStore_Proto::TimeUnit resolution, short row_length) {
    XStore_Proto::CreateDB_REP rep = store.CreateDB(name, resolution, row_length);
    // Replicates UpperCaseInto's 'a'-'z' masking (XStore_Internal.h), not std::toupper.
    std::string upper_name(name);
    for (char &c : upper_name) {
        if (c >= 'a' && c <= 'z') {
            c = static_cast<char>(c & ~0x20);
        }
    }
    ASSERT_EQ(rep.createdbstatus(), "[INFO] [CREATE DB] [DB: " + upper_name + "] - Created DB")
        << rep.createdbstatus();
}

// XStore's production class only exposes RangeQuery/BatchQuery
// (see XStore.h) -- the network server is the only production caller, and it
// needs their bounded-memory chunked contract, not a materialized REP. Tests
// want the old synchronous shape (call it, get a filled-in REP back) so they
// can assert on it directly without each writing its own accumulate+parse
// boilerplate -- these two centralize that, matching RangeQuery/BatchQuery's
// former behavior exactly (same checks, same wire shape, just reassembled
// from the streamed implementation instead of a separate one).
inline XStore_Proto::RangeQuery_REP RangeQuery(XStore &store, unsigned long long int start_epoch_t,
                                                unsigned long long int end_epoch_t, std::string_view db_name,
                                                bool streamed_reply = false) {
    std::string buffer;
    store.RangeQuery(start_epoch_t, end_epoch_t, db_name, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
        [&](std::string &&chunk, bool /*is_last*/) { buffer += chunk; }, xstore::MemoryPool::Clock::time_point::max(), streamed_reply);
    XStore_Proto::RangeQuery_REP rep;
    rep.ParseFromString(buffer);
    return rep;
}

inline XStore_Proto::BatchQuery_REP BatchQuery(XStore &store, XStore_Proto::BatchQuery_REQ *proto_from_client,
                                                bool sort_time = true, bool streamed_reply = false) {
    std::string buffer;
    store.BatchQuery(proto_from_client, sort_time, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
        [&](std::string &&chunk, bool /*is_last*/) { buffer += chunk; }, xstore::MemoryPool::Clock::time_point::max(), streamed_reply);
    XStore_Proto::BatchQuery_REP rep;
    rep.ParseFromString(buffer);
    return rep;
}

inline std::atomic<int> &DirCounter() {
    static std::atomic<int> counter{0};
    return counter;
}

// XStore::GetBaseDir() resolves baseDir as the process's current working
// directory (mirroring the production layout: the XStore binary runs with
// its cwd set to <baseDir>, and <baseDir>/DataStore/<dbName>/... lives
// alongside it). Constructing this moves the process cwd into
// "<tmp>/<prefix>_<pid>_<n>" and restores it on destruction.
class ScratchDir {
public:
    fs::path original_cwd;
    fs::path test_root;

    explicit ScratchDir(const std::string &prefix) {
        original_cwd = fs::current_path();
        test_root = fs::temp_directory_path() /
            fs::path(prefix + "_" + std::to_string(getpid()) + "_" + std::to_string(DirCounter()++));
        fs::create_directories(test_root / "DataStore");
        fs::current_path(test_root);
    }

    ~ScratchDir() {
        std::error_code ec;
        fs::current_path(original_cwd, ec);
        fs::remove_all(test_root, ec);
    }
};

// Blocks until every WAL record enqueued so far for `db_name` (ASYNC ones included) has been written AND applied, so
// a test that returns right after a timed ASYNC enqueue cannot have its ScratchDir removed under a still-applying
// writer. A shard's writer thread handles its queue in order, but the queue is FIFO only per enqueuing thread, and every record of
// one (db, year) goes to the same shard (EnqueueWalRecord routes by (db, year)). So an ASYNC sentinel row enqueued
// last FROM THE SAME THREAD as the records being drained, in the same year, becoming visible proves everything that
// thread enqueued before it for that (db, year) was applied (other threads' records have no order against it, and
// records of another year may sit on another shard: call once per year; the ring's applied==head
// alone cannot: a batch mid-write/fsync or a direct-path apply leaves the ring untouched). Then every ring must be
// quiescent (AppliedLsn()==HeadLsn()), which also covers the post-apply index updates; only the rings under
// `store.data_store_dir` count (the shard rings live in <data_store_dir>/WAL, shared by every DB of that store), so a
// stale ring of another test's scratch dir cannot hold this one up. `sentinel_epoch` must be a
// timestamp of this DB the test does not otherwise rely on (one extra one-column row is written there). Fails the
// test with a clear message on timeout.
inline void WaitForWalDrained(XStore &store, std::string_view db_name, unsigned long long sentinel_epoch,
                              std::chrono::seconds timeout = std::chrono::seconds(60)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(sentinel_epoch, MakeRow({FDouble(0.0)}), db_name, &insert_rep, XStore_Proto::ASYNC);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos) << "WAL drain sentinel not enqueued: " << insert_rep.insertstatus();
    const std::string ring_prefix = store.data_store_dir + "/";
    const auto drained = [&] {
        XStore_Proto::UnaryQuery_REP query_rep;
        store.UnaryQuery(sentinel_epoch, db_name, &query_rep);
        if (query_rep.data().fielddata_size() == 0) return false;
        for (const std::shared_ptr<walring::Ring> &ring : walring::AllRings()) {
            if (ring->path().rfind(ring_prefix, 0) != 0) continue;
            if (ring->AppliedLsn() != ring->HeadLsn()) return false;
        }
        return true;
    };
    while (!drained()) {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "WAL did not drain within " << timeout.count() << "s (sentinel row of DB " << db_name << " never applied, or a ring has applied != head)";
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

// An OS-assigned loopback TCP port (-1 on failure). It is free only until close(): another process can take it
// before the caller binds, so callers that must survive that retry on a fresh port.
inline int FreePort() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    int port = -1;
    socklen_t len = sizeof(addr);
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0 &&
        getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len) == 0) {
        port = ntohs(addr.sin_port);
    }
    close(fd);
    return port;
}

}  // namespace xstore_test

#endif  // XSTORE_TEST_SUPPORT_H
