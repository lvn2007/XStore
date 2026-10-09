// Black-box tests for graceful shutdown (Durability A, DD7): starts the real
// built XStore binary in a temp dir, inserts rows over ZMQ, then delivers
// SIGTERM / SIGINT / SIGKILL and inspects what is left on disk.
//
// Every server process is owned by a ServerProcess guard that SIGKILLs and
// reaps it by PID on any exit path, and the temp dir is removed after it, so
// no test can leave a server running or a (few hundred MiB: the server
// preallocates its WAL rings, 64 MiB each) directory behind.
//
// The two _exit(1) paths of src/main.cpp (drain timeout, second signal) need a worker
// that cannot finish its request, with no production hook: see StallOneWorker.
#include <gtest/gtest.h>
#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "include/Networking/ZMQ_Server.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_Tunables.h"
#include "XStore/XStore_WalRing.h"
#include "XStore/XStore_ZoneMap.h"
#include "XStoreTestSupport.h"

using namespace xstore_test;

namespace {

namespace chrono = std::chrono;
constexpr const char *DB_NAME = "GRACEFUL";
constexpr uint64_t BASE_TS = 1000;  // year files are sparse: ~1 GB apparent, a few KB on disk
constexpr int ROWS = 40;
constexpr uint16_t ROW_LENGTH = 32;

// Removes the directory on destruction; declared before the ServerProcess so
// the server is already dead by then.
struct TempDir {
    std::filesystem::path path;
    TempDir() {
        std::string tmpl = (std::filesystem::temp_directory_path() / "xstore_graceful_XXXXXX").string();
        std::vector<char> buf(tmpl.begin(), tmpl.end());
        buf.push_back('\0');
        if (mkdtemp(buf.data()) != nullptr) path = buf.data();
    }
    ~TempDir() {
        std::error_code ec;
        if (!path.empty()) std::filesystem::remove_all(path, ec);
    }
};

class ServerProcess {
public:
    // hold_checkpoints: XSTORE_CKPT_IDLE_MS / XSTORE_CKPT_MAX_AGE_MS = 1 h, so the WAL checkpointer cannot
    // run a cycle before a kill (a graceful stop still runs its final cycle).
    ServerProcess(const std::filesystem::path &dir, int port, int stats_port, bool hold_checkpoints = false) : port_(port) {
        std::string dir_s = dir.string(), port_s = std::to_string(port), stats_s = std::to_string(stats_port);
        pid_ = fork();
        if (pid_ == 0) {
            if (chdir(dir_s.c_str()) != 0) _exit(127);
            if (hold_checkpoints) {
                setenv("XSTORE_CKPT_IDLE_MS", "3600000", 1);
                setenv("XSTORE_CKPT_MAX_AGE_MS", "3600000", 1);
            }
            int devnull = open("/dev/null", O_RDWR);
            if (devnull >= 0) { dup2(devnull, 0); dup2(devnull, 1); dup2(devnull, 2); }
            execl(XSTORE_BIN, XSTORE_BIN, "-H", "127.0.0.1", "-p", port_s.c_str(), "-s", stats_s.c_str(),
                  "-t", "2", static_cast<char *>(nullptr));
            _exit(127);
        }
    }
    ~ServerProcess() {
        if (pid_ > 0) {
            kill(pid_, SIGKILL);
            waitpid(pid_, nullptr, 0);
        }
    }
    ServerProcess(const ServerProcess &) = delete;
    ServerProcess &operator=(const ServerProcess &) = delete;

    bool Started() const { return pid_ > 0; }
    pid_t pid() const { return pid_; }
    int port() const { return port_; }
    void Signal(int sig) const { kill(pid_, sig); }

    // Blocks until the process is gone or `timeout` elapsed. Returns the raw
    // wait status, or -1 on timeout (the destructor then SIGKILLs it).
    int WaitExit(chrono::seconds timeout) {
        auto deadline = chrono::steady_clock::now() + timeout;
        while (chrono::steady_clock::now() < deadline) {
            int status = 0;
            pid_t r = waitpid(pid_, &status, WNOHANG);
            if (r == pid_) { pid_ = -1; return status; }
            if (r < 0) { pid_ = -1; return -1; }
            std::this_thread::sleep_for(chrono::milliseconds(50));
        }
        return -1;
    }

    // Roundtrip a Request; a fresh REQ socket per call so a timeout can't
    // wedge the REQ state machine.
    template <typename RepT>
    bool Call(const XStore_Proto::Request &req, RepT &rep, int timeout_ms = 5000) {
        zmq::context_t ctx(1);
        zmq::socket_t sock(ctx, zmq::socket_type::req);
        sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
        sock.set(zmq::sockopt::linger, 0);
        sock.connect("tcp://127.0.0.1:" + std::to_string(port_));
        std::string payload = req.SerializeAsString();
        if (!sock.send(zmq::buffer(payload), zmq::send_flags::none)) return false;
        zmq::message_t reply;
        if (!sock.recv(reply, zmq::recv_flags::none)) return false;
        return rep.ParseFromArray(reply.data(), static_cast<int>(reply.size()));
    }

    bool WaitReady(chrono::seconds timeout) {
        auto deadline = chrono::steady_clock::now() + timeout;
        while (chrono::steady_clock::now() < deadline) {
            XStore_Proto::Request ping;
            ping.mutable_ping()->set_rtype(XStore_Proto::PING);
            XStore_Proto::Ping_REP rep;
            if (Call(ping, rep, 300)) return true;
            int status = 0;
            if (waitpid(pid_, &status, WNOHANG) == pid_) { pid_ = -1; return false; }  // died during boot
        }
        return false;
    }

private:
    pid_t pid_ = -1;
    int port_;
};

constexpr int MAX_START_ATTEMPTS = 5;

// Starts a server on fresh ports and waits for it to answer. A port taken by another process between FreePort()
// and the server's bind makes the server die during boot: that (and only that; a server still alive after
// `ready_timeout` is not retried) is retried on new ports, MAX_START_ATTEMPTS times. nullptr = no ready server.
std::unique_ptr<ServerProcess> StartReadyServer(const std::filesystem::path &dir, chrono::seconds ready_timeout,
                                                bool hold_checkpoints = false) {
    for (int attempt = 0; attempt < MAX_START_ATTEMPTS; ++attempt) {
        int port = FreePort(), stats_port = FreePort();
        if (port < 0 || stats_port < 0 || port == stats_port) continue;
        auto server = std::make_unique<ServerProcess>(dir, port, stats_port, hold_checkpoints);
        if (!server->Started()) return nullptr;
        if (server->WaitReady(ready_timeout)) return server;
        if (server->Started()) return nullptr;  // still alive but not answering: not a port problem
    }
    return nullptr;
}

void InsertRows(ServerProcess &server, uint32_t column_count = 0) {
    XStore_Proto::Request create;
    auto *c = create.mutable_createdb();
    c->set_rtype(XStore_Proto::CREATEDB);
    c->set_dbname(DB_NAME);
    c->set_timeresolution(XStore_Proto::SECOND);
    c->set_rowlength(ROW_LENGTH);
    c->set_columncount(column_count);
    XStore_Proto::CreateDB_REP create_rep;
    ASSERT_TRUE(server.Call(create, create_rep));
    ASSERT_EQ(create_rep.createdbstatus().find("[INFO]"), 0u) << create_rep.createdbstatus();

    for (int i = 0; i < ROWS; ++i) {
        XStore_Proto::Request req;
        auto *ins = req.mutable_unaryinsert();
        ins->set_rtype(XStore_Proto::UNARYINSERT);
        ins->set_dbname(DB_NAME);
        ins->set_timestamp(BASE_TS + static_cast<uint64_t>(i));
        ins->set_durabilitymode(XStore_Proto::SYNC);
        *ins->mutable_data() = MakeRow({FDouble(10.0 + i)});
        XStore_Proto::UnaryInsert_REP rep;
        ASSERT_TRUE(server.Call(req, rep));
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    }
}

// Returns the MIN of column 0 over all inserted rows, or NaN on a failed call.
double QueryMin(ServerProcess &server) {
    XStore_Proto::Request req;
    auto *m = req.mutable_min();
    m->set_rtype(XStore_Proto::MIN);
    m->set_dbname(DB_NAME);
    m->set_starttime(BASE_TS);
    m->set_endtime(BASE_TS + ROWS - 1);
    m->set_columnidx(0);
    XStore_Proto::Min_REP rep;
    if (!server.Call(req, rep)) return std::nan("");
    return rep.opresult().doubledata();
}

// dirty flag of every .zmap / .catidx under <dir>/DataStore/GRACEFUL.
struct IndexDirtyFlags {
    std::vector<uint32_t> zmap, catidx;
};

IndexDirtyFlags ReadDirtyFlags(const std::filesystem::path &dir) {
    IndexDirtyFlags flags;
    for (auto &entry : std::filesystem::directory_iterator(dir / "DataStore" / DB_NAME)) {
        const std::string ext = entry.path().extension().string();
        if (ext != ".zmap" && ext != ".catidx") continue;
        int fd = open(entry.path().c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        if (fd < 0) continue;
        if (ext == ".zmap") {
            ZoneMapFileHeader h{};
            EXPECT_EQ(pread(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h)));
            flags.zmap.push_back(h.dirty);
        } else {
            CatIdxFileHeader h{};
            EXPECT_EQ(pread(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h)));
            flags.catidx.push_back(h.dirty);
        }
        close(fd);
    }
    return flags;
}

// Highest checkpoint_lsn over every ring-format shard file under <dir>/DataStore/WAL
// (0 if none advanced). A graceful shutdown's final cycle moves the busy shard's
// checkpoint to its head (> 0 once any record was logged); a killed server that
// had checkpoints held at 1 h leaves every ring at 0.
uint64_t MaxRingCheckpoint(const std::filesystem::path &dir) {
    uint64_t best = 0;
    const auto wal_dir = dir / "DataStore" / "WAL";
    if (!std::filesystem::is_directory(wal_dir)) return 0;
    for (auto &entry : std::filesystem::directory_iterator(wal_dir)) {
        if (entry.path().extension() != ".WAL") continue;
        int fd = open(entry.path().c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        if (fd < 0) continue;
        walring::RingHeader h;
        if (walring::ReadBestHeader(fd, h)) best = std::max(best, h.checkpoint_lsn);
        close(fd);
    }
    return best;
}

std::string ReadLog(const std::filesystem::path &dir) {
    std::ifstream in(dir / "logs" / "server.log");
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

void ExpectAllDirty(const IndexDirtyFlags &flags, uint32_t expected) {
    ASSERT_FALSE(flags.zmap.empty()) << "no .zmap on disk";
    ASSERT_FALSE(flags.catidx.empty()) << "no .catidx on disk";
    for (uint32_t d : flags.zmap) EXPECT_EQ(d, expected) << ".zmap dirty flag";
    for (uint32_t d : flags.catidx) EXPECT_EQ(d, expected) << ".catidx dirty flag";
}

void ExpectCleanExit(int status) {
    ASSERT_NE(status, -1) << "server did not exit within the timeout";
    ASSERT_TRUE(WIFEXITED(status)) << "server was killed by signal " << (WIFSIGNALED(status) ? WTERMSIG(status) : 0);
    EXPECT_EQ(WEXITSTATUS(status), 0);
}


constexpr uint32_t WIDE_COLUMNS = 70;  // seeds the DB's index width past ZONE_MAP_MAX_TRACKED_COLUMNS (64)
constexpr uint64_t SECONDS_PER_YEAR = 365ull * 24 * 3600;
constexpr uint64_t STALL_EPOCH = SECONDS_PER_YEAR + 5;  // 1971: a year no row was ever written to

// Fire-and-forget request: a REQ socket that sends and never reads, kept alive for the test's duration.
struct PendingRequest {
    zmq::context_t ctx{1};
    zmq::socket_t sock{ctx, zmq::socket_type::req};
    bool Send(int port, const XStore_Proto::Request &req) {
        sock.set(zmq::sockopt::linger, 0);
        sock.connect("tcp://127.0.0.1:" + std::to_string(port));
        const std::string payload = req.SerializeAsString();
        return sock.send(zmq::buffer(payload), zmq::send_flags::none).has_value();
    }
};

// True when some thread of `pid` is blocked in the kernel's FIFO open (wait_for_partner / fifo_open as the
// wait channel). `readable` stays false if no /proc/<pid>/task/*/wchan could be read at all.
bool AnyThreadWaitsOnFifoOpen(pid_t pid, bool &readable) {
    std::error_code ec;
    for (auto &task : std::filesystem::directory_iterator("/proc/" + std::to_string(pid) + "/task", ec)) {
        std::ifstream in(task.path() / "wchan");
        std::string wchan;
        if (!in || !(in >> wchan)) continue;
        readable = true;
        if (wchan == "wait_for_partner" || wchan == "fifo_open") return true;
    }
    return false;
}

// Makes one worker of the real server unable to finish: the year-1971 zone-map path is a FIFO with no writer, and a
// MIN on column 65 (past the 64 floor, inside the seeded 70) makes the full-scan notice open that path O_RDONLY,
// which blocks in the kernel until a writer shows up. ZMQ shutdown cannot interrupt it, so after SIGTERM the
// drain wait in ZmqServer::StartServer runs into its timeout. Returns once a thread is verifiably blocked there.
// Call after InsertRows. The FIFO must be removed (RemoveStallFifo) before anything else reads the DB's files.
std::filesystem::path StallFifoPath(const std::filesystem::path &dir) {
    return dir / "DataStore" / DB_NAME / "1971.zmap";
}

void RemoveStallFifo(const std::filesystem::path &dir) {
    std::error_code ec;
    std::filesystem::remove(StallFifoPath(dir), ec);
}

void StallOneWorker(ServerProcess &server, const std::filesystem::path &dir, PendingRequest &pending) {
    ASSERT_EQ(mkfifo(StallFifoPath(dir).c_str(), 0600), 0) << "mkfifo failed";
    XStore_Proto::Request req;
    auto *m = req.mutable_min();
    m->set_rtype(XStore_Proto::MIN);
    m->set_dbname(DB_NAME);
    m->set_starttime(STALL_EPOCH);
    m->set_endtime(STALL_EPOCH + 10);
    m->set_columnidx(65);
    ASSERT_TRUE(pending.Send(server.port(), req));

    bool readable = false;
    const auto deadline = chrono::steady_clock::now() + chrono::seconds(10);
    while (chrono::steady_clock::now() < deadline) {
        if (AnyThreadWaitsOnFifoOpen(server.pid(), readable)) return;
        std::this_thread::sleep_for(chrono::milliseconds(20));
    }
    if (!readable) GTEST_SKIP() << "/proc/<pid>/task/*/wchan is not readable here: cannot tell a worker is stalled";
    FAIL() << "no server thread ever blocked opening the FIFO";
}

// Waits (bounded) until the checkpointer has moved a ring past 0 and left it there: from then on a restart has no
// WAL to replay (replay would re-extend the year file and force a ~50 s full rebuild).
void WaitForCheckpointToSettle(const std::filesystem::path &dir) {
    uint64_t last = 0;
    int stable = 0;
    const auto deadline = chrono::steady_clock::now() + chrono::seconds(10);
    while (chrono::steady_clock::now() < deadline && stable < 4) {
        const uint64_t now = MaxRingCheckpoint(dir);
        stable = (now > 0 && now == last) ? stable + 1 : 0;
        last = now;
        std::this_thread::sleep_for(chrono::milliseconds(100));
    }
    ASSERT_GE(stable, 4) << "the WAL checkpoint never settled";
}

}  // namespace

// The server preallocates (sparsely) the whole calendar year: ~31.5M rows,
// and every restart-time index rebuild / catidx compaction preads them all
// (tens of seconds). The server is dead when this runs; cutting the file to
// the written rows leaves the data intact and keeps restarts fast. (A restart
// that REPLAYS the WAL re-extends the file to its full length again.)
void TrimYearFile(const std::filesystem::path &dir) {
    for (auto &entry : std::filesystem::directory_iterator(dir / "DataStore" / DB_NAME)) {
        if (entry.path().extension() != ".XSTORE") continue;
        std::error_code ec;
        std::filesystem::resize_file(entry.path(), (BASE_TS + ROWS) * ROW_LENGTH, ec);
        EXPECT_FALSE(ec) << ec.message();
    }
}

class XStoreGracefulShutdownTest : public ::testing::Test {
protected:
    TempDir dir;
    void SetUp() override { ASSERT_FALSE(dir.path.empty()) << "mkdtemp failed"; }
};

// SIGTERM: exit code 0 well inside 15 s, every index marked clean, and the
// next start neither rebuilds an index nor replays the (checkpointed) WAL.
TEST_F(XStoreGracefulShutdownTest, SigtermExitsZeroMarksIndexesCleanAndNextStartDoesNotRebuild) {
    {
        auto server_ptr = StartReadyServer(dir.path, chrono::seconds(60));
        ASSERT_NE(server_ptr, nullptr) << "server did not come up";
        ServerProcess &server = *server_ptr;
        ASSERT_NO_FATAL_FAILURE(InsertRows(server));
        ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 1u));  // precondition

        server.Signal(SIGTERM);
        ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(server.WaitExit(chrono::seconds(15))));
    }
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 0u));
    EXPECT_GT(MaxRingCheckpoint(dir.path), 0u) << "the final checkpoint cycle did not advance the ring";
    TrimYearFile(dir.path);

    auto restarted_ptr = StartReadyServer(dir.path, chrono::seconds(60));
    ASSERT_NE(restarted_ptr, nullptr) << "restart did not come up";
    ServerProcess &restarted = *restarted_ptr;
    EXPECT_DOUBLE_EQ(QueryMin(restarted), 10.0);
    restarted.Signal(SIGTERM);
    ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(restarted.WaitExit(chrono::seconds(15))));

    const std::string log = ReadLog(dir.path);
    EXPECT_EQ(log.find("marked dirty"), std::string::npos) << "clean restart rebuilt a dirty index";
    EXPECT_EQ(log.find("REBUILD]"), std::string::npos) << "clean restart rebuilt an index (WAL not checkpointed?)";
    EXPECT_EQ(log.find("[WAL REPLAY] - Replayed"), std::string::npos) << "clean restart replayed the WAL";
}

TEST_F(XStoreGracefulShutdownTest, SigintAlsoShutsDownGracefully) {
    {
        auto server_ptr = StartReadyServer(dir.path, chrono::seconds(60));
        ASSERT_NE(server_ptr, nullptr) << "server did not come up";
        ServerProcess &server = *server_ptr;
        ASSERT_NO_FATAL_FAILURE(InsertRows(server));

        server.Signal(SIGINT);
        ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(server.WaitExit(chrono::seconds(15))));
    }
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 0u));
}

// SIGKILL gives the server no chance to clean up: headers stay dirty, the WAL
// ring still holds every un-checkpointed record (the server is started with the
// checkpointer's idle/age triggers held at 1 h, so no cycle can run before the
// kill: deterministic), and the next start replays them, rebuilds the indexes and
// still answers correctly. (The replayed year is force-rebuilt, which is why the
// dirty-flag-only rebuild path -- "marked dirty" in the log -- is not observable
// here; the in-process DurabilityRebuildTest cases cover it. Replay re-extends
// the year file to its full sparse length, so this restart rebuilds ~31.5M
// rows, ~50 s: a year has that many rows at every resolution CreateDB permits.)
TEST_F(XStoreGracefulShutdownTest, SigkillLeavesIndexesDirtyAndRestartRebuildsThem) {
    {
        auto server_ptr = StartReadyServer(dir.path, chrono::seconds(60), /*hold_checkpoints=*/true);
        ASSERT_NE(server_ptr, nullptr) << "server did not come up";
        ServerProcess &server = *server_ptr;
        ASSERT_NO_FATAL_FAILURE(InsertRows(server));

        server.Signal(SIGKILL);
        int status = server.WaitExit(chrono::seconds(15));
        ASSERT_NE(status, -1);
        ASSERT_TRUE(WIFSIGNALED(status));
    }
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 1u));
    EXPECT_EQ(MaxRingCheckpoint(dir.path), 0u) << "a killed server must leave every ring un-checkpointed";
    TrimYearFile(dir.path);

    {
        auto restarted_ptr = StartReadyServer(dir.path, chrono::seconds(240));
        ASSERT_NE(restarted_ptr, nullptr) << "restart did not come up";
        ServerProcess &restarted = *restarted_ptr;
        EXPECT_DOUBLE_EQ(QueryMin(restarted), 10.0);
        restarted.Signal(SIGTERM);  // graceful, so the log is flushed
        ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(restarted.WaitExit(chrono::seconds(15))));
    }
    const std::string log = ReadLog(dir.path);
    EXPECT_NE(log.find("REBUILD]"), std::string::npos)
        << "restart log lacks the index-rebuild lines; log head:\n" << log.substr(0, 3000);
    EXPECT_NE(log.find("rebuilding"), std::string::npos);
    EXPECT_NE(log.find("[WAL REPLAY] - Replayed"), std::string::npos)
        << "restart after SIGKILL must replay the ring from its checkpoint; log head:\n" << log.substr(0, 3000);
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 0u));
}

// main.cpp's first _exit(1): SIGTERM arrives, a worker cannot finish (StallOneWorker), the drain wait runs out its
// ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S and the process exits 1 WITHOUT the clean shutdown, so every index stays dirty. Rows
// acked before the signal (and checkpointed) must survive: the restart rebuilds the dirty indexes and answers.
TEST_F(XStoreGracefulShutdownTest, SigtermWithStalledWorkerExitsOneAfterTheDrainTimeoutAndDataSurvives) {
    const chrono::seconds drain_timeout(XStoreTunables::ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S);
    {
        auto server_ptr = StartReadyServer(dir.path, chrono::seconds(60));
        ASSERT_NE(server_ptr, nullptr) << "server did not come up";
        ServerProcess &server = *server_ptr;
        ASSERT_NO_FATAL_FAILURE(InsertRows(server, WIDE_COLUMNS));
        ASSERT_NO_FATAL_FAILURE(WaitForCheckpointToSettle(dir.path));
        PendingRequest pending;
        ASSERT_NO_FATAL_FAILURE(StallOneWorker(server, dir.path, pending));
        if (::testing::Test::IsSkipped()) return;

        const auto start = chrono::steady_clock::now();
        server.Signal(SIGTERM);
        const int status = server.WaitExit(drain_timeout + chrono::seconds(10));
        const auto elapsed = chrono::steady_clock::now() - start;
        ASSERT_NE(status, -1) << "server did not exit";
        ASSERT_TRUE(WIFEXITED(status)) << "server was killed by signal " << (WIFSIGNALED(status) ? WTERMSIG(status) : 0);
        EXPECT_EQ(WEXITSTATUS(status), 1);
        EXPECT_GE(elapsed, drain_timeout - chrono::seconds(1)) << "exited before the drain timeout";
        EXPECT_LT(elapsed, drain_timeout + chrono::seconds(5)) << "took far longer than the drain timeout";
    }
    RemoveStallFifo(dir.path);
    const std::string first_log = ReadLog(dir.path);
    EXPECT_NE(first_log.find("Workers did not drain in time"), std::string::npos) << first_log;
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 1u));
    TrimYearFile(dir.path);

    auto restarted_ptr = StartReadyServer(dir.path, chrono::seconds(60));
    ASSERT_NE(restarted_ptr, nullptr) << "restart did not come up";
    ServerProcess &restarted = *restarted_ptr;
    EXPECT_DOUBLE_EQ(QueryMin(restarted), 10.0);
    restarted.Signal(SIGTERM);
    ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(restarted.WaitExit(chrono::seconds(15))));
    EXPECT_NE(ReadLog(dir.path).find("REBUILD]"), std::string::npos) << "restart did not rebuild the dirty indexes";
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 0u));
}

// main.cpp's second _exit(1): a second signal while shutdown is already in progress means "now". Same stalled
// worker keeps the shutdown in progress (the drain would take the whole timeout); SIGTERM and SIGINT are sent
// back to back (different signals do not coalesce while blocked, so each is consumed by one of the signal thread's
// two sigwaits whatever the timing). The exit must be immediate, not the drain timeout.
TEST_F(XStoreGracefulShutdownTest, SecondSignalDuringShutdownExitsOneImmediately) {
    const chrono::seconds drain_timeout(XStoreTunables::ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S);
    auto server_ptr = StartReadyServer(dir.path, chrono::seconds(60));
    ASSERT_NE(server_ptr, nullptr) << "server did not come up";
    ServerProcess &server = *server_ptr;
    ASSERT_NO_FATAL_FAILURE(InsertRows(server, WIDE_COLUMNS));
    PendingRequest pending;
    ASSERT_NO_FATAL_FAILURE(StallOneWorker(server, dir.path, pending));
    if (::testing::Test::IsSkipped()) return;

    const auto start = chrono::steady_clock::now();
    server.Signal(SIGTERM);
    server.Signal(SIGINT);
    const int status = server.WaitExit(drain_timeout);
    const auto elapsed = chrono::steady_clock::now() - start;
    ASSERT_NE(status, -1) << "server did not exit";
    ASSERT_TRUE(WIFEXITED(status)) << "server was killed by signal " << (WIFSIGNALED(status) ? WTERMSIG(status) : 0);
    EXPECT_EQ(WEXITSTATUS(status), 1);
    EXPECT_LT(elapsed, drain_timeout / 2) << "a second signal must not wait for the drain";
    const std::string log = ReadLog(dir.path);
    EXPECT_NE(log.find("Received signal"), std::string::npos) << log;
    EXPECT_EQ(log.find("Workers did not drain in time"), std::string::npos) << "exited via the drain timeout, not the second signal";
}
