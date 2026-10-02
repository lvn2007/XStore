// Black-box tests for graceful shutdown (Durability A, DD7): starts the real
// built XStore binary in a temp dir, inserts rows over ZMQ, then delivers
// SIGTERM / SIGINT / SIGKILL and inspects what is left on disk.
//
// Every server process is owned by a ServerProcess guard that SIGKILLs and
// reaps it by PID on any exit path, and the temp dir is removed after it, so
// no test can leave a server running or an (up to 2 GB: the server
// preallocates its WAL shards) directory behind.
#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "include/Networking/ZMQ_Server.h"
#include "XStore/XStore_CatIdx.h"
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

int FreePort() {
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

class ServerProcess {
public:
    ServerProcess(const std::filesystem::path &dir, int port, int stats_port) : port_(port) {
        std::string dir_s = dir.string(), port_s = std::to_string(port), stats_s = std::to_string(stats_port);
        pid_ = fork();
        if (pid_ == 0) {
            if (chdir(dir_s.c_str()) != 0) _exit(127);
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

void InsertRows(ServerProcess &server) {
    XStore_Proto::Request create;
    auto *c = create.mutable_createdb();
    c->set_rtype(XStore_Proto::CREATEDB);
    c->set_dbname(DB_NAME);
    c->set_timeresolution(XStore_Proto::SECOND);
    c->set_rowlength(ROW_LENGTH);
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

// After a SIGKILL the WAL still holds each shard's un-checkpointed records.
// Replaying them would (a) force-rebuild the years they touched whatever the
// dirty flag says, which would hide what this test is about, and (b) re-extend
// the year file to its full length. The SYNC inserts were already applied to
// the data file, so checkpoint every WAL ring by hand (checkpoint_lsn := end of
// log, persisted in the ring header) to leave the dirty flag as the only
// rebuild trigger. (Until the ring-aware replay lands, ReplayWal skips ring
// files outright, so this is not yet load-bearing -- it keeps the test valid
// once replay consults checkpoint_lsn.)
void CheckpointWalByHand(const std::filesystem::path &dir) {
    for (auto &entry : std::filesystem::directory_iterator(dir / "DataStore" / "WAL")) {
        EXPECT_TRUE(xstore_test::CheckpointRingFileByHand(entry.path().string())) << entry.path();
    }
}

// The server preallocates (sparsely) the whole calendar year: ~31.5M rows,
// and every restart-time index rebuild / catidx compaction preads them all
// (tens of seconds). The server is dead when this runs; cutting the file to
// the written rows leaves the data intact and keeps restarts fast.
void TrimYearFile(const std::filesystem::path &dir) {
    for (auto &entry : std::filesystem::directory_iterator(dir / "DataStore" / DB_NAME)) {
        if (entry.path().extension() != ".XSTORE") continue;
        std::error_code ec;
        std::filesystem::resize_file(entry.path(), (BASE_TS + ROWS) * ROW_LENGTH, ec);
        EXPECT_FALSE(ec) << ec.message();
    }
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

}  // namespace

class XStoreGracefulShutdownTest : public ::testing::Test {
protected:
    TempDir dir;
    void SetUp() override { ASSERT_FALSE(dir.path.empty()) << "mkdtemp failed"; }
};

// SIGTERM: exit code 0 well inside 15 s, every index marked clean, and the
// next start neither rebuilds an index nor replays the (checkpointed) WAL.
TEST_F(XStoreGracefulShutdownTest, SigtermExitsZeroMarksIndexesCleanAndNextStartDoesNotRebuild) {
    {
        ServerProcess server(dir.path, FreePort(), FreePort());
        ASSERT_TRUE(server.Started());
        ASSERT_TRUE(server.WaitReady(chrono::seconds(60))) << "server did not come up";
        ASSERT_NO_FATAL_FAILURE(InsertRows(server));
        ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 1u));  // precondition

        server.Signal(SIGTERM);
        ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(server.WaitExit(chrono::seconds(15))));
    }
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 0u));
    TrimYearFile(dir.path);

    ServerProcess restarted(dir.path, FreePort(), FreePort());
    ASSERT_TRUE(restarted.Started());
    ASSERT_TRUE(restarted.WaitReady(chrono::seconds(60))) << "restart did not come up";
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
        ServerProcess server(dir.path, FreePort(), FreePort());
        ASSERT_TRUE(server.Started());
        ASSERT_TRUE(server.WaitReady(chrono::seconds(60))) << "server did not come up";
        ASSERT_NO_FATAL_FAILURE(InsertRows(server));

        server.Signal(SIGINT);
        ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(server.WaitExit(chrono::seconds(15))));
    }
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 0u));
}

// SIGKILL gives the server no chance to clean up: headers stay dirty, and the
// next start rebuilds them and still answers correctly.
TEST_F(XStoreGracefulShutdownTest, SigkillLeavesIndexesDirtyAndRestartRebuildsThem) {
    {
        ServerProcess server(dir.path, FreePort(), FreePort());
        ASSERT_TRUE(server.Started());
        ASSERT_TRUE(server.WaitReady(chrono::seconds(60))) << "server did not come up";
        ASSERT_NO_FATAL_FAILURE(InsertRows(server));

        server.Signal(SIGKILL);
        int status = server.WaitExit(chrono::seconds(15));
        ASSERT_NE(status, -1);
        ASSERT_TRUE(WIFSIGNALED(status));
    }
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 1u));
    ASSERT_NO_FATAL_FAILURE(CheckpointWalByHand(dir.path));
    TrimYearFile(dir.path);

    {
        ServerProcess restarted(dir.path, FreePort(), FreePort());
        ASSERT_TRUE(restarted.Started());
        ASSERT_TRUE(restarted.WaitReady(chrono::seconds(60))) << "restart did not come up";
        EXPECT_DOUBLE_EQ(QueryMin(restarted), 10.0);
        restarted.Signal(SIGTERM);  // graceful, so the log is flushed
        ASSERT_NO_FATAL_FAILURE(ExpectCleanExit(restarted.WaitExit(chrono::seconds(15))));
    }
    const std::string log = ReadLog(dir.path);
    EXPECT_NE(log.find("marked dirty"), std::string::npos)
        << "restart log lacks the dirty-rebuild line; log head:\n" << log.substr(0, 3000);
    EXPECT_NE(log.find("rebuilding"), std::string::npos);
    ASSERT_NO_FATAL_FAILURE(ExpectAllDirty(ReadDirtyFlags(dir.path), 0u));
}
