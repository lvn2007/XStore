// Standalone BatchInsert load generator for a real XStore server on another process or machine (the in-process
// XStoreZmqInsertPerfTest shares CPU and memory bandwidth with its server). Same request shape as that test
// (ZmqInsertPayload.h); requests are serialized before the start time, so only the server's work is timed.
//
//   xstore_zmq_insert_client create <host:port> <db>
//   xstore_zmq_insert_client run <host:port> <db> <seq|rand|scatter> <async|sync> <first_thread> <threads> <calls> <start_epoch_s>
//   seq: consecutive rows per call (XSTORE_SEQ_ROWS, default 1000); rand: the same rows shuffled inside the call; scatter: XSTORE_SCATTER_ROWS (default 40)
//   random timestamps from the thread's whole range per call, so the dirty bytes stay many separate ranges.
//
// `run` drives threads first_thread .. first_thread+threads-1 (each on its own disjoint timestamp range, its own REQ
// socket), waits for the wall-clock start epoch (NTP-synced nodes share one, so several machines start together),
// and prints one RESULT line with epoch-nanosecond start / last reply / last row landed. "Landed" polls a UnaryQuery
// for the last row of each thread's last call: a thread's calls all fall in one year, so its WAL records apply in order
// and the last row present means the thread's rows are applied (an ASYNC reply only says "enqueued").
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <zmq.hpp>

#include "tests/perf/ZmqInsertPayload.h"

using namespace zmq_insert_payload;

namespace {

size_t g_seq_rows = kRowsPerCall;   // rows per call for seq/rand (XSTORE_SEQ_ROWS; below ~700 the record takes the WAL ring)

using WallClock = std::chrono::system_clock;

long long EpochNs(WallClock::time_point t) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(t.time_since_epoch()).count();
}

// One request/reply on a fresh REQ socket; empty string when no reply arrived within `timeout_ms`.
std::string Call(zmq::context_t &ctx, const std::string &addr, const std::string &payload, int timeout_ms) {
    zmq::socket_t sock(ctx, zmq::socket_type::req);
    sock.set(zmq::sockopt::linger, 0);
    sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
    sock.connect("tcp://" + addr);
    if (!sock.send(zmq::buffer(payload), zmq::send_flags::none)) return {};
    zmq::message_t reply;
    if (!sock.recv(reply, zmq::recv_flags::none)) return {};
    return std::string(static_cast<const char *>(reply.data()), reply.size());
}

int Create(const std::string &addr, const std::string &db) {
    zmq::context_t ctx(1);
    XStore_Proto::Request request;
    XStore_Proto::CreateDB_REQ *create = request.mutable_createdb();
    create->set_rtype(XStore_Proto::CREATEDB);
    create->set_dbname(db);
    create->set_timeresolution(XStore_Proto::SECOND);
    create->set_rowlength(kRowLength);
    XStore_Proto::CreateDB_REP rep;
    const std::string reply = Call(ctx, addr, request.SerializeAsString(), 30000);
    if (reply.empty() || !rep.ParseFromString(reply)) {
        std::cerr << "create: no reply\n";
        return 1;
    }
    std::cout << rep.createdbstatus() << std::endl;
    return rep.createdbstatus().rfind("[INFO]", 0) == 0 ? 0 : 1;
}

bool RowPresent(zmq::context_t &ctx, const std::string &addr, const std::string &db, unsigned long long ts) {
    XStore_Proto::Request request;
    XStore_Proto::UnaryQuery_REQ *query = request.mutable_unaryquery();
    query->set_rtype(XStore_Proto::UNARYQUERY);
    query->set_timestamp(ts);
    query->set_dbname(db);
    XStore_Proto::UnaryQuery_REP rep;
    const std::string reply = Call(ctx, addr, request.SerializeAsString(), 10000);
    return !reply.empty() && rep.ParseFromString(reply) && rep.data().fielddata_size() > 0;
}

int Run(const std::string &addr, const std::string &db, bool shuffle, bool scatter, size_t scatter_rows,
        XStore_Proto::WalDurabilityMode mode, unsigned long long first_thread, int threads, int calls, double start_epoch_s) {
    zmq::context_t ctx(2);
    std::vector<std::vector<std::string>> payloads(static_cast<size_t>(threads));
    {
        std::vector<std::thread> builders;
        for (int t = 0; t < threads; ++t) {
            builders.emplace_back([&, t] {
                const unsigned long long id = first_thread + static_cast<unsigned long long>(t);
                std::mt19937_64 rng(1000 + id);
                for (int c = 0; c < calls; ++c) {
                    payloads[static_cast<size_t>(t)].push_back(
                        scatter ? BuildScatterPayload(db, mode, FirstTs(id, 0), rng, scatter_rows)
                                : BuildPayload(db, mode, FirstTs(id, 0) + static_cast<unsigned long long>(c) * g_seq_rows, shuffle, rng, g_seq_rows));
                }
            });
        }
        for (std::thread &b : builders) b.join();
    }

    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<unsigned long long> failed{0};
    std::mutex mu;
    long long start_ns = 0, last_reply_ns = 0, landed_ns = 0;
    unsigned long long not_landed = 0;
    std::vector<std::thread> clients;
    for (int t = 0; t < threads; ++t) {
        clients.emplace_back([&, t] {
            zmq::socket_t sock(ctx, zmq::socket_type::req);
            sock.set(zmq::sockopt::linger, 0);
            sock.set(zmq::sockopt::rcvtimeo, 120000);
            sock.connect("tcp://" + addr);
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            for (const std::string &payload : payloads[static_cast<size_t>(t)]) {
                zmq::message_t reply;
                if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) {
                    failed.fetch_add(1);
                    break;
                }
                XStore_Proto::BatchInsert_REP rep;
                if (!rep.ParseFromArray(reply.data(), static_cast<int>(reply.size())) ||
                    rep.insertstatus().find("successfully") == std::string::npos) {
                    failed.fetch_add(1);
                }
            }
            const long long reply_ns = EpochNs(WallClock::now());
            const unsigned long long id = first_thread + static_cast<unsigned long long>(t);
            const unsigned long long last_ts = FirstTs(id, 0) + static_cast<unsigned long long>(calls) * g_seq_rows - 1;
            bool landed = scatter;   // scattered calls have no single "last row" to poll for: the reply time is the result
            for (int i = 0; i < 24000 && !landed; ++i) {   // up to ~2 min
                landed = RowPresent(ctx, addr, db, last_ts);
                if (!landed) std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            const long long landed_at = EpochNs(WallClock::now());
            std::lock_guard<std::mutex> lock(mu);
            last_reply_ns = std::max(last_reply_ns, reply_ns);
            landed_ns = std::max(landed_ns, landed_at);
            if (!landed) ++not_landed;
        });
    }
    while (ready.load() < threads) std::this_thread::yield();
    const long long target_ns = static_cast<long long>(start_epoch_s * 1e9);
    while (EpochNs(WallClock::now()) < target_ns) std::this_thread::sleep_for(std::chrono::microseconds(200));
    start_ns = EpochNs(WallClock::now());
    go.store(true, std::memory_order_release);
    for (std::thread &c : clients) c.join();
    const unsigned long long rows = static_cast<unsigned long long>(threads) * static_cast<unsigned long long>(calls) * (scatter ? scatter_rows : g_seq_rows);
    std::printf("RESULT first_thread=%llu threads=%d rows=%llu failed=%llu not_landed=%llu start_ns=%lld reply_ns=%lld landed_ns=%lld lateness_us=%lld\n",
                first_thread, threads, rows, failed.load(), not_landed, start_ns, last_reply_ns, landed_ns,
                (start_ns - target_ns) / 1000);
    std::fflush(stdout);
    return failed.load() == 0 && not_landed == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char **argv) {
    if (argc == 4 && std::string(argv[1]) == "create") return Create(argv[2], argv[3]);
    if (argc == 10 && std::string(argv[1]) == "run") {
        const std::string pattern = argv[4], mode = argv[5];
        if ((pattern != "seq" && pattern != "rand" && pattern != "scatter") || (mode != "async" && mode != "sync")) {
            std::cerr << "pattern is seq|rand|scatter, mode is async|sync\n";
            return 2;
        }
        const char *scatter_rows_env = std::getenv("XSTORE_SCATTER_ROWS");   // rows per call for `scatter` (default 40: ~56 KiB, always a ring record)
        if (const char *seq_rows_env = std::getenv("XSTORE_SEQ_ROWS")) g_seq_rows = std::strtoull(seq_rows_env, nullptr, 10);
        const size_t scatter_rows = scatter_rows_env ? std::strtoull(scatter_rows_env, nullptr, 10) : 40;
        return Run(argv[2], argv[3], pattern == "rand", pattern == "scatter", scatter_rows,
                   mode == "sync" ? XStore_Proto::SYNC : XStore_Proto::ASYNC,
                   std::strtoull(argv[6], nullptr, 10), std::atoi(argv[7]), std::atoi(argv[8]), std::atof(argv[9]));
    }
    std::cerr << "usage: " << argv[0] << " create <host:port> <db> | run <host:port> <db> <seq|rand|scatter> <async|sync> "
              << "<first_thread> <threads> <calls> <start_epoch_s>\n";
    return 2;
}
