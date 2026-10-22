// BatchInsert over the real ZMQ server (loopback), opt-in: set XSTORE_ZMQ_PERF=1, one test per process
// (--gtest_filter), with XSTORE_TEST_RING_BYTES=67108864 like the other perf tests. The in-process perf cases hand
// XStore a ready BatchInsert_REQ; over ZMQ the server parses it into an arena-allocated request first, which is the
// path BatchInsert actually takes in production. Requests are serialized before the timer starts, so the client's
// protobuf cost is not measured and a server change shows up undiluted.
//
// Shape (BENCH_DB-like, as XBench's BATCH_INSERT_*): 128 double columns (1408 B rows), 1000 rows per call,
// XSTORE_ZMQ_CALLS (default 60) calls per client thread, one REQ socket per thread, each thread on its own disjoint
// timestamp range. SEQ sends ascending contiguous rows per call; RAND sends the same rows shuffled within the call.
// Tiers (client threads) from XSTORE_ZMQ_TIERS (default 1,8,16); server workers from XSTORE_ZMQ_WORKERS (default 16).
// ASYNC reports the reply rate (enqueue) and then the drain; SYNC replies after the rows are applied.
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <sys/resource.h>
#include <thread>
#include <vector>

#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include "tests/integration/XStoreTestSupport.h"
#include "tests/perf/ZmqInsertPayload.h"
#include "tests/integration/XStoreZmqTestSupport.h"

using namespace xstore_test;

namespace {

using namespace zmq_insert_payload;

int EnvInt(const char *name, int fallback) {
    const char *v = std::getenv(name);
    return v != nullptr && std::atoi(v) > 0 ? std::atoi(v) : fallback;
}

std::vector<int> Tiers() {
    std::vector<int> tiers;
    std::stringstream in(std::getenv("XSTORE_ZMQ_TIERS") != nullptr ? std::getenv("XSTORE_ZMQ_TIERS") : "1,8,16");
    for (std::string item; std::getline(in, item, ',');) {
        if (std::atoi(item.c_str()) > 0) tiers.push_back(std::atoi(item.c_str()));
    }
    return tiers;
}

using SteadyClock = std::chrono::steady_clock;

void RunTiers(const std::string &label, bool shuffle, XStore_Proto::WalDurabilityMode mode) {
    if (std::getenv("XSTORE_ZMQ_PERF") == nullptr) GTEST_SKIP() << "opt-in: set XSTORE_ZMQ_PERF=1";
    spdlog::set_level(spdlog::level::warn);
    ScratchDir scratch("xstore_zmq_perf");
    const int port = StartZmqServerOnFreePort(EnvInt("XSTORE_ZMQ_WORKERS", 16));
    ASSERT_GE(port, 0) << "no ZMQ server came up";
    const std::string addr = "tcp://127.0.0.1:" + std::to_string(port);
    const int calls = EnvInt("XSTORE_ZMQ_CALLS", 60);
    zmq::context_t ctx(2);
    XStore store;   // the drain wait reads the rings the server's workers write

    for (const int threads : Tiers()) {
        const std::string db = fmt::format("ZB{}", threads);
        {   // CreateDB over the wire, like a client
            XStore_Proto::CreateDB_REQ create;
            create.set_rtype(XStore_Proto::CREATEDB);
            create.set_dbname(db);
            create.set_timeresolution(XStore_Proto::SECOND);
            create.set_rowlength(kRowLength);
            XStore_Proto::Request request;
            *request.mutable_createdb() = create;
            zmq::socket_t sock(ctx, zmq::socket_type::req);
            sock.set(zmq::sockopt::linger, 0);
            sock.connect(addr);
            ASSERT_TRUE(sock.send(zmq::buffer(request.SerializeAsString()), zmq::send_flags::none));
            zmq::message_t reply;
            ASSERT_TRUE(sock.recv(reply, zmq::recv_flags::none));
        }

        std::vector<std::vector<std::string>> payloads(static_cast<size_t>(threads));
        std::vector<std::thread> builders;
        for (int t = 0; t < threads; ++t) {
            builders.emplace_back([&, t] {
                std::mt19937_64 rng(1000 + static_cast<unsigned>(t));
                for (int c = 0; c < calls; ++c) {
                    payloads[static_cast<size_t>(t)].push_back(
                        BuildPayload(db, mode, FirstTs(static_cast<unsigned long long>(t), static_cast<unsigned long long>(c)), shuffle, rng));
                }
            });
        }
        for (std::thread &b : builders) b.join();

        std::atomic<int> ready{0};
        std::atomic<bool> go{false};
        std::atomic<unsigned long long> failed{0};
        SteadyClock::time_point finished{};
        std::mutex finished_mu;
        std::vector<std::thread> clients;
        for (int t = 0; t < threads; ++t) {
            clients.emplace_back([&, t] {
                zmq::socket_t sock(ctx, zmq::socket_type::req);
                sock.set(zmq::sockopt::linger, 0);
                sock.set(zmq::sockopt::rcvtimeo, 120000);
                sock.connect(addr);
                ready.fetch_add(1);
                while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
                for (const std::string &payload : payloads[static_cast<size_t>(t)]) {
                    zmq::message_t reply;
                    if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) ||
                        !sock.recv(reply, zmq::recv_flags::none)) {
                        failed.fetch_add(1);
                        break;
                    }
                    XStore_Proto::BatchInsert_REP rep;
                    if (!rep.ParseFromArray(reply.data(), static_cast<int>(reply.size())) ||
                        rep.insertstatus().find("successfully") == std::string::npos) {
                        failed.fetch_add(1);
                    }
                }
                const SteadyClock::time_point now = SteadyClock::now();
                std::lock_guard<std::mutex> lock(finished_mu);
                finished = std::max(finished, now);
            });
        }
        while (ready.load() < threads) std::this_thread::yield();
        const SteadyClock::time_point start = SteadyClock::now();
        go.store(true, std::memory_order_release);
        for (std::thread &c : clients) c.join();
        const double reply_s = std::chrono::duration<double>(finished - start).count();
        const unsigned long long rows = static_cast<unsigned long long>(threads) * static_cast<unsigned long long>(calls) * kRowsPerCall;

        const SteadyClock::time_point drain_start = SteadyClock::now();
        WaitForWalDrained(store, db, kBaseTs - 1);
        const double drain_s = std::chrono::duration<double>(SteadyClock::now() - drain_start).count();
        rusage usage{};
        getrusage(RUSAGE_SELF, &usage);
        std::cout << fmt::format("[PERF] ZmqBatchInsert[{}] | clients={} | rows={} | failed={} | reply_elapsed={:.3f}s "
                                 "reply_rate={:.0f} rows/sec | drain={:.3f}s | e2e_rate={:.0f} rows/sec | peak_rss={} KiB",
                                 label, threads, rows, failed.load(), reply_s, static_cast<double>(rows) / reply_s,
                                 drain_s, static_cast<double>(rows) / (reply_s + drain_s), usage.ru_maxrss)
                  << std::endl;
        EXPECT_EQ(failed.load(), 0u);
    }
}
}  // namespace

TEST(XStoreZmqInsertPerfTest, SeqAsync) { RunTiers("SEQ ASYNC", false, XStore_Proto::ASYNC); }
TEST(XStoreZmqInsertPerfTest, RandAsync) { RunTiers("RAND ASYNC", true, XStore_Proto::ASYNC); }
TEST(XStoreZmqInsertPerfTest, SeqSync) { RunTiers("SEQ SYNC", false, XStore_Proto::SYNC); }
TEST(XStoreZmqInsertPerfTest, RandSync) { RunTiers("RAND SYNC", true, XStore_Proto::SYNC); }
