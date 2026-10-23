// Standalone mixed-workload load generator for a real XStore server on another machine: SYNC scattered BatchInserts
// (the writer) next to random point UnaryQuerys (the readers) on one year, the network version of the in-process
// ScatteredInsertWithConcurrentReads perf case (which shares CPU with its server and so cannot be trusted).
//
//   xstore_zmq_mixed_client create  <host:port> <db>
//   xstore_zmq_mixed_client seed    <host:port> <db>
//   xstore_zmq_mixed_client scatter <host:port> <db> <threads> <duration_s> <start_epoch_s> <seed>
//   xstore_zmq_mixed_client read    <host:port> <db> <threads> <duration_s> <start_epoch_s> <seed>
//
// `scatter` and `read` wait for the wall-clock start epoch (NTP-synced nodes start together), run for exactly
// <duration_s>, and print one RESULT line. Scatter sends 1000-row BatchInserts (SYNC) of distinct random seconds of
// 2024, rows of 3 columns (double, int64, "payload") like the perf case; requests are serialized before the start.
// Read keeps `threads` REQ sockets each querying a random second of the year one after the other.
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
#include <unordered_set>
#include <vector>

#include <zmq.hpp>

#include "XStore.pb.h"

namespace {

using WallClock = std::chrono::system_clock;
using SteadyClock = std::chrono::steady_clock;

constexpr unsigned long long kYearStart = 1704067200ULL;   // 2024-01-01 00:00:00 UTC
constexpr unsigned long long kYearSeconds = 366ULL * 86400;
constexpr short kRowLength = 64;
constexpr size_t kRowsPerCall = 1000;

long long EpochNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(WallClock::now().time_since_epoch()).count();
}

XStore_Proto::RowData ScatterRow(unsigned long long i) {
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(static_cast<double>(i));
    row.add_fielddata()->set_int64data(static_cast<int64_t>(i));
    row.add_fielddata()->set_stringdata("payload");
    return row;
}

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
    if (reply.empty() || !rep.ParseFromString(reply)) return 1;
    std::cout << rep.createdbstatus() << std::endl;
    return rep.createdbstatus().rfind("[INFO]", 0) == 0 ? 0 : 1;
}

// The year file must exist before the readers start, or they would time the missing-file path.
int Seed(const std::string &addr, const std::string &db) {
    zmq::context_t ctx(1);
    XStore_Proto::Request request;
    XStore_Proto::UnaryInsert_REQ *insert = request.mutable_unaryinsert();
    insert->set_rtype(XStore_Proto::UNARYINSERT);
    insert->set_timestamp(kYearStart);
    *insert->mutable_data() = ScatterRow(0);
    insert->set_dbname(db);
    insert->set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::UnaryInsert_REP rep;
    const std::string reply = Call(ctx, addr, request.SerializeAsString(), 60000);
    if (reply.empty() || !rep.ParseFromString(reply)) return 1;
    std::cout << rep.insertstatus() << std::endl;
    return rep.insertstatus().find("[INFO]") != std::string::npos ? 0 : 1;
}

void WaitUntilEpoch(double start_epoch_s) {
    const long long target = static_cast<long long>(start_epoch_s * 1e9);
    while (EpochNs() < target) std::this_thread::sleep_for(std::chrono::microseconds(200));
}

int Scatter(const std::string &addr, const std::string &db, int threads, double duration_s, double start_epoch_s,
            unsigned long long seed) {
    zmq::context_t ctx(2);
    // Enough requests for the whole window at several times the expected rate; a thread that runs out simply idles.
    const size_t per_thread = static_cast<size_t>(duration_s * 400) + 50;
    std::vector<std::vector<std::string>> payloads(static_cast<size_t>(threads));
    {
        std::vector<std::thread> builders;
        for (int t = 0; t < threads; ++t) {
            builders.emplace_back([&, t] {
                std::mt19937_64 rng(seed * 1000003ULL + static_cast<unsigned long long>(t));
                std::unordered_set<unsigned long long> used;
                for (size_t c = 0; c < per_thread; ++c) {
                    XStore_Proto::Request request;
                    XStore_Proto::BatchInsert_REQ *batch = request.mutable_batchinsert();
                    batch->set_dbname(db);
                    batch->set_durabilitymode(XStore_Proto::SYNC);
                    for (size_t i = 0; i < kRowsPerCall;) {
                        const unsigned long long r = rng() % kYearSeconds;
                        if (!used.insert(r).second) continue;
                        XStore_Proto::BatchInsert_DATA *entry = batch->add_batchinsert_data();
                        entry->set_timestamp(kYearStart + r);
                        *entry->mutable_data() = ScatterRow(r);
                        ++i;
                    }
                    payloads[static_cast<size_t>(t)].push_back(request.SerializeAsString());
                }
            });
        }
        for (std::thread &b : builders) b.join();
    }
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<unsigned long long> batches{0}, failed{0};
    std::vector<std::thread> clients;
    const long long deadline_ns = static_cast<long long>((start_epoch_s + duration_s) * 1e9);
    for (int t = 0; t < threads; ++t) {
        clients.emplace_back([&, t] {
            zmq::socket_t sock(ctx, zmq::socket_type::req);
            sock.set(zmq::sockopt::linger, 0);
            sock.set(zmq::sockopt::rcvtimeo, 60000);
            sock.connect("tcp://" + addr);
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            for (const std::string &payload : payloads[static_cast<size_t>(t)]) {
                if (EpochNs() >= deadline_ns) break;
                zmq::message_t reply;
                if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) {
                    failed.fetch_add(1);
                    break;
                }
                XStore_Proto::BatchInsert_REP rep;
                if (!rep.ParseFromArray(reply.data(), static_cast<int>(reply.size())) ||
                    rep.insertstatus().find("successfully") == std::string::npos) {
                    failed.fetch_add(1);
                } else if (EpochNs() <= deadline_ns) {
                    batches.fetch_add(1);   // only batches acknowledged inside the window count
                }
            }
        });
    }
    while (ready.load() < threads) std::this_thread::yield();
    WaitUntilEpoch(start_epoch_s);
    go.store(true, std::memory_order_release);
    for (std::thread &c : clients) c.join();
    std::printf("RESULT scatter batches=%llu rows=%llu failed=%llu window_s=%.3f rows_per_s=%.0f\n", batches.load(),
                batches.load() * kRowsPerCall, failed.load(), duration_s,
                static_cast<double>(batches.load() * kRowsPerCall) / duration_s);
    std::fflush(stdout);
    return failed.load() == 0 ? 0 : 1;
}

int Read(const std::string &addr, const std::string &db, int threads, double duration_s, double start_epoch_s,
         unsigned long long seed) {
    zmq::context_t ctx(2);
    std::atomic<int> ready{0};
    std::atomic<bool> go{false};
    std::atomic<unsigned long long> failed{0};
    std::mutex mu;
    std::vector<double> all_us;
    const long long deadline_ns = static_cast<long long>((start_epoch_s + duration_s) * 1e9);
    std::vector<std::thread> clients;
    for (int t = 0; t < threads; ++t) {
        clients.emplace_back([&, t] {
            zmq::socket_t sock(ctx, zmq::socket_type::req);
            sock.set(zmq::sockopt::linger, 0);
            sock.set(zmq::sockopt::rcvtimeo, 30000);
            sock.connect("tcp://" + addr);
            std::mt19937_64 rng(seed * 1000003ULL + static_cast<unsigned long long>(t));
            std::vector<double> mine;
            mine.reserve(1 << 20);
            XStore_Proto::Request request;
            XStore_Proto::UnaryQuery_REQ *query = request.mutable_unaryquery();
            query->set_rtype(XStore_Proto::UNARYQUERY);
            query->set_dbname(db);
            ready.fetch_add(1);
            while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
            std::string payload;
            while (EpochNs() < deadline_ns) {
                query->set_timestamp(kYearStart + rng() % kYearSeconds);
                request.SerializeToString(&payload);
                const SteadyClock::time_point t0 = SteadyClock::now();
                zmq::message_t reply;
                if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) {
                    failed.fetch_add(1);
                    break;
                }
                const SteadyClock::time_point t1 = SteadyClock::now();
                if (EpochNs() <= deadline_ns) mine.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
            }
            std::lock_guard<std::mutex> lock(mu);
            all_us.insert(all_us.end(), mine.begin(), mine.end());
        });
    }
    while (ready.load() < threads) std::this_thread::yield();
    WaitUntilEpoch(start_epoch_s);
    go.store(true, std::memory_order_release);
    for (std::thread &c : clients) c.join();
    std::sort(all_us.begin(), all_us.end());
    const auto at = [&](double q) { return all_us.empty() ? 0.0 : all_us[static_cast<size_t>(q * static_cast<double>(all_us.size() - 1))]; };
    std::printf("RESULT read ops=%zu failed=%llu window_s=%.3f ops_per_s=%.0f p50_us=%.1f p99_us=%.1f max_us=%.1f\n",
                all_us.size(), failed.load(), duration_s, static_cast<double>(all_us.size()) / duration_s, at(0.5),
                at(0.99), all_us.empty() ? 0.0 : all_us.back());
    std::fflush(stdout);
    return failed.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char **argv) {
    const std::string cmd = argc > 1 ? argv[1] : "";
    if (cmd == "create" && argc == 4) return Create(argv[2], argv[3]);
    if (cmd == "seed" && argc == 4) return Seed(argv[2], argv[3]);
    if ((cmd == "scatter" || cmd == "read") && argc == 8) {
        const int threads = std::atoi(argv[4]);
        const double duration_s = std::atof(argv[5]), start_epoch_s = std::atof(argv[6]);
        const unsigned long long seed = std::strtoull(argv[7], nullptr, 10);
        return cmd == "scatter" ? Scatter(argv[2], argv[3], threads, duration_s, start_epoch_s, seed)
                                : Read(argv[2], argv[3], threads, duration_s, start_epoch_s, seed);
    }
    std::cerr << "usage: " << argv[0] << " create|seed <host:port> <db> | scatter|read <host:port> <db> <threads> "
              << "<duration_s> <start_epoch_s> <seed>\n";
    return 2;
}
