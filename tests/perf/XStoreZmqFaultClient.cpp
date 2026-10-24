// Fault-injection companion of xstore_zmq_insert_client: SYNC BatchInserts that log every acknowledgement, then a
// verifier that checks, after the server was restarted (WAL replay), that every call the server acknowledged is
// really there with the right values. Run it against a server whose data directory sits on a device that starts
// failing writes (dm-flakey) to prove that no acknowledged SYNC write is lost.
//
//   xstore_zmq_fault_client run <host:port> <db> <threads> <seconds> <log_path> [rows_per_call[,rows...] [SYNC|ASYNC [first_thread]]]
//   xstore_zmq_fault_client verify <host:port> <db> <log_path> [fault_start_epoch_ms]
//
// `run` (the DB must exist: xstore_zmq_insert_client create) drives <threads> threads, each on its own timestamp
// range, one BatchInsert (rows_per_call wide rows, default 1000 = ZmqInsertPayload.h; a call of 1000 rows is ~1.4 MB and
// takes the direct path, fewer than ~700 rows (under WAL_DIRECT_MIN_BYTES, and under 45 rows to stay on the ring even
// under ring pressure) takes the WAL ring) after the other until <seconds> elapsed; the mode defaults to SYNC. The log's
// first line "# rows=<n>[,<n>...] mode=<SYNC|ASYNC>" tells `verify` what was run. A list ("20,1000") makes call c of every
// thread use rows[c % list size]: ring-sized and direct-sized records mixed on the same year files. Each
// call appends one line to <log_path>: "ACK|FAIL|TIMEOUT <thread> <call> <epoch_ms>"; only ACK means the server
// said "successfully". A thread stops at its first TIMEOUT (a REQ socket cannot continue after one). Row i of a
// call holds the doubles i*0.5 + column, so `verify` checks the value as well as the presence of 8 sampled rows
// (first, last, middle, 5 random) of every ACKed call, and prints
//   VERIFY acked=<n> missing=<calls with a missing sample> wrong=<calls with a wrong value> acks_after_fault=<n>
// (a log with more than 30000 ACKs is thinned to every k-th call: "checked=" says how many were looked at;
// acks_after_fault counts ACK lines stamped at or after fault_start_epoch_ms; 0 if the argument is absent).
// Exit status is non-zero if any acknowledged call is wrong, or missing in SYNC mode. An ASYNC ack promises no
// durability, so there a missing call is only reported (the exit status then checks that the survivors are right).
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <zmq.hpp>

#include "tests/perf/ZmqInsertPayload.h"

using namespace zmq_insert_payload;

namespace {

using WallClock = std::chrono::system_clock;

long long EpochMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(WallClock::now().time_since_epoch()).count();
}

// "20" or "20,1000" -> {20, 1000}; empty if anything is not in 1..kRowsPerCall (the timestamp stride of a call).
std::vector<size_t> ParseRows(const std::string &arg) {
    std::vector<size_t> rows;
    std::stringstream in(arg);
    std::string item;
    while (std::getline(in, item, ',')) {
        char *end = nullptr;
        const unsigned long long n = std::strtoull(item.c_str(), &end, 10);
        if (item.empty() || *end != '\0' || n == 0 || n > kRowsPerCall) return {};
        rows.push_back(static_cast<size_t>(n));
    }
    return rows;
}

int Run(const std::string &addr, const std::string &db, int threads, int seconds, const std::string &log_path, const std::string &rows_arg,
        const std::vector<size_t> &rows, XStore_Proto::WalDurabilityMode mode, int first_thread) {
    std::FILE *log = std::fopen(log_path.c_str(), "w");
    if (log == nullptr) {
        std::cerr << "cannot open " << log_path << "\n";
        return 2;
    }
    std::fprintf(log, "# rows=%s mode=%s\n", rows_arg.c_str(), mode == XStore_Proto::SYNC ? "SYNC" : "ASYNC");
    std::mutex log_mu;
    auto record = [&](const char *what, int t, int c) {
        std::lock_guard<std::mutex> lock(log_mu);
        std::fprintf(log, "%s %d %d %lld\n", what, t, c, EpochMs());
        std::fflush(log);
    };
    zmq::context_t ctx(2);
    const auto deadline = WallClock::now() + std::chrono::seconds(seconds);
    std::atomic<unsigned long long> acks{0}, fails{0}, timeouts{0};
    std::vector<std::thread> workers;
    // Thread ids (timestamp range, log lines) start at first_thread: several client nodes use disjoint ranges.
    for (int t = first_thread; t < first_thread + threads; ++t) {
        workers.emplace_back([&, t] {
            zmq::socket_t sock(ctx, zmq::socket_type::req);
            sock.set(zmq::sockopt::linger, 0);
            sock.set(zmq::sockopt::rcvtimeo, 20000);
            sock.connect("tcp://" + addr);
            std::mt19937_64 rng(1000 + static_cast<unsigned long long>(t));
            for (int c = 0; WallClock::now() < deadline; ++c) {
                const std::string payload = BuildPayload(db, mode, FirstTs(static_cast<unsigned long long>(t),
                                                         static_cast<unsigned long long>(c)), false, rng, rows[static_cast<size_t>(c) % rows.size()]);
                zmq::message_t reply;
                if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) {
                    record("TIMEOUT", t, c);
                    timeouts.fetch_add(1);
                    return;
                }
                XStore_Proto::BatchInsert_REP rep;
                if (rep.ParseFromArray(reply.data(), static_cast<int>(reply.size())) &&
                    rep.insertstatus().find("successfully") != std::string::npos) {
                    record("ACK", t, c);
                    acks.fetch_add(1);
                } else {
                    record("FAIL", t, c);
                    fails.fetch_add(1);
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
            }
        });
    }
    for (std::thread &w : workers) w.join();
    std::fclose(log);
    std::printf("RUN acks=%llu fails=%llu timeouts=%llu\n", acks.load(), fails.load(), timeouts.load());
    return 0;
}

// 1 = present with the expected value, 0 = missing, -1 = present with a wrong value, -2 = no reply.
int CheckRow(zmq::context_t &ctx, const std::string &addr, const std::string &db, unsigned long long ts) {
    zmq::socket_t sock(ctx, zmq::socket_type::req);
    sock.set(zmq::sockopt::linger, 0);
    sock.set(zmq::sockopt::rcvtimeo, 30000);
    sock.connect("tcp://" + addr);
    XStore_Proto::Request request;
    XStore_Proto::UnaryQuery_REQ *query = request.mutable_unaryquery();
    query->set_rtype(XStore_Proto::UNARYQUERY);
    query->set_timestamp(ts);
    query->set_dbname(db);
    const std::string payload = request.SerializeAsString();
    zmq::message_t reply;
    if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) return -2;
    XStore_Proto::UnaryQuery_REP rep;
    if (!rep.ParseFromArray(reply.data(), static_cast<int>(reply.size()))) return -2;
    if (rep.data().fielddata_size() != kColumns) return 0;
    for (int c = 0; c < kColumns; ++c) {
        if (rep.data().fielddata(c).doubledata() != static_cast<double>(ts) * 0.5 + c) return -1;
    }
    return 1;
}

int Verify(const std::string &addr, const std::string &db, const std::string &log_path, long long fault_start_ms) {
    std::ifstream in(log_path);
    if (!in) {
        std::cerr << "cannot open " << log_path << "\n";
        return 2;
    }
    struct Call { int t, c; };
    std::vector<Call> acked;
    unsigned long long after_fault = 0;
    std::vector<size_t> rows = {kRowsPerCall};
    std::string rows_arg = std::to_string(kRowsPerCall);
    bool sync = true;
    std::string line;
    if (std::getline(in, line) && line.rfind("# rows=", 0) == 0) {
        rows_arg = line.substr(7, line.find(' ', 7) - 7);
        rows = ParseRows(rows_arg);
        if (rows.empty()) {
            std::cerr << "bad header in " << log_path << "\n";
            return 2;
        }
        sync = line.find("mode=ASYNC") == std::string::npos;
    } else {
        in.clear();
        in.seekg(0);
    }
    std::string kind;
    int t = 0, c = 0;
    long long ms = 0;
    while (in >> kind >> t >> c >> ms) {
        if (kind != "ACK") continue;
        acked.push_back({t, c});
        if (fault_start_ms > 0 && ms >= fault_start_ms) ++after_fault;
    }
    // A very long log (fast ASYNC acks) would take hours of point queries: check every k-th call once past the cap.
    constexpr size_t kMaxChecked = 30000;
    const size_t total_acked = acked.size();
    if (total_acked > kMaxChecked) {
        const size_t stride = (total_acked + kMaxChecked - 1) / kMaxChecked;
        size_t kept = 0;
        for (size_t i = 0; i < total_acked; i += stride) acked[kept++] = acked[i];
        acked.resize(kept);
    }
    zmq::context_t ctx(2);
    std::atomic<size_t> next{0};
    std::atomic<unsigned long long> missing{0}, wrong{0}, unanswered{0};
    std::vector<std::thread> checkers;
    for (int w = 0; w < 8; ++w) {
        checkers.emplace_back([&] {
            for (size_t i = next.fetch_add(1); i < acked.size(); i = next.fetch_add(1)) {
                const unsigned long long first =
                    FirstTs(static_cast<unsigned long long>(acked[i].t), static_cast<unsigned long long>(acked[i].c));
                const size_t n = rows[static_cast<size_t>(acked[i].c) % rows.size()];
                std::mt19937_64 rng(first);
                std::vector<unsigned long long> samples = {first, first + n - 1, first + n / 2};
                for (int k = 0; k < 5; ++k) samples.push_back(first + rng() % n);
                bool is_missing = false, is_wrong = false, no_reply = false;
                for (const unsigned long long ts : samples) {
                    const int r = CheckRow(ctx, addr, db, ts);
                    if (r == 0) is_missing = true;
                    else if (r == -1) is_wrong = true;
                    else if (r == -2) no_reply = true;
                }
                if (is_missing) missing.fetch_add(1);
                if (is_wrong) wrong.fetch_add(1);
                if (no_reply) unanswered.fetch_add(1);
            }
        });
    }
    for (std::thread &w : checkers) w.join();
    std::printf("VERIFY mode=%s rows=%s acked=%zu checked=%zu missing=%llu wrong=%llu unanswered=%llu acks_after_fault=%llu\n",
                sync ? "SYNC" : "ASYNC", rows_arg.c_str(), total_acked, acked.size(), missing.load(), wrong.load(), unanswered.load(), after_fault);
    return (!sync || missing.load() == 0) && wrong.load() == 0 && unanswered.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char **argv) {
    if ((argc >= 7 && argc <= 10) && std::string(argv[1]) == "run") {
        const std::string rows_arg = argc >= 8 ? argv[7] : std::to_string(kRowsPerCall);
        const std::vector<size_t> rows = ParseRows(rows_arg);
        const std::string mode = argc == 9 ? argv[8] : "SYNC";
        if (rows.empty() || (mode != "SYNC" && mode != "ASYNC")) {
            std::cerr << "rows_per_call must be a comma list of 1..1000 and mode SYNC or ASYNC\n";
            return 2;
        }
        return Run(argv[2], argv[3], std::atoi(argv[4]), std::atoi(argv[5]), argv[6], rows_arg, rows,
                   mode == "SYNC" ? XStore_Proto::SYNC : XStore_Proto::ASYNC, argc == 10 ? std::atoi(argv[9]) : 0);
    }
    if ((argc == 5 || argc == 6) && std::string(argv[1]) == "verify") {
        return Verify(argv[2], argv[3], argv[4], argc == 6 ? std::atoll(argv[5]) : 0);
    }
    std::cerr << "usage: " << argv[0] << " run <host:port> <db> <threads> <seconds> <log_path> [rows_per_call[,rows...] [SYNC|ASYNC [first_thread]]] | verify <host:port> <db> <log_path> [fault_start_epoch_ms]\n";
    return 2;
}
