// Load generator for the live-widen study (queries next to a widening year), run from a client node against a real server.
//
//   xstore_zmq_widen_client grow  <host:port> <db> <timestamp> <width>
//   xstore_zmq_widen_client query <host:port> <db> <threads> <duration_s> <start_epoch_s> <pause_ms> <log> old|new <new_col>
//
// `grow` inserts one SYNC row of <width> columns (128 bools, then doubles) at <timestamp>: the insert that makes the
// server widen its index. `query` waits for the start epoch, then each thread loops its kinds until <duration_s> with
// <pause_ms> between calls and writes one log line per call: `<start_epoch_ns> <latency_us> <kind> <ok>`.
// `old` kinds (whole-year MAX of column 5 in 2009 and in 2005, a one-day SUM in 2009, a point read in 2009) are columns
// that existed before the grow; `new` is the whole-year MAX of 2009 on <new_col>, a column that exists only after it.
// A thread also stops when the file <log>.stop exists (the driver ends a run whose length it cannot know).
// ok column: 0 = unparsable or ERROR status, 1 = ok, 2 = ok with a WARNING in the status (the widen's rebuild notice).
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <zmq.hpp>

#include "XStore.pb.h"

namespace {

using WallClock = std::chrono::system_clock;
using SteadyClock = std::chrono::steady_clock;

constexpr unsigned long long kY2005 = 1104537600ULL, kY2006 = 1136073600ULL;
constexpr unsigned long long kY2009 = 1230768000ULL, kY2010 = 1262304000ULL;

long long EpochNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(WallClock::now().time_since_epoch()).count();
}

// 0 = error, 1 = ok, 2 = ok with a WARNING (the rebuild notice a query gets while its year is being widened).
int StatusCode(const std::string &status) {
    if (status.find("ERROR") != std::string::npos) return 0;
    return status.find("WARNING") != std::string::npos ? 2 : 1;
}

int Grow(const std::string &addr, const std::string &db, unsigned long long timestamp, int width) {
    zmq::context_t ctx(1);
    XStore_Proto::Request request;
    XStore_Proto::UnaryInsert_REQ *insert = request.mutable_unaryinsert();
    insert->set_rtype(XStore_Proto::UNARYINSERT);
    insert->set_timestamp(timestamp);
    insert->set_dbname(db);
    insert->set_durabilitymode(XStore_Proto::SYNC);
    for (int i = 0; i < width; ++i) {
        XStore_Proto::FData *field = insert->mutable_data()->add_fielddata();
        if (i < 128) field->set_booldata(true);
        else field->set_doubledata(static_cast<double>(i - 128));
    }
    zmq::socket_t sock(ctx, zmq::socket_type::req);
    sock.set(zmq::sockopt::linger, 0);
    sock.set(zmq::sockopt::rcvtimeo, 600000);
    sock.connect("tcp://" + addr);
    const std::string payload = request.SerializeAsString();
    zmq::message_t reply;
    if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) return 1;
    XStore_Proto::UnaryInsert_REP rep;
    if (!rep.ParseFromArray(reply.data(), static_cast<int>(reply.size()))) return 1;
    std::cout << rep.insertstatus() << std::endl;
    return StatusCode(rep.insertstatus()) != 0 ? 0 : 1;
}

struct Kind {
    const char *name;
    XStore_Proto::Request request;
    bool point;   // UnaryQuery_REP, otherwise Max_REP / Sum_REP
    bool sum;
};

Kind Aggregate(const char *name, bool sum, unsigned long long from, unsigned long long to, const std::string &db, int column) {
    Kind kind{name, {}, false, sum};
    if (sum) {
        XStore_Proto::Sum_REQ *q = kind.request.mutable_sum();
        q->set_rtype(XStore_Proto::SUM);
        q->set_starttime(from);
        q->set_endtime(to);
        q->set_dbname(db);
        q->set_columnidx(column);
    } else {
        XStore_Proto::Max_REQ *q = kind.request.mutable_max();
        q->set_rtype(XStore_Proto::MAX);
        q->set_starttime(from);
        q->set_endtime(to);
        q->set_dbname(db);
        q->set_columnidx(column);
    }
    return kind;
}

int ReplyCode(const Kind &kind, const zmq::message_t &reply) {
    const int size = static_cast<int>(reply.size());
    if (kind.point) {
        XStore_Proto::UnaryQuery_REP rep;
        return rep.ParseFromArray(reply.data(), size) ? StatusCode(rep.has_querystatus() ? rep.querystatus() : "") : 0;
    }
    if (kind.sum) {
        XStore_Proto::Sum_REP rep;
        return rep.ParseFromArray(reply.data(), size) ? StatusCode(rep.opstatus()) : 0;
    }
    XStore_Proto::Max_REP rep;
    return rep.ParseFromArray(reply.data(), size) ? StatusCode(rep.opstatus()) : 0;
}

int Query(const std::string &addr, const std::string &db, int threads, double duration_s, double start_epoch_s,
          int pause_ms, const std::string &log_path, const std::string &set, int new_col) {
    std::vector<Kind> kinds;
    if (set == "new") {
        kinds.push_back(Aggregate("C_max_2009_newcol", false, kY2009, kY2010 - 1, db, new_col));
    } else {
        kinds.push_back(Aggregate("A_max_2009_oldcol", false, kY2009, kY2010 - 1, db, 5));
        kinds.push_back(Aggregate("B_sum_2009_day100", true, kY2009 + 86400 * 100, kY2009 + 86400 * 101 - 1, db, 5));
        kinds.push_back(Aggregate("D_max_2005_control", false, kY2005, kY2006 - 1, db, 5));
        Kind point{"E_point_2009", {}, true, false};
        XStore_Proto::UnaryQuery_REQ *q = point.request.mutable_unaryquery();
        q->set_rtype(XStore_Proto::UNARYQUERY);
        q->set_timestamp(kY2009 + 12345);
        q->set_dbname(db);
        kinds.push_back(point);
    }
    const std::string stop_path = log_path + ".stop";
    zmq::context_t ctx(1);
    const long long start_ns = static_cast<long long>(start_epoch_s * 1e9);
    const long long deadline_ns = static_cast<long long>((start_epoch_s + duration_s) * 1e9);
    std::vector<std::vector<std::string>> lines(static_cast<size_t>(threads));
    std::atomic<unsigned long long> failed{0};
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            zmq::socket_t sock(ctx, zmq::socket_type::req);
            sock.set(zmq::sockopt::linger, 0);
            sock.set(zmq::sockopt::rcvtimeo, 120000);
            sock.connect("tcp://" + addr);
            while (EpochNs() < start_ns) std::this_thread::sleep_for(std::chrono::microseconds(200));
            size_t i = static_cast<size_t>(t);
            while (EpochNs() < deadline_ns && !std::filesystem::exists(stop_path)) {
                const Kind &kind = kinds[i++ % kinds.size()];
                const std::string payload = kind.request.SerializeAsString();
                const long long wall0 = EpochNs();
                const SteadyClock::time_point t0 = SteadyClock::now();
                zmq::message_t reply;
                if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) {
                    failed.fetch_add(1);
                    break;
                }
                const double us = std::chrono::duration<double, std::micro>(SteadyClock::now() - t0).count();
                char line[160];
                std::snprintf(line, sizeof line, "%lld %.1f %s %d\n", wall0, us, kind.name, ReplyCode(kind, reply));
                lines[static_cast<size_t>(t)].emplace_back(line);
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
            }
        });
    }
    for (std::thread &w : workers) w.join();
    std::ofstream log(log_path);
    size_t total = 0;
    for (const auto &per_thread : lines) {
        for (const std::string &line : per_thread) log << line;
        total += per_thread.size();
    }
    std::printf("RESULT query set=%s calls=%zu failed=%llu\n", set.c_str(), total, failed.load());
    std::fflush(stdout);
    return failed.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char **argv) {
    const std::string cmd = argc > 1 ? argv[1] : "";
    if (cmd == "grow" && argc == 6) return Grow(argv[2], argv[3], std::strtoull(argv[4], nullptr, 10), std::atoi(argv[5]));
    if (cmd == "query" && argc == 11)
        return Query(argv[2], argv[3], std::atoi(argv[4]), std::atof(argv[5]), std::atof(argv[6]), std::atoi(argv[7]),
                     argv[8], argv[9], std::atoi(argv[10]));
    std::cerr << "usage: " << argv[0] << " grow <host:port> <db> <timestamp> <width> | query <host:port> <db> <threads> "
              << "<duration_s> <start_epoch_s> <pause_ms> <log> old|new <new_col>\n";
    return 2;
}
