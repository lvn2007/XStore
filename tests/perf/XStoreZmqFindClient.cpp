// Timed FindByValue probe for a real XStore server on another machine (the bitmap-section study).
//
//   xstore_zmq_find_client create <host:port> <db> <row_length> <column_count>
//   xstore_zmq_find_client find   <host:port> <db> <start_epoch> <end_epoch> <column> <double_value> <repeat>
//   xstore_zmq_find_client put    <host:port> <db> <epoch> <columns> <double_value> <repeat>
//
// `find` sends <repeat> FindByValue requests for `column == value` over [start, end] one after the other and prints one
// line per call: wall milliseconds, rows returned, reply bytes and the first 100 characters of the queryStatus.
//   xstore_zmq_find_client bload  <host:port> <db> <year_start_epoch> <year_seconds> <threads> <seconds> <rows> <sync|async>
//   xstore_zmq_find_client scan   <host:port> <db> <start_epoch> <end_epoch> <column> <seconds>
// `bload` runs <threads> threads of BatchInserts of <rows> rows (128 doubles each) at random seconds of the year for
// <seconds>, and prints one line per wall second: second, acks, worst latency in ms. `scan` repeats a MAX over the range
// (a column above the index width is read from the data) back to back for <seconds> and prints each call's milliseconds.
// `put` sends <repeat> SYNC UnaryInserts of one row of <columns> doubles (all = value) at epoch, epoch+1, ... and prints
// the wall milliseconds of each (an insert into a year whose bitmap section exists invalidates it).
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include "XStore.pb.h"

namespace {

bool Exchange(const std::string &addr, const XStore_Proto::Request &request, std::string &reply, int timeout_ms) {
    zmq::context_t ctx(1);
    zmq::socket_t sock(ctx, zmq::socket_type::req);
    sock.set(zmq::sockopt::linger, 0);
    sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
    sock.set(zmq::sockopt::sndtimeo, timeout_ms);
    sock.connect("tcp://" + addr);
    const std::string payload = request.SerializeAsString();
    if (!sock.send(zmq::buffer(payload), zmq::send_flags::none)) return false;
    std::vector<zmq::message_t> frames;
    if (!zmq::recv_multipart(sock, std::back_inserter(frames)) || frames.empty()) return false;
    reply.clear();
    for (const zmq::message_t &frame : frames) reply.append(static_cast<const char *>(frame.data()), frame.size());
    return true;
}

int Create(const std::string &addr, const std::string &db, uint32_t row_length, uint32_t columns) {
    XStore_Proto::Request request;
    XStore_Proto::CreateDB_REQ *create = request.mutable_createdb();
    create->set_rtype(XStore_Proto::CREATEDB);
    create->set_dbname(db);
    create->set_timeresolution(XStore_Proto::SECOND);
    create->set_rowlength(row_length);
    create->set_columncount(columns);
    std::string reply;
    XStore_Proto::CreateDB_REP rep;
    if (!Exchange(addr, request, reply, 60000) || !rep.ParseFromString(reply)) {
        std::cerr << "create: no reply\n";
        return 1;
    }
    std::cout << rep.createdbstatus() << std::endl;
    return rep.createdbstatus().rfind("[INFO]", 0) == 0 ? 0 : 1;
}

int Find(const std::string &addr, const std::string &db, unsigned long long start, unsigned long long end, int column,
         double value, int repeat) {
    for (int i = 0; i < repeat; ++i) {
        XStore_Proto::Request request;
        XStore_Proto::FindByValue_REQ *find = request.mutable_findbyvalue();
        find->set_rtype(XStore_Proto::FINDBYVALUE);
        find->set_starttime(start);
        find->set_endtime(end);
        find->set_dbname(db);
        find->set_columnidx(column);
        find->mutable_value()->set_doubledata(value);
        std::string reply;
        const auto t0 = std::chrono::steady_clock::now();
        const bool ok = Exchange(addr, request, reply, 1800000);
        const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        XStore_Proto::FindByValue_REP rep;
        if (!ok || !rep.ParseFromString(reply)) {
            std::cout << "call " << i << ": FAILED after " << ms << " ms\n";
            return 1;
        }
        std::string status = rep.has_querystatus() ? rep.querystatus() : "";
        if (status.size() > 100) status.resize(100);
        std::cout << "call " << i << ": " << ms << " ms rows=" << rep.queryresponse_size() << " bytes=" << reply.size()
                  << " status=[" << status << "]" << std::endl;
    }
    return 0;
}

int Put(const std::string &addr, const std::string &db, unsigned long long epoch, int columns, double value, int repeat) {
    for (int i = 0; i < repeat; ++i) {
        XStore_Proto::Request request;
        XStore_Proto::UnaryInsert_REQ *insert = request.mutable_unaryinsert();
        insert->set_rtype(XStore_Proto::UNARYINSERT);
        insert->set_timestamp(epoch + static_cast<unsigned long long>(i));
        insert->set_dbname(db);
        insert->set_durabilitymode(XStore_Proto::SYNC);
        for (int c = 0; c < columns; ++c) insert->mutable_data()->add_fielddata()->set_doubledata(value);
        std::string reply;
        const auto t0 = std::chrono::steady_clock::now();
        const bool ok = Exchange(addr, request, reply, 600000);
        const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        XStore_Proto::UnaryInsert_REP rep;
        if (!ok || !rep.ParseFromString(reply)) {
            std::cout << "put " << i << ": FAILED after " << ms << " ms\n";
            return 1;
        }
        std::string status = rep.insertstatus();
        if (status.size() > 100) status.resize(100);
        std::cout << "put " << i << ": " << ms << " ms status=[" << status << "]" << std::endl;
    }
    return 0;
}

int Bload(const std::string &addr, const std::string &db, unsigned long long year_start, unsigned long long year_seconds,
          int threads, int seconds, int rows, bool sync) {
    struct Sample { int second; double ms; };
    std::vector<std::vector<Sample>> samples(static_cast<size_t>(threads));
    std::atomic<unsigned long long> failures{0};
    const auto t0 = std::chrono::steady_clock::now();
    const auto deadline = t0 + std::chrono::seconds(seconds);
    std::vector<std::thread> pool;
    for (int t = 0; t < threads; ++t) {
        pool.emplace_back([&, t] {
            zmq::context_t ctx(1);
            zmq::socket_t sock(ctx, zmq::socket_type::req);
            sock.set(zmq::sockopt::linger, 0);
            sock.set(zmq::sockopt::rcvtimeo, 600000);
            sock.set(zmq::sockopt::sndtimeo, 600000);
            sock.connect("tcp://" + addr);
            std::mt19937_64 rng(1234567ULL + static_cast<unsigned long long>(t));
            while (std::chrono::steady_clock::now() < deadline) {
                XStore_Proto::Request request;
                XStore_Proto::BatchInsert_REQ *batch = request.mutable_batchinsert();
                batch->set_rtype(XStore_Proto::BATCHINSERT);
                batch->set_dbname(db);
                batch->set_durabilitymode(sync ? XStore_Proto::SYNC : XStore_Proto::ASYNC);
                std::unordered_set<unsigned long long> used;
                while (static_cast<int>(used.size()) < rows) used.insert(year_start + rng() % year_seconds);
                for (unsigned long long ts : used) {
                    XStore_Proto::BatchInsert_DATA *entry = batch->add_batchinsert_data();
                    entry->set_timestamp(ts);
                    for (int c = 0; c < 128; ++c) entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(ts % 1000 + c));
                }
                const std::string payload = request.SerializeAsString();
                const auto start = std::chrono::steady_clock::now();
                zmq::message_t reply;
                if (!sock.send(zmq::buffer(payload), zmq::send_flags::none) || !sock.recv(reply, zmq::recv_flags::none)) { ++failures; return; }
                const auto end = std::chrono::steady_clock::now();
                samples[static_cast<size_t>(t)].push_back({static_cast<int>(std::chrono::duration_cast<std::chrono::seconds>(end - t0).count()),
                                                           std::chrono::duration<double, std::milli>(end - start).count()});
            }
        });
    }
    for (std::thread &th : pool) th.join();
    std::vector<unsigned long long> acks(static_cast<size_t>(seconds) + 2, 0);
    std::vector<double> worst(static_cast<size_t>(seconds) + 2, 0.0);
    unsigned long long total = 0;
    for (const auto &per_thread : samples) {
        for (const Sample &sample : per_thread) {
            const size_t second = std::min(static_cast<size_t>(sample.second), acks.size() - 1);
            ++acks[second];
            worst[second] = std::max(worst[second], sample.ms);
            ++total;
        }
    }
    for (size_t i = 0; i < acks.size(); ++i) std::cout << "sec " << i << " acks=" << acks[i] << " worst_ms=" << worst[i] << "\n";
    std::cout << "TOTAL acks=" << total << " rows=" << total * static_cast<unsigned long long>(rows) << " failures=" << failures.load() << std::endl;
    return failures.load() == 0 ? 0 : 1;
}

int ScanLoop(const std::string &addr, const std::string &db, unsigned long long start, unsigned long long end, int column, int seconds) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    int calls = 0;
    do {
        XStore_Proto::Request request;
        XStore_Proto::Max_REQ *max = request.mutable_max();
        max->set_rtype(XStore_Proto::MAX);
        max->set_starttime(start);
        max->set_endtime(end);
        max->set_dbname(db);
        max->set_columnidx(column);
        std::string reply;
        const auto t0 = std::chrono::steady_clock::now();
        const bool ok = Exchange(addr, request, reply, 1800000);
        const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
        std::cout << "scan " << calls++ << ": " << ms << " ms " << (ok ? "ok" : "FAILED") << std::endl;
        if (!ok) return 1;
    } while (std::chrono::steady_clock::now() < deadline);
    return 0;
}

} // namespace

int main(int argc, char **argv) {
    if (argc == 6 && std::string(argv[1]) == "create") {
        return Create(argv[2], argv[3], static_cast<uint32_t>(std::atoi(argv[4])), static_cast<uint32_t>(std::atoi(argv[5])));
    }
    if (argc == 9 && std::string(argv[1]) == "find") {
        return Find(argv[2], argv[3], std::strtoull(argv[4], nullptr, 10), std::strtoull(argv[5], nullptr, 10),
                    std::atoi(argv[6]), std::atof(argv[7]), std::atoi(argv[8]));
    }
    if (argc == 8 && std::string(argv[1]) == "put") {
        return Put(argv[2], argv[3], std::strtoull(argv[4], nullptr, 10), std::atoi(argv[5]), std::atof(argv[6]), std::atoi(argv[7]));
    }
    if (argc == 10 && std::string(argv[1]) == "bload") {
        return Bload(argv[2], argv[3], std::strtoull(argv[4], nullptr, 10), std::strtoull(argv[5], nullptr, 10), std::atoi(argv[6]),
                     std::atoi(argv[7]), std::atoi(argv[8]), std::string(argv[9]) == "sync");
    }
    if (argc == 8 && std::string(argv[1]) == "scan") {
        return ScanLoop(argv[2], argv[3], std::strtoull(argv[4], nullptr, 10), std::strtoull(argv[5], nullptr, 10), std::atoi(argv[6]),
                        std::atoi(argv[7]));
    }
    std::cerr << "usage: " << argv[0] << " create <host:port> <db> <row_length> <column_count>\n"
              << "       " << argv[0] << " find <host:port> <db> <start_epoch> <end_epoch> <column> <double_value> <repeat>\n"
              << "       " << argv[0] << " put <host:port> <db> <epoch> <columns> <double_value> <repeat>\n";
    return 2;
}
