// Timed FindByValue probe for a real XStore server on another machine (the bitmap-section study).
//
//   xstore_zmq_find_client create <host:port> <db> <row_length> <column_count>
//   xstore_zmq_find_client find   <host:port> <db> <start_epoch> <end_epoch> <column> <double_value> <repeat>
//   xstore_zmq_find_client put    <host:port> <db> <epoch> <columns> <double_value> <repeat>
//
// `find` sends <repeat> FindByValue requests for `column == value` over [start, end] one after the other and prints one
// line per call: wall milliseconds, rows returned, reply bytes and the first 100 characters of the queryStatus.
// `put` sends <repeat> SYNC UnaryInserts of one row of <columns> doubles (all = value) at epoch, epoch+1, ... and prints
// the wall milliseconds of each (an insert into a year whose bitmap section exists invalidates it).
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <iterator>
#include <string>
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
    std::cerr << "usage: " << argv[0] << " create <host:port> <db> <row_length> <column_count>\n"
              << "       " << argv[0] << " find <host:port> <db> <start_epoch> <end_epoch> <column> <double_value> <repeat>\n"
              << "       " << argv[0] << " put <host:port> <db> <epoch> <columns> <double_value> <repeat>\n";
    return 2;
}
