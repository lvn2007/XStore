#include "include/Networking/ZMQ_Server.h"
#include <google/protobuf/arena.h>
#include <chrono>
#include <fstream>
#include <set>
#include <string>
#include <utility>

namespace {

// Physical core count, not std::thread::hardware_concurrency()'s logical
// (SMT-inclusive) count. Measured on real hardware (torusnode15: 16
// physical cores, 2 SMT threads/core = 32 logical): running one spinning
// ZMQ worker per LOGICAL core (32 workers) under real concurrent load
// (t=64) was ~6% SLOWER at the p50 than one per PHYSICAL core (16
// workers) -- going past physical-core count leaves no free SMT sibling
// lane for the WAL writer threads (also spinning, WAL_ADAPTIVE_POLL_
// WINDOW_US) to run relatively uncontended on, forcing genuine execution-
// resource contention between front-end and back-end threads that doesn't
// exist when worker count matches physical cores. Parses /proc/cpuinfo
// (Linux-only -- consistent with this project's existing POSIX/Linux-only
// assumptions elsewhere: pwritev, posix_fallocate) by counting distinct
// (physical id, core id) pairs, since neither hardware_concurrency() nor
// std::thread::hardware_concurrency() report physical topology, only
// logical CPU count.
int PhysicalCoreCount() {
    std::ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo.is_open()) return 0;

    std::set<std::pair<int, int>> cores; // (physical_id, core_id)
    int physical_id = -1;
    std::string line;
    while (std::getline(cpuinfo, line)) {
        auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = line.substr(0, colon);
        std::string value = line.substr(colon + 1);
        // Trim trailing whitespace off the key (fields are "name\t: value").
        while (!key.empty() && (key.back() == ' ' || key.back() == '\t')) key.pop_back();

        if (key == "physical id") {
            try { physical_id = std::stoi(value); } catch (...) { physical_id = -1; }
        } else if (key == "core id" && physical_id >= 0) {
            try {
                cores.insert({physical_id, std::stoi(value)});
            } catch (...) {
                // Malformed line -- skip rather than crash boot over a
                // parsing edge case; falls through to the hardware_
                // concurrency fallback below if this leaves the set empty.
            }
        }
    }
    return static_cast<int>(cores.size());
}

}  // namespace

int ZmqServer::DefaultThreadCount() {
    int physical = PhysicalCoreCount();
    if (physical > 0) return physical;

    // /proc/cpuinfo unavailable or unparseable (non-Linux, containerized/
    // restricted environment, etc.) -- assume SMT2 as a reasonable guess
    // rather than defaulting to the (measurably worse, per the comment
    // above) logical count outright.
    unsigned logical = std::thread::hardware_concurrency();
    if (logical >= 2) return static_cast<int>(logical / 2);
    return logical > 0 ? static_cast<int>(logical) : 1;
}

namespace {
// Shared by every case below: serialize a response proto and dispatch it to
// the CLIENT. REP sockets enforce recv-then-send ordering already, so
// dontwait never actually blocks here -- kept only to match a fire-and-forget
// send with no result check, same as before.
//
// SerializeWithCachedSizesToArray(), not SerializeToArray(): per
// message_lite.h, SerializeToArray() is "just a simple wrapper around
// ByteSize() and SerializeWithCachedSizes()" -- it recomputes the size
// itself before writing, redundant with the ByteSizeLong() call directly
// above it (which already walked the tree once and cached every
// submessage's size). SerializeWithCachedSizesToArray() reuses those cached
// sizes instead of recomputing them, cutting SendProto from 2 size-compute
// passes + 1 write pass down to 1 size-compute pass + 1 write pass -- safe
// as long as msg isn't mutated between the two calls, which it isn't here.
template <typename T>
void SendProto(zmq::socket_t &socket, const T &msg) {
    size_t size = msg.ByteSizeLong();
    zmq::message_t reply(size);
    msg.SerializeWithCachedSizesToArray(static_cast<uint8_t*>(reply.data()));
    socket.send(reply, zmq::send_flags::dontwait);
}
}  // namespace

ZmqServer::ZmqServer(int num_threads) {
    this->num_threads = num_threads;
}

void ZmqServer::StartServer(std::string listen_addr, std::string listen_port) {
    //  Socket to talk to CLIENTS
    zmq::socket_t client_socket(ctx, zmq::socket_type::router);
    std::string server_addr = "tcp://" + listen_addr + ":" + listen_port;

    // No queue depth cap -- 0 means "no limit" (bounded only by available
    // memory) rather than the library default of 1000 messages/socket.
    // Chosen deliberately: the goal here is maximum accepted throughput, not
    // bounded-memory backpressure: under a burst that outpaces the worker
    // pool, this queues in RAM instead of blocking/dropping at the socket.
    client_socket.set(zmq::sockopt::sndhwm, 0);
    client_socket.set(zmq::sockopt::rcvhwm, 0);

    // Set listenAddr & port internally
    this->server_addr = server_addr;

    client_socket.bind(server_addr);
    spdlog::info("[NETWORKING] - Listening on: {}", server_addr);

    //  Socket to talk to WORKERS
    zmq::socket_t worker_socket(ctx, zmq::socket_type::dealer);
    worker_socket.set(zmq::sockopt::sndhwm, 0);
    worker_socket.set(zmq::sockopt::rcvhwm, 0);
    worker_socket.bind("inproc://workers");

    //  Launch pool of WORKER threads
    for (int i = 0; i < num_threads; i++) {
        std::thread worker(&ZmqServer::WorkerRoutine, this, server_addr.substr(6));
        worker.detach();
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", num_threads);

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void*>(client_socket), static_cast<void*>(worker_socket), nullptr);
}

void ZmqServer::WorkerRoutine(std::string server_addr) {
    // Instantiate XStore for each thread
    XStore my_store;

    // Each thread shares the server's ZMQ context. No queue-depth cap, same
    // reasoning as ClientSocket/WorkerSocket in StartServer.
    zmq::socket_t socket(ctx, zmq::socket_type::rep);
    socket.set(zmq::sockopt::sndhwm, 0);
    socket.set(zmq::sockopt::rcvhwm, 0);
    socket.connect("inproc://workers");

    // One arena per worker thread, reused for every request via Reset()
    // rather than constructed fresh each time. Reset() keeps the arena's
    // already-allocated blocks around for the next request instead of
    // returning them to the system allocator, so steady-state traffic
    // doesn't pay for the underlying block list's malloc/free churn either
    // -- only the (much cheaper) bump-pointer allocations within it.
    // Covers BOTH the incoming Request (RangeInsert_REQ/BatchInsert_REQ
    // carry the same repeated-RowData-with-FData-columns nesting on the
    // parse side that RangeQuery/BatchQuery/UnaryQuery pay on the response
    // side) and the three response types with nested repeated fields.
    google::protobuf::Arena arena;

    // Reused across iterations (this function IS the per-thread loop, so a
    // plain local -- no thread_local needed) instead of a fresh
    // zmq::message_t constructed every request. A single message_t, not a
    // vector: this REP socket only ever sees one body frame per request --
    // the client (XStore_Client.h) always does a single-frame Sock.send(),
    // never send_multipart -- so the more()-check loop and back_inserter
    // machinery recv_multipart pays for a possible second/third frame was
    // pure overhead here, never exercised in practice.
    zmq::message_t client_msg;

    // Same adaptive-poll idea as the WAL writer's own queue wait (see
    // ZMQ_ADAPTIVE_POLL_WINDOW_US, XStore_Tunables.h): epoch (not "now") so
    // the first iteration always takes the normal blocking path.
    auto last_activity = std::chrono::steady_clock::time_point{};

    while (true) {
        // RECEIVING
        //  Wait for next request from CLIENT
        if constexpr (XStoreTunables::ZMQ_ADAPTIVE_POLL_WINDOW_US > 0) {
            auto window = std::chrono::microseconds(XStoreTunables::ZMQ_ADAPTIVE_POLL_WINDOW_US);
            if (std::chrono::steady_clock::now() - last_activity < window) {
                // Recently active: tight non-blocking spin, bypassing the
                // socket's own blocking recv entirely, so a request
                // arriving during this window is picked up without paying
                // an OS park/wake. Self-terminates the moment the window
                // lapses with nothing arriving -- falls through to the
                // blocking path on the next outer-loop iteration.
                bool got = false;
                while (!got) {
                    got = socket.recv(client_msg, zmq::recv_flags::dontwait).has_value();
                    if (!got && std::chrono::steady_clock::now() - last_activity >= window) break;
                }
                if (!got) continue;
            } else {
                socket.recv(client_msg);
            }
        } else {
            socket.recv(client_msg);
        }
        last_activity = std::chrono::steady_clock::now();

        // Free everything the previous iteration allocated in one shot --
        // safe here because that request's response was already serialized
        // and sent by SendProto before the loop came back around.
        arena.Reset();

        //  Parse the envelope once -- the oneof case (payload_case()) below
        //  is the dispatch tag, so there's no separate lightweight parse
        //  followed by a second full parse of the same bytes.
        auto *client_request = google::protobuf::Arena::Create<XStore_Proto::Request>(&arena);
        if (!client_request->ParseFromArray(client_msg.data(), client_msg.size())) {
            spdlog::warn("[NETWORKING] - Failed to parse request, dropping");
            socket.send(zmq::message_t(0), zmq::send_flags::dontwait);
            continue;
        }

        switch (client_request->payload_case()) {
            // INFO
            case XStore_Proto::Request::kInfo: {
                XStore_Proto::SysInfo_REP rep = my_store.InfoQuery(server_addr, num_threads);
                SendProto(socket, rep);
                break;
            }

            // PING -- warmup/liveness probe, no XStore work, just the round trip
            case XStore_Proto::Request::kPing: {
                XStore_Proto::Ping_REP rep;
                SendProto(socket, rep);
                break;
            }

            // CREATE DB
            case XStore_Proto::Request::kCreatedb: {
                const auto &req = client_request->createdb();
                XStore_Proto::CreateDB_REP rep = my_store.CreateDB(req.dbname(), req.timeresolution(), req.rowlength());
                SendProto(socket, rep);
                break;
            }

            // REMOVE DB
            case XStore_Proto::Request::kRemovedb: {
                const auto &req = client_request->removedb();
                XStore_Proto::RemoveDB_REP rep = my_store.RemoveDB(req.dbname());
                SendProto(socket, rep);
                break;
            }

            // REMOVE DB FILES
            case XStore_Proto::Request::kRemovedbfiles: {
                auto *req = client_request->mutable_removedbfiles();
                XStore_Proto::RemoveDBFiles_REP rep = my_store.RemoveDBFiles(req, req->dbname());
                SendProto(socket, rep);
                break;
            }

            // UNARY INSERT
            case XStore_Proto::Request::kUnaryinsert: {
                const auto &req = client_request->unaryinsert();
                XStore_Proto::UnaryInsert_REP rep;
                my_store.UnaryInsert(req.timestamp(), req.data(), req.dbname(), &rep, req.durabilitymode());
                SendProto(socket, rep);
                break;
            }

            // UNARY QUERY
            case XStore_Proto::Request::kUnaryquery: {
                const auto &req = client_request->unaryquery();
                // Arena-allocated: same RowData -> repeated FData nesting as
                // Range/BatchQuery (up to one alloc per column), and this is
                // likely the highest-QPS RPC of all of them -- single-row
                // point queries. Not scaled by span like RangeQuery, but
                // paid on every call with no reuse across calls.
                auto *rep = google::protobuf::Arena::Create<XStore_Proto::UnaryQuery_REP>(&arena);
                my_store.UnaryQuery(req.timestamp(), req.dbname(), rep);
                SendProto(socket, *rep);
                break;
            }

            // UNARY DELETE
            case XStore_Proto::Request::kUnarydelete: {
                const auto &req = client_request->unarydelete();
                XStore_Proto::UnaryDelete_REP rep = my_store.UnaryDelete(req.timestamp(), req.dbname());
                SendProto(socket, rep);
                break;
            }

            // BATCH INSERT
            case XStore_Proto::Request::kBatchinsert: {
                // Req itself is already arena-allocated (it's a field inside
                // ClientRequest, which lives on Arena) -- BatchInsert_REQ's
                // repeated BatchInsert_DATA, each with its own RowData ->
                // repeated FData, was parsed straight into arena memory
                // above with no extra work needed here.
                auto *req = client_request->mutable_batchinsert();
                XStore_Proto::BatchInsert_REP rep;
                my_store.BatchInsert(req, &rep);
                SendProto(socket, rep);
                break;
            }

            // BATCH QUERY
            case XStore_Proto::Request::kBatchquery: {
                // No Arena/BatchQuery_REP here -- BatchQuery shares
                // RangeQuery's zero-object-tree encoder (same wire
                // shape, see XStore_WireEncode.h), for the same reason: no
                // UnaryQuery_REP/RowData/FData tree ever gets built, so
                // there's nothing for Reset() to leave stranded as a
                // permanent per-worker RSS floor. Multi-frame reply,
                // reassembled transparently by the client library.
                auto *req = client_request->mutable_batchquery();
                my_store.BatchQuery(req, /*sort_time=*/true, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                    [&](std::string &&chunk, bool is_last) {
                        socket.send(zmq::buffer(chunk),
                                    is_last ? zmq::send_flags::dontwait
                                            : (zmq::send_flags::dontwait | zmq::send_flags::sndmore));
                    });
                break;
            }

            // BATCH DELETE
            case XStore_Proto::Request::kBatchdelete: {
                auto *req = client_request->mutable_batchdelete();
                XStore_Proto::BatchDelete_REP rep = my_store.BatchDelete(req);
                SendProto(socket, rep);
                break;
            }

            // RANGE INSERT
            case XStore_Proto::Request::kRangeinsert: {
                // Same as BatchInsert above: RangeInsert_REQ's repeated
                // RangeInsert_DATA -> RowData -> repeated FData tree is
                // already arena-allocated from the ParseFromArray above --
                // this is the actual benefit for RangeInsert, since its own
                // response (RangeInsert_REP) is just a status string with
                // nothing to arena.
                const auto &req = client_request->rangeinsert();
                XStore_Proto::RangeInsert_REP rep;
                my_store.RangeInsert(&req, &rep);
                SendProto(socket, rep);
                break;
            }

            // RANGE QUERY
            case XStore_Proto::Request::kRangequery: {
                // No Arena/RangeQuery_REP here -- RangeQuery never
                // builds a UnaryQuery_REP/RowData/FData tree for this
                // response (see its declaration comment in XStore.h and the
                // wire-encode helpers in XStore_RangeOps.cpp), which is the
                // fix for the OOM a big single-shot range used to cause: the
                // old path's arena tree, once built, was never freed
                // (Reset() keeps blocks) and became this worker's permanent
                // RSS floor. Each chunk here is sent as its own ZMQ frame
                // (SNDMORE on all but the last), reassembled by the client
                // library into one logical RangeQuery_REP -- transparent to
                // whatever called RangeQuery at the API level.
                const auto &req = client_request->rangequery();
                my_store.RangeQuery(req.starttime(), req.endtime(), req.dbname(),
                    XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                    [&](std::string &&chunk, bool is_last) {
                        socket.send(zmq::buffer(chunk),
                                    is_last ? zmq::send_flags::dontwait
                                            : (zmq::send_flags::dontwait | zmq::send_flags::sndmore));
                    });
                break;
            }

            // RANGE DELETE
            case XStore_Proto::Request::kRangedelete: {
                const auto &req = client_request->rangedelete();
                XStore_Proto::RangeDelete_REP rep = my_store.RangeDelete(req.starttime(), req.endtime(), req.dbname());
                SendProto(socket, rep);
                break;
            }

            // MIN AGGREGATE
            case XStore_Proto::Request::kMin: {
                const auto &req = client_request->min();
                XStore_Proto::Min_REP rep;
                my_store.MinAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                SendProto(socket, rep);
                break;
            }

            // MAX AGGREGATE
            case XStore_Proto::Request::kMax: {
                const auto &req = client_request->max();
                XStore_Proto::Max_REP rep;
                my_store.MaxAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                SendProto(socket, rep);
                break;
            }

            // SUM AGGREGATE
            case XStore_Proto::Request::kSum: {
                const auto &req = client_request->sum();
                XStore_Proto::Sum_REP rep;
                my_store.SumAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                SendProto(socket, rep);
                break;
            }

            // AVG AGGREGATE
            case XStore_Proto::Request::kAvg: {
                const auto &req = client_request->avg();
                XStore_Proto::Avg_REP rep;
                my_store.AvgAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                SendProto(socket, rep);
                break;
            }

            // No payload set (malformed/unrecognized request) -- reply so
            // the client's REQ socket never hangs waiting for a response
            // that will never come.
            case XStore_Proto::Request::PAYLOAD_NOT_SET:
            default: {
                spdlog::warn("[NETWORKING] - Received request with no payload set, dropping");
                socket.send(zmq::message_t(0), zmq::send_flags::dontwait);
                break;
            }
        }
    }
}
