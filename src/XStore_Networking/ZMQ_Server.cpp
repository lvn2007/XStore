#include "include/Networking/ZMQ_Server.h"
#include <google/protobuf/arena.h>

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
    size_t Size = msg.ByteSizeLong();
    zmq::message_t Reply(Size);
    msg.SerializeWithCachedSizesToArray(static_cast<uint8_t*>(Reply.data()));
    socket.send(Reply, zmq::send_flags::dontwait);
}
}  // namespace

ZMQ_Server::ZMQ_Server(int numThreads) {
    this->NumThreads = numThreads;
}

void ZMQ_Server::StartServer(std::string listenAddr, std::string listenPort) {
    //  Socket to talk to CLIENTS
    zmq::socket_t ClientSocket(Ctx, zmq::socket_type::router);
    std::string ServerAddr = "tcp://" + listenAddr + ":" + listenPort;

    // No queue depth cap -- 0 means "no limit" (bounded only by available
    // memory) rather than the library default of 1000 messages/socket.
    // Chosen deliberately: the goal here is maximum accepted throughput, not
    // bounded-memory backpressure: under a burst that outpaces the worker
    // pool, this queues in RAM instead of blocking/dropping at the socket.
    ClientSocket.set(zmq::sockopt::sndhwm, 0);
    ClientSocket.set(zmq::sockopt::rcvhwm, 0);

    // Set listenAddr & port internally
    this->ServerAddr = ServerAddr;

    ClientSocket.bind(ServerAddr);
    spdlog::info("[NETWORKING] - Listening on: {}", ServerAddr);

    //  Socket to talk to WORKERS
    zmq::socket_t WorkerSocket(Ctx, zmq::socket_type::dealer);
    WorkerSocket.set(zmq::sockopt::sndhwm, 0);
    WorkerSocket.set(zmq::sockopt::rcvhwm, 0);
    WorkerSocket.bind("inproc://workers");

    //  Launch pool of WORKER threads
    for (int i = 0; i < NumThreads; i++) {
        std::thread Worker(&ZMQ_Server::WorkerRoutine, this, ServerAddr.substr(6));
        Worker.detach();
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", NumThreads);

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void*>(ClientSocket), static_cast<void*>(WorkerSocket), nullptr);
}

void ZMQ_Server::WorkerRoutine(std::string serverAddr) {
    // Instantiate XStore for each thread
    XStore MyStore;

    // Each thread shares the server's ZMQ context. No queue-depth cap, same
    // reasoning as ClientSocket/WorkerSocket in StartServer.
    zmq::socket_t Socket(Ctx, zmq::socket_type::rep);
    Socket.set(zmq::sockopt::sndhwm, 0);
    Socket.set(zmq::sockopt::rcvhwm, 0);
    Socket.connect("inproc://workers");

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
    google::protobuf::Arena Arena;

    // Reused across iterations (this function IS the per-thread loop, so a
    // plain local -- no thread_local needed) instead of a fresh
    // zmq::message_t constructed every request. A single message_t, not a
    // vector: this REP socket only ever sees one body frame per request --
    // the client (XStore_Client.h) always does a single-frame Sock.send(),
    // never send_multipart -- so the more()-check loop and back_inserter
    // machinery recv_multipart pays for a possible second/third frame was
    // pure overhead here, never exercised in practice.
    zmq::message_t ClientMsg;

    while (true) {
        // RECEIVING
        //  Wait for next request from CLIENT
        Socket.recv(ClientMsg);

        // Free everything the previous iteration allocated in one shot --
        // safe here because that request's response was already serialized
        // and sent by SendProto before the loop came back around.
        Arena.Reset();

        //  Parse the envelope once -- the oneof case (payload_case()) below
        //  is the dispatch tag, so there's no separate lightweight parse
        //  followed by a second full parse of the same bytes.
        auto *ClientRequest = google::protobuf::Arena::Create<XStore_Proto::Request>(&Arena);
        if (!ClientRequest->ParseFromArray(ClientMsg.data(), ClientMsg.size())) {
            spdlog::warn("[NETWORKING] - Failed to parse request, dropping");
            Socket.send(zmq::message_t(0), zmq::send_flags::dontwait);
            continue;
        }

        switch (ClientRequest->payload_case()) {
            // INFO
            case XStore_Proto::Request::kInfo: {
                XStore_Proto::SysInfo_REP Rep = MyStore.InfoQuery(serverAddr, NumThreads);
                SendProto(Socket, Rep);
                break;
            }

            // PING -- warmup/liveness probe, no XStore work, just the round trip
            case XStore_Proto::Request::kPing: {
                XStore_Proto::Ping_REP Rep;
                SendProto(Socket, Rep);
                break;
            }

            // CREATE DB
            case XStore_Proto::Request::kCreatedb: {
                const auto &Req = ClientRequest->createdb();
                XStore_Proto::CreateDB_REP Rep = MyStore.CreateDB(Req.dbname(), Req.timeresolution(), Req.rowlength());
                SendProto(Socket, Rep);
                break;
            }

            // REMOVE DB
            case XStore_Proto::Request::kRemovedb: {
                const auto &Req = ClientRequest->removedb();
                XStore_Proto::RemoveDB_REP Rep = MyStore.RemoveDB(Req.dbname());
                SendProto(Socket, Rep);
                break;
            }

            // REMOVE DB FILES
            case XStore_Proto::Request::kRemovedbfiles: {
                auto *Req = ClientRequest->mutable_removedbfiles();
                XStore_Proto::RemoveDBFiles_REP Rep = MyStore.RemoveDBFiles(Req, Req->dbname());
                SendProto(Socket, Rep);
                break;
            }

            // UNARY INSERT
            case XStore_Proto::Request::kUnaryinsert: {
                const auto &Req = ClientRequest->unaryinsert();
                XStore_Proto::UnaryInsert_REP Rep;
                MyStore.UnaryInsert(Req.timestamp(), Req.data(), Req.dbname(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // UNARY QUERY
            case XStore_Proto::Request::kUnaryquery: {
                const auto &Req = ClientRequest->unaryquery();
                // Arena-allocated: same RowData -> repeated FData nesting as
                // Range/BatchQuery (up to one alloc per column), and this is
                // likely the highest-QPS RPC of all of them -- single-row
                // point queries. Not scaled by span like RangeQuery, but
                // paid on every call with no reuse across calls.
                auto *Rep = google::protobuf::Arena::Create<XStore_Proto::UnaryQuery_REP>(&Arena);
                MyStore.UnaryQuery(Req.timestamp(), Req.dbname(), Rep);
                SendProto(Socket, *Rep);
                break;
            }

            // UNARY DELETE
            case XStore_Proto::Request::kUnarydelete: {
                const auto &Req = ClientRequest->unarydelete();
                XStore_Proto::UnaryDelete_REP Rep = MyStore.UnaryDelete(Req.timestamp(), Req.dbname());
                SendProto(Socket, Rep);
                break;
            }

            // BATCH INSERT
            case XStore_Proto::Request::kBatchinsert: {
                // Req itself is already arena-allocated (it's a field inside
                // ClientRequest, which lives on Arena) -- BatchInsert_REQ's
                // repeated BatchInsert_DATA, each with its own RowData ->
                // repeated FData, was parsed straight into arena memory
                // above with no extra work needed here.
                auto *Req = ClientRequest->mutable_batchinsert();
                XStore_Proto::BatchInsert_REP Rep;
                MyStore.BatchInsert(Req, &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // BATCH QUERY
            case XStore_Proto::Request::kBatchquery: {
                auto *Req = ClientRequest->mutable_batchquery();
                // Arena-allocated: queryResponse is repeated UnaryQuery_REP,
                // each entry owning its own RowData -> repeated FData, so a
                // wide/long response means one heap alloc per row plus one
                // per column inside it via the ordinary allocator. Arena
                // turns that whole tree into bump-pointer allocations off
                // one block, freed in one shot when Arena is Reset() at the
                // top of the loop -- no per-message new/delete.
                auto *Rep = google::protobuf::Arena::Create<XStore_Proto::BatchQuery_REP>(&Arena);
                MyStore.BatchQuery(Req, Rep);
                SendProto(Socket, *Rep);
                break;
            }

            // BATCH DELETE
            case XStore_Proto::Request::kBatchdelete: {
                auto *Req = ClientRequest->mutable_batchdelete();
                XStore_Proto::BatchDelete_REP Rep = MyStore.BatchDelete(Req);
                SendProto(Socket, Rep);
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
                const auto &Req = ClientRequest->rangeinsert();
                XStore_Proto::RangeInsert_REP Rep;
                MyStore.RangeInsert(&Req, &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // RANGE QUERY
            case XStore_Proto::Request::kRangequery: {
                const auto &Req = ClientRequest->rangequery();
                // Arena-allocated -- see BatchQuery's case above for why.
                // RangeQuery's row count scales with span (not a fixed
                // batch size), so this is where the per-row allocation
                // count matters most.
                auto *Rep = google::protobuf::Arena::Create<XStore_Proto::RangeQuery_REP>(&Arena);
                MyStore.RangeQuery(Req.starttime(), Req.endtime(), Req.dbname(), Rep);
                SendProto(Socket, *Rep);
                break;
            }

            // RANGE DELETE
            case XStore_Proto::Request::kRangedelete: {
                const auto &Req = ClientRequest->rangedelete();
                XStore_Proto::RangeDelete_REP Rep = MyStore.RangeDelete(Req.starttime(), Req.endtime(), Req.dbname());
                SendProto(Socket, Rep);
                break;
            }

            // MIN AGGREGATE
            case XStore_Proto::Request::kMin: {
                const auto &Req = ClientRequest->min();
                XStore_Proto::Min_REP Rep;
                MyStore.MinAggregate(Req.starttime(), Req.endtime(), Req.dbname(), Req.columnidx(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // MAX AGGREGATE
            case XStore_Proto::Request::kMax: {
                const auto &Req = ClientRequest->max();
                XStore_Proto::Max_REP Rep;
                MyStore.MaxAggregate(Req.starttime(), Req.endtime(), Req.dbname(), Req.columnidx(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // SUM AGGREGATE
            case XStore_Proto::Request::kSum: {
                const auto &Req = ClientRequest->sum();
                XStore_Proto::Sum_REP Rep;
                MyStore.SumAggregate(Req.starttime(), Req.endtime(), Req.dbname(), Req.columnidx(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // AVG AGGREGATE
            case XStore_Proto::Request::kAvg: {
                const auto &Req = ClientRequest->avg();
                XStore_Proto::Avg_REP Rep;
                MyStore.AvgAggregate(Req.starttime(), Req.endtime(), Req.dbname(), Req.columnidx(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // No payload set (malformed/unrecognized request) -- reply so
            // the client's REQ socket never hangs waiting for a response
            // that will never come.
            case XStore_Proto::Request::PAYLOAD_NOT_SET:
            default: {
                spdlog::warn("[NETWORKING] - Received request with no payload set, dropping");
                Socket.send(zmq::message_t(0), zmq::send_flags::dontwait);
                break;
            }
        }
    }
}
