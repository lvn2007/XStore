#include "include/Networking/ZMQ_Server.h"

namespace {
// Shared by every case below: serialize a response proto and dispatch it to
// the CLIENT. REP sockets enforce recv-then-send ordering already, so
// dontwait never actually blocks here -- kept only to match a fire-and-forget
// send with no result check, same as before.
template <typename T>
void SendProto(zmq::socket_t &socket, const T &msg) {
    size_t Size = msg.ByteSizeLong();
    zmq::message_t Reply(Size);
    msg.SerializeToArray(Reply.data(), Size);
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

    // Set listenAddr & port internally
    this->ServerAddr = ServerAddr;

    ClientSocket.bind(ServerAddr);
    spdlog::info("[NETWORKING] - Listening on: {}", ServerAddr);

    //  Socket to talk to WORKERS
    zmq::socket_t WorkerSocket(Ctx, zmq::socket_type::dealer);
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

    // Each thread shares the server's ZMQ context
    zmq::socket_t Socket(Ctx, zmq::socket_type::rep);
    Socket.connect("inproc://workers");

    while (true) {
        // RECEIVING
        //  Wait for next request from CLIENT
        std::vector<zmq::message_t> ClientMsgs;
        zmq::recv_multipart(Socket, std::back_inserter(ClientMsgs));

        //  Parse the envelope once -- the oneof case (payload_case()) below
        //  is the dispatch tag, so there's no separate lightweight parse
        //  followed by a second full parse of the same bytes.
        XStore_Proto::Request ClientRequest;
        if (!ClientRequest.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size())) {
            spdlog::warn("[NETWORKING] - Failed to parse request, dropping");
            Socket.send(zmq::message_t(0), zmq::send_flags::dontwait);
            continue;
        }

        switch (ClientRequest.payload_case()) {
            // INFO
            case XStore_Proto::Request::kInfo: {
                XStore_Proto::SysInfo_REP Rep = MyStore.InfoQuery(serverAddr, NumThreads);
                SendProto(Socket, Rep);
                break;
            }

            // CREATE DB
            case XStore_Proto::Request::kCreatedb: {
                const auto &Req = ClientRequest.createdb();
                XStore_Proto::CreateDB_REP Rep = MyStore.CreateDB(Req.dbname(), Req.timeresolution(), Req.rowlength());
                SendProto(Socket, Rep);
                break;
            }

            // REMOVE DB
            case XStore_Proto::Request::kRemovedb: {
                const auto &Req = ClientRequest.removedb();
                XStore_Proto::RemoveDB_REP Rep = MyStore.RemoveDB(Req.dbname());
                SendProto(Socket, Rep);
                break;
            }

            // REMOVE DB FILES
            case XStore_Proto::Request::kRemovedbfiles: {
                auto *Req = ClientRequest.mutable_removedbfiles();
                XStore_Proto::RemoveDBFiles_REP Rep = MyStore.RemoveDBFiles(Req, Req->dbname());
                SendProto(Socket, Rep);
                break;
            }

            // UNARY INSERT
            case XStore_Proto::Request::kUnaryinsert: {
                const auto &Req = ClientRequest.unaryinsert();
                XStore_Proto::UnaryInsert_REP Rep;
                MyStore.UnaryInsert(Req.timestamp(), Req.data(), Req.dbname(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // UNARY QUERY
            case XStore_Proto::Request::kUnaryquery: {
                const auto &Req = ClientRequest.unaryquery();
                XStore_Proto::UnaryQuery_REP Rep;
                MyStore.UnaryQuery(Req.timestamp(), Req.dbname(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // UNARY DELETE
            case XStore_Proto::Request::kUnarydelete: {
                const auto &Req = ClientRequest.unarydelete();
                XStore_Proto::UnaryDelete_REP Rep = MyStore.UnaryDelete(Req.timestamp(), Req.dbname());
                SendProto(Socket, Rep);
                break;
            }

            // BATCH INSERT
            case XStore_Proto::Request::kBatchinsert: {
                auto *Req = ClientRequest.mutable_batchinsert();
                XStore_Proto::BatchInsert_REP Rep;
                MyStore.BatchInsert(Req, &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // BATCH QUERY
            case XStore_Proto::Request::kBatchquery: {
                auto *Req = ClientRequest.mutable_batchquery();
                XStore_Proto::BatchQuery_REP Rep;
                MyStore.BatchQuery(Req, &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // BATCH DELETE
            case XStore_Proto::Request::kBatchdelete: {
                auto *Req = ClientRequest.mutable_batchdelete();
                XStore_Proto::BatchDelete_REP Rep = MyStore.BatchDelete(Req);
                SendProto(Socket, Rep);
                break;
            }

            // RANGE INSERT
            case XStore_Proto::Request::kRangeinsert: {
                const auto &Req = ClientRequest.rangeinsert();
                XStore_Proto::RangeInsert_REP Rep;
                MyStore.RangeInsert(&Req, &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // RANGE QUERY
            case XStore_Proto::Request::kRangequery: {
                const auto &Req = ClientRequest.rangequery();
                XStore_Proto::RangeQuery_REP Rep;
                MyStore.RangeQuery(Req.starttime(), Req.endtime(), Req.dbname(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // RANGE DELETE
            case XStore_Proto::Request::kRangedelete: {
                const auto &Req = ClientRequest.rangedelete();
                XStore_Proto::RangeDelete_REP Rep = MyStore.RangeDelete(Req.starttime(), Req.endtime(), Req.dbname());
                SendProto(Socket, Rep);
                break;
            }

            // MIN AGGREGATE
            case XStore_Proto::Request::kMin: {
                const auto &Req = ClientRequest.min();
                XStore_Proto::Min_REP Rep;
                MyStore.MinAggregate(Req.starttime(), Req.endtime(), Req.dbname(), Req.columnidx(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // MAX AGGREGATE
            case XStore_Proto::Request::kMax: {
                const auto &Req = ClientRequest.max();
                XStore_Proto::Max_REP Rep;
                MyStore.MaxAggregate(Req.starttime(), Req.endtime(), Req.dbname(), Req.columnidx(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // SUM AGGREGATE
            case XStore_Proto::Request::kSum: {
                const auto &Req = ClientRequest.sum();
                XStore_Proto::Sum_REP Rep;
                MyStore.SumAggregate(Req.starttime(), Req.endtime(), Req.dbname(), Req.columnidx(), &Rep);
                SendProto(Socket, Rep);
                break;
            }

            // AVG AGGREGATE
            case XStore_Proto::Request::kAvg: {
                const auto &Req = ClientRequest.avg();
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
