#include "include/Networking/ZMQ_Server.h"

struct ThreadArgs {
    zmq::context_t *ZmqCtx;
    int NumThreads;
    std::string ServerAddr;
};

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
        // Pack args struct for each thread
        ThreadArgs* TArgs = new ThreadArgs(); 
        TArgs->ZmqCtx = &this->Ctx;
        TArgs->NumThreads = NumThreads;
        TArgs->ServerAddr = ServerAddr.substr(6, ServerAddr.length());       // Proto string refuses to work with special char like ://

        // Spawn thread
        pthread_t Worker;
        pthread_create(&Worker, NULL, ZMQ_Server::WorkerRoutine, TArgs);
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", NumThreads);

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void*>(ClientSocket), static_cast<void*>(WorkerSocket), nullptr);
}

void* ZMQ_Server::WorkerRoutine(void *args) {
    // Unpack thread args
    ThreadArgs *TArgs = (ThreadArgs*) args;
    std::string ServerAddr = (std::string) TArgs->ServerAddr;

    // Instantiate XStore for each thread
    XStore MyStore;

    // Each thread owns its own ZMQ context
    zmq::context_t *Ctx = TArgs->ZmqCtx;
    zmq::socket_t Socket (*Ctx, zmq::socket_type::rep);
    Socket.connect("inproc://workers");

    while (true) {
        // RECEIVING
        //  Wait for next request from CLIENT
        std::vector<zmq::message_t> ClientMsgs;
        zmq::recv_multipart(Socket, std::back_inserter(ClientMsgs));

        //  Partially parse Protobuf request from CLIENT
        //      ONLY parse the CMD type -- We only interested in field "rType"
        //      Doesn't matter if actual msgLength > ReqCMD protobuf obj
        XStore_Proto::ReqCMD ClientReqCmd;
        ClientReqCmd.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

        //  Parsed CMD from CLIENT
        int RType = ClientReqCmd.rtype();
        ClientReqCmd.Clear();

        switch (RType) {
            // INFO
            case XStore_Proto::INFO: {
                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::SysInfo_REP ProtoToClient = MyStore.InfoQuery(ServerAddr, TArgs->NumThreads);

                //  Response message size
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // CREATE DB
            case XStore_Proto::CREATEDB: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::CreateDB_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::CreateDB_REP ProtoToClient = MyStore.CreateDB(ProtoFromClient.dbname(), ProtoFromClient.timeresolution(), ProtoFromClient.rowlength());

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // REMOVE DB
            case XStore_Proto::REMOVEDB: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RemoveDB_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RemoveDB_REP ProtoToClient = MyStore.RemoveDB(ProtoFromClient.dbname());

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // REMOVE DB FILES
            case XStore_Proto::REMOVEDBFILES: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RemoveDBFiles_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RemoveDBFiles_REP ProtoToClient = MyStore.RemoveDBFiles(&ProtoFromClient, ProtoFromClient.dbname());

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // UNARY INSERT
            case XStore_Proto::UNARYINSERT: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::UnaryInsert_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::UnaryInsert_REP ProtoToClient;
                MyStore.UnaryInsert(ProtoFromClient.timestamp(), ProtoFromClient.data(), ProtoFromClient.dbname(), &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // UNARY QUERY
            case XStore_Proto::UNARYQUERY: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::UnaryQuery_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::UnaryQuery_REP ProtoToClient;
                MyStore.UnaryQuery(ProtoFromClient.timestamp(), ProtoFromClient.dbname(), &ProtoToClient);

                //  Response message size
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // UNARY DELETE
            case XStore_Proto::UNARYDELETE: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::UnaryDelete_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::UnaryDelete_REP ProtoToClient = MyStore.UnaryDelete(ProtoFromClient.timestamp(), ProtoFromClient.dbname());

                //  Response message size
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // BATCH INSERT
            case XStore_Proto::BATCHINSERT: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::BatchInsert_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::BatchInsert_REP ProtoToClient;
                MyStore.BatchInsert(&ProtoFromClient, &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // BATCH QUERY
            case XStore_Proto::BATCHQUERY: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::BatchQuery_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::BatchQuery_REP ProtoToClient;
                MyStore.BatchQuery(&ProtoFromClient, &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // BATCH DELETE
            case XStore_Proto::BATCHDELETE: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::BatchDelete_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::BatchDelete_REP ProtoToClient = MyStore.BatchDelete(&ProtoFromClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // RANGE INSERT
            case XStore_Proto::RANGEINSERT: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RangeInsert_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RangeInsert_REP ProtoToClient;
                MyStore.RangeInsert(&ProtoFromClient, &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // RANGE QUERY
            case XStore_Proto::RANGEQUERY: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RangeQuery_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RangeQuery_REP ProtoToClient;
                MyStore.RangeQuery(ProtoFromClient.starttime(), ProtoFromClient.endtime(), ProtoFromClient.dbname(), &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // RANGE DELETE
            case XStore_Proto::RANGEDELETE: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RangeDelete_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RangeDelete_REP ProtoToClient = MyStore.RangeDelete(ProtoFromClient.starttime(), ProtoFromClient.endtime(), ProtoFromClient.dbname());

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // MIN AGGREGATE
            case XStore_Proto::MIN: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Min_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Min_REP ProtoToClient;
                MyStore.MinAggregate(ProtoFromClient.starttime(), ProtoFromClient.endtime(), ProtoFromClient.dbname(), ProtoFromClient.columnidx(), &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // MAX AGGREGATE
            case XStore_Proto::MAX: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Max_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Max_REP ProtoToClient;
                MyStore.MaxAggregate(ProtoFromClient.starttime(), ProtoFromClient.endtime(), ProtoFromClient.dbname(), ProtoFromClient.columnidx(), &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // SUM AGGREGATE
            case XStore_Proto::SUM: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Sum_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Sum_REP ProtoToClient;
                MyStore.SumAggregate(ProtoFromClient.starttime(), ProtoFromClient.endtime(), ProtoFromClient.dbname(), ProtoFromClient.columnidx(), &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }

            // AVG AGGREGATE
            case XStore_Proto::AVG: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Avg_REQ ProtoFromClient;
                ProtoFromClient.ParseFromArray(ClientMsgs.back().data(), ClientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Avg_REP ProtoToClient;
                MyStore.AvgAggregate(ProtoFromClient.starttime(), ProtoFromClient.endtime(), ProtoFromClient.dbname(), ProtoFromClient.columnidx(), &ProtoToClient);

                //  Response message
                size_t ProtoToClientSize = ProtoToClient.ByteSizeLong();
                zmq::message_t ResponseToClient(ProtoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                ProtoToClient.SerializeToArray(ResponseToClient.data(), ProtoToClientSize);
                Socket.send(ResponseToClient, zmq::send_flags::dontwait);
                break;
            }
        }
    }
}