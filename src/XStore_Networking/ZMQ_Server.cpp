#include "include/Networking/ZMQ_Server.h"

struct ThreadArgs {
    zmq::context_t *zmqCtx;
    int numThreads;
    std::string serverAddr;
};

ZMQ_Server::ZMQ_Server(int numThreads) {
    this->numThreads = numThreads;
}

void ZMQ_Server::StartServer(std::string listenAddr, std::string listenPort) {
    //  Socket to talk to CLIENTS
    zmq::socket_t clientSocket(ctx, zmq::socket_type::router);
    std::string serverAddr = "tcp://" + listenAddr + ":" + listenPort;

    // Set listenAddr & port internally
    this->serverAddr = serverAddr;

    clientSocket.bind(serverAddr);
    spdlog::info("[NETWORKING] - Listening on: {}", serverAddr);

    //  Socket to talk to WORKERS
    zmq::socket_t workerSocket(ctx, zmq::socket_type::dealer);
    workerSocket.bind("inproc://workers");

    //  Launch pool of WORKER threads
    for (int i = 0; i < numThreads; i++) {
        // Pack args struct for each thread
        ThreadArgs* tArgs = new ThreadArgs(); 
        tArgs->zmqCtx = &this->ctx;
        tArgs->numThreads = numThreads;
        tArgs->serverAddr = serverAddr.substr(6, serverAddr.length());       // Proto string refuses to work with special char like ://

        // Spawn thread
        pthread_t worker;
        pthread_create(&worker, NULL, ZMQ_Server::WorkerRoutine, tArgs);
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", numThreads);

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void*>(clientSocket), static_cast<void*>(workerSocket), nullptr);
}

void* ZMQ_Server::WorkerRoutine(void *args) {
    // Unpack thread args
    ThreadArgs *tArgs = (ThreadArgs*) args;
    std::string serverAddr = (std::string) tArgs->serverAddr;

    // Instantiate XStore for each thread
    XStore myStore;

    // Each thread owns its own ZMQ context
    zmq::context_t *ctx = tArgs->zmqCtx;
    zmq::socket_t socket (*ctx, zmq::socket_type::rep);
    socket.connect("inproc://workers");

    while (true) {
        // RECEIVING
        //  Wait for next request from CLIENT
        std::vector<zmq::message_t> clientMsgs;
        zmq::recv_multipart(socket, std::back_inserter(clientMsgs));

        //  Partially parse Protobuf request from CLIENT
        //      ONLY parse the CMD type -- We only interested in field "rType"
        //      Doesn't matter if actual msgLength > ReqCMD protobuf obj
        XStore_Proto::ReqCMD clientReqCmd;
        clientReqCmd.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

        //  Parsed CMD from CLIENT
        int rType = clientReqCmd.rtype();
        clientReqCmd.Clear();

        switch (rType) {
            // INFO
            case XStore_Proto::INFO: {
                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::SysInfo_REP protoToClient = myStore.InfoQuery(serverAddr, tArgs->numThreads);

                //  Response message size
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // CREATE DB
            case XStore_Proto::CREATEDB: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::CreateDB_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::CreateDB_REP protoToClient = myStore.CreateDB(protoFromClient.dbname(), protoFromClient.timeresolution(), protoFromClient.rowlength());

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // REMOVE DB
            case XStore_Proto::REMOVEDB: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RemoveDB_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RemoveDB_REP protoToClient = myStore.RemoveDB(protoFromClient.dbname());

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // REMOVE DB FILES
            case XStore_Proto::REMOVEDBFILES: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RemoveDBFiles_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RemoveDBFiles_REP protoToClient = myStore.RemoveDBFiles(&protoFromClient, protoFromClient.dbname());

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // UNARY INSERT
            case XStore_Proto::UNARYINSERT: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::UnaryInsert_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::UnaryInsert_REP protoToClient;
                myStore.UnaryInsert(protoFromClient.timestamp(), protoFromClient.data(), protoFromClient.dbname(), &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // UNARY QUERY
            case XStore_Proto::UNARYQUERY: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::UnaryQuery_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::UnaryQuery_REP protoToClient;
                myStore.UnaryQuery(protoFromClient.timestamp(), protoFromClient.dbname(), &protoToClient);

                //  Response message size
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // UNARY DELETE
            case XStore_Proto::UNARYDELETE: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::UnaryDelete_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::UnaryDelete_REP protoToClient = myStore.UnaryDelete(protoFromClient.timestamp(), protoFromClient.dbname());

                //  Response message size
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // BATCH INSERT
            case XStore_Proto::BATCHINSERT: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::BatchInsert_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::BatchInsert_REP protoToClient;
                myStore.BatchInsert(&protoFromClient, &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // BATCH QUERY
            case XStore_Proto::BATCHQUERY: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::BatchQuery_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::BatchQuery_REP protoToClient;
                myStore.BatchQuery(&protoFromClient, &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // BATCH DELETE
            case XStore_Proto::BATCHDELETE: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::BatchDelete_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::BatchDelete_REP protoToClient = myStore.BatchDelete(&protoFromClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // RANGE INSERT
            case XStore_Proto::RANGEINSERT: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RangeInsert_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RangeInsert_REP protoToClient;
                myStore.RangeInsert(&protoFromClient, &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // RANGE QUERY
            case XStore_Proto::RANGEQUERY: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RangeQuery_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RangeQuery_REP protoToClient;
                myStore.RangeQuery(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname(), &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // RANGE DELETE
            case XStore_Proto::RANGEDELETE: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::RangeDelete_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::RangeDelete_REP protoToClient = myStore.RangeDelete(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname());

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // MIN AGGREGATE
            case XStore_Proto::MIN: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Min_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Min_REP protoToClient;
                myStore.MinAggregateV3(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname(), protoFromClient.columnidx(), &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // MAX AGGREGATE
            case XStore_Proto::MAX: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Max_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Max_REP protoToClient;
                myStore.MaxAggregateV2(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname(), protoFromClient.columnidx(), &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // SUM AGGREGATE
            case XStore_Proto::SUM: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Sum_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Sum_REP protoToClient;
                myStore.SumAggregateV2(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname(), protoFromClient.columnidx(), &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }

            // AVG AGGREGATE
            case XStore_Proto::AVG: {
                // Properly parse Protobuf obj from CLIENT
                XStore_Proto::Avg_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::Avg_REP protoToClient;
                myStore.AvgAggregateV2(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname(), protoFromClient.columnidx(), &protoToClient);

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::dontwait);
                break;
            }
        }
    }
}