#include "include/Networking/XStore_zmqServer.h"

struct threadArgs {
    zmq::context_t *zmqCtx;
    int numThreads;
    std::string serverAddr;
};

XStore_zmqServer::XStore_zmqServer(int numThreads) {
    this->numThreads = numThreads;
}

void XStore_zmqServer::startServer(std::string listenAddr, std::string listenPort) {
    // Prepare ZMQ context & Sockets
    zmq::context_t ctx;

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
        threadArgs tArgs;
        tArgs.zmqCtx = &ctx;
        tArgs.numThreads = numThreads;
        tArgs.serverAddr = serverAddr.substr(6, serverAddr.length());       // Proto string refuses to work with special char like ://

        // Spawn thread
        pthread_t worker;
        pthread_create(&worker, NULL, XStore_zmqServer::workerRoutine, &tArgs);
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", numThreads);

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void*>(clientSocket), static_cast<void*>(workerSocket), nullptr);
}

void* XStore_zmqServer::workerRoutine(void *args) {
    // Unpack thread args
    threadArgs *tArgs = (threadArgs*) args;
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
        //      Doesn't matter if actual msgLength > reqCMD protobuf obj
        XStore_Proto::reqCMD clientReqCmd;
        clientReqCmd.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

        //  Parsed CMD from CLIENT
        int rType = clientReqCmd.rtype();
        clientReqCmd.Clear();

        switch (rType) {
            // INFO
            case XStore_Proto::INFO: {
                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::sysInfo_REP protoToClient = myStore.infoQuery(serverAddr, tArgs->numThreads);

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
                XStore_Proto::createDB_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::createDB_REP protoToClient = myStore.createDB(protoFromClient.dbname(), protoFromClient.timeresolution(), protoFromClient.rowlength());

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
                XStore_Proto::removeDB_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::removeDB_REP protoToClient = myStore.removeDB(protoFromClient.dbname());

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
                XStore_Proto::removeDBFiles_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::removeDBFiles_REP protoToClient = myStore.removeDBFiles(&protoFromClient, protoFromClient.dbname());

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
                XStore_Proto::unaryInsert_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::unaryInsert_REP protoToClient;
                myStore.unaryInsert(protoFromClient.timestamp(), protoFromClient.data(), protoFromClient.dbname(), &protoToClient);

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
                XStore_Proto::unaryQuery_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::unaryQuery_REP protoToClient;
                myStore.unaryQuery(protoFromClient.timestamp(), protoFromClient.dbname(), &protoToClient);

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
                XStore_Proto::unaryDelete_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::unaryDelete_REP protoToClient = myStore.unaryDelete(protoFromClient.timestamp(), protoFromClient.dbname());

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
                XStore_Proto::batchInsert_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::batchInsert_REP protoToClient;
                myStore.batchInsert(&protoFromClient, &protoToClient);

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
                XStore_Proto::batchQuery_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::batchQuery_REP protoToClient;
                myStore.batchQuery(&protoFromClient, &protoToClient);

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
                XStore_Proto::batchDelete_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::batchDelete_REP protoToClient = myStore.batchDelete(&protoFromClient);

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
                XStore_Proto::rangeInsert_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::rangeInsert_REP protoToClient;
                myStore.rangeInsert(&protoFromClient, &protoToClient);

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
                XStore_Proto::rangeQuery_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::rangeQuery_REP protoToClient;
                myStore.rangeQuery(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname(), &protoToClient);

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
                XStore_Proto::rangeDelete_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::rangeDelete_REP protoToClient = myStore.rangeDelete(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname());

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
                XStore_Proto::min_REQ protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                XStore_Proto::min_REP protoToClient;
                myStore.minAggregate(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname(), protoFromClient.columnidx(), &protoToClient);

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