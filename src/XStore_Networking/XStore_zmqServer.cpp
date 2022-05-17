#include "include/XStore_Networking/XStore_zmqServer.h"

XStore_zmqServer::XStore_zmqServer(int numThreads) {
    this->numThreads = numThreads;
}

void XStore_zmqServer::startServer(std::string listenAddr, std::string listenPort) {
    // Prepare ZMQ context & Sockets
    zmq::context_t ctx;

    //  Socket to talk to CLIENTS
    zmq::socket_t clientSocket(ctx, zmq::socket_type::router);
    clientSocket.bind("tcp://" + listenAddr + ":" + listenPort);
    std::cout << "[INFO - Networking] -- Listening on: " << listenAddr + ":" + listenPort << std::endl;

    //  Socket to talk to WORKERS
    zmq::socket_t workerSocket(ctx, zmq::socket_type::dealer);
    workerSocket.bind("inproc://workers");

    //  Launch pool of WORKER threads
    for (int i = 0; i < numThreads; i++) {
        pthread_t worker;
        pthread_create(&worker, NULL, XStore_zmqServer::workerRoutine, (void *) &ctx);
    }
    std::cout << "[INFO - Networking] -- Launched " << numThreads << " worker thread(s)" << std::endl;

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void *> (clientSocket), static_cast<void *> (workerSocket), nullptr);
}

void* XStore_zmqServer::workerRoutine(void *args) {
    // Instantiate XStore for each thread
    XStore myStore;

    // Each thread owns its own ZMQ context
    zmq::context_t *ctx = (zmq::context_t *) args;
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
        xstoreProto::reqCMD clientReqCmd;
        clientReqCmd.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

        //  Parsed CMD from CLIENT
        int rType = clientReqCmd.rtype();

        switch (rType) {
            // UNARY REQUEST
            case xstoreProto::UNARYREQUEST: {
                // Properly parse Protobuf obj from CLIENT
                xstoreProto::unaryRequest protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobuf obj
                xstoreProto::unaryResponse protoToClient = myStore.unaryQuery(protoFromClient.timestamp(), protoFromClient.dbname());

                //  Response message size
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::none);
                break;
            }

            // BATCH REQUEST
            case xstoreProto::BATCHREQUEST: {
                // Properly parse Protobuf obj from CLIENT
                xstoreProto::rangeRequest protoFromClient;
                protoFromClient.ParseFromArray(clientMsgs.back().data(), clientMsgs.back().size());

                // XStore stuffs
                //  Returning Protobug obj
                xstoreProto::batchResponse protoToClient = myStore.batchQuery(protoFromClient.starttime(), protoFromClient.endtime(), protoFromClient.dbname());

                //  Response message
                size_t protoToClientSize = protoToClient.ByteSizeLong();
                zmq::message_t responseToClient(protoToClientSize);

                //  Serialize & Dispatch msg to CLIENT
                protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
                socket.send(responseToClient, zmq::send_flags::none);
                break;
            }

            // UNARY INSERT
            case xstoreProto::UNARYINSERT: {
                std::cout << 2 << std::endl;
                break;
            }

            // BATCH INSERT
            case xstoreProto::BATCHINSERT: {
                std::cout << 3 << std::endl;
                break;
            }

            default: {
                std::cout << "Default" << std::endl;
                break;
            }
        }
    }
}