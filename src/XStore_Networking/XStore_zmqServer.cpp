#include "include/XStore_Networking/XStore_zmqServer.h"

XStore_zmqServer::XStore_zmqServer(int numThreads) {
    this->numThreads = numThreads;
}

void XStore_zmqServer::startServer() {
    // Prepare ZMQ context & Sockets
    zmq::context_t ctx;

    //  Socket to talk to CLIENTS
    zmq::socket_t clientSocket(ctx, zmq::socket_type::router);
    clientSocket.bind("tcp://*:" + PORT);

    //  Socket to talk to WORKERS
    zmq::socket_t workerSocket(ctx, zmq::socket_type::dealer);
    workerSocket.bind("inproc://workers");

    //  Launch pool of WORKER threads
    std::cout << "Num threads: " << numThreads << std::endl;
    for (int i = 0; i < numThreads; i++) {
        pthread_t worker;
        pthread_create(&worker, NULL, &XStore_zmqServer::workerRoutine, (void *) &ctx);
    }

    // Connect WORKER threads to CLIENT threads via queue
    zmq::proxy(static_cast<void *> (clientSocket), static_cast<void *> (workerSocket), nullptr);
}

void* XStore_zmqServer::workerRoutine(void *args) {
    zmq::context_t *ctx = (zmq::context_t *) args;

    zmq::socket_t socket (*ctx, zmq::socket_type::rep);
    socket.connect("inproc://workers");

    while (true) {
        // RECEIVING
        //  Wait for next request from CLIENT
        zmq::message_t requestFromClient;
        std::vector<zmq::message_t> messagesFromClient;
        zmq::recv_multipart(socket, std::back_inserter(messagesFromClient));

        //  Parse Protobuf request from CLIENT
        xStoregRPC::unaryRequestMsg protoFromClient;
        protoFromClient.ParseFromArray(messagesFromClient.back().data(), messagesFromClient.back().size());

        // SENDING
        //  Preparing Protobuf response to CLIENT
        xStoregRPC::responseData protoToClient;
        protoToClient.set_timestamp(123456789);
        protoToClient.set_dataset("From thread");

        //  Response message size
        size_t protoToClientSize = protoToClient.ByteSizeLong();
        zmq::message_t responseToClient(protoToClientSize);
        protoToClient.SerializeToArray(responseToClient.data(), protoToClientSize);
        socket.send(responseToClient);
    }
    return NULL;
}