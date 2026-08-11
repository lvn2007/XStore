#ifndef XSTORE_ZMQ_SERVER_H
#define XSTORE_ZMQ_SERVER_H

#include <include/XStore/XStore.h>
#include <thread>
#include "include/Networking/zmq.hpp"
#include "XStore.pb.h"
#include "include/Networking/zmq_addon.hpp"
#include "include/Logger/Logger.h"

class ZMQ_Server {
    private:
        // Prepare ZMQ context & Sockets
        zmq::context_t Ctx;

        int NumThreads;
        std::string ServerAddr;

        // Work routine run by each worker thread
        void WorkerRoutine(std::string serverAddr);

    public:
        ZMQ_Server(int numThreads);
        void StartServer(std::string listenAddr, std::string listenPort);
};

#endif //XSTORE_ZMQ_SERVER_H