#ifndef XSTORE_ZMQ_SERVER_H
#define XSTORE_ZMQ_SERVER_H

#include <include/XStore/XStore.h>
#include <pthread.h>
#include "include/Networking/zmq.hpp"
#include "XStore.pb.h"
#include "include/Networking/zmq_addon.hpp"
#include "include/Logger/Logger.h"

class ZMQ_Server {
    private:
        // Prepare ZMQ context & Sockets
        zmq::context_t ctx;

        int numThreads;
        std::string serverAddr;

        // Work routines in each thread
        static void* WorkerRoutine(void *args);

    public:
        ZMQ_Server(int numThreads);
        void StartServer(std::string listenAddr, std::string listenPort);
};

#endif //XSTORE_ZMQ_SERVER_H