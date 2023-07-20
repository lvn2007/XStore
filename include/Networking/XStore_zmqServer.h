#ifndef XSTORE_XSTORE_ZMQSERVER_H
#define XSTORE_XSTORE_ZMQSERVER_H

#include <include/XStore/XStore.h>
#include <pthread.h>
#include "include/Networking/zmq.hpp"
#include "XStore.pb.h"
#include "include/Networking/zmq_addon.hpp"
#include "include/Networking/zstdpp.hpp"

class XStore_zmqServer {
    private:
        int numThreads;
        std::string serverAddr;

        // Work routines in each thread
        static void* workerRoutine(void *args);

    public:
        XStore_zmqServer(int numThreads);
        void startServer(std::string listenAddr, std::string listenPort);
};

#endif //XSTORE_XSTORE_ZMQSERVER_H