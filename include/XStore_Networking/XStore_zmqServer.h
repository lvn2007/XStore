#ifndef XSTORE_XSTORE_ZMQSERVER_H
#define XSTORE_XSTORE_ZMQSERVER_H

#include <include/XStore/XStore.h>
#include <pthread.h>
#include "include/XStore_Networking/zmq.hpp"
#include "xStore.pb.h"
#include "include/XStore_Networking/zmq_addon.hpp"

class XStore_zmqServer {
    private:
        int numThreads = 1;

        // Work routines in each thread
        static void* workerRoutine(void *args);

    public:
        XStore_zmqServer(int numThreads);
        void startServer(std::string listenAddr, std::string listenPort);
};

#endif //XSTORE_XSTORE_ZMQSERVER_H
