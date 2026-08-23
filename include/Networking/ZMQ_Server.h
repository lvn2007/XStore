#ifndef XSTORE_ZMQ_SERVER_H
#define XSTORE_ZMQ_SERVER_H

#include <include/XStore/XStore.h>
#include <thread>
#include <zmq.hpp>
#include "XStore.pb.h"
#include <zmq_addon.hpp>
#include "include/Logger/Logger.h"

class ZmqServer {
    private:
        // Prepare ZMQ context & Sockets
        zmq::context_t ctx;

        int num_threads;
        std::string server_addr;

        // Work routine run by each worker thread
        void WorkerRoutine(std::string server_addr);

    public:
        ZmqServer(int num_threads);
        void StartServer(std::string listen_addr, std::string listen_port);
};

#endif //XSTORE_ZMQ_SERVER_H