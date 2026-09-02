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

        // Physical (not logical/SMT) CPU core count on this host, with a
        // graceful fallback -- see the .cpp definition for why this, not
        // std::thread::hardware_concurrency(), is the right default worker
        // count for production. Callers needing a SPECIFIC count for other
        // reasons (tests wanting a small, deterministic pool) should keep
        // passing an explicit value to the constructor instead of this.
        static int DefaultThreadCount();
};

#endif //XSTORE_ZMQ_SERVER_H