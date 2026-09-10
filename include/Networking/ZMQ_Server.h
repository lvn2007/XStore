#ifndef XSTORE_ZMQ_SERVER_H
#define XSTORE_ZMQ_SERVER_H

#include <include/XStore/XStore.h>
#include <memory>
#include <optional>
#include <thread>
#include <zmq.hpp>
#include "XStore.pb.h"
#include <zmq_addon.hpp>
#include "include/Logger/Logger.h"
#include "include/Networking/XStore_Curve.h"

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

        // curve_identity/curve_keystore both absent (default): identical to
        // today's behavior, NULL mechanism, no ZAP handler spawned. Both
        // present: CURVE_SERVER mode on the external ROUTER socket, ZAP
        // domain "xstore" set so ZAP is actually invoked, and an
        // XStoreZapHandler is started (bound to this instance's ctx) before
        // the socket binds. Passing only one of the two is a caller error
        // (asserted, not silently half-applied).
        void StartServer(std::string listen_addr, std::string listen_port,
                          std::optional<XStoreCurveKeyPair> curve_identity = std::nullopt,
                          std::shared_ptr<KeyStore> curve_keystore = nullptr);

        // Physical (not logical/SMT) CPU core count on this host, with a
        // graceful fallback -- see the .cpp definition for why this, not
        // std::thread::hardware_concurrency(), is the right default worker
        // count for production. Callers needing a SPECIFIC count for other
        // reasons (tests wanting a small, deterministic pool) should keep
        // passing an explicit value to the constructor instead of this.
        static int DefaultThreadCount();
};

#endif //XSTORE_ZMQ_SERVER_H