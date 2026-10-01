#ifndef XSTORE_ZMQ_SERVER_H
#define XSTORE_ZMQ_SERVER_H

#include <include/XStore/XStore.h>
#include <condition_variable>
#include <memory>
#include <mutex>
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

        // Worker threads are detached, so shutdown waits on this counter
        // instead of joining. shared_ptr: a worker's final decrement/notify
        // can run after this ZmqServer is destroyed.
        struct WorkerLatch {
            std::mutex mutex;
            std::condition_variable cv;
            int running = 0;
        };
        std::shared_ptr<WorkerLatch> latch_ = std::make_shared<WorkerLatch>();

        // Work routine run by each worker thread: counts the worker out of
        // latch_ on every exit path and swallows ETERM (server stopping).
        void WorkerRoutine(std::string server_addr);
        // The worker's request loop; throws zmq::error_t(ETERM) on shutdown.
        void WorkerLoop(const std::string &server_addr);
        // Sockets + proxy; throws zmq::error_t(ETERM) once RequestStop() ran.
        void Serve(const std::string &listen_addr, const std::string &listen_port,
                   const std::optional<XStoreCurveKeyPair> &curve_identity,
                   const std::shared_ptr<KeyStore> &curve_keystore);

    public:
        ZmqServer(int num_threads);

        // curve_identity/curve_keystore both absent (default): identical to
        // today's behavior, NULL mechanism, no ZAP handler spawned. Both
        // present: CURVE_SERVER mode on the external ROUTER socket, ZAP
        // domain "xstore" set so ZAP is actually invoked, and an
        // XStoreZapHandler is started (bound to this instance's ctx) before
        // the socket binds. Passing only one of the two is a caller error
        // (asserted, not silently half-applied).
        //
        // Blocks serving requests until RequestStop() is called, then waits up
        // to XStoreTunables::ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S for every worker to
        // finish its in-flight request and exit. Returns true if all workers
        // exited (the server is quiescent: safe to XStore::Shutdown()), false
        // on timeout (a worker may still be mutating data: do not Shutdown).
        bool StartServer(std::string listen_addr, std::string listen_port,
                          std::optional<XStoreCurveKeyPair> curve_identity = std::nullopt,
                          std::shared_ptr<KeyStore> curve_keystore = nullptr);

        // Thread-safe, idempotent, and valid at any time (even before
        // StartServer): shuts down the ZMQ context, so the proxy and every
        // worker's blocking recv fail with ETERM and StartServer winds down.
        // In-flight requests still execute, but their replies can no longer
        // be sent (a SYNC-acked insert may be durable yet unacknowledged).
        void RequestStop();

        // Physical (not logical/SMT) CPU core count on this host, with a
        // graceful fallback -- see the .cpp definition for why this, not
        // std::thread::hardware_concurrency(), is the right default worker
        // count for production. Callers needing a SPECIFIC count for other
        // reasons (tests wanting a small, deterministic pool) should keep
        // passing an explicit value to the constructor instead of this.
        static int DefaultThreadCount();
};

#endif //XSTORE_ZMQ_SERVER_H