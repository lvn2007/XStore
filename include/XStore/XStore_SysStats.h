#ifndef XSTORE_SYSSTATS_H
#define XSTORE_SYSSTATS_H

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "include/Networking/XStore_Curve.h"

// Background host-metrics broadcaster: its own thread, its own ZMQ PUB
// socket, its own zmq::context_t -- deliberately decoupled from ZmqServer
// and XStore state (reads only /proc, /proc/net/dev, and statvfs on the
// data dir). Publishes one XStore_Proto::SysStats_PUB frame every
// XStoreTunables::SYS_STATS_COLLECT_INTERVAL_MS, unconditionally (PUB
// sockets silently drop with zero subscribers, which is fine -- this is a
// best-effort telemetry feed, not a durable/acked channel). Never touches
// the WAL/ZMQ RPC hot path, so it has zero effect on insert/query latency;
// its thread also runs at SCHED_IDLE so it never contends with those
// threads for a CPU core under load.
class SysStatsCollector {
    public:
        // Starts the collector thread, bound to tcp://<host>:<port>, and
        // registers an atexit callback to stop and join it. Call once from
        // main(), before the RPC server starts accepting connections, so
        // the feed is already live by the time any client connects.
        //
        // curve_identity/curve_keystore both absent (default): identical to
        // today's behavior. Both present: CURVE_SERVER mode + ZAP domain
        // "xstore" on the PUB socket, gated by its own XStoreZapHandler
        // (this collector runs on its own zmq::context_t, so it needs its
        // own ZAP handler instance -- see ZmqServer::StartServer for the
        // analogous wiring). Typically shares the same KeyStore instance
        // (and the same node identity) as ZmqServer -- one allowlist governs
        // both sockets. Passing only one of the two is a caller error
        // (thrown, not silently half-applied -- same reasoning as
        // ZmqServer::StartServer's identical check).
        static void Start(const std::string &host, int port,
                           std::optional<XStoreCurveKeyPair> curve_identity = std::nullopt,
                           std::shared_ptr<KeyStore> curve_keystore = nullptr);

    private:
        static void CollectorLoop(std::string host, int port,
                                   std::optional<XStoreCurveKeyPair> curve_identity,
                                   std::shared_ptr<KeyStore> curve_keystore);

        // Same shutdown convention as XStore::wal_stats_stop_flag /
        // StartWalWriters' atexit callback (XStore_Wal.cpp) -- joined
        // before static-storage teardown (the default spdlog logger,
        // zmq::context_t destructors) starts, so this thread never races
        // that teardown.
        static std::atomic<bool> stop_flag;
        static std::thread collector_thread;
};

#endif //XSTORE_SYSSTATS_H
