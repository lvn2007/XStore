#ifndef XSTORE_SYSSTATS_H
#define XSTORE_SYSSTATS_H

#include <atomic>
#include <string>
#include <thread>

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
        static void Start(const std::string &host, int port);

    private:
        static void CollectorLoop(std::string host, int port);

        // Same shutdown convention as XStore::wal_stats_stop_flag /
        // StartWalWriters' atexit callback (XStore_Wal.cpp) -- joined
        // before static-storage teardown (the default spdlog logger,
        // zmq::context_t destructors) starts, so this thread never races
        // that teardown.
        static std::atomic<bool> stop_flag;
        static std::thread collector_thread;
};

#endif //XSTORE_SYSSTATS_H
