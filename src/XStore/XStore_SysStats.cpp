#include "include/XStore/XStore_SysStats.h"

#include <sched.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/sysmacros.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <zmq.hpp>

#include "XStore.pb.h"
#include "include/Logger/Logger.h"
#include "include/XStore/XStore_CpuJiffies.h"
#include "include/XStore/XStore_Tunables.h"

namespace {

using sysstats::ReadCpuJiffies;

// Cumulative counters read once per tick; every published field except the
// disk-space and mem ones is a DELTA between two consecutive samples, so
// the previous tick's raw reads have to survive across loop iterations.
struct RawSample {
    uint64_t cpu_active_jiffies = 0; // user+nice+system+irq+softirq+steal
    uint64_t cpu_total_jiffies  = 0;
    uint64_t net_rx_bytes       = 0;
    uint64_t net_tx_bytes       = 0;
    uint64_t disk_reads         = 0; // reads completed, /proc/diskstats field 4
    uint64_t disk_writes        = 0; // writes completed, /proc/diskstats field 8
    std::chrono::steady_clock::time_point sampled_at;
};

// Same base-dir convention as XStore::GetBaseDir() (XStore_Management.cpp):
// the binary runs with cwd == the directory it lives in, DataStore/ is a
// direct child. This collector is deliberately decoupled from any XStore
// instance (no XStore object may even exist yet when it starts), so the
// convention is re-derived here rather than reused via a live instance.
std::string DataStoreDir() {
    return (std::filesystem::current_path() / "DataStore").string();
}

// Major:minor of the block device backing the data dir, used to pick the
// matching line out of /proc/diskstats. Resolved once at startup -- the
// backing device doesn't change while the server is running. Falls back to
// {0,0} (matches nothing in /proc/diskstats, so read/write IOPS just stay
// 0) if the dir doesn't exist yet or stat() fails for any other reason.
std::pair<unsigned, unsigned> DataDirDevice() {
    std::string dir = DataStoreDir();
    struct stat st{};
    if (stat(dir.c_str(), &st) != 0) {
        // DataStore/ may not have been created yet (no DB created so far)
        // -- fall back to the base dir itself, which always exists.
        if (stat(std::filesystem::current_path().string().c_str(), &st) != 0) {
            return {0, 0};
        }
    }
    return {major(st.st_dev), minor(st.st_dev)};
}

// Which block device(s) the disk-iops gauge tracks. The data dir's own
// device number may match NOTHING in /proc/diskstats -- container,
// virtiofs (e.g. Docker Desktop bind mounts), overlay, NFS and tmpfs mounts
// all report pseudo device numbers that no block device line carries.
// Silently publishing 0/0 in that case is useless, so the collector falls
// back to the host-wide sum of every whole disk and says so at startup.
struct DiskTarget {
    bool hostwide = true; // fallback unless the device is positively matched
    unsigned major_num = 0;
    unsigned minor_num = 0;
    std::string device_name; // /proc/diskstats name of the data dir's device
};

DiskTarget ResolveDiskTarget(unsigned major_num, unsigned minor_num) {
    DiskTarget target;
    target.major_num = major_num;
    target.minor_num = minor_num;
    if (major_num == 0 && minor_num == 0) return target; // hostwide fallback
    std::ifstream f("/proc/diskstats");
    if (!f.is_open()) return target; // no /proc at all -- hostwide path no-ops
    unsigned maj, min;
    std::string name;
    while (f >> maj >> min >> name) {
        if (maj == major_num && min == minor_num) {
            target.hostwide = false;
            target.device_name = name;
            return target;
        }
        std::string rest;
        std::getline(f, rest);
    }
    return target; // data dir's device not listed -> hostwide fallback
}

// A partition's name extends its whole disk's name with a trailing number
// ("sda" -> "sda1", "nvme0n1" -> "nvme0n1p1", "mmcblk0" -> "mmcblk0p1"), so
// summing whole disks only counts each block of IO exactly once.
bool IsDiskPartition(const std::string &n, const std::set<std::string> &all_names) {
    const auto all_digits = [](const std::string &s) {
        return !s.empty() &&
               std::all_of(s.begin(), s.end(), [](char c) { return c >= '0' && c <= '9'; });
    };
    for (size_t cut = n.size(); cut > 0; --cut) { // n == M + digits
        if (all_digits(n.substr(cut)) && all_names.count(n.substr(0, cut))) return true;
    }
    for (size_t p = n.find('p'); p != std::string::npos; p = n.find('p', p + 1)) { // n == M + "p" + digits
        if (all_digits(n.substr(p + 1)) && all_names.count(n.substr(0, p))) return true;
    }
    return false;
}

void ReadMemBytes(uint64_t &mem_total, uint64_t &mem_used) {
    mem_total = 0;
    mem_used = 0;
    uint64_t mem_available_kb = 0;
    uint64_t mem_total_kb = 0;
    std::ifstream f("/proc/meminfo");
    if (!f.is_open()) return;
    std::string line;
    while (std::getline(f, line)) {
        uint64_t value_kb = 0;
        if (sscanf(line.c_str(), "MemTotal: %lu kB", &value_kb) == 1) {
            mem_total_kb = value_kb;
        } else if (sscanf(line.c_str(), "MemAvailable: %lu kB", &value_kb) == 1) {
            mem_available_kb = value_kb;
        }
    }
    mem_total = mem_total_kb * 1024;
    mem_used = mem_total_kb >= mem_available_kb ? (mem_total_kb - mem_available_kb) * 1024 : 0;
}

// Sums rx/tx byte counters across every non-loopback interface in
// /proc/net/dev. Good enough for a "how busy is this host's network"
// gauge; doesn't try to single out the interface actually carrying ZMQ
// traffic, since XStore doesn't pin itself to one.
void ReadNetBytes(uint64_t &rx_bytes, uint64_t &tx_bytes) {
    rx_bytes = 0;
    tx_bytes = 0;
    std::ifstream f("/proc/net/dev");
    if (!f.is_open()) return;
    std::string line;
    std::getline(f, line); // header line 1
    std::getline(f, line); // header line 2
    while (std::getline(f, line)) {
        auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string iface = line.substr(0, colon);
        iface.erase(0, iface.find_first_not_of(' '));
        if (iface == "lo") continue;
        std::istringstream iss(line.substr(colon + 1));
        uint64_t r_bytes, r_packets, r_errs, r_drop, r_fifo, r_frame, r_compressed, r_multicast;
        uint64_t t_bytes;
        iss >> r_bytes >> r_packets >> r_errs >> r_drop >> r_fifo >> r_frame >> r_compressed >> r_multicast >> t_bytes;
        if (iss.fail()) continue;
        rx_bytes += r_bytes;
        tx_bytes += t_bytes;
    }
}

void ReadDiskCounters(const DiskTarget &target, uint64_t &reads, uint64_t &writes) {
    reads = 0;
    writes = 0;
    std::ifstream f("/proc/diskstats");
    if (!f.is_open()) return;
    if (target.hostwide) {
        struct Line {
            std::string name;
            uint64_t r, w;
        };
        std::vector<Line> lines;
        std::set<std::string> names;
        unsigned maj, min;
        std::string name;
        while (f >> maj >> min >> name) {
            uint64_t r, r_merged, sectors_r, time_r, w, w_merged, sectors_w, time_w;
            if (!(f >> r >> r_merged >> sectors_r >> time_r >> w >> w_merged >> sectors_w >> time_w)) break;
            names.insert(name);
            lines.push_back({name, r, w});
            std::string rest;
            std::getline(f, rest);
        }
        for (const auto &L : lines) {
            if (L.name.rfind("zram", 0) == 0) continue; // RAM-backed, not disk IO
            if (IsDiskPartition(L.name, names)) continue;
            reads += L.r;
            writes += L.w;
        }
        return;
    }
    unsigned maj, min;
    std::string name;
    while (f >> maj >> min >> name) {
        uint64_t reads_completed, reads_merged, sectors_read, time_reading;
        uint64_t writes_completed, writes_merged, sectors_written, time_writing;
        f >> reads_completed >> reads_merged >> sectors_read >> time_reading
          >> writes_completed >> writes_merged >> sectors_written >> time_writing;
        if (f.fail()) break;
        if (maj == target.major_num && min == target.minor_num) {
            reads = reads_completed;
            writes = writes_completed;
            return;
        }
        // Rest of the line (queue depth, IO time, weighted time, discards on
        // newer kernels) is irrelevant here -- skip to the next line.
        std::string rest;
        std::getline(f, rest);
    }
}

void ReadDiskSpace(uint64_t &free_bytes, uint64_t &total_bytes) {
    free_bytes = 0;
    total_bytes = 0;
    struct statvfs vfs{};
    std::string dir = DataStoreDir();
    if (statvfs(dir.c_str(), &vfs) != 0) {
        dir = std::filesystem::current_path().string();
        if (statvfs(dir.c_str(), &vfs) != 0) return;
    }
    total_bytes = static_cast<uint64_t>(vfs.f_blocks) * vfs.f_frsize;
    free_bytes = static_cast<uint64_t>(vfs.f_bavail) * vfs.f_frsize; // avail-to-unprivileged, not raw f_bfree
}

} // namespace

std::atomic<bool> SysStatsCollector::stop_flag{false};
std::thread SysStatsCollector::collector_thread;

void SysStatsCollector::Start(const std::string &host, int port,
                               std::optional<XStoreCurveKeyPair> curve_identity,
                               std::shared_ptr<KeyStore> curve_keystore) {
    // Same reasoning as ZmqServer::StartServer's identical check (see its
    // comment, ZMQ_Server.cpp): a deliberate runtime throw, not assert(), so
    // a caller passing only one of the two params fails loudly instead of
    // silently falling through to an unauthenticated/unencrypted PUB socket
    // where an authenticated/encrypted one was expected.
    if (curve_identity.has_value() != (curve_keystore != nullptr)) {
        throw std::invalid_argument(
            "SysStatsCollector::Start: curve_identity and curve_keystore must both be set, or both absent");
    }

    // Force spdlog's default-logger registry to exist BEFORE the thread
    // below is spawned and the atexit callback that joins it is registered
    // -- mirrors the identical fix in XStore_Wal.cpp's StartWalWriters (see
    // its comment for the full [basic.start.term] reasoning). Without this,
    // the registry singleton would instead be lazily constructed by this
    // collector thread's own first spdlog:: call, sometimes after the
    // atexit callback below is already registered, which sequences the
    // registry's destructor to run BEFORE this callback's join() --
    // exploitable by CollectorLoop still calling spdlog::warn/error/info at
    // shutdown. Currently safe only by the same production accident Task 9
    // documented (src/main.cpp constructs a Logger before any of this
    // runs); this line makes the safe ordering hold unconditionally.
    static_cast<void>(spdlog::default_logger());

    // SysStatsCollector is a process-lifetime singleton by design -- one
    // atexit-joined static thread (collector_thread's declaration comment)
    // -- and production code (main.cpp) calls Start() exactly once. Tests
    // are the first callers to invoke Start() more than once within the
    // same process (SysStatsCurveTest, XStoreCurveTest.cpp, both hit by one
    // `--gtest_filter=SysStatsCurveTest.*` invocation): naively
    // move-assigning a new std::thread over a still-joinable
    // collector_thread is undefined behavior and reliably crashes via
    // std::terminate() per [thread.thread.assign] (reproduced during Task 6
    // verification). Stopping and joining any already-running collector
    // first makes repeated Start() calls safe without changing the
    // single-call production path at all -- collector_thread starts
    // non-joinable there, so this branch is always a no-op in that case.
    if (collector_thread.joinable()) {
        stop_flag.store(true, std::memory_order_relaxed);
        collector_thread.join();
        stop_flag.store(false, std::memory_order_relaxed);
    }

    collector_thread = std::thread(&SysStatsCollector::CollectorLoop, host, port, curve_identity, curve_keystore);
    // See stop_flag's declaration comment (XStore_SysStats.h) -- registered
    // here, at the moment the thread is actually started, so std::atexit's
    // reverse-registration-order guarantee runs this before any
    // static-storage object this thread touches (default spdlog logger)
    // begins its own teardown.
    std::atexit([]() {
        SysStatsCollector::stop_flag.store(true, std::memory_order_relaxed);
        if (SysStatsCollector::collector_thread.joinable()) {
            SysStatsCollector::collector_thread.join();
        }
    });
}

void SysStatsCollector::CollectorLoop(std::string host, int port,
                                       std::optional<XStoreCurveKeyPair> curve_identity,
                                       std::shared_ptr<KeyStore> curve_keystore) {
    // Best-effort: SCHED_IDLE needs no special privilege (unlike SCHED_FIFO/
    // RR) and guarantees this thread only runs when nothing else on the
    // host wants the core, so a busy WAL/ZMQ worker never loses a timeslice
    // to a stats sample. Failure just leaves it at the default scheduling
    // class -- not worth aborting startup over.
    sched_param sch{};
    sch.sched_priority = 0;
    if (sched_setscheduler(0, SCHED_IDLE, &sch) != 0) {
        spdlog::warn("[SYSSTATS] - Failed to set SCHED_IDLE, continuing at default scheduling priority");
    }

    zmq::context_t ctx(1);
    zmq::socket_t pub_sock(ctx, zmq::socket_type::pub);
    // Keep at most one queued frame -- a slow/stalled subscriber (or none
    // at all) must never make this socket buffer up a backlog of stale
    // snapshots; the next tick's frame always supersedes the last anyway.
    pub_sock.set(zmq::sockopt::conflate, 1);

    std::unique_ptr<XStoreZapHandler> zap_handler;
    if (curve_identity && curve_keystore) {
        // ZAP must be bound before this socket binds/accepts any handshake --
        // Start() blocks briefly to guarantee that ordering (see its header).
        zap_handler = std::make_unique<XStoreZapHandler>(ctx, curve_keystore);
        zap_handler->Start();

        pub_sock.set(zmq::sockopt::curve_server, true);
        pub_sock.set(zmq::sockopt::curve_secretkey, curve_identity->secret_key_z85);
        pub_sock.set(zmq::sockopt::zap_domain, "xstore");
        spdlog::info("[SYSSTATS] - CURVE enabled on stats PUB socket");
    }

    std::string bind_addr = "tcp://" + host + ":" + std::to_string(port);
    try {
        pub_sock.bind(bind_addr);
    } catch (const zmq::error_t &e) {
        spdlog::error("[SYSSTATS] - Failed to bind stats PUB socket at {}: {}", bind_addr, e.what());
        return;
    }
    spdlog::info("[SYSSTATS] - Broadcasting host stats on {} every {}ms", bind_addr, XStoreTunables::SYS_STATS_COLLECT_INTERVAL_MS);

    auto [dev_major, dev_minor] = DataDirDevice();
    const DiskTarget disk_target = ResolveDiskTarget(dev_major, dev_minor);
    // Logged once at startup so a 0/0 iops reading is diagnosable without
    // re-deriving the mount layout: this line says exactly which device
    // the gauge tracks (or that it fell back to host-wide).
    if (disk_target.hostwide) {
        spdlog::warn("[SYSSTATS] - DataStore dir device {}:{} not listed in /proc/diskstats "
                     "(container/virtiofs/overlay/NFS/tmpfs mount?) -- disk iops will be the "
                     "HOST-WIDE sum of all whole disks, not the data dir's own device",
                     dev_major, dev_minor);
    } else {
        spdlog::info("[SYSSTATS] - DataStore dir backed by /proc/diskstats device {} ({}:{}) -- "
                     "disk iops track that block device",
                     disk_target.device_name, dev_major, dev_minor);
    }

    RawSample prev{};
    ReadCpuJiffies(prev.cpu_active_jiffies, prev.cpu_total_jiffies);
    ReadNetBytes(prev.net_rx_bytes, prev.net_tx_bytes);
    ReadDiskCounters(disk_target, prev.disk_reads, prev.disk_writes);
    prev.sampled_at = std::chrono::steady_clock::now();

    while (!stop_flag.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(XStoreTunables::SYS_STATS_COLLECT_INTERVAL_MS));
        if (stop_flag.load(std::memory_order_relaxed)) break;

        RawSample curr{};
        ReadCpuJiffies(curr.cpu_active_jiffies, curr.cpu_total_jiffies);
        ReadNetBytes(curr.net_rx_bytes, curr.net_tx_bytes);
        ReadDiskCounters(disk_target, curr.disk_reads, curr.disk_writes);
        curr.sampled_at = std::chrono::steady_clock::now();

        double elapsed_s = std::chrono::duration<double>(curr.sampled_at - prev.sampled_at).count();
        if (elapsed_s <= 0) elapsed_s = 1.0; // guard against a stalled clock, avoid a div-by-zero

        XStore_Proto::SysStats_PUB stats;

        uint64_t d_active = curr.cpu_active_jiffies - prev.cpu_active_jiffies;
        uint64_t d_total = curr.cpu_total_jiffies - prev.cpu_total_jiffies;
        stats.set_cpuutilpercent(d_total > 0 ? (100.0 * static_cast<double>(d_active) / static_cast<double>(d_total)) : 0.0);

        uint64_t mem_total, mem_used;
        ReadMemBytes(mem_total, mem_used);
        stats.set_memtotalbytes(mem_total);
        stats.set_memusedbytes(mem_used);

        double d_rx_kb = static_cast<double>(curr.net_rx_bytes - prev.net_rx_bytes) / 1024.0;
        double d_tx_kb = static_cast<double>(curr.net_tx_bytes - prev.net_tx_bytes) / 1024.0;
        stats.set_netrxkbps(d_rx_kb / elapsed_s);
        stats.set_nettxkbps(d_tx_kb / elapsed_s);

        stats.set_diskreadiops(static_cast<uint64_t>(static_cast<double>(curr.disk_reads - prev.disk_reads) / elapsed_s));
        stats.set_diskwriteiops(static_cast<uint64_t>(static_cast<double>(curr.disk_writes - prev.disk_writes) / elapsed_s));

        uint64_t disk_free, disk_total;
        ReadDiskSpace(disk_free, disk_total);
        stats.set_diskfreebytes(disk_free);
        stats.set_disktotalbytes(disk_total);

        uint64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        stats.set_snapshotunixms(now_ms);

        std::string serialized = stats.SerializeAsString();
        try {
            pub_sock.send(zmq::buffer(serialized), zmq::send_flags::dontwait);
        } catch (const zmq::error_t &e) {
            spdlog::warn("[SYSSTATS] - Failed to publish stats frame: {}", e.what());
        }

        prev = curr;
    }
}
