#include "include/Networking/ZMQ_Server.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_DirLock.h"
#include "include/XStore/XStore_PreadPool.h"
#include "include/XStore/XStore_SysStats.h"
#include "include/XStore/XStore_WalCheckpoint.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/Networking/XStore_Curve.h"
#include <argparse/argparse.hpp>
#include <atomic>
#include <csignal>
#include <cerrno>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <optional>
#include <pthread.h>
#include <thread>
#include <unistd.h>

namespace {
// Guards g_server against the signal thread racing main's teardown of it.
std::mutex g_server_mutex;
ZmqServer *g_server = nullptr;
// Set once main is returning: the sigwait thread must then touch nothing (g_server_mutex / spdlog are about to be
// destroyed by exit()). A REAL second signal during shutdown still _exit(1)s because this is not yet set.
std::atomic<bool> g_exiting{false};

// Declared right after the sigwait thread starts: on every way out of main it sets g_exiting, wakes the thread
// (the stop signals are blocked process-wide, so a self-sent SIGTERM is only ever consumed by its sigwait) and joins it.
class SignalThreadGuard {
public:
    explicit SignalThreadGuard(std::thread &thread) : thread_(thread) {}
    ~SignalThreadGuard() {
        g_exiting.store(true, std::memory_order_release);
        pthread_kill(thread_.native_handle(), SIGTERM);
        thread_.join();
    }
    SignalThreadGuard(const SignalThreadGuard &) = delete;
    SignalThreadGuard &operator=(const SignalThreadGuard &) = delete;

private:
    std::thread &thread_;
};
}  // namespace

int main(int argc, char *argv[]) {
    // Graceful shutdown (Durability A, DD7). SIGINT/SIGTERM are blocked here,
    // before ANY thread exists (spdlog, ZMQ, WAL writers... all inherit the
    // mask), so they are only ever consumed by the sigwait thread started
    // below -- never by a default handler that would kill the process with
    // the indexes still marked dirty. The one exception is jemalloc's
    // background thread, which may start before main(): that is safe because
    // jemalloc blocks all signals in its own threads.
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);

    spdlog::level::level_enum console_level = spdlog::level::warn;
    spdlog::level::level_enum file_level = spdlog::level::trace;
    unsigned short int flush_interval = 10;
    argparse::ArgumentParser program("XStore");
    program.add_description(
        "XStore is a key-value/time-series store, served over ZMQ+protobuf");

    program.add_argument("-H", "--host")
        .required()
        .help("host/address to bind the server to");

    program.add_argument("-p", "--port")
        .required()
        .help("port to bind the server to");

    program.add_argument("-t", "--threads")
        .default_value(0)
        .scan<'i', int>()
        .help("worker thread count (<=0 or omitted defaults to one per physical core)");

    program.add_argument("-s", "--stats-port")
        .default_value(0)
        .scan<'i', int>()
        .help("port for the host-stats PUB broadcast (<=0 or omitted defaults to --port minus 1)");

    program.add_argument("--curve")
        .default_value(false)
        .implicit_value(true)
        .help("enable ZMQ CURVE transport encryption + ZAP-based client authentication (opt-in, off by default)");

    program.add_argument("--curve-secret-key")
        .default_value(std::string("keys/server/server.key"))
        .help("path to this server's CURVE secret key file (see xstore-keys gen server); its sibling .pub file must exist alongside it");

    program.add_argument("--curve-allowed-dir")
        .default_value(std::string("keys/allowed"))
        .help("directory of allowed clients' .pub files (see xstore-keys allow); hot-reloaded, no restart needed to pick up changes");

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception &err) {
        spdlog::error("[BOOT] - {}", err.what());
        std::cerr << program;
        exit(1);
    }

    // One XStore process per data directory. Taken after parse_args (--help and bad arguments exit inside it and must
    // not touch the lock) but before the Logger, SysStats, ZMQ and the bootstrap XStore (InitDB's replay and index
    // rebuilds): a second process must not truncate the running one's logs/server.log, so it appends instead. Same
    // directory as the bootstrap XStore below (cwd/DataStore). Startup path, not hot.
    const dirlock::Result dir_lock = dirlock::Acquire(std::filesystem::current_path() / "DataStore");

    // Ctor only sets process-global spdlog state; temporary avoids -Wunused-but-set-variable.
    Logger(console_level, file_level, flush_interval, /*truncate_log=*/dir_lock.ok);
    if (!dir_lock.ok) {
        spdlog::error("[BOOT] - {}", dir_lock.error);
        spdlog::default_logger()->flush();
        exit(1);
    }
    if (!dir_lock.warning.empty()) spdlog::warn("[BOOT] - {}", dir_lock.warning);

    // Arguments parsing
    std::string host = program.get<std::string>("--host");
    std::string port = program.get<std::string>("--port");
    int thread_count = program.get<int>("--threads");
    if (thread_count <= 0) {
        // Default to one worker per PHYSICAL core (not this host's
        // logical/SMT-inclusive hardware_concurrency()) -- measured best
        // on real hardware: going past physical core count leaves no free
        // SMT sibling lane for the WAL writer threads to run on relatively
        // uncontended, ~6% worse p50 under real concurrent load in that
        // configuration (see ZmqServer::DefaultThreadCount()'s comment,
        // ZMQ_Server.cpp). Still user-overridable via -t/--threads --
        // don't assume this default is right for every deployment's own
        // hardware/workload without benchmarking it there too.
        thread_count = ZmqServer::DefaultThreadCount();
        spdlog::info("[BOOT] - No/invalid thread count given, defaulting to {} (one per physical core)", thread_count);
    }

    int stats_port = program.get<int>("--stats-port");
    if (stats_port <= 0) {
        // Default convention: one below the main port (e.g. main 9497 ->
        // stats 9496) -- keeps the two sockets adjacent without requiring
        // a second required CLI arg for the common case. Still overridable
        // via -s/--stats-port for deployments where port-1 collides with
        // something else.
        try {
            stats_port = std::stoi(port) - 1;
        } catch (const std::exception &) {
            spdlog::error("[BOOT] - --port ('{}') is not numeric, cannot derive a default --stats-port; pass -s/--stats-port explicitly", port);
            exit(1);
        }
    }

    bool curve_enabled = program.get<bool>("--curve");
    std::optional<XStoreCurveKeyPair> curve_identity;
    std::shared_ptr<KeyStore> curve_keystore;
    if (curve_enabled) {
        if (!zmq_has("curve")) {
            spdlog::error("[BOOT] - --curve was requested but this build's libzmq was not compiled with CURVE support. "
                           "Rebuild with libsodium-dev installed, or reconfigure with -DXSTORE_FETCH_ZMQ=OFF against a "
                           "system libzmq that does have CURVE support.");
            exit(1);
        }
        std::string secret_key_path = program.get<std::string>("--curve-secret-key");
        std::string allowed_dir = program.get<std::string>("--curve-allowed-dir");
        try {
            curve_identity = XStoreCurveKeyPair::LoadFromSecretKeyFile(secret_key_path);
        } catch (const std::exception &e) {
            spdlog::error("[BOOT] - Failed to load CURVE server identity from {}: {}", secret_key_path, e.what());
            exit(1);
        }
        if (!std::filesystem::exists(allowed_dir)) {
            spdlog::error("[BOOT] - --curve-allowed-dir {} does not exist. Create it (xstore-keys allow writes into "
                           "it automatically) before starting with --curve.", allowed_dir);
            exit(1);
        }
        curve_keystore = std::make_shared<FilesystemKeyStore>(
            allowed_dir, std::chrono::milliseconds(XStoreTunables::CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS));
        spdlog::info("[BOOT] - CURVE enabled: identity={}, allowed-dir={}", secret_key_path, allowed_dir);
    }

    // Tells XStorePreadPool how many threads the ZMQ server is about to
    // claim, so its own worker count sizes off whatever's left over on
    // this box instead of independently sizing off hardware_concurrency()
    // too -- must run before ZmqServer starts (any query that reaches
    // BatchQuery starts the pread pool's threads on first use, and
    // that count is fixed from then on). See XStore_Tunables.h's
    // BATCH_QUERY_PREAD_POOL_SIZE_CAP comment for the regression this
    // fixes.
    XStorePreadPool::Init(static_cast<size_t>(thread_count));

    // Constructed (not started) before the slow boot work below so that a
    // signal arriving during InitDB/WAL replay is remembered: RequestStop()
    // shuts the context down and StartServer then returns immediately.
    ZmqServer x_store_zmq_server(thread_count);
    g_server = &x_store_zmq_server;
    std::thread signal_thread([stop_signals]() {
        int sig = 0;
        sigwait(&stop_signals, &sig);
        if (g_exiting.load(std::memory_order_acquire)) return;
        spdlog::warn("[SHUTDOWN] - Received signal {} ({}); stopping the server", sig, strsignal(sig));
        {
            std::lock_guard<std::mutex> lock(g_server_mutex);
            if (g_server != nullptr) g_server->RequestStop();
        }
        // A second signal while shutting down means "now": leave the indexes
        // dirty (the next start rebuilds them) rather than wait.
        sigwait(&stop_signals, &sig);
        if (g_exiting.load(std::memory_order_acquire)) return;
        spdlog::default_logger()->flush();
        _exit(1);
    });
    SignalThreadGuard signal_thread_guard(signal_thread);

    // Host-metrics broadcast: independent background thread + its own PUB
    // socket, started before the ZMQ server so it's already publishing by
    // the time any client (desktop viewer or otherwise) connects. Never
    // touches ZmqServer/XStore state -- see XStore_SysStats.h.
    SysStatsCollector::Start(host, stats_port, curve_identity, curve_keystore);

    // Ops/testing knobs, read once here (before the WAL checkpointer starts doing work): how long the
    // WAL writers must be idle (XSTORE_CKPT_IDLE_MS) / how old the oldest unsynced data may get
    // (XSTORE_CKPT_MAX_AGE_MS) before the checkpointer runs a cycle. Unset/0/invalid = the tunable
    // defaults. A graceful stop still runs a final cycle whatever these say.
    // Digits only (ParseMsKnob), <= UINT_MAX; anything else is a WARNING and the tunable default stays.
    auto apply_ms_knob = [](const char *name, void (walring::Checkpointer::*setter)(unsigned), unsigned default_ms) {
        const char *v = std::getenv(name);
        if (v == nullptr) return;
        unsigned ms = 0;
        if (!walring::ParseMsKnob(v, ms)) {
            spdlog::warn("[CONFIG] - Ignoring {}=\"{}\": expected an unsigned decimal integer of milliseconds <= {}; keeping the default ({} ms)",
                         name, v, UINT_MAX, default_ms);
            return;
        }
        (walring::Checkpointer::Instance().*setter)(ms);
        spdlog::info("[CONFIG] - {} = {} ms effective", name, ms == 0 ? default_ms : ms);
    };
    apply_ms_knob("XSTORE_CKPT_IDLE_MS", &walring::Checkpointer::SetIdleMs, XStoreTunables::WAL_CKPT_IDLE_MS);
    apply_ms_knob("XSTORE_CKPT_MAX_AGE_MS", &walring::Checkpointer::SetMaxAgeMs, XStoreTunables::WAL_CKPT_MAX_AGE_MS);

    // XSTORE_WAL_RING_MB: size of newly created WAL rings (digits only, 8..4096 MiB; else a WARNING and the
    // 64 MiB tunable stays). Only rings created afterwards use it: an existing ring keeps the size in its header.
    if (const char *v = std::getenv("XSTORE_WAL_RING_MB"); v != nullptr) {
        unsigned mb = 0;
        if (!walring::ParseMsKnob(v, mb) || mb < 8 || mb > 4096) {
            spdlog::warn("[CONFIG] - Ignoring XSTORE_WAL_RING_MB=\"{}\": expected an integer 8..4096 (MiB); keeping the default ({} MiB)",
                         v, XStoreTunables::WAL_RING_BYTES >> 20);
        } else {
            walring::SetDefaultRingBytesForTests(static_cast<uint64_t>(mb) << 20);   // the process-wide default, despite the name
            spdlog::info("[CONFIG] - XSTORE_WAL_RING_MB = {} MiB for newly created rings", mb);
        }
    }

    // Bootstrap XStore instance: its constructor is what actually starts
    // StartWalWriters (via a process-wide once_flag) and runs InitDB for
    // data_store_dir -- neither is called directly from main.cpp elsewhere.
    // Reusing that same construction here (rather than duplicating
    // GetBaseDir()'s cwd-based path logic) gives the compaction thread the
    // authoritative data_store_dir and is a harmless no-op duplicate of the
    // work every later ZmqServer worker's own XStore instance would trigger
    // anyway (InitDB's own once-per-directory guard makes this idempotent).
    // A failed InitDB (e.g. an index file of a year cannot be opened) throws out of the constructor: say why and exit 1
    // instead of std::terminate. _exit, not return: the WAL writer threads are already running (same as the drain
    // timeout below). No retry: a second XStore() in this process would skip InitDB (initialized_dirs).
    std::optional<XStore> bootstrap_store;
    try {
        bootstrap_store.emplace();
    } catch (const std::exception &e) {
        spdlog::error("[BOOT] - Startup failed (InitDB): {}", e.what());
        spdlog::default_logger()->flush();
        _exit(1);
    } catch (...) {
        spdlog::error("[BOOT] - Startup failed (InitDB): unknown exception");
        spdlog::default_logger()->flush();
        _exit(1);
    }
    XStore::StartCatIdxCompactionThread(bootstrap_store->data_store_dir);

    // Spin ZMQ Server. Returns after a stop signal, once the workers have
    // drained (or the drain timeout hit).
    const bool drained = x_store_zmq_server.StartServer(host, port, curve_identity, curve_keystore);
    {
        std::lock_guard<std::mutex> lock(g_server_mutex);
        g_server = nullptr;
    }
    if (!drained) {
        // A worker may still be inserting: Shutdown()'s "indexes are quiescent"
        // precondition does not hold, so do NOT mark anything clean. Dirty
        // flags stay set; the next start rebuilds.
        spdlog::error("[SHUTDOWN] - Workers did not drain in time; exiting without a clean shutdown (indexes stay dirty)");
        spdlog::default_logger()->flush();
        _exit(1);
    }

    // Server quiescent: no request can mutate data from here on. The
    // compaction thread is stopped first so it cannot re-dirty an index.
    XStore::StopCatIdxCompactionThread();
    XStore::Shutdown();
    spdlog::info("[SHUTDOWN] - Clean shutdown complete");
    return 0;
}
