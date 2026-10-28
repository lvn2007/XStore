#include "include/Networking/ZMQ_Server.h"
#include "include/Networking/ZMQ_Broker.h"
#include <google/protobuf/arena.h>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <fstream>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

// Physical core count from /proc/cpuinfo (Linux-only, like the rest of this project: pwritev, posix_fallocate) by counting
// distinct (physical id, core id) pairs; hardware_concurrency() only reports the logical CPU count. Used to charge the
// BatchQuery pread pool (ZmqServer::PreadPoolReserved), not to size the worker pool: see DefaultThreadCount.
int ReadPhysicalCoreCount() {
    std::ifstream cpuinfo("/proc/cpuinfo");
    if (!cpuinfo.is_open()) return 0;

    std::set<std::pair<int, int>> cores; // (physical_id, core_id)
    int physical_id = -1;
    std::string line;
    while (std::getline(cpuinfo, line)) {
        auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = line.substr(0, colon);
        std::string value = line.substr(colon + 1);
        // Trim trailing whitespace off the key (fields are "name\t: value").
        while (!key.empty() && (key.back() == ' ' || key.back() == '\t')) key.pop_back();

        if (key == "physical id") {
            try { physical_id = std::stoi(value); } catch (...) { physical_id = -1; }
        } else if (key == "core id" && physical_id >= 0) {
            try {
                cores.insert({physical_id, std::stoi(value)});
            } catch (...) {
                // Malformed line -- skip rather than crash boot over a
                // parsing edge case; falls through to the hardware_
                // concurrency fallback below if this leaves the set empty.
            }
        }
    }
    return static_cast<int>(cores.size());
}

}  // namespace

int ZmqServer::PhysicalCoreCount() {
    const int physical = ReadPhysicalCoreCount();
    if (physical > 0) return physical;
    // /proc/cpuinfo unavailable or unparseable (non-Linux, containerized/restricted environment): assume SMT2.
    const unsigned logical = std::thread::hardware_concurrency();
    return logical >= 2 ? static_cast<int>(logical / 2) : 1;
}

// One worker per LOGICAL core. A SYNC insert holds its worker for the whole fsync, so this is also the number of SYNC
// calls in flight, and group commit batches more records per fsync the more there are. Measured on Mystic (16 physical
// cores, 32 logical; real server alone, 8 client nodes, rotated arms with a baseline-vs-baseline control), 32 workers
// against 16: 20-row SYNC inserts 1.45x (32 client threads) to 1.64x (128), inserts next to light range queries 2.0x
// inserts / 2.0x queries (query p99 0.63x), next to CPU-saturating queries 1.18x / 1.24x (p99 0.85x); bulk 1000-row
// SYNC, queries alone and reads next to one writer unchanged within noise. 64 workers (past the logical count) cost
// inserts ~9% when the cores were saturated, so this stops at the logical count. An earlier measurement (before the
// ready-based broker) found 32 workers ~6% worse at p50 under concurrent load because the WAL writers lost their free
// SMT siblings; the numbers above supersede it for the workloads tested, but it is a reason to check other hardware.
// Still overridable with -t/--threads.
int ZmqServer::DefaultThreadCount() {
    const unsigned logical = std::thread::hardware_concurrency();
    return logical > 0 ? static_cast<int>(logical) : 1;
}

size_t ZmqServer::HeavyRequestCap(int workers) {
    if (workers < 1) return 1;
    const size_t total = static_cast<size_t>(workers);
    const size_t reserved = total * XStoreTunables::ZMQ_HEAVY_REQUEST_RESERVED_WORKERS_PERCENT / 100;
    return std::max<size_t>(1, total - reserved);
}

size_t ZmqServer::PreadPoolReserved(int workers, int physical_cores) {
    return static_cast<size_t>(std::max(0, std::min(workers, physical_cores)));
}

namespace {
// Every reply to one request leaves as ONE multipart message [client_id][empty]
// [frame...] on the worker's DEALER socket: the broker counts the worker idle
// again only after the reply's last frame, so a request must get exactly one
// complete reply on every path, including after a handler threw mid-stream.
// dontwait: a connected inproc DEALER with HWM 0 never blocks on send.
class ReplyWriter {
    public:
        ReplyWriter(zmq::socket_t &socket, zmq::message_t &client_id) : socket(socket), client_id(client_id) {}

        // A frame after the reply's last one is dropped: sent bare it would
        // reach the broker as a second reply and queue this worker twice.
        void Send(zmq::message_t &frame, bool last) {
            if (state == State::DONE) return;
            Open();
            socket.send(frame, Flags(last));
            state = last ? State::DONE : State::OPEN;
        }

        void Send(zmq::const_buffer frame, bool last) {
            if (state == State::DONE) return;
            Open();
            socket.send(frame, Flags(last));
            state = last ? State::DONE : State::OPEN;
        }

        // Completes the reply with an empty last frame unless it is already
        // complete: a single empty frame if nothing was sent yet, or the
        // terminator of a streamed reply cut short (same frames a REP socket
        // used to send in that case).
        void Finish() {
            if (state == State::DONE) return;
            zmq::message_t empty;
            Send(empty, true);
        }

    private:
        enum class State { NONE, OPEN, DONE };

        static zmq::send_flags Flags(bool last) {
            return last ? zmq::send_flags::dontwait : (zmq::send_flags::dontwait | zmq::send_flags::sndmore);
        }

        void Open() {
            if (state != State::NONE) return;
            socket.send(client_id, Flags(false));
            socket.send(zmq::message_t(), Flags(false));
            state = State::OPEN;
        }

        zmq::socket_t &socket;
        zmq::message_t &client_id;
        State state = State::NONE;
};

// Shared by every case below: serialize a response proto and dispatch it to
// the CLIENT as the reply's only frame.
//
// SerializeWithCachedSizesToArray(), not SerializeToArray(): per
// message_lite.h, SerializeToArray() is "just a simple wrapper around
// ByteSize() and SerializeWithCachedSizes()" -- it recomputes the size
// itself before writing, redundant with the ByteSizeLong() call directly
// above it (which already walked the tree once and cached every
// submessage's size). SerializeWithCachedSizesToArray() reuses those cached
// sizes instead of recomputing them, cutting SendProto from 2 size-compute
// passes + 1 write pass down to 1 size-compute pass + 1 write pass -- safe
// as long as msg isn't mutated between the two calls, which it isn't here.
template <typename T>
void SendProto(ReplyWriter &writer, const T &msg) {
    size_t size = msg.ByteSizeLong();
    zmq::message_t reply(size);
    msg.SerializeWithCachedSizesToArray(static_cast<uint8_t*>(reply.data()));
    writer.Send(reply, true);
}
}  // namespace

ZmqServer::ZmqServer(int num_threads) {
    this->num_threads = num_threads;
}

void ZmqServer::RequestStop() {
    ctx.shutdown();
}

bool ZmqServer::StartServer(std::string listen_addr, std::string listen_port,
                             std::optional<XStoreCurveKeyPair> curve_identity,
                             std::shared_ptr<KeyStore> curve_keystore) {
    try {
        Serve(listen_addr, listen_port, curve_identity, curve_keystore);
    } catch (const zmq::error_t &e) {
        if (e.num() != ETERM) throw;
    }
    // Sockets are closed by now. Workers see ETERM on their next recv (or on
    // the send of the reply they were computing) and leave.
    spdlog::info("[NETWORKING] - Stopping: waiting up to {}s for {} worker(s) to finish in-flight requests",
                 XStoreTunables::ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S, num_threads);
    std::unique_lock<std::mutex> lock(latch_->mutex);
    const bool drained = latch_->cv.wait_for(
        lock, std::chrono::seconds(XStoreTunables::ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S),
        [&] { return latch_->running == 0; });
    if (!drained) {
        spdlog::error("[NETWORKING] - {} worker(s) still busy after {}s; not safe to shut the store down",
                      latch_->running, XStoreTunables::ZMQ_SHUTDOWN_DRAIN_TIMEOUT_S);
    }
    return drained;
}

void ZmqServer::Serve(const std::string &listen_addr, const std::string &listen_port,
                      const std::optional<XStoreCurveKeyPair> &curve_identity,
                      const std::shared_ptr<KeyStore> &curve_keystore) {
    // Deliberately a runtime throw, not assert(): this project's Release
    // build defines NDEBUG (see CMakeLists.txt), which compiles an assert()
    // out, and that would make the failure mode for a caller
    // bug a silent one -- passing only one of the two params would fall
    // through to the plain NULL-mechanism bind below with no CURVE, no ZAP,
    // and nothing logged about it, i.e. an unauthenticated/unencrypted
    // socket where an authenticated/encrypted one was expected. Throwing
    // unconditionally, before any socket setup, fails loudly regardless of
    // build config instead of risking that silent security downgrade.
    if (curve_identity.has_value() != (curve_keystore != nullptr)) {
        throw std::invalid_argument(
            "ZmqServer::StartServer: curve_identity and curve_keystore must both be set, or both absent");
    }

    //  Socket to talk to CLIENTS
    zmq::socket_t client_socket(ctx, zmq::socket_type::router);
    std::string server_addr = "tcp://" + listen_addr + ":" + listen_port;

    // No queue depth cap -- 0 means "no limit" (bounded only by available
    // memory) rather than the library default of 1000 messages/socket.
    // Chosen deliberately: the goal here is maximum accepted throughput, not
    // bounded-memory backpressure: under a burst that outpaces the worker
    // pool, this queues in RAM instead of blocking/dropping at the socket.
    client_socket.set(zmq::sockopt::sndhwm, 0);
    client_socket.set(zmq::sockopt::rcvhwm, 0);
    // Bounded linger: after RequestStop() the context destructor blocks until
    // queued outgoing messages are delivered or this expires (default: forever,
    // e.g. a client that stopped reading).
    client_socket.set(zmq::sockopt::linger, 1000);

    std::unique_ptr<XStoreZapHandler> zap_handler;
    if (curve_identity && curve_keystore) {
        // ZAP must be bound before this socket binds/accepts any handshake --
        // Start() blocks briefly to guarantee that ordering (see its header).
        zap_handler = std::make_unique<XStoreZapHandler>(ctx, curve_keystore);
        zap_handler->Start();

        client_socket.set(zmq::sockopt::curve_server, true);
        client_socket.set(zmq::sockopt::curve_secretkey, curve_identity->secret_key_z85);
        // Non-empty ZAP domain is what actually makes libzmq invoke ZAP at
        // all for this socket (RFC 27) -- without it, CURVE still encrypts
        // but any client holding a valid keypair would connect regardless
        // of the allowlist.
        client_socket.set(zmq::sockopt::zap_domain, "xstore");
        spdlog::info("[NETWORKING] - CURVE enabled on client-facing socket");
    }

    // Set listenAddr & port internally
    this->server_addr = server_addr;

    client_socket.bind(server_addr);
    spdlog::info("[NETWORKING] - Listening on: {}", server_addr);

    //  Socket to talk to WORKERS (DEALERs, addressed by routing id)
    zmq::socket_t worker_socket(ctx, zmq::socket_type::router);
    worker_socket.set(zmq::sockopt::sndhwm, 0);
    worker_socket.set(zmq::sockopt::rcvhwm, 0);
    worker_socket.set(zmq::sockopt::linger, 0);
    worker_socket.bind("inproc://workers");

    //  Launch pool of WORKER threads
    {
        std::lock_guard<std::mutex> lock(latch_->mutex);
        latch_->running += num_threads;
    }
    for (int i = 0; i < num_threads; i++) {
        std::thread worker(&ZmqServer::WorkerRoutine, this, server_addr.substr(6), i);
        worker.detach();
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", num_threads);

    // Dispatches each request only to an idle worker (zmq::proxy's DEALER
    // round-robin queued requests behind a busy worker). Spins like the
    // workers after activity; a 0 tunable means it always blocks in poll.
    RunReadyBroker(client_socket, worker_socket, static_cast<size_t>(num_threads),
                   std::chrono::microseconds(XStoreTunables::ZMQ_ADAPTIVE_POLL_WINDOW_US),
                   HeavyRequestLimit{XStoreTunables::ZMQ_HEAVY_REQUEST_BYTES, HeavyRequestCap(num_threads)});
}

void ZmqServer::WorkerRoutine(std::string server_addr, int worker_index) {
    // Declared first => destroyed last: the count drops only after every
    // worker-owned socket/XStore below is gone, so StartServer's return (and
    // the context teardown after it) never races a live worker socket.
    struct ExitGuard {
        std::shared_ptr<WorkerLatch> latch;
        ~ExitGuard() {
            std::lock_guard<std::mutex> lock(latch->mutex);
            --latch->running;
            latch->cv.notify_all();
        }
    } guard{latch_};
    try {
        WorkerLoop(server_addr, worker_index);
    } catch (const zmq::error_t &e) {
        if (e.num() != ETERM) throw;  // anything else is a real bug: keep the old terminate-loudly behavior
    }
}

void ZmqServer::WorkerLoop(const std::string &server_addr, int worker_index) {
    // Instantiate XStore for each thread
    XStore my_store;

    // Each thread shares the server's ZMQ context. No queue-depth cap, same
    // reasoning as ClientSocket/WorkerSocket in StartServer.
    zmq::socket_t socket(ctx, zmq::socket_type::dealer);
    socket.set(zmq::sockopt::sndhwm, 0);
    socket.set(zmq::sockopt::rcvhwm, 0);
    socket.set(zmq::sockopt::linger, 0);
    // The broker's handle for this worker; a leading 0 byte is reserved for
    // ROUTER-generated ids.
    socket.set(zmq::sockopt::routing_id, "w" + std::to_string(worker_index));
    socket.connect("inproc://workers");
    socket.send(zmq::message_t(), zmq::send_flags::none);  // READY

    // One arena per worker thread, reused for every request via Reset()
    // rather than constructed fresh each time. Reset() keeps the arena's
    // already-allocated blocks around for the next request instead of
    // returning them to the system allocator, so steady-state traffic
    // doesn't pay for the underlying block list's malloc/free churn either
    // -- only the (much cheaper) bump-pointer allocations within it.
    // Covers BOTH the incoming Request (RangeInsert_REQ/BatchInsert_REQ
    // carry the same repeated-RowData-with-FData-columns nesting on the
    // parse side that RangeQuery/BatchQuery/UnaryQuery pay on the response
    // side) and the three response types with nested repeated fields.
    google::protobuf::Arena arena;

    // Reused across iterations (this function IS the per-thread loop, so a
    // plain local -- no thread_local needed) instead of fresh message_t
    // objects every request. A request is [client_id][empty][body]: the
    // client (XStore_Client.h) is a REQ socket that always sends a single
    // body frame. Frames past the body land in extra_frame and make the
    // request malformed.
    zmq::message_t client_id;
    zmq::message_t delimiter;
    zmq::message_t client_msg;
    zmq::message_t extra_frame;

    // Same adaptive-poll idea as the WAL writer's own queue wait (see
    // ZMQ_ADAPTIVE_POLL_WINDOW_US, XStore_Tunables.h): epoch (not "now") so
    // the first iteration always takes the normal blocking path.
    auto last_activity = std::chrono::steady_clock::time_point{};

    while (true) {
        // RECEIVING
        //  Wait for next request from CLIENT
        if constexpr (XStoreTunables::ZMQ_ADAPTIVE_POLL_WINDOW_US > 0) {
            auto window = std::chrono::microseconds(XStoreTunables::ZMQ_ADAPTIVE_POLL_WINDOW_US);
            if (std::chrono::steady_clock::now() - last_activity < window) {
                // Recently active: tight non-blocking spin, bypassing the
                // socket's own blocking recv entirely, so a request
                // arriving during this window is picked up without paying
                // an OS park/wake. Self-terminates the moment the window
                // lapses with nothing arriving -- falls through to the
                // blocking path on the next outer-loop iteration.
                bool got = false;
                while (!got) {
                    got = socket.recv(client_id, zmq::recv_flags::dontwait).has_value();
                    if (!got && std::chrono::steady_clock::now() - last_activity >= window) break;
                }
                if (!got) continue;
            } else {
                (void) socket.recv(client_id);
            }
        } else {
            (void) socket.recv(client_id);
        }
        last_activity = std::chrono::steady_clock::now();

        // The rest of the multipart request arrived with its first frame.
        int frame_count = 1;
        for (bool more = client_id.more(); more; ++frame_count) {
            zmq::message_t &dst = frame_count == 1 ? delimiter : (frame_count == 2 ? client_msg : extra_frame);
            (void) socket.recv(dst);
            more = dst.more();
        }
        ReplyWriter writer(socket, client_id);
        if (frame_count != 3 || delimiter.size() != 0) {
            spdlog::warn("[NETWORKING] - Malformed request envelope ({} frames), dropping", frame_count);
            writer.Finish();
            continue;
        }

        // Free everything the previous iteration allocated in one shot --
        // safe here because that request's response was already serialized
        // and sent by SendProto before the loop came back around.
        arena.Reset();

        // When this request's bytes were picked up: the moment a client-given-up deadline (Request.timeoutMs) is
        // measured from, below and inside BatchInsertFromWire/RangeInsertFromWire -- captured once, before either
        // path's own parsing, so parsing a huge request doesn't eat into the client's patience unnoticed.
        const auto pickup = xstore::MemoryPool::Clock::now();

        //  BatchInsert / RangeInsert are first offered to the wire path: the rows are read from the received bytes one at a time
        //  instead of being parsed into an object tree of the whole request (about 6 KB per 1.4 KB row, alive until the reply,
        //  and the parse itself is ~2x slower). It declines (nothing done) anything but a plain well-formed insert, and that
        //  request then takes the normal parse + dispatch below, which also gives the old behaviour for bad input.
        //  The first byte of a Request holding one is the tag of field 8 (BatchInsert) / 11 (RangeInsert), wire type 2.
        if (client_msg.size() > 1) {
            const uint8_t first_byte = *static_cast<const uint8_t *>(client_msg.data());
            if (first_byte == 0x42 || first_byte == 0x5A) {
                bool replied = false;
                try {
                    if (first_byte == 0x42) {
                        XStore_Proto::BatchInsert_REP rep;
                        replied = my_store.BatchInsertFromWire(static_cast<const char *>(client_msg.data()), client_msg.size(), &rep, pickup);
                        if (replied) SendProto(writer, rep);
                    } else {
                        XStore_Proto::RangeInsert_REP rep;
                        replied = my_store.RangeInsertFromWire(static_cast<const char *>(client_msg.data()), client_msg.size(), &rep, pickup);
                        if (replied) SendProto(writer, rep);
                    }
                    if (replied) writer.Finish();
                } catch (const std::exception &e) {
                    if (auto *ze = dynamic_cast<const zmq::error_t *>(&e); ze != nullptr && ze->num() == ETERM) throw;
                    spdlog::error("[NETWORKING] - Request handler threw ({}), replying empty and continuing", e.what());
                    try { writer.Finish(); } catch (...) {}
                    replied = true;
                } catch (...) {
                    spdlog::error("[NETWORKING] - Request handler threw a non-std exception, replying empty and continuing");
                    try { writer.Finish(); } catch (...) {}
                    replied = true;
                }
                if (replied) continue;
            }
        }

        //  Parse the envelope once -- the oneof case (payload_case()) below
        //  is the dispatch tag, so there's no separate lightweight parse
        //  followed by a second full parse of the same bytes.
        auto *client_request = google::protobuf::Arena::Create<XStore_Proto::Request>(&arena);
        if (!client_request->ParseFromArray(client_msg.data(), client_msg.size())) {
            spdlog::warn("[NETWORKING] - Failed to parse request, dropping");
            writer.Finish();
            continue;
        }

        // Safety net around the whole dispatch switch. Any handler below
        // that throws would otherwise unwind straight out of this worker
        // thread -- nothing above it catches, so the C++ runtime calls
        // std::terminate and the ENTIRE server process dies, taking every
        // other worker's in-flight request with it. That is not
        // hypothetical: XStore::GetOrOpenZoneMap throws std::runtime_error
        // when ZoneMapFile::Open fails, and a .CFG carrying an implausible
        // columnCount reached std::vector<ZoneMapEntry>'s std::length_error
        // on every insert (final whole-branch review, finding C2 -- that
        // particular input is now bounded in XStore::CreateDB/
        // MaybeGrowColumnCount, but bounding one known input is not the
        // same as surviving an unknown one).
        //
        // Deliberately just a net, not an error-handling redesign: reply
        // the same way this loop's other "can't produce a real answer"
        // paths already do -- a single empty frame, which unblocks the
        // client's REQ socket instead of leaving it waiting forever -- log,
        // and keep serving. ReplyWriter::Finish sends nothing if the reply
        // was already complete and only the terminating frame if a streamed
        // reply was cut short, so the request still gets exactly one reply.
        try {
            switch (client_request->payload_case()) {
                // INFO
                case XStore_Proto::Request::kInfo: {
                    XStore_Proto::SysInfo_REP rep = my_store.InfoQuery(server_addr, num_threads);
                    SendProto(writer, rep);
                    break;
                }

                // PING -- warmup/liveness probe, no XStore work, just the round trip
                case XStore_Proto::Request::kPing: {
                    XStore_Proto::Ping_REP rep;
                    SendProto(writer, rep);
                    break;
                }

                // CREATE DB
                case XStore_Proto::Request::kCreatedb: {
                    const auto &req = client_request->createdb();
                    const std::vector<std::string> column_names(req.columnnames().begin(), req.columnnames().end());
                    XStore_Proto::CreateDB_REP rep = my_store.CreateDB(req.dbname(), req.timeresolution(), req.rowlength(), req.columncount(), column_names);
                    SendProto(writer, rep);
                    break;
                }

                // REMOVE DB
                case XStore_Proto::Request::kRemovedb: {
                    const auto &req = client_request->removedb();
                    XStore_Proto::RemoveDB_REP rep = my_store.RemoveDB(req.dbname());
                    SendProto(writer, rep);
                    break;
                }

                // REMOVE DB FILES
                case XStore_Proto::Request::kRemovedbfiles: {
                    auto *req = client_request->mutable_removedbfiles();
                    XStore_Proto::RemoveDBFiles_REP rep = my_store.RemoveDBFiles(req, req->dbname());
                    SendProto(writer, rep);
                    break;
                }

                // UNARY INSERT
                case XStore_Proto::Request::kUnaryinsert: {
                    const auto &req = client_request->unaryinsert();
                    XStore_Proto::UnaryInsert_REP rep;
                    my_store.UnaryInsert(req.timestamp(), req.data(), req.dbname(), &rep, req.durabilitymode());
                    SendProto(writer, rep);
                    break;
                }

                // UNARY QUERY
                case XStore_Proto::Request::kUnaryquery: {
                    const auto &req = client_request->unaryquery();
                    // Arena-allocated: same RowData -> repeated FData nesting as
                    // Range/BatchQuery (up to one alloc per column), and this is
                    // likely the highest-QPS RPC of all of them -- single-row
                    // point queries. Not scaled by span like RangeQuery, but
                    // paid on every call with no reuse across calls.
                    auto *rep = google::protobuf::Arena::Create<XStore_Proto::UnaryQuery_REP>(&arena);
                    my_store.UnaryQuery(req.timestamp(), req.dbname(), rep);
                    SendProto(writer, *rep);
                    break;
                }

                // UNARY DELETE
                case XStore_Proto::Request::kUnarydelete: {
                    const auto &req = client_request->unarydelete();
                    XStore_Proto::UnaryDelete_REP rep = my_store.UnaryDelete(req.timestamp(), req.dbname());
                    SendProto(writer, rep);
                    break;
                }

                // BATCH INSERT
                case XStore_Proto::Request::kBatchinsert: {
                    // Req itself is already arena-allocated (it's a field inside
                    // ClientRequest, which lives on Arena) -- BatchInsert_REQ's
                    // repeated BatchInsert_DATA, each with its own RowData ->
                    // repeated FData, was parsed straight into arena memory
                    // above with no extra work needed here.
                    auto *req = client_request->mutable_batchinsert();
                    const uint32_t timeout_ms = client_request->timeoutms();
                    const auto deadline = timeout_ms > 0 ? pickup + std::chrono::milliseconds(timeout_ms) : xstore::MemoryPool::Clock::time_point::max();
                    XStore_Proto::BatchInsert_REP rep;
                    my_store.BatchInsert(req, &rep, deadline);
                    SendProto(writer, rep);
                    break;
                }

                // BATCH QUERY
                case XStore_Proto::Request::kBatchquery: {
                    // No Arena/BatchQuery_REP here -- BatchQuery shares
                    // RangeQuery's zero-object-tree encoder (same wire
                    // shape, see XStore_WireEncode.h), for the same reason: no
                    // UnaryQuery_REP/RowData/FData tree ever gets built, so
                    // there's nothing for Reset() to leave stranded as a
                    // permanent per-worker RSS floor. Multi-frame reply,
                    // reassembled transparently by the client library.
                    auto *req = client_request->mutable_batchquery();
                    my_store.BatchQuery(req, /*sort_time=*/true, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                        [&](std::string &&chunk, bool is_last) { writer.Send(zmq::buffer(chunk), is_last); });
                    break;
                }

                // BATCH DELETE
                case XStore_Proto::Request::kBatchdelete: {
                    auto *req = client_request->mutable_batchdelete();
                    XStore_Proto::BatchDelete_REP rep = my_store.BatchDelete(req);
                    SendProto(writer, rep);
                    break;
                }

                // RANGE INSERT
                case XStore_Proto::Request::kRangeinsert: {
                    // Same as BatchInsert above: RangeInsert_REQ's repeated
                    // RangeInsert_DATA -> RowData -> repeated FData tree is
                    // already arena-allocated from the ParseFromArray above --
                    // this is the actual benefit for RangeInsert, since its own
                    // response (RangeInsert_REP) is just a status string with
                    // nothing to arena.
                    auto *req = client_request->mutable_rangeinsert();   // non-const: RangeInsert sorts an unsorted request in place
                    const uint32_t timeout_ms = client_request->timeoutms();
                    const auto deadline = timeout_ms > 0 ? pickup + std::chrono::milliseconds(timeout_ms) : xstore::MemoryPool::Clock::time_point::max();
                    XStore_Proto::RangeInsert_REP rep;
                    my_store.RangeInsert(req, &rep, deadline);
                    SendProto(writer, rep);
                    break;
                }

                // RANGE QUERY
                case XStore_Proto::Request::kRangequery: {
                    // No Arena/RangeQuery_REP here -- RangeQuery never
                    // builds a UnaryQuery_REP/RowData/FData tree for this
                    // response (see its declaration comment in XStore.h and the
                    // wire-encode helpers in XStore_RangeOps.cpp), which is the
                    // fix for the OOM a big single-shot range used to cause: the
                    // old path's arena tree, once built, was never freed
                    // (Reset() keeps blocks) and became this worker's permanent
                    // RSS floor. Each chunk here is sent as its own ZMQ frame
                    // (SNDMORE on all but the last), reassembled by the client
                    // library into one logical RangeQuery_REP -- transparent to
                    // whatever called RangeQuery at the API level.
                    const auto &req = client_request->rangequery();
                    my_store.RangeQuery(req.starttime(), req.endtime(), req.dbname(),
                        XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                        [&](std::string &&chunk, bool is_last) { writer.Send(zmq::buffer(chunk), is_last); });
                    break;
                }

                // FIND BY VALUE (categorical equality lookup)
                case XStore_Proto::Request::kFindbyvalue: {
                    const auto &req = client_request->findbyvalue();
                    my_store.FindByValue(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), req.value(),
                        XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                        [&](std::string &&chunk, bool is_last) { writer.Send(zmq::buffer(chunk), is_last); });
                    break;
                }

                // FIND BY PATTERN (LIKE/ILIKE/REGEX) -- TEMPORARY IMPLEMENTATION (option A)
                // behind it: brute-force scan, see XStore_PatternOps.cpp.
                case XStore_Proto::Request::kFindbypattern: {
                    const auto &req = client_request->findbypattern();
                    my_store.FindByPattern(req.starttime(), req.endtime(), req.dbname(), req.columnidx(),
                        req.pattern(), req.mode(), req.caseinsensitive(),
                        XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                        [&](std::string &&chunk, bool is_last) { writer.Send(zmq::buffer(chunk), is_last); });
                    break;
                }

                // RANGE DELETE
                case XStore_Proto::Request::kRangedelete: {
                    const auto &req = client_request->rangedelete();
                    XStore_Proto::RangeDelete_REP rep = my_store.RangeDelete(req.starttime(), req.endtime(), req.dbname());
                    SendProto(writer, rep);
                    break;
                }

                // MIN AGGREGATE
                case XStore_Proto::Request::kMin: {
                    const auto &req = client_request->min();
                    XStore_Proto::Min_REP rep;
                    my_store.MinAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // MAX AGGREGATE
                case XStore_Proto::Request::kMax: {
                    const auto &req = client_request->max();
                    XStore_Proto::Max_REP rep;
                    my_store.MaxAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // SUM AGGREGATE
                case XStore_Proto::Request::kSum: {
                    const auto &req = client_request->sum();
                    XStore_Proto::Sum_REP rep;
                    my_store.SumAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // AVG AGGREGATE
                case XStore_Proto::Request::kAvg: {
                    const auto &req = client_request->avg();
                    XStore_Proto::Avg_REP rep;
                    my_store.AvgAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // VARIANCE AGGREGATE
                case XStore_Proto::Request::kVariance: {
                    const auto &req = client_request->variance();
                    XStore_Proto::Variance_REP rep;
                    my_store.VarianceAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // STDDEV AGGREGATE
                case XStore_Proto::Request::kStddev: {
                    const auto &req = client_request->stddev();
                    XStore_Proto::Stddev_REP rep;
                    my_store.StddevAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // EXISTS AGGREGATE
                case XStore_Proto::Request::kExists: {
                    const auto &req = client_request->exists();
                    XStore_Proto::Exists_REP rep;
                    my_store.ExistsAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // OHLC AGGREGATE
                case XStore_Proto::Request::kOhlc: {
                    const auto &req = client_request->ohlc();
                    XStore_Proto::Ohlc_REP rep;
                    my_store.OhlcAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // PERCENTILE AGGREGATE (exact)
                case XStore_Proto::Request::kPercentile: {
                    const auto &req = client_request->percentile();
                    XStore_Proto::Percentile_REP rep;
                    my_store.PercentileAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), req.percentile(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // DISTINCTCOUNT AGGREGATE (exact)
                case XStore_Proto::Request::kDistinctcount: {
                    const auto &req = client_request->distinctcount();
                    XStore_Proto::DistinctCount_REP rep;
                    my_store.DistinctCountAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // TOPK AGGREGATE (exact; k=1 gives the mode)
                case XStore_Proto::Request::kTopk: {
                    const auto &req = client_request->topk();
                    XStore_Proto::TopK_REP rep;
                    my_store.TopKAggregate(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), req.k(), &rep);
                    SendProto(writer, rep);
                    break;
                }

                // No payload set (malformed/unrecognized request) -- reply so
                // the client's REQ socket never hangs waiting for a response
                // that will never come.
                case XStore_Proto::Request::PAYLOAD_NOT_SET:
                default: {
                    spdlog::warn("[NETWORKING] - Received request with no payload set, dropping");
                    writer.Finish();
                    break;
                }
            }
            // No-op once the handler completed its reply; otherwise the
            // request still gets its one reply.
            writer.Finish();
        } catch (const std::exception &e) {
            // Context shut down mid-request (the reply send failed): not an
            // error, unwind to WorkerRoutine.
            if (auto *ze = dynamic_cast<const zmq::error_t *>(&e); ze != nullptr && ze->num() == ETERM) throw;
            spdlog::error("[NETWORKING] - Request handler threw ({}), replying empty and continuing", e.what());
            try { writer.Finish(); } catch (...) {}
        } catch (...) {
            spdlog::error("[NETWORKING] - Request handler threw a non-std exception, replying empty and continuing");
            try { writer.Finish(); } catch (...) {}
        }
    }
}
