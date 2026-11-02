#include "include/Networking/ZMQ_Broker.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <deque>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

// Messages relayed per poll wake before polling again; each round moves at most one reply and one request.
constexpr int MAX_DRAIN_ROUNDS = 256;

using Frames = std::vector<zmq::message_t>;

// The workers whose current request is heavy (HeavyRequestLimit), by routing id.
class HeavyInFlight {
  public:
    size_t Count() const { return ids_.size(); }
    void Add(const zmq::message_t &worker_id) { ids_.emplace_back(worker_id.data<char>(), worker_id.size()); }
    void Done(const zmq::message_t &worker_id) {
        const auto it = std::find_if(ids_.begin(), ids_.end(), [&](const std::string &id) {
            return id.size() == worker_id.size() && id.compare(0, id.size(), worker_id.data<char>(), worker_id.size()) == 0;
        });
        if (it != ids_.end()) ids_.erase(it);
    }

  private:
    std::vector<std::string> ids_;
};

// One worker message, if any: a lone id frame is READY; otherwise the id is followed by [client_id][delimiter][frames...],
// which goes to the frontend frame by frame. The worker is idle afterwards, except after a message of a STREAMED reply that
// is not its last: those carry a non-empty delimiter (a reply's last message, and every ordinary reply, has the empty one a
// REQ socket expects), and the worker stays busy until the message that has it. Parked ids are moved, not copied: a short id
// lives inline in the zmq_msg_t, so the stack never allocates past its reserve.
bool RelayReply(zmq::socket_t &backend, zmq::socket_t &frontend, std::vector<zmq::message_t> &idle, HeavyInFlight &heavy,
                zmq::message_t &worker_id, zmq::message_t &frame, xstore::parked::ParkedReplies *parked, size_t &parked_count,
                xstore::streamflow::GoneClients *gone) {
    if (!backend.recv(worker_id, zmq::recv_flags::dontwait)) return false;
    // The rest of a multipart message is delivered atomically with its first frame: these recvs never block.
    bool more = worker_id.more();
    bool more_messages_follow = false;
    bool released = false;
    bool client_gone = false;   // the client's connection is closed (ZMQ_ROUTER_MANDATORY): the rest of this message is read but not sent
    uint64_t token = 0;
    for (size_t index = 0; more; ++index) {
        (void)backend.recv(frame, zmq::recv_flags::none);
        more = frame.more();
        if (index == 1) {   // before the send: it empties `frame`
            more_messages_follow = frame.size() != 0;
            // A parked reply: relayed like any streamed message, but its worker is free.
            if (parked != nullptr && xstore::parked::ParseReleaseDelimiter(frame.data(), frame.size(), token)) {
                released = true;
                more_messages_follow = false;
            }
        }
        if (client_gone) continue;
        try {
            frontend.send(frame, more ? zmq::send_flags::sndmore : zmq::send_flags::none);
        } catch (const zmq::error_t &e) {
            if (e.num() != EHOSTUNREACH || index != 0) throw;
            // The first frame is the client's id; the send failed, so `frame` still holds it. Nothing of this message was sent.
            client_gone = true;
            xstore::streamflow::NoteGoneDrop();
            if (gone != nullptr) gone->Mark(std::string_view(frame.data<char>(), frame.size()));
        }
    }
    // After the relay: from here on the client's socket holds everything this worker sent, so a resume may send the next chunks.
    if (released && parked->OnReleased(token)) ++parked_count;
    if (more_messages_follow) return true;
    if (heavy.Count() > 0) heavy.Done(worker_id);
    idle.push_back(std::move(worker_id));
    return true;
}

// One client request, if any, into `frames` as [client_id][empty][body]; its total size in `bytes`. The frames of a
// multipart message arrive together, so after the first one these recvs never block.
bool RecvRequest(zmq::socket_t &frontend, Frames &frames, size_t &bytes) {
    frames.clear();
    bytes = 0;
    zmq::message_t frame;
    if (!frontend.recv(frame, zmq::recv_flags::dontwait)) return false;
    bool more = frame.more();
    bytes += frame.size();
    frames.push_back(std::move(frame));
    while (more) {
        zmq::message_t next;
        (void)frontend.recv(next, zmq::recv_flags::none);
        more = next.more();
        bytes += next.size();
        frames.push_back(std::move(next));
    }
    return true;
}

// `frames` to the most recently idle worker as [worker_id][client_id][empty][body].
void Dispatch(zmq::socket_t &backend, std::vector<zmq::message_t> &idle, Frames &frames) {
    backend.send(idle.back(), zmq::send_flags::sndmore);
    idle.pop_back();
    for (size_t i = 0; i < frames.size(); ++i) {
        backend.send(frames[i], i + 1 < frames.size() ? zmq::send_flags::sndmore : zmq::send_flags::none);
    }
}

}  // namespace

void RunReadyBroker(zmq::socket_t &frontend, zmq::socket_t &backend, size_t max_workers,
                    std::chrono::microseconds spin_window, HeavyRequestLimit heavy_limit, ParkSettings park,
                    xstore::streamflow::GoneClients *gone) {
    // LIFO: the most recently freed worker is the one still spinning in its adaptive-poll window, with a warm cache.
    std::vector<zmq::message_t> idle;
    idle.reserve(max_workers);
    zmq::message_t worker_id;
    zmq::message_t frame;
    Frames request;
    request.reserve(4);
    size_t request_bytes = 0;
    const size_t heavy_cap = std::max<size_t>(1, heavy_limit.max_in_flight);
    HeavyInFlight heavy;
    std::deque<Frames> deferred;   // heavy requests over the cap, oldest first
    xstore::parked::ParkedReplies *const parked = park.registry;
    size_t parked_count = 0;   // released replies not yet dispatched for resume; the registry's eventfd is polled only while this is not 0
    bool prefer_resume = true; // alternation: after a resume a new request goes first, and the other way round
    const auto sweep_every = std::clamp<std::chrono::milliseconds>(park.max_age / 4, std::chrono::milliseconds(50), std::chrono::milliseconds(1000));
    auto last_sweep = std::chrono::steady_clock::now();
    zmq::pollitem_t items[] = {{backend.handle(), 0, ZMQ_POLLIN, 0},
                               {frontend.handle(), 0, ZMQ_POLLIN, 0},
                               {nullptr, parked != nullptr ? parked->WakeFd() : -1, ZMQ_POLLIN, 0}};
    // A resume to the most recently idle worker: [worker_id][token]['R'][empty body].
    const auto try_resume = [&]() -> bool {
        if (parked_count == 0 || idle.empty()) return false;
        const std::optional<uint64_t> token = parked->NextReady();
        if (!token) return false;
        backend.send(idle.back(), zmq::send_flags::sndmore);
        idle.pop_back();
        backend.send(zmq::buffer(&*token, sizeof(uint64_t)), zmq::send_flags::sndmore);
        backend.send(zmq::buffer(&xstore::parked::RESUME_MARK, 1), zmq::send_flags::sndmore);
        backend.send(zmq::message_t(), zmq::send_flags::none);
        --parked_count;
        return true;
    };
    // Epoch, not now: the first iteration blocks. A zero-timeout poll still throws ETERM after shutdown.
    auto last_activity = std::chrono::steady_clock::time_point{};

    while (true) {
        // Frontend polled only while a worker is idle: otherwise requests stay queued in the frontend ROUTER.
        // The registry's fd matters only to an idle worker, and only while something is parked.
        const size_t n_items = idle.empty() ? 1 : (parked_count > 0 ? 3 : 2);
        int ready = 0;
        while (ready == 0 && std::chrono::steady_clock::now() - last_activity < spin_window) {
            ready = zmq::poll(items, n_items, std::chrono::milliseconds(0));
        }
        // While replies are parked the wait is bounded: the sweep below makes the ones that never drain ready.
        if (ready == 0) zmq::poll(items, n_items, parked_count > 0 ? sweep_every : std::chrono::milliseconds(-1));
        if (parked_count > 0) {
            const auto now = std::chrono::steady_clock::now();
            if (now - last_sweep >= sweep_every) {
                parked->ExpireOlderThan(now - park.max_age);
                last_sweep = now;
            }
        }
        for (int round = 0; round < MAX_DRAIN_ROUNDS; ++round) {
            const bool replied = RelayReply(backend, frontend, idle, heavy, worker_id, frame, parked, parked_count, gone);
            bool requested = false;
            if (prefer_resume && try_resume()) {
                requested = true;
                prefer_resume = false;
            } else if (!idle.empty() && !deferred.empty() && heavy.Count() < heavy_cap) {
                // A slot freed: the oldest deferred heavy request goes before anything newer.
                heavy.Add(idle.back());
                Dispatch(backend, idle, deferred.front());
                deferred.pop_front();
                requested = true;
                prefer_resume = true;
            } else if (!idle.empty() && RecvRequest(frontend, request, request_bytes)) {
                requested = true;
                prefer_resume = true;
                if (heavy_limit.min_bytes > 0 && request_bytes >= heavy_limit.min_bytes) {
                    if (heavy.Count() >= heavy_cap || !deferred.empty()) {
                        deferred.push_back(std::move(request));
                        request = Frames();
                        request.reserve(4);
                    } else {
                        heavy.Add(idle.back());
                        Dispatch(backend, idle, request);
                    }
                } else {
                    Dispatch(backend, idle, request);
                }
            }
            if (!requested && try_resume()) requested = true;   // no request waits: the worker is not left idle
            if (!replied && !requested) break;
            last_activity = std::chrono::steady_clock::now();
        }
    }
}
