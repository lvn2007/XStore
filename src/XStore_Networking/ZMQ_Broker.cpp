#include "include/Networking/ZMQ_Broker.h"

#include <algorithm>
#include <chrono>
#include <deque>
#include <string>
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

// One worker message, if any: a lone id frame is READY; otherwise the id is followed by a whole reply, which goes to
// the frontend frame by frame. Either way the worker is idle afterwards. Parked ids are moved, not copied: a short id
// lives inline in the zmq_msg_t, so the stack never allocates past its reserve.
bool RelayReply(zmq::socket_t &backend, zmq::socket_t &frontend, std::vector<zmq::message_t> &idle, HeavyInFlight &heavy,
                zmq::message_t &worker_id, zmq::message_t &frame) {
    if (!backend.recv(worker_id, zmq::recv_flags::dontwait)) return false;
    // The rest of a multipart message is delivered atomically with its first frame: these recvs never block.
    bool more = worker_id.more();
    while (more) {
        (void)backend.recv(frame, zmq::recv_flags::none);
        more = frame.more();
        frontend.send(frame, more ? zmq::send_flags::sndmore : zmq::send_flags::none);
    }
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
                    std::chrono::microseconds spin_window, HeavyRequestLimit heavy_limit) {
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
    zmq::pollitem_t items[] = {{backend.handle(), 0, ZMQ_POLLIN, 0}, {frontend.handle(), 0, ZMQ_POLLIN, 0}};
    // Epoch, not now: the first iteration blocks. A zero-timeout poll still throws ETERM after shutdown.
    auto last_activity = std::chrono::steady_clock::time_point{};

    while (true) {
        // Frontend polled only while a worker is idle: otherwise requests stay queued in the frontend ROUTER.
        const size_t n_items = idle.empty() ? 1 : 2;
        int ready = 0;
        while (ready == 0 && std::chrono::steady_clock::now() - last_activity < spin_window) {
            ready = zmq::poll(items, n_items, std::chrono::milliseconds(0));
        }
        if (ready == 0) zmq::poll(items, n_items, std::chrono::milliseconds(-1));
        for (int round = 0; round < MAX_DRAIN_ROUNDS; ++round) {
            const bool replied = RelayReply(backend, frontend, idle, heavy, worker_id, frame);
            bool requested = false;
            if (!idle.empty() && !deferred.empty() && heavy.Count() < heavy_cap) {
                // A slot freed: the oldest deferred heavy request goes before anything newer.
                heavy.Add(idle.back());
                Dispatch(backend, idle, deferred.front());
                deferred.pop_front();
                requested = true;
            } else if (!idle.empty() && RecvRequest(frontend, request, request_bytes)) {
                requested = true;
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
            if (!replied && !requested) break;
            last_activity = std::chrono::steady_clock::now();
        }
    }
}
