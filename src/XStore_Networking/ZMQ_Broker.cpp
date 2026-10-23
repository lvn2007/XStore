#include "include/Networking/ZMQ_Broker.h"

#include <chrono>
#include <utility>
#include <vector>

namespace {

// Messages relayed per poll wake before polling again; each round moves at most one reply and one request.
constexpr int MAX_DRAIN_ROUNDS = 256;

// One worker message, if any: a lone id frame is READY; otherwise the id is followed by a whole reply, which goes to
// the frontend frame by frame. Either way the worker is idle afterwards. Parked ids are moved, not copied: a short id
// lives inline in the zmq_msg_t, so the stack never allocates past its reserve.
bool RelayReply(zmq::socket_t &backend, zmq::socket_t &frontend, std::vector<zmq::message_t> &idle,
                zmq::message_t &worker_id, zmq::message_t &frame) {
    if (!backend.recv(worker_id, zmq::recv_flags::dontwait)) return false;
    // The rest of a multipart message is delivered atomically with its first frame: these recvs never block.
    bool more = worker_id.more();
    while (more) {
        (void)backend.recv(frame, zmq::recv_flags::none);
        more = frame.more();
        frontend.send(frame, more ? zmq::send_flags::sndmore : zmq::send_flags::none);
    }
    idle.push_back(std::move(worker_id));
    return true;
}

// One client request, if any, to the most recently idle worker as [worker_id][client_id][empty][body].
bool RelayRequest(zmq::socket_t &frontend, zmq::socket_t &backend, std::vector<zmq::message_t> &idle,
                  zmq::message_t &frame) {
    if (!frontend.recv(frame, zmq::recv_flags::dontwait)) return false;
    backend.send(idle.back(), zmq::send_flags::sndmore);
    idle.pop_back();
    bool more = frame.more();
    backend.send(frame, more ? zmq::send_flags::sndmore : zmq::send_flags::none);
    while (more) {
        (void)frontend.recv(frame, zmq::recv_flags::none);
        more = frame.more();
        backend.send(frame, more ? zmq::send_flags::sndmore : zmq::send_flags::none);
    }
    return true;
}

}  // namespace

void RunReadyBroker(zmq::socket_t &frontend, zmq::socket_t &backend, size_t max_workers) {
    // LIFO: the most recently freed worker is the one still spinning in its adaptive-poll window, with a warm cache.
    std::vector<zmq::message_t> idle;
    idle.reserve(max_workers);
    zmq::message_t worker_id;
    zmq::message_t frame;
    zmq::pollitem_t items[] = {{backend.handle(), 0, ZMQ_POLLIN, 0}, {frontend.handle(), 0, ZMQ_POLLIN, 0}};

    while (true) {
        // Frontend polled only while a worker is idle: otherwise requests stay queued in the frontend ROUTER.
        zmq::poll(items, idle.empty() ? 1 : 2, std::chrono::milliseconds(-1));
        for (int round = 0; round < MAX_DRAIN_ROUNDS; ++round) {
            const bool replied = RelayReply(backend, frontend, idle, worker_id, frame);
            const bool requested = !idle.empty() && RelayRequest(frontend, backend, idle, frame);
            if (!replied && !requested) break;
        }
    }
}
