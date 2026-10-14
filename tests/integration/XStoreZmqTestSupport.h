#pragma once

#include <atomic>
#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "include/Networking/XStore_Curve.h"
#include "include/Networking/ZMQ_Server.h"
#include "XStoreTestSupport.h"

namespace xstore_test {

// CURVE material for StartZmqServerOnFreePort: `probe_kp` must be in `store`'s allowed set, it is the identity of the
// readiness ping.
struct ZmqCurveServerConfig {
    XStoreCurveKeyPair server_kp;
    XStoreCurveKeyPair probe_kp;
    std::shared_ptr<KeyStore> store;
};

// One Ping round trip on a fresh short-lived REQ socket: true iff a reply arrived within `timeout_ms`.
inline bool PingZmqServer(int port, const ZmqCurveServerConfig *curve, int timeout_ms) {
    zmq::context_t ctx(1);
    zmq::socket_t sock(ctx, zmq::socket_type::req);
    sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
    sock.set(zmq::sockopt::linger, 0);
    if (curve != nullptr) {
        sock.set(zmq::sockopt::curve_serverkey, curve->server_kp.public_key_z85);
        sock.set(zmq::sockopt::curve_publickey, curve->probe_kp.public_key_z85);
        sock.set(zmq::sockopt::curve_secretkey, curve->probe_kp.secret_key_z85);
    }
    sock.connect("tcp://127.0.0.1:" + std::to_string(port));
    XStore_Proto::Request ping;
    ping.mutable_ping()->set_rtype(XStore_Proto::PING);
    const std::string payload = ping.SerializeAsString();
    if (!sock.send(zmq::buffer(payload), zmq::send_flags::none)) return false;
    zmq::message_t reply;
    return sock.recv(reply, zmq::recv_flags::none).has_value();
}

// Starts a detached ZmqServer on an OS-assigned loopback port and returns the port once it answers a Ping (-1 if no
// attempt did). ZmqServer has no bind-ready signal and a failed bind throws out of StartServer, so the server thread
// catches that: a port taken between FreePort() and the bind is retried on a fresh port (5 attempts), and the Ping is
// the positive signal that THIS server is the one listening. A successful server is deliberately never stopped (same as
// the previous fixed-port fixtures: its workers keep their XStore for the process's life).
inline int StartZmqServerOnFreePort(int num_threads, const ZmqCurveServerConfig *curve = nullptr) {
    constexpr int MAX_ATTEMPTS = 5;
    for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt) {
        const int port = FreePort();
        if (port < 0) continue;
        auto server = std::make_shared<ZmqServer>(num_threads);
        auto bind_failed = std::make_shared<std::atomic<bool>>(false);
        std::optional<XStoreCurveKeyPair> identity;
        std::shared_ptr<KeyStore> store;
        if (curve != nullptr) {
            identity = curve->server_kp;
            store = curve->store;
        }
        std::thread([server, bind_failed, port, identity, store]() {
            try {
                (void)server->StartServer("127.0.0.1", std::to_string(port), identity, store);  // drain result irrelevant
            } catch (const std::exception &) {
                bind_failed->store(true);
            }
        }).detach();

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (std::chrono::steady_clock::now() < deadline && !bind_failed->load()) {
            if (PingZmqServer(port, curve, 200)) return port;
        }
        if (!bind_failed->load()) return -1;  // listening but not answering: not a port problem
    }
    return -1;
}

}  // namespace xstore_test
