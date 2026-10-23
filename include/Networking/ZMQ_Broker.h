#ifndef XSTORE_ZMQ_BROKER_H
#define XSTORE_ZMQ_BROKER_H

#include <chrono>
#include <cstddef>
#include <zmq.hpp>

// Ready-based request dispatch between a client-facing ROUTER (`frontend`) and a worker-facing ROUTER (`backend`).
// Workers are DEALER sockets with a unique routing id; each sends one single-frame READY, then answers every request
// [client_id][empty][body] with exactly one multipart reply [client_id][empty][frame...]. A request is forwarded only
// to an idle worker (most recently freed first); while none is idle, requests wait in the frontend's queue.
// `max_workers` only pre-sizes the idle stack. For `spin_window` after the last relayed message the broker spins on
// zero-timeout polls instead of blocking, which skips the poll wake-up latency at low load; 0 = always block.
// Never returns normally: throws zmq::error_t (ETERM after
// zmq_ctx_shutdown, like zmq::proxy).
[[noreturn]] void RunReadyBroker(zmq::socket_t &frontend, zmq::socket_t &backend, size_t max_workers,
                                 std::chrono::microseconds spin_window);

#endif  // XSTORE_ZMQ_BROKER_H
