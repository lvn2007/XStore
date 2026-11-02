#ifndef XSTORE_ZMQ_BROKER_H
#define XSTORE_ZMQ_BROKER_H

#include <chrono>
#include <cstddef>
#include <zmq.hpp>

#include "include/Networking/ZMQ_ParkedReplies.h"

// Ready-based request dispatch between a client-facing ROUTER (`frontend`) and a worker-facing ROUTER (`backend`).
// Workers are DEALER sockets with a unique routing id; each sends one single-frame READY, then answers every request
// [client_id][empty][body] with exactly one multipart reply [client_id][empty][frame...]. A request is forwarded only
// to an idle worker (most recently freed first); while none is idle, requests wait in the frontend's queue.
// `max_workers` only pre-sizes the idle stack. For `spin_window` after the last relayed message the broker spins on
// zero-timeout polls instead of blocking, which skips the poll wake-up latency at low load; 0 = always block.
// Heavy requests: a request whose frames add up to `min_bytes` or more (0 = none is heavy) is only handed to a worker while
// fewer than `max_in_flight` heavy requests are being served (at least 1); the others wait in the broker, in arrival order,
// and lighter requests are dispatched past them. A heavy request can block its worker for a long time (it may wait for the
// memory pool), and without this a burst of them leaves no worker for a small query or delete.
// Never returns normally: throws zmq::error_t (ETERM after
// zmq_ctx_shutdown, like zmq::proxy).
struct HeavyRequestLimit {
    size_t min_bytes = 0;
    size_t max_in_flight = 0;
};
// Park and resume (ZMQ_ParkedReplies.h; docs/superpowers/specs/2026-11-01-park-resume-replies-design.md). With a `registry`, a worker message whose
// delimiter is RELEASED_MARK + token is relayed to the client like any streamed message and the worker is idle again; the registry is told
// (OnReleased) after the relay, and a reply the registry reports ready is resumed on an idle worker as [worker][token]['R'][empty], alternating with
// new requests when both wait. Replies released for `max_age` without draining are made ready as expired. While nothing is parked the broker polls
// and works exactly as without a registry.
struct ParkSettings {
    xstore::parked::ParkedReplies *registry = nullptr;
    std::chrono::milliseconds max_age{120000};
};
[[noreturn]] void RunReadyBroker(zmq::socket_t &frontend, zmq::socket_t &backend, size_t max_workers,
                                 std::chrono::microseconds spin_window, HeavyRequestLimit heavy = {}, ParkSettings park = {});

#endif  // XSTORE_ZMQ_BROKER_H
