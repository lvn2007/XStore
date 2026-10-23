// RunReadyBroker (ZMQ_Broker.h) on a private context with fake DEALER workers: requests go only to idle workers,
// wait when every worker is busy, multi-frame replies are relayed intact, and ctx.shutdown() ends the broker with
// ETERM. Negative "nothing arrived" checks use a short receive timeout; every positive step is synchronised by a
// message actually arriving. Every broker test runs with spin window 0 (block in poll) and 50 ms (spin first).
#include "include/Networking/ZMQ_Broker.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <ctime>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int RECV_TIMEOUT_MS = 5000;
constexpr int NEGATIVE_CHECK_MS = 100;
constexpr std::chrono::microseconds SPIN_WINDOW{50000};

std::vector<std::string> RecvFrames(zmq::socket_t &socket) {
    std::vector<std::string> frames;
    zmq::message_t frame;
    do {
        if (!socket.recv(frame, zmq::recv_flags::none)) return {};
        frames.emplace_back(static_cast<const char *>(frame.data()), frame.size());
    } while (frame.more());
    return frames;
}

struct FakeWorker {
    zmq::socket_t socket;
    std::string id;

    FakeWorker(zmq::context_t &ctx, const std::string &routing_id, const std::string &endpoint)
        : socket(ctx, zmq::socket_type::dealer), id(routing_id) {
        socket.set(zmq::sockopt::routing_id, routing_id);
        socket.set(zmq::sockopt::linger, 0);
        socket.set(zmq::sockopt::rcvtimeo, RECV_TIMEOUT_MS);
        socket.connect(endpoint);
        socket.send(zmq::message_t(), zmq::send_flags::none);  // READY
    }

    // [client_id][empty][body...] or {} on timeout.
    std::vector<std::string> Take(int timeout_ms = RECV_TIMEOUT_MS) {
        socket.set(zmq::sockopt::rcvtimeo, timeout_ms);
        return RecvFrames(socket);
    }

    void Reply(const std::vector<std::string> &request, const std::vector<std::string> &frames) {
        socket.send(zmq::buffer(request[0]), zmq::send_flags::sndmore);
        socket.send(zmq::message_t(), zmq::send_flags::sndmore);
        for (size_t i = 0; i < frames.size(); ++i) {
            socket.send(zmq::buffer(frames[i]),
                        i + 1 < frames.size() ? zmq::send_flags::sndmore : zmq::send_flags::none);
        }
    }
};

class XStoreZmqBrokerTest : public ::testing::TestWithParam<std::chrono::microseconds> {
  protected:
    zmq::context_t ctx;
    zmq::socket_t frontend{ctx, zmq::socket_type::router};
    zmq::socket_t backend{ctx, zmq::socket_type::router};
    std::future<int> broker_exit;
    std::vector<std::unique_ptr<FakeWorker>> workers;
    std::vector<std::unique_ptr<zmq::socket_t>> clients;

    void SetUp() override {
        frontend.set(zmq::sockopt::linger, 0);
        backend.set(zmq::sockopt::linger, 0);
        frontend.bind("inproc://broker-test-frontend");
        backend.bind("inproc://broker-test-backend");
        broker_exit = std::async(std::launch::async, [this] {
            try {
                RunReadyBroker(frontend, backend, 4, GetParam());
            } catch (const zmq::error_t &e) {
                return e.num();
            }
            return 0;
        });
    }

    void TearDown() override {
        ctx.shutdown();
        if (broker_exit.valid()) broker_exit.wait();
        clients.clear();
        workers.clear();
    }

    FakeWorker &AddWorker(const std::string &id) {
        workers.push_back(std::make_unique<FakeWorker>(ctx, id, "inproc://broker-test-backend"));
        return *workers.back();
    }

    zmq::socket_t &AddClient() {
        auto client = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::req);
        client->set(zmq::sockopt::linger, 0);
        client->set(zmq::sockopt::rcvtimeo, RECV_TIMEOUT_MS);
        client->connect("inproc://broker-test-frontend");
        clients.push_back(std::move(client));
        return *clients.back();
    }

    // Index of the worker that received the next request, with the request in `request` (-1 on timeout).
    int TakeFromAny(const std::vector<FakeWorker *> &candidates, std::vector<std::string> &request) {
        std::vector<zmq::pollitem_t> items;
        for (FakeWorker *w : candidates) items.push_back({w->socket.handle(), 0, ZMQ_POLLIN, 0});
        if (zmq::poll(items, std::chrono::milliseconds(RECV_TIMEOUT_MS)) <= 0) return -1;
        for (size_t i = 0; i < items.size(); ++i) {
            if (items[i].revents & ZMQ_POLLIN) {
                request = candidates[i]->Take();
                return static_cast<int>(i);
            }
        }
        return -1;
    }
};

TEST_P(XStoreZmqBrokerTest, RequestReachesAnIdleWorkerWithClientEnvelope) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddClient();
    client.send(zmq::str_buffer("hello"), zmq::send_flags::none);

    const auto request = worker.Take();
    ASSERT_EQ(request.size(), 3u);
    EXPECT_FALSE(request[0].empty());
    EXPECT_TRUE(request[1].empty());
    EXPECT_EQ(request[2], "hello");

    worker.Reply(request, {"world"});
    const auto reply = RecvFrames(client);
    ASSERT_EQ(reply.size(), 1u);
    EXPECT_EQ(reply[0], "world");
}

TEST_P(XStoreZmqBrokerTest, BusyWorkerGetsNoFurtherRequests) {
    std::vector<FakeWorker *> all = {&AddWorker("wA"), &AddWorker("wB"), &AddWorker("wC")};
    AddClient().send(zmq::str_buffer("hold"), zmq::send_flags::none);
    std::vector<std::string> held;
    const int busy = TakeFromAny(all, held);
    ASSERT_GE(busy, 0);
    ASSERT_EQ(held.back(), "hold");

    std::vector<FakeWorker *> idle;
    for (int i = 0; i < 3; ++i) {
        if (i != busy) idle.push_back(all[static_cast<size_t>(i)]);
    }
    zmq::socket_t &client = AddClient();
    for (int round = 0; round < 50; ++round) {
        const std::string body = "req" + std::to_string(round);
        client.send(zmq::buffer(body), zmq::send_flags::none);
        std::vector<std::string> request;
        const int got = TakeFromAny(idle, request);
        ASSERT_GE(got, 0) << "round " << round << ": no idle worker received the request";
        ASSERT_EQ(request.back(), body);
        idle[static_cast<size_t>(got)]->Reply(request, {"ok" + std::to_string(round)});
        const auto reply = RecvFrames(client);
        ASSERT_EQ(reply.size(), 1u);
        EXPECT_EQ(reply[0], "ok" + std::to_string(round));
    }
    // Every request above was accounted for by an idle worker, so the busy one holds only its own.
    EXPECT_TRUE(all[static_cast<size_t>(busy)]->Take(NEGATIVE_CHECK_MS).empty());
}

TEST_P(XStoreZmqBrokerTest, RequestWaitsWhileAllBusyThenGoesToFirstWorkerThatReplies) {
    std::vector<FakeWorker *> all = {&AddWorker("wA"), &AddWorker("wB")};
    zmq::socket_t &c1 = AddClient();
    zmq::socket_t &c2 = AddClient();
    c1.send(zmq::str_buffer("one"), zmq::send_flags::none);
    std::vector<std::string> r1;
    const int first = TakeFromAny(all, r1);
    ASSERT_GE(first, 0);
    c2.send(zmq::str_buffer("two"), zmq::send_flags::none);
    std::vector<std::string> r2;
    const int second = TakeFromAny({all[static_cast<size_t>(1 - first)]}, r2);
    ASSERT_EQ(second, 0) << "second request must go to the other (idle) worker";
    ASSERT_EQ(r2.back(), "two");

    zmq::socket_t &c3 = AddClient();
    c3.send(zmq::str_buffer("three"), zmq::send_flags::none);
    EXPECT_TRUE(all[0]->Take(NEGATIVE_CHECK_MS).empty());
    EXPECT_TRUE(all[1]->Take(NEGATIVE_CHECK_MS).empty());

    FakeWorker &freed = *all[static_cast<size_t>(1 - first)];
    freed.Reply(r2, {"two-done"});
    EXPECT_EQ(RecvFrames(c2), std::vector<std::string>{"two-done"});
    const auto r3 = freed.Take();
    ASSERT_EQ(r3.size(), 3u);
    EXPECT_EQ(r3.back(), "three");
    freed.Reply(r3, {"three-done"});
    EXPECT_EQ(RecvFrames(c3), std::vector<std::string>{"three-done"});
    EXPECT_TRUE(all[static_cast<size_t>(first)]->Take(NEGATIVE_CHECK_MS).empty());
}

TEST_P(XStoreZmqBrokerTest, MultiFrameReplyIsRelayedIntactAndInOrder) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddClient();
    client.send(zmq::str_buffer("stream"), zmq::send_flags::none);
    const auto request = worker.Take();
    ASSERT_EQ(request.size(), 3u);

    const std::vector<std::string> chunks = {"chunk-0", std::string(100000, 'x'), "", "chunk-3"};
    worker.Reply(request, chunks);
    EXPECT_EQ(RecvFrames(client), chunks);

    // The worker is idle again after the last frame.
    client.send(zmq::str_buffer("again"), zmq::send_flags::none);
    const auto again = worker.Take();
    ASSERT_EQ(again.size(), 3u);
    EXPECT_EQ(again.back(), "again");
}

TEST_P(XStoreZmqBrokerTest, ContextShutdownEndsBrokerWithEterm) {
    // A round trip first, so a spinning broker is inside its window when the context goes away.
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddClient();
    client.send(zmq::str_buffer("ping"), zmq::send_flags::none);
    const auto request = worker.Take();
    ASSERT_EQ(request.size(), 3u);
    worker.Reply(request, {"pong"});
    ASSERT_EQ(RecvFrames(client), std::vector<std::string>{"pong"});
    ctx.shutdown();
    ASSERT_EQ(broker_exit.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    EXPECT_EQ(broker_exit.get(), ETERM);
}

// With the only worker busy the frontend is not polled; the spin on the backend alone must still lapse with the
// window and fall back to the blocking poll (no CPU burnt afterwards).
TEST_P(XStoreZmqBrokerTest, SpinEndsAfterWindowEvenWithNoIdleWorker) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddClient();
    client.send(zmq::str_buffer("hold"), zmq::send_flags::none);
    const auto request = worker.Take();
    ASSERT_EQ(request.size(), 3u);

    std::this_thread::sleep_for(SPIN_WINDOW + std::chrono::milliseconds(50));
    const std::clock_t cpu_before = std::clock();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    const double cpu_ms = 1000.0 * static_cast<double>(std::clock() - cpu_before) / CLOCKS_PER_SEC;
    EXPECT_LT(cpu_ms, 100.0) << "broker still spinning after its window";

    // And it still wakes from the blocking poll for the reply.
    worker.Reply(request, {"done"});
    EXPECT_EQ(RecvFrames(client), std::vector<std::string>{"done"});
}

INSTANTIATE_TEST_SUITE_P(SpinWindow, XStoreZmqBrokerTest,
                         ::testing::Values(std::chrono::microseconds(0), SPIN_WINDOW),
                         [](const ::testing::TestParamInfo<std::chrono::microseconds> &info) {
                             return info.param.count() == 0 ? std::string("NoSpin") : std::string("Spin50ms");
                         });

// The spin loop relies on this: a zero-timeout poll on a shut-down context throws ETERM instead of returning 0.
TEST(XStoreZmqBrokerPollTest, ZeroTimeoutPollOnShutDownContextThrowsEterm) {
    zmq::context_t ctx;
    zmq::socket_t socket(ctx, zmq::socket_type::router);
    socket.set(zmq::sockopt::linger, 0);
    socket.bind("inproc://broker-test-eterm");
    zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}};
    ctx.shutdown();
    try {
        (void)zmq::poll(items, 1, std::chrono::milliseconds(0));
        FAIL() << "zero-timeout poll returned instead of throwing";
    } catch (const zmq::error_t &e) {
        EXPECT_EQ(e.num(), ETERM);
    }
}

}  // namespace
