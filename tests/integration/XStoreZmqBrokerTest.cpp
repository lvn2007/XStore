// RunReadyBroker (ZMQ_Broker.h) on a private context with fake DEALER workers: requests go only to idle workers,
// wait when every worker is busy, multi-frame replies are relayed intact, and ctx.shutdown() ends the broker with
// ETERM. Negative "nothing arrived" checks use a short receive timeout; every positive step is synchronised by a
// message actually arriving. Every broker test runs with spin window 0 (block in poll) and 50 ms (spin first).
#include "include/Networking/ZMQ_Broker.h"
#include "include/Networking/ZMQ_GoneClients.h"
#include "include/Networking/ZMQ_ParkedReplies.h"
#include "include/Networking/ZMQ_StreamFlow.h"

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
        ReplyWithDelimiter(request, "", frames);
    }

    // The release message of a parked reply: [client_id][delimiter] and no frame.
    void ReleaseReply(const std::vector<std::string> &request, const std::string &delimiter) {
        socket.send(zmq::buffer(request[0]), zmq::send_flags::sndmore);
        socket.send(zmq::buffer(delimiter), zmq::send_flags::none);
    }

    // One message of a reply: [client_id][delimiter][frames...]. An empty delimiter is a reply's last message, a non-empty one means more
    // messages of the same reply follow (a streamed reply).
    void ReplyWithDelimiter(const std::vector<std::string> &request, const std::string &delimiter, const std::vector<std::string> &frames) {
        socket.send(zmq::buffer(request[0]), zmq::send_flags::sndmore);
        socket.send(zmq::buffer(delimiter), zmq::send_flags::sndmore);
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

    virtual HeavyRequestLimit Heavy() const { return {}; }
    virtual ParkSettings Park() { return {}; }
    virtual xstore::streamflow::GoneClients *Gone() { return nullptr; }

    void SetUp() override {
        frontend.set(zmq::sockopt::linger, 0);
        frontend.set(zmq::sockopt::router_mandatory, 1);   // as the server sets it: a send to a client that is gone fails (EHOSTUNREACH)
        backend.set(zmq::sockopt::linger, 0);
        frontend.bind("inproc://broker-test-frontend");
        backend.bind("inproc://broker-test-backend");
        broker_exit = std::async(std::launch::async, [this] {
            try {
                RunReadyBroker(frontend, backend, 4, GetParam(), Heavy(), Park(), Gone());
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

    // A DEALER client: what a streaming client is. It sends the empty delimiter a REQ would, and receives [delimiter][frames...] messages.
    zmq::socket_t &AddDealerClient() {
        auto client = std::make_unique<zmq::socket_t>(ctx, zmq::socket_type::dealer);
        client->set(zmq::sockopt::linger, 0);
        client->set(zmq::sockopt::rcvtimeo, RECV_TIMEOUT_MS);
        client->connect("inproc://broker-test-frontend");
        clients.push_back(std::move(client));
        return *clients.back();
    }

    static void SendAsDealer(zmq::socket_t &client, const std::string &body) {
        client.send(zmq::message_t(), zmq::send_flags::sndmore);
        client.send(zmq::buffer(body), zmq::send_flags::none);
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

// Heavy = 1000 bytes or more; at most one heavy request is served at a time, the rest wait in the broker.
class XStoreZmqBrokerHeavyTest : public XStoreZmqBrokerTest {
  protected:
    HeavyRequestLimit Heavy() const override { return {1000, 1}; }
    static std::string HeavyBody(char tag) { return std::string(2000, tag); }
};

TEST_P(XStoreZmqBrokerHeavyTest, ALightRequestIsServedWhileAHeavyOneHoldsTheOnlyHeavySlot) {
    std::vector<FakeWorker *> all = {&AddWorker("wA"), &AddWorker("wB"), &AddWorker("wC")};
    zmq::socket_t &h1 = AddClient();
    zmq::socket_t &h2 = AddClient();
    zmq::socket_t &light = AddClient();
    h1.send(zmq::buffer(HeavyBody('1')), zmq::send_flags::none);
    std::vector<std::string> first;
    ASSERT_GE(TakeFromAny(all, first), 0);
    ASSERT_EQ(first.back(), HeavyBody('1'));

    h2.send(zmq::buffer(HeavyBody('2')), zmq::send_flags::none);
    for (FakeWorker *w : all) EXPECT_TRUE(w->Take(NEGATIVE_CHECK_MS).empty()) << "a second heavy request was dispatched past the cap";

    light.send(zmq::str_buffer("query"), zmq::send_flags::none);
    std::vector<std::string> q;
    const int got = TakeFromAny(all, q);
    ASSERT_GE(got, 0) << "the light request waited behind the deferred heavy one";
    EXPECT_EQ(q.back(), "query");
    all[static_cast<size_t>(got)]->Reply(q, {"answer"});
    EXPECT_EQ(RecvFrames(light), std::vector<std::string>{"answer"});
}

TEST_P(XStoreZmqBrokerHeavyTest, DeferredHeavyRequestsRunInArrivalOrderAsSlotsFree) {
    std::vector<FakeWorker *> all = {&AddWorker("wA"), &AddWorker("wB"), &AddWorker("wC")};
    zmq::socket_t &c1 = AddClient();
    zmq::socket_t &c2 = AddClient();
    zmq::socket_t &c3 = AddClient();
    c1.send(zmq::buffer(HeavyBody('1')), zmq::send_flags::none);
    std::vector<std::string> r1;
    const int w1 = TakeFromAny(all, r1);
    ASSERT_GE(w1, 0);
    c2.send(zmq::buffer(HeavyBody('2')), zmq::send_flags::none);
    c3.send(zmq::buffer(HeavyBody('3')), zmq::send_flags::none);
    for (FakeWorker *w : all) EXPECT_TRUE(w->Take(NEGATIVE_CHECK_MS).empty());

    all[static_cast<size_t>(w1)]->Reply(r1, {"done1"});
    EXPECT_EQ(RecvFrames(c1), std::vector<std::string>{"done1"});
    std::vector<std::string> r2;
    const int w2 = TakeFromAny(all, r2);
    ASSERT_GE(w2, 0);
    EXPECT_EQ(r2.back(), HeavyBody('2')) << "the older deferred heavy request goes first";
    for (FakeWorker *w : all) EXPECT_TRUE(w->Take(NEGATIVE_CHECK_MS).empty()) << "the third is still waiting for the slot";

    all[static_cast<size_t>(w2)]->Reply(r2, {"done2"});
    EXPECT_EQ(RecvFrames(c2), std::vector<std::string>{"done2"});
    std::vector<std::string> r3;
    const int w3 = TakeFromAny(all, r3);
    ASSERT_GE(w3, 0);
    EXPECT_EQ(r3.back(), HeavyBody('3'));
    all[static_cast<size_t>(w3)]->Reply(r3, {"done3"});
    EXPECT_EQ(RecvFrames(c3), std::vector<std::string>{"done3"});
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

TEST_P(XStoreZmqBrokerTest, AStreamedReplyKeepsItsWorkerBusyUntilTheMessageWithAnEmptyDelimiter) {
    const std::string more(1, '\x01');
    const std::string big(100000, 'x');
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &first = AddDealerClient();
    SendAsDealer(first, "stream");
    const auto request = worker.Take();
    ASSERT_EQ(request.size(), 3u);

    worker.ReplyWithDelimiter(request, more, {"chunk-0"});
    EXPECT_EQ(RecvFrames(first), (std::vector<std::string>{more, "chunk-0"}));

    // Another client's request waits: the worker is still producing the first one's reply.
    zmq::socket_t &second = AddDealerClient();
    SendAsDealer(second, "other");
    EXPECT_TRUE(worker.Take(NEGATIVE_CHECK_MS).empty());

    worker.ReplyWithDelimiter(request, more, {big});
    EXPECT_EQ(RecvFrames(first), (std::vector<std::string>{more, big}));
    EXPECT_TRUE(worker.Take(NEGATIVE_CHECK_MS).empty());

    worker.ReplyWithDelimiter(request, "", {"chunk-last"});   // the final message
    EXPECT_EQ(RecvFrames(first), (std::vector<std::string>{"", "chunk-last"}));
    const auto next = worker.Take();                           // idle again: the waiting request arrives
    ASSERT_EQ(next.size(), 3u);
    EXPECT_EQ(next.back(), "other");
}

TEST_P(XStoreZmqBrokerTest, TwoStreamedRepliesInterleavedThroughTheBrokerStayWithTheirOwnClients) {
    const std::string more(1, '\x01');
    FakeWorker &a = AddWorker("wA");
    FakeWorker &b = AddWorker("wB");
    zmq::socket_t &client_1 = AddDealerClient();
    zmq::socket_t &client_2 = AddDealerClient();
    SendAsDealer(client_1, "one");
    SendAsDealer(client_2, "two");
    std::vector<std::string> taken_first, taken_second;
    const int first = TakeFromAny({&a, &b}, taken_first);
    ASSERT_GE(first, 0);
    FakeWorker &first_worker = first == 0 ? a : b;
    FakeWorker &other_worker = first == 0 ? b : a;
    ASSERT_GE(TakeFromAny({&other_worker}, taken_second), 0);
    // The broker may hand the two requests out in either order: name the worker and request of each client by the body.
    const bool first_is_one = taken_first.back() == "one";
    FakeWorker &worker_1 = first_is_one ? first_worker : other_worker;
    FakeWorker &worker_2 = first_is_one ? other_worker : first_worker;
    const std::vector<std::string> &request_1 = first_is_one ? taken_first : taken_second;
    const std::vector<std::string> &request_2 = first_is_one ? taken_second : taken_first;
    ASSERT_EQ(request_1.back(), "one");
    ASSERT_EQ(request_2.back(), "two");

    // Both workers stream at once, their messages interleaved on the broker.
    worker_1.ReplyWithDelimiter(request_1, more, {"1-a"});
    worker_2.ReplyWithDelimiter(request_2, more, {"2-a"});
    worker_1.ReplyWithDelimiter(request_1, more, {"1-b"});
    worker_2.ReplyWithDelimiter(request_2, "", {"2-last"});
    worker_1.ReplyWithDelimiter(request_1, "", {"1-last"});

    const std::vector<std::vector<std::string>> expected_1 = {{more, "1-a"}, {more, "1-b"}, {"", "1-last"}};
    const std::vector<std::vector<std::string>> expected_2 = {{more, "2-a"}, {"", "2-last"}};
    for (const auto &message : expected_1) EXPECT_EQ(RecvFrames(client_1), message);
    for (const auto &message : expected_2) EXPECT_EQ(RecvFrames(client_2), message);
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

// Same as above with an idle worker: the frontend is polled too, and the spin must still lapse with the window.
TEST_P(XStoreZmqBrokerTest, SpinEndsAfterWindowWithAnIdleWorker) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddClient();
    client.send(zmq::str_buffer("ping"), zmq::send_flags::none);
    const auto request = worker.Take();
    ASSERT_EQ(request.size(), 3u);
    worker.Reply(request, {"pong"});
    ASSERT_EQ(RecvFrames(client), std::vector<std::string>{"pong"});

    std::this_thread::sleep_for(SPIN_WINDOW + std::chrono::milliseconds(50));
    const std::clock_t cpu_before = std::clock();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    const double cpu_ms = 1000.0 * static_cast<double>(std::clock() - cpu_before) / CLOCKS_PER_SEC;
    EXPECT_LT(cpu_ms, 100.0) << "broker still spinning after its window";
}

// ---- Park and resume (docs/superpowers/specs/2026-11-01-park-resume-replies-design.md) ----
// A worker that parks a reply sends [client_id][0x02 + 8-byte token] with no frame: the client gets the bare delimiter (it only tests empty / non-empty),
// the worker is idle again, and the broker resumes the reply on an idle worker with [token]['R'][empty] once the reply is released AND drained.
using xstore::parked::ParkedReplies;

class XStoreZmqBrokerParkTest : public XStoreZmqBrokerTest {
  protected:
    ParkedReplies parked;
    std::chrono::milliseconds max_age{600000};
    ParkSettings Park() override { return {&parked, max_age}; }

    static std::string TokenBytes(uint64_t token) { return std::string(reinterpret_cast<const char *>(&token), sizeof(token)); }
    static std::string ReleaseDelimiter(uint64_t token) { return std::string(1, '\x02') + TokenBytes(token); }
};

TEST_P(XStoreZmqBrokerParkTest, AReleaseMessageReachesTheClientAsABareDelimiterAndFreesTheWorker) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &first = AddDealerClient();
    SendAsDealer(first, "stream");
    const auto request = worker.Take();
    ASSERT_EQ(request.size(), 3u);
    const uint64_t token = parked.Park(std::make_shared<int>(1));

    worker.ReleaseReply(request, ReleaseDelimiter(token));
    EXPECT_EQ(RecvFrames(first), (std::vector<std::string>{ReleaseDelimiter(token)}));

    zmq::socket_t &second = AddDealerClient();   // the worker is idle again: it gets the next request
    SendAsDealer(second, "other");
    const auto next = worker.Take();
    ASSERT_EQ(next.size(), 3u);
    EXPECT_EQ(next.back(), "other");
}

TEST_P(XStoreZmqBrokerParkTest, AParkedReplyIsResumedOnAnIdleWorkerOnlyAfterTheQueueDrained) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddDealerClient();
    SendAsDealer(client, "stream");
    const auto request = worker.Take();
    const uint64_t token = parked.Park(std::make_shared<int>(5));
    worker.ReleaseReply(request, ReleaseDelimiter(token));
    ASSERT_EQ(RecvFrames(client).size(), 1u);

    EXPECT_TRUE(worker.Take(NEGATIVE_CHECK_MS).empty()) << "released but the queue has not drained: nothing to resume";
    const auto drained_at = std::chrono::steady_clock::now();
    parked.OnDrained(token);
    const auto resume = worker.Take();
    // Woken by the registry's fd (the broker is blocked by now), not by the sweep timeout (up to 1 s here).
    EXPECT_LT(std::chrono::steady_clock::now() - drained_at, std::chrono::milliseconds(300));
    ASSERT_EQ(resume.size(), 3u);
    EXPECT_EQ(resume[0], TokenBytes(token));
    EXPECT_EQ(resume[1], "R");
    EXPECT_EQ(resume[2], "");   // a worker sees [token]['R'][empty body], where a request is [client_id][empty][body]
    bool expired = true;
    const auto bookmark = parked.Take(token, &expired);
    ASSERT_NE(bookmark, nullptr);
    EXPECT_EQ(*static_cast<int *>(bookmark.get()), 5);
    EXPECT_FALSE(expired);
}

TEST_P(XStoreZmqBrokerParkTest, TheClientHasTheReleaseMessageBeforeAnyWorkerGetsTheResume) {
    // The queue drains BEFORE the release message is relayed: the resume must still wait for the relay, or the next chunks could overtake it.
    FakeWorker &a = AddWorker("wA");
    FakeWorker &b = AddWorker("wB");
    zmq::socket_t &client = AddDealerClient();
    SendAsDealer(client, "stream");
    std::vector<std::string> request;
    FakeWorker *serving = TakeFromAny({&a, &b}, request) == 0 ? &a : &b;
    FakeWorker *other = serving == &a ? &b : &a;
    const uint64_t token = parked.Park(std::make_shared<int>(1));
    parked.OnDrained(token);
    serving->ReleaseReply(request, ReleaseDelimiter(token));
    auto resume = other->Take(NEGATIVE_CHECK_MS);
    if (resume.empty()) resume = serving->Take();
    ASSERT_EQ(resume.size(), 3u);
    zmq::message_t delimiter;
    EXPECT_TRUE(client.recv(delimiter, zmq::recv_flags::dontwait).has_value()) << "the resume was dispatched before the release reached the client";
    EXPECT_EQ(std::string(static_cast<const char *>(delimiter.data()), delimiter.size()), ReleaseDelimiter(token));
}

TEST_P(XStoreZmqBrokerParkTest, AResumeWaitsForAnIdleWorker) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddDealerClient();
    SendAsDealer(client, "stream");
    const uint64_t token = parked.Park(std::make_shared<int>(1));
    worker.ReleaseReply(worker.Take(), ReleaseDelimiter(token));
    ASSERT_EQ(RecvFrames(client).size(), 1u);
    zmq::socket_t &other = AddDealerClient();
    SendAsDealer(other, "busy");   // the only worker is busy again when the reply becomes ready
    const auto busy = worker.Take();
    ASSERT_EQ(busy.size(), 3u);
    parked.OnDrained(token);
    EXPECT_TRUE(worker.Take(NEGATIVE_CHECK_MS).empty());
    worker.Reply(busy, {"done"});
    const auto resume = worker.Take();
    ASSERT_EQ(resume.size(), 3u);
    EXPECT_EQ(resume[1], "R");
}

TEST_P(XStoreZmqBrokerParkTest, ResumesAndNewRequestsAlternateWhenBothWait) {
    FakeWorker &worker = AddWorker("wA");
    uint64_t tokens[2];
    for (int i = 0; i < 2; ++i) {   // two replies parked by the one worker
        zmq::socket_t &client = AddDealerClient();
        SendAsDealer(client, "stream-" + std::to_string(i));
        tokens[i] = parked.Park(std::make_shared<int>(i));
        worker.ReleaseReply(worker.Take(), ReleaseDelimiter(tokens[i]));
        ASSERT_EQ(RecvFrames(client).size(), 1u);
    }
    zmq::socket_t &busy_client = AddDealerClient();
    SendAsDealer(busy_client, "busy");
    const auto busy = worker.Take();
    ASSERT_EQ(busy.size(), 3u);
    zmq::socket_t &c1 = AddDealerClient();
    zmq::socket_t &c2 = AddDealerClient();
    SendAsDealer(c1, "new-1");
    SendAsDealer(c2, "new-2");
    for (uint64_t t : tokens) parked.OnDrained(t);   // both ready, both new requests waiting, the one worker busy
    std::this_thread::sleep_for(std::chrono::milliseconds(NEGATIVE_CHECK_MS));

    std::vector<std::string> order;
    worker.Reply(busy, {"x"});
    for (int i = 0; i < 4; ++i) {
        const auto next = worker.Take();
        ASSERT_EQ(next.size(), 3u);
        order.push_back(next[1] == "R" ? "resume" : next.back());
        worker.Reply(next, {"x"});
    }
    // The router does not promise an order between two clients' requests: check the alternation and that both were served.
    ASSERT_EQ(order.size(), 4u);
    EXPECT_EQ(order[0], "resume");
    EXPECT_EQ(order[2], "resume");
    EXPECT_TRUE((order[1] == "new-1" && order[3] == "new-2") || (order[1] == "new-2" && order[3] == "new-1"));
}

TEST_P(XStoreZmqBrokerParkTest, ReadyRepliesAreAllResumedWhenNoNewRequestWaits) {
    FakeWorker &worker = AddWorker("wA");
    uint64_t tokens[3];
    for (int i = 0; i < 3; ++i) {
        zmq::socket_t &client = AddDealerClient();
        SendAsDealer(client, "stream-" + std::to_string(i));
        tokens[i] = parked.Park(std::make_shared<int>(i));
        worker.ReleaseReply(worker.Take(), ReleaseDelimiter(tokens[i]));
        ASSERT_EQ(RecvFrames(client).size(), 1u);
    }
    for (uint64_t t : tokens) parked.OnDrained(t);
    for (int i = 0; i < 3; ++i) {   // alternation must not hold a resume back when there is no request to alternate with
        const auto resume = worker.Take(2000);
        ASSERT_EQ(resume.size(), 3u) << "resume " << i;
        EXPECT_EQ(resume[0], TokenBytes(tokens[i]));
        worker.Reply(resume, {"x"});
    }
}

TEST_P(XStoreZmqBrokerParkTest, ARunningNonParkedStreamStillKeepsItsWorkerBusy) {
    const std::string more(1, '\x01');
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &first = AddDealerClient();
    SendAsDealer(first, "stream");
    const auto request = worker.Take();
    worker.ReplyWithDelimiter(request, more, {"chunk-0"});
    EXPECT_EQ(RecvFrames(first), (std::vector<std::string>{more, "chunk-0"}));
    zmq::socket_t &second = AddDealerClient();
    SendAsDealer(second, "other");
    EXPECT_TRUE(worker.Take(NEGATIVE_CHECK_MS).empty());
}

TEST_P(XStoreZmqBrokerParkTest, AReleaseOfAnUnknownTokenIsRelayedAndHarmless) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddDealerClient();
    SendAsDealer(client, "stream");
    const auto request = worker.Take();
    worker.ReleaseReply(request, ReleaseDelimiter(0x1122334455667788ull));
    EXPECT_EQ(RecvFrames(client).size(), 1u);
    zmq::socket_t &second = AddDealerClient();
    SendAsDealer(second, "other");
    EXPECT_EQ(worker.Take().back(), "other");
}

// A client that is gone (ZMQ_GoneClients.h): the server sets ZMQ_ROUTER_MANDATORY on the client socket, so the relay of a reply message to a client whose
// connection is closed fails with EHOSTUNREACH, and the broker cancels that client's registered replies.
class XStoreZmqBrokerGoneTest : public XStoreZmqBrokerParkTest {
  protected:
    xstore::streamflow::GoneClients gone;
    xstore::streamflow::GoneClients *Gone() override { return &gone; }

    // Closes the client and waits until the broker's socket has dropped its connection: a reply message that comes back is dropped (a cancel).
    // `send_one` sends one message of the reply to the client; returns when `window` was cancelled by it.
    template <typename Send>
    static bool CancelledBySending(const std::shared_ptr<xstore::streamflow::ReplyWindow> &window, Send send_one) {
        for (int i = 0; i < 500 && !window->Cancelled(); ++i) {
            send_one();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return window->Cancelled();
    }
};

TEST_P(XStoreZmqBrokerGoneTest, AReplyMessageToAClientThatIsGoneCancelsItsRegisteredReply) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddDealerClient();
    SendAsDealer(client, "stream");
    const auto request = worker.Take();
    const auto window = std::make_shared<xstore::streamflow::ReplyWindow>();
    gone.Register(request[0], window);
    worker.ReplyWithDelimiter(request, "\x01", {"chunk"});
    ASSERT_EQ(RecvFrames(client).size(), 2u) << "a live client gets its chunk and the reply is not cancelled";
    EXPECT_FALSE(window->Cancelled());

    clients.clear();   // the client closes its connection
    EXPECT_TRUE(CancelledBySending(window, [&] { worker.ReplyWithDelimiter(request, "\x01", {"chunk"}); }));
}

TEST_P(XStoreZmqBrokerGoneTest, OnlyTheRepliesOfTheClientThatLeftAreCancelled) {
    FakeWorker &worker = AddWorker("wA");
    FakeWorker &worker_b = AddWorker("wB");
    zmq::socket_t &leaving = AddDealerClient();
    SendAsDealer(leaving, "stream");
    std::vector<std::string> request_a;
    FakeWorker *first = TakeFromAny({&worker, &worker_b}, request_a) == 0 ? &worker : &worker_b;
    FakeWorker *second = first == &worker ? &worker_b : &worker;
    zmq::socket_t &staying = AddDealerClient();
    SendAsDealer(staying, "stream");
    const auto request_b = second->Take();
    ASSERT_NE(request_a[0], request_b[0]);
    const auto window_a = std::make_shared<xstore::streamflow::ReplyWindow>();
    const auto window_b = std::make_shared<xstore::streamflow::ReplyWindow>();
    gone.Register(request_a[0], window_a);
    gone.Register(request_b[0], window_b);

    clients.erase(clients.begin());   // `leaving` is the first client
    ASSERT_TRUE(CancelledBySending(window_a, [&] { first->ReplyWithDelimiter(request_a, "\x01", {"chunk"}); }));
    second->ReplyWithDelimiter(request_b, "\x01", {"chunk"});
    EXPECT_EQ(RecvFrames(*clients.front()).size(), 2u);
    EXPECT_FALSE(window_b->Cancelled());
}

TEST_P(XStoreZmqBrokerGoneTest, AReplyToAClientThatIsGoneStillFreesItsWorkerAndLeaksNoFramesToOthers) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &leaving = AddDealerClient();
    SendAsDealer(leaving, "stream");
    const auto request = worker.Take();
    const auto window = std::make_shared<xstore::streamflow::ReplyWindow>();
    gone.Register(request[0], window);
    clients.clear();
    // Several frames to the gone client: the frames after the first must be skipped, not sent as the start of another message.
    ASSERT_TRUE(CancelledBySending(window, [&] { worker.ReplyWithDelimiter(request, "\x01", {"a", "b", "c"}); }));
    worker.Reply(request, {"last", "frames", "here"});   // the reply's last message: the worker is idle again

    zmq::socket_t &next = AddDealerClient();
    SendAsDealer(next, "second request");
    const auto second = worker.Take();
    ASSERT_EQ(second.size(), 3u) << "the worker is idle again";
    EXPECT_EQ(second.back(), "second request");
    worker.Reply(second, {"for the new client"});
    const auto got = RecvFrames(next);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got.back(), "for the new client") << "nothing of the gone client's frames reached another client";
}

TEST_P(XStoreZmqBrokerGoneTest, TheRelayOfAReleaseToAGoneClientStillMakesTheReplyReadyForAResume) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &leaving = AddDealerClient();
    SendAsDealer(leaving, "stream");
    const auto request = worker.Take();
    const auto window = std::make_shared<xstore::streamflow::ReplyWindow>();
    gone.Register(request[0], window);
    clients.clear();
    // Wait until the broker's socket has dropped the client (a chunk to it cancels the reply), then park: the chunk's worker stays busy meanwhile.
    ASSERT_TRUE(CancelledBySending(window, [&] { worker.ReplyWithDelimiter(request, "\x01", {"chunk"}); }));
    const uint64_t token = parked.Park(std::make_shared<int>(3));
    parked.OnDrained(token);
    worker.ReleaseReply(request, ReleaseDelimiter(token));
    const auto resume = worker.Take();
    ASSERT_EQ(resume.size(), 3u) << "the release was parsed even though its relay failed: the reply became ready";
    EXPECT_EQ(resume[0], TokenBytes(token));
}

class XStoreZmqBrokerParkExpiryTest : public XStoreZmqBrokerParkTest {
  protected:
    XStoreZmqBrokerParkExpiryTest() { max_age = std::chrono::milliseconds(300); }
};

TEST_P(XStoreZmqBrokerParkExpiryTest, AReleasedReplyThatNeverDrainsIsResumedAsExpiredAfterTheMaxAge) {
    FakeWorker &worker = AddWorker("wA");
    zmq::socket_t &client = AddDealerClient();
    SendAsDealer(client, "stream");
    const auto request = worker.Take();
    const uint64_t token = parked.Park(std::make_shared<int>(9));
    worker.ReleaseReply(request, ReleaseDelimiter(token));
    ASSERT_EQ(RecvFrames(client).size(), 1u);
    const auto resume = worker.Take(3000);   // never drained: the sweep makes it ready
    ASSERT_EQ(resume.size(), 3u);
    bool expired = false;
    ASSERT_NE(parked.Take(token, &expired), nullptr);
    EXPECT_TRUE(expired);
}

INSTANTIATE_TEST_SUITE_P(SpinWindow, XStoreZmqBrokerParkTest,
                         ::testing::Values(std::chrono::microseconds(0), SPIN_WINDOW));
INSTANTIATE_TEST_SUITE_P(SpinWindow, XStoreZmqBrokerParkExpiryTest,
                         ::testing::Values(std::chrono::microseconds(0), SPIN_WINDOW));
INSTANTIATE_TEST_SUITE_P(SpinWindow, XStoreZmqBrokerGoneTest,
                         ::testing::Values(std::chrono::microseconds(0), SPIN_WINDOW));

INSTANTIATE_TEST_SUITE_P(SpinWindow, XStoreZmqBrokerTest,
                         ::testing::Values(std::chrono::microseconds(0), SPIN_WINDOW),
                         [](const ::testing::TestParamInfo<std::chrono::microseconds> &info) {
                             return info.param.count() == 0 ? std::string("NoSpin") : std::string("Spin50ms");
                         });
INSTANTIATE_TEST_SUITE_P(SpinWindow, XStoreZmqBrokerHeavyTest,
                         ::testing::Values(std::chrono::microseconds(0), SPIN_WINDOW),
                         [](const ::testing::TestParamInfo<std::chrono::microseconds> &info) {
                             return info.param.count() == 0 ? std::string("NoSpin") : std::string("Spin50ms");
                         });

// ETERM must come out of the zero-timeout polls of the spin itself: with a window far longer than the bound below, a
// spin that kept polling after shutdown would only end after 30 s, whereas falling through to the blocking poll
// (which would throw) is not reachable until the window lapses.
TEST(XStoreZmqBrokerSpinShutdownTest, ShutdownInsideALongSpinWindowEndsWithEterm) {
    zmq::context_t ctx;
    zmq::socket_t frontend(ctx, zmq::socket_type::router);
    zmq::socket_t backend(ctx, zmq::socket_type::router);
    frontend.set(zmq::sockopt::linger, 0);
    backend.set(zmq::sockopt::linger, 0);
    frontend.bind("inproc://broker-test-long-spin-frontend");
    backend.bind("inproc://broker-test-long-spin-backend");
    std::future<int> broker_exit = std::async(std::launch::async, [&] {
        try {
            RunReadyBroker(frontend, backend, 1, std::chrono::seconds(30));
        } catch (const zmq::error_t &e) {
            return e.num();
        }
        return 0;
    });
    // Destroyed before broker_exit: a failed ASSERT below must not leave the future waiting for a live broker.
    struct ShutdownGuard {
        zmq::context_t &context;
        ~ShutdownGuard() { context.shutdown(); }
    } guard{ctx};
    {
        FakeWorker worker(ctx, "wA", "inproc://broker-test-long-spin-backend");
        zmq::socket_t client(ctx, zmq::socket_type::req);
        client.set(zmq::sockopt::linger, 0);
        client.set(zmq::sockopt::rcvtimeo, RECV_TIMEOUT_MS);
        client.connect("inproc://broker-test-long-spin-frontend");
        client.send(zmq::str_buffer("ping"), zmq::send_flags::none);
        const auto request = worker.Take();
        ASSERT_EQ(request.size(), 3u);
        worker.Reply(request, {"pong"});
        ASSERT_EQ(RecvFrames(client), std::vector<std::string>{"pong"});
        // The client has its reply while the broker may still be draining (a dontwait recv there would throw ETERM
        // too): give it time to finish the drain and enter the 30 s spin before shutting down.
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        ctx.shutdown();
        EXPECT_EQ(broker_exit.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    }
    if (broker_exit.valid()) EXPECT_EQ(broker_exit.get(), ETERM);
}

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
