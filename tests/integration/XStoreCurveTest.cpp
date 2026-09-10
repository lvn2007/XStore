// Unit + end-to-end tests for the CURVE auth library (include/Networking/
// XStore_Curve.h). Lives in the xstore_network_tests binary since it already
// links libzmq + ZMQ_Server.cpp + the full XStore core -- this file adds
// XStore_Curve.cpp + XStore_SysStats.cpp to that target's sources (see
// tests/CMakeLists.txt) rather than duplicating the whole source list again
// in a fourth test binary.
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#include "include/Networking/XStore_Curve.h"
#include "XStoreTestSupport.h"

using namespace xstore_test;

TEST(XStoreCurveKeyPairTest, GeneratedKeyPairRoundTripsThroughZ85) {
    auto kp = XStoreCurveKeyPair::Generate();
    EXPECT_EQ(kp.public_key_z85.size(), 40u);
    EXPECT_EQ(kp.secret_key_z85.size(), 40u);
    EXPECT_NE(kp.public_key_z85, kp.secret_key_z85);

    auto raw = Z85Decode(kp.public_key_z85);
    EXPECT_EQ(Z85Encode(raw), kp.public_key_z85);
}

TEST(XStoreCurveKeyPairTest, SaveToFilesThenLoadFromSecretKeyFileRoundTrips) {
    ScratchDir scratch("xstore_curve_keypair_test");
    auto pub_path = scratch.test_root / "id.pub";
    auto sec_path = scratch.test_root / "id.key";

    auto original = XStoreCurveKeyPair::Generate();
    original.SaveToFiles(pub_path, sec_path);

    ASSERT_TRUE(std::filesystem::exists(pub_path));
    ASSERT_TRUE(std::filesystem::exists(sec_path));
    auto sec_perms = std::filesystem::status(sec_path).permissions();
    EXPECT_EQ(sec_perms & std::filesystem::perms::group_read, std::filesystem::perms::none);
    EXPECT_EQ(sec_perms & std::filesystem::perms::others_read, std::filesystem::perms::none);

    auto loaded = XStoreCurveKeyPair::LoadFromSecretKeyFile(sec_path);
    EXPECT_EQ(loaded.public_key_z85, original.public_key_z85);
    EXPECT_EQ(loaded.secret_key_z85, original.secret_key_z85);
}

TEST(XStoreCurveKeyPairTest, LoadFromSecretKeyFileThrowsWhenSiblingPubMissing) {
    ScratchDir scratch("xstore_curve_keypair_missing_pub_test");
    auto sec_path = scratch.test_root / "orphan.key";
    { std::ofstream f(sec_path); f << std::string(40, 'a'); }
    EXPECT_THROW(XStoreCurveKeyPair::LoadFromSecretKeyFile(sec_path), std::runtime_error);
}

#include "include/XStore/XStore_Tunables.h"

TEST(FilesystemKeyStoreTest, UnknownKeyIsNotAllowed) {
    ScratchDir scratch("fks_unknown_test");
    std::filesystem::create_directories(scratch.test_root / "allowed");
    FilesystemKeyStore store(scratch.test_root / "allowed", std::chrono::milliseconds(0));

    auto kp = XStoreCurveKeyPair::Generate();
    std::string name;
    EXPECT_FALSE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));
}

TEST(FilesystemKeyStoreTest, AllowedKeyIsFoundWithCorrectName) {
    ScratchDir scratch("fks_allowed_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);

    auto kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(allowed_dir / "alice.pub", kp.public_key_z85, /*is_secret=*/false);

    FilesystemKeyStore store(allowed_dir, std::chrono::milliseconds(0));
    std::string name;
    ASSERT_TRUE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));
    EXPECT_EQ(name, "alice");
}

TEST(FilesystemKeyStoreTest, RevokeThenRefreshDeniesSubsequentLookup) {
    ScratchDir scratch("fks_revoke_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);

    auto kp = XStoreCurveKeyPair::Generate();
    auto pub_path = allowed_dir / "bob.pub";
    Z85WriteFile(pub_path, kp.public_key_z85, /*is_secret=*/false);

    FilesystemKeyStore store(allowed_dir, std::chrono::milliseconds(0));
    std::string name;
    ASSERT_TRUE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));

    std::filesystem::remove(pub_path);
    // No sleep needed: MaybeRescan() no longer gates on directory mtime (see
    // XStore_Curve.h/.cpp), so Refresh() unconditionally re-lists allowed_dir
    // and picks up the removal immediately.
    store.Refresh();

    EXPECT_FALSE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));
}

TEST(FilesystemKeyStoreTest, MalformedFileIsSkippedWithoutBreakingOtherEntries) {
    ScratchDir scratch("fks_malformed_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);

    auto good_kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(allowed_dir / "good.pub", good_kp.public_key_z85, /*is_secret=*/false);
    { std::ofstream bad(allowed_dir / "bad.pub"); bad << "not-a-valid-z85-key"; }

    FilesystemKeyStore store(allowed_dir, std::chrono::milliseconds(0));
    std::string name;
    EXPECT_TRUE(store.IsAllowed(Z85Decode(good_kp.public_key_z85), &name));
    EXPECT_EQ(name, "good");
}

#include <zmq.hpp>
#include <zmq_addon.hpp>

namespace {
// Minimal CURVE-enabled ROUTER<->DEALER pair, no ZmqServer/XStore involved
// -- isolates ZAP correctness from the full server integration (Task 5).
struct CurvePair {
    zmq::context_t ctx{1};
    zmq::socket_t router{ctx, zmq::socket_type::router};
    XStoreCurveKeyPair server_kp = XStoreCurveKeyPair::Generate();

    explicit CurvePair(std::shared_ptr<KeyStore> store) : zap_(ctx, std::move(store)) {
        router.set(zmq::sockopt::curve_server, true);
        router.set(zmq::sockopt::curve_secretkey, server_kp.secret_key_z85);
        router.set(zmq::sockopt::zap_domain, "xstore-test");
        router.set(zmq::sockopt::rcvtimeo, 500);
        router.set(zmq::sockopt::linger, 0);
        zap_.Start();
        // A fixed-but-spread port (same pattern XStoreZmqNetworkTest and the
        // later ZmqServerCurveTest fixture use), not bind-to-port-0 --
        // avoids depending on an unverified ephemeral-port/last_endpoint
        // query API in this vendored cppzmq version.
        int port = 25900 + static_cast<int>(getpid() % 500);
        endpoint_ = "tcp://127.0.0.1:" + std::to_string(port);
        router.bind(endpoint_);
    }

    zmq::socket_t MakeClient(const XStoreCurveKeyPair &client_kp) {
        zmq::socket_t dealer(ctx, zmq::socket_type::dealer);
        dealer.set(zmq::sockopt::curve_serverkey, server_kp.public_key_z85);
        dealer.set(zmq::sockopt::curve_publickey, client_kp.public_key_z85);
        dealer.set(zmq::sockopt::curve_secretkey, client_kp.secret_key_z85);
        dealer.set(zmq::sockopt::rcvtimeo, 500);
        dealer.set(zmq::sockopt::linger, 0);
        dealer.connect(endpoint_);
        return dealer;
    }

    std::string endpoint_;
    XStoreZapHandler zap_;
};
}  // namespace

TEST(XStoreZapHandlerTest, AllowedClientHandshakeSucceeds) {
    ScratchDir scratch("zap_allow_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);
    auto client_kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(allowed_dir / "alice.pub", client_kp.public_key_z85, false);

    auto store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));
    CurvePair pair(store);
    auto client = pair.MakeClient(client_kp);

    client.send(zmq::str_buffer("ping"), zmq::send_flags::none);
    std::vector<zmq::message_t> frames;
    ASSERT_TRUE(zmq::recv_multipart(pair.router, std::back_inserter(frames)));
    ASSERT_GE(frames.size(), 2u);
    EXPECT_EQ(frames.back().to_string(), "ping");
}

TEST(XStoreZapHandlerTest, UnlistedClientHandshakeIsDenied) {
    ScratchDir scratch("zap_deny_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);  // empty -- nobody allowed

    auto store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));
    CurvePair pair(store);
    auto stranger_kp = XStoreCurveKeyPair::Generate();
    auto client = pair.MakeClient(stranger_kp);

    client.send(zmq::str_buffer("ping"), zmq::send_flags::none);
    std::vector<zmq::message_t> frames;
    // Handshake never completes -- ZAP-denied, so the ROUTER never sees a
    // message from this peer within the timeout.
    EXPECT_FALSE(zmq::recv_multipart(pair.router, std::back_inserter(frames)));
}

TEST(XStoreZapHandlerTest, MalformedRequestGetsReplyAndDoesNotDesyncSocket) {
    // Talks to the ZAP responder directly (REQ <-> its REP socket) rather
    // than through a full CURVE handshake, so a too-short request can be
    // crafted by hand. Before the fix, request.size() < 7 dropped the
    // request without replying, which desyncs a REP socket's recv/send
    // state machine -- the NEXT recv_multipart() call (for any future ZAP
    // request, from any client) throws zmq::error_t(EFSM) uncaught out of
    // the handler thread, calling std::terminate() and crashing the whole
    // process. This test would hang/crash on the second recv below if that
    // regressed.
    ScratchDir scratch("zap_malformed_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);
    auto store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));
    zmq::context_t ctx{1};
    XStoreZapHandler zap(ctx, store);
    zap.Start();

    zmq::socket_t req(ctx, zmq::socket_type::req);
    req.set(zmq::sockopt::rcvtimeo, 500);
    req.set(zmq::sockopt::linger, 0);
    req.connect("inproc://zeromq.zap.01");

    // Pathologically short (1-frame) request -- fewer than the 6 frames
    // RFC 27 requires to extract version/sequence/mechanism.
    zmq::multipart_t bad;
    bad.addstr("1.0");
    bad.send(req);

    std::vector<zmq::message_t> reply1;
    ASSERT_TRUE(zmq::recv_multipart(req, std::back_inserter(reply1)));
    ASSERT_GE(reply1.size(), 3u);
    EXPECT_EQ(reply1[2].to_string(), "500");

    // Socket must still be healthy: a well-formed 6-frame NULL-mechanism
    // request (the true RFC 27 minimum) gets a proper reply, not a hang.
    zmq::multipart_t good;
    good.addstr("1.0");
    good.addstr("2");
    good.addstr("domain");
    good.addstr("127.0.0.1");
    good.addstr("identity");
    good.addstr("NULL");
    good.send(req);

    std::vector<zmq::message_t> reply2;
    ASSERT_TRUE(zmq::recv_multipart(req, std::back_inserter(reply2)));
    ASSERT_GE(reply2.size(), 3u);
    EXPECT_EQ(reply2[1].to_string(), "2");
    EXPECT_EQ(reply2[2].to_string(), "400");  // NULL mechanism denied, not crashed
}

#include "include/Networking/ZMQ_Server.h"

namespace {
struct CurveServerFixture {
    ScratchDir scratch{"curve_zmq_server_test"};
    XStoreCurveKeyPair server_kp = XStoreCurveKeyPair::Generate();
    std::filesystem::path allowed_dir = scratch.test_root / "allowed";
    std::shared_ptr<FilesystemKeyStore> store;
    std::string server_addr;

    CurveServerFixture() {
        std::filesystem::create_directories(allowed_dir);
        store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));

        // Each fixture leaves its server socket bound forever (ZmqServer has
        // no shutdown API -- StartServer() blocks in zmq::proxy(), same
        // pattern documented at the top of XStoreZmqNetworkTest.cpp), and
        // all 3 ZmqServerCurveTest cases below share one process when run
        // via --gtest_filter (unlike plain `ctest`, which forks one process
        // per test). A port keyed only on getpid() is therefore identical
        // across all 3 fixture instances in that process, so the 2nd/3rd
        // bind() hits EADDRINUSE -- uncaught out of the detached server
        // thread, so it's a std::terminate() crash, not a test failure. A
        // static per-process counter gives every instance a distinct port;
        // the base is chosen clear of this suite's other fixed ranges
        // (CurvePair: 25900-26399, XStoreZmqNetworkTest: 25500-29499,
        // XStoreZmqConcurrentTest: 26500-30499).
        static std::atomic<int> instance_counter{0};
        int port = 31000 + static_cast<int>(getpid() % 1000) * 4 + instance_counter.fetch_add(1);
        server_addr = "tcp://127.0.0.1:" + std::to_string(port);

        auto *server = new ZmqServer(1);
        std::thread server_thread([server, port, this]() {
            server->StartServer("127.0.0.1", std::to_string(port), server_kp, store);
        });
        server_thread.detach();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    zmq::socket_t MakeClient(zmq::context_t &ctx, const XStoreCurveKeyPair &client_kp) {
        zmq::socket_t sock(ctx, zmq::socket_type::req);
        sock.set(zmq::sockopt::curve_serverkey, server_kp.public_key_z85);
        sock.set(zmq::sockopt::curve_publickey, client_kp.public_key_z85);
        sock.set(zmq::sockopt::curve_secretkey, client_kp.secret_key_z85);
        sock.set(zmq::sockopt::rcvtimeo, 2000);
        sock.set(zmq::sockopt::linger, 0);
        sock.connect(server_addr);
        return sock;
    }
};
}  // namespace

TEST(ZmqServerCurveTest, AllowedClientGetsPingReply) {
    CurveServerFixture fx;
    auto client_kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(fx.allowed_dir / "alice.pub", client_kp.public_key_z85, false);

    zmq::context_t ctx(1);
    auto sock = fx.MakeClient(ctx, client_kp);

    XStore_Proto::Request ping_req;
    ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
    std::string payload = ping_req.SerializeAsString();
    ASSERT_TRUE(sock.send(zmq::buffer(payload), zmq::send_flags::none));

    zmq::message_t reply;
    ASSERT_TRUE(sock.recv(reply, zmq::recv_flags::none));
}

TEST(ZmqServerCurveTest, UnlistedClientTimesOut) {
    CurveServerFixture fx;  // nobody allowed
    auto stranger_kp = XStoreCurveKeyPair::Generate();

    zmq::context_t ctx(1);
    auto sock = fx.MakeClient(ctx, stranger_kp);

    XStore_Proto::Request ping_req;
    ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
    std::string payload = ping_req.SerializeAsString();
    ASSERT_TRUE(sock.send(zmq::buffer(payload), zmq::send_flags::none));

    zmq::message_t reply;
    EXPECT_FALSE(sock.recv(reply, zmq::recv_flags::none));
}

TEST(ZmqServerCurveTest, RevokedClientDeniedOnNextConnection) {
    CurveServerFixture fx;
    auto client_kp = XStoreCurveKeyPair::Generate();
    auto pub_path = fx.allowed_dir / "bob.pub";
    Z85WriteFile(pub_path, client_kp.public_key_z85, false);

    {
        zmq::context_t ctx(1);
        auto sock = fx.MakeClient(ctx, client_kp);
        XStore_Proto::Request ping_req;
        ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
        std::string payload = ping_req.SerializeAsString();
        ASSERT_TRUE(sock.send(zmq::buffer(payload), zmq::send_flags::none));
        zmq::message_t reply;
        ASSERT_TRUE(sock.recv(reply, zmq::recv_flags::none));
    }

    std::filesystem::remove(pub_path);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    zmq::context_t ctx2(1);
    auto sock2 = fx.MakeClient(ctx2, client_kp);  // fresh connection, same identity
    XStore_Proto::Request ping_req2;
    ping_req2.mutable_ping()->set_rtype(XStore_Proto::PING);
    std::string payload2 = ping_req2.SerializeAsString();
    ASSERT_TRUE(sock2.send(zmq::buffer(payload2), zmq::send_flags::none));
    zmq::message_t reply2;
    EXPECT_FALSE(sock2.recv(reply2, zmq::recv_flags::none));
}
