// Exercises XStoreClient's warmupOnInit flag in isolation from the server.
// Deliberately does NOT link against the server-side protobuf sources (see
// tests/CMakeLists.txt) -- XStoreClient pulls its own generated XStore.pb.h
// copy via the XStoreClient CMake target, and mixing that with the server's
// separately-generated copy in one binary would be an ODR violation.
//
// warmupOnInit's only observable effect is *when* the ZMTP handshake fires:
// with it on, the constructor eagerly sends a PING and throws immediately if
// nothing answers; with it off, ZMQ's lazy connect() means construction
// never talks to the network at all, so it can't throw here regardless of
// whether anything is listening.
#include <gtest/gtest.h>

#include "XStore_Client.h"

namespace {
// Port nobody's listening on for the duration of this test binary.
constexpr const char *K_UNREACHABLE_ADDR = "127.0.0.1";
constexpr const char *K_UNREACHABLE_PORT = "1";
constexpr int K_SHORT_TIMEOUT_MS = 200;
}  // namespace

TEST(XStoreClientWarmupTest, WarmupOnInitTrueThrowsWhenServerUnreachable) {
    EXPECT_THROW(
        XStoreClient(K_UNREACHABLE_ADDR, K_UNREACHABLE_PORT, K_SHORT_TIMEOUT_MS, /*warmupOnInit=*/true),
        std::runtime_error);
}

TEST(XStoreClientWarmupTest, WarmupOnInitFalseDoesNotThrowEvenWhenServerUnreachable) {
    EXPECT_NO_THROW(
        XStoreClient(K_UNREACHABLE_ADDR, K_UNREACHABLE_PORT, K_SHORT_TIMEOUT_MS, /*warmupOnInit=*/false));
}

TEST(XStoreClientWarmupTest, WarmupDefaultsToOnMatchingPreviousBehavior) {
    EXPECT_THROW(
        XStoreClient(K_UNREACHABLE_ADDR, K_UNREACHABLE_PORT, K_SHORT_TIMEOUT_MS),
        std::runtime_error);
}
