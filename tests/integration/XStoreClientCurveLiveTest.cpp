// Opt-in: exercises XStoreClient's CURVE constructor against a real,
// --curve-enabled XStore server. Skipped by default (same convention as
// XStoreClientLiveTest.cpp) since it needs a live server plus a client
// identity that server has actually allowed -- set these env vars to run it:
//   XSTORE_CURVE_LIVE_ADDR, XSTORE_CURVE_LIVE_PORT
//   XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY   (path to an allowed client's .key)
//   XSTORE_CURVE_LIVE_SERVER_PUBKEY       (path to that server's .pub)
#include <gtest/gtest.h>
#include <cstdlib>
#include "XStore_Client.h"

namespace {
// Plain accessor (no GTEST_SKIP() here): GTEST_SKIP() expands to a `return`
// of a void-typed expression, which only compiles inside a function that
// itself returns void (a TEST body, or SetUp()). This helper returns
// `const char*`, so the skip decision is made in the TEST body below instead.
const char *GetEnvOrNull(const char *name) {
    const char *v = std::getenv(name);
    return (v && *v) ? v : nullptr;
}
}  // namespace

TEST(XStoreClientCurveLiveTest, ConnectsAndPingsWithCurveAuth) {
    const char *addr = GetEnvOrNull("XSTORE_CURVE_LIVE_ADDR");
    const char *port = GetEnvOrNull("XSTORE_CURVE_LIVE_PORT");
    const char *client_key = GetEnvOrNull("XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY");
    const char *server_pub = GetEnvOrNull("XSTORE_CURVE_LIVE_SERVER_PUBKEY");
    if (!addr) GTEST_SKIP() << "XSTORE_CURVE_LIVE_ADDR not set -- skipping live CURVE client test";
    if (!port) GTEST_SKIP() << "XSTORE_CURVE_LIVE_PORT not set -- skipping live CURVE client test";
    if (!client_key)
        GTEST_SKIP() << "XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY not set -- skipping live CURVE client test";
    if (!server_pub)
        GTEST_SKIP() << "XSTORE_CURVE_LIVE_SERVER_PUBKEY not set -- skipping live CURVE client test";

    auto auth = XStoreCurveClientAuth::LoadFromFiles(client_key, server_pub);
    XStoreClient client(addr, port, /*timeout_ms=*/3000, /*warmup_on_init=*/false, auth);
    EXPECT_NO_THROW(client.Ping());
}
