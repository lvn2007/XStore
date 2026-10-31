// Opt-in: exercises XStoreClient's CURVE constructor against a real,
// --curve-enabled XStore server. Skipped by default (same convention as
// XStoreClientLiveTest.cpp) since it needs a live server plus a client
// identity that server has actually allowed -- set these env vars to run it:
//   XSTORE_CURVE_LIVE_ADDR, XSTORE_CURVE_LIVE_PORT
//   XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY   (path to an allowed client's .key)
//   XSTORE_CURVE_LIVE_SERVER_PUBKEY       (path to that server's .pub)
#include <gtest/gtest.h>
#include <cstdlib>
#include <unistd.h>
#include <vector>
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

// A row reply is received on a second (DEALER) socket that must carry the same CURVE identity as the main one: insert enough rows for a reply
// of several chunks and read it back through that socket.
TEST(XStoreClientCurveLiveTest, AStreamedRowReplyRoundTripsWithCurveAuth) {
    const char *addr = GetEnvOrNull("XSTORE_CURVE_LIVE_ADDR");
    const char *port = GetEnvOrNull("XSTORE_CURVE_LIVE_PORT");
    const char *client_key = GetEnvOrNull("XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY");
    const char *server_pub = GetEnvOrNull("XSTORE_CURVE_LIVE_SERVER_PUBKEY");
    if (!addr || !port || !client_key || !server_pub) GTEST_SKIP() << "XSTORE_CURVE_LIVE_* not all set -- skipping live CURVE client test";

    auto auth = XStoreCurveClientAuth::LoadFromFiles(client_key, server_pub);
    XStoreClient client(addr, port, /*timeout_ms=*/10000, /*warmup_on_init=*/false, auth);
    const std::string db_name = "LIVE_CURVE_STREAM_" + std::to_string(getpid());
    ASSERT_NE(client.CreateDB(db_name, XStore_Proto::SECOND, 2048).find("[INFO]"), std::string::npos);

    constexpr uint64_t T0 = 1592222400ULL;   // 2020
    constexpr size_t ROWS = 2000;            // about 2 MB: several 512 KiB chunks
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    for (size_t i = 0; i < ROWS; ++i) {
        XStore_Proto::RowData row;
        *row.add_fielddata() = xstore_fdata::String(std::string(1000, static_cast<char>('a' + i % 26)));
        rows.emplace_back(T0 + i, std::move(row));
    }
    EXPECT_NE(client.BatchInsert(db_name, rows, XStore_Proto::SYNC).find("Inserted 2000 elements"), std::string::npos);

    const auto reply = client.RangeQuery(T0, T0 + ROWS - 1, db_name);
    ASSERT_EQ(reply->queryresponse_size(), static_cast<int>(ROWS)) << reply->querystatus();
    for (size_t i = 0; i < ROWS; i += 499) {
        EXPECT_EQ(reply->queryresponse(static_cast<int>(i)).timestamp(), T0 + i);
        EXPECT_EQ(reply->queryresponse(static_cast<int>(i)).data().fielddata(0).stringdata(), std::string(1000, static_cast<char>('a' + i % 26)));
    }

    (void)client.RemoveDBFiles(db_name, {"2020.XSTORE"});
    (void)client.RemoveDB(db_name);
}
