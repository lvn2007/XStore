// A BatchInsert too big for one protobuf message (2 GB) is sent as several requests, in order, each acknowledged before the next; the client
// against a fake server that parses each request and answers it the way the real server words its status.
#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "XStore_Client.h"
#include "XStoreClientTestServer.h"

namespace {

using xstore_test_client::AnsweringServer;

std::string Success(const XStore_Proto::Request &request) {
    XStore_Proto::BatchInsert_REP reply;
    reply.set_insertstatus("[INFO] [BATCH INSERT] [DB: " + request.batchinsert().dbname() + "] - Inserted " +
                           std::to_string(request.batchinsert().batchinsert_data_size()) + " elements successfully");
    return reply.SerializeAsString();
}

std::vector<std::pair<uint64_t, XStore_Proto::RowData>> Rows(size_t n, size_t bytes_each) {
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    for (size_t i = 0; i < n; ++i) {
        XStore_Proto::RowData row;
        *row.add_fielddata() = xstore_fdata::String(std::string(bytes_each, static_cast<char>('a' + i % 26)));
        rows.emplace_back(1000 + i, std::move(row));
    }
    return rows;
}

TEST(ClientBatchInsertSplitTest, ABatchThatFitsIsOneRequestAndItsStatusPassesThrough) {
    AnsweringServer server(1000, [](size_t, const XStore_Proto::Request &r) { return Success(r); });
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    const auto rows = Rows(50, 1000);
    const std::string status = client.BatchInsert("DB", rows, XStore_Proto::SYNC);
    EXPECT_EQ(status, "[INFO] [BATCH INSERT] [DB: DB] - Inserted 50 elements successfully");
    EXPECT_EQ(server.requests().size(), 1u);
}

TEST(ClientBatchInsertSplitTest, ABigBatchGoesInOrderedPartsEachWithinTheLimit) {
    const size_t limit = 30000;
    const auto rows = Rows(200, 1000);   // ~210 KB
    AnsweringServer server(1000, [](size_t, const XStore_Proto::Request &r) { return Success(r); });
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMaxRequestBytes(limit);
    const std::string status = client.BatchInsert("DB", rows, XStore_Proto::ASYNC);
    const auto requests = server.requests();
    const auto sizes = server.sizes();
    ASSERT_GE(requests.size(), 7u) << "210 KB in parts of at most 30 KB";
    size_t next = 0;
    for (size_t i = 0; i < requests.size(); ++i) {
        EXPECT_LE(sizes[i], limit + 1100) << "part " << i << ": within the limit by at most the one row that closed it";
        EXPECT_EQ(requests[i].batchinsert().durabilitymode(), XStore_Proto::ASYNC);
        for (const auto &entry : requests[i].batchinsert().batchinsert_data()) {
            ASSERT_LT(next, rows.size());
            EXPECT_EQ(entry.timestamp(), rows[next].first) << "rows in their original order, none repeated or lost";
            ++next;
        }
    }
    EXPECT_EQ(next, rows.size());
    EXPECT_NE(status.find("[INFO]"), std::string::npos) << status;
    EXPECT_NE(status.find("Inserted 200 elements successfully"), std::string::npos) << status;
    EXPECT_NE(status.find(std::to_string(requests.size()) + " requests"), std::string::npos) << status;
}

TEST(ClientBatchInsertSplitTest, AFailedPartStopsTheRestAndItsStatusIsReturned) {
    const auto rows = Rows(200, 1000);
    AnsweringServer server(1000, [](size_t index, const XStore_Proto::Request &r) {
        if (index == 1) {
            XStore_Proto::BatchInsert_REP reply;
            reply.set_insertstatus("[ERROR] [BATCH INSERT] [DB: DB] - the write failed");
            return reply.SerializeAsString();
        }
        return Success(r);
    });
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMaxRequestBytes(30000);
    EXPECT_EQ(client.BatchInsert("DB", rows, XStore_Proto::SYNC), "[ERROR] [BATCH INSERT] [DB: DB] - the write failed");
    EXPECT_EQ(server.requests().size(), 2u) << "nothing is sent after a part failed";
}

TEST(ClientBatchInsertSplitTest, ARowBiggerThanTheLimitGoesAlone) {
    const auto rows = Rows(20, 1000);
    AnsweringServer server(1000, [](size_t, const XStore_Proto::Request &r) { return Success(r); });
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMaxRequestBytes(100);
    const std::string status = client.BatchInsert("DB", rows, XStore_Proto::ASYNC);
    ASSERT_EQ(server.requests().size(), 20u);
    for (const auto &request : server.requests()) EXPECT_EQ(request.batchinsert().batchinsert_data_size(), 1);
    EXPECT_NE(status.find("Inserted 20 elements successfully"), std::string::npos) << status;
}

}  // namespace
