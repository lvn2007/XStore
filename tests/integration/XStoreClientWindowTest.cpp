// Reads bigger than memory (a range or a list of timestamps read as windows, each an ordinary streamed call, the next window fetched while the
// current one is consumed), and the calls that send many requests without waiting for each answer (BatchInsertPipelined, a split BatchDelete),
// against a fake server.
#include <gtest/gtest.h>

#include <chrono>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "XStore_Client.h"
#include "XStoreClientTestServer.h"

namespace {
using xstore_test_client::AnsweringServer;

XStore_Proto::UnaryQuery_REP Row(uint64_t ts) {
    XStore_Proto::UnaryQuery_REP row;
    row.set_timestamp(ts);
    row.mutable_data()->add_fielddata()->set_int64data(static_cast<int64_t>(ts));
    return row;
}

// The fake database: every timestamp has a row, RangeQuery returns one per timestamp of the range, BatchQuery the requested ones, FindByValue
// and FindByPattern the even ones of the range; INFO says DB has 100-byte rows.
std::string Answer(size_t, const XStore_Proto::Request &request) {
    switch (request.payload_case()) {
        case XStore_Proto::Request::kRangequery: {
            XStore_Proto::RangeQuery_REP reply;
            for (uint64_t t = request.rangequery().starttime(); t <= request.rangequery().endtime(); ++t) *reply.add_queryresponse() = Row(t);
            return reply.SerializeAsString();
        }
        case XStore_Proto::Request::kBatchquery: {
            XStore_Proto::BatchQuery_REP reply;
            for (uint64_t t : request.batchquery().timestamp()) *reply.add_queryresponse() = Row(t);
            return reply.SerializeAsString();
        }
        case XStore_Proto::Request::kFindbyvalue: {
            XStore_Proto::FindByValue_REP reply;
            for (uint64_t t = request.findbyvalue().starttime(); t <= request.findbyvalue().endtime(); ++t) {
                if (t % 2 == 0) *reply.add_queryresponse() = Row(t);
            }
            return reply.SerializeAsString();
        }
        case XStore_Proto::Request::kFindbypattern: {
            XStore_Proto::FindByPattern_REP reply;
            for (uint64_t t = request.findbypattern().starttime(); t <= request.findbypattern().endtime(); ++t) {
                if (t % 2 == 0) *reply.add_queryresponse() = Row(t);
            }
            return reply.SerializeAsString();
        }
        case XStore_Proto::Request::kInfo: {
            XStore_Proto::SysInfo_REP reply;
            auto *db = reply.add_dbinfo();
            db->set_dbname("DB");
            db->set_rowlength(100);
            return reply.SerializeAsString();
        }
        case XStore_Proto::Request::kBatchinsert: {
            XStore_Proto::BatchInsert_REP reply;
            reply.set_insertstatus("[INFO] [BATCH INSERT] [DB: DB] - Inserted " + std::to_string(request.batchinsert().batchinsert_data_size()) +
                                   " elements successfully");
            return reply.SerializeAsString();
        }
        case XStore_Proto::Request::kBatchdelete: {
            XStore_Proto::BatchDelete_REP reply;
            reply.set_deletestatus("[INFO] [BATCH DELETE] [DB: DB] - Deleted " + std::to_string(request.batchdelete().timestamp_size()) + " items");
            return reply.SerializeAsString();
        }
        default:
            return std::string();
    }
}

constexpr size_t WINDOW_BYTES = 11600;   // with 100-byte rows (+16): 100 timestamps a window

xstore_client::WindowOptions Options(bool prefetch, int32_t row_length = 100) {
    xstore_client::WindowOptions options;
    options.window_bytes = WINDOW_BYTES;
    options.prefetch = prefetch;
    options.row_length = row_length;
    return options;
}

// ---- how big a window is ----

TEST(WindowSizeTest, AWindowHoldsAsManyTimestampsAsFitInItsBytesAtTheRowsFullSize) {
    EXPECT_EQ(xstore_client::WindowTimestamps(11600, 100), 100u);
    EXPECT_EQ(xstore_client::WindowTimestamps(1, 100), 1u) << "never an empty window";
    EXPECT_EQ(xstore_client::WindowTimestamps(256ull << 20, 1420), (256ull << 20) / 1436);
}

TEST(WindowSizeTest, TheDefaultWindowIsATwelfthOfTheBudgetBetween4And256MiB) {
    EXPECT_EQ(xstore_client::DefaultWindowBytes(std::nullopt), 256ull << 20) << "no budget: the cap";
    EXPECT_EQ(xstore_client::DefaultWindowBytes(std::optional<size_t>(60ull << 30)), 256ull << 20);
    EXPECT_EQ(xstore_client::DefaultWindowBytes(std::optional<size_t>(1200ull << 20)), 100ull << 20) << "two windows in flight, ~6x as objects";
    EXPECT_EQ(xstore_client::DefaultWindowBytes(std::optional<size_t>(1)), 4ull << 20) << "a floor";
}

// ---- windowed reads ----

void ExpectContiguousWindows(const std::vector<std::vector<uint64_t>> &windows, uint64_t first, uint64_t last, size_t per_window) {
    uint64_t next = first;
    for (size_t w = 0; w < windows.size(); ++w) {
        ASSERT_FALSE(windows[w].empty()) << "window " << w;
        if (w + 1 < windows.size()) EXPECT_EQ(windows[w].size(), per_window) << "window " << w;
        for (uint64_t ts : windows[w]) EXPECT_EQ(ts, next++);
    }
    EXPECT_EQ(next, last + 1) << "every timestamp once, in order";
}

TEST(ClientWindowedReadTest, RangeQueryEachDeliversTheRangeInOrderedWindows) {
    for (bool prefetch : {false, true}) {
        AnsweringServer server(1000, Answer);
        XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
        std::vector<std::vector<uint64_t>> windows;
        const uint64_t rows = client.RangeQueryEach(5, 1054, "DB", [&](const XStore_Proto::RangeQuery_REP &window) {
            windows.emplace_back();
            for (const auto &row : window.queryresponse()) windows.back().push_back(row.timestamp());
        }, Options(prefetch));
        EXPECT_EQ(rows, 1050u) << "prefetch=" << prefetch;
        ASSERT_EQ(windows.size(), 11u) << "10 windows of 100 and one of 50, prefetch=" << prefetch;
        ExpectContiguousWindows(windows, 5, 1054, 100);
        EXPECT_EQ(windows.back().size(), 50u);
    }
}

TEST(ClientWindowedReadTest, TheNextWindowIsFetchedWhileTheCurrentOneIsBeingConsumed) {
    AnsweringServer server(1000, Answer);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    bool second_request_arrived_during_the_first_callback = false;
    int window = 0;
    (void)client.RangeQueryEach(0, 399, "DB", [&](const XStore_Proto::RangeQuery_REP &) {
        if (window++ == 0) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while (std::chrono::steady_clock::now() < deadline && server.requests().size() < 2) std::this_thread::sleep_for(std::chrono::milliseconds(2));
            second_request_arrived_during_the_first_callback = server.requests().size() >= 2;
        }
    }, Options(/*prefetch=*/true));
    EXPECT_TRUE(second_request_arrived_during_the_first_callback);
}

TEST(ClientWindowedReadTest, ACallbackThatThrowsStopsTheReadAndTheClientStillWorks) {
    AnsweringServer server(1000, Answer);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    int calls = 0;
    EXPECT_THROW((void)client.RangeQueryEach(0, 999, "DB", [&](const XStore_Proto::RangeQuery_REP &) {
        if (++calls == 2) throw std::runtime_error("consumer failed");
    }, Options(true)), std::runtime_error);
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(client.RangeQuery(0, 9, "DB")->queryresponse_size(), 10);
}

TEST(ClientWindowedReadTest, TheRowLengthIsLookedUpFromTheServerWhenNotGiven) {
    AnsweringServer server(1000, Answer);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    size_t windows = 0;
    (void)client.RangeQueryEach(0, 299, "db", [&](const XStore_Proto::RangeQuery_REP &) { ++windows; }, Options(false, /*row_length=*/0));
    EXPECT_EQ(windows, 3u);
    const auto requests = server.requests();
    EXPECT_TRUE(std::any_of(requests.begin(), requests.end(), [](const auto &r) { return r.payload_case() == XStore_Proto::Request::kInfo; }));
}

TEST(ClientWindowedReadTest, AnEmptyOrBackwardsRangeIsRefused) {
    AnsweringServer server(10, Answer);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    EXPECT_THROW((void)client.RangeQueryEach(10, 5, "DB", [](const XStore_Proto::RangeQuery_REP &) {}, Options(false)), std::invalid_argument);
}

TEST(ClientWindowedReadTest, BatchQueryEachDeliversTheTimestampsInWindowsOfTheirCount) {
    for (bool prefetch : {false, true}) {
        AnsweringServer server(1000, Answer);
        XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
        std::vector<uint64_t> timestamps;
        for (uint64_t t = 0; t < 450; ++t) timestamps.push_back(1000 - t * 2);   // descending
        std::vector<std::vector<uint64_t>> windows;
        const uint64_t rows = client.BatchQueryEach("DB", timestamps, /*sort_time=*/true, [&](const XStore_Proto::BatchQuery_REP &window) {
            windows.emplace_back();
            for (const auto &row : window.queryresponse()) windows.back().push_back(row.timestamp());
        }, Options(prefetch));
        EXPECT_EQ(rows, 450u);
        ASSERT_EQ(windows.size(), 5u);
        std::vector<uint64_t> flat;
        for (const auto &w : windows) flat.insert(flat.end(), w.begin(), w.end());
        EXPECT_TRUE(std::is_sorted(flat.begin(), flat.end())) << "sort_time: sorted across windows, not just within one";
        EXPECT_EQ(flat.size(), 450u);
    }
}

TEST(ClientWindowedReadTest, FindByValueEachAndFindByPatternEachDeliverTheMatchesInOrderedWindows) {
    AnsweringServer server(1000, Answer);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    std::vector<uint64_t> by_value, by_pattern;
    const uint64_t a = client.FindByValueEach(0, 999, "DB", 0, xstore_fdata::Int64(1), [&](const XStore_Proto::FindByValue_REP &window) {
        for (const auto &row : window.queryresponse()) by_value.push_back(row.timestamp());
    }, Options(true));
    const uint64_t b = client.FindByPatternEach(0, 999, "DB", 0, "x%", XStore_Proto::PATTERN_LIKE, false, [&](const XStore_Proto::FindByPattern_REP &window) {
        for (const auto &row : window.queryresponse()) by_pattern.push_back(row.timestamp());
    }, Options(false));
    EXPECT_EQ(a, 500u);
    EXPECT_EQ(b, 500u);
    EXPECT_EQ(by_value, by_pattern);
    EXPECT_TRUE(std::is_sorted(by_value.begin(), by_value.end()));
    for (uint64_t ts : by_value) EXPECT_EQ(ts % 2, 0u);
}

// ---- requests sent without waiting for each answer ----

std::vector<std::pair<uint64_t, XStore_Proto::RowData>> InsertRows(size_t n) {
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    for (size_t i = 0; i < n; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_bytesdata(std::string(1000, 'x'));
        rows.emplace_back(1000 + i, std::move(row));
    }
    return rows;
}

TEST(ClientPipelinedTest, BatchInsertPipelinedKeepsSeveralPartsInFlightAndCountsThemAll) {
    AnsweringServer server(1000, Answer, /*hold=*/4);   // answers only once four requests are waiting
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMaxRequestBytes(30000);
    const auto rows = InsertRows(300);   // ~300 KB: 10+ parts
    const std::string status = client.BatchInsertPipelined("DB", rows, XStore_Proto::ASYNC);
    EXPECT_GE(server.max_pending(), 4u) << "four parts were sent before any answer was read";
    EXPECT_NE(status.find("[INFO]"), std::string::npos) << status;
    EXPECT_NE(status.find("Inserted 300 elements successfully"), std::string::npos) << status;
    size_t seen = 0;
    for (const auto &request : server.requests()) seen += static_cast<size_t>(request.batchinsert().batchinsert_data_size());
    EXPECT_EQ(seen, 300u) << "every row sent once";
}

TEST(ClientPipelinedTest, ASmallBatchInsertPipelinedIsOneRequest) {
    AnsweringServer server(1000, Answer);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    EXPECT_EQ(client.BatchInsertPipelined("DB", InsertRows(20), XStore_Proto::ASYNC), "[INFO] [BATCH INSERT] [DB: DB] - Inserted 20 elements successfully");
    EXPECT_EQ(server.requests().size(), 1u);
}

TEST(ClientPipelinedTest, AFailedPartIsReportedAndNoMoreAreSent) {
    AnsweringServer server(1000, [](size_t index, const XStore_Proto::Request &r) {
        if (index == 1) {
            XStore_Proto::BatchInsert_REP reply;
            reply.set_insertstatus("[ERROR] [BATCH INSERT] [DB: DB] - the write failed");
            return reply.SerializeAsString();
        }
        return Answer(index, r);
    });
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMaxRequestBytes(30000);
    EXPECT_EQ(client.BatchInsertPipelined("DB", InsertRows(300), XStore_Proto::ASYNC), "[ERROR] [BATCH INSERT] [DB: DB] - the write failed");
    EXPECT_LT(server.requests().size(), 11u) << "it stopped sending once the failure was read";
}

TEST(ClientPipelinedTest, ABatchDeleteThatFitsIsOneRequestAndItsStatusPassesThrough) {
    AnsweringServer server(1000, Answer);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    std::vector<uint64_t> timestamps(50);
    for (size_t i = 0; i < timestamps.size(); ++i) timestamps[i] = i;
    EXPECT_EQ(client.BatchDelete("DB", timestamps), "[INFO] [BATCH DELETE] [DB: DB] - Deleted 50 items");
    EXPECT_EQ(server.requests().size(), 1u);
}

TEST(ClientPipelinedTest, ABigBatchDeleteIsSplitSentPipelinedAndItsCountsAreAdded) {
    AnsweringServer server(1000, Answer, /*hold=*/4);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMaxRequestBytes(200);   // 10 bytes a timestamp: 20 timestamps a part
    std::vector<uint64_t> timestamps(100);
    for (size_t i = 0; i < timestamps.size(); ++i) timestamps[i] = 5000 + i;
    const std::string status = client.BatchDelete("DB", timestamps);
    EXPECT_NE(status.find("Deleted 100 items"), std::string::npos) << status;
    EXPECT_NE(status.find("(5 requests)"), std::string::npos) << status;
    EXPECT_GE(server.max_pending(), 4u);
    std::set<uint64_t> seen;
    for (const auto &request : server.requests()) {
        EXPECT_LE(request.batchdelete().timestamp_size(), 20);
        for (uint64_t t : request.batchdelete().timestamp()) seen.insert(t);
    }
    EXPECT_EQ(seen.size(), 100u);
}

}  // namespace
