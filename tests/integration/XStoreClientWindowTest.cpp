// Reads bigger than memory (a range or a list of timestamps read as windows, each an ordinary streamed call, the next window fetched while the
// current one is consumed), and the calls that send many requests without waiting for each answer (BatchInsertPipelined, a split BatchDelete),
// against a fake server.
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
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

// ---- FindBy*Each windows grow while the matches are sparse ----

// A fake database where FindByValue returns the timestamps of `matches` that lie in the requested range.
std::function<std::string(size_t, const XStore_Proto::Request &)> MatchesAnswer(std::set<uint64_t> matches) {
    return [matches = std::move(matches)](size_t, const XStore_Proto::Request &request) {
        XStore_Proto::FindByValue_REP reply;
        if (request.payload_case() == XStore_Proto::Request::kFindbyvalue) {
            for (auto it = matches.lower_bound(request.findbyvalue().starttime()); it != matches.end() && *it <= request.findbyvalue().endtime(); ++it) {
                *reply.add_queryresponse() = Row(*it);
            }
        }
        return reply.SerializeAsString();
    };
}

// The ranges of the FindByValue requests the server saw, in the order it saw them: {start, end} each.
std::vector<std::pair<uint64_t, uint64_t>> RangesSeen(AnsweringServer &server) {
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    for (const auto &request : server.requests()) ranges.emplace_back(request.findbyvalue().starttime(), request.findbyvalue().endtime());
    return ranges;
}

// Windows of 100 timestamps to begin with (WINDOW_BYTES, 100-byte rows); with this budget a window never goes over 1600 timestamps (a quarter of the
// budget at the rows' full size).
constexpr size_t SMALL_BUDGET = 4 * 116 * 1600;

void ExpectTheRangeIsCoveredOnceInOrder(const std::vector<std::pair<uint64_t, uint64_t>> &ranges, uint64_t first, uint64_t last) {
    ASSERT_FALSE(ranges.empty());
    EXPECT_EQ(ranges.front().first, first);
    EXPECT_EQ(ranges.back().second, last);
    for (size_t i = 1; i < ranges.size(); ++i) EXPECT_EQ(ranges[i].first, ranges[i - 1].second + 1) << "window " << i;
}

TEST(AdaptiveWindowTest, SparseMatchesMakeTheWindowsGrowUpToTheCapAndEveryRowIsDeliveredOnce) {
    for (bool prefetch : {false, true}) {
        AnsweringServer server(1000, MatchesAnswer({5, 7777}));
        XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
        client.SetMemoryBudget(SMALL_BUDGET);
        std::vector<uint64_t> seen;
        const uint64_t rows = client.FindByValueEach(0, 9999, "DB", 0, xstore_fdata::Int64(1), [&](const XStore_Proto::FindByValue_REP &window) {
            for (const auto &row : window.queryresponse()) seen.push_back(row.timestamp());
        }, Options(prefetch));
        EXPECT_EQ(rows, 2u);
        EXPECT_EQ(seen, (std::vector<uint64_t>{5, 7777}));
        const auto ranges = RangesSeen(server);
        ExpectTheRangeIsCoveredOnceInOrder(ranges, 0, 9999);
        EXPECT_EQ(ranges.front().second - ranges.front().first + 1, 100u) << "the first window is the size the bytes give";
        for (size_t i = 0; i < ranges.size(); ++i) {
            const uint64_t length = ranges[i].second - ranges[i].first + 1;
            EXPECT_LE(length, 1600u) << "window " << i << ": never over the cap";
            if (i > 0 && i + 1 < ranges.size()) {
                const uint64_t before = ranges[i - 1].second - ranges[i - 1].first + 1;
                EXPECT_LE(length, 2 * before) << "window " << i << ": at most twice the one before";
            }
        }
        EXPECT_LE(ranges.size(), 12u) << "100 fixed windows would have been 100 calls";
    }
}

TEST(AdaptiveWindowTest, WindowsThatReturnEveryRowStayAtTheSizeTheBytesGive) {
    std::set<uint64_t> all;
    for (uint64_t t = 0; t < 1000; ++t) all.insert(t);
    AnsweringServer server(1000, MatchesAnswer(all));
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMemoryBudget(SMALL_BUDGET);
    const uint64_t rows = client.FindByValueEach(0, 999, "DB", 0, xstore_fdata::Int64(1), [](const XStore_Proto::FindByValue_REP &) {}, Options(false));
    EXPECT_EQ(rows, 1000u);
    const auto ranges = RangesSeen(server);
    ExpectTheRangeIsCoveredOnceInOrder(ranges, 0, 999);
    EXPECT_EQ(ranges.size(), 10u);
    for (const auto &range : ranges) EXPECT_EQ(range.second - range.first + 1, 100u);
}

TEST(AdaptiveWindowTest, ADenseStretchAfterASparseOneBringsTheWindowsBackToTheirSize) {
    std::set<uint64_t> matches;
    for (uint64_t t = 4000; t < 6000; ++t) matches.insert(t);
    AnsweringServer server(1000, MatchesAnswer(matches));
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMemoryBudget(SMALL_BUDGET);
    uint64_t delivered = 0;
    const uint64_t rows = client.FindByValueEach(0, 9999, "DB", 0, xstore_fdata::Int64(1),
                                                 [&](const XStore_Proto::FindByValue_REP &window) { delivered += static_cast<uint64_t>(window.queryresponse_size()); }, Options(false));
    EXPECT_EQ(rows, 2000u);
    EXPECT_EQ(delivered, 2000u);
    const auto ranges = RangesSeen(server);
    ExpectTheRangeIsCoveredOnceInOrder(ranges, 0, 9999);
    bool shrank = false;
    for (size_t i = 1; i < ranges.size(); ++i) {
        const uint64_t before = ranges[i - 1].second - ranges[i - 1].first + 1, length = ranges[i].second - ranges[i].first + 1;
        if (before > 100 && length == 100 && ranges[i].first >= 4000 && ranges[i].first < 6000) shrank = true;
    }
    EXPECT_TRUE(shrank) << "a window that came back full is followed by one of the base size";
}

// A modest growth is not taken: the window grows only when the matches are sparse enough for at least GROWTH_MIN_FACTOR times the base window.
TEST(AdaptiveWindowTest, AWindowThatCouldOnlyGrowAFewTimesStaysAtTheBaseSize) {
    std::set<uint64_t> half;   // every 2nd timestamp matches: the reply bound allows about 2x the base window
    for (uint64_t t = 0; t < 2000; t += 2) half.insert(t);
    AnsweringServer server(1000, MatchesAnswer(half));
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMemoryBudget(SMALL_BUDGET);
    EXPECT_EQ(client.FindByValueEach(0, 1999, "DB", 0, xstore_fdata::Int64(1), [](const XStore_Proto::FindByValue_REP &) {}, Options(false)), 1000u);
    const auto ranges = RangesSeen(server);
    ExpectTheRangeIsCoveredOnceInOrder(ranges, 0, 1999);
    for (const auto &range : ranges) EXPECT_EQ(range.second - range.first + 1, 100u);
}

TEST(AdaptiveWindowTest, AWindowThatCanGrowSeveralTimesDoes) {
    std::set<uint64_t> one_in_ten;   // 10% match: the reply bound allows about 10x the base window
    for (uint64_t t = 0; t < 4000; t += 10) one_in_ten.insert(t);
    AnsweringServer server(1000, MatchesAnswer(one_in_ten));
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMemoryBudget(SMALL_BUDGET);
    EXPECT_EQ(client.FindByValueEach(0, 3999, "DB", 0, xstore_fdata::Int64(1), [](const XStore_Proto::FindByValue_REP &) {}, Options(false)), 400u);
    const auto ranges = RangesSeen(server);
    ExpectTheRangeIsCoveredOnceInOrder(ranges, 0, 3999);
    EXPECT_LT(ranges.size(), 40u) << "fixed windows of 100 would be 40 calls";
    uint64_t longest = 0;
    for (const auto &range : ranges) longest = std::max(longest, range.second - range.first + 1);
    EXPECT_GE(longest, 400u);
}

TEST(AdaptiveWindowTest, TheGrowthThresholdIsExactlyFourTimesTheBaseWindow) {
    // The first window of 100 timestamps comes back with `found` rows: the next window may hold 100 * 100 / (found + 1) timestamps, 400 for 24 rows
    // (4x: grows, to twice the last window), 384 for 25 (under 4x: stays).
    for (auto [found, second_window] : {std::pair<uint64_t, uint64_t>{24, 200}, {25, 100}}) {
        std::set<uint64_t> matches;
        for (uint64_t i = 0; i < found; ++i) matches.insert(i * 4);
        AnsweringServer server(1000, MatchesAnswer(matches));
        XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
        client.SetMemoryBudget(SMALL_BUDGET);
        EXPECT_EQ(client.FindByValueEach(0, 999, "DB", 0, xstore_fdata::Int64(1), [](const XStore_Proto::FindByValue_REP &) {}, Options(false)), found);
        const auto ranges = RangesSeen(server);
        ASSERT_GE(ranges.size(), 2u);
        EXPECT_EQ(ranges[1].second - ranges[1].first + 1, second_window) << found << " rows in the first window";
    }
}

TEST(AdaptiveWindowTest, WithAdaptiveOffTheWindowsStayFixed) {
    AnsweringServer server(1000, MatchesAnswer({5}));
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMemoryBudget(SMALL_BUDGET);
    xstore_client::WindowOptions options = Options(false);
    options.adaptive = false;
    EXPECT_EQ(client.FindByValueEach(0, 999, "DB", 0, xstore_fdata::Int64(1), [](const XStore_Proto::FindByValue_REP &) {}, options), 1u);
    EXPECT_EQ(RangesSeen(server).size(), 10u);
}

TEST(AdaptiveWindowTest, RangeQueryEachWindowsAreTheFixedSizeEvenOverAnEmptyStretch) {
    std::set<uint64_t> rows_present{5, 7777};   // a database with holes: RangeQuery returns only the rows that exist
    AnsweringServer server(1000, [rows_present](size_t, const XStore_Proto::Request &request) {
        XStore_Proto::RangeQuery_REP reply;
        for (auto it = rows_present.lower_bound(request.rangequery().starttime()); it != rows_present.end() && *it <= request.rangequery().endtime(); ++it) {
            *reply.add_queryresponse() = Row(*it);
        }
        return reply.SerializeAsString();
    });
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMemoryBudget(SMALL_BUDGET);
    EXPECT_EQ(client.RangeQueryEach(0, 9999, "DB", [](const XStore_Proto::RangeQuery_REP &) {}, Options(false)), 2u);
    ASSERT_EQ(server.requests().size(), 100u);
    for (const auto &request : server.requests()) EXPECT_EQ(request.rangequery().endtime() - request.rangequery().starttime() + 1, 100u);
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

TEST(ClientPipelinedTest, NoMoreThanPipelineInFlightPartsAreWaitingForAnAnswerAtOnce) {
    // The server answers only after 400 ms of silence (hold larger than the parts), so the client sends until it must wait: exactly the limit.
    AnsweringServer server(1000, Answer, /*hold=*/1000);
    XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
    client.SetMaxRequestBytes(30000);
    const auto rows = InsertRows(100 * xstore_client::PIPELINE_IN_FLIGHT);   // ~29 rows a part: three times the limit in parts or more
    const std::string status = client.BatchInsertPipelined("DB", rows, XStore_Proto::ASYNC);
    EXPECT_NE(status.find("Inserted " + std::to_string(rows.size()) + " elements successfully"), std::string::npos) << status;
    EXPECT_EQ(server.max_pending(), xstore_client::PIPELINE_IN_FLIGHT);
}

// Rows of very different sizes, many more than one measuring block (4096), so the parts are cut across the blocks.
std::vector<std::pair<uint64_t, XStore_Proto::RowData>> UnevenRows(size_t n) {
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    for (size_t i = 0; i < n; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_bytesdata(std::string(20 + (i * 37) % 400 + (i % 97 == 0 ? 3000 : 0), 'x'));
        rows.emplace_back(1000 + i, std::move(row));
    }
    return rows;
}

std::vector<int> PartSizes(AnsweringServer &server) {
    std::vector<int> sizes;
    for (const auto &request : server.requests()) sizes.push_back(request.batchinsert().batchinsert_data_size());
    return sizes;
}

// With one thread the sizes are measured block by block as the parts are cut (so measuring overlaps the sends); with more, all up front. Either way the
// parts are the same.
TEST(ClientPipelinedTest, ThePartsAreCutAtTheSameRowsWhetherTheSizesAreMeasuredUpFrontOrBlockByBlock) {
    const auto rows = UnevenRows(12000);
    std::vector<std::vector<int>> parts;
    for (size_t threads : {size_t{1}, size_t{4}}) {
        AnsweringServer server(1000, Answer);
        XStoreClient client("127.0.0.1", server.port(), 5000, /*warmup_on_init=*/false);
        client.SetThreads(threads);
        client.SetMaxRequestBytes(200000);
        const std::string status = client.BatchInsertPipelined("DB", rows, XStore_Proto::ASYNC);
        EXPECT_NE(status.find("Inserted 12000 elements successfully"), std::string::npos) << status;
        parts.push_back(PartSizes(server));
        int total = 0;
        for (int n : parts.back()) total += n;
        EXPECT_EQ(total, 12000);
    }
    ASSERT_GT(parts[0].size(), 10u) << "several parts, across several measuring blocks";
    EXPECT_EQ(parts[0], parts[1]);
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
