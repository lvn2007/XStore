// The BatchInsert request is encoded straight from the caller's rows (no copy into a Request message, no second walk over it). The bytes
// must be exactly what protobuf makes of the same Request, for every shape of row.
#include <gtest/gtest.h>

#include <cstdlib>
#include <atomic>
#include <climits>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include "XStore_Client.h"

namespace {
using Rows = std::vector<std::pair<uint64_t, XStore_Proto::RowData>>;

// The request as the client used to build it: copy each row into a Request, serialize that.
std::string Reference(const std::string &db, const Rows &rows, size_t begin, size_t end, XStore_Proto::WalDurabilityMode durability, int timeout_ms) {
    XStore_Proto::Request request;
    auto *payload = request.mutable_batchinsert();
    payload->set_rtype(XStore_Proto::BATCHINSERT);
    payload->set_dbname(db);
    payload->set_durabilitymode(durability);
    for (size_t i = begin; i < end; ++i) {
        auto *entry = payload->add_batchinsert_data();
        entry->set_timestamp(rows[i].first);
        *entry->mutable_data() = rows[i].second;
    }
    if (timeout_ms >= 0) request.set_timeoutms(static_cast<uint32_t>(timeout_ms));
    return request.SerializeAsString();
}

std::string Encoded(const std::string &db, const Rows &rows, size_t begin, size_t end, XStore_Proto::WalDurabilityMode durability, int timeout_ms) {
    std::vector<uint32_t> sizes;
    for (size_t i = begin; i < end; ++i) sizes.push_back(static_cast<uint32_t>(rows[i].second.ByteSizeLong()));
    const zmq::message_t message = xstore_client::EncodeBatchInsert(db, rows, begin, end, sizes, durability, timeout_ms);
    return std::string(static_cast<const char *>(message.data()), message.size());
}

XStore_Proto::RowData RandomRow(std::mt19937_64 &rng) {
    XStore_Proto::RowData row;
    const int columns = static_cast<int>(rng() % 6);   // 0 columns too: an empty RowData
    for (int c = 0; c < columns; ++c) {
        switch (rng() % 6) {
            case 0: *row.add_fielddata() = xstore_fdata::Double(static_cast<double>(rng() % 1000) / 7.0); break;
            case 1: *row.add_fielddata() = xstore_fdata::Int64(static_cast<int64_t>(rng())); break;
            case 2: *row.add_fielddata() = xstore_fdata::String(std::string(rng() % 300, static_cast<char>('a' + rng() % 26))); break;
            case 3: *row.add_fielddata() = xstore_fdata::Bool(rng() % 2 == 0); break;
            case 4: *row.add_fielddata() = xstore_fdata::UInt64(rng()); break;
            default: *row.add_fielddata() = xstore_fdata::Bytes(std::string(rng() % 40, '\x01')); break;
        }
    }
    return row;
}

TEST(EncodeBatchInsertTest, TheBytesAreThoseOfTheSerializedRequestForEveryShapeOfRowAndSetting) {
    std::mt19937_64 rng(42);
    Rows rows;
    const uint64_t timestamps[] = {0, 1, 127, 128, 16383, 16384, 1672531200ull, (uint64_t{1} << 56), ~uint64_t{0}};
    for (size_t i = 0; i < 400; ++i) rows.emplace_back(i < 9 ? timestamps[i] : rng(), RandomRow(rng));
    rows.emplace_back(5, XStore_Proto::RowData());   // an empty row, present
    for (const std::string db : {std::string("BENCH_DB"), std::string(), std::string(200, 'D')}) {
        for (auto durability : {XStore_Proto::ASYNC, XStore_Proto::SYNC, XStore_Proto::BYPASS}) {
            for (int timeout_ms : {-1, 0, 1, 5000, 1800000}) {
                for (auto [begin, end] : {std::pair<size_t, size_t>{0, rows.size()}, {0, 1}, {3, 4}, {10, 250}, {rows.size() - 1, rows.size()}, {5, 5}}) {
                    EXPECT_TRUE(Encoded(db, rows, begin, end, durability, timeout_ms) == Reference(db, rows, begin, end, durability, timeout_ms))
                        << "db '" << db.substr(0, 8) << "' durability " << durability << " timeout " << timeout_ms << " rows [" << begin << "," << end << ")";
                }
            }
        }
    }
}

TEST(EncodeBatchInsertTest, ARequestOverTheLimitIsRefusedLikeAnySerializedRequest) {
    std::mt19937_64 rng(7);
    Rows rows;
    for (size_t i = 0; i < 50; ++i) rows.emplace_back(i, RandomRow(rng));
    const size_t exact = Reference("DB", rows, 0, rows.size(), XStore_Proto::ASYNC, -1).size();
    std::vector<uint32_t> sizes;
    for (const auto &row : rows) sizes.push_back(static_cast<uint32_t>(row.second.ByteSizeLong()));
    EXPECT_THROW((void)xstore_client::EncodeBatchInsert("DB", rows, 0, rows.size(), sizes, XStore_Proto::ASYNC, -1, exact - 1), std::length_error);
    EXPECT_NO_THROW((void)xstore_client::EncodeBatchInsert("DB", rows, 0, rows.size(), sizes, XStore_Proto::ASYNC, -1, exact));
}

}  // namespace

// ---- the team: the size pass and the encoding of a big request run on several threads, with the same result as one thread ----

// Rows of very different sizes (empty, tiny, ~1.5 KB, one of 2 MiB), enough of them to pass the size from which the encoding is split up.
Rows BigRows(std::mt19937_64 &rng, size_t count) {
    Rows rows;
    for (size_t i = 0; i < count; ++i) {
        XStore_Proto::RowData row;
        if (i % 97 == 0) {
            // an empty row
        } else if (i == count / 3) {
            *row.add_fielddata() = xstore_fdata::String(std::string(2u << 20, 'x'));
        } else if (i % 5 == 0) {
            *row.add_fielddata() = xstore_fdata::Int64(static_cast<int64_t>(i));
        } else {
            *row.add_fielddata() = xstore_fdata::String(std::string(1000 + rng() % 1000, static_cast<char>('a' + i % 26)));
        }
        rows.emplace_back(i % 11 == 0 ? 0 : rng(), std::move(row));
    }
    return rows;
}

xstore_client::ParallelismPolicy Fixed(size_t threads) {
    xstore_client::ParallelismPolicy policy;
    policy.fixed_threads = threads;
    return policy;
}

TEST(MeasureRowSizesTest, EverySizeIsTheRowsByteSizeWhateverTheTeamAndTheRange) {
    std::mt19937_64 rng(11);
    const Rows rows = BigRows(rng, 20000);
    for (auto policy : {Fixed(1), Fixed(2), Fixed(8), xstore_client::ParallelismPolicy()}) {
        for (auto [begin, end] : {std::pair<size_t, size_t>{0, rows.size()}, {0, 0}, {5, 6}, {1, 4099}, {4096, 12289}, {rows.size() - 3, rows.size()}}) {
            const std::vector<uint32_t> sizes = xstore_client::MeasureRowSizes(rows, begin, end, policy);
            ASSERT_EQ(sizes.size(), end - begin);
            for (size_t i = begin; i < end; ++i) {
                ASSERT_EQ(sizes[i - begin], rows[i].second.ByteSizeLong()) << "row " << i << " threads " << policy.fixed_threads;
            }
        }
    }
}

TEST(EncodeBatchInsertTeamTest, ABigRequestIsTheSameBytesOnOneThreadAndOnAnyTeam) {
    std::mt19937_64 rng(21);
    const Rows rows = BigRows(rng, 9000);
    for (auto [begin, end] : {std::pair<size_t, size_t>{0, rows.size()}, {17, 5001}, {4000, rows.size()}}) {
        const std::string reference = Reference("BENCH_DB", rows, begin, end, XStore_Proto::SYNC, 1800000);
        ASSERT_GE(reference.size(), xstore_client::MIN_PARALLEL_PARSE_BYTES) << "the request must be big enough to be split up";
        const std::vector<uint32_t> sizes = xstore_client::MeasureRowSizes(rows, begin, end, Fixed(1));
        for (auto policy : {Fixed(1), Fixed(2), Fixed(3), Fixed(8), xstore_client::ParallelismPolicy()}) {
            const zmq::message_t message = xstore_client::EncodeBatchInsert("BENCH_DB", rows, begin, end, sizes, XStore_Proto::SYNC, 1800000,
                                                                            static_cast<size_t>(INT_MAX), policy);
            EXPECT_TRUE(std::string(static_cast<const char *>(message.data()), message.size()) == reference)
                << "rows [" << begin << "," << end << ") threads " << policy.fixed_threads;
        }
    }
}

TEST(EncodeBatchInsertTeamTest, ASmallRequestIsEncodedOnTheCallingThreadAlone) {
    std::mt19937_64 rng(5);
    Rows rows;
    for (size_t i = 0; i < 1000; ++i) rows.emplace_back(i + 1, RandomRow(rng));
    const std::vector<uint32_t> sizes = xstore_client::MeasureRowSizes(rows, 0, rows.size(), Fixed(8));
    std::atomic<int> consulted{0};
    xstore_client::ParallelismPolicy policy;   // adaptive: it asks the machine's load once a team is started
    policy.runnable = [&]() -> std::optional<size_t> { ++consulted; return 0; };
    (void)xstore_client::EncodeBatchInsert("DB", rows, 0, rows.size(), sizes, XStore_Proto::ASYNC, -1, static_cast<size_t>(INT_MAX), policy);
    (void)xstore_client::MeasureRowSizes(rows, 0, rows.size(), policy);
    EXPECT_EQ(consulted.load(), 0) << "no team, no controller thread, for a request this small";
}
