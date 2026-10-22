#pragma once

// The request shape shared by XStoreZmqInsertPerfTest (server in-process) and xstore_zmq_insert_client (server in
// another process or machine): BENCH_DB-like rows of 128 doubles, 1000 rows per BatchInsert, serialized into the
// Request envelope the server expects before any timer starts.
#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include "XStore.pb.h"

namespace zmq_insert_payload {

constexpr int kColumns = 128;
constexpr short kRowLength = kColumns * 11;   // a double column serializes to 11 B
constexpr size_t kRowsPerCall = 1000;
constexpr unsigned long long kBaseTs = 946684800ULL;        // 2000-01-01
constexpr unsigned long long kThreadStride = 10'000'000ULL; // seconds between client threads' ranges

inline XStore_Proto::RowData WideRow(unsigned long long i) {
    XStore_Proto::RowData row;
    for (int c = 0; c < kColumns; ++c) row.add_fielddata()->set_doubledata(static_cast<double>(i) * 0.5 + c);
    return row;
}

inline unsigned long long FirstTs(unsigned long long thread_id, unsigned long long call) {
    return kBaseTs + thread_id * kThreadStride + call * kRowsPerCall;
}

// One serialized Request envelope holding a BatchInsert of `kRowsPerCall` rows starting at `first_ts`.
inline std::string BuildPayload(const std::string &db, XStore_Proto::WalDurabilityMode mode, unsigned long long first_ts,
                                bool shuffle, std::mt19937_64 &rng) {
    std::vector<size_t> order(kRowsPerCall);
    for (size_t i = 0; i < order.size(); ++i) order[i] = i;
    if (shuffle) std::shuffle(order.begin(), order.end(), rng);
    XStore_Proto::Request request;
    XStore_Proto::BatchInsert_REQ *batch = request.mutable_batchinsert();
    batch->set_dbname(db);
    batch->set_durabilitymode(mode);
    for (const size_t i : order) {
        XStore_Proto::BatchInsert_DATA *entry = batch->add_batchinsert_data();
        entry->set_timestamp(first_ts + i);
        *entry->mutable_data() = WideRow(first_ts + i);
    }
    return request.SerializeAsString();
}

}  // namespace zmq_insert_payload
