#ifndef XSTORE_TEST_SUPPORT_H
#define XSTORE_TEST_SUPPORT_H

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

#include "include/XStore/XStore.h"

namespace xstore_test {

namespace fs = std::filesystem;

// One factory per `oneof oneOfData` branch in FData (see proto/XStore.proto)
// so tests can build rows out of every type the wire format actually allows,
// not just double/string/bool.
inline XStore_Proto::FData FDouble(double v)                { XStore_Proto::FData f; f.set_doubledata(v); return f; }
inline XStore_Proto::FData FFloat(float v)                  { XStore_Proto::FData f; f.set_floatdata(v); return f; }
inline XStore_Proto::FData FInt32(int32_t v)                { XStore_Proto::FData f; f.set_int32data(v); return f; }
inline XStore_Proto::FData FInt64(int64_t v)                { XStore_Proto::FData f; f.set_int64data(v); return f; }
inline XStore_Proto::FData FUInt32(uint32_t v)               { XStore_Proto::FData f; f.set_uint32data(v); return f; }
inline XStore_Proto::FData FUInt64(uint64_t v)               { XStore_Proto::FData f; f.set_uint64data(v); return f; }
inline XStore_Proto::FData FSInt32(int32_t v)                { XStore_Proto::FData f; f.set_sint32data(v); return f; }
inline XStore_Proto::FData FSInt64(int64_t v)                { XStore_Proto::FData f; f.set_sint64data(v); return f; }
inline XStore_Proto::FData FFixed32(uint32_t v)              { XStore_Proto::FData f; f.set_fixed32data(v); return f; }
inline XStore_Proto::FData FFixed64(uint64_t v)              { XStore_Proto::FData f; f.set_fixed64data(v); return f; }
inline XStore_Proto::FData FSFixed32(int32_t v)              { XStore_Proto::FData f; f.set_sfixed32data(v); return f; }
inline XStore_Proto::FData FSFixed64(int64_t v)              { XStore_Proto::FData f; f.set_sfixed64data(v); return f; }
inline XStore_Proto::FData FBool(bool v)                     { XStore_Proto::FData f; f.set_booldata(v); return f; }
inline XStore_Proto::FData FString(const std::string &v)     { XStore_Proto::FData f; f.set_stringdata(v); return f; }
inline XStore_Proto::FData FBytes(const std::string &v)      { XStore_Proto::FData f; f.set_bytesdata(v); return f; }

// Builds a RowData out of a mix of column types/counts so tests can express
// "1 column here, 3 columns there" without hand-rolling FData boilerplate.
inline XStore_Proto::RowData MakeRow(const std::vector<XStore_Proto::FData> &fields) {
    XStore_Proto::RowData row;
    for (const auto &f : fields) {
        *row.add_fielddata() = f;
    }
    return row;
}

// XStore's production class only exposes RangeQuery/BatchQuery
// (see XStore.h) -- the network server is the only production caller, and it
// needs their bounded-memory chunked contract, not a materialized REP. Tests
// want the old synchronous shape (call it, get a filled-in REP back) so they
// can assert on it directly without each writing its own accumulate+parse
// boilerplate -- these two centralize that, matching RangeQuery/BatchQuery's
// former behavior exactly (same checks, same wire shape, just reassembled
// from the streamed implementation instead of a separate one).
inline XStore_Proto::RangeQuery_REP RangeQuery(XStore &store, unsigned long long int start_epoch_t,
                                                unsigned long long int end_epoch_t, std::string_view db_name) {
    std::string buffer;
    store.RangeQuery(start_epoch_t, end_epoch_t, db_name, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
        [&](std::string &&chunk, bool /*is_last*/) { buffer += chunk; });
    XStore_Proto::RangeQuery_REP rep;
    rep.ParseFromString(buffer);
    return rep;
}

inline XStore_Proto::BatchQuery_REP BatchQuery(XStore &store, XStore_Proto::BatchQuery_REQ *proto_from_client,
                                                bool sort_time = true) {
    std::string buffer;
    store.BatchQuery(proto_from_client, sort_time, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
        [&](std::string &&chunk, bool /*is_last*/) { buffer += chunk; });
    XStore_Proto::BatchQuery_REP rep;
    rep.ParseFromString(buffer);
    return rep;
}

inline std::atomic<int> &DirCounter() {
    static std::atomic<int> counter{0};
    return counter;
}

// XStore::GetBaseDir() resolves baseDir as the process's current working
// directory (mirroring the production layout: the XStore binary runs with
// its cwd set to <baseDir>, and <baseDir>/DataStore/<dbName>/... lives
// alongside it). Constructing this moves the process cwd into
// "<tmp>/<prefix>_<pid>_<n>" and restores it on destruction.
class ScratchDir {
public:
    fs::path original_cwd;
    fs::path test_root;

    explicit ScratchDir(const std::string &prefix) {
        original_cwd = fs::current_path();
        test_root = fs::temp_directory_path() /
            fs::path(prefix + "_" + std::to_string(getpid()) + "_" + std::to_string(DirCounter()++));
        fs::create_directories(test_root / "DataStore");
        fs::current_path(test_root);
    }

    ~ScratchDir() {
        std::error_code ec;
        fs::current_path(original_cwd, ec);
        fs::remove_all(test_root, ec);
    }
};

}  // namespace xstore_test

#endif  // XSTORE_TEST_SUPPORT_H
