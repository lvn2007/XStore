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

// One factory per `oneof oneOfData` branch in FData (see ProtoFile/XStore.proto)
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
    XStore_Proto::RowData Row;
    for (const auto &f : fields) {
        *Row.add_fielddata() = f;
    }
    return Row;
}

inline std::atomic<int> &DirCounter() {
    static std::atomic<int> Counter{0};
    return Counter;
}

// XStore::GetBaseDir() resolves baseDir as the PARENT of the process's current
// working directory (mirroring the production layout: <baseDir>/build/<binary>,
// <baseDir>/DataStore/<dbName>/...). Constructing this moves the process cwd
// into "<tmp>/<prefix>_<pid>_<n>/build" and restores it on destruction.
class ScratchDir {
public:
    fs::path OriginalCwd;
    fs::path TestRoot;

    explicit ScratchDir(const std::string &prefix) {
        OriginalCwd = fs::current_path();
        TestRoot = fs::temp_directory_path() /
            fs::path(prefix + "_" + std::to_string(getpid()) + "_" + std::to_string(DirCounter()++));
        fs::create_directories(TestRoot / "build");
        fs::create_directories(TestRoot / "DataStore");
        fs::current_path(TestRoot / "build");
    }

    ~ScratchDir() {
        std::error_code Ec;
        fs::current_path(OriginalCwd, Ec);
        fs::remove_all(TestRoot, Ec);
    }
};

}  // namespace xstore_test

#endif  // XSTORE_TEST_SUPPORT_H
