#pragma once
// The per-DB config file <DB>.CFG: a small checked binary file (it used to be a serialized protobuf ConfigStruct).
//
//   "XSCF" | u16 format_version | u32 body_len | body | u32 CRC32C(body)      (little-endian)
//   body: u32 timeResolution | i32 rowLength | u32 columnCount | str dbName
//         | u32 nameCount, str names... | u32 schemaCount, (u8 kind, u8 storage, u8 capacity)...        (str = u32 length + bytes)
//
// The in-memory form stays XStore_Proto::ConfigStruct (the rest of the server reads it as before); only the bytes on disk change.
#include "XStore.pb.h"

#include <cstdint>
#include <string>
#include <string_view>

namespace xstore::config {

inline constexpr uint16_t FORMAT_VERSION = 1;

enum class LoadStatus : uint8_t {
    kOk,
    kMissing,             // the file does not exist or cannot be opened
    kOldFormat,           // no "XSCF" magic: a protobuf .CFG written by an older server
    kCorrupt,             // magic present but truncated, wrong length, or the CRC does not match
    kUnsupportedVersion,  // written by a newer format version
};

[[nodiscard]] const char *StatusMessage(LoadStatus status);

[[nodiscard]] std::string Serialize(const XStore_Proto::ConfigStruct &cfg);
[[nodiscard]] LoadStatus Parse(std::string_view bytes, XStore_Proto::ConfigStruct *out);
[[nodiscard]] LoadStatus Load(const std::string &path, XStore_Proto::ConfigStruct *out);

// Writes path + ".tmp", fsyncs it, renames it over path, fsyncs the directory: a crash leaves the old file or the new file,
// never a half-written one. false: *error says why.
[[nodiscard]] bool Save(const std::string &path, const XStore_Proto::ConfigStruct &cfg, std::string *error);

}  // namespace xstore::config
