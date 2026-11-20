#ifndef XSTORE_WIREENCODE_H
#define XSTORE_WIREENCODE_H

#include "XStore/XStore_RowLayout.h"

#include <cstddef>
#include <cstdint>
#include <string>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite.h>

// Shared by RangeQuery, BatchQuery and the find/scan replies' streamed (zero-object-tree) response paths -- all produce
// the exact same wire shape (repeated UnaryQuery_REP entries: field 1 timestamp/varint, field 2 data/length-delimited),
// just addressed differently (a contiguous mmap'd span, or scattered preads). The row bytes of each entry are encoded
// from the slot's cells by the DB's RowLayout; kept here once so no path carries a drifted copy of the framing.
namespace XStoreWireEncode {

using google::protobuf::internal::WireFormatLite;
using google::protobuf::io::CodedOutputStream;

inline void AppendTag(std::string &buf, uint32_t field_number, WireFormatLite::WireType wt) {
    uint8_t tmp[5];
    uint8_t *end = CodedOutputStream::WriteTagToArray(WireFormatLite::MakeTag(field_number, wt), tmp);
    buf.append(reinterpret_cast<char *>(tmp), end - tmp);
}

inline void AppendVarint32(std::string &buf, uint32_t v) {
    uint8_t tmp[5];
    uint8_t *end = CodedOutputStream::WriteVarint32ToArray(v, tmp);
    buf.append(reinterpret_cast<char *>(tmp), end - tmp);
}

inline void AppendVarint64(std::string &buf, uint64_t v) {
    uint8_t tmp[10];
    uint8_t *end = CodedOutputStream::WriteVarint64ToArray(v, tmp);
    buf.append(reinterpret_cast<char *>(tmp), end - tmp);
}

// Appends one UnaryQuery_REP entry (field 1 of the reply, length-delimited: timestamp, then the row as RowData) straight
// into the chunk buffer, without building a UnaryQuery_REP/RowData/FData. The row is encoded from its cells as canonical
// protobuf (RowLayout::EncodeToWire), so a client parses it exactly as before. A hole encodes as an empty row.
inline void EncodeQueryResponseEntry(std::string &buf, uint64_t epoch_time, const char *row_ptr,
                                     const xstore::layout::RowLayout &layout) {
    const size_t row_bytes = layout.EncodedSize(row_ptr);
    const size_t inner_len = 1 + CodedOutputStream::VarintSize64(epoch_time)
                           + 1 + CodedOutputStream::VarintSize32(static_cast<uint32_t>(row_bytes)) + row_bytes;
    AppendTag(buf, /*queryresponse*/ 1, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
    AppendVarint32(buf, static_cast<uint32_t>(inner_len));
    AppendTag(buf, /*UnaryQuery_REP.timestamp*/ 1, WireFormatLite::WIRETYPE_VARINT);
    AppendVarint64(buf, epoch_time);
    AppendTag(buf, /*UnaryQuery_REP.data*/ 2, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
    AppendVarint32(buf, static_cast<uint32_t>(row_bytes));
    layout.EncodeToWire(row_ptr, buf);
}

// Appends querystatus (field 3, length-delimited -- same field number in
// both RangeQuery_REP and BatchQuery_REP) as the final bytes of the last
// chunk. Only call once, at the very end, iff a warning/error occurred.
inline void AppendQueryStatus(std::string &buf, const std::string &status) {
    AppendTag(buf, /*querystatus*/ 3, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
    AppendVarint32(buf, static_cast<uint32_t>(status.size()));
    buf.append(status);
}

}  // namespace XStoreWireEncode

#endif //XSTORE_WIREENCODE_H
