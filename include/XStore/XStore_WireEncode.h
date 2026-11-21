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

// Bytes of the entry header (everything before the row bytes) for a row of row_bytes: tag, entry length, timestamp field, data tag, row length.
inline size_t EntryHeaderSize(uint64_t epoch_time, size_t row_bytes) {
    const size_t ts = 1 + CodedOutputStream::VarintSize64(epoch_time);
    const size_t data = 1 + CodedOutputStream::VarintSize32(static_cast<uint32_t>(row_bytes));
    return 1 + CodedOutputStream::VarintSize32(static_cast<uint32_t>(ts + data + row_bytes)) + ts + data;
}

// Appends one UnaryQuery_REP entry (field 1 of the reply, length-delimited: timestamp, then the row as RowData) straight
// into the chunk buffer, without building a UnaryQuery_REP/RowData/FData. The row is encoded from its cells as canonical
// protobuf (RowLayout::EncodeToWire), so a client parses it exactly as before. A hole encodes as an empty row.
// The header carries the row's length, so its size is guessed from the previous row of this thread (rows of one reply are
// near-equal in size), a slot of that size is reserved, the row is encoded straight behind it and the header is written in
// place. Only a row whose size crosses a varint boundary (and the first row) moves its bytes: no scratch copy in the common case.
inline void EncodeQueryResponseEntry(std::string &buf, uint64_t epoch_time, const char *row_ptr,
                                     const xstore::layout::RowLayout &layout) {
    thread_local size_t guess = 0;
    const size_t at = buf.size();
    const size_t slot = EntryHeaderSize(epoch_time, guess);
    buf.resize(at + slot);
    layout.EncodeToWire(row_ptr, buf);
    const size_t row_bytes = buf.size() - at - slot;
    const size_t header = EntryHeaderSize(epoch_time, row_bytes);
    const size_t inner_len = 1 + CodedOutputStream::VarintSize64(epoch_time) + 1 + CodedOutputStream::VarintSize32(static_cast<uint32_t>(row_bytes)) + row_bytes;
    if (header > slot) buf.insert(at + slot, header - slot, '\0');
    else if (header < slot) buf.erase(at + header, slot - header);
    uint8_t *p = reinterpret_cast<uint8_t *>(buf.data()) + at;
    p = CodedOutputStream::WriteTagToArray(WireFormatLite::MakeTag(/*queryresponse*/ 1, WireFormatLite::WIRETYPE_LENGTH_DELIMITED), p);
    p = CodedOutputStream::WriteVarint32ToArray(static_cast<uint32_t>(inner_len), p);
    p = CodedOutputStream::WriteTagToArray(WireFormatLite::MakeTag(/*UnaryQuery_REP.timestamp*/ 1, WireFormatLite::WIRETYPE_VARINT), p);
    p = CodedOutputStream::WriteVarint64ToArray(epoch_time, p);
    p = CodedOutputStream::WriteTagToArray(WireFormatLite::MakeTag(/*UnaryQuery_REP.data*/ 2, WireFormatLite::WIRETYPE_LENGTH_DELIMITED), p);
    CodedOutputStream::WriteVarint32ToArray(static_cast<uint32_t>(row_bytes), p);
    if (row_bytes != 0) guess = row_bytes;
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
