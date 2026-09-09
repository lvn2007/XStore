#ifndef XSTORE_WIREENCODE_H
#define XSTORE_WIREENCODE_H

#include <cstddef>
#include <cstdint>
#include <string>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite.h>

// Shared by RangeQuery and BatchQuery's streamed (zero-object-tree) response
// paths -- both produce the exact same wire shape (repeated UnaryQuery_REP
// entries: field 1 timestamp/varint, field 2 data/length-delimited), just
// addressed differently (RangeQuery walks a contiguous mmap'd span,
// BatchQuery preads scattered offsets). Kept here once rather than
// duplicated per .cpp specifically because RealRowDataLength already had one
// real off-by-one bug found and fixed during RangeQuery's implementation --
// a second, drifted copy in BatchOps would be exactly the kind of thing that
// silently reintroduces it.
namespace XStoreWireEncode {

using google::protobuf::internal::WireFormatLite;
using google::protobuf::io::CodedInputStream;
using google::protobuf::io::CodedOutputStream;

// A row's on-disk slot is a real serialized RowData followed by zero
// padding out to rowLength (SerializeToArray then memset the remainder --
// see UnaryInsert/ApplyWalRecord's own row-write steps). Field number 0 is
// reserved/invalid in protobuf, so
// CodedInputStream::ReadTag() returns exactly 0 both at true end-of-buffer
// AND the instant it hits the first zero-padding byte -- either way, that's
// the boundary between real content and padding. WireFormatLite::SkipField
// only needs the wire type to skip a field's value (varint/fixed32/fixed64/
// length-delimited), so this never has to know which of FData's 15 oneof
// cases a given field actually is -- it works identically whether the row
// has every column populated, only some (sparse columns), or none (a hole,
// where real_len comes back 0).
inline size_t RealRowDataLength(const char *row_ptr, size_t row_length) {
    CodedInputStream input(reinterpret_cast<const uint8_t *>(row_ptr), static_cast<int>(row_length));
    // CurrentPosition() right after the loop breaks is NOT the answer:
    // ReadTag() consumes a byte to learn the tag is 0 (real EOF or the
    // first zero-padding byte) even though that byte isn't part of any
    // real field, so position-after-break is always 1 too high (a hole
    // row -- read_tag hits padding on the very first byte -- would
    // otherwise come back as real_len=1, not 0). Track position only after
    // each field that actually skipped successfully instead.
    size_t last_good_position = 0;
    while (true) {
        uint32_t tag = input.ReadTag();
        if (tag == 0) break;
        if (!WireFormatLite::SkipField(&input, tag)) break;
        last_good_position = static_cast<size_t>(input.CurrentPosition());
    }
    return last_good_position;
}

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

// Appends one queryresponse entry (field 1, length-delimited -- same field
// number in both RangeQuery_REP and BatchQuery_REP) directly to buf -- the
// wire-identical equivalent of add_queryresponse()/set_timestamp()/
// mutable_data()->ParseFromArray(), without ever constructing a
// UnaryQuery_REP/RowData/FData to do it. row_ptr's first real_len bytes ARE
// already a valid serialized RowData (that's what's on disk) -- copied
// verbatim, not re-encoded field by field.
inline void EncodeQueryResponseEntry(std::string &buf, uint64_t epoch_time, const char *row_ptr, size_t row_length) {
    size_t real_len = RealRowDataLength(row_ptr, row_length);

    // Inner UnaryQuery_REP content: field 1 timestamp (varint), field 2 data
    // (length-delimited, holding the row's real_len raw bytes). Both field
    // numbers are 1-2, so both tags are always exactly 1 byte.
    size_t inner_len = 1 + CodedOutputStream::VarintSize64(epoch_time)
                      + 1 + CodedOutputStream::VarintSize32(static_cast<uint32_t>(real_len)) + real_len;

    AppendTag(buf, /*queryresponse*/ 1, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
    AppendVarint32(buf, static_cast<uint32_t>(inner_len));

    AppendTag(buf, /*UnaryQuery_REP.timestamp*/ 1, WireFormatLite::WIRETYPE_VARINT);
    AppendVarint64(buf, epoch_time);
    AppendTag(buf, /*UnaryQuery_REP.data*/ 2, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
    AppendVarint32(buf, static_cast<uint32_t>(real_len));
    buf.append(row_ptr, real_len);
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
