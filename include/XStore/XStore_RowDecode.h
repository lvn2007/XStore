#pragma once
// Reads one column of a stored row straight off the protobuf bytes, as a double, without building protobuf objects.
// The scan kernels used to run a CodedInputStream and a full FData parse per row; here a plain pointer walks the row.
//
// It answers exactly what the protobuf path (ExtractColumnByIndexSkipParse, then oneOfData_case() in 1..12, then a
// conversion through long double to double) answers, or says kFallback and the caller runs that path. Anything this code
// does not model (a field number other than RowData.fieldData, a group, a number after a string, a non-minimal tag, a
// truncated or over-long varint, a length past the row) is kFallback, so a row it cannot vouch for is never decided here.
#include <cstddef>
#include <cstdint>
#include <cstring>

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "fixed32/fixed64 are read with memcpy");

namespace xstore {

enum class RowDecode {
    kValue,     // the column holds a number (oneof fields 1..12): *out is set
    kSkip,      // not found, a hole, or not a number (bool, bytes, unset): the row contributes nothing
    kFallback,  // not decided here: run the protobuf path
};

namespace rowdecode_detail {

// At most 10 bytes, bits past 64 dropped, as protobuf does. false: truncated or longer than 10 bytes.
inline bool ReadVarint(const unsigned char *&p, const unsigned char *end, uint64_t &out) {
    uint64_t result = 0;
    for (unsigned shift = 0; shift < 70 && p < end; shift += 7) {
        const uint64_t byte = *p++;
        result |= (byte & 0x7F) << shift;
        if (!(byte & 0x80)) {
            out = result;
            return true;
        }
    }
    return false;
}

// Moves past one field value of wire type `wire_type`. false: truncated, or a wire type this code does not model.
inline bool SkipValue(unsigned wire_type, const unsigned char *&p, const unsigned char *end) {
    uint64_t length = 0;
    switch (wire_type) {
        case 0: return ReadVarint(p, end, length);
        case 1: if (end - p < 8) return false; p += 8; return true;
        case 2: if (!ReadVarint(p, end, length) || length > static_cast<uint64_t>(end - p)) return false; p += length; return true;
        case 5: if (end - p < 4) return false; p += 4; return true;
        default: return false;
    }
}

// Wire type of FData oneof field `field` (1..15), the one the generated parser accepts for it.
constexpr unsigned ExpectedWireType(unsigned field) {
    switch (field) {
        case 1: case 10: case 12: return 1;           // double, fixed64, sfixed64
        case 2: case 9: case 11: return 5;            // float, fixed32, sfixed32
        case 14: case 15: return 2;                   // string, bytes
        default: return 0;                            // int32 .. sint64, bool
    }
}

inline RowDecode DecodeFData(const unsigned char *p, const unsigned char *end, double *out) {
    unsigned field = 0;  // the oneof member that is set: the last one on the wire, as in protobuf
    bool saw_string = false;
    uint64_t raw = 0;
    while (p < end) {
        uint64_t tag = 0;
        if (!ReadVarint(p, end, tag) || tag > UINT32_MAX) return RowDecode::kFallback;
        const unsigned number = static_cast<unsigned>(tag >> 3), wire_type = static_cast<unsigned>(tag & 7);
        if (number == 0) return RowDecode::kFallback;   // not a valid field number: protobuf's call, not ours
        if (number > 15 || wire_type != ExpectedWireType(number)) {  // unknown to FData: protobuf skips it
            if (!SkipValue(wire_type, p, end)) return RowDecode::kFallback;
            continue;
        }
        if (number == 14) saw_string = true;
        if (wire_type == 0) {
            if (!ReadVarint(p, end, raw)) return RowDecode::kFallback;
        } else if (wire_type == 1) {
            if (end - p < 8) return RowDecode::kFallback;
            std::memcpy(&raw, p, 8);
            p += 8;
        } else if (wire_type == 5) {
            uint32_t narrow = 0;
            if (end - p < 4) return RowDecode::kFallback;
            std::memcpy(&narrow, p, 4);
            raw = narrow;
            p += 4;
        } else if (!SkipValue(wire_type, p, end)) {
            return RowDecode::kFallback;
        }
        field = number;
    }

    // protobuf validates a string's UTF-8 and fails the whole row on bad bytes: a string last is non-numeric either way, but a
    // number after a string is only a number if that string was valid, which this code does not check.
    if (field != 14 && saw_string) return RowDecode::kFallback;
    const uint32_t low = static_cast<uint32_t>(raw);
    switch (field) {
        case 1: std::memcpy(out, &raw, 8); return RowDecode::kValue;
        case 2: { float f; std::memcpy(&f, &low, 4); *out = static_cast<double>(f); return RowDecode::kValue; }
        case 3: case 11: *out = static_cast<double>(static_cast<int32_t>(low)); return RowDecode::kValue;
        case 4: case 12: *out = static_cast<double>(static_cast<int64_t>(raw)); return RowDecode::kValue;
        case 5: case 9: *out = static_cast<double>(low); return RowDecode::kValue;
        case 6: case 10: *out = static_cast<double>(raw); return RowDecode::kValue;
        case 7: *out = static_cast<double>(static_cast<int32_t>((low >> 1) ^ (0u - (low & 1u)))); return RowDecode::kValue;
        case 8: *out = static_cast<double>(static_cast<int64_t>((raw >> 1) ^ (0ull - (raw & 1ull)))); return RowDecode::kValue;
        default: return RowDecode::kSkip;  // unset, bool, string, bytes
    }
}

}  // namespace rowdecode_detail

// Column `column_idx` of a row (RowData: repeated fieldData, each one tag 0x0A + length + FData bytes; zero padding after).
[[nodiscard]] inline RowDecode DecodeColumnAsDouble(const char *row, size_t row_len, unsigned short column_idx, double *out) {
    const unsigned char *p = reinterpret_cast<const unsigned char *>(row);
    const unsigned char *const end = p + row_len;
    unsigned occurrence = 0;
    while (p < end) {
        if (*p == 0) return RowDecode::kSkip;           // padding: the row has fewer columns
        if (*p != 0x0A) return RowDecode::kFallback;    // anything but fieldData in its minimal tag form
        ++p;
        uint64_t length = 0;
        if (!rowdecode_detail::ReadVarint(p, end, length) || length > static_cast<uint64_t>(end - p)) return RowDecode::kFallback;
        if (occurrence++ == column_idx) return rowdecode_detail::DecodeFData(p, p + length, out);
        p += length;
    }
    return RowDecode::kSkip;
}

}  // namespace xstore
