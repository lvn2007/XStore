#pragma once
// The persisted row format: one fixed-size slot per timestamp (offset = (t - first) * rowLength), cells at fixed
// offsets. See docs/superpowers/specs/2026-11-19-fixed-layout-rows-and-binary-cfg-design.md section 1 for the layout.
//
//   byte 0      marker 0x01 (live row; a fully zero slot is a hole)
//   bytes 1-2   n (u16): the columns the row had
//   next        null bitmap, ceil(S/8) bytes, S = declared (typed) columns
//   next        typed cells, fixed width, schema order
//   next        dynamic cells for columns S..n-1, 9 bytes each: [type byte][8 byte payload]
//   next        tail: bytes of string/bytes values referenced by (u32 offset, u32 length) cells
//   rest        zero padding up to rowLength
#include "XStore.pb.h"

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "cells are stored little-endian and read with memcpy");

namespace xstore::layout {

// The numbers are the field numbers of XStore_Proto.FData's oneof, so a type byte IS the FData case.
enum class Kind : uint8_t {
    kUnset = 0, kDouble = 1, kFloat = 2, kInt32 = 3, kInt64 = 4, kUint32 = 5, kUint64 = 6, kSint32 = 7, kSint64 = 8,
    kFixed32 = 9, kFixed64 = 10, kSfixed32 = 11, kSfixed64 = 12, kBool = 13, kString = 14, kBytes = 15
};

enum class Storage : uint8_t { kDefault = 0, kInt8 = 1, kInt16 = 2, kUint8 = 3, kUint16 = 4, kInlineString = 5 };

// One declared column: the wire kind the client sends and receives, and how it is stored.
struct ColumnSpec {
    Kind kind = Kind::kDouble;
    Storage storage = Storage::kDefault;
    uint8_t capacity = 0;   // kInlineString only: 1..255
};

// How a typed column's bytes are interpreted.
enum class Cls : uint8_t { kF64, kF32, kI64, kU64, kI32, kU32, kBool, kRef, kI16, kI8, kU16, kU8, kInline };

// The column map a block codec reads: where each typed column lives and how wide it is.
struct TypedColumn {
    uint32_t offset = 0;
    uint16_t width = 0;
    Kind kind = Kind::kDouble;
    Cls cls = Cls::kF64;
    uint8_t capacity = 0;
};

// kUnknownField: a protobuf field the schema does not know (RowData.unknown_fields() or an FData's); the row is rejected, never
// stored with the field dropped. Its column is ROW_LEVEL when the field sits on the RowData itself.
enum class PackStatus : uint8_t { kOk, kRowTooLarge, kKindMismatch, kOutOfRange, kStringTooLong, kMalformed, kTooManyColumns, kUnknownField };

struct PackResult {
    static constexpr uint32_t ROW_LEVEL = UINT32_MAX;
    PackStatus status = PackStatus::kOk;
    uint32_t column = 0;   // the column the problem was found at
    [[nodiscard]] bool Ok() const { return status == PackStatus::kOk; }
};

// Forgets when the corrupt-row warning last fired (it is rate-limited to once a minute for the whole process): tests only.
void ResetCorruptRowLogForTests();

// Human text for a rejected row ("column 3 holds the wrong kind for its declared type", ...).
[[nodiscard]] std::string DescribePackResult(const PackResult &result);

// A decoded cell. raw holds the value for the numeric kinds: double/float bits, sign-extended for int32/sint32/
// sfixed32/int64/sint64/sfixed64, zero-extended for unsigned and fixed32/64, 0 or 1 for bool. text holds string/bytes.
struct Cell {
    Kind kind = Kind::kUnset;
    uint64_t raw = 0;
    std::string_view text;
};

struct Observation {
    double value = 0.0;       // numeric kinds only; a NaN is quieted (what the long double path always did)
    bool is_numeric = false;  // kinds 1..12
    bool present = false;     // the cell is set
};

// Numeric value of a cell as a double, the conversion the protobuf path made (kinds 1..12 only).
[[nodiscard]] double CellAsDouble(const Cell &cell);
// The same value without rounding to double (every int64/uint64 is exact): what Min/Max compare, as the protobuf path did.
[[nodiscard]] long double CellAsLongDouble(const Cell &cell);

class RowLayout {
public:
    static constexpr uint8_t MARKER = 0x01;
    static constexpr size_t HEADER_BYTES = 3;
    static constexpr size_t DYNAMIC_CELL_BYTES = 9;
    static constexpr size_t MAX_COLUMNS = 65535;

    // schema may be empty (every column dynamic). On failure returns nullopt and fills *error.
    [[nodiscard]] static std::optional<RowLayout> Make(std::span<const ColumnSpec> schema, size_t row_length, std::string *error);

    // Schema <-> the protobuf ColumnSpec messages (kind, storage and capacity are plain numbers there).
    [[nodiscard]] static std::vector<ColumnSpec> SchemaFromProto(const google::protobuf::RepeatedPtrField<XStore_Proto::ColumnSpec> &specs);
    static void SchemaToProto(std::span<const ColumnSpec> schema, google::protobuf::RepeatedPtrField<XStore_Proto::ColumnSpec> *out);

    [[nodiscard]] size_t RowLength() const { return row_length_; }
    [[nodiscard]] uint32_t TypedColumns() const { return static_cast<uint32_t>(typed_.size()); }
    [[nodiscard]] size_t MinRowBytes() const { return dyn_start_; }          // header + bitmap + typed cells
    [[nodiscard]] size_t DynamicStart() const { return dyn_start_; }
    [[nodiscard]] const std::vector<TypedColumn> &Typed() const { return typed_; }
    // false: a row with this many columns cannot fit the slot even before any string bytes (a cheap pre-check for a request).
    [[nodiscard]] bool CanFit(size_t columns) const {
        return dyn_start_ + (columns > typed_.size() ? DYNAMIC_CELL_BYTES * (columns - typed_.size()) : 0) <= row_length_;
    }
    // Upper bound on the encoded (protobuf) size of ANY row that fits the slot: for sizing reply limits and memory leases.
    [[nodiscard]] size_t MaxEncodedRowBytes() const { return max_encoded_; }

    [[nodiscard]] static bool IsLive(const char *row) { return static_cast<uint8_t>(row[0]) == MARKER; }
    [[nodiscard]] static uint32_t ColumnCount(const char *row) {
        uint16_t n;
        std::memcpy(&n, row + 1, sizeof(n));
        return n;
    }
    // The one trust check of the read side (spec section 4: reads never trust the disk). true: a live row whose column count the
    // slot can hold (Finish never writes a larger one, so every packed row passes). false: a hole (silently), or a slot damaged on
    // disk (marker other than 0 or 1, or an impossible column count), which is skipped and logged once a minute.
    [[nodiscard]] bool LiveAndSane(const char *row) const {
        const uint8_t marker = static_cast<uint8_t>(row[0]);
        if (marker == 0) return false;
        const uint32_t n = ColumnCount(row);
        const size_t typed = typed_.size();
        if (marker == MARKER && (n <= typed || dyn_start_ + DYNAMIC_CELL_BYTES * (n - typed) <= row_length_)) return true;
        ReportCorruptRow(marker, n);
        return false;
    }

    // ---- writing: slot must point at row_length ZEROED bytes; on failure the slot content is unspecified ----
    [[nodiscard]] PackResult PackFromRowData(const XStore_Proto::RowData &row, char *slot) const;
    [[nodiscard]] PackResult PackFromWire(const char *wire, size_t length, char *slot) const;

    // ---- reading (row = a slot of row_length bytes; a hole or a corrupt marker reads as "not present") ----
    // Never reads outside the slot: a string reference that leaves it reads as "not present".
    [[nodiscard]] bool Load(const char *row, uint32_t column, Cell *cell) const;
    [[nodiscard]] bool ReadNumeric(const char *row, uint32_t column, double *out) const;   // false: not present or not kinds 1..12
    [[nodiscard]] bool ReadNumericExact(const char *row, uint32_t column, long double *out) const;   // same, unrounded
    [[nodiscard]] Observation Observe(const char *row, uint32_t column) const;
    [[nodiscard]] static Observation ObservationOf(const Cell &cell);   // a present cell's zone-map observation
    static void CellToFData(const Cell &cell, XStore_Proto::FData *out);
    // Fills *out with the column's FData. false: the column is not present
    // (a hole, a column past the row, or a null/unset cell). Allocates for string/bytes values; scans that only look at text
    // use Load() instead.
    [[nodiscard]] bool ReadFData(const char *row, uint32_t column, XStore_Proto::FData *out) const;

    // The bytes catalog-index keys are made of: [kind byte][kind's natural-width little-endian value, or the string bytes].
    // Identical to AppendFDataForCatIdx of the same logical value, whatever the storage width. Out: push_back(char), append(const char*, size_t).
    template <class Out>
    static void AppendCatIdxKey(Out &out, const Cell &cell) {
        out.push_back(static_cast<char>(cell.kind));
        switch (cell.kind) {
            case Kind::kString: case Kind::kBytes: out.append(cell.text.data(), cell.text.size()); break;
            case Kind::kBool: out.push_back(cell.raw != 0 ? 1 : 0); break;
            case Kind::kUnset: break;
            default: out.append(reinterpret_cast<const char *>(&cell.raw), NaturalWidth(cell.kind)); break;
        }
    }
    [[nodiscard]] static constexpr size_t NaturalWidth(Kind kind) {
        switch (kind) {
            case Kind::kDouble: case Kind::kInt64: case Kind::kUint64: case Kind::kSint64: case Kind::kFixed64: case Kind::kSfixed64: return 8;
            case Kind::kFloat: case Kind::kInt32: case Kind::kUint32: case Kind::kSint32: case Kind::kFixed32: case Kind::kSfixed32: return 4;
            case Kind::kBool: return 1;
            default: return 0;
        }
    }

    // ---- replies: canonical protobuf, byte-identical to SerializeToString of the same RowData ----
    [[nodiscard]] size_t EncodedSize(const char *row) const;
    void EncodeToWire(const char *row, std::string &out) const;                       // appends EncodedSize(row) bytes
    void DecodeToRowData(const char *row, XStore_Proto::RowData *out) const;         // Clear()s out first

private:
    struct Deferred {
        uint32_t cell_offset;   // where the (offset, length) pair goes
        std::string_view text;
    };
    [[nodiscard]] PackResult PutCell(char *slot, uint32_t column, const Cell &cell, std::vector<Deferred> &deferred) const;
    [[nodiscard]] PackResult Finish(char *slot, uint32_t columns, std::vector<Deferred> &deferred) const;
    [[nodiscard]] bool RefInRow(uint32_t columns, uint32_t offset, uint32_t length) const;
    void ReportCorruptRow(uint8_t marker, uint32_t columns) const;   // cold: rate-limited warning
    void ReportOversizedRow() const;                                  // cold: rate-limited warning

    size_t row_length_ = 0;
    size_t dyn_start_ = 0;
    size_t max_encoded_ = 0;
    std::vector<TypedColumn> typed_;
};

// A reader bound to one column: everything that depends only on (layout, column) is resolved once, so the per-row work in a
// scan loop is a marker check, a column-count check and one load. Answers exactly what RowLayout::ReadNumeric answers; kinds it
// has no fast path for (narrow ints, bool, strings, a dynamic cell that is not a double) go through the generic call.
// The layout must outlive the reader.
class ColumnReader {
public:
    ColumnReader(const RowLayout &layout, uint32_t column) : layout_(&layout), column_(column) {
        if (column < layout.TypedColumns()) {
            const TypedColumn &t = layout.Typed()[column];
            typed_ = true;
            offset_ = t.offset;
            cls_ = t.cls;
            bitmap_byte_ = static_cast<uint32_t>(RowLayout::HEADER_BYTES + column / 8);
            bitmap_mask_ = static_cast<uint8_t>(1u << (column % 8));
        } else {
            offset_ = layout.DynamicStart() + RowLayout::DYNAMIC_CELL_BYTES * (column - layout.TypedColumns());
            dynamic_fits_ = offset_ + RowLayout::DYNAMIC_CELL_BYTES <= layout.RowLength();
        }
    }

    // true: *out is the column's numeric value. false: not present, or not a numeric kind.
    [[nodiscard]] bool ReadNumeric(const char *row, double *out) const {
        if (static_cast<uint8_t>(row[0]) != RowLayout::MARKER) return false;   // a hole, or a corrupt row
        uint16_t n;
        std::memcpy(&n, row + 1, sizeof(n));
        if (n <= column_) return false;
        if (typed_) {
            if (!(static_cast<uint8_t>(row[bitmap_byte_]) & bitmap_mask_)) return false;
            const char *p = row + offset_;
            switch (cls_) {
                case Cls::kF64: { uint64_t v; std::memcpy(&v, p, 8); *out = std::bit_cast<double>(v); return true; }
                case Cls::kF32: { uint32_t v; std::memcpy(&v, p, 4); *out = static_cast<double>(std::bit_cast<float>(v)); return true; }
                case Cls::kI64: { int64_t v; std::memcpy(&v, p, 8); *out = static_cast<double>(v); return true; }
                case Cls::kU64: { uint64_t v; std::memcpy(&v, p, 8); *out = static_cast<double>(v); return true; }
                case Cls::kI32: { int32_t v; std::memcpy(&v, p, 4); *out = static_cast<double>(v); return true; }
                case Cls::kU32: { uint32_t v; std::memcpy(&v, p, 4); *out = static_cast<double>(v); return true; }
                default: break;
            }
        } else if (dynamic_fits_ && static_cast<uint8_t>(row[offset_]) == static_cast<uint8_t>(Kind::kDouble)) {
            uint64_t v;
            std::memcpy(&v, row + offset_ + 1, 8);
            *out = std::bit_cast<double>(v);
            return true;
        }
        return layout_->ReadNumeric(row, column_, out);
    }

private:
    const RowLayout *layout_;
    uint32_t column_;
    size_t offset_ = 0;
    uint32_t bitmap_byte_ = 0;
    uint8_t bitmap_mask_ = 0;
    bool typed_ = false;
    bool dynamic_fits_ = false;
    Cls cls_ = Cls::kF64;
};

}  // namespace xstore::layout
