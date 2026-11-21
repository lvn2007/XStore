#include "XStore/XStore_RowLayout.h"

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>

#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>

namespace xstore::layout {
namespace {

// Tag byte and wire type of each FData case (field number << 3 | wire type); index = Kind.
constexpr uint8_t TAG_OF[16] = {0, 0x09, 0x15, 0x18, 0x20, 0x28, 0x30, 0x38, 0x40, 0x4D, 0x51, 0x5D, 0x61, 0x68, 0x72, 0x7A};
constexpr unsigned WIRE_TYPE_OF[16] = {0, 1, 5, 0, 0, 0, 0, 0, 0, 5, 1, 5, 1, 0, 2, 2};
// Largest encoded size of one cell of each kind (0A, length, tag, value; strings excluded).
constexpr size_t MAX_CELL_BYTES[16] = {2, 11, 7, 13, 13, 8, 13, 8, 13, 7, 11, 7, 11, 4, 0, 0};

[[nodiscard]] constexpr bool IsSigned32(Kind k) { return k == Kind::kInt32 || k == Kind::kSint32 || k == Kind::kSfixed32; }
[[nodiscard]] constexpr bool IsString(Kind k) { return k == Kind::kString || k == Kind::kBytes; }
[[nodiscard]] constexpr bool IsNumeric(Kind k) { return k >= Kind::kDouble && k <= Kind::kSfixed64; }

size_t VarintSize(uint64_t v) {
    size_t n = 1;
    while (v >= 0x80) {
        v >>= 7;
        ++n;
    }
    return n;
}

char *PutVarint(char *p, uint64_t v) {
    while (v >= 0x80) {
        *p++ = static_cast<char>(v | 0x80);
        v >>= 7;
    }
    *p++ = static_cast<char>(v);
    return p;
}

// At most 10 bytes, bits past 64 dropped, as protobuf does. false: truncated or longer than 10 bytes.
bool GetVarint(const unsigned char *&p, const unsigned char *end, uint64_t &out) {
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

uint32_t ZigZagDecode32(uint32_t v) { return (v >> 1) ^ (0u - (v & 1u)); }
uint64_t ZigZagDecode64(uint64_t v) { return (v >> 1) ^ (0ull - (v & 1ull)); }
uint32_t ZigZagEncode32(int32_t v) { return (static_cast<uint32_t>(v) << 1) ^ static_cast<uint32_t>(v >> 31); }
uint64_t ZigZagEncode64(int64_t v) { return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63); }

// Strict UTF-8 (no overlong forms, no surrogates, nothing above U+10FFFF): a string field with bad bytes was a parse
// failure for the whole row when rows went through protobuf, and stays a rejected row.
bool ValidUtf8(std::string_view s) {
    size_t i = 0;
    while (i < s.size()) {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (c < 0x80) {
            ++i;
            continue;
        }
        size_t need;
        uint32_t cp;
        if ((c & 0xE0) == 0xC0) { need = 1; cp = c & 0x1F; }
        else if ((c & 0xF0) == 0xE0) { need = 2; cp = c & 0x0F; }
        else if ((c & 0xF8) == 0xF0) { need = 3; cp = c & 0x07; }
        else return false;
        if (i + need >= s.size()) return false;
        for (size_t k = 1; k <= need; ++k) {
            const unsigned char b = static_cast<unsigned char>(s[i + k]);
            if ((b & 0xC0) != 0x80) return false;
            cp = (cp << 6) | (b & 0x3F);
        }
        if ((need == 1 && cp < 0x80) || (need == 2 && cp < 0x800) || (need == 3 && cp < 0x10000)) return false;
        if (cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) return false;
        i += need + 1;
    }
    return true;
}

// raw of a numeric kind from its natural-width little-endian bytes.
uint64_t LoadRaw(Kind kind, const char *p) {
    switch (RowLayout::NaturalWidth(kind)) {
        case 8: {
            uint64_t v;
            std::memcpy(&v, p, 8);
            return v;
        }
        case 4: {
            uint32_t v;
            std::memcpy(&v, p, 4);
            return IsSigned32(kind) ? static_cast<uint64_t>(static_cast<int64_t>(static_cast<int32_t>(v))) : v;
        }
        default: return static_cast<uint8_t>(*p) != 0 ? 1 : 0;
    }
}

[[nodiscard]] Cls DefaultCls(Kind kind) {
    switch (kind) {
        case Kind::kDouble: return Cls::kF64;
        case Kind::kFloat: return Cls::kF32;
        case Kind::kInt32: case Kind::kSint32: case Kind::kSfixed32: return Cls::kI32;
        case Kind::kUint32: case Kind::kFixed32: return Cls::kU32;
        case Kind::kInt64: case Kind::kSint64: case Kind::kSfixed64: return Cls::kI64;
        case Kind::kUint64: case Kind::kFixed64: return Cls::kU64;
        case Kind::kBool: return Cls::kBool;
        default: return Cls::kRef;
    }
}

[[nodiscard]] uint16_t ClsWidth(Cls cls, uint8_t capacity) {
    switch (cls) {
        case Cls::kF64: case Cls::kI64: case Cls::kU64: case Cls::kRef: return 8;
        case Cls::kF32: case Cls::kI32: case Cls::kU32: return 4;
        case Cls::kI16: case Cls::kU16: return 2;
        case Cls::kBool: case Cls::kI8: case Cls::kU8: return 1;
        case Cls::kInline: return static_cast<uint16_t>(capacity + 1);
    }
    return 0;
}

// ---- encoding one cell as RowData.fieldData bytes: 0A <len> <FData bytes> ----

size_t FDataPayloadSize(const Cell &cell) {   // the bytes after the tag byte
    switch (cell.kind) {
        case Kind::kDouble: case Kind::kFixed64: case Kind::kSfixed64: return 8;
        case Kind::kFloat: case Kind::kFixed32: case Kind::kSfixed32: return 4;
        case Kind::kInt32: case Kind::kInt64: case Kind::kUint32: case Kind::kUint64: return VarintSize(cell.raw);
        case Kind::kSint32: return VarintSize(ZigZagEncode32(static_cast<int32_t>(cell.raw)));
        case Kind::kSint64: return VarintSize(ZigZagEncode64(static_cast<int64_t>(cell.raw)));
        case Kind::kBool: return 1;
        case Kind::kString: case Kind::kBytes: return VarintSize(cell.text.size()) + cell.text.size();
        default: return 0;
    }
}

size_t CellWireSize(const Cell &cell) {
    if (cell.kind == Kind::kUnset) return 2;
    const size_t inner = 1 + FDataPayloadSize(cell);
    return 1 + VarintSize(inner) + inner;
}

// Room PutCellWire may need past the write position: 0A, length (5), tag, and for a string its length (5); every other cell is <= 13.
constexpr size_t MAX_CELL_HEAD = 16;

// Writes one cell as 0A <len> <FData bytes> at p and returns the end. A string/bytes cell is written up to its text, which the
// caller appends (it may not fit the chunk).
char *PutCellWire(char *p, const Cell &cell) {
    if (cell.kind == Kind::kUnset) {
        p[0] = '\x0A';
        p[1] = '\x00';
        return p + 2;
    }
    *p++ = '\x0A';
    p = PutVarint(p, 1 + FDataPayloadSize(cell));
    *p++ = static_cast<char>(TAG_OF[static_cast<unsigned>(cell.kind)]);
    switch (cell.kind) {
        case Kind::kDouble: case Kind::kFixed64: case Kind::kSfixed64: std::memcpy(p, &cell.raw, 8); return p + 8;
        case Kind::kFloat: case Kind::kFixed32: case Kind::kSfixed32: std::memcpy(p, &cell.raw, 4); return p + 4;
        case Kind::kInt32: case Kind::kInt64: case Kind::kUint32: case Kind::kUint64: return PutVarint(p, cell.raw);
        case Kind::kSint32: return PutVarint(p, ZigZagEncode32(static_cast<int32_t>(cell.raw)));
        case Kind::kSint64: return PutVarint(p, ZigZagEncode64(static_cast<int64_t>(cell.raw)));
        case Kind::kBool: *p = cell.raw != 0 ? 1 : 0; return p + 1;
        case Kind::kString: case Kind::kBytes: return PutVarint(p, cell.text.size());
        default: return p;
    }
}

// ---- reading one FData's bytes (the wire side of PackFromWire) ----

// false: malformed, or a field FData does not have (unknown fields are rejected, not kept).
bool ParseFData(const unsigned char *p, const unsigned char *end, Cell *cell) {
    *cell = Cell{};
    while (p < end) {
        uint64_t tag = 0;
        if (!GetVarint(p, end, tag) || tag > 0xFFFFFFFFull) return false;
        const unsigned number = static_cast<unsigned>(tag >> 3), wire_type = static_cast<unsigned>(tag & 7);
        if (number < 1 || number > 15 || wire_type != WIRE_TYPE_OF[number]) return false;
        const Kind kind = static_cast<Kind>(number);
        Cell next;
        next.kind = kind;
        if (wire_type == 0) {
            uint64_t v = 0;
            if (!GetVarint(p, end, v)) return false;
            switch (kind) {
                case Kind::kInt32: next.raw = static_cast<uint64_t>(static_cast<int64_t>(static_cast<int32_t>(static_cast<uint32_t>(v)))); break;
                case Kind::kUint32: next.raw = static_cast<uint32_t>(v); break;
                case Kind::kSint32: next.raw = static_cast<uint64_t>(static_cast<int64_t>(static_cast<int32_t>(ZigZagDecode32(static_cast<uint32_t>(v))))); break;
                case Kind::kSint64: next.raw = ZigZagDecode64(v); break;
                case Kind::kBool: next.raw = v != 0 ? 1 : 0; break;
                default: next.raw = v; break;   // int64, uint64
            }
        } else if (wire_type == 1) {
            if (end - p < 8) return false;
            std::memcpy(&next.raw, p, 8);
            p += 8;
        } else if (wire_type == 5) {
            if (end - p < 4) return false;
            uint32_t v;
            std::memcpy(&v, p, 4);
            p += 4;
            next.raw = kind == Kind::kSfixed32 ? static_cast<uint64_t>(static_cast<int64_t>(static_cast<int32_t>(v))) : v;
        } else {
            uint64_t len = 0;
            if (!GetVarint(p, end, len) || len > static_cast<uint64_t>(end - p)) return false;
            next.text = std::string_view(reinterpret_cast<const char *>(p), static_cast<size_t>(len));
            p += len;
            if (kind == Kind::kString && !ValidUtf8(next.text)) return false;
        }
        *cell = next;   // the last member on the wire wins, as in protobuf
    }
    return true;
}

[[nodiscard]] Cell CellFromFData(const XStore_Proto::FData &f) {
    Cell c;
    switch (f.oneOfData_case()) {
        case XStore_Proto::FData::kDoubleData: c.kind = Kind::kDouble; c.raw = std::bit_cast<uint64_t>(f.doubledata()); break;
        case XStore_Proto::FData::kFloatData: c.kind = Kind::kFloat; c.raw = std::bit_cast<uint32_t>(f.floatdata()); break;
        case XStore_Proto::FData::kInt32Data: c.kind = Kind::kInt32; c.raw = static_cast<uint64_t>(static_cast<int64_t>(f.int32data())); break;
        case XStore_Proto::FData::kInt64Data: c.kind = Kind::kInt64; c.raw = static_cast<uint64_t>(f.int64data()); break;
        case XStore_Proto::FData::kUint32Data: c.kind = Kind::kUint32; c.raw = f.uint32data(); break;
        case XStore_Proto::FData::kUint64Data: c.kind = Kind::kUint64; c.raw = f.uint64data(); break;
        case XStore_Proto::FData::kSint32Data: c.kind = Kind::kSint32; c.raw = static_cast<uint64_t>(static_cast<int64_t>(f.sint32data())); break;
        case XStore_Proto::FData::kSint64Data: c.kind = Kind::kSint64; c.raw = static_cast<uint64_t>(f.sint64data()); break;
        case XStore_Proto::FData::kFixed32Data: c.kind = Kind::kFixed32; c.raw = f.fixed32data(); break;
        case XStore_Proto::FData::kFixed64Data: c.kind = Kind::kFixed64; c.raw = f.fixed64data(); break;
        case XStore_Proto::FData::kSfixed32Data: c.kind = Kind::kSfixed32; c.raw = static_cast<uint64_t>(static_cast<int64_t>(f.sfixed32data())); break;
        case XStore_Proto::FData::kSfixed64Data: c.kind = Kind::kSfixed64; c.raw = static_cast<uint64_t>(f.sfixed64data()); break;
        case XStore_Proto::FData::kBoolData: c.kind = Kind::kBool; c.raw = f.booldata() ? 1 : 0; break;
        case XStore_Proto::FData::kStringData: c.kind = Kind::kString; c.text = f.stringdata(); break;
        case XStore_Proto::FData::kBytesData: c.kind = Kind::kBytes; c.text = f.bytesdata(); break;
        case XStore_Proto::FData::ONEOFDATA_NOT_SET: break;
    }
    return c;
}

}  // namespace

std::string DescribePackResult(const PackResult &result) {
    switch (result.status) {
        case PackStatus::kOk: return "ok";
        case PackStatus::kRowTooLarge: return "the row does not fit the DB's row length";
        case PackStatus::kKindMismatch: return fmt::format("column {} holds a different kind than its declared type", result.column);
        case PackStatus::kOutOfRange: return fmt::format("column {} holds a value outside its declared storage range", result.column);
        case PackStatus::kStringTooLong: return fmt::format("column {} holds a string longer than its declared capacity", result.column);
        case PackStatus::kMalformed: return fmt::format("the row is malformed at column {}", result.column);
        case PackStatus::kTooManyColumns: return "the row has more than 65535 columns";
        case PackStatus::kUnknownField:
            return result.column == PackResult::ROW_LEVEL ? std::string("the row carries an unknown protobuf field")
                                                          : fmt::format("column {} carries an unknown protobuf field", result.column);
    }
    return "unknown";
}

namespace {
// Steady-clock nanoseconds of the last corrupt-row warning; 0 = none yet. One warning a minute for the whole process: a damaged
// file can hold millions of bad slots and every read of one would otherwise log.
std::atomic<int64_t> last_corrupt_row_log_ns{0};

[[nodiscard]] bool ClaimCorruptRowLog() {
    const int64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    constexpr int64_t MINUTE_NS = 60'000'000'000;
    int64_t last = last_corrupt_row_log_ns.load(std::memory_order_relaxed);
    while (last == 0 || now - last >= MINUTE_NS) {
        if (last_corrupt_row_log_ns.compare_exchange_weak(last, now, std::memory_order_relaxed)) return true;
    }
    return false;
}
}  // namespace

void ResetCorruptRowLogForTests() { last_corrupt_row_log_ns.store(0, std::memory_order_relaxed); }

void RowLayout::ReportCorruptRow(uint8_t marker, uint32_t columns) const {
    if (!ClaimCorruptRowLog()) return;
    spdlog::warn("[ROW] - corrupt row skipped: marker {:#04x}, {} columns in a {}-byte slot (the data file is damaged); further corrupt rows "
                 "are not logged for a minute", marker, columns, row_length_);
}

void RowLayout::ReportOversizedRow() const {
    if (!ClaimCorruptRowLog()) return;
    spdlog::warn("[ROW] - corrupt row skipped: its string references overlap and would encode beyond {} bytes in a {}-byte slot (the data file "
                 "is damaged); further corrupt rows are not logged for a minute", max_encoded_, row_length_);
}

double CellAsDouble(const Cell &cell) {
    switch (cell.kind) {
        case Kind::kDouble: return std::bit_cast<double>(cell.raw);
        case Kind::kFloat: return static_cast<double>(std::bit_cast<float>(static_cast<uint32_t>(cell.raw)));
        case Kind::kInt32: case Kind::kSint32: case Kind::kSfixed32: return static_cast<double>(static_cast<int32_t>(cell.raw));
        case Kind::kInt64: case Kind::kSint64: case Kind::kSfixed64: return static_cast<double>(static_cast<int64_t>(cell.raw));
        default: return static_cast<double>(cell.raw);   // uint32, uint64, fixed32, fixed64
    }
}

long double CellAsLongDouble(const Cell &cell) {
    switch (cell.kind) {
        case Kind::kDouble: return static_cast<long double>(std::bit_cast<double>(cell.raw));
        case Kind::kFloat: return static_cast<long double>(std::bit_cast<float>(static_cast<uint32_t>(cell.raw)));
        case Kind::kInt32: case Kind::kSint32: case Kind::kSfixed32: return static_cast<long double>(static_cast<int32_t>(cell.raw));
        case Kind::kInt64: case Kind::kSint64: case Kind::kSfixed64: return static_cast<long double>(static_cast<int64_t>(cell.raw));
        default: return static_cast<long double>(cell.raw);
    }
}

std::optional<RowLayout> RowLayout::Make(std::span<const ColumnSpec> schema, size_t row_length, std::string *error) {
    auto fail = [&](std::string message) -> std::optional<RowLayout> {
        if (error != nullptr) *error = std::move(message);
        return std::nullopt;
    };
    if (schema.size() > MAX_COLUMNS) return fail(fmt::format("a schema holds at most {} columns", MAX_COLUMNS));
    RowLayout layout;
    layout.row_length_ = row_length;
    size_t offset = HEADER_BYTES + (schema.size() + 7) / 8;
    for (size_t i = 0; i < schema.size(); ++i) {
        const ColumnSpec &spec = schema[i];
        const unsigned kind = static_cast<unsigned>(spec.kind);
        if (kind < 1 || kind > 15) return fail(fmt::format("column {}: kind {} is not one of the 15 data kinds", i, kind));
        Cls cls = DefaultCls(spec.kind);
        switch (spec.storage) {
            case Storage::kDefault: break;
            case Storage::kInt8: case Storage::kInt16:
                if (!(IsSigned32(spec.kind) || spec.kind == Kind::kInt64 || spec.kind == Kind::kSint64 || spec.kind == Kind::kSfixed64))
                    return fail(fmt::format("column {}: INT8/INT16 storage needs a signed integer kind", i));
                cls = spec.storage == Storage::kInt8 ? Cls::kI8 : Cls::kI16;
                break;
            case Storage::kUint8: case Storage::kUint16:
                if (!(spec.kind == Kind::kUint32 || spec.kind == Kind::kFixed32 || spec.kind == Kind::kUint64 || spec.kind == Kind::kFixed64))
                    return fail(fmt::format("column {}: UINT8/UINT16 storage needs an unsigned integer kind", i));
                cls = spec.storage == Storage::kUint8 ? Cls::kU8 : Cls::kU16;
                break;
            case Storage::kInlineString:
                if (!IsString(spec.kind)) return fail(fmt::format("column {}: INLINE_STRING storage needs the string or bytes kind", i));
                if (spec.capacity < 1) return fail(fmt::format("column {}: INLINE_STRING capacity must be 1..255", i));
                cls = Cls::kInline;
                break;
            default: return fail(fmt::format("column {}: unknown storage {}", i, static_cast<unsigned>(spec.storage)));
        }
        if (spec.storage != Storage::kInlineString && spec.capacity != 0) return fail(fmt::format("column {}: capacity is only for INLINE_STRING", i));
        TypedColumn col;
        col.offset = static_cast<uint32_t>(offset);
        col.kind = spec.kind;
        col.cls = cls;
        col.capacity = spec.capacity;
        col.width = ClsWidth(cls, spec.capacity);
        offset += col.width;
        layout.typed_.push_back(col);
    }
    layout.dyn_start_ = offset;
    if (offset > row_length) {
        return fail(fmt::format("row length {} is below the schema minimum {}", row_length, offset));
    }
    // Upper bound of any row's encoded size, without counting a slot byte twice. Typed columns: a numeric/bool column at its
    // kind's largest cell; an INLINE_STRING(N) column N bytes plus its header (its bytes live in the typed cell); a reference
    // string column only its header (its bytes come out of the pool below). Pool P = everything after the typed cells: a
    // numeric dynamic cell takes 9 slot bytes and encodes to at most 13; a string cell takes 9 + n slot bytes and encodes to
    // header + n with header <= 12 < 13; string tail bytes encode 1:1. So the pool's worst case is all numeric cells:
    // (P / 9) * 13, plus P % 9 leftover tail bytes.
    // A string header (0A + len + 72/7A + len) is 6 bytes while n < 16 KB. A longer reference string's 2 extra header bytes
    // stay covered: its n pool bytes are counted at 13/9 above, a slack of about 4n/9.
    size_t bound = 0;
    for (const TypedColumn &col : layout.typed_) {
        if (col.cls == Cls::kInline) bound += col.capacity + 6;   // capacity <= 255: both lengths fit 2-byte varints
        else if (IsString(col.kind)) bound += 6;
        else bound += MAX_CELL_BYTES[static_cast<unsigned>(col.kind)];
    }
    const size_t pool = row_length - offset;
    bound += (pool / DYNAMIC_CELL_BYTES) * 13 + pool % DYNAMIC_CELL_BYTES;
    layout.max_encoded_ = bound;
    return layout;
}

std::vector<ColumnSpec> RowLayout::SchemaFromProto(const google::protobuf::RepeatedPtrField<XStore_Proto::ColumnSpec> &specs) {
    std::vector<ColumnSpec> out;
    out.reserve(static_cast<size_t>(specs.size()));
    for (const XStore_Proto::ColumnSpec &s : specs) {
        ColumnSpec c;
        c.kind = s.kind() <= 15 ? static_cast<Kind>(s.kind()) : Kind::kUnset;   // Make() rejects kUnset
        c.storage = s.storage() <= 5 ? static_cast<Storage>(s.storage()) : static_cast<Storage>(255);
        c.capacity = s.capacity() <= 255 ? static_cast<uint8_t>(s.capacity()) : static_cast<uint8_t>(0);
        out.push_back(c);
    }
    return out;
}

void RowLayout::SchemaToProto(std::span<const ColumnSpec> schema, google::protobuf::RepeatedPtrField<XStore_Proto::ColumnSpec> *out) {
    out->Clear();
    for (const ColumnSpec &c : schema) {
        XStore_Proto::ColumnSpec *s = out->Add();
        s->set_kind(static_cast<uint32_t>(c.kind));
        s->set_storage(static_cast<uint32_t>(c.storage));
        s->set_capacity(c.capacity);
    }
}

// ---------------------------------------------------------------- writing

PackResult RowLayout::PutCell(char *slot, uint32_t column, const Cell &cell, std::vector<Deferred> &deferred) const {
    const uint32_t typed = TypedColumns();
    if (column < typed) {
        if (cell.kind == Kind::kUnset) return {};   // null: the bitmap bit stays clear
        const TypedColumn &tc = typed_[column];
        if (cell.kind != tc.kind) return {PackStatus::kKindMismatch, column};
        char *p = slot + tc.offset;
        switch (tc.cls) {
            case Cls::kF64: case Cls::kI64: case Cls::kU64: std::memcpy(p, &cell.raw, 8); break;
            case Cls::kF32: case Cls::kI32: case Cls::kU32: {
                const uint32_t v = static_cast<uint32_t>(cell.raw);
                std::memcpy(p, &v, 4);
                break;
            }
            case Cls::kBool: *p = cell.raw != 0 ? 1 : 0; break;
            case Cls::kI8: case Cls::kI16: {
                const int64_t v = static_cast<int64_t>(cell.raw);
                const int64_t lo = tc.cls == Cls::kI8 ? INT8_MIN : INT16_MIN, hi = tc.cls == Cls::kI8 ? INT8_MAX : INT16_MAX;
                if (v < lo || v > hi) return {PackStatus::kOutOfRange, column};
                std::memcpy(p, &v, tc.width);
                break;
            }
            case Cls::kU8: case Cls::kU16: {
                const uint64_t hi = tc.cls == Cls::kU8 ? UINT8_MAX : UINT16_MAX;
                if (cell.raw > hi) return {PackStatus::kOutOfRange, column};
                std::memcpy(p, &cell.raw, tc.width);
                break;
            }
            case Cls::kInline:
                if (cell.text.size() > tc.capacity) return {PackStatus::kStringTooLong, column};
                *p = static_cast<char>(cell.text.size());
                std::memcpy(p + 1, cell.text.data(), cell.text.size());
                break;
            case Cls::kRef: deferred.push_back({tc.offset, cell.text}); break;
        }
        slot[HEADER_BYTES + column / 8] = static_cast<char>(static_cast<uint8_t>(slot[HEADER_BYTES + column / 8]) | (1u << (column % 8)));
        return {};
    }
    const size_t off = dyn_start_ + DYNAMIC_CELL_BYTES * (column - typed);
    if (off + DYNAMIC_CELL_BYTES > row_length_) return {PackStatus::kRowTooLarge, column};
    if (cell.kind == Kind::kUnset) return {};   // type byte 0, already zero
    char *p = slot + off;
    p[0] = static_cast<char>(cell.kind);
    if (IsString(cell.kind)) deferred.push_back({static_cast<uint32_t>(off + 1), cell.text});
    else std::memcpy(p + 1, &cell.raw, NaturalWidth(cell.kind));   // little-endian: the low bytes
    return {};
}

PackResult RowLayout::Finish(char *slot, uint32_t columns, std::vector<Deferred> &deferred) const {
    if (columns > MAX_COLUMNS) return {PackStatus::kTooManyColumns, columns};
    if (columns == 0) return {};   // an empty row stays a hole, as an empty protobuf row (0 bytes) always did
    const uint32_t typed = TypedColumns();
    size_t tail = dyn_start_ + (columns > typed ? DYNAMIC_CELL_BYTES * (columns - typed) : 0);
    if (tail > row_length_) return {PackStatus::kRowTooLarge, columns};
    for (const Deferred &d : deferred) {
        if (tail + d.text.size() > row_length_) return {PackStatus::kRowTooLarge, columns};
        std::memcpy(slot + tail, d.text.data(), d.text.size());
        const uint32_t offset = static_cast<uint32_t>(tail), length = static_cast<uint32_t>(d.text.size());
        std::memcpy(slot + d.cell_offset, &offset, 4);
        std::memcpy(slot + d.cell_offset + 4, &length, 4);
        tail += d.text.size();
    }
    slot[0] = static_cast<char>(MARKER);
    const uint16_t n = static_cast<uint16_t>(columns);
    std::memcpy(slot + 1, &n, sizeof(n));
    return {};
}

PackResult RowLayout::PackFromRowData(const XStore_Proto::RowData &row, char *slot) const {
    thread_local std::vector<Deferred> deferred;
    deferred.clear();
    // A request that reached libprotobuf keeps the fields it does not know; storing the row without them would drop a value the
    // client sent without a word, so such a row is rejected (what PackFromWire does to the same bytes: ParseFData/kMalformed).
    if (!row.unknown_fields().empty()) return {PackStatus::kUnknownField, PackResult::ROW_LEVEL};
    const int count = row.fielddata_size();
    if (static_cast<size_t>(count) > MAX_COLUMNS) return {PackStatus::kTooManyColumns, static_cast<uint32_t>(count)};
    for (int i = 0; i < count; ++i) {
        if (!row.fielddata(i).unknown_fields().empty()) return {PackStatus::kUnknownField, static_cast<uint32_t>(i)};
        const PackResult r = PutCell(slot, static_cast<uint32_t>(i), CellFromFData(row.fielddata(i)), deferred);
        if (!r.Ok()) return r;
    }
    return Finish(slot, static_cast<uint32_t>(count), deferred);
}

PackResult RowLayout::PackFromWire(const char *wire, size_t length, char *slot) const {
    thread_local std::vector<Deferred> deferred;
    deferred.clear();
    const unsigned char *p = reinterpret_cast<const unsigned char *>(wire);
    const unsigned char *const end = p + length;
    uint32_t column = 0;
    while (p < end) {
        // The commonest cell by far: a double, 0A 09 09 <8 bytes> (fieldData, length 9, doubleData tag, fixed64).
        if (end - p >= 11 && p[0] == 0x0A && p[1] == 0x09 && p[2] == 0x09 && column < MAX_COLUMNS) {
            Cell fast;
            fast.kind = Kind::kDouble;
            std::memcpy(&fast.raw, p + 3, 8);
            const PackResult r = PutCell(slot, column, fast, deferred);
            if (!r.Ok()) return r;
            p += 11;
            ++column;
            continue;
        }
        uint64_t tag = 0, cell_length = 0;
        if (!GetVarint(p, end, tag) || tag != 0x0A) return {PackStatus::kMalformed, column};   // RowData.fieldData only
        if (!GetVarint(p, end, cell_length) || cell_length > static_cast<uint64_t>(end - p)) return {PackStatus::kMalformed, column};
        Cell cell;
        if (!ParseFData(p, p + cell_length, &cell)) return {PackStatus::kMalformed, column};
        p += cell_length;
        if (column >= MAX_COLUMNS) return {PackStatus::kTooManyColumns, column};
        const PackResult r = PutCell(slot, column, cell, deferred);
        if (!r.Ok()) return r;
        ++column;
    }
    return Finish(slot, column, deferred);
}

// ---------------------------------------------------------------- reading

bool RowLayout::RefInRow(uint32_t columns, uint32_t offset, uint32_t length) const {
    const uint32_t typed = TypedColumns();
    const size_t tail_start = dyn_start_ + (columns > typed ? DYNAMIC_CELL_BYTES * (columns - typed) : 0);
    return offset >= tail_start && static_cast<size_t>(offset) + length <= row_length_;
}

bool RowLayout::Load(const char *row, uint32_t column, Cell *cell) const {
    if (!IsLive(row)) return false;
    const uint32_t n = ColumnCount(row);
    if (column >= n) return false;
    const uint32_t typed = TypedColumns();
    if (column < typed) {
        if (!((static_cast<uint8_t>(row[HEADER_BYTES + column / 8]) >> (column % 8)) & 1u)) return false;
        const TypedColumn &tc = typed_[column];
        const char *p = row + tc.offset;
        cell->kind = tc.kind;
        cell->text = {};
        switch (tc.cls) {
            case Cls::kF64: case Cls::kI64: case Cls::kU64: std::memcpy(&cell->raw, p, 8); return true;
            case Cls::kF32: case Cls::kU32: {
                uint32_t v;
                std::memcpy(&v, p, 4);
                cell->raw = v;
                return true;
            }
            case Cls::kI32: {
                int32_t v;
                std::memcpy(&v, p, 4);
                cell->raw = static_cast<uint64_t>(static_cast<int64_t>(v));
                return true;
            }
            case Cls::kBool: cell->raw = static_cast<uint8_t>(*p) != 0 ? 1 : 0; return true;
            case Cls::kI8: cell->raw = static_cast<uint64_t>(static_cast<int64_t>(static_cast<int8_t>(*p))); return true;
            case Cls::kI16: {
                int16_t v;
                std::memcpy(&v, p, 2);
                cell->raw = static_cast<uint64_t>(static_cast<int64_t>(v));
                return true;
            }
            case Cls::kU8: cell->raw = static_cast<uint8_t>(*p); return true;
            case Cls::kU16: {
                uint16_t v;
                std::memcpy(&v, p, 2);
                cell->raw = v;
                return true;
            }
            case Cls::kInline: {
                const size_t len = static_cast<uint8_t>(*p);
                if (len > tc.capacity) return false;
                cell->text = std::string_view(p + 1, len);
                return true;
            }
            case Cls::kRef: {
                uint32_t off, len;
                std::memcpy(&off, p, 4);
                std::memcpy(&len, p + 4, 4);
                if (!RefInRow(n, off, len)) return false;
                cell->text = std::string_view(row + off, len);
                return true;
            }
        }
        return false;
    }
    const size_t off = dyn_start_ + DYNAMIC_CELL_BYTES * (column - typed);
    if (off + DYNAMIC_CELL_BYTES > row_length_) return false;
    const unsigned kind = static_cast<uint8_t>(row[off]);
    if (kind < 1 || kind > 15) return false;
    cell->kind = static_cast<Kind>(kind);
    cell->text = {};
    cell->raw = 0;
    if (IsString(cell->kind)) {
        uint32_t o, len;
        std::memcpy(&o, row + off + 1, 4);
        std::memcpy(&len, row + off + 5, 4);
        if (!RefInRow(n, o, len)) return false;
        cell->text = std::string_view(row + o, len);
    } else {
        cell->raw = LoadRaw(cell->kind, row + off + 1);
    }
    return true;
}

bool RowLayout::ReadNumeric(const char *row, uint32_t column, double *out) const {
    Cell cell;
    if (!Load(row, column, &cell) || !IsNumeric(cell.kind)) return false;
    *out = CellAsDouble(cell);
    return true;
}

bool RowLayout::ReadNumericExact(const char *row, uint32_t column, long double *out) const {
    Cell cell;
    if (!Load(row, column, &cell) || !IsNumeric(cell.kind)) return false;
    *out = CellAsLongDouble(cell);
    return true;
}

Observation RowLayout::Observe(const char *row, uint32_t column) const {
    Cell cell;
    if (!Load(row, column, &cell)) return Observation{};
    return ObservationOf(cell);
}

Observation RowLayout::ObservationOf(const Cell &cell) {
    Observation obs;
    obs.present = true;
    if (IsNumeric(cell.kind)) {
        obs.is_numeric = true;
        obs.value = QuietNaN(CellAsDouble(cell));
    }
    return obs;
}

bool RowLayout::ReadFData(const char *row, uint32_t column, XStore_Proto::FData *out) const {
    Cell cell;
    if (!Load(row, column, &cell)) return false;
    CellToFData(cell, out);
    return true;
}

void RowLayout::CellToFData(const Cell &cell, XStore_Proto::FData *out) {
    out->Clear();
    switch (cell.kind) {
        case Kind::kDouble: out->set_doubledata(std::bit_cast<double>(cell.raw)); break;
        case Kind::kFloat: out->set_floatdata(std::bit_cast<float>(static_cast<uint32_t>(cell.raw))); break;
        case Kind::kInt32: out->set_int32data(static_cast<int32_t>(cell.raw)); break;
        case Kind::kInt64: out->set_int64data(static_cast<int64_t>(cell.raw)); break;
        case Kind::kUint32: out->set_uint32data(static_cast<uint32_t>(cell.raw)); break;
        case Kind::kUint64: out->set_uint64data(cell.raw); break;
        case Kind::kSint32: out->set_sint32data(static_cast<int32_t>(cell.raw)); break;
        case Kind::kSint64: out->set_sint64data(static_cast<int64_t>(cell.raw)); break;
        case Kind::kFixed32: out->set_fixed32data(static_cast<uint32_t>(cell.raw)); break;
        case Kind::kFixed64: out->set_fixed64data(cell.raw); break;
        case Kind::kSfixed32: out->set_sfixed32data(static_cast<int32_t>(cell.raw)); break;
        case Kind::kSfixed64: out->set_sfixed64data(static_cast<int64_t>(cell.raw)); break;
        case Kind::kBool: out->set_booldata(cell.raw != 0); break;
        case Kind::kString: out->set_stringdata(std::string(cell.text)); break;
        case Kind::kBytes: out->set_bytesdata(std::string(cell.text)); break;
        case Kind::kUnset: break;
    }
}

// ---------------------------------------------------------------- replies

// The three reply functions agree on every slot, corrupt ones included: a slot that is not LiveAndSane, or whose cells would encode
// beyond max_encoded_ (string references may overlap, so the bound is not implied by the column count), is skipped whole: size 0,
// nothing appended, an empty RowData. So no reply is ever larger than the bound the lease and the 2 GB check were sized with.
size_t RowLayout::EncodedSize(const char *row) const {
    if (!LiveAndSane(row)) return 0;
    const uint32_t n = ColumnCount(row);
    size_t total = 0;
    Cell cell;
    for (uint32_t c = 0; c < n; ++c) {
        if (!Load(row, c, &cell)) cell = Cell{};
        total += CellWireSize(cell);
    }
    if (total > max_encoded_) {
        ReportOversizedRow();
        return 0;
    }
    return total;
}

void RowLayout::EncodeToWire(const char *row, std::string &out) const {
    if (!LiveAndSane(row)) return;
    const uint32_t n = ColumnCount(row);
    const uint32_t typed = TypedColumns();
    const size_t start = out.size();
    // Cells are written into a stack chunk and appended in a few big pieces; flushed = bytes already in out since start.
    constexpr size_t CHUNK = 2048;
    char chunk[CHUNK + MAX_CELL_HEAD];
    char *p = chunk;
    size_t flushed = 0;
    const auto flush = [&] {
        out.append(chunk, static_cast<size_t>(p - chunk));
        flushed += static_cast<size_t>(p - chunk);
        p = chunk;
    };
    bool oversized = false;
    Cell cell;
    for (uint32_t c = 0; c < n; ++c) {
        if (static_cast<size_t>(p - chunk) >= CHUNK) flush();
        // A present double is always 0A 09 09 <8 payload bytes>. LiveAndSane already proved every dynamic cell lies inside the slot.
        if (c < typed) {
            if (typed_[c].cls == Cls::kF64) {
                if ((static_cast<uint8_t>(row[HEADER_BYTES + c / 8]) >> (c % 8)) & 1u) {
                    p[0] = '\x0A';
                    p[1] = '\x09';
                    p[2] = '\x09';
                    std::memcpy(p + 3, row + typed_[c].offset, 8);
                    p += 11;
                } else {
                    p[0] = '\x0A';
                    p[1] = '\x00';
                    p += 2;
                }
                continue;
            }
        } else {
            const char *cellp = row + dyn_start_ + DYNAMIC_CELL_BYTES * (c - typed);
            if (static_cast<uint8_t>(cellp[0]) == static_cast<uint8_t>(Kind::kDouble)) {
                // A run of doubles (the common wide row): as many as the chunk has room for, with no per-cell room check.
                uint32_t run = std::min<uint32_t>(n - c, static_cast<uint32_t>((CHUNK + MAX_CELL_HEAD - static_cast<size_t>(p - chunk)) / 11));
                do {
                    p[0] = '\x0A';
                    p[1] = '\x09';
                    p[2] = '\x09';
                    std::memcpy(p + 3, cellp + 1, 8);
                    p += 11;
                    cellp += DYNAMIC_CELL_BYTES;
                    ++c;
                } while (--run != 0 && static_cast<uint8_t>(cellp[0]) == static_cast<uint8_t>(Kind::kDouble));
                --c;   // the for loop's increment moves on to the first cell not yet written
                continue;
            }
        }
        if (!Load(row, c, &cell)) cell = Cell{};
        p = PutCellWire(p, cell);
        const bool text = IsString(cell.kind);
        // String references of a damaged row may overlap, so the total is checked at every cell, before the text is copied.
        if (flushed + static_cast<size_t>(p - chunk) + (text ? cell.text.size() : 0) > max_encoded_) {
            oversized = true;
            break;
        }
        if (text) {
            flush();
            out.append(cell.text.data(), cell.text.size());
            flushed += cell.text.size();
        }
    }
    if (!oversized) {
        flush();
        if (flushed <= max_encoded_) return;
    }
    out.resize(start);
    ReportOversizedRow();
}

void RowLayout::DecodeToRowData(const char *row, XStore_Proto::RowData *out) const {
    out->Clear();
    if (!LiveAndSane(row)) return;
    const uint32_t n = ColumnCount(row);
    size_t total = 0;
    Cell cell;
    for (uint32_t c = 0; c < n; ++c) {
        if (!Load(row, c, &cell)) cell = Cell{};
        total += CellWireSize(cell);
        if (total > max_encoded_) {   // the same bound as EncodedSize, checked before the cell is copied into an object
            out->Clear();
            ReportOversizedRow();
            return;
        }
        CellToFData(cell, out->add_fielddata());
    }
}

}  // namespace xstore::layout
