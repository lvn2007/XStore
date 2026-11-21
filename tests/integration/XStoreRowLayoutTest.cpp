#include "XStore/XStore_RowLayout.h"
#include "XStore/XStore_WireEncode.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
#include <random>
#include <string>
#include <vector>

using namespace xstore::layout;

namespace {

constexpr size_t SLOT = 96;

std::vector<ColumnSpec> NoSchema() { return {}; }

RowLayout MakeLayout(const std::vector<ColumnSpec> &schema, size_t row_length = SLOT) {
    std::string error;
    auto layout = RowLayout::Make(schema, row_length, &error);
    EXPECT_TRUE(layout.has_value()) << error;
    return *layout;
}

// One cell of every kind, in field-number order, with values that exercise sign and size.
XStore_Proto::RowData EveryKindRow() {
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(-2.5);
    row.add_fielddata()->set_floatdata(1.5f);
    row.add_fielddata()->set_int32data(-7);
    row.add_fielddata()->set_int64data(-9000000000LL);
    row.add_fielddata()->set_uint32data(4000000000u);
    row.add_fielddata()->set_uint64data(18000000000000000000ull);
    row.add_fielddata()->set_sint32data(-123456);
    row.add_fielddata()->set_sint64data(-5000000000LL);
    row.add_fielddata()->set_fixed32data(0xDEADBEEFu);
    row.add_fielddata()->set_fixed64data(0x0123456789ABCDEFull);
    row.add_fielddata()->set_sfixed32data(-42);
    row.add_fielddata()->set_sfixed64data(-4200000000LL);
    row.add_fielddata()->set_booldata(true);
    row.add_fielddata()->set_stringdata("hello");
    row.add_fielddata()->set_bytesdata(std::string("\x00\x01\xFF", 3));
    row.add_fielddata();   // unset
    return row;
}

std::vector<char> Slot(size_t n = SLOT) { return std::vector<char>(n, 0); }

}  // namespace

// ---------------------------------------------------------------- schema

TEST(RowLayoutMake, EmptySchemaIsAllDynamic) {
    const RowLayout layout = MakeLayout(NoSchema());
    EXPECT_EQ(layout.TypedColumns(), 0u);
    EXPECT_EQ(layout.MinRowBytes(), 3u);
    EXPECT_EQ(layout.RowLength(), SLOT);
}

TEST(RowLayoutMake, TypedColumnMapOffsetsAndWidths) {
    const std::vector<ColumnSpec> schema = {
        {Kind::kDouble, Storage::kDefault, 0}, {Kind::kInt32, Storage::kInt16, 0}, {Kind::kBool, Storage::kDefault, 0},
        {Kind::kString, Storage::kDefault, 0}, {Kind::kString, Storage::kInlineString, 5}, {Kind::kFloat, Storage::kDefault, 0}};
    const RowLayout layout = MakeLayout(schema);
    const auto &t = layout.Typed();
    ASSERT_EQ(t.size(), 6u);
    // header 3 + bitmap ceil(6/8)=1 -> first cell at 4
    EXPECT_EQ(t[0].offset, 4u);  EXPECT_EQ(t[0].width, 8);
    EXPECT_EQ(t[1].offset, 12u); EXPECT_EQ(t[1].width, 2);
    EXPECT_EQ(t[2].offset, 14u); EXPECT_EQ(t[2].width, 1);
    EXPECT_EQ(t[3].offset, 15u); EXPECT_EQ(t[3].width, 8);
    EXPECT_EQ(t[4].offset, 23u); EXPECT_EQ(t[4].width, 6);
    EXPECT_EQ(t[5].offset, 29u); EXPECT_EQ(t[5].width, 4);
    EXPECT_EQ(layout.MinRowBytes(), 33u);
}

TEST(RowLayoutMake, RejectsBadSchemas) {
    std::string error;
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kUnset, Storage::kDefault, 0}}, SLOT, &error));
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kDouble, Storage::kInt16, 0}}, SLOT, &error));      // narrow int on a double
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kUint32, Storage::kInt8, 0}}, SLOT, &error));       // signed storage, unsigned kind
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kInt32, Storage::kUint8, 0}}, SLOT, &error));       // unsigned storage, signed kind
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kInt32, Storage::kInlineString, 4}}, SLOT, &error)); // inline on a number
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kString, Storage::kInlineString, 0}}, SLOT, &error));// capacity 0
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kDouble, Storage::kDefault, 3}}, SLOT, &error));    // stray capacity
    EXPECT_FALSE(RowLayout::Make(std::vector<ColumnSpec>{{Kind::kDouble, static_cast<Storage>(77), 0}}, SLOT, &error));
}

TEST(RowLayoutMake, RejectsRowLengthBelowTheSchemaMinimum) {
    std::string error;
    const std::vector<ColumnSpec> schema(4, ColumnSpec{Kind::kDouble, Storage::kDefault, 0});   // 3 + 1 + 32 = 36
    EXPECT_FALSE(RowLayout::Make(schema, 35, &error));
    EXPECT_NE(error.find("36"), std::string::npos) << error;
    EXPECT_TRUE(RowLayout::Make(schema, 36, &error));
}

TEST(RowLayoutMake, SchemaRoundTripsThroughTheProtoMessages) {
    const std::vector<ColumnSpec> schema = {{Kind::kInt64, Storage::kInt8, 0}, {Kind::kBytes, Storage::kInlineString, 9}};
    google::protobuf::RepeatedPtrField<XStore_Proto::ColumnSpec> specs;
    RowLayout::SchemaToProto(schema, &specs);
    const std::vector<ColumnSpec> back = RowLayout::SchemaFromProto(specs);
    ASSERT_EQ(back.size(), 2u);
    EXPECT_EQ(back[0].kind, Kind::kInt64);   EXPECT_EQ(back[0].storage, Storage::kInt8);
    EXPECT_EQ(back[1].kind, Kind::kBytes);   EXPECT_EQ(back[1].storage, Storage::kInlineString); EXPECT_EQ(back[1].capacity, 9);
    XStore_Proto::ColumnSpec junk;
    junk.set_kind(99);
    specs.Add()->CopyFrom(junk);
    std::string error;
    EXPECT_FALSE(RowLayout::Make(RowLayout::SchemaFromProto(specs), SLOT, &error));   // an out-of-range kind never becomes valid
}

// ---------------------------------------------------------------- golden bytes

TEST(RowLayoutPack, DynamicRowGoldenBytes) {
    // double 3.14 | int32 7 | bool true | string "AB" | unset      (the example in the spec)
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(3.14);
    row.add_fielddata()->set_int32data(7);
    row.add_fielddata()->set_booldata(true);
    row.add_fielddata()->set_stringdata("AB");
    row.add_fielddata();
    const RowLayout layout = MakeLayout(NoSchema(), 64);
    std::vector<char> slot = Slot(64);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());

    std::vector<uint8_t> want(64, 0);
    want[0] = 0x01; want[1] = 5; want[2] = 0;
    want[3] = 0x01;                                    // cell 0: double
    const double d = 3.14;
    std::memcpy(&want[4], &d, 8);
    want[12] = 0x03; want[13] = 7;                     // cell 1: int32 7 (4 natural bytes, rest zero)
    want[21] = 0x0D; want[22] = 1;                     // cell 2: bool
    want[30] = 0x0E; want[31] = 48; want[35] = 2;      // cell 3: string, offset 48 (= 3 + 9*5), length 2
    // cell 4 (unset) is nine zero bytes at 39..47
    want[48] = 'A'; want[49] = 'B';                    // tail
    EXPECT_EQ(std::vector<uint8_t>(slot.begin(), slot.end()), want);

    // The same row from its wire bytes gives the same slot.
    std::vector<char> from_wire = Slot(64);
    ASSERT_TRUE(layout.PackFromWire(row.SerializeAsString().data(), row.ByteSizeLong(), from_wire.data()).Ok());
    EXPECT_EQ(from_wire, slot);
}

TEST(RowLayoutPack, TypedRowGoldenBytesAndNulls) {
    const std::vector<ColumnSpec> schema = {{Kind::kDouble, Storage::kDefault, 0}, {Kind::kInt32, Storage::kInt16, 0},
                                            {Kind::kString, Storage::kInlineString, 4}, {Kind::kBool, Storage::kDefault, 0}};
    const RowLayout layout = MakeLayout(schema, 40);   // 3 + 1 + 8 + 2 + 5 + 1 = 20 minimum
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(1.0);
    row.add_fielddata()->set_int32data(-2);
    row.add_fielddata()->set_stringdata("xyz");
    row.add_fielddata();   // column 3: explicit null
    row.add_fielddata()->set_int64data(99);   // one dynamic column after the typed prefix
    std::vector<char> slot = Slot(40);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    EXPECT_EQ(static_cast<uint8_t>(slot[0]), 0x01);
    EXPECT_EQ(static_cast<uint8_t>(slot[1]), 5);
    EXPECT_EQ(static_cast<uint8_t>(slot[3]), 0b0000'0111);   // columns 0,1,2 present; 3 null
    int16_t narrow;
    std::memcpy(&narrow, &slot[12], 2);
    EXPECT_EQ(narrow, -2);
    EXPECT_EQ(static_cast<uint8_t>(slot[14]), 3);            // inline string length
    EXPECT_EQ(std::string(&slot[15], 3), "xyz");
    EXPECT_EQ(static_cast<uint8_t>(slot[20]), 0x04);         // dynamic cell for column 4: int64
    EXPECT_EQ(layout.EncodedSize(slot.data()), row.ByteSizeLong());
}

// ---------------------------------------------------------------- canonical round trips

TEST(RowLayoutRoundTrip, EveryKindDynamicIsByteIdenticalToProtobuf) {
    const RowLayout layout = MakeLayout(NoSchema(), 512);
    const XStore_Proto::RowData row = EveryKindRow();
    const std::string wire = row.SerializeAsString();
    std::vector<char> a = Slot(512), b = Slot(512);
    ASSERT_TRUE(layout.PackFromRowData(row, a.data()).Ok());
    ASSERT_TRUE(layout.PackFromWire(wire.data(), wire.size(), b.data()).Ok());
    EXPECT_EQ(a, b);
    EXPECT_EQ(layout.EncodedSize(a.data()), wire.size());
    std::string out;
    layout.EncodeToWire(a.data(), out);
    EXPECT_EQ(out, wire);                       // the encoder is canonical protobuf
    XStore_Proto::RowData back;
    layout.DecodeToRowData(a.data(), &back);
    EXPECT_EQ(back.SerializeAsString(), wire);
    EXPECT_LE(wire.size(), layout.MaxEncodedRowBytes());
}

TEST(RowLayoutRoundTrip, EveryKindTypedIsByteIdenticalToProtobuf) {
    std::vector<ColumnSpec> schema;
    for (unsigned k = 1; k <= 15; ++k) schema.push_back({static_cast<Kind>(k), Storage::kDefault, 0});   // 15 typed, the 16th unset is dynamic
    const RowLayout layout = MakeLayout(schema, 512);
    const XStore_Proto::RowData row = EveryKindRow();
    const std::string wire = row.SerializeAsString();
    std::vector<char> slot = Slot(512);
    ASSERT_TRUE(layout.PackFromWire(wire.data(), wire.size(), slot.data()).Ok());
    std::string out;
    layout.EncodeToWire(slot.data(), out);
    EXPECT_EQ(out, wire);
    std::vector<char> via_rowdata = Slot(512);
    ASSERT_TRUE(layout.PackFromRowData(row, via_rowdata.data()).Ok());
    EXPECT_EQ(via_rowdata, slot);
    EXPECT_LE(wire.size(), layout.MaxEncodedRowBytes());
}

TEST(RowLayoutRoundTrip, NarrowAndInlineStorageStillEmitTheDeclaredKind) {
    const std::vector<ColumnSpec> schema = {{Kind::kInt32, Storage::kInt8, 0}, {Kind::kUint64, Storage::kUint16, 0},
                                            {Kind::kSint64, Storage::kInt16, 0}, {Kind::kBytes, Storage::kInlineString, 8}};
    const RowLayout layout = MakeLayout(schema, 64);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int32data(-100);
    row.add_fielddata()->set_uint64data(60000);
    row.add_fielddata()->set_sint64data(-30000);
    row.add_fielddata()->set_bytesdata(std::string("\x00\xFF", 2));
    std::vector<char> slot = Slot(64);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    std::string out;
    layout.EncodeToWire(slot.data(), out);
    EXPECT_EQ(out, row.SerializeAsString());
}

TEST(RowLayoutRoundTrip, ShortRowAgainstALongerSchemaEmitsOnlyItsOwnColumns) {
    const std::vector<ColumnSpec> schema(4, ColumnSpec{Kind::kDouble, Storage::kDefault, 0});
    const RowLayout layout = MakeLayout(schema, 64);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(1.0);
    row.add_fielddata()->set_doubledata(2.0);
    std::vector<char> slot = Slot(64);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    EXPECT_EQ(RowLayout::ColumnCount(slot.data()), 2u);
    std::string out;
    layout.EncodeToWire(slot.data(), out);
    EXPECT_EQ(out, row.SerializeAsString());
    double v;
    EXPECT_TRUE(layout.ReadNumeric(slot.data(), 1, &v));
    EXPECT_FALSE(layout.ReadNumeric(slot.data(), 2, &v));   // typed, beyond n
}

TEST(RowLayoutRoundTrip, RandomRowsAcrossSchemas) {
    std::mt19937_64 rng(12345);
    for (int iteration = 0; iteration < 300; ++iteration) {
        const size_t typed = rng() % 6;
        std::vector<ColumnSpec> schema;
        for (size_t i = 0; i < typed; ++i) schema.push_back({static_cast<Kind>(1 + rng() % 13), Storage::kDefault, 0});   // numeric/bool kinds
        const RowLayout layout = MakeLayout(schema, 400);
        XStore_Proto::RowData row;
        const size_t columns = rng() % 12;
        for (size_t c = 0; c < columns; ++c) {
            XStore_Proto::FData *f = row.add_fielddata();
            const Kind kind = c < typed ? schema[c].kind : static_cast<Kind>(1 + rng() % 15);
            if (c < typed && rng() % 4 == 0) continue;   // a null typed cell
            switch (kind) {
                case Kind::kDouble: f->set_doubledata(static_cast<double>(rng() % 100000) / 7.0); break;
                case Kind::kFloat: f->set_floatdata(static_cast<float>(rng() % 1000) / 3.0f); break;
                case Kind::kInt32: f->set_int32data(static_cast<int32_t>(rng())); break;
                case Kind::kInt64: f->set_int64data(static_cast<int64_t>(rng())); break;
                case Kind::kUint32: f->set_uint32data(static_cast<uint32_t>(rng())); break;
                case Kind::kUint64: f->set_uint64data(rng()); break;
                case Kind::kSint32: f->set_sint32data(static_cast<int32_t>(rng())); break;
                case Kind::kSint64: f->set_sint64data(static_cast<int64_t>(rng())); break;
                case Kind::kFixed32: f->set_fixed32data(static_cast<uint32_t>(rng())); break;
                case Kind::kFixed64: f->set_fixed64data(rng()); break;
                case Kind::kSfixed32: f->set_sfixed32data(static_cast<int32_t>(rng())); break;
                case Kind::kSfixed64: f->set_sfixed64data(static_cast<int64_t>(rng())); break;
                case Kind::kBool: f->set_booldata(rng() % 2 == 0); break;
                case Kind::kString: f->set_stringdata(std::string(rng() % 6, 'a' + static_cast<char>(rng() % 26))); break;
                default: f->set_bytesdata(std::string(rng() % 6, static_cast<char>(rng()))); break;
            }
        }
        if (row.fielddata_size() == 0) continue;
        const std::string wire = row.SerializeAsString();
        std::vector<char> slot = Slot(400);
        const PackResult r = layout.PackFromWire(wire.data(), wire.size(), slot.data());
        ASSERT_TRUE(r.Ok()) << DescribePackResult(r);
        std::string out;
        layout.EncodeToWire(slot.data(), out);
        ASSERT_EQ(out, wire) << "iteration " << iteration;
        ASSERT_EQ(layout.EncodedSize(slot.data()), wire.size());
    }
}

// ---------------------------------------------------------------- rejections

TEST(RowLayoutPack, RejectsWrongKindForATypedColumn) {
    const RowLayout layout = MakeLayout({{Kind::kDouble, Storage::kDefault, 0}}, 32);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int32data(1);
    std::vector<char> slot = Slot(32);
    const PackResult r = layout.PackFromRowData(row, slot.data());
    EXPECT_EQ(r.status, PackStatus::kKindMismatch);
    EXPECT_EQ(r.column, 0u);
}

TEST(RowLayoutPack, RejectsNarrowIntOverflowAndLongInlineStrings) {
    const RowLayout layout = MakeLayout({{Kind::kInt32, Storage::kInt8, 0}, {Kind::kUint32, Storage::kUint16, 0}, {Kind::kString, Storage::kInlineString, 3}}, 64);
    std::vector<char> slot = Slot(64);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int32data(128);
    EXPECT_EQ(layout.PackFromRowData(row, slot.data()).status, PackStatus::kOutOfRange);
    row.Clear();
    row.add_fielddata()->set_int32data(-128);
    row.add_fielddata()->set_uint32data(65536);
    slot = Slot(64);
    EXPECT_EQ(layout.PackFromRowData(row, slot.data()).status, PackStatus::kOutOfRange);
    row.Clear();
    row.add_fielddata()->set_int32data(-128);
    row.add_fielddata()->set_uint32data(65535);
    row.add_fielddata()->set_stringdata("abcd");
    slot = Slot(64);
    EXPECT_EQ(layout.PackFromRowData(row, slot.data()).status, PackStatus::kStringTooLong);
    row.mutable_fielddata(2)->set_stringdata("abc");
    slot = Slot(64);
    EXPECT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
}

TEST(RowLayoutPack, RejectsRowsThatDoNotFitTheSlot) {
    const RowLayout layout = MakeLayout(NoSchema(), 30);   // 3 + 9*3 = 30: three dynamic columns fit, a fourth does not
    std::vector<char> slot = Slot(30);
    XStore_Proto::RowData row;
    for (int i = 0; i < 3; ++i) row.add_fielddata()->set_doubledata(i);
    EXPECT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    row.add_fielddata()->set_doubledata(3);
    slot = Slot(30);
    EXPECT_EQ(layout.PackFromRowData(row, slot.data()).status, PackStatus::kRowTooLarge);
    // a string whose bytes do not fit the tail
    const RowLayout small = MakeLayout(NoSchema(), 14);   // 3 + 9 = 12, tail room 2
    XStore_Proto::RowData text;
    text.add_fielddata()->set_stringdata("abc");
    slot = Slot(14);
    EXPECT_EQ(small.PackFromRowData(text, slot.data()).status, PackStatus::kRowTooLarge);
    text.mutable_fielddata(0)->set_stringdata("ab");
    slot = Slot(14);
    EXPECT_TRUE(small.PackFromRowData(text, slot.data()).Ok());   // exactly full
}

TEST(RowLayoutPack, RejectsMalformedWire) {
    const RowLayout layout = MakeLayout(NoSchema(), 128);
    auto pack = [&](const std::string &wire) {
        std::vector<char> slot = Slot(128);
        return layout.PackFromWire(wire.data(), wire.size(), slot.data()).status;
    };
    EXPECT_EQ(pack(std::string("\x12\x00", 2)), PackStatus::kMalformed);                // field 2, not fieldData
    EXPECT_EQ(pack(std::string("\x0A\x05\x09", 3)), PackStatus::kMalformed);            // length past the end
    EXPECT_EQ(pack(std::string("\x0A\x03\x09\x00\x00", 5)), PackStatus::kMalformed);    // truncated fixed64
    EXPECT_EQ(pack(std::string("\x0A\x02\x78\x01", 4)), PackStatus::kMalformed);        // field 15 with the wrong wire type
    EXPECT_EQ(pack(std::string("\x0A\x02\x80\x01", 4)), PackStatus::kMalformed);        // field 16 is not an FData member
    EXPECT_EQ(pack(std::string("\x0A\x04\x72\x02\xC3\x28", 6)), PackStatus::kMalformed);// string with invalid UTF-8
    EXPECT_EQ(pack(std::string("\x0A\x0C\x18\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x02", 14)), PackStatus::kMalformed);   // varint longer than 10 bytes
}

TEST(RowLayoutPack, LastOneofMemberWinsAndNonMinimalVarintsAreNormalized) {
    const RowLayout layout = MakeLayout(NoSchema(), 64);
    // One cell whose FData holds int32 = 1 and then double = 2.0: the last member on the wire wins, as in protobuf.
    std::string wire("\x0A\x0B\x18\x01\x09", 5);
    const double two = 2.0;
    wire.append(reinterpret_cast<const char *>(&two), 8);
    std::vector<char> slot = Slot(64);
    ASSERT_TRUE(layout.PackFromWire(wire.data(), wire.size(), slot.data()).Ok());
    XStore_Proto::RowData expect;
    ASSERT_TRUE(expect.ParseFromString(wire));
    std::string out;
    layout.EncodeToWire(slot.data(), out);
    EXPECT_EQ(out, expect.SerializeAsString());
    // A non-minimal tag is accepted and comes back minimal.
    const std::string loose("\x8A\x00\x02\x18\x05", 5);   // tag 0x0A written in two bytes, length 2, int32 = 5
    std::vector<char> slot2 = Slot(64);
    ASSERT_TRUE(layout.PackFromWire(loose.data(), loose.size(), slot2.data()).Ok());
    std::string out2;
    layout.EncodeToWire(slot2.data(), out2);
    EXPECT_EQ(out2, std::string("\x0A\x02\x18\x05", 4));
}

TEST(RowLayoutPack, TooManyColumnsAndTheEmptyRow) {
    const RowLayout layout = MakeLayout(NoSchema(), 64);
    XStore_Proto::RowData empty;
    std::vector<char> slot = Slot(64);
    EXPECT_TRUE(layout.PackFromRowData(empty, slot.data()).Ok());
    EXPECT_EQ(slot, Slot(64));   // an empty row stays a hole, as 0 serialized bytes always did
    const RowLayout wide = MakeLayout(NoSchema(), 700000);   // room for 65535 dynamic cells, so the column limit is what trips
    std::string wire;
    for (int i = 0; i < 65536; ++i) wire.append("\x0A\x00", 2);
    std::vector<char> big = Slot(700000);
    EXPECT_EQ(wide.PackFromWire(wire.data(), wire.size(), big.data()).status, PackStatus::kTooManyColumns);
}

TEST(RowLayoutPack, FuzzedWireNeverCrashes) {
    const RowLayout layout = MakeLayout({{Kind::kDouble, Storage::kDefault, 0}, {Kind::kString, Storage::kInlineString, 4}}, 96);
    std::mt19937_64 rng(777);
    for (int i = 0; i < 20000; ++i) {
        std::string wire(rng() % 40, 0);
        for (char &c : wire) c = static_cast<char>(rng() % 4 == 0 ? 0x0A : rng());
        std::vector<char> slot = Slot(96);
        const PackResult r = layout.PackFromWire(wire.data(), wire.size(), slot.data());
        if (r.Ok()) {
            std::string out;
            layout.EncodeToWire(slot.data(), out);   // whatever packs must also encode
            EXPECT_LE(out.size(), layout.MaxEncodedRowBytes());
        }
    }
}

// ---------------------------------------------------------------- reading

TEST(RowLayoutRead, NumericValuesMatchTheProtobufMeaning) {
    const RowLayout layout = MakeLayout(NoSchema(), 512);
    const XStore_Proto::RowData row = EveryKindRow();
    std::vector<char> slot = Slot(512);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    double v = 0;
    const double expect[12] = {-2.5, 1.5, -7, -9000000000.0, 4000000000.0, 18000000000000000000.0, -123456, -5000000000.0,
                               static_cast<double>(0xDEADBEEFu), static_cast<double>(0x0123456789ABCDEFull), -42, -4200000000.0};
    for (uint32_t c = 0; c < 12; ++c) {
        ASSERT_TRUE(layout.ReadNumeric(slot.data(), c, &v)) << c;
        EXPECT_EQ(v, expect[c]) << c;
    }
    EXPECT_FALSE(layout.ReadNumeric(slot.data(), 12, &v));   // bool
    EXPECT_FALSE(layout.ReadNumeric(slot.data(), 13, &v));   // string
    EXPECT_FALSE(layout.ReadNumeric(slot.data(), 14, &v));   // bytes
    EXPECT_FALSE(layout.ReadNumeric(slot.data(), 15, &v));   // unset
    EXPECT_FALSE(layout.ReadNumeric(slot.data(), 16, &v));   // past n
    EXPECT_TRUE(layout.Observe(slot.data(), 12).present);
    EXPECT_FALSE(layout.Observe(slot.data(), 12).is_numeric);
    EXPECT_FALSE(layout.Observe(slot.data(), 15).present);
}

TEST(RowLayoutRead, HolesAndCorruptRowsReadAsNotPresent) {
    const RowLayout layout = MakeLayout(NoSchema(), 64);
    std::vector<char> hole = Slot(64);
    double v;
    EXPECT_FALSE(layout.ReadNumeric(hole.data(), 0, &v));
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(1.0);
    row.add_fielddata()->set_stringdata("abc");
    std::vector<char> slot = Slot(64);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    Cell cell;
    EXPECT_TRUE(layout.Load(slot.data(), 1, &cell));
    EXPECT_EQ(cell.text, "abc");
    std::vector<char> bad_marker = slot;
    bad_marker[0] = 0x07;
    EXPECT_FALSE(layout.Load(bad_marker.data(), 0, &cell));
    std::vector<char> bad_ref = slot;
    const uint32_t outside = 200;   // past the slot
    std::memcpy(&bad_ref[3 + 9 + 1], &outside, 4);
    EXPECT_FALSE(layout.Load(bad_ref.data(), 1, &cell));
    const uint32_t before_tail = 5;   // inside the cells, not the tail
    std::memcpy(&bad_ref[3 + 9 + 1], &before_tail, 4);
    EXPECT_FALSE(layout.Load(bad_ref.data(), 1, &cell));
    std::vector<char> bad_kind = slot;
    bad_kind[3] = 0x63;   // type byte out of range
    EXPECT_FALSE(layout.Load(bad_kind.data(), 0, &cell));
    EXPECT_EQ(layout.EncodedSize(bad_marker.data()), 0u);
}

// A row is trusted only as far as its slot: a column count the slot cannot hold, or string references that make the encoding
// larger than any packable row of this layout, make the whole row "not present" (spec section 4: skip the row), whatever it
// holds. Every reply bound (MaxEncodedRowBytes) therefore holds for corrupt rows too.
TEST(RowLayoutRead, AnImpossibleColumnCountSkipsTheRow) {
    const RowLayout layout = MakeLayout(NoSchema(), 32);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(1.0);
    row.add_fielddata()->set_doubledata(2.0);
    std::vector<char> slot = Slot(32);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    ASSERT_TRUE(layout.LiveAndSane(slot.data()));
    EXPECT_GT(layout.EncodedSize(slot.data()), 0u);
    const uint16_t too_many = 65535;   // 3 + 9 * 65535 bytes would be needed
    std::memcpy(&slot[1], &too_many, sizeof too_many);
    EXPECT_FALSE(layout.LiveAndSane(slot.data()));
    EXPECT_EQ(layout.EncodedSize(slot.data()), 0u);
    std::string wire = "prefix";
    layout.EncodeToWire(slot.data(), wire);
    EXPECT_EQ(wire, "prefix");
    XStore_Proto::RowData decoded;
    decoded.add_fielddata();
    layout.DecodeToRowData(slot.data(), &decoded);
    EXPECT_EQ(decoded.fielddata_size(), 0);
    const uint16_t one_past = 4;   // 3 + 9 * 4 = 39 > 32
    std::memcpy(&slot[1], &one_past, sizeof one_past);
    EXPECT_FALSE(layout.LiveAndSane(slot.data()));
    const uint16_t last_that_fits = 3;   // 3 + 9 * 3 = 30 <= 32
    std::memcpy(&slot[1], &last_that_fits, sizeof last_that_fits);
    EXPECT_TRUE(layout.LiveAndSane(slot.data()));
    std::vector<char> hole = Slot(32), bad_marker = slot;
    bad_marker[0] = 0x07;
    EXPECT_FALSE(layout.LiveAndSane(hole.data()));
    EXPECT_FALSE(layout.LiveAndSane(bad_marker.data()));
}

TEST(RowLayoutRead, StringReferencesThatOverlapNeverEncodeBeyondTheBound) {
    for (const std::vector<ColumnSpec> &schema : {NoSchema(), std::vector<ColumnSpec>{{Kind::kString, Storage::kDefault, 0}, {Kind::kString, Storage::kDefault, 0}}}) {
        const RowLayout layout = MakeLayout(schema, 64);
        XStore_Proto::RowData row;
        row.add_fielddata()->set_stringdata("a");
        row.add_fielddata()->set_stringdata("b");
        std::vector<char> slot = Slot(64);
        ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
        ASSERT_TRUE(layout.LiveAndSane(slot.data()));
        // Both cells now reference the whole tail: each reference is inside the slot, together they encode twice the tail.
        const size_t tail = layout.DynamicStart() + (schema.empty() ? 2 * RowLayout::DYNAMIC_CELL_BYTES : 0);
        const uint32_t off = static_cast<uint32_t>(tail), len = static_cast<uint32_t>(64 - tail);
        const size_t first = schema.empty() ? layout.DynamicStart() + 1 : layout.Typed()[0].offset;
        const size_t second = schema.empty() ? layout.DynamicStart() + RowLayout::DYNAMIC_CELL_BYTES + 1 : layout.Typed()[1].offset;
        for (const size_t cell : {first, second}) {
            std::memcpy(&slot[cell], &off, 4);
            std::memcpy(&slot[cell + 4], &len, 4);
        }
        EXPECT_LE(layout.EncodedSize(slot.data()), layout.MaxEncodedRowBytes());
        std::string wire;
        layout.EncodeToWire(slot.data(), wire);
        EXPECT_LE(wire.size(), layout.MaxEncodedRowBytes());
        EXPECT_EQ(wire.size(), layout.EncodedSize(slot.data()));
        XStore_Proto::RowData decoded;
        layout.DecodeToRowData(slot.data(), &decoded);
        EXPECT_LE(decoded.ByteSizeLong(), layout.MaxEncodedRowBytes());
        EXPECT_EQ(decoded.ByteSizeLong(), wire.size());
    }
}

TEST(RowLayoutRead, ObservationsQuietANaN) {
    const RowLayout layout = MakeLayout(NoSchema(), 64);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(std::bit_cast<double>(0x7FF0000000000001ull));   // a signaling NaN
    std::vector<char> slot = Slot(64);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    const Observation obs = layout.Observe(slot.data(), 0);
    ASSERT_TRUE(obs.is_numeric);
    EXPECT_TRUE(std::isnan(obs.value));
    EXPECT_EQ(std::bit_cast<uint64_t>(obs.value), 0x7FF8000000000001ull);
}

namespace {
struct Collect {
    std::string bytes;
    void push_back(char c) { bytes.push_back(c); }
    void append(const char *p, size_t n) { bytes.append(p, n); }
};
}  // namespace

TEST(RowLayoutRead, CatIdxKeysDoNotDependOnStorageWidth) {
    // The key of a value is [kind byte][natural-width little-endian value, or the string bytes]; INT16 storage of an int32 value
    // must give the same key as a dynamic int32 cell.
    const RowLayout typed = MakeLayout({{Kind::kInt32, Storage::kInt16, 0}, {Kind::kString, Storage::kInlineString, 8}, {Kind::kBool, Storage::kDefault, 0}}, 64);
    const RowLayout dynamic = MakeLayout(NoSchema(), 64);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int32data(-5);
    row.add_fielddata()->set_stringdata("tick");
    row.add_fielddata()->set_booldata(true);
    std::vector<char> a = Slot(64), b = Slot(64);
    ASSERT_TRUE(typed.PackFromRowData(row, a.data()).Ok());
    ASSERT_TRUE(dynamic.PackFromRowData(row, b.data()).Ok());
    for (uint32_t c = 0; c < 3; ++c) {
        Cell ca, cb;
        ASSERT_TRUE(typed.Load(a.data(), c, &ca));
        ASSERT_TRUE(dynamic.Load(b.data(), c, &cb));
        Collect ka, kb;
        RowLayout::AppendCatIdxKey(ka, ca);
        RowLayout::AppendCatIdxKey(kb, cb);
        EXPECT_EQ(ka.bytes, kb.bytes) << c;
    }
    Cell cell;
    ASSERT_TRUE(typed.Load(a.data(), 0, &cell));
    Collect key;
    RowLayout::AppendCatIdxKey(key, cell);
    const int32_t v = -5;
    EXPECT_EQ(key.bytes, std::string(1, '\x03') + std::string(reinterpret_cast<const char *>(&v), 4));
}

TEST(RowLayoutRead, MaxEncodedRowBytesBoundsEveryPackableRow) {
    const RowLayout layout = MakeLayout({{Kind::kInt32, Storage::kDefault, 0}, {Kind::kString, Storage::kDefault, 0}}, 80);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int32data(-1);                         // 13 encoded bytes against a 4-byte cell
    row.add_fielddata()->set_stringdata(std::string(30, 'x'));
    for (int i = 0; i < 3; ++i) row.add_fielddata()->set_int64data(-1);   // dynamic
    std::vector<char> slot = Slot(80);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    EXPECT_LE(layout.EncodedSize(slot.data()), layout.MaxEncodedRowBytes());
    EXPECT_GT(layout.EncodedSize(slot.data()), 80u - 32u);   // the encoded row can be larger than what the dynamic cells took
}

// ---------------------------------------------------------------- the bound reader

TEST(ColumnReaderTest, AgreesWithTheGenericReadOnEveryKindLayoutAndOddSlot) {
    std::vector<ColumnSpec> typed_schema;
    for (unsigned k = 1; k <= 15; ++k) typed_schema.push_back({static_cast<Kind>(k), Storage::kDefault, 0});
    typed_schema.push_back({Kind::kInt32, Storage::kInt8, 0});
    typed_schema.push_back({Kind::kUint32, Storage::kUint16, 0});
    for (const std::vector<ColumnSpec> &schema : {NoSchema(), typed_schema}) {
        const RowLayout layout = MakeLayout(schema, 640);
        XStore_Proto::RowData row = EveryKindRow();
        row.mutable_fielddata(15)->set_int32data(-100);   // the unset cell of EveryKindRow becomes the narrow int32 column
        row.add_fielddata()->set_uint32data(60000);
        row.add_fielddata()->set_doubledata(8.5);   // a dynamic double after the typed prefix (or a plain dynamic one)
        std::vector<char> slot = Slot(640);
        ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
        std::vector<char> hole = Slot(640), corrupt = slot;
        corrupt[0] = 0x05;
        for (uint32_t column = 0; column < 22; ++column) {   // past n as well
            const ColumnReader reader(layout, column);
            for (const std::vector<char> *s : {&slot, &hole, &corrupt}) {
                double a = -1, b = -2;
                const bool fast = reader.ReadNumeric(s->data(), &a);
                const bool generic = layout.ReadNumeric(s->data(), column, &b);
                ASSERT_EQ(fast, generic) << "column " << column;
                if (fast) {
                    ASSERT_EQ(a, b) << "column " << column;
                }
            }
        }
    }
}

// Randomly corrupted slots (what the old RowDecodeTest did for protobuf rows): the bound reader answers exactly what the
// generic reader answers, and no read leaves the slot. The slot is a vector of exactly row_length bytes so an ASan build
// also catches any read past it; without ASan the string views are checked to lie inside the slot.
TEST(ColumnReaderTest, NeverDisagreesWithTheGenericReadOnRandomlyCorruptedSlots) {
    const std::vector<ColumnSpec> typed_schema = {{Kind::kDouble, Storage::kDefault, 0}, {Kind::kInt32, Storage::kInt16, 0},
                                                  {Kind::kBool, Storage::kDefault, 0}, {Kind::kString, Storage::kInlineString, 8},
                                                  {Kind::kString, Storage::kDefault, 0}, {Kind::kInt64, Storage::kDefault, 0},
                                                  {Kind::kFloat, Storage::kDefault, 0}, {Kind::kUint32, Storage::kDefault, 0},
                                                  {Kind::kUint64, Storage::kDefault, 0}, {Kind::kInt32, Storage::kInt8, 0}};
    constexpr size_t ITERATIONS = 20000;   // per layout
    std::mt19937_64 gen(20261120);
    size_t agreed_true = 0, agreed_false = 0, loads_with_text = 0;
    for (const std::vector<ColumnSpec> &schema : {NoSchema(), typed_schema}) {
        const size_t row_length = schema.empty() ? 128 : 200;
        const RowLayout layout = MakeLayout(schema, row_length);
        const uint32_t typed = layout.TypedColumns();
        const uint32_t columns_to_read = typed + 24;   // well past n and past the last cell that fits the slot
        std::vector<ColumnReader> readers;
        for (uint32_t c = 0; c < columns_to_read; ++c) readers.emplace_back(layout, c);
        for (size_t iteration = 0; iteration < ITERATIONS; ++iteration) {
            XStore_Proto::RowData row;
            for (uint32_t c = 0; c < typed; ++c) {
                XStore_Proto::FData *f = row.add_fielddata();
                if (gen() % 6 == 0) continue;   // null typed cell
                switch (schema[c].kind) {
                    case Kind::kDouble: f->set_doubledata(static_cast<double>(gen() % 100000) / 8); break;
                    case Kind::kFloat: f->set_floatdata(static_cast<float>(gen() % 1000)); break;
                    case Kind::kInt32: f->set_int32data(static_cast<int32_t>(gen() % 100) - 50); break;
                    case Kind::kInt64: f->set_int64data(static_cast<int64_t>(gen())); break;
                    case Kind::kUint32: f->set_uint32data(static_cast<uint32_t>(gen())); break;
                    case Kind::kUint64: f->set_uint64data(gen()); break;
                    case Kind::kBool: f->set_booldata(gen() % 2 == 0); break;
                    default: f->set_stringdata(std::string(gen() % (schema[c].storage == Storage::kInlineString ? 8 : 10), 's')); break;
                }
            }
            const size_t dynamic = gen() % 8;
            for (size_t d = 0; d < dynamic; ++d) {
                XStore_Proto::FData *f = row.add_fielddata();
                switch (gen() % 5) {
                    case 0: f->set_doubledata(static_cast<double>(gen() % 1000) * 0.25); break;
                    case 1: f->set_int64data(static_cast<int64_t>(gen())); break;
                    case 2: f->set_stringdata(std::string(gen() % 12, 'd')); break;
                    case 3: f->set_booldata(gen() % 2 == 0); break;
                    default: break;   // unset
                }
            }
            std::vector<char> slot(row_length, 0);
            ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
            const int edits = static_cast<int>(gen() % 4);   // 0: also compare the intact slot
            for (int e = 0; e < edits; ++e) {
                switch (gen() % 7) {
                    case 0: slot[0] = static_cast<char>(gen() % 3); break;                                   // marker: 0, 1 or 2
                    case 1: {                                                                                 // n: small, around the slot, or random
                        const uint16_t n = gen() % 2 == 0 ? static_cast<uint16_t>(gen() % 40) : static_cast<uint16_t>(gen());
                        std::memcpy(&slot[1], &n, sizeof n);
                        break;
                    }
                    case 2: slot[layout.DynamicStart() + (gen() % ((row_length - layout.DynamicStart()) / 9)) * 9] = static_cast<char>(gen() % 20); break;   // a type byte
                    case 3: {   // a (u32 offset, u32 length) pair: in a typed reference cell or a dynamic cell, values around the slot size
                        const size_t cell = typed != 0 && gen() % 2 == 0 ? layout.Typed()[4].offset
                                                                          : layout.DynamicStart() + (gen() % ((row_length - layout.DynamicStart()) / 9)) * 9 + 1;
                        const uint32_t v[2] = {static_cast<uint32_t>(gen() % (row_length + 8)), gen() % 3 == 0 ? static_cast<uint32_t>(gen()) : static_cast<uint32_t>(gen() % (row_length + 8))};
                        std::memcpy(&slot[cell], v, sizeof v);
                        break;
                    }
                    case 4: slot[gen() % row_length] = static_cast<char>(gen()); break;                       // a random byte anywhere
                    case 5: slot[layout.DynamicStart() + 1 + (gen() % ((row_length - layout.DynamicStart()) / 9)) * 9] = static_cast<char>(gen()); break;   // a payload byte
                    default: if (typed != 0) slot[RowLayout::HEADER_BYTES + gen() % ((typed + 7) / 8)] = static_cast<char>(gen()); break;   // the null bitmap
                }
            }
            for (uint32_t c = 0; c < columns_to_read; ++c) {
                double fast = -1, generic = -2;
                const bool a = readers[c].ReadNumeric(slot.data(), &fast), b = layout.ReadNumeric(slot.data(), c, &generic);
                ASSERT_EQ(a, b) << "iteration " << iteration << " column " << c;
                if (a) {
                    ASSERT_TRUE(fast == generic || (std::isnan(fast) && std::isnan(generic))) << "iteration " << iteration << " column " << c;
                    long double exact = 0;
                    ASSERT_TRUE(layout.ReadNumericExact(slot.data(), c, &exact));
                    ++agreed_true;
                } else {
                    ++agreed_false;
                }
                Cell cell;
                if (layout.Load(slot.data(), c, &cell) && cell.text.data() != nullptr) {
                    ++loads_with_text;
                    ASSERT_GE(cell.text.data(), slot.data()) << "iteration " << iteration << " column " << c;
                    ASSERT_LE(cell.text.data() + cell.text.size(), slot.data() + row_length) << "iteration " << iteration << " column " << c;
                }
            }
            XStore_Proto::RowData decoded;   // decoding and encoding a corrupt slot must stay inside it too
            layout.DecodeToRowData(slot.data(), &decoded);
            std::string wire;
            layout.EncodeToWire(slot.data(), wire);
            ASSERT_EQ(wire.size(), layout.EncodedSize(slot.data()));
            ASSERT_LE(wire.size(), layout.MaxEncodedRowBytes()) << "iteration " << iteration;   // a corrupt row never outgrows the reply bound
            ASSERT_EQ(decoded.ByteSizeLong(), wire.size()) << "iteration " << iteration;
        }
    }
    EXPECT_GT(agreed_true, 100000u) << "the corruption was too heavy: most reads were of absent cells";
    EXPECT_GT(agreed_false, 100000u);
    EXPECT_GT(loads_with_text, 10000u);
}

TEST(RowLayoutRead, ExactReadKeepsBigIntegersThatADoubleCannotHold) {
    const RowLayout layout = MakeLayout(NoSchema(), 64);
    const int64_t big = (1LL << 60) + 1;   // not representable as a double
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int64data(big);
    row.add_fielddata()->set_uint64data(~0ull);
    row.add_fielddata()->set_stringdata("x");
    std::vector<char> slot = Slot(64);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    long double exact = 0;
    ASSERT_TRUE(layout.ReadNumericExact(slot.data(), 0, &exact));
    EXPECT_EQ(exact, static_cast<long double>(big));
    ASSERT_TRUE(layout.ReadNumericExact(slot.data(), 1, &exact));
    EXPECT_EQ(exact, static_cast<long double>(~0ull));
    EXPECT_FALSE(layout.ReadNumericExact(slot.data(), 2, &exact));
    double rounded = 0;
    ASSERT_TRUE(layout.ReadNumeric(slot.data(), 0, &rounded));
    EXPECT_EQ(rounded, static_cast<double>(static_cast<long double>(big)));   // the double path rounds exactly as it always did
}

TEST(RowLayoutMake, CanFitIsTheCheapLowerBound) {
    const RowLayout layout = MakeLayout({{Kind::kDouble, Storage::kDefault, 0}}, 40);   // minimum 3 + 1 + 8 = 12, then 9 per dynamic column
    EXPECT_TRUE(layout.CanFit(0));
    EXPECT_TRUE(layout.CanFit(1));
    EXPECT_TRUE(layout.CanFit(4));    // 12 + 27 = 39
    EXPECT_FALSE(layout.CanFit(5));   // 48 > 40
}

TEST(RowLayoutRead, ReadFDataGivesBackTheOriginalCellsAndFalseForNullOrAbsent) {
    const RowLayout layout = MakeLayout(NoSchema(), 512);
    const XStore_Proto::RowData row = EveryKindRow();
    std::vector<char> slot = Slot(512);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    XStore_Proto::FData f;
    for (int c = 0; c < 15; ++c) {
        ASSERT_TRUE(layout.ReadFData(slot.data(), static_cast<uint32_t>(c), &f)) << c;
        EXPECT_EQ(f.SerializeAsString(), row.fielddata(c).SerializeAsString()) << c;
    }
    EXPECT_FALSE(layout.ReadFData(slot.data(), 15, &f));   // the unset cell
    EXPECT_FALSE(layout.ReadFData(slot.data(), 16, &f));   // past n
}

// The reply entry for a row is byte-identical to what a UnaryQuery_REP carrying the same RowData serializes to.
TEST(RowLayoutReply, ReplyEntryEqualsAProtobufUnaryQueryReply) {
    const RowLayout layout = MakeLayout(NoSchema(), 512);
    const XStore_Proto::RowData row = EveryKindRow();
    std::vector<char> slot = Slot(512);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    std::string entry;
    XStoreWireEncode::EncodeQueryResponseEntry(entry, 1700000123ull, slot.data(), layout);
    XStore_Proto::RangeQuery_REP expected;   // field 1 = repeated UnaryQuery_REP queryResponse
    XStore_Proto::UnaryQuery_REP *one = expected.add_queryresponse();
    one->set_timestamp(1700000123ull);
    *one->mutable_data() = row;
    EXPECT_EQ(entry, expected.SerializeAsString());
    std::string hole_entry;
    std::vector<char> hole = Slot(512);
    XStoreWireEncode::EncodeQueryResponseEntry(hole_entry, 5, hole.data(), layout);
    XStore_Proto::RangeQuery_REP hole_expected;
    hole_expected.add_queryresponse()->set_timestamp(5);
    hole_expected.mutable_queryresponse(0)->mutable_data();   // an empty RowData is written as a zero-length field
    EXPECT_EQ(hole_entry, hole_expected.SerializeAsString());
}

// The reply is encoded in 2 KB stack chunks and framed from a scratch string: rows that cross chunk edges, huge strings, typed and
// dynamic doubles, nulls and every other kind must all come out byte-identical to what libprotobuf serializes.
TEST(RowLayoutReply, ChunkedEncodeMatchesProtobufAcrossShapes) {
    const std::vector<std::vector<ColumnSpec>> schemas = {
        {},
        std::vector<ColumnSpec>(200, ColumnSpec{Kind::kDouble, Storage::kDefault, 0}),
        {{Kind::kDouble, Storage::kDefault, 0}, {Kind::kString, Storage::kInlineString, 12}, {Kind::kInt64, Storage::kInt16, 0},
         {Kind::kString, Storage::kDefault, 0}, {Kind::kDouble, Storage::kDefault, 0}, {Kind::kBool, Storage::kDefault, 0}},
    };
    std::mt19937_64 rng(12345);
    size_t checked = 0;
    for (const std::vector<ColumnSpec> &schema : schemas) {
        const RowLayout layout = MakeLayout(schema, 12000);
        for (const size_t cells : {0u, 1u, 7u, 100u, 183u, 250u, 600u}) {
            for (const size_t big_string : {0u, 5u, 2047u, 2048u, 2049u, 5000u}) {
                XStore_Proto::RowData row;
                for (size_t i = 0; i < cells; ++i) {
                    XStore_Proto::FData *f = row.add_fielddata();
                    const bool typed = i < schema.size();
                    if (typed) {
                        switch (schema[i].kind) {
                            case Kind::kDouble: if (rng() % 5 != 0) f->set_doubledata(static_cast<double>(rng() % 1000) / 7.0); break;
                            case Kind::kString: if (rng() % 4 != 0) f->set_stringdata(std::string(schema[i].storage == Storage::kInlineString ? 12 : (i == 3 ? big_string : 3), 's')); break;
                            case Kind::kInt64: f->set_int64data(-static_cast<int64_t>(rng() % 30000)); break;
                            default: f->set_booldata(rng() & 1); break;
                        }
                        continue;
                    }
                    switch (rng() % 6) {
                        case 0: case 1: case 2: f->set_doubledata(static_cast<double>(rng()) / 3.0); break;
                        case 3: f->set_int64data(-static_cast<int64_t>(rng())); break;
                        case 4: f->set_sint32data(static_cast<int32_t>(rng())); break;
                        default: break;   // unset: 0A 00
                    }
                }
                if (schema.empty() && big_string > 0 && cells > 0) row.add_fielddata()->set_bytesdata(std::string(big_string, '\xAB'));
                std::vector<char> slot = Slot(12000);
                if (!layout.PackFromRowData(row, slot.data()).Ok()) continue;   // does not fit: not a shape under test
                ++checked;
                std::string wire = "prefix";   // EncodeToWire appends, whatever is already there stays
                layout.EncodeToWire(slot.data(), wire);
                ASSERT_EQ(wire, "prefix" + row.SerializeAsString()) << "cells " << cells << " string " << big_string;
                std::string entry;
                XStoreWireEncode::EncodeQueryResponseEntry(entry, 1700000000123ull + cells, slot.data(), layout);
                XStore_Proto::RangeQuery_REP expected;
                XStore_Proto::UnaryQuery_REP *one = expected.add_queryresponse();
                one->set_timestamp(1700000000123ull + cells);
                *one->mutable_data() = row;
                ASSERT_EQ(entry, expected.SerializeAsString()) << "cells " << cells << " string " << big_string;
            }
        }
    }
    EXPECT_GT(checked, 60u);
}

// A damaged slot whose string references overlap would encode far past the bound: it is skipped whole (an empty row) and what the
// caller's buffer already held is left alone, even though earlier cells had already been flushed into it.
TEST(RowLayoutReply, OverlappingStringReferencesAreSkippedWholeAndLeaveThePrefix) {
    const RowLayout layout = MakeLayout(NoSchema(), 2000);
    XStore_Proto::RowData row;
    for (int i = 0; i < 30; ++i) row.add_fielddata()->set_stringdata(std::string(10, 'x'));
    std::vector<char> slot = Slot(2000);
    ASSERT_TRUE(layout.PackFromRowData(row, slot.data()).Ok());
    const uint32_t tail = static_cast<uint32_t>(layout.DynamicStart() + 30 * RowLayout::DYNAMIC_CELL_BYTES), len = 1500;
    for (uint32_t c = 0; c < 30; ++c) {
        char *cell = slot.data() + layout.DynamicStart() + c * RowLayout::DYNAMIC_CELL_BYTES;
        std::memcpy(cell + 1, &tail, 4);
        std::memcpy(cell + 5, &len, 4);
    }
    std::string wire = "keep";
    layout.EncodeToWire(slot.data(), wire);
    EXPECT_EQ(wire, "keep");
    std::string entry;
    XStoreWireEncode::EncodeQueryResponseEntry(entry, 9, slot.data(), layout);
    XStore_Proto::RangeQuery_REP expected;
    expected.add_queryresponse()->set_timestamp(9);
    expected.mutable_queryresponse(0)->mutable_data();
    EXPECT_EQ(entry, expected.SerializeAsString());
}

// The bound is the real worst case, not a loose multiple: every slot byte after the typed cells is either a 9-byte dynamic cell
// (at most 13 encoded bytes) or a string tail byte (1:1).
TEST(RowLayoutRead, MaxEncodedRowBytesIsTight) {
    EXPECT_EQ(MakeLayout(NoSchema(), 32).MaxEncodedRowBytes(), 41u);   // pool 29: three 13-byte cells + 2 tail bytes
    const std::vector<ColumnSpec> doubles(128, ColumnSpec{Kind::kDouble, Storage::kDefault, 0});
    const RowLayout typed = MakeLayout(doubles, 1280);
    const size_t pool = 1280 - typed.DynamicStart();
    EXPECT_EQ(typed.MaxEncodedRowBytes(), 128u * 11u + (pool / 9) * 13 + pool % 9);
}

namespace {
// Typed columns at their widest encodings (narrow storage at its most negative value, full inline strings), then
// `numeric_cells` dynamic -1 int64/int32 cells (10-byte varints), and a string of `string_bytes` either in the typed reference
// column or as one more dynamic cell.
XStore_Proto::RowData WorstRow(const std::vector<ColumnSpec> &schema, size_t numeric_cells, size_t string_bytes, bool string_in_typed) {
    XStore_Proto::RowData row;
    for (const ColumnSpec &spec : schema) {
        XStore_Proto::FData *f = row.add_fielddata();
        if (spec.storage == Storage::kInlineString) f->set_stringdata(std::string(spec.capacity, 'y'));
        else if (spec.kind == Kind::kString) f->set_stringdata(string_in_typed ? std::string(string_bytes, 'r') : std::string());
        else if (spec.storage == Storage::kInt8) f->set_int64data(-128);
        else if (spec.storage == Storage::kInt16) f->set_int32data(-32768);
        else f->set_int32data(-1);
    }
    for (size_t i = 0; i < numeric_cells; ++i) {
        if (i % 2 == 0) row.add_fielddata()->set_int64data(-1);
        else row.add_fielddata()->set_int32data(-1);
    }
    if (!string_in_typed && string_bytes > 0) row.add_fielddata()->set_bytesdata(std::string(string_bytes, '\xFF'));
    return row;
}
}  // namespace

TEST(RowLayoutRead, MaxEncodedRowBytesBoundsWorstCaseRows) {
    const std::vector<std::vector<ColumnSpec>> schemas = {
        {},
        {{Kind::kString, Storage::kDefault, 0}, {Kind::kString, Storage::kInlineString, 20}, {Kind::kInt64, Storage::kInt8, 0},
         {Kind::kInt32, Storage::kInt16, 0}, {Kind::kInt32, Storage::kDefault, 0}},
    };
    size_t packed = 0;
    for (const std::vector<ColumnSpec> &schema : schemas) {
        for (const size_t row_length : {32u, 64u, 131u, 300u, 20000u}) {   // 20000: string lengths need 3-byte varints
            std::string error;
            const std::optional<RowLayout> made = RowLayout::Make(schema, row_length, &error);
            if (!made) continue;   // below the schema minimum
            const RowLayout &layout = *made;
            std::vector<char> slot(row_length);
            auto check = [&](size_t cells, size_t s, bool in_typed) {
                std::fill(slot.begin(), slot.end(), 0);
                if (!layout.PackFromRowData(WorstRow(schema, cells, s, in_typed), slot.data()).Ok()) return false;
                ++packed;
                EXPECT_LE(layout.EncodedSize(slot.data()), layout.MaxEncodedRowBytes())
                    << "row_length " << row_length << " cells " << cells << " string " << s << " typed " << in_typed;
                return true;
            };
            const size_t max_cells = row_length / RowLayout::DYNAMIC_CELL_BYTES;
            for (size_t cells = 0; cells <= max_cells; cells += (row_length > 1000 ? max_cells : 1)) {
                for (const bool in_typed : {false, true}) {
                    if (in_typed && schema.empty()) continue;
                    check(cells, 0, in_typed);   // the widest numeric row
                    // The longest string that still fits next to `cells` numeric cells (fitting is monotone in its length).
                    size_t lo = 0, hi = row_length;
                    while (lo < hi) {
                        const size_t mid = (lo + hi + 1) / 2;
                        std::vector<char> probe(row_length, 0);
                        if (layout.PackFromRowData(WorstRow(schema, cells, mid, in_typed), probe.data()).Ok()) lo = mid;
                        else hi = mid - 1;
                    }
                    if (lo > 0) check(cells, lo, in_typed);
                }
            }
        }
    }
    EXPECT_GT(packed, 100u);
}
