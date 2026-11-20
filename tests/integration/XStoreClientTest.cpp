// Exercises XStoreClient's warmupOnInit flag in isolation from the server.
// Deliberately does NOT link against the server-side protobuf sources (see
// tests/CMakeLists.txt) -- XStoreClient pulls its own generated XStore.pb.h
// copy via the XStoreClient CMake target, and mixing that with the server's
// separately-generated copy in one binary would be an ODR violation.
//
// warmupOnInit's only observable effect is *when* the ZMTP handshake fires:
// with it on, the constructor eagerly sends a PING and throws immediately if
// nothing answers; with it off, ZMQ's lazy connect() means construction
// never talks to the network at all, so it can't throw here regardless of
// whether anything is listening.
#include <gtest/gtest.h>

#include "XStore_Client.h"

namespace {
// Port nobody's listening on for the duration of this test binary.
constexpr const char *K_UNREACHABLE_ADDR = "127.0.0.1";
constexpr const char *K_UNREACHABLE_PORT = "1";
constexpr int K_SHORT_TIMEOUT_MS = 200;
}  // namespace

TEST(XStoreClientWarmupTest, WarmupOnInitTrueThrowsWhenServerUnreachable) {
    EXPECT_THROW(
        XStoreClient(K_UNREACHABLE_ADDR, K_UNREACHABLE_PORT, K_SHORT_TIMEOUT_MS, /*warmupOnInit=*/true),
        std::runtime_error);
}

TEST(XStoreClientWarmupTest, WarmupOnInitFalseDoesNotThrowEvenWhenServerUnreachable) {
    EXPECT_NO_THROW(
        XStoreClient(K_UNREACHABLE_ADDR, K_UNREACHABLE_PORT, K_SHORT_TIMEOUT_MS, /*warmupOnInit=*/false));
}

TEST(XStoreClientWarmupTest, WarmupDefaultsToOnMatchingPreviousBehavior) {
    EXPECT_THROW(
        XStoreClient(K_UNREACHABLE_ADDR, K_UNREACHABLE_PORT, K_SHORT_TIMEOUT_MS),
        std::runtime_error);
}

// SuggestRowLength is a pure static function (no socket needed) -- mirrors
// the fixed-layout slot: 3 header bytes + 9 bytes per dynamic column (+ string bytes), rounded up to Alignment (default 8).
TEST(XStoreClientSuggestRowLengthTest, TwoDynamicColumnsRoundUp) {
    // 2 bool fields: 3 + 9 * 2 = 21 -- rounds up to 24.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Bool(true);
    *row.add_fielddata() = xstore_fdata::Bool(false);
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 24u);
}

TEST(XStoreClientSuggestRowLengthTest, PartialPaddingRoundsUpToAlignment) {
    // 1 bool field: 3 + 9 = 12 -- rounds up to 16.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Bool(true);
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 16u);
}

TEST(XStoreClientSuggestRowLengthTest, EmptyRowSuggestsTheHeaderRoundedUp) {
    XStore_Proto::RowData row;   // 3 header bytes, aligned
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 8u);
}

TEST(XStoreClientSuggestRowLengthTest, SingleFieldRowAlignsUp) {
    // 1 int32 field: 3 + 9 = 12 -- rounds up to 16.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Int32(42);
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 16u);
}

// ResolveColumnIndex: pure name->index lookup, no server/socket needed --
// same style as SuggestRowLength above. Callers fetch columnNames once
// (e.g. from InfoRequest()'s DbInfo.column_names, or right after CreateDB)
// and pass it in; this never touches the network itself.
TEST(XStoreClientResolveColumnIndexTest, FindsExactNameMatch) {
    std::vector<std::string> column_names = {"Open", "High", "Low", "Close"};
    EXPECT_EQ(XStoreClient::ResolveColumnIndex(column_names, "Low"), 2);
}

TEST(XStoreClientResolveColumnIndexTest, ThrowsOnUnknownName) {
    std::vector<std::string> column_names = {"Open", "High"};
    EXPECT_THROW(XStoreClient::ResolveColumnIndex(column_names, "Volume"), std::invalid_argument);
}

TEST(XStoreClientResolveColumnIndexTest, ThrowsOnEmptyColumnNamesList) {
    std::vector<std::string> column_names;
    EXPECT_THROW(XStoreClient::ResolveColumnIndex(column_names, "Anything"), std::invalid_argument);
}

TEST(XStoreClientSuggestRowLengthTest, CustomAlignmentIsRespected) {
    // 3 int32 fields: 3 + 9 * 3 = 30 -- rounds up to 32 under Alignment=16.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Int32(1);
    *row.add_fielddata() = xstore_fdata::Int32(2);
    *row.add_fielddata() = xstore_fdata::Int32(3);
    EXPECT_EQ((XStoreClient::SuggestRowLength<16>(row)), 32u);
}

TEST(XStoreClientSuggestRowLengthTest, FiveDynamicDoublesAndATypedSchema) {
    XStore_Proto::RowData row;
    for (int i = 0; i < 5; ++i) *row.add_fielddata() = xstore_fdata::Double(1.0);
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 48u);   // 3 + 9 * 5

    // double, int32 stored as INT16, string stored INLINE (capacity 8) + a dynamic string of 20 bytes:
    // 3 + 1 (bitmap) + 8 + 2 + 9 = 23 typed, + 9 dynamic cell + 20 tail bytes = 52 -- rounds up to 56.
    XStore_Proto::RowData typed_row;
    *typed_row.add_fielddata() = xstore_fdata::Double(1.0);
    *typed_row.add_fielddata() = xstore_fdata::Int32(2);
    *typed_row.add_fielddata() = xstore_fdata::String("abc");
    *typed_row.add_fielddata() = xstore_fdata::String(std::string(20, 'x'));
    std::vector<XStore_Proto::ColumnSpec> schema(3);
    schema[0].set_kind(1);
    schema[1].set_kind(3);
    schema[1].set_storage(2);
    schema[2].set_kind(14);
    schema[2].set_storage(5);
    schema[2].set_capacity(8);
    EXPECT_EQ(XStoreClient::SuggestRowLength(typed_row, schema), 56u);
}
