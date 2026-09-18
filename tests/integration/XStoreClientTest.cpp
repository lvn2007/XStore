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
// XBench's XStore_Adapter::ComputeRowLength. Default Alignment=8,
// PROTOBUF_FIELD_OVERHEAD=3 per field (see XStore_Client.h for why).
TEST(XStoreClientSuggestRowLengthTest, ExactFitNeedsNoPadding) {
    // 2 bool fields: (sizeof(bool)=1 + 3) * 2 = 8 -- already 8-aligned.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Bool(true);
    *row.add_fielddata() = xstore_fdata::Bool(false);
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 8u);
}

TEST(XStoreClientSuggestRowLengthTest, PartialPaddingRoundsUpToAlignment) {
    // 1 bool field: sizeof(bool)=1 + 3 = 4 -- rounds up to 8.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Bool(true);
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 8u);
}

TEST(XStoreClientSuggestRowLengthTest, EmptyRowSuggestsZeroLength) {
    XStore_Proto::RowData row;
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 0u);
}

TEST(XStoreClientSuggestRowLengthTest, SingleFieldRowAlignsUp) {
    // 1 int32 field: sizeof(int32_t)=4 + 3 = 7 -- rounds up to 8.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Int32(42);
    EXPECT_EQ(XStoreClient::SuggestRowLength(row), 8u);
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
    // 3 int32 fields: (4 + 3) * 3 = 21 -- rounds up to 32 under Alignment=16.
    XStore_Proto::RowData row;
    *row.add_fielddata() = xstore_fdata::Int32(1);
    *row.add_fielddata() = xstore_fdata::Int32(2);
    *row.add_fielddata() = xstore_fdata::Int32(3);
    EXPECT_EQ((XStoreClient::SuggestRowLength<16>(row)), 32u);
}
