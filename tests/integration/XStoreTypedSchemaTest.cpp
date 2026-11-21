#include "XStore/XStore_RowLayout.h"
#include "include/XStore/XStore.h"

#include <gtest/gtest.h>

#include <cstring>

#include "XStoreTestSupport.h"

using namespace xstore_test;
using xstore::layout::ColumnSpec;
using xstore::layout::Kind;
using xstore::layout::Storage;

namespace {

constexpr unsigned long long FIRST = 1'700'000'000ULL;   // all rows land in one year
constexpr int ROWS = 120;

// Five declared columns; a sixth, dynamic one follows.
const std::vector<ColumnSpec> SCHEMA = {{Kind::kDouble, Storage::kDefault, 0},
                                        {Kind::kInt32, Storage::kInt16, 0},
                                        {Kind::kBool, Storage::kDefault, 0},
                                        {Kind::kString, Storage::kInlineString, 12},
                                        {Kind::kInt64, Storage::kDefault, 0}};

XStore_Proto::RowData SampleRow(int i) {
    std::vector<XStore_Proto::FData> f = {FDouble(i * 1.5), FInt32(-i), FBool(i % 2 == 0), FString("tick" + std::to_string(i % 7)),
                                          FInt64((1LL << 40) + i)};
    if (i % 5 == 0) f.push_back(FString("extra-" + std::to_string(i)));   // a dynamic column on some rows
    if (i % 11 == 0) f[1] = XStore_Proto::FData();                         // a null typed cell
    return MakeRow(f);
}

}  // namespace

TEST(TypedSchemaDifferential, EveryApiAnswersTheSameForATypedAndADynamicDb) {
    ScratchDir scratch("typed_diff");
    XStore store;
    ASSERT_EQ(store.CreateDB("TYPED", XStore_Proto::SECOND, 160, 0, {}, SCHEMA).createdbstatus(), "[INFO] [CREATE DB] [DB: TYPED] - Created DB");
    CreateTestDb(store, "DYN", XStore_Proto::SECOND, 160);

    for (const char *db : {"TYPED", "DYN"}) {
        for (int i = 0; i < 40; ++i) {   // unary
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(FIRST + static_cast<unsigned>(i), SampleRow(i), db, &rep);
            ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << db << " " << rep.insertstatus();
        }
        XStore_Proto::BatchInsert_REQ batch;   // batch
        batch.set_dbname(db);
        batch.set_durabilitymode(XStore_Proto::SYNC);
        for (int i = 40; i < 80; ++i) {
            XStore_Proto::BatchInsert_DATA *d = batch.add_batchinsert_data();
            d->set_timestamp(FIRST + static_cast<unsigned>(i));
            *d->mutable_data() = SampleRow(i);
        }
        XStore_Proto::BatchInsert_REP batch_rep;
        store.BatchInsert(&batch, &batch_rep);
        ASSERT_NE(batch_rep.insertstatus().find("[INFO]"), std::string::npos) << db << " " << batch_rep.insertstatus();
        XStore_Proto::RangeInsert_REQ range;   // range
        range.set_dbname(db);
        range.set_durabilitymode(XStore_Proto::SYNC);
        for (int i = 80; i < ROWS; ++i) {
            XStore_Proto::RangeInsert_DATA *d = range.add_rangeinsert_data();
            d->set_timestamp(FIRST + static_cast<unsigned>(i));
            *d->mutable_data() = SampleRow(i);
        }
        XStore_Proto::RangeInsert_REP range_rep;
        store.RangeInsert(&range, &range_rep);
        ASSERT_NE(range_rep.insertstatus().find("[INFO]"), std::string::npos) << db << " " << range_rep.insertstatus();
    }

    for (bool streamed : {false, true}) {   // range query, both reply modes
        const XStore_Proto::RangeQuery_REP dyn = RangeQuery(store, FIRST, FIRST + ROWS, "DYN", streamed);
        EXPECT_EQ(RangeQuery(store, FIRST, FIRST + ROWS, "TYPED", streamed).SerializeAsString(), dyn.SerializeAsString());
        int with_data = 0;   // not vacuous: every inserted row comes back, exactly as inserted
        for (const XStore_Proto::UnaryQuery_REP &r : dyn.queryresponse()) {
            if (r.data().fielddata_size() == 0) continue;
            ++with_data;
            const int i = static_cast<int>(r.timestamp() - FIRST);
            EXPECT_EQ(r.data().SerializeAsString(), SampleRow(i).SerializeAsString()) << i;
        }
        EXPECT_EQ(with_data, ROWS) << streamed;
    }
    XStore_Proto::BatchQuery_REQ query;   // batch query over scattered timestamps, plus ones that were never written
    for (int i = 0; i < ROWS + 5; i += 3) query.add_timestamp(FIRST + static_cast<unsigned>(i));
    XStore_Proto::BatchQuery_REQ query_dyn = query;
    query.set_dbname("TYPED");
    query_dyn.set_dbname("DYN");
    const XStore_Proto::BatchQuery_REP batch_dyn = BatchQuery(store, &query_dyn);
    EXPECT_EQ(BatchQuery(store, &query).SerializeAsString(), batch_dyn.SerializeAsString());
    int batch_rows = 0;
    for (const XStore_Proto::UnaryQuery_REP &r : batch_dyn.queryresponse()) batch_rows += r.data().fielddata_size() > 0 ? 1 : 0;
    EXPECT_EQ(batch_rows, 40);   // i = 0, 3, ..., 117 were written; 120 and 123 were not
    for (int i : {0, 11, 25, 55, 119}) {   // unary query: exactly what was inserted (11: the null typed cell; 0, 25, 55: the 6th column)
        XStore_Proto::UnaryQuery_REP a, b;
        store.UnaryQuery(FIRST + static_cast<unsigned>(i), "TYPED", &a);
        store.UnaryQuery(FIRST + static_cast<unsigned>(i), "DYN", &b);
        EXPECT_EQ(a.SerializeAsString(), b.SerializeAsString()) << i;
        EXPECT_GT(a.data().fielddata_size(), 0) << i;
        EXPECT_EQ(a.data().SerializeAsString(), SampleRow(i).SerializeAsString()) << i;
        EXPECT_EQ(b.data().SerializeAsString(), SampleRow(i).SerializeAsString()) << i;
    }
    for (unsigned short column : {0, 1, 4}) {   // aggregates over typed numeric columns
        XStore_Proto::Min_REP min_a, min_b;
        store.MinAggregate(FIRST, FIRST + ROWS, "TYPED", column, &min_a);
        store.MinAggregate(FIRST, FIRST + ROWS, "DYN", column, &min_b);
        EXPECT_EQ(min_a.opresult().SerializeAsString(), min_b.opresult().SerializeAsString()) << column;
        XStore_Proto::Max_REP max_a, max_b;
        store.MaxAggregate(FIRST, FIRST + ROWS, "TYPED", column, &max_a);
        store.MaxAggregate(FIRST, FIRST + ROWS, "DYN", column, &max_b);
        EXPECT_EQ(max_a.opresult().SerializeAsString(), max_b.opresult().SerializeAsString()) << column;
        XStore_Proto::Sum_REP sum_a, sum_b;
        store.SumAggregate(FIRST, FIRST + ROWS, "TYPED", column, &sum_a);
        store.SumAggregate(FIRST, FIRST + ROWS, "DYN", column, &sum_b);
        EXPECT_EQ(sum_a.opresult(), sum_b.opresult()) << column;   // Sum_REP.opResult is a double
        // Not vacuous: every aggregate found data.
        for (const std::string *status : {&min_a.opstatus(), &min_b.opstatus(), &max_a.opstatus(), &max_b.opstatus(),
                                          &sum_a.opstatus(), &sum_b.opstatus()}) {
            EXPECT_EQ(status->find("No valid data"), std::string::npos) << column << " " << *status;
        }
        EXPECT_TRUE(min_a.has_opresult() && min_a.opresult().oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) << column;
    }
    XStore_Proto::Sum_REP sum0;
    store.SumAggregate(FIRST, FIRST + ROWS, "TYPED", 0, &sum0);
    EXPECT_DOUBLE_EQ(sum0.opresult(), 1.5 * (ROWS - 1) * ROWS / 2);   // sum of i * 1.5 over every row
    XStore_Proto::Max_REP max4;
    store.MaxAggregate(FIRST, FIRST + ROWS, "TYPED", 4, &max4);
    EXPECT_EQ(max4.opresult().int64data(), (1LL << 40) + ROWS - 1);
}

namespace {
// Concatenated reply bytes of a find call: byte-equal for a typed and a dynamic DB when they answer the same.
template <class Call>
std::string Collect(Call &&call) {
    std::string out;
    call([&](std::string &&chunk, bool /*is_last*/) { out += chunk; });
    return out;
}
}  // namespace

// The rest of the read and delete surface (exact aggregates, bars, find, deletes) on a typed schema and on a dynamic one: every answer
// must be the same bytes, and none of them may be vacuous.
TEST(TypedSchemaDifferential, ExactAggregatesFindAndDeletesAnswerTheSameForATypedAndADynamicDb) {
    ScratchDir scratch("typed_diff2");
    XStore store;
    ASSERT_EQ(store.CreateDB("TYPED", XStore_Proto::SECOND, 160, 0, {}, SCHEMA).createdbstatus(), "[INFO] [CREATE DB] [DB: TYPED] - Created DB");
    CreateTestDb(store, "DYN", XStore_Proto::SECOND, 160);
    for (const char *db : {"TYPED", "DYN"}) {
        XStore_Proto::BatchInsert_REQ batch;
        batch.set_dbname(db);
        batch.set_durabilitymode(XStore_Proto::SYNC);
        for (int i = 0; i < ROWS; ++i) {
            XStore_Proto::BatchInsert_DATA *d = batch.add_batchinsert_data();
            d->set_timestamp(FIRST + static_cast<unsigned>(i));
            *d->mutable_data() = SampleRow(i);
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&batch, &rep);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << db << " " << rep.insertstatus();
    }
    const unsigned long long lo = FIRST, hi = FIRST + ROWS;

    for (unsigned short column : {0, 1, 4}) {
        XStore_Proto::Avg_REP avg_t, avg_d;
        store.AvgAggregate(lo, hi, "TYPED", column, &avg_t);
        store.AvgAggregate(lo, hi, "DYN", column, &avg_d);
        EXPECT_EQ(avg_t.SerializeAsString(), avg_d.SerializeAsString()) << column;
        EXPECT_EQ(avg_d.opstatus().find("No valid data"), std::string::npos) << column << " " << avg_d.opstatus();
        XStore_Proto::Variance_REP var_t, var_d;
        store.VarianceAggregate(lo, hi, "TYPED", column, &var_t);
        store.VarianceAggregate(lo, hi, "DYN", column, &var_d);
        EXPECT_EQ(var_t.SerializeAsString(), var_d.SerializeAsString()) << column;
        XStore_Proto::Ohlc_REP ohlc_t, ohlc_d;
        store.OhlcAggregate(lo, hi, "TYPED", column, &ohlc_t);
        store.OhlcAggregate(lo, hi, "DYN", column, &ohlc_d);
        EXPECT_EQ(ohlc_t.SerializeAsString(), ohlc_d.SerializeAsString()) << column;
        EXPECT_EQ(ohlc_d.opstatus(), "SUCCESS") << column;
        XStore_Proto::Percentile_REP pct_t, pct_d;
        store.PercentileAggregate(lo, hi, "TYPED", column, 0.5, &pct_t);
        store.PercentileAggregate(lo, hi, "DYN", column, 0.5, &pct_d);
        EXPECT_EQ(pct_t.SerializeAsString(), pct_d.SerializeAsString()) << column;
        XStore_Proto::DistinctCount_REP dc_t, dc_d;
        store.DistinctCountAggregate(lo, hi, "TYPED", column, &dc_t);
        store.DistinctCountAggregate(lo, hi, "DYN", column, &dc_d);
        EXPECT_EQ(dc_t.SerializeAsString(), dc_d.SerializeAsString()) << column;
        EXPECT_GT(dc_d.count(), 1u) << column;
        XStore_Proto::TopK_REP topk_t, topk_d;
        store.TopKAggregate(lo, hi, "TYPED", column, 3, &topk_t);
        store.TopKAggregate(lo, hi, "DYN", column, 3, &topk_d);
        EXPECT_EQ(topk_t.SerializeAsString(), topk_d.SerializeAsString()) << column;
        EXPECT_EQ(topk_d.entries_size(), 3) << column;
    }
    for (unsigned short column : {0, 1, 3, 5, 9}) {   // typed double, INT16 with nulls, inline string, the dynamic 6th (a string), a column no row has
        XStore_Proto::Exists_REP ex_t, ex_d;
        store.ExistsAggregate(lo, hi, "TYPED", column, &ex_t);
        store.ExistsAggregate(lo, hi, "DYN", column, &ex_d);
        EXPECT_EQ(ex_t.SerializeAsString(), ex_d.SerializeAsString()) << column;
        EXPECT_EQ(ex_d.exists(), column == 0 || column == 1) << column;   // Exists is about numeric values: the string columns have none
    }

    const auto find_value = [&](const char *db, int column, const XStore_Proto::FData &target) {
        return Collect([&](const auto &emit) { store.FindByValue(lo, hi, db, column, target, 1 << 20, emit); });
    };
    for (const auto &[column, target] : std::vector<std::pair<int, XStore_Proto::FData>>{
             {0, FDouble(3 * 1.5)}, {3, FString("tick3")}, {4, FInt64((1LL << 40) + 7)}, {2, FBool(true)}}) {
        const std::string typed = find_value("TYPED", column, target), dyn = find_value("DYN", column, target);
        EXPECT_EQ(typed, dyn) << column;
        EXPECT_FALSE(dyn.empty()) << column;
    }
    for (const auto &[pattern, mode] : std::vector<std::pair<std::string, XStore_Proto::PatternMode>>{
             {"tick%", XStore_Proto::PATTERN_LIKE}, {"%ck3", XStore_Proto::PATTERN_LIKE}, {"^tick[24]$", XStore_Proto::PATTERN_REGEX}}) {
        const auto find_pattern = [&](const char *db) {
            return Collect([&](const auto &emit) { store.FindByPattern(lo, hi, db, 3, pattern, mode, false, 1 << 20, emit); });
        };
        const std::string typed = find_pattern("TYPED"), dyn = find_pattern("DYN");
        EXPECT_EQ(typed, dyn) << pattern;
        EXPECT_FALSE(dyn.empty()) << pattern;
    }

    // Deletes (a range, then scattered timestamps), then the same reads again: the holes read the same on both.
    const auto same_status = [](std::string status, const char *db) {   // the status line names the DB: compare it with the name masked
        const size_t at = status.find(db);
        if (at != std::string::npos) status.replace(at, std::strlen(db), "<db>");
        return status;
    };
    const XStore_Proto::RangeDelete_REP range_del_t = store.RangeDelete(FIRST + 10, FIRST + 19, "TYPED");
    const XStore_Proto::RangeDelete_REP range_del_d = store.RangeDelete(FIRST + 10, FIRST + 19, "DYN");
    EXPECT_EQ(same_status(range_del_t.deletestatus(), "TYPED"), same_status(range_del_d.deletestatus(), "DYN"));
    EXPECT_NE(range_del_d.deletestatus().find("Deleted 10 items"), std::string::npos) << range_del_d.deletestatus();
    XStore_Proto::BatchDelete_REQ del_t, del_d;
    for (int i : {2, 40, 41, 77, 119, 500}) {   // 500 was never written
        del_t.add_timestamp(FIRST + static_cast<unsigned>(i));
        del_d.add_timestamp(FIRST + static_cast<unsigned>(i));
    }
    del_t.set_dbname("TYPED");
    del_d.set_dbname("DYN");
    const XStore_Proto::BatchDelete_REP batch_del_t = store.BatchDelete(&del_t);
    const XStore_Proto::BatchDelete_REP batch_del_d = store.BatchDelete(&del_d);
    EXPECT_EQ(same_status(batch_del_t.deletestatus(), "TYPED"), same_status(batch_del_d.deletestatus(), "DYN"));
    EXPECT_NE(batch_del_d.deletestatus().find("Deleted 6 items"), std::string::npos) << batch_del_d.deletestatus();
    for (bool streamed : {false, true}) {
        const XStore_Proto::RangeQuery_REP dyn = RangeQuery(store, lo, hi, "DYN", streamed);
        EXPECT_EQ(RangeQuery(store, lo, hi, "TYPED", streamed).SerializeAsString(), dyn.SerializeAsString());
        int with_data = 0;
        for (const XStore_Proto::UnaryQuery_REP &r : dyn.queryresponse()) with_data += r.data().fielddata_size() > 0 ? 1 : 0;
        EXPECT_EQ(with_data, ROWS - 10 - 5) << streamed;   // 10..19 and 2, 40, 41, 77, 119 are gone
    }
    XStore_Proto::Sum_REP sum_t, sum_d;
    store.SumAggregate(lo, hi, "TYPED", 0, &sum_t);
    store.SumAggregate(lo, hi, "DYN", 0, &sum_d);
    EXPECT_EQ(sum_t.SerializeAsString(), sum_d.SerializeAsString());
}

TEST(TypedSchemaDifferential, ATypedColumnRejectsTheWrongKindAndTheReplySaysWhy) {
    ScratchDir scratch("typed_reject");
    XStore store;
    ASSERT_EQ(store.CreateDB("TYPED", XStore_Proto::SECOND, 160, 0, {}, SCHEMA).createdbstatus(), "[INFO] [CREATE DB] [DB: TYPED] - Created DB");
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(FIRST, MakeRow({FInt32(1)}), "TYPED", &rep);   // column 0 is declared double
    EXPECT_NE(rep.insertstatus().find("[WARNING]"), std::string::npos) << rep.insertstatus();
    EXPECT_NE(rep.insertstatus().find("column 0"), std::string::npos) << rep.insertstatus();
    store.UnaryInsert(FIRST, MakeRow({FDouble(1.0), FInt32(40000)}), "TYPED", &rep);   // column 1 is INT16
    EXPECT_NE(rep.insertstatus().find("column 1"), std::string::npos) << rep.insertstatus();
}
