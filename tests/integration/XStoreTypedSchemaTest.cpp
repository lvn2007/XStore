#include "XStore/XStore_RowLayout.h"
#include "include/XStore/XStore.h"

#include <gtest/gtest.h>

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
