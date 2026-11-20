#include "XStore/XStore_Config.h"
#include "XStore/XStore_RowLayout.h"
#include "include/XStore/XStore.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "XStoreTestSupport.h"

namespace fs = std::filesystem;
using namespace xstore_test;
using xstore::layout::ColumnSpec;
using xstore::layout::Kind;
using xstore::layout::Storage;

namespace {

std::string CfgPath(const std::string &db) { return "DataStore/" + db + "/" + db + ".CFG"; }

const std::vector<ColumnSpec> PRICE_SCHEMA = {{Kind::kDouble, Storage::kDefault, 0},
                                              {Kind::kInt32, Storage::kInt16, 0},
                                              {Kind::kString, Storage::kInlineString, 8}};

}  // namespace

TEST(SchemaCfgTest, CreateDbWithASchemaWritesABinaryCfgAndInfoReturnsTheSchema) {
    ScratchDir scratch("schema_cfg");
    XStore store;
    const XStore_Proto::CreateDB_REP rep = store.CreateDB("PRICES", XStore_Proto::SECOND, 256, 0, {}, PRICE_SCHEMA);
    ASSERT_EQ(rep.createdbstatus(), "[INFO] [CREATE DB] [DB: PRICES] - Created DB") << rep.createdbstatus();
    XStore_Proto::ConfigStruct on_disk;
    ASSERT_EQ(xstore::config::Load(CfgPath("PRICES"), &on_disk), xstore::config::LoadStatus::kOk);
    EXPECT_EQ(on_disk.rowlength(), 256);
    ASSERT_EQ(on_disk.schema_size(), 3);
    EXPECT_EQ(on_disk.schema(1).storage(), 2u);
    const XStore_Proto::SysInfo_REP info = store.InfoQuery("tcp://test", 1);
    ASSERT_EQ(info.dbinfo_size(), 1);
    ASSERT_EQ(info.dbinfo(0).schema_size(), 3);
    EXPECT_EQ(info.dbinfo(0).schema(2).capacity(), 8u);
}

TEST(SchemaCfgTest, CreateDbRejectsARowLengthBelowTheSchemaMinimumAndAnInvalidSchema) {
    ScratchDir scratch("schema_reject");
    XStore store;
    const std::vector<ColumnSpec> four_doubles(4, ColumnSpec{Kind::kDouble, Storage::kDefault, 0});   // minimum 3 + 1 + 32 = 36
    const XStore_Proto::CreateDB_REP too_small = store.CreateDB("SMALL", XStore_Proto::SECOND, 35, 0, {}, four_doubles);
    EXPECT_NE(too_small.createdbstatus().find("[WARNING]"), std::string::npos) << too_small.createdbstatus();
    EXPECT_NE(too_small.createdbstatus().find("36"), std::string::npos) << too_small.createdbstatus();
    EXPECT_FALSE(fs::exists("DataStore/SMALL"));   // rejected before any directory is created
    const std::vector<ColumnSpec> bad = {{Kind::kDouble, Storage::kInt16, 0}};
    const XStore_Proto::CreateDB_REP invalid = store.CreateDB("BAD", XStore_Proto::SECOND, 64, 0, {}, bad);
    EXPECT_NE(invalid.createdbstatus().find("Invalid schema"), std::string::npos) << invalid.createdbstatus();
    EXPECT_FALSE(fs::exists("DataStore/BAD"));
}

TEST(SchemaCfgTest, AnOldProtobufCfgIsNotOpened) {
    ScratchDir scratch("old_cfg");
    fs::create_directories("DataStore/OLDDB");
    XStore_Proto::ConfigStruct old_cfg;   // what an older server wrote
    old_cfg.set_dbname("OLDDB");
    old_cfg.set_timeresolution(XStore_Proto::SECOND);
    old_cfg.set_rowlength(64);
    {
        std::ofstream file(CfgPath("OLDDB"), std::ios::binary);
        file << old_cfg.SerializeAsString();
    }
    XStore store;
    store.InitDB();
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1000, MakeRow({FDouble(1.0)}), "OLDDB", &rep);
    EXPECT_NE(rep.insertstatus().find("DB does not exist"), std::string::npos) << rep.insertstatus();
}

TEST(SchemaCfgTest, TheColumnCountGrowthRewriteKeepsTheSchemaAndLeavesNoTemporaryFile) {
    ScratchDir scratch("schema_growth");
    XStore store;
    ASSERT_EQ(store.CreateDB("GROW", XStore_Proto::SECOND, 1536, 0, {}, PRICE_SCHEMA).createdbstatus(),
              "[INFO] [CREATE DB] [DB: GROW] - Created DB");
    std::vector<XStore_Proto::FData> fields = {FDouble(1.0), FInt32(2), FString("abc")};
    for (int i = 0; i < 77; ++i) fields.push_back(FDouble(static_cast<double>(i)));   // 80 columns: past the zone-map floor
    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1000, MakeRow(fields), "GROW", &insert_rep);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos) << insert_rep.insertstatus();
    XStore_Proto::ConfigStruct on_disk;
    ASSERT_EQ(xstore::config::Load(CfgPath("GROW"), &on_disk), xstore::config::LoadStatus::kOk);
    EXPECT_GE(on_disk.columncount(), 80u);
    EXPECT_EQ(on_disk.schema_size(), 3);
    EXPECT_FALSE(fs::exists(CfgPath("GROW") + ".tmp"));
}

TEST(SchemaCfgTest, ACfgWhoseLayoutCannotBeBuiltIsNeverCachedAndTheDbIsNotOpened) {
    ScratchDir scratch("schema_nolayout");
    fs::create_directories("DataStore/NOLAYOUT");
    XStore_Proto::ConfigStruct cfg;   // 4 typed doubles need 36 bytes; the row length is 20 (CreateDB would refuse this)
    cfg.set_dbname("NOLAYOUT");
    cfg.set_timeresolution(XStore_Proto::SECOND);
    cfg.set_rowlength(20);
    xstore::layout::RowLayout::SchemaToProto(std::vector<ColumnSpec>(4, ColumnSpec{Kind::kDouble, Storage::kDefault, 0}), cfg.mutable_schema());
    std::string error;
    ASSERT_TRUE(xstore::config::Save(CfgPath("NOLAYOUT"), cfg, &error)) << error;
    XStore store;
    store.InitDB();
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1000, MakeRow({FDouble(1.0)}), "NOLAYOUT", &rep);
    EXPECT_NE(rep.insertstatus().find("DB does not exist"), std::string::npos) << rep.insertstatus();
    auto &shard = XStore::cfg_shards[std::hash<std::string>{}("NOLAYOUT") % XStore::CFG_SHARD_COUNT];
    std::shared_lock lock(shard.mutex);
    EXPECT_EQ(shard.map.count("NOLAYOUT"), 0u);
    EXPECT_EQ(shard.layouts.count("NOLAYOUT"), 0u);
}
