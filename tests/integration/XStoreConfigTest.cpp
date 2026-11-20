#include "XStore/XStore_Config.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>

namespace {

namespace fs = std::filesystem;

class ConfigFileTest : public ::testing::Test {
protected:
    void SetUp() override {
        char pattern[] = "/tmp/xstore_cfg_test_XXXXXX";
        ASSERT_NE(mkdtemp(pattern), nullptr);
        dir_ = pattern;
    }
    void TearDown() override { fs::remove_all(dir_); }
    [[nodiscard]] std::string Path(const std::string &name = "DB.CFG") const { return dir_ + "/" + name; }
    static std::string Slurp(const std::string &path) {
        std::ifstream f(path, std::ios::binary);
        return std::string((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    }
    static void Spit(const std::string &path, const std::string &bytes) {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        f << bytes;
    }
    static XStore_Proto::ConfigStruct Sample() {
        XStore_Proto::ConfigStruct cfg;
        cfg.set_dbname("PRICES");
        cfg.set_timeresolution(XStore_Proto::SECOND);
        cfg.set_rowlength(1408);
        cfg.set_columncount(128);
        cfg.add_columnnames("open");
        cfg.add_columnnames("high");
        XStore_Proto::ColumnSpec *a = cfg.add_schema();
        a->set_kind(1);
        XStore_Proto::ColumnSpec *b = cfg.add_schema();
        b->set_kind(3);
        b->set_storage(2);
        XStore_Proto::ColumnSpec *c = cfg.add_schema();
        c->set_kind(14);
        c->set_storage(5);
        c->set_capacity(16);
        return cfg;
    }
    std::string dir_;
};

}  // namespace

using xstore::config::LoadStatus;

TEST_F(ConfigFileTest, SerializeParseRoundTripKeepsEveryField) {
    const XStore_Proto::ConfigStruct cfg = Sample();
    XStore_Proto::ConfigStruct back;
    ASSERT_EQ(xstore::config::Parse(xstore::config::Serialize(cfg), &back), LoadStatus::kOk);
    EXPECT_EQ(back.SerializeAsString(), cfg.SerializeAsString());
    EXPECT_EQ(back.schema_size(), 3);
    EXPECT_EQ(back.schema(2).capacity(), 16u);
}

TEST_F(ConfigFileTest, SaveThenLoadAndNoTemporaryFileLeftBehind) {
    std::string error;
    ASSERT_TRUE(xstore::config::Save(Path(), Sample(), &error)) << error;
    EXPECT_FALSE(fs::exists(Path() + ".tmp"));
    XStore_Proto::ConfigStruct back;
    ASSERT_EQ(xstore::config::Load(Path(), &back), LoadStatus::kOk);
    EXPECT_EQ(back.dbname(), "PRICES");
    EXPECT_EQ(back.rowlength(), 1408);
    EXPECT_EQ(back.columncount(), 128u);
    ASSERT_EQ(back.columnnames_size(), 2);
    EXPECT_EQ(back.columnnames(1), "high");
}

TEST_F(ConfigFileTest, SaveReplacesAnExistingFileAtomically) {
    std::string error;
    ASSERT_TRUE(xstore::config::Save(Path(), Sample(), &error)) << error;
    XStore_Proto::ConfigStruct next = Sample();
    next.set_columncount(140);
    ASSERT_TRUE(xstore::config::Save(Path(), next, &error)) << error;
    XStore_Proto::ConfigStruct back;
    ASSERT_EQ(xstore::config::Load(Path(), &back), LoadStatus::kOk);
    EXPECT_EQ(back.columncount(), 140u);
}

TEST_F(ConfigFileTest, ALeftoverTemporaryFileNeverAffectsLoad) {
    std::string error;
    ASSERT_TRUE(xstore::config::Save(Path(), Sample(), &error)) << error;
    Spit(Path() + ".tmp", "half a write");   // a crash between the write and the rename
    XStore_Proto::ConfigStruct back;
    EXPECT_EQ(xstore::config::Load(Path(), &back), LoadStatus::kOk);
}

TEST_F(ConfigFileTest, SaveFailsCleanlyWhenTheDirectoryIsMissing) {
    std::string error;
    EXPECT_FALSE(xstore::config::Save(dir_ + "/missing/DB.CFG", Sample(), &error));
    EXPECT_FALSE(error.empty());
}

TEST_F(ConfigFileTest, MissingFile) {
    XStore_Proto::ConfigStruct back;
    EXPECT_EQ(xstore::config::Load(Path("NOPE.CFG"), &back), LoadStatus::kMissing);
}

TEST_F(ConfigFileTest, AnOldProtobufConfigIsRefusedAsOldFormat) {
    Spit(Path(), Sample().SerializeAsString());   // what an older server wrote
    XStore_Proto::ConfigStruct back;
    EXPECT_EQ(xstore::config::Load(Path(), &back), LoadStatus::kOldFormat);
    Spit(Path(), "");
    EXPECT_EQ(xstore::config::Load(Path(), &back), LoadStatus::kOldFormat);
    Spit(Path(), std::string("\x08\x01", 2));   // a tiny protobuf .CFG is not a prefix of the magic
    EXPECT_EQ(xstore::config::Load(Path(), &back), LoadStatus::kOldFormat);
    Spit(Path(), "XS");   // the start of the magic and then nothing: an XSCF file cut short, not an old one
    EXPECT_EQ(xstore::config::Load(Path(), &back), LoadStatus::kCorrupt);
}

TEST_F(ConfigFileTest, CorruptionIsDetected) {
    const std::string good = xstore::config::Serialize(Sample());
    XStore_Proto::ConfigStruct back;
    for (size_t i = 10; i < good.size(); ++i) {   // every single flipped byte past the frame head: CRC or length catches it
        std::string bad = good;
        bad[i] = static_cast<char>(bad[i] ^ 0x40);
        EXPECT_EQ(xstore::config::Parse(bad, &back), LoadStatus::kCorrupt) << "byte " << i;
    }
    EXPECT_EQ(xstore::config::Parse(good.substr(0, good.size() - 1), &back), LoadStatus::kCorrupt);   // truncated
    EXPECT_EQ(xstore::config::Parse(good.substr(0, 8), &back), LoadStatus::kCorrupt);                  // magic then nothing
    EXPECT_EQ(xstore::config::Parse(good + "x", &back), LoadStatus::kCorrupt);                         // trailing byte
}

TEST_F(ConfigFileTest, ANewerFormatVersionIsRefusedNotMisread) {
    std::string bytes = xstore::config::Serialize(Sample());
    bytes[4] = 2;   // version 2
    XStore_Proto::ConfigStruct back;
    EXPECT_EQ(xstore::config::Parse(bytes, &back), LoadStatus::kUnsupportedVersion);
}
