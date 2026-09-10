// Unit + end-to-end tests for the CURVE auth library (include/Networking/
// XStore_Curve.h). Lives in the xstore_network_tests binary since it already
// links libzmq + ZMQ_Server.cpp + the full XStore core -- this file adds
// XStore_Curve.cpp + XStore_SysStats.cpp to that target's sources (see
// tests/CMakeLists.txt) rather than duplicating the whole source list again
// in a fourth test binary.
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#include "include/Networking/XStore_Curve.h"
#include "XStoreTestSupport.h"

using namespace xstore_test;

TEST(XStoreCurveKeyPairTest, GeneratedKeyPairRoundTripsThroughZ85) {
    auto kp = XStoreCurveKeyPair::Generate();
    EXPECT_EQ(kp.public_key_z85.size(), 40u);
    EXPECT_EQ(kp.secret_key_z85.size(), 40u);
    EXPECT_NE(kp.public_key_z85, kp.secret_key_z85);

    auto raw = Z85Decode(kp.public_key_z85);
    EXPECT_EQ(Z85Encode(raw), kp.public_key_z85);
}

TEST(XStoreCurveKeyPairTest, SaveToFilesThenLoadFromSecretKeyFileRoundTrips) {
    ScratchDir scratch("xstore_curve_keypair_test");
    auto pub_path = scratch.test_root / "id.pub";
    auto sec_path = scratch.test_root / "id.key";

    auto original = XStoreCurveKeyPair::Generate();
    original.SaveToFiles(pub_path, sec_path);

    ASSERT_TRUE(std::filesystem::exists(pub_path));
    ASSERT_TRUE(std::filesystem::exists(sec_path));
    auto sec_perms = std::filesystem::status(sec_path).permissions();
    EXPECT_EQ(sec_perms & std::filesystem::perms::group_read, std::filesystem::perms::none);
    EXPECT_EQ(sec_perms & std::filesystem::perms::others_read, std::filesystem::perms::none);

    auto loaded = XStoreCurveKeyPair::LoadFromSecretKeyFile(sec_path);
    EXPECT_EQ(loaded.public_key_z85, original.public_key_z85);
    EXPECT_EQ(loaded.secret_key_z85, original.secret_key_z85);
}

TEST(XStoreCurveKeyPairTest, LoadFromSecretKeyFileThrowsWhenSiblingPubMissing) {
    ScratchDir scratch("xstore_curve_keypair_missing_pub_test");
    auto sec_path = scratch.test_root / "orphan.key";
    { std::ofstream f(sec_path); f << std::string(40, 'a'); }
    EXPECT_THROW(XStoreCurveKeyPair::LoadFromSecretKeyFile(sec_path), std::runtime_error);
}
