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

#include <thread>
#include "include/XStore/XStore_Tunables.h"

TEST(FilesystemKeyStoreTest, UnknownKeyIsNotAllowed) {
    ScratchDir scratch("fks_unknown_test");
    std::filesystem::create_directories(scratch.test_root / "allowed");
    FilesystemKeyStore store(scratch.test_root / "allowed", std::chrono::milliseconds(0));

    auto kp = XStoreCurveKeyPair::Generate();
    std::string name;
    EXPECT_FALSE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));
}

TEST(FilesystemKeyStoreTest, AllowedKeyIsFoundWithCorrectName) {
    ScratchDir scratch("fks_allowed_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);

    auto kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(allowed_dir / "alice.pub", kp.public_key_z85, /*is_secret=*/false);

    FilesystemKeyStore store(allowed_dir, std::chrono::milliseconds(0));
    std::string name;
    ASSERT_TRUE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));
    EXPECT_EQ(name, "alice");
}

TEST(FilesystemKeyStoreTest, RevokeThenRefreshDeniesSubsequentLookup) {
    ScratchDir scratch("fks_revoke_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);

    auto kp = XStoreCurveKeyPair::Generate();
    auto pub_path = allowed_dir / "bob.pub";
    Z85WriteFile(pub_path, kp.public_key_z85, /*is_secret=*/false);

    FilesystemKeyStore store(allowed_dir, std::chrono::milliseconds(0));
    std::string name;
    ASSERT_TRUE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));

    std::filesystem::remove(pub_path);
    // Directory mtime granularity on some filesystems is 1s -- sleep past it
    // so Refresh()'s mtime check reliably observes the change.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    store.Refresh();

    EXPECT_FALSE(store.IsAllowed(Z85Decode(kp.public_key_z85), &name));
}

TEST(FilesystemKeyStoreTest, MalformedFileIsSkippedWithoutBreakingOtherEntries) {
    ScratchDir scratch("fks_malformed_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);

    auto good_kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(allowed_dir / "good.pub", good_kp.public_key_z85, /*is_secret=*/false);
    { std::ofstream bad(allowed_dir / "bad.pub"); bad << "not-a-valid-z85-key"; }

    FilesystemKeyStore store(allowed_dir, std::chrono::milliseconds(0));
    std::string name;
    EXPECT_TRUE(store.IsAllowed(Z85Decode(good_kp.public_key_z85), &name));
    EXPECT_EQ(name, "good");
}
