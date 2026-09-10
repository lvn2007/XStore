// Smoke tests for the xstore-keys binary: invokes it via std::system() in a
// scratch directory and checks the resulting files, mirroring the black-box
// nature of a CLI tool rather than unit-testing internals already covered by
// XStoreCurveKeyPairTest (tests/integration/XStoreCurveTest.cpp).
#include <gtest/gtest.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "include/Networking/XStore_Curve.h"
#include "XStoreTestSupport.h"

using namespace xstore_test;

namespace {
int RunKeysTool(const std::string &args) {
    std::string cmd = std::string(XSTORE_KEYS_BIN) + " " + args + " > /dev/null 2>&1";
    return std::system(cmd.c_str());
}
}  // namespace

TEST(XStoreKeysCliTest, GenWritesValidKeyPair) {
    ScratchDir scratch("xstore_keys_gen_test");
    std::string out_dir = (scratch.test_root / "out").string();
    ASSERT_EQ(RunKeysTool("gen alice --out-dir " + out_dir), 0);

    auto pub = Z85ReadFile(std::filesystem::path(out_dir) / "alice.pub");
    auto sec = Z85ReadFile(std::filesystem::path(out_dir) / "alice.key");
    EXPECT_EQ(pub.size(), 40u);
    EXPECT_EQ(sec.size(), 40u);
    auto raw = Z85Decode(pub);
    EXPECT_EQ(Z85Encode(raw), pub);
}

TEST(XStoreKeysCliTest, GenRefusesToOverwriteWithoutForce) {
    ScratchDir scratch("xstore_keys_gen_overwrite_test");
    std::string out_dir = (scratch.test_root / "out").string();
    ASSERT_EQ(RunKeysTool("gen bob --out-dir " + out_dir), 0);
    EXPECT_NE(RunKeysTool("gen bob --out-dir " + out_dir), 0);
    EXPECT_EQ(RunKeysTool("gen bob --out-dir " + out_dir + " --force"), 0);
}

TEST(XStoreKeysCliTest, AllowThenRevokeRoundTrips) {
    ScratchDir scratch("xstore_keys_allow_revoke_test");
    std::string out_dir = (scratch.test_root / "gen").string();
    std::string allowed_dir = (scratch.test_root / "allowed").string();
    ASSERT_EQ(RunKeysTool("gen carol --out-dir " + out_dir), 0);

    ASSERT_EQ(RunKeysTool("allow carol --pubkey-file " + out_dir + "/carol.pub --allowed-dir " + allowed_dir), 0);
    EXPECT_TRUE(std::filesystem::exists(std::filesystem::path(allowed_dir) / "carol.pub"));

    ASSERT_EQ(RunKeysTool("revoke carol --allowed-dir " + allowed_dir), 0);
    EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(allowed_dir) / "carol.pub"));

    // Revoking again is a no-op, not an error.
    EXPECT_EQ(RunKeysTool("revoke carol --allowed-dir " + allowed_dir), 0);
}

TEST(XStoreKeysCliTest, ListShowsAllowedIdentities) {
    ScratchDir scratch("xstore_keys_list_test");
    std::string out_dir = (scratch.test_root / "gen").string();
    std::string allowed_dir = (scratch.test_root / "allowed").string();
    ASSERT_EQ(RunKeysTool("gen dave --out-dir " + out_dir), 0);
    ASSERT_EQ(RunKeysTool("allow dave --pubkey-file " + out_dir + "/dave.pub --allowed-dir " + allowed_dir), 0);
    EXPECT_EQ(RunKeysTool("list --allowed-dir " + allowed_dir), 0);
}
