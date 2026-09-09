# ZMQ CURVE Auth + Key Housekeeping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Encrypt and authenticate XStore's ZMQ transport (RPC ROUTER socket + SysStats PUB socket) with CURVE, backed by a filesystem-based, hot-reloadable client allowlist and a small `xstore-keys` CLI for key housekeeping — fully opt-in, zero behavior change when not enabled.

**Architecture:** A small shared `XStore_Curve` library (keypair gen/load, Z85 helpers, a `KeyStore` interface + `FilesystemKeyStore` impl, and a reusable `XStoreZapHandler`) is linked into the server and the new `xstore-keys` CLI tool. `ZmqServer` and `SysStatsCollector` each gain an optional CURVE identity + shared `KeyStore`, wiring `ZMQ_CURVE_SERVER`/`ZMQ_ZAP_DOMAIN` on their external-facing socket and spinning up their own `XStoreZapHandler` (ZAP is per-`zmq::context_t`, and these two components deliberately run on separate contexts already). Both C++ and Python clients gain optional CURVE parameters that set the matching client-side sockopts before connecting. `client_src` stays self-contained — the client-side Z85/key-loading logic is duplicated inline rather than sharing a header with the server's core tree, matching this repo's existing "client_src keeps its own copy" convention.

**Tech Stack:** libzmq 4.3.5 (already vendored/fetched) CURVE mechanism, libsodium (already an unused CI apt dependency — libzmq's own build auto-detects and links it once `ENABLE_CURVE` is on; falls back to libzmq's bundled tweetnacl if absent), cppzmq's `zmq::sockopt::curve_*`/`zap_domain` wrappers (confirmed present in the vendored `third_party/zmq.hpp`), GoogleTest, `argparse` (already vendored, supports `add_subparser`).

**Spec:** [docs/superpowers/specs/2026-09-09-zmq-curve-auth-design.md](../specs/2026-09-09-zmq-curve-auth-design.md) (commit `dbce858f`)

## Global Constraints

- C++17 (`CMAKE_CXX_STANDARD 17`), must compile clean under `-Wall -Wextra -Wpedantic`-equivalent (this repo runs `-Wall -Wextra`, `-Werror` not currently set but treat warnings as bugs to fix, per CPP.md).
- Fully opt-in: omitting `--curve` (server) or the curve constructor params (clients) must leave 100% of today's behavior unchanged — no existing test, tool, or benchmark harness may need to change.
- No universal config file — CLI-flags-only, consistent with every existing XStore setting (deferred per spec §3).
- No new external crypto dependency beyond libsodium (already an unused CI apt dependency) / libzmq's own bundled tweetnacl fallback.
- On-disk key format is always 40-character Z85 text (never raw binary files) — this is both what `zmq_curve_keypair()` natively emits and what cppzmq's `curve_*` sockopts accept directly (`ZMQ_DEFINE_ARRAY_OPT_BIN_OR_Z85`), so no encode/decode round trip on the socket-setup hot path.
- Every identity's secret key file is written with mode `0600`.
- Follow existing code style: `spdlog` for logging (with the same `[TAG]` bracket-prefix convention seen in `ZMQ_Server.cpp`/`XStore_SysStats.cpp`, e.g. `[CURVE]`), `std::filesystem`, comments only where they explain a non-obvious *why* (per CLAUDE.md/CPP.md — no restating what code does).

---

## File Structure

| File | Responsibility |
|---|---|
| `include/Networking/XStore_Curve.h` (new) | Public interface: Z85 helpers, `XStoreCurveKeyPair`, `KeyStore`, `FilesystemKeyStore`, `XStoreZapHandler`. |
| `src/XStore_Networking/XStore_Curve.cpp` (new) | Implementation of everything in the header above. |
| `include/XStore/XStore_Tunables.h` (modify) | Add `CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS`. |
| `CMakeLists.txt` (modify) | Flip `ENABLE_CURVE` on, add the two new files to the `XStore` target, add `tools/xstore-keys` subdirectory. |
| `tools/xstore-keys/CMakeLists.txt` (new), `tools/xstore-keys/main.cpp` (new) | The `xstore-keys` CLI (`gen`/`allow`/`revoke`/`list`). |
| `include/Networking/ZMQ_Server.h`, `src/XStore_Networking/ZMQ_Server.cpp` (modify) | `StartServer` gains an optional CURVE identity + `KeyStore`. |
| `include/XStore/XStore_SysStats.h`, `src/XStore/XStore_SysStats.cpp` (modify) | `Start` gains the same optional params for the PUB socket. |
| `src/main.cpp` (modify) | New `--curve`/`--curve-secret-key`/`--curve-allowed-dir` flags, wiring into both components above. |
| `client_src/Cpp/XStore_Client.h` (modify) | `XStoreCurveClientAuth` struct + optional ctor/`Reconnect` param. |
| `client_src/Python/XStore.py` (modify) | Optional curve kwargs. |
| `tests/integration/XStoreCurveTest.cpp` (new) | Unit + end-to-end tests for everything server-side. |
| `tests/integration/XStoreKeysCliTest.cpp` (new) | `xstore-keys` binary smoke tests. |
| `tests/integration/XStoreClientCurveLiveTest.cpp` (new) | Opt-in live test for the C++ client's curve constructor (same convention as the existing `XStoreClientLiveTest.cpp`). |
| `tests/CMakeLists.txt` (modify) | Wire the three new test files into `xstore_network_tests` / a new `xstore_keys_cli_tests` target / `xstore_client_tests`. |
| `TODO.md`, `docs/XStore_Technical_Reference.md` (modify) | Mark the CURVE item done, document the opt-in flags. |

---

### Task 1: Enable CURVE support + core Z85/keypair primitives

**Files:**
- Modify: `CMakeLists.txt:100-104` (the `XSTORE_FETCH_ZMQ` branch), `CMakeLists.txt:208-230` (the `XStore` executable's source list)
- Modify: `include/XStore/XStore_Tunables.h` (add tunable near `SYS_STATS_COLLECT_INTERVAL_MS`, line ~368)
- Create: `include/Networking/XStore_Curve.h`
- Create: `src/XStore_Networking/XStore_Curve.cpp`
- Test: `tests/integration/XStoreCurveTest.cpp` (new file, first section only for this task)
- Modify: `tests/CMakeLists.txt` (add the new test file + `XStore_Curve.cpp` to the `xstore_network_tests` target)

**Interfaces:**
- Produces (used by every later task):
  ```cpp
  namespace XStoreTunables { inline constexpr unsigned CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS = 1000; }

  std::string Z85ReadFile(const std::filesystem::path &path);
  void Z85WriteFile(const std::filesystem::path &path, const std::string &z85, bool is_secret);
  std::array<uint8_t, 32> Z85Decode(const std::string &z85);
  std::string Z85Encode(const std::array<uint8_t, 32> &raw);

  struct XStoreCurveKeyPair {
      std::string public_key_z85;
      std::string secret_key_z85;
      static XStoreCurveKeyPair Generate();
      static XStoreCurveKeyPair LoadFromSecretKeyFile(const std::filesystem::path &secret_key_file);
      void SaveToFiles(const std::filesystem::path &public_key_file, const std::filesystem::path &secret_key_file) const;
  };
  ```

- [ ] **Step 1: Flip `ENABLE_CURVE` on and confirm libsodium is picked up**

In `CMakeLists.txt`, line 101, change:

```cmake
    set(ENABLE_CURVE   OFF CACHE BOOL "" FORCE)
```

to:

```cmake
    # ON: libzmq's own CMake auto-detects libsodium (already an unused CI apt
    # dependency, see TODO.md's compression-work section) via its bundled
    # FindSodium module and links it if present; if absent it falls back to
    # libzmq's own vendored tweetnacl implementation. Either way CURVE
    # support compiles in -- runtime code below (Task 5) verifies via
    # zmq_has("curve") rather than assuming this at configure time.
    set(ENABLE_CURVE   ON  CACHE BOOL "" FORCE)
```

- [ ] **Step 2: Write the failing test for Z85 round trip + keypair generation**

Create `tests/integration/XStoreCurveTest.cpp`:

```cpp
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
```

`ScratchDir`'s constructor takes a label and exposes the scratch directory as the public `fs::path test_root` member (`tests/integration/XStoreTestSupport.h:100-114`) — it also moves the process's cwd into that directory for the object's lifetime and restores it on destruction, which is harmless here since every path used above is already fully qualified via `scratch.test_root`.

- [ ] **Step 3: Run the test to verify it fails to compile (header doesn't exist yet)**

Run: `cmake --build build --target xstore_network_tests 2>&1 | tail -30`
Expected: FAIL — `include/Networking/XStore_Curve.h: No such file or directory`

- [ ] **Step 4: Add the tunable**

In `include/XStore/XStore_Tunables.h`, immediately after the `SYS_STATS_COLLECT_INTERVAL_MS` block (line 368):

```cpp
// FilesystemKeyStore (XStore_Curve.*): minimum gap between two mtime checks
// on the CURVE allowlist directory. Refresh() is invoked synchronously on
// every ZAP request (not on a background timer -- the ZAP handler thread
// already blocks on recv(), so there's no independent timer to run one
// against) -- this is a debounce so a burst of concurrent connection
// attempts pays one stat() call, not one per request.
inline constexpr unsigned CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS = 1000;
```

- [ ] **Step 5: Implement `include/Networking/XStore_Curve.h`**

```cpp
#ifndef XSTORE_CURVE_H
#define XSTORE_CURVE_H

#include <array>
#include <cstdint>
#include <filesystem>
#include <string>

// CURVE transport auth: keypair generation/persistence, Z85 encode/decode,
// and (in this same library, added by later tasks) the KeyStore/
// FilesystemKeyStore allowlist abstraction and XStoreZapHandler. See
// docs/superpowers/specs/2026-09-09-zmq-curve-auth-design.md for the full
// design.
//
// On-disk key format is always 40-character Z85 text, never raw binary --
// this is both what zmq_curve_keypair() natively emits and what cppzmq's
// curve_* sockopts accept directly, so callers never need to decode/re-encode
// just to hand a key to a socket.

// Reads a single Z85-encoded key from a text file (the format every .pub and
// .key file uses). Throws std::runtime_error if the file can't be opened or
// doesn't contain exactly 40 characters.
std::string Z85ReadFile(const std::filesystem::path &path);

// Writes z85 to path as plain text. When is_secret is true, chmods the file
// to owner-read/write only (0600) after writing -- callers must pass true
// for every .key file and false for every .pub file.
void Z85WriteFile(const std::filesystem::path &path, const std::string &z85, bool is_secret);

// Decodes a 40-character Z85 string to its raw 32 bytes. Needed only to
// compare against ZAP's raw-binary CURVE credential frame (RFC 27), which
// always arrives as raw bytes, never Z85 text. Throws std::invalid_argument
// on malformed input.
std::array<uint8_t, 32> Z85Decode(const std::string &z85);

// Re-encodes raw bytes back to Z85 text -- used only for logging a
// human-readable fingerprint of a key ZAP received as raw bytes.
std::string Z85Encode(const std::array<uint8_t, 32> &raw);

struct XStoreCurveKeyPair {
    std::string public_key_z85;  // 40 chars
    std::string secret_key_z85;  // 40 chars

    // Generates a fresh Curve25519 keypair via zmq_curve_keypair().
    static XStoreCurveKeyPair Generate();

    // Loads secret_key_file and its sibling public key -- same directory and
    // stem, ".pub" extension. This is exactly what SaveToFiles() below (and
    // xstore-keys gen, Task 4) produces, and is the one loading convention
    // used everywhere in this design (server identity, client identity).
    // Throws std::runtime_error if either file is missing or malformed.
    static XStoreCurveKeyPair LoadFromSecretKeyFile(const std::filesystem::path &secret_key_file);

    // Writes both files (creating parent directories if needed). secret_key_file
    // is written mode 0600; public_key_file is left at the process's default umask.
    void SaveToFiles(const std::filesystem::path &public_key_file,
                      const std::filesystem::path &secret_key_file) const;
};

#endif  // XSTORE_CURVE_H
```

- [ ] **Step 6: Implement `src/XStore_Networking/XStore_Curve.cpp`**

```cpp
#include "include/Networking/XStore_Curve.h"

#include <zmq.h>

#include <cstring>
#include <fstream>
#include <stdexcept>

#include "include/Logger/Logger.h"

std::string Z85ReadFile(const std::filesystem::path &path) {
    std::ifstream f(path);
    if (!f.is_open()) {
        throw std::runtime_error("Z85ReadFile: cannot open " + path.string());
    }
    std::string key;
    f >> key;
    if (key.size() != 40) {
        throw std::runtime_error("Z85ReadFile: " + path.string() +
                                  " does not contain a 40-character Z85 key (got " +
                                  std::to_string(key.size()) + " characters)");
    }
    return key;
}

void Z85WriteFile(const std::filesystem::path &path, const std::string &z85, bool is_secret) {
    if (path.has_parent_path()) {
        std::filesystem::create_directories(path.parent_path());
    }
    {
        std::ofstream f(path, std::ios::trunc);
        if (!f.is_open()) {
            throw std::runtime_error("Z85WriteFile: cannot open " + path.string() + " for writing");
        }
        f << z85;
    }
    if (is_secret) {
        std::filesystem::permissions(path,
            std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
            std::filesystem::perm_options::replace);
    }
}

std::array<uint8_t, 32> Z85Decode(const std::string &z85) {
    if (z85.size() != 40) {
        throw std::invalid_argument("Z85Decode: expected a 40-character Z85 string, got " +
                                     std::to_string(z85.size()) + " characters");
    }
    std::array<uint8_t, 32> raw{};
    if (zmq_z85_decode(raw.data(), z85.c_str()) == nullptr) {
        throw std::invalid_argument("Z85Decode: input is not valid Z85");
    }
    return raw;
}

std::string Z85Encode(const std::array<uint8_t, 32> &raw) {
    char buf[41];
    if (zmq_z85_encode(buf, raw.data(), raw.size()) == nullptr) {
        throw std::runtime_error("Z85Encode: zmq_z85_encode failed");
    }
    return std::string(buf, 40);
}

XStoreCurveKeyPair XStoreCurveKeyPair::Generate() {
    char pub[41];
    char sec[41];
    if (zmq_curve_keypair(pub, sec) != 0) {
        throw std::runtime_error(std::string("zmq_curve_keypair failed: ") + zmq_strerror(zmq_errno()));
    }
    XStoreCurveKeyPair kp;
    kp.public_key_z85.assign(pub, 40);
    kp.secret_key_z85.assign(sec, 40);
    return kp;
}

XStoreCurveKeyPair XStoreCurveKeyPair::LoadFromSecretKeyFile(const std::filesystem::path &secret_key_file) {
    std::filesystem::path public_key_file = secret_key_file;
    public_key_file.replace_extension(".pub");
    XStoreCurveKeyPair kp;
    kp.secret_key_z85 = Z85ReadFile(secret_key_file);
    kp.public_key_z85 = Z85ReadFile(public_key_file);
    return kp;
}

void XStoreCurveKeyPair::SaveToFiles(const std::filesystem::path &public_key_file,
                                      const std::filesystem::path &secret_key_file) const {
    Z85WriteFile(public_key_file, public_key_z85, /*is_secret=*/false);
    Z85WriteFile(secret_key_file, secret_key_z85, /*is_secret=*/true);
}
```

- [ ] **Step 7: Wire the new files into the build**

In `CMakeLists.txt`, add the two new files to the `XStore` executable's source list (after the `ZMQ_Server.cpp` line, ~226):

```cmake
        include/Networking/ZMQ_Server.h src/XStore_Networking/ZMQ_Server.cpp
        include/Networking/XStore_Curve.h src/XStore_Networking/XStore_Curve.cpp
```

In `tests/CMakeLists.txt`, add to `xstore_network_tests`'s source list (after `${CMAKE_SOURCE_DIR}/src/XStore_Networking/ZMQ_Server.cpp`, line 59):

```cmake
        ${CMAKE_SOURCE_DIR}/src/XStore_Networking/XStore_Curve.cpp
        integration/XStoreCurveTest.cpp
```

Also extend that target's `POST_BUILD` ctest filter (line 73) to include the new suite:

```cmake
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^XStoreZmqNetworkTest\\.\\|^XStoreZmqConcurrentTest\\.\\|^XStoreCurve"
```

- [ ] **Step 8: Build and run**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --target xstore_network_tests -j$(nproc)`
Expected: builds clean, 0 warnings.

Run: `./build/tests/xstore_network_tests --gtest_filter=XStoreCurveKeyPairTest.*`
Expected: 3/3 PASS.

- [ ] **Step 9: Verify `zmq_has("curve")` is actually true after the rebuild**

Run: `cat <<'EOF' > /tmp/curve_check.cpp
#include <zmq.h>
#include <cstdio>
int main() { printf("curve=%d\n", zmq_has("curve")); }
EOF
g++ -I build/_deps/zmq-src/include /tmp/curve_check.cpp -Lbuild -lzmq -Wl,-rpath,build -o /tmp/curve_check 2>&1 | tail -20
/tmp/curve_check`

Expected: prints `curve=1`. If it prints `curve=0`, stop and investigate the libzmq FetchContent build log (`grep -i sodium build/_deps/zmq-build/CMakeCache.txt`) before proceeding to any later task — every later task depends on CURVE actually being compiled in.

- [ ] **Step 10: Commit**

```bash
git add CMakeLists.txt include/XStore/XStore_Tunables.h include/Networking/XStore_Curve.h src/XStore_Networking/XStore_Curve.cpp tests/integration/XStoreCurveTest.cpp tests/CMakeLists.txt
git commit -m "feat(curve): enable libzmq CURVE support, add Z85/keypair primitives

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 2: `KeyStore` interface + `FilesystemKeyStore`

**Files:**
- Modify: `include/Networking/XStore_Curve.h` (append)
- Modify: `src/XStore_Networking/XStore_Curve.cpp` (append)
- Test: `tests/integration/XStoreCurveTest.cpp` (append)

**Interfaces:**
- Consumes: `Z85Decode` (Task 1), `XStoreTunables::CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS` (Task 1)
- Produces:
  ```cpp
  class KeyStore {
  public:
      virtual ~KeyStore() = default;
      virtual bool IsAllowed(const std::array<uint8_t, 32> &pubkey_raw, std::string *name_out) const = 0;
      virtual void Refresh() = 0;
  };

  class FilesystemKeyStore : public KeyStore {
  public:
      FilesystemKeyStore(std::filesystem::path allowed_dir, std::chrono::milliseconds min_check_interval);
      bool IsAllowed(const std::array<uint8_t, 32> &pubkey_raw, std::string *name_out) const override;
      void Refresh() override;
  };
  ```

- [ ] **Step 1: Write the failing tests**

Append to `tests/integration/XStoreCurveTest.cpp`:

```cpp
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
```

- [ ] **Step 2: Run to verify it fails to compile**

Run: `cmake --build build --target xstore_network_tests 2>&1 | tail -20`
Expected: FAIL — `KeyStore`/`FilesystemKeyStore` undeclared.

- [ ] **Step 3: Append the interface to `include/Networking/XStore_Curve.h`**

```cpp
#include <chrono>
#include <memory>
#include <unordered_map>

// ... (after XStoreCurveKeyPair)

class KeyStore {
public:
    virtual ~KeyStore() = default;

    // pubkey_raw: the 32 raw bytes ZAP hands the caller for a connecting
    // peer's CURVE public key. Returns true and fills *name_out (if
    // non-null) with the identity's name when pubkey_raw is currently
    // allowed.
    virtual bool IsAllowed(const std::array<uint8_t, 32> &pubkey_raw, std::string *name_out) const = 0;

    // Forces an immediate re-scan/re-fetch of the backing store, bypassing
    // any internal debounce. Implementations do not spawn their own
    // threads -- callers (XStoreZapHandler, or a test) decide when to call
    // this; IsAllowed() itself also keeps its snapshot reasonably fresh on
    // its own (see FilesystemKeyStore).
    virtual void Refresh() = 0;
};

// Backed by one Z85-encoded ".pub" file per identity under allowed_dir
// (e.g. keys/allowed/alice.pub) -- see the design spec's Approach A for why
// per-identity files were chosen over a single serialized allowlist file.
class FilesystemKeyStore : public KeyStore {
public:
    FilesystemKeyStore(std::filesystem::path allowed_dir, std::chrono::milliseconds min_check_interval);

    bool IsAllowed(const std::array<uint8_t, 32> &pubkey_raw, std::string *name_out) const override;
    void Refresh() override;

private:
    struct RawKeyHash {
        size_t operator()(const std::array<uint8_t, 32> &k) const noexcept;
    };
    using SnapshotMap = std::unordered_map<std::array<uint8_t, 32>, std::string, RawKeyHash>;

    // Re-scans allowed_dir_ if last_check_ is due and the directory's mtime
    // has advanced since the last scan; otherwise a cheap no-op (one
    // stat() call). Called from both IsAllowed() (const) and Refresh()
    // (non-const, after resetting last_check_ to force an immediate scan)
    // -- hence all touched state is mutable, the standard const-correctness
    // idiom for an internally-cached const method.
    void MaybeRescan() const;

    std::filesystem::path allowed_dir_;
    std::chrono::milliseconds min_check_interval_;
    mutable std::chrono::steady_clock::time_point last_check_{};
    mutable std::filesystem::file_time_type last_mtime_{};
    mutable std::shared_ptr<const SnapshotMap> snapshot_;
};
```

- [ ] **Step 4: Append the implementation to `src/XStore_Networking/XStore_Curve.cpp`**

```cpp
#include "include/XStore/XStore_Tunables.h"

size_t FilesystemKeyStore::RawKeyHash::operator()(const std::array<uint8_t, 32> &k) const noexcept {
    // FNV-1a over the 32 bytes -- a lookup table sized in the dozens/
    // hundreds of identities needs no cryptographic hash property, just a
    // decent distribution.
    size_t h = 14695981039346656037ULL;
    for (uint8_t b : k) {
        h ^= b;
        h *= 1099511628211ULL;
    }
    return h;
}

FilesystemKeyStore::FilesystemKeyStore(std::filesystem::path allowed_dir, std::chrono::milliseconds min_check_interval)
    : allowed_dir_(std::move(allowed_dir)), min_check_interval_(min_check_interval) {
    snapshot_ = std::make_shared<const SnapshotMap>();
    MaybeRescan();
}

bool FilesystemKeyStore::IsAllowed(const std::array<uint8_t, 32> &pubkey_raw, std::string *name_out) const {
    MaybeRescan();
    auto snap = std::atomic_load(&snapshot_);
    auto it = snap->find(pubkey_raw);
    if (it == snap->end()) return false;
    if (name_out) *name_out = it->second;
    return true;
}

void FilesystemKeyStore::Refresh() {
    last_check_ = std::chrono::steady_clock::time_point{};  // forces MaybeRescan to treat it as due
    MaybeRescan();
}

void FilesystemKeyStore::MaybeRescan() const {
    auto now = std::chrono::steady_clock::now();
    if (now - last_check_ < min_check_interval_) return;
    last_check_ = now;

    std::error_code ec;
    auto mtime = std::filesystem::last_write_time(allowed_dir_, ec);
    if (ec) {
        spdlog::warn("[CURVE] - Cannot stat allowed-dir {}: {}", allowed_dir_.string(), ec.message());
        return;
    }
    if (mtime == last_mtime_) return;  // unchanged since last scan
    last_mtime_ = mtime;

    auto fresh = std::make_shared<SnapshotMap>();
    for (const auto &entry : std::filesystem::directory_iterator(allowed_dir_, ec)) {
        if (ec) break;
        if (entry.path().extension() != ".pub") continue;
        std::string z85;
        try {
            z85 = Z85ReadFile(entry.path());
        } catch (const std::exception &e) {
            spdlog::warn("[CURVE] - Skipping malformed allowlist entry {}: {}", entry.path().string(), e.what());
            continue;
        }
        std::array<uint8_t, 32> raw{};
        try {
            raw = Z85Decode(z85);
        } catch (const std::exception &e) {
            spdlog::warn("[CURVE] - Skipping malformed allowlist entry {}: {}", entry.path().string(), e.what());
            continue;
        }
        (*fresh)[raw] = entry.path().stem().string();
    }
    std::atomic_store(&snapshot_, std::shared_ptr<const SnapshotMap>(fresh));
}
```

- [ ] **Step 5: Build and run**

Run: `cmake --build build --target xstore_network_tests -j$(nproc) && ./build/tests/xstore_network_tests --gtest_filter=FilesystemKeyStoreTest.*`
Expected: 4/4 PASS.

- [ ] **Step 6: Commit**

```bash
git add include/Networking/XStore_Curve.h src/XStore_Networking/XStore_Curve.cpp tests/integration/XStoreCurveTest.cpp
git commit -m "feat(curve): add KeyStore interface + FilesystemKeyStore allowlist

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 3: `XStoreZapHandler`

**Files:**
- Modify: `include/Networking/XStore_Curve.h` (append)
- Modify: `src/XStore_Networking/XStore_Curve.cpp` (append)
- Test: `tests/integration/XStoreCurveTest.cpp` (append)

**Interfaces:**
- Consumes: `KeyStore` (Task 2), `Z85Encode` (Task 1)
- Produces:
  ```cpp
  class XStoreZapHandler {
  public:
      XStoreZapHandler(zmq::context_t &ctx, std::shared_ptr<KeyStore> store);
      void Start();
      void Stop();
      ~XStoreZapHandler();
  };
  ```

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/XStoreCurveTest.cpp`:

```cpp
#include <zmq.hpp>
#include <zmq_addon.hpp>

namespace {
// Minimal CURVE-enabled ROUTER<->DEALER pair, no ZmqServer/XStore involved
// -- isolates ZAP correctness from the full server integration (Task 5).
struct CurvePair {
    zmq::context_t ctx{1};
    zmq::socket_t router{ctx, zmq::socket_type::router};
    XStoreCurveKeyPair server_kp = XStoreCurveKeyPair::Generate();

    explicit CurvePair(std::shared_ptr<KeyStore> store) : zap_(ctx, std::move(store)) {
        router.set(zmq::sockopt::curve_server, true);
        router.set(zmq::sockopt::curve_secretkey, server_kp.secret_key_z85);
        router.set(zmq::sockopt::zap_domain, "xstore-test");
        router.set(zmq::sockopt::rcvtimeo, 500);
        router.set(zmq::sockopt::linger, 0);
        zap_.Start();
        // A fixed-but-spread port (same pattern XStoreZmqNetworkTest and the
        // later ZmqServerCurveTest fixture use), not bind-to-port-0 --
        // avoids depending on an unverified ephemeral-port/last_endpoint
        // query API in this vendored cppzmq version.
        int port = 25900 + static_cast<int>(getpid() % 500);
        endpoint_ = "tcp://127.0.0.1:" + std::to_string(port);
        router.bind(endpoint_);
    }

    zmq::socket_t MakeClient(const XStoreCurveKeyPair &client_kp) {
        zmq::socket_t dealer(ctx, zmq::socket_type::dealer);
        dealer.set(zmq::sockopt::curve_serverkey, server_kp.public_key_z85);
        dealer.set(zmq::sockopt::curve_publickey, client_kp.public_key_z85);
        dealer.set(zmq::sockopt::curve_secretkey, client_kp.secret_key_z85);
        dealer.set(zmq::sockopt::rcvtimeo, 500);
        dealer.set(zmq::sockopt::linger, 0);
        dealer.connect(endpoint_);
        return dealer;
    }

    std::string endpoint_;
    XStoreZapHandler zap_;
};
}  // namespace

TEST(XStoreZapHandlerTest, AllowedClientHandshakeSucceeds) {
    ScratchDir scratch("zap_allow_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);
    auto client_kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(allowed_dir / "alice.pub", client_kp.public_key_z85, false);

    auto store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));
    CurvePair pair(store);
    auto client = pair.MakeClient(client_kp);

    client.send(zmq::str_buffer("ping"), zmq::send_flags::none);
    std::vector<zmq::message_t> frames;
    ASSERT_TRUE(zmq::recv_multipart(pair.router, std::back_inserter(frames)));
    ASSERT_GE(frames.size(), 2u);
    EXPECT_EQ(frames.back().to_string(), "ping");
}

TEST(XStoreZapHandlerTest, UnlistedClientHandshakeIsDenied) {
    ScratchDir scratch("zap_deny_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);  // empty -- nobody allowed

    auto store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));
    CurvePair pair(store);
    auto stranger_kp = XStoreCurveKeyPair::Generate();
    auto client = pair.MakeClient(stranger_kp);

    client.send(zmq::str_buffer("ping"), zmq::send_flags::none);
    std::vector<zmq::message_t> frames;
    // Handshake never completes -- ZAP-denied, so the ROUTER never sees a
    // message from this peer within the timeout.
    EXPECT_FALSE(zmq::recv_multipart(pair.router, std::back_inserter(frames)));
}
```

- [ ] **Step 2: Run to verify it fails to compile**

Run: `cmake --build build --target xstore_network_tests 2>&1 | tail -20`
Expected: FAIL — `XStoreZapHandler` undeclared.

- [ ] **Step 3: Append the interface to `include/Networking/XStore_Curve.h`**

```cpp
#include <atomic>
#include <thread>
#include <zmq.hpp>

// Reusable ZAP (RFC 27) responder. One instance per zmq::context_t --
// inproc://zeromq.zap.01 is a per-context endpoint, and ZmqServer/
// SysStatsCollector deliberately run on separate contexts already, so each
// gets its own XStoreZapHandler sharing the same KeyStore instance.
class XStoreZapHandler {
public:
    XStoreZapHandler(zmq::context_t &ctx, std::shared_ptr<KeyStore> store);
    ~XStoreZapHandler();

    // Binds inproc://zeromq.zap.01 on ctx and starts the responder thread.
    // Blocks briefly after spawning so the handler is guaranteed bound
    // before returning -- callers must call this BEFORE binding/connecting
    // any CURVE-enabled socket on the same context, or an early handshake
    // could race an as-yet-unbound ZAP endpoint.
    void Start();

    // Signals the loop to stop and joins it. Called automatically by the
    // destructor; safe to call explicitly first if a caller needs a
    // deterministic stop point before ctx is torn down.
    void Stop();

private:
    void Loop();

    zmq::context_t &ctx_;
    std::shared_ptr<KeyStore> store_;
    std::thread thread_;
    std::atomic<bool> stop_flag_{false};
};
```

- [ ] **Step 4: Append the implementation to `src/XStore_Networking/XStore_Curve.cpp`**

```cpp
#include <cstring>
#include <zmq_addon.hpp>

XStoreZapHandler::XStoreZapHandler(zmq::context_t &ctx, std::shared_ptr<KeyStore> store)
    : ctx_(ctx), store_(std::move(store)) {}

XStoreZapHandler::~XStoreZapHandler() { Stop(); }

void XStoreZapHandler::Start() {
    stop_flag_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&XStoreZapHandler::Loop, this);
    // Give the handler socket a moment to bind before returning -- Start()
    // is documented as a hard ordering requirement (see header), not just a
    // latency nicety.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

void XStoreZapHandler::Stop() {
    stop_flag_.store(true, std::memory_order_relaxed);
    if (thread_.joinable()) thread_.join();
}

void XStoreZapHandler::Loop() {
    zmq::socket_t zap(ctx_, zmq::socket_type::rep);
    zap.set(zmq::sockopt::rcvtimeo, 200);  // periodic wake to observe stop_flag_
    zap.set(zmq::sockopt::linger, 0);
    try {
        zap.bind("inproc://zeromq.zap.01");
    } catch (const zmq::error_t &e) {
        spdlog::error("[CURVE] - ZAP handler failed to bind inproc://zeromq.zap.01: {}", e.what());
        return;
    }

    while (!stop_flag_.load(std::memory_order_relaxed)) {
        std::vector<zmq::message_t> request;
        if (!zmq::recv_multipart(zap, std::back_inserter(request))) continue;  // timeout
        if (request.size() < 7) {
            spdlog::warn("[CURVE] - Malformed ZAP request ({} frames), dropping", request.size());
            continue;
        }

        std::string version = request[0].to_string();
        std::string sequence = request[1].to_string();
        std::string mechanism = request[5].to_string();

        std::string status_code = "400";
        std::string status_text = "Denied";
        std::string user_id;

        if (mechanism == "CURVE" && request[6].size() == 32) {
            std::array<uint8_t, 32> pubkey_raw{};
            std::memcpy(pubkey_raw.data(), request[6].data(), 32);
            std::string name;
            if (store_->IsAllowed(pubkey_raw, &name)) {
                status_code = "200";
                status_text = "OK";
                user_id = name;
                spdlog::info("[CURVE] - ZAP allowed '{}'", name);
            } else {
                spdlog::warn("[CURVE] - ZAP denied unknown CURVE key (fingerprint {}...{})",
                             Z85Encode(pubkey_raw).substr(0, 6), Z85Encode(pubkey_raw).substr(34));
            }
        } else {
            spdlog::warn("[CURVE] - ZAP request with unexpected mechanism '{}', denying", mechanism);
        }

        zmq::multipart_t reply;
        reply.addstr(version);
        reply.addstr(sequence);
        reply.addstr(status_code);
        reply.addstr(status_text);
        reply.addstr(user_id);
        reply.addstr("");  // metadata, unused
        reply.send(zap);
    }
}
```

- [ ] **Step 5: Build and run**

Run: `cmake --build build --target xstore_network_tests -j$(nproc) && ./build/tests/xstore_network_tests --gtest_filter=XStoreZapHandlerTest.*`
Expected: 2/2 PASS. If `UnlistedClientHandshakeIsDenied` instead hangs or fails intermittently, check that `router.set(zmq::sockopt::zap_domain, ...)` is actually present — libzmq only invokes ZAP at all when a non-empty ZAP domain is set on the server-mode socket; without it every CURVE handshake succeeds unconditionally regardless of the allowlist.

- [ ] **Step 6: Commit**

```bash
git add include/Networking/XStore_Curve.h src/XStore_Networking/XStore_Curve.cpp tests/integration/XStoreCurveTest.cpp
git commit -m "feat(curve): add XStoreZapHandler (RFC 27 ZAP responder)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 4: `xstore-keys` CLI tool

**Files:**
- Create: `tools/xstore-keys/CMakeLists.txt`
- Create: `tools/xstore-keys/main.cpp`
- Modify: `CMakeLists.txt` (add `add_subdirectory(tools/xstore-keys)`)
- Modify: `tests/CMakeLists.txt` (add `xstore_keys_cli_tests` target)
- Test: `tests/integration/XStoreKeysCliTest.cpp` (new)

**Interfaces:**
- Consumes: `XStoreCurveKeyPair`, `Z85ReadFile`, `Z85WriteFile` (Task 1)
- Produces: the `xstore-keys` binary (`gen <name>`, `allow <name> --pubkey-file <path>`, `revoke <name>`, `list`), each accepting `--out-dir`/`--allowed-dir` overrides.

- [ ] **Step 1: Write the failing CLI test**

Create `tests/integration/XStoreKeysCliTest.cpp`:

```cpp
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
```

- [ ] **Step 2: Run to verify it fails (binary/definition don't exist)**

Run: `cmake --build build --target xstore_keys_cli_tests 2>&1 | tail -20`
Expected: FAIL — target doesn't exist yet.

- [ ] **Step 3: Implement `tools/xstore-keys/main.cpp`**

```cpp
#include <argparse/argparse.hpp>
#include <filesystem>
#include <iostream>

#include "include/Networking/XStore_Curve.h"

namespace fs = std::filesystem;

namespace {

int RunGen(const std::string &name, const std::string &out_dir_arg, bool force) {
    fs::path out_dir = out_dir_arg;
    if (out_dir.empty()) {
        out_dir = (name == "server") ? fs::path("keys/server") : (fs::path("keys/clients") / name);
    }
    fs::path pub_path = out_dir / (name + ".pub");
    fs::path sec_path = out_dir / (name + ".key");
    if (!force && (fs::exists(pub_path) || fs::exists(sec_path))) {
        std::cerr << "xstore-keys: " << pub_path << " or " << sec_path
                  << " already exists -- pass --force to overwrite\n";
        return 1;
    }
    auto kp = XStoreCurveKeyPair::Generate();
    kp.SaveToFiles(pub_path, sec_path);
    std::cout << "Generated '" << name << "':\n"
              << "  public:  " << pub_path << " (share this)\n"
              << "  secret:  " << sec_path << " (keep private, mode 0600)\n";
    return 0;
}

int RunAllow(const std::string &name, const std::string &pubkey_file, const std::string &allowed_dir_arg, bool force) {
    fs::path allowed_dir = allowed_dir_arg;
    fs::path dest = allowed_dir / (name + ".pub");
    if (!force && fs::exists(dest)) {
        std::cerr << "xstore-keys: " << dest << " already exists -- pass --force to re-key '" << name << "'\n";
        return 1;
    }
    std::string z85;
    try {
        z85 = Z85ReadFile(pubkey_file);
    } catch (const std::exception &e) {
        std::cerr << "xstore-keys: " << e.what() << "\n";
        return 1;
    }
    Z85WriteFile(dest, z85, /*is_secret=*/false);
    std::cout << "Allowed '" << name << "' -> " << dest << "\n";
    return 0;
}

int RunRevoke(const std::string &name, const std::string &allowed_dir_arg) {
    fs::path dest = fs::path(allowed_dir_arg) / (name + ".pub");
    std::error_code ec;
    bool removed = fs::remove(dest, ec);
    if (removed) {
        std::cout << "Revoked '" << name << "'\n";
    } else {
        std::cout << "xstore-keys: no such identity '" << name << "' in " << allowed_dir_arg << " (nothing to revoke)\n";
    }
    return 0;
}

int RunList(const std::string &allowed_dir_arg) {
    fs::path allowed_dir = allowed_dir_arg;
    std::error_code ec;
    if (!fs::exists(allowed_dir, ec)) {
        std::cout << "(no allowlist directory at " << allowed_dir << ")\n";
        return 0;
    }
    bool any = false;
    for (const auto &entry : fs::directory_iterator(allowed_dir, ec)) {
        if (ec) break;
        if (entry.path().extension() != ".pub") continue;
        any = true;
        std::string z85;
        try {
            z85 = Z85ReadFile(entry.path());
        } catch (const std::exception &e) {
            std::cout << entry.path().stem().string() << "  <malformed: " << e.what() << ">\n";
            continue;
        }
        std::cout << entry.path().stem().string() << "  " << z85.substr(0, 6) << "..." << z85.substr(34) << "\n";
    }
    if (!any) std::cout << "(no identities allowed in " << allowed_dir << ")\n";
    return 0;
}

}  // namespace

int main(int argc, char *argv[]) {
    argparse::ArgumentParser program("xstore-keys");
    program.add_description("CURVE key housekeeping for XStore: generate identities, manage the server allowlist.");

    argparse::ArgumentParser gen_cmd("gen");
    gen_cmd.add_description("Generate a new Curve25519 keypair for an identity.");
    gen_cmd.add_argument("name");
    gen_cmd.add_argument("--out-dir").default_value(std::string(""))
        .help("output directory (default: keys/server/ for 'server', else keys/clients/<name>/)");
    gen_cmd.add_argument("--force").default_value(false).implicit_value(true)
        .help("overwrite an existing keypair for this name");

    argparse::ArgumentParser allow_cmd("allow");
    allow_cmd.add_description("Add an identity's public key to the server's allowlist.");
    allow_cmd.add_argument("name");
    allow_cmd.add_argument("--pubkey-file").required().help("path to the identity's .pub file");
    allow_cmd.add_argument("--allowed-dir").default_value(std::string("keys/allowed"));
    allow_cmd.add_argument("--force").default_value(false).implicit_value(true)
        .help("overwrite an existing allowlist entry for this name");

    argparse::ArgumentParser revoke_cmd("revoke");
    revoke_cmd.add_description("Remove an identity from the server's allowlist.");
    revoke_cmd.add_argument("name");
    revoke_cmd.add_argument("--allowed-dir").default_value(std::string("keys/allowed"));

    argparse::ArgumentParser list_cmd("list");
    list_cmd.add_description("List identities currently on the server's allowlist.");
    list_cmd.add_argument("--allowed-dir").default_value(std::string("keys/allowed"));

    program.add_subparser(gen_cmd);
    program.add_subparser(allow_cmd);
    program.add_subparser(revoke_cmd);
    program.add_subparser(list_cmd);

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception &err) {
        std::cerr << err.what() << "\n" << program;
        return 1;
    }

    try {
        if (program.is_subcommand_used(gen_cmd)) {
            return RunGen(gen_cmd.get<std::string>("name"), gen_cmd.get<std::string>("--out-dir"), gen_cmd.get<bool>("--force"));
        }
        if (program.is_subcommand_used(allow_cmd)) {
            return RunAllow(allow_cmd.get<std::string>("name"), allow_cmd.get<std::string>("--pubkey-file"),
                             allow_cmd.get<std::string>("--allowed-dir"), allow_cmd.get<bool>("--force"));
        }
        if (program.is_subcommand_used(revoke_cmd)) {
            return RunRevoke(revoke_cmd.get<std::string>("name"), revoke_cmd.get<std::string>("--allowed-dir"));
        }
        if (program.is_subcommand_used(list_cmd)) {
            return RunList(list_cmd.get<std::string>("--allowed-dir"));
        }
    } catch (const std::exception &e) {
        std::cerr << "xstore-keys: " << e.what() << "\n";
        return 1;
    }

    std::cerr << program;
    return 1;
}
```

- [ ] **Step 4: Create `tools/xstore-keys/CMakeLists.txt`**

```cmake
add_executable(xstore-keys main.cpp)
target_include_directories(xstore-keys PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(xstore-keys PRIVATE "zmq")
```

Note: `XStore_Curve.cpp` must be compiled into this target too since it's not a library elsewhere yet. Add it directly:

```cmake
add_executable(xstore-keys main.cpp ${CMAKE_SOURCE_DIR}/src/XStore_Networking/XStore_Curve.cpp)
target_include_directories(xstore-keys PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(xstore-keys PRIVATE "zmq")
```

- [ ] **Step 5: Wire the subdirectory into the root `CMakeLists.txt`**

After the `XStore` executable's `target_link_libraries` block (~line 235):

```cmake
add_subdirectory(tools/xstore-keys)
```

- [ ] **Step 6: Add the `xstore_keys_cli_tests` target to `tests/CMakeLists.txt`**

After the `xstore_client_tests` block (~line 102):

```cmake
# xstore_keys_cli_tests: black-box smoke tests for the xstore-keys binary
# (tools/xstore-keys) -- invokes the real built binary via std::system(),
# not a unit test of its internals (those are covered by
# XStoreCurveKeyPairTest in xstore_network_tests).
add_executable(xstore_keys_cli_tests integration/XStoreKeysCliTest.cpp
        ${CMAKE_SOURCE_DIR}/src/XStore_Networking/XStore_Curve.cpp)
target_include_directories(xstore_keys_cli_tests PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(xstore_keys_cli_tests PRIVATE GTest::gtest_main "zmq" "stdc++fs")
target_compile_definitions(xstore_keys_cli_tests PRIVATE XSTORE_KEYS_BIN="$<TARGET_FILE:xstore-keys>")
add_dependencies(xstore_keys_cli_tests xstore-keys)

include(GoogleTest)
gtest_discover_tests(xstore_keys_cli_tests)

add_custom_command(TARGET xstore_keys_cli_tests POST_BUILD
        COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure -R "^XStoreKeysCliTest\\."
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Running xstore_keys_cli_tests via ctest"
)
```

- [ ] **Step 7: Build and run**

Run: `cmake -B build && cmake --build build --target xstore_keys_cli_tests -j$(nproc)`
Expected: builds clean, then ctest run reports 4/4 PASS.

- [ ] **Step 8: Commit**

```bash
git add tools/xstore-keys CMakeLists.txt tests/CMakeLists.txt tests/integration/XStoreKeysCliTest.cpp
git commit -m "feat(curve): add xstore-keys CLI (gen/allow/revoke/list)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 5: `ZmqServer` CURVE + ZAP integration

**Files:**
- Modify: `include/Networking/ZMQ_Server.h`
- Modify: `src/XStore_Networking/ZMQ_Server.cpp`
- Test: `tests/integration/XStoreCurveTest.cpp` (append end-to-end section)

**Interfaces:**
- Consumes: `XStoreCurveKeyPair`, `KeyStore`, `XStoreZapHandler` (Tasks 1-3)
- Produces: `ZmqServer::StartServer` gains an optional 3rd/4th param; existing 2-arg call sites (all current tests, `main.cpp` pre-Task-6) keep compiling unchanged via default arguments.
  ```cpp
  void StartServer(std::string listen_addr, std::string listen_port,
                    std::optional<XStoreCurveKeyPair> curve_identity = std::nullopt,
                    std::shared_ptr<KeyStore> curve_keystore = nullptr);
  ```

- [ ] **Step 1: Write the failing end-to-end test**

Append to `tests/integration/XStoreCurveTest.cpp`:

```cpp
#include "include/Networking/ZMQ_Server.h"

namespace {
struct CurveServerFixture {
    ScratchDir scratch{"curve_zmq_server_test"};
    XStoreCurveKeyPair server_kp = XStoreCurveKeyPair::Generate();
    std::filesystem::path allowed_dir = scratch.test_root / "allowed";
    std::shared_ptr<FilesystemKeyStore> store;
    std::string server_addr;

    CurveServerFixture() {
        std::filesystem::create_directories(allowed_dir);
        store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));

        int port = 26500 + static_cast<int>(getpid() % 3000);
        server_addr = "tcp://127.0.0.1:" + std::to_string(port);

        auto *server = new ZmqServer(1);
        std::thread server_thread([server, port, this]() {
            server->StartServer("127.0.0.1", std::to_string(port), server_kp, store);
        });
        server_thread.detach();
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    zmq::socket_t MakeClient(zmq::context_t &ctx, const XStoreCurveKeyPair &client_kp) {
        zmq::socket_t sock(ctx, zmq::socket_type::req);
        sock.set(zmq::sockopt::curve_serverkey, server_kp.public_key_z85);
        sock.set(zmq::sockopt::curve_publickey, client_kp.public_key_z85);
        sock.set(zmq::sockopt::curve_secretkey, client_kp.secret_key_z85);
        sock.set(zmq::sockopt::rcvtimeo, 2000);
        sock.set(zmq::sockopt::linger, 0);
        sock.connect(server_addr);
        return sock;
    }
};
}  // namespace

TEST(ZmqServerCurveTest, AllowedClientGetsPingReply) {
    CurveServerFixture fx;
    auto client_kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(fx.allowed_dir / "alice.pub", client_kp.public_key_z85, false);

    zmq::context_t ctx(1);
    auto sock = fx.MakeClient(ctx, client_kp);

    XStore_Proto::Request ping_req;
    ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
    std::string payload = ping_req.SerializeAsString();
    ASSERT_TRUE(sock.send(zmq::buffer(payload), zmq::send_flags::none));

    zmq::message_t reply;
    ASSERT_TRUE(sock.recv(reply, zmq::recv_flags::none));
}

TEST(ZmqServerCurveTest, UnlistedClientTimesOut) {
    CurveServerFixture fx;  // nobody allowed
    auto stranger_kp = XStoreCurveKeyPair::Generate();

    zmq::context_t ctx(1);
    auto sock = fx.MakeClient(ctx, stranger_kp);

    XStore_Proto::Request ping_req;
    ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
    std::string payload = ping_req.SerializeAsString();
    ASSERT_TRUE(sock.send(zmq::buffer(payload), zmq::send_flags::none));

    zmq::message_t reply;
    EXPECT_FALSE(sock.recv(reply, zmq::recv_flags::none));
}

TEST(ZmqServerCurveTest, RevokedClientDeniedOnNextConnection) {
    CurveServerFixture fx;
    auto client_kp = XStoreCurveKeyPair::Generate();
    auto pub_path = fx.allowed_dir / "bob.pub";
    Z85WriteFile(pub_path, client_kp.public_key_z85, false);

    {
        zmq::context_t ctx(1);
        auto sock = fx.MakeClient(ctx, client_kp);
        XStore_Proto::Request ping_req;
        ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
        std::string payload = ping_req.SerializeAsString();
        ASSERT_TRUE(sock.send(zmq::buffer(payload), zmq::send_flags::none));
        zmq::message_t reply;
        ASSERT_TRUE(sock.recv(reply, zmq::recv_flags::none));
    }

    std::filesystem::remove(pub_path);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    zmq::context_t ctx2(1);
    auto sock2 = fx.MakeClient(ctx2, client_kp);  // fresh connection, same identity
    XStore_Proto::Request ping_req2;
    ping_req2.mutable_ping()->set_rtype(XStore_Proto::PING);
    std::string payload2 = ping_req2.SerializeAsString();
    ASSERT_TRUE(sock2.send(zmq::buffer(payload2), zmq::send_flags::none));
    zmq::message_t reply2;
    EXPECT_FALSE(sock2.recv(reply2, zmq::recv_flags::none));
}
```

- [ ] **Step 2: Run to verify it fails to compile**

Run: `cmake --build build --target xstore_network_tests 2>&1 | tail -20`
Expected: FAIL — `StartServer` has no 4-argument overload.

- [ ] **Step 3: Modify `include/Networking/ZMQ_Server.h`**

```cpp
#include <include/XStore/XStore.h>
#include <memory>
#include <optional>
#include <thread>
#include <zmq.hpp>
#include "XStore.pb.h"
#include <zmq_addon.hpp>
#include "include/Logger/Logger.h"
#include "include/Networking/XStore_Curve.h"

class ZmqServer {
    private:
        zmq::context_t ctx;

        int num_threads;
        std::string server_addr;

        void WorkerRoutine(std::string server_addr);

    public:
        ZmqServer(int num_threads);

        // curve_identity/curve_keystore both absent (default): identical to
        // today's behavior, NULL mechanism, no ZAP handler spawned. Both
        // present: CURVE_SERVER mode on the external ROUTER socket, ZAP
        // domain "xstore" set so ZAP is actually invoked, and an
        // XStoreZapHandler is started (bound to this instance's ctx) before
        // the socket binds. Passing only one of the two is a caller error
        // (asserted, not silently half-applied).
        void StartServer(std::string listen_addr, std::string listen_port,
                          std::optional<XStoreCurveKeyPair> curve_identity = std::nullopt,
                          std::shared_ptr<KeyStore> curve_keystore = nullptr);

        static int DefaultThreadCount();
};
```

- [ ] **Step 4: Modify `src/XStore_Networking/ZMQ_Server.cpp`**

Replace the `StartServer` definition (currently lines 99-133):

```cpp
void ZmqServer::StartServer(std::string listen_addr, std::string listen_port,
                             std::optional<XStoreCurveKeyPair> curve_identity,
                             std::shared_ptr<KeyStore> curve_keystore) {
    assert(curve_identity.has_value() == (curve_keystore != nullptr) &&
           "StartServer: curve_identity and curve_keystore must both be set, or both absent");

    //  Socket to talk to CLIENTS
    zmq::socket_t client_socket(ctx, zmq::socket_type::router);
    std::string server_addr = "tcp://" + listen_addr + ":" + listen_port;

    client_socket.set(zmq::sockopt::sndhwm, 0);
    client_socket.set(zmq::sockopt::rcvhwm, 0);

    std::unique_ptr<XStoreZapHandler> zap_handler;
    if (curve_identity && curve_keystore) {
        // ZAP must be bound before this socket binds/accepts any handshake --
        // Start() blocks briefly to guarantee that ordering (see its header).
        zap_handler = std::make_unique<XStoreZapHandler>(ctx, curve_keystore);
        zap_handler->Start();

        client_socket.set(zmq::sockopt::curve_server, true);
        client_socket.set(zmq::sockopt::curve_secretkey, curve_identity->secret_key_z85);
        // Non-empty ZAP domain is what actually makes libzmq invoke ZAP at
        // all for this socket (RFC 27) -- without it, CURVE still encrypts
        // but any client holding a valid keypair would connect regardless
        // of the allowlist.
        client_socket.set(zmq::sockopt::zap_domain, "xstore");
        spdlog::info("[NETWORKING] - CURVE enabled on client-facing socket");
    }

    this->server_addr = server_addr;

    client_socket.bind(server_addr);
    spdlog::info("[NETWORKING] - Listening on: {}", server_addr);

    //  Socket to talk to WORKERS
    zmq::socket_t worker_socket(ctx, zmq::socket_type::dealer);
    worker_socket.set(zmq::sockopt::sndhwm, 0);
    worker_socket.set(zmq::sockopt::rcvhwm, 0);
    worker_socket.bind("inproc://workers");

    //  Launch pool of WORKER threads
    for (int i = 0; i < num_threads; i++) {
        std::thread worker(&ZmqServer::WorkerRoutine, this, server_addr.substr(6));
        worker.detach();
    }
    spdlog::info("[NETWORKING] - Launched: {0:d} worker thread", num_threads);

    // socket_t overload (void* one deprecated since cppzmq 4.3.1); same as previous nullptr capture.
    zmq::proxy(client_socket, worker_socket);
}
```

Add `#include <cassert>` and `#include <memory>` near the top of the file if not already present via the header.

- [ ] **Step 5: Build and run**

Run: `cmake --build build --target xstore_network_tests -j$(nproc) && ./build/tests/xstore_network_tests --gtest_filter=ZmqServerCurveTest.*`
Expected: 3/3 PASS.

- [ ] **Step 6: Confirm zero behavior change for existing callers**

Run: `cmake --build build --target xstore_tests xstore_network_tests -j$(nproc)`
Run: `cd build && ctest --output-on-failure -R "^XStoreTest\.|^XStoreZmqNetworkTest\.|^XStoreZmqConcurrentTest\."`
Expected: same pass count as before this task — every existing 2-arg `StartServer(host, port)` call site (all current tests) must still compile and behave identically, since the two new params default to disabled.

- [ ] **Step 7: Commit**

```bash
git add include/Networking/ZMQ_Server.h src/XStore_Networking/ZMQ_Server.cpp tests/integration/XStoreCurveTest.cpp
git commit -m "feat(curve): wire CURVE + ZAP into ZmqServer, opt-in via new StartServer params

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 6: `SysStatsCollector` CURVE + ZAP integration, and `main.cpp` flags

**Files:**
- Modify: `include/XStore/XStore_SysStats.h`
- Modify: `src/XStore/XStore_SysStats.cpp`
- Modify: `src/main.cpp`
- Modify: `tests/CMakeLists.txt` (add `XStore_SysStats.cpp`/`.h` to `xstore_network_tests` sources)
- Test: `tests/integration/XStoreCurveTest.cpp` (append)

**Interfaces:**
- Consumes: `XStoreCurveKeyPair`, `KeyStore`, `XStoreZapHandler` (Tasks 1-3), `ZmqServer::StartServer`'s new signature (Task 5)
- Produces: `SysStatsCollector::Start` gains the same optional pair of params, defaulted identically.

- [ ] **Step 1: Write the failing test**

Append to `tests/integration/XStoreCurveTest.cpp`:

```cpp
#include "include/XStore/XStore_SysStats.h"

TEST(SysStatsCurveTest, AllowedSubscriberReceivesAFrame) {
    ScratchDir scratch("sysstats_curve_allow_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);
    auto server_kp = XStoreCurveKeyPair::Generate();
    auto client_kp = XStoreCurveKeyPair::Generate();
    Z85WriteFile(allowed_dir / "viewer.pub", client_kp.public_key_z85, false);
    auto store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));

    int port = 27500 + static_cast<int>(getpid() % 2000);
    SysStatsCollector::Start("127.0.0.1", port, server_kp, store);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    zmq::context_t ctx(1);
    zmq::socket_t sub(ctx, zmq::socket_type::sub);
    sub.set(zmq::sockopt::curve_serverkey, server_kp.public_key_z85);
    sub.set(zmq::sockopt::curve_publickey, client_kp.public_key_z85);
    sub.set(zmq::sockopt::curve_secretkey, client_kp.secret_key_z85);
    sub.set(zmq::sockopt::subscribe, "");
    sub.set(zmq::sockopt::rcvtimeo, 3000);
    sub.connect("tcp://127.0.0.1:" + std::to_string(port));

    zmq::message_t frame;
    EXPECT_TRUE(sub.recv(frame, zmq::recv_flags::none));
}

TEST(SysStatsCurveTest, UnlistedSubscriberReceivesNothing) {
    ScratchDir scratch("sysstats_curve_deny_test");
    auto allowed_dir = scratch.test_root / "allowed";
    std::filesystem::create_directories(allowed_dir);  // empty
    auto server_kp = XStoreCurveKeyPair::Generate();
    auto stranger_kp = XStoreCurveKeyPair::Generate();
    auto store = std::make_shared<FilesystemKeyStore>(allowed_dir, std::chrono::milliseconds(0));

    int port = 29500 + static_cast<int>(getpid() % 2000);
    SysStatsCollector::Start("127.0.0.1", port, server_kp, store);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    zmq::context_t ctx(1);
    zmq::socket_t sub(ctx, zmq::socket_type::sub);
    sub.set(zmq::sockopt::curve_serverkey, server_kp.public_key_z85);
    sub.set(zmq::sockopt::curve_publickey, stranger_kp.public_key_z85);
    sub.set(zmq::sockopt::curve_secretkey, stranger_kp.secret_key_z85);
    sub.set(zmq::sockopt::subscribe, "");
    sub.set(zmq::sockopt::rcvtimeo, 1500);
    sub.connect("tcp://127.0.0.1:" + std::to_string(port));

    zmq::message_t frame;
    EXPECT_FALSE(sub.recv(frame, zmq::recv_flags::none));
}
```

Note: `SysStatsCollector::Start` has no shutdown API (same as `ZmqServer`) and is process-lifetime by design (registers its own `atexit`) — these two tests each bind a distinct port so they don't collide with each other or with a real server if run in the same process.

- [ ] **Step 2: Run to verify it fails to compile**

Run: `cmake --build build --target xstore_network_tests 2>&1 | tail -20`
Expected: FAIL — `Start` has no 4-argument overload.

- [ ] **Step 3: Modify `include/XStore/XStore_SysStats.h`**

```cpp
#ifndef XSTORE_SYSSTATS_H
#define XSTORE_SYSSTATS_H

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include "include/Networking/XStore_Curve.h"

class SysStatsCollector {
    public:
        // curve_identity/curve_keystore both absent (default): identical to
        // today's behavior. Both present: CURVE_SERVER mode + ZAP domain
        // "xstore" on the PUB socket, gated by its own XStoreZapHandler
        // (this collector runs on its own zmq::context_t, so it needs its
        // own ZAP handler instance -- see ZmqServer::StartServer for the
        // analogous wiring). Typically shares the same KeyStore instance
        // (and the same node identity) as ZmqServer -- one allowlist governs
        // both sockets.
        static void Start(const std::string &host, int port,
                           std::optional<XStoreCurveKeyPair> curve_identity = std::nullopt,
                           std::shared_ptr<KeyStore> curve_keystore = nullptr);

    private:
        static void CollectorLoop(std::string host, int port,
                                   std::optional<XStoreCurveKeyPair> curve_identity,
                                   std::shared_ptr<KeyStore> curve_keystore);

        static std::atomic<bool> stop_flag;
        static std::thread collector_thread;
};

#endif  //XSTORE_SYSSTATS_H
```

- [ ] **Step 4: Modify `src/XStore/XStore_SysStats.cpp`**

Change the `Start`/`CollectorLoop` signatures and add the CURVE/ZAP wiring right after the PUB socket is constructed:

```cpp
void SysStatsCollector::Start(const std::string &host, int port,
                               std::optional<XStoreCurveKeyPair> curve_identity,
                               std::shared_ptr<KeyStore> curve_keystore) {
    static_cast<void>(spdlog::default_logger());

    collector_thread = std::thread(&SysStatsCollector::CollectorLoop, host, port, curve_identity, curve_keystore);
    std::atexit([]() {
        SysStatsCollector::stop_flag.store(true, std::memory_order_relaxed);
        if (SysStatsCollector::collector_thread.joinable()) {
            SysStatsCollector::collector_thread.join();
        }
    });
}

void SysStatsCollector::CollectorLoop(std::string host, int port,
                                       std::optional<XStoreCurveKeyPair> curve_identity,
                                       std::shared_ptr<KeyStore> curve_keystore) {
    sched_param sch{};
    sch.sched_priority = 0;
    if (sched_setscheduler(0, SCHED_IDLE, &sch) != 0) {
        spdlog::warn("[SYSSTATS] - Failed to set SCHED_IDLE, continuing at default scheduling priority");
    }

    zmq::context_t ctx(1);
    zmq::socket_t pub_sock(ctx, zmq::socket_type::pub);
    pub_sock.set(zmq::sockopt::conflate, 1);

    std::unique_ptr<XStoreZapHandler> zap_handler;
    if (curve_identity && curve_keystore) {
        zap_handler = std::make_unique<XStoreZapHandler>(ctx, curve_keystore);
        zap_handler->Start();

        pub_sock.set(zmq::sockopt::curve_server, true);
        pub_sock.set(zmq::sockopt::curve_secretkey, curve_identity->secret_key_z85);
        pub_sock.set(zmq::sockopt::zap_domain, "xstore");
        spdlog::info("[SYSSTATS] - CURVE enabled on stats PUB socket");
    }

    std::string bind_addr = "tcp://" + host + ":" + std::to_string(port);
    try {
        pub_sock.bind(bind_addr);
    } catch (const zmq::error_t &e) {
        spdlog::error("[SYSSTATS] - Failed to bind stats PUB socket at {}: {}", bind_addr, e.what());
        return;
    }
    spdlog::info("[SYSSTATS] - Broadcasting host stats on {} every {}ms", bind_addr, XStoreTunables::SYS_STATS_COLLECT_INTERVAL_MS);

    // ... rest of the function (disk target resolution, sampling loop) is unchanged.
```

Everything below the existing `spdlog::info("[SYSSTATS] - Broadcasting...")` line stays exactly as it is today — only the function signature and the block right before that log line change.

- [ ] **Step 5: Wire `XStore_SysStats.cpp`/`.h` into `xstore_network_tests`**

In `tests/CMakeLists.txt`, add to `xstore_network_tests`'s source list:

```cmake
        ${CMAKE_SOURCE_DIR}/src/XStore/XStore_SysStats.cpp
        ${CMAKE_SOURCE_DIR}/include/XStore/XStore_SysStats.h
```

- [ ] **Step 6: Add the new CLI flags to `src/main.cpp`**

After the existing `-s/--stats-port` argument block:

```cpp
    program.add_argument("--curve")
        .default_value(false)
        .implicit_value(true)
        .help("enable ZMQ CURVE transport encryption + ZAP-based client authentication (opt-in, off by default)");

    program.add_argument("--curve-secret-key")
        .default_value(std::string("keys/server/server.key"))
        .help("path to this server's CURVE secret key file (see xstore-keys gen server); its sibling .pub file must exist alongside it");

    program.add_argument("--curve-allowed-dir")
        .default_value(std::string("keys/allowed"))
        .help("directory of allowed clients' .pub files (see xstore-keys allow); hot-reloaded, no restart needed to pick up changes");
```

After parsing (right after the existing `thread_count`/`stats_port` derivation, before `SysStatsCollector::Start`):

```cpp
    bool curve_enabled = program.get<bool>("--curve");
    std::optional<XStoreCurveKeyPair> curve_identity;
    std::shared_ptr<KeyStore> curve_keystore;
    if (curve_enabled) {
        if (!zmq_has("curve")) {
            spdlog::error("[BOOT] - --curve was requested but this build's libzmq was not compiled with CURVE support. "
                           "Rebuild with libsodium-dev installed, or reconfigure without -DXSTORE_FETCH_ZMQ=OFF pointed "
                           "at a CURVE-less system libzmq.");
            exit(1);
        }
        std::string secret_key_path = program.get<std::string>("--curve-secret-key");
        std::string allowed_dir = program.get<std::string>("--curve-allowed-dir");
        try {
            curve_identity = XStoreCurveKeyPair::LoadFromSecretKeyFile(secret_key_path);
        } catch (const std::exception &e) {
            spdlog::error("[BOOT] - Failed to load CURVE server identity from {}: {}", secret_key_path, e.what());
            exit(1);
        }
        if (!std::filesystem::exists(allowed_dir)) {
            spdlog::error("[BOOT] - --curve-allowed-dir {} does not exist. Create it (xstore-keys allow writes into "
                           "it automatically) before starting with --curve.", allowed_dir);
            exit(1);
        }
        curve_keystore = std::make_shared<FilesystemKeyStore>(
            allowed_dir, std::chrono::milliseconds(XStoreTunables::CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS));
        spdlog::info("[BOOT] - CURVE enabled: identity={}, allowed-dir={}", secret_key_path, allowed_dir);
    }
```

Update the two call sites to pass the new params:

```cpp
    SysStatsCollector::Start(host, stats_port, curve_identity, curve_keystore);

    ZmqServer x_store_zmq_server(thread_count);
    x_store_zmq_server.StartServer(host, port, curve_identity, curve_keystore);
```

Add `#include <filesystem>` and `#include "include/Networking/XStore_Curve.h"` to `main.cpp`'s includes (the latter is already transitively included via `ZMQ_Server.h`/`XStore_SysStats.h`, but include it directly since `main.cpp` names `XStoreCurveKeyPair`/`FilesystemKeyStore` explicitly).

- [ ] **Step 7: Build and run**

Run: `cmake --build build --target XStore xstore_network_tests -j$(nproc)`
Expected: clean build.

Run: `./build/tests/xstore_network_tests --gtest_filter=SysStatsCurveTest.*`
Expected: 2/2 PASS.

- [ ] **Step 8: Manual end-to-end smoke test of the real binary**

```bash
./build/tools/xstore-keys/xstore-keys gen server --out-dir /tmp/xstore_curve_smoke/keys/server
./build/tools/xstore-keys/xstore-keys gen alice --out-dir /tmp/xstore_curve_smoke/keys/clients/alice
mkdir -p /tmp/xstore_curve_smoke/keys/allowed
./build/tools/xstore-keys/xstore-keys allow alice \
  --pubkey-file /tmp/xstore_curve_smoke/keys/clients/alice/alice.pub \
  --allowed-dir /tmp/xstore_curve_smoke/keys/allowed
cd /tmp/xstore_curve_smoke && /home/ubuntu/XStore/build/XStore --curve \
  --curve-secret-key keys/server/server.key --curve-allowed-dir keys/allowed \
  -H 127.0.0.1 -p 28999 &
sleep 1
tail -5 nohup.out 2>/dev/null || true
kill %1
```

Expected: log lines `[BOOT] - CURVE enabled: ...`, `[NETWORKING] - CURVE enabled on client-facing socket`, `[SYSSTATS] - CURVE enabled on stats PUB socket`, server starts and binds without error. Confirm with `ps`/log inspection, then kill the background process — no automated assertion needed here, this is a manual sanity check that the wiring holds together in the real binary, not just the test fixtures.

- [ ] **Step 9: Confirm zero behavior change for existing default (no `--curve`) usage**

Run: `cd build && ctest --output-on-failure` (full suite)
Expected: identical pass count to the pre-Task-1 baseline — every test that never passes `--curve`/curve params must be completely unaffected.

- [ ] **Step 10: Commit**

```bash
git add include/XStore/XStore_SysStats.h src/XStore/XStore_SysStats.cpp src/main.cpp tests/CMakeLists.txt tests/integration/XStoreCurveTest.cpp
git commit -m "feat(curve): wire CURVE + ZAP into SysStatsCollector, add --curve CLI flags

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 7: C++ client integration (`XStoreClient`)

**Files:**
- Modify: `client_src/Cpp/XStore_Client.h`
- Create: `tests/integration/XStoreClientCurveLiveTest.cpp`
- Modify: `tests/CMakeLists.txt` (add the new file to `xstore_client_tests`)

**Interfaces:**
- Produces:
  ```cpp
  struct XStoreCurveClientAuth {
      std::string client_public_key_z85;
      std::string client_secret_key_z85;
      std::string server_public_key_z85;
      static XStoreCurveClientAuth LoadFromFiles(std::string_view client_secret_key_file,
                                                  std::string_view server_public_key_file);
  };
  // XStoreClient ctor and Reconnect() each gain:
  //   std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt
  ```

- [ ] **Step 1: Write the failing (opt-in) live test**

Create `tests/integration/XStoreClientCurveLiveTest.cpp`, mirroring the opt-in convention of the existing `XStoreClientLiveTest.cpp` (env-var driven, `GTEST_SKIP()` by default):

```cpp
// Opt-in: exercises XStoreClient's CURVE constructor against a real,
// --curve-enabled XStore server. Skipped by default (same convention as
// XStoreClientLiveTest.cpp) since it needs a live server plus a client
// identity that server has actually allowed -- set these env vars to run it:
//   XSTORE_CURVE_LIVE_ADDR, XSTORE_CURVE_LIVE_PORT
//   XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY   (path to an allowed client's .key)
//   XSTORE_CURVE_LIVE_SERVER_PUBKEY       (path to that server's .pub)
#include <gtest/gtest.h>
#include <cstdlib>
#include "XStore_Client.h"

namespace {
const char *EnvOrSkip(const char *name) {
    const char *v = std::getenv(name);
    if (!v || std::string(v).empty()) {
        GTEST_SKIP() << name << " not set -- skipping live CURVE client test";
    }
    return v;
}
}  // namespace

TEST(XStoreClientCurveLiveTest, ConnectsAndPingsWithCurveAuth) {
    const char *addr = EnvOrSkip("XSTORE_CURVE_LIVE_ADDR");
    const char *port = EnvOrSkip("XSTORE_CURVE_LIVE_PORT");
    const char *client_key = EnvOrSkip("XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY");
    const char *server_pub = EnvOrSkip("XSTORE_CURVE_LIVE_SERVER_PUBKEY");

    auto auth = XStoreCurveClientAuth::LoadFromFiles(client_key, server_pub);
    XStoreClient client(addr, port, /*timeout_ms=*/3000, /*warmup_on_init=*/false, auth);
    EXPECT_NO_THROW(client.Ping());
}
```

`Ping()` is confirmed as a public bare-round-trip method at `client_src/Cpp/XStore_Client.h:187`.

- [ ] **Step 2: Run to verify it fails to compile**

Run: `cmake --build build --target xstore_client_tests 2>&1 | tail -20`
Expected: FAIL — `XStoreCurveClientAuth` undeclared.

- [ ] **Step 3: Add `XStoreCurveClientAuth` and the optional ctor/Reconnect params to `client_src/Cpp/XStore_Client.h`**

Add near the top of the file, before `class XStoreClient` (this repo's "client_src keeps its own copy" convention — no include of the server's `include/Networking/XStore_Curve.h`, since this file must stay buildable standalone if `client_src/Cpp` is copied out on its own):

```cpp
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>

// Client-side CURVE identity: this client's own keypair plus the server's
// public key (which the client must already know out-of-band -- see the
// design doc). Deliberately duplicates the tiny amount of Z85-file-reading
// logic XStore_Curve.h has server-side rather than including that header,
// so this file keeps compiling standalone if client_src/Cpp is copied out
// on its own without the rest of the repo.
struct XStoreCurveClientAuth {
    std::string client_public_key_z85;
    std::string client_secret_key_z85;
    std::string server_public_key_z85;

    // client_secret_key_file's sibling ".pub" (same directory/stem) supplies
    // the client's own public key -- the same xstore-keys gen convention
    // used server-side.
    static XStoreCurveClientAuth LoadFromFiles(std::string_view client_secret_key_file,
                                                std::string_view server_public_key_file) {
        auto read_z85 = [](const std::filesystem::path &p) {
            std::ifstream f(p);
            if (!f.is_open()) {
                throw std::runtime_error("XStoreCurveClientAuth: cannot open " + p.string());
            }
            std::string key;
            f >> key;
            if (key.size() != 40) {
                throw std::runtime_error("XStoreCurveClientAuth: " + p.string() +
                                          " does not contain a 40-character Z85 key");
            }
            return key;
        };
        std::filesystem::path secret_path(client_secret_key_file);
        std::filesystem::path public_path = secret_path;
        public_path.replace_extension(".pub");

        XStoreCurveClientAuth auth;
        auth.client_secret_key_z85 = read_z85(secret_path);
        auth.client_public_key_z85 = read_z85(public_path);
        auth.server_public_key_z85 = read_z85(std::filesystem::path(server_public_key_file));
        return auth;
    }
};
```

Modify the constructor (line 138):

```cpp
  XStoreClient(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
               bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      if (curve_auth) {
          sock.set(zmq::sockopt::curve_serverkey, curve_auth->server_public_key_z85);
          sock.set(zmq::sockopt::curve_publickey, curve_auth->client_public_key_z85);
          sock.set(zmq::sockopt::curve_secretkey, curve_auth->client_secret_key_z85);
      }
      std::string full_addr = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(full_addr);
      if (warmup_on_init) {
          Warmup();
      }
  }
```

Modify `Reconnect` (line 164) identically:

```cpp
  void Reconnect(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
                 bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      sock = zmq::socket_t(ctx, zmq::socket_type::req);
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      if (curve_auth) {
          sock.set(zmq::sockopt::curve_serverkey, curve_auth->server_public_key_z85);
          sock.set(zmq::sockopt::curve_publickey, curve_auth->client_public_key_z85);
          sock.set(zmq::sockopt::curve_secretkey, curve_auth->client_secret_key_z85);
      }
      std::string full_addr = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(full_addr);
      if (warmup_on_init) {
          Warmup();
      }
  }
```

- [ ] **Step 4: Wire the new test file into `tests/CMakeLists.txt`**

In the `xstore_client_tests` block:

```cmake
    add_executable(xstore_client_tests
            integration/XStoreClientTest.cpp
            integration/XStoreClientLiveTest.cpp
            integration/XStoreClientCurveLiveTest.cpp
    )
```

- [ ] **Step 5: Build**

Run: `cmake --build build --target xstore_client_tests -j$(nproc)`
Expected: builds clean (the new test itself will `GTEST_SKIP()` without the env vars set — that's expected, this task's build succeeding plus a manual live run below is the actual verification).

- [ ] **Step 6: Manually verify against a real server**

```bash
# Terminal 1 (reuse the /tmp/xstore_curve_smoke setup from Task 6 Step 8, or redo it):
cd /tmp/xstore_curve_smoke && /home/ubuntu/XStore/build/XStore --curve \
  --curve-secret-key keys/server/server.key --curve-allowed-dir keys/allowed \
  -H 127.0.0.1 -p 28999

# Terminal 2:
XSTORE_CURVE_LIVE_ADDR=127.0.0.1 XSTORE_CURVE_LIVE_PORT=28999 \
XSTORE_CURVE_LIVE_CLIENT_SECRET_KEY=/tmp/xstore_curve_smoke/keys/clients/alice/alice.key \
XSTORE_CURVE_LIVE_SERVER_PUBKEY=/tmp/xstore_curve_smoke/keys/server/server.pub \
./build/tests/xstore_client_tests --gtest_filter=XStoreClientCurveLiveTest.*
```

Expected: 1/1 PASS (not skipped).

- [ ] **Step 7: Confirm existing client tests unaffected**

Run: `./build/tests/xstore_client_tests --gtest_filter=-XStoreClientCurveLiveTest.*`
Expected: identical results to before this task (the pre-existing `XStoreClientWarmupTest`/`XStoreClientLiveTest` suites untouched).

- [ ] **Step 8: Commit**

```bash
git add client_src/Cpp/XStore_Client.h tests/integration/XStoreClientCurveLiveTest.cpp tests/CMakeLists.txt
git commit -m "feat(curve): add optional CURVE auth to the C++ client (XStoreClient)

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 8: Python client integration (`XStore.py`)

**Files:**
- Modify: `client_src/Python/XStore.py`

**Interfaces:**
- Produces: `XStoreClient.__init__` gains `curve_client_secret_key_file=None, curve_server_public_key_file=None` kwargs (all-or-none).

- [ ] **Step 1: Modify `client_src/Python/XStore.py`**

```python
import os
import zmq
from zmq.utils import z85


class XStoreClient:
    def __init__(self, SERVER_ADDR, SERVER_PORT, verbose=False,
                 curve_client_secret_key_file=None, curve_server_public_key_file=None):
        self.verbose = verbose
        self.zmqContext = zmq.Context()
        self.socket = self.zmqContext.socket(zmq.REQ)

        have_client_key = curve_client_secret_key_file is not None
        have_server_key = curve_server_public_key_file is not None
        if have_client_key != have_server_key:
            raise ValueError(
                "XStoreClient: curve_client_secret_key_file and curve_server_public_key_file "
                "must be given together, or not at all")
        if have_client_key:
            client_secret_z85 = self._read_z85_file(curve_client_secret_key_file)
            client_public_path = os.path.splitext(curve_client_secret_key_file)[0] + ".pub"
            client_public_z85 = self._read_z85_file(client_public_path)
            server_public_z85 = self._read_z85_file(curve_server_public_key_file)

            # pyzmq's curve_* socket attributes take raw 32-byte keys, unlike
            # cppzmq's sockopt wrappers which accept Z85 text directly -- decode
            # here since this on-disk format is always 40-char Z85 (see the
            # design doc's Global Constraints).
            self.socket.curve_secretkey = z85.decode(client_secret_z85.encode("ascii"))
            self.socket.curve_publickey = z85.decode(client_public_z85.encode("ascii"))
            self.socket.curve_serverkey = z85.decode(server_public_z85.encode("ascii"))

        self.socket.connect(f'tcp://{SERVER_ADDR}:{SERVER_PORT}')

    @staticmethod
    def _read_z85_file(path):
        with open(path, "r") as f:
            key = f.read().strip()
        if len(key) != 40:
            raise ValueError(f"XStoreClient: {path} does not contain a 40-character Z85 key")
        return key
```

Leave every other method (`send`/`recv`/RPC wrappers) exactly as-is — only `__init__` changes.

- [ ] **Step 2: Manually verify against a real server**

```bash
python3 - <<'EOF'
import sys
sys.path.insert(0, "client_src/Python")
from XStore import XStoreClient

client = XStoreClient(
    "127.0.0.1", "28999",
    curve_client_secret_key_file="/tmp/xstore_curve_smoke/keys/clients/alice/alice.key",
    curve_server_public_key_file="/tmp/xstore_curve_smoke/keys/server/server.pub",
)
print("connected OK")
EOF
```

(Run this with the same `--curve`-enabled server from Task 6/7's manual smoke test still running.) Expected: prints `connected OK` with no exception. This repo has no existing Python test suite for `XStore.py` (confirmed: only `client_src/Python/XStore.py` and its generated `_pb2.py` exist, no test files) — this manual script is the verification for this task, consistent with there being no established automated harness to extend.

- [ ] **Step 3: Verify the all-or-none validation**

```bash
python3 -c "
import sys; sys.path.insert(0, 'client_src/Python')
from XStore import XStoreClient
try:
    XStoreClient('127.0.0.1', '28999', curve_client_secret_key_file='/tmp/x.key')
    print('FAIL: should have raised')
except ValueError:
    print('OK: raised ValueError as expected')
"
```

Expected: `OK: raised ValueError as expected`.

- [ ] **Step 4: Commit**

```bash
git add client_src/Python/XStore.py
git commit -m "feat(curve): add optional CURVE auth to the Python client

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```

---

### Task 9: Docs, TODO.md, and full backward-compatibility verification

**Files:**
- Modify: `TODO.md`
- Modify: `docs/XStore_Technical_Reference.md` (§7 Networking Layer)

**Interfaces:** none (documentation + final verification only)

- [ ] **Step 1: Update `TODO.md`**

Change the existing High Effort bullet (currently: `- [ ] **ZMQ transport auth/encryption (CURVE)** — only if XStore ever runs on a shared/untrusted network segment...`) to a `[x]` entry summarizing what shipped, in the same style as other completed items in this file (see e.g. the CRC32C entry for the level of detail expected): what was built (`XStore_Curve` library, `FilesystemKeyStore` allowlist, `xstore-keys` CLI, ZAP wiring on both `ZmqServer` and `SysStatsCollector`), that it's fully opt-in (`--curve`, off by default), the commit range, and the explicitly-deferred items from the spec (universal config file, mid-session forced disconnect on revoke, real `EtcdKeyStore`).

- [ ] **Step 2: Update `docs/XStore_Technical_Reference.md` §7 (Networking Layer)**

Add a short subsection (matching the existing §4.12/§4.13 WAL-subsection style) describing: CURVE is opt-in via `--curve`, backed by `FilesystemKeyStore` + ZAP, managed via `xstore-keys`, covers both the RPC ROUTER socket and the SysStats PUB socket, and point to the design spec (`docs/superpowers/specs/2026-09-09-zmq-curve-auth-design.md`) for the full rationale.

- [ ] **Step 3: Run the complete existing test suite unmodified**

Run: `cd build && ctest --output-on-failure`
Expected: every suite passes, including the new `XStoreCurveTest`/`XStoreKeysCliTest` cases added by this plan, with the same pass count for every pre-existing suite as the pre-Task-1 baseline (opt-in boundary holds — nothing that doesn't pass `--curve`/curve params anywhere behaves differently).

- [ ] **Step 4: Commit**

```bash
git add TODO.md docs/XStore_Technical_Reference.md
git commit -m "docs(curve): mark CURVE auth done in TODO.md, document in technical reference

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>"
```
