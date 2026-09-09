# ZMQ CURVE Auth + Key Housekeeping — Design

Status: approved for planning
Date: 2026-09-09
Origin: TODO.md "ZMQ transport auth/encryption (CURVE)" (High Effort section)

## 1. Context

XStore's ZMQ transport (`ZmqServer` — ROUTER/DEALER/REP broker) and the
independent `SysStatsCollector` PUB socket currently run with no
transport security (ZMTP `NULL` mechanism): any party that can reach the
listening port can send requests or subscribe to host telemetry. This
is acceptable only as long as XStore stays on a fully trusted network
segment.

Investigated and rejected as a dependency target: the standalone
`zeromq/libcurve` repo (last commit ~9 years ago). It's superseded —
CURVE support has lived inside `libzmq` core since 4.x, backed by
`libsodium` (already an unused CI apt dependency, per TODO.md's
compression-work section) or libzmq's bundled tweetnacl fallback. No
new external crypto library needed beyond wiring up `libsodium`
properly.

## 2. Goals

- Encrypt + mutually authenticate the ZMQ transport (both the RPC
  ROUTER socket and the SysStats PUB socket) using CURVE.
- Real access control: only client public keys explicitly present on
  an allowlist may connect — not just "any party with a keypair."
- Elegant, low-friction key housekeeping for a growing/dynamic set of
  client identities: generate, distribute, authorize, and revoke
  without confusion or hand-editing a shared file under contention.
- Revocation takes effect on a running server without a restart.
- Fully opt-in and backward compatible: omitting the new flags leaves
  every existing client/tool/test/benchmark harness (XBench
  `Data_Manager`, `xstore-viewer`, `xstore_tests`,
  `xstore_network_tests`) working completely unmodified.
- Lay a seam (not a full implementation) for the future
  etcd-backed distributed design (`docs/XStore_Distributed_Architecture.md`)
  to plug in its own key/allowlist backend later without redesigning
  this scheme.

## 3. Explicit non-goals (deferred, not forgotten)

- **Universal config file.** XStore is CLI-flags-only today
  (`argparse`, no config file mechanism anywhere). This feature adds
  3-4 new flags on top of the existing 4, which is a real signal the
  CLI-only pattern is getting crowded — but designing a universal
  config file (format, CLI-flag precedence, hot-reload story,
  migrating existing `-H/-p/-t/-s`) is its own architectural project.
  Deferred to a future dedicated brainstorm; this feature stays
  CLI-flags-only like everything else in XStore today.
- **Mid-session forced disconnect on revocation.** Revoking a client
  stops its *next* new connection attempt. An already-established
  session is not force-killed. Same limitation SSH key revocation has
  (existing sessions keep working until they naturally reconnect).
  Acceptable for now; an active-disconnect mechanism (e.g. server
  tracking live peer identities and issuing `zmq_disconnect`) is a
  separate future enhancement if it's ever actually needed.
- **Real `EtcdKeyStore` implementation.** The distributed architecture
  is unstarted (long Phase 0 prerequisite list, this isn't on it). This
  design only introduces the `KeyStore` interface seam; the concrete
  etcd-backed implementation is future distributed-mode work.
- **CURVE for `xstore_network_tests`' internal transport assumptions.**
  Existing tests keep running unauthenticated (`--curve` omitted) —
  a curve-enabled test path is added net-new (§8), not retrofitted
  onto every existing network test.

## 4. Architecture

Four new pieces:

1. **`XStore_Curve` library** —
   `include/Networking/XStore_Curve.h` /
   `src/XStore_Networking/XStore_Curve.cpp`. Shared core: keypair
   generation/load/save, Z85 encoding, the `KeyStore` interface and its
   `FilesystemKeyStore` implementation, and `XStoreZapHandler`. Used by
   the server (`main.cpp`, `ZmqServer`, `SysStatsCollector`) and by the
   new `xstore-keys` CLI tool.
2. **`xstore-keys` CLI** — `tools/xstore-keys/` (sibling to
   `tools/xstore-viewer/`). Subcommands: `gen`, `allow`, `revoke`,
   `list`. Talks only to the filesystem — never to a running server
   (no IPC/RPC between them, by design: it must work with the server
   down, and a client's `gen` typically runs on a different machine
   than the server's `allow`).
3. **`XStoreZapHandler`** — reusable ZAP (RFC 27) responder thread,
   bindable to any `zmq::context_t`. One instance per context (the
   ZAP inproc endpoint `inproc://zeromq.zap.01` is per-context, and
   `ZmqServer` and `SysStatsCollector` deliberately run on separate
   contexts today).
4. **`KeyStore` interface + `FilesystemKeyStore`** — the allowlist
   abstraction. `FilesystemKeyStore` is backed by one Z85-encoded
   `.pub` file per identity under a directory (Approach A, chosen over
   a single-file allowlist specifically because per-identity files are
   atomic per operation — no locking needed between two concurrent
   `xstore-keys allow`/`revoke` calls for different identities — and
   because a per-identity key maps 1:1 onto etcd's natural
   prefix-key/watch idiom later, unlike a single serialized blob key
   which would need read-modify-write + compare-and-swap).

## 5. Components

```cpp
// include/Networking/XStore_Curve.h

struct XStoreCurveKeyPair {
    std::array<uint8_t, 32> public_key;
    std::array<uint8_t, 32> secret_key;

    static XStoreCurveKeyPair Generate();  // wraps zmq_curve_keypair()
    static XStoreCurveKeyPair LoadFromFiles(
        const std::filesystem::path &public_key_file,
        const std::filesystem::path &secret_key_file);  // Z85-decoded
    void SaveToFiles(
        const std::filesystem::path &public_key_file,
        const std::filesystem::path &secret_key_file) const;  // secret written mode 0600
};

// Z85-encode/decode helpers wrapping zmq_z85_encode/zmq_z85_decode --
// used for both on-disk files and any in-memory pubkey comparisons
// (ZAP credentials arrive as raw 32 bytes; allowlist files store Z85 text).
std::string Z85Encode(const std::array<uint8_t, 32> &raw);
std::array<uint8_t, 32> Z85Decode(const std::string &z85);

class KeyStore {
public:
    virtual ~KeyStore() = default;
    // pubkey_raw: the 32 raw bytes ZAP hands us for the connecting peer.
    // Returns true and fills *name_out if that key is currently allowed.
    virtual bool IsAllowed(const std::array<uint8_t, 32> &pubkey_raw,
                           std::string *name_out) const = 0;
    // Re-scans/re-fetches and atomically swaps the live snapshot if the
    // backing store looks like it changed since the last call. Called
    // by IsAllowed() itself (see FilesystemKeyStore below) -- KeyStore
    // implementations do not spawn their own threads.
    virtual void Refresh() = 0;
};

class FilesystemKeyStore : public KeyStore {
public:
    explicit FilesystemKeyStore(std::filesystem::path allowed_dir,
                                 std::chrono::milliseconds min_check_interval);
    // Calls Refresh() first (see below), then resolves against the
    // current snapshot.
    bool IsAllowed(const std::array<uint8_t, 32> &pubkey_raw,
                   std::string *name_out) const override;
    // Cheap stat() on allowed_dir_; if its mtime advanced since the last
    // check AND at least min_check_interval_ has elapsed since the last
    // check (debounce -- avoids a stat() storm under a connection burst),
    // re-scans all *.pub files and atomically swaps the snapshot map.
    // No-op otherwise -- most calls cost one stat().
    void Refresh() override;

private:
    std::filesystem::path allowed_dir_;
    std::chrono::milliseconds min_check_interval_;
    // shared_ptr<const unordered_map<array<uint8_t,32>, string>>, swapped
    // via atomic store/load -- concurrent IsAllowed() calls never block
    // on a Refresh() rebuild in progress.
};

class XStoreZapHandler {
public:
    XStoreZapHandler(zmq::context_t &ctx, std::shared_ptr<KeyStore> store);
    void Start();  // binds inproc://zeromq.zap.01 on ctx, spawns the ZAP loop thread
    void Stop();   // signals stop, joins -- same atexit-before-static-teardown
                   // convention as StartWalWriters/SysStatsCollector (XStore_Wal.cpp)
    ~XStoreZapHandler();
};
```

The ZAP loop thread does a normal blocking `recv()` on the ZAP socket
(matches every other worker loop in this codebase — no independent
timer wakeup needed). `Refresh()`'s mtime check runs synchronously,
inline, right before each request is evaluated — so a revoke only
needs to be visible by the time the *next* connection attempt reaches
ZAP, which is exactly what §7 already specifies. `min_check_interval`
is the new tunable
(`XStoreTunables::CURVE_ALLOWLIST_MIN_CHECK_INTERVAL_MS`, default
1000) — a debounce on the `stat()` call under a connection burst, not
a background poll period.

## 6. `xstore-keys` CLI

```
xstore-keys gen <name> [--out-dir <dir>]
    Generates a Curve25519 keypair, writes <out-dir>/<name>.pub and
    <out-dir>/<name>.key (mode 0600). Default out-dir: ./keys/server/
    if <name> == "server", else ./keys/clients/<name>/.

xstore-keys allow <name> --pubkey-file <path> [--allowed-dir <dir>]
    Validates <path> decodes to a 32-byte key, copies it to
    <allowed-dir>/<name>.pub. Default allowed-dir: ./keys/allowed/.
    Refuses to overwrite an existing <name>.pub without --force (avoids
    silently re-keying an identity by accident).

xstore-keys revoke <name> [--allowed-dir <dir>]
    Deletes <allowed-dir>/<name>.pub. No-op (warns, exit 0) if absent.

xstore-keys list [--allowed-dir <dir>]
    Prints a table: name, pubkey (Z85), and a short fingerprint
    (first/last 6 chars) for eyeballing without dumping full keys to a
    terminal that might be logged/screen-shared.
```

Default directory convention (overridable everywhere via explicit
flags): `./keys/server/`, `./keys/clients/<name>/`, `./keys/allowed/`.
`main.cpp` defaults `--curve-secret-key` to `keys/server/server.key`
and `--curve-allowed-dir` to `keys/allowed/` when `--curve` is passed
without those flags — so a single-machine dev/test setup is
`xstore-keys gen server && ./build/XStore --curve -H <host> -p <port>`
with no path-juggling, while a real multi-machine deploy passes
explicit paths (a client's `gen` output never lives next to the
server's directory in that case anyway).

## 7. Server integration

`main.cpp` new flags: `--curve` (bool, default off), `--curve-secret-key
<path>`, `--curve-allowed-dir <path>` (both default per §6 when
`--curve` is set and they're omitted).

At boot, when `--curve` is set:

1. `zmq_has("curve")` checked first — fail fast with an actionable
   message ("libzmq was not built with CURVE support — install
   libsodium-dev and rebuild, or reconfigure with
   -DXSTORE_FETCH_ZMQ=ON") before any thread spawns.
2. Load the server's own `XStoreCurveKeyPair` via `LoadFromFiles` —
   fail fast on missing/malformed files (an empty/broken identity that
   "starts anyway" is worse than refusing to boot).
3. Construct one `std::shared_ptr<FilesystemKeyStore>` from
   `--curve-allowed-dir` — fail fast if the directory doesn't exist or
   isn't readable.
4. Pass the keypair + KeyStore to both `ZmqServer::StartServer` and
   `SysStatsCollector::Start`.

`ZmqServer::StartServer`: when curve-enabled, sets
`ZMQ_CURVE_SERVER=1` + the server secret key on `client_socket` only
(the external-facing ROUTER socket — the DEALER/REP legs behind it are
`inproc://`, which has no ZMTP handshake and no wire security concept;
CURVE is a TCP-transport mechanism, so applying it there would be a
no-op at best). Spawns one `XStoreZapHandler` bound to `ctx`, started
*before* `client_socket.bind()` so no handshake can race the ZAP
handler's own bind.

`SysStatsCollector::Start`: same keypair, same `KeyStore` instance,
its own `XStoreZapHandler` on its own `ctx` (per its header comment,
already deliberately decoupled from `ZmqServer`) — CURVE'd on its PUB
socket. One server identity, one allowlist, governs both sockets.

When `--curve` is omitted (default): zero behavior change from today
— sockets stay on the `NULL` mechanism.

## 8. Client integration

C++ (`client_src/Cpp/XStore_Client.h`): `XStoreClient`'s constructor
and `Reconnect()` gain an optional parameter:

```cpp
struct XStoreCurveClientAuth {
    XStoreCurveKeyPair client_keys;
    std::array<uint8_t, 32> server_public_key;

    static XStoreCurveClientAuth LoadFromFiles(
        const std::filesystem::path &client_public_key_file,
        const std::filesystem::path &client_secret_key_file,
        const std::filesystem::path &server_public_key_file);
};

XStoreClient(std::string_view server_addr, std::string_view server_port,
             int timeout_ms = -1, bool warmup_on_init = true,
             std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt);
```

When present, sets `ZMQ_CURVE_SERVERKEY`/`ZMQ_CURVE_PUBLICKEY`/
`ZMQ_CURVE_SECRETKEY` on `sock` before `connect()`. Omitted (default):
today's behavior, unchanged.

Python (`client_src/Python/XStore.py`): `XStoreClient.__init__` gains
optional `curve_client_public_key_file`, `curve_client_secret_key_file`,
`curve_server_public_key_file` kwargs (all-or-none — partial curve
config is a startup `ValueError`, not a silent partial-security state).
Sets `socket.curve_publickey` / `curve_secretkey` / `curve_serverkey`
via pyzmq (ships with libsodium already, no new Python dependency).

## 9. Error handling summary

| Condition | Behavior |
|---|---|
| `--curve` set, libzmq lacks CURVE support | Fail fast at boot, before thread spawn |
| `--curve` set, server keypair missing/malformed | Fail fast at boot |
| `--curve` set, allowed-dir missing/unreadable | Fail fast at boot |
| One malformed `.pub` file during a `Refresh()` scan | Warn-log, skip that file, keep previous good entries for everything else |
| Client uses wrong/no server pubkey | Handshake never completes (ZMTP design) — surfaces as the client's existing REQ-timeout path, no new client-side error handling needed |
| ZAP denies an unlisted pubkey | Same silent-timeout on the client; server warn-logs the rejected key's fingerprint |
| Revoked client already mid-session | Not disconnected (§3 non-goal) — denied on its next new connection |

## 10. Testing

New `tests/integration/XStoreCurveTest.cpp`:
- Keypair gen + Z85 encode/decode round-trip.
- `FilesystemKeyStore`: allow → `IsAllowed` true; revoke → `IsAllowed`
  false after a `Refresh()`; malformed file in the directory doesn't
  break resolution of the other valid entries.
- End-to-end with a real `--curve`-enabled server: an allowed client
  connects and gets a normal reply; an unlisted client's connection
  times out; revoke mid-run, previously-allowed client's *next*
  connection attempt is denied.

`xstore-keys` gets its own small test target: `gen` produces two files
decoding to valid 32-byte keys that round-trip through Z85; `allow`/
`revoke` are idempotent (repeat calls don't error or corrupt state).

Full existing suite (`ctest`, direct `xstore_tests` binary) must pass
unmodified with `--curve` never passed anywhere — confirms the opt-in
boundary holds. No existing test, XBench `Data_Manager`, or
`xstore-viewer` code needs to change for this feature to ship.

## 11. Build/dependency changes

- `libsodium`: `find_package` (or pkg-config fallback) added to root
  `CMakeLists.txt`, linked into the `XStore` target. Already present
  as an unused CI apt dependency — no new install step in CI.
  `client_src/Cpp/CMakeLists.txt` links it into `XStoreClient` too.
- New CMake target `xstore-keys` under `tools/xstore-keys/`.
- No change to `proto/XStore.proto` — CURVE is a transport-layer
  concern, entirely below the RPC wire format.
