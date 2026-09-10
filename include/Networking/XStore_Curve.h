#ifndef XSTORE_CURVE_H
#define XSTORE_CURVE_H

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <zmq.hpp>

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

    // Re-scans allowed_dir_ if last_check_ is due; otherwise a cheap no-op
    // (a duration comparison, no filesystem access at all). Called from both
    // IsAllowed() (const) and Refresh() (non-const, after resetting
    // last_check_ to force an immediate scan) -- hence all touched state is
    // mutable, the standard const-correctness idiom for an internally-cached
    // const method.
    //
    // Earlier revisions additionally skipped the directory listing when
    // std::filesystem::last_write_time(allowed_dir_) hadn't advanced since
    // the last scan. That mtime check was dropped: on this host, creating
    // and then removing a file in quick succession can produce identical
    // directory mtimes (a kernel timestamp-granularity artifact, reproduced
    // with both std::filesystem::last_write_time and raw POSIX stat()), so
    // a revocation could be silently missed. Rescanning unconditionally
    // once per debounce interval is cheap enough (a directory listing over
    // the dozens/hundreds of identities this is sized for) and removes the
    // false-negative risk entirely.
    void MaybeRescan() const;

    std::filesystem::path allowed_dir_;
    std::chrono::milliseconds min_check_interval_;
    // atomic: IsAllowed() is called concurrently from independent ZAP
    // handler threads (XStoreZapHandler instances for ZmqServer and
    // SysStatsCollector can share one FilesystemKeyStore) -- plain
    // read-compare-then-write here would be a data race. Relaxed ordering
    // is enough: a benign double-rescan if two threads race past the
    // debounce check together is harmless and already tolerated by design.
    mutable std::atomic<std::chrono::steady_clock::time_point> last_check_{};
    mutable std::shared_ptr<const SnapshotMap> snapshot_;
};

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

#endif  // XSTORE_CURVE_H
