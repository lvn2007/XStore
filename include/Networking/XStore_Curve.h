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
