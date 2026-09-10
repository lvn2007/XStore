#include "include/Networking/XStore_Curve.h"

#include <zmq.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>
#include <zmq_addon.hpp>

#include "include/Logger/Logger.h"
#include "include/XStore/XStore_Tunables.h"

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
    mode_t mode = is_secret ? 0600 : 0644;
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd < 0) {
        throw std::runtime_error("Z85WriteFile: cannot open " + path.string() + " for writing: " + std::strerror(errno));
    }
    ssize_t written = ::write(fd, z85.data(), z85.size());
    int close_result = ::close(fd);
    if (written != static_cast<ssize_t>(z85.size()) || close_result != 0) {
        throw std::runtime_error("Z85WriteFile: failed writing to " + path.string());
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
    // forces MaybeRescan to treat it as due
    last_check_.store(std::chrono::steady_clock::time_point{}, std::memory_order_relaxed);
    MaybeRescan();
}

void FilesystemKeyStore::MaybeRescan() const {
    auto now = std::chrono::steady_clock::now();
    auto prev = last_check_.load(std::memory_order_relaxed);
    if (now - prev < min_check_interval_) return;
    last_check_.store(now, std::memory_order_relaxed);

    std::error_code ec;
    auto it = std::filesystem::directory_iterator(allowed_dir_, ec);
    if (ec) {
        spdlog::warn("[CURVE] - Cannot list allowed-dir {}: {}", allowed_dir_.string(), ec.message());
        return;  // keep the previous good snapshot
    }

    // Manual iteration via increment(ec) rather than a range-based for: the
    // latter calls directory_iterator::operator++(), which throws
    // filesystem_error on a mid-scan failure (dir removed/unmounted/perm
    // change). This runs on every IsAllowed() call including ZAP auth
    // requests, so an uncaught exception here must not be possible.
    auto fresh = std::make_shared<SnapshotMap>();
    for (; it != std::filesystem::directory_iterator(); it.increment(ec)) {
        if (ec) {
            spdlog::warn("[CURVE] - Error while scanning allowed-dir {}: {}", allowed_dir_.string(), ec.message());
            return;  // keep the previous good snapshot rather than replacing it with a partial one
        }
        const auto &entry = *it;
        if (entry.path().extension() != ".pub") continue;
        try {
            auto z85 = Z85ReadFile(entry.path());
            (*fresh)[Z85Decode(z85)] = entry.path().stem().string();
        } catch (const std::exception &e) {
            spdlog::warn("[CURVE] - Skipping malformed allowlist entry {}: {}", entry.path().string(), e.what());
            continue;
        }
    }
    std::atomic_store(&snapshot_, std::shared_ptr<const SnapshotMap>(fresh));
}

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
        if (request.size() < 6) {
            // Pathological -- should never happen from a real libzmq ZAP client, but
            // this REP socket's recv/send state machine must stay in sync regardless,
            // or the NEXT (unrelated) ZAP request throws EFSM and crashes the process.
            spdlog::warn("[CURVE] - Malformed ZAP request ({} frames), denying with best-effort reply",
                         request.size());
            zmq::multipart_t reply;
            reply.addstr(request.size() > 0 ? request[0].to_string() : "1.0");
            reply.addstr(request.size() > 1 ? request[1].to_string() : "");
            reply.addstr("500");
            reply.addstr("Malformed request");
            reply.addstr("");
            reply.addstr("");
            reply.send(zap);
            continue;
        }

        std::string version = request[0].to_string();
        std::string sequence = request[1].to_string();
        std::string mechanism = request[5].to_string();

        std::string status_code = "400";
        std::string status_text = "Denied";
        std::string user_id;

        if (mechanism == "CURVE" && request.size() >= 7 && request[6].size() == 32) {
            std::array<uint8_t, 32> pubkey_raw{};
            std::memcpy(pubkey_raw.data(), request[6].data(), 32);
            std::string name;
            if (store_->IsAllowed(pubkey_raw, &name)) {
                status_code = "200";
                status_text = "OK";
                user_id = name;
                spdlog::info("[CURVE] - ZAP allowed '{}'", name);
            } else {
                std::string fingerprint = Z85Encode(pubkey_raw);
                spdlog::warn("[CURVE] - ZAP denied unknown CURVE key (fingerprint {}...{})",
                             fingerprint.substr(0, 6), fingerprint.substr(34));
            }
        } else {
            spdlog::warn("[CURVE] - ZAP request denied (mechanism={}, frames={})", mechanism, request.size());
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
