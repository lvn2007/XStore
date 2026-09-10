#include "include/Networking/XStore_Curve.h"

#include <zmq.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>

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
