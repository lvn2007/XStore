#include "include/XStore/XStore_DirLock.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <mutex>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace dirlock {
namespace {

struct Held {
    int fd;  // -1: the lock file could not be opened (warned once, nothing to hold)
    std::filesystem::path lock_path;
};

// Leaked on purpose: the fds are left to the kernel at exit.
std::mutex &HeldMutex() {
    static std::mutex &m = *new std::mutex();
    return m;
}
std::map<std::filesystem::path, Held> &HeldMap() {
    static auto &m = *new std::map<std::filesystem::path, Held>();
    return m;
}

// Tests build hundreds of scratch directories in one process; without this every
// one of them would keep an fd for the life of the process.
void SweepDeleted(std::map<std::filesystem::path, Held> &held) {
    for (auto it = held.begin(); it != held.end();) {
        struct stat st {};
        std::error_code ec;
        const bool lock_unlinked = it->second.fd >= 0 && fstat(it->second.fd, &st) == 0 && st.st_nlink == 0;
        if (lock_unlinked || !std::filesystem::exists(it->first, ec)) {
            if (it->second.fd >= 0) close(it->second.fd);
            it = held.erase(it);
        } else {
            ++it;
        }
    }
}

std::filesystem::path CanonicalDir(const std::filesystem::path &dir) {
    std::error_code ec;
    std::filesystem::path canonical = std::filesystem::weakly_canonical(dir, ec);
    if (ec) canonical = std::filesystem::absolute(dir);
    // Drop a trailing '/' ("DataStore/" has an empty last element).
    if (!canonical.has_filename()) canonical = canonical.parent_path();
    return canonical;
}

std::string OwnerPidSuffix(int fd) {
    char buf[32] = {};
    const ssize_t n = pread(fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) return {};
    char *end = nullptr;
    const long pid = std::strtol(buf, &end, 10);
    if (end == buf || pid <= 0) return {};
    return " (PID " + std::to_string(pid) + ")";
}

}  // namespace

std::filesystem::path LockPathFor(const std::filesystem::path &data_store_dir) {
    return CanonicalDir(data_store_dir).string() + ".lock";
}

Result Acquire(const std::filesystem::path &data_store_dir) {
    const std::filesystem::path key = CanonicalDir(data_store_dir);
    const std::filesystem::path lock_path = key.string() + ".lock";

    std::lock_guard<std::mutex> lock(HeldMutex());
    auto &held = HeldMap();
    if (held.count(key) != 0) return {};
    SweepDeleted(held);

    Result result;
    // O_NOFOLLOW: a symlink planted at the lock path must not redirect the ftruncate/pwrite below.
    int fd = open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0644);
    int open_errno = errno;
    bool writable = fd >= 0;
    // A lock file left by another user (a sudo run) or on a read-only mount cannot be opened read-write,
    // but flock() works on a read-only descriptor, so the rule must not silently switch off.
    if (fd < 0 && (open_errno == EACCES || open_errno == EPERM || open_errno == EROFS)) {
        fd = open(lock_path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    }
    if (fd < 0) {
        result.warning = "cannot create lock file " + lock_path.string() + " (" + std::strerror(open_errno) +
                         "); single-process ownership of " + key.string() + " is not enforced";
        held.emplace(key, Held{-1, lock_path});
        return result;
    }
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        const int err = errno;
        if (err == EWOULDBLOCK) {
            result.ok = false;
            result.error = "another XStore process" + OwnerPidSuffix(fd) + " is already using the data directory " +
                           key.string() + " (lock file " + lock_path.string() + "); refusing to start";
        } else {
            result.warning = "cannot enforce single-process ownership on this filesystem: " +
                             std::string(std::strerror(err)) + " (data directory " + key.string() + ")";
            held.emplace(key, Held{fd, lock_path});
            return result;
        }
        close(fd);
        return result;
    }

    // Best effort, only for the owner's error message; the lock itself is the truth.
    if (writable) {
        const std::string pid = std::to_string(getpid()) + "\n";
        if (ftruncate(fd, 0) == 0) {
            const ssize_t ignored = pwrite(fd, pid.data(), pid.size(), 0);
            (void)ignored;
        }
    }
    held.emplace(key, Held{fd, lock_path});
    return result;
}

}  // namespace dirlock
