// Link-time interposition: see XStoreSyscallFault.h. This TU is compiled without _FILE_OFFSET_BITS=64 so that
// open/open64, stat/stat64 and pwrite/pwrite64 are separate symbols that can each be defined here (production code is
// built with the 64 spellings, libstdc++ imports the plain stat/open ones).
#undef _FILE_OFFSET_BITS

#include "XStoreSyscallFault.h"

#include <atomic>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>

namespace xstore_fault {
namespace {

struct PathSlot {
    std::mutex mu;
    std::string prefix;   // exact path, or "<dir>/" for a subtree
    bool subtree = false;
    unsigned ops = 0;
    int err = 0;
};
struct FdSlot {
    std::mutex mu;
    std::string path;
    int skip = 0, fail = 0, err = 0, calls = 0, failures = 0;
};

// Heap-allocated and leaked: interposed calls can run from threads still alive during static destruction.
PathSlot &PathState() { static PathSlot *s = new PathSlot; return *s; }
FdSlot &FdState(FdOp op) {
    static FdSlot *sync = new FdSlot;
    static FdSlot *write = new FdSlot;
    return op == FdOp::kSync ? *sync : *write;
}
std::atomic<bool> g_path_armed{false};
std::atomic<bool> g_sync_armed{false};
std::atomic<bool> g_pwrite_armed{false};
std::atomic<uint64_t> g_open_hits{0};
std::atomic<uint64_t> g_stat_hits{0};

int PathFaultErrno(const char *path, std::atomic<uint64_t> &hits, PathOp op) {
    if (!g_path_armed.load(std::memory_order_relaxed) || path == nullptr || path[0] != '/') return 0;
    PathSlot &s = PathState();
    std::lock_guard<std::mutex> lock(s.mu);
    if (s.err == 0 || (s.ops & op) == 0) return 0;
    const bool match = s.subtree ? std::strncmp(path, s.prefix.c_str(), s.prefix.size()) == 0 : s.prefix == path;
    if (!match) return 0;
    hits.fetch_add(1, std::memory_order_relaxed);
    return s.err;
}

int FdFaultErrno(FdOp op, int fd) {
    if (!(op == FdOp::kSync ? g_sync_armed : g_pwrite_armed).load(std::memory_order_relaxed)) return 0;
    char link[64];
    std::snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    char target[4096];
    const ssize_t n = readlink(link, target, sizeof target - 1);
    if (n <= 0) return 0;
    target[n] = '\0';
    FdSlot &s = FdState(op);
    std::lock_guard<std::mutex> lock(s.mu);
    if (s.err == 0 || s.path != target) return 0;
    ++s.calls;
    if (s.calls <= s.skip || s.failures >= s.fail) return 0;
    ++s.failures;
    return s.err;
}

}  // namespace

ScopedPathFault::ScopedPathFault(std::string path, bool subtree, int err, unsigned ops) {
    PathSlot &s = PathState();
    std::lock_guard<std::mutex> lock(s.mu);
    if (subtree && (path.empty() || path.back() != '/')) path += '/';
    s.prefix = std::move(path);
    s.subtree = subtree;
    s.ops = ops;
    s.err = err;
    g_open_hits.store(0);
    g_stat_hits.store(0);
    g_path_armed.store(true);
}
ScopedPathFault::~ScopedPathFault() {
    g_path_armed.store(false);
    PathSlot &s = PathState();
    std::lock_guard<std::mutex> lock(s.mu);
    s.err = 0;
}
uint64_t ScopedPathFault::OpenHits() const { return g_open_hits.load(); }
uint64_t ScopedPathFault::StatHits() const { return g_stat_hits.load(); }

ScopedFdFault::ScopedFdFault(FdOp op, const std::string &path, int skip_calls, int fail_calls, int err) : op_(op) {
    std::error_code ec;
    const std::filesystem::path canonical = std::filesystem::canonical(path, ec);   // /proc/self/fd links are resolved
    FdSlot &s = FdState(op);
    std::lock_guard<std::mutex> lock(s.mu);
    s.path = ec ? path : canonical.string();
    s.skip = skip_calls;
    s.fail = fail_calls;
    s.err = err;
    s.calls = 0;
    s.failures = 0;
    (op == FdOp::kSync ? g_sync_armed : g_pwrite_armed).store(true);
}
ScopedFdFault::~ScopedFdFault() {
    (op_ == FdOp::kSync ? g_sync_armed : g_pwrite_armed).store(false);
    FdSlot &s = FdState(op_);
    std::lock_guard<std::mutex> lock(s.mu);
    s.err = 0;
}
int ScopedFdFault::Calls() const {
    FdSlot &s = FdState(op_);
    std::lock_guard<std::mutex> lock(s.mu);
    return s.calls;
}
int ScopedFdFault::Failures() const {
    FdSlot &s = FdState(op_);
    std::lock_guard<std::mutex> lock(s.mu);
    return s.failures;
}

}  // namespace xstore_fault

namespace {

template <typename Fn>
Fn RealSymbol(const char *name) {
    return reinterpret_cast<Fn>(dlsym(RTLD_NEXT, name));
}

xstore_fault::PathOp OpenOp(int flags) {
    const bool writes = (flags & O_ACCMODE) != O_RDONLY || (flags & (O_CREAT | O_TRUNC)) != 0;
    return writes ? xstore_fault::kOpenWrite : xstore_fault::kOpenRead;
}

mode_t OpenMode(int flags, va_list &ap) {
    const bool has_mode = (flags & O_CREAT) != 0 || (flags & O_TMPFILE) == O_TMPFILE;
    return has_mode ? va_arg(ap, mode_t) : 0;
}

using xstore_fault::g_open_hits;
using xstore_fault::g_stat_hits;
using xstore_fault::PathFaultErrno;
using xstore_fault::FdFaultErrno;
using xstore_fault::FdOp;
using xstore_fault::kFopen;
using xstore_fault::kStat;

}  // namespace

extern "C" {

#define XSTORE_FAULT_OPEN(NAME)                                                                          \
    int NAME(const char *path, int flags, ...) {                                                         \
        using Fn = int (*)(const char *, int, ...);                                                      \
        static const Fn real = RealSymbol<Fn>(#NAME);                                                    \
        va_list ap;                                                                                      \
        va_start(ap, flags);                                                                             \
        const mode_t mode = OpenMode(flags, ap);                                                         \
        va_end(ap);                                                                                      \
        if (const int err = PathFaultErrno(path, g_open_hits, OpenOp(flags)); err != 0) { errno = err; return -1; }     \
        return real(path, flags, mode);                                                                  \
    }
XSTORE_FAULT_OPEN(open)
XSTORE_FAULT_OPEN(open64)

#define XSTORE_FAULT_OPENAT(NAME)                                                                        \
    int NAME(int dirfd, const char *path, int flags, ...) {                                              \
        using Fn = int (*)(int, const char *, int, ...);                                                 \
        static const Fn real = RealSymbol<Fn>(#NAME);                                                    \
        va_list ap;                                                                                      \
        va_start(ap, flags);                                                                             \
        const mode_t mode = OpenMode(flags, ap);                                                         \
        va_end(ap);                                                                                      \
        if (const int err = PathFaultErrno(path, g_open_hits, OpenOp(flags)); err != 0) { errno = err; return -1; }     \
        return real(dirfd, path, flags, mode);                                                           \
    }
XSTORE_FAULT_OPENAT(openat)
XSTORE_FAULT_OPENAT(openat64)

#define XSTORE_FAULT_FOPEN(NAME)                                                                         \
    FILE *NAME(const char *path, const char *mode) {                                                     \
        using Fn = FILE *(*)(const char *, const char *);                                                \
        static const Fn real = RealSymbol<Fn>(#NAME);                                                    \
        if (const int err = PathFaultErrno(path, g_open_hits, kFopen); err != 0) { errno = err; return nullptr; } \
        return real(path, mode);                                                                         \
    }
XSTORE_FAULT_FOPEN(fopen)
XSTORE_FAULT_FOPEN(fopen64)

#define XSTORE_FAULT_STAT(NAME, STRUCT)                                                                  \
    int NAME(const char *path, struct STRUCT *buf) noexcept {                                            \
        using Fn = int (*)(const char *, struct STRUCT *);                                               \
        static const Fn real = RealSymbol<Fn>(#NAME);                                                    \
        if (const int err = PathFaultErrno(path, g_stat_hits, kStat); err != 0) { errno = err; return -1; }     \
        return real(path, buf);                                                                          \
    }
XSTORE_FAULT_STAT(stat, stat)
XSTORE_FAULT_STAT(stat64, stat64)
XSTORE_FAULT_STAT(lstat, stat)
XSTORE_FAULT_STAT(lstat64, stat64)

#define XSTORE_FAULT_SYNC(NAME)                                                                          \
    int NAME(int fd) {                                                                                   \
        using Fn = int (*)(int);                                                                         \
        static const Fn real = RealSymbol<Fn>(#NAME);                                                    \
        if (const int err = FdFaultErrno(FdOp::kSync, fd); err != 0) { errno = err; return -1; }         \
        return real(fd);                                                                                 \
    }
XSTORE_FAULT_SYNC(fdatasync)
XSTORE_FAULT_SYNC(fsync)

#define XSTORE_FAULT_PWRITE(NAME, OFF_T)                                                                 \
    ssize_t NAME(int fd, const void *buf, size_t count, OFF_T offset) {                                  \
        using Fn = ssize_t (*)(int, const void *, size_t, OFF_T);                                        \
        static const Fn real = RealSymbol<Fn>(#NAME);                                                    \
        if (const int err = FdFaultErrno(FdOp::kPwrite, fd); err != 0) { errno = err; return -1; }       \
        return real(fd, buf, count, offset);                                                             \
    }
XSTORE_FAULT_PWRITE(pwrite, off_t)
XSTORE_FAULT_PWRITE(pwrite64, off64_t)

}  // extern "C"
