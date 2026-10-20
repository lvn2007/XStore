// Link-time interposition: see XStoreSyscallFault.h. This TU is compiled without
// _FILE_OFFSET_BITS=64 so that open/open64, stat/stat64 and pwrite/pwrite64 are separate symbols
// that can each be defined here (production code is built with the 64 spellings, libstdc++ imports
// the plain stat/open ones).
#undef _FILE_OFFSET_BITS

#include "XStoreSyscallFault.h"

#include <atomic>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <system_error>
#include <thread>
#include <unistd.h>

namespace xstore_fault {
namespace {

struct PathSlot {
    std::mutex mu;
    std::string prefix;  // exact path, or "<dir>/" for a subtree
    bool subtree = false;
    unsigned ops = 0;
    int err = 0;
};
struct FdSlot {
    std::mutex mu;
    std::string path;
    int skip = 0, fail = 0, err = 0, calls = 0, failures = 0;
};

constexpr size_t kFdOpCount = 3;
size_t Index(FdOp op) { return static_cast<size_t>(op); }

// Heap-allocated and leaked: interposed calls can run from threads still alive during static
// destruction.
PathSlot &PathState() {
    static PathSlot *s = new PathSlot;
    return *s;
}
FdSlot &FdState(FdOp op) {
    static FdSlot *slots = new FdSlot[kFdOpCount];
    return slots[Index(op)];
}
std::atomic<bool> g_path_armed{false};
std::atomic<bool> g_fd_armed[kFdOpCount];
std::atomic<uint64_t> g_open_hits{0};
std::atomic<uint64_t> g_stat_hits{0};

// "At most one guard alive" is a documented contract; a second one would silently overwrite the
// first's slot.
std::atomic<bool> g_path_guard_active{false};
std::atomic<bool> g_fd_guard_active[kFdOpCount];

void ClaimOrAbort(std::atomic<bool> &active, const char *what) {
    if (active.exchange(true)) {
        std::fprintf(stderr, "xstore_fault: a second %s is alive; at most one per class\n", what);
        std::abort();
    }
}

int PathFaultErrno(const char *path, std::atomic<uint64_t> &hits, PathOp op) {
    if (!g_path_armed.load(std::memory_order_relaxed) || path == nullptr || path[0] != '/') {
        return 0;
    }
    PathSlot &s = PathState();
    std::lock_guard<std::mutex> lock(s.mu);
    if (s.err == 0 || (s.ops & op) == 0) return 0;
    const bool match = s.subtree ? std::strncmp(path, s.prefix.c_str(), s.prefix.size()) == 0
                                 : s.prefix == path;
    if (!match) return 0;
    hits.fetch_add(1, std::memory_order_relaxed);
    return s.err;
}

int FdFaultErrno(FdOp op, int fd) {
    if (!g_fd_armed[Index(op)].load(std::memory_order_relaxed)) return 0;
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

struct ObserverSlot {
    std::mutex mu;
    std::string path;
    std::thread::id thread;
    std::function<void(ObservedOp, uint64_t, uint64_t)> fn;
};
ObserverSlot &ObserverState() {
    static ObserverSlot *s = new ObserverSlot;
    return *s;
}
std::atomic<bool> g_observer_armed{false};
std::atomic<bool> g_observer_guard_active{false};

bool FdRefersTo(int fd, const std::string &path) {
    char link[64];
    std::snprintf(link, sizeof link, "/proc/self/fd/%d", fd);
    char target[4096];
    const ssize_t n = readlink(link, target, sizeof target - 1);
    if (n <= 0) return false;
    target[n] = '\0';
    return path == target;
}

void Observe(ObservedOp op, int fd, uint64_t offset, uint64_t length) {
    if (!g_observer_armed.load(std::memory_order_relaxed)) return;
    ObserverSlot &s = ObserverState();
    std::lock_guard<std::mutex> lock(s.mu);
    if (!s.fn || s.thread != std::this_thread::get_id() || !FdRefersTo(fd, s.path)) return;
    s.fn(op, offset, length);
}

struct OpenHookSlot {
    std::mutex mu;
    std::string path, canonical;  // the caller's spelling and its canonical form: either matches
    std::thread::id thread;
    std::function<void()> fn;
    bool fired = false;
};
OpenHookSlot &OpenHookState() {
    static OpenHookSlot *s = new OpenHookSlot;
    return *s;
}
std::atomic<bool> g_open_hook_armed{false};
std::atomic<bool> g_open_hook_guard_active{false};

void AfterOpen(const char *path, int fd) {
    if (fd < 0 || !g_open_hook_armed.load(std::memory_order_relaxed) || path == nullptr) return;
    OpenHookSlot &s = OpenHookState();
    std::lock_guard<std::mutex> lock(s.mu);
    if (s.fired || !s.fn || s.thread != std::this_thread::get_id() || (s.path != path && s.canonical != path)) return;
    s.fired = true;
    s.fn();
}

std::string Canonical(const std::string &path) {
    std::error_code ec;
    const std::filesystem::path canonical = std::filesystem::weakly_canonical(path, ec);
    return ec ? path : canonical.string();
}

}  // namespace

ScopedPathFault::ScopedPathFault(std::string path, bool subtree, int err, unsigned ops) {
    ClaimOrAbort(g_path_guard_active, "ScopedPathFault");
    path = Canonical(path);
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
    {
        PathSlot &s = PathState();
        std::lock_guard<std::mutex> lock(s.mu);
        s.err = 0;
    }
    g_path_guard_active.store(false);
}
uint64_t ScopedPathFault::OpenHits() const { return g_open_hits.load(); }
uint64_t ScopedPathFault::StatHits() const { return g_stat_hits.load(); }

ScopedFdFault::ScopedFdFault(FdOp op, const std::string &path, int skip_calls, int fail_calls,
                             int err)
    : op_(op) {
    ClaimOrAbort(g_fd_guard_active[Index(op)], "ScopedFdFault of the same FdOp");
    const std::string resolved = Canonical(path);  // /proc/self/fd links are resolved
    FdSlot &s = FdState(op);
    std::lock_guard<std::mutex> lock(s.mu);
    s.path = resolved;
    s.skip = skip_calls;
    s.fail = fail_calls;
    s.err = err;
    s.calls = 0;
    s.failures = 0;
    g_fd_armed[Index(op)].store(true);
}
ScopedFdFault::~ScopedFdFault() {
    g_fd_armed[Index(op_)].store(false);
    {
        FdSlot &s = FdState(op_);
        std::lock_guard<std::mutex> lock(s.mu);
        s.err = 0;
    }
    g_fd_guard_active[Index(op_)].store(false);
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

ScopedFdObserver::ScopedFdObserver(const std::string &path,
                                   std::function<void(ObservedOp, uint64_t, uint64_t)> fn) {
    ClaimOrAbort(g_observer_guard_active, "ScopedFdObserver");
    ObserverSlot &s = ObserverState();
    std::lock_guard<std::mutex> lock(s.mu);
    s.path = Canonical(path);
    s.thread = std::this_thread::get_id();
    s.fn = std::move(fn);
    g_observer_armed.store(true);
}
ScopedFdObserver::~ScopedFdObserver() {
    g_observer_armed.store(false);
    {
        ObserverSlot &s = ObserverState();
        std::lock_guard<std::mutex> lock(s.mu);
        s.fn = nullptr;
    }
    g_observer_guard_active.store(false);
}

ScopedOpenHook::ScopedOpenHook(const std::string &path, std::function<void()> fn) {
    ClaimOrAbort(g_open_hook_guard_active, "ScopedOpenHook");
    OpenHookSlot &s = OpenHookState();
    std::lock_guard<std::mutex> lock(s.mu);
    s.path = path;
    s.canonical = Canonical(path);
    s.thread = std::this_thread::get_id();
    s.fn = std::move(fn);
    s.fired = false;
    g_open_hook_armed.store(true);
}
ScopedOpenHook::~ScopedOpenHook() {
    g_open_hook_armed.store(false);
    {
        OpenHookSlot &s = OpenHookState();
        std::lock_guard<std::mutex> lock(s.mu);
        s.fn = nullptr;
    }
    g_open_hook_guard_active.store(false);
}
bool ScopedOpenHook::Fired() const {
    OpenHookSlot &s = OpenHookState();
    std::lock_guard<std::mutex> lock(s.mu);
    return s.fired;
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
using xstore_fault::Observe;
using xstore_fault::AfterOpen;
using xstore_fault::ObservedOp;
using xstore_fault::kFopen;
using xstore_fault::kStat;

}  // namespace

extern "C" {

#define XSTORE_FAULT_OPEN(NAME)                                                                    \
    int NAME(const char *path, int flags, ...) {                                                   \
        using Fn = int (*)(const char *, int, ...);                                                \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        va_list ap;                                                                                \
        va_start(ap, flags);                                                                       \
        const mode_t mode = OpenMode(flags, ap);                                                   \
        va_end(ap);                                                                                \
        if (const int err = PathFaultErrno(path, g_open_hits, OpenOp(flags)); err != 0) {          \
            errno = err;                                                                           \
            return -1;                                                                             \
        }                                                                                          \
        const int fd = real(path, flags, mode);                                                    \
        AfterOpen(path, fd);                                                                       \
        return fd;                                                                                 \
    }
XSTORE_FAULT_OPEN(open)
XSTORE_FAULT_OPEN(open64)

#define XSTORE_FAULT_OPENAT(NAME)                                                                  \
    int NAME(int dirfd, const char *path, int flags, ...) {                                        \
        using Fn = int (*)(int, const char *, int, ...);                                           \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        va_list ap;                                                                                \
        va_start(ap, flags);                                                                       \
        const mode_t mode = OpenMode(flags, ap);                                                   \
        va_end(ap);                                                                                \
        if (const int err = PathFaultErrno(path, g_open_hits, OpenOp(flags)); err != 0) {          \
            errno = err;                                                                           \
            return -1;                                                                             \
        }                                                                                          \
        return real(dirfd, path, flags, mode);                                                     \
    }
XSTORE_FAULT_OPENAT(openat)
XSTORE_FAULT_OPENAT(openat64)

#define XSTORE_FAULT_FOPEN(NAME)                                                                   \
    FILE *NAME(const char *path, const char *mode) {                                               \
        using Fn = FILE *(*)(const char *, const char *);                                          \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        if (const int err = PathFaultErrno(path, g_open_hits, kFopen); err != 0) {                 \
            errno = err;                                                                           \
            return nullptr;                                                                        \
        }                                                                                          \
        return real(path, mode);                                                                   \
    }
XSTORE_FAULT_FOPEN(fopen)
XSTORE_FAULT_FOPEN(fopen64)

#define XSTORE_FAULT_STAT(NAME, STRUCT)                                                            \
    int NAME(const char *path, struct STRUCT *buf) noexcept {                                      \
        using Fn = int (*)(const char *, struct STRUCT *);                                         \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        if (const int err = PathFaultErrno(path, g_stat_hits, kStat); err != 0) {                  \
            errno = err;                                                                           \
            return -1;                                                                             \
        }                                                                                          \
        return real(path, buf);                                                                    \
    }
XSTORE_FAULT_STAT(stat, stat)
XSTORE_FAULT_STAT(stat64, stat64)
XSTORE_FAULT_STAT(lstat, stat)
XSTORE_FAULT_STAT(lstat64, stat64)

int rename(const char *from, const char *to) noexcept {
    using Fn = int (*)(const char *, const char *);
    static const Fn real = RealSymbol<Fn>("rename");
    if (const int err = PathFaultErrno(from, g_open_hits, xstore_fault::kRename); err != 0) {
        errno = err;
        return -1;
    }
    return real(from, to);
}

#define XSTORE_FAULT_SYNC(NAME)                                                                    \
    int NAME(int fd) {                                                                             \
        using Fn = int (*)(int);                                                                   \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        if (const int err = FdFaultErrno(FdOp::kSync, fd); err != 0) {                             \
            errno = err;                                                                           \
            return -1;                                                                             \
        }                                                                                          \
        return real(fd);                                                                           \
    }
XSTORE_FAULT_SYNC(fdatasync)
XSTORE_FAULT_SYNC(fsync)

#define XSTORE_FAULT_PWRITE(NAME, OFF_T)                                                           \
    ssize_t NAME(int fd, const void *buf, size_t count, OFF_T offset) {                            \
        using Fn = ssize_t (*)(int, const void *, size_t, OFF_T);                                  \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        if (const int err = FdFaultErrno(FdOp::kPwrite, fd); err != 0) {                           \
            errno = err;                                                                           \
            return -1;                                                                             \
        }                                                                                          \
        return real(fd, buf, count, offset);                                                       \
    }
XSTORE_FAULT_PWRITE(pwrite, off_t)
XSTORE_FAULT_PWRITE(pwrite64, off64_t)

#define XSTORE_FAULT_PREAD(NAME, OFF_T)                                                            \
    ssize_t NAME(int fd, void *buf, size_t count, OFF_T offset) {                                  \
        using Fn = ssize_t (*)(int, void *, size_t, OFF_T);                                        \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        if (const int err = FdFaultErrno(FdOp::kPread, fd); err != 0) {                            \
            errno = err;                                                                           \
            return -1;                                                                             \
        }                                                                                          \
        Observe(ObservedOp::kPread, fd, static_cast<uint64_t>(offset), count);                     \
        return real(fd, buf, count, offset);                                                       \
    }
XSTORE_FAULT_PREAD(pread, off_t)
XSTORE_FAULT_PREAD(pread64, off64_t)

#define XSTORE_FAULT_LSEEK(NAME, OFF_T)                                                            \
    OFF_T NAME(int fd, OFF_T offset, int whence) noexcept {                                        \
        using Fn = OFF_T (*)(int, OFF_T, int);                                                     \
        static const Fn real = RealSymbol<Fn>(#NAME);                                              \
        if (whence == SEEK_DATA) Observe(ObservedOp::kSeekData, fd, static_cast<uint64_t>(offset), 0); \
        return real(fd, offset, whence);                                                           \
    }
XSTORE_FAULT_LSEEK(lseek, off_t)
XSTORE_FAULT_LSEEK(lseek64, off64_t)

}  // extern "C"
