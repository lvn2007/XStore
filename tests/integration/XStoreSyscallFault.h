#pragma once
// Fault injection for libc calls WITHOUT any hook in production code: XStoreSyscallFault.cpp
// defines open/openat/stat/lstat/fdatasync/fsync/pwrite/pread itself (link-time interposition,
// same pattern as XStoreDeleteFaultTest.cpp) and forwards to the real function via
// dlsym(RTLD_NEXT) unless one of these guards is alive. Unarmed, every interposed call is a single
// relaxed atomic load plus the forward. At most one guard of each class (for ScopedFdFault: of
// each FdOp) may be alive at a time; a second one aborts.
#include <cstdint>
#include <string>

namespace xstore_fault {

// Which kinds of path-based calls a ScopedPathFault fails (a mask). kOpenWrite is open/openat with
// write access or O_CREAT/O_TRUNC; kOpenRead is every other open/openat; kFopen is fopen in any
// mode; kStat is stat/lstat.
enum PathOp : unsigned { kOpenRead = 1, kOpenWrite = 2, kFopen = 4, kStat = 8 };

// The selected calls on `path` (or on everything beneath it when `subtree`) fail with `err`, as
// they would for a file or directory the process may not access but without the root exemption of
// chmod. Only absolute paths are matched; `path` is canonicalised first (a symlinked TMPDIR must
// not make the guard miss).
class ScopedPathFault {
public:
    ScopedPathFault(std::string path, bool subtree, int err, unsigned ops);
    ~ScopedPathFault();
    ScopedPathFault(const ScopedPathFault &) = delete;
    ScopedPathFault &operator=(const ScopedPathFault &) = delete;
    // The counters are process-global (the one live guard resets them on construction), not
    // per-guard state.
    uint64_t OpenHits() const;  // open/openat/fopen calls failed
    uint64_t StatHits() const;  // stat/lstat calls failed
};

enum class FdOp { kSync, kPwrite, kPread };  // kSync covers fdatasync and fsync

// Calls of `op` on a descriptor that refers to `path` (resolved through /proc/self/fd): the first
// `skip_calls` pass through, the next `fail_calls` fail with `err`, later ones pass again.
class ScopedFdFault {
public:
    ScopedFdFault(FdOp op, const std::string &path, int skip_calls, int fail_calls, int err);
    ~ScopedFdFault();
    ScopedFdFault(const ScopedFdFault &) = delete;
    ScopedFdFault &operator=(const ScopedFdFault &) = delete;
    int Calls() const;     // matching calls seen, passed or failed
    int Failures() const;  // matching calls that were failed
private:
    FdOp op_;
};

}  // namespace xstore_fault
