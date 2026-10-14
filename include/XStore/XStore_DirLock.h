#pragma once

#include <filesystem>
#include <string>

// One XStore process per data directory. The owner holds an exclusive
// flock() on "<data_store_dir>.lock" (a sibling of the directory, so no scan
// of the data or WAL directory can mistake it for a database or a shard). The
// kernel drops the lock when the process exits for any reason, so a crash
// leaves nothing stale behind.
namespace dirlock {

struct Result {
    // false only when another live process owns the directory.
    bool ok = true;
    // ok == true but ownership could not be enforced (lock file not openable, or
    // flock unsupported on this filesystem). Callers log it and carry on.
    std::string warning;
    // ok == false: the full message, naming the directory and the owner's PID when it is known.
    std::string error;
};

// "<canonical data_store_dir>.lock". weakly_canonical, so "DataStore/" and a symlink
// to it name the same file.
[[nodiscard]] std::filesystem::path LockPathFor(const std::filesystem::path &data_store_dir);

// Takes the lock for data_store_dir. Never blocks. Idempotent per process: a second call
// for a directory this process already holds is a successful no-op (a second flock from
// the same process would otherwise conflict with itself). Each new acquire first closes
// the locks of directories that have since been deleted.
[[nodiscard]] Result Acquire(const std::filesystem::path &data_store_dir);

}  // namespace dirlock
