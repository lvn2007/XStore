#pragma once
// Lazy, paced WAL checkpointer (Durability C, spec §2/§3.2).
//
// INVARIANT: for every data-file write, either its WAL record is still inside
// the live ring, or a data sync that STARTED AFTER the write finished has
// completed. A ring's checkpoint_lsn (the space the writer may reuse) therefore
// moves only after the data files it covers were fdatasync'd.
//
// RunCycle ordering (all under one cycle mutex; each file / directory sync additionally takes that path's own
// sync lock, which is what keeps it exclusive with a direct-path or replay sync of the same path):
//   1. snapshot every ring's AppliedLsn()           (acquire)
//   2. Dirty().Take()  -> merged into the carried set of unsynced files
//   3. fdatasync every carried file (opened by path; ENOENT = deleted DB, skip)
//   4. fsync each file's parent directory (a new year file's directory entry is
//      not made durable by fdatasync of the file)
//   5. only if 3 and 4 fully succeeded: AdvanceCheckpoint(snapshot) per ring
// Snapshot-before-Take is what makes it safe: every apply with lsn < snapshot
// had finished (and Note'd its file) before the Take, so its bytes are in the
// set that gets synced. Records applied after the snapshot are deliberately not
// passed; the next cycle covers them. A failed data/dir sync keeps the carried set for
// retry and advances nothing; a header write that fails for ONE ring leaves the other
// rings free to advance (conservative: each ring's checkpoint is independent).
//
// A REAL sync error is sticky ("fsyncgate"): after a genuine writeback error the
// kernel may mark the lost pages clean, so a RETRIED fdatasync can return 0 with
// the data not on disk, and the retry would then advance the checkpoint over rows
// only the WAL still holds. The first real error from fdatasync(data file),
// fsync(directory), the ring HEADER write (pwrite/fdatasync, through the ring's
// separate header file description), the WAL writer's own ring pwritev/fdatasync,
// or sync_file_range (every errno except EINTR/EAGAIN, retried in place, and
// EINVAL/ESPIPE/ENOSYS/EBADF, "unsupported here", skipped) therefore POISONS the
// checkpointer for the life of the process:
// RunCycle() returns false without advancing anything, the thread stops cycling,
// and a CRITICAL line is logged once. Writers then stall at the full-ring
// threshold and fail callers as "overloaded"; a restart replays from the last good
// checkpoint. Not poisoning (still retryable): open() failures other than ENOENT
// and the plain test-injected failures. Directory fsync failing with EINVAL/ENOTSUP/EOPNOTSUPP/EROFS
// means the filesystem cannot fsync directories at all: that neither poisons nor fails the cycle (one
// WARNING per process; new-file directory entries just cannot be made durable there).
//
// Apply failure (decided 2026-10-10): if ApplyWalRecord fails for a record in a durable ring, the
// checkpoint is NOT held: the applied position still advances and the checkpoint may pass the
// record, so one failing file cannot wedge the shard. The caller was told "failed" and the rows may
// be partly present; retrying converges because inserts overwrite by timestamp. The failed
// record's data file is noted dirty anyway (ApplyWalRecord), so RingClear() is false for it until
// the checkpoint has passed the record and the direct path cannot overtake it.
//
// Hot path: the writer does ONE relaxed store of last_writer_activity_ns per
// gathered batch; nothing per row.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace walring {

// One cache line of its own: every shard writer stores to it, so it must not
// false-share with neighbouring globals.
struct alignas(64) PaddedAtomicI64 : std::atomic<int64_t> {
    constexpr PaddedAtomicI64(int64_t v = 0) noexcept : std::atomic<int64_t>(v) {}
};
// steady_clock ns of the last batch a WAL writer gathered; read by the idle trigger.
extern PaddedAtomicI64 last_writer_activity_ns;

// Parses an ops knob given in milliseconds (XSTORE_CKPT_IDLE_MS / XSTORE_CKPT_MAX_AGE_MS): the text must
// start with a digit and be all digits (no whitespace, sign, hex or trailing junk) and fit an unsigned.
bool ParseMsKnob(const char *text, unsigned &out);

// RAII boot hold (see Checkpointer::HoldForBoot).
class BootHold {
public:
    BootHold();
    ~BootHold();
    BootHold(const BootHold &) = delete;
    BootHold &operator=(const BootHold &) = delete;
};

// Leaked singleton (detached threads / exit-time statics may still reach it).
class Checkpointer {
public:
    static Checkpointer &Instance();

    // Idempotent; starts the thread if it is not running (also restarts after a stop).
    void Start();
    // Runs RunCycle() once, then stops and joins the thread. Idempotent; safe if Start() never ran.
    // Returns that final cycle's outcome (also kept for FinalCycleOk()).
    bool StopAfterFinalCycle();
    // Outcome of the last StopAfterFinalCycle(): true = the cycle succeeded (nothing dirty counts as
    // success); false = it failed, the checkpointer was poisoned, or no final cycle has run since Start().
    // XStore::Shutdown() marks indexes clean only if this is true (a poisoned checkpointer never completes a cycle, so it makes this false).
    bool FinalCycleOk() const { return final_cycle_ok_.load(std::memory_order_acquire); }
    // Synchronous full cycle. false = a data/dir sync failed (or a header write
    // did not stick): nothing (or not everything) advanced; retry is safe -- unless the
    // checkpointer got poisoned (a real error), in which case it never advances again.
    bool RunCycle();

    // Writer-side forced cycle (a record larger than half the ring on a file that is not ring-clear): asks the
    // checkpointer THREAD to run one cycle that starts after this call and waits for it, so the wait is bounded
    // by `deadline` even while a long cycle (fdatasync under cycle_mutex_) is in flight -- the writer never
    // takes cycle_mutex_ itself. Bypasses the test pause like RunCycle(). Wakes every 10 ms to look at
    // `stop_flag` and Poisoned(). kNotRunning: no loop thread (never started, or stopping): the caller may
    // RunCycle() itself, nothing else is cycling then.
    enum class ForcedCycle { kDone, kTimedOut, kStopped, kPoisoned, kNotRunning };
    [[nodiscard]] ForcedCycle RunForcedCycle(std::chrono::steady_clock::time_point deadline, const std::atomic<bool> &stop_flag);

    // Boot hold: while at least one hold is taken (XStore::ReplayWal takes one for its whole duration)
    // the loop's IDLE trigger does not fire -- replay Notes files while last_writer_activity_ns is
    // still 0, which would otherwise look like "dirty and idle for decades" and cycle every
    // millisecond, contending with the replay's syncs on the per-path sync locks. The fill, max-age and retry
    // triggers keep working. Prefer the RAII BootHold above.
    void HoldForBoot() { boot_holds_.fetch_add(1, std::memory_order_acq_rel); }
    void ReleaseBoot() { boot_holds_.fetch_sub(1, std::memory_order_acq_rel); }

    // A REAL error on a descriptor the checkpointer does not own (the WAL writer's ring pwritev/fdatasync,
    // a replay header commit): poisons exactly as a failed data fdatasync would.
    void ReportRealError(const char *what, const std::string &path, int err) { Poison(what, path, err); }

    // Ops/testing knobs (src/main.cpp: XSTORE_CKPT_IDLE_MS / XSTORE_CKPT_MAX_AGE_MS): the writer-idle and
    // oldest-unsynced-data triggers of a cycle. 0 restores XStoreTunables::WAL_CKPT_IDLE_MS / WAL_CKPT_MAX_AGE_MS.
    // The final cycle of a graceful stop runs regardless.
    void SetIdleMs(unsigned ms);
    void SetMaxAgeMs(unsigned ms);

    // Replay (XStore::ReplayWal) and the WAL writer's direct path (spec §3.3): fdatasync each data file in `paths`
    // (by path) and fsync their parent directories, through the SAME fault hooks / poison rule as RunCycle. Touches no ring,
    // no dirty state. true = everything durable (ENOENT = deleted DB is skipped, as in a cycle).
    // false = nothing may be checkpointed over these records: a hook/open failure (retryable) or a
    // real sync error (poisons). Always false once poisoned, and false if the checkpointer was poisoned
    // (by any thread) while these syncs ran.
    // Does NOT take cycle_mutex_: concurrent calls (one per WAL shard) and a running cycle overlap, except that
    // syncs of one path (file or directory) are mutually exclusive, each held across its Poison().
    [[nodiscard]] bool SyncPathsForReplay(const std::set<std::string> &paths);

    // Data files fdatasync'd by the last successful cycle. Only files dirtied since the cycle before it,
    // so it is NOT a "durable" set (clean-since-then files are absent): never gate MarkClean on it.
    std::unordered_set<std::string> LastCycleSyncedPaths() const;

    // True once a real (non-retryable) sync error occurred; sticky for the process.
    bool Poisoned() const { return poisoned_.load(std::memory_order_acquire); }

    // ---- test hooks ----
    void ClearPoisonForTests();                   // also drops the sticky hooks
    void FailNextDataSyncsStickyForTests(int n);  // like a real fdatasync error: poisons
    void FailNextDirSyncsStickyForTests(int n);   // like a real directory fsync error: poisons
    // Directory fsyncs report `err` (e.g. EINVAL) until cleared with 0; also re-arms the once-per-process
    // "cannot fsync directories" warning so a test can observe it.
    void SimulateDirSyncErrnoForTests(int err);
    // sync_file_range (the paced flush) reports `err` until cleared with 0.
    void SimulateSyncFileRangeErrnoForTests(int err) { sim_sfr_errno_.store(err, std::memory_order_relaxed); }
    // SyncPathsForReplay sleeps `ms` BEFORE taking the cycle mutex and records how many cycles ran in
    // that window (ReplaySyncWindowCyclesForTests): lets a test observe the loop during a slow replay.
    void SetReplaySyncDelayMsForTests(unsigned ms) { replay_sync_delay_ms_.store(ms, std::memory_order_relaxed); }
    uint64_t ReplaySyncWindowCyclesForTests() const { return replay_window_cycles_.load(std::memory_order_relaxed); }
    uint64_t DirSyncUnsupportedWarningsForTests() const { return dir_unsupported_warnings_.load(std::memory_order_relaxed); }
    std::unordered_set<std::string> LastCycleSyncedDirsForTests() const;   // directories fsync'd by the last successful cycle
    // Simulated kill -9: joins the thread WITHOUT the final cycle and forgets carried/flushed state.
    void StopWithoutFinalCycleForTests();
    // Every cycle sleeps `ms` (under the cycle mutex) before it starts; 0 = off. Lets a test hold a cycle in flight.
    void SetCycleDelayMsForTests(unsigned ms) { cycle_delay_ms_.store(ms, std::memory_order_relaxed); }
    // Every data-file fdatasync / directory fsync sleeps `ms` right before the syscall, inside the lock that
    // covers it (whichever that is); 0 = off. Lets a test hold one sync in flight and observe what else runs.
    void SetDataSyncDelayMsForTests(unsigned ms) { data_sync_delay_ms_.store(ms, std::memory_order_relaxed); }
    // Which per-path sync lock a data file / directory path maps to (two paths with the same value share a lock).
    static size_t SyncStripeForTests(const std::string &path) { return SyncStripe(path); }
    void SetDirSyncDelayMsForTests(unsigned ms) { dir_sync_delay_ms_.store(ms, std::memory_order_relaxed); }
    void SetPausedForTests(bool paused);          // waits for an in-flight cycle/flush; RunCycle() still works
    void FailNextDataSyncsForTests(int n);        // next n data-file fdatasyncs report failure
    void FailNextDirSyncsForTests(int n);         // next n directory fsyncs report failure
    uint64_t CyclesForTests() const { return cycles_.load(std::memory_order_relaxed); }
    uint64_t DirSyncsForTests() const { return dir_syncs_.load(std::memory_order_relaxed); }
    uint64_t DataSyncsForTests() const { return data_syncs_.load(std::memory_order_relaxed); }   // successful data-file fdatasyncs, any caller
    uint64_t PacedFlushBytesForTests() const { return paced_bytes_.load(std::memory_order_relaxed); }
    void SetIdleMsForTests(unsigned ms) { SetIdleMs(ms); }
    void SetMaxAgeMsForTests(unsigned ms) { SetMaxAgeMs(ms); }

private:
    Checkpointer() = default;
    void Loop();
    bool RunCycleLocked();                        // cycle_mutex_ held
    bool PacedFlushStep();                        // one <= WAL_FLUSH_CHUNK_BYTES sync_file_range; true if it did work
    static bool ConsumeFailure(std::atomic<int> &counter);
    static constexpr size_t SYNC_STRIPES = 256;
    static size_t SyncStripe(const std::string &path) { return std::hash<std::string>{}(path) % SYNC_STRIPES; }
    // One data-file fdatasync / directory fsync with the hooks and poison rule. Takes the path's own sync lock
    // (PathSyncMutex) for the whole call; kPoisoned if the checkpointer is, or becomes, poisoned.
    enum class SyncResult { kOk, kSkipped, kRetryable, kPoisoned };
    std::mutex &PathSyncMutex(const std::string &path) { return path_sync_mutex_[SyncStripe(path)]; }
    SyncResult SyncDataFile(const std::string &path);
    SyncResult SyncDirectory(const std::string &dir);
    // A directory fsync only makes directory ENTRIES durable. Once one has succeeded while a file existed, that file's
    // entry stays durable, so later syncs of the same file need not fsync its directory again (the direct path did, once
    // per record). The file is recognised by device + inode + inode generation: ext4 gives a deleted file's inode number
    // to the next file created, but never its generation, so a recreated or renamed-over file does not match and its
    // directory is synced again. A filesystem that reports no generation (0 or an error) is never skipped.
    struct FileId {
        uint64_t dev = 0, ino = 0, generation = 0;
        bool operator==(const FileId &o) const { return dev == o.dev && ino == o.ino && generation == o.generation; }
    };
    static bool ReadFileId(const std::string &path, FileId &id);
    // Fsyncs the parent directory of every file in `files` (data files just fdatasync'd) that holds an entry not yet known
    // durable; remembers the entries once their directory fsync succeeded. Worst outcome of the directory syncs;
    // `synced_dirs` (optional) receives the directories actually fsync'd.
    SyncResult SyncDirectoriesOf(const std::vector<std::string> &files, std::unordered_set<std::string> *synced_dirs);
    void Poison(const char *what, const std::string &path, int err);

    std::mutex cycle_mutex_;                      // one cycle / paced step at a time
    // Syncs of one path (a data file, a directory) are mutually exclusive across every caller (cycle, paced
    // flush, replay, the WAL writer's direct path); syncs of different paths overlap. Striped by path hash: two
    // paths on one stripe merely serialize. Lock order: cycle_mutex_ then a path lock; a path lock is never held
    // while taking another lock.
    std::mutex path_sync_mutex_[SYNC_STRIPES];
    std::mutex dirent_mutex_;
    std::unordered_map<std::string, FileId> dirent_durable_;   // path -> the file whose directory entry is durable (dirent_mutex_)
    std::unordered_set<std::string> carried_;     // unsynced data files (guarded by cycle_mutex_)
    mutable std::mutex synced_mutex_;
    std::unordered_set<std::string> last_synced_;
    std::unordered_set<std::string> last_synced_dirs_;

    std::mutex lifecycle_mutex_;                  // serializes Start / StopAfterFinalCycle (held across the join)
    std::thread thread_;                          // guarded by lifecycle_mutex_
    std::mutex wake_mutex_;                       // guards stop_ for the loop's timed wait
    std::condition_variable wake_;
    bool stop_ = false;
    bool running_ = false;                        // loop thread alive and not stopping (guarded by wake_mutex_)
    bool force_pending_ = false;                  // a writer wants a cycle that starts after its request (wake_mutex_)
    uint64_t cycles_started_ = 0;                 // loop cycles begun / finished, for RunForcedCycle (wake_mutex_)
    uint64_t cycles_finished_ = 0;
    std::condition_variable forced_done_;

    std::atomic<bool> paused_{false};
    std::atomic<int> fail_data_syncs_{0};
    std::atomic<int> fail_dir_syncs_{0};
    std::atomic<int> fail_data_sticky_{0};
    std::atomic<int> fail_dir_sticky_{0};
    std::atomic<int> sim_dir_errno_{0};
    std::atomic<int> sim_sfr_errno_{0};
    std::atomic<int> boot_holds_{0};
    std::atomic<unsigned> replay_sync_delay_ms_{0};
    std::atomic<uint64_t> replay_window_cycles_{0};
    std::atomic<bool> dir_unsupported_warned_{false};
    std::atomic<uint64_t> dir_unsupported_warnings_{0};
    std::atomic<bool> final_cycle_ok_{false};
    std::atomic<bool> poisoned_{false};
    std::atomic<bool> poison_logged_{false};
    std::atomic<uint64_t> cycles_{0};
    std::atomic<uint64_t> dir_syncs_{0};
    std::atomic<uint64_t> data_syncs_{0};
    std::atomic<uint64_t> paced_bytes_{0};
    std::atomic<uint64_t> advances_{0};           // rings advanced, ever (progress detector for back-to-back cycles)
    std::atomic<unsigned> cycle_delay_ms_{0};
    std::atomic<unsigned> data_sync_delay_ms_{0};
    std::atomic<unsigned> dir_sync_delay_ms_{0};
    std::atomic<unsigned> idle_ms_override_{0};
    std::atomic<unsigned> max_age_override_{0};
};

}  // namespace walring
