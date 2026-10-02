#pragma once
// WAL ring file format (Durability C). One file per WAL shard:
//   [4096-byte header region: two 512-byte slots, ping-pong by slot_seq % 2]
//   [ring_bytes of ring, addressed by LSN: file offset = 4096 + lsn % ring_bytes]
// LSNs are monotonically increasing byte positions in the logical log; a
// record's stored LSN lets replay reject stale bytes left by a previous lap.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace walring {

inline constexpr uint32_t RING_MAGIC = 0x474E5258;      // "XRNG" little-endian
inline constexpr uint32_t RING_VERSION = 1;
inline constexpr size_t HEADER_REGION_BYTES = 4096;
inline constexpr size_t HEADER_SLOT_BYTES = 512;
inline constexpr size_t RECORD_FIXED_BYTES = 28;        // recordLen4 crc4 lsn8 dbNameLen2 rowLength2 timeRes4 rowCount4
inline constexpr uint16_t PAD_DB_NAME_LEN = 0xFFFF;     // dbNameLen value marking a PAD record

struct RingHeader {
    uint32_t magic = RING_MAGIC;
    uint32_t version = RING_VERSION;
    uint32_t shard_id = 0;
    uint32_t reserved = 0;
    uint64_t ring_bytes = 0;
    uint64_t checkpoint_lsn = 0;
    uint64_t slot_seq = 0;
    uint32_t crc = 0;      // CRC32C of all bytes before this field
    uint32_t pad = 0;
};
static_assert(sizeof(RingHeader) == 48);

// Valid slot with the highest slot_seq; false if neither slot is valid.
[[nodiscard]] bool ReadBestHeader(int fd, RingHeader &out);
// Computes crc, writes the slot (header.slot_seq % 2), fdatasync.
[[nodiscard]] bool WriteHeaderSlot(int fd, RingHeader header);
// FRESH (empty or about-to-be-overwritten) files only: it zero-fills the ring,
// so calling it on a ring holding live records destroys them.
// posix_fallocate header+ring, zero-fill the ring in 4 MiB writes (so extents
// are "written", not unwritten), fsync, THEN write slot_seq=1 ckpt=0 (a header
// is never durable before the ring it describes). Rejects ring_bytes < RECORD_FIXED_BYTES.
[[nodiscard]] bool InitRingFile(int fd, uint32_t shard_id, uint64_t ring_bytes);

inline uint64_t RingOffset(uint64_t lsn, uint64_t ring_bytes) { return HEADER_REGION_BYTES + lsn % ring_bytes; }

// Where a record of `total_bytes` goes when the log head is at `head_lsn`. Never straddles the ring end.
// Returns the LSN the record starts at; the ring-end bytes consumed before it are `start - head_lsn`.
// `pad_bytes` = size of the PAD record to write at head_lsn (0 if skipped silently or no wrap).
// PRECONDITION: callers must reject total_bytes > ring_bytes first -- such a record can never fit
// a lap, and PlanAppend would return head+rem forever.
[[nodiscard]] uint64_t PlanAppend(uint64_t head_lsn, uint64_t ring_bytes, size_t total_bytes, size_t &pad_bytes);

// Writes a PAD record of exactly `pad_bytes` (>= RECORD_FIXED_BYTES) into `out`.
void BuildPad(char *out, size_t pad_bytes, uint64_t lsn);

// One ring file (one WAL shard): LSN accounting plus the stall/backpressure
// gate. Exactly one writer thread appends (Reserve/CommitAppended/
// PublishApplied); AdvanceCheckpoint/ResetTo/readers may run on other threads.
//   checkpoint_lsn <= applied_lsn <= head_lsn
// Owns the fd (closed on destruction). Shared via std::shared_ptr.
class Ring {
public:
    // head = applied = checkpoint = start_lsn. The next header slot_seq is
    // seeded from the file's current best header (1 if none).
    Ring(int fd, std::string path, uint32_t shard_id, uint64_t ring_bytes, uint64_t start_lsn);
    ~Ring();
    Ring(const Ring &) = delete;
    Ring &operator=(const Ring &) = delete;

    // Finds where `bytes` (a contiguous, non-straddling run, <= ring_bytes/2 by
    // the caller's contract) goes, PAD included (PlanAppend), and blocks until
    // it fits below STALL_FILL of the ring (or the ring is empty), or timeout.
    // false = timeout / bytes > ring_bytes (caller fails the batch "overloaded").
    // On true: start_lsn = LSN of the first byte, pad_bytes = PAD record to write
    // at (start_lsn - pad_bytes) first (0 = none). Does not move head.
    [[nodiscard]] bool Reserve(size_t bytes, std::chrono::milliseconds timeout, uint64_t &start_lsn, size_t &pad_bytes);
    void CommitAppended(uint64_t end_lsn);   // head_lsn := end_lsn (after the WAL fdatasync succeeded)
    void PublishApplied(uint64_t end_lsn);   // applied_lsn := end_lsn (after the batch's data apply finished)
    uint64_t HeadLsn() const { return head_.load(std::memory_order_acquire); }
    uint64_t AppliedLsn() const { return applied_.load(std::memory_order_acquire); }
    uint64_t CheckpointLsn() const { return checkpoint_.load(std::memory_order_acquire); }
    double Fill() const;                     // (head - checkpoint) / ring_bytes
    // Persist checkpoint_lsn in the alternate header slot, then free the space and
    // wake stalled Reserve()s. No-op if lsn <= current; refused (logged) if lsn > head;
    // unchanged if the header write fails.
    void AdvanceCheckpoint(uint64_t lsn);
    // head = applied = checkpoint = lsn, in memory only (replay re-seats the ring).
    void ResetTo(uint64_t lsn);

    int fd() const { return fd_; }
    const std::string &path() const { return path_; }
    uint64_t ring_bytes() const { return ring_bytes_; }
    uint32_t ShardId() const { return shard_id_; }

private:
    bool TryPlan(size_t bytes, uint64_t &start_lsn, size_t &pad_bytes) const;

    const int fd_;
    const std::string path_;
    const uint32_t shard_id_;
    const uint64_t ring_bytes_;
    const uint64_t stall_limit_bytes_;       // WAL_STALL_FILL * ring_bytes
    alignas(64) std::atomic<uint64_t> head_;
    std::atomic<uint64_t> applied_;
    std::atomic<uint64_t> checkpoint_;
    uint64_t slot_seq_;                      // guarded by mu_
    std::mutex mu_;                          // slow-path Reserve wait + AdvanceCheckpoint/ResetTo
    std::condition_variable cv_;
};

// Test hooks (process-wide; 0 = use XStoreTunables). Ring size applies to rings opened afterwards.
//   SetRingBytesForTests(n)        per-test override; 0 restores the DEFAULT below (not the raw tunable)
//   SetDefaultRingBytesForTests(n) process-wide default (e.g. a smaller suite-wide ring); 0 = XStoreTunables::WAL_RING_BYTES
void SetRingBytesForTests(uint64_t ring_bytes);
void SetDefaultRingBytesForTests(uint64_t ring_bytes);
void SetStallTimeoutMsForTests(unsigned ms);
// The next n WAL fdatasyncs by the writer are treated as failed (n = 0 clears). The bytes are still written.
void FailNextWalSyncsForTests(int n);
// True (and counts the hook down) if this writer sync should be treated as failed.
bool ConsumeInjectedWalSyncFailure();
uint64_t DefaultRingBytes();
unsigned StallTimeoutMs();
// Bumped by ClearRingRegistryForTests; writer threads drop their per-path Ring caches when it changes.
uint64_t RegistryEpoch();

// Static, mutex-guarded registry keyed by WAL file path.
//  * valid ring header      -> ring, start_lsn = checkpoint_lsn (ring size from the header)
//  * empty / new file       -> InitRingFile
//  * non-empty, no header   -> a legacy WAL awaiting replay: nullptr (never wiped here),
//                              unless reinit_legacy: truncate + InitRingFile (the writer's
//                              last resort when no replay ever converted the file)
//  * open/init failure      -> nullptr
std::shared_ptr<Ring> GetOrOpenRing(const std::string &wal_path, uint32_t shard_id, bool reinit_legacy = false);
std::vector<std::shared_ptr<Ring>> AllRings();
void ClearRingRegistryForTests();

}  // namespace walring
