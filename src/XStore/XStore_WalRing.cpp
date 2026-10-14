// src/XStore/XStore_WalRing.cpp
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_Crc32c.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace walring {

namespace {

bool PwriteAll(int fd, const char *buf, size_t len, off_t off) {
    while (len > 0) {
        ssize_t n = pwrite(fd, buf, len, off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        buf += n; off += n; len -= static_cast<size_t>(n);
    }
    return true;
}

bool ReadSlot(int fd, size_t slot, RingHeader &out) {
    RingHeader h;
    ssize_t n;
    do {
        n = pread(fd, &h, sizeof h, static_cast<off_t>(slot * HEADER_SLOT_BYTES));
    } while (n < 0 && errno == EINTR);
    if (n != static_cast<ssize_t>(sizeof h)) return false;
    if (h.magic != RING_MAGIC || h.version != RING_VERSION) return false;
    if (h.crc != Crc32c(&h, offsetof(RingHeader, crc))) return false;
    if (h.ring_bytes < RECORD_FIXED_BYTES) return false;   // RingOffset/PlanAppend divide by ring_bytes
    out = h;
    return true;
}

}  // namespace

bool FsyncDirectory(const std::string &dir) {
    const int fd = open(dir.empty() ? "." : dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) return false;
    const bool ok = fsync(fd) == 0;
    close(fd);
    return ok;
}

bool ReadBestHeader(int fd, RingHeader &out) {
    RingHeader a, b;
    const bool ok_a = ReadSlot(fd, 0, a);
    const bool ok_b = ReadSlot(fd, 1, b);
    if (!ok_a && !ok_b) return false;
    out = (ok_a && (!ok_b || a.slot_seq >= b.slot_seq)) ? a : b;
    return true;
}

bool WriteHeaderSlot(int fd, RingHeader header) {
    header.crc = Crc32c(&header, offsetof(RingHeader, crc));
    char slot[HEADER_SLOT_BYTES] = {};
    std::memcpy(slot, &header, sizeof header);
    if (!PwriteAll(fd, slot, sizeof slot, static_cast<off_t>((header.slot_seq % 2) * HEADER_SLOT_BYTES))) return false;
    return fdatasync(fd) == 0;
}

bool InitRingFile(int fd, uint32_t shard_id, uint64_t ring_bytes) {
    if (ring_bytes < RECORD_FIXED_BYTES) return false;
    if (posix_fallocate(fd, 0, static_cast<off_t>(HEADER_REGION_BYTES + ring_bytes)) != 0) return false;
    // Zero-fill so every ring extent is "written": fdatasync on an unwritten
    // (fallocated) extent still has to journal an extent-conversion later.
    constexpr size_t CHUNK = 4u << 20;
    const std::vector<char> zeros(CHUNK, 0);
    for (uint64_t done = 0; done < ring_bytes;) {
        const size_t n = static_cast<size_t>(std::min<uint64_t>(CHUNK, ring_bytes - done));
        if (!PwriteAll(fd, zeros.data(), n, static_cast<off_t>(HEADER_REGION_BYTES + done))) return false;
        done += n;
    }
    // The zeroed ring must be durable before a header claims it exists.
    if (fsync(fd) != 0) return false;
    RingHeader h;
    h.shard_id = shard_id;
    h.ring_bytes = ring_bytes;
    h.checkpoint_lsn = 0;
    h.slot_seq = 1;
    return WriteHeaderSlot(fd, h);
}

uint64_t PlanAppend(uint64_t head_lsn, uint64_t ring_bytes, size_t total_bytes, size_t &pad_bytes) {
    const uint64_t rem = ring_bytes - head_lsn % ring_bytes;
    if (total_bytes <= rem) {
        pad_bytes = 0;
        return head_lsn;
    }
    pad_bytes = rem >= RECORD_FIXED_BYTES ? static_cast<size_t>(rem) : 0;
    return head_lsn + rem;
}

void BuildPad(char *out, size_t pad_bytes, uint64_t lsn) {
    assert(pad_bytes >= RECORD_FIXED_BYTES);
    std::memset(out, 0, pad_bytes);
    const uint32_t record_len = static_cast<uint32_t>(pad_bytes - 4);
    std::memcpy(out, &record_len, 4);
    std::memcpy(out + 8, &lsn, 8);
    const uint16_t marker = PAD_DB_NAME_LEN;
    std::memcpy(out + 16, &marker, 2);
    const uint32_t crc = Crc32c(out + 8, pad_bytes - 8);
    std::memcpy(out + 4, &crc, 4);
}

// ---- Ring ------------------------------------------------------------------

namespace {
std::atomic<uint64_t> g_ring_bytes_override{0};
std::atomic<uint64_t> g_default_ring_bytes_override{0};
std::atomic<unsigned> g_stall_timeout_override{0};
std::atomic<int> g_fail_wal_syncs{0};
std::atomic<bool> g_wal_sync_failure_sticky{false};   // read on the failure path only
std::atomic<int> g_injected_wal_write_errno{0};   // read on the failure path only
std::atomic<int> g_fail_zero_range_syncs{0};        // read on the failure path only
std::atomic<int> g_fail_ring_zero_writes{0};        // read on the failure path only
std::atomic<int> g_fail_ring_zero_write_errno{EIO};
std::atomic<int> g_fail_header_writes{0};
std::atomic<int> g_fail_header_writes_sticky{0};
bool ConsumeCounter(std::atomic<int> &c) {
    int n = c.load(std::memory_order_relaxed);
    while (n > 0) {
        if (c.compare_exchange_weak(n, n - 1, std::memory_order_relaxed)) return true;
    }
    return false;
}
std::atomic<uint64_t> g_registry_epoch{0};
std::atomic<size_t> g_direct_min_override{0};
std::atomic<size_t> g_direct_pressure_min_override{0};
std::atomic<int> g_fail_direct_syncs{0};
std::atomic<int> g_throw_in_writer_apply{0};
}  // namespace

void SetRingBytesForTests(uint64_t ring_bytes) { g_ring_bytes_override.store(ring_bytes, std::memory_order_relaxed); }
void SetDefaultRingBytesForTests(uint64_t ring_bytes) { g_default_ring_bytes_override.store(ring_bytes, std::memory_order_relaxed); }
void FailNextWalSyncsForTests(int n) {
    g_wal_sync_failure_sticky.store(false, std::memory_order_relaxed);
    g_fail_wal_syncs.store(n, std::memory_order_relaxed);
}
void FailNextWalSyncsStickyForTests(int n) {
    g_wal_sync_failure_sticky.store(true, std::memory_order_relaxed);
    g_fail_wal_syncs.store(n, std::memory_order_relaxed);
}
void SetInjectedWalWriteErrnoForTests(int err) { g_injected_wal_write_errno.store(err, std::memory_order_relaxed); }
int InjectedWalWriteErrno() { return g_injected_wal_write_errno.load(std::memory_order_relaxed); }
void FailNextWalZeroRangeSyncsForTests(int n) { g_fail_zero_range_syncs.store(n, std::memory_order_relaxed); }
bool ConsumeInjectedWalZeroRangeSyncFailure() { return ConsumeCounter(g_fail_zero_range_syncs); }
void FailNextRingZeroWritesForTests(int n, int err) {
    g_fail_ring_zero_write_errno.store(err, std::memory_order_relaxed);
    g_fail_ring_zero_writes.store(n, std::memory_order_relaxed);
}
bool ConsumeInjectedRingZeroWriteFailure(int &err) {
    if (!ConsumeCounter(g_fail_ring_zero_writes)) return false;
    err = g_fail_ring_zero_write_errno.load(std::memory_order_relaxed);
    return true;
}
bool InjectedWalSyncFailureIsSticky() { return g_wal_sync_failure_sticky.load(std::memory_order_relaxed); }
void FailNextHeaderWritesForTests(int n) { g_fail_header_writes.store(n, std::memory_order_relaxed); }
void FailNextHeaderWritesStickyForTests(int n) { g_fail_header_writes_sticky.store(n, std::memory_order_relaxed); }
bool ConsumeInjectedWalSyncFailure() {
    int n = g_fail_wal_syncs.load(std::memory_order_relaxed);
    while (n > 0) {
        if (g_fail_wal_syncs.compare_exchange_weak(n, n - 1, std::memory_order_relaxed)) return true;
    }
    return false;
}
void SetDirectMinBytesForTests(size_t bytes) { g_direct_min_override.store(bytes, std::memory_order_relaxed); }
void SetDirectPressureMinBytesForTests(size_t bytes) { g_direct_pressure_min_override.store(bytes, std::memory_order_relaxed); }
size_t DirectMinBytes() {
    const size_t o = g_direct_min_override.load(std::memory_order_relaxed);
    return o != 0 ? o : XStoreTunables::WAL_DIRECT_MIN_BYTES;
}
size_t DirectPressureMinBytes() {
    const size_t o = g_direct_pressure_min_override.load(std::memory_order_relaxed);
    return o != 0 ? o : XStoreTunables::WAL_DIRECT_PRESSURE_MIN_BYTES;
}
void ThrowInNextWriterApplyForTests(int n) { g_throw_in_writer_apply.store(n, std::memory_order_relaxed); }
bool ConsumeInjectedWriterApplyThrow() { return ConsumeCounter(g_throw_in_writer_apply); }
void FailNextDirectSyncsForTests(int n) { g_fail_direct_syncs.store(n, std::memory_order_relaxed); }
bool ConsumeInjectedDirectSyncFailure() {
    int n = g_fail_direct_syncs.load(std::memory_order_relaxed);
    while (n > 0) {
        if (g_fail_direct_syncs.compare_exchange_weak(n, n - 1, std::memory_order_relaxed)) return true;
    }
    return false;
}

Route ChooseRoute(size_t payload_bytes, bool is_bypass, bool is_unary, bool pressure, bool ring_clear, size_t ring_bytes) {
    if (is_bypass || is_unary) return Route::kRing;
    if (payload_bytes > ring_bytes / 2) return ring_clear ? Route::kDirect : Route::kRing;   // not clear: the writer forces cycles first
    const size_t threshold = pressure ? DirectPressureMinBytes() : DirectMinBytes();
    return (payload_bytes >= threshold && ring_clear) ? Route::kDirect : Route::kRing;
}

bool RingPressure() {
    for (const std::shared_ptr<Ring> &ring : AllRings()) {
        if (ring->Fill() >= XStoreTunables::WAL_CKPT_CYCLE_FILL) return true;
    }
    return false;
}
uint64_t RegistryEpoch() { return g_registry_epoch.load(std::memory_order_relaxed); }
void SetStallTimeoutMsForTests(unsigned ms) { g_stall_timeout_override.store(ms, std::memory_order_relaxed); }

uint64_t DefaultRingBytes() {
    if (const uint64_t o = g_ring_bytes_override.load(std::memory_order_relaxed); o != 0) return o;
    if (const uint64_t d = g_default_ring_bytes_override.load(std::memory_order_relaxed); d != 0) return d;
    return XStoreTunables::WAL_RING_BYTES;
}

unsigned StallTimeoutMs() {
    const unsigned o = g_stall_timeout_override.load(std::memory_order_relaxed);
    return o != 0 ? o : XStoreTunables::WAL_STALL_TIMEOUT_MS;
}

Ring::Ring(int fd, int hdr_fd, std::string path, uint32_t shard_id, uint64_t ring_bytes, uint64_t start_lsn)
    : fd_(fd),
      hdr_fd_(hdr_fd),
      path_(std::move(path)),
      shard_id_(shard_id),
      ring_bytes_(ring_bytes),
      stall_limit_bytes_(static_cast<uint64_t>(static_cast<double>(ring_bytes) * XStoreTunables::WAL_STALL_FILL)),
      head_(start_lsn),
      applied_(start_lsn),
      checkpoint_(start_lsn),
      slot_seq_(0) {
    RingHeader h;
    slot_seq_ = ReadBestHeader(fd_, h) ? h.slot_seq : 0;
}

Ring::~Ring() {
    if (fd_ >= 0) close(fd_);
    if (hdr_fd_ >= 0) close(hdr_fd_);
}

bool Ring::WriteHeader(const RingHeader &header, HeaderWriteStatus &status) {
    status = HeaderWriteStatus{};
    bool ok;
    if (ConsumeCounter(g_fail_header_writes_sticky)) {
        ok = false; status.real = true; status.err = EIO;
    } else if (ConsumeCounter(g_fail_header_writes)) {
        ok = false; status.err = EIO;   // injected: retryable
    } else {
        errno = 0;
        ok = WriteHeaderSlot(hdr_fd_, header);
        if (!ok) { status.real = true; status.err = errno != 0 ? errno : EIO; }
    }
    status.failed = !ok;
    return ok;
}

// The checkpoint only moves forward and only the single writer moves head, so a
// plan that passes here cannot be invalidated by a concurrent AdvanceCheckpoint.
// Soft limit: STALL_FILL of the ring. An EMPTY ring always admits the record
// (a record <= ring/2 plus its pad < ring_bytes, so it cannot lap itself).
bool Ring::TryPlan(size_t bytes, uint64_t &start_lsn, size_t &pad_bytes) const {
    const uint64_t head = head_.load(std::memory_order_relaxed);
    const uint64_t ckpt = checkpoint_.load(std::memory_order_acquire);
    start_lsn = PlanAppend(head, ring_bytes_, bytes, pad_bytes);
    const uint64_t used_after = start_lsn + bytes - ckpt;
    return used_after <= stall_limit_bytes_ || (head == ckpt && used_after <= ring_bytes_);
}

bool Ring::Reserve(size_t bytes, std::chrono::milliseconds timeout, uint64_t &start_lsn, size_t &pad_bytes) {
    if (bytes > ring_bytes_) return false;
    if (TryPlan(bytes, start_lsn, pad_bytes)) return true;   // no-stall fast path: lock-free
    std::unique_lock<std::mutex> lock(mu_);
    return cv_.wait_for(lock, timeout, [&] { return TryPlan(bytes, start_lsn, pad_bytes); });
}

void Ring::CommitAppended(uint64_t end_lsn) { head_.store(end_lsn, std::memory_order_release); }
void Ring::PublishApplied(uint64_t end_lsn) { applied_.store(end_lsn, std::memory_order_release); }

double Ring::Fill() const {
    const uint64_t ckpt = CheckpointLsn();
    const uint64_t head = HeadLsn();
    return static_cast<double>(head - std::min(head, ckpt)) / static_cast<double>(ring_bytes_);
}

void Ring::AdvanceCheckpoint(uint64_t lsn, HeaderWriteStatus *status) {
    if (status) *status = HeaderWriteStatus{};
    std::lock_guard<std::mutex> lock(mu_);
    if (lsn <= checkpoint_.load(std::memory_order_relaxed)) return;
    if (lsn > head_.load(std::memory_order_acquire)) {
        spdlog::error("[WAL] - Refusing to checkpoint {} beyond head {} in {}", lsn, HeadLsn(), path_);
        return;
    }
    RingHeader h;
    h.shard_id = shard_id_;
    h.ring_bytes = ring_bytes_;
    h.checkpoint_lsn = lsn;
    h.slot_seq = slot_seq_ + 1;
    HeaderWriteStatus local;
    if (!WriteHeader(h, status ? *status : local)) {
        spdlog::error("[WAL] - Failed to persist checkpoint {} for {}", lsn, path_);
        return;
    }
    slot_seq_ = h.slot_seq;
    checkpoint_.store(lsn, std::memory_order_release);
    cv_.notify_all();
}

namespace {
// Set by the first NoteUnattributedThrough in the process and never cleared (except by
// ClearRingRegistryForTests): RingClear only pays for the registry-mutex walk after a writer exception.
// Release/acquire (stored before the per-ring mark): the writer's own later RingClear is same-thread, and the
// mark is set before PublishApplied, so the checkpoint cannot pass a marked record before the gate is visible
// to the checkpoint-acquiring side; other shards need no mutex to read it.
std::atomic<bool> g_any_unattributed_marked{false};
}  // namespace

void Ring::NoteUnattributedThrough(uint64_t lsn) noexcept {
    g_any_unattributed_marked.store(true, std::memory_order_release);
    uint64_t cur = unattributed_.load(std::memory_order_relaxed);
    while (cur < lsn && !unattributed_.compare_exchange_weak(cur, lsn, std::memory_order_release, std::memory_order_relaxed)) {}
}

void Ring::ResetTo(uint64_t lsn) {
    std::lock_guard<std::mutex> lock(mu_);
    unattributed_.store(0, std::memory_order_release);
    head_.store(lsn, std::memory_order_release);
    applied_.store(lsn, std::memory_order_release);
    checkpoint_.store(lsn, std::memory_order_release);
    cv_.notify_all();
}

bool Ring::CommitReplay(uint64_t end_lsn, HeaderWriteStatus *status) {
    if (status) *status = HeaderWriteStatus{};
    std::lock_guard<std::mutex> lock(mu_);
    if (end_lsn <= checkpoint_.load(std::memory_order_relaxed)) return true;
    RingHeader best;
    if (ReadBestHeader(hdr_fd_, best)) slot_seq_ = std::max(slot_seq_, best.slot_seq);
    RingHeader h;
    h.shard_id = shard_id_;
    h.ring_bytes = ring_bytes_;
    h.checkpoint_lsn = end_lsn;
    h.slot_seq = slot_seq_ + 1;
    HeaderWriteStatus local;
    if (!WriteHeader(h, status ? *status : local)) {
        spdlog::error("[WAL REPLAY] - Failed to persist checkpoint {} for {}", end_lsn, path_);
        return false;
    }
    slot_seq_ = h.slot_seq;
    if (head_.load(std::memory_order_relaxed) < end_lsn) head_.store(end_lsn, std::memory_order_release);
    if (applied_.load(std::memory_order_relaxed) < end_lsn) applied_.store(end_lsn, std::memory_order_release);
    checkpoint_.store(end_lsn, std::memory_order_release);
    cv_.notify_all();
    return true;
}

// ---- Registry ---------------------------------------------------------------

namespace {
std::mutex g_registry_mutex;
std::unordered_map<std::string, std::shared_ptr<Ring>> g_rings;
std::unordered_set<std::string> g_fenced;   // normalized WAL paths; guarded by g_registry_mutex
std::atomic<size_t> g_fenced_count{0};      // == g_fenced.size(), republished under g_registry_mutex after every change

std::string NormalizedKey(const std::string &wal_path) {
    return std::filesystem::path(wal_path).lexically_normal().string();
}

// Walks the registry in place (no copy, no allocation); the registry mutex is never held with the tracker's.
bool AnyUnattributedMark() {
    if (!g_any_unattributed_marked.load(std::memory_order_acquire)) return false;
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    for (const auto &entry : g_rings) {
        if (entry.second->UnattributedThrough() > entry.second->CheckpointLsn()) return true;
    }
    return false;
}
}  // namespace

void FenceWalFile(const std::string &wal_path) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    const std::string key = NormalizedKey(wal_path);
    g_fenced.insert(key);
    g_fenced_count.store(g_fenced.size(), std::memory_order_relaxed);
    // Drop the live Ring (if any) and make the writers drop their cached copies.
    if (g_rings.erase(key) != 0) g_registry_epoch.fetch_add(1, std::memory_order_relaxed);
}
void UnfenceWalFile(const std::string &wal_path) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_fenced.erase(NormalizedKey(wal_path));
    g_fenced_count.store(g_fenced.size(), std::memory_order_relaxed);
}
bool IsFenced(const std::string &wal_path) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    return g_fenced.count(NormalizedKey(wal_path)) != 0;
}
bool AnyFenced() { return g_fenced_count.load(std::memory_order_relaxed) != 0; }

namespace {
std::atomic<uint64_t> g_replay_zeroed_bytes{0};
}
namespace {
std::atomic<int> g_fail_replay_reads{0};
std::atomic<int> g_fail_replay_zeroings{0};
std::atomic<bool> g_stop_replay_zeroing_after_tail{false};
}  // namespace
void FailNextReplayReadsForTests(int n) { g_fail_replay_reads.store(n, std::memory_order_relaxed); }
bool ConsumeInjectedReplayReadFailure() { return ConsumeCounter(g_fail_replay_reads); }
void FailNextReplayZeroingsForTests(int n) { g_fail_replay_zeroings.store(n, std::memory_order_relaxed); }
bool ConsumeInjectedReplayZeroingFailure() { return ConsumeCounter(g_fail_replay_zeroings); }
void StopReplayZeroingAfterTailForTests(bool stop) { g_stop_replay_zeroing_after_tail.store(stop, std::memory_order_relaxed); }
bool ReplayStopsAfterTailZeroing() { return g_stop_replay_zeroing_after_tail.load(std::memory_order_relaxed); }
uint64_t ReplayZeroedBytesForTests() { return g_replay_zeroed_bytes.load(std::memory_order_relaxed); }
void CountReplayZeroedBytes(uint64_t bytes) { g_replay_zeroed_bytes.fetch_add(bytes, std::memory_order_relaxed); }

bool ZeroRingRange(int fd, uint64_t from_lsn, uint64_t to_lsn, uint64_t ring_bytes, int *errno_out) {
    // Heap-allocated and leaked on purpose: a function-local static vector would be destroyed by exit()'s
    // static destructors before the atexit hook has joined the writers / checkpointer (same bug class as
    // XStorePreadPool's ticket queue).
    static const std::vector<char> *const zeros = new std::vector<char>(1u << 20, 0);
    for (uint64_t lsn = from_lsn; lsn < to_lsn;) {
        const uint64_t lap_left = ring_bytes - lsn % ring_bytes;
        const size_t n = static_cast<size_t>(std::min<uint64_t>({zeros->size(), to_lsn - lsn, lap_left}));
        if (!PwriteAll(fd, zeros->data(), n, static_cast<off_t>(RingOffset(lsn, ring_bytes)))) {
            if (errno_out != nullptr) *errno_out = errno;   // PwriteAll returns straight after the failed pwrite
            return false;
        }
        lsn += n;
    }
    return true;
}

std::shared_ptr<Ring> GetOrOpenRing(const std::string &wal_path, uint32_t shard_id, bool reinit_legacy) {
    // One ring per file no matter how the path is spelled ("a//b", "a/./b", ...):
    // two Rings over one file would each think they own the head.
    const std::string key = std::filesystem::path(wal_path).lexically_normal().string();
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    if (g_fenced.count(key) != 0) return nullptr;   // replay failed: must replay again, nothing may append or wipe
    if (auto it = g_rings.find(key); it != g_rings.end()) return it->second;

    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(wal_path).parent_path(), ec);
    const int fd = open(wal_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        spdlog::error("[WAL] - Unable to open {}", wal_path);
        return nullptr;
    }

    RingHeader h;
    uint64_t ring_bytes = 0, start_lsn = 0;
    if (ReadBestHeader(fd, h)) {
        ring_bytes = h.ring_bytes;
        start_lsn = h.checkpoint_lsn;
    } else {
        struct stat st{};
        if (fstat(fd, &st) != 0) { close(fd); return nullptr; }
        if (st.st_size != 0) {
            // Legacy WAL (possibly still awaiting replay): never initialize it behind ReplayWal's back.
            if (!reinit_legacy || ftruncate(fd, 0) != 0) { close(fd); return nullptr; }
        }
        ring_bytes = DefaultRingBytes();
        if (!InitRingFile(fd, shard_id, ring_bytes)) {
            spdlog::error("[WAL] - Unable to initialize ring file {}", wal_path);
            close(fd);
            return nullptr;
        }
        // fsync of the file does not make its directory entry durable: a brand-new
        // shard_N.WAL could vanish in a crash together with every record in it.
        if (!FsyncDirectory(std::filesystem::path(wal_path).parent_path().string())) {
            spdlog::error("[WAL] - Unable to fsync the directory of {}", wal_path);
            close(fd);
            return nullptr;
        }
    }
    // A second, independent open file description for the header writes (see Ring): opened AFTER init so
    // it never sees an error from the zero-fill. Failing to open it fails the ring.
    const int hdr_fd = open(wal_path.c_str(), O_RDWR | O_CLOEXEC);
    if (hdr_fd < 0) {
        spdlog::error("[WAL] - Unable to open a header descriptor for {}: {}", wal_path, std::strerror(errno));
        close(fd);
        return nullptr;
    }
    auto ring = std::make_shared<Ring>(fd, hdr_fd, wal_path, shard_id, ring_bytes, start_lsn);
    g_rings.emplace(key, ring);
    return ring;
}

std::vector<std::shared_ptr<Ring>> AllRings() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    std::vector<std::shared_ptr<Ring>> all;
    all.reserve(g_rings.size());
    for (const auto &entry : g_rings) all.push_back(entry.second);
    return all;
}

void ClearRingRegistryForTests() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_rings.clear();
    g_fenced.clear();
    g_fenced_count.store(0, std::memory_order_relaxed);
    g_any_unattributed_marked.store(false, std::memory_order_relaxed);
    g_registry_epoch.fetch_add(1, std::memory_order_relaxed);
}

// ---- Dirty tracking ---------------------------------------------------------

void DirtyTracker::AddMark(FileState &file, const std::shared_ptr<Ring> &ring, uint64_t ring_lsn_end) {
    bool found = false;
    for (RingMark &m : file.marks) {
        if (m.key != ring.get()) continue;
        if (m.ring.owner_before(ring) || ring.owner_before(m.ring)) {
            // Recycled address: the old Ring is gone and this is a NEW one, whose LSNs have nothing to do
            // with the old mark's -- start over instead of taking the max. (Same control block = no
            // weak-count churn on the normal path below.)
            m.ring = ring;
            m.last_lsn = ring_lsn_end;
        } else if (ring_lsn_end > m.last_lsn) {
            m.last_lsn = ring_lsn_end;
        }
        found = true;
        break;
    }
    if (!found) file.marks.push_back(RingMark{ring.get(), ring, ring_lsn_end});
}

void DirtyTracker::NoteRingMarkOnly(const std::shared_ptr<Ring> &ring, const std::string &data_path, uint64_t ring_lsn_end) {
    if (!ring || ring_lsn_end == 0) return;
    std::lock_guard<std::mutex> lock(mu_);
    AddMark(files_[data_path], ring, ring_lsn_end);
}

void DirtyTracker::Note(const std::shared_ptr<Ring> &ring, const std::string &data_path, uint64_t begin, uint64_t end, uint64_t ring_lsn_end) {
    if (end <= begin) return;
    std::lock_guard<std::mutex> lock(mu_);
    if (!have_oldest_) {
        have_oldest_ = true;
        oldest_ = std::chrono::steady_clock::now();
    }
    FileState &file = files_[data_path];   // allocates only the first time a path is seen

    if (ring && ring_lsn_end != 0) AddMark(file, ring, ring_lsn_end);

    // Merge [begin, end) into the sorted disjoint map. The common cases
    // (extend the previous range / fall inside it) edit a node in place.
    auto &ranges = file.ranges;
    auto next = ranges.upper_bound(begin);
    std::map<uint64_t, uint64_t>::iterator target;
    if (next != ranges.begin() && std::prev(next)->second >= begin) {
        target = std::prev(next);
        if (end > target->second) {
            bytes_ += end - target->second;
            target->second = end;
        }
    } else {
        target = ranges.emplace_hint(next, begin, end);
        bytes_ += end - begin;
    }
    // Absorb following ranges the (possibly extended) target now reaches.
    for (auto it = std::next(target); it != ranges.end() && it->first <= target->second; it = ranges.erase(it)) {
        const uint64_t len = it->second - it->first;
        bytes_ -= len;
        if (it->second > target->second) {
            bytes_ += it->second - target->second;
            target->second = it->second;
        }
    }
}

DirtyTracker::Snapshot DirtyTracker::Take() {
    Snapshot snap;
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = files_.begin(); it != files_.end(); ) {
        FileState &file = it->second;
        if (!file.ranges.empty()) {
            std::vector<DirtyRange> out;
            out.reserve(file.ranges.size());
            for (const auto &r : file.ranges) out.push_back(DirtyRange{r.first, r.second});
            snap.ranges.emplace(it->first, std::move(out));
            file.ranges.clear();
        }
        it = file.marks.empty() ? files_.erase(it) : std::next(it);
    }
    bytes_ = 0;
    have_oldest_ = false;
    return snap;
}

DirtyTracker::Snapshot DirtyTracker::PeekRanges() const {
    Snapshot snap;
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto &entry : files_) {
        if (entry.second.ranges.empty()) continue;
        std::vector<DirtyRange> out;
        out.reserve(entry.second.ranges.size());
        for (const auto &r : entry.second.ranges) out.push_back(DirtyRange{r.first, r.second});
        snap.ranges.emplace(entry.first, std::move(out));
    }
    return snap;
}

bool DirtyTracker::RingClear(const std::string &data_path) const {
    if (AnyUnattributedMark()) return false;
    std::lock_guard<std::mutex> lock(mu_);
    const auto it = files_.find(data_path);
    if (it == files_.end()) return true;
    for (const RingMark &m : it->second.marks) {
        const std::shared_ptr<Ring> ring = m.ring.lock();
        if (ring && m.last_lsn > ring->CheckpointLsn()) return false;
    }
    return true;
}

void DirtyTracker::NoteCheckpointed(const Ring *ring, uint64_t checkpoint_lsn) {
    std::lock_guard<std::mutex> lock(mu_);
    for (auto it = files_.begin(); it != files_.end(); ) {
        auto &marks = it->second.marks;
        marks.erase(std::remove_if(marks.begin(), marks.end(),
                                   [&](const RingMark &m) { return m.key == ring && m.last_lsn <= checkpoint_lsn; }),
                    marks.end());
        it = (marks.empty() && it->second.ranges.empty()) ? files_.erase(it) : std::next(it);
    }
}

uint64_t DirtyTracker::OldestUnsyncedAgeMs() const {
    std::lock_guard<std::mutex> lock(mu_);
    if (!have_oldest_) return 0;
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - oldest_).count());
}

uint64_t DirtyTracker::UnflushedBytes() const {
    std::lock_guard<std::mutex> lock(mu_);
    return bytes_;
}

void DirtyTracker::ClearForTests() {
    std::lock_guard<std::mutex> lock(mu_);
    files_.clear();
    bytes_ = 0;
    have_oldest_ = false;
}

DirtyTracker &Dirty() {
    static DirtyTracker *tracker = new DirtyTracker();   // leaked on purpose: see header
    return *tracker;
}

void ClearDirtyForTests() { Dirty().ClearForTests(); }

}  // namespace walring
