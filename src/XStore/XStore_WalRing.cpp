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
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unordered_map>
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
std::atomic<uint64_t> g_registry_epoch{0};
}  // namespace

void SetRingBytesForTests(uint64_t ring_bytes) { g_ring_bytes_override.store(ring_bytes, std::memory_order_relaxed); }
void SetDefaultRingBytesForTests(uint64_t ring_bytes) { g_default_ring_bytes_override.store(ring_bytes, std::memory_order_relaxed); }
void FailNextWalSyncsForTests(int n) { g_fail_wal_syncs.store(n, std::memory_order_relaxed); }
bool ConsumeInjectedWalSyncFailure() {
    int n = g_fail_wal_syncs.load(std::memory_order_relaxed);
    while (n > 0) {
        if (g_fail_wal_syncs.compare_exchange_weak(n, n - 1, std::memory_order_relaxed)) return true;
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

Ring::Ring(int fd, std::string path, uint32_t shard_id, uint64_t ring_bytes, uint64_t start_lsn)
    : fd_(fd),
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

void Ring::AdvanceCheckpoint(uint64_t lsn) {
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
    if (!WriteHeaderSlot(fd_, h)) {
        spdlog::error("[WAL] - Failed to persist checkpoint {} for {}", lsn, path_);
        return;
    }
    slot_seq_ = h.slot_seq;
    checkpoint_.store(lsn, std::memory_order_release);
    cv_.notify_all();
}

void Ring::ResetTo(uint64_t lsn) {
    std::lock_guard<std::mutex> lock(mu_);
    head_.store(lsn, std::memory_order_release);
    applied_.store(lsn, std::memory_order_release);
    checkpoint_.store(lsn, std::memory_order_release);
    cv_.notify_all();
}

// ---- Registry ---------------------------------------------------------------

namespace {
std::mutex g_registry_mutex;
std::unordered_map<std::string, std::shared_ptr<Ring>> g_rings;
}  // namespace

std::shared_ptr<Ring> GetOrOpenRing(const std::string &wal_path, uint32_t shard_id, bool reinit_legacy) {
    // One ring per file no matter how the path is spelled ("a//b", "a/./b", ...):
    // two Rings over one file would each think they own the head.
    const std::string key = std::filesystem::path(wal_path).lexically_normal().string();
    std::lock_guard<std::mutex> lock(g_registry_mutex);
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
    }
    auto ring = std::make_shared<Ring>(fd, wal_path, shard_id, ring_bytes, start_lsn);
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
    g_registry_epoch.fetch_add(1, std::memory_order_relaxed);
}

}  // namespace walring
