// src/XStore/XStore_WalRing.cpp
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_Crc32c.h"
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
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
    RingHeader h;
    h.shard_id = shard_id;
    h.ring_bytes = ring_bytes;
    h.checkpoint_lsn = 0;
    h.slot_seq = 1;
    if (!WriteHeaderSlot(fd, h)) return false;
    return fsync(fd) == 0;
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
    std::memset(out, 0, pad_bytes);
    const uint32_t record_len = static_cast<uint32_t>(pad_bytes - 4);
    std::memcpy(out, &record_len, 4);
    std::memcpy(out + 8, &lsn, 8);
    const uint16_t marker = PAD_DB_NAME_LEN;
    std::memcpy(out + 16, &marker, 2);
    const uint32_t crc = Crc32c(out + 8, pad_bytes - 8);
    std::memcpy(out + 4, &crc, 4);
}

}  // namespace walring
