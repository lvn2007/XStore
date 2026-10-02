#pragma once
// WAL ring file format (Durability C). One file per WAL shard:
//   [4096-byte header region: two 512-byte slots, ping-pong by slot_seq % 2]
//   [ring_bytes of ring, addressed by LSN: file offset = 4096 + lsn % ring_bytes]
// LSNs are monotonically increasing byte positions in the logical log; a
// record's stored LSN lets replay reject stale bytes left by a previous lap.
#include <cstddef>
#include <cstdint>

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
// posix_fallocate header+ring, zero-fill the ring in 4 MiB writes (so extents
// are "written", not unwritten), write slot_seq=1 ckpt=0, fsync.
[[nodiscard]] bool InitRingFile(int fd, uint32_t shard_id, uint64_t ring_bytes);

inline uint64_t RingOffset(uint64_t lsn, uint64_t ring_bytes) { return HEADER_REGION_BYTES + lsn % ring_bytes; }

// Where a record of `total_bytes` goes when the log head is at `head_lsn`. Never straddles the ring end.
// Returns the LSN the record starts at; the ring-end bytes consumed before it are `start - head_lsn`.
// `pad_bytes` = size of the PAD record to write at head_lsn (0 if skipped silently or no wrap).
[[nodiscard]] uint64_t PlanAppend(uint64_t head_lsn, uint64_t ring_bytes, size_t total_bytes, size_t &pad_bytes);

// Writes a PAD record of exactly `pad_bytes` (>= RECORD_FIXED_BYTES) into `out`.
void BuildPad(char *out, size_t pad_bytes, uint64_t lsn);

}  // namespace walring
