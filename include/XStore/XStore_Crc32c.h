#pragma once
#include <cstddef>
#include <cstdint>

// CRC32C (Castagnoli, polynomial 0x1EDC6F41 / reflected 0x82F63B78) --
// hardware-accelerated via the SSE4.2 CRC32 instruction (_mm_crc32_u64) on x86
// or the ARMv8 CRC32C instructions (__crc32cd) on aarch64 Linux, when the
// running CPU supports it (checked once, cached), falling back to a software table-driven implementation of the
// same polynomial otherwise. This is XStore's WAL record framing checksum
// (see XStore_Wal.cpp) -- not the standard zlib/IEEE 802.3 CRC-32, and not
// interoperable with it. It's only ever written and read back by XStore
// itself (never a cross-system wire format), so the polynomial choice is
// free to prioritize speed.
//
// Split into Init/Update/Final so a record spread across multiple
// non-contiguous buffers can be CRC'd without concatenating them first --
// call Update once per piece in wire order, threading the running crc
// value through. Crc32c() is the single-buffer convenience wrapper.
uint32_t Crc32cInit();
uint32_t Crc32cUpdate(uint32_t crc, const void *data, size_t len);
uint32_t Crc32cFinal(uint32_t crc);
uint32_t Crc32c(const void *data, size_t len);

// Reference software-table update, bypassing hardware dispatch -- exists so
// tests can assert the hardware and software paths agree; production code
// should call Crc32cUpdate/Crc32c above, not this directly.
uint32_t Crc32cSoftwareUpdate(uint32_t crc, const void *data, size_t len);
