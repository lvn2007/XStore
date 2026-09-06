// src/XStore/XStore_Crc32c.cpp
#include "include/XStore/XStore_Crc32c.h"
#include <array>
#include <cstring>

#if defined(__x86_64__) || defined(__i386__)
#include <nmmintrin.h>
#define XSTORE_CRC32C_X86 1
#endif

namespace {

// Reflected CRC32C (Castagnoli, polynomial 0x82F63B78), byte-at-a-time
// table lookup -- correct on every platform, used as the fallback when the
// running CPU lacks SSE4.2 and as the cross-check reference in tests.
uint32_t Crc32cSoftwareTableUpdate(uint32_t crc, const void *data, size_t len) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1u) ? (0x82F63B78u ^ (c >> 1)) : (c >> 1);
            }
            t[i] = c;
        }
        return t;
    }();

    const auto *bytes = static_cast<const unsigned char *>(data);
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc;
}

#if XSTORE_CRC32C_X86
// target("sse4.2") lets this one function use the CRC32 instruction even
// though the project isn't built with -msse4.2/-march=native globally --
// Crc32cHardwareSupported() below gates the call at runtime so this code
// path never executes on a CPU that doesn't support it.
__attribute__((target("sse4.2")))
uint32_t Crc32cHardwareUpdate(uint32_t crc, const void *data, size_t len) {
    const auto *bytes = static_cast<const unsigned char *>(data);
    uint64_t crc64 = crc;

    while (len >= 8) {
        uint64_t chunk;
        std::memcpy(&chunk, bytes, 8);
        crc64 = _mm_crc32_u64(crc64, chunk);
        bytes += 8;
        len -= 8;
    }
    auto crc32 = static_cast<uint32_t>(crc64);
    if (len >= 4) {
        uint32_t chunk;
        std::memcpy(&chunk, bytes, 4);
        crc32 = _mm_crc32_u32(crc32, chunk);
        bytes += 4;
        len -= 4;
    }
    while (len > 0) {
        crc32 = _mm_crc32_u8(crc32, *bytes);
        ++bytes;
        --len;
    }
    return crc32;
}

bool Crc32cHardwareSupported() {
    static const bool supported = __builtin_cpu_supports("sse4.2");
    return supported;
}
#endif

} // namespace

uint32_t Crc32cInit() { return 0xFFFFFFFFu; }

uint32_t Crc32cUpdate(uint32_t crc, const void *data, size_t len) {
#if XSTORE_CRC32C_X86
    if (Crc32cHardwareSupported()) return Crc32cHardwareUpdate(crc, data, len);
#endif
    return Crc32cSoftwareTableUpdate(crc, data, len);
}

uint32_t Crc32cFinal(uint32_t crc) { return crc ^ 0xFFFFFFFFu; }

uint32_t Crc32c(const void *data, size_t len) {
    return Crc32cFinal(Crc32cUpdate(Crc32cInit(), data, len));
}

uint32_t Crc32cSoftwareUpdate(uint32_t crc, const void *data, size_t len) {
    return Crc32cSoftwareTableUpdate(crc, data, len);
}
