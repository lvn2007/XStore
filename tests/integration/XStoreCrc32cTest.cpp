// tests/integration/XStoreCrc32cTest.cpp
#include <gtest/gtest.h>
#include "include/XStore/XStore_Crc32c.h"
#include <cstdlib>
#include <string>
#include <vector>

// Unit tests for XStore's hardware-accelerated CRC32C (Castagnoli), the WAL
// record framing checksum implemented in XStore_Crc32c.cpp. Pure function,
// no XStore/filesystem state needed -- unlike the rest of tests/integration.

TEST(XStoreCrc32cTest, KnownCheckVectorMatchesStandardCrc32c) {
    // "123456789" is the standard CRC check-vector string; 0xE3069283 is the
    // published check value for CRC-32C/Castagnoli specifically (distinct
    // from the 0xCBF43926 check value for CRC-32 IEEE/zlib).
    const std::string data = "123456789";
    EXPECT_EQ(Crc32c(data.data(), data.size()), 0xE3069283u);
}

TEST(XStoreCrc32cTest, EmptyInputIsZero) {
    EXPECT_EQ(Crc32c(nullptr, 0), 0u);
}

TEST(XStoreCrc32cTest, StreamingAcrossMultiplePiecesMatchesSingleBufferOverSameData) {
    std::string data(97, '\0');
    for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 7 + 3);

    uint32_t whole = Crc32c(data.data(), data.size());

    // Odd, non-power-of-two split sizes on purpose -- exercises the
    // hardware path's 8-byte-chunk/4-byte/1-byte tail handling at every
    // possible remainder boundary, not just aligned splits.
    uint32_t crc = Crc32cInit();
    crc = Crc32cUpdate(crc, data.data(), 1);
    crc = Crc32cUpdate(crc, data.data() + 1, 6);
    crc = Crc32cUpdate(crc, data.data() + 7, 3);
    crc = Crc32cUpdate(crc, data.data() + 10, data.size() - 10);
    crc = Crc32cFinal(crc);

    EXPECT_EQ(crc, whole);
}

TEST(XStoreCrc32cTest, DispatchedPathAgreesWithSoftwareReferenceAcrossTailLengths) {
    // Lengths 0..71 walk every possible remainder mod 8 and mod 4 so the
    // hardware path's tail loop (u64 chunks -> u32 -> u8) is checked against
    // the software table implementation at each boundary.
    std::vector<unsigned char> data(71);
    for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<unsigned char>(i * 31 + 11);

    for (size_t len = 0; len <= data.size(); ++len) {
        uint32_t dispatched = Crc32cFinal(Crc32cUpdate(Crc32cInit(), data.data(), len));
        uint32_t reference = Crc32cFinal(Crc32cSoftwareUpdate(Crc32cInit(), data.data(), len));
        ASSERT_EQ(dispatched, reference) << "mismatch at len=" << len;
    }
}
