// xstore::ChunkPool (include/XStore/XStore_ChunkPool.h): the 512 KiB buffers a streamed reply is cut into are kept for reuse instead of being allocated
// and freed for every chunk (a fresh buffer means fresh pages for the kernel to zero: about 6% of the server's CPU on big reads). Buffers go back from the
// network layer's thread after they were written to the connection.
#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "include/XStore/XStore_ChunkPool.h"

namespace {
using xstore::ChunkPool;
constexpr size_t kChunk = 512 * 1024 + 4096;

TEST(ChunkPoolTest, TakeGivesAnEmptyBufferWithTheRoomAsked) {
    ChunkPool pool;
    const std::string buffer = pool.Take(kChunk);
    EXPECT_TRUE(buffer.empty());
    EXPECT_GE(buffer.capacity(), kChunk);
}

TEST(ChunkPoolTest, ABufferPutBackIsTheOneTakenNextAndComesBackEmpty) {
    ChunkPool pool;
    std::string buffer = pool.Take(kChunk);
    buffer.assign(100000, 'x');
    const char *where = buffer.data();
    pool.Put(std::move(buffer));
    const std::string again = pool.Take(kChunk);
    EXPECT_EQ(again.data(), where) << "a new buffer was allocated although one was kept";
    EXPECT_TRUE(again.empty()) << "the old content must not come back";
    EXPECT_GE(again.capacity(), kChunk);
}

TEST(ChunkPoolTest, ABufferTooSmallForTheRequestStaysAndANewOneIsGiven) {
    ChunkPool pool;
    std::string small(64 * 1024, 'x');
    small.reserve(100 * 1024);
    pool.Put(std::move(small));
    const std::string big = pool.Take(kChunk);
    EXPECT_GE(big.capacity(), kChunk);
    EXPECT_EQ(pool.KeptCount(), 1u) << "the small one is still there for a smaller request";
}

TEST(ChunkPoolTest, ThePoolKeepsAtMostAFixedNumberOfBuffers) {
    ChunkPool pool;
    for (size_t i = 0; i < ChunkPool::kMaxKept + 50; ++i) {
        std::string buffer;
        buffer.reserve(kChunk);
        pool.Put(std::move(buffer));
    }
    EXPECT_EQ(pool.KeptCount(), ChunkPool::kMaxKept);
}

TEST(ChunkPoolTest, TinyBuffersAreNotKept) {
    ChunkPool pool;
    pool.Put(std::string("short"));
    std::string small;
    small.reserve(1000);
    pool.Put(std::move(small));
    EXPECT_EQ(pool.KeptCount(), 0u);
}

TEST(ChunkPoolTest, AHugeBufferIsNotKeptForASmallRequest) {
    ChunkPool pool;
    std::string huge;
    huge.reserve(64u << 20);
    pool.Put(std::move(huge));
    EXPECT_EQ(pool.KeptCount(), 0u) << "keeping a 64 MiB buffer for 512 KiB chunks would pin memory for nothing";
}

TEST(ChunkPoolTest, BuffersHandedOutAreNeverSharedBetweenThreads) {
    ChunkPool pool;
    std::atomic<bool> clash{false};
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < 4000; ++i) {
                std::string buffer = pool.Take(kChunk);
                buffer.assign(1000, static_cast<char>('a' + t));
                // another thread holding the same storage would overwrite it
                for (int spin = 0; spin < 50; ++spin) {
                    if (buffer.find_first_not_of(static_cast<char>('a' + t)) != std::string::npos) clash.store(true);
                }
                pool.Put(std::move(buffer));
            }
        });
    }
    for (auto &t : threads) t.join();
    EXPECT_FALSE(clash.load());
    EXPECT_LE(pool.KeptCount(), ChunkPool::kMaxKept);
}
}  // namespace
