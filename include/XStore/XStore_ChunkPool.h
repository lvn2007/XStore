#pragma once
// Buffers for the chunks of a streamed reply (512 KiB each), kept for reuse. A chunk is built in a std::string, handed to the network layer without a
// copy, and freed from the network layer's thread once it was written to the connection; allocating and freeing one per chunk means fresh pages for the
// kernel to zero (about 6% of the server's CPU on big reads, perf). The network layer Put()s the buffer back instead of freeing it, and the encoders Take() it.
//
// At most kMaxKept buffers are kept (64 MiB at most, not counted in the memory pool; on a small machine a 256th of the pool's limit, KeptLimitFor), and
// only ones whose capacity is about a chunk's: a tiny buffer is cheap to allocate and a huge one would pin memory for nothing.
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "include/XStore/XStore_MemoryPool.h"

namespace xstore {

class ChunkPool {
public:
    static constexpr size_t kMaxKept = 128;
    static constexpr size_t kMinKeptCapacity = 64 * 1024;
    static constexpr size_t kMaxKeptCapacity = 8 * 1024 * 1024;

    static constexpr size_t kBufferBytes = 512 * 1024;

    explicit ChunkPool(size_t max_kept = kMaxKept) : max_kept_(max_kept) {}

    // How many buffers a process whose memory pool has `pool_limit_bytes` keeps: a 256th of the limit, between 4 MiB and 64 MiB worth.
    static constexpr size_t KeptLimitFor(uint64_t pool_limit_bytes) {
        constexpr uint64_t kFloor = 4ull * 1024 * 1024;
        constexpr uint64_t kCeiling = kMaxKept * kBufferBytes;
        return static_cast<size_t>(std::clamp<uint64_t>(pool_limit_bytes / 256, kFloor, kCeiling) / kBufferBytes);
    }

    // The pool the encoders and the network layer share. Never destroyed: a buffer may come back from a network thread during shutdown.
    static ChunkPool &Instance() {
        static ChunkPool *pool = new ChunkPool(KeptLimitFor(MemoryPool::Instance().Limit()));
        return *pool;
    }

    // An empty buffer with room for at least `bytes`: a kept one if there is one that big, a new one otherwise.
    std::string Take(size_t bytes) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (size_t i = kept_.size(); i-- > 0;) {
                if (kept_[i].capacity() >= bytes) {
                    std::string buffer = std::move(kept_[i]);
                    kept_[i] = std::move(kept_.back());
                    kept_.pop_back();
                    buffer.clear();
                    return buffer;
                }
            }
        }
        std::string buffer;
        buffer.reserve(bytes);
        return buffer;
    }

    // Keeps `buffer` for reuse if it is worth it (see the header); otherwise it is freed here.
    void Put(std::string &&buffer) noexcept {
        const size_t capacity = buffer.capacity();
        if (capacity < kMinKeptCapacity || capacity > kMaxKeptCapacity) return;
        try {
            std::lock_guard<std::mutex> lock(mutex_);
            if (kept_.size() < max_kept_) kept_.push_back(std::move(buffer));
        } catch (...) {
        }
    }

    size_t KeptCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return kept_.size();
    }

private:
    mutable std::mutex mutex_;
    std::vector<std::string> kept_;
    size_t max_kept_ = kMaxKept;
};

}  // namespace xstore
