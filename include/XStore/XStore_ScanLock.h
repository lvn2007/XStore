#pragma once
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_PreadPool.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/XStore/XStore_ZoneMap.h"

// How a range scan (aggregates, RangeQuery, FindByValue, FindByPattern) holds its READ range lock. A WRITE on an overlapping
// range (the WAL apply of an insert) waits for every READ on it, so a scan that holds its whole range for its whole run holds
// off the inserts into that range for as long as it runs (Tech Reference 11; measured in
// XStoreTunables::SCAN_READ_LOCK_CHUNK_BYTES' comment). ScanUnderReadLock takes the lock one chunk at a time instead. The price:
// a scan that spans a multi-row record may see it applied in one chunk and not yet in the next (each row stays consistent).
namespace xstore::scanlock {

struct Config {
    uint64_t chunk_bytes = XStoreTunables::SCAN_READ_LOCK_CHUNK_BYTES;   // 0 = the whole range under one lock
    // Aggregates only (WalkYearSpanParallel): a year segment is cut into pieces of piece_bytes that run parallel_threads at a time
    // on the shared XStorePreadPool, partial results merged in piece order.
    // parallel_threads: 1 = off (one piece after the other), N >= 2 = N pieces at a time, 0 = adaptive (the default): before each
    // wave the number of threads is the machine's idle cores less adaptive_headroom, between 1 and adaptive_max (ThreadsFor). Idle
    // cores come from /proc/stat, so the scan's own threads, other scans and the inserts' own work all count as load.
    // adaptive_max defaults to half the hardware threads: a year-wide scan then leaves the other half to the inserts and the readers.
    uint32_t parallel_threads = 0;
    uint32_t adaptive_max = std::max(1u, std::thread::hardware_concurrency() / 2);
    uint32_t adaptive_headroom = 2;
    uint64_t piece_bytes = uint64_t{64} << 20;
    // Row-emitting scans (ScanRegionOrdered): the pieces are smaller, because each in-flight piece holds its output until it is
    // sent in order (the same number of threads).
    uint64_t ordered_piece_bytes = uint64_t{8} << 20;
};

namespace detail {
inline std::mutex &ConfigMutex() { static std::mutex m; return m; }
inline Config &Stored() { static Config c; return c; }
inline Config FromEnv() {
    Config c;
    if (const char *env = std::getenv("XSTORE_SCAN_LOCK_CHUNK_MB")) c.chunk_bytes = std::strtoull(env, nullptr, 10) << 20;
    if (const char *env = std::getenv("XSTORE_SCAN_PARALLEL_THREADS")) {
        c.parallel_threads = std::strcmp(env, "adaptive") == 0 ? 0 : static_cast<uint32_t>(std::max<unsigned long>(1, std::strtoul(env, nullptr, 10)));
    }
    if (const char *env = std::getenv("XSTORE_SCAN_ADAPTIVE_MAX")) c.adaptive_max = static_cast<uint32_t>(std::max<unsigned long>(1, std::strtoul(env, nullptr, 10)));
    if (const char *env = std::getenv("XSTORE_SCAN_ADAPTIVE_HEADROOM")) c.adaptive_headroom = static_cast<uint32_t>(std::strtoul(env, nullptr, 10));
    if (const char *env = std::getenv("XSTORE_SCAN_ORDERED_PIECE_MB")) c.ordered_piece_bytes = std::max<uint64_t>(1, std::strtoull(env, nullptr, 10)) << 20;
    if (const char *env = std::getenv("XSTORE_SCAN_PIECE_MB")) c.piece_bytes = std::max<uint64_t>(1, std::strtoull(env, nullptr, 10)) << 20;
    return c;
}
inline std::function<void(uint64_t)> &Hook() { static std::function<void(uint64_t)> h; return h; }
inline std::atomic<uint64_t> &Relaunches() { static std::atomic<uint64_t> n{0}; return n; }
}  // namespace detail

// How many times a parallel row-emitting scan had to start new producers after all had retired (tests).
inline uint64_t ProducerRelaunchesForTests() { return detail::Relaunches().load(); }

inline Config Get() {
    static const bool initialised = (detail::Stored() = detail::FromEnv(), true);
    (void)initialised;
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    return detail::Stored();
}
inline void Set(const Config &config) {
    (void)Get();   // the env default must not overwrite this later
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    detail::Stored() = config;
}
// The adaptive number of threads for a machine with `cores` logical CPUs of which `busy_fraction` are busy: the idle cores less `headroom`,
// between 1 and `max_threads`.
inline uint32_t ThreadsFor(double busy_fraction, unsigned cores, unsigned headroom, unsigned max_threads) {
    const double busy = std::min(1.0, std::max(0.0, busy_fraction));
    const long idle_cores = static_cast<long>(static_cast<double>(cores) * (1.0 - busy));
    const long threads = idle_cores - static_cast<long>(headroom);
    return static_cast<uint32_t>(std::max<long>(1, std::min<long>(threads, static_cast<long>(max_threads))));
}

// Whole-machine busy fraction (user + nice + system + irq + softirq + steal over all, iowait counts as idle) over the last
// 250 ms or more, from /proc/stat; 0 until a second sample. One mutex hold per call.
inline double BusyFraction() {
    static std::mutex mutex;
    static std::chrono::steady_clock::time_point last_time{};
    static uint64_t last_busy = 0, last_total = 0;
    static double fraction = 0.0;
    std::lock_guard<std::mutex> lock(mutex);
    const auto now = std::chrono::steady_clock::now();
    if (now - last_time < std::chrono::milliseconds(250)) return fraction;
    std::ifstream stat("/proc/stat");
    std::string cpu;
    uint64_t user = 0, nice = 0, system = 0, idle = 0, iowait = 0, irq = 0, softirq = 0, steal = 0;
    if (stat >> cpu >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal) {
        const uint64_t busy = user + nice + system + irq + softirq + steal;
        const uint64_t total = busy + idle + iowait;
        if (last_time != std::chrono::steady_clock::time_point{} && total > last_total) {
            fraction = static_cast<double>(busy - last_busy) / static_cast<double>(total - last_total);
        }
        last_busy = busy;
        last_total = total;
        last_time = now;
    }
    return fraction;
}

// The number of pieces to run at a time for the next wave.
inline uint32_t EffectiveThreads(const Config &config) {
    if (config.parallel_threads != 0) return config.parallel_threads;
    return ThreadsFor(BusyFraction(), std::max(1u, std::thread::hardware_concurrency()), config.adaptive_headroom, config.adaptive_max);
}

// Called with the chunk number after each chunk, with no lock held (tests: a writer asks for a range mid-scan).
inline void SetChunkHookForTests(std::function<void(uint64_t)> hook) {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    detail::Hook() = std::move(hook);
}
inline std::function<void(uint64_t)> ChunkHook() {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    return detail::Hook();
}

// Runs body(rows_base, rows, row_offset) over the `region_length` bytes at `first_offset` of the mapped year file, each call
// under a READ lock on its chunk only. row_offset is the first row of the call counted from the start of the region; the
// calls are in order and together cover the region once. body returns false to stop the scan. after() runs after each
// chunk's lock is released: the place to send output the chunk produced (a slow client must not hold a lock).
// first_row_in_year places the chunk boundaries on multiples of the chunk's rows, which are whole zone-map blocks, so a
// block-pruning kernel still sees whole blocks.
template <typename Body, typename After>
void ScanUnderReadLock(const std::string &file_path, void *mapped_base, unsigned long long first_offset,
                       unsigned long long region_length, uint16_t row_length, unsigned long long first_row_in_year,
                       Body &&body, After &&after) {
    static const long PAGE_SIZE = sysconf(_SC_PAGE_SIZE);
    const auto advise = [&](unsigned long long offset, unsigned long long length) {
        char *span_start = static_cast<char *>(mapped_base) + offset;
        uintptr_t aligned_addr = reinterpret_cast<uintptr_t>(span_start) & ~(static_cast<uintptr_t>(PAGE_SIZE) - 1);
        size_t align_slop = reinterpret_cast<uintptr_t>(span_start) - aligned_addr;
        madvise(reinterpret_cast<void *>(aligned_addr), length + align_slop, MADV_WILLNEED);
    };
    const auto rows_at = [&](unsigned long long offset) { return static_cast<const char *>(mapped_base) + offset; };
    const uint64_t chunk_bytes = Get().chunk_bytes;
    const unsigned long long rows_total = region_length / row_length;

    if (chunk_bytes == 0) {
        {
            FileRangeLockGuard guard(file_path, first_offset, region_length, FileRangeLock::LockType::READ);
            // Bulk prefetch hint before the row-by-row walk: one madvise() covering the whole segment instead of one page fault
            // per first-touched page as the scan goes forward.
            advise(first_offset, region_length);
            body(rows_at(first_offset), rows_total, 0);
        }
        after();
        return;
    }

    const unsigned long long rows_per_block = RowsPerBlock(row_length);
    unsigned long long chunk_rows = std::max<unsigned long long>(1, chunk_bytes / row_length);
    chunk_rows = ((chunk_rows + rows_per_block - 1) / rows_per_block) * rows_per_block;
    const std::function<void(uint64_t)> hook = ChunkHook();
    uint64_t chunk_number = 0;
    for (unsigned long long row = 0; row < rows_total; ++chunk_number) {
        // The chunk ends at the next multiple of chunk_rows of the year's row numbers (or the end of the region).
        const unsigned long long end = std::min(rows_total, row + (chunk_rows - (first_row_in_year + row) % chunk_rows));
        const unsigned long long offset = first_offset + row * row_length;
        const unsigned long long length = (end - row) * row_length;
        bool more = true;
        {
            FileRangeLockGuard guard(file_path, offset, length, FileRangeLock::LockType::READ);
            advise(offset, length);
            more = body(rows_at(offset), end - row, row);
        }
        after();
        if (hook) hook(chunk_number);
        if (!more) return;
        row = end;
    }
}

// A row-emitting scan (FindByValue, FindByPattern; RangeQuery stays sequential: its time is not in the scan) in parallel with its output in order: the region is cut into pieces
// of ordered_piece_bytes (whole zone-map blocks), parallel_threads pieces at a time are produced on the shared XStorePreadPool, and
// after each wave the calling thread consume()s the pieces' outputs in order, with no lock held. produce(rows_base, rows,
// row_offset, out) runs under a READ lock on its chunk (see ScanUnderReadLock) and appends the chunk's output to out; consume(out)
// returns false to stop. Returns false, having done nothing, when this scan should not run parallel (one thread, or fewer than two
// pieces): the caller then runs its sequential path.
template <typename ProduceChunk, typename ConsumePiece>
bool ScanRegionOrdered(const std::string &file_path, void *mapped_base, unsigned long long first_offset,
                       unsigned long long region_length, uint16_t row_length, unsigned long long first_row_in_year,
                       ProduceChunk &&produce, ConsumePiece &&consume) {
    const Config config = Get();
    if (config.parallel_threads == 1) return false;
    const unsigned long long rows_total = region_length / row_length;
    const unsigned long long rows_per_block = RowsPerBlock(row_length);
    unsigned long long piece_rows = std::max<unsigned long long>(1, config.ordered_piece_bytes / row_length);
    piece_rows = ((piece_rows + rows_per_block - 1) / rows_per_block) * rows_per_block;
    struct Piece { unsigned long long row; unsigned long long end; };
    std::vector<Piece> pieces;
    for (unsigned long long row = 0; row < rows_total;) {
        const unsigned long long end = std::min(rows_total, row + (piece_rows - (first_row_in_year + row) % piece_rows));
        pieces.push_back({row, end});
        row = end;
    }
    if (pieces.size() < 2) return false;
    const size_t team = std::min<size_t>(pieces.size(), EffectiveThreads(config));
    if (team <= 1) return false;

    // A pipeline, not waves: `team` producers pull pieces one after the other without waiting for each other (a barrier every
    // few milliseconds left the pool threads idle at the CPU's lowest frequency half the time, and was SLOWER than one thread at
    // two threads), into a window of 2 * team slots; the calling thread consumes the slots in order while the producers go on.
    // A producer that finds no room for 100 ms retires (a slow consumer must not park the shared pool); the consumer starts
    // new ones when all have retired. Adaptive: a producer whose rank is over the current number of threads retires too.
    using namespace std::chrono_literals;
    const size_t window = 2 * team;
    struct Slot { std::string out; bool ready = false; };
    std::vector<Slot> slots(window);
    std::mutex mutex;
    std::condition_variable ready_cv, space_cv;
    size_t next_piece = 0, consumed = 0, active_producers = 0;
    bool stop = false;

    const auto produce_piece = [&](size_t i) {
        const Piece &piece = pieces[i];
        Slot &slot = slots[i % window];
        slot.out.clear();
        ScanUnderReadLock(
            file_path, mapped_base, first_offset + piece.row * row_length, (piece.end - piece.row) * row_length, row_length,
            first_row_in_year + piece.row,
            [&](const char *rows_base, unsigned long long rows, unsigned long long row_offset) {
                produce(rows_base, rows, piece.row + row_offset, slot.out);
                return true;
            },
            [] {});
    };
    const auto producer = [&](size_t rank) {
        for (;;) {
            if (config.parallel_threads == 0 && rank > 0 && rank >= EffectiveThreads(config)) break;
            size_t i;
            {
                std::unique_lock<std::mutex> lock(mutex);
                if (!space_cv.wait_for(lock, 100ms, [&] { return stop || next_piece >= pieces.size() || next_piece < consumed + window; })) break;
                if (stop || next_piece >= pieces.size()) break;
                i = next_piece++;   // claimed and checked for room under one hold: a claimed piece is always produced
            }
            produce_piece(i);
            {
                std::lock_guard<std::mutex> lock(mutex);
                slots[i % window].ready = true;
            }
            ready_cv.notify_all();
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            --active_producers;
        }
        ready_cv.notify_all();
    };
    std::thread helper;   // the ParallelFor that runs the producers (it blocks, and the calling thread consumes meanwhile)
    const auto launch = [&] {
        if (helper.joinable()) helper.join();
        {
            std::lock_guard<std::mutex> lock(mutex);
            active_producers = team;
        }
        helper = std::thread([&] { XStorePreadPool::ParallelFor(team, producer); });
    };
    launch();
    bool more = true;
    for (size_t i = 0; i < pieces.size() && more; ++i) {
        Slot &slot = slots[i % window];
        std::unique_lock<std::mutex> lock(mutex);
        while (!slot.ready) {
            if (ready_cv.wait_for(lock, 50ms, [&] { return slot.ready; })) break;
            if (active_producers == 0 && next_piece < pieces.size()) {
                lock.unlock();
                detail::Relaunches().fetch_add(1);
                launch();
                lock.lock();
            }
        }
        lock.unlock();
        more = consume(slot.out);
        lock.lock();
        slot.ready = false;
        consumed = i + 1;
        lock.unlock();
        space_cv.notify_all();
    }
    {
        std::lock_guard<std::mutex> lock(mutex);
        stop = true;
    }
    space_cv.notify_all();
    if (helper.joinable()) helper.join();
    return true;
}

}  // namespace xstore::scanlock
