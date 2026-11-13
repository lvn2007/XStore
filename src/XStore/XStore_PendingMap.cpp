#include "include/XStore/XStore_PendingMap.h"

#include <algorithm>
#include <cassert>
#include <unordered_set>

#include <spdlog/spdlog.h>

#include "include/XStore/XStore_Tunables.h"

namespace xstore {

static_assert(PendingMaps::SWEEP_ABOVE >= 2 * std::max(XStoreTunables::ZONE_MAP_REGISTRY_CAP, XStoreTunables::CATIDX_REGISTRY_CAP),
              "the sweep threshold must stay above what the file registries can hold");

namespace {
std::atomic<bool> g_notices_enabled{false};      // see PendingMaps::NoticesEnabled
std::atomic<size_t> g_pending_blocks_total{0};   // pending blocks over all live maps: the AnyPending() fast path

// Mask of bits [lo, hi) of word `word` for a row range [lo, hi) of a block (lo < hi), word index `word`.
inline uint64_t WordMask(size_t word, uint64_t lo, uint64_t hi) {
    const uint64_t word_lo = static_cast<uint64_t>(word) << 6;
    const uint64_t from = lo > word_lo ? lo - word_lo : 0;
    const uint64_t to = hi - word_lo >= 64 ? 64 : hi - word_lo;
    const uint64_t upto = to == 64 ? ~uint64_t{0} : ((uint64_t{1} << to) - 1);
    return upto & (~uint64_t{0} << from);
}
}  // namespace

PendingMap::PendingMap(uint64_t rows_per_block)
    : rows_per_block_(std::max<uint64_t>(1, rows_per_block)), summary_(new std::atomic<uint16_t>[SUMMARY_SLOTS]) {
    for (size_t i = 0; i < SUMMARY_SLOTS; ++i) summary_[i].store(0, std::memory_order_relaxed);
}

PendingMap::~PendingMap() {
    // Nobody else can hold the map now; the lock only orders this against a last concurrent user's writes.
    std::lock_guard<std::mutex> lock(mutex_);
    if (!blocks_.empty()) g_pending_blocks_total.fetch_sub(blocks_.size(), std::memory_order_release);
}

size_t PendingMap::SlotOf(uint64_t block) {
    return static_cast<size_t>((block * 0x9E3779B97F4A7C15ULL) >> (64 - 15));   // SUMMARY_SLOTS = 2^15
}

void PendingMap::MarkRows(uint64_t first_row, uint64_t row_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    total_marked_ += row_count;
    const size_t words_per_block = static_cast<size_t>((rows_per_block_ + 63) / 64);
    const uint64_t end_row = first_row + row_count;
    for (uint64_t row = first_row; row < end_row;) {
        const uint64_t block = row / rows_per_block_;   // one division per block segment
        const uint64_t base = block * rows_per_block_;
        const uint64_t lo = row - base;
        const uint64_t hi = std::min(end_row - base, rows_per_block_);
        row = base + hi;
        const size_t first_word = static_cast<size_t>(lo >> 6);
        const size_t last_word = static_cast<size_t>((hi - 1) >> 6);
        const auto it = blocks_.find(block);
        if (it != blocks_.end()) {
            BlockBits &bits = it->second;
            for (size_t w = first_word; w <= last_word; ++w) {
                const uint64_t mask = WordMask(w, lo, hi);
                uint64_t &word = bits.words[w];
                bits.set_bits += static_cast<uint64_t>(__builtin_popcountll(mask & ~word));
                word |= mask;
            }
            continue;
        }
        // A new pending block: build the finished object first so a failed allocation leaves no empty entry.
        BlockBits fresh;
        fresh.words.assign(words_per_block, 0);
        for (size_t w = first_word; w <= last_word; ++w) {
            const uint64_t mask = WordMask(w, lo, hi);
            fresh.words[w] = mask;
            fresh.set_bits += static_cast<uint64_t>(__builtin_popcountll(mask));
        }
        blocks_.emplace(block, std::move(fresh));
        summary_[SlotOf(block)].fetch_add(1, std::memory_order_release);
        g_pending_blocks_total.fetch_add(1, std::memory_order_release);
    }
}

void PendingMap::ClearRows(uint64_t first_row, uint64_t row_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    ClearRowsLocked(first_row, row_count);
}

void PendingMap::ClearRuns(const std::vector<IndexRun> &runs) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const IndexRun &run : runs) ClearRowsLocked(run.first_row, run.row_count);
}

void PendingMap::ClearRowsLocked(uint64_t first_row, uint64_t row_count) {
    const uint64_t end_row = first_row + row_count;
    for (uint64_t row = first_row; row < end_row;) {
        const uint64_t block = row / rows_per_block_;
        const uint64_t base = block * rows_per_block_;
        const uint64_t lo = row - base;
        const uint64_t hi = std::min(end_row - base, rows_per_block_);
        row = base + hi;
        const auto it = blocks_.find(block);
        if (it == blocks_.end()) continue;
        BlockBits &bits = it->second;
        const size_t last_word = static_cast<size_t>((hi - 1) >> 6);
        for (size_t w = static_cast<size_t>(lo >> 6); w <= last_word; ++w) {
            const uint64_t mask = WordMask(w, lo, hi);
            uint64_t &word = bits.words[w];
            bits.set_bits -= static_cast<uint64_t>(__builtin_popcountll(mask & word));
            word &= ~mask;
        }
        if (bits.set_bits == 0) {
            blocks_.erase(it);
            summary_[SlotOf(block)].fetch_sub(1, std::memory_order_release);
            g_pending_blocks_total.fetch_sub(1, std::memory_order_release);
        }
    }
}

bool PendingMap::IsPending(uint64_t block) const {
    return summary_[SlotOf(block)].load(std::memory_order_acquire) != 0;
}

bool PendingMap::AnyPendingInRows(uint64_t first_row, uint64_t last_row) const {
    if (last_row < first_row) return false;
    const uint64_t first_block = first_row / rows_per_block_;
    const uint64_t last_block = last_row / rows_per_block_;
    std::lock_guard<std::mutex> lock(mutex_);
    // Whichever is fewer: look up each block of the range, or test each pending block against the range.
    if (last_block - first_block < blocks_.size()) {
        for (uint64_t block = first_block; block <= last_block; ++block) {
            if (blocks_.find(block) != blocks_.end()) return true;
        }
        return false;
    }
    for (const auto &entry : blocks_) {
        if (entry.first >= first_block && entry.first <= last_block) return true;
    }
    return false;
}

std::vector<IndexRun> PendingMap::RunsOfBits(uint64_t base_row, const std::vector<uint64_t> &words) {
    std::vector<IndexRun> runs;
    for (size_t w = 0; w < words.size(); ++w) {
        uint64_t bits = words[w];
        while (bits != 0) {
            const uint64_t start = static_cast<uint64_t>(__builtin_ctzll(bits));
            const uint64_t shifted = bits >> start;
            const uint64_t ones = shifted == ~uint64_t{0} ? 64 : static_cast<uint64_t>(__builtin_ctzll(~shifted));
            const uint64_t first = base_row + (static_cast<uint64_t>(w) << 6) + start;
            if (!runs.empty() && runs.back().first_row + runs.back().row_count == first) {
                runs.back().row_count += ones;   // a run continuing across a word boundary
            } else {
                runs.push_back({first, ones});
            }
            bits &= ones + start >= 64 ? 0 : ~(((uint64_t{1} << ones) - 1) << start);
        }
    }
    return runs;
}

std::vector<IndexRun> PendingMap::PendingRunsOfBlock(uint64_t block) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = blocks_.find(block);
    if (it == blocks_.end()) return {};
    return RunsOfBits(block * rows_per_block_, it->second.words);
}

std::vector<IndexRun> PendingMap::AllPendingRuns() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<uint64_t> order;
    order.reserve(blocks_.size());
    for (const auto &entry : blocks_) order.push_back(entry.first);
    std::sort(order.begin(), order.end());
    std::vector<IndexRun> runs;
    for (const uint64_t block : order) {
        const std::vector<IndexRun> of_block = RunsOfBits(block * rows_per_block_, blocks_.at(block).words);
        runs.insert(runs.end(), of_block.begin(), of_block.end());
    }
    return runs;
}

size_t PendingMap::PendingBlockCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return blocks_.size();
}

uint64_t PendingMap::TotalMarkedRowsForTests() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return total_marked_;
}

namespace {
std::mutex &RegistryMutex() { static std::mutex m; return m; }
std::unordered_map<std::string, std::shared_ptr<PendingMap>> &Registry() {
    static std::unordered_map<std::string, std::shared_ptr<PendingMap>> r;
    return r;
}
size_t g_next_sweep_at = PendingMaps::SWEEP_ABOVE + 1;   // registry mutex; the registry size at which For() sweeps next
std::atomic<size_t> g_sweeps{0};

// A map of `rows_per_block` holding the marks of `old`, which is left empty (absolute rows, regrouped).
std::shared_ptr<PendingMap> RegroupedMoving(const std::shared_ptr<PendingMap> &old, uint64_t rows_per_block) {
    auto fresh = std::make_shared<PendingMap>(rows_per_block);
    const std::vector<IndexRun> runs = old->AllPendingRuns();
    for (const IndexRun &run : runs) fresh->MarkRows(run.first_row, run.row_count);
#ifndef NDEBUG
    const std::vector<IndexRun> again = old->AllPendingRuns();
    assert(again.size() == runs.size() && "a mark arrived during Rehome");
    for (size_t i = 0; i < runs.size(); ++i) {
        assert(again[i].first_row == runs[i].first_row && again[i].row_count == runs[i].row_count &&
               "a mark arrived during Rehome");
    }
#endif
    old->ClearRuns(runs);
    return fresh;
}
std::unordered_set<std::string> g_geometry_logged;   // registry mutex; keys already reported by For()
}  // namespace

std::shared_ptr<PendingMap> PendingMaps::For(const std::string &key, uint64_t rows_per_block) {
    std::lock_guard<std::mutex> lock(RegistryMutex());
    auto &registry = Registry();
    auto it = registry.find(key);
    const uint64_t wanted = std::max<uint64_t>(1, rows_per_block);
    if (it != registry.end() && it->second->RowsPerBlock() == wanted) return it->second;
    if (it != registry.end() && it->second->PendingBlockCount() != 0) {
        // The pending marks are writers' in-flight marks that only this map's holders clear: dropping or copying them
        // would lose them or leak them. Keep the map; its file's block geometry is the thing that is wrong.
        if (g_geometry_logged.insert(key).second) {
            spdlog::error("[PENDING] - {}: the index file uses {} rows per block, {} expected, and rows are pending; "
                          "keeping the existing map, the index needs a rebuild", key, it->second->RowsPerBlock(), wanted);
        }
        return it->second;
    }
    if (registry.size() >= g_next_sweep_at) {
        g_sweeps.fetch_add(1, std::memory_order_relaxed);
        // use_count() == 1: only the registry holds it, and a new holder needs this mutex, so the count cannot rise.
        // Lock order registry -> map (PendingBlockCount, and the destructor of an erased map), as everywhere.
        for (auto sweep = registry.begin(); sweep != registry.end();) {
            if (sweep->second.use_count() == 1 && sweep->second->PendingBlockCount() == 0) {
                sweep = registry.erase(sweep);
            } else {
                ++sweep;
            }
        }
        g_next_sweep_at = std::max<size_t>(SWEEP_ABOVE + 1, 2 * registry.size());
    }
    auto map = std::make_shared<PendingMap>(rows_per_block);
    registry[key] = map;
    return map;
}

std::shared_ptr<PendingMap> PendingMaps::Rehome(const std::string &key, uint32_t rows_per_block) {
    std::lock_guard<std::mutex> lock(RegistryMutex());
    auto &registry = Registry();
    const uint64_t wanted = std::max<uint64_t>(1, rows_per_block);
    const auto it = registry.find(key);
    if (it == registry.end()) return registry[key] = std::make_shared<PendingMap>(rows_per_block);
    if (it->second->RowsPerBlock() == wanted) return it->second;
    it->second = RegroupedMoving(it->second, wanted);
    return it->second;
}

std::shared_ptr<PendingMap> PendingMaps::Find(const std::string &key) {
    std::lock_guard<std::mutex> lock(RegistryMutex());
    const auto it = Registry().find(key);
    return it == Registry().end() ? nullptr : it->second;
}

bool PendingMaps::AnyPending() { return g_pending_blocks_total.load(std::memory_order_acquire) != 0; }

void PendingMaps::ForgetMatching(const std::function<bool(const std::string &)> &match) {
    std::lock_guard<std::mutex> lock(RegistryMutex());
    auto &registry = Registry();
    g_next_sweep_at = SWEEP_ABOVE + 1;
    g_geometry_logged.clear();
    for (auto it = registry.begin(); it != registry.end();) {
        if (match(it->first)) {
            it = registry.erase(it);
        } else {
            ++it;
        }
    }
}

void PendingMaps::ClearForTests() {
    std::lock_guard<std::mutex> lock(RegistryMutex());
    Registry().clear();
    g_next_sweep_at = SWEEP_ABOVE + 1;
    g_geometry_logged.clear();
}

size_t PendingMaps::SizeForTests() {
    std::lock_guard<std::mutex> lock(RegistryMutex());
    return Registry().size();
}

size_t PendingMaps::SweepCountForTests() { return g_sweeps.load(std::memory_order_relaxed); }

bool PendingMaps::NoticesEnabled() { return g_notices_enabled.load(std::memory_order_relaxed); }

void PendingMaps::SetNoticesEnabled(bool enabled) { g_notices_enabled.store(enabled, std::memory_order_relaxed); }

void PendingMaps::SetNoticesEnabledForTests(bool enabled) { SetNoticesEnabled(enabled); }

}  // namespace xstore
