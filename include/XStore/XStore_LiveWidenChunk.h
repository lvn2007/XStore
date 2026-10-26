#pragma once
// Chunk-size adaptation of the live widen (XStore::LiveWidenOneYear, Tech Reference 4.18 "Chunk size").
#include <algorithm>
#include <cstdint>

namespace livewiden {

// Rows of the next chunk after one that held its lock for hold_us: rows * target_us / hold_us with the factor clamped
// to [0.5, 2], then clamped to [min(floor_rows, max_rows), max_rows].
[[nodiscard]] inline uint64_t NextChunkRows(uint64_t rows, uint64_t hold_us, uint64_t target_us, uint64_t floor_rows,
                                            uint64_t max_rows) noexcept {
    const double factor = std::clamp(static_cast<double>(target_us) / static_cast<double>(std::max<uint64_t>(hold_us, 1)),
                                     0.5, 2.0);
    return std::clamp<uint64_t>(static_cast<uint64_t>(static_cast<double>(rows) * factor), std::min(floor_rows, max_rows),
                                 max_rows);
}

}  // namespace livewiden
