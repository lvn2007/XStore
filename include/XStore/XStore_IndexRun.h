#pragma once

#include <cstdint>

namespace xstore {

// A run of consecutive rows of one year file, by row index (row 0 = the year's first slot).
struct IndexRun {
    uint64_t first_row = 0;
    uint64_t row_count = 0;
};

}  // namespace xstore
