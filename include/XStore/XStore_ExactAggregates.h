#pragma once
// The reductions of the exact aggregates (Percentile, DistinctCount, TopK) over every value of a range. Each reorders `values`
// (the caller owns it and is done with it) and holds almost no memory beyond it, so a call costs about 8 bytes per value:
//   * ExactPercentile: nth_element, not a full sort.
//   * ExactDistinctCount: a flat open-addressing table while there are at most `max_table_distinct` distinct values (fast when
//     there are few), then sort + unique in place (no extra memory when nearly every value is distinct).
//   * ExactTopK: sort, then count runs and keep the k best in a heap: no hash map, no list of every (value, count) pair.
// Results equal the straightforward hash-set / hash-map / full-sort versions. A NaN is never equal to anything, so each NaN is
// one distinct value and one entry of count 1, as it was with std::unordered_set / std::unordered_map.
#include "include/XStore/XStore_Tunables.h"
#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace xstore {

// `values` is not empty, `percentile` in [0, 1]. PERCENTILE_CONT: linear interpolation between the two nearest ranks.
[[nodiscard]] double ExactPercentile(std::vector<double> &values, double percentile);

[[nodiscard]] uint64_t ExactDistinctCount(std::vector<double> &values,
                                          size_t max_table_distinct = XStoreTunables::EXACT_DISTINCT_TABLE_MAX_DISTINCT);

// The k most frequent values, by count descending, ties by value ascending; fewer when there are fewer distinct values.
[[nodiscard]] std::vector<std::pair<double, uint64_t>> ExactTopK(std::vector<double> &values, size_t k);

}  // namespace xstore
