#pragma once
// The reductions of the exact aggregates (Percentile, DistinctCount, TopK) over every value of a range. Each reorders `values`
// (the caller owns it and is done with it) and holds almost no memory beyond it, so a call costs about 8 bytes per value:
//   * ExactPercentile: nth_element, not a full sort.
//   * ExactDistinctCount: a flat open-addressing table while there are at most `max_table_distinct` distinct values (fast when
//     there are few), then sort + unique in place (no extra memory when nearly every value is distinct).
//   * ExactTopK: sort, then count runs and keep the k best in a heap: no hash map, no list of every (value, count) pair.
// Results equal the straightforward hash-set / hash-map / full-sort versions. A NaN is never equal to anything, so each NaN is
// one distinct value and one entry of count 1, as it was with std::unordered_set / std::unordered_map. Percentile skips NaNs.
#include "include/XStore/XStore_Tunables.h"
#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

namespace xstore {

// `values` is not empty, `percentile` in [0, 1]. PERCENTILE_CONT: linear interpolation between the two nearest ranks. NaNs are skipped (the
// percentile of the numbers, as pandas does); *nan_skipped, when given, is how many. NaN when every value is NaN.
[[nodiscard]] double ExactPercentile(std::vector<double> &values, double percentile, uint64_t *nan_skipped = nullptr);

[[nodiscard]] uint64_t ExactDistinctCount(std::vector<double> &values,
                                          size_t max_table_distinct = XStoreTunables::EXACT_DISTINCT_TABLE_MAX_DISTINCT);

// The k most frequent values, by count descending, ties by value ascending; fewer when there are fewer distinct values.
[[nodiscard]] std::vector<std::pair<double, uint64_t>> ExactTopK(std::vector<double> &values, size_t k);

// ---- the same reductions over the values where they lie ----------------------------------------------------------------------
// The collected values are never concatenated: they are spans over the pieces' vectors, and the work is cut into tasks that a
// TaskRunner runs (the server's on the shared scan pool at the adaptive thread count; RunInline for none). Extra memory is a few
// MB: a histogram per running task for Percentile, a small heap per range for DistinctCount/TopK. Results equal the single-vector
// versions above; a NaN is counted as there (DistinctCount: one each; TopK: one entry of count 1 each, ranked after every number).
struct ValueSpan {
    double *data;
    size_t size;
};

// Runs body(0) .. body(tasks - 1), in any order and on any threads, and returns when all have finished.
using TaskRunner = std::function<void(size_t tasks, const std::function<void(size_t)> &body)>;
inline void RunInline(size_t tasks, const std::function<void(size_t)> &body) {
    for (size_t i = 0; i < tasks; ++i) body(i);
}

struct ReduceTuning {
    size_t slice_values = XStoreTunables::EXACT_SLICE_VALUES;
    size_t max_ranges = XStoreTunables::EXACT_MAX_RANGES;
    size_t samples_per_slice = XStoreTunables::EXACT_RANGE_SAMPLES_PER_SLICE;
    size_t gather_max = XStoreTunables::EXACT_SELECT_GATHER_MAX;
};

// Percentile does not modify the values; DistinctCount and TopK sort every slice in place (NaNs moved behind the numbers).
[[nodiscard]] double ExactPercentile(const std::vector<ValueSpan> &spans, double percentile, const TaskRunner &run,
                                     uint64_t *nan_skipped = nullptr, const ReduceTuning &tuning = {});
[[nodiscard]] uint64_t ExactDistinctCount(const std::vector<ValueSpan> &spans, const TaskRunner &run, const ReduceTuning &tuning = {});
[[nodiscard]] std::vector<std::pair<double, uint64_t>> ExactTopK(const std::vector<ValueSpan> &spans, size_t k, const TaskRunner &run,
                                                                 const ReduceTuning &tuning = {});

}  // namespace xstore
