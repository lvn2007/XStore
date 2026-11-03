#include "include/XStore/XStore_ExactAggregates.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <cstring>
#include <mutex>

namespace xstore {

namespace {

// Open addressing over the bit patterns of doubles, linear probing, kept at most half full. A zero is stored as +0.0 so that
// -0.0 and 0.0 are one value, like operator==. NaN is not added (the callers count NaNs themselves).
class FlatDoubleSet {
public:
    FlatDoubleSet() : keys_(kInitialSlots), used_(kInitialSlots, 0), mask_(kInitialSlots - 1) {}

    size_t Size() const { return size_; }

    void Add(double value) {
        if (value == 0.0) value = 0.0;
        uint64_t bits;
        std::memcpy(&bits, &value, sizeof bits);
        if (size_ * 2 >= keys_.size()) Grow();
        Insert(bits);
    }

private:
    static constexpr size_t kInitialSlots = 1024;

    static size_t Slot(uint64_t bits) {
        bits ^= bits >> 33;
        bits *= 0xff51afd7ed558ccdULL;
        bits ^= bits >> 33;
        return static_cast<size_t>(bits);
    }

    void Insert(uint64_t bits) {
        size_t i = Slot(bits) & mask_;
        while (used_[i]) {
            if (keys_[i] == bits) return;
            i = (i + 1) & mask_;
        }
        used_[i] = 1;
        keys_[i] = bits;
        ++size_;
    }

    void Grow() {
        std::vector<uint64_t> old_keys = std::move(keys_);
        std::vector<uint8_t> old_used = std::move(used_);
        keys_.assign(old_keys.size() * 2, 0);
        used_.assign(old_keys.size() * 2, 0);
        mask_ = keys_.size() - 1;
        size_ = 0;
        for (size_t i = 0; i < old_keys.size(); ++i) {
            if (old_used[i]) Insert(old_keys[i]);
        }
    }

    std::vector<uint64_t> keys_;
    std::vector<uint8_t> used_;
    size_t mask_;
    size_t size_ = 0;
};

// Moves the NaNs to the back (sorting them is undefined); returns the end of the non-NaN part.
std::vector<double>::iterator PartitionOutNaN(std::vector<double> &values) {
    return std::partition(values.begin(), values.end(), [](double d) { return !std::isnan(d); });
}

// The first of two (value, count) entries is the better one: higher count, then lower value.
bool Better(const std::pair<double, uint64_t> &a, const std::pair<double, uint64_t> &b) {
    if (a.second != b.second) return a.second > b.second;
    return a.first < b.first;
}

}  // namespace

double ExactPercentile(std::vector<double> &values, double percentile, uint64_t *nan_skipped) {
    // A NaN is not less than, greater than or equal to anything: nth_element over data that holds one is not a sort of anything, and the
    // result depended on where the NaNs sat. They are left out (the percentile of the numbers), and counted.
    const auto end = PartitionOutNaN(values);
    const size_t count = static_cast<size_t>(end - values.begin());
    if (nan_skipped != nullptr) *nan_skipped = values.size() - count;
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();
    const double rank = percentile * static_cast<double>(count - 1);
    const size_t lower = static_cast<size_t>(std::floor(rank));
    const size_t upper = static_cast<size_t>(std::ceil(rank));
    const double fraction = rank - static_cast<double>(lower);
    std::nth_element(values.begin(), values.begin() + static_cast<std::ptrdiff_t>(lower), end);
    const double low = values[lower];
    // Everything after `lower` is now >= it, so the next rank is the minimum of that part.
    const double high = upper == lower ? low : *std::min_element(values.begin() + static_cast<std::ptrdiff_t>(lower) + 1, end);
    if (high == low) return low;   // also two equal infinities: inf - inf would be NaN
    return low + fraction * (high - low);
}

uint64_t ExactDistinctCount(std::vector<double> &values, size_t max_table_distinct) {
    FlatDoubleSet table;
    uint64_t nans = 0;
    bool fits = true;
    for (double d : values) {
        if (std::isnan(d)) {
            ++nans;
            continue;
        }
        table.Add(d);
        if (table.Size() > max_table_distinct) {
            fits = false;
            break;
        }
    }
    if (fits) return table.Size() + nans;

    const auto end = PartitionOutNaN(values);
    nans = static_cast<uint64_t>(values.end() - end);
    std::sort(values.begin(), end);
    return static_cast<uint64_t>(std::unique(values.begin(), end) - values.begin()) + nans;
}

std::vector<std::pair<double, uint64_t>> ExactTopK(std::vector<double> &values, size_t k) {
    std::vector<std::pair<double, uint64_t>> best;   // a heap whose top is the worst of the k kept
    if (k == 0) return best;
    const auto end = PartitionOutNaN(values);
    std::sort(values.begin(), end);

    auto offer = [&](double value, uint64_t count) {
        const std::pair<double, uint64_t> entry{value, count};
        if (best.size() < k) {
            best.push_back(entry);
            std::push_heap(best.begin(), best.end(), Better);
        } else if (Better(entry, best.front())) {
            std::pop_heap(best.begin(), best.end(), Better);
            best.back() = entry;
            std::push_heap(best.begin(), best.end(), Better);
        }
    };
    for (auto run = values.begin(); run != end;) {
        const auto run_end = std::find_if(run, end, [&](double d) { return d != *run; });
        offer(*run, static_cast<uint64_t>(run_end - run));
        run = run_end;
    }
    for (auto nan = end; nan != values.end(); ++nan) offer(*nan, 1);

    std::sort_heap(best.begin(), best.end(), Better);   // best first
    return best;
}

// ---- over spans ------------------------------------------------------------------------------------------------------------------

namespace {

// Spans cut into slices of at most `slice_values` (no empty ones): the unit of work.
std::vector<ValueSpan> CutIntoSlices(const std::vector<ValueSpan> &spans, size_t slice_values) {
    const size_t step = std::max<size_t>(1, slice_values);
    std::vector<ValueSpan> slices;
    for (const ValueSpan &span : spans) {
        for (size_t at = 0; at < span.size; at += step) slices.push_back({span.data + at, std::min(step, span.size - at)});
    }
    return slices;
}

// Moves the NaNs of every slice behind its numbers, sorts the numbers, shrinks the slices to them (and drops the empty ones);
// returns how many NaNs there were.
uint64_t SortSlices(std::vector<ValueSpan> &slices, const TaskRunner &run) {
    std::vector<uint64_t> nans(slices.size(), 0);
    run(slices.size(), [&](size_t i) {
        ValueSpan &slice = slices[i];
        double *end = std::partition(slice.data, slice.data + slice.size, [](double d) { return !std::isnan(d); });
        nans[i] = static_cast<uint64_t>(slice.data + slice.size - end);
        slice.size = static_cast<size_t>(end - slice.data);
        std::sort(slice.data, end);
    });
    uint64_t total = 0;
    for (uint64_t n : nans) total += n;
    slices.erase(std::remove_if(slices.begin(), slices.end(), [](const ValueSpan &s) { return s.size == 0; }), slices.end());
    return total;
}

// Entries (value, count) kept best first: the k best by Better, as ExactTopK keeps them.
class BestK {
public:
    explicit BestK(size_t k) : k_(k) {}
    void Offer(double value, uint64_t count) {
        const std::pair<double, uint64_t> entry{value, count};
        if (best_.size() < k_) {
            best_.push_back(entry);
            std::push_heap(best_.begin(), best_.end(), Better);
        } else if (!best_.empty() && Better(entry, best_.front())) {
            std::pop_heap(best_.begin(), best_.end(), Better);
            best_.back() = entry;
            std::push_heap(best_.begin(), best_.end(), Better);
        }
    }
    std::vector<std::pair<double, uint64_t>> Take() { return std::move(best_); }

private:
    size_t k_;
    std::vector<std::pair<double, uint64_t>> best_;   // a heap whose top is the worst of the kept
};

struct RangeResult {
    uint64_t distinct = 0;
    std::vector<std::pair<double, uint64_t>> top;   // TopK only
};

// The splitters between the ranges: values taken evenly from every sorted slice, then evenly from those, strictly increasing.
std::vector<double> RangeSplitters(const std::vector<ValueSpan> &slices, size_t total, const ReduceTuning &tuning) {
    const size_t ranges = std::min(std::max<size_t>(1, tuning.max_ranges), std::max<size_t>(1, total / 8));
    std::vector<double> splitters;
    if (ranges < 2) return splitters;
    std::vector<double> samples;
    for (const ValueSpan &slice : slices) {
        const size_t take = std::min(std::max<size_t>(1, tuning.samples_per_slice), slice.size);
        for (size_t i = 0; i < take; ++i) samples.push_back(slice.data[i * slice.size / take]);
    }
    std::sort(samples.begin(), samples.end());
    for (size_t j = 1; j < ranges; ++j) {
        const double candidate = samples[j * samples.size() / ranges];
        if (splitters.empty() || splitters.back() < candidate) splitters.push_back(candidate);
    }
    return splitters;
}

// Length of the run of values equal to *p (p < end, sorted ascending): the next one first, then doubling, then a binary search.
size_t RunLength(const double *p, const double *end) {
    const size_t available = static_cast<size_t>(end - p);
    if (available == 1 || *p < p[1]) return 1;
    size_t known = 1, bound = 2;   // p[known] equals *p
    while (bound < available && !(*p < p[bound])) {
        known = bound;
        bound *= 2;
    }
    bound = std::min(bound, available);
    return static_cast<size_t>(std::partition_point(p + known + 1, p + bound, [&](double d) { return !(*p < d); }) - p);
}

// Merges the sorted slices' parts that lie in [lower, upper) (nullptr: unbounded), run by run in ascending order, and calls
// emit(value, count) for every distinct value: a binary min-heap of cursors, one per slice, replaced at the top as a cursor advances.
template <typename Emit>
void MergeRange(const std::vector<ValueSpan> &slices, const double *lower, const double *upper, Emit &&emit) {
    struct Cursor {
        const double *p;
        const double *end;
    };
    std::vector<Cursor> heap;
    for (const ValueSpan &slice : slices) {
        const double *begin = slice.data, *end = slice.data + slice.size;
        const double *from = lower == nullptr ? begin : std::lower_bound(begin, end, *lower);
        const double *to = upper == nullptr ? end : std::lower_bound(begin, end, *upper);
        if (from < to) heap.push_back({from, to});
    }
    const auto sift_down = [&heap](size_t i) {
        const size_t n = heap.size();
        const Cursor moving = heap[i];
        for (;;) {
            size_t child = 2 * i + 1;
            if (child >= n) break;
            if (child + 1 < n && *heap[child + 1].p < *heap[child].p) ++child;
            if (!(*heap[child].p < *moving.p)) break;
            heap[i] = heap[child];
            i = child;
        }
        heap[i] = moving;
    };
    for (size_t i = heap.size() / 2; i-- > 0;) sift_down(i);

    while (!heap.empty()) {
        const double value = *heap[0].p;
        uint64_t count = 0;
        while (!heap.empty() && !(value < *heap[0].p)) {   // the top is never below `value`, so not above it means equal
            const size_t run = RunLength(heap[0].p, heap[0].end);
            count += run;
            heap[0].p += run;
            if (heap[0].p == heap[0].end) {
                heap[0] = heap.back();
                heap.pop_back();
            }
            if (!heap.empty()) sift_down(0);
        }
        emit(value, count);
    }
}

// Cuts the value range at the splitters and merges every range on its own task.
std::vector<RangeResult> MergeRanges(const std::vector<ValueSpan> &slices, const TaskRunner &run, const ReduceTuning &tuning,
                                     size_t k, bool want_top) {
    size_t total = 0;
    for (const ValueSpan &slice : slices) total += slice.size;
    const std::vector<double> splitters = RangeSplitters(slices, total, tuning);
    const size_t ranges = splitters.size() + 1;
    std::vector<RangeResult> results(ranges);
    run(ranges, [&](size_t r) {
        const double *lower = r == 0 ? nullptr : &splitters[r - 1];
        const double *upper = r + 1 == ranges ? nullptr : &splitters[r];
        BestK best(want_top ? k : 0);
        uint64_t distinct = 0;
        MergeRange(slices, lower, upper, [&](double value, uint64_t count) {
            ++distinct;
            if (want_top) best.Offer(value, count);
        });
        results[r].distinct = distinct;
        if (want_top) results[r].top = best.Take();
    });
    return results;
}

// Order-preserving map of a double to 64 bits (all keys of negative numbers below those of positive ones, -0.0 just below +0.0).
uint64_t ToKey(double d) {
    uint64_t bits;
    std::memcpy(&bits, &d, sizeof bits);
    return (bits >> 63) != 0 ? ~bits : (bits | (uint64_t{1} << 63));
}

double FromKey(uint64_t key) {
    const uint64_t bits = (key >> 63) != 0 ? (key & ~(uint64_t{1} << 63)) : ~key;
    double d;
    std::memcpy(&d, &bits, sizeof d);
    return d;
}

constexpr size_t kSelectBuckets = size_t{1} << 16;

// The value of 0-based rank `rank` among the non-NaN values of the spans, found 16 bits of the key at a time: a histogram pass over the
// values whose higher bits match, then the bucket that holds the rank; when few enough remain they are copied and finished with
// nth_element. *next is set to the value of rank + 1 when that is known without another pass.
double SelectRank(const std::vector<ValueSpan> &slices, const TaskRunner &run, const ReduceTuning &tuning, uint64_t rank,
                  double *next, bool *has_next) {
    *has_next = false;
    uint64_t prefix = 0;   // the bits of the key above `shift` that every remaining value shares
    for (int shift = 48;; shift -= 16) {
        const bool first = shift == 48;
        std::vector<uint64_t> histogram(kSelectBuckets, 0);
        std::mutex merge_mutex;
        run(slices.size(), [&](size_t i) {
            std::vector<uint32_t> local(kSelectBuckets, 0);
            const ValueSpan &slice = slices[i];
            for (size_t j = 0; j < slice.size; ++j) {
                if (std::isnan(slice.data[j])) continue;
                const uint64_t key = ToKey(slice.data[j]);
                if (first || (key >> (shift + 16)) == prefix) ++local[(key >> shift) & (kSelectBuckets - 1)];
            }
            std::lock_guard<std::mutex> lock(merge_mutex);
            for (size_t b = 0; b < kSelectBuckets; ++b) histogram[b] += local[b];
        });
        size_t bucket = 0;
        for (; bucket < kSelectBuckets && rank >= histogram[bucket]; ++bucket) rank -= histogram[bucket];
        const uint64_t remaining = histogram[bucket];
        prefix = first ? bucket : (prefix << 16) | bucket;

        if (shift == 0) {   // every remaining value has this exact key
            *has_next = rank + 1 < remaining;
            *next = FromKey(prefix);
            return *next;
        }
        if (remaining <= tuning.gather_max) {
            std::vector<double> gathered;
            gathered.reserve(remaining);
            run(slices.size(), [&](size_t i) {
                std::vector<double> local;
                const ValueSpan &slice = slices[i];
                for (size_t j = 0; j < slice.size; ++j) {
                    if (!std::isnan(slice.data[j]) && (ToKey(slice.data[j]) >> shift) == prefix) local.push_back(slice.data[j]);
                }
                std::lock_guard<std::mutex> lock(merge_mutex);
                gathered.insert(gathered.end(), local.begin(), local.end());
            });
            std::nth_element(gathered.begin(), gathered.begin() + static_cast<std::ptrdiff_t>(rank), gathered.end());
            const double value = gathered[rank];
            if (rank + 1 < gathered.size()) {
                *has_next = true;
                *next = *std::min_element(gathered.begin() + static_cast<std::ptrdiff_t>(rank) + 1, gathered.end());
            }
            return value;
        }
    }
}

}  // namespace

double ExactPercentile(const std::vector<ValueSpan> &spans, double percentile, const TaskRunner &run, uint64_t *nan_skipped,
                       const ReduceTuning &tuning) {
    const std::vector<ValueSpan> slices = CutIntoSlices(spans, tuning.slice_values);
    std::vector<uint64_t> numbers(slices.size(), 0);
    run(slices.size(), [&](size_t i) {
        uint64_t count = 0;
        for (size_t j = 0; j < slices[i].size; ++j) count += std::isnan(slices[i].data[j]) ? 0 : 1;
        numbers[i] = count;
    });
    uint64_t count = 0, all = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        count += numbers[i];
        all += slices[i].size;
    }
    if (nan_skipped != nullptr) *nan_skipped = all - count;
    if (count == 0) return std::numeric_limits<double>::quiet_NaN();

    const double rank = percentile * static_cast<double>(count - 1);
    const uint64_t lower = static_cast<uint64_t>(std::floor(rank));
    const uint64_t upper = static_cast<uint64_t>(std::ceil(rank));
    const double fraction = rank - static_cast<double>(lower);
    double next = 0;
    bool has_next = false;
    const double low = SelectRank(slices, run, tuning, lower, &next, &has_next);
    double high = low;
    if (upper != lower) {
        if (has_next) high = next;
        else high = SelectRank(slices, run, tuning, upper, &next, &has_next);
    }
    if (high == low) return low;   // also two equal infinities: inf - inf would be NaN
    return low + fraction * (high - low);
}

uint64_t ExactDistinctCount(const std::vector<ValueSpan> &spans, const TaskRunner &run, const ReduceTuning &tuning) {
    std::vector<ValueSpan> slices = CutIntoSlices(spans, tuning.slice_values);
    const uint64_t nans = SortSlices(slices, run);
    uint64_t distinct = nans;
    for (const RangeResult &r : MergeRanges(slices, run, tuning, 0, false)) distinct += r.distinct;
    return distinct;
}

std::vector<std::pair<double, uint64_t>> ExactTopK(const std::vector<ValueSpan> &spans, size_t k, const TaskRunner &run,
                                                   const ReduceTuning &tuning) {
    if (k == 0) return {};
    std::vector<ValueSpan> slices = CutIntoSlices(spans, tuning.slice_values);
    const uint64_t nans = SortSlices(slices, run);
    std::vector<std::pair<double, uint64_t>> candidates;
    for (RangeResult &r : MergeRanges(slices, run, tuning, k, true)) {
        candidates.insert(candidates.end(), r.top.begin(), r.top.end());
        std::vector<std::pair<double, uint64_t>>().swap(r.top);
    }
    std::sort(candidates.begin(), candidates.end(), Better);   // the values differ, so Better is a strict order here
    if (candidates.size() > k) candidates.resize(k);
    for (uint64_t i = 0; i < nans && candidates.size() < k; ++i) candidates.emplace_back(std::numeric_limits<double>::quiet_NaN(), 1);
    return candidates;
}

}  // namespace xstore
