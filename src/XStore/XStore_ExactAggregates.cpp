#include "include/XStore/XStore_ExactAggregates.h"

#include <algorithm>
#include <cmath>
#include <cstring>

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

double ExactPercentile(std::vector<double> &values, double percentile) {
    const double rank = percentile * static_cast<double>(values.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(rank));
    const size_t upper = static_cast<size_t>(std::ceil(rank));
    const double fraction = rank - static_cast<double>(lower);
    std::nth_element(values.begin(), values.begin() + static_cast<std::ptrdiff_t>(lower), values.end());
    const double low = values[lower];
    // Everything after `lower` is now >= it, so the next rank is the minimum of that part.
    const double high = upper == lower ? low : *std::min_element(values.begin() + static_cast<std::ptrdiff_t>(lower) + 1, values.end());
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

}  // namespace xstore
