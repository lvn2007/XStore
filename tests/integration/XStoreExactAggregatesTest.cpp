// The reductions behind Percentile / DistinctCount / TopK must give exactly what the straightforward versions give (full sort,
// std::unordered_set, std::unordered_map + ranked vector), on every data shape: few and many distinct values, around the point
// where DistinctCount leaves its flat table for sort + unique, duplicates, signed zeros, NaN.
#include <gtest/gtest.h>
#include "include/XStore/XStore_ExactAggregates.h"
#include <algorithm>
#include <cmath>
#include <limits>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

using Counted = std::vector<std::pair<double, uint64_t>>;

double ReferencePercentile(std::vector<double> v, double percentile) {
    std::sort(v.begin(), v.end());
    const double rank = percentile * static_cast<double>(v.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(rank));
    const size_t upper = static_cast<size_t>(std::ceil(rank));
    return v[lower] + (rank - static_cast<double>(lower)) * (v[upper] - v[lower]);
}

uint64_t ReferenceDistinct(const std::vector<double> &v) {
    return std::unordered_set<double>(v.begin(), v.end()).size();
}

Counted ReferenceTopK(const std::vector<double> &v, size_t k) {
    std::unordered_map<double, uint64_t> freq;
    for (double d : v) freq[d]++;
    Counted ranked(freq.begin(), freq.end());
    std::sort(ranked.begin(), ranked.end(), [](const auto &a, const auto &b) {
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });
    ranked.resize(std::min(k, ranked.size()));
    return ranked;
}

// `n` values drawn from `distinct` different ones (every value distinct when distinct >= n), shuffled.
std::vector<double> Values(size_t n, size_t distinct, uint32_t seed) {
    std::mt19937_64 gen(seed);
    std::vector<double> v;
    v.reserve(n);
    for (size_t i = 0; i < n; ++i) v.push_back(static_cast<double>(distinct >= n ? i : gen() % distinct) * 0.25 - 100.0);
    std::shuffle(v.begin(), v.end(), gen);
    return v;
}

TEST(ExactPercentileTest, MatchesAFullSortOnEveryShape) {
    for (size_t n : {1u, 2u, 3u, 10u, 11u, 1000u, 4097u}) {
        for (size_t distinct : {1u, 3u, 50u, 100000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n * 31 + distinct));
            for (double p : {0.0, 0.01, 0.25, 0.5, 0.75, 0.9, 0.999, 1.0}) {
                std::vector<double> work = data;
                EXPECT_EQ(xstore::ExactPercentile(work, p), ReferencePercentile(data, p)) << "n=" << n << " distinct=" << distinct << " p=" << p;
            }
        }
    }
}

TEST(ExactDistinctCountTest, MatchesAHashSetBelowAndAboveTheTableLimit) {
    for (size_t n : {0u, 1u, 5u, 1000u, 20000u}) {
        for (size_t distinct : {1u, 7u, 300u, 5000u, 1000000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n + distinct * 7));
            // limits below, at and above the number of distinct values, and one that is never reached
            for (size_t limit : {1u, 64u, 300u, 4096u, 1u << 20}) {
                std::vector<double> work = data;
                EXPECT_EQ(xstore::ExactDistinctCount(work, limit), ReferenceDistinct(data))
                    << "n=" << n << " distinct=" << distinct << " limit=" << limit;
            }
        }
    }
}

TEST(ExactDistinctCountTest, SignedZerosAreOneValueAndEveryNaNIsItsOwn) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const std::vector<double> data = {0.0, -0.0, 1.0, nan, nan, 2.0, -0.0, nan, 1.0};   // 0, 1, 2 and three NaNs
    for (size_t limit : {1u, 2u, 3u, 100u}) {
        std::vector<double> work = data;
        EXPECT_EQ(xstore::ExactDistinctCount(work, limit), ReferenceDistinct(data)) << "limit=" << limit;
        EXPECT_EQ(ReferenceDistinct(data), 6u);
    }
}

TEST(ExactTopKTest, MatchesAHashMapRankingOnEveryShape) {
    for (size_t n : {0u, 1u, 2u, 10u, 1000u, 20000u}) {
        for (size_t distinct : {1u, 2u, 7u, 300u, 1000000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n * 13 + distinct));
            for (size_t k : {1u, 3u, 10u, 100000u}) {
                std::vector<double> work = data;
                EXPECT_EQ(xstore::ExactTopK(work, k), ReferenceTopK(data, k)) << "n=" << n << " distinct=" << distinct << " k=" << k;
            }
        }
    }
}

TEST(ExactTopKTest, TiesAreBrokenByAscendingValueAndKZeroGivesNothing) {
    std::vector<double> data = {5.0, 3.0, 5.0, 3.0, 9.0, 1.0, 9.0, 1.0, 7.0};   // 1, 3, 5, 9 twice each; 7 once
    EXPECT_EQ(xstore::ExactTopK(data, 3), (Counted{{1.0, 2}, {3.0, 2}, {5.0, 2}}));
    std::vector<double> again = data;
    EXPECT_TRUE(xstore::ExactTopK(again, 0).empty());
}

TEST(ExactTopKTest, EveryNaNIsOneEntryOfCountOne) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<double> data = {4.0, 4.0, nan, 2.0, nan, 4.0, 2.0};   // 4 x3, 2 x2, two NaNs x1
    const Counted top = xstore::ExactTopK(data, 10);
    ASSERT_EQ(top.size(), 4u);
    EXPECT_EQ(top[0], (std::pair<double, uint64_t>{4.0, 3}));
    EXPECT_EQ(top[1], (std::pair<double, uint64_t>{2.0, 2}));
    EXPECT_TRUE(std::isnan(top[2].first) && top[2].second == 1);
    EXPECT_TRUE(std::isnan(top[3].first) && top[3].second == 1);
}

}  // namespace
