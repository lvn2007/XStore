#include "XStore/XStore_RowLayout.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>

using namespace xstore::layout;

namespace {
double NsPerRow(std::chrono::steady_clock::time_point start, double rows) {
    return std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count() / rows;
}
}  // namespace

TEST(RowLayoutPerf, ReadPackAndEncodeCostsPerRow) {
    constexpr size_t COLS = 128, ROWS = 200000, SLOT = 1280;
    XStore_Proto::RowData row;
    for (size_t c = 0; c < COLS; ++c) row.add_fielddata()->set_doubledata(static_cast<double>(c) * 1.37);
    const std::string wire = row.SerializeAsString();
    for (int typed = 0; typed < 2; ++typed) {
        std::vector<ColumnSpec> schema;
        if (typed != 0) schema.assign(COLS, ColumnSpec{Kind::kDouble, Storage::kDefault, 0});
        std::string error;
        const RowLayout layout = *RowLayout::Make(schema, SLOT, &error);
        std::vector<char> slots(ROWS * SLOT, 0);
        for (size_t r = 0; r < ROWS; ++r) ASSERT_TRUE(layout.PackFromWire(wire.data(), wire.size(), slots.data() + r * SLOT).Ok());
        for (unsigned column : {0u, 64u, 127u}) {
            const ColumnReader reader(layout, column);
            double sum = 0, v = 0;
            const auto start = std::chrono::steady_clock::now();
            for (int rep = 0; rep < 3; ++rep) {
                for (size_t r = 0; r < ROWS; ++r) {
                    if (reader.ReadNumeric(slots.data() + r * SLOT, &v)) sum += v;
                }
            }
            std::printf("[PERF] %s ColumnReader column %3u: %5.1f ns/row (sum %g)\n", typed != 0 ? "typed  " : "dynamic", column, NsPerRow(start, 3.0 * ROWS), sum);
        }
        std::vector<char> one(SLOT);
        auto start = std::chrono::steady_clock::now();
        size_t accepted = 0;
        for (size_t i = 0; i < ROWS; ++i) {
            std::fill(one.begin(), one.end(), 0);
            accepted += layout.PackFromWire(wire.data(), wire.size(), one.data()).Ok() ? 1 : 0;
        }
        std::printf("[PERF] %s PackFromWire (incl. zero fill): %6.0f ns/row (%zu)\n", typed != 0 ? "typed  " : "dynamic", NsPerRow(start, ROWS), accepted);
        std::string out;
        start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < ROWS; ++i) {
            out.clear();
            layout.EncodeToWire(slots.data() + i * SLOT, out);
        }
        std::printf("[PERF] %s EncodeToWire: %6.0f ns/row\n", typed != 0 ? "typed  " : "dynamic", NsPerRow(start, ROWS));
        size_t encoded = 0;
        start = std::chrono::steady_clock::now();
        for (size_t i = 0; i < ROWS; ++i) encoded += layout.EncodedSize(slots.data() + i * SLOT);
        std::printf("[PERF] %s EncodedSize: %6.0f ns/row (%zu)\n", typed != 0 ? "typed  " : "dynamic", NsPerRow(start, ROWS), encoded);
    }
}
