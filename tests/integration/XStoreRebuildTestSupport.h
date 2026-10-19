#pragma once
// Startup-rebuild test helpers shared by XStoreRebuildTest.cpp and XStoreParallelRebuildTest.cpp: the per-row
// reference rebuild (the oracle), the mixed-type row generator and raw year-file writers.
#include <gtest/gtest.h>
#include "XStore/XStore.h"
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_RowScan.h"
#include "XStore/XStore_WireEncode.h"
#include <fstream>
#include <iterator>
#include <mutex>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

namespace xstore_rebuild_test {

inline std::string ReadFileBytes(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

template <typename Header>
inline Header ReadHeader(const std::string &path) {
    Header h{};
    const int fd = open(path.c_str(), O_RDONLY);
    EXPECT_GE(fd, 0) << path;
    EXPECT_EQ(pread(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h))) << path;
    close(fd);
    return h;
}

// Independent of XStore::GetFDataValue: same "cases 1..12 are numeric" rule and the same value -> long double -> double
// path.
inline std::vector<ZoneMapObservation> RefZoneMapObservations(const XStore_Proto::RowData &row, uint32_t width) {
    std::vector<ZoneMapObservation> out;
    const int tracked = std::min(static_cast<int>(width), row.fielddata_size());
    for (int c = 0; c < tracked; ++c) {
        const XStore_Proto::FData &f = row.fielddata(c);
        long double v = 0;
        bool numeric = true;
        switch (f.oneOfData_case()) {
            case XStore_Proto::FData::kDoubleData: v = f.doubledata(); break;
            case XStore_Proto::FData::kFloatData: v = f.floatdata(); break;
            case XStore_Proto::FData::kInt32Data: v = f.int32data(); break;
            case XStore_Proto::FData::kInt64Data: v = f.int64data(); break;
            case XStore_Proto::FData::kUint32Data: v = f.uint32data(); break;
            case XStore_Proto::FData::kUint64Data: v = f.uint64data(); break;
            case XStore_Proto::FData::kSint32Data: v = f.sint32data(); break;
            case XStore_Proto::FData::kSint64Data: v = f.sint64data(); break;
            case XStore_Proto::FData::kFixed32Data: v = f.fixed32data(); break;
            case XStore_Proto::FData::kFixed64Data: v = f.fixed64data(); break;
            case XStore_Proto::FData::kSfixed32Data: v = f.sfixed32data(); break;
            case XStore_Proto::FData::kSfixed64Data: v = f.sfixed64data(); break;
            default: numeric = false; break;
        }
        out.push_back({static_cast<uint32_t>(c), static_cast<double>(v), numeric});
    }
    return out;
}

// The pre-stage-2a rebuild of one year, written to the given paths.
inline void BuildReferenceIndexes(XStore &store, const std::string &data_path, const std::string &zmap_path,
                           const std::string &catidx_path, uint16_t row_length, uint32_t width) {
    std::filesystem::remove(zmap_path);
    std::filesystem::remove(catidx_path);
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(zmap_path, row_length, width));
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open(catidx_path, row_length, width));
    const int fd = open(data_path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0) << data_path;
    const uint64_t total_rows = static_cast<uint64_t>(lseek(fd, 0, SEEK_END)) / row_length;
    const bool complete = ForEachDataRow(fd, total_rows, row_length, [&](uint64_t r, const char *buf) {
        if (buf[0] == 0) return;
        const size_t real_len = XStoreWireEncode::RealRowDataLength(buf, row_length);
        XStore_Proto::RowData row;
        if (real_len > 0) static_cast<void>(row.ParseFromArray(buf, static_cast<int>(real_len)));
        zmap.UpdateRow(r, RefZoneMapObservations(row, width), width);
        std::vector<CatIdxObservation> observations;
        for (uint32_t c = 0; c < width; ++c) {
            XStore_Proto::FData f;
            // An unset (null) column is skipped, like BuildCatIdxObservations (live inserts).
            if (store.ExtractColumnByIndexSkipParse(buf, row_length, static_cast<unsigned short>(c), f) &&
                f.oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) {
                observations.push_back({c, EncodeFDataForCatIdx(f), true});
            } else {
                observations.push_back({c, std::string(), false});
            }
        }
        catidx.UpdateRow(r, observations);
    });
    close(fd);
    ASSERT_TRUE(complete);
    ASSERT_TRUE(zmap.MarkClean());
    ASSERT_TRUE(catidx.MarkClean());
}

// Epoch of Jan 1 of `year` (SECOND resolution, UTC).
inline unsigned long long Jan1(unsigned short year) {
    unsigned long long days = 0;
    for (unsigned y = 1970; y < year; ++y) days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    return days * 86400ULL;
}

// A row of `columns` columns cycling through every FData type, with unset (null) columns sprinkled in.
inline XStore_Proto::RowData MixedRow(uint64_t i, int columns) {
    XStore_Proto::RowData row;
    for (int c = 0; c < columns; ++c) {
        XStore_Proto::FData *f = row.add_fielddata();
        const uint64_t v = i * 31 + static_cast<uint64_t>(c);
        if ((i + static_cast<uint64_t>(c)) % 17 == 0) continue; // null column (serialized as an empty column)
        switch (c % 16) {
            case 0: f->set_doubledata(static_cast<double>(v) * 0.5); break;
            case 1: f->set_floatdata(static_cast<float>(v) * 0.25f); break;
            case 2: f->set_int32data(-static_cast<int32_t>(v % 100000)); break;
            case 3: f->set_int64data(static_cast<int64_t>(v) * 1000003); break;
            case 4: f->set_uint32data(static_cast<uint32_t>(v)); break;
            case 5: f->set_uint64data(0xFFFFFFFFFFFF0000ULL + v); break; // above 2^53: rounding must match
            case 6: f->set_sint32data(-static_cast<int32_t>(v % 777)); break;
            case 7: f->set_sint64data(-static_cast<int64_t>(v)); break;
            case 8: f->set_fixed32data(static_cast<uint32_t>(v * 3)); break;
            case 9: f->set_fixed64data(v * 5); break;
            case 10: f->set_sfixed32data(-static_cast<int32_t>(v % 1000)); break;
            case 11: f->set_sfixed64data(-static_cast<int64_t>(v * 7)); break;
            case 12: f->set_booldata(v % 2 == 0); break;
            case 13: f->set_stringdata("sym" + std::to_string(v % 23)); break;
            case 14: f->set_bytesdata(std::string("\x01\x00\x02", 3) + std::to_string(v % 5)); break;
            default: f->set_doubledata(-static_cast<double>(v)); break;
        }
    }
    return row;
}

inline void PwriteRow(const std::string &data_path, uint64_t row, uint16_t row_length, const std::string &bytes) {
    std::string slot = bytes;
    slot.resize(row_length, '\0');
    const int fd = open(data_path.c_str(), O_WRONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, slot.data(), slot.size(), static_cast<off_t>(row * row_length)), static_cast<ssize_t>(slot.size()));
    close(fd);
}

inline std::string DoubleColumn(double v) {
    std::string out = "\x0a\x09\x09";
    out.append(reinterpret_cast<const char *>(&v), sizeof(v));
    return out;
}

// Progress options for a test: log lines (never a bar, whatever the terminal) every `interval_ms`, captured instead of
// going to spdlog. The reporter thread appends, the test thread reads after the engine returned.
struct ProgressCapture {
    std::mutex mutex;
    std::vector<std::string> lines;

    rebuildprogress::Options Options(unsigned interval_ms = 1) {
        rebuildprogress::Options options;
        options.mode = rebuildprogress::Mode::kLog;
        options.log_interval = std::chrono::milliseconds(interval_ms);
        options.log_line = [this](const std::string &line) {
            std::lock_guard<std::mutex> lock(mutex);
            lines.push_back(line);
        };
        return options;
    }
    std::vector<std::string> Lines() {
        std::lock_guard<std::mutex> lock(mutex);
        return lines;
    }
};

} // namespace xstore_rebuild_test
