#pragma once
#include <cstdint>
#include <cstring>
#include <string>

#include "XStore.pb.h"

// Canonical byte encoding for an FData value, used identically at insert
// time (BuildCatIdxObservations) and query time (FindByValue) so a bloom
// bit set for a value at insert is found by the same value's lookup at
// query time. Distinguishes oneof case explicitly (int64Data=5 and
// doubleData=5.0 encode differently) -- this is exact-type equality, not
// GetFDataValue's aggregate-style numeric coercion into one double.
inline std::string EncodeFDataForCatIdx(const XStore_Proto::FData &f_data) {
    std::string out;
    auto case_id = f_data.oneOfData_case();
    out.push_back(static_cast<char>(static_cast<uint8_t>(case_id)));

    switch (case_id) {
        case XStore_Proto::FData::kDoubleData: {
            double v = f_data.doubledata();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFloatData: {
            float v = f_data.floatdata();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kInt32Data: {
            int32_t v = f_data.int32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kInt64Data: {
            int64_t v = f_data.int64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kUint32Data: {
            uint32_t v = f_data.uint32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kUint64Data: {
            uint64_t v = f_data.uint64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSint32Data: {
            int32_t v = f_data.sint32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSint64Data: {
            int64_t v = f_data.sint64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFixed32Data: {
            uint32_t v = f_data.fixed32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFixed64Data: {
            uint64_t v = f_data.fixed64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSfixed32Data: {
            int32_t v = f_data.sfixed32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSfixed64Data: {
            int64_t v = f_data.sfixed64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kBoolData: {
            char v = f_data.booldata() ? 1 : 0;
            out.push_back(v);
            break;
        }
        case XStore_Proto::FData::kStringData:
            out.append(f_data.stringdata());
            break;
        case XStore_Proto::FData::kBytesData:
            out.append(f_data.bytesdata());
            break;
        default:
            // ONEOFDATA_NOT_SET -- a hole. Caller (BuildCatIdxObservations)
            // is responsible for not emitting an observation for holes at
            // all; this function is never called on an unset FData in
            // practice, but returns the tag-only bytes rather than
            // asserting, matching the codebase's existing "hole is not an
            // error" precedent (see BuildZoneMapObservations).
            break;
    }
    return out;
}

#include <array>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "XStore/XStore_Crc32c.h"
#include "XStore/XStore_Tunables.h"
#include "XStore/XStore_ZoneMap.h" // reuse RowsPerBlock/BlockIndexForRow -- same block geometry as .zmap

// One tracked-column value observed for one row, ready to fold into that
// row's block's bloom entry. is_present=false means a hole/unset field --
// contributes nothing (mirrors ZoneMapObservation's is_numeric=false case).
struct CatIdxObservation {
    uint32_t    column_idx;
    std::string encoded_value; // from EncodeFDataForCatIdx -- empty/ignored if !is_present
    bool        is_present;
};

struct CatIdxFileHeader {
    uint32_t magic                = 0x43415449; // 'CATI'
    uint32_t version               = XStoreTunables::CATIDX_VERSION;
    uint32_t tracked_column_count  = 0;
    uint32_t block_count           = 0;
    uint64_t bitmap_section_offset = 0; // 0 = not compacted yet
    // 1 = may hold writes not yet synced alongside the data file (set before
    // the first write of a session, cleared only after data + index are
    // fsynced); 1 at startup => rebuild. See Durability A plan.
    uint32_t dirty    = 0;
    uint32_t reserved = 0;
};
static_assert(sizeof(CatIdxFileHeader) == 32);

// FNV-1a 64-bit -- trivial, no external dependency, used as the second of
// two hash functions for Kirsch-Mitzenmacher double-hashing
// (g_i(x) = h1(x) + i*h2(x)), h1 being the existing hardware-accelerated
// Crc32c(). Repurposing Crc32c outside its original WAL-framing-checksum
// scope is fine functionally (it's just a fast, well-distributed hash here).
inline uint64_t Fnv1a64(const void *data, size_t len) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    const auto *bytes = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < len; ++i) {
        hash ^= bytes[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

class CatIdxFile {
public:
    [[nodiscard]] bool Open(const std::string &path, uint16_t row_length,
                             uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS);

    void UpdateRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations);

    bool MightContain(uint64_t block_index, uint32_t column_idx, const std::string &encoded_value);

    // Appends ALL surviving columns' value->row-indices directories to the
    // bitmap section in one call (commit-safety: writes every column's
    // directory bytes and fsyncs ONCE BEFORE updating+fsyncing
    // header_.bitmap_section_offset ONCE -- a crash between the two leaves
    // harmless unreferenced bytes past the last known-good offset, same
    // "uncommitted tail is safely ignorable" shape the WAL itself relies
    // on). Deliberately one call per compaction pass, not one per column:
    // calling this once per column used to mean only the FIRST column's
    // write got crash-safe commit semantics -- header_.bitmap_section_offset
    // became non-zero after that first call, so a crash during a LATER
    // column's write left torn bytes INSIDE the already-committed,
    // scannable range. Row indices per value need not be pre-sorted --
    // stored as given (compaction naturally produces them in scan order).
    void AppendBitmapSection(const std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> &columns);

    bool HasBitmapSection() const { return header_.bitmap_section_offset != 0; }

    // Returns the exact row indices for (column_idx, encoded_value) if a
    // directory exists for that column AND the value is present in it.
    // nullopt covers BOTH "no directory for this column" (over cardinality
    // cap, or year not compacted yet) and "directory exists but value
    // absent" -- both mean the same thing to the caller: fall back to
    // bloom+real-scan.
    std::optional<std::vector<uint64_t>> LookupExact(uint32_t column_idx, const std::string &encoded_value);

    ~CatIdxFile();

    uint64_t RowsPerBlockValue() const { return rows_per_block_; }

    // Clears the on-disk dirty flag: fdatasync(index content), then header
    // dirty=0 + fdatasync. Returns false on any failure, leaving the file
    // marked dirty. The caller must have fdatasync'd the .XSTORE data file
    // FIRST -- this only makes the index itself durable.
    [[nodiscard]] bool MarkClean();

    // Number of successful header dirty-mark writes (test-visible).
    [[nodiscard]] uint64_t DirtyMarkWritesForTests() const;

private:
    // Every method that writes index content calls this first (mutex_ held):
    // one predictable branch once the flag is on disk.
    void EnsureDirtyOnDisk() { if (!dirty_on_disk_) MarkDirtyOnDisk(); }
    void MarkDirtyOnDisk();

    using BloomEntry = std::array<unsigned char, XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY>;

    void EnsureBlockLoaded(uint64_t block_index);
    void EvictIfOverCapacity();
    off_t BloomSectionFileOffset(uint64_t block_index) const;
    bool WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count);
    void FlushBlock(uint64_t block_index);

    int fd_ = -1;
    CatIdxFileHeader header_;
    uint64_t rows_per_block_ = 0;
    bool dirty_on_disk_ = false; // header_.dirty == 1 is durable on disk
    uint64_t dirty_mark_writes_ = 0;
    std::unordered_map<uint64_t, std::vector<BloomEntry>> cache_; // block_index -> per-column bloom bytes
    std::vector<uint64_t> lru_order_;
    mutable std::mutex mutex_;
};
