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
