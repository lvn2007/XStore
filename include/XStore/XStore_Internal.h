#ifndef XSTORE_XSTORE_INTERNAL_H
#define XSTORE_XSTORE_INTERNAL_H

#include "include/XStore/XStore.h"
#include "include/DataStructs/FileRangeLock.h"
#include <cstring>

// Helpers shared by the XStore_*.cpp translation units. Not part of the
// public XStore.h surface -- callers outside src/XStore/ have no reason to
// see these.

// Writes tag+body directly into *status (a protobuf string field's own
// storage, via mutable_*()/add_*()) instead of building a separate "[TAG] "
// + body temporary and letting set_*() copy that in -- one allocation
// (sized exactly via reserve) instead of two, no intermediate std::string.
// Returns status so callers needing to append more (see RemoveDBFiles'
// filename-append site) can chain off it.
std::string* SetTaggedStatus(std::string *status, const char *tag, const std::string &body);

// Shared by XStore_BatchOps.cpp's BatchInsert and XStore_RangeOps.cpp's
// RangeInsert: writes one year-group's rows into an already-mapped file
// under a single lock spanning the group's min-to-max offset (instead of one
// lock per row). GetEntry(idx) must return a message with
// .timestamp()/.data() -- BatchInsert_DATA and RangeInsert_DATA both
// qualify, so this is templated on the accessor rather than the message
// type to avoid materializing an intermediate row vector per call.
// Precondition (same as RangeInsert's existing one): indices is ordered so
// that indices.front()/back() carry the group's earliest/latest timestamp.
// Templated on EntryAccessor, a distinct lambda type at each of the two call
// sites, so the definition has to stay visible here (a header) rather than
// living in a single .cpp -- both callers instantiate it with their own
// accessor lambda.
template <typename EntryAccessor>
void WriteRowGroupToMappedFile(const std::string &targetFile, void *mappedBase, long firstEpoch,
                                unsigned int rowLength, const std::vector<unsigned long long int> &indices,
                                EntryAccessor &&getEntry) {
    const unsigned long long int FirstOffset = (static_cast<unsigned long long>(getEntry(indices.front()).timestamp()) - firstEpoch) * rowLength;
    const unsigned long long int LastOffset = (static_cast<unsigned long long>(getEntry(indices.back()).timestamp()) - firstEpoch) * rowLength;
    const unsigned long long int RegionLength = (LastOffset + rowLength) - FirstOffset;

    FileRangeLockGuard Guard(targetFile, FirstOffset, RegionLength, FileRangeLock::LockType::WRITE);

    for (unsigned long long int Idx : indices) {
        const auto &Entry = getEntry(Idx);
        const XStore_Proto::RowData &Data = Entry.data();

        // Rows that don't fit at all are rejected rather than silently
        // truncated (matches UnaryInsert's check).
        if (Data.ByteSizeLong() > static_cast<size_t>(rowLength)) {
            continue;
        }

        unsigned long long int Offset = (static_cast<unsigned long long>(Entry.timestamp()) - firstEpoch) * rowLength;

        // Zero the slot first, then serialize directly into it -- avoids a
        // heap allocation + copy per row, and guarantees no stale trailing
        // bytes from a previous occupant leak through (same as UnaryInsert).
        char *Slot = static_cast<char*>(mappedBase) + Offset;
        std::memset(Slot, 0, rowLength);
        Data.SerializeToArray(Slot, rowLength);
    }
}

#endif //XSTORE_XSTORE_INTERNAL_H
