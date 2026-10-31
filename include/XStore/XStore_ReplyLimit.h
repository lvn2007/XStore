#pragma once

#include <atomic>

namespace xstore {

// The largest reply, in bytes, that may be given to a client which joins the chunks of a reply and parses ONE protobuf message: protobuf
// cannot parse or serialize a message of 2 GB or more. RangeQuery and BatchQuery refuse a request estimated over it before sending anything;
// FindByValue and FindByPattern, whose size is only known as they scan, stop at it and say so in the reply's status. A reply sent as separate
// messages to a client that parses chunk by chunk (Request.streamOk) is not subject to it. Tests lower it to exercise the guards on a few KB.
inline std::atomic<unsigned long long> &ReplyLimitBytes() {
    static std::atomic<unsigned long long> limit{2'000'000'000};
    return limit;
}

}  // namespace xstore
