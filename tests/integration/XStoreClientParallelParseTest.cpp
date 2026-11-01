// The parallel reply parse (part 2 of XStore_Client.h) and its use in XStoreClient::SendRecv, without a real server: the pure helpers are driven directly,
// and a one-shot in-process ZMQ REP socket plays the server for the end-to-end checks (a streamed reply of several frames).
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "XStore_Client.h"

namespace {
constexpr size_t K_ROWS = 5000;          // x ~1 KiB = ~5 MB, above the client's parallel-parse threshold
constexpr size_t K_FRAMES = 8;
constexpr const char *K_STATUS = "[WARNING] fragment test";

XStore_Proto::UnaryQuery_REP MakeRow(uint64_t ts) {
    XStore_Proto::UnaryQuery_REP row;
    row.set_timestamp(ts);
    *row.mutable_data()->add_fielddata() = xstore_fdata::String(std::string(1000, static_cast<char>('a' + ts % 26)));
    *row.mutable_data()->add_fielddata() = xstore_fdata::Bool(ts % 2 == 0);
    return row;
}

// One reply message (RangeQuery_REP unless asked otherwise) per frame, each cut on a row boundary like the server does; the status rides on
// the last one.
template <class RepT = XStore_Proto::RangeQuery_REP>
std::vector<std::string> MakeFrames(size_t frames, size_t rows, bool with_status) {
    std::vector<std::string> out;
    for (size_t f = 0; f < frames; ++f) {
        RepT part;
        for (size_t r = f * rows / frames; r < (f + 1) * rows / frames; ++r) *part.add_queryresponse() = MakeRow(r);
        if (with_status && f + 1 == frames) part.set_querystatus(K_STATUS);
        out.push_back(part.SerializeAsString());
    }
    return out;
}

std::string Whole(const std::vector<std::string> &frames) {
    std::string all;
    for (const std::string &f : frames) all += f;
    return all;
}

std::vector<xstore_client::Fragment> Fragments(const std::vector<std::string> &frames) {
    std::vector<xstore_client::Fragment> out;
    for (const std::string &f : frames) out.push_back({f.data(), f.size()});
    return out;
}

// Answers one request with the given frames, then exits. Bound to an ephemeral port.
class OneShotServer {
 public:
    explicit OneShotServer(std::vector<std::string> frames) : frames_(std::move(frames)), sock_(ctx_, zmq::socket_type::rep) {
        sock_.set(zmq::sockopt::rcvtimeo, 10000);
        sock_.set(zmq::sockopt::linger, 2000);
        sock_.bind("tcp://127.0.0.1:*");
        const std::string ep = sock_.get(zmq::sockopt::last_endpoint);
        port_ = ep.substr(ep.rfind(':') + 1);
        thread_ = std::thread([this] {
            zmq::message_t request;
            if (!sock_.recv(request)) return;
            for (size_t i = 0; i < frames_.size(); ++i)
                sock_.send(zmq::buffer(frames_[i]), i + 1 < frames_.size() ? zmq::send_flags::sndmore : zmq::send_flags::none);
        });
    }
    ~OneShotServer() { thread_.join(); }
    const std::string &port() const { return port_; }

 private:
    std::vector<std::string> frames_;
    zmq::context_t ctx_;
    zmq::socket_t sock_;
    std::string port_;
    std::thread thread_;
};
}  // namespace

namespace {
using xstore_client::Fragment;
using xstore_client::ParallelismPolicy;

ParallelismPolicy Policy(size_t fixed) {
    ParallelismPolicy policy;
    policy.fixed_threads = fixed;
    policy.max_threads = 8;
    policy.cores = 8;
    policy.tick = std::chrono::milliseconds(1);
    policy.runnable = [] { return std::optional<size_t>(1); };   // an idle machine
    return policy;
}
}  // namespace

// ---- splitting a reply into pieces ----

TEST(SplitIntoPiecesTest, PiecesJoinBackToTheFramesAndEachIsAWholeReply) {
    const std::vector<std::string> frames = MakeFrames(3, 900, true);
    for (size_t piece_bytes : {size_t{1}, size_t{1500}, size_t{20000}, size_t{1} << 30}) {
        std::vector<Fragment> pieces;
        ASSERT_TRUE(xstore_client::SplitIntoPieces(Fragments(frames), piece_bytes, pieces)) << piece_bytes;
        std::string joined;
        int rows = 0;
        for (const Fragment &piece : pieces) {
            joined.append(static_cast<const char *>(piece.data), piece.size);
            XStore_Proto::RangeQuery_REP part;
            ASSERT_TRUE(part.ParseFromArray(piece.data, static_cast<int>(piece.size))) << piece_bytes;   // cut between rows
            rows += part.queryresponse_size();
        }
        EXPECT_EQ(joined, Whole(frames));
        EXPECT_EQ(rows, 900);
        if (piece_bytes == 1) EXPECT_EQ(pieces.size(), 900u + 1u);       // a row each, and the status
        if (piece_bytes == (size_t{1} << 30)) EXPECT_EQ(pieces.size(), 3u);   // never across frames
    }
}

TEST(SplitIntoPiecesTest, SplittingFramesInParallelGivesTheSamePiecesInTheSameOrder) {
    const std::vector<std::string> frames = MakeFrames(8, 3000, true);
    for (size_t piece_bytes : {size_t{1}, size_t{4000}, size_t{300000}, size_t{1} << 30}) {
        std::vector<Fragment> serial, parallel;
        ASSERT_TRUE(xstore_client::SplitIntoPieces(Fragments(frames), piece_bytes, serial));
        for (size_t fixed : {size_t{1}, size_t{4}, size_t{0}}) {
            ASSERT_TRUE(xstore_client::SplitIntoPiecesParallel(Fragments(frames), piece_bytes, Policy(fixed), parallel)) << piece_bytes;
            ASSERT_EQ(parallel.size(), serial.size()) << piece_bytes << " fixed " << fixed;
            for (size_t i = 0; i < serial.size(); ++i) {
                ASSERT_EQ(parallel[i].data, serial[i].data) << i;
                ASSERT_EQ(parallel[i].size, serial[i].size) << i;
            }
        }
    }
}

TEST(SplitIntoPiecesTest, ParallelSplitRefusesMalformedFramingInAnyFrame) {
    std::vector<std::string> frames = MakeFrames(6, 600, true);
    frames[4] += std::string("\x0a\x7f\x01", 3);   // a dangling row at the end of one frame
    std::vector<Fragment> pieces;
    EXPECT_FALSE(xstore_client::SplitIntoPiecesParallel(Fragments(frames), 4096, Policy(4), pieces));
}

TEST(SplitIntoPiecesTest, AnEmptyReplyHasNoPieces) {
    std::vector<Fragment> pieces;
    const std::string empty;
    EXPECT_TRUE(xstore_client::SplitIntoPieces({{empty.data(), 0}}, 100, pieces));
    EXPECT_TRUE(pieces.empty());
}

TEST(SplitIntoPiecesTest, MalformedFramingIsRefused) {
    std::vector<Fragment> pieces;
    const std::string overrun("\x0a\x7f\x01\x02", 4);                  // a row claiming 127 bytes, 2 present
    const std::string truncated_varint("\x0a\xff\xff", 3);              // a length that never ends
    const std::string group("\x0b\x01", 2);                               // wire type 3 (start group)
    for (const std::string *bad : {&overrun, &truncated_varint, &group})
        EXPECT_FALSE(xstore_client::SplitIntoPieces({{bad->data(), bad->size()}}, 100, pieces));
}

// ---- parsing the pieces in parallel ----

TEST(ParallelParseTest, MergedReplyEqualsASinglePassParseForAnyPolicy) {
    const std::vector<std::string> frames = MakeFrames(K_FRAMES, K_ROWS, true);
    XStore_Proto::RangeQuery_REP expected;
    ASSERT_TRUE(expected.ParseFromString(Whole(frames)));
    for (size_t fixed : {size_t{1}, size_t{2}, size_t{8}, size_t{0}}) {
        XStore_Proto::RangeQuery_REP got;
        ASSERT_TRUE(xstore_client::ParseFragmentsParallel(Fragments(frames), got, Policy(fixed), /*piece_bytes=*/256 * 1024));
        ASSERT_EQ(got.queryresponse_size(), static_cast<int>(K_ROWS));
        ASSERT_EQ(got.queryresponse(0).timestamp(), 0u);                       // row order = frame order
        ASSERT_EQ(got.queryresponse(K_ROWS - 1).timestamp(), K_ROWS - 1);
        ASSERT_TRUE(got.has_querystatus());
        EXPECT_EQ(got.querystatus(), K_STATUS);
        EXPECT_TRUE(got.SerializeAsString() == expected.SerializeAsString()) << "fixed=" << fixed;
    }
}

TEST(ParallelParseTest, ReplyWithoutAStatusHasNone) {
    XStore_Proto::RangeQuery_REP got;
    ASSERT_TRUE(xstore_client::ParseFragmentsParallel(Fragments(MakeFrames(4, 100, false)), got, Policy(4), 4096));
    EXPECT_FALSE(got.has_querystatus());
    EXPECT_EQ(got.queryresponse_size(), 100);
}

TEST(ParallelParseTest, ASingleBigFrameIsParallelToo) {
    const std::vector<std::string> frames = MakeFrames(1, K_ROWS, true);
    XStore_Proto::RangeQuery_REP got;
    ASSERT_TRUE(xstore_client::ParseFragmentsParallel(Fragments(frames), got, Policy(4), 256 * 1024));
    EXPECT_EQ(got.queryresponse_size(), static_cast<int>(K_ROWS));
}

TEST(ParallelParseTest, ASmallReplyIsLeftToTheSerialParse) {
    XStore_Proto::RangeQuery_REP got;
    EXPECT_FALSE(xstore_client::ParseFragmentsParallel(Fragments(MakeFrames(1, 10, true)), got, Policy(4), 1 << 20));   // one piece
    EXPECT_EQ(got.queryresponse_size(), 0);
}

TEST(ParallelParseTest, MalformedFramingAndBadRowsFailAndLeaveTheReplyUntouched) {
    const std::vector<std::string> good = MakeFrames(K_FRAMES, 800, true);
    const std::string all = Whole(good);
    // a cut inside a row: the split refuses it
    const std::vector<std::string> cut = {all.substr(0, 3), all.substr(3)};
    // well framed, but the row inside is not a message
    const std::string bad_row("\x0a\x03\xff\xff\xff", 5);
    const std::vector<std::string> bad_inside = {good[0], bad_row, good[1]};
    for (const std::vector<std::string> *frames : {&cut, &bad_inside}) {
        XStore_Proto::RangeQuery_REP got;
        got.set_querystatus("untouched");
        EXPECT_FALSE(xstore_client::ParseFragmentsParallel(Fragments(*frames), got, Policy(4), 4096));
        EXPECT_EQ(got.queryresponse_size(), 0);
        EXPECT_EQ(got.querystatus(), "untouched");
    }
}

TEST(ParallelParseTest, EnvironmentChoosesFixedOrAdaptiveAndTheCap) {
    const size_t cores = std::max<size_t>(1, std::thread::hardware_concurrency());
    unsetenv("XSTORE_CLIENT_THREADS");
    unsetenv("XSTORE_CLIENT_THREADS_MAX");
    EXPECT_EQ(xstore_client::TeamPolicy(0).fixed_threads, 0u);   // 0 = adaptive
    EXPECT_EQ(xstore_client::TeamPolicy(0).max_threads, cores);
    EXPECT_EQ(xstore_client::TeamPolicy(6).fixed_threads, 6u);   // the caller's choice wins
    setenv("XSTORE_CLIENT_THREADS", "3", 1);
    EXPECT_EQ(xstore_client::TeamPolicy(0).fixed_threads, 3u);
    EXPECT_EQ(xstore_client::TeamPolicy(6).fixed_threads, 6u);
    setenv("XSTORE_CLIENT_THREADS", "adaptive", 1);
    EXPECT_EQ(xstore_client::TeamPolicy(0).fixed_threads, 0u);
    setenv("XSTORE_CLIENT_THREADS", "nonsense", 1);
    EXPECT_EQ(xstore_client::TeamPolicy(0).fixed_threads, 0u);
    setenv("XSTORE_CLIENT_THREADS_MAX", "5", 1);
    EXPECT_EQ(xstore_client::TeamPolicy(0).max_threads, 5u);
    unsetenv("XSTORE_CLIENT_THREADS");
    unsetenv("XSTORE_CLIENT_THREADS_MAX");
}

TEST(ParallelParseTest, OnlyRepliesWithRowsAreMergeable) {
    static_assert(xstore_client::IsFragmentMergeable<XStore_Proto::RangeQuery_REP>::value);
    static_assert(xstore_client::IsFragmentMergeable<XStore_Proto::BatchQuery_REP>::value);
    static_assert(xstore_client::IsFragmentMergeable<XStore_Proto::FindByValue_REP>::value);
    static_assert(xstore_client::IsFragmentMergeable<XStore_Proto::FindByPattern_REP>::value);
    static_assert(!xstore_client::IsFragmentMergeable<XStore_Proto::Ping_REP>::value);
}

// ---- end to end through the four calls that return rows ----

// The reply a call gives for `frames`, as the bytes of one message, for each of the four row-returning calls.
template <class RepT>
std::string ViaTheCall(XStoreClient &client);
template <>
std::string ViaTheCall<XStore_Proto::RangeQuery_REP>(XStoreClient &client) {
    return client.RangeQuery(0, K_ROWS, "ANY")->SerializeAsString();
}
template <>
std::string ViaTheCall<XStore_Proto::BatchQuery_REP>(XStoreClient &client) {
    return client.BatchQuery("ANY", {0, 1, 2}, true)->SerializeAsString();
}
template <>
std::string ViaTheCall<XStore_Proto::FindByValue_REP>(XStoreClient &client) {
    return client.FindByValue(0, K_ROWS, "ANY", 0, xstore_fdata::Double(1.0))->SerializeAsString();
}
template <>
std::string ViaTheCall<XStore_Proto::FindByPattern_REP>(XStoreClient &client) {
    return client.FindByPattern(0, K_ROWS, "ANY", 0, "a%")->SerializeAsString();
}

template <class RepT>
void ExpectEveryShapeOfReplyGivesTheSameBytesAsASerialParse() {
    struct Shape {
        const char *name;
        std::vector<std::string> frames;
    };
    const std::string all = Whole(MakeFrames<RepT>(1, K_ROWS, true));
    const std::vector<Shape> shapes = {
        {"streamed frames", MakeFrames<RepT>(K_FRAMES, K_ROWS, true)},
        {"one big frame", MakeFrames<RepT>(1, K_ROWS, true)},
        {"small reply (serial parse)", MakeFrames<RepT>(1, 3, true)},
        {"small streamed reply (join)", MakeFrames<RepT>(3, 30, false)},
        {"cut inside a row (join fallback)", {all.substr(0, 3), all.substr(3)}},
    };
    for (const Shape &shape : shapes) {
        RepT expected;
        ASSERT_TRUE(expected.ParseFromString(Whole(shape.frames))) << shape.name;
        for (size_t threads : {size_t{1}, size_t{8}, size_t{0}}) {   // 0 = adaptive
            OneShotServer server(shape.frames);
            XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
            client.SetThreads(threads);
            EXPECT_TRUE(ViaTheCall<RepT>(client) == expected.SerializeAsString()) << shape.name << " threads=" << threads;
        }
    }
}

TEST(ClientStreamedReplyTest, RangeQueryGivesTheSameReplyForEveryParseThreadSetting) {
    ExpectEveryShapeOfReplyGivesTheSameBytesAsASerialParse<XStore_Proto::RangeQuery_REP>();
}
TEST(ClientStreamedReplyTest, BatchQueryGivesTheSameReplyForEveryParseThreadSetting) {
    ExpectEveryShapeOfReplyGivesTheSameBytesAsASerialParse<XStore_Proto::BatchQuery_REP>();
}
TEST(ClientStreamedReplyTest, FindByValueGivesTheSameReplyForEveryParseThreadSetting) {
    ExpectEveryShapeOfReplyGivesTheSameBytesAsASerialParse<XStore_Proto::FindByValue_REP>();
}
TEST(ClientStreamedReplyTest, FindByPatternGivesTheSameReplyForEveryParseThreadSetting) {
    ExpectEveryShapeOfReplyGivesTheSameBytesAsASerialParse<XStore_Proto::FindByPattern_REP>();
}

TEST(ClientStreamedReplyTest, TheStatusAndTheRowCountComeThrough) {
    const std::vector<std::string> frames = MakeFrames(K_FRAMES, K_ROWS, true);
    OneShotServer server(frames);
    XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
    client.SetThreads(8);
    const auto rep = client.RangeQuery(0, K_ROWS, "ANY");
    EXPECT_EQ(rep->queryresponse_size(), static_cast<int>(K_ROWS));
    EXPECT_EQ(rep->querystatus(), K_STATUS);
}

TEST(ClientStreamedReplyTest, ASingleFrameReplyIsParsedInParallelToo) {
    const std::vector<std::string> frames = MakeFrames(1, K_ROWS * 4, true);   // ~20 MB in one frame
    OneShotServer server(frames);
    XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
    client.SetThreads(4);
    const auto rep = client.RangeQuery(0, K_ROWS * 4, "ANY");
    XStore_Proto::RangeQuery_REP expected;
    ASSERT_TRUE(expected.ParseFromString(Whole(frames)));
    EXPECT_TRUE(rep->SerializeAsString() == expected.SerializeAsString());
}

// ---- the reply lives on one Arena, and the wrapper owns it ----

// Every message of a reply must sit on the SAME arena: a row left on the heap (or on another arena) would be freed by whoever owns it
// and again by the arena, or read after its owner is gone.
void ExpectOneArena(const XStore_Proto::RangeQuery_REP &rep) {
    ASSERT_NE(rep.GetArena(), nullptr);
    for (const auto &row : rep.queryresponse()) {
        ASSERT_EQ(row.GetArena(), rep.GetArena());
        ASSERT_EQ(row.data().GetArena(), rep.GetArena());
    }
}

TEST(ArenaReplyTest, EveryRowOfEveryShapeOfReplySitsOnTheRepliesArena) {
    const std::string all = Whole(MakeFrames(1, K_ROWS, true));
    const std::vector<std::vector<std::string>> shapes = {MakeFrames(K_FRAMES, K_ROWS, true), MakeFrames(1, K_ROWS, true), MakeFrames(1, 3, true),
                                                          MakeFrames(3, 30, false), {all.substr(0, 3), all.substr(3)}};
    for (const auto &frames : shapes) {
        OneShotServer server(frames);
        XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
        client.SetThreads(4);
        const auto rep = client.RangeQuery(0, K_ROWS, "ANY");
        ExpectOneArena(*rep);
    }
}

TEST(ArenaReplyTest, TheReplyOutlivesTheClientAndSurvivesMoves) {
    const std::vector<std::string> frames = MakeFrames(K_FRAMES, K_ROWS, true);
    XStore_Proto::RangeQuery_REP expected;
    ASSERT_TRUE(expected.ParseFromString(Whole(frames)));
    std::optional<xstore_client::ArenaReply<XStore_Proto::RangeQuery_REP>> kept;
    {
        OneShotServer server(frames);
        XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
        kept.emplace(client.RangeQuery(0, K_ROWS, "ANY"));
    }   // client and server gone: the frames the reply was parsed from are gone too
    xstore_client::ArenaReply<XStore_Proto::RangeQuery_REP> moved(std::move(*kept));
    EXPECT_EQ(kept->get(), nullptr);   // the moved-from wrapper owns nothing
    EXPECT_TRUE(moved->SerializeAsString() == expected.SerializeAsString());
    xstore_client::ArenaReply<XStore_Proto::RangeQuery_REP> assigned;
    assigned = std::move(moved);       // frees the empty reply `assigned` held, takes the big one
    EXPECT_EQ(moved.get(), nullptr);
    EXPECT_TRUE(assigned->SerializeAsString() == expected.SerializeAsString());
}

TEST(ArenaReplyTest, AssigningOverAReplyFreesTheOldOneAndKeepsTheNewOne) {
    xstore_client::ArenaReply<XStore_Proto::RangeQuery_REP> reply;
    for (int round = 0; round < 3; ++round) {
        const std::vector<std::string> frames = MakeFrames(K_FRAMES, K_ROWS + static_cast<size_t>(round), false);
        OneShotServer server(frames);
        XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
        reply = client.RangeQuery(0, K_ROWS, "ANY");   // the pattern a benchmark loop uses
        EXPECT_EQ(reply->queryresponse_size(), static_cast<int>(K_ROWS) + round);
    }
}

TEST(ArenaReplyTest, ParsingIntoAnArenaGivesTheHeapParseAndLeavesTheResultUntouchedOnFailure) {
    const std::vector<std::string> frames = MakeFrames(4, 2000, true);
    XStore_Proto::RangeQuery_REP expected;
    ASSERT_TRUE(expected.ParseFromString(Whole(frames)));
    google::protobuf::Arena arena;
    auto *out = google::protobuf::Arena::Create<XStore_Proto::RangeQuery_REP>(&arena);
    ASSERT_TRUE(xstore_client::ParseFragmentsParallel(Fragments(frames), *out, Policy(4), 1 << 14, &arena));
    EXPECT_TRUE(out->SerializeAsString() == expected.SerializeAsString());
    ExpectOneArena(*out);

    auto *untouched = google::protobuf::Arena::Create<XStore_Proto::RangeQuery_REP>(&arena);
    std::string bad = frames[0];
    bad.resize(bad.size() - 5);   // cut mid-row
    EXPECT_FALSE(xstore_client::ParseFragmentsParallel(Fragments({bad, frames[1]}), *untouched, Policy(4), 1 << 14, &arena));
    EXPECT_EQ(untouched->queryresponse_size(), 0);
}
