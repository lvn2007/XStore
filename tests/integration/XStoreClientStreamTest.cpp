// Replies that arrive as several messages (ZMQ_Server.cpp ReplyWriter, Request.streamOk): the client parses each chunk while the next is still
// on the wire. Part 1 (JobStream: jobs fed one at a time to a load-aware team), part 2 (ReplyStreamParser: chunks fed one at a time, rows
// merged at the end) and the whole client against a fake server that plays the streamed protocol.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "XStore_Client.h"

namespace {
using xstore_client::Fragment;
using xstore_client::ParallelismPolicy;
using Clock = std::chrono::steady_clock;

constexpr size_t K_ROWS = 6000;   // x ~1 KiB = ~6 MB, above the client's parallel-parse threshold
constexpr const char *K_STATUS = "[WARNING] stream test";

ParallelismPolicy Fixed(size_t threads) {
    ParallelismPolicy policy;
    policy.fixed_threads = threads;
    return policy;
}

// An adaptive policy on a pretend machine: `runnable` threads are runnable whatever the team does.
ParallelismPolicy Machine(size_t cores, size_t runnable) {
    ParallelismPolicy policy;
    policy.cores = cores;
    policy.max_threads = cores;
    policy.tick = std::chrono::milliseconds(1);
    policy.runnable = [runnable]() -> std::optional<size_t> { return runnable; };
    return policy;
}

template <class Pred>
bool WaitFor(Pred pred, std::chrono::milliseconds limit = std::chrono::milliseconds(5000)) {
    const auto deadline = Clock::now() + limit;
    while (!pred()) {
        if (Clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

// ---- part 1: JobStream ----

TEST(JobStreamTest, EveryJobRunsOnceAndFinishReturnsWhenAllAreDone) {
    for (const ParallelismPolicy &policy : {Fixed(1), Fixed(4), Machine(8, 1), Machine(8, 100)}) {
        std::vector<std::atomic<int>> runs(300);
        auto fn = [&](size_t &job) { runs[job].fetch_add(1); };
        xstore_client::JobStream<size_t, decltype(fn)> stream(policy, fn);
        for (size_t job = 0; job < runs.size(); ++job) stream.Submit(size_t{job});
        stream.Finish();
        for (size_t job = 0; job < runs.size(); ++job) ASSERT_EQ(runs[job].load(), 1) << "job " << job << " fixed=" << policy.fixed_threads;
    }
}

TEST(JobStreamTest, HelpersWorkWhileTheProducerIsStillSubmitting) {
    std::atomic<int> done{0};
    auto fn = [&](int &) { done.fetch_add(1); };
    xstore_client::JobStream<int, decltype(fn)> stream(Fixed(3), fn);   // the caller + 2 helpers
    // One job at a time, each after the helpers have gone back to sleep: a job submitted to a sleeping team must wake it.
    for (int job = 0; job < 20; ++job) {
        stream.Submit(int{job});
        ASSERT_TRUE(WaitFor([&] { return done.load() == job + 1; })) << "job " << job << " was not taken before Finish() was called";
    }
    stream.Finish();
}

TEST(JobStreamTest, ASingleThreadTeamLeavesTheJobsToFinish) {
    std::atomic<int> done{0};
    auto fn = [&](int &) { done.fetch_add(1); };
    xstore_client::JobStream<int, decltype(fn)> stream(Fixed(1), fn);   // no helpers
    for (int job = 0; job < 20; ++job) stream.Submit(int{job});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(done.load(), 0);
    stream.Finish();
    EXPECT_EQ(done.load(), 20);
}

TEST(JobStreamTest, AnAdaptiveTeamStartsWithOneHelperAndGrowsWhileThereIsABacklogAndRoom) {
    std::mutex mutex;
    std::set<std::thread::id> threads;
    auto fn = [&](int &) {
        std::this_thread::sleep_for(std::chrono::milliseconds(4));
        std::lock_guard<std::mutex> lock(mutex);
        threads.insert(std::this_thread::get_id());
    };
    xstore_client::JobStream<int, decltype(fn)> stream(Machine(16, 1), fn);   // an idle 16-core machine
    for (int job = 0; job < 200; ++job) stream.Submit(int{job});
    stream.Finish();
    EXPECT_GE(threads.size(), 3u);
}

TEST(JobStreamTest, ABusyMachineSendsTheHelpersHomeAndTheCallerFinishesTheRest) {
    std::atomic<int> done{0};
    auto fn = [&](int &) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        done.fetch_add(1);
    };
    xstore_client::JobStream<int, decltype(fn)> stream(Machine(4, 40), fn);   // 40 runnable threads on 4 cores
    for (int job = 0; job < 100; ++job) stream.Submit(int{job});
    stream.Finish();
    EXPECT_EQ(done.load(), 100);
}

TEST(JobStreamTest, AJobThatThrowsStopsTheTeamAndFinishRethrows) {
    auto fn = [&](int &job) {
        if (job == 7) throw std::runtime_error("job 7 failed");
    };
    for (const ParallelismPolicy &policy : {Fixed(1), Fixed(4)}) {
        xstore_client::JobStream<int, decltype(fn)> stream(policy, fn);
        for (int job = 0; job < 50; ++job) stream.Submit(int{job});
        EXPECT_THROW(stream.Finish(), std::runtime_error);
    }
}

TEST(JobStreamTest, DroppingAStreamWithoutFinishingStopsItsHelpers) {
    std::atomic<int> started{0};
    auto fn = [&](int &) {
        started.fetch_add(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    };
    {
        xstore_client::JobStream<int, decltype(fn)> stream(Fixed(4), fn);
        for (int job = 0; job < 1000; ++job) stream.Submit(int{job});
    }   // the producer failed (a recv timed out, say): the destructor must not hang or leave threads behind
    const int after = started.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    EXPECT_EQ(started.load(), after);
}

// ---- part 2: ReplyStreamParser ----

XStore_Proto::UnaryQuery_REP MakeRow(uint64_t ts) {
    XStore_Proto::UnaryQuery_REP row;
    row.set_timestamp(ts);
    *row.mutable_data()->add_fielddata() = xstore_fdata::String(std::string(1000, static_cast<char>('a' + ts % 26)));
    *row.mutable_data()->add_fielddata() = xstore_fdata::Bool(ts % 2 == 0);
    return row;
}

// Chunks cut on row boundaries like the server's: `chunks` pieces of `rows` rows, the status on the last.
std::vector<std::string> MakeChunks(size_t chunks, size_t rows, bool with_status) {
    std::vector<std::string> out;
    for (size_t c = 0; c < chunks; ++c) {
        XStore_Proto::RangeQuery_REP part;
        for (size_t r = c * rows / chunks; r < (c + 1) * rows / chunks; ++r) *part.add_queryresponse() = MakeRow(r);
        if (with_status && c + 1 == chunks) part.set_querystatus(K_STATUS);
        out.push_back(part.SerializeAsString());
    }
    return out;
}

std::string Whole(const std::vector<std::string> &chunks) {
    std::string all;
    for (const std::string &c : chunks) all += c;
    return all;
}

TEST(ReplyStreamParserTest, ChunksAddedOneByOneGiveTheRowsOfASerialParse) {
    const std::vector<std::string> chunks = MakeChunks(5, K_ROWS, true);
    XStore_Proto::RangeQuery_REP expected;
    ASSERT_TRUE(expected.ParseFromString(Whole(chunks)));
    for (size_t threads : {size_t{1}, size_t{4}, size_t{0}}) {
        for (bool on_arena : {false, true}) {
            google::protobuf::Arena arena;
            XStore_Proto::RangeQuery_REP heap_out;
            XStore_Proto::RangeQuery_REP *out = on_arena ? google::protobuf::Arena::Create<XStore_Proto::RangeQuery_REP>(&arena) : &heap_out;
            xstore_client::ReplyStreamParser<XStore_Proto::RangeQuery_REP> parser(*out, on_arena ? &arena : nullptr, Fixed(threads));
            for (const std::string &chunk : chunks) parser.Add({chunk.data(), chunk.size()});
            ASSERT_TRUE(parser.Finish()) << "threads=" << threads << " arena=" << on_arena;
            EXPECT_TRUE(out->SerializeAsString() == expected.SerializeAsString()) << "threads=" << threads << " arena=" << on_arena;
            if (on_arena) {
                ASSERT_NE(out->GetArena(), nullptr);
                for (const auto &row : out->queryresponse()) ASSERT_EQ(row.GetArena(), out->GetArena());
            }
        }
    }
}

TEST(ReplyStreamParserTest, AChunkIsParsedBeforeTheNextOneArrivesNotAtTheEnd) {
    const std::vector<std::string> chunks = MakeChunks(2, K_ROWS, false);
    XStore_Proto::RangeQuery_REP out;
    xstore_client::ReplyStreamParser<XStore_Proto::RangeQuery_REP> parser(out, nullptr, Fixed(3));
    parser.Add({chunks[0].data(), chunks[0].size()});
    // The second chunk has not "arrived": the helpers must already be parsing the first.
    EXPECT_TRUE(WaitFor([&] { return parser.PiecesParsed() >= 1; }));
    parser.Add({chunks[1].data(), chunks[1].size()});
    ASSERT_TRUE(parser.Finish());
    EXPECT_EQ(out.queryresponse_size(), static_cast<int>(K_ROWS));
}

TEST(ReplyStreamParserTest, ABadChunkMakesFinishFalseAndLeavesTheResultUntouched) {
    const std::vector<std::string> chunks = MakeChunks(3, K_ROWS, true);
    std::string cut = chunks[1];
    cut.resize(cut.size() - 5);   // ends inside a row
    for (const std::vector<std::string> &stream : {std::vector<std::string>{chunks[0], cut, chunks[2]}, std::vector<std::string>{chunks[0], std::string("\xff\xff\xff", 3)}}) {
        XStore_Proto::RangeQuery_REP out;
        xstore_client::ReplyStreamParser<XStore_Proto::RangeQuery_REP> parser(out, nullptr, Fixed(3));
        for (const std::string &chunk : stream) parser.Add({chunk.data(), chunk.size()});
        EXPECT_FALSE(parser.Finish());
        EXPECT_EQ(out.queryresponse_size(), 0);
    }
}

TEST(ReplyStreamParserTest, DroppingAParserMidReplyDoesNotHang) {
    const std::vector<std::string> chunks = MakeChunks(4, K_ROWS, false);
    XStore_Proto::RangeQuery_REP out;
    {
        xstore_client::ReplyStreamParser<XStore_Proto::RangeQuery_REP> parser(out, nullptr, Fixed(4));
        parser.Add({chunks[0].data(), chunks[0].size()});
        parser.Add({chunks[1].data(), chunks[1].size()});
    }   // the receive failed before the rest arrived
    SUCCEED();
}

// ---- the client against a fake streaming server ----

// One message of a scripted reply: [delimiter][frames...], sent after `pause_ms`.
struct Step {
    std::string delimiter;
    std::vector<std::string> frames;
    int pause_ms = 0;
};

// A ROUTER that answers the requests it gets, in order, with one script each, then exits. Records each request's bytes.
class ScriptedServer {
 public:
    explicit ScriptedServer(std::vector<std::vector<Step>> scripts) : scripts_(std::move(scripts)), sock_(ctx_, zmq::socket_type::router) {
        sock_.set(zmq::sockopt::rcvtimeo, 15000);
        sock_.set(zmq::sockopt::linger, 2000);
        sock_.bind("tcp://127.0.0.1:*");
        const std::string ep = sock_.get(zmq::sockopt::last_endpoint);
        port_ = ep.substr(ep.rfind(':') + 1);
        thread_ = std::thread([this] {
            for (const std::vector<Step> &script : scripts_) {
                std::vector<zmq::message_t> request;   // [identity][empty][body]
                if (!zmq::recv_multipart(sock_, std::back_inserter(request)) || request.size() != 3) return;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    requests_.emplace_back(static_cast<const char *>(request[2].data()), request[2].size());
                }
                for (const Step &step : script) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(step.pause_ms));
                    zmq::message_t id;
                    id.copy(request[0]);
                    (void)sock_.send(id, zmq::send_flags::sndmore);
                    (void)sock_.send(zmq::buffer(step.delimiter), zmq::send_flags::sndmore);
                    for (size_t i = 0; i < step.frames.size(); ++i)
                        (void)sock_.send(zmq::buffer(step.frames[i]), i + 1 < step.frames.size() ? zmq::send_flags::sndmore : zmq::send_flags::none);
                    if (step.frames.empty()) (void)sock_.send(zmq::message_t(), zmq::send_flags::none);
                }
            }
        });
    }
    ~ScriptedServer() { thread_.join(); }
    const std::string &port() const { return port_; }
    std::vector<std::string> requests() {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_;
    }

 private:
    std::vector<std::vector<Step>> scripts_;
    zmq::context_t ctx_;
    zmq::socket_t sock_;
    std::string port_;
    std::thread thread_;
    std::mutex mutex_;
    std::vector<std::string> requests_;
};

const std::string MORE(1, '\x01');

// The chunks as a streamed reply: every message but the last says more follows.
std::vector<Step> Streamed(const std::vector<std::string> &chunks, int pause_ms = 0) {
    std::vector<Step> script;
    for (size_t i = 0; i < chunks.size(); ++i) script.push_back({i + 1 < chunks.size() ? MORE : std::string(), {chunks[i]}, pause_ms});
    return script;
}

template <class RepT>
std::vector<std::string> ChunksOf(size_t chunks, size_t rows, bool with_status) {
    std::vector<std::string> out;
    for (size_t c = 0; c < chunks; ++c) {
        RepT part;
        for (size_t r = c * rows / chunks; r < (c + 1) * rows / chunks; ++r) *part.add_queryresponse() = MakeRow(r);
        if (with_status && c + 1 == chunks) part.set_querystatus(K_STATUS);
        out.push_back(part.SerializeAsString());
    }
    return out;
}

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
void ExpectEveryStreamedShapeGivesTheBytesOfASerialParse() {
    struct Shape {
        const char *name;
        std::vector<Step> script;
        std::string wire;
    };
    const auto chunks = ChunksOf<RepT>(6, K_ROWS, true);
    const auto small = ChunksOf<RepT>(3, 30, false);
    const auto one = ChunksOf<RepT>(1, K_ROWS, true);
    std::vector<Shape> shapes;
    shapes.push_back({"six chunks", Streamed(chunks), Whole(chunks)});
    shapes.push_back({"three small chunks", Streamed(small), Whole(small)});
    shapes.push_back({"one big final message", Streamed(one), Whole(one)});
    {   // the last message carries no chunk, as when a handler's reply is cut short and completed
        std::vector<Step> script = Streamed(std::vector<std::string>(chunks.begin(), chunks.end() - 1));
        script.back().delimiter = MORE;
        script.push_back({"", {std::string()}, 0});
        shapes.push_back({"empty last message", script, Whole(std::vector<std::string>(chunks.begin(), chunks.end() - 1))});
    }
    {   // two frames in one message, and a chunk cut inside a row (the join fallback)
        const std::string all = Whole(chunks);
        shapes.push_back({"cut inside a row", {{MORE, {all.substr(0, 3)}, 0}, {"", {all.substr(3)}, 0}}, all});
        shapes.push_back({"two frames in a message", {{MORE, {chunks[0], chunks[1]}, 0}, {"", {chunks[2]}, 0}}, chunks[0] + chunks[1] + chunks[2]});
    }
    for (Shape &shape : shapes) {
        RepT expected;
        ASSERT_TRUE(expected.ParseFromString(shape.wire)) << shape.name;
        for (size_t threads : {size_t{1}, size_t{8}, size_t{0}}) {   // 0 = adaptive
            ScriptedServer server({shape.script});
            XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
            client.SetThreads(threads);
            EXPECT_TRUE(ViaTheCall<RepT>(client) == expected.SerializeAsString()) << shape.name << " threads=" << threads;
        }
    }
}

TEST(ClientStreamedMessagesTest, RangeQuery) { ExpectEveryStreamedShapeGivesTheBytesOfASerialParse<XStore_Proto::RangeQuery_REP>(); }
TEST(ClientStreamedMessagesTest, BatchQuery) { ExpectEveryStreamedShapeGivesTheBytesOfASerialParse<XStore_Proto::BatchQuery_REP>(); }
TEST(ClientStreamedMessagesTest, FindByValue) { ExpectEveryStreamedShapeGivesTheBytesOfASerialParse<XStore_Proto::FindByValue_REP>(); }
TEST(ClientStreamedMessagesTest, FindByPattern) { ExpectEveryStreamedShapeGivesTheBytesOfASerialParse<XStore_Proto::FindByPattern_REP>(); }

XStore_Proto::Request ParseRequest(const std::string &bytes) {
    XStore_Proto::Request request;
    EXPECT_TRUE(request.ParseFromString(bytes));
    return request;
}

TEST(ClientStreamedMessagesTest, TheRowCallsAskForStreamingAndOtherCallsDoNot) {
    const auto chunks = ChunksOf<XStore_Proto::RangeQuery_REP>(1, 10, false);
    ScriptedServer server({Streamed(chunks), {{"", {std::string()}, 0}}});
    XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
    (void)client.RangeQuery(0, 9, "ANY");
    client.Ping();
    const auto requests = server.requests();
    ASSERT_EQ(requests.size(), 2u);
    EXPECT_TRUE(ParseRequest(requests[0]).streamok());
    EXPECT_FALSE(ParseRequest(requests[1]).streamok());
}

// A request protobuf cannot serialize (2 GB or more: SerializeToArray fails and leaves the buffer zeroed) must not be sent as that zeroed
// buffer, which the server drops and answers with an empty reply the caller takes for a status. (The limit is lowered here so no 2 GB is built.)
TEST(SerializeRequestTest, ARequestWithinTheLimitSerializesLikeSerializeAsString) {
    XStore_Proto::Request request;
    request.mutable_batchquery()->set_dbname("DB");
    for (uint64_t t = 0; t < 100; ++t) request.mutable_batchquery()->add_timestamp(t);
    const zmq::message_t message = xstore_client::SerializeRequest(request);
    EXPECT_EQ(std::string(static_cast<const char *>(message.data()), message.size()), request.SerializeAsString());
}

TEST(SerializeRequestTest, ARequestOverTheLimitIsRefusedWithAnErrorNotSentZeroed) {
    XStore_Proto::Request request;
    request.mutable_batchquery()->set_dbname("DB");
    for (uint64_t t = 0; t < 100; ++t) request.mutable_batchquery()->add_timestamp(t);
    const size_t bytes = request.ByteSizeLong();
    EXPECT_THROW((void)xstore_client::SerializeRequest(request, bytes - 1), std::length_error);
    EXPECT_NO_THROW((void)xstore_client::SerializeRequest(request, bytes));
}

TEST(ParseFragmentsSeriallyTest, OneFragmentOrSeveralParseToTheSameMessage) {
    const std::vector<std::string> chunks = MakeChunks(3, 30, true);
    XStore_Proto::RangeQuery_REP expected;
    ASSERT_TRUE(expected.ParseFromString(Whole(chunks)));
    const std::string whole = Whole(chunks);
    XStore_Proto::RangeQuery_REP from_one, from_three, from_none;
    xstore_client::ParseFragmentsSerially({{whole.data(), whole.size()}}, from_one);
    xstore_client::ParseFragmentsSerially({{chunks[0].data(), chunks[0].size()}, {chunks[1].data(), chunks[1].size()}, {chunks[2].data(), chunks[2].size()}}, from_three);
    xstore_client::ParseFragmentsSerially({}, from_none);
    EXPECT_TRUE(from_one.SerializeAsString() == expected.SerializeAsString());
    EXPECT_TRUE(from_three.SerializeAsString() == expected.SerializeAsString());
    EXPECT_EQ(from_none.queryresponse_size(), 0);
}

// Protobuf cannot parse a message of 2 GB or more as one: past that the serial parse must say so, not hand the size to ParseFromArray as a
// wrapped int. (The bytes are never read: the sizes are checked first.)
TEST(ParseFragmentsSeriallyTest, ARepliesOfTwoGBOrMoreIsRefusedRatherThanParsedWrongly) {
    XStore_Proto::RangeQuery_REP out;
    const void *nowhere = nullptr;
    EXPECT_THROW(xstore_client::ParseFragmentsSerially({{nowhere, size_t{3} << 30}}, out), std::length_error);
    EXPECT_THROW(xstore_client::ParseFragmentsSerially({{nowhere, size_t{1} << 30}, {nowhere, size_t{1} << 30}}, out), std::length_error);
    EXPECT_EQ(out.queryresponse_size(), 0);
}

TEST(ClientStreamedMessagesTest, ATimeoutInTheMiddleOfAReplyThrowsAndTheNextCallGetsItsOwnReply) {
    const auto first = ChunksOf<XStore_Proto::RangeQuery_REP>(3, K_ROWS, false);
    const auto second = ChunksOf<XStore_Proto::RangeQuery_REP>(2, 500, true);
    XStore_Proto::RangeQuery_REP expected;
    ASSERT_TRUE(expected.ParseFromString(Whole(second)));
    // Call 1 gets its first chunk, then the server stalls past the client's patience and finishes the reply anyway (late). Call 2 must see
    // none of those late messages.
    std::vector<Step> script1 = {{MORE, {first[0]}, 0}, {MORE, {first[1]}, 900}, {"", {first[2]}, 0}};
    ScriptedServer server({script1, Streamed(second)});
    XStoreClient client("127.0.0.1", server.port(), 300, /*warmup_on_init=*/false);
    EXPECT_THROW((void)client.RangeQuery(0, K_ROWS, "ANY"), std::runtime_error);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));   // the late messages are sent (to a connection that is gone)
    const auto reply = client.RangeQuery(0, 500, "ANY");
    EXPECT_TRUE(reply->SerializeAsString() == expected.SerializeAsString());
}

// ---- the memory budget ----

TEST(MemoryBudgetTest, MemAvailableAndCgroupLimitsAreRead) {
    EXPECT_EQ(xstore_client::ParseMemAvailableBytes("MemTotal:       65000000 kB\nMemFree:  1 kB\nMemAvailable:   61000000 kB\nBuffers: 3 kB\n"),
              std::optional<size_t>(61000000ull * 1024));
    EXPECT_EQ(xstore_client::ParseMemAvailableBytes("MemTotal: 1 kB\n"), std::nullopt);
    EXPECT_EQ(xstore_client::ParseCgroupBytes("1073741824\n"), std::optional<size_t>(1073741824ull));
    EXPECT_EQ(xstore_client::ParseCgroupBytes("max\n"), std::nullopt) << "no cgroup limit";
    EXPECT_EQ(xstore_client::ParseCgroupBytes(""), std::nullopt);
}

TEST(MemoryBudgetTest, TheBudgetIsTheConfiguredBytesOrAFractionOfWhatIsAvailable) {
    EXPECT_EQ(xstore_client::MemoryBudgetBytes(/*configured=*/1000, /*available=*/std::optional<size_t>(1'000'000), 0.95), std::optional<size_t>(1000));
    EXPECT_EQ(xstore_client::MemoryBudgetBytes(0, std::optional<size_t>(1'000'000), 0.95), std::optional<size_t>(950'000));
    EXPECT_EQ(xstore_client::MemoryBudgetBytes(0, std::nullopt, 0.95), std::nullopt) << "unknown memory: no limit";
    EXPECT_EQ(xstore_client::MemoryBudgetBytes(xstore_client::NO_MEMORY_BUDGET, std::optional<size_t>(1'000'000), 0.95), std::nullopt) << "explicitly unlimited";
}

// A reply that has used its budget is refused while it is still arriving (the memory in use is what is counted, not a guess at the final size).
TEST(ClientMemoryBudgetTest, AReplyThatUsesUpItsBudgetIsRefusedAndTheNextCallStillWorks) {
    const auto big = ChunksOf<XStore_Proto::RangeQuery_REP>(8, 6000, false);   // ~6 MB in 8 chunks
    const auto small = ChunksOf<XStore_Proto::RangeQuery_REP>(3, 90, false);
    ScriptedServer server({Streamed(big), Streamed(small), Streamed(big)});
    XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
    client.SetMemoryBudget(3 << 20);
    EXPECT_THROW((void)client.RangeQuery(0, 6000, "ANY"), xstore_client::MemoryBudgetExceeded);
    EXPECT_EQ(client.RangeQuery(0, 90, "ANY")->queryresponse_size(), 90) << "a fresh socket: nothing of the refused reply is left to confuse it";
    client.SetMemoryBudget(xstore_client::NO_MEMORY_BUDGET);
    EXPECT_EQ(client.RangeQuery(0, 6000, "ANY")->queryresponse_size(), 6000);
}

TEST(ClientStreamedMessagesTest, AReconnectedClientStreamsAgain) {
    const auto chunks = ChunksOf<XStore_Proto::RangeQuery_REP>(3, 900, false);
    ScriptedServer server({Streamed(chunks), Streamed(chunks)});
    XStoreClient client("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
    EXPECT_EQ(client.RangeQuery(0, 900, "ANY")->queryresponse_size(), 900);
    client.Reconnect("127.0.0.1", server.port(), 10000, /*warmup_on_init=*/false);
    EXPECT_EQ(client.RangeQuery(0, 900, "ANY")->queryresponse_size(), 900);
}

}  // namespace
