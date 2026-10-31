#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY
#endif
#include "fmt/format.h"

#include <google/protobuf/arena.h>
#include <zmq.hpp>
#include <zmq_addon.hpp>
#include "XStore.pb.h"

// ======================================================================================================================================
// Part 1 of 3: a load-aware job runner (namespace xstore_client). Knows nothing about XStore, ZMQ or protobuf: lift this part as is for
// any batch of independent jobs.
//
//   RunJobs(n, policy, fn)   runs fn(0) .. fn(n-1) and returns when all are done. The calling thread always works. With a fixed policy
//                            the team is exactly that big. Otherwise it is adaptive: the team starts at the caller alone, a short-lived
//                            controller thread looks at how many threads are runnable on the whole machine every `tick`, and
//                            - while there is room it doubles the team (slow start), up to the cap;
//                            - when the machine is over-subscribed the helpers above the surplus leave, each after its current job.
//                            Independent processes never talk to each other: the machine's run queue is what they share, so a second
//                            client that starts parsing makes the first one's helpers retreat within one job, and both settle.
//
// Jobs should be short (milliseconds to tens of milliseconds): that is the latency of the team reacting to the load.
// ======================================================================================================================================
namespace xstore_client {

[[nodiscard]] inline size_t CoreCount() { return std::max<size_t>(1, std::thread::hardware_concurrency()); }

// "0.20 0.18 0.15 3/1234 5678": the fourth field of /proc/loadavg is runnable/total scheduling entities right now.
[[nodiscard]] inline std::optional<size_t> ParseRunnableFromLoadAvg(std::string_view text) {
    size_t pos = 0;
    for (int field = 0; field < 3; ++field) {
        pos = text.find(' ', pos);
        if (pos == std::string_view::npos) return std::nullopt;
        ++pos;
    }
    size_t runnable = 0;
    size_t digits = 0;
    for (; pos < text.size() && text[pos] >= '0' && text[pos] <= '9'; ++pos, ++digits) runnable = runnable * 10 + static_cast<size_t>(text[pos] - '0');
    if (digits == 0 || pos >= text.size() || text[pos] != '/') return std::nullopt;
    return runnable;
}

// Threads runnable on this machine right now, this one included; nullopt where /proc/loadavg does not exist.
[[nodiscard]] inline std::optional<size_t> ReadRunnableThreads() {
    std::ifstream loadavg("/proc/loadavg");
    char buffer[128];
    loadavg.read(buffer, sizeof buffer);
    const std::streamsize bytes = loadavg.gcount();
    return bytes > 0 ? ParseRunnableFromLoadAvg(std::string_view(buffer, static_cast<size_t>(bytes))) : std::nullopt;
}

struct ParallelismPolicy {
    size_t fixed_threads = 0;                 // > 0: exactly this many threads (at most one per job), the load is ignored
    size_t max_threads = CoreCount();         // adaptive: the team never grows past this
    size_t cores = CoreCount();               // what the runnable threads are compared with
    size_t headroom = 1;                      // cores left free for everyone else
    std::chrono::milliseconds tick{10};       // how often the controller looks at the load
    std::function<std::optional<size_t>()> runnable = ReadRunnableThreads;
};

// The team size the controller wants next. `runnable` counts every runnable thread of the machine, this team's included.
// Room (cores - runnable - headroom > 0) doubles the team, but never beyond that room, the cap, or the jobs left; more runnable
// threads than cores sends the surplus home; in between the team holds, so it does not flap. Never below the calling thread.
[[nodiscard]] inline size_t NextWorkerTarget(size_t target, size_t runnable, size_t cores, size_t headroom, size_t cap, size_t jobs_left) {
    if (runnable > cores) {
        const size_t surplus = runnable - cores;
        return target > surplus ? target - surplus : 1;
    }
    const size_t room = cores > runnable + headroom ? cores - runnable - headroom : 0;
    if (room == 0 || target >= jobs_left || target >= cap) return target;
    return std::min({target + std::min(room, target), cap, jobs_left});
}

namespace detail {

template <class Fn>
class Team {
 public:
    Team(size_t jobs, size_t cap, const ParallelismPolicy &policy, Fn &fn)
        : jobs_(jobs), cap_(cap), policy_(policy), fn_(fn), helpers_(cap), alive_(cap) {}

    void Run() {
        const bool adaptive = policy_.fixed_threads == 0;
        target_ = adaptive ? 1 : cap_;
        std::thread controller;
        if (adaptive) {
            try {
                controller = std::thread([this] { Control(); });
            } catch (const std::system_error &) {   // no controller: the caller alone
            }
        } else {
            Grow();
        }
        Work(0);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finished_ = true;
        }
        wake_.notify_all();
        if (controller.joinable()) controller.join();
        for (std::thread &helper : helpers_) {
            if (helper.joinable()) helper.join();
        }
        if (error_) std::rethrow_exception(error_);
    }

 private:
    // Slot 0 is the calling thread and never leaves. A helper works only while its slot is below the target.
    void Work(size_t slot) {
        try {
            while (!stop_.load(std::memory_order_relaxed) && (slot == 0 || slot < target_.load(std::memory_order_relaxed))) {
                const size_t job = next_.fetch_add(1, std::memory_order_relaxed);
                if (job >= jobs_) return;
                fn_(job);
            }
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!error_) error_ = std::current_exception();
            stop_.store(true, std::memory_order_relaxed);
        }
    }

    // Starts (or restarts, after a retreat) the helper of every slot below the target. Only one thread calls this at a time.
    void Grow() {
        for (size_t slot = 1; slot < target_.load(); ++slot) {
            if (alive_[slot].load()) continue;
            if (helpers_[slot].joinable()) helpers_[slot].join();   // exited earlier
            alive_[slot].store(true);
            try {
                helpers_[slot] = std::thread([this, slot] {
                    Work(slot);
                    alive_[slot].store(false);
                });
            } catch (const std::system_error &) {   // cannot start more threads: keep what runs
                alive_[slot].store(false);
                target_.store(slot);
                return;
            }
        }
    }

    void Tick() {
        const std::optional<size_t> runnable = policy_.runnable();
        if (!runnable) return;
        const size_t target = target_.load();
        const size_t jobs_left = jobs_ - std::min(jobs_, next_.load(std::memory_order_relaxed));
        const size_t wanted = NextWorkerTarget(target, *runnable, policy_.cores, policy_.headroom, cap_, jobs_left);
        if (wanted == target) return;
        target_.store(wanted);
        if (wanted > target) Grow();
    }

    void Control() {
        std::unique_lock<std::mutex> lock(mutex_);
        do {
            lock.unlock();
            Tick();
            lock.lock();
        } while (!wake_.wait_for(lock, policy_.tick, [this] { return finished_; }));
    }

    const size_t jobs_;
    const size_t cap_;
    const ParallelismPolicy &policy_;
    Fn &fn_;
    std::vector<std::thread> helpers_;          // by slot; slot 0 is unused
    std::vector<std::atomic<bool>> alive_;
    std::atomic<size_t> next_{0};
    std::atomic<size_t> target_{1};
    std::atomic<bool> stop_{false};
    std::mutex mutex_;
    std::condition_variable wake_;
    bool finished_ = false;
    std::exception_ptr error_;
};

}  // namespace detail

// Runs fn(0) .. fn(jobs - 1) on a team the policy sizes, and returns when all are done. fn must be safe to call concurrently for
// different jobs. The first exception fn throws stops the team taking jobs and is rethrown here once every thread has stopped.
template <class Fn>
void RunJobs(size_t jobs, ParallelismPolicy policy, Fn &&fn) {
    if (jobs == 0) return;
    // Where the load cannot be read (not Linux) an adaptive team has nothing to adapt to: a modest fixed one.
    if (policy.fixed_threads == 0 && !(policy.runnable && policy.runnable())) policy.fixed_threads = std::max<size_t>(1, policy.cores / 2);
    const size_t cap = std::max<size_t>(1, std::min(jobs, policy.fixed_threads != 0 ? policy.fixed_threads : policy.max_threads));
    if (cap == 1) {
        for (size_t job = 0; job < jobs; ++job) fn(job);
        return;
    }
    detail::Team<std::remove_reference_t<Fn>> team(jobs, cap, policy, fn);
    team.Run();
}

// ======================================================================================================================================
// Part 2 of 3: parallel parse of a query reply (RangeQuery, BatchQuery, FindByValue, FindByPattern); uses part 1.
//
// The server cuts a reply into frames only between whole rows, and a reply is just its rows (field 1) plus an optional status (field 3),
// so any run of whole top-level fields is a reply of its own: the reply is cut into pieces of a few MB at row boundaries, the pieces are
// parsed by a team that sizes itself to the machine's load (part 1), and the rows are moved into the result in order. A reply that cannot
// be split or parsed this way makes ParseFragmentsParallel return false with the result untouched; the caller parses it serially.
// ======================================================================================================================================

// Below this many reply bytes the serial parse is quicker than starting a team.
inline constexpr size_t MIN_PARALLEL_PARSE_BYTES = 4ull * 1024 * 1024;
inline constexpr size_t PARSE_PIECE_BYTES = 4ull * 1024 * 1024;

[[nodiscard]] inline size_t PositiveFromEnv(const char *name) {
    const char *env = std::getenv(name);
    if (env == nullptr) return 0;
    char *end = nullptr;
    const unsigned long long n = std::strtoull(env, &end, 10);
    return (end != env && *end == '\0' && n > 0) ? static_cast<size_t>(n) : 0;
}

// The policy for one parse. fixed_threads > 0 forces that team size; 0 reads XSTORE_CLIENT_PARSE_THREADS (a number forces it,
// anything else is adaptive). XSTORE_CLIENT_PARSE_MAX caps an adaptive team (default the machine's hardware threads).
[[nodiscard]] inline ParallelismPolicy ParsePolicy(size_t fixed_threads) {
    ParallelismPolicy policy;
    policy.fixed_threads = fixed_threads != 0 ? fixed_threads : PositiveFromEnv("XSTORE_CLIENT_PARSE_THREADS");
    if (const size_t max_threads = PositiveFromEnv("XSTORE_CLIENT_PARSE_MAX"); max_threads != 0) policy.max_threads = max_threads;
    return policy;
}

struct Fragment {
    const void *data;
    size_t size;
};

namespace detail {

[[nodiscard]] inline bool ReadVarint(const uint8_t *&p, const uint8_t *end, uint64_t &value) {
    value = 0;
    for (int shift = 0; shift < 64 && p < end; shift += 7) {
        const uint8_t byte = *p++;
        value |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return true;
    }
    return false;
}

// The end of the top-level field that starts at p, or nullptr if it is malformed (or a group, which a reply never has).
[[nodiscard]] inline const uint8_t *FieldEnd(const uint8_t *p, const uint8_t *end) {
    uint64_t tag = 0;
    if (!ReadVarint(p, end, tag)) return nullptr;
    uint64_t length = 0;
    switch (tag & 7) {
        case 0: return ReadVarint(p, end, length) ? p : nullptr;   // varint
        case 1: length = 8; break;                                  // fixed64
        case 2: if (!ReadVarint(p, end, length)) return nullptr; break;   // length-delimited
        case 5: length = 4; break;                                  // fixed32
        default: return nullptr;
    }
    return length <= static_cast<uint64_t>(end - p) ? p + length : nullptr;
}

}  // namespace detail

namespace detail {

// Appends the pieces of one fragment. False if the fragment is not well framed.
[[nodiscard]] inline bool SplitFragment(const Fragment &fragment, size_t piece_bytes, std::vector<Fragment> &pieces) {
    const auto *begin = static_cast<const uint8_t *>(fragment.data);
    const uint8_t *end = begin + fragment.size;
    const uint8_t *piece = begin;
    for (const uint8_t *p = begin; p < end;) {
        p = FieldEnd(p, end);
        if (p == nullptr) return false;
        if (static_cast<size_t>(p - piece) >= piece_bytes) {
            pieces.push_back({piece, static_cast<size_t>(p - piece)});
            piece = p;
        }
    }
    if (piece < end) pieces.push_back({piece, static_cast<size_t>(end - piece)});
    return true;
}

}  // namespace detail

// Cuts every fragment into pieces of about piece_bytes (never shorter than one field) between top-level fields; a piece never spans
// fragments. False if a fragment is not well framed.
[[nodiscard]] inline bool SplitIntoPieces(const std::vector<Fragment> &fragments, size_t piece_bytes, std::vector<Fragment> &pieces) {
    pieces.clear();
    for (const Fragment &fragment : fragments) {
        if (!detail::SplitFragment(fragment, piece_bytes, pieces)) return false;
    }
    return true;
}

// The same pieces in the same order, with the fragments split on the policy's team: walking the row headers of a big reply touches every
// row's cache line, and a fragment start is always a row boundary, so the fragments are independent jobs.
[[nodiscard]] inline bool SplitIntoPiecesParallel(const std::vector<Fragment> &fragments, size_t piece_bytes, const ParallelismPolicy &policy,
                                                  std::vector<Fragment> &pieces) {
    if (fragments.size() < 2) return SplitIntoPieces(fragments, piece_bytes, pieces);
    std::vector<std::vector<Fragment>> by_fragment(fragments.size());
    std::atomic<bool> well_framed{true};
    RunJobs(fragments.size(), policy, [&](size_t k) {
        if (!detail::SplitFragment(fragments[k], piece_bytes, by_fragment[k])) well_framed.store(false, std::memory_order_relaxed);
    });
    if (!well_framed.load()) return false;
    pieces.clear();
    for (const std::vector<Fragment> &one : by_fragment) pieces.insert(pieces.end(), one.begin(), one.end());
    return true;
}

// True for the reply messages made of repeated queryResponse rows plus an optional queryStatus.
template <class RepT, class = void>
struct IsFragmentMergeable : std::false_type {};
template <class RepT>
struct IsFragmentMergeable<RepT, std::void_t<decltype(std::declval<RepT &>().mutable_queryresponse()),
                                              decltype(std::declval<const RepT &>().has_querystatus())>> : std::true_type {};

// Parses the fragments piece by piece on a team the policy sizes and appends the rows to `out` in order (moving the row objects, not
// copying them), taking the last queryStatus. Returns false, with `out` untouched, when the reply is not worth splitting (fewer than two
// pieces), not well framed, or a piece does not parse.
// `arena` is the arena `out` was created on (nullptr when `out` lives on the heap): the pieces are created on it too, so the rows move
// between messages of ONE arena. A row moved between two different owners would be freed twice or read after its owner is gone.
template <class RepT>
[[nodiscard]] bool ParseFragmentsParallel(const std::vector<Fragment> &fragments, RepT &out, const ParallelismPolicy &policy,
                                          size_t piece_bytes = PARSE_PIECE_BYTES, google::protobuf::Arena *arena = nullptr) {
    static_assert(IsFragmentMergeable<RepT>::value, "RepT needs repeated queryResponse and optional queryStatus");
    std::vector<Fragment> pieces;
    std::vector<std::unique_ptr<RepT>> heap_parts;   // used when there is no arena; an arena owns its pieces
    std::vector<RepT *> parts;
    std::atomic<bool> failed{false};
    try {
        if (!SplitIntoPiecesParallel(fragments, piece_bytes, policy, pieces) || pieces.size() < 2) return false;
        for (const Fragment &piece : pieces) {
            if (piece.size > static_cast<size_t>(INT_MAX)) return false;
        }
        parts.reserve(pieces.size());
        for (size_t k = 0; k < pieces.size(); ++k) {
            if (arena != nullptr) {
                parts.push_back(google::protobuf::Arena::Create<RepT>(arena));
            } else {
                heap_parts.push_back(std::make_unique<RepT>());
                parts.push_back(heap_parts.back().get());
            }
        }
        RunJobs(parts.size(), policy, [&](size_t k) {
            if (failed.load(std::memory_order_relaxed)) return;
            if (!parts[k]->ParseFromArray(pieces[k].data, static_cast<int>(pieces[k].size))) failed.store(true, std::memory_order_relaxed);
        });
    } catch (...) {   // out of memory in a worker: let the caller try the serial parse
        return false;
    }
    if (failed.load()) return false;

    using Row = typename std::remove_reference_t<decltype(*out.mutable_queryresponse())>::value_type;
    int total_rows = out.queryresponse_size();
    for (const RepT *part : parts) total_rows += part->queryresponse_size();
    out.mutable_queryresponse()->Reserve(total_rows);
    std::vector<Row *> moved;
    for (RepT *part : parts) {
        const int rows = part->queryresponse_size();
        moved.resize(static_cast<size_t>(rows));
        part->mutable_queryresponse()->UnsafeArenaExtractSubrange(0, rows, moved.data());   // same arena on both sides: no copy
        for (Row *row : moved) out.mutable_queryresponse()->UnsafeArenaAddAllocated(row);
        if (part->has_querystatus()) out.set_querystatus(*part->mutable_querystatus());
    }
    return true;
}

// A reply of rows together with the arena it lives on. Freeing a parsed reply message by message was the biggest cost left in a 1M-row
// RangeQuery (4-7 s of 10-14 s: 128 million small objects, one free each); an arena hands its memory back in a few large blocks (about
// 0.5 s) and also makes the parse itself cheaper. Move-only; use it like a pointer to the reply (`reply->queryresponse()`, `*reply`).
// Everything read from it (rows, strings) stays valid exactly as long as the ArenaReply that produced it, and no longer.
template <class RepT>
class ArenaReply {
 public:
    ArenaReply() : arena_(std::make_unique<google::protobuf::Arena>()), rep_(google::protobuf::Arena::Create<RepT>(arena_.get())) {}
    ArenaReply(std::unique_ptr<google::protobuf::Arena> arena, RepT *rep) : arena_(std::move(arena)), rep_(rep) {}
    ArenaReply(ArenaReply &&other) noexcept : arena_(std::move(other.arena_)), rep_(std::exchange(other.rep_, nullptr)) {}
    ArenaReply &operator=(ArenaReply &&other) noexcept {
        if (this != &other) {
            arena_ = std::move(other.arena_);   // frees the reply this one held
            rep_ = std::exchange(other.rep_, nullptr);
        }
        return *this;
    }
    ArenaReply(const ArenaReply &) = delete;
    ArenaReply &operator=(const ArenaReply &) = delete;

    RepT *get() const noexcept { return rep_; }
    RepT *operator->() const noexcept { return rep_; }
    RepT &operator*() const noexcept { return *rep_; }

 private:
    std::unique_ptr<google::protobuf::Arena> arena_;
    RepT *rep_ = nullptr;
};

}  // namespace xstore_client

// ======================================================================================================================================
// Part 3 of 3: the XStore client.
// ======================================================================================================================================

// Client-side CURVE identity: this client's own keypair plus the server's
// public key (which the client must already know out-of-band -- see the
// design doc). Deliberately duplicates the tiny amount of Z85-file-reading
// logic XStore_Curve.h has server-side rather than including that header,
// so this file keeps compiling standalone if client_src/Cpp is copied out
// on its own without the rest of the repo.
struct XStoreCurveClientAuth {
  std::string client_public_key_z85;
  std::string client_secret_key_z85;
  std::string server_public_key_z85;

  // client_secret_key_file's sibling ".pub" (same directory/stem) supplies
  // the client's own public key -- the same xstore-keys gen convention
  // used server-side.
  static XStoreCurveClientAuth LoadFromFiles(std::string_view client_secret_key_file,
                                              std::string_view server_public_key_file) {
      auto read_z85 = [](const std::filesystem::path &p) {
          std::ifstream f(p);
          if (!f.is_open()) {
              throw std::runtime_error("XStoreCurveClientAuth: cannot open " + p.string());
          }
          std::string key;
          f >> key;
          if (key.size() != 40) {
              throw std::runtime_error("XStoreCurveClientAuth: " + p.string() +
                                        " does not contain a 40-character Z85 key");
          }
          return key;
      };
      std::filesystem::path secret_path(client_secret_key_file);
      std::filesystem::path public_path = secret_path;
      public_path.replace_extension(".pub");

      XStoreCurveClientAuth auth;
      auth.client_secret_key_z85 = read_z85(secret_path);
      auth.client_public_key_z85 = read_z85(public_path);
      auth.server_public_key_z85 = read_z85(std::filesystem::path(server_public_key_file));
      return auth;
  }
};

struct FileInfo {
  std::string file_name;
  // actual = real bytes on disk (st_blocks*512, sparse-hole-aware);
  // apparent = logical/reported size (st_size). apparent can exceed actual
  // for a preallocated file that still has unwritten holes.
  uint64_t actual_size_bytes;
  uint64_t apparent_size_bytes;
};

struct DbInfo {
  std::string db_name;
  XStore_Proto::TimeUnit db_resolution;
  // Matches XStore_Proto::DbStats.totalSizeInByte (uint64) -- sum of every
  // file's actual_size_bytes, i.e. real disk usage of this DB. A float here
  // silently loses precision past 2^24 bytes (16MB) -- easy to hit for any
  // real time-series file.
  uint64_t total_size_in_byte;
  std::vector<FileInfo> files;
  // Matches XStore_Proto::DbStats.rowLength -- the configured per-row slot
  // size (bytes) every insert's serialized RowData is zero-padded to.
  // Compare against a real row's actual serialized size to tell whether
  // this was sized too large (wasted padding/row) or too small.
  int32_t row_length;
  // Matches XStore_Proto::DbStats.columnNames -- optional per-index column
  // names set at CreateDB time (see ResolveColumnIndex). Empty if the DB
  // was created without names.
  std::vector<std::string> column_names;
};

struct SysInfo {
  std::string server_addr;
  int num_threads;
  std::vector<DbInfo> db_info_list;
};

// Factory helpers for XStore_Proto::FData's oneof. proto3 picks a different
// wire encoding for sint32/fixed32/sfixed32 even though they all take a
// plain int32_t in C++, so overloading on parameter type alone can't tell
// them apart -- name the intent instead. Purely additive: existing callers
// that build FData by hand (field->set_stringdata(...)) still work.
namespace xstore_fdata {
  inline XStore_Proto::FData Double(double v)        { XStore_Proto::FData f; f.set_doubledata(v);   return f; }
  inline XStore_Proto::FData Float(float v)           { XStore_Proto::FData f; f.set_floatdata(v);    return f; }
  inline XStore_Proto::FData Int32(int32_t v)         { XStore_Proto::FData f; f.set_int32data(v);    return f; }
  inline XStore_Proto::FData Int64(int64_t v)         { XStore_Proto::FData f; f.set_int64data(v);    return f; }
  inline XStore_Proto::FData UInt32(uint32_t v)       { XStore_Proto::FData f; f.set_uint32data(v);   return f; }
  inline XStore_Proto::FData UInt64(uint64_t v)       { XStore_Proto::FData f; f.set_uint64data(v);   return f; }
  inline XStore_Proto::FData SInt32(int32_t v)        { XStore_Proto::FData f; f.set_sint32data(v);   return f; }
  inline XStore_Proto::FData SInt64(int64_t v)        { XStore_Proto::FData f; f.set_sint64data(v);   return f; }
  inline XStore_Proto::FData Fixed32(uint32_t v)      { XStore_Proto::FData f; f.set_fixed32data(v);  return f; }
  inline XStore_Proto::FData Fixed64(uint64_t v)      { XStore_Proto::FData f; f.set_fixed64data(v);  return f; }
  inline XStore_Proto::FData SFixed32(int32_t v)      { XStore_Proto::FData f; f.set_sfixed32data(v); return f; }
  inline XStore_Proto::FData SFixed64(int64_t v)      { XStore_Proto::FData f; f.set_sfixed64data(v); return f; }
  inline XStore_Proto::FData Bool(bool v)             { XStore_Proto::FData f; f.set_booldata(v);     return f; }
  inline XStore_Proto::FData String(std::string v)    { XStore_Proto::FData f; f.set_stringdata(std::move(v)); return f; }
  inline XStore_Proto::FData Bytes(std::string v)     { XStore_Proto::FData f; f.set_bytesdata(std::move(v));  return f; }
}

class XStoreClient {
 private:
  zmq::context_t ctx;
  zmq::socket_t sock { ctx, zmq::socket_type::req };

  // Reused across calls instead of a fresh std::vector<zmq::message_t>
  // constructed on every SendRecv -- .clear() keeps the already-grown
  // backing storage. Safe as a plain member (not thread_local): a single
  // XStore_Client instance's REQ socket is strict send-then-recv lockstep
  // already, so it was never safe to call concurrently from multiple
  // threads to begin with.
  std::vector<zmq::message_t> recv_buf;

  // This client's own send/recv timeout (ctor/Reconnect's timeout_ms); -1 = wait forever. Filled into every
  // Request's timeoutMs field (clamped to uint32, 0 = not sent) so the server can abandon a request this socket
  // would already have given up waiting for, instead of a worker blocking on it (or the server's memory pool)
  // forever. A plain member, not thread_local: this client's REQ socket is already single-threaded use only.
  int timeout_ms_ = -1;

  // Threads used to parse a big reply of rows; 0 = adaptive. See SetParseThreads.
  size_t parse_threads_ = 0;

  // Every RPC is: serialize request, blocking send, blocking recv, parse
  // reply. Sharing that here means the 17 request types can't drift out of
  // sync with each other the way copy-pasted send/recv blocks do. ToServer
  // is always the Request envelope -- the oneof case tells the server which
  // payload it's holding, so it only has to parse the wire bytes once.
  // Sends the request and leaves the reply's frames in recv_buf.
  void Exchange(XStore_Proto::Request &to_server) {
      if (timeout_ms_ >= 0) to_server.set_timeoutms(static_cast<uint32_t>(timeout_ms_));
      size_t byte_size = to_server.ByteSizeLong();
      zmq::message_t z_out(byte_size);
      to_server.SerializeToArray(z_out.data(), byte_size);
      if (!sock.send(z_out, zmq::send_flags::none)) {
          throw std::runtime_error("XStore_Client: send failed/timed out");
      }

      recv_buf.clear();
      if (!zmq::recv_multipart(sock, std::back_inserter(recv_buf)) || recv_buf.empty()) {
          throw std::runtime_error("XStore_Client: recv failed/timed out");
      }
  }

  size_t ReplyBytes() const {
      size_t total = 0;
      for (const auto &part : recv_buf) total += part.size();
      return total;
  }

  // A big reply of rows (RangeQuery, BatchQuery, FindByValue, FindByPattern) is cut at row boundaries and parsed on a team of threads
  // that sizes itself to the machine's load (part 2 above) -- the single ParseFromArray was most of a big RangeQuery's time.
  // Returns false (`into` untouched) for every other reply and for any reply that cannot be handled that way.
  template <typename RepT>
  bool TryParseInParallel(RepT &into, google::protobuf::Arena *arena) {
      if constexpr (xstore_client::IsFragmentMergeable<RepT>::value) {
          if (ReplyBytes() < xstore_client::MIN_PARALLEL_PARSE_BYTES) return false;
          std::vector<xstore_client::Fragment> fragments;
          fragments.reserve(recv_buf.size());
          for (const auto &part : recv_buf) fragments.push_back({part.data(), part.size()});
          return xstore_client::ParseFragmentsParallel(fragments, into, xstore_client::ParsePolicy(parse_threads_),
                                                        xstore_client::PARSE_PIECE_BYTES, arena);
      } else {
          (void)into;
          (void)arena;
          return false;
      }
  }

  // A streamed reply (currently RangeQuery and the like) arrives as several frames that are pieces of ONE logical message's bytes
  // (protobuf doesn't care where a message's serialized bytes get split, only that they're concatenated back in order before
  // parsing) -- reassembled here so this stays invisible to every caller above SendRecv.
  template <typename RepT>
  void ParseSerially(RepT &into) {
      if (recv_buf.size() == 1) {
          into.ParseFromArray(recv_buf.front().data(), recv_buf.front().size());
      } else {
          std::string joined;
          joined.reserve(ReplyBytes());
          for (const auto &part : recv_buf) joined.append(static_cast<const char *>(part.data()), part.size());
          into.ParseFromArray(joined.data(), joined.size());
      }
  }

  // Every RPC is: serialize request, blocking send, blocking recv, parse
  // reply. Sharing that here means the 17 request types can't drift out of
  // sync with each other the way copy-pasted send/recv blocks do. ToServer
  // is always the Request envelope -- the oneof case tells the server which
  // payload it's holding, so it only has to parse the wire bytes once.
  template <typename RepT>
  RepT SendRecv(XStore_Proto::Request &to_server) {
      Exchange(to_server);
      RepT from_server;
      if (!TryParseInParallel(from_server, nullptr)) ParseSerially(from_server);
      return from_server;
  }

  // The same for the calls that return rows: the reply is parsed onto one arena that the returned ArenaReply owns (see there for why).
  // A big reply gets 1-8 MiB blocks (a few allocations for the whole reply); a small one keeps the default small blocks.
  template <typename RepT>
  xstore_client::ArenaReply<RepT> SendRecvOnArena(XStore_Proto::Request &to_server) {
      Exchange(to_server);
      google::protobuf::ArenaOptions options;
      if (ReplyBytes() >= xstore_client::MIN_PARALLEL_PARSE_BYTES) {
          options.start_block_size = size_t{1} << 20;
          options.max_block_size = size_t{8} << 20;
      }
      auto arena = std::make_unique<google::protobuf::Arena>(options);
      RepT *from_server = google::protobuf::Arena::Create<RepT>(arena.get());
      if (!TryParseInParallel(*from_server, arena.get())) ParseSerially(*from_server);
      return xstore_client::ArenaReply<RepT>(std::move(arena), from_server);
  }

 public:
  // Threads used to parse a big reply of rows -- RangeQuery, BatchQuery, FindByValue, FindByPattern. 0 (the default) is adaptive: the
  // parse starts on the calling thread and adds helpers while the machine has idle cores, and they leave when other work needs the cores
  // (cap: XSTORE_CLIENT_PARSE_MAX, default the machine's hardware threads). N > 0 forces N threads (XSTORE_CLIENT_PARSE_THREADS=N does the
  // same for every client of a process); 1 parses on the calling thread only.
  void SetParseThreads(size_t threads) { parse_threads_ = threads; }

  // timeoutMs < 0 (default) blocks forever, matching the previous behavior.
  // Pass a positive value so an unreachable/dead server raises instead of
  // hanging this REQ socket indefinitely. warmupOnInit lets callers who
  // don't care about paying the handshake cost upfront (e.g. short-lived
  // clients that may never send a request) skip it.
  XStoreClient(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
               bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      timeout_ms_ = timeout_ms;
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      if (curve_auth) {
          sock.set(zmq::sockopt::curve_serverkey, curve_auth->server_public_key_z85);
          sock.set(zmq::sockopt::curve_publickey, curve_auth->client_public_key_z85);
          sock.set(zmq::sockopt::curve_secretkey, curve_auth->client_secret_key_z85);
      }
      std::string full_addr = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(full_addr);
      if (warmup_on_init) {
          Warmup();
      }
  }

  // ZMQ connect() is lazy -- TCP + ZMTP handshake actually fires on first
  // send(). Call this right after construction to eat that cost now instead
  // of on the first real RPC. PING is a bare round trip with no server-side
  // XStore work (unlike INFO, which scans the data store directory), so it
  // forces the handshake without the extra I/O. Throws same as any RPC on
  // timeout/unreachable server -- caller must Reconnect() before retrying,
  // same as elsewhere.
  void Warmup() {
      Ping();
  }

  // A REQ socket that hits a send/recv timeout has its strict
  // send-then-recv state machine left in an undefined spot per ZMQ's own
  // docs -- the fix is a fresh socket, not another send. Call this after
  // catching a timeout exception from any RPC below before retrying.
  void Reconnect(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
                 bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      timeout_ms_ = timeout_ms;
      sock = zmq::socket_t(ctx, zmq::socket_type::req);
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      if (curve_auth) {
          sock.set(zmq::sockopt::curve_serverkey, curve_auth->server_public_key_z85);
          sock.set(zmq::sockopt::curve_publickey, curve_auth->client_public_key_z85);
          sock.set(zmq::sockopt::curve_secretkey, curve_auth->client_secret_key_z85);
      }
      std::string full_addr = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(full_addr);
      if (warmup_on_init) {
          Warmup();
      }
  }

  std::string GetCurrDT() {
      std::stringstream formatted_time;
      std::time_t time_now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      formatted_time << std::put_time(std::localtime(&time_now), "%b %d, %Y %r");
      std::string to_return = formatted_time.str();
      return to_return;
  }

  // ==== PING ====
  // Bare round trip, no server-side XStore work -- cheaper than InfoRequest()
  // for warmup/liveness checks that don't need SysInfo's contents.
  void Ping() {
      XStore_Proto::Request to_server;
      to_server.mutable_ping()->set_rtype(XStore_Proto::PING);
      SendRecv<XStore_Proto::Ping_REP>(to_server);
  }

  // ==== INFO ====
  SysInfo InfoRequest() {
      XStore_Proto::Request to_server;
      to_server.mutable_info()->set_rtype(XStore_Proto::INFO);
      XStore_Proto::SysInfo_REP from_server = SendRecv<XStore_Proto::SysInfo_REP>(to_server);

      SysInfo to_return;
      to_return.server_addr = from_server.serveraddr();
      to_return.num_threads = from_server.threadcount();

      for (int i = 0; i < from_server.dbinfo_size(); i++) {
          DbInfo curr_db;
          curr_db.db_name = from_server.dbinfo(i).dbname();
          curr_db.db_resolution = from_server.dbinfo(i).dbresolution();
          curr_db.total_size_in_byte = from_server.dbinfo(i).totalsizeinbyte();
          curr_db.row_length = from_server.dbinfo(i).rowlength();

          for (int j = 0; j < from_server.dbinfo(i).columnnames_size(); j++) {
              curr_db.column_names.push_back(from_server.dbinfo(i).columnnames(j));
          }

          for (int j = 0; j < from_server.dbinfo(i).files_size(); j++) {
              const XStore_Proto::FileStats &f = from_server.dbinfo(i).files(j);
              curr_db.files.push_back(FileInfo{f.filename(), f.actualsizebytes(), f.apparentsizebytes()});
          }
          to_return.db_info_list.push_back(curr_db);
      }

      return to_return;
  }

  // ==== DB management ====
  [[nodiscard]] std::string CreateDB(std::string_view db_name, XStore_Proto::TimeUnit time_resolution, short int row_length,
                                      const std::vector<std::string> &column_names = {}) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_createdb();
      payload->set_rtype(XStore_Proto::CREATEDB);
      payload->set_dbname(std::string(db_name));
      payload->set_timeresolution(time_resolution);
      payload->set_rowlength(row_length);
      for (const std::string &name : column_names) {
          payload->add_columnnames(name);
      }
      return SendRecv<XStore_Proto::CreateDB_REP>(to_server).createdbstatus();
  }

  // Pure name->index lookup, no network call -- callers fetch column_names
  // once (InfoRequest()'s DbInfo.column_names, or the same list they just
  // passed to CreateDB) and pass it in here before serializing a query/
  // aggregate request with the resolved index. Throws rather than silently
  // guessing on an empty list or an unknown name.
  [[nodiscard]] static int ResolveColumnIndex(const std::vector<std::string> &column_names, std::string_view name) {
      for (size_t i = 0; i < column_names.size(); i++) {
          if (column_names[i] == name) return static_cast<int>(i);
      }
      throw std::invalid_argument(fmt::format("XStoreClient::ResolveColumnIndex: no column named '{}' (DB has {} named column(s))",
                                                name, column_names.size()));
  }

  [[nodiscard]] std::string RemoveDB(std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_removedb();
      payload->set_rtype(XStore_Proto::REMOVEDB);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RemoveDB_REP>(to_server).removestatus();
  }

  // Per-file status, one entry per requested fileName -- can't be flattened
  // to a single string without losing which file(s) failed.
  [[nodiscard]] XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(std::string_view db_name, const std::vector<std::string> &file_names) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_removedbfiles();
      payload->set_rtype(XStore_Proto::REMOVEDBFILES);
      payload->set_dbname(std::string(db_name));
      for (const std::string &name : file_names) {
          payload->add_filename(name);
      }
      return SendRecv<XStore_Proto::RemoveDBFiles_REP>(to_server);
  }

  // ==== Row sizing ====
  // Suggests a CreateDB rowLength/slot size from one sample RowData, so
  // callers don't have to hand-guess a value (and over/under-provision --
  // see DbInfo::row_length above). Mirrors XBench's
  // XStore_Adapter::ComputeRowLength, ported here so client_src callers get
  // it natively instead of reimplementing it.
  //
  // PROTOBUF_FIELD_OVERHEAD=3 is exact for every fixed-width/varint oneof
  // case (double/int32/int64/etc.): 1B outer repeated-field tag (RowData's
  // fieldData=1) + 1B outer length-varint (assumes the encoded FData stays
  // under 128B, true for any scalar or short string) + 1B inner oneof tag.
  // It undercounts stringData/bytesData by 1B: those are themselves
  // length-delimited, so they need a second inner length-varint the
  // fixed/varint cases don't. That 1B/string-field gap already existed in
  // the XBench original this mirrors -- flagged here, not silently fixed,
  // since this helper is meant to match, not diverge from, the logic it's
  // porting.
  template <uint32_t Alignment = 8>
  [[nodiscard]] static uint32_t SuggestRowLength(const XStore_Proto::RowData &sample_row) {
      static_assert((Alignment & (Alignment - 1)) == 0, "Alignment must be power of 2");
      constexpr uint32_t PROTOBUF_FIELD_OVERHEAD = 3;

      uint32_t row_size = 0;
      for (int i = 0; i < sample_row.fielddata_size(); i++) {
          const XStore_Proto::FData &field = sample_row.fielddata(i);
          switch (field.oneOfData_case()) {
              case XStore_Proto::FData::kStringData:
                  row_size += static_cast<uint32_t>(field.stringdata().size()) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kBytesData:
                  row_size += static_cast<uint32_t>(field.bytesdata().size()) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kDoubleData:
                  row_size += sizeof(double) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kFloatData:
                  row_size += sizeof(float) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kBoolData:
                  row_size += sizeof(bool) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kInt32Data:
              case XStore_Proto::FData::kUint32Data:
              case XStore_Proto::FData::kSint32Data:
              case XStore_Proto::FData::kFixed32Data:
              case XStore_Proto::FData::kSfixed32Data:
                  row_size += sizeof(int32_t) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kInt64Data:
              case XStore_Proto::FData::kUint64Data:
              case XStore_Proto::FData::kSint64Data:
              case XStore_Proto::FData::kFixed64Data:
              case XStore_Proto::FData::kSfixed64Data:
                  row_size += sizeof(int64_t) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::ONEOFDATA_NOT_SET:
                  break;
          }
      }

      return (row_size + Alignment - 1) & ~(Alignment - 1);
  }

  // ==== Unary ====
  // durability_mode defaults to ASYNC -- no fsync wait, lowest latency. A
  // row is only in-memory/queued until the writer thread flushes it, so a
  // crash before that flush loses it. Pass SYNC explicitly for writes that
  // must survive a crash.
  [[nodiscard]] std::string UnaryInsert(std::time_t insert_unix_time, const XStore_Proto::RowData &data, std::string_view db_name,
                                         XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unaryinsert();
      payload->set_rtype(XStore_Proto::UNARYINSERT);
      payload->set_timestamp(insert_unix_time);
      *payload->mutable_data() = data;
      payload->set_dbname(std::string(db_name));
      payload->set_durabilitymode(durability_mode);
      return SendRecv<XStore_Proto::UnaryInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] XStore_Proto::UnaryQuery_REP UnaryQuery(std::time_t target_unix_time, std::string_view target_db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unaryquery();
      payload->set_rtype(XStore_Proto::UNARYQUERY);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryQuery_REP>(to_server);
  }

  [[nodiscard]] std::string UnaryDelete(std::time_t target_unix_time, std::string_view target_db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unarydelete();
      payload->set_rtype(XStore_Proto::UNARYDELETE);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryDelete_REP>(to_server).deletestatus();
  }

  // ==== Batch (arbitrary timestamp set) ====
  // durability_mode defaults to ASYNC, same WAL-routed semantics as
  // UnaryInsert's: see BatchInsert_REQ's durabilityMode comment in
  // XStore.proto. SYNC blocks until the write is durably fsync'd and
  // applied; ASYNC enqueues and returns immediately -- note that for the
  // default ASYNC mode, both the fsync AND the data-file write itself are
  // deferred to the server's WAL writer thread, so (unlike this client's
  // pre-WAL behavior) a read immediately after an ASYNC BatchInsert is not
  // guaranteed to see it yet.
  [[nodiscard]] std::string BatchInsert(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows,
                                         XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchinsert();
      payload->set_rtype(XStore_Proto::BATCHINSERT);
      payload->set_dbname(std::string(db_name));
      payload->set_durabilitymode(durability_mode);
      payload->mutable_batchinsert_data()->Reserve(static_cast<int>(rows.size()));
      for (const auto &row : rows) {
          XStore_Proto::BatchInsert_DATA *entry = payload->add_batchinsert_data();
          entry->set_timestamp(row.first);
          *entry->mutable_data() = row.second;
      }
      return SendRecv<XStore_Proto::BatchInsert_REP>(to_server).insertstatus();
  }

  // Advanced/perf overload: build the request directly (e.g. reuse a
  // populated message across calls) instead of via the (timestamp, RowData)
  // vector above. Takes by value so an rvalue caller moves in for free.
  [[nodiscard]] std::string BatchInsert(XStore_Proto::BatchInsert_REQ insert_data) {
      XStore_Proto::Request to_server;
      *to_server.mutable_batchinsert() = std::move(insert_data);
      return SendRecv<XStore_Proto::BatchInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::BatchQuery_REP> BatchQuery(std::string_view db_name, const std::vector<uint64_t> &timestamps, bool sort_time = true) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchquery();
      payload->set_rtype(XStore_Proto::BATCHQUERY);
      payload->set_dbname(std::string(db_name));
      payload->set_sorttime(sort_time);
      for (uint64_t ts : timestamps) {
          payload->add_timestamp(ts);
      }
      return SendRecvOnArena<XStore_Proto::BatchQuery_REP>(to_server);
  }

  [[nodiscard]] std::string BatchDelete(std::string_view db_name, const std::vector<uint64_t> &timestamps) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchdelete();
      payload->set_rtype(XStore_Proto::BATCHDELETE);
      payload->set_dbname(std::string(db_name));
      for (uint64_t ts : timestamps) {
          payload->add_timestamp(ts);
      }
      return SendRecv<XStore_Proto::BatchDelete_REP>(to_server).deletestatus();
  }

  // ==== Range (contiguous timestamp span) ====
  // There is no RangeInsert here: BatchInsert takes timestamp-ordered, gapped or unordered rows alike (the server
  // detects order itself; the last duplicate wins). The RANGEINSERT wire message stays server-side for old clients.

  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::RangeQuery_REP> RangeQuery(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_rangequery();
      payload->set_rtype(XStore_Proto::RANGEQUERY);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecvOnArena<XStore_Proto::RangeQuery_REP>(to_server);
  }

  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::FindByValue_REP> FindByValue(uint64_t start_time, uint64_t end_time,
                                                                                  std::string_view db_name, int32_t column_idx,
                                                                                  const XStore_Proto::FData &value) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_findbyvalue();
      payload->set_rtype(XStore_Proto::FINDBYVALUE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      *payload->mutable_value() = value;
      return SendRecvOnArena<XStore_Proto::FindByValue_REP>(to_server);
  }

  // Server side is a TEMPORARY IMPLEMENTATION (option A, brute-force scan);
  // this call's shape is stable. LIKE: % any run, _ one byte, \ escapes.
  // REGEX: RE2, unanchored. case_insensitive: ASCII fold for LIKE.
  [[nodiscard]] xstore_client::ArenaReply<XStore_Proto::FindByPattern_REP> FindByPattern(
      uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, std::string_view pattern,
      XStore_Proto::PatternMode mode = XStore_Proto::PATTERN_LIKE, bool case_insensitive = false) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_findbypattern();
      payload->set_rtype(XStore_Proto::FINDBYPATTERN);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      payload->set_pattern(std::string(pattern));
      payload->set_mode(mode);
      payload->set_caseinsensitive(case_insensitive);
      return SendRecvOnArena<XStore_Proto::FindByPattern_REP>(to_server);
  }

  [[nodiscard]] std::string RangeDelete(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_rangedelete();
      payload->set_rtype(XStore_Proto::RANGEDELETE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RangeDelete_REP>(to_server).deletestatus();
  }

  // ==== Aggregates ====
  [[nodiscard]] XStore_Proto::Min_REP Min(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_min();
      payload->set_rtype(XStore_Proto::MIN);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Min_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Max_REP Max(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_max();
      payload->set_rtype(XStore_Proto::MAX);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Max_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Sum_REP Sum(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_sum();
      payload->set_rtype(XStore_Proto::SUM);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Sum_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Avg_REP Avg(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_avg();
      payload->set_rtype(XStore_Proto::AVG);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Avg_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Variance_REP Variance(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_variance();
      payload->set_rtype(XStore_Proto::VARIANCE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Variance_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Stddev_REP Stddev(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_stddev();
      payload->set_rtype(XStore_Proto::STDDEV);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Stddev_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Exists_REP Exists(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_exists();
      payload->set_rtype(XStore_Proto::EXISTS);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Exists_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Ohlc_REP Ohlc(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_ohlc();
      payload->set_rtype(XStore_Proto::OHLC);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Ohlc_REP>(to_server);
  }

  // Exact percentile (PERCENTILE_CONT/linear-interpolation semantics),
  // percentile in [0.0, 1.0] (0.5 = median). Not zone-map-accelerated --
  // see Percentile_REQ's comment in XStore.proto.
  [[nodiscard]] XStore_Proto::Percentile_REP Percentile(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, double percentile) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_percentile();
      payload->set_rtype(XStore_Proto::PERCENTILE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      payload->set_percentile(percentile);
      return SendRecv<XStore_Proto::Percentile_REP>(to_server);
  }

  // Exact distinct-value count. Not zone-map-accelerated -- see
  // DistinctCount_REQ's comment in XStore.proto.
  [[nodiscard]] XStore_Proto::DistinctCount_REP DistinctCount(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_distinctcount();
      payload->set_rtype(XStore_Proto::DISTINCTCOUNT);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::DistinctCount_REP>(to_server);
  }

  // Exact top-k most-frequent values (k=1 gives the mode). Not zone-map-
  // accelerated -- see TopK_REQ's comment in XStore.proto.
  [[nodiscard]] XStore_Proto::TopK_REP TopK(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx, uint32_t k) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_topk();
      payload->set_rtype(XStore_Proto::TOPK);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      payload->set_k(k);
      return SendRecv<XStore_Proto::TopK_REP>(to_server);
  }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
