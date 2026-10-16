#include "include/XStore/XStore_Internal.h"
#include "include/XStore/XStore_DirLock.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/XStore/XStore_RowScan.h"
#include "include/XStore/XStore_WireEncode.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_WalCheckpoint.h"
#include "include/XStore/XStore_LiveWidenGovernor.h"
#include "include/XStore/XStore_LiveWidenLoadSource.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <limits>
#include <optional>
#include <cerrno>
#include <chrono>
#include <ctime>
#include <exception>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <set>
#include <stdexcept>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_set>

std::array<XStore::CfgShard, XStore::CFG_SHARD_COUNT> XStore::cfg_shards;

namespace {
// One live widen's status per (db_base_path, year) (Tech Reference 4.18 "Rebuild notice"), read lock-free by queries.
// BeginLiveRebuild admits one builder per index FILE, so a year can have two (one per file, or a later widen of one file
// while the other is left in rebuild by an earlier stopped one): each builder sets, resets and clears only the flag and
// watermark of the files it began, and the year stops being counted only when both flags are false. Transitions run
// under RebuildStatusMutex(); readers never lock. Nodes and the mutex are allocated once and never freed (like the WAL
// ring registry): query threads that are never joined may still walk the list while static destructors run at exit.
struct LiveRebuildStatus {
    LiveRebuildStatus(std::string path, unsigned short y) : db_base_path(std::move(path)), year(y) {}
    const std::string db_base_path;
    const unsigned short year;
    LiveRebuildStatus *next = nullptr;          // set once, before the node is published
    std::atomic<bool> active{false};            // counted in g_years_rebuilding (== zmap || catidx, under the mutex)
    std::atomic<bool> zmap{false};              // a live widen's .zmap rebuild is in progress or left stopped/failed
    std::atomic<bool> catidx{false};
    std::atomic<uint64_t> zmap_rows{0};         // each file's contiguous watermark
    std::atomic<uint64_t> catidx_rows{0};
    std::atomic<uint64_t> total_rows{0};
    std::atomic<bool> zmap_governed{false};     // that file's builder runs under a governor (cleared when it ends)
    std::atomic<bool> catidx_governed{false};
    std::atomic<unsigned> allowed{0};           // the governor's state (two governed builders on one year share it)
    std::atomic<unsigned> max_workers{0};
    std::atomic<livewiden::GovernorReason> reason{livewiden::GovernorReason::kStart};
};

std::atomic<uint32_t> g_years_rebuilding{0};
std::atomic<LiveRebuildStatus *> g_rebuild_status_head{nullptr};

std::mutex &RebuildStatusMutex() {
    static std::mutex *const m = new std::mutex; // never destroyed: see LiveRebuildStatus
    return *m;
}

LiveRebuildStatus *FindRebuildStatus(std::string_view db_base_path, unsigned short year) noexcept {
    for (LiveRebuildStatus *n = g_rebuild_status_head.load(std::memory_order_acquire); n != nullptr; n = n->next) {
        if (n->year == year && n->db_base_path == db_base_path) return n;
    }
    return nullptr;
}

LiveRebuildStatus &RebuildStatusFor(const std::string &db_base_path, unsigned short year) {
    std::lock_guard<std::mutex> lock(RebuildStatusMutex());
    if (LiveRebuildStatus *n = FindRebuildStatus(db_base_path, year)) return *n;
    auto *n = new LiveRebuildStatus(db_base_path, year); // never freed: see LiveRebuildStatus
    n->next = g_rebuild_status_head.load(std::memory_order_relaxed);
    g_rebuild_status_head.store(n, std::memory_order_release);
    return *n;
}

// Mutex held. Counts the year while either file is flagged.
void SyncRebuildActive(LiveRebuildStatus &n) noexcept {
    const bool want = n.zmap.load(std::memory_order_relaxed) || n.catidx.load(std::memory_order_relaxed);
    if (n.active.exchange(want, std::memory_order_acq_rel) != want) {
        if (want) {
            g_years_rebuilding.fetch_add(1, std::memory_order_relaxed);
        } else {
            g_years_rebuilding.fetch_sub(1, std::memory_order_relaxed);
        }
    }
}

// A builder begins (true) its files: their watermarks restart at 0.
void BeginRebuildFiles(LiveRebuildStatus &n, bool zmap, bool catidx) {
    std::lock_guard<std::mutex> lock(RebuildStatusMutex());
    if (zmap) {
        n.zmap_rows.store(0, std::memory_order_relaxed);
        n.zmap_governed.store(false, std::memory_order_relaxed);
        n.zmap.store(true, std::memory_order_relaxed);
    }
    if (catidx) {
        n.catidx_rows.store(0, std::memory_order_relaxed);
        n.catidx_governed.store(false, std::memory_order_relaxed);
        n.catidx.store(true, std::memory_order_relaxed);
    }
    SyncRebuildActive(n);
}

// The files given (true) are no longer in a live rebuild: finished, or their index state was dropped.
void EndRebuildFiles(LiveRebuildStatus &n, bool zmap, bool catidx) noexcept {
    if (zmap) n.zmap.store(false, std::memory_order_relaxed);
    if (catidx) n.catidx.store(false, std::memory_order_relaxed);
    SyncRebuildActive(n);
}

template <typename Match>
void ForgetRebuildStatusMatching(const Match &match) {
    std::lock_guard<std::mutex> lock(RebuildStatusMutex());
    for (LiveRebuildStatus *n = g_rebuild_status_head.load(std::memory_order_acquire); n != nullptr; n = n->next) {
        if (match(*n)) EndRebuildFiles(*n, true, true);
    }
}
} // namespace

// Initialization
XStore::XStore() {
    // One XStore instance is constructed per worker thread (see ZMQ_Server),
    // and in production every one of them shares the same data_store_dir for
    // the life of the process -- so creating it and scanning it into the
    // shared CfgMap only needs to happen once per distinct data_store_dir, not
    // once per thread. Keyed by the path itself (rather than a single
    // once_flag) so this still does real work again for a genuinely
    // different data_store_dir, which is what lets tests construct many
    // XStore instances against many distinct scratch directories within one
    // process.
    // Heap-allocated and leaked: these are constructed AFTER StartWalWriters registered its atexit hook,
    // so as plain function-local statics they would be destroyed before the hook joins the threads.
    static std::mutex &init_mutex = *new std::mutex();
    static std::unordered_set<std::string> &initialized_dirs = *new std::unordered_set<std::string>();

    std::lock_guard<std::mutex> lock(init_mutex);
    const bool new_dir = initialized_dirs.count(data_store_dir) == 0;
    if (new_dir) {
        std::filesystem::create_directories(data_store_dir);
        // Backstop of main.cpp's own early lock: no other process may use this directory. Taken before
        // StartWalWriters (opens/zero-fills the WAL rings) and InitDB (replay, index rebuilds). A refusal
        // leaves the directory out of initialized_dirs, so a retry tries again.
        const dirlock::Result dir_lock = dirlock::Acquire(data_store_dir);
        if (!dir_lock.warning.empty()) spdlog::warn("[BOOT] - {}", dir_lock.warning);
        if (!dir_lock.ok) throw std::runtime_error(dir_lock.error);
    }

    // WAL writer threads are process-wide static state (wal_shards is a
    // static array, see XStore.h) -- started exactly once per process,
    // independent of how many distinct data_store_dirs get initialized
    // (unlike InitDB, which is per-directory).
    static std::once_flag wal_writers_once;
    std::call_once(wal_writers_once, &XStore::StartWalWriters, data_store_dir);

    if (new_dir) {
        initialized_dirs.insert(data_store_dir);
        InitDB();
    }
}

bool XStore::GetOrLoadCfg(const std::string &db_name, const std::string &db_base_path, XStore_Proto::ConfigStruct &cfg_out) {
    CfgShard &shard = cfg_shards[std::hash<std::string>{}(db_name) % CFG_SHARD_COUNT];

    // Fast path: another thread may have already cached this config
    {
        std::shared_lock read_lock(shard.mutex);
        auto it = shard.map.find(db_name);
        if (it != shard.map.end()) {
            cfg_out = it->second;
            return true;
        }
    }

    // Cache miss -> take exclusive lock & re-check (another thread may have
    // loaded it while we were waiting for the lock)
    std::unique_lock write_lock(shard.mutex);
    auto it = shard.map.find(db_name);
    if (it != shard.map.end()) {
        cfg_out = it->second;
        return true;
    }

    std::string cfg_path = JoinPath(db_base_path, db_name + ".CFG");
    if (!std::filesystem::exists(cfg_path)) {
        return false;
    }

    FILE *c_file = std::fopen(cfg_path.c_str(), "rb");
    if (c_file == nullptr) {
        return false;
    }
    cfg_out.ParseFromFileDescriptor(fileno(c_file));
    std::fclose(c_file);

    // Sanitize BEFORE this value is cached or handed back -- everything
    // downstream (EffectiveTrackedColumnCount, BuildZoneMapObservations,
    // every GetOrOpenZoneMap call site) treats a cached cfg as already
    // trustworthy. See ClampPersistedColumnCount's comment in XStore.h.
    ClampPersistedColumnCount(db_name, cfg_out);

    shard.map[db_name] = cfg_out;
    return true;
}

void XStore::ClampPersistedColumnCount(const std::string &db_name, XStore_Proto::ConfigStruct &cfg) {
    if (cfg.columncount() <= XStoreTunables::MAX_TRACKED_COLUMN_COUNT) return;

    // Warn, don't silently correct: an out-of-bound value can only get onto
    // disk via a binary older than CreateDB's seed check, a restored/foreign
    // .CFG, or genuine corruption -- all of which an operator wants to see
    // named once at load time rather than inferred later from a DB that
    // quietly stopped tracking the columns it was configured for. Same
    // spdlog::warn + "[TAG] [DB: x] - msg" shape as CreateDB's rejection of
    // the identical bound.
    spdlog::warn("[CONFIG LOAD] [DB: {}] - Persisted column count {} exceeds the {} maximum; clamping",
                 db_name, cfg.columncount(), XStoreTunables::MAX_TRACKED_COLUMN_COUNT);
    cfg.set_columncount(XStoreTunables::MAX_TRACKED_COLUMN_COUNT);
}

void XStore::PutCfg(const std::string &db_name, const XStore_Proto::ConfigStruct &cfg) {
    XStore_Proto::ConfigStruct sanitized = cfg;
    ClampPersistedColumnCount(db_name, sanitized);

    CfgShard &shard = cfg_shards[std::hash<std::string>{}(db_name) % CFG_SHARD_COUNT];
    std::unique_lock write_lock(shard.mutex);
    shard.map[db_name] = std::move(sanitized);
}

void XStore::EraseCfg(const std::string &db_name) {
    CfgShard &shard = cfg_shards[std::hash<std::string>{}(db_name) % CFG_SHARD_COUNT];
    std::unique_lock write_lock(shard.mutex);
    shard.map.erase(db_name);
}

std::string XStore::DbBasePath(const std::string &db_name) const {
    return JoinPath(data_store_dir, db_name);
}

const std::string& XStore::CachedDbBasePath(const std::string &db_name) const {
    // Keyed by (data_store_dir, dbName), not dbName alone: data_store_dir is
    // constant for the life of a real server process (one fixed cwd at
    // startup), so in production this collapses to a single outer-map
    // entry and behaves exactly like the old dbName-only cache. It only
    // matters when the same thread constructs XStore instances against
    // different DataStoreDirs -- e.g. this codebase's own test suite,
    // which chdir()s into a fresh scratch directory per test case but
    // reuses "TESTDB" as the dbName across nearly all of them. A dbName-
    // only, thread_local memo doesn't expire when a test's directory is
    // torn down, so a later test's lookup for "TESTDB" would silently
    // return an earlier, already-deleted test's path instead of its own.
    thread_local std::unordered_map<std::string, std::unordered_map<std::string, std::string>> tls_base_paths;
    std::unordered_map<std::string, std::string> &dir_map = tls_base_paths[data_store_dir];
    auto it = dir_map.find(db_name);
    if (it != dir_map.end()) {
        return it->second;
    }
    return dir_map.emplace(db_name, DbBasePath(db_name)).first->second;
}

const std::string& XStore::CachedYearFilePath(const std::string &db_base_path, unsigned short year) {
    thread_local std::unordered_map<std::string, std::unordered_map<unsigned short, std::string>> tls_year_paths;
    std::unordered_map<unsigned short, std::string> &year_map = tls_year_paths[db_base_path];
    auto it = year_map.find(year);
    if (it != year_map.end()) {
        return it->second;
    }
    return year_map.emplace(year, fmt::format("{}/{}.XSTORE", db_base_path, year)).first->second;
}

std::mutex XStore::zone_map_registry_mutex_;
std::unordered_map<std::string, std::unique_ptr<ZoneMapFile>> XStore::zone_map_registry_;
std::vector<std::string> XStore::zone_map_registry_lru_;
std::vector<std::unique_ptr<ZoneMapFile>> XStore::zone_map_registry_retired_;

namespace {
bool zone_map_over_cap_warned = false; // guarded by zone_map_registry_mutex_: one warning per over-cap episode
} // namespace

ZoneMapFile& XStore::GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length, uint32_t tracked_column_count) {
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
    return GetOrOpenZoneMapLocked(db_base_path, year, row_length, tracked_column_count);
}

ZoneMapFile::Pin XStore::PinZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                    uint32_t tracked_column_count) {
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
    return ZoneMapFile::Pin(&GetOrOpenZoneMapLocked(db_base_path, year, row_length, tracked_column_count));
}

// Lock order is registry mutex -> file mutex, and nothing that holds a file mutex takes a registry mutex. The lookup/
// open/evict path below (run per insert) must never take a file mutex: one can be held across fdatasync or fallocate,
// and every insert would queue behind it. Evictable() is lock-free for that reason. (Only the rare
// PeekOpenZoneMapWidth and Shutdown take a file mutex under the registry mutex.)
ZoneMapFile& XStore::GetOrOpenZoneMapLocked(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                            uint32_t tracked_column_count) {
    std::string key = fmt::format("{}/{}", db_base_path, year);
    auto it = zone_map_registry_.find(key);
    if (it != zone_map_registry_.end()) {
        zone_map_registry_lru_.erase(
            std::remove(zone_map_registry_lru_.begin(), zone_map_registry_lru_.end(), key),
            zone_map_registry_lru_.end());
        zone_map_registry_lru_.push_back(key);
        return *it->second;
    }

    if (!zone_map_registry_retired_.empty()) SweepRetired(zone_map_registry_retired_);

    // Pinned and rebuilding entries are never evicted: their holder has a pointer, and a cold reopen of a half-built
    // file would trust blocks the builder has not reached (the width clamp below would happily serve it). With every
    // entry held the registry grows past the cap, and shrinks back once they are released.
    while (zone_map_registry_.size() >= XStoreTunables::ZONE_MAP_REGISTRY_CAP) {
        auto victim = std::find_if(zone_map_registry_lru_.begin(), zone_map_registry_lru_.end(),
                                   [](const std::string &k) { return zone_map_registry_.at(k)->Evictable(); });
        if (victim == zone_map_registry_lru_.end()) {
            if (!zone_map_over_cap_warned) {
                spdlog::warn("[ZONEMAP] - Every one of {} open zone maps is pinned or being rebuilt; growing past the cap",
                             zone_map_registry_.size());
                zone_map_over_cap_warned = true;
            }
            break;
        }
        zone_map_registry_.erase(*victim); // ~ZoneMapFile() closes the fd -- no flush needed, already durable
        zone_map_registry_lru_.erase(victim);
    }
    if (zone_map_registry_.size() < XStoreTunables::ZONE_MAP_REGISTRY_CAP) zone_map_over_cap_warned = false;

    auto zmap = std::make_unique<ZoneMapFile>();
    std::string path = fmt::format("{}/{}.zmap", db_base_path, year);

    // Non-destructive guard for ordinary (non-rebuild) callers. Every call
    // site now passes the DB's real EffectiveTrackedColumnCount so a
    // FRESHLY created zone map is born at the right width (which is what
    // stops a brand-new year file from being flagged stale on the very next
    // restart). But ZoneMapFile::Open()'s widen branch DISCARDS the file
    // (ftruncate + fresh header) whenever the requested width exceeds what's
    // on disk -- correct for a rebuild, which immediately re-folds every row
    // from raw data, and catastrophic for anyone else: the discarded stats
    // would never come back (ZoneMapNeedsRebuild would then see a header
    // already matching the desired width and skip it forever), turning every
    // pushdown-eligible query on that file into a silent zero/skip.
    //
    // So: if a current-version file already exists here, clamp the request
    // down to its width. Open() then takes its "keep it" path and the file
    // simply stays narrower than this caller's observations -- always safe
    // (UpdateRow drops observations past the cap; TryGetBlockStats returns
    // exactly tracked_column_count entries, so a column past it is never
    // trusted and always falls back to a real scan). Widening stays the
    // exclusive business of RebuildOneYearZoneMap, which is unaffected by
    // this clamp because it ftruncate()s the .zmap to 0 bytes immediately
    // before calling in here -- an empty/absent file has no header to clamp
    // against.
    {
        int peek_fd = open(path.c_str(), O_RDONLY);
        if (peek_fd >= 0) {
            struct stat peek_st{};
            ZoneMapFileHeader on_disk{};
            if (fstat(peek_fd, &peek_st) == 0 &&
                peek_st.st_size >= static_cast<off_t>(sizeof(ZoneMapFileHeader)) &&
                pread(peek_fd, &on_disk, sizeof(on_disk), 0) == static_cast<ssize_t>(sizeof(on_disk)) &&
                on_disk.magic == ZoneMapFileHeader{}.magic &&
                on_disk.version == ZONE_MAP_VERSION &&
                on_disk.tracked_column_count > 0 &&
                on_disk.tracked_column_count < tracked_column_count) {
                tracked_column_count = on_disk.tracked_column_count;
            }
            close(peek_fd);
        }
    }

    if (!zmap->Open(path, row_length, tracked_column_count)) {
        spdlog::error("[ZONEMAP] - Unable to open {}", path);
        throw std::runtime_error(fmt::format("ZoneMapFile::Open failed for {}", path));
    }
    ZoneMapFile &ref = *zmap;
    zone_map_registry_.emplace(key, std::move(zmap));
    zone_map_registry_lru_.push_back(key);
    return ref;
}

ZoneMapFile::Pin XStore::OpenZoneMapForDelete(const std::string &db_base_path, unsigned short year,
                                              const XStore_Proto::ConfigStruct &cfg_struct) {
    try {
        return PinZoneMap(db_base_path, year, static_cast<uint16_t>(cfg_struct.rowlength()),
                          EffectiveTrackedColumnCount(cfg_struct));
    } catch (const std::exception &e) {
        spdlog::error("[ZONE MAP] [{}/{}] - Cannot open the zone map for a delete: {}", db_base_path, year, e.what());
        return ZoneMapFile::Pin();
    }
}

std::optional<uint32_t> XStore::PeekOpenZoneMapWidth(std::string_view db_base_path, unsigned short year, bool *rebuilding) {
    std::string key = fmt::format("{}/{}", db_base_path, year);
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
    auto it = zone_map_registry_.find(key);
    if (rebuilding != nullptr) *rebuilding = it != zone_map_registry_.end() && it->second->Rebuilding();
    if (it == zone_map_registry_.end()) return std::nullopt;
    return it->second->TrackedColumnCount();
}

XStore::YearIndexWidths XStore::PeekYearIndexWidths(const std::string &db_base_path, unsigned short year) {
    YearIndexWidths w;
    const std::string key = fmt::format("{}/{}", db_base_path, year);
    {
        std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
        auto it = zone_map_registry_.find(key);
        if (it != zone_map_registry_.end()) {
            w.zmap = it->second->TrackedColumnCount();
            w.rebuilding = it->second->Rebuilding();
        }
    }
    {
        std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
        auto it = catidx_registry_.find(key);
        if (it != catidx_registry_.end()) {
            w.catidx = it->second->TrackedColumnCount();
            w.rebuilding = w.rebuilding || it->second->Rebuilding();
        }
    }
    // Cold: the header. A rebuilding object is never evicted, so a cold file is not in rebuild in this process.
    const auto read_header = [](const std::string &path, auto &header) {
        const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) return false;
        const bool ok = pread(fd, &header, sizeof(header), 0) == static_cast<ssize_t>(sizeof(header));
        close(fd);
        return ok;
    };
    if (!w.zmap.has_value()) {
        ZoneMapFileHeader h{};
        if (read_header(fmt::format("{}/{}.zmap", db_base_path, year), h) && h.magic == ZoneMapFileHeader{}.magic &&
            h.version == ZONE_MAP_VERSION && h.tracked_column_count > 0) {
            w.zmap = h.tracked_column_count;
        }
    }
    if (!w.catidx.has_value()) {
        CatIdxFileHeader h{};
        if (read_header(fmt::format("{}/{}.catidx", db_base_path, year), h) && h.magic == CatIdxFileHeader{}.magic &&
            h.version == XStoreTunables::CATIDX_VERSION) {
            w.catidx = h.tracked_column_count;
        }
    }
    return w;
}

// Ignores pins, like the explicit erase in RebuildYearIndexes: neither may run while a builder or another pin
// holder is alive (today they run only in InitDB and in tests).
void XStore::ClearZoneMapRegistryForTests() {
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
    zone_map_registry_.clear();
    zone_map_registry_lru_.clear();
    zone_map_registry_retired_.clear();
    ForgetRebuildStatusMatching([](const LiveRebuildStatus &) { return true; });
}

template <typename Match>
void XStore::RetireIndexesMatching(const Match &match) {
    {
        std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
        RetireMatching(zone_map_registry_, zone_map_registry_lru_, zone_map_registry_retired_, match);
    }
    {
        std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
        RetireMatching(catidx_registry_, catidx_registry_lru_, catidx_registry_retired_, match);
    }
    ForgetRebuildStatusMatching([&match](const LiveRebuildStatus &n) {
        return match(fmt::format("{}/{}", n.db_base_path, n.year)); // the registry key format
    });
}

void XStore::RetireIndexesUnderPrefix(const std::string &db_dir_prefix) {
    RetireIndexesMatching([&db_dir_prefix](const std::string &key) {
        return key.compare(0, db_dir_prefix.size(), db_dir_prefix) == 0;
    });
}

void XStore::RetireYearIndexes(std::string_view db_base_path, unsigned short year) {
    const std::string year_key = fmt::format("{}/{}", db_base_path, year); // same format as GetOrOpen*Locked
    RetireIndexesMatching([&year_key](const std::string &key) { return key == year_key; });
}

size_t XStore::RetiredIndexCountForTests() {
    size_t count = 0;
    {
        std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
        count += zone_map_registry_retired_.size();
    }
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
    return count + catidx_registry_retired_.size();
}

XStore_Proto::SysInfo_REP XStore::InfoQuery(std::string_view server_addr, int thread_count) {
    // Protobuf obj to be returned
    XStore_Proto::SysInfo_REP to_return;

    // Overview info
    to_return.set_serveraddr(server_addr.data(), server_addr.size());
    to_return.set_threadcount(thread_count);

    // Loop through all db available in DataStore
    for (auto& dir : std::filesystem::directory_iterator(data_store_dir)) {
        std::string curr_db_name = dir.path().filename().string();

        // WAL/ lives as a sibling of the per-DB directories directly under
        // data_store_dir (see WalFilePathFor/InitDB) -- it holds
        // shard_<N>.WAL files, not a DB, and now exists eagerly from
        // StartWalWriters (constructor time) rather than lazily on first
        // insert, so it must be excluded here the same way InitDB already
        // excludes it.
        if (curr_db_name == XStoreTunables::WAL_DIR_NAME) {
            continue;
        }

        std::string curr_db_dir = JoinPath(data_store_dir, curr_db_name);

        // Set DB Name
        XStore_Proto::DbStats *curr_db = to_return.add_dbinfo();
        curr_db->set_dbname(curr_db_name);

        // Fetch config of a target DB -- via the shared cache every other
        // operation already uses, instead of a fresh open+parse per DB on
        // every InfoQuery call (a status/monitoring endpoint, likely polled
        // repeatedly).
        XStore_Proto::ConfigStruct cfg_struct;
        if (GetOrLoadCfg(curr_db_name, curr_db_dir, cfg_struct)) {
            // Set DB Resolution
            curr_db->set_dbresolution(cfg_struct.timeresolution());
            curr_db->set_rowlength(cfg_struct.rowlength());
            for (const std::string &name : cfg_struct.columnnames()) {
                curr_db->add_columnnames(name);
            }
        }

        // Size of all data files in current DB
        unsigned long int all_size = 0;
        struct stat stat_struct;

        // Loop through all files in currentDBDir
        for (auto& db_dir : std::filesystem::directory_iterator(curr_db_dir)) {
            // Current file name & path in dbDir
            std::string curr_file_name = db_dir.path().filename().string();
            stat(JoinPath(curr_db_dir, curr_file_name).c_str(), &stat_struct);

            // actual = real bytes on disk (st_blocks*512, sparse-hole-aware);
            // apparent = logical/reported size (st_size) -- data files are
            // preallocated via fallocate and may still have unwritten holes,
            // so apparent can exceed actual for a file that hasn't been
            // fully written yet.
            uint64_t actual_size = static_cast<uint64_t>(stat_struct.st_blocks) * 512;
            uint64_t apparent_size = static_cast<uint64_t>(stat_struct.st_size);

            all_size += actual_size;

            XStore_Proto::FileStats *curr_file = curr_db->add_files();
            curr_file->set_filename(curr_file_name);
            curr_file->set_actualsizebytes(actual_size);
            curr_file->set_apparentsizebytes(apparent_size);
        }
        curr_db->set_totalsizeinbyte(all_size);
    }

    return to_return;
}

uint32_t XStore::EffectiveTrackedColumnCount(const XStore_Proto::ConfigStruct &cfg_struct) {
    // max(), not `seeded > 0 ? seeded : default` -- see the long rationale
    // on the declaration in XStore.h: the global default is a FLOOR because
    // a narrowing effective width silently breaks every aggregate on this
    // DB (tracked-but-never-folded columns read back valid_count==0).
    return std::max(cfg_struct.columncount(), XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);
}

void XStore::MaybeGrowColumnCount(const std::string &db_name, const std::string &db_base_path, int observed_column_count) {
    if (observed_column_count <= 0) return;

    // Defensive upper bound, matching CreateDB's own seed check. In
    // practice observed_column_count is RowData::fielddata_size(), already
    // bounded by the row's serialized size (which UnaryInsert/BatchInsert/
    // RangeInsert reject above cfg.rowLength, itself a uint16-ish byte
    // budget), so this is not currently reachable with a huge value -- but
    // this is the OTHER writer of the same persisted field that sizes every
    // later ZoneMapEntry allocation, and a growth path that can outrun the
    // bound CreateDB enforces would just reopen that hole from the insert
    // side. Clamp rather than reject: growth is explicitly never allowed to
    // fail an insert (design doc, "Per-DB column count" -- a wide row
    // always inserts normally), so the row still lands, it just stops
    // driving the tracked mark past what the zone map will honor.
    if (observed_column_count > static_cast<int>(XStoreTunables::MAX_TRACKED_COLUMN_COUNT)) {
        observed_column_count = static_cast<int>(XStoreTunables::MAX_TRACKED_COLUMN_COUNT);
    }

    CfgShard &shard = cfg_shards[std::hash<std::string>{}(db_name) % CFG_SHARD_COUNT];

    // Fast path: shared lock, one map lookup + one integer comparison. The
    // common case once a DB's tracked width has stabilized -- no exclusive
    // lock, no I/O.
    {
        std::shared_lock read_lock(shard.mutex);
        auto it = shard.map.find(db_name);
        if (it == shard.map.end()) return; // cfg not cached yet; nothing to grow here
        if (static_cast<uint32_t>(observed_column_count) <= it->second.columncount()) return;
    }

    // Rare path: this row is wider than anything seen before for this DB.
    // The compare-and-set AND the persist happen inside one exclusive lock
    // so two threads racing with *different* observed widths can never
    // regress the persisted value -- whichever wins the lock first raises
    // the floor the other thread's compare then runs against, and the
    // on-disk write is ordered the same way as the in-memory update
    // (doing the rare disk write while holding the lock, rather than
    // after releasing it, is what actually closes the race -- releasing
    // first would let a second grower's file write land out of order
    // relative to a first grower's, corrupting the durable value even if
    // the in-memory map stayed correct).
    std::unique_lock write_lock(shard.mutex);
    auto it = shard.map.find(db_name);
    if (it == shard.map.end()) return;
    if (static_cast<uint32_t>(observed_column_count) <= it->second.columncount()) return; // lost the race to a larger grower

    uint32_t old_effective_tracked = EffectiveTrackedColumnCount(it->second);
    it->second.set_columncount(static_cast<uint32_t>(observed_column_count));
    XStore_Proto::ConfigStruct cfg_copy = it->second;
    // Only worth anything when this growth actually raised the effective width (observed_column_count clamped to a
    // value <= the global floor changes nothing and leaves no narrower index file behind). Recorded before the .CFG
    // rewrite: the cached width already grew, and it is what the background widen reads. Leaf lock, nothing else.
    const uint32_t new_effective_tracked = EffectiveTrackedColumnCount(cfg_copy);
    if (new_effective_tracked > old_effective_tracked) RecordLiveWidenRequest(db_name, db_base_path);

    std::string cfg_path = JoinPath(db_base_path, db_name + ".CFG");
    FILE *b_file = std::fopen(cfg_path.c_str(), "wb+");
    if (b_file == nullptr) {
        spdlog::error("[COLUMN COUNT GROWTH] [DB: {}] - Unable to open {} for rewrite", db_name, cfg_path);
        return;
    }
    {
        FileRangeLockGuard guard(cfg_path, 0, cfg_copy.ByteSizeLong(), FileRangeLock::LockType::WRITE);
        cfg_copy.SerializeToFileDescriptor(fileno(b_file));
    }
    std::fclose(b_file);

    spdlog::info("[COLUMN COUNT GROWTH] [DB: {}] - columnCount grew to {}", db_name, observed_column_count);

    // Task 4 / M4 stage 3: the "growth recorded" line.
    if (new_effective_tracked > old_effective_tracked) {
        spdlog::warn("[ZONEMAP] [DB: {}] - tracked column width grows {} -> {}; existing year index files (if any) are widened in "
                     "the background once the width has not grown for {} s (queries on the new columns full-scan meanwhile)",
                     db_name, old_effective_tracked, new_effective_tracked, XStoreTunables::LIVE_WIDEN_QUIET_PERIOD_SEC);
    }
}

std::mutex XStore::live_widen_mutex_;
std::map<std::string, XStore::LiveWidenRequest> XStore::live_widen_requests_;
std::set<std::string> XStore::live_widen_skip_logged_;
std::atomic<bool> XStore::live_widen_pending_{false};
std::atomic<bool> XStore::bg_cancel_{false};
std::mutex XStore::bg_mutex_;
std::condition_variable XStore::bg_cv_;
std::string XStore::bg_active_key_;
std::multiset<std::string> XStore::bg_removing_scopes_;

void XStore::RecordLiveWidenRequest(const std::string &db_name, const std::string &db_base_path) {
    std::lock_guard<std::mutex> lock(live_widen_mutex_);
    LiveWidenRequest &request = live_widen_requests_[db_base_path];
    request.db_name = db_name;
    request.last_growth = std::chrono::steady_clock::now(); // every growth restarts the quiet period
    ++request.growth_seq;
    request.failed_attempts = 0; // a new growth also ends a give-up / retry wait
    request.retry_after = {};
    live_widen_pending_.store(true, std::memory_order_relaxed);
}

void XStore::DropLiveWidenRequest(const std::string &db_base_path) {
    std::lock_guard<std::mutex> lock(live_widen_mutex_);
    live_widen_requests_.erase(db_base_path);
    if (live_widen_requests_.empty()) live_widen_pending_.store(false, std::memory_order_relaxed);
}

size_t XStore::LiveWidenRequestCountForTests() {
    std::lock_guard<std::mutex> lock(live_widen_mutex_);
    return live_widen_requests_.size();
}

std::optional<std::chrono::steady_clock::time_point> XStore::LiveWidenLastGrowthForTests(const std::string &db_base_path) {
    std::lock_guard<std::mutex> lock(live_widen_mutex_);
    auto it = live_widen_requests_.find(db_base_path);
    if (it == live_widen_requests_.end()) return std::nullopt;
    return it->second.last_growth;
}

void XStore::ClearLiveWidenRequestsForTests() {
    std::lock_guard<std::mutex> lock(live_widen_mutex_);
    live_widen_requests_.clear();
    live_widen_skip_logged_.clear();
    live_widen_pending_.store(false, std::memory_order_relaxed);
}

std::string XStore::BackgroundActiveKeyForTests() {
    std::lock_guard<std::mutex> lock(bg_mutex_);
    return bg_active_key_;
}

namespace {
// A removal scope covers a year key "<db dir>/<year>": RemoveDB's scope is the DB directory with its trailing '/',
// RemoveDBFiles' scope is the exact year key.
bool ScopeCovers(const std::string &scope, const std::string &year_key) {
    if (!scope.empty() && scope.back() == '/') return year_key.compare(0, scope.size(), scope) == 0;
    return scope == year_key;
}
} // namespace

XStore::BgEnter XStore::EnterBackgroundYear(const std::string &year_key) {
    std::lock_guard<std::mutex> lock(bg_mutex_);
    if (bg_cancel_.load(std::memory_order_relaxed)) return BgEnter::kCancelled;
    for (const std::string &scope : bg_removing_scopes_) {
        if (ScopeCovers(scope, year_key)) return BgEnter::kRemoving;
    }
    bg_active_key_ = year_key;
    return BgEnter::kGo;
}

void XStore::LeaveBackgroundYear() {
    {
        std::lock_guard<std::mutex> lock(bg_mutex_);
        bg_active_key_.clear();
    }
    bg_cv_.notify_all();
}

bool XStore::StopBackgroundWork(const std::string &scope) {
    std::unique_lock<std::mutex> lock(bg_mutex_);
    bg_removing_scopes_.insert(scope);
    bool cancelled = false;
    if (!bg_active_key_.empty() && ScopeCovers(scope, bg_active_key_)) {
        bg_cancel_.store(true, std::memory_order_relaxed);
        cancelled = true;
    }
    const auto released = [&] { return bg_active_key_.empty() || !ScopeCovers(scope, bg_active_key_); };
    if (!bg_cv_.wait_for(lock, std::chrono::milliseconds(XStoreTunables::BACKGROUND_REMOVE_WAIT_MS), released)) {
        spdlog::error("[REMOVE] [{}] - The background index thread did not let go of {} within {} ms; removing anyway",
                      scope, bg_active_key_, XStoreTunables::BACKGROUND_REMOVE_WAIT_MS);
    }
    return cancelled;
}

void XStore::ResumeBackgroundWork(const std::string &scope, bool cancelled) {
    std::lock_guard<std::mutex> lock(bg_mutex_);
    auto it = bg_removing_scopes_.find(scope);
    if (it != bg_removing_scopes_.end()) bg_removing_scopes_.erase(it);
    // Back to "only a shutdown cancels": the thread never starts a year while bg_cancel_ is set, so nothing else of
    // its work was begun under this cancel.
    if (cancelled) bg_cancel_.store(catidx_compaction_stop_flag_.load(std::memory_order_relaxed), std::memory_order_relaxed);
}

XStore_Proto::CreateDB_REP XStore::CreateDB(std::string_view db_name_view, XStore_Proto::TimeUnit time_resolution, short int row_length, uint32_t column_count_seed,
                                             const std::vector<std::string> &column_names) {
    // Protobuf obj to be returned
    XStore_Proto::CreateDB_REP to_return;
    std::string create_db_status;

    // Ensure dbName & timeResolution string is all capitalized. dbNameView
    // is a trivial pointer+length pass at the call boundary (no
    // std::string construction/copy there); UpperCaseInto does the one
    // read-uppercase-write pass into a reused thread_local buffer instead
    // of the old by-value-copy-then-transform-in-place's two passes.
    const std::string &db_name = UpperCaseInto(db_name_view);

    if (row_length < 4) {
        create_db_status = fmt::format("[CREATE DB] [DB: {}] - Row length must be >= 4", db_name);
        SetTaggedStatus(to_return.mutable_createdbstatus(), "[WARNING] ", create_db_status);
        spdlog::warn(create_db_status);
        return to_return;
    }

    // column_count_seed arrives straight off the wire (CreateDB_REQ
    // .columnCount, see ZMQ_Server.cpp) and is persisted to .CFG, from where
    // it sizes std::vector<ZoneMapEntry> allocations on every later insert
    // and read. An absurd value is therefore a DURABLE remote crash, not a
    // one-shot bad request: 0xFFFFFFFF would make the first insert on this
    // DB throw std::length_error, and the poisoned .CFG would reproduce it
    // on every retry. Rejected, not silently clamped, so a caller finds out
    // immediately instead of quietly getting a different DB than it asked
    // for -- same posture as the row_length guard directly above.
    if (column_count_seed > XStoreTunables::MAX_TRACKED_COLUMN_COUNT) {
        create_db_status = fmt::format("[CREATE DB] [DB: {}] - Column count must be <= {}", db_name, XStoreTunables::MAX_TRACKED_COLUMN_COUNT);
        SetTaggedStatus(to_return.mutable_createdbstatus(), "[WARNING] ", create_db_status);
        spdlog::warn(create_db_status);
        return to_return;
    }

    // Target directory of a given DB name
    std::string target_path = DbBasePath(db_name);

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(target_path) == 0) {
        // Check if within permitted time resolution bounds
        if (std::find(std::begin(permitted_time_resolution), std::end(permitted_time_resolution), XStore_Proto::TimeUnit_Name(time_resolution)) == std::end(permitted_time_resolution)) {
            create_db_status = fmt::format("[CREATE DB] [DB: {}] - Invalid time resolution", db_name);
            SetTaggedStatus(to_return.mutable_createdbstatus(), "[WARNING] ", create_db_status);
            spdlog::warn(create_db_status);
            return to_return;
        }

        // Create DB directory
        if (std::filesystem::create_directory(target_path) == 1) {
            std::string success_msg = fmt::format("[CREATE DB] [DB: {}] - Created DB", db_name);
            SetTaggedStatus(to_return.mutable_createdbstatus(), "[INFO] ", success_msg);

            // Dumps DB configurations info to disk
            //  Organize configs into a struct
            XStore_Proto::ConfigStruct cfg_struct;
            cfg_struct.set_dbname(db_name);
            cfg_struct.set_timeresolution(time_resolution);
            cfg_struct.set_rowlength(row_length);
            cfg_struct.set_columncount(column_count_seed);
            for (const std::string &name : column_names) {
                cfg_struct.add_columnnames(name);
            }

            // Writing cfgStruct to file
            FILE *b_file;
            std::string cfg_path  = JoinPath(target_path, db_name + ".CFG");
            b_file = std::fopen(cfg_path.c_str(), "wb+");

            {
                // Lock prior to writing
                FileRangeLockGuard guard(cfg_path, 0, cfg_struct.ByteSizeLong(), FileRangeLock::LockType::WRITE);

                // Writing actual data to disk
                cfg_struct.SerializeToFileDescriptor(fileno(b_file));
            }
            //  Close file after write
            std::fclose(b_file);

            //  Add this cfgStruct to cfgMap
            PutCfg(db_name, cfg_struct);

            spdlog::info(success_msg);
            return to_return;
        }
    }

    create_db_status = fmt::format("[CREATE DB] [DB: {}] - Already existed", db_name);
    SetTaggedStatus(to_return.mutable_createdbstatus(), "[WARNING] ", create_db_status);
    spdlog::warn(create_db_status);
    return to_return;
}

namespace {
// Runs one synchronous checkpoint cycle ahead of a removal (see RemoveDB). A failed or refused cycle (poisoned
// checkpointer, sync error) must not hang or block the removal: warn and proceed.
void CheckpointBeforeRemoval(const std::string &db_name) {
    if (!walring::Checkpointer::Instance().RunCycle()) {
        spdlog::warn("[REMOVE DB] [DB: {}] - Could not checkpoint the WAL before removing files (checkpointer poisoned or a sync "
                     "failed); un-checkpointed WAL records for this DB may be replayed into a DB re-created under the same name",
                     db_name);
    }
}
}  // namespace

XStore_Proto::RemoveDB_REP XStore::RemoveDB(std::string_view db_name_view) {
    // Protobuf obj to be returned
    std::string remove_status;
    XStore_Proto::RemoveDB_REP to_return;

    // Ensure dbName string is all capitalized
    const std::string &db_name = UpperCaseInto(db_name_view);

    // Location of DB
    std::string db_loc = JoinPath(data_store_dir, db_name);

    // IF DB does NOT exist
    //  -> Return remove error
    if (!std::filesystem::exists(db_loc)) {
        remove_status = fmt::format("[REMOVE DB] [DB: {}] - DB does not exists", db_name);
        SetTaggedStatus(to_return.mutable_removestatus(), "[WARNING] ", remove_status);
        spdlog::warn(remove_status);
        return to_return;
    }

    // Un-checkpointed ring records for this DB would replay into a DB re-created under the same name (with a
    // possibly different row length, corrupting it, or resurrecting deleted years). Checkpoint first so every
    // acknowledged record is past the checkpoint before the files go. Rare operation; the cycle mutex makes it
    // safe against the background checkpointer.
    CheckpointBeforeRemoval(db_name);

    // The background index thread (live widen, compaction) must let go of this DB first: it is cancelled if it works
    // on one of its years, and waited for (bounded). Until ResumeBackgroundWork it starts no year of this DB. A
    // cancelled widen leaves its files in rebuild state (dirty, untrusted); they are deleted and retired below.
    const std::string bg_scope = db_loc + "/";
    struct ResumeOnExit {
        const std::string &scope;
        bool cancelled;
        ~ResumeOnExit() { XStore::ResumeBackgroundWork(scope, cancelled); }
    } resume{bg_scope, StopBackgroundWork(bg_scope)};

    // Recursively remove entire DB with given DB Name
    std::filesystem::remove_all(db_loc);

    // Remove DB from cfgMap
    EraseCfg(db_name);

    // Drop & unmap any cached read mappings under this DB -- otherwise a
    // future query (via GetOrMapFile) could keep reading an orphaned,
    // unlinked mapping instead of a freshly-recreated DB with the same name.
    EraseMappedFilesUnderPrefix(db_loc + "/");
    // Same reasoning for UnaryInsert's cached fds (see GetOrOpenFile).
    EraseOpenFilesUnderPrefix(db_loc + "/");
    // And for the warm index objects: a DB re-created under this name must not get the old stats and blooms back.
    RetireIndexesUnderPrefix(db_loc + "/");
    // A pending widen of the removed DB is dropped (a DB re-created under the name records its own growth).
    DropLiveWidenRequest(db_loc);

    remove_status = fmt::format("[REMOVE DB] [DB: {}] - Removed DB with name: {}", db_name, db_name);
    SetTaggedStatus(to_return.mutable_removestatus(), "[INFO] ", remove_status);
    spdlog::info(remove_status);
    return to_return;
}

XStore_Proto::RemoveDBFiles_REP XStore::RemoveDBFiles(XStore_Proto::RemoveDBFiles_REQ *proto_from_client, std::string_view db_name_view) {
    // Protobuf obj to be returned
    std::string remove_status;
    XStore_Proto::RemoveDBFiles_REP to_return;

    // Ensure dbName string is all capitalized
    const std::string &db_name = UpperCaseInto(db_name_view);

    // Location of DB
    std::string db_loc = JoinPath(data_store_dir, db_name);

    if (!std::filesystem::exists(db_loc)) {
        for (int i = 0; i < proto_from_client->filename_size(); i++) {
            remove_status = fmt::format("[REMOVE DB FILES] [DB: {}] -- Unable to remove {}", db_name, proto_from_client->filename(i));
            SetTaggedStatus(to_return.add_removestatus(), "[WARNING] ", remove_status)->append(proto_from_client->filename(i));
            spdlog::warn(remove_status);
        }
        return to_return;
    }

    // Same reason as RemoveDB: records for the files about to be unlinked must be checkpointed first.
    CheckpointBeforeRemoval(db_name);

    // Removes one file directly inside the DB directory and drops its cached mapping/fd. Never throws. Reports the
    // outcome when the file existed, when its removal failed, or (absent) when `report_missing` (a requested name is
    // always reported; a year's index files only if present or not removable). Returns false only when the file
    // exists and could not be removed.
    auto remove_one = [&](const std::string &file_name, bool report_missing) {
        const std::filesystem::path target_path = JoinPath(db_loc, file_name);
        std::error_code ec;
        if (std::filesystem::remove(target_path, ec)) {
            // Drop this file's cached read mapping, if any -- same
            // reasoning as RemoveDB, scoped to a single file.
            EraseMappedFile(target_path.string());
            EraseOpenFile(target_path.string());
            std::string info_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - Removed {}", db_name, file_name);
            SetTaggedStatus(to_return.add_removestatus(), "[INFO] ", info_msg);
            spdlog::info(info_msg);
            return true;
        }
        if (ec) { // it is there (or cannot be checked) and the removal failed: EIO, EBUSY, a non-empty directory...
            std::string err_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - Unable to remove {} ({})", db_name, file_name, ec.message());
            SetTaggedStatus(to_return.add_removestatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
            return false;
        }
        if (report_missing) {
            std::string err_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - Unable to remove {}", db_name, file_name);
            SetTaggedStatus(to_return.add_removestatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
        }
        return true;
    };

    // Loop through each requested file name
    for (int i = 0; i < proto_from_client->filename_size(); i++) {
        const std::string &file_name = proto_from_client->filename(i);
        // filename is client-controlled and must name a single entry directly
        // inside this DB's directory -- reject anything with a path separator
        // (e.g. "../other.CFG") so a crafted request can't escape DbLoc and
        // delete arbitrary files elsewhere on disk.
        if (file_name.find('/') != std::string::npos) {
            std::string err_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - Invalid file name {}", db_name, file_name);
            SetTaggedStatus(to_return.add_removestatus(), "[WARNING] ", err_msg);
            spdlog::warn(err_msg);
            continue;
        }

        if (file_name == fmt::format("{}.CFG", db_name)) {
            std::string warn_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - {} is prohibited from remove", db_name, file_name);
            SetTaggedStatus(to_return.add_removestatus(), "[WARNING] ", warn_msg);
            spdlog::warn(warn_msg);
            continue;
        }
        // An index file alone is refused: the next touch would create a fresh, empty index for a year whose data still
        // exists, and every "no rows here" answer from it would be wrong. Removing the year's data file removes it.
        const auto ends_with = [&file_name](std::string_view suffix) {
            return file_name.size() >= suffix.size() &&
                   file_name.compare(file_name.size() - suffix.size(), suffix.size(), suffix) == 0;
        };
        if (ends_with(".zmap") || ends_with(".catidx")) {
            std::string warn_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - {} is an index file and cannot be removed on "
                                               "its own; remove the year's .XSTORE file instead", db_name, file_name);
            SetTaggedStatus(to_return.add_removestatus(), "[WARNING] ", warn_msg);
            spdlog::warn(warn_msg);
            continue;
        }

        // "<year>.XSTORE" (canonical decimal year): its index files go too, then that one (db, year) registry key is
        // retired. The other years' files stay on disk, so their entries must stay the live ones (a retired entry
        // plus a cold-opened one on the same file would overwrite each other's blocks). Other names retire nothing.
        std::optional<unsigned short> year;
        if (ends_with(".XSTORE")) {
            const std::string stem = file_name.substr(0, file_name.size() - std::string_view(".XSTORE").size());
            if (!stem.empty() && stem.size() <= 5 && !(stem.size() > 1 && stem.front() == '0') &&
                std::all_of(stem.begin(), stem.end(), [](char c) { return c >= '0' && c <= '9'; }) &&
                std::stoul(stem) <= std::numeric_limits<unsigned short>::max()) {
                year = static_cast<unsigned short>(std::stoul(stem));
            }
        }
        if (!year.has_value()) {
            remove_one(file_name, /*report_missing=*/true);
            continue;
        }

        // The background index thread must let go of this year first (cancel + bounded wait, as in RemoveDB); a
        // cancelled widen's half-built files are deleted and its entries retired below.
        const std::string bg_scope = fmt::format("{}/{}", db_loc, *year); // the registry / thread key format
        struct ResumeOnExit {
            const std::string &scope;
            bool cancelled;
            ~ResumeOnExit() { XStore::ResumeBackgroundWork(scope, cancelled); }
        } resume{bg_scope, StopBackgroundWork(bg_scope)};
        // The data file still exists when its removal failed: then its index files and entries stay as they are (a
        // fresh, empty index next to rows that are still there would answer "no rows here" wrongly).
        if (remove_one(file_name, /*report_missing=*/true)) {
            // A sidecar that cannot be removed is reported, and the entries are retired anyway: a live entry would
            // let a later insert re-create the year through it with the deleted rows' stats.
            remove_one(std::to_string(*year) + ".zmap", /*report_missing=*/false);
            remove_one(std::to_string(*year) + ".catidx", /*report_missing=*/false);
            RetireYearIndexes(db_loc, *year);
        }
    }
    return to_return;
}

void XStore::InitDB() {
    // Replay any WAL records left over from an unclean shutdown before
    // this DB's data files are trusted -- see ReplayWal for why this is
    // safe to run unconditionally (idempotent re-apply, and a no-op when
    // no WAL/ directory exists yet, e.g. a brand-new data_store_dir).
    //
    // Whatever it replayed landed in the DATA file only -- a replayed row
    // carries no ZoneMapObservations, so its zone-map update is a no-op (see
    // ReplayedYearMap in XStore.h). Those (db, year) pairs therefore have to
    // be rebuilt unconditionally below, independent of ZoneMapNeedsRebuild's
    // verdict, or the replayed rows stay invisible to every zone-map-
    // accelerated aggregate forever.
    const ReplayedYearMap replayed_years = ReplayWal();
    static const std::set<unsigned short> kNoForcedYears;

    // Loop through each dbName
    for (auto& p : std::filesystem::directory_iterator(data_store_dir))
    {
        // dbName is just the last path component -- reading it off the
        // filesystem::path itself instead of stripping a known-length
        // data_store_dir prefix means this doesn't care whether data_store_dir
        // carries a trailing '/' or not.
        std::string curr_db_name = p.path().filename().string();

        // WAL/ lives as a sibling of the per-DB directories directly under
        // data_store_dir (see WalFilePathFor) -- it holds shard_<N>.WAL
        // files, not a DB, so it must never be treated as one here.
        if (curr_db_name == XStoreTunables::WAL_DIR_NAME) {
            continue;
        }

        // Open & Read cfgStruct of all DB currently exists
        std::string cfg_file_path = JoinPath(p.path().string(), curr_db_name + ".CFG");

        XStore_Proto::ConfigStruct cfg_struct;
        FILE *c_file = std::fopen(cfg_file_path.c_str(), "rb");
        if (c_file == nullptr) {
            spdlog::error("[INIT DB] [DB: {}] - Unable to open {}", curr_db_name, cfg_file_path);
            continue;
        }
        cfg_struct.ParseFromFileDescriptor(fileno(c_file));
        std::fclose(c_file);

        // Clamp on THIS local too, not just inside PutCfg's own copy: the
        // local is what flows into RebuildStaleZoneMapsForDb ->
        // EffectiveTrackedColumnCount -> RebuildOneYearZoneMap a few lines
        // below, where BuildZoneMapObservations' reserve() and
        // EnsureBlockLoaded's std::vector<ZoneMapEntry> are sized from it --
        // the allocations an out-of-bound on-disk value actually throws on.
        // See ClampPersistedColumnCount in XStore.h.
        ClampPersistedColumnCount(curr_db_name, cfg_struct);
        PutCfg(curr_db_name, cfg_struct);

        // Task 5 / Task 11: rebuild every stale/missing zone map and catidx of
        // this DB before InitDB() returns -- i.e. before any worker thread
        // sharing this data_store_dir can serve a request against it (see
        // RebuildStaleIndexesForDb's declaration comment in XStore.h for why
        // this must never run on the request path). The WAL-replayed years of
        // this DB are force-rebuilt (both indexes): a replayed row carries no
        // observations, and the headers alone cannot show it.
        auto replayed_it = replayed_years.find(curr_db_name);
        const std::set<unsigned short> &force_rebuild_years_for_db =
            replayed_it == replayed_years.end() ? kNoForcedYears : replayed_it->second;
        RebuildStaleIndexesForDb(curr_db_name, p.path().string(), cfg_struct, force_rebuild_years_for_db);
    }
}

bool XStore::ZoneMapNeedsRebuild(const std::string &zmap_path, uint32_t desired_tracked_columns, uint16_t row_length) {
    if (!std::filesystem::exists(zmap_path)) return true;

    int fd = open(zmap_path.c_str(), O_RDONLY);
    if (fd < 0) return true; // can't even read it -- treat as needing rebuild, Open() will recreate it

    ZoneMapFileHeader header{};
    ssize_t read_bytes = pread(fd, &header, sizeof(header), 0);
    close(fd);

    if (read_bytes != static_cast<ssize_t>(sizeof(header))) return true; // truncated/corrupt
    if (header.magic != ZoneMapFileHeader{}.magic) return true;
    if (header.version != ZONE_MAP_VERSION) return true;
    if (header.tracked_column_count != desired_tracked_columns) return true;
    // A corrupt block size (e.g. 0: the first aggregate would divide by it) or one this row length no longer gives.
    if (header.rows_per_block != RowsPerBlock(row_length)) return true;
    if (header.dirty != 0) {
        // Durability A: the previous session never reached MarkClean (crash /
        // kill -9), so index writes may be lost or torn. The index is derived
        // data -- re-derive it from the .XSTORE rows.
        spdlog::info("[ZONE MAP REBUILD] [{}] - marked dirty (unclean shutdown) -- rebuilding", zmap_path);
        return true;
    }
    return false;
}

std::string XStore::UnindexedColumnNoticeText(std::string_view op, const std::string &db_name, int32_t column_idx,
                                               unsigned short year, uint32_t index_width) {
    return fmt::format("[WARNING] [{}] [DB: {}] - column {} is not yet indexed in {} (index width {}); "
                        "answered by full scan -- the index is widened in the background once the column count has "
                        "not grown for {} s (or at the next start)",
                        op, db_name, column_idx, year, index_width, XStoreTunables::LIVE_WIDEN_QUIET_PERIOD_SEC);
}


uint32_t XStore::YearsRebuilding() noexcept { return g_years_rebuilding.load(std::memory_order_relaxed); }

bool XStore::FindRebuildProgress(std::string_view db_base_path, unsigned short year, bool catidx,
                                 LiveRebuildProgress &out) noexcept {
    const LiveRebuildStatus *n = FindRebuildStatus(db_base_path, year);
    if (n == nullptr || !n->active.load(std::memory_order_acquire)) return false;
    if (!(catidx ? n->catidx : n->zmap).load(std::memory_order_relaxed)) return false;
    out.done_rows = (catidx ? n->catidx_rows : n->zmap_rows).load(std::memory_order_relaxed);
    out.total_rows = n->total_rows.load(std::memory_order_relaxed);
    out.slowed = (catidx ? n->catidx_governed : n->zmap_governed).load(std::memory_order_relaxed)
                     ? livewiden::SlowedText(n->allowed.load(std::memory_order_relaxed),
                                             n->max_workers.load(std::memory_order_relaxed),
                                             n->reason.load(std::memory_order_relaxed))
                     : nullptr;
    return true;
}

std::string XStore::RebuildingColumnNoticeText(std::string_view op, const std::string &db_name, int32_t column_idx,
                                               unsigned short year, const LiveRebuildProgress *progress) {
    if (progress == nullptr) {
        return fmt::format("[WARNING] [{}] [DB: {}] - column {} is being indexed in the background in {}; answered by "
                           "full scan until it finishes (a rebuild that was interrupted finishes at the next start)",
                           op, db_name, column_idx, year);
    }
    const uint64_t pct = progress->total_rows == 0
                             ? 0
                             : std::min<uint64_t>(100, progress->done_rows * 100 / progress->total_rows);
    const std::string state = progress->slowed != nullptr ? fmt::format("{}% done; slowed: {}", pct, progress->slowed)
                                                          : fmt::format("{}% done", pct);
    // A column >= ZONE_MAP_MAX_TRACKED_COLUMNS is noticed anywhere in the year (below the watermark it is indexed at
    // the new width already), so its text names only the part not yet rebuilt, like a low column's.
    if (column_idx >= static_cast<int32_t>(XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS)) {
        return fmt::format("[WARNING] [{}] [DB: {}] - column {} is being indexed in the background in {} ({}); the part "
                           "not yet rebuilt is answered by full scan (a rebuild that was interrupted finishes at the next "
                           "start)", op, db_name, column_idx, year, state);
    }
    return fmt::format("[WARNING] [{}] [DB: {}] - column {} is being indexed in the background in {} ({}); the part not "
                       "yet rebuilt is answered by scan, results are correct (a rebuild that was interrupted finishes at "
                       "the next start)", op, db_name, column_idx, year, state);
}

namespace {
// The rebuild notices of the years in [start_epoch, end_epoch] with a live widen of their .zmap: one per year whose
// part of the range reaches a row at or past the zone map's contiguous watermark (a column >= the always-indexed width
// counts in any part of the year). Exact for block usability: TryGetBlockStats serves only blocks that end at or below
// the watermark, and every block a range ending below the watermark fully covers ends at or below it (the partial
// blocks at a range's edges are scanned anyway). Allocates only for a notice.
std::string LiveRebuildNotices(std::string_view op, const std::string &db_name, const std::string &db_base_path,
                               const XStore_Proto::ConfigStruct &cfg, unsigned long long start_epoch,
                               unsigned long long end_epoch, int32_t column_idx) {
    std::string out;
    const bool high = column_idx >= static_cast<int32_t>(XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);
    unsigned long long iter_epoch = start_epoch;
    while (iter_epoch <= end_epoch) {
        TimeStruct curr_ts(static_cast<std::time_t>(iter_epoch), cfg.timeresolution());
        const unsigned long long end_of_current_year = std::min(end_epoch, static_cast<unsigned long long>(curr_ts.last_epoch));
        XStore::LiveRebuildProgress p;
        if (XStore::FindRebuildProgress(db_base_path, curr_ts.year, /*catidx=*/false, p) &&
            (high || end_of_current_year - static_cast<unsigned long long>(curr_ts.first_epoch) >= p.done_rows)) {
            if (!out.empty()) out += " | ";
            out += XStore::RebuildingColumnNoticeText(op, db_name, column_idx, curr_ts.year, &p);
        }
        iter_epoch = end_of_current_year + 1;
    }
    return out;
}
} // namespace

std::string XStore::UnindexedColumnNotice(std::string_view op, const std::string &db_name, const std::string &db_base_path,
                                           const XStore_Proto::ConfigStruct &cfg, unsigned long long start_epoch,
                                           unsigned long long end_epoch, int32_t column_idx) {
    if (column_idx < 0 || static_cast<uint32_t>(column_idx) >= EffectiveTrackedColumnCount(cfg)) return "";

    // While no year of any DB is live-widened (the common case) this load and its branch are all this adds.
    if (__builtin_expect(g_years_rebuilding.load(std::memory_order_relaxed) != 0, 0)) {
        std::string notices = LiveRebuildNotices(op, db_name, db_base_path, cfg, start_epoch, end_epoch, column_idx);
        if (!notices.empty()) return notices;
    }

    // Every index file is created at least ZONE_MAP_MAX_TRACKED_COLUMNS wide
    // (EffectiveTrackedColumnCount's floor), so a column below it is always
    // indexed: answer without any I/O on the common path. This is the path
    // every ordinary low-column-index query takes, so it must stay a single
    // integer comparison -- no syscalls, no allocation.
    if (column_idx < static_cast<int32_t>(XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS)) return "";

    unsigned long long iter_epoch = start_epoch;
    while (iter_epoch <= end_epoch) {
        TimeStruct curr_ts(static_cast<std::time_t>(iter_epoch), cfg.timeresolution());
        unsigned long long end_of_current_year = std::min(end_epoch, static_cast<unsigned long long>(curr_ts.last_epoch));

        // Cheapest source of truth first: if this (db, year) is already a
        // warm ZoneMapFile in the process-wide registry (the overwhelmingly
        // common case -- the same query path's own zone-map pushdown either
        // just opened it or already had it warm), PeekOpenZoneMapWidth
        // answers from that live instance with one mutex + one map lookup,
        // no open()/pread() at all. Only a genuinely cold (db, year) falls
        // through to the header pread below.
        bool rebuilding = false;
        std::optional<uint32_t> warm_width = PeekOpenZoneMapWidth(db_base_path, curr_ts.year, &rebuilding);
        if (warm_width.has_value()) {
            // During a rebuild the object already reports the new width, but blocks past the frontier are scanned.
            if (rebuilding) return RebuildingColumnNoticeText(op, db_name, column_idx, curr_ts.year);
            if (static_cast<uint32_t>(column_idx) >= *warm_width) {
                return UnindexedColumnNoticeText(op, db_name, column_idx, curr_ts.year, *warm_width);
            }
            iter_epoch = end_of_current_year + 1;
            continue;
        }

        std::string zmap_path = fmt::format("{}/{}.zmap", db_base_path, curr_ts.year);
        int fd = open(zmap_path.c_str(), O_RDONLY);
        if (fd >= 0) {
            ZoneMapFileHeader header{};
            ssize_t read_bytes = pread(fd, &header, sizeof(header), 0);
            close(fd);

            if (read_bytes == static_cast<ssize_t>(sizeof(header)) &&
                header.magic == ZoneMapFileHeader{}.magic &&
                header.version == ZONE_MAP_VERSION &&
                static_cast<uint32_t>(column_idx) >= header.tracked_column_count) {
                return UnindexedColumnNoticeText(op, db_name, column_idx, curr_ts.year, header.tracked_column_count);
            }
        }
        // Missing/corrupt/stale-version file -- no information either way
        // (a subsequent GetOrOpenZoneMap on this year will (re)build it at
        // the current width); just move on to the next year.

        iter_epoch = end_of_current_year + 1;
    }
    return "";
}

void XStore::RebuildOneYearZoneMap(const std::string &db_base_path, unsigned short year, uint16_t row_length, uint32_t tracked_column_count) {
    RebuildYearIndexes(db_base_path, year, row_length, tracked_column_count, /*need_zmap=*/true, /*need_catidx=*/false);
}

namespace {
// ZoneMapFile::Open refuses a file of at least a header's length whose magic is not a zone map's, and BeginRebuild keeps
// the header's rows_per_block. A file with no valid magic, or with a current-version header whose rows_per_block is not
// the one this row length gives (corrupt, e.g. 0), holds no usable index, so it is emptied (a file too short for a header
// reads as "rebuild" at every start, and Open gives it a fresh dirty header with the computed rows_per_block): the
// rebuild then opens it like a new file. (An older version is emptied by Open itself.)
void EmptyUnusableZoneMap(const std::string &zmap_path, uint16_t row_length) {
    const int fd = open(zmap_path.c_str(), O_RDWR);
    if (fd < 0) return;
    struct stat st{};
    ZoneMapFileHeader header{};
    if (fstat(fd, &st) == 0 && st.st_size >= static_cast<off_t>(sizeof(header))) {
        const bool readable = pread(fd, &header, sizeof(header), 0) == static_cast<ssize_t>(sizeof(header));
        const bool unusable = !readable || header.magic != ZoneMapFileHeader{}.magic ||
                              (header.version == ZONE_MAP_VERSION && header.rows_per_block != RowsPerBlock(row_length));
        if (unusable && ftruncate(fd, 0) != 0) {
            spdlog::error("[ZONE MAP REBUILD] [{}] - Cannot empty an unusable zone map", zmap_path);
        }
    }
    close(fd);
}
} // namespace

struct XStore::YearRebuild {
    unsigned short year = 0;
    bool need_zmap = false;
    bool need_catidx = false;
    bool active = false; // begun: its groups are scanned and FinishYearRebuild runs
    ZoneMapFile::Pin zmap;
    CatIdxFile::Pin catidx;
    int fd = -1;            // the year's own descriptor: fdatasync at the end only; groups read through their own
    std::string data_path;
    uint64_t total_rows = 0;
    uint64_t chunk_rows = 0;
    uint64_t group_rows = 0;
    size_t group_count = 0;
    // Shared by the workers on this year. scan_failed: a chunk could not be read (or a group threw); no worker
    // starts another chunk of this year. *_written: that file's fold write failed; nobody folds into it any more.
    std::atomic<bool> scan_failed{false};
    // Rows covered by the chunks scanned so far (holes included), for the progress display only.
    std::atomic<uint64_t> rows_done{0};
    std::atomic<bool> zmap_written{true};
    std::atomic<bool> catidx_written{true};
    // Contiguous watermark of each file, under `mutex` (see RebuildWatermark). Frontiers taken from it are monotone and
    // never pass the first row a still-running (or stopped) group has not folded: RebuildFoldRows skips rows below the
    // frontier as already folded, so a frontier past a hole would drop that hole's rows for good.
    std::mutex mutex;
    RebuildWatermark zmap_fold;
    RebuildWatermark catidx_fold;
    std::exception_ptr failure; // the first exception of this year; under `mutex` while workers run

    YearRebuild() = default;
    YearRebuild(const YearRebuild &) = delete;
    YearRebuild &operator=(const YearRebuild &) = delete;
    ~YearRebuild() {
        if (fd >= 0) close(fd);
    }

    [[nodiscard]] uint64_t GroupBegin(size_t g) const { return g * group_rows; }
    [[nodiscard]] uint64_t GroupEnd(size_t g) const { return std::min(total_rows, (g + 1) * group_rows); }
};

void XStore::BeginYearRebuild(const std::string &db_base_path, uint16_t row_length, uint32_t tracked_column_count,
                              uint64_t group_bytes, YearRebuild &y) {
    const unsigned short year = y.year;
    const std::string data_path = fmt::format("{}/{}.XSTORE", db_base_path, year);
    if ((!y.need_zmap && !y.need_catidx) || !std::filesystem::exists(data_path)) return;
    const std::string registry_key = fmt::format("{}/{}", db_base_path, year);
    // A live widen left stopped or failed in this process ends here: this rebuild replaces its state.
    ForgetRebuildStatusMatching([&](const LiveRebuildStatus &n) { return n.year == year && n.db_base_path == db_base_path; });

    // Startup only. A registry instance may already exist for this year (WAL replay opened it, possibly at another
    // width; a test may have deleted its file under it): drop it, so the pin below opens the file that is on disk and
    // the rebuilt index lands there. Ignores pins: never run while a builder or another pin holder is alive.
    // The pins are held from before BeginRebuild until after MarkClean. If BeginRebuild fails, the file (the registry
    // entry just opened) is put in the untrusted rebuild state instead and its pin dropped: it stays in the registry
    // (a rebuilding entry is never evicted) and is scanned, never trusted, until the next start rebuilds it.
    if (y.need_zmap) {
        {
            std::lock_guard<std::mutex> registry_lock(zone_map_registry_mutex_);
            zone_map_registry_.erase(registry_key);
            zone_map_registry_lru_.erase(
                std::remove(zone_map_registry_lru_.begin(), zone_map_registry_lru_.end(), registry_key),
                zone_map_registry_lru_.end());
        }
        EmptyUnusableZoneMap(fmt::format("{}/{}.zmap", db_base_path, year), row_length);
        y.zmap = PinZoneMap(db_base_path, year, row_length, tracked_column_count);
        // BeginRebuild empties the file whatever its width: an old file WIDER than tracked_column_count (a first
        // insert created it at the global default before MaybeGrowColumnCount persisted a narrower count) must not
        // keep its stats, or the scan would fold every row a second time on top of them.
        if (!y.zmap->BeginRebuild(tracked_column_count)) {
            spdlog::error("[ZONE MAP REBUILD] [{}/{}.zmap] - Cannot start the rebuild; the index is not trusted until "
                          "the next start", db_base_path, year);
            y.zmap->EnterUntrustedRebuildState();
            y.zmap = ZoneMapFile::Pin();
        }
    }
    if (y.need_catidx) {
        {
            std::lock_guard<std::mutex> registry_lock(catidx_registry_mutex_);
            catidx_registry_.erase(registry_key);
            catidx_registry_lru_.erase(
                std::remove(catidx_registry_lru_.begin(), catidx_registry_lru_.end(), registry_key),
                catidx_registry_lru_.end());
        }
        y.catidx = PinCatIdx(db_base_path, year, row_length, tracked_column_count);
        if (!y.catidx->BeginRebuild(tracked_column_count)) { // also drops the bitmap section; compaction rebuilds it
            spdlog::error("[CATIDX REBUILD] [{}/{}.catidx] - Cannot start the rebuild; the index is not trusted until "
                          "the next start", db_base_path, year);
            y.catidx->EnterUntrustedRebuildState();
            y.catidx = CatIdxFile::Pin();
        }
    }
    if (y.zmap.get() == nullptr && y.catidx.get() == nullptr) return;

    y.data_path = data_path;
    y.fd = open(data_path.c_str(), O_RDONLY);
    if (y.fd < 0) {
        // Both files stay in rebuild at F = 0 (every block scanned, not trusted) and dirty: the next start retries.
        spdlog::error("[INDEX REBUILD] [{}] - Unable to open for rebuild scan", data_path);
        y.zmap = ZoneMapFile::Pin();
        y.catidx = CatIdxFile::Pin();
        return;
    }
    struct stat st{};
    if (fstat(y.fd, &st) != 0 || row_length == 0) {
        // Same as a failed open: never scan "0 rows" and mark an empty index clean.
        spdlog::error("[INDEX REBUILD] [{}] - Cannot size the year file for the rebuild scan", data_path);
        close(y.fd);
        y.fd = -1;
        y.zmap = ZoneMapFile::Pin();
        y.catidx = CatIdxFile::Pin();
        return;
    }
    y.total_rows = static_cast<uint64_t>(st.st_size) / row_length;

    // Chunks of whole zone-map blocks, groups of whole chunks: two groups never share a block. Startup needs no
    // FileRangeLock: no request can run before InitDB returns. (The live builder calls the same FoldChunk under the
    // READ range lock of each chunk and advances the frontiers before unlocking.)
    const uint64_t block_bytes = RowsPerBlock(row_length) * row_length;
    y.chunk_rows = std::max<uint64_t>(1, XStoreTunables::REBUILD_CHUNK_BYTES / block_bytes) * RowsPerBlock(row_length);
    const uint64_t chunk_bytes = y.chunk_rows * row_length;
    y.group_rows = std::max<uint64_t>(1, (group_bytes + chunk_bytes - 1) / chunk_bytes) * y.chunk_rows;
    y.group_count = static_cast<size_t>((y.total_rows + y.group_rows - 1) / y.group_rows);
    y.zmap_fold.Reset(y.group_rows, y.total_rows);
    y.catidx_fold.Reset(y.group_rows, y.total_rows);
    y.zmap_written.store(y.zmap.get() != nullptr, std::memory_order_relaxed);
    y.catidx_written.store(y.catidx.get() != nullptr, std::memory_order_relaxed);
    y.active = true;
}

void XStore::RunRebuildGroup(YearRebuild &y, size_t g, uint16_t row_length, uint32_t tracked_column_count,
                             RebuildScratch &scratch) {
    const uint64_t group_end = y.GroupEnd(g);
    // Each group reads through a descriptor of its own (own readahead state) with a sequential hint. Measured on Mystic
    // (cold cache, 11 years): this made NO difference (1026 s without, 1017 s with, year-major order); what fixed the
    // slowdown was the round-robin group order below. Kept because the 441 s result was measured with it; an A/B
    // without it would show whether it can go.
    struct GroupFd {
        int fd;
        ~GroupFd() {
            if (fd >= 0) close(fd);
        }
    } data{open(y.data_path.c_str(), O_RDONLY | O_CLOEXEC)};
    if (data.fd < 0) {
        spdlog::error("[INDEX REBUILD] [{}] - Unable to open for a rebuild group", y.data_path);
        y.scan_failed.store(true, std::memory_order_relaxed);
        return;
    }
    static_cast<void>(posix_fadvise(data.fd, static_cast<off_t>(y.GroupBegin(g) * row_length),
                                    static_cast<off_t>((group_end - y.GroupBegin(g)) * row_length), POSIX_FADV_SEQUENTIAL));
    bool zmap_ok = true;
    bool catidx_ok = true;
    for (uint64_t begin = y.GroupBegin(g); begin < group_end; begin += y.chunk_rows) {
        // A file whose write failed (in any group) stops here, behind its frontier; the other one carries on.
        zmap_ok = zmap_ok && y.zmap_written.load(std::memory_order_relaxed);
        catidx_ok = catidx_ok && y.catidx_written.load(std::memory_order_relaxed);
        if ((!zmap_ok && !catidx_ok) || y.scan_failed.load(std::memory_order_relaxed)) return;
        const uint64_t end = std::min(group_end, begin + y.chunk_rows);
        const FoldResult result = FoldChunk(data.fd, begin, end, row_length, tracked_column_count,
                                            zmap_ok ? y.zmap.get() : nullptr, catidx_ok ? y.catidx.get() : nullptr,
                                            scratch);
        if (!result.scan_complete) {
            // Nothing of this chunk was folded: the frontiers stay below it, its blocks are never trusted this session.
            y.scan_failed.store(true, std::memory_order_relaxed);
            return;
        }
        y.rows_done.fetch_add(end - begin, std::memory_order_relaxed);
        if (zmap_ok && !result.zmap_written) {
            zmap_ok = false;
            y.zmap_written.store(false, std::memory_order_relaxed);
        }
        if (catidx_ok && !result.catidx_written) {
            catidx_ok = false;
            y.catidx_written.store(false, std::memory_order_relaxed);
        }
        std::lock_guard<std::mutex> lock(y.mutex);
        if (zmap_ok) y.zmap->AdvanceFrontier(y.zmap_fold.MarkFolded(begin, end));
        if (catidx_ok) y.catidx->AdvanceFrontier(y.catidx_fold.MarkFolded(begin, end));
    }
}

void XStore::FinishYearRebuild(const std::string &db_base_path, uint32_t tracked_column_count, YearRebuild &y) {
    if (!y.active) return;
    const unsigned short year = y.year;
    if (y.failure) {
        // A group threw: like the old single-loop rebuild, nothing is finished, both files stay dirty in rebuild.
        y.zmap = ZoneMapFile::Pin();
        y.catidx = CatIdxFile::Pin();
        return;
    }
    const bool scan_complete = !y.scan_failed.load(std::memory_order_relaxed);
    const bool zmap_written = y.zmap.get() != nullptr && y.zmap_written.load(std::memory_order_relaxed);
    const bool catidx_written = y.catidx.get() != nullptr && y.catidx_written.load(std::memory_order_relaxed);

    // Durability A (DD4): FinishRebuild (the dirty header is durable), then the data file, then MarkClean -- the index
    // must never claim "clean" while the rows it summarizes could still be lost. Every failure leaves it dirty.
    const bool zmap_finished = scan_complete && zmap_written && y.zmap->FinishRebuild();
    const bool catidx_finished = scan_complete && catidx_written && y.catidx->FinishRebuild();
    const bool data_synced = (zmap_finished || catidx_finished) && fdatasync(y.fd) == 0;
    close(y.fd);
    y.fd = -1;

    if (y.zmap.get() != nullptr) {
        if (!scan_complete) {
            spdlog::error("[ZONE MAP REBUILD] [{}/{}.XSTORE] - Incomplete scan; leaving zone map dirty", db_base_path, year);
        } else if (!zmap_written) {
            spdlog::error("[ZONE MAP REBUILD] [{}/{}.zmap] - Index write failed; leaving zone map dirty", db_base_path, year);
        } else if (!zmap_finished) {
            spdlog::error("[ZONE MAP REBUILD] [{}/{}.zmap] - No durable header; leaving zone map dirty", db_base_path, year);
        } else if (!data_synced) {
            spdlog::error("[ZONE MAP REBUILD] [{}/{}.XSTORE] - fdatasync of data file failed; leaving zone map dirty", db_base_path, year);
        } else if (!y.zmap->MarkClean()) {
            spdlog::error("[ZONE MAP REBUILD] [{}/{}.zmap] - MarkClean failed; leaving zone map dirty", db_base_path, year);
        }
        spdlog::info("[ZONE MAP REBUILD] [{}/{}.XSTORE] - Rebuilt {} rows", db_base_path, year, y.total_rows);
    }
    if (y.catidx.get() != nullptr) {
        if (!scan_complete) {
            spdlog::error("[CATIDX REBUILD] [{}/{}.XSTORE] - Incomplete scan; leaving catidx dirty", db_base_path, year);
        } else if (!catidx_written) {
            spdlog::error("[CATIDX REBUILD] [{}/{}.catidx] - Index write failed; leaving catidx dirty", db_base_path, year);
        } else if (!catidx_finished) {
            spdlog::error("[CATIDX REBUILD] [{}/{}.catidx] - No durable header; leaving catidx dirty", db_base_path, year);
        } else if (!data_synced) {
            spdlog::error("[CATIDX REBUILD] [{}/{}.XSTORE] - fdatasync of data file failed; leaving catidx dirty", db_base_path, year);
        } else if (!y.catidx->MarkClean()) {
            spdlog::error("[CATIDX REBUILD] [{}/{}.catidx] - MarkClean failed; leaving catidx dirty", db_base_path, year);
        }
        spdlog::info("[CATIDX REBUILD] [{}/{}.XSTORE] - Rebuilt bloom for {} rows at tracked_column_count {}",
                     db_base_path, year, y.total_rows, tracked_column_count);
    }
    y.zmap = ZoneMapFile::Pin();
    y.catidx = CatIdxFile::Pin();
}

std::vector<std::exception_ptr> XStore::RebuildYearsIndexes(const std::string &db_base_path,
                                                            const std::vector<RebuildYearTask> &tasks,
                                                            uint16_t row_length, uint32_t tracked_column_count,
                                                            unsigned thread_count, uint64_t group_bytes,
                                                            const rebuildprogress::Options &progress) {
    std::vector<YearRebuild> years(tasks.size());
    struct GroupRef {
        size_t year;
        size_t group;
    };
    std::vector<GroupRef> groups;
    size_t max_groups = 0;
    for (size_t i = 0; i < tasks.size(); ++i) {
        YearRebuild &y = years[i];
        y.year = tasks[i].year;
        y.need_zmap = tasks[i].zmap;
        y.need_catidx = tasks[i].catidx;
        try {
            BeginYearRebuild(db_base_path, row_length, tracked_column_count, group_bytes, y);
        } catch (...) {
            y.failure = std::current_exception();
            y.active = false;
            y.zmap = ZoneMapFile::Pin();
            y.catidx = CatIdxFile::Pin();
            continue;
        }
        if (!y.active) continue;
        max_groups = std::max(max_groups, y.group_count);
    }
    // Group-major round robin (group 0 of every year, then group 1 of every year, ...), NOT year-major: the fold into a
    // year's index files is serial under that file's mutex (about 16% of the work, Tech Reference 4.14), so threads
    // spread over many files scale and threads piled onto one file do not. Year-major sent all threads to the first
    // year or two and measured 1017 s on Mystic against 670 s for the old one-thread-per-year loop (same data, cold).
    for (size_t g = 0; g < max_groups; ++g) {
        for (size_t i = 0; i < years.size(); ++i) {
            if (years[i].active && g < years[i].group_count) groups.push_back({i, g});
        }
    }

    // An exception must not escape a thread (std::terminate): it is kept as its year's failure, that year stops, every
    // other year carries on.
    std::atomic<size_t> next{0};
    const auto work = [&] {
        RebuildScratch scratch;
        for (size_t t = next.fetch_add(1, std::memory_order_relaxed); t < groups.size();
             t = next.fetch_add(1, std::memory_order_relaxed)) {
            YearRebuild &y = years[groups[t].year];
            if (y.scan_failed.load(std::memory_order_relaxed)) continue;
            try {
                RunRebuildGroup(y, groups[t].group, row_length, tracked_column_count, scratch);
            } catch (...) {
                y.scan_failed.store(true, std::memory_order_relaxed);
                std::lock_guard<std::mutex> lock(y.mutex);
                if (!y.failure) y.failure = std::current_exception();
            }
        }
    };
    // Progress display: reads only the years' atomics; joined (reset) before any year finishes, and by its destructor
    // on every other path.
    std::optional<rebuildprogress::Reporter> reporter;
    if (!groups.empty()) {
        reporter.emplace(std::filesystem::path(db_base_path).filename().string(), [&years] {
            rebuildprogress::Sample sample;
            for (const YearRebuild &y : years) {
                if (!y.active) continue;
                const uint64_t done = std::min(y.rows_done.load(std::memory_order_relaxed), y.total_rows);
                sample.rows_done += done;
                sample.rows_total += y.total_rows;
                ++sample.years_total;
                if (done >= y.total_rows) ++sample.years_done;
            }
            return sample;
        }, progress);
    }
    const size_t workers = std::min<size_t>(std::max(1u, thread_count), groups.size());
    std::vector<std::thread> threads;
    if (workers > 1) {
        threads.reserve(workers - 1);
        try {
            while (threads.size() + 1 < workers) threads.emplace_back(work);
        } catch (const std::exception &e) { // system_error (no more threads) or bad_alloc: carry on with those started
            spdlog::warn("[INDEX REBUILD] [{}] - Started {} of {} rebuild threads: {}", db_base_path,
                         threads.size() + 1, workers, e.what());
        }
    }
    work();
    for (std::thread &t : threads) t.join();
    reporter.reset();

    std::vector<std::exception_ptr> failures(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        try {
            FinishYearRebuild(db_base_path, tracked_column_count, years[i]);
        } catch (...) {
            if (!years[i].failure) years[i].failure = std::current_exception();
        }
        failures[i] = years[i].failure;
    }
    return failures;
}

void XStore::RebuildYearIndexes(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                uint32_t tracked_column_count, bool need_zmap, bool need_catidx) {
    const std::vector<std::exception_ptr> failures =
        RebuildYearsIndexes(db_base_path, {{year, need_zmap, need_catidx}}, row_length, tracked_column_count, 1);
    if (failures.front()) std::rethrow_exception(failures.front());
}

XStore::FoldResult XStore::FoldChunk(int data_fd, uint64_t chunk_begin, uint64_t chunk_end, uint16_t row_length,
                                     uint32_t tracked_column_count, ZoneMapFile *zmap, CatIdxFile *catidx,
                                     RebuildScratch &scratch) {
    FoldResult result;
    scratch.zmap.Clear();
    scratch.zmap.pad_to = tracked_column_count;
    scratch.catidx.Clear();
    ZoneMapFoldBatch *zmap_batch = zmap != nullptr ? &scratch.zmap : nullptr;
    CatIdxFoldBatch *catidx_batch = catidx != nullptr ? &scratch.catidx : nullptr;
    result.scan_complete = ForEachDataRow(data_fd, chunk_end, row_length, [&](uint64_t row_index, const char *row) {
        if (row[0] == 0) return; // never-written sparse hole -- same fast skip ProcessFileRangeAccumulate uses
        AppendRebuildObservations(row_index, row, row_length, tracked_column_count, zmap_batch, catidx_batch, scratch);
    }, chunk_begin, &scratch.scan_buffer);
    if (!result.scan_complete) return result;
    if (zmap != nullptr) result.zmap_written = zmap->RebuildFoldRows(scratch.zmap);
    if (catidx != nullptr) result.catidx_written = catidx->RebuildFoldRows(scratch.catidx);
    return result;
}

XStore::LiveWidenResult XStore::LiveWidenOneYear(const std::string &db_base_path, unsigned short year,
                                                 uint16_t row_length, uint32_t tracked_column_count, bool widen_zmap,
                                                 bool widen_catidx, const std::atomic<bool> *stop,
                                                 LiveWidenStats *stats, unsigned workers, uint64_t group_bytes,
                                                 livewiden::LiveWidenGovernor *governor,
                                                 livewiden::LiveWidenLoadSource *load) try {
    using Clock = std::chrono::steady_clock;
    LiveWidenStats local_stats;
    LiveWidenStats &st = stats != nullptr ? *stats : local_stats;
    st = LiveWidenStats{};
    // The writers' lock key, byte for byte (ApplyWalRecord and the deletes lock CachedYearFilePath): any other
    // spelling of the path would be a different FileRangeLock and protect nothing.
    const std::string data_path = CachedYearFilePath(db_base_path, year);
    const auto stopped = [stop] { return stop != nullptr && stop->load(std::memory_order_relaxed); };
    std::error_code exists_ec;
    if ((!widen_zmap && !widen_catidx) || row_length == 0 || !std::filesystem::exists(data_path, exists_ec)) {
        return LiveWidenResult::kNothingToDo;
    }

    // Pins before BeginRebuild, held until after MarkClean (they live in this frame): the registry never evicts these
    // objects while the builder uses them. Registry entries are never erased here: in-flight queries hold references
    // to these same objects (BeginRebuild works in place).
    ZoneMapFile::Pin zmap;
    CatIdxFile::Pin catidx;
    if (widen_zmap) zmap = PinZoneMap(db_base_path, year, row_length, tracked_column_count);
    if (widen_catidx) catidx = PinCatIdx(db_base_path, year, row_length, tracked_column_count);
    // BeginLiveRebuild checks and begins in one step under the file mutex. A file another builder already rebuilds is
    // left alone (nothing of it touched: the other builder owns it); resuming a rebuild is the trigger's business. A
    // refused begin changes nothing: that file's old, narrower index stays in place and trusted (unlike startup, no
    // EnterUntrustedRebuildState; BeginRebuild marks the old index dirty, durably, before it truncates anything). The
    // other file still widens (kPartial).
    bool refused = false;
    bool busy = false;
    const auto begin = [&](auto &pin, const char *ext, bool &began, bool &file_busy) {
        if (pin.get() == nullptr) return;
        const BeginRebuildResult r = pin->BeginLiveRebuild(tracked_column_count);
        began = r == BeginRebuildResult::kStarted;
        file_busy = r == BeginRebuildResult::kAlreadyRebuilding;
        if (began) return;
        if (file_busy) {
            busy = true;
        } else {
            spdlog::error("[LIVE WIDEN] [{}/{}.{}] - Cannot start the rebuild; the old index stays in use", db_base_path, year, ext);
            refused = true;
        }
        pin = std::remove_reference_t<decltype(pin)>();
    };
    begin(zmap, "zmap", st.zmap_began, st.zmap_busy);
    if (busy) return LiveWidenResult::kAlreadyRebuilding; // one builder per year: the catidx is not touched either
    begin(catidx, "catidx", st.catidx_began, st.catidx_busy); // busy here: a catidx-only builder owns it; finish the zone map (kPartial)
    if (zmap.get() == nullptr && catidx.get() == nullptr) {
        return busy ? LiveWidenResult::kAlreadyRebuilding : LiveWidenResult::kFailed;
    }
    refused = refused || busy;

    // The query notice's status of this year (Tech Reference 4.18 "Rebuild notice"): this builder flags only the files
    // it began; each stays flagged until it finishes here (an early return, a stop or a failure leaves it flagged at
    // its last progress). The year is counted while either file is flagged.
    LiveRebuildStatus &rebuild_status = RebuildStatusFor(db_base_path, year);
    BeginRebuildFiles(rebuild_status, zmap.get() != nullptr, catidx.get() != nullptr);

    // From here every early return leaves the files that began in rebuild at the frontier they reached: dirty on
    // disk, blocks past F scanned (never trusted), MarkClean refusing, unevictable; the next start rebuilds them.
    struct FdCloser {
        int fd;
        ~FdCloser() { if (fd >= 0) close(fd); }
    } data{open(data_path.c_str(), O_RDONLY | O_CLOEXEC)};
    struct stat data_st{};
    if (data.fd < 0 || fstat(data.fd, &data_st) != 0) {
        spdlog::error("[LIVE WIDEN] [{}] - Cannot open the year file for the scan; the index stays in rebuild until "
                      "the next start", data_path);
        return LiveWidenResult::kFailed;
    }
    // Read ONCE: the year file is preallocated to its full size. If it is cut or replaced meanwhile, the scan fails
    // (short read) or the identity check below stops the build.
    const uint64_t total_rows = static_cast<uint64_t>(data_st.st_size) / row_length;
    rebuild_status.total_rows.store(total_rows, std::memory_order_relaxed);
    const uint64_t row_len = row_length;
    const uint64_t max_data_rows = std::max<uint64_t>(XStoreTunables::LIVE_WIDEN_MIN_CHUNK_ROWS,
                                                      XStoreTunables::LIVE_WIDEN_MAX_CHUNK_BYTES / row_len);
    // Groups of whole zone-map blocks (two groups never share a block, so folds of different groups touch different
    // blocks, each in row order: the bytes equal the sequential build's). One worker: one group, the whole year, which
    // is exactly the sequential builder (same chunks, same frontier sequence, same stats).
    // A governor decides how many of its MaxWorkers() fold at once (Tech Reference 4.18 "Governor"): all of them are
    // started and the year is always cut into groups, even if only one may run now.
    unsigned want_workers = governor != nullptr ? std::max(1u, governor->MaxWorkers()) : std::max(1u, workers);
    // The groups are block-aligned only if the files' own block size is the one computed here (always so today: a file
    // with another block size is rebuilt at startup). If it ever differs, a block could span two groups and the writer
    // checks (rows owned by writers form a prefix of every block) would no longer hold: use one group instead.
    if (want_workers > 1 && ((zmap.get() != nullptr && zmap->RowsPerBlockValue() != RowsPerBlock(row_length)) ||
                             (catidx.get() != nullptr && catidx->RowsPerBlockValue() != RowsPerBlock(row_length)))) {
        spdlog::warn("[LIVE WIDEN] [{}] - Index block size differs from the computed one; widening with one worker{}",
                     data_path, governor != nullptr ? " (the load governor is not consulted)" : "");
        want_workers = 1;
        governor = nullptr;
    }
    if (governor != nullptr) {
        rebuild_status.allowed.store(governor->Allowed(), std::memory_order_relaxed);
        rebuild_status.reason.store(governor->Reason(), std::memory_order_relaxed);
        rebuild_status.max_workers.store(governor->MaxWorkers(), std::memory_order_relaxed);
        if (zmap.get() != nullptr) rebuild_status.zmap_governed.store(true, std::memory_order_relaxed);
        if (catidx.get() != nullptr) rebuild_status.catidx_governed.store(true, std::memory_order_relaxed);
    }
    const auto publish_governor = [&rebuild_status, governor] {
        if (governor == nullptr) return;
        rebuild_status.allowed.store(governor->Allowed(), std::memory_order_relaxed);
        rebuild_status.reason.store(governor->Reason(), std::memory_order_relaxed);
    };
    // Several workers publish watermarks they read at different moments: keep the largest.
    const auto store_max = [](std::atomic<uint64_t> &a, uint64_t v) {
        for (uint64_t seen = a.load(std::memory_order_relaxed);
             v > seen && !a.compare_exchange_weak(seen, v, std::memory_order_relaxed);) {
        }
    };
    const uint64_t block_bytes = RowsPerBlock(row_length) * row_len;
    const uint64_t group_rows = want_workers == 1 && governor == nullptr
                                    ? std::max<uint64_t>(1, total_rows)
                                    : std::max<uint64_t>(1, (group_bytes + block_bytes - 1) / block_bytes) * RowsPerBlock(row_length);
    const uint64_t group_count = (total_rows + group_rows - 1) / group_rows;
    // Before any fold. From here the frontier moves only through MarkFolded, which also records the folded group
    // prefixes the writers check (a group above F may be folded already: see ZoneMapFile::kNoRebuild).
    if (zmap.get() != nullptr) zmap->SetLiveFoldGroups(group_rows, total_rows);
    if (catidx.get() != nullptr) catidx->SetLiveFoldGroups(group_rows, total_rows);

    std::atomic<uint64_t> next_group{0};
    std::atomic<bool> zmap_ok{zmap.get() != nullptr};
    std::atomic<bool> catidx_ok{catidx.get() != nullptr};
    std::atomic<bool> failed{false};
    std::atomic<bool> was_stopped{false};
    std::atomic<uint64_t> watermark{0}; // for the log only
    const Clock::time_point started = Clock::now();
    const livewiden::LiveWidenGovernor::Snapshot gov_before =
        governor != nullptr ? governor->GetSnapshot() : livewiden::LiveWidenGovernor::Snapshot{};

    // One governed group turn: from a granted Permit to the end of that group, whatever way it ends. Reports the
    // worker's own CPU for the group (the governor subtracts it from the machine's busy CPU), then releases.
    struct GroupTurn {
        livewiden::LiveWidenGovernor *gov;
        unsigned index;
        uint64_t cpu0;
        static uint64_t ThreadCpuNs() noexcept {
            timespec ts{};
            if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) return 0;
            return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ull + static_cast<uint64_t>(ts.tv_nsec);
        }
        GroupTurn(livewiden::LiveWidenGovernor *g, unsigned i) noexcept : gov(g), index(i), cpu0(g ? ThreadCpuNs() : 0) {}
        ~GroupTurn() {
            if (gov == nullptr) return;
            const uint64_t cpu1 = ThreadCpuNs();
            if (cpu1 > cpu0) gov->AddBuilderCpu(cpu1 - cpu0);
            gov->Release(index);
        }
        GroupTurn(const GroupTurn &) = delete;
        GroupTurn &operator=(const GroupTurn &) = delete;
    };

    // One worker: claims groups in order and walks each chunk by chunk. Never throws (an exception is a failure).
    const auto work = [&](LiveWidenStats &ws, unsigned index) noexcept {
        try {
            uint64_t data_rows = std::clamp<uint64_t>(XStoreTunables::LIVE_WIDEN_INITIAL_CHUNK_ROWS,
                                                      XStoreTunables::LIVE_WIDEN_MIN_CHUNK_ROWS, max_data_rows);
            uint64_t span_rows = data_rows; // rows locked per attempt (>= data_rows after a hole)
            uint64_t pause_us = XStoreTunables::LIVE_WIDEN_PAUSE_US;
            uint64_t consecutive_timeouts = 0;
            bool timeout_warned = false;
            RebuildScratch scratch; // per worker, never shared
            const auto halt = [&] {
                if (failed.load(std::memory_order_relaxed) || was_stopped.load(std::memory_order_relaxed)) return true;
                if (!stopped()) return false;
                was_stopped.store(true, std::memory_order_relaxed);
                return true;
            };
            const auto fail = [&] { failed.store(true, std::memory_order_relaxed); };
            // Governed: waits for a Permit between groups (never inside one: a parked worker owns no group, so the
            // watermark never waits for it). False: stopped, failed, or no group left. Permit is called every slice
            // even while parked, which keeps the governor evaluating when every worker is parked (floor 0).
            const auto permitted = [&]() -> bool {
                for (;;) {
                    if (next_group.load(std::memory_order_relaxed) >= group_count) return false;
                    const bool granted = governor->Permit(index);
                    publish_governor();
                    if (granted) return true;
                    if (halt()) return false;
                    const Clock::time_point p0 = Clock::now();
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                    ws.governor_parked_s += std::chrono::duration<double>(Clock::now() - p0).count();
                }
            };
            for (;;) {
                if (governor != nullptr && !permitted()) return;
                const GroupTurn turn(governor, index); // releases on every exit from this group
                const uint64_t g = next_group.fetch_add(1, std::memory_order_relaxed);
                if (g >= group_count) return;
                uint64_t pos = g * group_rows; // this group's folded end: rows below it are folded
                const uint64_t end = std::min(total_rows, pos + group_rows);
                while (pos < end) {
                    if (halt()) return;
                    struct stat now_st{};
                    if (stat(data_path.c_str(), &now_st) != 0 || now_st.st_ino != data_st.st_ino ||
                        now_st.st_dev != data_st.st_dev) {
                        spdlog::error("[LIVE WIDEN] [{}] - The year file was removed or replaced during the build; stopping",
                                      data_path);
                        return fail();
                    }
                    const uint64_t lock_end = pos + std::min(span_rows, end - pos);
                    bool locked = false;
                    bool hole_only = false;
                    bool tail_empty = false;
                    uint64_t hold_us = 0;
                    FileRangeLock::WriterYield yield;
                    {
                        // Yields to waiting writers (at most LIVE_WIDEN_YIELD_MAX_MS): see the tunable.
                        TimedFileRangeLockGuard lock(data_path, pos * row_len, (lock_end - pos) * row_len,
                                                     std::chrono::milliseconds(XStoreTunables::LIVE_WIDEN_LOCK_WAIT_MS),
                                                     std::chrono::milliseconds(XStoreTunables::LIVE_WIDEN_YIELD_MAX_MS),
                                                     yield);
                        if (yield.yielded) {
                            ++ws.writer_yields;
                            ws.writer_yield_us += yield.us;
                        }
                        if (lock.owns_lock()) {
                            locked = true;
                            const Clock::time_point t0 = Clock::now();
                            // Everything about rows [pos, lock_end) is decided under this lock: where data starts (a
                            // hole seen without the lock may be written the next instant, and writers skip the index
                            // for unfolded rows >= F), the reads, the fold into both files and MarkFolded. Nothing
                            // outside [pos, lock_end) is folded or marked.
                            uint64_t first_data_row = pos;
                            const off_t data_off = lseek(data.fd, static_cast<off_t>(pos * row_len), SEEK_DATA);
                            if (data_off < 0) {
                                if (errno == ENXIO) {
                                    hole_only = true; // no data from pos to the end of the file
                                    tail_empty = true;
                                } else if (errno != EINVAL) { // EINVAL: no SEEK_DATA support, scan everything
                                    spdlog::error("[LIVE WIDEN] [{}] - lseek failed ({}); the index stays in rebuild until "
                                                  "the next start", data_path, strerror(errno));
                                    return fail();
                                }
                            } else {
                                first_data_row = std::max(pos, static_cast<uint64_t>(data_off) / row_len);
                                hole_only = first_data_row >= lock_end;
                            }
                            uint64_t new_pos = lock_end;
                            // A file whose write failed (in any worker) stops here, behind its frontier; the other one
                            // carries on.
                            bool z = zmap_ok.load(std::memory_order_relaxed);
                            bool c = catidx_ok.load(std::memory_order_relaxed);
                            if (!hole_only) {
                                new_pos = std::min(lock_end, first_data_row + data_rows);
                                const FoldResult result = FoldChunk(data.fd, pos, new_pos, row_length, tracked_column_count,
                                                                    z ? zmap.get() : nullptr, c ? catidx.get() : nullptr,
                                                                    scratch);
                                if (!result.scan_complete) { // nothing of this chunk was folded: F stays below it
                                    spdlog::error("[LIVE WIDEN] [{}] - Cannot read rows [{}, {}); the index stays in rebuild "
                                                  "until the next start", data_path, pos, new_pos);
                                    return fail();
                                }
                                ws.rows_folded += z ? scratch.zmap.row_index.size() : scratch.catidx.row_index.size();
                                if (z && !result.zmap_written) {
                                    z = false;
                                    zmap_ok.store(false, std::memory_order_relaxed);
                                }
                                if (c && !result.catidx_written) {
                                    c = false;
                                    catidx_ok.store(false, std::memory_order_relaxed);
                                }
                                if (!zmap_ok.load(std::memory_order_relaxed) && !catidx_ok.load(std::memory_order_relaxed)) {
                                    spdlog::error("[LIVE WIDEN] [{}] - Index write failed; the index stays in rebuild until "
                                                  "the next start", data_path);
                                    return fail();
                                }
                            }
                            // After the fold, before the unlock: no writer can sit on a row that changes side here.
                            uint64_t w = 0;
                            if (z) {
                                w = zmap->MarkFolded(pos, new_pos);
                                store_max(rebuild_status.zmap_rows, w);
                            }
                            if (c) {
                                const uint64_t wc = catidx->MarkFolded(pos, new_pos);
                                store_max(rebuild_status.catidx_rows, wc);
                                w = std::max(w, wc);
                            }
                            store_max(watermark, w);
                            publish_governor();
                            pos = new_pos;
                            hold_us = static_cast<uint64_t>(
                                std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count());
                        }
                    }

                    if (locked) {
                        consecutive_timeouts = 0;
                        timeout_warned = false;
                        pause_us = XStoreTunables::LIVE_WIDEN_PAUSE_US;
                        ++ws.chunks;
                        ws.total_lock_hold_us += hold_us;
                        ws.max_lock_hold_us = std::max(ws.max_lock_hold_us, hold_us);
                        if (stats != nullptr) {
                            ws.lock_hold_us.push_back(static_cast<uint32_t>(std::min<uint64_t>(hold_us, UINT32_MAX)));
                        }
                        if (hole_only) {
                            ++ws.hole_chunks;
                            span_rows = tail_empty ? total_rows
                                                   : std::min(XStoreTunables::LIVE_WIDEN_MAX_HOLE_SPAN_ROWS, span_rows * 4);
                            span_rows = std::max(span_rows, data_rows);
                            continue; // the lock was held for microseconds: no pause owed
                        }
                        const double factor = std::clamp(static_cast<double>(XStoreTunables::LIVE_WIDEN_LOCK_HOLD_TARGET_US) /
                                                             static_cast<double>(std::max<uint64_t>(hold_us, 1)), 0.5, 2.0);
                        data_rows = std::clamp<uint64_t>(static_cast<uint64_t>(static_cast<double>(data_rows) * factor),
                                                         XStoreTunables::LIVE_WIDEN_MIN_CHUNK_ROWS, max_data_rows);
                        span_rows = data_rows;
                    } else {
                        // A group's folded part is one contiguous prefix: a busy chunk cannot be skipped, only shrunk
                        // and retried after a longer pause (other workers carry on with their own groups). With a
                        // one-row minimum the worker never needs two neighbouring busy rows free at once.
                        ++ws.lock_timeouts;
                        ++consecutive_timeouts;
                        ws.max_consecutive_timeouts = std::max(ws.max_consecutive_timeouts, consecutive_timeouts);
                        span_rows = std::max<uint64_t>(XStoreTunables::LIVE_WIDEN_MIN_CHUNK_ROWS, span_rows / 2);
                        data_rows = std::min(data_rows, span_rows);
                        pause_us = std::min<uint64_t>(XStoreTunables::LIVE_WIDEN_MAX_PAUSE_US, pause_us * 2);
                        if (span_rows == XStoreTunables::LIVE_WIDEN_MIN_CHUNK_ROWS && !timeout_warned &&
                            consecutive_timeouts >= XStoreTunables::LIVE_WIDEN_TIMEOUT_WARN_AFTER) {
                            spdlog::warn("[LIVE WIDEN] [{}] - {} lock waits in a row timed out at row {}; still retrying",
                                         data_path, consecutive_timeouts, pos);
                            timeout_warned = true;
                        }
                    }
                    if (pos >= end) break;
                    if (halt()) return;
                    std::this_thread::sleep_for(std::chrono::microseconds(pause_us));
                }
            }
        } catch (const std::exception &e) {
            spdlog::error("[LIVE WIDEN] [{}] - A build worker failed: {}", data_path, e.what());
            failed.store(true, std::memory_order_relaxed);
        } catch (...) {
            failed.store(true, std::memory_order_relaxed);
        }
    };

    // The calling (background index) thread is worker 0; workers - 1 dedicated short-lived threads join it, all joined
    // before this returns. A thread that cannot be started only means fewer workers.
    const size_t worker_count = static_cast<size_t>(std::min<uint64_t>(want_workers, std::max<uint64_t>(1, group_count)));
    std::vector<LiveWidenStats> per_worker(worker_count);
    std::vector<std::thread> threads;
    // RULE (G2 review, finding 1): pointing the sampler at this year's lock and RebaselineWriterCounters() are NOT
    // race-free against concurrent Permit calls (an evaluation in between would take one file's counters against the
    // other's baseline). So both happen here, on the calling thread, before any worker of this year starts; the
    // previous year's workers were all joined before its LiveWidenOneYear returned, so no Permit runs in between and
    // thread start/join orders everything. Never call either from a worker.
    struct YearSwitch {
        livewiden::LiveWidenLoadSource *load;
        ~YearSwitch() { if (load != nullptr) load->SetYear(nullptr, nullptr, 0); } // after the joins below
    } year_switch{governor != nullptr ? load : nullptr};
    if (governor != nullptr) {
        // A FileLockRegistry entry is never erased, so the pointer stays valid after this year.
        if (load != nullptr) load->SetYear(&FileLockRegistry::Get(data_path), &watermark, total_rows);
        governor->RebaselineWriterCounters();
    }
    try {
        threads.reserve(worker_count - 1);
        for (size_t i = 1; i < worker_count; ++i) threads.emplace_back(work, std::ref(per_worker[i]), static_cast<unsigned>(i));
    } catch (const std::exception &e) { // system_error (no more threads) or bad_alloc: carry on with those started
        spdlog::warn("[LIVE WIDEN] [{}] - Started {} of {} build workers: {}", data_path, threads.size() + 1,
                     worker_count, e.what());
    }
    work(per_worker[0], 0);
    for (std::thread &t : threads) t.join();
    // No governor holds this builder's files back any more (a stopped or failed file never says "slowed"). Two governed
    // builders on one year, one per file, would share allowed/max/reason (last writer wins); the trigger never makes
    // that: it skips a year with a file in rebuild.
    if (zmap.get() != nullptr) rebuild_status.zmap_governed.store(false, std::memory_order_relaxed);
    if (catidx.get() != nullptr) rebuild_status.catidx_governed.store(false, std::memory_order_relaxed);
    for (const LiveWidenStats &ws : per_worker) {
        st.chunks += ws.chunks;
        st.hole_chunks += ws.hole_chunks;
        st.rows_folded += ws.rows_folded;
        st.lock_timeouts += ws.lock_timeouts;
        st.max_consecutive_timeouts = std::max(st.max_consecutive_timeouts, ws.max_consecutive_timeouts);
        st.total_lock_hold_us += ws.total_lock_hold_us;
        st.max_lock_hold_us = std::max(st.max_lock_hold_us, ws.max_lock_hold_us);
        st.writer_yields += ws.writer_yields;
        st.writer_yield_us += ws.writer_yield_us;
        st.lock_hold_us.insert(st.lock_hold_us.end(), ws.lock_hold_us.begin(), ws.lock_hold_us.end());
        st.governor_parked_s += ws.governor_parked_s;
    }
    if (governor != nullptr) {
        const livewiden::LiveWidenGovernor::Snapshot gov_after = governor->GetSnapshot();
        st.governed = true;
        st.governor_allowed_min = gov_after.min_allowed_seen;
        st.governor_allowed_max = gov_after.max_allowed_seen;
        st.governor_grants = gov_after.grants - gov_before.grants;
        st.governor_brakes = gov_after.brakes - gov_before.brakes;
    }
    // Every early end leaves the files that began in rebuild at the contiguous watermark they reached: dirty on disk,
    // blocks past F scanned (never trusted), MarkClean refusing, unevictable; the next start rebuilds them.
    if (failed.load(std::memory_order_relaxed)) return LiveWidenResult::kFailed;
    if (was_stopped.load(std::memory_order_relaxed)) {
        spdlog::warn("[LIVE WIDEN] [{}] - Stopped at row {} of {}; the index stays in rebuild (dirty) until the next "
                     "start rebuilds it", data_path, watermark.load(std::memory_order_relaxed), total_rows);
        return LiveWidenResult::kStopped;
    }
    const bool zmap_good = zmap_ok.load(std::memory_order_relaxed);
    const bool catidx_good = catidx_ok.load(std::memory_order_relaxed);
    // Durability A (DD4), as at startup: FinishRebuild (the dirty header is durable), then the data file, then
    // MarkClean, pins still held. Every failure leaves the file dirty (the next start rebuilds it).
    const bool zmap_finished = zmap_good && zmap->FinishRebuild();
    const bool catidx_finished = catidx_good && catidx->FinishRebuild();
    st.zmap_widened = zmap_finished;
    st.catidx_widened = catidx_finished;
    // FinishRebuild ended the rebuild state of a finished file (a later sync/MarkClean failure only leaves it dirty).
    {
        std::lock_guard<std::mutex> lock(RebuildStatusMutex());
        EndRebuildFiles(rebuild_status, zmap_finished, catidx_finished); // only files this builder began can finish here
    }
    const bool data_synced = (zmap_finished || catidx_finished) && fdatasync(data.fd) == 0;
    bool all_ok = (zmap.get() == nullptr || zmap_finished) && (catidx.get() == nullptr || catidx_finished) && data_synced;
    if (zmap.get() != nullptr && !zmap_finished) {
        spdlog::error("[LIVE WIDEN] [{}/{}.zmap] - {}; it stays in rebuild (dirty) until the next start", db_base_path, year,
                      zmap_good ? "Its rebuilt header could not be made durable" : "An index write failed during the build");
    }
    if (catidx.get() != nullptr && !catidx_finished) {
        spdlog::error("[LIVE WIDEN] [{}/{}.catidx] - {}; it stays in rebuild (dirty) until the next start", db_base_path, year,
                      catidx_good ? "Its rebuilt header could not be made durable" : "An index write failed during the build");
    }
    if ((zmap_finished || catidx_finished) && !data_synced) {
        spdlog::error("[LIVE WIDEN] [{}] - fdatasync of the data file failed; leaving the rebuilt index dirty", data_path);
    }
    if (zmap_finished && data_synced && !zmap->MarkClean()) {
        spdlog::error("[LIVE WIDEN] [{}/{}.zmap] - MarkClean failed; leaving it dirty", db_base_path, year);
        all_ok = false;
    }
    if (catidx_finished && data_synced && !catidx->MarkClean()) {
        spdlog::error("[LIVE WIDEN] [{}/{}.catidx] - MarkClean failed; leaving it dirty", db_base_path, year);
        all_ok = false;
    }
    spdlog::info("[LIVE WIDEN] [{}] - Rebuilt at {} columns (zone map: {}, catidx: {}): {} rows in {} chunks, {} lock "
                 "timeouts, max lock hold {} us, {} ms, {} writer yields ({} ms)", data_path, tracked_column_count,
                 zmap_finished, catidx_finished, st.rows_folded, st.chunks, st.lock_timeouts, st.max_lock_hold_us,
                 std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - started).count(), st.writer_yields,
                 st.writer_yield_us / 1000);
    if (st.governed) {
        spdlog::info("[LIVE WIDEN] [{}] - Load governor: {} of {} workers started, {}-{} allowed, {} group grants, {} "
                     "writer brakes, {:.1f} s parked (summed over workers)", data_path, worker_count,
                     governor->MaxWorkers(), st.governor_allowed_min, st.governor_allowed_max, st.governor_grants,
                     st.governor_brakes, st.governor_parked_s);
    }
    if (!all_ok) return LiveWidenResult::kFailed;
    return refused ? LiveWidenResult::kPartial : LiveWidenResult::kDone;
} catch (const std::exception &e) {
    spdlog::error("[LIVE WIDEN] [{}/{}] - Failed: {}", db_base_path, year, e.what());
    return LiveWidenResult::kFailed;
}

std::vector<XStore::LiveWidenPassEntry> XStore::RunLiveWidenPass(std::chrono::steady_clock::time_point now,
                                                                 unsigned workers,
                                                                 livewiden::LiveWidenGovernor *governor,
                                                                 livewiden::LiveWidenLoadSource *load) {
    std::vector<LiveWidenPassEntry> processed;
    if (!live_widen_pending_.load(std::memory_order_relaxed)) return processed; // the per-tick cost when idle
    struct Due {
        std::string db_base_path;
        std::string db_name;
        uint64_t growth_seq;
    };
    std::vector<Due> due;
    {
        std::lock_guard<std::mutex> lock(live_widen_mutex_);
        for (const auto &[db_base_path, request] : live_widen_requests_) {
            if (now - request.last_growth >= std::chrono::seconds(XStoreTunables::LIVE_WIDEN_QUIET_PERIOD_SEC) &&
                now >= request.retry_after) {
                due.push_back({db_base_path, request.db_name, request.growth_seq});
            }
        }
    }
    for (const Due &d : due) {
        if (bg_cancel_.load(std::memory_order_relaxed)) break;
        spdlog::info("[LIVE WIDEN] [DB: {}] - Width stable for {} s; widening its year index files, newest year first",
                     d.db_name, XStoreTunables::LIVE_WIDEN_QUIET_PERIOD_SEC);
        // Production governor (off by default): one per DB, shared by its years so the ramp-up carries over.
        std::unique_ptr<livewiden::LiveWidenLoadSource> own_load;
        std::unique_ptr<livewiden::LiveWidenGovernor> own_governor;
        livewiden::LiveWidenGovernor *db_governor = governor;
        livewiden::LiveWidenLoadSource *db_load = load;
        if (governor == nullptr && XStoreTunables::LIVE_WIDEN_GOVERNOR) {
            own_load = std::make_unique<livewiden::LiveWidenLoadSource>();
            own_governor = livewiden::MakeLiveWidenGovernor(*own_load, d.db_name);
            db_governor = own_governor.get();
            db_load = own_load.get();
        }
        const LiveWidenDbOutcome outcome = LiveWidenDb(d.db_base_path, d.db_name, processed, workers, db_governor, db_load);
        bool gave_up = false;
        uint32_t attempts = 0;
        uint64_t wait_sec = 0;
        {
            std::lock_guard<std::mutex> lock(live_widen_mutex_);
            auto it = live_widen_requests_.find(d.db_base_path);
            if (it != live_widen_requests_.end()) { // else dropped meanwhile (RemoveDB)
                LiveWidenRequest &request = it->second;
                if (outcome == LiveWidenDbOutcome::kGone) {
                    live_widen_requests_.erase(it);
                } else if (request.growth_seq != d.growth_seq) {
                    // Grew while this pass ran: the record stays and runs again after its new quiet period, with
                    // fresh widths (the growth already reset its retry state).
                } else if (outcome == LiveWidenDbOutcome::kStopped) {
                    // Kept: the next pass (after a removal, or never at shutdown) looks again.
                } else if (outcome == LiveWidenDbOutcome::kRetry) {
                    attempts = ++request.failed_attempts;
                    if (attempts >= XStoreTunables::LIVE_WIDEN_MAX_ATTEMPTS) {
                        gave_up = true;
                        live_widen_requests_.erase(it);
                    } else {
                        wait_sec = XStoreTunables::LIVE_WIDEN_RETRY_BASE_SEC << (attempts - 1);
                        request.retry_after = now + std::chrono::seconds(wait_sec);
                    }
                } else {
                    live_widen_requests_.erase(it);
                }
            }
            if (live_widen_requests_.empty()) live_widen_pending_.store(false, std::memory_order_relaxed);
        }
        if (gave_up) {
            spdlog::error("[LIVE WIDEN] [DB: {}] - Could not start rebuilding some index files after {} attempts; giving up "
                          "until its width grows again or the next start rebuilds them (the old, narrower indexes stay in "
                          "use; queries on the new columns full-scan)", d.db_name, attempts);
        } else if (wait_sec > 0) {
            spdlog::warn("[LIVE WIDEN] [DB: {}] - Could not start rebuilding some index files (attempt {} of {}); retrying "
                         "in {} s", d.db_name, attempts, XStoreTunables::LIVE_WIDEN_MAX_ATTEMPTS, wait_sec);
        }
        if (outcome == LiveWidenDbOutcome::kStopped) break;
    }
    return processed;
}

XStore::LiveWidenDbOutcome XStore::LiveWidenDb(const std::string &db_base_path, const std::string &db_name,
                                               std::vector<LiveWidenPassEntry> &out, unsigned workers,
                                               livewiden::LiveWidenGovernor *governor,
                                               livewiden::LiveWidenLoadSource *load) try {
    XStore_Proto::ConfigStruct cfg;
    if (!GetOrLoadCfg(db_name, db_base_path, cfg)) {
        spdlog::info("[LIVE WIDEN] [DB: {}] - The DB no longer exists; nothing to widen", db_name);
        return LiveWidenDbOutcome::kGone;
    }
    const uint32_t desired = EffectiveTrackedColumnCount(cfg);
    const uint16_t row_length = static_cast<uint16_t>(cfg.rowlength());

    std::vector<unsigned short> years;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(db_base_path, ec), end; !ec && it != end; it.increment(ec)) {
        const std::string filename = it->path().filename().string();
        if (filename.size() < 7 || filename.compare(filename.size() - 7, 7, ".XSTORE") != 0) continue;
        try {
            years.push_back(static_cast<unsigned short>(std::stoi(filename.substr(0, filename.size() - 7))));
        } catch (...) {
            continue; // not a <YEAR>.XSTORE-shaped filename (same rule as the startup rebuild)
        }
    }
    if (ec) {
        if (ec == std::errc::no_such_file_or_directory) return LiveWidenDbOutcome::kGone;
        spdlog::error("[LIVE WIDEN] [DB: {}] - Cannot list {}: {}", db_name, db_base_path, ec.message());
        return LiveWidenDbOutcome::kRetry;
    }
    std::sort(years.begin(), years.end(), std::greater<>()); // newest (the current) year first

    bool retry = false;
    for (const unsigned short year : years) {
        const std::string year_key = fmt::format("{}/{}", db_base_path, year);
        const BgEnter enter = EnterBackgroundYear(year_key);
        if (enter == BgEnter::kCancelled) return LiveWidenDbOutcome::kStopped;
        if (enter == BgEnter::kRemoving) continue; // that year (or the whole DB) is being removed
        struct Leave {
            ~Leave() { XStore::LeaveBackgroundYear(); }
        } leave;

        LiveWidenPassEntry entry;
        entry.db_name = db_name;
        entry.year = year;
        const YearIndexWidths widths = PeekYearIndexWidths(db_base_path, year);
        if (widths.rebuilding) {
            // A stopped or failed rebuild (or a startup rebuild that could not begin): untrusted until the next start
            // rebuilds it. Never resumed here: its owner may still be alive.
            entry.skipped_rebuilding = true;
            entry.result = LiveWidenResult::kAlreadyRebuilding;
            out.push_back(entry);
            bool first = false;
            {
                std::lock_guard<std::mutex> lock(live_widen_mutex_);
                first = live_widen_skip_logged_.insert(year_key).second;
            }
            if (first) {
                spdlog::warn("[LIVE WIDEN] [DB: {}] - {} is already being rebuilt (or a rebuild stopped); not widened now, "
                             "it is rebuilt at the next start", db_name, year);
            }
            continue;
        }
        // Widths only (mirrors ZoneMapNeedsRebuild / CatIdxNeedsRebuild): the dirty flag says nothing at runtime.
        const bool widen_zmap = !widths.zmap.has_value() || *widths.zmap != desired;
        const bool widen_catidx = !widths.catidx.has_value() || *widths.catidx < desired;
        if (!widen_zmap && !widen_catidx) {
            out.push_back(entry); // kNothingToDo
            continue;
        }
        spdlog::info("[LIVE WIDEN] [DB: {}] - Widening year {} to {} columns (zone map: {}, catidx: {})", db_name, year,
                     desired, widen_zmap, widen_catidx);
        LiveWidenStats stats;
        entry.result = LiveWidenOneYear(db_base_path, year, row_length, desired, widen_zmap, widen_catidx, &bg_cancel_, &stats,
                                        workers, XStoreTunables::LIVE_WIDEN_GROUP_BYTES, governor, load);
        out.push_back(entry);
        if (entry.result == LiveWidenResult::kDone || entry.result == LiveWidenResult::kNothingToDo) continue;
        if (entry.result == LiveWidenResult::kStopped) return LiveWidenDbOutcome::kStopped;
        if (stats.zmap_busy || stats.catidx_busy) {
            // Another rebuild owns a file: retrying in a loop would only meet it again.
            spdlog::warn("[LIVE WIDEN] [DB: {}] - An index file of {} is owned by another rebuild; left to it", db_name, year);
            continue;
        }
        const bool failed_begin = (widen_zmap && !stats.zmap_began) || (widen_catidx && !stats.catidx_began);
        if (failed_begin) {
            retry = true; // that file's old index is intact and trusted: worth another try later
        } else {
            spdlog::error("[LIVE WIDEN] [DB: {}] - Rebuilding {} failed midway; its index stays untrusted (scanned) until the "
                          "next start rebuilds it", db_name, year);
        }
    }
    return retry ? LiveWidenDbOutcome::kRetry : LiveWidenDbOutcome::kDone;
} catch (const std::exception &e) {
    spdlog::error("[LIVE WIDEN] [DB: {}] - Pass failed: {}", db_name, e.what());
    return LiveWidenDbOutcome::kRetry;
}

void XStore::AppendRebuildObservations(uint64_t row_index, const char *row, uint16_t row_length,
                                       uint32_t tracked_column_count, ZoneMapFoldBatch *zmap, CatIdxFoldBatch *catidx,
                                       RebuildScratch &scratch) {
    using google::protobuf::internal::WireFormatLite;
    static const uint32_t kColumnTag =
        WireFormatLite::MakeTag(XStore_Proto::RowData::kFieldDataFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED);
    const size_t zmap_mark = zmap != nullptr ? zmap->observations.size() : 0;
    const size_t catidx_mark = catidx != nullptr ? catidx->values.size() : 0;

    // One pass: walk the tags as RealRowDataLength / ExtractColumnByIndexSkipParse do (ReadTag() == 0 ends the row) and
    // decode each column once. Accepted only if every tag is a well-formed column (field 1, length-delimited, inside
    // the slot) and every column decodes. Then the per-row rebuild got exactly these columns: RowData parse of
    // [0, real_len) sees the same n columns in the same order (no unknown or mis-typed field to skip or keep), and
    // ExtractColumnByIndexSkipParse(c) skips the same c columns and decodes column c from the same bytes (an
    // empty, unset column included: it is present and encodes as its tag byte alone).
    google::protobuf::io::CodedInputStream in(reinterpret_cast<const uint8_t *>(row), row_length);
    bool one_pass = true;
    uint32_t column = 0;
    for (uint32_t tag = in.ReadTag(); tag != 0; tag = in.ReadTag(), ++column) {
        int length = 0;
        if (tag != kColumnTag || !in.ReadVarintSizeAsInt(&length) ||
            length > static_cast<int>(row_length) - in.CurrentPosition() ||
            !scratch.column.ParseFromArray(row + in.CurrentPosition(), length)) {
            one_pass = false;
            break;
        }
        if (column < tracked_column_count) {
            if (zmap != nullptr) zmap->observations.push_back(ZoneMapObservationOf(*this, scratch.column, column));
            if (catidx != nullptr) catidx->Add(column, EncodeFDataForCatIdx(scratch.column));
        }
        static_cast<void>(in.Skip(length)); // cannot fail: length was checked against the slot
    }

    if (!one_pass) {
        // A damaged or unusual row: the per-row rebuild's own calls, so it gets exactly what that rebuild got.
        if (zmap != nullptr) {
            zmap->observations.resize(zmap_mark);
            const size_t real_len = XStoreWireEncode::RealRowDataLength(row, row_length);
            scratch.row.Clear();
            if (real_len > 0) static_cast<void>(scratch.row.ParseFromArray(row, static_cast<int>(real_len)));
            const int tracked = std::min(static_cast<int>(tracked_column_count), scratch.row.fielddata_size());
            for (int c = 0; c < tracked; ++c) {
                zmap->observations.push_back(ZoneMapObservationOf(*this, scratch.row.fielddata(c), static_cast<uint32_t>(c)));
            }
        }
        if (catidx != nullptr) {
            catidx->values.resize(catidx_mark);
            for (uint32_t c = 0; c < tracked_column_count; ++c) {
                scratch.column.Clear();
                if (ExtractColumnByIndexSkipParse(row, row_length, static_cast<unsigned short>(c), scratch.column)) {
                    catidx->Add(c, EncodeFDataForCatIdx(scratch.column));
                }
            }
        }
    }
    if (zmap != nullptr) zmap->EndRow(row_index);
    if (catidx != nullptr) catidx->EndRow(row_index);
}

bool XStore::CatIdxNeedsRebuild(const std::string &catidx_path, uint32_t desired_tracked_columns) {
    if (!std::filesystem::exists(catidx_path)) return true; // no sidecar at all (pre-existing data)
    const int fd = open(catidx_path.c_str(), O_RDONLY);
    if (fd < 0) return true;
    CatIdxFileHeader on_disk{};
    const bool readable = pread(fd, &on_disk, sizeof(on_disk), 0) == static_cast<ssize_t>(sizeof(on_disk));
    close(fd);
    if (!readable || on_disk.magic != CatIdxFileHeader{}.magic || on_disk.version != XStoreTunables::CATIDX_VERSION ||
        on_disk.tracked_column_count < desired_tracked_columns) {
        return true;
    }
    if (on_disk.dirty != 0) {
        // Durability A: unclean shutdown -- treat like a stale width.
        spdlog::info("[CATIDX REBUILD] [{}] - marked dirty (unclean shutdown) -- rebuilding", catidx_path);
        return true;
    }
    return false;
}

void XStore::RebuildStaleIndexesForDb(const std::string &db_name, const std::string &db_base_path,
                                      const XStore_Proto::ConfigStruct &cfg_struct,
                                      const std::set<unsigned short> &force_rebuild_years) {
    const uint32_t desired_tracked_columns = EffectiveTrackedColumnCount(cfg_struct);
    const uint16_t row_length = static_cast<uint16_t>(cfg_struct.rowlength());

    // Read-only pass first: the rebuilds below create and truncate files in this directory, which must not happen
    // while a directory_iterator over it is alive.
    std::vector<RebuildYearTask> work;
    for (auto &f : std::filesystem::directory_iterator(db_base_path)) {
        std::string filename = f.path().filename().string();
        if (filename.size() < 7 || filename.substr(filename.size() - 7) != ".XSTORE") continue;
        unsigned short year;
        try {
            year = static_cast<unsigned short>(std::stoi(filename.substr(0, filename.size() - 7)));
        } catch (...) {
            continue; // not a <YEAR>.XSTORE-shaped filename -- skip
        }
        // A WAL-replayed year: both indexes are structurally fine (right version, right width) and semantically short
        // by exactly the replayed rows, which carry no observations. Only a full re-derivation from raw data can tell,
        // and a rebuild is idempotent, so forcing it on an already-correct year costs time, never correctness.
        const bool forced = force_rebuild_years.count(year) > 0;
        const bool zmap = forced || ZoneMapNeedsRebuild(fmt::format("{}/{}.zmap", db_base_path, year), desired_tracked_columns, row_length);
        const bool catidx = forced || CatIdxNeedsRebuild(fmt::format("{}/{}.catidx", db_base_path, year), desired_tracked_columns);
        if (zmap || catidx) work.push_back({year, zmap, catidx});
    }
    if (work.empty()) return;

    const size_t zmap_years = static_cast<size_t>(std::count_if(work.begin(), work.end(), [](const RebuildYearTask &w) { return w.zmap; }));
    if (zmap_years > 0) spdlog::info("[ZONE MAP REBUILD] [DB: {}] - {} year file(s) need rebuild", db_name, zmap_years);
    for (const RebuildYearTask &w : work) {
        if (w.catidx) spdlog::info("[CATIDX REBUILD] [DB: {}] - rebuilding stale/missing {}.catidx", db_name, w.year);
    }
    // All years at once on every core (no request can run before InitDB returns), zone map and catidx together (one
    // scan per year). A year that throws does not stop the others; then the lowest year's exception is rethrown, so
    // InitDB (and the XStore constructor) fails loudly instead of serving an index this rebuild did not finish.
    const unsigned cores = std::thread::hardware_concurrency();
    const std::vector<std::exception_ptr> failures = RebuildYearsIndexes(
        db_base_path, work, row_length, desired_tracked_columns,
        std::min<unsigned>(cores != 0 ? cores : 4, XStoreTunables::REBUILD_MAX_THREADS));
    size_t lowest = work.size();
    for (size_t i = 0; i < work.size(); ++i) {
        if (!failures[i]) continue;
        try {
            std::rethrow_exception(failures[i]);
        } catch (const std::exception &e) {
            spdlog::error("[INDEX REBUILD] [DB: {}] - Rebuild of {} failed: {}", db_name, work[i].year, e.what());
        } catch (...) {
            spdlog::error("[INDEX REBUILD] [DB: {}] - Rebuild of {} failed", db_name, work[i].year);
        }
        // The lowest failed year's exception, so the error a failed start reports does not depend on directory order.
        if (lowest == work.size() || work[i].year < work[lowest].year) lowest = i;
    }
    if (lowest < work.size()) std::rethrow_exception(failures[lowest]);
}

namespace {
// "The DB this record was written for is gone" (deleted, or never recreated before replay) is the one
// benign reason an apply fails; a DB that is not positively gone failing to apply is an I/O or
// permission problem that must not be papered over by moving the checkpoint past the record.
bool PositivelyGone(const std::string &path) {
    std::error_code ec;
    const std::filesystem::file_status st = std::filesystem::status(path, ec);
    if (!ec) return st.type() == std::filesystem::file_type::not_found;
    return ec == std::errc::no_such_file_or_directory || ec == std::errc::not_a_directory;
}
// FAIL-SAFE: the DB counts as deleted only when the filesystem positively says its directory or CFG is
// gone (not_found / ENOENT / ENOTDIR). EACCES (a directory that lost search permission), EIO, ... mean
// "cannot tell" => the DB is treated as existing, so a failed apply fences the shard instead of
// silently skipping the record.
bool DbStillExists(const std::string &db_base_path, const std::string &db_name) {
    return !PositivelyGone(db_base_path) && !PositivelyGone(db_base_path + "/" + db_name + ".CFG");
}
}  // namespace

XStore::ReplayedYearMap XStore::ReplayWal() {
    // No idle-triggered checkpoint cycles while replay Notes files (see Checkpointer::HoldForBoot).
    const walring::BootHold boot_hold;
    ReplayedYearMap replayed_years;

    std::filesystem::path wal_dir = std::filesystem::path(data_store_dir) / XStoreTunables::WAL_DIR_NAME;
    if (!std::filesystem::is_directory(wal_dir)) {
        return replayed_years;
    }

    for (size_t shard_idx = 0; shard_idx < XStoreTunables::WAL_SHARD_COUNT; ++shard_idx) {
        std::filesystem::path wal_path = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");
        // Every replay attempt starts unfenced, including when the file is missing/empty this time.
        walring::UnfenceWalFile(wal_path.string());
        std::error_code file_size_ec;
        const uintmax_t on_disk_bytes = std::filesystem::file_size(wal_path, file_size_ec);
        if (file_size_ec) {
            if (!std::filesystem::exists(wal_path)) continue;   // shard never used
            spdlog::error("[WAL REPLAY] - Unable to stat {}: {}; the shard is fenced until restart", wal_path.string(), file_size_ec.message());
            walring::FenceWalFile(wal_path.string());
            continue;
        }
        // An empty file is neither a ring nor a legacy WAL: GetOrOpenRing initializes it.
        if (on_disk_bytes == 0) continue;

        const int fd = open(wal_path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            spdlog::error("[WAL REPLAY] - Unable to open {}; the shard is fenced until restart", wal_path.string());
            walring::FenceWalFile(wal_path.string());
            continue;
        }
        walring::RingHeader header;
        const bool is_ring = walring::ReadBestHeader(fd, header);
        if (is_ring) {
            // The HEADER's ring size, never the process default: the file may
            // have been created under a different setting.
            ReplayRingShard(wal_path.string(), shard_idx, fd, header.ring_bytes, header.checkpoint_lsn, replayed_years);
        }
        close(fd);
        if (!is_ring) ReplayLegacyShard(wal_path.string(), shard_idx, replayed_years);
    }

    return replayed_years;
}

void XStore::ReplayRingShard(const std::string &wal_path, size_t shard_idx, int fd, uint64_t ring_bytes,
                             uint64_t checkpoint_lsn, ReplayedYearMap &replayed_years) {
    // InitDB must precede all inserts to this data_store_dir (CommitReplay is not concurrency-safe
    // against the writer's blind CommitAppended/PublishApplied).
    walring::UnfenceWalFile(wal_path);   // a previous failed attempt in this process is retried from scratch
    const uint64_t start_lsn = checkpoint_lsn;
    uint64_t lsn = start_lsn;

    // buf holds the file bytes for LSNs [window_lsn, window_lsn + buf.size()), never
    // crossing a ring end (a record never straddles it). Refilled in
    // WAL_REPLAY_READ_CHUNK_BYTES reads, or one record's size if larger.
    std::string buf;
    uint64_t window_lsn = 0;
    bool read_error = false;
    auto fill = [&](size_t needed) -> bool {
        const uint64_t lap_left = ring_bytes - lsn % ring_bytes;
        if (needed > lap_left) return false;
        if (!buf.empty() && lsn >= window_lsn && lsn + needed <= window_lsn + buf.size()) return true;
        const size_t want = static_cast<size_t>(std::min<uint64_t>(
            lap_left, std::max<uint64_t>(needed, XStoreTunables::WAL_REPLAY_READ_CHUNK_BYTES)));
        buf.resize(want);
        const off_t base = static_cast<off_t>(walring::RingOffset(lsn, ring_bytes));
        size_t got = 0;
        while (got < want) {
            if (walring::ConsumeInjectedReplayReadFailure()) { read_error = true; break; }
            const ssize_t n = pread(fd, buf.data() + got, want - got, base + static_cast<off_t>(got));
            if (n < 0) {
                if (errno == EINTR) continue;
                read_error = true;   // NOT the end of the log: the checkpoint must not move
                break;
            }
            if (n == 0) break;       // EOF: bytes past the file end hold no records
            got += static_cast<size_t>(n);
        }
        buf.resize(got);
        window_lsn = lsn;
        return got >= needed;
    };

    unsigned replayed_count = 0;
    std::set<std::string> touched_paths;
    WalRecord record;
    // Why the scan stopped. A DAMAGED record (CRC failure, impossible length, structure mismatch) whose
    // UNCHECKED LSN field (bytes [8,16) of the candidate) equals the expected LSN is a torn record of THIS
    // lap: "corrupt_end". Bytes after it may hold intact leftovers of unacknowledged records of this lap
    // whose LSNs will match once the log grows back over them, so they are zeroed below. Everything else
    // is a CLEAN end and is never zeroed (that would cost a ring write per shard on every start): all-zero
    // bytes, a CRC-valid record carrying another LSN, and damaged-looking bytes whose LSN field does not
    // match -- after the ring has lapped, the bytes at the log end are mid-record leftovers of an older lap.
    // ACCEPTED TRADE-OFF: a torn record whose first sector (the one holding the LSN field) was lost reads
    // as a clean end -- the same class as the accepted "zero hole in front of an intact N+1" case.
    bool corrupt_end = false;
    bool apply_failed = false;
    auto lsn_field_matches = [&]() {   // needs >= 16 bytes in the window: the loop guarantees RECORD_FIXED_BYTES
        uint64_t stored;
        std::memcpy(&stored, buf.data() + (lsn - window_lsn) + 8, sizeof stored);
        return stored == lsn;
    };
    // A legitimate log never spans more than one lap (writers stall instead of overwriting), so a
    // step budget of ring_bytes also stops a pathological file from looping.
    while (lsn - start_lsn < ring_bytes) {
        const uint64_t rem = ring_bytes - lsn % ring_bytes;
        if (rem < walring::RECORD_FIXED_BYTES) { lsn += rem; continue; }   // implicit wrap, same rule as the writer
        if (!fill(walring::RECORD_FIXED_BYTES)) break;
        uint32_t declared_record_len;
        std::memcpy(&declared_record_len, buf.data() + (lsn - window_lsn), 4);
        if (declared_record_len == 0) break;                               // zeros: clean end
        if (declared_record_len > XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES) {
            spdlog::error("[WAL REPLAY] - {} has a record declaring {} bytes, exceeding the {}-byte "
                          "single-record replay sanity ceiling -- stopping replay of this shard here; "
                          "check for corruption.",
                          wal_path, declared_record_len, XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES);
            corrupt_end = lsn_field_matches();
            break;
        }
        const size_t needed = 4 + static_cast<size_t>(declared_record_len);
        if (needed < walring::RECORD_FIXED_BYTES || needed > rem) { corrupt_end = lsn_field_matches(); break; }   // impossible length
        if (!fill(needed)) break;                                          // file ends inside the record

        size_t consumed = 0;
        bool damaged = false;
        const RingParse parsed = ParseWalRecordV2(buf.data() + (lsn - window_lsn), needed, lsn, consumed, record, &damaged);
        if (parsed == RingParse::kEnd) { corrupt_end = damaged && lsn_field_matches(); break; }
        if (parsed == RingParse::kPad) { lsn += consumed; continue; }

        record.db_base_path = DbBasePath(record.db_name);
        if (!ApplyWalRecord(record)) {
            if (DbStillExists(record.db_base_path, record.db_name)) {
                spdlog::error("[WAL REPLAY] [DB: {}] - Record at LSN {} ({} rows) failed to apply although the DB is not gone (reason: see the "
                              "preceding [WAL APPLY] line); this shard's replay FAILS and the shard is fenced until restart",
                              record.db_name, lsn, record.rows.size());
                apply_failed = true;
            } else {
                spdlog::error("[WAL REPLAY] [DB: {}] - One or more rows failed to apply (DB no longer exists; skipped)", record.db_name);
            }
        }
        // Recorded whether or not the apply fully succeeded: a partial apply still put SOME rows
        // into the data file with no zone-map fold. Same derivation as ApplyWalRecord (one (db, year)
        // per record); keyed by db_name so InitDB's lookup cannot be defeated by another spelling.
        if (!record.rows.empty()) {
            TimeStruct replayed_ts(static_cast<std::time_t>(record.rows.front().epoch_time), record.time_resolution);
            replayed_years[record.db_name].insert(replayed_ts.year);
            touched_paths.insert(CachedYearFilePath(record.db_base_path, replayed_ts.year));
        }
        replayed_count += static_cast<unsigned>(record.rows.size());
        lsn += consumed;
    }
    const uint64_t end_lsn = lsn;

    // Scan-ahead (closes the "all-zero hole / lost LSN sector in front of an intact N+1" case): a clean end can hide a
    // torn record whose bytes were lost, with an intact later record of THIS lap behind it (sectors persisted out of
    // order). Such a record is CRC-valid and its LSN equals its own position, which no leftover of an older lap can be,
    // and it cannot have been acknowledged (an ack needs an fdatasync covering everything before it). If one exists the
    // log end is treated as a corrupt end, so everything after it is zeroed below. Byte-granular: ~one pass over the
    // ring (measured ~0.2-0.4 s per 64 MiB shard, page cache) on every start that stops cleanly.
    if (!corrupt_end && !read_error && !apply_failed) {
        const uint64_t limit = start_lsn + ring_bytes;
        for (uint64_t p = end_lsn + 1; p < limit && !corrupt_end;) {
            const uint64_t rem = ring_bytes - p % ring_bytes;
            if (rem < walring::RECORD_FIXED_BYTES) { p += rem; continue; }
            lsn = p;
            if (!fill(walring::RECORD_FIXED_BYTES)) break;
            const uint64_t win_end = window_lsn + buf.size();   // never crosses a ring end
            uint64_t q = p;
            for (; q + walring::RECORD_FIXED_BYTES <= win_end; ++q) {
                uint64_t stored;
                std::memcpy(&stored, buf.data() + (q - window_lsn) + 8, sizeof stored);
                if (stored != q) continue;
                uint32_t len;
                std::memcpy(&len, buf.data() + (q - window_lsn), sizeof len);
                const size_t needed = 4 + static_cast<size_t>(len);
                ++q;   // resume after this candidate; the check below may move the window
                if (len == 0 || len > XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES || needed < walring::RECORD_FIXED_BYTES ||
                    needed > ring_bytes - (q - 1) % ring_bytes) break;
                lsn = q - 1;
                if (!fill(needed)) break;
                size_t consumed = 0;
                bool damaged = false;
                if (ParseWalRecordV2(buf.data() + (lsn - window_lsn), needed, lsn, consumed, record, &damaged) == RingParse::kOk) {
                    spdlog::warn("[WAL REPLAY] - Intact record at LSN {} lies beyond the end of the log at LSN {} in {}: a torn record "
                                 "was lost in front of it; discarding everything after the end", lsn, end_lsn, wal_path);
                    corrupt_end = true;
                }
                break;
            }
            p = std::max(p + 1, q);
        }
        lsn = end_lsn;
    }

    if (read_error) {
        spdlog::error("[WAL REPLAY] - Read error in {}; leaving its checkpoint untouched (records replay again next start); "
                      "the shard is fenced until restart", wal_path);
        walring::FenceWalFile(wal_path);
        return;
    }
    if (replayed_count > 0) {
        spdlog::info("[WAL REPLAY] - Replayed {} record(s) from {}", replayed_count, wal_path);
    }
    if (apply_failed) {
        // The records stay in the log (checkpoint untouched) and replay again next start; until then
        // nothing may append to this ring.
        walring::FenceWalFile(wal_path);
        return;
    }
    if (end_lsn == start_lsn && !corrupt_end) return;

    // Spec §2 ordering: every touched data file (and its directory entry) must be durable BEFORE the
    // checkpoint moves. Any failure leaves the header alone: the records replay again (idempotent).
    if (!walring::Checkpointer::Instance().SyncPathsForReplay(touched_paths)) {
        spdlog::error("[WAL REPLAY] - Could not sync the data files replayed from {}; checkpoint left at {}; the shard is fenced until restart",
                      wal_path, start_lsn);
        walring::FenceWalFile(wal_path);   // appends would restart on top of un-checkpointed records
        return;
    }
    if (corrupt_end && end_lsn < start_lsn + ring_bytes) {
        // Zero everything past the damaged record, durably, before the checkpoint can move: see corrupt_end.
        // Order: everything behind the first 4 bytes of the damaged record first, sync, THEN its length
        // field. After a crash in between, the record's LSN field (bytes [8,16)) is already zero, so the
        // next start reads it as a clean end -- with everything behind it already zeroed, nothing stale is
        // left ahead. What can never happen is "zeros at X with an intact N+1 behind them", which would
        // also read as a clean end but leave a stale same-lap record.
        // The 4 bytes cannot straddle the ring end: the scan only stops here with >= RECORD_FIXED_BYTES left.
        const int wfd = open(wal_path.c_str(), O_RDWR | O_CLOEXEC);
        const uint64_t zero_to = start_lsn + ring_bytes;
        const uint64_t head_end = std::min<uint64_t>(end_lsn + 4, zero_to);
        bool zeroed = wfd >= 0 && !walring::ConsumeInjectedReplayZeroingFailure() &&
                      walring::ZeroRingRange(wfd, head_end, zero_to, ring_bytes) && fdatasync(wfd) == 0 &&
                      !walring::ReplayStopsAfterTailZeroing() &&   // test hook: "crash" between the two syncs
                      walring::ZeroRingRange(wfd, end_lsn, head_end, ring_bytes) && fdatasync(wfd) == 0;
        if (wfd >= 0) close(wfd);
        if (!zeroed) {
            spdlog::error("[WAL REPLAY] - Could not zero the ring past the damaged record at LSN {} in {}; checkpoint left at {}; "
                          "the shard is fenced until restart", end_lsn, wal_path, start_lsn);
            walring::FenceWalFile(wal_path);   // a stale same-lap record could come back if appends resumed
            return;
        }
        walring::CountReplayZeroedBytes(zero_to - end_lsn);
        spdlog::warn("[WAL REPLAY] - Damaged/torn record at LSN {} in {}: discarded everything after it", end_lsn, wal_path);
    }
    const std::shared_ptr<walring::Ring> ring = walring::GetOrOpenRing(wal_path, static_cast<uint32_t>(shard_idx));
    if (!ring || ring->ring_bytes() != ring_bytes) {
        spdlog::error("[WAL REPLAY] - No matching ring for {}; checkpoint left at {}; the shard is fenced until restart", wal_path, start_lsn);
        walring::FenceWalFile(wal_path);
        return;
    }
    walring::Ring::HeaderWriteStatus header_status;
    if (!ring->CommitReplay(end_lsn, &header_status)) {
        spdlog::error("[WAL REPLAY] - Could not persist the checkpoint for {}; records replay again next start; "
                      "the shard is fenced until restart", wal_path);
        walring::FenceWalFile(wal_path);
        // A genuine header pwrite/fdatasync error is fsyncgate too (the page may have been dropped while a
        // retry reports success): also poison, so no later cycle advances a ring over this device.
        if (header_status.real) walring::Checkpointer::Instance().ReportRealError("ring header write (replay)", wal_path, header_status.err);
    }
}

void XStore::ReplayLegacyShard(const std::string &wal_path, size_t shard_idx, ReplayedYearMap &replayed_years) {
    // Pre-ring format (no ring header): the original streaming parse, unchanged,
    // then a one-time upgrade to the ring layout.
    walring::UnfenceWalFile(wal_path);   // a previous failed attempt in this process is retried from scratch
    std::ifstream in(wal_path, std::ios::binary);
    if (!in) {
        spdlog::error("[WAL REPLAY] - Unable to open {}; the shard is fenced until restart", wal_path);
        walring::FenceWalFile(wal_path);
        return;
    }

    // Streams this file's records in bounded WAL_REPLAY_READ_CHUNK_BYTES
    // chunks rather than loading the whole file into one std::string --
    // a shard file's on-disk size is no longer a trustworthy bound for a
    // single allocation (see WAL_REPLAY_MAX_RECORD_BYTES's comment in
    // XStore_Tunables.h for why). buffer only ever holds "a chunk or two
    // plus one in-flight record's worth of bytes", grown on demand and
    // shrunk (erase) as each record is consumed.
    std::string buffer;
    // A short read that only hit EOF sets eofbit|failbit; a real I/O error sets badbit (or is injected by
    // tests). The latter is NOT the end of the log: the records behind it are unread.
    bool read_error = false;
    auto ensure_available = [&](size_t needed) -> bool {
        while (buffer.size() < needed) {
            size_t old_size = buffer.size();
            buffer.resize(old_size + XStoreTunables::WAL_REPLAY_READ_CHUNK_BYTES);
            in.read(buffer.data() + old_size, static_cast<std::streamsize>(XStoreTunables::WAL_REPLAY_READ_CHUNK_BYTES));
            std::streamsize got = in.gcount();
            buffer.resize(old_size + static_cast<size_t>(got));
            if (in.bad() || walring::ConsumeInjectedReplayReadFailure()) { read_error = true; return false; }
            if (got <= 0) return false; // true EOF -- can't satisfy `needed`
        }
        return true;
    };

    std::set<std::string> touched_paths;
    unsigned replayed_count = 0;
    unsigned parsed_records = 0;
    bool apply_failed = false;
    while (true) {
        if (!ensure_available(8)) {
            // Fewer than 8 bytes left anywhere in the file -- clean end or torn tail.
            break;
        }
        uint32_t declared_record_len;
        std::memcpy(&declared_record_len, buffer.data(), 4);
        if (declared_record_len > XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES) {
            // Checked BEFORE trying to satisfy it -- a corrupt/bogus record_len can't force
            // ensure_available to keep growing the buffer chasing it.
            spdlog::error("[WAL REPLAY] - {} has a record declaring {} bytes, exceeding the "
                          "{}-byte single-record replay sanity ceiling -- stopping replay of "
                          "this shard here; check for corruption.",
                          wal_path, declared_record_len,
                          XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES);
            break;
        }
        if (!ensure_available(4 + static_cast<size_t>(declared_record_len))) {
            // Torn tail: file ends before this record's declared length is satisfied.
            break;
        }

        XStore::WalRecord record;
        size_t consumed = 0;
        if (!ParseWalRecord(buffer.data(), buffer.size(), consumed, record)) {
            // CRC/structure failure -- genuine corruption/torn record; every record before
            // this point already passed its own CRC check, and already-applied.
            break;
        }
        ++parsed_records;
        record.db_base_path = DbBasePath(record.db_name);
        if (!ApplyWalRecord(record)) {
            if (DbStillExists(record.db_base_path, record.db_name)) {
                spdlog::error("[WAL REPLAY] [DB: {}] - Legacy record ({} rows) failed to apply although the DB is not gone (reason: see the "
                              "preceding [WAL APPLY] line); the file is NOT converted and the shard is fenced until restart",
                              record.db_name, record.rows.size());
                apply_failed = true;
            } else {
                spdlog::error("[WAL REPLAY] [DB: {}] - One or more rows failed to apply (DB no longer exists; skipped)", record.db_name);
            }
        }

        // Recorded whether or not the apply above fully succeeded: a partial apply still put
        // SOME rows into the data file with no matching zone-map fold, which is the same gap.
        // Year is derived the exact same way ApplyWalRecord derives it. Keyed by db_name rather
        // than record.db_base_path so InitDB's lookup can't be defeated by a second spelling of
        // the same directory.
        if (!record.rows.empty()) {
            TimeStruct replayed_ts(static_cast<std::time_t>(record.rows.front().epoch_time), record.time_resolution);
            replayed_years[record.db_name].insert(replayed_ts.year);
            touched_paths.insert(CachedYearFilePath(record.db_base_path, replayed_ts.year));
        }

        replayed_count += static_cast<unsigned>(record.rows.size());
        buffer.erase(0, consumed);
    }
    in.close();

    if (replayed_count > 0) {
        spdlog::info("[WAL REPLAY] - Replayed {} record(s) from {}", replayed_count, wal_path);
    }
    if (read_error) {
        // Records behind the failed read are unread: converting would wipe them. The file stays legacy.
        spdlog::error("[WAL REPLAY] - Read error in legacy {}; NOT converting it; inserts to this shard fail (fenced) until the next start retries",
                      wal_path);
        walring::FenceWalFile(wal_path);
        return;
    }

    // Upgrade: the legacy file becomes a ring ONLY once what it held is durable in the data
    // files. A record for a DB that no longer exists does not block this (it would recur
    // identically on every restart); a failed apply for an existing DB or a failed SYNC does, and
    // the file is left as is, fenced so the writer cannot wipe it meanwhile.
    if (apply_failed) {
        walring::FenceWalFile(wal_path);
        return;
    }
    // Nothing parsed, yet the first 4 KiB is not all zero and the first u32 is not the zeroed
    // record_len of an invalidated legacy file (and no slot carries the ring magic): this may be a RING file whose two header slots are
    // both damaged (or a torn first legacy record). Never wipe it automatically.
    if (parsed_records == 0) {
        char head[walring::HEADER_REGION_BYTES] = {};
        const int hfd = open(wal_path.c_str(), O_RDONLY | O_CLOEXEC);
        ssize_t got = hfd >= 0 ? pread(hfd, head, sizeof head, 0) : -1;
        if (hfd >= 0) close(hfd);
        uint32_t first_u32 = 0, slot_magic[2] = {0, 0};
        std::memcpy(&first_u32, head, sizeof first_u32);
        // A fresh ring only ever wrote one slot (slot_seq 1 -> slot 1), so look at both.
        std::memcpy(&slot_magic[0], head, sizeof slot_magic[0]);
        std::memcpy(&slot_magic[1], head + walring::HEADER_SLOT_BYTES, sizeof slot_magic[1]);
        const bool ring_magic = slot_magic[0] == walring::RING_MAGIC || slot_magic[1] == walring::RING_MAGIC;
        const bool all_zero = got >= 0 && std::all_of(head, head + (got > 0 ? got : 0), [](char c) { return c == 0; });
        if (got < 0 || (!all_zero && (first_u32 != 0 || ring_magic))) {
            spdlog::error("[WAL REPLAY] - {} has no valid ring header and no parseable legacy record, but is not blank: "
                          "either a ring file with BOTH header slots damaged, or a legacy WAL whose first record was torn. "
                          "NOT converting/wiping it; inserts to this shard fail (fenced) until the next start. Inspect the file "
                          "and, if its records are not needed, remove it (a new ring is created on the next start).",
                          wal_path);
            walring::FenceWalFile(wal_path);
            return;
        }
    }
    if (!walring::Checkpointer::Instance().SyncPathsForReplay(touched_paths)) {
        spdlog::error("[WAL REPLAY] - Could not sync the data files replayed from legacy {}; leaving it for the next start", wal_path);
        walring::FenceWalFile(wal_path);   // the writer must not wipe it meanwhile
        return;
    }
    const int fd = open(wal_path.c_str(), O_RDWR | O_CLOEXEC);
    bool converted = false;
    if (fd >= 0) {
        // ftruncate(0) then InitRingFile: a crash in between leaves an empty or zero-headed file,
        // which replays as "nothing" (the data is already synced) and is re-initialized.
        // InitRingFile fsyncs the zero-filled ring before writing a header that describes it.
        converted = ftruncate(fd, 0) == 0 && walring::InitRingFile(fd, static_cast<uint32_t>(shard_idx), walring::DefaultRingBytes());
        close(fd);
    }
    if (!converted) {
        spdlog::error("[WAL REPLAY] - Unable to convert legacy {} to a ring; the writer will retry", wal_path);
        return;
    }
    // A Ring the registry already holds over this file (opened before the upgrade) restarts at LSN 0.
    if (const std::shared_ptr<walring::Ring> ring = walring::GetOrOpenRing(wal_path, static_cast<uint32_t>(shard_idx))) {
        ring->ResetTo(0);
    }
    spdlog::info("[WAL REPLAY] - Converted legacy WAL {} to a ring", wal_path);
}

std::string XStore::GetBaseDir() {
    // The XStore binary is expected to run with its working directory set to
    // the directory it lives in (e.g. /home/ubuntu/XStore/), with DataStore/
    // living alongside it as a direct child -- so base_dir is just cwd, not
    // cwd's parent. No trailing '/' -- see the convention note in XStore.h.
    return std::filesystem::current_path().string();
}
