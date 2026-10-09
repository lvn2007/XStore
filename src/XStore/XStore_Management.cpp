#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/XStore/XStore_PreadPool.h"
#include "include/XStore/XStore_RowScan.h"
#include "include/XStore/XStore_WireEncode.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_WalCheckpoint.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <set>
#include <stdexcept>
#include <unordered_set>

std::array<XStore::CfgShard, XStore::CFG_SHARD_COUNT> XStore::cfg_shards;

// Initialization
XStore::XStore() {
    // WAL writer threads are process-wide static state (wal_shards is a
    // static array, see XStore.h) -- started exactly once per process,
    // independent of how many distinct data_store_dirs get initialized
    // below (unlike InitDB, which is per-directory).
    static std::once_flag wal_writers_once;
    std::call_once(wal_writers_once, &XStore::StartWalWriters, data_store_dir);

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
    if (initialized_dirs.insert(data_store_dir).second) {
        std::filesystem::create_directories(data_store_dir);
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

ZoneMapFile& XStore::GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length, uint32_t tracked_column_count) {
    std::string key = fmt::format("{}/{}", db_base_path, year);
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);

    auto it = zone_map_registry_.find(key);
    if (it != zone_map_registry_.end()) {
        zone_map_registry_lru_.erase(
            std::remove(zone_map_registry_lru_.begin(), zone_map_registry_lru_.end(), key),
            zone_map_registry_lru_.end());
        zone_map_registry_lru_.push_back(key);
        return *it->second;
    }

    if (zone_map_registry_.size() >= XStoreTunables::ZONE_MAP_REGISTRY_CAP && !zone_map_registry_lru_.empty()) {
        std::string victim = zone_map_registry_lru_.front();
        zone_map_registry_lru_.erase(zone_map_registry_lru_.begin());
        zone_map_registry_.erase(victim); // ~ZoneMapFile() closes the fd -- no flush needed, already durable
    }

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

std::optional<uint32_t> XStore::PeekOpenZoneMapWidth(std::string_view db_base_path, unsigned short year) {
    std::string key = fmt::format("{}/{}", db_base_path, year);
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
    auto it = zone_map_registry_.find(key);
    if (it == zone_map_registry_.end()) return std::nullopt;
    return it->second->TrackedColumnCount();
}

void XStore::ClearZoneMapRegistryForTests() {
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);
    zone_map_registry_.clear();
    zone_map_registry_lru_.clear();
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

    // Task 4: only worth a notice when this growth actually raised the
    // effective width past what it was before -- e.g. observed_column_count
    // clamped to a value <= the global floor never changes
    // EffectiveTrackedColumnCount and doesn't leave any existing .zmap file
    // behind.
    uint32_t new_effective_tracked = EffectiveTrackedColumnCount(cfg_copy);
    if (new_effective_tracked > old_effective_tracked) {
        spdlog::warn("[ZONEMAP] [DB: {}] - tracked column width grows {} -> {}; existing year index files (if any) keep their width until restart "
                     "(queries on the new columns full-scan meanwhile -- restart XStore to rebuild)",
                     db_name, old_effective_tracked, new_effective_tracked);
    }
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

    // Loop through each requested file name
    for (int i = 0; i < proto_from_client->filename_size(); i++) {
        // filename is client-controlled and must name a single entry directly
        // inside this DB's directory -- reject anything with a path separator
        // (e.g. "../other.CFG") so a crafted request can't escape DbLoc and
        // delete arbitrary files elsewhere on disk.
        if (proto_from_client->filename(i).find('/') != std::string::npos) {
            std::string err_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - Invalid file name {}", db_name, proto_from_client->filename(i));
            SetTaggedStatus(to_return.add_removestatus(), "[WARNING] ", err_msg);
            spdlog::warn(err_msg);
            continue;
        }

        std::filesystem::path target_path = JoinPath(db_loc, proto_from_client->filename(i));
        if (proto_from_client->filename(i) == fmt::format("{}.CFG", db_name)) {
            std::string warn_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - {} is prohibited from remove", db_name, proto_from_client->filename(i));
            SetTaggedStatus(to_return.add_removestatus(), "[WARNING] ", warn_msg);
            spdlog::warn(warn_msg);
            continue;
        }
        bool curr_remove = std::filesystem::remove(target_path);
        if (curr_remove) {
            // Drop this file's cached read mapping, if any -- same
            // reasoning as RemoveDB, scoped to a single file.
            EraseMappedFile(target_path.string());
            EraseOpenFile(target_path.string());

            std::string info_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - Removed {}", db_name, proto_from_client->filename(i));
            SetTaggedStatus(to_return.add_removestatus(), "[INFO] ", info_msg);
            spdlog::info(info_msg);
        }
        else {
            std::string err_msg = fmt::format("[REMOVE DB FILES] [DB: {}] - Unable to remove {}", db_name, proto_from_client->filename(i));
            SetTaggedStatus(to_return.add_removestatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
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

        // Task 5: rebuild any stale/missing zone map for this DB before
        // InitDB() returns -- i.e. before any worker thread sharing this
        // data_store_dir can serve a request against it. Deliberately
        // inside this loop (not deferred/backgrounded past InitDB()'s
        // return) -- see RebuildStaleZoneMapsForDb's declaration comment
        // in XStore.h for why this must never run on the request path
        // instead.
        auto replayed_it = replayed_years.find(curr_db_name);
        const std::set<unsigned short> &force_rebuild_years_for_db =
            replayed_it == replayed_years.end() ? kNoForcedYears : replayed_it->second;
        RebuildStaleZoneMapsForDb(curr_db_name, p.path().string(), cfg_struct, force_rebuild_years_for_db);

        // Task 11 (+ review fix round): mirrors the zone-map rebuild above,
        // but for .catidx -- same synchronous-during-InitDB() timing, so no
        // worker thread can ever see a schema-grown/WAL-replayed/missing
        // .catidx left able to silently under-return via FindByValue. Same
        // force_rebuild_years_for_db as the zone-map call above -- a
        // WAL-replayed year needs the identical unconditional-rebuild
        // treatment here (see RebuildStaleCatIdxForDb's declaration comment
        // in XStore.h for why the header-only staleness checks alone can't
        // catch that case).
        RebuildStaleCatIdxForDb(curr_db_name, p.path().string(), cfg_struct, force_rebuild_years_for_db);
    }
}

bool XStore::ZoneMapNeedsRebuild(const std::string &zmap_path, uint32_t desired_tracked_columns) {
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
                        "answered by full scan -- restart XStore to rebuild the index",
                        op, db_name, column_idx, year, index_width);
}

std::string XStore::UnindexedColumnNotice(std::string_view op, const std::string &db_name, const std::string &db_base_path,
                                           const XStore_Proto::ConfigStruct &cfg, unsigned long long start_epoch,
                                           unsigned long long end_epoch, int32_t column_idx) {
    if (column_idx < 0 || static_cast<uint32_t>(column_idx) >= EffectiveTrackedColumnCount(cfg)) return "";

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
        std::optional<uint32_t> warm_width = PeekOpenZoneMapWidth(db_base_path, curr_ts.year);
        if (warm_width.has_value()) {
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
    std::string data_path = fmt::format("{}/{}.XSTORE", db_base_path, year);
    if (!std::filesystem::exists(data_path)) return;

    // Force a genuine cache miss for this (db_base_path, year) key before
    // reopening -- Task 6 fix. GetOrOpenZoneMap's registry never re-Opens()
    // an already-warm entry no matter what tracked_column_count is passed
    // (see its comment in XStore.h), and this key is NOT guaranteed cold
    // here even on a genuine server restart: InitDB()'s WAL replay (which
    // runs before RebuildStaleZoneMapsForDb, per the loop above) applies
    // any un-checkpointed record for this exact (db, year) first, via
    // ApplyWalRecord's own GetOrOpenZoneMap call -- and that call always
    // requests the global default, never this DB's real
    // EffectiveTrackedColumnCount. Left alone, that first touch would warm
    // the registry at the stale width, and this function's own (correctly
    // wide) GetOrOpenZoneMap call below would then just hand back that
    // stale cached instance without ever reaching ZoneMapFile::Open() --
    // silently defeating Task 6's whole fix to Open() (widening only
    // happens there) and leaving ZoneMapNeedsRebuild re-flagging this file
    // as stale on every subsequent InitDB() run, forever. Erasing here
    // guarantees GetOrOpenZoneMap below always does a real disk-backed
    // Open() with the authoritative tracked_column_count, regardless of
    // what touched this key first. Only closes an fd (write-through,
    // Task 2) -- never loses durable data.
    {
        std::string registry_key = fmt::format("{}/{}", db_base_path, year);
        std::lock_guard<std::mutex> registry_lock(zone_map_registry_mutex_);
        zone_map_registry_.erase(registry_key);
        zone_map_registry_lru_.erase(
            std::remove(zone_map_registry_lru_.begin(), zone_map_registry_lru_.end(), registry_key),
            zone_map_registry_lru_.end());
    }

    // Force a genuine cold start on disk too, regardless of which direction
    // any existing header's tracked_column_count mismatches
    // tracked_column_count (the caller here always passes the current,
    // authoritative desired_tracked_columns -- freshly read from the .CFG
    // this same InitDB() pass just loaded). ZoneMapFile::Open()'s own
    // width-mismatch guard (`tracked_column_count > header_.
    // tracked_column_count`) is deliberately asymmetric FOR THE ORDINARY
    // INSERT PATH (XStore_UnaryOps.cpp et al.), which always opens with the
    // global default width and must never truncate a file some earlier
    // rebuild already widened past that default (Task 6) -- but that
    // asymmetry is wrong for THIS caller. A too-WIDE existing file here is
    // reachable with zero concurrency at all, not just under a racing
    // insert: every insert path builds this row's ZoneMapObservations and
    // calls GetOrOpenZoneMap/UpdateRow BEFORE calling MaybeGrowColumnCount
    // (see the call order in XStore_UnaryOps.cpp/XStore_BatchOps.cpp/
    // XStore_RangeOps.cpp) -- so a DB created without a column_count_seed
    // has its very first insert's zone map created at the (wide) global
    // default, and only AFTER that same insert does MaybeGrowColumnCount
    // persist the DB's real, narrower columnCount. A plain restart right
    // after that single insert then reaches this function with
    // header.tracked_column_count > desired tracked_column_count, which
    // Open()'s `>` check alone reads as "wide enough, keep it" and leaves
    // the existing (already-correct) stats in place -- so the raw scan
    // below would silently DOUBLE every row's stats by folding them again
    // on top of what's already durably there, corrupting future pushdown-
    // eligible queries against this exact block (a real-scan-fallback query
    // would still read correct raw bytes and mask this, which is how this
    // was found: via a zone-map-stats check, not an aggregate-result one).
    // Truncating unconditionally here is always safe for this caller
    // specifically -- RebuildOneYearZoneMap is already committed to a full
    // raw-data rescan below regardless of what Open() would have done, so
    // it never needs to preserve whatever was on disk before it runs.
    {
        std::string zmap_path_to_reset = fmt::format("{}/{}.zmap", db_base_path, year);
        int reset_fd = open(zmap_path_to_reset.c_str(), O_WRONLY);
        if (reset_fd >= 0) {
            if (ftruncate(reset_fd, 0) != 0) {
                spdlog::error("[ZONE MAP REBUILD] [{}] - Failed to truncate before rebuild", zmap_path_to_reset);
            }
            close(reset_fd);
        }
        // else: no pre-existing file to reset -- GetOrOpenZoneMap below
        // creates it fresh, same as ever.
    }

    // GetOrOpenZoneMap -> ZoneMapFile::Open() -- guaranteed both a genuine
    // cache miss (eviction above) AND a genuine empty-file cold start
    // (truncate above), so every row folded below starts from a genuinely
    // clean, correctly-sized baseline no matter which direction the old
    // header's width was wrong in.
    ZoneMapFile &zmap = GetOrOpenZoneMap(db_base_path, year, row_length, tracked_column_count);
    // The truncate above wiped the old dirty flag. Mark the fresh (empty) index dirty before anything can fail, so
    // every early exit below (data open, first read) leaves it dirty; only MarkClean at the end clears it.
    zmap.MarkDirty();

    uintmax_t file_size = std::filesystem::file_size(data_path);
    uint64_t total_rows = file_size / row_length; // exact by construction -- data files are preallocated to whole-row multiples

    int fd = open(data_path.c_str(), O_RDONLY);
    if (fd < 0) {
        spdlog::error("[ZONE MAP REBUILD] [{}/{}.XSTORE] - Unable to open for rebuild scan", db_base_path, year);
        return;
    }

    // Hole-skipping chunked scan: a year file is mostly sparse holes, and a
    // pread per row made this rebuild syscall-bound (see XStore_RowScan.h).
    bool scan_complete = ForEachDataRow(fd, total_rows, row_length, [&](uint64_t row_index, const char *row_buf) {
        if (row_buf[0] == 0) return; // never-written sparse hole -- same fast skip ProcessFileRangeAccumulate uses

        size_t real_len = XStoreWireEncode::RealRowDataLength(row_buf, row_length);
        XStore_Proto::RowData row;
        if (real_len > 0) {
            row.ParseFromArray(row_buf, static_cast<int>(real_len));
        }
        std::vector<ZoneMapObservation> observations = BuildZoneMapObservations(*this, row, tracked_column_count);
        zmap.UpdateRow(row_index, observations, tracked_column_count);
    });
    // Durability A (DD4): data file first, then clear the index's dirty flag --
    // the index must never claim "clean" while the rows it summarizes could
    // still be lost. Any failure leaves it dirty, so the next start rebuilds.
    // `zmap` is the registry-open instance, so its dirty_on_disk_ stays in
    // step with the header it just cleared.
    if (!scan_complete) {
        spdlog::error("[ZONE MAP REBUILD] [{}/{}.XSTORE] - Incomplete scan; leaving zone map dirty", db_base_path, year);
    } else if (fdatasync(fd) != 0) {
        spdlog::error("[ZONE MAP REBUILD] [{}/{}.XSTORE] - fdatasync of data file failed; leaving zone map dirty", db_base_path, year);
    } else if (!zmap.MarkClean()) {
        spdlog::error("[ZONE MAP REBUILD] [{}/{}.zmap] - MarkClean failed; leaving zone map dirty", db_base_path, year);
    }
    close(fd);

    spdlog::info("[ZONE MAP REBUILD] [{}/{}.XSTORE] - Rebuilt {} rows", db_base_path, year, total_rows);
}

void XStore::RebuildStaleZoneMapsForDb(const std::string &db_name, const std::string &db_base_path, const XStore_Proto::ConfigStruct &cfg_struct,
                                        const std::set<unsigned short> &force_rebuild_years) {
    uint32_t desired_tracked_columns = EffectiveTrackedColumnCount(cfg_struct);
    uint16_t row_length = static_cast<uint16_t>(cfg_struct.rowlength());

    std::vector<unsigned short> years_needing_rebuild;
    for (auto &f : std::filesystem::directory_iterator(db_base_path)) {
        std::string filename = f.path().filename().string();
        if (filename.size() < 7 || filename.substr(filename.size() - 7) != ".XSTORE") continue;

        std::string year_str = filename.substr(0, filename.size() - 7);
        unsigned short year;
        try {
            year = static_cast<unsigned short>(std::stoi(year_str));
        } catch (...) {
            continue; // not a <YEAR>.XSTORE-shaped filename -- skip
        }

        // force_rebuild_years short-circuits the header check entirely --
        // a year WAL replay just wrote rows into has a zone map that is
        // *structurally* fine (right version, right width, so
        // ZoneMapNeedsRebuild says "no") and *semantically* short by
        // exactly those rows, because a replayed row carries no
        // observations. Only a full re-derivation from raw data can tell,
        // and RebuildOneYearZoneMap is idempotent (truncate, then re-fold
        // every row), so forcing it on an already-correct year costs time,
        // never correctness. See ReplayedYearMap in XStore.h.
        std::string zmap_path = fmt::format("{}/{}.zmap", db_base_path, year);
        if (force_rebuild_years.count(year) > 0 || ZoneMapNeedsRebuild(zmap_path, desired_tracked_columns)) {
            years_needing_rebuild.push_back(year);
        }
    }

    if (years_needing_rebuild.empty()) return;

    spdlog::info("[ZONE MAP REBUILD] [DB: {}] - {} year file(s) need rebuild", db_name, years_needing_rebuild.size());
    XStorePreadPool::ParallelFor(years_needing_rebuild.size(), [&](size_t i) {
        RebuildOneYearZoneMap(db_base_path, years_needing_rebuild[i], row_length, desired_tracked_columns);
    });
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
