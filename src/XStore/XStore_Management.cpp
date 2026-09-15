#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
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
    static std::mutex init_mutex;
    static std::unordered_set<std::string> initialized_dirs;

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

    shard.map[db_name] = cfg_out;
    return true;
}

void XStore::PutCfg(const std::string &db_name, const XStore_Proto::ConfigStruct &cfg) {
    CfgShard &shard = cfg_shards[std::hash<std::string>{}(db_name) % CFG_SHARD_COUNT];
    std::unique_lock write_lock(shard.mutex);
    shard.map[db_name] = cfg;
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

ZoneMapFile& XStore::GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length) {
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
    if (!zmap->Open(path, row_length)) {
        spdlog::error("[ZONEMAP] - Unable to open {}", path);
        throw std::runtime_error(fmt::format("ZoneMapFile::Open failed for {}", path));
    }
    ZoneMapFile &ref = *zmap;
    zone_map_registry_.emplace(key, std::move(zmap));
    zone_map_registry_lru_.push_back(key);
    return ref;
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

XStore_Proto::CreateDB_REP XStore::CreateDB(std::string_view db_name_view, XStore_Proto::TimeUnit time_resolution, short int row_length) {
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

    // Recursively remove entire DB with given DB Name
    std::filesystem::remove_all(db_loc);

    // Remove DB from cfgMap
    EraseCfg(db_name);

    // Drop & unmap any cached UnaryInsert mappings under this DB -- otherwise
    // a future insert (via GetOrMapFile) could keep writing into an orphaned,
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
            // Drop this file's cached UnaryInsert mapping, if any -- same
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
    ReplayWal();

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
        PutCfg(curr_db_name, cfg_struct);
    }
}

void XStore::ReplayWal() {
    std::filesystem::path wal_dir = std::filesystem::path(data_store_dir) / XStoreTunables::WAL_DIR_NAME;
    if (!std::filesystem::is_directory(wal_dir)) {
        return;
    }

    for (size_t shard_idx = 0; shard_idx < XStoreTunables::WAL_SHARD_COUNT; ++shard_idx) {
        std::filesystem::path wal_path = wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL");
        // No file_size()==0 fast-skip: shard files are now preallocated
        // (see OpenAndPreallocateWalFile) and stay at that fixed size even
        // when logically empty, so a real "nothing pending" file no longer
        // reads as size 0 -- ParseWalRecord's CRC check below correctly
        // and cheaply finds nothing on the first attempt instead.
        if (!std::filesystem::exists(wal_path)) {
            continue;
        }

        // Stat purely to hand to ReclaimWalFileSize afterward (it needs to
        // know whether this file grew past default) -- no longer used to
        // gate whether replay happens at all, see the streaming read below.
        std::error_code file_size_ec;
        uintmax_t on_disk_bytes = std::filesystem::file_size(wal_path, file_size_ec);
        if (file_size_ec) {
            spdlog::error("[WAL REPLAY] - Unable to stat {}: {}", wal_path.string(), file_size_ec.message());
            continue;
        }

        std::ifstream in(wal_path, std::ios::binary);
        if (!in) {
            spdlog::error("[WAL REPLAY] - Unable to open {}", wal_path.string());
            continue;
        }

        // Streams this file's records in bounded WAL_REPLAY_READ_CHUNK_BYTES
        // chunks rather than loading the whole file into one std::string --
        // a shard file's on-disk size is no longer a trustworthy bound for a
        // single allocation (see WAL_REPLAY_MAX_RECORD_BYTES's comment in
        // XStore_Tunables.h for why). buffer only ever holds "a chunk or two
        // plus one in-flight record's worth of bytes", grown on demand and
        // shrunk (erase) as each record is consumed, never proportional to
        // on_disk_bytes.
        std::string buffer;
        auto ensure_available = [&](size_t needed) -> bool {
            while (buffer.size() < needed) {
                size_t old_size = buffer.size();
                buffer.resize(old_size + XStoreTunables::WAL_REPLAY_READ_CHUNK_BYTES);
                in.read(buffer.data() + old_size, static_cast<std::streamsize>(XStoreTunables::WAL_REPLAY_READ_CHUNK_BYTES));
                std::streamsize got = in.gcount();
                buffer.resize(old_size + static_cast<size_t>(got));
                if (got <= 0) return false; // true EOF -- can't satisfy `needed`
            }
            return true;
        };

        unsigned replayed_count = 0;
        while (true) {
            if (!ensure_available(8)) {
                // Fewer than 8 bytes left anywhere in the file -- clean end
                // or torn tail, same as before.
                break;
            }
            uint32_t declared_record_len;
            std::memcpy(&declared_record_len, buffer.data(), 4);
            if (declared_record_len > XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES) {
                // Checked BEFORE trying to satisfy it -- a corrupt/bogus
                // record_len can't force ensure_available to keep growing
                // the buffer chasing it. Same "stop here" outcome as a torn
                // or CRC-invalid record below, just caught earlier.
                spdlog::error("[WAL REPLAY] - {} has a record declaring {} bytes, exceeding the "
                              "{}-byte single-record replay sanity ceiling -- stopping replay of "
                              "this shard here; check for corruption.",
                              wal_path.string(), declared_record_len,
                              XStoreTunables::WAL_REPLAY_MAX_RECORD_BYTES);
                break;
            }
            if (!ensure_available(4 + static_cast<size_t>(declared_record_len))) {
                // Torn tail: file ends before this record's declared length
                // is satisfied.
                break;
            }

            XStore::WalRecord record;
            size_t consumed = 0;
            if (!ParseWalRecord(buffer.data(), buffer.size(), consumed, record)) {
                // CRC/structure failure -- genuine corruption/torn record;
                // every record before this point already passed its own CRC
                // check, and already-applied.
                break;
            }
            record.db_base_path = DbBasePath(record.db_name);
            if (!ApplyWalRecord(record)) {
                spdlog::error("[WAL REPLAY] [DB: {}] - One or more rows failed to apply", record.db_name);
            }
            replayed_count += static_cast<unsigned>(record.rows.size());
            buffer.erase(0, consumed);
        }
        in.close();

        if (replayed_count > 0) {
            spdlog::info("[WAL REPLAY] - Replayed {} record(s) from {}", replayed_count, wal_path.string());
        }

        // Checkpoint unconditionally, whether or not any record was found
        // and regardless of individual ApplyWalRecord outcomes -- this
        // file's WAL evidence has now been fully consulted. Critically
        // NOT conditioned on apply success: a record here can legitimately
        // fail to apply because its DB no longer exists (deleted, or -- as
        // here -- not yet recreated at this point in startup, since
        // ReplayWal always runs before any client can reach CreateDB), and
        // that failure will recur identically on every future restart
        // unless the stale bytes are invalidated now. No file-size
        // change, so no metadata-journal cost -- see
        // InvalidateWalFileHeader.
        int fd = open(wal_path.c_str(), O_WRONLY);
        if (fd >= 0) {
            InvalidateWalFileHeader(fd, wal_path.string());
            // Now that this file's WAL evidence is fully consulted, shrink
            // it back to default if a prior oversized BatchInsert/RangeInsert
            // pass grew it -- see ReclaimWalFileSize's comment in XStore.h
            // for why this is the one safe place to pay that ftruncate cost.
            ReclaimWalFileSize(fd, wal_path.string(), on_disk_bytes);
            close(fd);
        }
    }
}

std::string XStore::GetBaseDir() {
    // The XStore binary is expected to run with its working directory set to
    // the directory it lives in (e.g. /home/ubuntu/XStore/), with DataStore/
    // living alongside it as a direct child -- so base_dir is just cwd, not
    // cwd's parent. No trailing '/' -- see the convention note in XStore.h.
    return std::filesystem::current_path().string();
}
