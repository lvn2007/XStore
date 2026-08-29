#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unordered_set>

std::array<XStore::CfgShard, XStore::CFG_SHARD_COUNT> XStore::cfg_shards;

// Initialization
XStore::XStore() {
    // WAL writer threads are process-wide static state (wal_shards is a
    // static array, see XStore.h) -- started exactly once per process,
    // independent of how many distinct data_store_dirs get initialized
    // below (unlike InitDB, which is per-directory).
    static std::once_flag wal_writers_once;
    std::call_once(wal_writers_once, &XStore::StartWalWriters);

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

XStore_Proto::SysInfo_REP XStore::InfoQuery(std::string_view server_addr, int thread_count) {
    // Protobuf obj to be returned
    XStore_Proto::SysInfo_REP to_return;

    // Overview info
    to_return.set_serveraddr(server_addr.data(), server_addr.size());
    to_return.set_threadcount(thread_count);

    // Loop through all db available in DataStore
    for (auto& dir : std::filesystem::directory_iterator(data_store_dir)) {
        std::string curr_db_name = dir.path().filename().string();
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
        }

        // Size of all data files in current DB
        unsigned long int all_size = 0;
        struct stat stat_struct;

        // Loop through all files in currentDBDir
        for (auto& db_dir : std::filesystem::directory_iterator(curr_db_dir)) {
            // Current file name & path in dbDir
            std::string curr_file_name = db_dir.path().filename().string();
            stat(JoinPath(curr_db_dir, curr_file_name).c_str(), &stat_struct);

            // Number of 512B blocks allocated
            all_size += stat_struct.st_blocks * 512;
            curr_db->add_filename(curr_file_name);
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
        // data_store_dir (see WalFilePathFor) -- it holds shard_<N>.wal
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

namespace {
// Same wire-format parse as XStore_Wal.cpp's ParseWalRecord -- duplicated
// here (rather than shared via a header) because it's a small, self-
// contained ~25-line function and the alternative (a shared header just for
// this one helper) isn't worth it for a single caller in each translation
// unit. Keep the two in sync if the wire format changes.
bool ParseWalRecordForReplay(const char *data, size_t len, size_t &consumed, XStore::WalRecord &out_record) {
    if (len < 8) return false;
    uint32_t record_len;
    std::memcpy(&record_len, data, 4);
    if (len < 4u + record_len || record_len < 4u + 2 + 2 + 4 + 4) return false;

    uint32_t stored_crc;
    std::memcpy(&stored_crc, data + 4, 4);

    const char *payload = data + 8;
    size_t payload_len = record_len - 4;

    // CRC-32 (IEEE 802.3 / zlib polynomial), same algorithm as
    // XStore_Wal.cpp's Crc32 -- recomputed here rather than shared for the
    // same reason as this function itself.
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            t[i] = c;
        }
        return t;
    }();
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < payload_len; ++i) {
        crc = table[(crc ^ static_cast<unsigned char>(payload[i])) & 0xFFu] ^ (crc >> 8);
    }
    crc ^= 0xFFFFFFFFu;
    if (crc != stored_crc) return false;

    size_t off = 0;
    uint16_t db_name_len; std::memcpy(&db_name_len, payload + off, 2); off += 2;
    uint16_t row_length;  std::memcpy(&row_length, payload + off, 2); off += 2;
    int32_t time_res;     std::memcpy(&time_res, payload + off, 4); off += 4;
    uint32_t row_count;   std::memcpy(&row_count, payload + off, 4); off += 4;

    if (off + db_name_len > payload_len) return false;
    out_record.db_name.assign(payload + off, db_name_len);
    off += db_name_len;

    size_t expected_rows_bytes = static_cast<size_t>(row_count) * (8u + row_length);
    if (off + expected_rows_bytes != payload_len) return false;

    out_record.row_length = row_length;
    out_record.time_resolution = static_cast<XStore_Proto::TimeUnit>(time_res);
    out_record.rows.clear();
    out_record.rows.reserve(row_count);
    for (uint32_t i = 0; i < row_count; ++i) {
        uint64_t epoch_time; std::memcpy(&epoch_time, payload + off, 8); off += 8;
        XStore::WalRow row;
        row.epoch_time = epoch_time;
        row.row_bytes.assign(payload + off, row_length);
        off += row_length;
        out_record.rows.push_back(std::move(row));
    }

    consumed = 4 + record_len;
    return true;
}
} // namespace

void XStore::ReplayWal() {
    std::filesystem::path wal_dir = std::filesystem::path(data_store_dir) / XStoreTunables::WAL_DIR_NAME;
    if (!std::filesystem::is_directory(wal_dir)) {
        return;
    }

    for (size_t shard_idx = 0; shard_idx < XStoreTunables::WAL_SHARD_COUNT; ++shard_idx) {
        std::filesystem::path wal_path = wal_dir / ("shard_" + std::to_string(shard_idx) + ".wal");
        if (!std::filesystem::exists(wal_path) || std::filesystem::file_size(wal_path) == 0) {
            continue;
        }

        std::ifstream in(wal_path, std::ios::binary);
        std::string contents((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
        in.close();

        size_t offset = 0;
        unsigned replayed_count = 0;
        while (offset < contents.size()) {
            XStore::WalRecord record;
            size_t consumed = 0;
            if (!ParseWalRecordForReplay(contents.data() + offset, contents.size() - offset, consumed, record)) {
                // Torn tail record (crash mid-write) or corruption -- stop
                // here; every record before this point already passed its
                // own CRC check.
                break;
            }
            record.db_base_path = DbBasePath(record.db_name);
            ApplyWalRecord(record);
            replayed_count += static_cast<unsigned>(record.rows.size());
            offset += consumed;
        }

        if (replayed_count > 0) {
            spdlog::info("[WAL REPLAY] - Replayed {} record(s) from {}", replayed_count, wal_path.string());
        }

        // Checkpoint -- everything in this file has now been reapplied,
        // truncate so a future crash-free restart doesn't redo this work.
        std::ofstream(wal_path, std::ios::binary | std::ios::trunc).close();
    }
}

std::string XStore::GetBaseDir() {
    // The XStore binary is expected to run with its working directory set to
    // the directory it lives in (e.g. /home/ubuntu/XStore/), with DataStore/
    // living alongside it as a direct child -- so base_dir is just cwd, not
    // cwd's parent. No trailing '/' -- see the convention note in XStore.h.
    return std::filesystem::current_path().string();
}
