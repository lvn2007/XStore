#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <unordered_set>

std::array<XStore::CfgShard, XStore::CfgShardCount> XStore::CfgShards;

// Initialization
XStore::XStore() {
    // One XStore instance is constructed per worker thread (see ZMQ_Server),
    // and in production every one of them shares the same DataStoreDir for
    // the life of the process -- so creating it and scanning it into the
    // shared CfgMap only needs to happen once per distinct DataStoreDir, not
    // once per thread. Keyed by the path itself (rather than a single
    // once_flag) so this still does real work again for a genuinely
    // different DataStoreDir, which is what lets tests construct many
    // XStore instances against many distinct scratch directories within one
    // process.
    static std::mutex InitMutex;
    static std::unordered_set<std::string> InitializedDirs;

    std::lock_guard<std::mutex> Lock(InitMutex);
    if (InitializedDirs.insert(DataStoreDir).second) {
        std::filesystem::create_directories(DataStoreDir);
        InitDB();
    }
}

bool XStore::GetOrLoadCfg(const std::string &dbName, const std::string &dbBasePath, XStore_Proto::ConfigStruct &cfgOut) {
    CfgShard &Shard = CfgShards[std::hash<std::string>{}(dbName) % CfgShardCount];

    // Fast path: another thread may have already cached this config
    {
        std::shared_lock ReadLock(Shard.Mutex);
        auto It = Shard.Map.find(dbName);
        if (It != Shard.Map.end()) {
            cfgOut = It->second;
            return true;
        }
    }

    // Cache miss -> take exclusive lock & re-check (another thread may have
    // loaded it while we were waiting for the lock)
    std::unique_lock WriteLock(Shard.Mutex);
    auto It = Shard.Map.find(dbName);
    if (It != Shard.Map.end()) {
        cfgOut = It->second;
        return true;
    }

    std::string CfgPath = JoinPath(dbBasePath, dbName + ".CFG");
    if (!std::filesystem::exists(CfgPath)) {
        return false;
    }

    FILE *CFile = std::fopen(CfgPath.c_str(), "rb");
    if (CFile == nullptr) {
        return false;
    }
    cfgOut.ParseFromFileDescriptor(fileno(CFile));
    std::fclose(CFile);

    Shard.Map[dbName] = cfgOut;
    return true;
}

void XStore::PutCfg(const std::string &dbName, const XStore_Proto::ConfigStruct &cfg) {
    CfgShard &Shard = CfgShards[std::hash<std::string>{}(dbName) % CfgShardCount];
    std::unique_lock WriteLock(Shard.Mutex);
    Shard.Map[dbName] = cfg;
}

void XStore::EraseCfg(const std::string &dbName) {
    CfgShard &Shard = CfgShards[std::hash<std::string>{}(dbName) % CfgShardCount];
    std::unique_lock WriteLock(Shard.Mutex);
    Shard.Map.erase(dbName);
}

std::string XStore::DbBasePath(const std::string &dbName) const {
    return JoinPath(DataStoreDir, dbName);
}

const std::string& XStore::CachedDbBasePath(const std::string &dbName) const {
    // Keyed by (DataStoreDir, dbName), not dbName alone: DataStoreDir is
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
    thread_local std::unordered_map<std::string, std::unordered_map<std::string, std::string>> TlsBasePaths;
    std::unordered_map<std::string, std::string> &DirMap = TlsBasePaths[DataStoreDir];
    auto It = DirMap.find(dbName);
    if (It != DirMap.end()) {
        return It->second;
    }
    return DirMap.emplace(dbName, DbBasePath(dbName)).first->second;
}

const std::string& XStore::CachedYearFilePath(const std::string &dbBasePath, unsigned short year) const {
    thread_local std::unordered_map<std::string, std::unordered_map<unsigned short, std::string>> TlsYearPaths;
    std::unordered_map<unsigned short, std::string> &YearMap = TlsYearPaths[dbBasePath];
    auto It = YearMap.find(year);
    if (It != YearMap.end()) {
        return It->second;
    }
    return YearMap.emplace(year, fmt::format("{}/{}.XSTORE", dbBasePath, year)).first->second;
}

XStore_Proto::SysInfo_REP XStore::InfoQuery(std::string serverAddr, int threadCount) {
    // Protobuf obj to be returned
    XStore_Proto::SysInfo_REP ToReturn;

    // Overview info
    ToReturn.set_serveraddr(serverAddr);
    ToReturn.set_threadcount(threadCount);

    // Loop through all db available in DataStore
    for (auto& dir : std::filesystem::directory_iterator(DataStoreDir)) {
        std::string CurrDbName = dir.path().filename().string();
        std::string CurrDbDir = JoinPath(DataStoreDir, CurrDbName);

        // Set DB Name
        XStore_Proto::DbStats *CurrDb = ToReturn.add_dbinfo();
        CurrDb->set_dbname(CurrDbName);

        // Fetch config of a target DB -- via the shared cache every other
        // operation already uses, instead of a fresh open+parse per DB on
        // every InfoQuery call (a status/monitoring endpoint, likely polled
        // repeatedly).
        XStore_Proto::ConfigStruct CfgStruct;
        if (GetOrLoadCfg(CurrDbName, CurrDbDir, CfgStruct)) {
            // Set DB Resolution
            CurrDb->set_dbresolution(CfgStruct.timeresolution());
        }

        // Size of all data files in current DB
        unsigned long int AllSize = 0;
        struct stat StatStruct;

        // Loop through all files in currentDBDir
        for (auto& dbDir : std::filesystem::directory_iterator(CurrDbDir)) {
            // Current file name & path in dbDir
            std::string CurrFileName = dbDir.path().filename().string();
            stat(JoinPath(CurrDbDir, CurrFileName).c_str(), &StatStruct);

            // Number of 512B blocks allocated
            AllSize += StatStruct.st_blocks * 512;
            CurrDb->add_filename(CurrFileName);
        }
        CurrDb->set_totalsizeinbyte(AllSize);
    }

    return ToReturn;
}

XStore_Proto::CreateDB_REP XStore::CreateDB(std::string_view dbNameView, XStore_Proto::TimeUnit timeResolution, short int rowLength) {
    // Protobuf obj to be returned
    XStore_Proto::CreateDB_REP ToReturn;
    std::string CreateDbStatus;

    // Ensure dbName & timeResolution string is all capitalized. dbNameView
    // is a trivial pointer+length pass at the call boundary (no
    // std::string construction/copy there); UpperCaseInto does the one
    // read-uppercase-write pass into a reused thread_local buffer instead
    // of the old by-value-copy-then-transform-in-place's two passes.
    const std::string &dbName = UpperCaseInto(dbNameView);

    if (rowLength < 4) {
        CreateDbStatus = fmt::format("[CREATE DB] [DB: {}] - Row length must be >= 4", dbName);
        SetTaggedStatus(ToReturn.mutable_createdbstatus(), "[WARNING] ", CreateDbStatus);
        spdlog::warn(CreateDbStatus);
        return ToReturn;
    }

    // Target directory of a given DB name
    std::string TargetPath = DbBasePath(dbName);

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(TargetPath) == 0) {
        // Check if within permitted time resolution bounds
        if (std::find(std::begin(PermittedTimeResolution), std::end(PermittedTimeResolution), XStore_Proto::TimeUnit_Name(timeResolution)) == std::end(PermittedTimeResolution)) {
            CreateDbStatus = fmt::format("[CREATE DB] [DB: {}] - Invalid time resolution", dbName);
            SetTaggedStatus(ToReturn.mutable_createdbstatus(), "[WARNING] ", CreateDbStatus);
            spdlog::warn(CreateDbStatus);
            return ToReturn;
        }

        // Create DB directory
        if (std::filesystem::create_directory(TargetPath) == 1) {
            std::string SuccessMsg = fmt::format("[CREATE DB] [DB: {}] - Created DB", dbName);
            SetTaggedStatus(ToReturn.mutable_createdbstatus(), "[INFO] ", SuccessMsg);

            // Dumps DB configurations info to disk
            //  Organize configs into a struct
            XStore_Proto::ConfigStruct CfgStruct;
            CfgStruct.set_dbname(dbName);
            CfgStruct.set_timeresolution(timeResolution);
            CfgStruct.set_rowlength(rowLength);

            // Writing cfgStruct to file
            FILE *BFile;
            std::string CfgPath  = JoinPath(TargetPath, dbName + ".CFG");
            BFile = std::fopen(CfgPath.c_str(), "wb+");

            {
                // Lock prior to writing
                FileRangeLockGuard Guard(CfgPath, 0, CfgStruct.ByteSizeLong(), FileRangeLock::LockType::WRITE);

                // Writing actual data to disk
                CfgStruct.SerializeToFileDescriptor(fileno(BFile));
            }
            //  Close file after write
            std::fclose(BFile);

            //  Add this cfgStruct to cfgMap
            PutCfg(dbName, CfgStruct);

            spdlog::info(SuccessMsg);
            return ToReturn;
        }
    }

    CreateDbStatus = fmt::format("[CREATE DB] [DB: {}] - Already existed", dbName);
    SetTaggedStatus(ToReturn.mutable_createdbstatus(), "[WARNING] ", CreateDbStatus);
    spdlog::warn(CreateDbStatus);
    return ToReturn;
}

XStore_Proto::RemoveDB_REP XStore::RemoveDB(std::string_view dbNameView) {
    // Protobuf obj to be returned
    std::string RemoveStatus;
    XStore_Proto::RemoveDB_REP ToReturn;

    // Ensure dbName string is all capitalized
    const std::string &dbName = UpperCaseInto(dbNameView);

    // Location of DB
    std::string DbLoc = JoinPath(DataStoreDir, dbName);

    // IF DB does NOT exist
    //  -> Return remove error
    if (!std::filesystem::exists(DbLoc)) {
        RemoveStatus = fmt::format("[REMOVE DB] [DB: {}] - DB does not exists", dbName);
        SetTaggedStatus(ToReturn.mutable_removestatus(), "[WARNING] ", RemoveStatus);
        spdlog::warn(RemoveStatus);
        return ToReturn;
    }

    // Recursively remove entire DB with given DB Name
    std::filesystem::remove_all(DbLoc);

    // Remove DB from cfgMap
    EraseCfg(dbName);

    // Drop & unmap any cached UnaryInsert mappings under this DB -- otherwise
    // a future insert (via GetOrMapFile) could keep writing into an orphaned,
    // unlinked mapping instead of a freshly-recreated DB with the same name.
    EraseMappedFilesUnderPrefix(DbLoc + "/");
    // Same reasoning for UnaryInsert's cached fds (see GetOrOpenFile).
    EraseOpenFilesUnderPrefix(DbLoc + "/");

    RemoveStatus = fmt::format("[REMOVE DB] [DB: {}] - Removed DB with name: {}", dbName, dbName);
    SetTaggedStatus(ToReturn.mutable_removestatus(), "[INFO] ", RemoveStatus);
    spdlog::info(RemoveStatus);
    return ToReturn;
}

XStore_Proto::RemoveDBFiles_REP XStore::RemoveDBFiles(XStore_Proto::RemoveDBFiles_REQ *protoFromClient, std::string_view dbNameView) {
    // Protobuf obj to be returned
    std::string RemoveStatus;
    XStore_Proto::RemoveDBFiles_REP ToReturn;

    // Ensure dbName string is all capitalized
    const std::string &dbName = UpperCaseInto(dbNameView);

    // Location of DB
    std::string DbLoc = JoinPath(DataStoreDir, dbName);

    if (!std::filesystem::exists(DbLoc)) {
        for (int i = 0; i < protoFromClient->filename_size(); i++) {
            RemoveStatus = fmt::format("[REMOVE DB FILES] [DB: {}] -- Unable to remove {}", dbName, protoFromClient->filename(i));
            SetTaggedStatus(ToReturn.add_removestatus(), "[WARNING] ", RemoveStatus)->append(protoFromClient->filename(i));
            spdlog::warn(RemoveStatus);
        }
        return ToReturn;
    }

    // Loop through each requested file name
    for (int i = 0; i < protoFromClient->filename_size(); i++) {
        // filename is client-controlled and must name a single entry directly
        // inside this DB's directory -- reject anything with a path separator
        // (e.g. "../other.CFG") so a crafted request can't escape DbLoc and
        // delete arbitrary files elsewhere on disk.
        if (protoFromClient->filename(i).find('/') != std::string::npos) {
            std::string ErrMsg = fmt::format("[REMOVE DB FILES] [DB: {}] - Invalid file name {}", dbName, protoFromClient->filename(i));
            SetTaggedStatus(ToReturn.add_removestatus(), "[WARNING] ", ErrMsg);
            spdlog::warn(ErrMsg);
            continue;
        }

        std::filesystem::path TargetPath = JoinPath(DbLoc, protoFromClient->filename(i));
        if (protoFromClient->filename(i) == fmt::format("{}.CFG", dbName)) {
            std::string WarnMsg = fmt::format("[REMOVE DB FILES] [DB: {}] - {} is prohibited from remove", dbName, protoFromClient->filename(i));
            SetTaggedStatus(ToReturn.add_removestatus(), "[WARNING] ", WarnMsg);
            spdlog::warn(WarnMsg);
            continue;
        }
        bool CurrRemove = std::filesystem::remove(TargetPath);
        if (CurrRemove) {
            // Drop this file's cached UnaryInsert mapping, if any -- same
            // reasoning as RemoveDB, scoped to a single file.
            EraseMappedFile(TargetPath.string());
            EraseOpenFile(TargetPath.string());

            std::string InfoMsg = fmt::format("[REMOVE DB FILES] [DB: {}] - Removed {}", dbName, protoFromClient->filename(i));
            SetTaggedStatus(ToReturn.add_removestatus(), "[INFO] ", InfoMsg);
            spdlog::info(InfoMsg);
        }
        else {
            std::string ErrMsg = fmt::format("[REMOVE DB FILES] [DB: {}] - Unable to remove {}", dbName, protoFromClient->filename(i));
            SetTaggedStatus(ToReturn.add_removestatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
        }
    }
    return ToReturn;
}

void XStore::InitDB() {
    // Loop through each dbName
    for (auto& p : std::filesystem::directory_iterator(DataStoreDir))
    {
        // dbName is just the last path component -- reading it off the
        // filesystem::path itself instead of stripping a known-length
        // DataStoreDir prefix means this doesn't care whether DataStoreDir
        // carries a trailing '/' or not.
        std::string CurrDbName = p.path().filename().string();

        // Open & Read cfgStruct of all DB currently exists
        std::string CfgFilePath = JoinPath(p.path().string(), CurrDbName + ".CFG");

        XStore_Proto::ConfigStruct CfgStruct;
        FILE *CFile = std::fopen(CfgFilePath.c_str(), "rb");
        if (CFile == nullptr) {
            spdlog::error("[INIT DB] [DB: {}] - Unable to open {}", CurrDbName, CfgFilePath);
            continue;
        }
        CfgStruct.ParseFromFileDescriptor(fileno(CFile));
        std::fclose(CFile);
        PutCfg(CurrDbName, CfgStruct);
    }
}

std::string XStore::GetBaseDir() {
    // The XStore binary is expected to run with its working directory set to
    // the directory it lives in (e.g. /home/ubuntu/XStore/), with DataStore/
    // living alongside it as a direct child -- so BaseDir is just cwd, not
    // cwd's parent. No trailing '/' -- see the convention note in XStore.h.
    return std::filesystem::current_path().string();
}
