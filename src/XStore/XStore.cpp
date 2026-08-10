#include "include/XStore/XStore.h"

std::map<std::string, XStore_Proto::ConfigStruct> XStore::CfgMap;
std::shared_mutex XStore::CfgMutex;

std::list<std::pair<std::string, XStore::MappedFile>> XStore::MmapLru;
std::unordered_map<std::string, std::list<std::pair<std::string, XStore::MappedFile>>::iterator> XStore::MmapIndex;
std::mutex XStore::MmapMutex;

// Initialization
XStore::XStore() {
    // Initialize DB
    InitDB();

    // First, check if there's any existing data exists
//    HasDB();
}

bool XStore::GetOrLoadCfg(const std::string &dbName, const std::string &dbBasePath, XStore_Proto::ConfigStruct &cfgOut) {
    // Fast path: another thread may have already cached this config
    {
        std::shared_lock ReadLock(CfgMutex);
        auto It = CfgMap.find(dbName);
        if (It != CfgMap.end()) {
            cfgOut = It->second;
            return true;
        }
    }

    // Cache miss -> take exclusive lock & re-check (another thread may have
    // loaded it while we were waiting for the lock)
    std::unique_lock WriteLock(CfgMutex);
    auto It = CfgMap.find(dbName);
    if (It != CfgMap.end()) {
        cfgOut = It->second;
        return true;
    }

    std::string CfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);
    if (!std::filesystem::exists(CfgPath)) {
        return false;
    }

    FILE *CFile = std::fopen(CfgPath.c_str(), "rb");
    if (CFile == nullptr) {
        return false;
    }
    cfgOut.ParseFromFileDescriptor(fileno(CFile));
    std::fclose(CFile);

    CfgMap[dbName] = cfgOut;
    return true;
}

void XStore::PutCfg(const std::string &dbName, const XStore_Proto::ConfigStruct &cfg) {
    std::unique_lock WriteLock(CfgMutex);
    CfgMap[dbName] = cfg;
}

void XStore::EraseCfg(const std::string &dbName) {
    std::unique_lock WriteLock(CfgMutex);
    CfgMap.erase(dbName);
}

void* XStore::GetOrMapFile(const std::string &targetFile, size_t rowLength, unsigned long long totalRows) {
    std::lock_guard<std::mutex> Lock(MmapMutex);

    auto it = MmapIndex.find(targetFile);
    if (it != MmapIndex.end()) {
        // Cache hit -> promote to MRU front
        MmapLru.splice(MmapLru.begin(), MmapLru, it->second);
        return it->second->second.Base;
    }

    // Cache miss -> open (create if needed), ensure size, map, cache
    int Fd = open(targetFile.c_str(), O_RDWR | O_CREAT, 0644);
    if (Fd < 0) {
        return nullptr;
    }

    // +1 row: totalRows is (LastEpoch - FirstEpoch), but a write at
    // unixTime == LastEpoch (the year's final second, inclusive) lands at
    // offset totalRows*rowLength -- one row past that span. The original
    // fwrite()-based writer silently grew the file to cover this via
    // EOF-extension; a fixed-size mapping needs the extra row up front.
    size_t FileLen = (static_cast<size_t>(totalRows) + 1) * rowLength;

    struct stat St{};
    if (fstat(Fd, &St) != 0 || static_cast<size_t>(St.st_size) < FileLen) {
        if (ftruncate(Fd, FileLen) != 0) {
            close(Fd);
            return nullptr;
        }
    }

    void *Base = mmap(NULL, FileLen, PROT_READ | PROT_WRITE, MAP_SHARED, Fd, 0);
    // Mapping stays valid after the fd is closed -- no need to keep it open.
    close(Fd);
    if (Base == MAP_FAILED) {
        return nullptr;
    }

    MmapLru.emplace_front(targetFile, MappedFile{Base, FileLen});
    MmapIndex[targetFile] = MmapLru.begin();

    if (MmapLru.size() > MmapCacheCap) {
        munmap(MmapLru.back().second.Base, MmapLru.back().second.Len);
        MmapIndex.erase(MmapLru.back().first);
        MmapLru.pop_back();
    }

    return Base;
}

void XStore::EraseMappedFile(const std::string &targetFile) {
    std::lock_guard<std::mutex> Lock(MmapMutex);

    auto It = MmapIndex.find(targetFile);
    if (It == MmapIndex.end()) {
        return;
    }

    munmap(It->second->second.Base, It->second->second.Len);
    MmapLru.erase(It->second);
    MmapIndex.erase(It);
}

void XStore::EraseMappedFilesUnderPrefix(const std::string &dbDirPrefix) {
    std::lock_guard<std::mutex> Lock(MmapMutex);

    for (auto It = MmapLru.begin(); It != MmapLru.end();) {
        if (It->first.compare(0, dbDirPrefix.size(), dbDirPrefix) == 0) {
            munmap(It->second.Base, It->second.Len);
            MmapIndex.erase(It->first);
            It = MmapLru.erase(It);
        } else {
            ++It;
        }
    }
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
        std::string CurrDbDir = DataStoreDir + CurrDbName;

        // Set DB Name
        XStore_Proto::DbStats *CurrDb = ToReturn.add_dbinfo();
        CurrDb->set_dbname(CurrDbName);

        // Fetch config of a target DB
        XStore_Proto::ConfigStruct CfgStruct;
        std::string CurrCfgFilePath = CurrDbDir + "/" + CurrDbName + ".CFG";

        // Check if .CFG file is exist
        if (std::filesystem::exists(CurrCfgFilePath)) {
            int CfgFile = open(CurrCfgFilePath.c_str(), O_RDONLY);
            CfgStruct.ParseFromFileDescriptor(CfgFile);

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
            stat((CurrDbDir + "/" + CurrFileName).c_str(), &StatStruct);

            // Fetch current file size & convert to GB
            unsigned int CurrFileSizeInGb = file_size(dbDir.path());

            // Number of 512B blocks allocated
            AllSize += StatStruct.st_blocks * 512;
            CurrDb->add_filename(CurrFileName);
        }
        CurrDb->set_totalsizeinbyte(AllSize);
    }

    return ToReturn;
}

XStore_Proto::CreateDB_REP XStore::CreateDB(std::string dbName, XStore_Proto::TimeUnit timeResolution, short int rowLength) {
    // Protobuf obj to be returned
    XStore_Proto::CreateDB_REP ToReturn;
    std::string CreateDbStatus;

    // Ensure dbName & timeResolution string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    if (rowLength < 4) {
        CreateDbStatus = fmt::format("[ERROR] [CREATE DB] [DB: {}] - Row length must be >= 4", dbName);
        ToReturn.set_createdbstatus(CreateDbStatus);
        spdlog::error(CreateDbStatus.substr(ERROR_LEN));
        return ToReturn;
    }

    // Target directory of a given DB name
    std::string TargetPath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(TargetPath) == 0) {
        // Check if within permitted time resolution bounds
        if (std::find(std::begin(PermittedTimeResolution), std::end(PermittedTimeResolution), XStore_Proto::TimeUnit_Name(timeResolution)) == std::end(PermittedTimeResolution)) {
            CreateDbStatus = fmt::format("[ERROR] [CREATE DB] [DB: {}] - Invalid time resolution", dbName);
            ToReturn.set_createdbstatus(CreateDbStatus);
            spdlog::error(CreateDbStatus.substr(ERROR_LEN));
            return ToReturn;
        }

        // Create DB directory
        if (std::filesystem::create_directory(TargetPath) == 1) {
            std::string SuccessMsg = fmt::format("[INFO] [CREATE DB] [DB: {}] - Created DB", dbName);
            ToReturn.set_createdbstatus(SuccessMsg);

            // Dumps DB configurations info to disk
            //  Organize configs into a struct
            XStore_Proto::ConfigStruct CfgStruct;
            CfgStruct.set_dbname(dbName);
            CfgStruct.set_timeresolution(timeResolution);
            CfgStruct.set_rowlength(rowLength);

            // Writing cfgStruct to file
            FILE *BFile;
            std::string CfgPath  = fmt::format("{}{}.CFG", TargetPath, dbName);
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

            spdlog::info(SuccessMsg.substr(INFO_LEN));
            return ToReturn;
        }
    }

    CreateDbStatus = fmt::format("[ERROR] [CREATE DB] [DB: {}] - Already existed", dbName);
    ToReturn.set_createdbstatus(CreateDbStatus);
    spdlog::error(CreateDbStatus.substr(ERROR_LEN));
    return ToReturn;
}

// UNARY INSERT
void XStore::UnaryInsert(const unsigned long long int &unixTime, const XStore_Proto::RowData &data, std::string dbName, XStore_Proto::UnaryInsert_REP *returnData) {
    // Protobuf obj to be returned
    std::string InsertStatus;

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped.
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY INSERT] [DB: {}] - DB does not exist", dbName);
        returnData->set_insertstatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    TimeStruct CurrTs(unixTime, CfgStruct.timeresolution());

    // Cached PROT_WRITE mapping for this (dbName, year) file -- created,
    // ftruncate'd to the full year's row count, and mapped once; every
    // subsequent UnaryInsert to the same file reuses it (see GetOrMapFile).
    std::string TargetFile = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);
    void *MappedBase = GetOrMapFile(TargetFile, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (MappedBase == nullptr) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY INSERT] [DB: {}] - Unable to open/map {}.XSTORE", dbName, CurrTs.Year);
        returnData->set_insertstatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // A row that doesn't fit in the configured slot would otherwise silently
    // truncate (SerializeToArray's return value was previously unchecked),
    // corrupting whatever follows it in the file. Reject up front instead.
    if (data.ByteSizeLong() > static_cast<size_t>(CfgStruct.rowlength())) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY INSERT] [DB: {}] - Row size {} exceeds configured row length {}", dbName, data.ByteSizeLong(), CfgStruct.rowlength());
        returnData->set_insertstatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    //  Computes diff & offset
    unsigned long long int Diff = unixTime - CurrTs.FirstEpoch;
    unsigned long long int Offset = Diff * CfgStruct.rowlength();

    {
        // Lock prior to file OP
        FileRangeLockGuard Guard(TargetFile, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::WRITE);

        // Zero the fixed-width row slot first, then serialize directly into
        // it -- avoids the SerializeAsString() heap allocation + copy the
        // original fwrite() path paid on every insert, and guarantees any
        // bytes past a shorter re-serialization don't leak a previous
        // occupant's stale trailing bytes.
        char *Slot = static_cast<char*>(MappedBase) + Offset;
        std::memset(Slot, 0, CfgStruct.rowlength());
        data.SerializeToArray(Slot, CfgStruct.rowlength());
    }

    std::string SuccessMsg = fmt::format("[INFO] [UNARY INSERT] [DB: {}] - Inserted {} to {}.XSTORE", dbName, unixTime, CurrTs.Year);
    returnData->set_insertstatus(SuccessMsg);
    spdlog::info(SuccessMsg.substr(INFO_LEN));
    return;
}

// UNARY QUERY
void XStore::UnaryQuery(const unsigned long long int &unixTime, std::string dbName, XStore_Proto::UnaryQuery_REP *returnData) {
    // Protobuf obj to be returned
    std::string QueryStatus;
    returnData->set_timestamp(unixTime);

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped.
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY QUERY] [DB: {}] - DB does not exist", dbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    //  Time structure contains all components per UNIXTIME:
    //      Year, month, day, hour, minute, sec, ...
    TimeStruct CurrTs(unixTime, CfgStruct.timeresolution());

    // Base directory of a DB & Target path of file to READ
    std::string TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

    // IF target year file DOES NOT exist -- checked explicitly because
    // GetOrMapFile below opens with O_CREAT and would otherwise silently
    // create+map an empty file for a query that should just report no data.
    if (std::filesystem::exists(TargetFilePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY QUERY] [DB: {}] - DB does not exist", dbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Cached mapping for this (dbName, year) file, same mmap LRU cache
    // UnaryInsert uses (see GetOrMapFile) -- avoids a fresh
    // fopen/fseek/fread/fclose (2 syscalls + stdio buffering) on every query.
    void *MappedBase = GetOrMapFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (MappedBase == nullptr) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY QUERY] [DB: {}] - Unable to open/map {}.XSTORE", dbName, CurrTs.Year);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    //  Computes diff & offset
    unsigned long long int Diff = unixTime - CurrTs.FirstEpoch;
    unsigned long long int Offset = Diff * CfgStruct.rowlength();

    {
        //  Lock range prior to reading
        FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::READ);

        // Parse straight from the mapped row into the response -- skips the
        // stack buffer + extra TempData copy the previous fread()-based path paid.
        returnData->mutable_data()->ParseFromArray(static_cast<char*>(MappedBase) + Offset, CfgStruct.rowlength());
    }
}

// UNARY DELETE
XStore_Proto::UnaryDelete_REP XStore::UnaryDelete(const unsigned long long int &epochTime, std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::UnaryDelete_REP ToReturn;
    std::string DeleteStatus;

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY DELETE] [DB: {}] - DB does not exist", dbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY DELETE] [DB: {}] - DB does not exist", dbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    TimeStruct CurrTs(epochTime, CfgStruct.timeresolution());

    // Prepare to delete (Writing NULL byte)
    FILE *BFile;
    std::string TargetFileName = fmt::format("{}.XSTORE", CurrTs.Year);
    std::string TargetFilePath = fmt::format("{}{}", DbBasePath, TargetFileName);

    // IF target file does NOT exist
    if (std::filesystem::exists(TargetFilePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [UNARY DELETE] [DB: {}] - {} does not exist", dbName, TargetFileName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    // OTHERWISE - Target file DOES exist
    //  Open in append mode
    BFile = std::fopen(TargetFilePath.c_str(), "rb+");

    //  Computes diff & offset
    unsigned long long int Diff = epochTime - CurrTs.FirstEpoch;
    unsigned long long int Offset = Diff * CfgStruct.rowlength();

    {
        // Lock prior to file OP
        FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::WRITE);

        // Deallocate data according to N bytes from rowLength
        fallocate64(fileno(BFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength());
    }
    // Close file after write
    std::fclose(BFile);

    std::string SuccessMsg = fmt::format("[INFO] [UNARY DELETE] [DB: {}] - Deleted {} in {}", dbName, epochTime, TargetFileName);
    ToReturn.set_deletestatus(SuccessMsg);
    spdlog::info(SuccessMsg.substr(INFO_LEN));
    return ToReturn;
}

// BATCH INSERT
void XStore::BatchInsert(XStore_Proto::BatchInsert_REQ *protoFromClient, XStore_Proto::BatchInsert_REP *returnData) {
    // Protobuf obj to be returned
    std::string InsertStatus;

    // Ensure DB Name string is all capitalized
    std::string DbName = protoFromClient->dbname();
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, DbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH INSERT] [DB: {}] - DB does not exist", DbName);
        returnData->set_insertstatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH INSERT] [DB: {}] - DB does not exist", DbName);
        returnData->set_insertstatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Assuming incoming data is unsorted
    //  -> Ascending sort
    std::sort(std::execution::par_unseq, protoFromClient->mutable_batchinsert_data()->begin(), protoFromClient->mutable_batchinsert_data()->end(),
              [](const XStore_Proto::BatchInsert_DATA& a, const XStore_Proto::BatchInsert_DATA &b) {
                    return a.timestamp() < b.timestamp();
    });

    // Organize incoming data into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    std::time_t CurrTime;
    tm EpochStruct;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->batchinsert_data_size(); i++) {
        CurrTime = protoFromClient->batchinsert_data(i).timestamp();
        gmtime_r(&CurrTime, &EpochStruct);
        DataMap[(EpochStruct.tm_year + 1900)].push_back(i);
    }

    // Iterate through each key (year) of dataMap
    XStore_Proto::BatchInsert_DATA CurrInsertData;
    XStore_Proto::RowData Data;
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& Pair : DataMap) {
        unsigned short int YearKey = Pair.first;
        FILE *BFile;
        std::string TargetFile = fmt::format("{}{}.XSTORE", DbBasePath, YearKey);

        // Grab first inserting element for TimeStruct purposes
        //  Get length of time to truncate
        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->batchinsert_data().at(DataMap[YearKey].at(0)).timestamp()), CfgStruct.timeresolution()};

        // IF target file does NOT exist
        if (std::filesystem::exists(TargetFile) == 0) {
            // Truncate
            BFile = std::fopen(TargetFile.c_str(), "wb+");

            // Lock prior to file OP
            FileRangeLockGuard TruncGuard(TargetFile, 0, 0, FileRangeLock::LockType::WRITE);
            ftruncate(fileno(BFile), (ThisTs.LastEpoch - ThisTs.FirstEpoch) * CfgStruct.rowlength());
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in append mode
            BFile = std::fopen(TargetFile.c_str(), "rb+");
        }

        long long int Diff, Offset;
        // Iterate through each YEAR of inserting data
        for (int i = 0; i < DataMap[YearKey].size(); i++) {
            // Parse each data in a batch collection
            CurrInsertData = protoFromClient->batchinsert_data(DataMap[YearKey].at(i));
            std::time_t UnixTime = CurrInsertData.timestamp();
            Data = CurrInsertData.data();

            // Compute diff & offset
            Diff = UnixTime - ThisTs.FirstEpoch;
            Offset = Diff * CfgStruct.rowlength();

            // Rows shorter than rowLength must not leave a previous
            // occupant's stale trailing bytes in the slot (see the matching
            // memset in UnaryInsert); rows that don't fit at all are rejected
            // rather than silently truncated.
            std::string Serialized = Data.SerializeAsString();
            if (Serialized.size() > static_cast<size_t>(CfgStruct.rowlength())) {
                continue;
            }
            std::string RowBuffer(CfgStruct.rowlength(), '\0');
            std::memcpy(RowBuffer.data(), Serialized.data(), Serialized.size());

            // Lock prior to file OP
            FileRangeLockGuard Guard(TargetFile, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::WRITE);

            // Seek to target offset prior to write
            fseeko64(BFile, Offset, SEEK_SET);

            // Writing actual data to disk
            fwrite(RowBuffer.data(), CfgStruct.rowlength(), 1, BFile);
        }

        // Close file after write
        std::fclose(BFile);
    }

    // Return insert status
    std::string SuccessMsg = fmt::format("[INFO] [BATCH INSERT] [DB: {}] - Inserted {} elements successfully", DbName, protoFromClient->batchinsert_data_size());
    returnData->set_insertstatus(SuccessMsg);
    spdlog::info(SuccessMsg.substr(INFO_LEN));
}

// BATCH QUERY
void XStore::BatchQuery(XStore_Proto::BatchQuery_REQ *protoFromClient, XStore_Proto::BatchQuery_REP *returnData, bool SortTime) {
    // Protobuf obj to be returned
    XStore_Proto::RowData TempData;

    // Ensure dbName string is all capitalized
    std::string DbName = protoFromClient->dbname();
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, DbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH QUERY] [DB: {}] - {}.CFG does not exist", DbName, DbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // IF query range > 2GB limit of Protobuf or Empty
    if ((protoFromClient->timestamp_size() * CfgStruct.rowlength()) > 2'000'000'000 || protoFromClient->timestamp_size() < 0) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH QUERY] [DB: {}] - Query results is larger than 2GB or empty", DbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH QUERY] [DB: {}] - DB does not exist", DbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    if (SortTime)
    {
        std::sort(std::execution::par_unseq, protoFromClient->mutable_timestamp()->begin(), protoFromClient->mutable_timestamp()->end(),
                      [](const unsigned long long int& a, const unsigned long long int& b) {
                        return a < b;
        });
    }

    // Organize incoming batch query into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    unsigned long long int CurrTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        CurrTime = protoFromClient->timestamp(i);
        struct TimeStruct CurrTs(CurrTime, CfgStruct.timeresolution());
        DataMap[CurrTs.Year].push_back(i);
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& Pair : DataMap) {
        FILE *BFile;
        unsigned short int YearKey = Pair.first;
        std::string TargetFile = fmt::format("{}{}.XSTORE", DbBasePath, YearKey);

        // IF target file does NOT exist
        if (std::filesystem::exists(TargetFile) == 0) {
            // Skip to next iteration
            std::string WarnMsg = fmt::format("[WARNING] [BATCH QUERY] [DB: {}] - {}.XSTORE does not exist", DbName, YearKey);
            returnData->set_querystatus(WarnMsg);
            spdlog::warn(WarnMsg.substr(WARNING_LEN));
            continue;
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in read mode
            BFile = std::fopen(TargetFile.c_str(), "rb");
        }

        // Time-related & Auxiliary
        unsigned long long int Diff, Offset;
        char BufferIn[CfgStruct.rowlength()];
        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->timestamp(DataMap[YearKey].at(0))), CfgStruct.timeresolution()};

        for (int i = 0; i < DataMap[YearKey].size(); i++) {
            // Computes diff & offset
            Diff = protoFromClient->timestamp(DataMap[YearKey].at(i)) - ThisTs.FirstEpoch;
            Offset = Diff * CfgStruct.rowlength();

            {
                // Lock prior to file OP
                FileRangeLockGuard Guard(TargetFile, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::READ);

                // Seek to target offset prior to read
                fseeko64(BFile, Offset, SEEK_SET);

                // Extract N bytes of data according to N bytes from .CFG file
                //  .CFG file is set by user when creating DB
                fread(&BufferIn[0], 1, CfgStruct.rowlength(), BFile);
            }

            // IF fetched data contains nothing
            //  -> Skip to next iteration. BufferIn is a fixed-width binary
            //  buffer (not a C string), so strlen() would over-read past its
            //  end whenever a real row's bytes fill the whole slot with no
            //  zero byte in it; checking the first byte (a hole is always
            //  fully zeroed) is what the aggregate scan kernels do too.
            if (BufferIn[0] == 0) {
                continue;
            }

            // Add fetched data to returning Protobuf obj
            TempData.ParseFromArray(&BufferIn, CfgStruct.rowlength());
            XStore_Proto::UnaryQuery_REP *CurrEntry = returnData->add_queryresponse();
            CurrEntry->set_timestamp(protoFromClient->timestamp(DataMap[YearKey].at(i)));
            *CurrEntry->mutable_data() = TempData;
        }

        // Close file & Release lock after read
        std::fclose(BFile);
    }
}

// BATCH DELETE
XStore_Proto::BatchDelete_REP XStore::BatchDelete(XStore_Proto::BatchDelete_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::BatchDelete_REP ToReturn;
    std::string DbName = protoFromClient->dbname();
    int DeleteCount = 0;

    // Ensure dbName string is all capitalized
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, DbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH DELETE] [DB: {}] - {}.CFG does not exist", DbName, DbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    // IF target delete timestamp(s) is empty OR db directory does not exist
    if (protoFromClient->timestamp_size() < 0) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH DELETE] [DB: {}] - Empty target timestamp(s)", DbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }
    else if (std::filesystem::is_directory(DbBasePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [BATCH DELETE] [DB: {}] - DB does not exist", DbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    std::sort(std::execution::par_unseq, protoFromClient->mutable_timestamp()->begin(), protoFromClient->mutable_timestamp()->end(),
              [](const unsigned long long int& a, const unsigned long long int& b) {
                return a < b;
    });

    // Organize incoming batch delete timestamp into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    std::time_t CurrTime;
    tm EpochStruct;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        CurrTime = protoFromClient->timestamp(i);
        gmtime_r(&CurrTime, &EpochStruct);
        DataMap[EpochStruct.tm_year + 1900].push_back(i);
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& Pair : DataMap) {
        FILE *BFile;
        unsigned short int YearKey = Pair.first;
        std::string TargetFile = fmt::format("{}{}.XSTORE", DbBasePath, YearKey);

        // IF target file does NOT exist
        if (std::filesystem::exists(TargetFile) == 0) {
            // Skip to next iteration
            continue;
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in read mode
            BFile = std::fopen(TargetFile.c_str(), "rb+");
        }

        // Time-related & Auxiliary
        unsigned long long int Diff, Offset;
        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->timestamp(DataMap[YearKey].at(0))), CfgStruct.timeresolution()};

        for (int i = 0; i < DataMap[YearKey].size(); i++) {
            // Computes diff & offset
            Diff = protoFromClient->timestamp(DataMap[YearKey].at(i)) - ThisTs.FirstEpoch;
            Offset = Diff * CfgStruct.rowlength();

            // Lock prior to file OP
            FileRangeLockGuard Guard(TargetFile, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::WRITE);

            // Deallocate data according to N bytes from rowLength
            fallocate64(fileno(BFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength());
            DeleteCount++;
        }

        // Close file & Release lock after read
        std::fclose(BFile);
    }
    std::string InfoMsg = fmt::format("[INFO] [BATCH DELETE] [DB: {}] - Deleted {} items", DbName, DeleteCount);
    ToReturn.set_deletestatus(InfoMsg);
    spdlog::info(InfoMsg.substr(INFO_LEN));
    return ToReturn;
}

// RANGE INSERT
void XStore::RangeInsert(const XStore_Proto::RangeInsert_REQ *protoFromClient, XStore_Proto::RangeInsert_REP *returnData) {
    // Protobuf obj to be returned
    std::string InsertStatus;

    // Ensure DB Name string is all capitalized
    std::string DbName = protoFromClient->dbname();
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, DbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE INSERT] [DB: {}] - DB does not exist", DbName);
        returnData->set_insertstatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE INSERT] [DB: {}] - DB does not exist", DbName);
        returnData->set_insertstatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Assuming incoming data is sorted --> Skip sorting
    // Organize incoming data into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    std::time_t CurrTime;
    tm EpochStruct;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->rangeinsert_data_size(); i++) {
        CurrTime = protoFromClient->rangeinsert_data(i).timestamp();
        gmtime_r(&CurrTime, &EpochStruct);
        DataMap[(EpochStruct.tm_year + 1900)].push_back(i);
    }

    // Iterate through each key (year) of dataMap
    XStore_Proto::RangeInsert_DATA FirstInsertData, CurrInsertData;
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& Pair : DataMap) {
        unsigned short int YearKey = Pair.first;
        FILE *BFile;
        std::string TargetFile = fmt::format("{}{}.XSTORE", DbBasePath, YearKey);

        // Grab first inserting element for TimeStruct purposes
        //  Get length of time to truncate
        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->rangeinsert_data().at(DataMap[YearKey].at(0)).timestamp()), CfgStruct.timeresolution()};

        // IF target file does NOT exist
        if (std::filesystem::exists(TargetFile) == 0) {
            // Truncate
            BFile = std::fopen(TargetFile.c_str(), "wb+");

            // Lock prior to file OP
            FileRangeLockGuard TruncGuard(TargetFile, 0, 0, FileRangeLock::LockType::WRITE);
            ftruncate(fileno(BFile), (ThisTs.LastEpoch - ThisTs.FirstEpoch) * CfgStruct.rowlength());
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in append mode
            BFile = std::fopen(TargetFile.c_str(), "rb+");
        }

        // Metadata of first datapoint
        FirstInsertData = protoFromClient->rangeinsert_data(DataMap[YearKey].at(0));
        const unsigned long long int &FirstUnixTime = FirstInsertData.timestamp();
        const unsigned long long int FirstOffset = (FirstUnixTime - ThisTs.FirstEpoch) * CfgStruct.rowlength();

        // RegionLength must cover the actual byte span of the writes below,
        // not just count*rowLength -- that undersizes the lock (and races
        // unprotected writes past its end) whenever the timestamps in this
        // year aren't contiguous. Relies on the same "incoming data is
        // sorted" precondition the rest of this function already assumes, so
        // the last entry in the group carries the highest offset.
        const XStore_Proto::RangeInsert_DATA &LastInsertData = protoFromClient->rangeinsert_data(DataMap[YearKey].back());
        const unsigned long long int LastOffset = (LastInsertData.timestamp() - ThisTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RegionLength = (LastOffset + CfgStruct.rowlength()) - FirstOffset;

        {
            // Lock prior to file OP
            FileRangeLockGuard Guard(TargetFile, FirstOffset, RegionLength, FileRangeLock::LockType::WRITE);

            unsigned long long int Diff, Offset;
            // Iterate through each YEAR of inserting data
            for (int i = 0; i < DataMap[YearKey].size(); i++) {
                // Parse each data in a batch collection
                CurrInsertData = protoFromClient->rangeinsert_data(DataMap[YearKey].at(i));
                const unsigned long long int &UnixTime = CurrInsertData.timestamp();
                const XStore_Proto::RowData &Data = CurrInsertData.data();

                // Compute diff & offset
                Diff = UnixTime - ThisTs.FirstEpoch;
                Offset = Diff * CfgStruct.rowlength();

                // Rows shorter than rowLength must not leave a previous
                // occupant's stale trailing bytes in the slot; rows that
                // don't fit at all are rejected rather than silently truncated.
                std::string Serialized = Data.SerializeAsString();
                if (Serialized.size() > static_cast<size_t>(CfgStruct.rowlength())) {
                    continue;
                }
                std::string RowBuffer(CfgStruct.rowlength(), '\0');
                std::memcpy(RowBuffer.data(), Serialized.data(), Serialized.size());

                // Seek to target offset prior to write
                fseeko64(BFile, Offset, SEEK_SET);

                // Writing actual data to disk
                fwrite(RowBuffer.data(), CfgStruct.rowlength(), 1, BFile);
            }
        }
        // Close file after write
        std::fclose(BFile);
    }

    // Return insert status
    std::string InfoMsg = fmt::format("[INFO] [RANGE INSERT] [DB: {}] - Inserted {} elements successfully", DbName, protoFromClient->rangeinsert_data_size());
    returnData->set_insertstatus(InfoMsg);
    spdlog::info(InfoMsg.substr(INFO_LEN));
}

// RANGE QUERY
void XStore::RangeQuery(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, XStore_Proto::RangeQuery_REP *returnData) {
    // Protobuf obj to be returned
    XStore_Proto::RowData TempData;
    XStore_Proto::UnaryQuery_REP *CurrEntry;
    std::string QueryStatus;

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // IF query range > 2GB limit of Protobuf
    if ((endEpochT - startEpochT) * CfgStruct.rowlength() > 2'000'000'000) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - Query results is larger than 2GB", dbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    if ((endEpochT - startEpochT) < 0) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - Target end time must be after start time", dbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - DB does not exist", dbName);
        returnData->set_querystatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return;
    }

    // Organize into TimeStruct
    //  TimeStruct provides many auxiliary vars/methods useful for read/write ops
    TimeStruct CurrTs(startEpochT, CfgStruct.timeresolution());

    // Placeholder for READ op
    FILE *BFile;
    std::string TargetFilePath;
    unsigned long long int Diff, Offset, DataRead;
    char BufferIn[CfgStruct.rowlength()];
    std::string SuccessMsg;

    // IF query range is within 1-year
    if (CurrTs.LastEpoch >= endEpochT) {
        // Open file prior to reading
        TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

        // IF target YEAR.XSTORE does NOT exist
        if (std::filesystem::exists(TargetFilePath) == 0) {
            // Return without error
            std::string WarningMsg = fmt::format("[WARNING] [RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
            returnData->set_querystatus(WarningMsg);
            spdlog::warn(WarningMsg.substr(WARNING_LEN));
            return;
        }

        BFile = std::fopen(TargetFilePath.c_str(), "rb");

        // Metadata of first datapoint
        const unsigned long long int FirstOffset = (startEpochT - CurrTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RegionLength = CfgStruct.rowlength() * (endEpochT - startEpochT);

        {
            // Lock file prior to reading
            FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);

            // Iterate over range of query data & add queried data and status
            for (long EpochTime = startEpochT; EpochTime <= endEpochT; EpochTime++) {
                // Computes diff & offset
                Diff = EpochTime - CurrTs.FirstEpoch;
                Offset = Diff * CfgStruct.rowlength();

                // Seek to computed offset
                fseeko64(BFile, Offset, SEEK_SET);

                // Extract N bytes of data according to N bytes from .CFG file
                //  .CFG file is set by user when creating DB
                DataRead = fread(&BufferIn[0], 1, CfgStruct.rowlength(), BFile);

                // IF fetched data contains nothing
                //  -> Skip to next iteration
                if (DataRead == 0) {
                    continue;
                }

                // Add fetched data to returning Protobuf obj
                TempData.ParseFromArray(&BufferIn, CfgStruct.rowlength());
                CurrEntry = returnData->add_queryresponse();
                CurrEntry->set_timestamp(EpochTime);
                *CurrEntry->mutable_data() = TempData;
            }
        }
        // Close file after reading
        fclose(BFile);
    }
    // Otherwise -> Involves multi-year
    else {
        // Make a copy of start epoch time for incrementing
        unsigned long long int StartEpoch = startEpochT;

        // Loop through each year individually
        //  NOTE: "<=" (not "<") -- once startEpoch has caught up to equal
        //  endEpochT, there is still exactly one remaining timestamp (endEpochT
        //  itself) that the final-year branch below must process; using "<"
        //  here would exit before that last point is ever read.
        while (StartEpoch <= endEpochT) {
            TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

            // IF the requested start epoch spans ONLY 1 year
            if (CurrTs.LastEpoch < endEpochT) {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(TargetFilePath) == 0) {
                    std::string WarningMsg = fmt::format("[WARNING] [RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
                    returnData->set_querystatus(WarningMsg);
                    spdlog::warn(WarningMsg.substr(WARNING_LEN));

                    // Advance StartEpoch into the next year before recomputing
                    // CurrTs -- the old "StartEpoch + (CurrTs.LastEpoch -
                    // StartEpoch)" simplifies to just CurrTs.LastEpoch, which
                    // stays in the SAME year (LastEpoch is Dec 31 23:59:59 of
                    // that year), leaving StartEpoch untouched. TargetFilePath
                    // above is built straight from CurrTs.Year without
                    // recomputing it from StartEpoch, so the old code re-checked
                    // the exact same missing file forever whenever a
                    // multi-year span's non-final year had no data file.
                    StartEpoch = CurrTs.LastEpoch + 1;
                    CurrTs = TimeStruct(StartEpoch, CfgStruct.timeresolution());
                    continue;
                }

                BFile = std::fopen(TargetFilePath.c_str(), "rb");

                // Metadata of first datapoint
                const unsigned long long int FirstOffset = (StartEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
                const unsigned long long int RegionLength = CfgStruct.rowlength() * (CurrTs.LastEpoch - StartEpoch);

                {
                    // Lock file prior to reading
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);

                    // Iterate over range of query data & add queried data and status
                    for (unsigned long long int EpochTime = StartEpoch; EpochTime <= CurrTs.LastEpoch; EpochTime++, StartEpoch++) {
                        // Computes diff & offset
                        Diff = EpochTime - CurrTs.FirstEpoch;
                        Offset = Diff * CfgStruct.rowlength();

                        // Seek to computed offset
                        fseeko64(BFile, Offset, SEEK_SET);

                        // Extract N bytes of data according to N bytes from .CFG file
                        //  .CFG file is set by user when creating DB
                        DataRead = fread(&BufferIn[0], 1, CfgStruct.rowlength(), BFile);

                        // IF fetched data contains nothing
                        //  -> Skip to next iteration
                        if (DataRead == 0) {
                            continue;
                        }

                        // Add fetched data to returning Protobuf obj
                        TempData.ParseFromArray(&BufferIn, CfgStruct.rowlength());
                        CurrEntry = returnData->add_queryresponse();
                        CurrEntry->set_timestamp(EpochTime);
                        *CurrEntry->mutable_data() = TempData;
                    }
                }

                // Update currTS with latest startEpoch after incrementing
                CurrTs = TimeStruct(StartEpoch, CfgStruct.timeresolution());

                // Close file after read
                std::fclose(BFile);
            }
            else {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(TargetFilePath) == 0) {
                    std::string WarningMsg = fmt::format("[WARNING] [RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
                    returnData->set_querystatus(WarningMsg);
                    spdlog::warn(WarningMsg.substr(WARNING_LEN));

                    // Update currTS with latest startEpoch after incrementing
                    CurrTs = TimeStruct(StartEpoch + (endEpochT - StartEpoch), CfgStruct.timeresolution());
                    continue;
                }

                BFile = std::fopen(TargetFilePath.c_str(), "rb");

                // Metadata of first datapoint
                const unsigned long long int FirstOffset = (StartEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
                const unsigned long long int RegionLength = CfgStruct.rowlength() * (endEpochT - StartEpoch);

                {
                    // Lock file prior to reading
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);

                    // Iterate over range of query data & add queried data and status
                    for (unsigned long long int EpochTime = StartEpoch; EpochTime <= endEpochT; EpochTime++, StartEpoch++) {
                        // Computes diff & offset
                        Diff = EpochTime - CurrTs.FirstEpoch;
                        Offset = Diff * CfgStruct.rowlength();

                        // Seek to computed offset
                        fseeko64(BFile, Offset, SEEK_SET);

                        // Extract N bytes of data according to N bytes from .CFG file
                        //  .CFG file is set by user when creating DB
                        DataRead = fread(&BufferIn[0], 1, CfgStruct.rowlength(), BFile);

                        // IF fetched data contains nothing
                        //  -> Skip to next iteration
                        if (DataRead == 0) {
                            continue;
                        }

                        // Add fetched data to returning Protobuf obj
                        TempData.ParseFromArray(&BufferIn, CfgStruct.rowlength());
                        CurrEntry = returnData->add_queryresponse();
                        CurrEntry->set_timestamp(EpochTime);
                        *CurrEntry->mutable_data() = TempData;
                    }
                }
                // Close file after read
                std::fclose(BFile);
                break;
            }
        }
    }
}

// RANGE DELETE
XStore_Proto::RangeDelete_REP XStore::RangeDelete(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName) {
    // Protobuf obj to be returned
    std::string QueryStatus;
    XStore_Proto::RangeDelete_REP ToReturn;
    int DeleteCount = 0;

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    if ((endEpochT - startEpochT) < 0) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - Target end time must be after start time", dbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePath) == 0) {
        std::string ErrMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - DB does not exist", dbName);
        ToReturn.set_deletestatus(ErrMsg);
        spdlog::error(ErrMsg.substr(ERROR_LEN));
        return ToReturn;
    }

    // Organize into TimeStruct
    //  TimeStruct provides many auxiliary vars/methods useful for read/write ops
    TimeStruct CurrTs(startEpochT, CfgStruct.timeresolution());

    // Placeholder for READ op
    FILE *BFile;
    std::string TargetFilePath;
    unsigned long long int Diff, Offset, RangeCount;

    // IF query range is within 1-year
    if (CurrTs.LastEpoch >= endEpochT) {
        // Open file prior to reading
        TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

        // IF target YEAR.XSTORE does NOT exist
        if (std::filesystem::exists(TargetFilePath) == 0) {
            // Return without error
            std::string ErrorMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - DB does not exist", dbName);
            ToReturn.set_deletestatus(ErrorMsg);
            spdlog::error(ErrorMsg.substr(ERROR_LEN));
            return ToReturn;
        }

        BFile = std::fopen(TargetFilePath.c_str(), "rb+");

        // Compute diff & offset
        Diff = startEpochT - CurrTs.FirstEpoch;
        Offset = Diff * CfgStruct.rowlength();
        // +1: rangeCount covers [startEpochT, endEpochT] inclusive.
        RangeCount = endEpochT - startEpochT + 1;

        {
            // Lock file prior to deleting
            FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength() * RangeCount, FileRangeLock::LockType::WRITE);

            // Deallocate data according to N bytes from rowLength
            fallocate64(fileno(BFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength() * RangeCount);
            DeleteCount += RangeCount;
        }
        // Close file after delete
        fclose(BFile);
    }
    // Otherwise -> Involves multi-year
    else {
        // Make a copy of start epoch time for incrementing
        unsigned long long int StartEpoch = startEpochT;

        // Loop through each year individually
        //  NOTE: "<=" (not "<") -- once startEpoch has caught up to equal
        //  endEpochT, there is still exactly one remaining timestamp (endEpochT
        //  itself) that the final-year branch below must process; using "<"
        //  here would exit before that last point is ever deleted (see the
        //  matching fix/comment in RangeQuery above).
        while (StartEpoch <= endEpochT) {
            TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

            // IF the requested start epoch spans ONLY 1 year
            if (CurrTs.LastEpoch < endEpochT) {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(TargetFilePath) == 0) {
                    std::string WarningMsg = fmt::format("[WARNING] [RANGE DELETE] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
                    ToReturn.set_deletestatus(WarningMsg);
                    spdlog::warn(WarningMsg.substr(WARNING_LEN));

                    // Advance StartEpoch into the next year before recomputing
                    // CurrTs -- see the matching fix/comment in RangeQuery
                    // above; the old expression never actually advanced
                    // StartEpoch, so a missing non-final year's file spun the
                    // loop forever re-checking the same year.
                    StartEpoch = CurrTs.LastEpoch + 1;
                    CurrTs = TimeStruct(StartEpoch, CfgStruct.timeresolution());
                    continue;
                }

                BFile = std::fopen(TargetFilePath.c_str(), "rb+");

                // Computes diff & offset
                Diff = StartEpoch - CurrTs.FirstEpoch;
                Offset = Diff * CfgStruct.rowlength();
                // +1: rangeCount covers [startEpoch, currTS.lastEpoch] inclusive.
                // Without it, the last row of this year's segment (currTS.lastEpoch
                // itself) never gets deallocated.
                RangeCount = CurrTs.LastEpoch - StartEpoch + 1;

                {
                    // Lock file prior to deleting
                    FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength() * RangeCount, FileRangeLock::LockType::WRITE);

                    // Deallocate data according to N bytes from rowLength
                    fallocate64(fileno(BFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength() * RangeCount);
                    DeleteCount += RangeCount;
                }

                // Advance startEpoch past the year just processed -- without this,
                // startEpoch never changes and the outer while loop spins forever
                // on the same year for any genuinely multi-year delete.
                StartEpoch = CurrTs.LastEpoch + 1;

                // Update currTS with latest startEpoch after incrementing
                CurrTs = TimeStruct(StartEpoch, CfgStruct.timeresolution());

                // Close file after delete
                std::fclose(BFile);
            }
            else {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(TargetFilePath) == 0) {
                    std::string WarningMsg = fmt::format("[WARNING] [RANGE DELETE] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
                    ToReturn.set_deletestatus(WarningMsg);
                    spdlog::warn(WarningMsg.substr(WARNING_LEN));

                    // Update currTS with latest startEpoch after incrementing
                    CurrTs = TimeStruct(StartEpoch + (endEpochT - StartEpoch), CfgStruct.timeresolution());
                    continue;
                }

                BFile = std::fopen(TargetFilePath.c_str(), "rb+");

                // Computes diff & offset
                Diff = StartEpoch - CurrTs.FirstEpoch;
                Offset = Diff * CfgStruct.rowlength();
                // +1: rangeCount covers [startEpoch, endEpochT] inclusive.
                RangeCount = endEpochT - StartEpoch + 1;

                {
                    // Lock file prior to deleting
                    FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength() * RangeCount, FileRangeLock::LockType::WRITE);

                    // Deallocate data according to N bytes from rowLength
                    fallocate64(fileno(BFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength() * RangeCount);
                    DeleteCount += RangeCount;
                }
                // Close file after delete
                std::fclose(BFile);
                break;
            }
        }
    }
    std::string InfoMsg = fmt::format("[INFO] [RANGE DELETE] [DB: {}] - Deleted {} items", dbName, DeleteCount);
    ToReturn.set_deletestatus(InfoMsg);
    spdlog::info(InfoMsg.substr(INFO_LEN));
    return ToReturn;
}

// Shared mmap-backed scan kernels used by MinAggregate/MaxAggregate/SumAggregate/AvgAggregate.
//  One page-aligned mmap per file range, MADV_SEQUENTIAL readahead, zero per-row syscalls,
//  CodedInputStream parses directly off mapped pages (no row buffer copy).

template <typename Cmp>
void XStore::ProcessFileRangeSelect(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     ValueStruct& bestValue, XStore_Proto::FData& bestData) {

    if (totalRows == 0) return;

    int Fd = fileno(bFile);
    long PageSize = sysconf(_SC_PAGE_SIZE);

    unsigned long long AlignedOffset = (startOffset / PageSize) * PageSize;
    unsigned long long AlignmentSlop = startOffset - AlignedOffset;
    size_t TotalBytesToMap = (totalRows * rowLen) + AlignmentSlop;

    void* MmappedPtr = mmap(NULL, TotalBytesToMap, PROT_READ, MAP_SHARED, Fd, AlignedOffset);
    if (MmappedPtr == MAP_FAILED) return;

    madvise(MmappedPtr, TotalBytesToMap, MADV_SEQUENTIAL | MADV_WILLNEED);

    const char* BasePtr = static_cast<const char*>(MmappedPtr) + AlignmentSlop;
    XStore_Proto::RowData TempData;
    ValueStruct TempValue;
    const Cmp Better{}; // stateless functor, no per-call construction cost

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = BasePtr + (i * rowLen);

        // Instant skip for empty slots
        if (RowPtr[0] == 0) continue;

        TempData.Clear();

        // We MUST use CodedInputStream to properly ignore trailing zeros in the fixed-width row slot
        google::protobuf::io::ArrayInputStream ArrayIn(RowPtr, (int)rowLen);
        google::protobuf::io::CodedInputStream CodedIn(&ArrayIn);

        if (!TempData.MergePartialFromCodedStream(&CodedIn)) {
            continue;
        }

        if (columnIdx < (unsigned int)TempData.fielddata_size()) {
            const auto& CurrData = TempData.fielddata(columnIdx);

            if (CurrData.oneOfData_case() > 0 && CurrData.oneOfData_case() <= 12) {
                XStore::GetFDataValue(&CurrData, &TempValue);

                if (!bestValue.IsInitialized || Better(TempValue.Value, bestValue.Value)) {
                    bestValue.Value = TempValue.Value;
                    bestValue.IsInitialized = true;
                    bestData = CurrData;
                }
            }
        }
    }

    munmap(MmappedPtr, TotalBytesToMap);
}

// Explicit instantiation: these are the only two comparators ever used, so the compiler
// generates two fully-inlined, branch-free (on Cmp) versions -- no vtable/std::function overhead.
template void XStore::ProcessFileRangeSelect<std::less<long double>>(
    FILE*, unsigned long long, unsigned long long, size_t, unsigned short, ValueStruct&, XStore_Proto::FData&);
template void XStore::ProcessFileRangeSelect<std::greater<long double>>(
    FILE*, unsigned long long, unsigned long long, size_t, unsigned short, ValueStruct&, XStore_Proto::FData&);

void XStore::ProcessFileRangeAccumulate(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                                         size_t rowLen, unsigned short columnIdx,
                                         double& sumOut, unsigned long long& validCountOut) {

    if (totalRows == 0) return;

    int Fd = fileno(bFile);
    long PageSize = sysconf(_SC_PAGE_SIZE);

    unsigned long long AlignedOffset = (startOffset / PageSize) * PageSize;
    unsigned long long AlignmentSlop = startOffset - AlignedOffset;
    size_t TotalBytesToMap = (totalRows * rowLen) + AlignmentSlop;

    void* MmappedPtr = mmap(NULL, TotalBytesToMap, PROT_READ, MAP_SHARED, Fd, AlignedOffset);
    if (MmappedPtr == MAP_FAILED) return;

    madvise(MmappedPtr, TotalBytesToMap, MADV_SEQUENTIAL | MADV_WILLNEED);

    const char* BasePtr = static_cast<const char*>(MmappedPtr) + AlignmentSlop;
    XStore_Proto::RowData TempData;
    ValueStruct TempValue;

    for (unsigned long long i = 0; i < totalRows; ++i) {
        const char* RowPtr = BasePtr + (i * rowLen);

        if (RowPtr[0] == 0) continue;

        TempData.Clear();

        google::protobuf::io::ArrayInputStream ArrayIn(RowPtr, (int)rowLen);
        google::protobuf::io::CodedInputStream CodedIn(&ArrayIn);

        if (!TempData.MergePartialFromCodedStream(&CodedIn)) {
            continue;
        }

        if (columnIdx < (unsigned int)TempData.fielddata_size()) {
            const auto& CurrData = TempData.fielddata(columnIdx);

            if (CurrData.oneOfData_case() > 0 && CurrData.oneOfData_case() <= 12) {
                XStore::GetFDataValue(&CurrData, &TempValue);
                sumOut += (double)TempValue.Value;
                validCountOut++;
            }
        }
    }

    munmap(MmappedPtr, TotalBytesToMap);
}

void XStore::MinAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    // Config Fetching
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        returnData->set_opstatus(fmt::format("[ERROR] [MIN AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName));
        return;
    }

    // Safety Checks
    if (startEpochT > endEpochT) {
        returnData->set_opstatus(fmt::format("[ERROR] [MIN AGGREGATE] [DB: {}] - Target end time must be after start time", dbName));
        return;
    }

    ValueStruct MinValue;
    MinValue.IsInitialized = false;
    XStore_Proto::FData MinData;

    unsigned long long IterEpoch = startEpochT;

    // The logic below handles both single-year and multi-year spans efficiently
    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, CfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

        // Determine the end of the current file range
        unsigned long long EndOfCurrentFile = std::min(endEpochT, (unsigned long long)CurrTs.LastEpoch);
        unsigned long long RowsToProcess = (EndOfCurrentFile - IterEpoch) + 1;

        if (std::filesystem::exists(TargetFilePath)) {
            FILE* BFile = std::fopen(TargetFilePath.c_str(), "rb");
            if (BFile) {
                unsigned long long FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
                unsigned long long RegionLength = RowsToProcess * CfgStruct.rowlength();

                {
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);
                    ProcessFileRangeSelect<std::less<long double>>(BFile, FirstOffset, RowsToProcess, CfgStruct.rowlength(), columnIdx, MinValue, MinData);
                }
                std::fclose(BFile);
            }
        }

        // Move to the start of the next year
        IterEpoch = EndOfCurrentFile + 1;
    }

    // Return Results
    if (MinValue.IsInitialized) {
        returnData->set_opstatus("SUCCESS");
        *returnData->mutable_opresult() = MinData;
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
    }
}

void XStore::MaxAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Max_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        returnData->set_opstatus(fmt::format("[ERROR] [MAX AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName));
        return;
    }

    if (startEpochT > endEpochT) {
        returnData->set_opstatus(fmt::format("[ERROR] [MAX AGGREGATE] [DB: {}] - Target end time must be after start time", dbName));
        return;
    }

    ValueStruct MaxValue;
    MaxValue.IsInitialized = false;
    XStore_Proto::FData MaxData;

    unsigned long long IterEpoch = startEpochT;

    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, CfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

        unsigned long long EndOfCurrentFile = std::min(endEpochT, (unsigned long long)CurrTs.LastEpoch);
        unsigned long long RowsToProcess = (EndOfCurrentFile - IterEpoch) + 1;

        if (std::filesystem::exists(TargetFilePath)) {
            FILE* BFile = std::fopen(TargetFilePath.c_str(), "rb");
            if (BFile) {
                unsigned long long FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
                unsigned long long RegionLength = RowsToProcess * CfgStruct.rowlength();

                {
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);
                    ProcessFileRangeSelect<std::greater<long double>>(BFile, FirstOffset, RowsToProcess, CfgStruct.rowlength(), columnIdx, MaxValue, MaxData);
                }
                std::fclose(BFile);
            }
        }

        IterEpoch = EndOfCurrentFile + 1;
    }

    if (MaxValue.IsInitialized) {
        returnData->set_opstatus("SUCCESS");
        *returnData->mutable_opresult() = MaxData;
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
    }
}

void XStore::SumAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Sum_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        returnData->set_opstatus(fmt::format("[ERROR] [SUM AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName));
        return;
    }

    if (startEpochT > endEpochT) {
        returnData->set_opstatus(fmt::format("[ERROR] [SUM AGGREGATE] [DB: {}] - Target end time must be after start time", dbName));
        return;
    }

    double SumResult = 0.0;
    unsigned long long ValidCount = 0;
    unsigned long long IterEpoch = startEpochT;

    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, CfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

        unsigned long long EndOfCurrentFile = std::min(endEpochT, (unsigned long long)CurrTs.LastEpoch);
        unsigned long long RowsToProcess = (EndOfCurrentFile - IterEpoch) + 1;

        if (std::filesystem::exists(TargetFilePath)) {
            FILE* BFile = std::fopen(TargetFilePath.c_str(), "rb");
            if (BFile) {
                unsigned long long FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
                unsigned long long RegionLength = RowsToProcess * CfgStruct.rowlength();

                {
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);
                    ProcessFileRangeAccumulate(BFile, FirstOffset, RowsToProcess, CfgStruct.rowlength(), columnIdx, SumResult, ValidCount);
                }
                std::fclose(BFile);
            }
        }

        IterEpoch = EndOfCurrentFile + 1;
    }

    if (ValidCount > 0) {
        returnData->set_opstatus("SUCCESS");
        returnData->set_opresult(SumResult);
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
    }
}

void XStore::AvgAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Avg_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePath = fmt::format("{}DataStore/{}/", BaseDir, dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePath, CfgStruct)) {
        returnData->set_opstatus(fmt::format("[ERROR] [AVG AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName));
        return;
    }

    if (startEpochT > endEpochT) {
        returnData->set_opstatus(fmt::format("[ERROR] [AVG AGGREGATE] [DB: {}] - Target end time must be after start time", dbName));
        return;
    }

    double SumResult = 0.0;
    unsigned long long ValidCount = 0;
    unsigned long long IterEpoch = startEpochT;

    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, CfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}{}.XSTORE", DbBasePath, CurrTs.Year);

        unsigned long long EndOfCurrentFile = std::min(endEpochT, (unsigned long long)CurrTs.LastEpoch);
        unsigned long long RowsToProcess = (EndOfCurrentFile - IterEpoch) + 1;

        if (std::filesystem::exists(TargetFilePath)) {
            FILE* BFile = std::fopen(TargetFilePath.c_str(), "rb");
            if (BFile) {
                unsigned long long FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
                unsigned long long RegionLength = RowsToProcess * CfgStruct.rowlength();

                {
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);
                    ProcessFileRangeAccumulate(BFile, FirstOffset, RowsToProcess, CfgStruct.rowlength(), columnIdx, SumResult, ValidCount);
                }
                std::fclose(BFile);
            }
        }

        IterEpoch = EndOfCurrentFile + 1;
    }

    if (ValidCount > 0) {
        returnData->set_opstatus("SUCCESS");
        returnData->set_opresult(SumResult / (double)ValidCount);
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
    }
}

XStore_Proto::RemoveDB_REP XStore::RemoveDB(std::string dbName) {
    // Protobuf obj to be returned
    std::string RemoveStatus;
    XStore_Proto::RemoveDB_REP ToReturn;

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Location of DB
    std::string DbLoc = fmt::format("{}{}", DataStoreDir, dbName);

    // IF DB does NOT exist
    //  -> Return remove error
    if (!std::filesystem::exists(DbLoc)) {
        RemoveStatus = fmt::format("[ERROR] [REMOVE DB] [DB: {}] - DB does not exists", dbName);
        ToReturn.set_removestatus(RemoveStatus);
        spdlog::error(RemoveStatus.substr(ERROR_LEN));
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

    RemoveStatus = fmt::format("[INFO] [REMOVE DB] [DB: {}] - Removed DB with name: {}", dbName, dbName);
    ToReturn.set_removestatus(RemoveStatus);
    spdlog::info(RemoveStatus.substr(INFO_LEN));
    return ToReturn;
}

XStore_Proto::RemoveDBFiles_REP XStore::RemoveDBFiles(XStore_Proto::RemoveDBFiles_REQ *protoFromClient, std::string dbName) {
    // Protobuf obj to be returned
    std::string RemoveStatus;
    XStore_Proto::RemoveDBFiles_REP ToReturn;

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Location of DB
    std::string DbLoc = fmt::format("{}{}", DataStoreDir, dbName);

    if (!std::filesystem::exists(DbLoc)) {
        for (int i = 0; i < protoFromClient->filename_size(); i++) {
            RemoveStatus = fmt::format("[ERROR] [REMOVE DB FILES] [DB: {}] -- Unable to remove {}", dbName, protoFromClient->filename(i));
            ToReturn.add_removestatus(RemoveStatus + protoFromClient->filename(i));
            spdlog::error(RemoveStatus.substr(ERROR_LEN));
        }
        return ToReturn;
    }

    // Loop through each requested file name
    for (int i = 0; i < protoFromClient->filename_size(); i++) {
        std::filesystem::path TargetPath = fmt::format("{}{}/{}", DataStoreDir, dbName, protoFromClient->filename(i));
        if (protoFromClient->filename(i) == fmt::format("{}.CFG", dbName)) {
            std::string WarnMsg = fmt::format("[WARNING] [REMOVE DB FILES] [DB: {}] - {} is prohibited from remove", dbName, protoFromClient->filename(i));
            ToReturn.add_removestatus(WarnMsg);
            spdlog::warn(WarnMsg.substr(WARNING_LEN));
            continue;
        }
        bool CurrRemove = std::filesystem::remove(TargetPath);
        if (CurrRemove) {
            // Drop this file's cached UnaryInsert mapping, if any -- same
            // reasoning as RemoveDB, scoped to a single file.
            EraseMappedFile(TargetPath.string());

            std::string InfoMsg = fmt::format("[INFO] [REMOVE DB FILES] [DB: {}] - Removed {}", dbName, protoFromClient->filename(i));
            ToReturn.add_removestatus(InfoMsg);
            spdlog::info(InfoMsg.substr(INFO_LEN));
        }
        else {
            std::string ErrMsg = fmt::format("[ERROR] [REMOVE DB FILES] [DB: {}] - Unable to remove {}", dbName, protoFromClient->filename(i));
            ToReturn.add_removestatus(ErrMsg);
            spdlog::error(ErrMsg.substr(ERROR_LEN));
        }
    }
    return ToReturn;
}

void XStore::InitDB() {
    // Loop through each dbName
    for (auto& p : std::filesystem::directory_iterator(DataStoreDir))
    {
        // Full path of each DB
        std::string CurrDbPath = p.path().string();

        // Extract -- Getting only dbName
        std::string CurrDbName = CurrDbPath.erase(0, DataStoreDir.size());

        // Open & Read cfgStruct of all DB currently exists
        std::string CfgFilePath = fmt::format("{}/{}.CFG", p.path().string(), CurrDbName);

        XStore_Proto::ConfigStruct CfgStruct;
        FILE *CFile = std::fopen(CfgFilePath.c_str(), "rb");
        CfgStruct.ParseFromFileDescriptor(fileno(CFile));
        std::fclose(CFile);
        PutCfg(CurrDbName, CfgStruct);
    }
}

std::string XStore::GetBaseDir() {
    std::string ToReturn;

    // Getting current path of XStore executable
    ToReturn = std::filesystem::current_path().parent_path();

    // Extract baseDir for XStore i.e., /home/ubuntu/XStore/
//    toReturn.erase(toReturn.find("XStore") + 7, toReturn.size());
    return ToReturn + "/";
}

// Check if there's any existing database
bool XStore::HasDB() {
    // Path to DataStore -- Currently hard-coded | WILL NEED TO FIX SO IT IS DYNAMIC
    const std::filesystem::path TmpDir{ std::filesystem::path(BaseDir + "DataStore")};
    return !std::filesystem::is_empty(TmpDir);
}

bool XStore::HasDBFile(std::string filePath) {
    struct stat Buffer;
    return (stat(filePath.c_str(), &Buffer) == 0);
}


void XStore::GetFDataValue(const XStore_Proto::FData *fDataInput, unsigned int columnIdx, ValueStruct *returnData) {
    switch (fDataInput->oneOfData_case()) {
        case (XStore_Proto::FData::OneOfDataCase::kDoubleData): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->doubledata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFloatData): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->floatdata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->int32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->int64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->uint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->uint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->fixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->fixed64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed32Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sfixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed64Data): {
            returnData->Index = columnIdx;
            returnData->Value = fDataInput->sfixed64data();
            break;
        }
    }
}

void XStore::GetFDataValue(const XStore_Proto::FData *fDataInput, ValueStruct *returnData) {
    switch (fDataInput->oneOfData_case()) {
        case (XStore_Proto::FData::OneOfDataCase::kDoubleData): {
            returnData->Value = fDataInput->doubledata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFloatData): {
            returnData->Value = fDataInput->floatdata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt32Data): {
            returnData->Value = fDataInput->int32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt64Data): {
            returnData->Value = fDataInput->int64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint32Data): {
            returnData->Value = fDataInput->uint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint64Data): {
            returnData->Value = fDataInput->uint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint32Data): {
            returnData->Value = fDataInput->sint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint64Data): {
            returnData->Value = fDataInput->sint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed32Data): {
            returnData->Value = fDataInput->fixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed64Data): {
            returnData->Value = fDataInput->fixed64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed32Data): {
            returnData->Value = fDataInput->sfixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed64Data): {
            returnData->Value = fDataInput->sfixed64data();
            break;
        }
    }
}