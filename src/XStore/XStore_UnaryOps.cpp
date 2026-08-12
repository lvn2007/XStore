#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include <cstring>
#include <filesystem>
#include <fcntl.h>

// UNARY INSERT
void XStore::UnaryInsert(const unsigned long long int &unixTime, const XStore_Proto::RowData &data, std::string dbName, XStore_Proto::UnaryInsert_REP *returnData) {
    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(dbName);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped.
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[UNARY INSERT] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    TimeStruct CurrTs(unixTime, CfgStruct.timeresolution());

    // Cached PROT_WRITE mapping for this (dbName, year) file -- created,
    // ftruncate'd to the full year's row count, and mapped once; every
    // subsequent UnaryInsert to the same file reuses it (see GetOrMapFile).
    std::string TargetFile = fmt::format("{}/{}.XSTORE", DbBasePathStr, CurrTs.Year);
    void *MappedBase = GetOrMapFile(TargetFile, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (MappedBase == nullptr) {
        std::string ErrMsg = fmt::format("[UNARY INSERT] [DB: {}] - Unable to open/map {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
        return;
    }

    // A row that doesn't fit in the configured slot would otherwise silently
    // truncate (SerializeToArray's return value was previously unchecked),
    // corrupting whatever follows it in the file. Reject up front instead.
    if (data.ByteSizeLong() > static_cast<size_t>(CfgStruct.rowlength())) {
        std::string ErrMsg = fmt::format("[UNARY INSERT] [DB: {}] - Row size {} exceeds configured row length {}", dbName, data.ByteSizeLong(), CfgStruct.rowlength());
        SetTaggedStatus(returnData->mutable_insertstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
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

    std::string SuccessMsg = fmt::format("[UNARY INSERT] [DB: {}] - Inserted {} to {}.XSTORE", dbName, unixTime, CurrTs.Year);
    SetTaggedStatus(returnData->mutable_insertstatus(), "[INFO] ", SuccessMsg);
    return;
}

// UNARY QUERY
void XStore::UnaryQuery(const unsigned long long int &unixTime, std::string dbName, XStore_Proto::UnaryQuery_REP *returnData) {
    returnData->set_timestamp(unixTime);

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(dbName);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped.
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[UNARY QUERY] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    //  Time structure contains all components per UNIXTIME:
    //      Year, month, day, hour, minute, sec, ...
    TimeStruct CurrTs(unixTime, CfgStruct.timeresolution());

    // Base directory of a DB & Target path of file to READ
    std::string TargetFilePath = fmt::format("{}/{}.XSTORE", DbBasePathStr, CurrTs.Year);

    // IF target year file DOES NOT exist -- checked explicitly because
    // GetOrMapFile below opens with O_CREAT and would otherwise silently
    // create+map an empty file for a query that should just report no data.
    if (std::filesystem::exists(TargetFilePath) == 0) {
        std::string ErrMsg = fmt::format("[UNARY QUERY] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Cached mapping for this (dbName, year) file, same mmap LRU cache
    // UnaryInsert uses (see GetOrMapFile) -- avoids a fresh
    // fopen/fseek/fread/fclose (2 syscalls + stdio buffering) on every query.
    void *MappedBase = GetOrMapFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (MappedBase == nullptr) {
        std::string ErrMsg = fmt::format("[UNARY QUERY] [DB: {}] - Unable to open/map {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(returnData->mutable_querystatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
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

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(dbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePathStr) == 0) {
        std::string ErrMsg = fmt::format("[UNARY DELETE] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[UNARY DELETE] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    TimeStruct CurrTs(epochTime, CfgStruct.timeresolution());

    // Prepare to delete (Writing NULL byte)
    FILE *BFile;
    std::string TargetFileName = fmt::format("{}.XSTORE", CurrTs.Year);
    std::string TargetFilePath = JoinPath(DbBasePathStr, TargetFileName);

    // IF target file does NOT exist
    if (std::filesystem::exists(TargetFilePath) == 0) {
        std::string ErrMsg = fmt::format("[UNARY DELETE] [DB: {}] - {} does not exist", dbName, TargetFileName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
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

    std::string SuccessMsg = fmt::format("[UNARY DELETE] [DB: {}] - Deleted {} in {}", dbName, epochTime, TargetFileName);
    SetTaggedStatus(ToReturn.mutable_deletestatus(), "[INFO] ", SuccessMsg);
    return ToReturn;
}
