#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace {
// TargetFile is rebuilt from DbBasePathStr + Year on every call otherwise --
// a heap allocation paid even on a warm fd-cache hit, since fmt::format
// always produces a fresh std::string. Real time-series workloads mostly
// insert/query the current (dbName, year) repeatedly in a row, so a
// single-entry thread_local memo skips the rebuild entirely on the common
// case and only reformats on an actual db/year change. Shared by
// UnaryInsert and UnaryQuery -- safe, since a single worker thread only
// ever handles one request at a time (see ZMQ_Server::WorkerRoutine), so
// there's no cross-call aliasing while the returned reference is live.
const std::string& CachedYearFilePath(const std::string &dbName, const std::string &dbBasePathStr, unsigned short year) {
    thread_local std::string TlsDbName;
    thread_local unsigned short TlsYear = 0;
    thread_local std::string TlsPath;

    if (TlsDbName != dbName || TlsYear != year) {
        TlsPath = fmt::format("{}/{}.XSTORE", dbBasePathStr, year);
        TlsDbName = dbName;
        TlsYear = year;
    }
    return TlsPath;
}
}  // namespace

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

    // Cached fd for this (dbName, year) file -- created and ftruncate'd to
    // the full year's row count once; every subsequent UnaryInsert to the
    // same file reuses it (see GetOrOpenFile). Writes go through pwrite()
    // rather than a PROT_WRITE mmap: a pwrite() into a still-sparse region
    // is a buffered write that defers real block allocation to async
    // writeback, same cost whether the target offset is sequential or
    // random. An mmap write-fault into an untouched page can't defer that
    // -- it has to synchronously allocate the block before the fault
    // returns -- which made random-offset inserts far more expensive than
    // sequential ones.
    const std::string &TargetFile = CachedYearFilePath(dbName, DbBasePathStr, CurrTs.Year);
    int Fd = GetOrOpenFile(TargetFile, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (Fd < 0) {
        std::string ErrMsg = fmt::format("[UNARY INSERT] [DB: {}] - Unable to open {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
        return;
    }

    // A row that doesn't fit in the configured slot would otherwise silently
    // truncate (SerializeToArray's return value was previously unchecked),
    // corrupting whatever follows it in the file. Reject up front instead.
    // ByteSizeLong() walks every field, so it's computed once here and
    // reused below rather than called again to learn the same size.
    size_t SerializedSize = data.ByteSizeLong();
    if (SerializedSize > static_cast<size_t>(CfgStruct.rowlength())) {
        std::string ErrMsg = fmt::format("[UNARY INSERT] [DB: {}] - Row size {} exceeds configured row length {}", dbName, SerializedSize, CfgStruct.rowlength());
        SetTaggedStatus(returnData->mutable_insertstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    //  Computes diff & offset
    unsigned long long int Diff = unixTime - CurrTs.FirstEpoch;
    unsigned long long int Offset = Diff * CfgStruct.rowlength();

    // Thread-local scratch buffer, grown once and reused across calls --
    // avoids the SerializeAsString() heap allocation the original fwrite()
    // path paid on every insert. Serialize first, then zero only the tail
    // past the actual serialized size -- covers the same "no stale bytes
    // from a previous larger row leak through" guarantee as zeroing the
    // whole buffer up front, without redundantly touching the bytes
    // SerializeToArray is about to overwrite anyway.
    thread_local std::vector<char> RowBuf;
    if (RowBuf.size() < CfgStruct.rowlength()) {
        RowBuf.resize(CfgStruct.rowlength());
    }
    data.SerializeToArray(RowBuf.data(), CfgStruct.rowlength());
    std::memset(RowBuf.data() + SerializedSize, 0, CfgStruct.rowlength() - SerializedSize);

    ssize_t Written;
    {
        // Lock prior to file OP
        FileRangeLockGuard Guard(TargetFile, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::WRITE);
        Written = pwrite(Fd, RowBuf.data(), CfgStruct.rowlength(), static_cast<off_t>(Offset));
    }
    if (Written != static_cast<ssize_t>(CfgStruct.rowlength())) {
        std::string ErrMsg = fmt::format("[UNARY INSERT] [DB: {}] - Short/failed write to {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
        return;
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
    const std::string &TargetFilePath = CachedYearFilePath(dbName, DbBasePathStr, CurrTs.Year);

    // IF target year file DOES NOT exist -- checked explicitly because
    // GetOrOpenFile below opens with O_CREAT and would otherwise silently
    // create an empty file for a query that should just report no data.
    // IsFdCached short-circuits the stat() syscall on a warm cache: only
    // files actually opened via GetOrOpenFile are ever cached, and both
    // removal paths drop the cache entry in the same call that deletes the
    // file, so a cache hit is already proof the file exists.
    if (!IsFdCached(TargetFilePath) && std::filesystem::exists(TargetFilePath) == 0) {
        std::string ErrMsg = fmt::format("[UNARY QUERY] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Cached fd for this (dbName, year) file -- same fd cache UnaryInsert
    // uses (see GetOrOpenFile). Reads go through pread() rather than a
    // PROT_READ mmap: a RAND query's offset almost never lands on an
    // already-faulted page in a multi-GB sparse file, so mmap access paid a
    // synchronous major page fault on nearly every call -- the same
    // "major page faulting is too expensive" cost UnaryInsert's pwrite()
    // switch already fixed on the write side, just never carried over here.
    int Fd = GetOrOpenFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (Fd < 0) {
        std::string ErrMsg = fmt::format("[UNARY QUERY] [DB: {}] - Unable to open {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(returnData->mutable_querystatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
        return;
    }

    //  Computes diff & offset
    unsigned long long int Diff = unixTime - CurrTs.FirstEpoch;
    unsigned long long int Offset = Diff * CfgStruct.rowlength();

    // Thread-local scratch buffer, grown once and reused across calls --
    // same reasoning as UnaryInsert's RowBuf.
    thread_local std::vector<char> RowBuf;
    if (RowBuf.size() < CfgStruct.rowlength()) {
        RowBuf.resize(CfgStruct.rowlength());
    }

    {
        //  Lock range prior to reading
        FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::READ);
        pread(Fd, RowBuf.data(), CfgStruct.rowlength(), static_cast<off_t>(Offset));
    }

    returnData->mutable_data()->ParseFromArray(RowBuf.data(), CfgStruct.rowlength());
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
