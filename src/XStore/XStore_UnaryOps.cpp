#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

// UNARY INSERT
void XStore::UnaryInsert(const unsigned long long int &unixTime, const XStore_Proto::RowData &data, std::string_view dbNameView, XStore_Proto::UnaryInsert_REP *returnData) {
    // Ensure dbName string is all capitalized
    const std::string &dbName = UpperCaseInto(dbNameView);

    // Base directory of a DB
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

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
    const std::string &TargetFile = CachedYearFilePath(DbBasePathStr, CurrTs.Year);
    // Held for the rest of this function's scope (through the pwrite()
    // below) so a concurrent eviction on another thread can't close this
    // fd out from under us -- see FdShard's comment in XStore.h.
    std::shared_ptr<int> FdHandle = GetOrOpenFile(TargetFile, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (!FdHandle) {
        std::string ErrMsg = fmt::format("[UNARY INSERT] [DB: {}] - Unable to open {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
        return;
    }
    int Fd = *FdHandle;

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

    // Tried fmt::format_to() straight into the destination field here to
    // save the SuccessMsg temporary's copy -- benchmarked SLOWER (~13%) than
    // this, even with an upfront reserve(): format_to() through a generic
    // back_insert_iterator loses fmt::format()'s internal contiguous-buffer
    // fast path, so it does more per-byte work than fmt::format() + a plain
    // reserve()'d assign/append despite looking like fewer copies on paper.
    // SetTaggedStatus already reserves the exact final size once and does
    // two flat memcpy-style writes -- reverted to it after measuring.
    std::string SuccessMsg = fmt::format("[UNARY INSERT] [DB: {}] - Inserted {} to {}.XSTORE", dbName, unixTime, CurrTs.Year);
    SetTaggedStatus(returnData->mutable_insertstatus(), "[INFO] ", SuccessMsg);
    return;
}

// UNARY QUERY
void XStore::UnaryQuery(const unsigned long long int &unixTime, std::string_view dbNameView, XStore_Proto::UnaryQuery_REP *returnData) {
    returnData->set_timestamp(unixTime);

    // Ensure dbName string is all capitalized
    const std::string &dbName = UpperCaseInto(dbNameView);

    // Base directory of a DB
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

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
    const std::string &TargetFilePath = CachedYearFilePath(DbBasePathStr, CurrTs.Year);

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
    // Held for the rest of this function's scope (through the pread()
    // below) so a concurrent eviction on another thread can't close this
    // fd out from under us -- see FdShard's comment in XStore.h.
    std::shared_ptr<int> FdHandle = GetOrOpenFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (!FdHandle) {
        std::string ErrMsg = fmt::format("[UNARY QUERY] [DB: {}] - Unable to open {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(returnData->mutable_querystatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
        return;
    }
    int Fd = *FdHandle;

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
XStore_Proto::UnaryDelete_REP XStore::UnaryDelete(const unsigned long long int &epochTime, std::string_view dbNameView) {
    // Protobuf obj to be returned
    XStore_Proto::UnaryDelete_REP ToReturn;

    // Ensure dbName string is all capitalized
    const std::string &dbName = UpperCaseInto(dbNameView);

    // Base directory of a DB
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped, same fix
    // already applied to UnaryInsert/UnaryQuery.
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

    const std::string &TargetFilePath = CachedYearFilePath(DbBasePathStr, CurrTs.Year);

    // IF target file does NOT exist
    if (std::filesystem::exists(TargetFilePath) == 0) {
        std::string ErrMsg = fmt::format("[UNARY DELETE] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    // Cached fd instead of a fresh fopen()/fclose() per call -- fallocate64()
    // just needs a plain fd (punching holes isn't an mmap operation), and
    // GetOrOpenFile already hands one back with no FILE*/fileno() layer
    // needed, same fd cache UnaryInsert/UnaryQuery use. Existence was
    // already confirmed above, so O_CREAT/ftruncate here are no-ops. Held
    // through the fallocate64() call below so a concurrent eviction on
    // another thread can't close this fd mid-delete -- see FdShard's
    // comment in XStore.h.
    std::shared_ptr<int> FdHandle = GetOrOpenFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
    if (!FdHandle) {
        std::string ErrMsg = fmt::format("[UNARY DELETE] [DB: {}] - Unable to open {}.XSTORE", dbName, CurrTs.Year);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[ERROR] ", ErrMsg);
        spdlog::error(ErrMsg);
        return ToReturn;
    }
    int Fd = *FdHandle;

    //  Computes diff & offset
    unsigned long long int Diff = epochTime - CurrTs.FirstEpoch;
    unsigned long long int Offset = Diff * CfgStruct.rowlength();

    {
        // Lock prior to file OP
        FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength(), FileRangeLock::LockType::WRITE);

        // Deallocate data according to N bytes from rowLength
        fallocate64(Fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength());
    }

    std::string SuccessMsg = fmt::format("[UNARY DELETE] [DB: {}] - Deleted {} in {}.XSTORE", dbName, epochTime, CurrTs.Year);
    SetTaggedStatus(ToReturn.mutable_deletestatus(), "[INFO] ", SuccessMsg);
    return ToReturn;
}
