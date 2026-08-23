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
void XStore::UnaryInsert(const unsigned long long int &unix_time, const XStore_Proto::RowData &data, std::string_view db_name_view, XStore_Proto::UnaryInsert_REP *return_data) {
    // Ensure dbName string is all capitalized
    const std::string &db_name = UpperCaseInto(db_name_view);

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped.
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[UNARY INSERT] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    TimeStruct curr_ts(unix_time, cfg_struct.timeresolution());

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
    const std::string &target_file = CachedYearFilePath(db_base_path_str, curr_ts.year);
    // Held for the rest of this function's scope (through the pwrite()
    // below) so a concurrent eviction on another thread can't close this
    // fd out from under us -- see FdShard's comment in XStore.h.
    std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
    if (!fd_handle) {
        std::string err_msg = fmt::format("[UNARY INSERT] [DB: {}] - Unable to open {}.XSTORE", db_name, curr_ts.year);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return;
    }
    int fd = *fd_handle;

    // A row that doesn't fit in the configured slot would otherwise silently
    // truncate (SerializeToArray's return value was previously unchecked),
    // corrupting whatever follows it in the file. Reject up front instead.
    // ByteSizeLong() walks every field, so it's computed once here and
    // reused below rather than called again to learn the same size.
    size_t serialized_size = data.ByteSizeLong();
    if (serialized_size > static_cast<size_t>(cfg_struct.rowlength())) {
        std::string err_msg = fmt::format("[UNARY INSERT] [DB: {}] - Row size {} exceeds configured row length {}", db_name, serialized_size, cfg_struct.rowlength());
        SetTaggedStatus(return_data->mutable_insertstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    //  Computes diff & offset
    unsigned long long int diff = unix_time - curr_ts.first_epoch;
    unsigned long long int offset = diff * cfg_struct.rowlength();

    // Thread-local scratch buffer, grown once and reused across calls --
    // avoids the SerializeAsString() heap allocation the original fwrite()
    // path paid on every insert. Serialize first, then zero only the tail
    // past the actual serialized size -- covers the same "no stale bytes
    // from a previous larger row leak through" guarantee as zeroing the
    // whole buffer up front, without redundantly touching the bytes
    // SerializeToArray is about to overwrite anyway.
    thread_local std::vector<char> row_buf;
    if (row_buf.size() < cfg_struct.rowlength()) {
        row_buf.resize(cfg_struct.rowlength());
    }
    data.SerializeToArray(row_buf.data(), cfg_struct.rowlength());
    std::memset(row_buf.data() + serialized_size, 0, cfg_struct.rowlength() - serialized_size);

    ssize_t written;
    {
        // Lock prior to file OP
        FileRangeLockGuard guard(target_file, offset, cfg_struct.rowlength(), FileRangeLock::LockType::WRITE);
        written = pwrite(fd, row_buf.data(), cfg_struct.rowlength(), static_cast<off_t>(offset));
    }
    if (written != static_cast<ssize_t>(cfg_struct.rowlength())) {
        std::string err_msg = fmt::format("[UNARY INSERT] [DB: {}] - Short/failed write to {}.XSTORE", db_name, curr_ts.year);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
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
    std::string success_msg = fmt::format("[UNARY INSERT] [DB: {}] - Inserted {} to {}.XSTORE", db_name, unix_time, curr_ts.year);
    SetTaggedStatus(return_data->mutable_insertstatus(), "[INFO] ", success_msg);
    return;
}

// UNARY QUERY
void XStore::UnaryQuery(const unsigned long long int &unix_time, std::string_view db_name_view, XStore_Proto::UnaryQuery_REP *return_data) {
    return_data->set_timestamp(unix_time);

    // Ensure dbName string is all capitalized
    const std::string &db_name = UpperCaseInto(db_name_view);

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped.
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[UNARY QUERY] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(return_data->mutable_querystatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    //  Time structure contains all components per UNIXTIME:
    //      Year, month, day, hour, minute, sec, ...
    TimeStruct curr_ts(unix_time, cfg_struct.timeresolution());

    // Base directory of a DB & Target path of file to READ
    const std::string &target_file_path = CachedYearFilePath(db_base_path_str, curr_ts.year);

    // IF target year file DOES NOT exist -- checked explicitly because
    // GetOrOpenFile below opens with O_CREAT and would otherwise silently
    // create an empty file for a query that should just report no data.
    // IsFdCached short-circuits the stat() syscall on a warm cache: only
    // files actually opened via GetOrOpenFile are ever cached, and both
    // removal paths drop the cache entry in the same call that deletes the
    // file, so a cache hit is already proof the file exists.
    if (!IsFdCached(target_file_path) && std::filesystem::exists(target_file_path) == 0) {
        std::string err_msg = fmt::format("[UNARY QUERY] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(return_data->mutable_querystatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
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
    std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file_path, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
    if (!fd_handle) {
        std::string err_msg = fmt::format("[UNARY QUERY] [DB: {}] - Unable to open {}.XSTORE", db_name, curr_ts.year);
        SetTaggedStatus(return_data->mutable_querystatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return;
    }
    int fd = *fd_handle;

    //  Computes diff & offset
    unsigned long long int diff = unix_time - curr_ts.first_epoch;
    unsigned long long int offset = diff * cfg_struct.rowlength();

    // Thread-local scratch buffer, grown once and reused across calls --
    // same reasoning as UnaryInsert's RowBuf.
    thread_local std::vector<char> row_buf;
    if (row_buf.size() < cfg_struct.rowlength()) {
        row_buf.resize(cfg_struct.rowlength());
    }

    {
        //  Lock range prior to reading
        FileRangeLockGuard guard(target_file_path, offset, cfg_struct.rowlength(), FileRangeLock::LockType::READ);
        pread(fd, row_buf.data(), cfg_struct.rowlength(), static_cast<off_t>(offset));
    }

    return_data->mutable_data()->ParseFromArray(row_buf.data(), cfg_struct.rowlength());
}

// UNARY DELETE
XStore_Proto::UnaryDelete_REP XStore::UnaryDelete(const unsigned long long int &epoch_time, std::string_view db_name_view) {
    // Protobuf obj to be returned
    XStore_Proto::UnaryDelete_REP to_return;

    // Ensure dbName string is all capitalized
    const std::string &db_name = UpperCaseInto(db_name_view);

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped, same fix
    // already applied to UnaryInsert/UnaryQuery.
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[UNARY DELETE] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return to_return;
    }

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    TimeStruct curr_ts(epoch_time, cfg_struct.timeresolution());

    const std::string &target_file_path = CachedYearFilePath(db_base_path_str, curr_ts.year);

    // IF target file does NOT exist
    if (std::filesystem::exists(target_file_path) == 0) {
        std::string err_msg = fmt::format("[UNARY DELETE] [DB: {}] - {}.XSTORE does not exist", db_name, curr_ts.year);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return to_return;
    }

    // Cached fd instead of a fresh fopen()/fclose() per call -- fallocate64()
    // just needs a plain fd (punching holes isn't an mmap operation), and
    // GetOrOpenFile already hands one back with no FILE*/fileno() layer
    // needed, same fd cache UnaryInsert/UnaryQuery use. Existence was
    // already confirmed above, so O_CREAT/ftruncate here are no-ops. Held
    // through the fallocate64() call below so a concurrent eviction on
    // another thread can't close this fd mid-delete -- see FdShard's
    // comment in XStore.h.
    std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file_path, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
    if (!fd_handle) {
        std::string err_msg = fmt::format("[UNARY DELETE] [DB: {}] - Unable to open {}.XSTORE", db_name, curr_ts.year);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return to_return;
    }
    int fd = *fd_handle;

    //  Computes diff & offset
    unsigned long long int diff = epoch_time - curr_ts.first_epoch;
    unsigned long long int offset = diff * cfg_struct.rowlength();

    {
        // Lock prior to file OP
        FileRangeLockGuard guard(target_file_path, offset, cfg_struct.rowlength(), FileRangeLock::LockType::WRITE);

        // Deallocate data according to N bytes from rowLength
        fallocate64(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfg_struct.rowlength());
    }

    std::string success_msg = fmt::format("[UNARY DELETE] [DB: {}] - Deleted {} in {}.XSTORE", db_name, epoch_time, curr_ts.year);
    SetTaggedStatus(to_return.mutable_deletestatus(), "[INFO] ", success_msg);
    return to_return;
}
