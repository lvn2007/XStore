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
void XStore::UnaryInsert(unsigned long long int unix_time, const XStore_Proto::RowData &data, std::string_view db_name_view,
                          XStore_Proto::UnaryInsert_REP *return_data, XStore_Proto::WalDurabilityMode durability_mode) {
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

    // A row that doesn't fit in the configured slot would otherwise silently
    // truncate (SerializeToArray's return value was previously unchecked),
    // corrupting whatever follows it in the file. Reject up front instead.
    size_t serialized_size = data.ByteSizeLong();
    if (serialized_size > static_cast<size_t>(cfg_struct.rowlength())) {
        std::string err_msg = fmt::format("[UNARY INSERT] [DB: {}] - Row size {} exceeds configured row length {}", db_name, serialized_size, cfg_struct.rowlength());
        SetTaggedStatus(return_data->mutable_insertstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    // Thread-local scratch buffer, grown once and reused across calls.
    // Serialize first, then zero only the tail past the actual serialized
    // size -- no stale bytes from a previous larger row leak through.
    thread_local std::vector<char> row_buf;
    if (row_buf.size() < static_cast<size_t>(cfg_struct.rowlength())) {
        row_buf.resize(cfg_struct.rowlength());
    }
    data.SerializeToArray(row_buf.data(), cfg_struct.rowlength());
    std::memset(row_buf.data() + serialized_size, 0, cfg_struct.rowlength() - serialized_size);

    // Hand off to the WAL instead of writing the data file directly here --
    // EnqueueWalRecord's shard writer thread fsyncs (for SYNC) and applies
    // this exact row before returning control (ASYNC returns immediately
    // after the lock-free enqueue). A WalRecord represents one write
    // REQUEST (1..N rows, one completion future per request) --
    // UnaryInsert's request is always exactly 1 row; BatchInsert/
    // RangeInsert build the same type with N rows per year-bucket group
    // instead of needing a separate WAL path.
    WalRecord record;
    record.db_name = db_name;
    record.db_base_path = db_base_path_str;
    record.time_resolution = cfg_struct.timeresolution();
    record.row_length = cfg_struct.rowlength();
    record.durability_mode = durability_mode;
    // Grow the index width to this row's width BEFORE any WalRecord/zone-map
    // file is created for it, so a fresh DB's first insert is born at its
    // true width instead of staying at the floor until a restart picks up
    // the grown column count (see BatchInsert/RangeInsert for the same
    // fix's multi-row form).
    const int widest = data.fielddata_size();
    MaybeGrowColumnCount(db_name, db_base_path_str, widest);
    // One width for both the observations and the zone map ApplyWalRecord
    // will open/create for them -- see WalRecord::tracked_column_count.
    // Folded in with the pre-grown `widest` so a brand-new index file is
    // created at this row's true width on this very first write, not just
    // EffectiveTrackedColumnCount's pre-growth floor.
    record.tracked_column_count = std::max(EffectiveTrackedColumnCount(cfg_struct),
                                            std::min<uint32_t>(static_cast<uint32_t>(widest), XStoreTunables::MAX_TRACKED_COLUMN_COUNT));
    record.rows.push_back(WalRow{unix_time, std::string(row_buf.data(), cfg_struct.rowlength()),
                                  BuildZoneMapObservations(*this, data, record.tracked_column_count),
                                  BuildCatIdxObservations(*this, data, record.tracked_column_count),
                                  record.tracked_column_count});

    if (durability_mode == XStore_Proto::BYPASS) {
        // Skip WAL entirely -- no queue, no CRC32, no second write. Applies
        // the row straight to the data file on THIS (ZMQ worker) thread,
        // reusing the exact same helper the WAL writer thread calls for
        // every other mode's apply step (GetOrOpenFile fd cache +
        // FileRangeLockGuard + pwrite) -- no WAL record is ever written for
        // this row, so a crash before the OS's own writeback flushes it is
        // unrecoverable (see the BYPASS comment in XStore.proto: durability
        // window is kernel writeback timing, not a fast WAL fdatasync).
        if (!ApplyWalRecord(record)) {
            std::string err_msg = fmt::format("[UNARY INSERT] [DB: {}] - BYPASS write failed", db_name);
            SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
            return;
        }
    } else if (!EnqueueWalRecord(std::move(record))) {
        std::string err_msg = fmt::format("[UNARY INSERT] [DB: {}] - Failed to enqueue WAL record", db_name);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return;
    }

    TimeStruct curr_ts(unix_time, cfg_struct.timeresolution());
    std::string success_msg = fmt::format("[UNARY INSERT] [DB: {}] - Inserted {} to {}.XSTORE", db_name, unix_time, curr_ts.year);
    SetTaggedStatus(return_data->mutable_insertstatus(), "[INFO] ", success_msg);
}

// UNARY QUERY
void XStore::UnaryQuery(unsigned long long int unix_time, std::string_view db_name_view, XStore_Proto::UnaryQuery_REP *return_data) {
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
    if (row_buf.size() < static_cast<size_t>(cfg_struct.rowlength())) {
        row_buf.resize(cfg_struct.rowlength());
    }

    {
        //  Lock range prior to reading
        FileRangeLockGuard guard(target_file_path, offset, cfg_struct.rowlength(), FileRangeLock::LockType::READ);
        // (void) doesn't suppress __wur under _FORTIFY_SOURCE; short read is ignored on purpose.
        [[maybe_unused]] ssize_t pread_ret = pread(fd, row_buf.data(), cfg_struct.rowlength(), static_cast<off_t>(offset));
    }

    return_data->mutable_data()->ParseFromArray(row_buf.data(), cfg_struct.rowlength());
}

// UNARY DELETE
XStore_Proto::UnaryDelete_REP XStore::UnaryDelete(unsigned long long int epoch_time, std::string_view db_name_view) {
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
