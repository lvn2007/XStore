// TEMPORARY IMPLEMENTATION (option A) of FINDBYPATTERN: a brute-force per-row
// scan with no index. Kept in its own file so the pending A/B/C decision
// (TODO.md) replaces exactly this file. Loop shape and locking mirror
// XStore::FindByValue (XStore_Aggregates.cpp) minus its catidx tiers.
// A+ (2026-09-26): a raw-bytes memmem prefilter on the pattern's longest fixed
// literal skips decoding rows that cannot match.
#include "include/XStore/XStore_ReplyLimit.h"
#include "include/XStore/XStore_Internal.h"
#include "include/XStore/XStore_PatternMatch.h"
#include "include/XStore/XStore_WireEncode.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_ScanLock.h"
#include <spdlog/fmt/fmt.h>
#include <cstring>
#include <filesystem>
#include <limits>
#include <optional>
#include <sys/mman.h>
#include <unistd.h>

void XStore::FindByPattern(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                           std::string_view db_name_view, int32_t column_idx, std::string_view pattern,
                           XStore_Proto::PatternMode mode, bool case_insensitive,
                           size_t chunk_size_bytes, const std::function<void(std::string&&, bool)> &emit_chunk, bool streamed_reply) {
    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    bool has_warning = false;
    std::string last_warning;
    auto set_warning = [&](const char *tag, const std::string &body) {
        has_warning = true;
        last_warning = tag;
        last_warning += body;
    };
    auto finish = [&](std::string &&chunk_buf) {
        if (has_warning) {
            XStoreWireEncode::AppendQueryStatus(chunk_buf, last_warning);
        }
        emit_chunk(std::move(chunk_buf), true);
    };

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        set_warning("[WARNING] ", fmt::format("[FINDBYPATTERN] [DB: {}] - {}.CFG does not exist", db_name, db_name));
        finish(std::string());
        return;
    }
    if (start_epoch_t > end_epoch_t) {
        set_warning("[WARNING] ", fmt::format("[FINDBYPATTERN] [DB: {}] - Target end time must be after start time", db_name));
        finish(std::string());
        return;
    }
    if (std::filesystem::is_directory(db_base_path_str) == 0) {
        set_warning("[WARNING] ", fmt::format("[FINDBYPATTERN] [DB: {}] - DB does not exist", db_name));
        finish(std::string());
        return;
    }
    if (column_idx < 0 || column_idx > std::numeric_limits<unsigned short>::max()) {
        set_warning("[WARNING] ", fmt::format("[FINDBYPATTERN] [DB: {}] - columnIdx out of range: {}", db_name, column_idx));
        finish(std::string());
        return;
    }
    std::string pattern_error;
    std::optional<PatternMatcher> matcher = PatternMatcher::Compile(pattern, mode, case_insensitive, pattern_error);
    if (!matcher.has_value()) {
        set_warning("[ERROR] ", fmt::format("[FINDBYPATTERN] [DB: {}] - Invalid pattern: {}", db_name, pattern_error));
        finish(std::string());
        return;
    }

    std::string chunk_buf;
    chunk_buf.reserve(chunk_size_bytes + 4096);
    // Same 2GB client-reassembly guard as FindByValue (and the same exemption for a streamed_reply): match count is unknown until scanned,
    // so it's a running total checked after every match.
    const unsigned long long reply_limit = streamed_reply ? std::numeric_limits<unsigned long long>::max() : xstore::ReplyLimitBytes().load();
    unsigned long long int total_emitted_bytes = 0;
    bool size_limit_exceeded = false;
    // Hoisted out of the row loop so a string column's buffer is recycled
    // across rows instead of reallocated per row.
    XStore_Proto::FData f_data;

    unsigned long long int iter_epoch = start_epoch_t;
    while (iter_epoch <= end_epoch_t && !size_limit_exceeded) {
        TimeStruct curr_ts(iter_epoch, cfg_struct.timeresolution());
        const std::string &target_file_path = CachedYearFilePath(db_base_path_str, curr_ts.year);
        const unsigned long long int segment_end = std::min(end_epoch_t, static_cast<unsigned long long int>(curr_ts.last_epoch));

        if (std::filesystem::exists(target_file_path) == 0) {
            set_warning("[WARNING] ", fmt::format("[FINDBYPATTERN] [DB: {}] - {}.XSTORE does not exist", db_name, curr_ts.year));
            iter_epoch = segment_end + 1;
            continue;
        }
        std::shared_ptr<void> mapped_handle = GetOrMapFile(target_file_path, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
        if (mapped_handle == nullptr) {
            set_warning("[ERROR] ", fmt::format("[FINDBYPATTERN] [DB: {}] - Unable to open/map {}.XSTORE", db_name, curr_ts.year));
            iter_epoch = segment_end + 1;
            continue;
        }
        const char *mapped_base = static_cast<const char *>(mapped_handle.get());
        const unsigned long long int first_offset = (iter_epoch - curr_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int region_length = (segment_end - iter_epoch + 1) * cfg_struct.rowlength();

        {
            // The rows are read under a READ lock one scan chunk at a time (xstore::scanlock); a completed output buffer waits
            // in `ready` until that chunk's lock is released, so sending to a slow client never holds off the inserts.
            std::vector<std::string> ready;

            // Whether the row's column holds a string/bytes value matching the pattern. fd is the caller's decode buffer
            // (ExtractColumnByIndexSkipParse merges into it, ReadMessage semantics, so it is cleared first or an unset column
            // would inherit the previous row's value).
            auto row_matches = [&](const char *row_ptr, XStore_Proto::FData &fd) -> bool {
                fd.Clear();
                if (!ExtractColumnByIndexSkipParse(row_ptr, cfg_struct.rowlength(),
                                                   static_cast<unsigned short>(column_idx), fd)) {
                    return false;
                }
                std::string_view text;
                if (fd.oneOfData_case() == XStore_Proto::FData::kStringData) {
                    text = fd.stringdata();
                } else if (fd.oneOfData_case() == XStore_Proto::FData::kBytesData) {
                    text = fd.bytesdata();
                } else {
                    return false;
                }
                return matcher->Matches(text);
            };
            // The sequential emitter: appends the matching row to chunk_buf, holds a full buffer in `ready`. Returns false once
            // the 2GB guard trips.
            auto emit_row = [&](unsigned long long int e, const char *row_ptr) -> bool {
                size_t size_before = chunk_buf.size();
                XStoreWireEncode::EncodeQueryResponseEntry(chunk_buf, e, row_ptr, cfg_struct.rowlength());
                total_emitted_bytes += static_cast<unsigned long long int>(chunk_buf.size() - size_before);
                if (chunk_buf.size() >= chunk_size_bytes) {
                    ready.push_back(std::move(chunk_buf));
                    chunk_buf.clear();
                    chunk_buf.reserve(chunk_size_bytes + 4096);
                }
                if (total_emitted_bytes > reply_limit) {
                    set_warning("[WARNING] ", fmt::format("[FINDBYPATTERN] [DB: {}] - Query results exceeded 2GB, results truncated", db_name));
                    size_limit_exceeded = true;
                    return false;
                }
                return true;
            };

            const std::string_view literal = matcher->PrefilterLiteral();
            // One scan chunk: rows [chunk_lo, chunk_lo + rows) at chunk_rows; the prefilter below starts afresh on each chunk.
            // emit(e, row_ptr) returns false to stop. Only reads shared state, so pieces may run it in parallel.
            auto scan_rows = [&](const char *chunk_rows, unsigned long long rows, unsigned long long int chunk_lo,
                                 XStore_Proto::FData &fd, auto &&emit) -> bool {
            const unsigned long long int chunk_end = chunk_lo + rows - 1;
            const size_t row_len = cfg_struct.rowlength();
            auto test_row = [&](unsigned long long int e) -> bool {
                const char *row_ptr = chunk_rows + (e - chunk_lo) * row_len;
                return !row_matches(row_ptr, fd) || emit(e, row_ptr);
            };
            if (literal.empty()) {
                for (unsigned long long int e = chunk_lo; e <= chunk_end; ++e) {
                    if (!test_row(e)) return false;
                }
            } else {
                // A+ raw-bytes prefilter: string/bytes values sit verbatim in the
                // serialized row, so every matching row contains `literal`. One
                // memmem over the contiguous span skips decoding rows without it;
                // hits in another column, across a row boundary, or in a hole's
                // zero bytes are rejected by test_row's full check. ILIKE (and only
                // ILIKE) searches ASCII case-insensitively for its lowercased literal.
                const bool literal_ci = matcher->PrefilterCaseInsensitive();
                const char *span_begin = chunk_rows;
                const char *span_end = span_begin + rows * row_len;
                const char *cursor = span_begin;
                // Stateful across hits: it remembers each case's next memchr hit, so
                // an anchor case that never occurs is scanned once per span, not per row.
                patternfold::CaseInsensitiveFinder ci_finder(span_end, literal.data(), literal.size());
                size_t hits = 0;
                while (cursor < span_end) {
                    const size_t remaining = static_cast<size_t>(span_end - cursor);
                    const void *hit = literal_ci ? ci_finder.Next(cursor)
                                                 : memmem(cursor, remaining, literal.data(), literal.size());
                    if (hit == nullptr) break;
                    const size_t row_in_span = static_cast<size_t>(static_cast<const char *>(hit) - span_begin) / row_len;
                    if (!test_row(chunk_lo + row_in_span)) return false;
                    // Adaptive fallback: a literal in most rows skips nothing, so
                    // finish the segment with the plain loop (the span is exactly
                    // segment_end - iter_epoch + 1 rows; rows up to this hit's are done).
                    const size_t rows_covered = row_in_span + 1;
                    if (++hits * 100 > rows_covered * XStoreTunables::FIND_BY_PATTERN_PREFILTER_ADAPT_HIT_PERCENT &&
                        rows_covered >= XStoreTunables::FIND_BY_PATTERN_PREFILTER_ADAPT_MIN_ROWS) {
                        for (unsigned long long int e = chunk_lo + rows_covered; e <= chunk_end; ++e) {
                            if (!test_row(e)) return false;
                        }
                        break;
                    }
                    cursor = span_begin + rows_covered * row_len;
                }
            }
            return true;
            };

            // Parallel (xstore::scanlock::Config::parallel_threads > 1 and at least two pieces): the pieces are scanned in parallel
            // and their matches are appended here in order; otherwise the sequential scan.
            if (!xstore::scanlock::ScanRegionOrdered(
                    target_file_path, const_cast<char *>(mapped_base), first_offset, region_length, cfg_struct.rowlength(),
                    iter_epoch - curr_ts.first_epoch,
                    [&](const char *chunk_rows, unsigned long long rows, unsigned long long row_offset, std::string &out) {
                        XStore_Proto::FData piece_fd;
                        scan_rows(chunk_rows, rows, iter_epoch + row_offset, piece_fd, [&](unsigned long long int e, const char *row_ptr) {
                            XStoreWireEncode::EncodeQueryResponseEntry(out, e, row_ptr, cfg_struct.rowlength());
                            return true;
                        });
                    },
                    [&](std::string &piece_out) {
                        total_emitted_bytes += static_cast<unsigned long long int>(piece_out.size());
                        chunk_buf.append(piece_out);
                        if (chunk_buf.size() >= chunk_size_bytes) {
                            emit_chunk(std::move(chunk_buf), false);
                            chunk_buf.clear();
                            chunk_buf.reserve(chunk_size_bytes + 4096);
                        }
                        if (total_emitted_bytes > reply_limit) {
                            set_warning("[WARNING] ", fmt::format("[FINDBYPATTERN] [DB: {}] - Query results exceeded 2GB, results truncated", db_name));
                            size_limit_exceeded = true;
                            return false;
                        }
                        return true;
                    })) {
                xstore::scanlock::ScanUnderReadLock(
                    target_file_path, const_cast<char *>(mapped_base), first_offset, region_length, cfg_struct.rowlength(),
                    iter_epoch - curr_ts.first_epoch,
                    [&](const char *chunk_rows, unsigned long long rows, unsigned long long row_offset) {
                        return scan_rows(chunk_rows, rows, iter_epoch + row_offset, f_data, emit_row) && !size_limit_exceeded;
                    },
                    [&] {
                        for (std::string &full : ready) emit_chunk(std::move(full), false);
                        ready.clear();
                    });
            }
        }
        iter_epoch = segment_end + 1;
    }

    finish(std::move(chunk_buf));
}
