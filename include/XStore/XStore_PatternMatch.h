#pragma once

#include "XStore.pb.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#ifdef XSTORE_HAVE_RE2
namespace re2 { class RE2; }
#endif

namespace patternfold {

// Folds only A-Z, exactly like ILIKE; every other byte (including >= 0x80) is
// compared exactly.
constexpr char AsciiLower(char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c | 0x20) : c;
}

// memmem with ASCII case folding over a span ending at `end`, callable repeatedly with a
// non-decreasing cursor. `lit` MUST already be AsciiLower-folded and outlive the
// finder. It searches for one byte of the literal (preferring a non-letter,
// which needs a single memchr; a letter needs one per case) and verifies the
// rest folded. The last memchr hit of each case is kept ACROSS Next() calls, so
// a case that never occurs (or occurs far ahead) is scanned once per span, not
// once per call -- per-call state would make a row-by-row scan quadratic.
class CaseInsensitiveFinder {
public:
    CaseInsensitiveFinder(const void *end, const char *lit, size_t lit_len)
        : end_(static_cast<const char *>(end)), lit_(lit), lit_len_(lit_len) {
        for (size_t i = 0; i < lit_len; ++i) {
            if (lit[i] < 'a' || lit[i] > 'z') {
                k_ = i;
                break;
            }
        }
        letter_ = lit_len != 0 && lit[k_] >= 'a' && lit[k_] <= 'z';
        lower_byte_ = lit_len != 0 ? static_cast<unsigned char>(lit[k_]) : 0;
        upper_byte_ = letter_ ? lower_byte_ - 0x20 : lower_byte_;
        upper_done_ = !letter_;
    }

    // First match start >= cursor (cursors must not decrease between calls) with
    // the whole literal inside [cursor, end); nullptr if none.
    [[nodiscard]] const void *Next(const void *cursor_v) {
        const char *cursor = static_cast<const char *>(cursor_v);
        if (lit_len_ == 0) return cursor;
        if (static_cast<size_t>(end_ - cursor) < lit_len_) return nullptr;
        // A candidate start p needs p[k] == lit[k] and p + lit_len <= end.
        const char *const scan_end = end_ - (lit_len_ - k_);  // last probe position (inclusive)
        const char *cur = cursor + k_;
        while (cur <= scan_end) {
            const size_t span = static_cast<size_t>(scan_end - cur) + 1;
            if (!lower_done_ && (next_lower_ == nullptr || next_lower_ < cur)) {
                next_lower_ = static_cast<const char *>(std::memchr(cur, lower_byte_, span));
                lower_done_ = next_lower_ == nullptr;
            }
            if (!upper_done_ && (next_upper_ == nullptr || next_upper_ < cur)) {
                next_upper_ = static_cast<const char *>(std::memchr(cur, upper_byte_, span));
                upper_done_ = next_upper_ == nullptr;
            }
            const char *hit = nullptr;
            if (!lower_done_) hit = next_lower_;
            if (!upper_done_ && (hit == nullptr || next_upper_ < hit)) hit = next_upper_;
            if (hit == nullptr) return nullptr;
            const char *start = hit - k_;
            size_t i = 0;
            while (i < lit_len_ && AsciiLower(start[i]) == lit_[i]) ++i;
            if (i == lit_len_) return start;
            cur = hit + 1;
        }
        return nullptr;
    }

private:
    const char *end_;
    const char *lit_;
    size_t lit_len_;
    size_t k_ = 0;
    int lower_byte_ = 0;
    int upper_byte_ = 0;
    bool letter_ = false;
    // Cached first hit >= the last cursor for each case; *_done_ = none remains
    // before the end of the span (stays true: cursors only move forward).
    const char *next_lower_ = nullptr;
    const char *next_upper_ = nullptr;
    bool lower_done_ = false;
    bool upper_done_ = true;
};

// One-shot memmem-style wrapper over CaseInsensitiveFinder.
[[nodiscard]] inline const void *FindCaseInsensitive(const void *hay_v, size_t hay_len, const char *lit, size_t lit_len) {
    const char *hay = static_cast<const char *>(hay_v);
    CaseInsensitiveFinder finder(hay + hay_len, lit, lit_len);
    return finder.Next(hay);
}

} // namespace patternfold

// Compiled string predicate for XStore::FindByPattern: compile once per
// query, then Matches() once per row. Not thread-safe -- case-insensitive
// LIKE reuses an internal fold buffer -- so each query owns its own instance.
class PatternMatcher {
public:
    // nullopt + error filled on a malformed pattern, an over-long pattern, an
    // unknown mode, or REGEX on a server built without RE2.
    [[nodiscard]] static std::optional<PatternMatcher> Compile(std::string_view pattern, XStore_Proto::PatternMode mode,
                                                               bool case_insensitive, std::string &error);

    PatternMatcher(PatternMatcher &&) noexcept;
    PatternMatcher &operator=(PatternMatcher &&) noexcept;
    ~PatternMatcher();

    [[nodiscard]] bool Matches(std::string_view text) const;

    // A byte string every matching column must contain in its serialized bytes,
    // so a raw search over row storage can skip rows without it (FindByPattern
    // A+). LIKE: the longest fixed run (ASCII-lowercased for ILIKE, see
    // PrefilterCaseInsensitive()). REGEX: a conservative required literal (see
    // RegexRequiredLiteral()). Empty when none can be proved.
    [[nodiscard]] std::string_view PrefilterLiteral() const { return prefilter_literal_; }

    // True when PrefilterLiteral() is lowercased and must be searched with
    // patternfold::CaseInsensitiveFinder (ILIKE); false for a plain memmem.
    [[nodiscard]] bool PrefilterCaseInsensitive() const { return prefilter_case_insensitive_; }

    // Longest byte run every text matching `pattern` (RE2 syntax, partial
    // match) must contain, or "" if none is proved. Strictly conservative: empty
    // for case-insensitive, '|', '(', ')', non-ASCII, \Q \E, \x \p \P
    // and other alphanumeric escapes, or a malformed quantifier. Public for tests.
    [[nodiscard]] static std::string RegexRequiredLiteral(std::string_view pattern, bool case_insensitive);

private:
    PatternMatcher() = default;

    // The fixed-width run of a LIKE pattern between two unescaped '%'.
    // any_mask[i] == 1 where the pattern had an unescaped '_'.
    struct Segment {
        std::string bytes;
        std::vector<uint8_t> any_mask;
        bool has_any = false;
    };

    [[nodiscard]] static bool SegmentEqualsAt(const Segment &seg, std::string_view text, size_t at);
    [[nodiscard]] static size_t FindSegment(const Segment &seg, std::string_view text, size_t from, size_t limit);
    [[nodiscard]] bool MatchesLike(std::string_view text) const;

    std::vector<Segment> segments_;  // k unescaped '%' -> k + 1 segments
    bool case_insensitive_ = false;
    std::string prefilter_literal_;
    bool prefilter_case_insensitive_ = false;
    mutable std::string fold_buf_;
#ifdef XSTORE_HAVE_RE2
    std::unique_ptr<re2::RE2> regex_;  // non-null iff compiled in PATTERN_REGEX mode
#endif
};
