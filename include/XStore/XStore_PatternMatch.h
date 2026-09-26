#pragma once

#include "XStore.pb.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#ifdef XSTORE_HAVE_RE2
namespace re2 { class RE2; }
#endif

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

    // Longest run of fixed bytes in a case-sensitive LIKE pattern. Every row
    // whose column matches contains it verbatim in its serialized bytes, so a
    // raw memmem over row storage can skip rows without it (FindByPattern A+).
    // Empty for ILIKE, REGEX, or a pattern with no fixed byte.
    [[nodiscard]] std::string_view PrefilterLiteral() const { return prefilter_literal_; }

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
    mutable std::string fold_buf_;
#ifdef XSTORE_HAVE_RE2
    std::unique_ptr<re2::RE2> regex_;  // non-null iff compiled in PATTERN_REGEX mode
#endif
};
