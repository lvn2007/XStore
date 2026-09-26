#include "include/XStore/XStore_PatternMatch.h"
#include "include/XStore/XStore_Tunables.h"

#include <cstring>

#ifdef XSTORE_HAVE_RE2
#include <re2/re2.h>
#endif

namespace {
constexpr char AsciiLower(char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c | 0x20) : c;
}
} // namespace

PatternMatcher::PatternMatcher(PatternMatcher &&) noexcept = default;
PatternMatcher &PatternMatcher::operator=(PatternMatcher &&) noexcept = default;
PatternMatcher::~PatternMatcher() = default;

std::optional<PatternMatcher> PatternMatcher::Compile(std::string_view pattern, XStore_Proto::PatternMode mode,
                                                      bool case_insensitive, std::string &error) {
    if (pattern.size() > XStoreTunables::FIND_BY_PATTERN_MAX_PATTERN_BYTES) {
        error = "pattern exceeds " + std::to_string(XStoreTunables::FIND_BY_PATTERN_MAX_PATTERN_BYTES) + " bytes";
        return std::nullopt;
    }
    PatternMatcher matcher;
    matcher.case_insensitive_ = case_insensitive;

    if (mode == XStore_Proto::PATTERN_REGEX) {
#ifdef XSTORE_HAVE_RE2
        RE2::Options opts;
        opts.set_log_errors(false);
        opts.set_case_sensitive(!case_insensitive);
        opts.set_max_mem(XStoreTunables::FIND_BY_PATTERN_REGEX_MAX_MEM_BYTES);
        matcher.regex_ = std::make_unique<re2::RE2>(re2::StringPiece(pattern.data(), pattern.size()), opts);
        if (!matcher.regex_->ok()) {
            error = matcher.regex_->error();
            return std::nullopt;
        }
        return std::optional<PatternMatcher>(std::move(matcher));
#else
        error = "REGEX mode unavailable: server built without RE2";
        return std::nullopt;
#endif
    }
    if (mode != XStore_Proto::PATTERN_LIKE) {
        error = "unknown pattern mode " + std::to_string(static_cast<int>(mode));
        return std::nullopt;
    }

    matcher.segments_.emplace_back();
    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];
        if (c == '%') {
            matcher.segments_.emplace_back();
            continue;
        }
        bool any = false;
        if (c == '\\') {
            if (++i == pattern.size()) {
                error = "pattern ends with a dangling escape '\\'";
                return std::nullopt;
            }
            c = pattern[i];
        } else if (c == '_') {
            any = true;
        }
        Segment &seg = matcher.segments_.back();
        seg.bytes.push_back(case_insensitive ? AsciiLower(c) : c);
        seg.any_mask.push_back(any ? 1 : 0);
        seg.has_any = seg.has_any || any;
    }
    if (!case_insensitive) {
        for (const Segment &seg : matcher.segments_) {
            size_t run_start = 0;
            for (size_t i = 0; i <= seg.bytes.size(); ++i) {
                if (i == seg.bytes.size() || seg.any_mask[i] != 0) {
                    if (i - run_start > matcher.prefilter_literal_.size()) {
                        matcher.prefilter_literal_ = seg.bytes.substr(run_start, i - run_start);
                    }
                    run_start = i + 1;
                }
            }
        }
    }
    return std::optional<PatternMatcher>(std::move(matcher));
}

bool PatternMatcher::Matches(std::string_view text) const {
#ifdef XSTORE_HAVE_RE2
    if (regex_ != nullptr) {
        return re2::RE2::PartialMatch(re2::StringPiece(text.data(), text.size()), *regex_);
    }
#endif
    if (case_insensitive_) {
        fold_buf_.assign(text.data(), text.size());
        for (char &c : fold_buf_) c = AsciiLower(c);
        text = fold_buf_;
    }
    return MatchesLike(text);
}

bool PatternMatcher::SegmentEqualsAt(const Segment &seg, std::string_view text, size_t at) {
    if (seg.bytes.empty()) return true;
    if (!seg.has_any) return std::memcmp(text.data() + at, seg.bytes.data(), seg.bytes.size()) == 0;
    for (size_t i = 0; i < seg.bytes.size(); ++i) {
        if (seg.any_mask[i] == 0 && seg.bytes[i] != text[at + i]) return false;
    }
    return true;
}

// Leftmost start p in [from, limit - |seg|] where seg fits, else npos.
size_t PatternMatcher::FindSegment(const Segment &seg, std::string_view text, size_t from, size_t limit) {
    const size_t n = seg.bytes.size();
    if (limit < from || limit - from < n) return std::string_view::npos;
    if (n == 0) return from;
    if (!seg.has_any) {
        // glibc memmem: two-way algorithm with SSE2 on x86-64.
        const void *hit = memmem(text.data() + from, limit - from, seg.bytes.data(), n);
        return hit == nullptr ? std::string_view::npos
                              : static_cast<size_t>(static_cast<const char *>(hit) - text.data());
    }
    for (size_t p = from; p + n <= limit; ++p) {
        if (SegmentEqualsAt(seg, text, p)) return p;
    }
    return std::string_view::npos;
}

bool PatternMatcher::MatchesLike(std::string_view text) const {
    const Segment &front = segments_.front();
    if (segments_.size() == 1) {
        return text.size() == front.bytes.size() && SegmentEqualsAt(front, text, 0);
    }
    const Segment &back = segments_.back();
    if (text.size() < front.bytes.size() + back.bytes.size()) return false;
    if (!SegmentEqualsAt(front, text, 0)) return false;
    const size_t back_at = text.size() - back.bytes.size();
    if (!SegmentEqualsAt(back, text, back_at)) return false;
    // Middle segments are fixed-width and '%'-separated, so the leftmost fit
    // for each is always safe: it leaves the most room for the ones after it.
    size_t pos = front.bytes.size();
    for (size_t s = 1; s + 1 < segments_.size(); ++s) {
        const size_t hit = FindSegment(segments_[s], text, pos, back_at);
        if (hit == std::string_view::npos) return false;
        pos = hit + segments_[s].bytes.size();
    }
    return true;
}
