#include <gtest/gtest.h>
#include "XStore/XStore_PatternMatch.h"
#include "XStore/XStore_Tunables.h"
#include "XStore/XStore.h"
#include "XStoreTestSupport.h"
#include "XStore.pb.h"
#include <chrono>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace {
bool Like(std::string_view pattern, std::string_view text, bool case_insensitive = false) {
    std::string error;
    auto matcher = PatternMatcher::Compile(pattern, XStore_Proto::PATTERN_LIKE, case_insensitive, error);
    EXPECT_TRUE(matcher.has_value()) << "pattern '" << pattern << "': " << error;
    return matcher.has_value() && matcher->Matches(text);
}
} // namespace

TEST(PatternMatcherTest, NoWildcardsMeansWholeValueEquality) {
    EXPECT_TRUE(Like("AAPL", "AAPL"));
    EXPECT_FALSE(Like("AAPL", "AAP"));
    EXPECT_FALSE(Like("AAPL", "AAPLX"));
    EXPECT_TRUE(Like("", ""));
    EXPECT_FALSE(Like("", "A"));
}

TEST(PatternMatcherTest, PercentMatchesAnyRunIncludingEmpty) {
    EXPECT_TRUE(Like("%", ""));
    EXPECT_TRUE(Like("%", "anything"));
    EXPECT_TRUE(Like("AA%", "AAPL"));
    EXPECT_TRUE(Like("%PL", "AAPL"));
    EXPECT_TRUE(Like("%AP%", "AAPL"));
    EXPECT_TRUE(Like("A%L", "AL"));
    EXPECT_FALSE(Like("A%L", "A"));   // front and back segments must not overlap
    EXPECT_FALSE(Like("AA%", "MSFT"));
}

TEST(PatternMatcherTest, UnderscoreMatchesExactlyOneByte) {
    EXPECT_TRUE(Like("A_PL", "AAPL"));
    EXPECT_FALSE(Like("A_PL", "APL"));
    EXPECT_FALSE(Like("A_PL", "AAAPL"));
    EXPECT_TRUE(Like("%a_c%", "xxabcxx"));
    // One byte, not one UTF-8 code point: "é" is two bytes.
    EXPECT_FALSE(Like("_", "\xC3\xA9"));
    EXPECT_TRUE(Like("__", "\xC3\xA9"));
}

TEST(PatternMatcherTest, MiddleSegmentsTakeLeftmostFit) {
    EXPECT_TRUE(Like("%ab%ab%", "abab"));
    EXPECT_FALSE(Like("%ab%ab%", "aba"));
    EXPECT_TRUE(Like("a%b%c", "acbc"));
    EXPECT_FALSE(Like("a%b%c", "acb"));
    EXPECT_TRUE(Like("%x_z%x_z%", "x1zx2z"));
}

TEST(PatternMatcherTest, EscapedWildcardsAreLiteral) {
    EXPECT_TRUE(Like("100\\%", "100%"));
    EXPECT_FALSE(Like("100\\%", "1000"));
    EXPECT_TRUE(Like("a\\_b", "a_b"));
    EXPECT_FALSE(Like("a\\_b", "axb"));
    EXPECT_TRUE(Like("a\\\\b", "a\\b"));
}

TEST(PatternMatcherTest, DanglingEscapeIsRejected) {
    std::string error;
    auto matcher = PatternMatcher::Compile("abc\\", XStore_Proto::PATTERN_LIKE, false, error);
    EXPECT_FALSE(matcher.has_value());
    EXPECT_NE(error.find("escape"), std::string::npos) << error;
}

TEST(PatternMatcherTest, CaseInsensitiveFoldsAsciiOnly) {
    EXPECT_TRUE(Like("%aapl%", "xxAAPLyy", /*case_insensitive=*/true));
    EXPECT_FALSE(Like("%aapl%", "xxAAPLyy", /*case_insensitive=*/false));
    EXPECT_TRUE(Like("A_pL", "aapl", true));
    // é (C3 A9) vs É (C3 89): non-ASCII bytes are compared exactly.
    EXPECT_FALSE(Like("\xC3\xA9", "\xC3\x89", true));
}

TEST(PatternMatcherTest, EmbeddedNulBytesAreOrdinaryBytes) {
    EXPECT_TRUE(Like("a_b", std::string_view("a\0b", 3)));
    EXPECT_TRUE(Like(std::string_view("%\0%", 3), std::string_view("xx\0yy", 5)));
}

TEST(PatternMatcherTest, PatternLongerThanCapIsRejected) {
    std::string error;
    std::string too_long(XStoreTunables::FIND_BY_PATTERN_MAX_PATTERN_BYTES + 1, 'a');
    EXPECT_FALSE(PatternMatcher::Compile(too_long, XStore_Proto::PATTERN_LIKE, false, error).has_value());
    EXPECT_NE(error.find("exceeds"), std::string::npos) << error;
    std::string at_cap(XStoreTunables::FIND_BY_PATTERN_MAX_PATTERN_BYTES, 'a');
    EXPECT_TRUE(PatternMatcher::Compile(at_cap, XStore_Proto::PATTERN_LIKE, false, error).has_value());
}

TEST(PatternMatcherTest, UnknownModeIsRejected) {
    std::string error;
    EXPECT_FALSE(PatternMatcher::Compile("x", static_cast<XStore_Proto::PatternMode>(99), false, error).has_value());
    EXPECT_NE(error.find("unknown pattern mode"), std::string::npos) << error;
}

namespace {
std::string Literal(std::string_view pattern, bool case_insensitive = false) {
    std::string error;
    auto matcher = PatternMatcher::Compile(pattern, XStore_Proto::PATTERN_LIKE, case_insensitive, error);
    EXPECT_TRUE(matcher.has_value()) << error;
    return matcher.has_value() ? std::string(matcher->PrefilterLiteral()) : std::string("<invalid>");
}
} // namespace

TEST(PatternMatcherTest, PrefilterLiteralIsLongestFixedRun) {
    EXPECT_EQ(Literal("AAPL%"), "AAPL");
    EXPECT_EQ(Literal("%order-1234%"), "order-1234");
    EXPECT_EQ(Literal("%ab_cde%x"), "cde");
    EXPECT_EQ(Literal("ab%cd"), "ab");            // tie -> first
    EXPECT_EQ(Literal("a\\%bc%"), "a%bc");        // escaped % is a fixed byte
    EXPECT_EQ(Literal("x\\_yz"), "x_yz");         // escaped _ is a fixed byte
    EXPECT_EQ(Literal(std::string_view("%a\0b%", 5)), std::string("a\0b", 3));
}

namespace {
// Reference for the finder: first start >= from with the folded literal fully inside hay.
long NaiveCaseFind(std::string_view hay, size_t from, std::string_view lit) {
    for (size_t p = from; p + lit.size() <= hay.size(); ++p) {
        size_t i = 0;
        while (i < lit.size() && patternfold::AsciiLower(hay[p + i]) == lit[i]) ++i;
        if (i == lit.size()) return static_cast<long>(p);
    }
    return -1;
}
} // namespace

TEST(PatternMatcherTest, CaseInsensitiveFinderRepeatedCallsMatchNaiveSearch) {
    // Row-by-row cursor, as FindByPattern drives it. Only upper case 'A'/'B' ever
    // occur, so the lowercase anchor's cached memchr stays "exhausted" across calls.
    std::mt19937_64 rng(77);
    const char alphabet[] = {'A', 'B', 'a', 'x', '-', '1', 'P', 'L'};
    for (const std::string lit : {"aapl", "-1", "b-a", "x", "ab1"}) {
        std::string hay(20'000, ' ');
        for (char &c : hay) c = alphabet[rng() % 6 == 0 ? rng() % sizeof(alphabet) : rng() % 2];
        for (size_t at = 37; at + lit.size() < hay.size(); at += 97) {
            for (size_t i = 0; i < lit.size(); ++i) hay[at + i] = ((rng() & 1) && lit[i] >= 'a' && lit[i] <= 'z') ? static_cast<char>(lit[i] & ~0x20) : lit[i];
        }
        patternfold::CaseInsensitiveFinder finder(hay.data() + hay.size(), lit.data(), lit.size());
        size_t cursor = 0;
        for (int call = 0; call < 2000 && cursor < hay.size(); ++call) {
            const void *got = finder.Next(hay.data() + cursor);
            const long want = NaiveCaseFind(hay, cursor, lit);
            if (want < 0) {
                ASSERT_EQ(got, nullptr) << lit << " cursor=" << cursor;
                break;
            }
            ASSERT_NE(got, nullptr) << lit << " cursor=" << cursor;
            ASSERT_EQ(static_cast<const char *>(got) - hay.data(), want) << lit << " cursor=" << cursor;
            // Next row start (64-byte rows), sometimes skipping several rows.
            cursor = (static_cast<size_t>(want) / 64 + 1 + rng() % 3 * (rng() % 2)) * 64;
        }
    }
}

TEST(PatternMatcherTest, CaseInsensitiveFinderIsLinearOverRepeatedCalls) {
    // 10 MB of 100-byte rows, "AAPL" (upper case only) at the start of each: the
    // lowercase 'a' anchor never occurs. A per-call rescan would be ~100k x 5 MB
    // of memchr (minutes); the cached finder is one pass. Generous bound, coarse guard.
    std::string hay(10'000'000, '-');
    for (size_t at = 0; at + 4 <= hay.size(); at += 100) hay.replace(at, 4, "AAPL");
    patternfold::CaseInsensitiveFinder finder(hay.data() + hay.size(), "aapl", 4);
    const auto start = std::chrono::steady_clock::now();
    size_t cursor = 0;
    size_t calls = 0;
    while (cursor < hay.size()) {
        const void *hit = finder.Next(hay.data() + cursor);
        if (hit == nullptr) break;
        ASSERT_EQ(static_cast<size_t>(static_cast<const char *>(hit) - hay.data()), cursor);
        cursor += 100;
        ++calls;
    }
    EXPECT_EQ(calls, 100'000u);
    EXPECT_LT(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count(), 2.0);
}

TEST(PatternMatcherTest, PrefilterLiteralEmptyWhenUnusable) {
    EXPECT_EQ(Literal("%"), "");
    EXPECT_EQ(Literal(""), "");
    EXPECT_EQ(Literal("___"), "");
    EXPECT_EQ(Literal("%_%"), "");
}

TEST(PatternMatcherTest, IlikePrefilterLiteralIsLowercasedAndFlagged) {
    auto compile = [](std::string_view pattern, bool ci) {
        std::string error;
        auto matcher = PatternMatcher::Compile(pattern, XStore_Proto::PATTERN_LIKE, ci, error);
        EXPECT_TRUE(matcher.has_value()) << error;
        return std::move(*matcher);
    };
    auto ilike = compile("%AaPl-Order_1%", true);
    EXPECT_EQ(ilike.PrefilterLiteral(), "aapl-order");  // '_' splits runs; longest wins
    EXPECT_TRUE(ilike.PrefilterCaseInsensitive());
    auto like = compile("%AaPl%", false);
    EXPECT_EQ(like.PrefilterLiteral(), "AaPl");
    EXPECT_FALSE(like.PrefilterCaseInsensitive());
    EXPECT_EQ(Literal("%aapl%", /*case_insensitive=*/true), "aapl");
    EXPECT_EQ(Literal("x\\_Yz", /*case_insensitive=*/true), "x_yz");  // escaped '_' is a fixed byte
}

TEST(PatternMatcherTest, FindCaseInsensitiveFoldsAsciiAndRespectsBounds) {
    using patternfold::FindCaseInsensitive;
    auto find = [](std::string_view hay, std::string_view lit) -> long {
        const void *hit = FindCaseInsensitive(hay.data(), hay.size(), lit.data(), lit.size());
        return hit == nullptr ? -1 : static_cast<const char *>(hit) - hay.data();
    };
    EXPECT_EQ(find("xxAaPLyy", "aapl"), 2);
    EXPECT_EQ(find("xxAAPL", "aapl"), 2);          // hit flush with the end
    EXPECT_EQ(find("AAPLxx", "aapl"), 0);          // hit flush with the start
    EXPECT_EQ(find("aapaaPL", "aapl"), 3);         // earlier partial candidate then real hit
    EXPECT_EQ(find("order-12", "-1"), 5);          // non-letter anchor byte
    EXPECT_EQ(find("a1B2", "1b2"), 1);
    EXPECT_EQ(find("xxAB", "abcd"), -1);
    EXPECT_EQ(find("xxAB", "abc"), -1);
    EXPECT_EQ(find("ab", "abc"), -1);              // haystack shorter than literal
    EXPECT_EQ(find("", "a"), -1);
    EXPECT_EQ(find("zz", ""), 0);
    // A hit that would straddle the end of the searched span must not be reported
    // even though the next bytes in memory complete it.
    const std::string_view two_rows = "xxABCDyy";
    EXPECT_EQ(FindCaseInsensitive(two_rows.data(), 4, "abcd", 4), nullptr);
    EXPECT_NE(FindCaseInsensitive(two_rows.data(), 6, "abcd", 4), nullptr);
    // Only A-Z fold: '@' (0x40) vs '`' (0x60), '[' (0x5B) vs '{' (0x7B), and bytes >= 0x80 compare exactly.
    EXPECT_EQ(find("@", "`"), -1);
    EXPECT_EQ(find("[", "{"), -1);
    EXPECT_EQ(find("\xC3\xA9", "\xC3\x89"), -1);
    EXPECT_EQ(find("\xC3\xA9", "\xC3\xA9"), 0);
    EXPECT_EQ(find(std::string_view("a\0B", 3), std::string_view("\0b", 2)), 1);
}

#ifdef XSTORE_HAVE_RE2
TEST(PatternMatcherTest, RegexPrefilterLiteralIsCaseSensitiveRequiredLiteral) {
    std::string error;
    auto matcher = PatternMatcher::Compile("AAPL.*", XStore_Proto::PATTERN_REGEX, false, error);
    ASSERT_TRUE(matcher.has_value()) << error;
    EXPECT_EQ(matcher->PrefilterLiteral(), "AAPL");
    EXPECT_FALSE(matcher->PrefilterCaseInsensitive());
    auto ci = PatternMatcher::Compile("AAPL.*", XStore_Proto::PATTERN_REGEX, true, error);
    ASSERT_TRUE(ci.has_value()) << error;
    EXPECT_TRUE(ci->PrefilterLiteral().empty());
    EXPECT_FALSE(ci->PrefilterCaseInsensitive());
}

TEST(PatternMatcherTest, RegexRequiredLiteralTable) {
    struct Case {
        std::string pattern;
        const char *expected;
    };
    // Rules: runs end at ., ^, $, class escapes (\d \w \s \b ...), '+' (atom kept) and at
    // a quantifier that makes the previous atom optional (* ? {n,m}: that atom is
    // dropped). Longest run wins, ties go to the first. Bail ("") on | ( ) \Q \E
    // \x \p \P and other alphanumeric escapes, stacked quantifiers, a '{' that is
    // not {n} / {n,} / {n,m}, bytes >= 0x80, or case-insensitive.
    const std::vector<Case> cases = {
        {"order-12", "order-12"},
        {"^AAPL-order-[0-9]+7$", "AAPL-order-"},  // class = one non-literal atom
        {"[ab]cd", "cd"},
        {"order-1234[0-9]", "order-1234"},
        {"[^]]x", "x"},                       // leading ']' after '^' is a class member
        {"[a\\]b]cd", "cd"},                  // escaped ']' does not close the class
        {"[[:alpha:]]ab", "ab"},
        {"a[b]*c", "a"},
        {"ab[c]?d", "ab"},
        {"[abc", ""},                         // unterminated: bail
        {"^AAPL-order-\\d+7$", "AAPL-order-"},
        {"ab*c", "a"},                        // b optional; runs "a","c": tie -> first
        {"abc*", "ab"},
        {"ab+c", "ab"},                       // b kept, run ends after it; "c" is shorter
        {"ab+cde", "cde"},
        {"ab+?cde", "cde"},                   // lazy marker is harmless
        {"ab??cde", "cde"},                   // optional + lazy marker: b dropped
        {"a.b", "a"},
        {"a.bc", "bc"},
        {"foo\\.bar", "foo.bar"},
        {"foo\\_bar", "foo_bar"},
        {"x{2}y", "y"},
        {"abc{2,}d", "ab"},
        {"a{2,3}bcd", "bcd"},
        {"a{x}b", ""},                        // not a repetition: bail
        {"a{2", ""},
        {"a+*", ""},                          // stacked quantifiers bail
        {"a*+", ""},
        {"(a|b)c", ""},
        {"ab|cd", ""},
        {"(abc)", ""},
        {"(?i)abc", ""},
        {"abc\\Qx.y\\E", ""},
        {"\\x41bc", ""},
        {"\\pLabc", ""},
        {"\\p{Greek}abc", ""},
        {"\\1abc", ""},
        {"\\d+abc", "abc"},
        {"abc\\bdefg", "defg"},
        {"a]b}", "a]b}"},                     // ']' and '}' outside a class are literals
        {"abc$", "abc"},
        {"x*", ""},
        {"", ""},
        {"caf\xC3\xA9", ""},                // non-ASCII bails
    };
    for (const Case &c : cases) {
        EXPECT_EQ(PatternMatcher::RegexRequiredLiteral(c.pattern, false), c.expected) << "pattern '" << c.pattern << "'";
    }
    EXPECT_EQ(PatternMatcher::RegexRequiredLiteral("abc", true), "");  // case-insensitive bails
}
#endif

#ifndef XSTORE_HAVE_RE2
TEST(PatternMatcherTest, RegexModeReportsUnavailableWithoutRe2) {
    std::string error;
    EXPECT_FALSE(PatternMatcher::Compile("A.*", XStore_Proto::PATTERN_REGEX, false, error).has_value());
    EXPECT_NE(error.find("built without RE2"), std::string::npos) << error;
}
#endif

using xstore_test::CreateTestDb;
using xstore_test::FBytes;
using xstore_test::FInt64;
using xstore_test::FString;
using xstore_test::MakeRow;
using xstore_test::ScratchDir;

class XStorePatternTestFixture : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_pattern_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
    }
    void TearDown() override { scratch.reset(); }

    static XStore_Proto::FindByPattern_REP Run(XStore &store, std::string_view db, unsigned long long start,
                                               unsigned long long end, int32_t column_idx, std::string_view pattern,
                                               XStore_Proto::PatternMode mode = XStore_Proto::PATTERN_LIKE,
                                               bool case_insensitive = false) {
        std::string reassembled;
        bool saw_last = false;
        store.FindByPattern(start, end, db, column_idx, pattern, mode, case_insensitive,
                            XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                            [&](std::string &&chunk, bool is_last) {
                                reassembled += chunk;
                                saw_last = is_last;
                            });
        EXPECT_TRUE(saw_last);
        XStore_Proto::FindByPattern_REP rep;
        EXPECT_TRUE(rep.ParseFromString(reassembled));
        return rep;
    }

    // 300 rows at t=1000..1299: column 0 = i, column 1 = one of 5 symbols (i % 5).
    static void InsertSymbols(XStore &store, std::string_view db) {
        static const char *const SYMBOLS[] = {"AAPL", "MSFT", "GOOG", "AMZN", "META"};
        for (int64_t i = 0; i < 300; ++i) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(1000 + i, MakeRow({FInt64(i), FString(SYMBOLS[i % 5])}), db, &rep, XStore_Proto::SYNC);
        }
    }

    // 500 rows at t=8000..8499: column 0 is a hole (10%), unset (10%), an int (10%)
    // or a random string over `alphabet`; column 1 is a random decoy string.
    // Returns ts -> column-0 value for the string rows.
    static std::map<uint64_t, std::string> LoadRandomRows(XStore &store, std::string_view db, std::mt19937_64 &rng,
                                                          std::string_view alphabet) {
        auto random_text = [&](size_t max_len) {
            std::string s(rng() % (max_len + 1), ' ');
            for (char &c : s) c = alphabet[rng() % alphabet.size()];
            return s;
        };
        std::map<uint64_t, std::string> col0_strings;
        XStore_Proto::UnaryInsert_REP rep;
        for (uint64_t k = 0; k < 500; ++k) {
            const uint64_t ts = 8000 + k;
            const uint64_t kind = rng() % 10;
            if (kind == 0) continue;  // hole
            XStore_Proto::FData col0;
            if (kind == 1) {
                col0 = XStore_Proto::FData();  // unset
            } else if (kind == 2) {
                col0 = FInt64(static_cast<int64_t>(k));
            } else {
                std::string v = random_text(8);
                col0 = FString(v);
                col0_strings[ts] = v;
            }
            store.UnaryInsert(ts, MakeRow({col0, FString(random_text(8))}), db, &rep, XStore_Proto::SYNC);
        }
        return col0_strings;
    }

    // FindByPattern (prefiltered) vs the matcher applied to every stored value.
    // Returns false if the pattern is invalid (then the query must return no rows).
    static bool ExpectMatchesReference(XStore &store, std::string_view db,
                                       const std::map<uint64_t, std::string> &col0_strings, std::string_view pattern,
                                       XStore_Proto::PatternMode mode, bool ci) {
        std::string error;
        auto matcher = PatternMatcher::Compile(pattern, mode, ci, error);
        auto result = Run(store, db, 8000, 8499, 0, pattern, mode, ci);
        if (!matcher.has_value()) {
            EXPECT_EQ(result.queryresponse_size(), 0) << pattern;
            return false;
        }
        std::set<uint64_t> expected;
        for (const auto &[ts, v] : col0_strings) {
            if (matcher->Matches(v)) expected.insert(ts);
        }
        std::set<uint64_t> got;
        for (const auto &entry : result.queryresponse()) got.insert(entry.timestamp());
        EXPECT_EQ(got, expected) << "pattern '" << pattern << "' ci=" << ci << " literal='" << matcher->PrefilterLiteral() << "'";
        return true;
    }
};

TEST_F(XStorePatternTestFixture, LikeReturnsOnlyMatchingStringRows) {
    XStore store;
    CreateTestDb(store, "PATTERN1", XStore_Proto::SECOND, 64);
    InsertSymbols(store, "PATTERN1");

    auto rep = Run(store, "PATTERN1", 1000, 1299, /*column_idx=*/1, "A%");  // AAPL (0), AMZN (3)
    EXPECT_FALSE(rep.has_querystatus()) << rep.querystatus();
    ASSERT_EQ(rep.queryresponse_size(), 120);
    for (const auto &entry : rep.queryresponse()) {
        const auto slot = (entry.timestamp() - 1000) % 5;
        EXPECT_TRUE(slot == 0 || slot == 3) << entry.timestamp();
        EXPECT_EQ(entry.data().fielddata(1).stringdata().front(), 'A');
    }
}

TEST_F(XStorePatternTestFixture, CaseInsensitiveFlagControlsAsciiFolding) {
    XStore store;
    CreateTestDb(store, "PATTERN2", XStore_Proto::SECOND, 64);
    InsertSymbols(store, "PATTERN2");

    EXPECT_EQ(Run(store, "PATTERN2", 1000, 1299, 1, "%oo%", XStore_Proto::PATTERN_LIKE, true).queryresponse_size(), 60);
    auto sensitive = Run(store, "PATTERN2", 1000, 1299, 1, "%oo%", XStore_Proto::PATTERN_LIKE, false);
    EXPECT_EQ(sensitive.queryresponse_size(), 0);
    EXPECT_FALSE(sensitive.has_querystatus());
}

TEST_F(XStorePatternTestFixture, BytesDataColumnsAreSearchable) {
    XStore store;
    CreateTestDb(store, "PATTERN3", XStore_Proto::SECOND, 64);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(2000, MakeRow({FBytes(std::string("\x01" "ab" "\x02", 4))}), "PATTERN3", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(2001, MakeRow({FBytes("zz")}), "PATTERN3", &rep, XStore_Proto::SYNC);

    auto result = Run(store, "PATTERN3", 2000, 2001, 0, "%ab%");
    ASSERT_EQ(result.queryresponse_size(), 1);
    EXPECT_EQ(result.queryresponse(0).timestamp(), 2000u);
}

TEST_F(XStorePatternTestFixture, NonStringColumnNeverMatchesEvenPercent) {
    XStore store;
    CreateTestDb(store, "PATTERN4", XStore_Proto::SECOND, 64);
    InsertSymbols(store, "PATTERN4");
    // '%' matches every string, so zero rows proves the type filter, not the pattern.
    EXPECT_EQ(Run(store, "PATTERN4", 1000, 1299, /*column_idx=*/0, "%").queryresponse_size(), 0);
}

TEST_F(XStorePatternTestFixture, UnwrittenRowsInRangeAreSkipped) {
    XStore store;
    CreateTestDb(store, "PATTERN5", XStore_Proto::SECOND, 64);
    XStore_Proto::UnaryInsert_REP rep;
    for (unsigned long long ts : {3000ULL, 3500ULL, 3999ULL}) {
        store.UnaryInsert(ts, MakeRow({FString("x")}), "PATTERN5", &rep, XStore_Proto::SYNC);
    }
    auto result = Run(store, "PATTERN5", 3000, 3999, 0, "%");
    ASSERT_EQ(result.queryresponse_size(), 3);
    std::set<uint64_t> seen;
    for (const auto &entry : result.queryresponse()) seen.insert(entry.timestamp());
    EXPECT_EQ(seen, (std::set<uint64_t>{3000, 3500, 3999}));
}

TEST_F(XStorePatternTestFixture, SpansYearBoundary) {
    XStore store;
    CreateTestDb(store, "PATTERN6", XStore_Proto::SECOND, 64);
    constexpr unsigned long long LAST_SEC_2019 = 1577836799ULL;  // 2019-12-31 23:59:59 UTC
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(LAST_SEC_2019, MakeRow({FString("edge-a")}), "PATTERN6", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(LAST_SEC_2019 + 1, MakeRow({FString("edge-b")}), "PATTERN6", &rep, XStore_Proto::SYNC);

    auto result = Run(store, "PATTERN6", LAST_SEC_2019, LAST_SEC_2019 + 1, 0, "edge-_");
    EXPECT_EQ(result.queryresponse_size(), 2);
}

TEST_F(XStorePatternTestFixture, InvalidPatternReturnsErrorAndNoRows) {
    XStore store;
    CreateTestDb(store, "PATTERN7", XStore_Proto::SECOND, 64);
    InsertSymbols(store, "PATTERN7");
    auto rep = Run(store, "PATTERN7", 1000, 1299, 1, "abc\\");
    EXPECT_EQ(rep.queryresponse_size(), 0);
    EXPECT_NE(rep.querystatus().find("[ERROR] [FINDBYPATTERN] [DB: PATTERN7] - Invalid pattern:"), std::string::npos)
        << rep.querystatus();
}

TEST_F(XStorePatternTestFixture, UnsetColumnNeverInheritsPreviousRowValue) {
    XStore store;
    CreateTestDb(store, "PATTERN9", XStore_Proto::SECOND, 64);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(5000, MakeRow({FString("AAPL"), FString("a")}), "PATTERN9", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(5001, MakeRow({XStore_Proto::FData(), FString("b")}), "PATTERN9", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(5002, MakeRow({FString("MSFT"), FString("c")}), "PATTERN9", &rep, XStore_Proto::SYNC);

    auto all = Run(store, "PATTERN9", 5000, 5002, 0, "%");
    EXPECT_FALSE(all.has_querystatus()) << all.querystatus();
    std::set<uint64_t> seen_all;
    for (const auto &entry : all.queryresponse()) seen_all.insert(entry.timestamp());
    EXPECT_EQ(seen_all, (std::set<uint64_t>{5000, 5002}));

    auto aapl = Run(store, "PATTERN9", 5000, 5002, 0, "AAPL");
    ASSERT_EQ(aapl.queryresponse_size(), 1);
    EXPECT_EQ(aapl.queryresponse(0).timestamp(), 5000u);
}

TEST_F(XStorePatternTestFixture, ValidationWarnings) {
    XStore store;
    EXPECT_NE(Run(store, "NOSUCHDB", 1, 2, 0, "%").querystatus().find(".CFG does not exist"), std::string::npos);

    CreateTestDb(store, "PATTERN8", XStore_Proto::SECOND, 64);
    EXPECT_NE(Run(store, "PATTERN8", 2, 1, 0, "%").querystatus().find("Target end time must be after start time"),
              std::string::npos);
    EXPECT_NE(Run(store, "PATTERN8", 1, 2, -1, "%").querystatus().find("columnIdx out of range"), std::string::npos);
    EXPECT_NE(Run(store, "PATTERN8", 1, 2, 70000, "%").querystatus().find("columnIdx out of range"), std::string::npos);
}

TEST_F(XStorePatternTestFixture, PrefilterHitInOtherColumnIsRejected) {
    XStore store;
    CreateTestDb(store, "PATTERNPF1", XStore_Proto::SECOND, 64);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(6000, MakeRow({FString("x"), FString("AAPL")}), "PATTERNPF1", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(6001, MakeRow({FString("AAPL"), FString("y")}), "PATTERNPF1", &rep, XStore_Proto::SYNC);
    auto col0 = Run(store, "PATTERNPF1", 6000, 6001, 0, "%AAPL%");
    ASSERT_EQ(col0.queryresponse_size(), 1);
    EXPECT_EQ(col0.queryresponse(0).timestamp(), 6001u);
    auto col1 = Run(store, "PATTERNPF1", 6000, 6001, 1, "%AAPL%");
    ASSERT_EQ(col1.queryresponse_size(), 1);
    EXPECT_EQ(col1.queryresponse(0).timestamp(), 6000u);
}

TEST_F(XStorePatternTestFixture, PrefilterNulLiteralOverHolesStaysCorrect) {
    // Holes are all-zero bytes, so a NUL literal "hits" inside every hole. The
    // decode must reject those hits.
    XStore store;
    CreateTestDb(store, "PATTERNPF2", XStore_Proto::SECOND, 64);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(7000, MakeRow({FString(std::string("a\0b", 3))}), "PATTERNPF2", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(7050, MakeRow({FString("ab")}), "PATTERNPF2", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(7099, MakeRow({FString(std::string("\0", 1))}), "PATTERNPF2", &rep, XStore_Proto::SYNC);
    auto result = Run(store, "PATTERNPF2", 7000, 7099, 0, std::string_view("%\0%", 3));
    std::set<uint64_t> seen;
    for (const auto &entry : result.queryresponse()) seen.insert(entry.timestamp());
    EXPECT_EQ(seen, (std::set<uint64_t>{7000, 7099}));
}

TEST_F(XStorePatternTestFixture, RandomizedResultsEqualPerRowReference) {
    // Differential guard for the prefilter: FindByPattern's result set must equal
    // the matcher applied to every stored column-0 value, across holes, unset and
    // non-string columns, a second string column holding decoy bytes, and ILIKE.
    XStore store;
    CreateTestDb(store, "PATTERNPF3", XStore_Proto::SECOND, 64);
    std::mt19937_64 rng(20260926);
    const char alphabet[] = {'a', 'b', 'A', 'B', '_', '%', '\\'};
    auto random_text = [&](size_t max_len) {
        std::string s(rng() % (max_len + 1), ' ');
        for (char &c : s) c = alphabet[rng() % sizeof(alphabet)];
        return s;
    };
    std::map<uint64_t, std::string> col0_strings;  // ts -> column-0 value, string rows only
    XStore_Proto::UnaryInsert_REP rep;
    for (uint64_t k = 0; k < 500; ++k) {
        const uint64_t ts = 8000 + k;
        const uint64_t kind = rng() % 10;
        if (kind == 0) continue;  // hole
        XStore_Proto::FData col0;
        if (kind == 1) {
            col0 = XStore_Proto::FData();  // unset
        } else if (kind == 2) {
            col0 = FInt64(static_cast<int64_t>(k));
        } else {
            std::string v = random_text(6);
            col0 = FString(v);
            col0_strings[ts] = v;
        }
        store.UnaryInsert(ts, MakeRow({col0, FString(random_text(6))}), "PATTERNPF3", &rep, XStore_Proto::SYNC);
    }
    const char pattern_alphabet[] = {'a', 'b', 'A', '%', '_', '\\'};
    for (int q = 0; q < 300; ++q) {
        std::string pattern(rng() % 6, ' ');
        for (char &c : pattern) c = pattern_alphabet[rng() % sizeof(pattern_alphabet)];
        const bool ci = (rng() % 4) == 0;
        std::string error;
        auto matcher = PatternMatcher::Compile(pattern, XStore_Proto::PATTERN_LIKE, ci, error);
        auto result = Run(store, "PATTERNPF3", 8000, 8499, 0, pattern, XStore_Proto::PATTERN_LIKE, ci);
        if (!matcher.has_value()) {
            EXPECT_EQ(result.queryresponse_size(), 0) << pattern;
            continue;
        }
        std::set<uint64_t> expected;
        for (const auto &[ts, v] : col0_strings) {
            if (matcher->Matches(v)) expected.insert(ts);
        }
        std::set<uint64_t> got;
        for (const auto &entry : result.queryresponse()) got.insert(entry.timestamp());
        EXPECT_EQ(got, expected) << "pattern '" << pattern << "' ci=" << ci;
    }
}

TEST_F(XStorePatternTestFixture, AdaptiveFallbackEqualsPerRowReference) {
    // 20000 rows in one year segment, well past FIND_BY_PATTERN_PREFILTER_ADAPT_MIN_ROWS. Column 0 holds
    // "order-N" (contains 'e', 'E' under ILIKE and "r-<digit>") where `dense(i)` is true and "zzzz-N" otherwise;
    // every 40th row carries the literal only in the decoy column 1, every 97th row is a hole, every 101st an int.
    // The prefilter must give the exact per-row-reference output, in order, whether the hit rate is always high
    // (switches at row 4096), starts low then rises (switches late), starts high then falls (switches early,
    // then the plain loop handles the sparse tail), or stays high only briefly (never switches).
    constexpr uint64_t BASE_TS = 100000;
    constexpr uint64_t ROWS = 20000;
    struct Scenario {
        const char *db;
        bool (*dense)(uint64_t);
    };
    static const Scenario SCENARIOS[] = {
        {"PATTERNAD1", [](uint64_t) { return true; }},
        {"PATTERNAD2", [](uint64_t i) { return i >= 8000 || i % 50 == 0; }},
        {"PATTERNAD3", [](uint64_t i) { return i < 6000 || i % 50 == 0; }},
        {"PATTERNAD4", [](uint64_t i) { return i < 3000 || i % 50 == 0; }},
    };
    struct Query {
        const char *pattern;
        XStore_Proto::PatternMode mode;
        bool ci;
    };
    static const Query QUERIES[] = {
        {"%e%", XStore_Proto::PATTERN_LIKE, false},
        {"%E%", XStore_Proto::PATTERN_LIKE, true},
#ifdef XSTORE_HAVE_RE2
        {"r-[0-9]", XStore_Proto::PATTERN_REGEX, false},
#endif
    };
    for (const Scenario &sc : SCENARIOS) {
        XStore store;
        CreateTestDb(store, sc.db, XStore_Proto::SECOND, 64);
        std::map<uint64_t, std::string> col0_strings;
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(sc.db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (uint64_t i = 0; i < ROWS; ++i) {
            if (i % 97 == 0) continue;  // hole
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(BASE_TS + i);
            XStore_Proto::FData col0;
            if (i % 101 == 0) {
                col0 = FInt64(static_cast<int64_t>(i));
            } else {
                std::string v = (sc.dense(i) ? "order-" : "zzzz-") + std::to_string(i);
                col0 = FString(v);
                col0_strings[BASE_TS + i] = v;
            }
            *entry->mutable_data() = MakeRow({col0, FString(i % 40 == 0 ? "Er-1e" : "x")});
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("Inserted"), std::string::npos) << rep.insertstatus();

        for (const Query &q : QUERIES) {
            std::string error;
            auto matcher = PatternMatcher::Compile(q.pattern, q.mode, q.ci, error);
            ASSERT_TRUE(matcher.has_value()) << q.pattern << ": " << error;
            ASSERT_FALSE(matcher->PrefilterLiteral().empty()) << q.pattern;
            std::vector<uint64_t> expected;
            for (const auto &[ts, v] : col0_strings) {
                if (matcher->Matches(v)) expected.push_back(ts);
            }
            auto result = Run(store, sc.db, BASE_TS, BASE_TS + ROWS - 1, 0, q.pattern, q.mode, q.ci);
            std::vector<uint64_t> got;
            for (const auto &entry : result.queryresponse()) got.push_back(entry.timestamp());
            EXPECT_EQ(got, expected) << sc.db << " pattern '" << q.pattern << "' ci=" << q.ci;
            EXPECT_GT(expected.size(), 500u) << sc.db << " pattern '" << q.pattern << "'";
        }
    }
}

TEST_F(XStorePatternTestFixture, IlikePrefilterRandomizedEqualsPerRowReference) {
    // ILIKE only: letters of both cases, digits/punctuation anchors, wildcards, escapes.
    XStore store;
    CreateTestDb(store, "PATTERNIL1", XStore_Proto::SECOND, 64);
    std::mt19937_64 rng(20261019);
    const auto col0_strings = LoadRandomRows(store, "PATTERNIL1", rng, "abABxX1-_");
    const std::string pattern_alphabet = "abABxX1-_%\\";
    size_t with_literal = 0;
    for (int q = 0; q < 400; ++q) {
        std::string pattern(1 + rng() % 6, ' ');
        for (char &c : pattern) c = pattern_alphabet[rng() % pattern_alphabet.size()];
        std::string error;
        auto matcher = PatternMatcher::Compile(pattern, XStore_Proto::PATTERN_LIKE, true, error);
        if (matcher.has_value() && !matcher->PrefilterLiteral().empty()) ++with_literal;
        ExpectMatchesReference(store, "PATTERNIL1", col0_strings, pattern, XStore_Proto::PATTERN_LIKE, true);
    }
    EXPECT_GE(with_literal, 200u) << "too few patterns exercised the prefilter";
}

TEST_F(XStorePatternTestFixture, IlikePrefilterIgnoresCaseButNotBytes) {
    XStore store;
    CreateTestDb(store, "PATTERNIL2", XStore_Proto::SECOND, 64);
    XStore_Proto::UnaryInsert_REP rep;
    auto put = [&](uint64_t ts, const std::string &v, const std::string &decoy = "zz") {
        store.UnaryInsert(ts, MakeRow({FString(v), FString(decoy)}), "PATTERNIL2", &rep, XStore_Proto::SYNC);
    };
    put(9000, "AAPL-order-1");
    put(9001, "aapl-order-2");
    put(9002, "AaPl-ORDER-3");
    put(9003, "msft-order-4");
    put(9004, "xx", "AAPL");                       // literal only in the decoy column
    put(9005, "AAP");                              // near miss
    put(9006, "a@pl");                             // '@' must not fold onto '`' / 'A'
    auto ts_of = [](const XStore_Proto::FindByPattern_REP &r) {
        std::set<uint64_t> out;
        for (const auto &e : r.queryresponse()) out.insert(e.timestamp());
        return out;
    };
    const auto kLike = XStore_Proto::PATTERN_LIKE;
    EXPECT_EQ(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 0, "%aapl%", kLike, true)), (std::set<uint64_t>{9000, 9001, 9002}));
    EXPECT_EQ(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 0, "%AAPL%", kLike, true)), (std::set<uint64_t>{9000, 9001, 9002}));
    EXPECT_EQ(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 0, "%aapl%", kLike, false)), (std::set<uint64_t>{9001}));
    EXPECT_EQ(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 1, "%aapl%", kLike, true)), (std::set<uint64_t>{9004}));
    // Literal of digits/punctuation only: needs a single memchr, no case work.
    EXPECT_EQ(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 0, "%-1", kLike, true)), (std::set<uint64_t>{9000}));
    EXPECT_EQ(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 0, "%-3", kLike, true)), (std::set<uint64_t>{9002}));
    EXPECT_EQ(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 0, "%r-4%", kLike, true)), (std::set<uint64_t>{9003}));
    EXPECT_TRUE(ts_of(Run(store, "PATTERNIL2", 9000, 9099, 0, "%`pl%", kLike, true)).empty());
}

TEST_F(XStorePatternTestFixture, IlikePrefilterLiteralStraddlingRowsIsNotAMatch) {
    // Row 9100's 60-byte string serializes to exactly 64 bytes, so "AB" is the
    // last row byte; row 9101 holds "CD". Neither row contains "abcd" and the
    // prefilter's span search must not invent it across the boundary (row 9101's
    // own header bytes sit between them in the mapped span).
    XStore store;
    CreateTestDb(store, "PATTERNIL3", XStore_Proto::SECOND, 64);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(9100, MakeRow({FString(std::string(58, 'x') + "AB")}), "PATTERNIL3", &rep, XStore_Proto::SYNC);
    store.UnaryInsert(9101, MakeRow({FString("CDyy")}), "PATTERNIL3", &rep, XStore_Proto::SYNC);
    EXPECT_EQ(Run(store, "PATTERNIL3", 9100, 9101, 0, "%abcd%", XStore_Proto::PATTERN_LIKE, true).queryresponse_size(), 0);
    EXPECT_EQ(Run(store, "PATTERNIL3", 9100, 9101, 0, "%cd%", XStore_Proto::PATTERN_LIKE, true).queryresponse_size(), 1);
    EXPECT_EQ(Run(store, "PATTERNIL3", 9100, 9101, 0, "%xab", XStore_Proto::PATTERN_LIKE, true).queryresponse_size(), 1);
}

#ifdef XSTORE_HAVE_RE2
#include <chrono>

namespace {
bool Regex(std::string_view pattern, std::string_view text, bool case_insensitive = false) {
    std::string error;
    auto matcher = PatternMatcher::Compile(pattern, XStore_Proto::PATTERN_REGEX, case_insensitive, error);
    EXPECT_TRUE(matcher.has_value()) << "regex '" << pattern << "': " << error;
    return matcher.has_value() && matcher->Matches(text);
}
} // namespace

TEST(PatternMatcherTest, RegexIsUnanchoredPartialMatch) {
    EXPECT_TRUE(Regex("P.", "AAPL"));
    EXPECT_TRUE(Regex("^A+P", "AAPL"));
    EXPECT_FALSE(Regex("^P", "AAPL"));
    EXPECT_TRUE(Regex("^(AAPL|MSFT)$", "MSFT"));
    EXPECT_FALSE(Regex("^(AAPL|MSFT)$", "MSFTX"));
}

TEST(PatternMatcherTest, RegexCaseInsensitiveFlag) {
    EXPECT_TRUE(Regex("aapl", "xAAPLx", /*case_insensitive=*/true));
    EXPECT_FALSE(Regex("aapl", "xAAPLx", /*case_insensitive=*/false));
}

TEST(PatternMatcherTest, InvalidRegexIsRejectedWithRe2Message) {
    std::string error;
    EXPECT_FALSE(PatternMatcher::Compile("(unclosed", XStore_Proto::PATTERN_REGEX, false, error).has_value());
    EXPECT_NE(error.find("missing )"), std::string::npos) << error;
}

TEST(PatternMatcherTest, PathologicalRegexStaysLinearTime) {
    // Catastrophic for a backtracking engine; RE2 is linear in the input.
    const std::string text = std::string(200'000, 'a') + "b";
    auto start = std::chrono::steady_clock::now();
    EXPECT_FALSE(Regex("^(a+)+$", text));
    EXPECT_LT(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count(), 2.0);
}

TEST_F(XStorePatternTestFixture, RegexModeFiltersRowsEndToEnd) {
    XStore store;
    CreateTestDb(store, "PATTERNRX", XStore_Proto::SECOND, 64);
    InsertSymbols(store, "PATTERNRX");
    auto rep = Run(store, "PATTERNRX", 1000, 1299, 1, "^(AAPL|MSFT)$", XStore_Proto::PATTERN_REGEX);
    EXPECT_FALSE(rep.has_querystatus()) << rep.querystatus();
    EXPECT_EQ(rep.queryresponse_size(), 120);
}

TEST_F(XStorePatternTestFixture, RegexPrefilterRandomizedEqualsPerRowReference) {
    // Random regexes assembled from atoms and quantifiers covering every
    // extractor rule (classes, groups, alternation, escapes, optional atoms);
    // stops after 300 compile-valid ones. Deterministic seed.
    XStore store;
    CreateTestDb(store, "PATTERNRX2", XStore_Proto::SECOND, 64);
    std::mt19937_64 rng(20261020);
    const auto col0_strings = LoadRandomRows(store, "PATTERNRX2", rng, "abAx-.2d");
    static const char *const ATOMS[] = {"a", "b", "A", "x", "-", "2", "ab", "xa-", "\\.", "\\d", ".", "^", "$",
                                        "[ab]", "(a|b)", "a|b", "\\w", "\\bx", "\\-"};
    static const char *const QUANTS[] = {"", "", "", "", "*", "+", "?", "{2}", "{1,2}", "*?", "+?", "??"};
    size_t valid = 0;
    size_t with_literal = 0;
    for (int attempt = 0; attempt < 5000 && valid < 300; ++attempt) {
        std::string pattern;
        const size_t atoms = 1 + rng() % 5;
        for (size_t i = 0; i < atoms; ++i) {
            pattern += ATOMS[rng() % (sizeof(ATOMS) / sizeof(ATOMS[0]))];
            pattern += QUANTS[rng() % (sizeof(QUANTS) / sizeof(QUANTS[0]))];
        }
        if (!ExpectMatchesReference(store, "PATTERNRX2", col0_strings, pattern, XStore_Proto::PATTERN_REGEX, false)) continue;
        ++valid;
        if (!PatternMatcher::RegexRequiredLiteral(pattern, false).empty()) ++with_literal;
    }
    EXPECT_EQ(valid, 300u);
    EXPECT_GE(with_literal, 100u) << "too few patterns exercised the prefilter";
}

TEST_F(XStorePatternTestFixture, RegexPrefilterEndToEnd) {
    XStore store;
    CreateTestDb(store, "PATTERNRX3", XStore_Proto::SECOND, 64);
    InsertSymbols(store, "PATTERNRX3");
    // Required literal "AAPL" (AAPL rows are i % 5 == 0 -> 60 of 300); case-insensitive has none.
    EXPECT_EQ(Run(store, "PATTERNRX3", 1000, 1299, 1, "^AAPL$", XStore_Proto::PATTERN_REGEX).queryresponse_size(), 60);
    EXPECT_EQ(Run(store, "PATTERNRX3", 1000, 1299, 1, "^aapl$", XStore_Proto::PATTERN_REGEX, true).queryresponse_size(), 60);
    EXPECT_EQ(Run(store, "PATTERNRX3", 1000, 1299, 1, "AA?PL", XStore_Proto::PATTERN_REGEX).queryresponse_size(), 60);
    EXPECT_EQ(Run(store, "PATTERNRX3", 1000, 1299, 1, "ZZZZ", XStore_Proto::PATTERN_REGEX).queryresponse_size(), 0);
}
#endif
