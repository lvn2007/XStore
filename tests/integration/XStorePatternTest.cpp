#include <gtest/gtest.h>
#include "XStore/XStore_PatternMatch.h"
#include "XStore/XStore_Tunables.h"
#include "XStore/XStore.h"
#include "XStoreTestSupport.h"
#include "XStore.pb.h"
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <string_view>

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

TEST(PatternMatcherTest, PrefilterLiteralEmptyWhenUnusable) {
    EXPECT_EQ(Literal("%"), "");
    EXPECT_EQ(Literal(""), "");
    EXPECT_EQ(Literal("___"), "");
    EXPECT_EQ(Literal("%_%"), "");
    EXPECT_EQ(Literal("%aapl%", /*case_insensitive=*/true), "");
}

#ifdef XSTORE_HAVE_RE2
TEST(PatternMatcherTest, PrefilterLiteralEmptyForRegex) {
    std::string error;
    auto matcher = PatternMatcher::Compile("AAPL.*", XStore_Proto::PATTERN_REGEX, false, error);
    ASSERT_TRUE(matcher.has_value()) << error;
    EXPECT_TRUE(matcher->PrefilterLiteral().empty());
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
#endif
