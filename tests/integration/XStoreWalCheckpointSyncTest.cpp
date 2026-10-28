// Checkpointer data/directory syncs under concurrency: syncs of ONE path stay mutually exclusive (and ordered
// against that path's Poison()), syncs of DIFFERENT paths overlap, and a real error anywhere fails every sync
// that was in flight or arrives later. Timing is made visible with the data/dir sync delay hooks, which sleep
// inside whatever lock covers the syscall.
#include <gtest/gtest.h>
#include "include/XStore/XStore_WalCheckpoint.h"
#include "include/XStore/XStore_WalRing.h"
#include <spdlog/fmt/fmt.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <set>
#include <string>
#include <thread>
#include <unordered_set>
#include <unistd.h>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;
constexpr unsigned DELAY_MS = 400;

class WalCheckpointSyncTest : public ::testing::Test {
protected:
    static walring::Checkpointer &Ckpt() { return walring::Checkpointer::Instance(); }

    void SetUp() override {
        Ckpt().SetPausedForTests(true);   // no background cycles: the tests drive RunCycle() themselves
        Ckpt().ClearPoisonForTests();
        (void)walring::Dirty().Take();
        // Distinct stripes for every path used (a stripe collision only serializes two unrelated paths, but
        // would make the "overlaps" tests depend on the hash of a pid-based name).
        for (int attempt = 0;; ++attempt) {
            root_ = std::filesystem::temp_directory_path() / fmt::format("xstore_ckpt_sync_{}_{}", getpid(), attempt);
            std::set<size_t> stripes;
            for (const std::string &p : {A(), B(), C(), Dir1(), Dir2()}) stripes.insert(Ckpt().SyncStripeForTests(p));
            if (stripes.size() == 5) break;
            ASSERT_LT(attempt, 200) << "no collision-free path set found";
        }
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_ / "d1");
        std::filesystem::create_directories(root_ / "d2");
        for (const std::string &p : {A(), B(), C()}) std::ofstream(p) << std::string(4096, 'x');
    }

    void TearDown() override {
        Ckpt().SetDataSyncDelayMsForTests(0);
        Ckpt().SetDirSyncDelayMsForTests(0);
        Ckpt().FailNextDataSyncsStickyForTests(0);
        Ckpt().ClearPoisonForTests();
        (void)walring::Dirty().Take();
        std::filesystem::remove_all(root_);
    }

    std::string A() const { return (root_ / "d1" / "a.XSTORE").string(); }
    std::string C() const { return (root_ / "d1" / "c.XSTORE").string(); }   // same directory as A
    std::string B() const { return (root_ / "d2" / "b.XSTORE").string(); }
    std::string Dir1() const { return (root_ / "d1").string(); }
    std::string Dir2() const { return (root_ / "d2").string(); }

    static bool Sync(const std::string &path) { return Ckpt().SyncPathsForReplay({path}); }

    // Runs every function on its own thread, released together; returns the wall time until all are done.
    static std::chrono::milliseconds RunTogether(const std::vector<std::function<void()>> &fns) {
        std::atomic<bool> go{false};
        std::vector<std::thread> threads;
        for (const auto &fn : fns) {
            threads.emplace_back([&go, &fn] {
                while (!go.load(std::memory_order_acquire)) std::this_thread::yield();
                fn();
            });
        }
        const auto t0 = Clock::now();
        go.store(true, std::memory_order_release);
        for (auto &t : threads) t.join();
        return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0);
    }

    std::filesystem::path root_;
};

// Different data files on different directories: nothing orders them, so the two syncs overlap (2x the delay when
// one process-wide lock serializes them).
TEST_F(WalCheckpointSyncTest, SyncsOfDifferentFilesOverlap) {
    Ckpt().SetDataSyncDelayMsForTests(DELAY_MS);
    bool ra = false, rb = false;
    const auto elapsed = RunTogether({[&] { ra = Sync(A()); }, [&] { rb = Sync(B()); }});
    EXPECT_TRUE(ra);
    EXPECT_TRUE(rb);
    EXPECT_LT(elapsed.count(), 2 * DELAY_MS - 100) << "two syncs of different files ran one after the other";
}

// The same file: its syncs are mutually exclusive (the fsyncgate rule: a failing sync and its Poison() must not
// interleave with another sync of that file).
TEST_F(WalCheckpointSyncTest, SyncsOfTheSameFileAreExclusive) {
    Ckpt().SetDataSyncDelayMsForTests(DELAY_MS);
    bool r1 = false, r2 = false;
    const auto elapsed = RunTogether({[&] { r1 = Sync(A()); }, [&] { r2 = Sync(A()); }});
    EXPECT_TRUE(r1);
    EXPECT_TRUE(r2);
    EXPECT_GE(elapsed.count(), 2 * DELAY_MS - 50) << "two syncs of one file overlapped";
}

// A cycle syncing file A must not hold up a direct sync of a different file.
TEST_F(WalCheckpointSyncTest, DirectSyncOfAnotherFileDoesNotWaitForACycle) {
    Ckpt().SetDataSyncDelayMsForTests(DELAY_MS);
    walring::Dirty().Note(nullptr, A(), 0, 4096, 0);
    bool cycle_ok = false;
    std::thread cycle([&] { cycle_ok = Ckpt().RunCycle(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));   // the cycle is now inside A's sync
    const auto t0 = Clock::now();
    const bool direct_ok = Sync(B());
    const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0);
    cycle.join();
    EXPECT_TRUE(direct_ok);
    EXPECT_TRUE(cycle_ok);
    EXPECT_LT(waited.count(), DELAY_MS + 150) << "the direct sync of B waited for the cycle's sync of A";
}

// A cycle syncing file A and a direct sync of the same file A are exclusive.
TEST_F(WalCheckpointSyncTest, DirectSyncOfTheSameFileWaitsForACycle) {
    Ckpt().SetDataSyncDelayMsForTests(DELAY_MS);
    walring::Dirty().Note(nullptr, A(), 0, 4096, 0);
    bool cycle_ok = false;
    std::thread cycle([&] { cycle_ok = Ckpt().RunCycle(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto t0 = Clock::now();
    const bool direct_ok = Sync(A());
    const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - t0);
    cycle.join();
    EXPECT_TRUE(direct_ok);
    EXPECT_TRUE(cycle_ok);
    EXPECT_GE(waited.count(), DELAY_MS + (DELAY_MS - 100) - 50) << "the direct sync of A overlapped the cycle's sync of A";
}

// A real error that poisons while another sync is in flight fails that sync: it must not report durable.
TEST_F(WalCheckpointSyncTest, PoisonDuringASyncFailsIt) {
    Ckpt().SetDataSyncDelayMsForTests(DELAY_MS);
    bool result = true;
    std::thread syncing([&] { result = Sync(A()); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Ckpt().ReportRealError("test: real error elsewhere", B(), EIO);
    syncing.join();
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_FALSE(result) << "a sync that finished after the checkpointer was poisoned reported durable";
}

// Same, with the error landing during the LAST step (the directory fsync): only the check after the final sync
// can catch it.
TEST_F(WalCheckpointSyncTest, PoisonDuringTheDirectorySyncFailsIt) {
    Ckpt().SetDirSyncDelayMsForTests(DELAY_MS);
    bool result = true;
    std::thread syncing([&] { result = Sync(A()); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    Ckpt().ReportRealError("test: real error elsewhere", B(), EIO);
    syncing.join();
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_FALSE(result) << "a sync whose last step finished after the checkpointer was poisoned reported durable";
}

// A sync waiting behind a sync of the same file that fails for real must fail without syncing.
TEST_F(WalCheckpointSyncTest, SyncWaitingBehindAFailingSyncOfTheSameFileFails) {
    Ckpt().SetDataSyncDelayMsForTests(DELAY_MS);
    Ckpt().FailNextDataSyncsStickyForTests(1);
    const uint64_t syncs_before = Ckpt().DataSyncsForTests();
    bool first = true, second = true;
    std::thread failing([&] { first = Sync(A()); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::thread waiting([&] { second = Sync(A()); });
    failing.join();
    waiting.join();
    EXPECT_FALSE(first);
    EXPECT_FALSE(second);
    EXPECT_TRUE(Ckpt().Poisoned());
    EXPECT_EQ(Ckpt().DataSyncsForTests(), syncs_before) << "the waiting sync performed an fdatasync after the poison";
}

// Directory fsyncs of ONE directory stay exclusive (a directory error is the same kind of report-once hazard).
TEST_F(WalCheckpointSyncTest, DirectorySyncsOfOneDirectoryAreExclusive) {
    Ckpt().SetDirSyncDelayMsForTests(DELAY_MS);
    bool ra = false, rc = false;
    const auto elapsed = RunTogether({[&] { ra = Sync(A()); }, [&] { rc = Sync(C()); }});   // both in d1
    EXPECT_TRUE(ra);
    EXPECT_TRUE(rc);
    EXPECT_GE(elapsed.count(), 2 * DELAY_MS - 50) << "two fsyncs of one directory overlapped";
}

// Directory fsyncs of different directories overlap.
TEST_F(WalCheckpointSyncTest, DirectorySyncsOfDifferentDirectoriesOverlap) {
    Ckpt().SetDirSyncDelayMsForTests(DELAY_MS);
    bool ra = false, rb = false;
    const auto elapsed = RunTogether({[&] { ra = Sync(A()); }, [&] { rb = Sync(B()); }});   // d1 and d2
    EXPECT_TRUE(ra);
    EXPECT_TRUE(rb);
    EXPECT_LT(elapsed.count(), 2 * DELAY_MS - 100) << "fsyncs of different directories ran one after the other";
}

// One call over several files still syncs every file and every distinct directory exactly once.
TEST_F(WalCheckpointSyncTest, OneCallSyncsEveryFileAndDirectory) {
    const uint64_t data_before = Ckpt().DataSyncsForTests();
    const uint64_t dir_before = Ckpt().DirSyncsForTests();
    EXPECT_TRUE(Ckpt().SyncPathsForReplay({A(), B(), C()}));
    EXPECT_EQ(Ckpt().DataSyncsForTests(), data_before + 3);
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 2);
}

// A retryable failure of one file in a cycle over several files advances nothing and keeps every file for the retry,
// including the ones whose sync succeeded.
TEST_F(WalCheckpointSyncTest, OneFailedSyncInACycleKeepsEveryFileForTheRetry) {
    for (const std::string &p : {A(), B(), C()}) walring::Dirty().Note(nullptr, p, 0, 4096, 0);
    Ckpt().FailNextDataSyncsForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_FALSE(Ckpt().Poisoned());
    EXPECT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(Ckpt().LastCycleSyncedPaths(), (std::unordered_set<std::string>{A(), B(), C()}));
}

// A real (sticky) error in one file of a cycle poisons and fails the cycle.
TEST_F(WalCheckpointSyncTest, AStickyFailureInACyclePoisonsAndFailsIt) {
    for (const std::string &p : {A(), B(), C()}) walring::Dirty().Note(nullptr, p, 0, 4096, 0);
    Ckpt().FailNextDataSyncsStickyForTests(1);
    EXPECT_FALSE(Ckpt().RunCycle());
    EXPECT_TRUE(Ckpt().Poisoned());
}

// A directory fsync only makes directory ENTRIES durable. Once one succeeded while a file existed, syncing that same
// file again must not fsync its directory again (the writer's direct path does this once per record, ~3.5 ms each).
TEST_F(WalCheckpointSyncTest, TheDirectoryOfAFileWhoseEntryIsDurableIsNotSyncedAgain) {
    const uint64_t dir_before = Ckpt().DirSyncsForTests();
    const uint64_t data_before = Ckpt().DataSyncsForTests();
    EXPECT_TRUE(Sync(A()));
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 1);
    EXPECT_TRUE(Sync(A()));
    EXPECT_TRUE(Sync(A()));
    EXPECT_EQ(Ckpt().DataSyncsForTests(), data_before + 3) << "the data file itself is synced every time";
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 1) << "the directory was synced again for an unchanged file";
}

// A file deleted and created again at the same path gets the same inode number back on ext4 but is a new directory
// entry: its directory must be synced again.
TEST_F(WalCheckpointSyncTest, ARecreatedFileHasItsDirectorySyncedAgain) {
    EXPECT_TRUE(Sync(A()));
    const uint64_t dir_before = Ckpt().DirSyncsForTests();
    std::filesystem::remove(A());
    std::ofstream(A()) << std::string(4096, 'y');
    EXPECT_TRUE(Sync(A()));
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 1);
}

// A file renamed over the path (how an index or a restored file is put in place) is a new entry too.
TEST_F(WalCheckpointSyncTest, AFileRenamedOverTheOldOneHasItsDirectorySyncedAgain) {
    EXPECT_TRUE(Sync(A()));
    const std::string incoming = (root_ / "d1" / "incoming.XSTORE").string();
    std::ofstream(incoming) << std::string(4096, 'z');
    std::filesystem::rename(incoming, A());
    const uint64_t dir_before = Ckpt().DirSyncsForTests();
    EXPECT_TRUE(Sync(A()));
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 1);
}

// The entry counts as durable only after the directory fsync SUCCEEDED: a failed one is retried by the next sync.
TEST_F(WalCheckpointSyncTest, AFailedDirectorySyncIsNotRememberedAsDone) {
    const uint64_t dir_before = Ckpt().DirSyncsForTests();
    Ckpt().FailNextDirSyncsForTests(1);
    EXPECT_FALSE(Sync(A()));
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before);
    EXPECT_TRUE(Sync(A()));
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 1) << "the retry skipped the directory sync that had failed";
}

// One durable entry says nothing about its neighbours: another file of the same directory still needs the sync.
TEST_F(WalCheckpointSyncTest, ANewFileInADirectoryWithADurableEntryStillGetsTheDirectorySynced) {
    const uint64_t dir_before = Ckpt().DirSyncsForTests();
    EXPECT_TRUE(Sync(A()));
    EXPECT_TRUE(Sync(C()));   // same directory as A
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 2);
    EXPECT_TRUE(Sync(A()));
    EXPECT_TRUE(Sync(C()));
    EXPECT_EQ(Ckpt().DirSyncsForTests(), dir_before + 2);
}

// The checkpoint cycle takes the same shortcut.
TEST_F(WalCheckpointSyncTest, ACycleDoesNotSyncTheDirectoryOfAFileItAlreadySynced) {
    walring::Dirty().Note(nullptr, A(), 0, 4096, 0);
    EXPECT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(Ckpt().LastCycleSyncedDirsForTests().count(Dir1()), 1u) << "a first sync of A must sync its directory";
    walring::Dirty().Note(nullptr, A(), 0, 4096, 0);
    EXPECT_TRUE(Ckpt().RunCycle());
    EXPECT_EQ(Ckpt().LastCycleSyncedPaths(), (std::unordered_set<std::string>{A()}));
    EXPECT_TRUE(Ckpt().LastCycleSyncedDirsForTests().empty()) << "the second cycle synced the directory again";
}

// Already poisoned: nothing is synced and the call fails.
TEST_F(WalCheckpointSyncTest, PoisonedCheckpointerSyncsNothing) {
    Ckpt().ReportRealError("test", A(), EIO);
    const uint64_t data_before = Ckpt().DataSyncsForTests();
    EXPECT_FALSE(Ckpt().SyncPathsForReplay({A(), B()}));
    EXPECT_EQ(Ckpt().DataSyncsForTests(), data_before);
}

}  // namespace
