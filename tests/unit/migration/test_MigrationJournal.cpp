#include <gtest/gtest.h>
#include "iotdbfs/migration/MigrationJournal.h"
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sys/stat.h>
#include <unistd.h>

namespace {
using namespace iotdbfs;
class MigrationJournalTest : public ::testing::Test {
protected:
  std::filesystem::path dir;
  MigrationReportBinding binding{"127.0.0.1", 6667, "journal_test", "hdf5", "IOTDBFS-HDF5-LAYOUT/3"};
  MigrationInventoryEntry entry;
  void SetUp() override {
    char name[] = "/tmp/iotdbfs-journal-XXXXXX";
    auto* made = ::mkdtemp(name); ASSERT_NE(made, nullptr); dir = made;
    entry.source = {"/science/series.bin", 17}; entry.bytes = 123;
    entry.sha256 = std::string(64, 'a'); entry.layout_sha256 = std::string(64, 'b');
    entry.kind = MigrationKind::Candidate;
  }
  void TearDown() override { std::error_code ec; std::filesystem::remove_all(dir, ec); }
  std::string read(const char* name) {
    std::ifstream in(dir / name, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
  }
  void write(const char* name, const std::string& bytes) {
    std::ofstream out(dir / name, std::ios::binary); out << bytes; ASSERT_TRUE(out.good());
  }
  void inventory() {
    std::string error; ASSERT_TRUE(save_migration_inventory(dir, binding, {entry}, &error)) << error;
  }
  MigrationResult event(std::string state = "started") {
    MigrationResult result; result.source = entry.source; result.destination = {entry.source.path, 23};
    result.bytes = entry.bytes; result.source_sha256 = entry.sha256; result.layout_sha256 = entry.layout_sha256;
    result.adapter_id = binding.adapter_id; result.layout_version = binding.layout_protocol;
    result.state = state;
    if (state == "verified") { result.destination_sha256 = result.source_sha256; result.layout_bytes = 321; }
    return result;
  }
};
TEST_F(MigrationJournalTest, InventoryAndEventsRoundTripWithFrozenIdentity) {
  inventory(); ASSERT_TRUE(append_migration_event(dir, event()));
  ASSERT_TRUE(append_migration_event(dir, event("verified")));
  std::string error; auto loaded = load_migration_report(dir, binding, &error);
  ASSERT_TRUE(loaded) << error; ASSERT_EQ(loaded->inventory.size(), 1u);
  EXPECT_EQ(loaded->inventory[0].layout_sha256, entry.layout_sha256);
  ASSERT_EQ(loaded->journal.size(), 1u);
  const auto& recovered = loaded->journal.begin()->second;
  EXPECT_TRUE(recovered.ok()); EXPECT_EQ(recovered.destination.version, 23);
  EXPECT_EQ(recovered.adapter_id, "hdf5"); EXPECT_EQ(recovered.layout_version, binding.layout_protocol);
  EXPECT_TRUE(save_migration_results(dir, loaded->journal));
  EXPECT_NE(read("results.jsonl").find("\"state\":\"verified\""), std::string::npos);
}
TEST_F(MigrationJournalTest, EveryBindingComponentIsRequiredBeforeJournalRepair) {
  inventory(); ASSERT_TRUE(append_migration_event(dir, event()));
  const auto torn = read("journal.log") + "incomplete"; write("journal.log", torn);
  std::vector<MigrationReportBinding> wrong(5, binding);
  wrong[0].host = "another-host"; wrong[1].port++; wrong[2].database = "another_db";
  wrong[3].adapter_id = "arrow"; wrong[4].layout_protocol = "IOTDBFS-HDF5-LAYOUT/2";
  for (const auto& other : wrong) {
    std::string error; EXPECT_FALSE(load_migration_report(dir, other, &error));
    EXPECT_EQ(read("journal.log"), torn);
  }
}
TEST_F(MigrationJournalTest, FrozenInventoryTamperingRejectsBeforeTailRepair) {
  inventory(); write("journal.log", "torn");
  write("inventory.tsv", read("inventory.tsv") + "extra\n");
  std::string error; EXPECT_FALSE(load_migration_report(dir, binding, &error));
  EXPECT_EQ(read("journal.log"), "torn");
}
TEST_F(MigrationJournalTest, ValidTornTailRetainsOnlyDurableEvents) {
  inventory(); ASSERT_TRUE(append_migration_event(dir, event()));
  auto complete = read("journal.log"); write("journal.log", complete + "IOTDBFS-MIGRATION-JOURNAL/1 partial");
  std::string error; auto recovered = load_migration_report(dir, binding, &error);
  ASSERT_TRUE(recovered) << error; EXPECT_EQ(recovered->journal.size(), 1u);
  EXPECT_EQ(read("journal.log"), complete);
}
TEST_F(MigrationJournalTest, LegacyReportAndExistingReportCannotBeReused) {
  write("inventory.tsv", "A12I1\n"); std::string error;
  EXPECT_FALSE(load_migration_report(dir, binding, &error));
  EXPECT_FALSE(save_migration_inventory(dir, binding, {entry}, &error));
  EXPECT_EQ(read("inventory.tsv"), "A12I1\n");
}
TEST_F(MigrationJournalTest, InvalidAndDuplicateInventoryKeysRejectBeforeWriting) {
  std::string error;
  EXPECT_FALSE(save_migration_inventory(dir, binding, {entry, entry}, &error));
  entry.source.path = "/science/../series.bin";
  EXPECT_FALSE(save_migration_inventory(dir, binding, {entry}, &error));
  EXPECT_FALSE(std::filesystem::exists(dir / "report.manifest"));
}
TEST_F(MigrationJournalTest, InvalidHashesStatesAndDestinationKeysCannotBeAppended) {
  inventory(); auto invalid = event(); invalid.source_sha256[0] = 'X';
  EXPECT_FALSE(append_migration_event(dir, invalid));
  invalid = event(); invalid.state = "pretend_success";
  EXPECT_FALSE(append_migration_event(dir, invalid));
  invalid = event(); invalid.destination.path = "/other.bin";
  EXPECT_FALSE(append_migration_event(dir, invalid));
  invalid = event(); invalid.destination.version = invalid.source.version;
  EXPECT_FALSE(append_migration_event(dir, invalid));
  invalid = event("verified"); invalid.destination_sha256 = std::string(64, 'c');
  EXPECT_FALSE(append_migration_event(dir, invalid));
  EXPECT_FALSE(std::filesystem::exists(dir / "journal.log"));
}
TEST_F(MigrationJournalTest, CompleteCorruptionIsFatalAndNeverTruncated) {
  inventory(); ASSERT_TRUE(append_migration_event(dir, event()));
  auto corrupt = read("journal.log") + "invalid complete event\ntrailing";
  write("journal.log", corrupt); std::string error;
  EXPECT_FALSE(load_migration_report(dir, binding, &error));
  EXPECT_EQ(read("journal.log"), corrupt);
}
TEST_F(MigrationJournalTest, JournalMustMatchInventoryAndPolicy) {
  inventory(); auto mismatch = event(); mismatch.layout_sha256 = std::string(64, 'c');
  ASSERT_TRUE(append_migration_event(dir, mismatch));
  std::string error; EXPECT_FALSE(load_migration_report(dir, binding, &error));
  EXPECT_NE(error.find("frozen inventory/policy"), std::string::npos);
}
TEST_F(MigrationJournalTest, JournalSymlinkIsNotFollowedOrModified) {
  inventory(); write("outside.txt", "untouched");
  ASSERT_EQ(::symlink((dir / "outside.txt").c_str(), (dir / "journal.log").c_str()), 0);
  EXPECT_FALSE(append_migration_event(dir, event()));
  std::string error; EXPECT_FALSE(load_migration_report(dir, binding, &error));
  EXPECT_EQ(read("outside.txt"), "untouched");
}
TEST_F(MigrationJournalTest, NonRegularJournalRejectsWithoutBlocking) {
  inventory(); ASSERT_EQ(::mkfifo((dir / "journal.log").c_str(), 0600), 0);
  EXPECT_FALSE(append_migration_event(dir, event()));
  std::string error; EXPECT_FALSE(load_migration_report(dir, binding, &error));
}
TEST_F(MigrationJournalTest, FailureBeforeTargetAllocationRoundTrips) {
  inventory(); auto failed = event("conflict"); failed.destination = {}; failed.error = "latest changed";
  ASSERT_TRUE(append_migration_event(dir, failed)); std::string error;
  auto loaded = load_migration_report(dir, binding, &error); ASSERT_TRUE(loaded) << error;
  EXPECT_EQ(loaded->journal.begin()->second.destination.version, 0);
  EXPECT_EQ(loaded->journal.begin()->second.state, "conflict");
}
TEST_F(MigrationJournalTest, ReportSizeLimitRejectsOversizedJournalWithoutRepair) {
  inventory(); write("journal.log", "x");
  ASSERT_EQ(::truncate((dir / "journal.log").c_str(), 64 * 1024 * 1024 + 1), 0);
  std::string error; EXPECT_FALSE(load_migration_report(dir, binding, &error));
  EXPECT_EQ(std::filesystem::file_size(dir / "journal.log"), 64u * 1024 * 1024 + 1);
}
TEST_F(MigrationJournalTest, EveryReservedVersionSurvivesLaterPreallocationFailure) {
  inventory(); ASSERT_TRUE(append_migration_event(dir, event()));
  auto later = event("started"); later.destination.version = 42;
  ASSERT_TRUE(append_migration_event(dir, later));
  auto failed = event("conflict"); failed.destination = {};
  ASSERT_TRUE(append_migration_event(dir, failed));
  std::string error; auto loaded = load_migration_report(dir, binding, &error);
  ASSERT_TRUE(loaded) << error; EXPECT_EQ(loaded->max_reserved_version, 42);
  EXPECT_EQ(loaded->journal.begin()->second.destination.version, 0);
}
TEST_F(MigrationJournalTest, ReusingEarlierReservedVersionIsRejected) {
  inventory(); auto later = event(); later.destination.version = 42;
  ASSERT_TRUE(append_migration_event(dir, later));
  ASSERT_TRUE(append_migration_event(dir, event()));
  std::string error; EXPECT_FALSE(load_migration_report(dir, binding, &error));
  EXPECT_NE(error.find("backwards"), std::string::npos);
}
} // namespace
