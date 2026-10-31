#include "iotdbfs/MetadataConfigStore.h"

#include <gtest/gtest.h>

#include <fstream>
#include <unistd.h>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>
#include <vector>
#include "TestSupport.h"

using namespace iotdbfs;

namespace {
struct TempConfig {
  std::string path;
  explicit TempConfig(const std::string& body) {
    static int sequence = 0;
    path = "/tmp/iotdbfs-config-store-" + std::to_string(getpid()) + "-" +
      std::to_string(++sequence) + ".properties";
    std::ofstream out(path);
    out << body;
  }
  ~TempConfig() { std::remove(path.c_str()); }
};
}

TEST(MetadataConfigStore, PublishesImmutableSnapshotsAndGenerations) {
  TempConfig file("metadata.schema_version=1\nmetadata.field.a.source=a\nmetadata.field.a.type=string\n");
  MetadataConfigStore store;
  auto first = store.reload(file.path);
  ASSERT_TRUE(first.ok()) << first.error();
  ASSERT_EQ(first.value(), 1u);
  auto snapshot = store.snapshot();
  ASSERT_TRUE(snapshot);
  ASSERT_EQ(snapshot->fields().size(), 1u);
  EXPECT_EQ(snapshot->fields().front().id, "a");
}

TEST(MetadataConfigStore, FailedReloadKeepsPreviousSnapshot) {
  TempConfig good("metadata.schema_version=1\nmetadata.field.a.source=a\nmetadata.field.a.type=string\n");
  TempConfig bad("metadata.schema_version=1\nmetadata.field.a.type=broken\n");
  MetadataConfigStore store;
  ASSERT_TRUE(store.reload(good.path).ok());
  auto failed = store.reload(bad.path);
  EXPECT_FALSE(failed.ok());
  EXPECT_EQ(store.generation(), 1u);
  ASSERT_TRUE(store.snapshot());
  EXPECT_EQ(store.snapshot()->fields().front().id, "a");
}

TEST(MetadataConfigStore, ExistingSnapshotRemainsImmutableAfterReload) {
  TempConfig first("metadata.schema_version=1\nmetadata.field.first.source=first\nmetadata.field.first.type=string\n");
  TempConfig second("metadata.schema_version=1\nmetadata.field.second.source=second\nmetadata.field.second.type=int64\n");
  MetadataConfigStore store;
  ASSERT_TRUE(store.reload(first.path).ok());
  const auto before = store.snapshot();
  ASSERT_TRUE(before);
  ASSERT_EQ(before->fields().size(), 1u);
  EXPECT_EQ(before->fields().front().id, "first");

  ASSERT_TRUE(store.reload(second.path).ok());
  const auto after = store.snapshot();
  ASSERT_TRUE(after);
  ASSERT_EQ(after->fields().size(), 1u);
  EXPECT_EQ(after->fields().front().id, "second");

  // Readers holding a prior snapshot must continue to see the old schema.
  ASSERT_EQ(before->fields().size(), 1u);
  EXPECT_EQ(before->fields().front().id, "first");
}

TEST(MetadataConfigStore, NewStoreAndFailedInitialReloadStayEmpty) {
  test_support::TempDirectory dir;
  MetadataConfigStore store;
  EXPECT_EQ(store.generation(), 0u);
  EXPECT_FALSE(store.snapshot());
  EXPECT_FALSE(store.reload(dir.file("missing")).ok());
  EXPECT_EQ(store.generation(), 0u);
  EXPECT_FALSE(store.snapshot());
}

TEST(MetadataConfigStore, ConcurrentReloadsPublishCompleteImmutableSnapshots) {
  test_support::TempDirectory dir;
  const auto first = dir.write("first", "metadata.schema_version=1\n"
    "metadata.field.first.source=edition\nmetadata.field.first.type=int64\n");
  const auto second = dir.write("second", "metadata.schema_version=1\n"
    "metadata.field.second.source=centre\nmetadata.field.second.type=string\n");
  MetadataConfigStore store;
  ASSERT_TRUE(store.reload(first).ok());
  const auto retained = store.snapshot();
  std::atomic<bool> start{false};
  std::atomic<int> invalid_snapshots{0};
  std::vector<std::uint64_t> generations;
  std::mutex generations_mutex;
  std::vector<std::thread> workers;
  for (int writer = 0; writer < 3; ++writer) {
    workers.emplace_back([&, writer] {
      while (!start.load()) std::this_thread::yield();
      for (int iteration = 0; iteration < 20; ++iteration) {
        const auto result = store.reload(writer % 2 ? first : second);
        if (!result.ok()) { ++invalid_snapshots; continue; }
        std::lock_guard<std::mutex> lock(generations_mutex);
        generations.push_back(result.value());
      }
    });
  }
  for (int reader = 0; reader < 3; ++reader) {
    workers.emplace_back([&] {
      while (!start.load()) std::this_thread::yield();
      for (int iteration = 0; iteration < 1000; ++iteration) {
        const auto snapshot = store.snapshot();
        if (!snapshot || snapshot->fields().size() != 1) { ++invalid_snapshots; continue; }
        const auto& field = snapshot->fields()[0];
        if (!((field.id == "first" && field.source == "edition" && field.type == MetadataValueType::Int64) ||
              (field.id == "second" && field.source == "centre" && field.type == MetadataValueType::String)))
          ++invalid_snapshots;
      }
    });
  }
  start = true;
  for (auto& worker : workers) worker.join();
  EXPECT_EQ(invalid_snapshots.load(), 0);
  ASSERT_EQ(generations.size(), 60u);
  std::sort(generations.begin(), generations.end());
  for (std::size_t i = 0; i < generations.size(); ++i) EXPECT_EQ(generations[i], i + 2);
  EXPECT_EQ(store.generation(), 61u);
  ASSERT_TRUE(retained);
  ASSERT_EQ(retained->fields().size(), 1u);
  EXPECT_EQ(retained->fields()[0].id, "first");
}
