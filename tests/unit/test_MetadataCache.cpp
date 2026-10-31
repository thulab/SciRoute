#include "iotdbfs/MetadataCache.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace iotdbfs;

namespace {
const std::string kJson = R"({"name":{"type":"string","value":"alpha"}})";
}

TEST(MetadataCache, DeserializesOnMissAndReturnsCachedResult) {
  MetadataCache cache(4);
  auto first = cache.get_or_deserialize(1, "/a", kJson);
  ASSERT_TRUE(first.ok()) << first.error();
  auto second = cache.get(1, "/a", kJson);
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->ok()) << second->error();
  EXPECT_EQ(second->value().at("name").value(),
    first.value().at("name").value());
  EXPECT_EQ(cache.size(), 1u);
}

TEST(MetadataCache, MalformedMetadataIsNotCached) {
  MetadataCache cache(2);
  auto result = cache.get_or_deserialize(2, "/bad", "not-json");
  EXPECT_FALSE(result.ok());
  auto hit = cache.get(2, "/bad", "not-json");
  EXPECT_FALSE(hit.has_value());
  EXPECT_EQ(cache.size(), 0u);
}

TEST(MetadataCache, ExplicitPutNeverRetainsFailedDecoding) {
  MetadataCache cache(2);
  cache.put(1, "/bad", "not-json",
    MetadataResult<MetadataMap>::failure("invalid metadata"));
  EXPECT_FALSE(cache.get(1, "/bad", "not-json").has_value());
  EXPECT_EQ(cache.size(), 0u);
}

TEST(MetadataCache, EnforcesStricterSizeLimitOnCachedSuccess) {
  MetadataCache cache(2);
  ASSERT_TRUE(cache.get_or_deserialize(3, "/a", kJson).ok());
  auto limited = cache.get_or_deserialize(3, "/a", kJson, 8);
  EXPECT_FALSE(limited.ok());
  EXPECT_NE(limited.error().find("size cap"), std::string::npos);
}

TEST(MetadataCache, EnforcesMemoryBudgetInAdditionToEntryCapacity) {
  MetadataCache cache(10, 128);
  EXPECT_TRUE(cache.get_or_deserialize(1, "/large", kJson).ok());
  EXPECT_LE(cache.memory_usage(), cache.memory_budget());
  EXPECT_EQ(cache.size(), 0u);
}

TEST(MetadataCache, CapacityEvictsLeastRecentlyUsedEntry) {
  MetadataCache cache(2);
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  ASSERT_TRUE(cache.get_or_deserialize(2, "/b", kJson).ok());
  ASSERT_TRUE(cache.get(1, "/a", kJson).has_value()); // refresh /a
  ASSERT_TRUE(cache.get_or_deserialize(3, "/c", kJson).ok());
  EXPECT_TRUE(cache.get(1, "/a", kJson).has_value());
  EXPECT_FALSE(cache.get(2, "/b", kJson).has_value());
  EXPECT_TRUE(cache.get(3, "/c", kJson).has_value());
  EXPECT_EQ(cache.size(), 2u);
}

TEST(MetadataCache, InvalidateRemovesAllMetadataVersionsForPathAndRow) {
  MetadataCache cache(4);
  const std::string changed = R"({"name":{"type":"string","value":"beta"}})";
  ASSERT_TRUE(cache.get_or_deserialize(7, "/a", kJson).ok());
  ASSERT_TRUE(cache.get_or_deserialize(7, "/a", changed).ok());
  ASSERT_TRUE(cache.get_or_deserialize(8, "/a", kJson).ok());
  cache.invalidate(7, "/a");
  EXPECT_FALSE(cache.get(7, "/a", kJson).has_value());
  EXPECT_FALSE(cache.get(7, "/a", changed).has_value());
  EXPECT_TRUE(cache.get(8, "/a", kJson).has_value());
}

TEST(MetadataCache, ClearAndSetCapacityAreThreadSafe) {
  MetadataCache cache(8);
  std::vector<std::thread> workers;
  for (int i = 0; i < 8; ++i) {
    workers.emplace_back([&cache, i] {
      for (int j = 0; j < 100; ++j)
        cache.get_or_deserialize(i * 100 + j, "/f", kJson);
    });
  }
  for (auto& worker : workers) worker.join();
  cache.set_capacity(3);
  EXPECT_LE(cache.size(), 3u);
  cache.clear();
  EXPECT_EQ(cache.size(), 0u);
}

TEST(MetadataCache, ZeroCapacityOrBudgetDisablesRetentionButStillDecodes) {
  MetadataCache disabled(0);
  ASSERT_TRUE(disabled.get_or_deserialize(1, "/a", kJson).ok());
  EXPECT_EQ(disabled.capacity(), 0u);
  EXPECT_EQ(disabled.size(), 0u);
  EXPECT_EQ(disabled.memory_usage(), 0u);
  disabled.set_capacity(2);
  ASSERT_TRUE(disabled.get_or_deserialize(1, "/a", kJson).ok());
  EXPECT_EQ(disabled.size(), 1u);
  disabled.set_memory_budget(0);
  EXPECT_EQ(disabled.memory_budget(), 0u);
  EXPECT_EQ(disabled.memory_usage(), 0u);
  EXPECT_FALSE(disabled.get(1, "/a", kJson).has_value());
  ASSERT_TRUE(disabled.get_or_deserialize(1, "/a", kJson).ok());
  EXPECT_EQ(disabled.size(), 0u);
}

TEST(MetadataCache, MemoryBudgetEvictsLeastRecentlyUsedAndAcceptsExactBoundary) {
  MetadataCache cache(8);
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  const auto entry_bytes = cache.memory_usage();
  ASSERT_GT(entry_bytes, 0u);
  ASSERT_TRUE(cache.get_or_deserialize(2, "/b", kJson).ok());
  ASSERT_TRUE(cache.get_or_deserialize(3, "/c", kJson).ok());
  ASSERT_TRUE(cache.get(1, "/a", kJson).has_value());
  cache.set_memory_budget(entry_bytes * 2);
  EXPECT_EQ(cache.size(), 2u);
  EXPECT_EQ(cache.memory_usage(), entry_bytes * 2);
  EXPECT_FALSE(cache.get(2, "/b", kJson).has_value());
  EXPECT_TRUE(cache.get(1, "/a", kJson).has_value());
  EXPECT_TRUE(cache.get(3, "/c", kJson).has_value());
  cache.set_memory_budget(entry_bytes - 1);
  EXPECT_EQ(cache.size(), 0u);
  EXPECT_EQ(cache.memory_usage(), 0u);
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  EXPECT_EQ(cache.size(), 0u);
  cache.set_memory_budget(entry_bytes);
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_EQ(cache.memory_usage(), entry_bytes);
}

TEST(MetadataCache, RepeatedPutRefreshesRecencyWithoutDoubleCountingMemory) {
  MetadataCache cache(2);
  const auto value = cache.get_or_deserialize(1, "/a", kJson);
  ASSERT_TRUE(value.ok()) << value.error();
  ASSERT_TRUE(cache.get_or_deserialize(2, "/b", kJson).ok());
  const auto before = cache.memory_usage();
  cache.put(1, "/a", kJson, value);
  EXPECT_EQ(cache.size(), 2u);
  EXPECT_EQ(cache.memory_usage(), before);
  ASSERT_TRUE(cache.get_or_deserialize(3, "/c", kJson).ok());
  EXPECT_TRUE(cache.get(1, "/a", kJson).has_value());
  EXPECT_FALSE(cache.get(2, "/b", kJson).has_value());
  EXPECT_EQ(cache.memory_usage(), before);
}

TEST(MetadataCache, FailedOrOversizedInsertionPreservesPreviouslyCachedEntries) {
  MetadataCache cache(2);
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  const auto before = cache.memory_usage();
  cache.set_memory_budget(before);
  cache.put(1, "/a", kJson, MetadataResult<MetadataMap>::failure("decode failed"));
  ASSERT_TRUE(cache.get_or_deserialize(2, std::string(1000, 'x'), kJson).ok());
  EXPECT_TRUE(cache.get(1, "/a", kJson).has_value());
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_EQ(cache.memory_usage(), before);
}

TEST(MetadataCache, ReturnedMapsAreCopiesAndCannotModifyCachedValues) {
  MetadataCache cache(2);
  auto first = cache.get_or_deserialize(1, "/a", kJson);
  ASSERT_TRUE(first.ok()) << first.error();
  first.value()["name"] = MetadataValue("changed");
  first.value().emplace("extra", MetadataValue(true));
  auto second = cache.get(1, "/a", kJson);
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->ok()) << second->error();
  EXPECT_EQ(second->value().size(), 1u);
  EXPECT_EQ(std::get<std::string>(second->value().at("name").value()), "alpha");
}

TEST(MetadataCache, InvalidationIsExactAndReleasesMemory) {
  MetadataCache cache(8);
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  ASSERT_TRUE(cache.get_or_deserialize(1, "/ab", kJson).ok());
  ASSERT_TRUE(cache.get_or_deserialize(2, "/a", kJson).ok());
  const auto before = cache.memory_usage();
  cache.invalidate(42, "/missing");
  EXPECT_EQ(cache.memory_usage(), before);
  cache.invalidate(1, "/a");
  EXPECT_LT(cache.memory_usage(), before);
  EXPECT_FALSE(cache.get(1, "/a", kJson).has_value());
  EXPECT_TRUE(cache.get(1, "/ab", kJson).has_value());
  EXPECT_TRUE(cache.get(2, "/a", kJson).has_value());
  cache.clear();
  EXPECT_EQ(cache.memory_usage(), 0u);
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  cache.set_capacity(0);
  EXPECT_EQ(cache.size(), 0u);
  EXPECT_EQ(cache.memory_usage(), 0u);
}

TEST(MetadataCache, ExactReadBudgetAndHardLimitApplyBeforeCacheLookup) {
  MetadataCache cache(2);
  EXPECT_TRUE(cache.get_or_deserialize(1, "/a", kJson, kJson.size()).ok());
  EXPECT_FALSE(cache.get_or_deserialize(1, "/a", kJson, kJson.size() - 1).ok());
  EXPECT_TRUE(cache.get(1, "/a", kJson).has_value());
  EXPECT_TRUE(cache.get_or_deserialize(2, "/empty", "", 0).ok());
  EXPECT_FALSE(cache.get_or_deserialize(3, "/oversized", std::string(65537, ' '), 1000000).ok());
  EXPECT_FALSE(cache.get(3, "/oversized", std::string(65537, ' ')).has_value());
}

TEST(MetadataCache, ConcurrentReadersAndMutatorsKeepCacheUsableAndBounded) {
  MetadataCache cache(16, 65536);
  std::atomic<bool> start{false};
  std::atomic<bool> decoded{true};
  std::vector<std::thread> workers;
  for (int worker = 0; worker < 4; ++worker) {
    workers.emplace_back([&, worker] {
      while (!start.load()) std::this_thread::yield();
      for (int iteration = 0; iteration < 200; ++iteration) {
        const auto row = worker * 1000 + iteration % 16;
        const auto result = cache.get_or_deserialize(row, "/shared", kJson);
        if (!result.ok() || result.value().size() != 1 ||
            std::get<std::string>(result.value().at("name").value()) != "alpha")
          decoded.store(false);
        if (iteration % 3 == 0) cache.invalidate(row, "/shared");
      }
    });
  }
  workers.emplace_back([&] {
    while (!start.load()) std::this_thread::yield();
    for (int iteration = 0; iteration < 100; ++iteration) {
      cache.set_capacity(iteration % 17);
      cache.set_memory_budget(iteration % 2 ? 8192 : 65536);
      if (iteration % 7 == 0) cache.clear();
    }
  });
  start.store(true);
  for (auto& worker : workers) worker.join();
  EXPECT_TRUE(decoded.load());
  EXPECT_LE(cache.size(), cache.capacity());
  EXPECT_LE(cache.memory_usage(), cache.memory_budget());
  cache.set_capacity(2);
  cache.set_memory_budget(65536);
  cache.clear();
  ASSERT_TRUE(cache.get_or_deserialize(1, "/a", kJson).ok());
  EXPECT_TRUE(cache.get(1, "/a", kJson).has_value());
  EXPECT_EQ(cache.size(), 1u);
}
