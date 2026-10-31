#include "iotdbfs/query/frontend/TypedResultStore.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {
using namespace iotdbfs;

TypedResultStore::Config limits(std::uint64_t per_result,
    std::uint64_t resident, std::size_t entries) {
  TypedResultStore::Config config;
  config.max_result_bytes = per_result;
  config.max_resident_bytes = resident;
  config.max_entries = entries;
  return config;
}

TEST(TypedResultStoreTest, RequiresCompleteNonemptyResultAndAdmitsAtomically) {
  TypedResultStore store(limits(4, 8, 2));
  EXPECT_EQ(store.max_publish_bytes(), 4u);
  auto empty = store.publish({});
  EXPECT_EQ(empty.status().code(), QueryError::InvalidQuery);
  auto large = store.publish({1, 2, 3, 4, 5});
  EXPECT_EQ(large.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(store.stats().registered_entries, 0);
  EXPECT_EQ(store.stats().resident_bytes, 0);

  auto result = store.publish({1, 2, 3, 4});
  ASSERT_TRUE(result.ok());
  EXPECT_FALSE(result.value().empty());
  EXPECT_EQ(store.stats().registered_entries, 1);
  EXPECT_EQ(store.stats().resident_bytes, 4);
}

TEST(TypedResultStoreTest, EqualEncodedBytesShareAContentIdAndOneResidentPayload) {
  TypedResultStore store(limits(8, 8, 2));
  auto first = store.publish({1, 2, 3, 4});
  ASSERT_TRUE(first.ok());
  EXPECT_EQ(first.value(),
    "9f64a747e1b97f131fabb6b447296c9b6f0201e79fb3c5356e6c77e89b6a806a");
  std::vector<std::uint8_t> duplicate{1, 2, 3, 4};
  TypedResultStore::Handle pinned;
  auto second = store.publish_retryable(duplicate, &pinned);
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(second.value(), first.value());
  EXPECT_EQ(pinned.id(), first.value());
  EXPECT_TRUE(duplicate.empty());
  EXPECT_EQ(store.stats().registered_entries, 1u);
  EXPECT_EQ(store.stats().resident_bytes, 4u);
  EXPECT_EQ(pinned.read(0, 4).value(),
    (std::vector<std::uint8_t>{1, 2, 3, 4}));
}

TEST(TypedResultStoreTest, StaticPublishCeilingAlsoRespectsResidentLimit) {
  TypedResultStore store(limits(16, 6, 2));
  EXPECT_EQ(store.max_publish_bytes(), 6u);
  EXPECT_EQ(store.publish({1, 2, 3, 4, 5, 6, 7}).status().code(),
    QueryError::ResourceExhausted);
}

TEST(TypedResultStoreTest, OffsetReadsAndEofUseImmutableSnapshot) {
  TypedResultStore store(limits(16, 16, 2));
  auto result = store.publish({10, 20, 30, 40, 50});
  ASSERT_TRUE(result.ok());
  auto opened = store.open(result.value());
  ASSERT_TRUE(opened.ok());
  auto& handle = opened.value();
  EXPECT_EQ(handle.size(), 5);
  EXPECT_EQ(store.stats().open_handles, 1);

  auto middle = handle.read(2, 2);
  ASSERT_TRUE(middle.ok());
  EXPECT_EQ(middle.value(), (std::vector<std::uint8_t>{30, 40}));
  auto prefix = handle.read(0, 100);
  ASSERT_TRUE(prefix.ok());
  EXPECT_EQ(prefix.value(), (std::vector<std::uint8_t>{10, 20, 30, 40, 50}));
  EXPECT_TRUE(handle.read(5, 1).value().empty());
  EXPECT_TRUE(handle.read(100, 1).value().empty());
  EXPECT_TRUE(handle.read(0, 0).value().empty());

  handle.release();
  EXPECT_EQ(store.stats().open_handles, 0);
  EXPECT_EQ(handle.read(0, 1).status().code(), QueryError::InvalidQuery);
}

TEST(TypedResultStoreTest, EvictsOldestRegistrationUnderEntryAndByteCaps) {
  TypedResultStore store(limits(6, 10, 2));
  auto first = store.publish({1, 1, 1, 1});
  auto second = store.publish({2, 2, 2, 2});
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  auto third = store.publish({3, 3, 3, 3});
  ASSERT_TRUE(third.ok());
  EXPECT_EQ(store.open(first.value()).status().code(), QueryError::SourceNotFound);
  EXPECT_TRUE(store.open(second.value()).ok());
  EXPECT_TRUE(store.open(third.value()).ok());
  EXPECT_EQ(store.stats().registered_entries, 2);
  EXPECT_EQ(store.stats().resident_bytes, 8);
}

TEST(TypedResultStoreTest, OpenHandleSurvivesEntryEvictionAndRemainsAccounted) {
  TypedResultStore store(limits(4, 8, 1));
  auto first = store.publish({1, 2, 3, 4});
  ASSERT_TRUE(first.ok());
  auto pinned = store.open(first.value());
  ASSERT_TRUE(pinned.ok());
  auto second = store.publish({5, 6, 7, 8});
  ASSERT_TRUE(second.ok());
  auto second_pinned = store.open(second.value());
  ASSERT_TRUE(second_pinned.ok());
  EXPECT_EQ(store.open(first.value()).status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(store.stats().resident_bytes, 8);
  EXPECT_EQ(store.stats().registered_entries, 1);
  EXPECT_EQ(pinned.value().read(1, 2).value(),
    (std::vector<std::uint8_t>{2, 3}));

  // The evicted handle's bytes still block a third result until release.
  EXPECT_EQ(store.publish({9, 9, 9, 9}).status().code(),
    QueryError::ResourceExhausted);
  EXPECT_TRUE(store.open(second.value()).ok());
  pinned.value().release();
  EXPECT_EQ(store.stats().resident_bytes, 4);
  auto third = store.publish({9, 9, 9, 9});
  EXPECT_TRUE(third.ok());
}

TEST(TypedResultStoreTest, RetryableAdmissionPreservesEncodedBytes) {
  TypedResultStore store(limits(4, 4, 2));
  auto first = store.publish({1, 2, 3, 4});
  ASSERT_TRUE(first.ok());
  auto pinned = store.open(first.value());
  ASSERT_TRUE(pinned.ok());

  std::vector<std::uint8_t> encoded{5, 6};
  auto failed = store.publish_retryable(encoded);
  EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(encoded, (std::vector<std::uint8_t>{5, 6}));
  pinned.value().release();

  auto retried = store.publish_retryable(encoded);
  ASSERT_TRUE(retried.ok()) << retried.status().message();
  EXPECT_TRUE(encoded.empty());
  auto opened = store.open(retried.value());
  ASSERT_TRUE(opened.ok());
  EXPECT_EQ(opened.value().read(0, 2).value(),
    (std::vector<std::uint8_t>{5, 6}));
}

TEST(TypedResultStoreTest, PublicationCanPinBeforeAnotherPublishEvictsItsId) {
  TypedResultStore store(limits(4, 8, 1));
  std::vector<std::uint8_t> first_bytes{1, 2, 3, 4};
  TypedResultStore::Handle pinned;
  auto first = store.publish_retryable(first_bytes, &pinned);
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(pinned.valid());
  EXPECT_TRUE(first_bytes.empty());

  auto second = store.publish({5, 6, 7, 8});
  ASSERT_TRUE(second.ok());
  EXPECT_EQ(store.open(first.value()).status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(pinned.read(0, 4).value(),
    (std::vector<std::uint8_t>{1, 2, 3, 4}));
}

TEST(TypedResultStoreTest, ExpiryInvalidatesIdButNotOpenHandle) {
  TypedResultStore::Clock::time_point now{};
  auto config = limits(4, 4, 2);
  config.ttl = std::chrono::milliseconds(10);
  TypedResultStore store(config, [&] { return now; });
  auto result = store.publish({1, 2, 3, 4});
  ASSERT_TRUE(result.ok());
  auto pinned = store.open(result.value());
  ASSERT_TRUE(pinned.ok());

  now += std::chrono::milliseconds(10);
  EXPECT_EQ(store.open(result.value()).status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(store.stats().registered_entries, 0);
  EXPECT_EQ(store.stats().resident_bytes, 4);
  EXPECT_EQ(pinned.value().read(0, 4).value(),
    (std::vector<std::uint8_t>{1, 2, 3, 4}));
  EXPECT_EQ(store.publish({5}).status().code(), QueryError::ResourceExhausted);
  pinned.value().release();
  EXPECT_EQ(store.stats().resident_bytes, 0);
  EXPECT_TRUE(store.publish({5}).ok());
}

TEST(TypedResultStoreTest, OpenHandleOutlivesTheRegistryOwner) {
  TypedResultStore::Handle handle;
  {
    TypedResultStore store(limits(4, 4, 1));
    auto result = store.publish({11, 12, 13});
    ASSERT_TRUE(result.ok());
    auto opened = store.open(result.value());
    ASSERT_TRUE(opened.ok());
    handle = std::move(opened.value());
  }
  ASSERT_TRUE(handle.valid());
  EXPECT_EQ(handle.read(1, 2).value(),
    (std::vector<std::uint8_t>{12, 13}));
  handle.release();
}

TEST(TypedResultStoreTest, FailureDoesNotEvictPinnedResult) {
  TypedResultStore store(limits(6, 8, 2));
  auto first = store.publish({1, 2, 3, 4, 5, 6});
  ASSERT_TRUE(first.ok());
  auto pinned = store.open(first.value());
  ASSERT_TRUE(pinned.ok());
  auto failed = store.publish({7, 8, 9, 10});
  EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(store.open(first.value()).ok());
  EXPECT_EQ(store.stats().registered_entries, 1);
  EXPECT_EQ(store.stats().resident_bytes, 6);
}

TEST(TypedResultStoreTest, FailedAdmissionDoesNotFlushUsefulEntries) {
  TypedResultStore store(limits(8, 10, 3));
  auto pinned_result = store.publish({1, 1, 1, 1, 1, 1, 1, 1});
  auto other = store.publish({2});
  ASSERT_TRUE(pinned_result.ok());
  ASSERT_TRUE(other.ok());
  auto pinned = store.open(pinned_result.value());
  ASSERT_TRUE(pinned.ok());
  EXPECT_EQ(store.publish({3, 3, 3}).status().code(),
    QueryError::ResourceExhausted);
  EXPECT_TRUE(store.open(other.value()).ok());
  EXPECT_EQ(store.stats().registered_entries, 2);
  EXPECT_EQ(store.stats().resident_bytes, 9);
}

TEST(TypedResultStoreTest, ConcurrentHandlesReadTheSameBytes) {
  TypedResultStore store(limits(256, 256, 4));
  std::vector<std::uint8_t> original(128);
  for (std::size_t index = 0; index < original.size(); ++index)
    original[index] = static_cast<std::uint8_t>(index);
  auto published = store.publish(original);
  ASSERT_TRUE(published.ok());
  std::atomic<bool> correct{true};
  std::vector<std::thread> readers;
  for (int worker = 0; worker < 8; ++worker) {
    readers.emplace_back([&] {
      auto opened = store.open(published.value());
      if (!opened.ok()) { correct = false; return; }
      for (std::size_t offset = 0; offset < 128; offset += 7) {
        auto bytes = opened.value().read(offset, 7);
        if (!bytes.ok()) { correct = false; return; }
        for (std::size_t part = 0; part < bytes.value().size(); ++part) {
          if (bytes.value()[part] != original[offset + part]) correct = false;
        }
      }
    });
  }
  for (auto& reader : readers) reader.join();
  EXPECT_TRUE(correct);
  EXPECT_EQ(store.stats().open_handles, 0);
  EXPECT_EQ(store.stats().resident_bytes, 128);
}

TEST(TypedResultStoreTest, RejectsInvalidLimits) {
  auto config = limits(4, 8, 2);
  config.max_entries = 0;
  EXPECT_THROW(TypedResultStore store(config), std::invalid_argument);
  config.max_entries = 2;
  config.ttl = std::chrono::milliseconds(0);
  EXPECT_THROW(TypedResultStore store(config), std::invalid_argument);
}
TEST(TypedResultStoreTest, PreparedPublicationDoesNotExposeIdAndRechecksCapacity) {
  TypedResultStore store(limits(4, 4, 1));
  auto prepared = store.prepare_publication({1, 2, 3, 4});
  ASSERT_TRUE(prepared.ok());
  ASSERT_TRUE(store.prepare_capacity(prepared.value()).ok());
  EXPECT_EQ(store.stats().registered_entries, 0u);
  auto competing = store.publish({5, 6, 7, 8});
  ASSERT_TRUE(competing.ok());
  auto pinned = store.open(competing.value());
  ASSERT_TRUE(pinned.ok());
  EXPECT_EQ(store.publish_prepared_retryable(prepared.value()).status().code(),
    QueryError::ResourceExhausted);
  EXPECT_EQ(prepared.value().size(), 4u);
  pinned.value().release();
  ASSERT_TRUE(store.prepare_capacity(prepared.value()).ok());
  TypedResultStore::Handle final_handle;
  auto published = store.publish_prepared_retryable(prepared.value(), &final_handle);
  ASSERT_TRUE(published.ok());
  EXPECT_EQ(final_handle.read(0, 4).value(), (std::vector<std::uint8_t>{1, 2, 3, 4}));
}

TEST(TypedResultStoreTest, ImmutableSlicePinsExpiredPayloadAndResidentAccounting) {
  auto now = TypedResultStore::Clock::now();
  TypedResultStore store(limits(4, 4, 1), [&] { return now; });
  auto published = store.publish({1, 2, 3, 4});
  ASSERT_TRUE(published.ok());
  auto handle = store.open(published.value());
  ASSERT_TRUE(handle.ok());
  auto slice = handle.value().slice(1, 99);
  ASSERT_TRUE(slice.ok());
  EXPECT_EQ(slice.value().size(), 3u);
  handle.value().release();
  now += std::chrono::hours(1);
  EXPECT_EQ(store.stats().registered_entries, 0u);
  EXPECT_EQ(store.stats().open_handles, 0u);
  EXPECT_EQ(store.stats().resident_bytes, 4u);
  EXPECT_EQ(store.publish({5}).status().code(), QueryError::ResourceExhausted);
  slice.value().with_bytes([](const std::uint8_t* bytes, std::size_t count) {
    EXPECT_EQ((std::vector<std::uint8_t>(bytes, bytes + count)),
      (std::vector<std::uint8_t>{2, 3, 4}));
  });
  slice = QueryResult<TypedResultStore::ReadOnlySlice>::success({});
  EXPECT_EQ(store.stats().resident_bytes, 0u);
  EXPECT_TRUE(store.publish({5}).ok());
}

TEST(TypedResultStoreTest, ImmutableSliceOutlivesStoreAndBoundsOffsets) {
  TypedResultStore::ReadOnlySlice retained;
  {
    TypedResultStore store(limits(4, 4, 1));
    auto id = store.publish({8, 9, 10});
    ASSERT_TRUE(id.ok());
    auto handle = store.open(id.value());
    ASSERT_TRUE(handle.ok());
    EXPECT_EQ(handle.value().slice(UINT64_MAX, 4).value().size(), 0u);
    EXPECT_EQ(handle.value().slice(0, 0).value().size(), 0u);
    retained = handle.value().slice(0, 2).value();
    handle.value().release();
    EXPECT_EQ(handle.value().slice(0, 2).status().code(), QueryError::InvalidQuery);
  }
  retained.with_bytes([](const std::uint8_t* bytes, std::size_t count) {
    ASSERT_EQ(count, 2u);
    EXPECT_EQ(bytes[0], 8u);
    EXPECT_EQ(bytes[1], 9u);
  });
}

} // namespace
