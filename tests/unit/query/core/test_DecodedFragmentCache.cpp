#include "iotdbfs/query/core/DecodedFragmentCache.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryRuntime.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryReadContext.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {
using namespace iotdbfs;
using namespace std::chrono_literals;
using Loaded = QueryResult<std::shared_ptr<const DecodedFragment>>;
using BatchLoaded = QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>>;

class TestFragment final : public DecodedFragment {
public:
  TestFragment(std::uint64_t bytes, int value = 42,
    std::shared_ptr<std::atomic<int>> destroyed = {})
    : value(value), bytes_(bytes), destroyed_(std::move(destroyed)) {}
  ~TestFragment() override { if (destroyed_) ++*destroyed_; }
  std::uint64_t resident_bytes() const override { return bytes_; }
  const int value;
private:
  std::uint64_t bytes_;
  std::shared_ptr<std::atomic<int>> destroyed_;
};

Loaded loaded(std::uint64_t bytes, int value = 42) {
  return Loaded::success(std::make_shared<TestFragment>(bytes, value));
}
QueryStatus failed(QueryError code) {
  return QueryStatus::failure(code, QueryPhase::Execution, "injected test failure");
}
QueryRuntimeConfig config() {
  QueryRuntimeConfig result;
  result.memory_budget_bytes = 1024;
  result.max_entries = 16;
  result.max_inflight = 4;
  result.worker_threads = 4;
  result.wait_timeout_ms = 2000;
  return result;
}

// The loader cannot finish before the test opens its gate. Timed waits are
// failure safeguards, not timing assumptions about thread scheduling.
class LoadGate {
public:
  Loaded run(const FragmentLoadContext& context, std::uint64_t bytes = 64) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      ++started_;
      cv_.notify_all();
      if (!cv_.wait_for(lock, 5s, [&] { return open_; }))
        return Loaded::failure(failed(QueryError::Internal));
    }
    const bool cancelled = context.cancelled();
    {
      std::lock_guard<std::mutex> lock(mutex_);
      cancelled_ = cancelled;
      ++finished_;
      cv_.notify_all();
    }
    if (cancelled) return Loaded::failure(failed(QueryError::Cancelled));
    context.record_decode(bytes);
    return loaded(bytes);
  }
  bool started(unsigned count = 1) {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, 2s, [&] { return started_ >= count; });
  }
  bool finished() {
    std::unique_lock<std::mutex> lock(mutex_);
    return cv_.wait_for(lock, 2s, [&] { return finished_ != 0; });
  }
  bool observed_cancel() {
    std::lock_guard<std::mutex> lock(mutex_);
    return cancelled_;
  }
  void open() {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = true;
    cv_.notify_all();
  }
private:
  std::mutex mutex_;
  std::condition_variable cv_;
  unsigned started_{0}, finished_{0};
  bool open_{false}, cancelled_{false};
};

template<class Predicate> bool eventually(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  do {
    if (predicate()) return true;
    std::this_thread::yield();
  } while (std::chrono::steady_clock::now() < deadline);
  return predicate();
}

TEST(DecodedFragmentCacheTest, ConcurrentSameKeyHasOneProducerAndIdenticalAllocation) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  std::atomic<int> calls{0};
  auto loader = [&](const FragmentLoadContext& context) { ++calls; return gate.run(context); };
  auto first = std::async(std::launch::async, [&] { return cache.get("shared", {64, 32}, loader); });
  EXPECT_TRUE(gate.started());
  const auto initial_waits = cache.stats().waits;
  std::vector<std::future<QueryResult<FragmentLease>>> followers;
  for (int i = 0; i < 3; ++i)
    followers.push_back(std::async(std::launch::async, [&] { return cache.get("shared", {64, 32}, loader); }));
  EXPECT_TRUE(eventually([&] { return cache.stats().waits >= initial_waits + 3; }));
  gate.open();
  auto result = first.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  for (auto& future : followers) {
    auto other = future.get();
    ASSERT_TRUE(other.ok()) << other.status().message();
    EXPECT_EQ(other.value().shared().get(), result.value().shared().get());
  }
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(cache.stats().producer_count, 1u);
  EXPECT_EQ(cache.stats().decode_count, 1u);
  EXPECT_EQ(cache.stats().decoded_bytes, 64u);
}

TEST(DecodedFragmentCacheTest, BatchClaimsOnlyMissesAndSharesKeysWithScalarGet) {
  DecodedFragmentCache cache(config());
  auto warm = cache.get("warm", {32, 0}, [](const auto&) { return loaded(32, 1); });
  ASSERT_TRUE(warm.ok());
  std::vector<std::size_t> observed;
  const std::vector<FragmentBatchItem> items{{"warm", {32, 0}},
    {"cold-a", {48, 0}}, {"cold-b", {64, 0}}};
  auto batch = cache.get_batch(items, 80,
    [&](const auto& indices, const FragmentLoadContext& context) {
      observed = indices;
      EXPECT_TRUE(context.record_io(80, 2).ok());
      std::vector<std::shared_ptr<const DecodedFragment>> fragments;
      for (auto index : indices) {
        const auto bytes = items[index].memory.resident_bytes;
        context.record_decode(bytes);
        fragments.push_back(std::make_shared<TestFragment>(bytes, static_cast<int>(index)));
      }
      return BatchLoaded::success(std::move(fragments));
    });
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  EXPECT_EQ(observed, (std::vector<std::size_t>{1, 2}));
  ASSERT_EQ(batch.value().size(), 3u);
  EXPECT_EQ(batch.value()[0].shared().get(), warm.value().shared().get());
  auto scalar = cache.get("cold-a", {48, 0}, [](const auto&) {
    ADD_FAILURE() << "batch-produced key should be a scalar cache hit";
    return loaded(48);
  });
  ASSERT_TRUE(scalar.ok());
  EXPECT_EQ(scalar.value().shared().get(), batch.value()[1].shared().get());
  const auto stats = cache.stats();
  EXPECT_EQ(stats.producer_count, 3u);
  EXPECT_EQ(stats.read_ranges, 2u);
  EXPECT_EQ(stats.temporary_bytes, 0u);
  EXPECT_EQ(stats.inflight_bytes, 0u);
}

TEST(DecodedFragmentCacheTest, ScalarFollowerJoinsBatchProducer) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  std::atomic<int> calls{0};
  const std::vector<FragmentBatchItem> items{{"a", {64, 0}}, {"b", {64, 0}}};
  auto batch = std::async(std::launch::async, [&] {
    return cache.get_batch(items, 16,
      [&](const auto& indices, const FragmentLoadContext& context) {
        ++calls;
        auto held = gate.run(context);
        if (!held.ok()) return BatchLoaded::failure(held.status());
        std::vector<std::shared_ptr<const DecodedFragment>> fragments;
        for (auto index : indices)
          fragments.push_back(std::make_shared<TestFragment>(64, static_cast<int>(index)));
        return BatchLoaded::success(std::move(fragments));
      });
  });
  ASSERT_TRUE(gate.started());
  auto follower = std::async(std::launch::async, [&] {
    return cache.get("b", {64, 0}, [](const auto&) {
      ADD_FAILURE() << "scalar follower must not start a second producer";
      return loaded(64);
    });
  });
  ASSERT_TRUE(eventually([&] { return cache.stats().waits >= 1; }));
  gate.open();
  auto result = batch.get();
  auto joined = follower.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_TRUE(joined.ok()) << joined.status().message();
  EXPECT_EQ(joined.value().shared().get(), result.value()[1].shared().get());
  EXPECT_EQ(calls, 1);
}

TEST(DecodedFragmentCacheTest, BatchRejectsPartialOrOversizedOutputWithoutPublishingAnyKey) {
  DecodedFragmentCache cache(config());
  const std::vector<FragmentBatchItem> items{{"a", {32, 0}}, {"b", {32, 0}}};
  auto partial = cache.get_batch(items, 0,
    [](const auto&, const auto&) {
      return BatchLoaded::success(std::vector<std::shared_ptr<const DecodedFragment>>{
        std::make_shared<TestFragment>(32)});
    });
  EXPECT_EQ(partial.status().code(), QueryError::Internal);
  auto oversized = cache.get_batch(items, 0,
    [](const auto&, const auto&) {
      return BatchLoaded::success(std::vector<std::shared_ptr<const DecodedFragment>>{
        std::make_shared<TestFragment>(32), std::make_shared<TestFragment>(64)});
    });
  EXPECT_EQ(oversized.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
  auto retry = cache.get("a", {32, 0}, [](const auto&) { return loaded(32); });
  ASSERT_TRUE(retry.ok());
}

TEST(DecodedFragmentCacheTest, CancellingBatchWaiterPreservesScalarFollower) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken token;
  const std::vector<FragmentBatchItem> items{{"a", {64, 0}}, {"b", {64, 0}}};
  auto batch = std::async(std::launch::async, [&] {
    return cache.get_batch(items, 16,
      [&](const auto& indices, const FragmentLoadContext& context) {
        auto held = gate.run(context);
        if (!held.ok()) return BatchLoaded::failure(held.status());
        std::vector<std::shared_ptr<const DecodedFragment>> fragments;
        for (auto index : indices)
          fragments.push_back(std::make_shared<TestFragment>(64, static_cast<int>(index)));
        return BatchLoaded::success(std::move(fragments));
      }, token);
  });
  ASSERT_TRUE(gate.started());
  auto follower = std::async(std::launch::async, [&] {
    return cache.get("b", {64, 0}, [](const auto&) {
      ADD_FAILURE() << "scalar follower must join the batch";
      return loaded(64);
    });
  });
  ASSERT_TRUE(eventually([&] { return cache.stats().waits >= 1; }));
  token.cancel();
  EXPECT_EQ(batch.get().status().code(), QueryError::Cancelled);
  gate.open();
  auto joined = follower.get();
  ASSERT_TRUE(joined.ok()) << joined.status().message();
  auto reused = cache.get("b", {64, 0}, [](const auto&) {
    ADD_FAILURE() << "follower's fragment should remain cached";
    return loaded(64);
  });
  ASSERT_TRUE(reused.ok());
  EXPECT_EQ(reused.value().shared().get(), joined.value().shared().get());
}

TEST(DecodedFragmentCacheTest, BatchSharedTemporaryReservationIsChargedOnce) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  const std::vector<FragmentBatchItem> items{{"a", {64, 0}}, {"b", {64, 0}}};
  auto batch = std::async(std::launch::async, [&] {
    return cache.get_batch(items, 128,
      [&](const auto& indices, const FragmentLoadContext& context) {
        auto held = gate.run(context);
        if (!held.ok()) return BatchLoaded::failure(held.status());
        std::vector<std::shared_ptr<const DecodedFragment>> fragments;
        for (auto index : indices)
          fragments.push_back(std::make_shared<TestFragment>(64, static_cast<int>(index)));
        return BatchLoaded::success(std::move(fragments));
      });
  });
  ASSERT_TRUE(gate.started());
  const auto during = cache.stats();
  EXPECT_EQ(during.inflight_bytes, 128u);
  EXPECT_EQ(during.temporary_bytes, 128u);
  EXPECT_EQ(during.live_bytes, 256u);
  gate.open();
  auto result = batch.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  const auto after = cache.stats();
  EXPECT_EQ(after.cache_owned_bytes, 128u);
  EXPECT_EQ(after.temporary_bytes, 0u);
  EXPECT_EQ(after.live_bytes, 128u);
}

TEST(DecodedFragmentCacheTest, CancelledBatchDoesNotBlockOrEraseReplacementKey) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken token;
  const std::vector<FragmentBatchItem> items{{"a", {32, 0}}, {"b", {32, 0}}};
  auto abandoned = std::async(std::launch::async, [&] {
    return cache.get_batch(items, 0,
      [&](const auto& indices, const FragmentLoadContext& context) {
        auto held = gate.run(context, 32);
        if (!held.ok()) return BatchLoaded::failure(held.status());
        std::vector<std::shared_ptr<const DecodedFragment>> fragments;
        for (auto index : indices)
          fragments.push_back(std::make_shared<TestFragment>(32, static_cast<int>(index)));
        return BatchLoaded::success(std::move(fragments));
      }, token);
  });
  ASSERT_TRUE(gate.started());
  token.cancel();
  EXPECT_EQ(abandoned.get().status().code(), QueryError::Cancelled);
  auto replacement = cache.get("a", {32, 0}, [](const auto&) { return loaded(32, 99); });
  ASSERT_TRUE(replacement.ok()) << replacement.status().message();
  gate.open();
  ASSERT_TRUE(gate.finished());
  auto reused = cache.get("a", {32, 0}, [](const auto&) {
    ADD_FAILURE() << "old batch must not erase replacement";
    return loaded(32);
  });
  ASSERT_TRUE(reused.ok());
  EXPECT_EQ(reused.value().shared().get(), replacement.value().shared().get());
}

TEST(DecodedFragmentCacheTest, DifferentKeysRemainIndependent) {
  DecodedFragmentCache cache(config());
  auto a = cache.get("a", {32, 0}, [](const auto&) { return loaded(32, 11); });
  auto b = cache.get("b", {32, 0}, [](const auto&) { return loaded(32, 22); });
  ASSERT_TRUE(a.ok()); ASSERT_TRUE(b.ok());
  EXPECT_EQ(a.value().as<TestFragment>()->value, 11);
  EXPECT_EQ(b.value().as<TestFragment>()->value, 22);
  EXPECT_NE(a.value().shared().get(), b.value().shared().get());
  EXPECT_EQ(cache.stats().producer_count, 2u);
}

TEST(DecodedFragmentCacheTest, EntryLimitEvictsUnpinnedBeforeOlderPinnedFragment) {
  auto limits = config();
  limits.max_entries = 2;
  DecodedFragmentCache cache(limits);
  auto a = cache.get("a", {10, 0}, [](const auto&) { return loaded(10, 11); });
  ASSERT_TRUE(a.ok());
  {
    auto b = cache.get("b", {10, 0}, [](const auto&) { return loaded(10, 22); });
    ASSERT_TRUE(b.ok());
  }

  auto c = cache.get("c", {10, 0}, [](const auto&) { return loaded(10, 33); });
  ASSERT_TRUE(c.ok()) << c.status().message();
  EXPECT_EQ(cache.stats().evictions, 1u);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 20u);
  EXPECT_EQ(cache.stats().pinned_bytes, 20u);
  EXPECT_EQ(cache.stats().live_bytes, 20u);

  bool decoded_again = false;
  auto a_again = cache.get("a", {10, 0}, [&](const auto&) {
    decoded_again = true;
    return loaded(10, 44);
  });
  ASSERT_TRUE(a_again.ok()) << a_again.status().message();
  EXPECT_FALSE(decoded_again);
  EXPECT_EQ(a_again.value().shared().get(), a.value().shared().get());
}

TEST(DecodedFragmentCacheTest, EntryLimitCanEvictPinnedWhenNoUnpinnedEntryExists) {
  auto limits = config();
  limits.max_entries = 2;
  DecodedFragmentCache cache(limits);
  auto a = cache.get("a", {10, 0}, [](const auto&) { return loaded(10, 11); });
  auto b = cache.get("b", {10, 0}, [](const auto&) { return loaded(10, 22); });
  ASSERT_TRUE(a.ok()); ASSERT_TRUE(b.ok());

  auto c = cache.get("c", {10, 0}, [](const auto&) { return loaded(10, 33); });
  ASSERT_TRUE(c.ok()) << c.status().message();
  EXPECT_EQ(cache.stats().evictions, 1u);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 20u);
  EXPECT_EQ(cache.stats().pinned_bytes, 30u);
  EXPECT_EQ(cache.stats().live_bytes, 30u);
  EXPECT_EQ(a.value().as<TestFragment>()->value, 11);
}

TEST(DecodedFragmentCacheTest, MemoryPressureEvictsOnlyTheOldestNeededFragment) {
  auto limits = config();
  limits.memory_budget_bytes = 200;
  DecodedFragmentCache cache(limits);
  for (int i = 0; i < 10; ++i) {
    auto result = cache.get("small-" + std::to_string(i), {15, 0},
      [](const auto&) { return loaded(15); });
    ASSERT_TRUE(result.ok()) << result.status().message();
  }
  ASSERT_EQ(cache.stats().live_bytes, 150u);

  auto large = cache.get("large", {60, 0},
    [](const auto&) { return loaded(60); });
  ASSERT_TRUE(large.ok()) << large.status().message();
  EXPECT_EQ(cache.stats().evictions, 1u);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 195u);
  EXPECT_EQ(cache.stats().live_bytes, 195u);

  bool unexpected_decode = false;
  auto retained = cache.get("small-1", {15, 0},
    [&](const auto&) {
      unexpected_decode = true;
      return loaded(15);
    });
  ASSERT_TRUE(retained.ok()) << retained.status().message();
  EXPECT_FALSE(unexpected_decode);
  EXPECT_EQ(cache.stats().evictions, 1u);
}

TEST(DecodedFragmentCacheTest, LaterWaiterExtendsSharedProducerMemoryDeadline) {
  auto limits = config();
  limits.memory_budget_bytes = 64;
  limits.wait_timeout_ms = 800;
  limits.enable_detailed_profile = true;
  DecodedFragmentCache cache(limits);
  auto held = cache.get("held", {64, 0}, [](const auto&) { return loaded(64, 1); });
  ASSERT_TRUE(held.ok());

  std::atomic<int> loads{0};
  auto loader = [&](const auto&) {
    ++loads;
    return loaded(64, 2);
  };
  auto first = std::async(std::launch::async, [&] {
    return cache.get("shared", {64, 0}, loader);
  });
  ASSERT_TRUE(eventually([&] { return cache.stats().producer_count == 2; }));

  // The first producer cannot reserve memory while the other fragment is
  // leased. Join later, so this waiter's own deadline extends beyond the first.
  std::this_thread::sleep_for(350ms);
  auto later = std::async(std::launch::async, [&] {
    return cache.get("shared", {64, 0}, loader);
  });
  ASSERT_TRUE(eventually([&] { return cache.stats().waits >= 1; }));
  ASSERT_EQ(first.wait_for(1s), std::future_status::ready);
  auto timed_out = first.get();
  ASSERT_FALSE(timed_out.ok());
  EXPECT_EQ(timed_out.status().code(), QueryError::ResourceExhausted);

  // Free memory after the first deadline but before the later waiter's own
  // deadline. The shared producer must still be alive for that waiter.
  held.value() = FragmentLease{};
  auto result = later.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value().as<TestFragment>()->value, 2);
  EXPECT_GT(cache.stats().fragment_memory_wait_nanoseconds, 0u);
  EXPECT_EQ(loads, 1);
  EXPECT_EQ(cache.stats().producer_count, 2u);
}

TEST(DecodedFragmentCacheTest, FailedProducerIsNotPublishedAndNextCallRetries) {
  DecodedFragmentCache cache(config());
  std::atomic<int> calls{0};
  auto first = cache.get("retry", {64, 32}, [&](const auto&) {
    ++calls;
    return Loaded::failure(failed(QueryError::CorruptPayload));
  });
  ASSERT_FALSE(first.ok());
  EXPECT_EQ(first.status().code(), QueryError::CorruptPayload);
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
  auto second = cache.get("retry", {64, 32}, [&](const auto&) { ++calls; return loaded(64, 9); });
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_EQ(second.value().as<TestFragment>()->value, 9);
  EXPECT_EQ(calls, 2);
}

TEST(DecodedFragmentCacheTest, CancellingFollowerDoesNotCancelProducerOrOtherWaiter) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken follower_token;
  auto loader = [&](const auto& context) { return gate.run(context); };
  auto producer_waiter = std::async(std::launch::async, [&] { return cache.get("a", {64, 0}, loader); });
  EXPECT_TRUE(gate.started());
  const auto initial_waits = cache.stats().waits;
  auto follower = std::async(std::launch::async, [&] { return cache.get("a", {64, 0}, loader, follower_token); });
  EXPECT_TRUE(eventually([&] { return cache.stats().waits > initial_waits; }));
  follower_token.cancel();
  EXPECT_EQ(follower.wait_for(1s), std::future_status::ready);
  auto cancelled = follower.get();
  EXPECT_FALSE(cancelled.ok());
  EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  gate.open();
  auto result = producer_waiter.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_FALSE(gate.observed_cancel());
  EXPECT_EQ(cache.stats().producer_count, 1u);
}

TEST(DecodedFragmentCacheTest, InitiatingWaiterCanCancelWhileFollowerReceivesResult) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken initial_token;
  auto loader = [&](const auto& context) { return gate.run(context); };
  auto initial = std::async(std::launch::async, [&] { return cache.get("a", {64, 0}, loader, initial_token); });
  EXPECT_TRUE(gate.started());
  const auto initial_waits = cache.stats().waits;
  auto survivor = std::async(std::launch::async, [&] { return cache.get("a", {64, 0}, loader); });
  EXPECT_TRUE(eventually([&] { return cache.stats().waits > initial_waits; }));
  initial_token.cancel();
  EXPECT_EQ(initial.wait_for(1s), std::future_status::ready);
  auto cancelled = initial.get();
  EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  gate.open();
  auto result = survivor.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_FALSE(gate.observed_cancel());
  EXPECT_EQ(cache.stats().producer_count, 1u);
}

TEST(DecodedFragmentCacheTest, CancelledLastWaiterDoesNotBlockOrEraseReplacement) {
  auto limits = config();
  limits.wait_timeout_ms = 300;
  DecodedFragmentCache cache(limits);
  LoadGate old_gate;
  CancellationToken old_waiter;
  auto old = std::async(std::launch::async, [&] {
    return cache.get("shared", {32, 0},
      [&](const auto& context) -> Loaded {
        (void)old_gate.run(context, 32);
        // Simulate a loader that completes successfully despite cancellation.
        return loaded(32, 77);
      }, old_waiter);
  });
  if (!old_gate.started()) {
    old_gate.open();
    old.get();
    FAIL() << "old producer did not start";
  }
  old_waiter.cancel();
  auto cancelled = old.get();
  ASSERT_FALSE(cancelled.ok());
  EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);

  // The old producer is still blocked in its loader. A new request for the
  // same key must start its own producer without waiting for that callback.
  auto replacement = cache.get("shared", {32, 0},
    [](const auto&) { return loaded(32, 99); });
  old_gate.open();
  ASSERT_TRUE(replacement.ok()) << replacement.status().message();
  ASSERT_TRUE(eventually([&] { return cache.stats().inflight_bytes == 0; }));

  // The old worker finishes after the replacement and discards its returned
  // fragment. That cleanup must not erase the new entry with the same key.
  bool loaded_again = false;
  auto reuse = cache.get("shared", {32, 0}, [&](const auto&) {
    loaded_again = true;
    return loaded(32, 111);
  });
  ASSERT_TRUE(reuse.ok()) << reuse.status().message();
  EXPECT_FALSE(loaded_again);
  EXPECT_EQ(reuse.value().shared().get(), replacement.value().shared().get());
  EXPECT_EQ(cache.stats().producer_count, 2u);
}

TEST(DecodedFragmentCacheTest, AllWaitersCancelProducerAndReleaseReservations) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken first_token, second_token;
  auto loader = [&](const auto& context) { return gate.run(context); };
  auto first = std::async(std::launch::async, [&] { return cache.get("a", {64, 32}, loader, first_token); });
  EXPECT_TRUE(gate.started());
  const auto initial_waits = cache.stats().waits;
  auto second = std::async(std::launch::async, [&] { return cache.get("a", {64, 32}, loader, second_token); });
  EXPECT_TRUE(eventually([&] { return cache.stats().waits > initial_waits; }));
  first_token.cancel(); second_token.cancel();
  EXPECT_EQ(first.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(second.wait_for(1s), std::future_status::ready);
  EXPECT_EQ(first.get().status().code(), QueryError::Cancelled);
  EXPECT_EQ(second.get().status().code(), QueryError::Cancelled);
  gate.open();
  EXPECT_TRUE(gate.finished());
  EXPECT_TRUE(gate.observed_cancel());
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
}

TEST(DecodedFragmentCacheTest, AlreadyCancelledWaiterStartsNoProducer) {
  DecodedFragmentCache cache(config());
  CancellationToken token;
  token.cancel();
  bool called = false;
  auto result = cache.get("a", {64, 0}, [&](const auto&) { called = true; return loaded(64); }, token);
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_FALSE(called);
  EXPECT_EQ(cache.stats().producer_count, 0u);
}

TEST(DecodedFragmentCacheTest, WaitTimeoutIsBoundedAndCancelsAbandonedLoad) {
  auto limits = config(); limits.wait_timeout_ms = 30;
  DecodedFragmentCache cache(limits);
  LoadGate gate;
  const auto started = std::chrono::steady_clock::now();
  auto result = cache.get("a", {64, 0}, [&](const auto& context) { return gate.run(context); });
  const auto elapsed = std::chrono::steady_clock::now() - started;
  gate.open();
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_LT(elapsed, 1s);
  EXPECT_TRUE(gate.finished());
  EXPECT_TRUE(gate.observed_cancel());
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
}

TEST(DecodedFragmentCacheTest, InflightResidentAndTemporaryMemoryAreReservedBeforeLoader) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  auto result = std::async(std::launch::async, [&] {
    return cache.get("a", {64, 32}, [&](const auto& context) { return gate.run(context); });
  });
  EXPECT_TRUE(gate.started());
  const auto running = cache.stats();
  EXPECT_EQ(running.inflight_bytes, 64u);
  EXPECT_EQ(running.temporary_bytes, 32u);
  EXPECT_EQ(running.live_bytes, 96u);
  EXPECT_GE(running.peak_bytes, 96u);
  EXPECT_EQ(running.cache_owned_bytes, 0u);
  gate.open();
  auto finished = result.get();
  ASSERT_TRUE(finished.ok()) << finished.status().message();
  const auto after = cache.stats();
  EXPECT_EQ(after.inflight_bytes, 0u);
  EXPECT_EQ(after.temporary_bytes, 0u);
  EXPECT_EQ(after.live_bytes, 64u);
  EXPECT_EQ(after.cache_owned_bytes, 64u);
  EXPECT_EQ(after.pinned_bytes, 64u);
}

TEST(DecodedFragmentCacheTest, OversizedReservationNeverInvokesLoader) {
  auto limits = config(); limits.memory_budget_bytes = 100;
  DecodedFragmentCache cache(limits);
  bool called = false;
  auto result = cache.get("a", {80, 30}, [&](const auto&) { called = true; return loaded(80); });
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_FALSE(called);
  EXPECT_EQ(cache.stats().peak_bytes, 0u);
  EXPECT_EQ(cache.stats().producer_count, 0u);
}

TEST(DecodedFragmentCacheTest, PinnedEvictedLeaseBlocksAllocationUntilReleased) {
  auto limits = config(); limits.memory_budget_bytes = 100; limits.wait_timeout_ms = 30;
  DecodedFragmentCache cache(limits);
  auto result = cache.get("a", {80, 0}, [](const auto&) { return loaded(80); });
  ASSERT_TRUE(result.ok());
  auto pinned = result.value().shared();
  result = QueryResult<FragmentLease>::success({});
  cache.evict_all();
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
  EXPECT_EQ(cache.stats().pinned_bytes, 80u);
  EXPECT_EQ(cache.stats().live_bytes, 80u);
  bool called = false;
  auto blocked = cache.get("b", {32, 0}, [&](const auto&) { called = true; return loaded(32); });
  EXPECT_EQ(blocked.status().code(), QueryError::ResourceExhausted);
  EXPECT_FALSE(called);
  pinned.reset();
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
  auto retry = cache.get("b", {32, 0}, [&](const auto&) { called = true; return loaded(32); });
  ASSERT_TRUE(retry.ok()) << retry.status().message();
  EXPECT_TRUE(called);
  EXPECT_LE(cache.stats().peak_bytes, limits.memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, LeaseAndSharedOwnerOutliveRuntime) {
  auto destroyed = std::make_shared<std::atomic<int>>(0);
  FragmentLease lease;
  std::shared_ptr<const DecodedFragment> alias;
  {
    QueryRuntime runtime(config());
    auto result = runtime.cache().get("a", {64, 0}, [&](const auto&) {
      return Loaded::success(std::make_shared<TestFragment>(64, 17, destroyed));
    });
    ASSERT_TRUE(result.ok());
    lease = result.value();
    alias = lease.shared();
    runtime.cache().evict_all();
    EXPECT_EQ(runtime.stats().live_bytes, 64u);
    EXPECT_EQ(runtime.stats().pinned_bytes, 64u);
  }
  EXPECT_EQ(*destroyed, 0);
  EXPECT_EQ(lease.as<TestFragment>()->value, 17);
  lease = {};
  EXPECT_EQ(*destroyed, 0);
  alias.reset();
  EXPECT_EQ(*destroyed, 1);
}

TEST(DecodedFragmentCacheTest, CacheHitAndEvictionRedecodeMetricsTrackActualWork) {
  auto limits = config(); limits.max_entries = 1;
  DecodedFragmentCache cache(limits);
  int calls = 0;
  auto loader = [&](const FragmentLoadContext& context) {
    ++calls;
    auto status = context.record_io(20, 2);
    if (!status.ok()) return Loaded::failure(status);
    context.record_decode(64, 1);
    context.record_copy(16);
    return loaded(64);
  };
  {
    auto first = cache.get("a", {64, 16}, loader); ASSERT_TRUE(first.ok());
    auto hit = cache.get("a", {64, 16}, loader); ASSERT_TRUE(hit.ok());
    EXPECT_EQ(first.value().shared().get(), hit.value().shared().get());
  }
  { auto second = cache.get("b", {64, 16}, loader); ASSERT_TRUE(second.ok()); }
  { auto third = cache.get("a", {64, 16}, loader); ASSERT_TRUE(third.ok()); }
  auto stats = cache.stats();
  EXPECT_EQ(calls, 3);
  EXPECT_GE(stats.hits, 1u);
  EXPECT_EQ(stats.producer_count, 3u);
  EXPECT_EQ(stats.decode_count, 3u);
  EXPECT_EQ(stats.read_bytes, 60u);
  EXPECT_EQ(stats.read_ranges, 6u);
  EXPECT_EQ(stats.decoded_bytes, 192u);
  EXPECT_EQ(stats.decompressions, 3u);
  EXPECT_EQ(stats.copy_bytes, 48u);
  EXPECT_GE(stats.evictions, 2u);
  EXPECT_GE(stats.redecodes, 1u);
}

TEST(DecodedFragmentCacheTest, ServiceReadByteLimitIsStickyEvenIfLoaderIgnoresFailure) {
  auto limits = config(); limits.max_read_bytes_per_load = 10;
  DecodedFragmentCache cache(limits);
  auto result = cache.get("a", {64, 0}, [](const FragmentLoadContext& context) {
    EXPECT_TRUE(context.record_io(8).ok());
    EXPECT_EQ(context.record_io(3).code(), QueryError::ResourceExhausted);
    return loaded(64);
  });
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(cache.stats().read_bytes, 8u);
  EXPECT_EQ(cache.stats().read_ranges, 1u);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
}

TEST(DecodedFragmentCacheTest, WallTimingsIncludeFailedLoadsAndArePublishedBeforeGetReturns) {
  DecodedFragmentCache cache(config());
  auto first = cache.get("timed", {64, 0}, [](const FragmentLoadContext& context) {
    EXPECT_TRUE(context.record_io(20, 2).ok());
    // Reserving two ranges does not claim either source read has happened.
    context.begin_payload_read();
    context.record_payload_read(20, 2, 1234);
    std::this_thread::sleep_for(1ms);
    return loaded(64);
  });
  ASSERT_TRUE(first.ok());
  const auto completed = cache.stats();
  EXPECT_EQ(completed.read_ranges, 2u);
  EXPECT_EQ(completed.payload_read_calls, 1u);
  EXPECT_EQ(completed.payload_read_bytes, 20u);
  EXPECT_EQ(completed.payload_read_ranges, 2u);
  EXPECT_EQ(completed.payload_read_nanoseconds, 1234u);
  EXPECT_EQ(completed.payload_read_shape[5].calls, 1u);
  EXPECT_EQ(completed.payload_read_shape[5].bytes, 20u);
  EXPECT_EQ(completed.payload_read_shape[5].ranges, 2u);
  EXPECT_EQ(completed.payload_read_shape[5].nanoseconds, 1234u);
  EXPECT_GE(completed.fragment_load_nanoseconds, 1000000u);
  auto hit = cache.get("timed", {64, 0}, [](const auto&) { return loaded(64); });
  ASSERT_TRUE(hit.ok());
  EXPECT_EQ(cache.stats().fragment_load_nanoseconds, completed.fragment_load_nanoseconds);
  auto thrown = cache.get("throws", {64, 0}, [](const FragmentLoadContext& context) -> Loaded {
    context.begin_payload_read();
    context.record_payload_read(1048577, 33, 5678);
    std::this_thread::sleep_for(1ms);
    throw std::runtime_error("injected failure");
  });
  EXPECT_EQ(thrown.status().code(), QueryError::Internal);
  const auto failed_stats = cache.stats();
  EXPECT_EQ(failed_stats.payload_read_calls, 2u);
  EXPECT_EQ(failed_stats.payload_read_bytes, 20u + 1048577u);
  EXPECT_EQ(failed_stats.payload_read_ranges, 35u);
  EXPECT_EQ(failed_stats.payload_read_nanoseconds, 1234u + 5678u);
  EXPECT_EQ(failed_stats.payload_read_shape[24].calls, 1u);
  EXPECT_EQ(failed_stats.payload_read_shape[24].bytes, 1048577u);
  EXPECT_EQ(failed_stats.payload_reads_active, 0u);
  EXPECT_EQ(failed_stats.payload_reads_peak, 1u);
  EXPECT_GE(failed_stats.fragment_load_nanoseconds, completed.fragment_load_nanoseconds + 1000000u);
}

TEST(DecodedFragmentCacheTest, PayloadConcurrencyGaugeMeasuresOverlappingSourceReads) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  auto loader = [&](const FragmentLoadContext& context) {
    context.begin_payload_read();
    auto result = gate.run(context);
    context.record_payload_read(64, 1, 123);
    return result;
  };
  auto first = std::async(std::launch::async, [&] {
    return cache.get("read-1", {64, 0}, loader);
  });
  auto second = std::async(std::launch::async, [&] {
    return cache.get("read-2", {64, 0}, loader);
  });
  const auto both_started = gate.started(2);
  const auto active = cache.stats();
  gate.open();
  ASSERT_TRUE(first.get().ok());
  ASSERT_TRUE(second.get().ok());
  ASSERT_TRUE(both_started);
  EXPECT_EQ(active.payload_reads_active, 2u);
  EXPECT_EQ(active.payload_reads_peak, 2u);
  const auto completed = cache.stats();
  EXPECT_EQ(completed.payload_reads_active, 0u);
  EXPECT_EQ(completed.payload_reads_peak, 2u);
  EXPECT_EQ(completed.payload_read_calls, 2u);
}

TEST(DecodedFragmentCacheTest, ServiceReadRangeLimitIsIndependentOfByteLimit) {
  auto limits = config(); limits.max_read_ranges_per_load = 1;
  DecodedFragmentCache cache(limits);
  auto result = cache.get("a", {64, 0}, [](const FragmentLoadContext& context) {
    auto first = context.record_io(1);
    if (!first.ok()) return Loaded::failure(first);
    return Loaded::failure(context.record_io(1));
  });
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(cache.stats().read_bytes, 1u);
  EXPECT_EQ(cache.stats().read_ranges, 1u);
}

TEST(DecodedFragmentCacheTest, LoaderCannotPublishMoreResidentBytesThanReserved) {
  DecodedFragmentCache cache(config());
  auto result = cache.get("a", {32, 0}, [](const auto&) { return loaded(64); });
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
}

TEST(DecodedFragmentCacheTest, EntryLimitReservesProducerSlotsBeforeDecode) {
  auto limits = config(); limits.max_entries = 1; limits.worker_threads = 2;
  limits.wait_timeout_ms = 100;
  DecodedFragmentCache cache(limits);
  LoadGate first_gate;
  std::atomic<bool> second_entered{false};
  auto first = std::async(std::launch::async, [&] {
    return cache.get("first", {64, 16}, [&](const auto& context) { return first_gate.run(context); });
  });
  EXPECT_TRUE(first_gate.started());
  auto second = cache.get("second", {64, 16}, [&](const auto&) {
    second_entered = true;
    return loaded(64, 99);
  });
  // The first producer retains its reservation while blocked, including after
  // its waiter times out. A second decode must not run before that producer exits.
  EXPECT_FALSE(second.ok());
  EXPECT_EQ(second.status().code(), QueryError::ResourceExhausted);
  EXPECT_FALSE(second_entered);
  first_gate.open();
  first.get();
  EXPECT_TRUE(eventually([&] { return cache.stats().inflight_bytes == 0; }));
  auto retry = cache.get("second", {64, 16}, [](const auto&) { return loaded(64, 99); });
  ASSERT_TRUE(retry.ok()) << retry.status().message();
  EXPECT_EQ(retry.value().as<TestFragment>()->value, 99);
  EXPECT_LE(cache.stats().cache_owned_bytes, 64u);
}

TEST(DecodedFragmentCacheTest, InflightLimitPreventsConcurrentDistinctProducerAdmission) {
  auto limits = config(); limits.max_inflight = 1; limits.worker_threads = 2;
  limits.enable_detailed_profile = true;
  DecodedFragmentCache cache(limits);
  LoadGate first_gate;
  std::atomic<bool> second_entered{false};
  auto first = std::async(std::launch::async, [&] {
    return cache.get("first", {64, 16}, [&](const auto& context) { return first_gate.run(context); });
  });
  EXPECT_TRUE(first_gate.started());
  const auto initial_waits = cache.stats().waits;
  auto second = std::async(std::launch::async, [&] {
    return cache.get("second", {64, 16}, [&](const auto&) { second_entered = true; return loaded(64); });
  });
  EXPECT_TRUE(eventually([&] { return cache.stats().waits > initial_waits; }));
  EXPECT_FALSE(second_entered);
  first_gate.open();
  auto first_result = first.get(); auto second_result = second.get();
  ASSERT_TRUE(first_result.ok()); ASSERT_TRUE(second_result.ok());
  EXPECT_TRUE(second_entered);
  EXPECT_GT(cache.stats().fragment_admission_nanoseconds, 0u);
  EXPECT_GT(cache.stats().fragment_queue_nanoseconds, 0u);
  EXPECT_EQ(cache.stats().producer_count, 2u);
}

class DependentFragment final : public DecodedFragment {
public:
  explicit DependentFragment(std::shared_ptr<const DecodedFragment> dependency)
    : dependency_(std::move(dependency)) {}
  std::uint64_t resident_bytes() const override { return 32; }
private:
  std::shared_ptr<const DecodedFragment> dependency_;
};

TEST(DecodedFragmentCacheTest, ReleasingDependencyLeasesDuringEvictionDoesNotHoldCacheLock) {
  DecodedFragmentCache cache(config());
  auto base = cache.get("base", {64, 0}, [](const auto&) { return loaded(64); });
  ASSERT_TRUE(base.ok());
  auto dependency = base.value().shared();
  auto dependent = cache.get("dependent", {32, 0}, [dependency](const auto&) {
    return Loaded::success(std::make_shared<DependentFragment>(dependency));
  });
  ASSERT_TRUE(dependent.ok());
  dependency.reset();
  base = QueryResult<FragmentLease>::success({});
  dependent = QueryResult<FragmentLease>::success({});
  cache.evict_all();
  EXPECT_EQ(cache.stats().live_bytes, 0u);
  EXPECT_EQ(cache.stats().pinned_bytes, 0u);
}

TEST(DecodedFragmentCacheTest, LastPinnedDependentLeaseReleasesItsPinnedDependency) {
  DecodedFragmentCache cache(config());
  auto base = cache.get("base", {64, 0}, [](const auto&) { return loaded(64); });
  ASSERT_TRUE(base.ok());
  auto dependency = base.value().shared();
  auto dependent = cache.get("dependent", {32, 0}, [dependency](const auto&) {
    return Loaded::success(std::make_shared<DependentFragment>(dependency));
  });
  ASSERT_TRUE(dependent.ok());
  dependency.reset();
  base = QueryResult<FragmentLease>::success({});
  cache.evict_all();
  EXPECT_EQ(cache.stats().live_bytes, 96u);
  EXPECT_EQ(cache.stats().pinned_bytes, 96u);
  dependent = QueryResult<FragmentLease>::success({});
  EXPECT_EQ(cache.stats().live_bytes, 0u);
  EXPECT_EQ(cache.stats().pinned_bytes, 0u);
}

} // namespace

TEST(DecodedFragmentCacheTest, DetailedTimingIsOptionalAndPropagatesFromCallerScope) {
  DecodedFragmentCache cache(config());
  auto disabled = cache.get("off", {64, 0}, [](const FragmentLoadContext& context) {
    EXPECT_FALSE(context.detailed_profile_enabled());
    context.record_decode_stages({11, 13, 17, 19});
    return loaded(64);
  });
  ASSERT_TRUE(disabled.ok());
  EXPECT_EQ(cache.stats().decode_filter_nanoseconds, 0u);
  QueryProfileRecord profile;
  {
    ScopedQueryProfile scope(&profile);
    auto enabled = cache.get_batch({{"on", {64, 0}}}, 0,
      [](const std::vector<std::size_t>&, const FragmentLoadContext& context) {
        EXPECT_TRUE(context.detailed_profile_enabled());
        context.record_decode_stages({11, 13, 17, 19});
        using R = QueryResult<std::vector<std::shared_ptr<const DecodedFragment>>>;
        return R::success({loaded(64).value()});
      });
    ASSERT_TRUE(enabled.ok());
  }
  auto stats = cache.stats();
  EXPECT_EQ(stats.decode_inflate_nanoseconds, 11u);
  EXPECT_EQ(stats.decode_shuffle_nanoseconds, 13u);
  EXPECT_EQ(stats.decode_checksum_nanoseconds, 17u);
  EXPECT_EQ(stats.decode_filter_nanoseconds, 41u);
  EXPECT_EQ(stats.decode_conversion_nanoseconds, 19u);
  EXPECT_GT(stats.fragment_queue_nanoseconds, 0u);
  // Joining a completed key does not repeat any producer-stage observation.
  ASSERT_TRUE(cache.get("on", {64, 0}, [](const auto&) { return loaded(64); }).ok());
  EXPECT_EQ(cache.stats().decode_filter_nanoseconds, 41u);
}

TEST(DecodedFragmentCacheTest, SharedProducerReplaysUniqueFeesAgainstEachWaitersOwnLedger) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  std::atomic<int> calls{0};
  auto loader = [&](const FragmentLoadContext& context) {
    ++calls;
    EXPECT_FALSE(current_query_metadata_budget());
    auto result = gate.run(context);
    require_query_metadata_unique(2);
    return result;
  };
  auto tight = std::async(std::launch::async, [&] {
    ScopedQueryMetadataBudget request;
    require_query_metadata_unique(QueryMetadataBudget::maximum_bytes - 1);
    auto result = cache.get("shared-metadata", {64, 0}, loader);
    EXPECT_FALSE(request.status().ok());
    EXPECT_THROW(require_query_metadata_unique(0), QueryMetadataBudgetExceeded);
    EXPECT_EQ(request.budget()->consumed_bytes(), QueryMetadataBudget::maximum_bytes - 1);
    return result.status().code();
  });
  const bool started = gate.started();
  auto fresh = std::async(std::launch::async, [&] {
    ScopedQueryMetadataBudget request;
    auto result = cache.get("shared-metadata", {64, 0}, loader);
    EXPECT_TRUE(request.status().ok());
    EXPECT_EQ(request.budget()->consumed_bytes(), 2u);
    return result;
  });
  const bool joined = eventually([&] { return cache.stats().waits >= 1; });
  gate.open();
  EXPECT_TRUE(started);
  EXPECT_TRUE(joined);
  EXPECT_EQ(tight.get(), QueryError::ResourceExhausted);
  auto result = fresh.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(cache.stats().producer_count, 1u);
}

TEST(DecodedFragmentCacheTest, IgnoredOversizedUniqueProducerDemandStillRejectsEveryWaiter) {
  DecodedFragmentCache cache(config());
  std::atomic<int> calls{0};
  auto loader = [&](const auto&) {
    ++calls;
    EXPECT_FALSE(current_query_metadata_budget());
    // A loader may catch a budget exception or ignore its demand's outcome.
    // Returning a valid allocation cannot make that demand disappear.
    try { require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1); }
    catch (const QueryMetadataBudgetExceeded&) {}
    return loaded(64);
  };
  for (int i = 0; i < 2; ++i) {
    ScopedQueryMetadataBudget request;
    auto result = cache.get("oversized-metadata", {64, 0}, loader);
    EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
    EXPECT_EQ(request.status().code(), QueryError::ResourceExhausted);
    EXPECT_THROW(require_query_metadata_unique(0), QueryMetadataBudgetExceeded);
  }
  EXPECT_EQ(calls, 1);
}

TEST(DecodedFragmentCacheTest, HotMetadataTraceIsIdempotentAndNeverRetainsRequestLedger) {
  DecodedFragmentCache cache(config());
  std::atomic<int> calls{0};
  auto loader = [&](const auto&) {
    ++calls;
    EXPECT_FALSE(current_query_metadata_budget());
    require_query_metadata("source/a@7/layout", 5);
    require_query_metadata_unique(3);
    return loaded(64);
  };
  std::weak_ptr<QueryMetadataBudget> previous_request;
  for (int request_index = 0; request_index < 2; ++request_index) {
    {
      ScopedQueryMetadataBudget request;
      previous_request = request.budget();
      for (int hit = 0; hit < 2; ++hit) {
        auto result = cache.get("hot-metadata", {64, 0}, loader);
        ASSERT_TRUE(result.ok()) << result.status().message();
        EXPECT_EQ(request.budget()->consumed_bytes(), 8u);
      }
    }
    EXPECT_TRUE(previous_request.expired());
  }
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(cache.stats().hits, 3u);
}

TEST(DecodedFragmentCacheTest, BatchMembersReplayOneSharedTraceWithoutMultiplyingLogicalFees) {
  DecodedFragmentCache cache(config());
  const std::vector<FragmentBatchItem> items{{"metadata-a", {32, 0}}, {"metadata-b", {32, 0}}};
  std::atomic<int> calls{0};
  {
    ScopedQueryMetadataBudget request;
    auto result = cache.get_batch(items, 0, [&](const auto& indices, const auto&) {
      ++calls;
      EXPECT_FALSE(current_query_metadata_budget());
      require_query_metadata("source/batch@9/layout", 5);
      require_query_metadata_unique(3);
      std::vector<std::shared_ptr<const DecodedFragment>> fragments;
      for (auto index : indices) fragments.push_back(std::make_shared<TestFragment>(32, index));
      return BatchLoaded::success(std::move(fragments));
    });
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(request.budget()->consumed_bytes(), 8u);
  }
  {
    ScopedQueryMetadataBudget next_request;
    for (const auto& item : items) {
      auto result = cache.get(item.key, item.memory, [](const auto&) {
        ADD_FAILURE() << "batch member must remain cached";
        return loaded(32);
      });
      ASSERT_TRUE(result.ok()) << result.status().message();
      EXPECT_EQ(next_request.budget()->consumed_bytes(), 8u);
    }
  }
  EXPECT_EQ(calls, 1);
}

TEST(DecodedFragmentCacheTest, MetadataTraceMemoryRemainsChargedUntilLastEvictedLeaseReleases) {
  const std::string identity = "source/memory@11/layout";
  std::uint64_t trace_bytes;
  {
    ScopedQueryMetadataTrace trace("");
    require_query_metadata(identity, 2);
    trace_bytes = trace.estimated_bytes();
  }
  ASSERT_GT(trace_bytes, 0u);
  auto cfg = config();
  cfg.memory_budget_bytes = 4096;
  DecodedFragmentCache cache(cfg);
  auto result = cache.get("trace-memory", {64, 0}, [&](const auto&) {
    require_query_metadata(identity, 2);
    return loaded(64);
  });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(cache.stats().live_bytes, 64u + trace_bytes);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 64u + trace_bytes);
  EXPECT_EQ(cache.stats().pinned_bytes, 64u + trace_bytes);
  cache.evict_all();
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
  EXPECT_EQ(cache.stats().live_bytes, 64u + trace_bytes);
  result = QueryResult<FragmentLease>::success({});
  EXPECT_EQ(cache.stats().live_bytes, 0u);
  EXPECT_EQ(cache.stats().pinned_bytes, 0u);
  EXPECT_EQ(cache.stats().inflight_bytes, 0u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
}

TEST(DecodedFragmentCacheTest, MetadataTraceCannotBypassRuntimeBudgetEvenIfLoaderCatchesFailure) {
  const std::string identity = "source/bounded@13/layout";
  std::uint64_t trace_bytes;
  {
    ScopedQueryMetadataTrace trace("");
    require_query_metadata(identity, 2);
    trace_bytes = trace.estimated_bytes();
  }
  auto cfg = config();
  cfg.memory_budget_bytes = 64 + trace_bytes - 1;
  DecodedFragmentCache cache(cfg);
  auto result = cache.get("bounded-trace", {64, 0}, [&](const auto&) {
    try { require_query_metadata(identity, 2); }
    catch (const QueryMetadataBudgetExceeded&) {}
    return loaded(64);
  });
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
  EXPECT_EQ(cache.stats().live_bytes, 0u);
  EXPECT_EQ(cache.stats().inflight_bytes, 0u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
  EXPECT_LE(cache.stats().peak_bytes, cfg.memory_budget_bytes);
  auto retry = cache.get("bounded-trace", {64, 0}, [](const auto&) { return loaded(64); });
  ASSERT_TRUE(retry.ok()) << retry.status().message();
  EXPECT_EQ(cache.stats().live_bytes, 64u);
  retry = QueryResult<FragmentLease>::success({});
  cache.evict_all();
  EXPECT_EQ(cache.stats().live_bytes, 0u);
}

namespace {
class CountingExecutionLease final : public QueryExecutionLease {
public:
  explicit CountingExecutionLease(std::function<void()> release) : release_(std::move(release)) {}
  ~CountingExecutionLease() override { release_(); }
private:
  std::function<void()> release_;
};
} // namespace

TEST(DecodedFragmentCacheTest, CancelledWaiterKeepsProducerLeaseUntilLoaderCapturesRelease) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken cancelled;
  std::weak_ptr<QueryExecutionLease> lease;
  std::weak_ptr<int> loader_capture;
  std::atomic<unsigned> released{0};
  auto waiter = std::async(std::launch::async, [&] {
    auto token = std::make_shared<CountingExecutionLease>([&] {
      // A destructor may release other cache-backed resources. This reentry
      // would deadlock if the producer dropped its lease under cache mutex.
      (void)cache.stats();
      EXPECT_TRUE(loader_capture.expired());
      ++released;
    });
    lease = token;
    ScopedQueryExecutionLease scope(std::move(token));
    auto capture = std::make_shared<int>(7);
    loader_capture = capture;
    DecodedFragmentCache::Loader loader = [capture, &gate](const auto& context) {
      return gate.run(context);
    };
    capture.reset();
    return cache.get("cancelled-execution-lease", {64, 0}, std::move(loader), cancelled);
  });
  const bool started = gate.started();
  cancelled.cancel();
  const auto state = waiter.wait_for(1s);
  if (state != std::future_status::ready) gate.open();
  auto result = waiter.get();
  EXPECT_TRUE(started);
  EXPECT_EQ(state, std::future_status::ready);
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_FALSE(lease.expired());
  EXPECT_FALSE(loader_capture.expired());
  EXPECT_EQ(released, 0u);
  gate.open();
  EXPECT_TRUE(eventually([&] { return released.load() == 1; }));
  EXPECT_TRUE(lease.expired());
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
}

TEST(DecodedFragmentCacheTest, TimedOutWaiterCannotUnpinUnfinishedProducer) {
  auto cfg = config();
  cfg.wait_timeout_ms = 40;
  DecodedFragmentCache cache(cfg);
  LoadGate gate;
  std::weak_ptr<QueryExecutionLease> lease;
  std::atomic<unsigned> released{0};
  auto waiter = std::async(std::launch::async, [&] {
    auto token = std::make_shared<CountingExecutionLease>([&] { (void)cache.stats(); ++released; });
    lease = token;
    ScopedQueryExecutionLease scope(std::move(token));
    return cache.get("timeout-execution-lease", {64, 0}, [&](const auto& context) {
      return gate.run(context);
    });
  });
  const bool started = gate.started();
  const auto state = waiter.wait_for(1s);
  if (state != std::future_status::ready) gate.open();
  auto result = waiter.get();
  EXPECT_TRUE(started);
  EXPECT_EQ(state, std::future_status::ready);
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_FALSE(lease.expired());
  EXPECT_EQ(released, 0u);
  gate.open();
  EXPECT_TRUE(eventually([&] { return released.load() == 1; }));
  EXPECT_TRUE(lease.expired());
}

TEST(DecodedFragmentCacheTest, CancellingInitiatorPreservesJoinedProducerLeaseButCacheDoesNotRetainIt) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken cancelled;
  std::weak_ptr<QueryExecutionLease> lease;
  std::atomic<unsigned> released{0}, calls{0};
  auto loader = [&](const auto& context) { ++calls; return gate.run(context); };
  auto initial = std::async(std::launch::async, [&] {
    auto token = std::make_shared<CountingExecutionLease>([&] { (void)cache.stats(); ++released; });
    lease = token;
    ScopedQueryExecutionLease scope(std::move(token));
    return cache.get("joined-execution-lease", {64, 0}, loader, cancelled);
  });
  const bool started = gate.started();
  auto survivor = std::async(std::launch::async, [&] {
    return cache.get("joined-execution-lease", {64, 0}, loader);
  });
  const bool joined = eventually([&] { return cache.stats().waits >= 1; });
  cancelled.cancel();
  const auto state = initial.wait_for(1s);
  if (state != std::future_status::ready) gate.open();
  const auto first = initial.get();
  EXPECT_TRUE(started);
  EXPECT_TRUE(joined);
  EXPECT_EQ(state, std::future_status::ready);
  EXPECT_EQ(first.status().code(), QueryError::Cancelled);
  EXPECT_FALSE(lease.expired());
  gate.open();
  auto result = survivor.get();
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_FALSE(gate.observed_cancel());
  EXPECT_TRUE(eventually([&] { return released.load() == 1; }));
  EXPECT_TRUE(lease.expired());
  auto cached = cache.get("joined-execution-lease", {64, 0}, loader);
  ASSERT_TRUE(cached.ok());
  EXPECT_EQ(cached.value().shared().get(), result.value().shared().get());
  EXPECT_EQ(calls, 1u);
  EXPECT_EQ(released, 1u); // Both leases and the cache still hold fragment bytes.
}

TEST(DecodedFragmentCacheTest, StoppingCacheReleasesCancelledQueuedJobExecutionLease) {
  auto cfg = config();
  cfg.worker_threads = 1;
  auto cache = std::make_unique<DecodedFragmentCache>(cfg);
  DecodedFragmentCache other_cache(config());
  auto* runtime = cache.get();
  LoadGate gate;
  CancellationToken first_cancel, queued_cancel;
  std::weak_ptr<QueryExecutionLease> lease;
  std::atomic<unsigned> released{0}, queued_calls{0};
  auto running = std::async(std::launch::async, [&] {
    return runtime->get("running-before-stop", {64, 0}, [&](const auto& context) {
      return gate.run(context);
    }, first_cancel);
  });
  const bool started = gate.started();
  auto queued = std::async(std::launch::async, [&] {
    // The stopping runtime is already being destroyed; calling its methods
    // from a member destructor would access an object past its API lifetime.
    auto token = std::make_shared<CountingExecutionLease>([&] { (void)other_cache.stats(); ++released; });
    lease = token;
    ScopedQueryExecutionLease scope(std::move(token));
    return runtime->get("queued-before-stop", {64, 0}, [&](const auto&) {
      ++queued_calls;
      return loaded(64);
    }, queued_cancel);
  });
  const bool enqueued = eventually([&] { return runtime->stats().producer_count == 2; });
  queued_cancel.cancel();
  first_cancel.cancel();
  const auto queued_state = queued.wait_for(1s);
  const auto running_state = running.wait_for(1s);
  if (queued_state != std::future_status::ready || running_state != std::future_status::ready) gate.open();
  const auto queued_result = queued.get();
  const auto running_result = running.get();
  EXPECT_TRUE(started);
  EXPECT_TRUE(enqueued);
  EXPECT_EQ(queued_result.status().code(), QueryError::Cancelled);
  EXPECT_EQ(running_result.status().code(), QueryError::Cancelled);
  EXPECT_FALSE(lease.expired());
  std::promise<void> stopping;
  auto stop_started = stopping.get_future();
  auto stopped = std::async(std::launch::async, [&] { stopping.set_value(); cache.reset(); });
  const auto stop_attempt = stop_started.wait_for(1s);
  const auto blocked = stopped.wait_for(20ms);
  gate.open();
  stopped.get();
  EXPECT_EQ(stop_attempt, std::future_status::ready);
  EXPECT_EQ(blocked, std::future_status::timeout);
  EXPECT_EQ(queued_calls, 0u);
  EXPECT_EQ(released, 1u);
  EXPECT_TRUE(lease.expired());
}

TEST(DecodedFragmentCacheTest, CancelledQueuedJobReleasesExecutionLeaseOutsideCacheMutex) {
  auto cfg = config();
  cfg.worker_threads = 1;
  DecodedFragmentCache cache(cfg);
  LoadGate gate;
  CancellationToken first_cancel, queued_cancel;
  std::weak_ptr<QueryExecutionLease> lease;
  std::atomic<unsigned> released{0}, queued_calls{0};
  auto running = std::async(std::launch::async, [&] {
    return cache.get("running-before-queued-cleanup", {64, 0}, [&](const auto& context) {
      return gate.run(context);
    }, first_cancel);
  });
  const bool started = gate.started();
  auto queued = std::async(std::launch::async, [&] {
    auto token = std::make_shared<CountingExecutionLease>([&] { (void)cache.stats(); ++released; });
    lease = token;
    ScopedQueryExecutionLease scope(std::move(token));
    return cache.get("cancelled-queued-cleanup", {64, 0}, [&](const auto&) {
      ++queued_calls;
      return loaded(64);
    }, queued_cancel);
  });
  const bool enqueued = eventually([&] { return cache.stats().producer_count == 2; });
  queued_cancel.cancel();
  first_cancel.cancel();
  const auto queued_state = queued.wait_for(1s);
  const auto running_state = running.wait_for(1s);
  if (queued_state != std::future_status::ready || running_state != std::future_status::ready) gate.open();
  const auto queued_result = queued.get();
  const auto running_result = running.get();
  EXPECT_TRUE(started);
  EXPECT_TRUE(enqueued);
  EXPECT_EQ(queued_result.status().code(), QueryError::Cancelled);
  EXPECT_EQ(running_result.status().code(), QueryError::Cancelled);
  EXPECT_FALSE(lease.expired());
  gate.open();
  EXPECT_TRUE(eventually([&] { return released.load() == 1; }));
  EXPECT_TRUE(lease.expired());
  EXPECT_EQ(queued_calls, 0u);
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
}

TEST(DecodedFragmentCacheTest, ThrowingAbandonedLoaderReleasesExecutionLeaseOutsideCacheMutex) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken cancelled;
  std::weak_ptr<QueryExecutionLease> lease;
  std::atomic<unsigned> released{0};
  auto waiter = std::async(std::launch::async, [&] {
    auto token = std::make_shared<CountingExecutionLease>([&] { (void)cache.stats(); ++released; });
    lease = token;
    ScopedQueryExecutionLease scope(std::move(token));
    return cache.get("throwing-execution-lease", {64, 0}, [&](const auto& context) -> Loaded {
      (void)gate.run(context);
      throw std::runtime_error("abandoned producer failed");
    }, cancelled);
  });
  const bool started = gate.started();
  cancelled.cancel();
  const auto state = waiter.wait_for(1s);
  if (state != std::future_status::ready) gate.open();
  const auto result = waiter.get();
  EXPECT_TRUE(started);
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_FALSE(lease.expired());
  gate.open();
  EXPECT_TRUE(eventually([&] { return released.load() == 1; }));
  EXPECT_TRUE(lease.expired());
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
}

TEST(DecodedFragmentCacheTest, StorageTemporaryReservationsChargeAndReleaseRuntimeGauges) {
  DecodedFragmentCache cache(config());
  auto result = cache.get("storage-temporary", {64, 32}, [&](const auto&) {
    EXPECT_NE(active_storage_read_control, nullptr);
    const auto before = cache.stats();
    EXPECT_EQ(before.live_bytes, 96u);
    auto reservation = reserve_storage_read_temporary(100);
    EXPECT_TRUE(reservation);
    EXPECT_EQ(cache.stats().live_bytes, 196u);
    EXPECT_EQ(cache.stats().temporary_bytes, 132u);
    reservation.reset();
    EXPECT_EQ(cache.stats().live_bytes, 96u);
    EXPECT_EQ(cache.stats().temporary_bytes, 32u);
    return loaded(64);
  });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(cache.stats().live_bytes, 64u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
  EXPECT_EQ(cache.stats().peak_bytes, 196u);
}

TEST(DecodedFragmentCacheTest, IgnoredStorageTemporaryExhaustionStillRejectsProducerWithoutUnderflow) {
  DecodedFragmentCache cache(config());
  auto result = cache.get("storage-temporary-overflow", {64, 0}, [&](const auto&) {
    try { (void)reserve_storage_read_temporary(1025); }
    catch (const QueryStorageReadInterrupted& error) {
      EXPECT_EQ(error.status().code(), QueryError::ResourceExhausted);
    }
    return loaded(64);
  });
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(cache.stats().live_bytes, 0u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
  EXPECT_LE(cache.stats().peak_bytes, config().memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, OptionalStorageReuseYieldsBeforeTemporaryAdmissionWithoutPoisoningProducer) {
  DecodedFragmentCache cache(config());
  unsigned reclaimed=0;
  auto result=cache.get("optional-storage-pressure",{64,0},[&](const auto&) {
    auto retained=reserve_storage_read_temporary(700);
    struct State { std::shared_ptr<void>* pin; unsigned* calls; } state{&retained,&reclaimed};
    ScopedStorageReadOptionalReclaim reclaim([](void* value) noexcept {
      auto& s=*static_cast<State*>(value); ++*s.calls; s.pin->reset();
    },&state);
    auto next=reserve_storage_read_temporary(500);
    EXPECT_FALSE(retained);
    EXPECT_TRUE(next);
    EXPECT_EQ(cache.stats().live_bytes,564u);
    return loaded(64);
  });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(reclaimed,1u);
  EXPECT_EQ(cache.stats().temporary_bytes,0u);
  EXPECT_LE(cache.stats().peak_bytes,config().memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, OptionalStorageReuseYieldsBeforeMetadataTraceAdmission) {
  DecodedFragmentCache cache(config());
  unsigned reclaimed=0;
  auto result=cache.get("optional-trace-pressure",{64,0},[&](const auto&) {
    auto retained=reserve_storage_read_temporary(940);
    struct State { std::shared_ptr<void>* pin; unsigned* calls; } state{&retained,&reclaimed};
    ScopedStorageReadOptionalReclaim reclaim([](void* value) noexcept {
      auto& s=*static_cast<State*>(value); ++*s.calls; s.pin->reset();
    },&state);
    require_query_metadata("source/optional-reuse@1/layout",5);
    EXPECT_FALSE(retained);
    return loaded(64);
  });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(reclaimed,1u);
  EXPECT_EQ(cache.stats().temporary_bytes,0u);
  EXPECT_LE(cache.stats().peak_bytes,config().memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, ActualStorageReadsAddToLogicalIoBudgetAndKeepFailureSticky) {
  auto cfg = config(); cfg.max_read_bytes_per_load = 10;
  DecodedFragmentCache cache(cfg);
  auto result = cache.get("storage-actual-io", {64, 0}, [&](const FragmentLoadContext& context) {
    EXPECT_TRUE(context.record_io(4, 1).ok());
    try { record_storage_read_io(7, 1); }
    catch (const QueryStorageReadInterrupted& error) {
      EXPECT_EQ(error.status().code(), QueryError::ResourceExhausted);
    }
    return loaded(64);
  });
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(cache.stats().live_bytes, 0u);
  EXPECT_EQ(cache.stats().cache_owned_bytes, 0u);
}

TEST(DecodedFragmentCacheTest, StorageCheckpointObservesAbandonedProducerCancellation) {
  DecodedFragmentCache cache(config());
  LoadGate gate;
  CancellationToken token;
  std::atomic<bool> cancelled_checkpoint{false};
  auto request = std::async(std::launch::async, [&] {
    return cache.get("storage-cancel-checkpoint", {64, 0}, [&](const auto& context) -> Loaded {
      (void)gate.run(context);
      try { storage_read_checkpoint(); }
      catch (const QueryStorageReadInterrupted& error) {
        cancelled_checkpoint = error.status().code() == QueryError::Cancelled;
        throw;
      }
      return loaded(64);
    }, token);
  });
  const bool started = gate.started();
  token.cancel();
  const auto state = request.wait_for(1s);
  if (state != std::future_status::ready) gate.open();
  const auto result = request.get();
  EXPECT_TRUE(started);
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  gate.open();
  EXPECT_TRUE(eventually([&] { return cancelled_checkpoint.load(); }));
  EXPECT_TRUE(eventually([&] { return cache.stats().live_bytes == 0; }));
}

TEST(DecodedFragmentCacheTest, StorageInterruptionPreservesItsSpecificErrorInsteadOfInternal) {
  DecodedFragmentCache cache(config());
  auto result = cache.get("storage-interruption", {64, 0}, [](const auto&) -> Loaded {
    throw QueryStorageReadInterrupted(failed(QueryError::StalePlan));
  });
  EXPECT_EQ(result.status().code(), QueryError::StalePlan);
  EXPECT_EQ(cache.stats().live_bytes, 0u);
}

namespace {
class RegisteredOptionalStorageCache final : public StorageReadOptionalCache {
public:
  explicit RegisteredOptionalStorageCache(DecodedFragmentCache& runtime) : runtime_(runtime) {}
  void reclaim() noexcept override {
    // Both operations reenter the runtime mutex. Pressure callbacks must run
    // unlocked, including the initial reservation before a loader has TLS.
    (void)runtime_.stats();
    reservation.reset();
    ++calls;
  }
  std::shared_ptr<void> reservation;
  std::atomic<unsigned> calls{0};
private:
  DecodedFragmentCache& runtime_;
};
}

TEST(DecodedFragmentCacheTest, RegisteredOptionalCacheYieldsBeforeNextLoaderInitialReservation) {
  DecodedFragmentCache cache(config());
  auto optional = std::make_shared<RegisteredOptionalStorageCache>(cache);
  auto first = cache.get("resident-storage-first", {64, 0}, [&](const auto&) {
    optional->reservation = reserve_storage_read_temporary(700);
    EXPECT_TRUE(register_storage_read_optional_cache(optional));
    return loaded(64);
  });
  ASSERT_TRUE(first.ok()) << first.status().message();
  EXPECT_EQ(cache.stats().temporary_bytes, 700u);
  unsigned loaders = 0;
  auto second = cache.get("resident-storage-next", {500, 0}, [&](const auto&) {
    ++loaders;
    EXPECT_FALSE(optional->reservation);
    return loaded(500);
  });
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_EQ(loaders, 1u);
  EXPECT_GE(optional->calls.load(), 1u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
  EXPECT_LE(cache.stats().peak_bytes, config().memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, RegisteredOptionalCacheYieldsToIndependentRequestTemporaryPressure) {
  DecodedFragmentCache cache(config());
  auto optional = std::make_shared<RegisteredOptionalStorageCache>(cache);
  auto first = cache.get("resident-storage-temporary-first", {64, 0}, [&](const auto&) {
    optional->reservation = reserve_storage_read_temporary(700);
    EXPECT_TRUE(register_storage_read_optional_cache(optional));
    return loaded(64);
  });
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto second = cache.get("resident-storage-temporary-next", {64, 0}, [&](const auto&) {
    EXPECT_TRUE(optional->reservation); // Its initial reservation still fits.
    auto next = reserve_storage_read_temporary(500);
    EXPECT_TRUE(next);
    EXPECT_FALSE(optional->reservation);
    return loaded(64);
  });
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_GE(optional->calls.load(), 1u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
  EXPECT_LE(cache.stats().peak_bytes, config().memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, RegisteredOptionalCacheYieldsToIndependentRequestMetadataPressure) {
  DecodedFragmentCache cache(config());
  auto optional = std::make_shared<RegisteredOptionalStorageCache>(cache);
  auto first = cache.get("resident-storage-metadata-first", {64, 0}, [&](const auto&) {
    optional->reservation = reserve_storage_read_temporary(800);
    EXPECT_TRUE(register_storage_read_optional_cache(optional));
    return loaded(64);
  });
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto second = cache.get("resident-storage-metadata-next", {64, 0}, [&](const auto&) {
    EXPECT_TRUE(optional->reservation);
    require_query_metadata(std::string(200, 'm'), 5);
    EXPECT_FALSE(optional->reservation);
    return loaded(64);
  });
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_GE(optional->calls.load(), 1u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
  EXPECT_LE(cache.stats().peak_bytes, config().memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, RegisteredOptionalCacheReclaimDoesNotHideRealExhaustionOrRetryLoader) {
  DecodedFragmentCache cache(config());
  auto optional = std::make_shared<RegisteredOptionalStorageCache>(cache);
  auto first = cache.get("resident-storage-failure-first", {64, 0}, [&](const auto&) {
    optional->reservation = reserve_storage_read_temporary(700);
    EXPECT_TRUE(register_storage_read_optional_cache(optional));
    return loaded(64);
  });
  ASSERT_TRUE(first.ok()) << first.status().message();
  unsigned loaders = 0;
  auto second = cache.get("resident-storage-failure-next", {64, 0}, [&](const auto&) {
    ++loaders;
    try { (void)reserve_storage_read_temporary(2000); }
    catch (const QueryStorageReadInterrupted& error) {
      EXPECT_EQ(error.status().code(), QueryError::ResourceExhausted);
    }
    return loaded(64); // Ignoring the failure must not clear its sticky status.
  });
  EXPECT_EQ(second.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(loaders, 1u);
  EXPECT_FALSE(optional->reservation);
  EXPECT_GE(optional->calls.load(), 1u);
  EXPECT_EQ(cache.stats().temporary_bytes, 0u);
  EXPECT_LE(cache.stats().peak_bytes, config().memory_budget_bytes);
}

TEST(DecodedFragmentCacheTest, OptionalStorageCacheRegistrationIsWeakAndUnavailableOutsideLoader) {
  DecodedFragmentCache cache(config());
  auto optional = std::make_shared<RegisteredOptionalStorageCache>(cache);
  EXPECT_FALSE(register_storage_read_optional_cache(optional));
  auto result = cache.get("resident-storage-weak-registration", {64, 0}, [&](const auto&) {
    EXPECT_TRUE(register_storage_read_optional_cache(optional));
    EXPECT_TRUE(register_storage_read_optional_cache(optional)); // Repeated identity is bounded/deduplicated.
    return loaded(64);
  });
  ASSERT_TRUE(result.ok()) << result.status().message();
  std::weak_ptr<RegisteredOptionalStorageCache> weak = optional;
  optional.reset();
  EXPECT_TRUE(weak.expired());
  auto next = cache.get("resident-storage-after-expired", {700, 0}, [](const auto&) {
    return loaded(700);
  });
  EXPECT_TRUE(next.ok()) << next.status().message();
  EXPECT_LE(cache.stats().peak_bytes, config().memory_budget_bytes);
}
