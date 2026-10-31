#include "iotdbfs/query/core/QueryRuntime.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/storage/versioning/detail/BoundedReaderPool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <thread>

namespace {
using namespace iotdbfs;
using namespace std::chrono_literals;
using Loaded = QueryResult<std::shared_ptr<const DecodedFragment>>;

struct Reader {};
using Pool = versioning::detail::BoundedReaderPool<Reader>;

class Fragment final : public DecodedFragment {
public:
  std::uint64_t resident_bytes() const override { return 64; }
};

Loaded loaded() { return Loaded::success(std::make_shared<const Fragment>()); }
auto factory() { return [] { return std::make_unique<Reader>(); }; }
void checkpoint() { storage_read_checkpoint(); }

QueryRuntimeConfig runtime_config() {
  QueryRuntimeConfig config;
  config.memory_budget_bytes = 1024;
  config.max_entries = 8;
  config.max_inflight = 2;
  config.worker_threads = 2;
  // Both cache waiting and pool waiting have independent finite safeguards.
  config.wait_timeout_ms = 3000;
  return config;
}

template<class Predicate>
bool eventually(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 2s;
  do {
    if (predicate()) return true;
    std::this_thread::sleep_for(1ms);
  } while (std::chrono::steady_clock::now() < deadline);
  return predicate();
}
} // namespace

TEST(ReaderPoolRuntime, CancellingLastWaiterInterruptsProducerInsideSaturatedPool) {
  Pool pool(1, 1500ms);
  QueryRuntime runtime(runtime_config());
  auto held = pool.acquire(factory(), [] {});
  CancellationToken cancellation;
  std::atomic<bool> has_runtime_control{false}, interrupted{false};
  auto request = std::async(std::launch::async, [&] {
    return runtime.cache().get("pool-cancelled", {64, 0},
      [&](const FragmentLoadContext&) -> Loaded {
        has_runtime_control = active_storage_read_control != nullptr;
        try {
          auto reader = pool.acquire(factory(), checkpoint);
          reader.reuse();
          return loaded();
        } catch (const QueryStorageReadInterrupted& error) {
          interrupted = error.status().code() == QueryError::Cancelled;
          // A backend may translate the exception. Runtime cancellation must
          // still win over this ordinary storage failure at publication.
          return Loaded::failure(QueryStatus::failure(QueryError::ReadFailure,
            QueryPhase::Storage, "translated storage interruption"));
        }
      }, cancellation);
  });

  const bool waiting = eventually([&] { return pool.stats().waits == 1; });
  cancellation.cancel();
  const auto ready = request.wait_for(2s);
  // get() remains bounded by the cache's configured timeout if an assertion
  // fails; the fake connection factory and pool wait cannot block forever.
  auto result = request.get();
  EXPECT_TRUE(waiting);
  EXPECT_EQ(ready, std::future_status::ready);
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_TRUE(has_runtime_control.load());
  EXPECT_TRUE(eventually([&] { return interrupted.load(); }));
  EXPECT_TRUE(eventually([&] { return runtime.stats().live_bytes == 0; }));
  EXPECT_EQ(runtime.stats().cache_owned_bytes, 0u);
  EXPECT_EQ(pool.stats().active, 1u); // Only the independent held reader remains.
  EXPECT_EQ(pool.stats().created, 1u);
  EXPECT_EQ(pool.stats().discarded, 0u);
  EXPECT_EQ(pool.stats().acquire_timeouts, 0u);
  held.reuse();
}

TEST(ReaderPoolRuntime, CancellingInitiatorPreservesJoinedProducerWaitingForReader) {
  Pool pool(1, 1500ms);
  QueryRuntime runtime(runtime_config());
  std::optional<Pool::Lease> held(pool.acquire(factory(), [] {}));
  CancellationToken cancellation;
  std::atomic<unsigned> calls{0};
  std::atomic<bool> interrupted{false};
  auto loader = [&](const FragmentLoadContext&) -> Loaded {
    ++calls;
    EXPECT_NE(active_storage_read_control, nullptr);
    try {
      auto reader = pool.acquire(factory(), checkpoint);
      storage_read_checkpoint();
      reader.reuse();
      return loaded();
    } catch (const QueryStorageReadInterrupted&) {
      interrupted = true;
      throw;
    }
  };
  auto initial = std::async(std::launch::async, [&] {
    return runtime.cache().get("pool-shared", {64, 0}, loader, cancellation);
  });
  const bool waiting = eventually([&] { return pool.stats().waits == 1; });
  const auto previous_waits = runtime.stats().waits;
  auto follower = std::async(std::launch::async, [&] {
    return runtime.cache().get("pool-shared", {64, 0}, loader);
  });
  const bool joined = eventually([&] { return runtime.stats().waits > previous_waits; });
  cancellation.cancel();
  const auto initial_ready = initial.wait_for(1s);
  auto cancelled = initial.get();
  EXPECT_EQ(pool.stats().active, 1u);
  EXPECT_FALSE(interrupted.load());
  held->reuse();
  held.reset();
  const auto follower_ready = follower.wait_for(2s);
  auto result = follower.get();

  EXPECT_TRUE(waiting);
  EXPECT_TRUE(joined);
  EXPECT_EQ(initial_ready, std::future_status::ready);
  EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  EXPECT_EQ(follower_ready, std::future_status::ready);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_NE(result.value().as<Fragment>(), nullptr);
  EXPECT_FALSE(interrupted.load());
  EXPECT_EQ(calls.load(), 1u);
  EXPECT_EQ(runtime.stats().producer_count, 1u);
  EXPECT_EQ(pool.stats().created, 1u);
  EXPECT_EQ(pool.stats().active, 0u);
  EXPECT_EQ(pool.stats().discarded, 0u);
  EXPECT_EQ(pool.stats().acquire_timeouts, 0u);
}

TEST(ReaderPoolRuntime, StorageBudgetFailureStaysStickyAndDoesNotPoisonNextProducer) {
  auto config = runtime_config();
  config.max_read_bytes_per_load = 3;
  Pool pool(1, 1500ms);
  QueryRuntime runtime(config);
  std::atomic<unsigned> calls{0};
  bool interrupted = false, checkpoint_sticky = false;
  auto loader = [&](const FragmentLoadContext&) -> Loaded {
    EXPECT_NE(active_storage_read_control, nullptr);
    auto reader = pool.acquire(factory(), checkpoint);
    if (++calls == 1) {
      try { record_storage_read_io(4, 1); }
      catch (const QueryStorageReadInterrupted& error) {
        interrupted = error.status().code() == QueryError::ResourceExhausted;
      }
      try { storage_read_checkpoint(); }
      catch (const QueryStorageReadInterrupted& error) {
        checkpoint_sticky = error.status().code() == QueryError::ResourceExhausted;
      }
      // Even a loader that ignores the error cannot publish its result. This
      // lease is deliberately not reusable after failed storage validation.
      return loaded();
    }
    record_storage_read_io(2, 1);
    storage_read_checkpoint();
    reader.reuse();
    return loaded();
  };

  auto failed = runtime.cache().get("pool-budget", {64, 0}, loader);
  EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(interrupted);
  EXPECT_TRUE(checkpoint_sticky);
  EXPECT_EQ(pool.stats().active, 0u);
  EXPECT_EQ(pool.stats().discarded, 1u);
  EXPECT_EQ(runtime.stats().live_bytes, 0u);
  EXPECT_EQ(runtime.stats().temporary_bytes, 0u);
  EXPECT_EQ(runtime.stats().cache_owned_bytes, 0u);

  auto retried = runtime.cache().get("pool-budget", {64, 0}, loader);
  ASSERT_TRUE(retried.ok()) << retried.status().message();
  EXPECT_EQ(calls.load(), 2u);
  EXPECT_EQ(pool.stats().created, 2u);
  EXPECT_EQ(pool.stats().active, 0u);
  EXPECT_EQ(pool.stats().discarded, 1u);
  EXPECT_EQ(runtime.stats().read_bytes, 2u);
  EXPECT_EQ(runtime.stats().cache_owned_bytes, 64u);
}
