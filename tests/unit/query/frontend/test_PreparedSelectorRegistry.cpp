#include "iotdbfs/query/frontend/PreparedSelectorRegistry.h"
#include "iotdbfs/query/core/QueryTaskScheduler.h"
#include <gtest/gtest.h>
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>

namespace {
using namespace iotdbfs;
using Registry = PreparedSelectorRegistry;
using Payload = Registry::Payload;
using BuildResult = QueryResult<Payload>;
using namespace std::chrono_literals;

BuildResult value(int n = 7) { return BuildResult::success(std::make_shared<const int>(n)); }
Registry::Builder builder() { return [] { return value(); }; }

TEST(PreparedSelectorRegistry, UnknownExpiredAndCrossUidHaveTheSameError) {
  auto pool = std::make_shared<HotQueryCache>();
  auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  EXPECT_EQ(inserted.value().size(), 48U);
  auto unknown = registry.acquire(1, "missing");
  auto other = registry.acquire(2, inserted.value());
  EXPECT_EQ(unknown.status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(other.status().message(), unknown.status().message());
  now += 9ms;
  { auto lease = registry.acquire(1, inserted.value()); ASSERT_TRUE(lease.ok());
    EXPECT_EQ(*static_cast<const int*>(lease.value().get()), 7); }
  now += 9ms; EXPECT_TRUE(registry.acquire(1, inserted.value()).ok());
  now += 10ms;
  auto expired = registry.acquire(1, inserted.value());
  EXPECT_EQ(expired.status().message(), unknown.status().message());
  EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_EQ(pool->stats().resident_bytes, 4096U + Registry::registry_overhead_bytes);
}

TEST(PreparedSelectorRegistry, RetiredLeasesContinueToChargeBytesAndOwnerCount) {
  auto pool = std::make_shared<HotQueryCache>();
  Registry::Config config; config.max_entries_per_uid = 1;
  Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  auto lease = registry.acquire(1, inserted.value()); ASSERT_TRUE(lease.ok());
  EXPECT_FALSE(registry.erase(2, inserted.value()));
  EXPECT_TRUE(registry.erase(1, inserted.value()));
  EXPECT_FALSE(registry.acquire(1, inserted.value()).ok());
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_EQ(registry.stats().resident_bytes, 2048U);
  EXPECT_FALSE(registry.insert(1, 2048, builder()).ok());
  EXPECT_TRUE(registry.insert(2, 2048, builder()).ok());
  lease.value().reset();
  EXPECT_TRUE(registry.insert(1, 2048, builder()).ok());
  registry.clear(); EXPECT_EQ(registry.stats().resident_bytes, 0U);
}

TEST(PreparedSelectorRegistry, ReserveBeforeBuilderAndRollBackAllFailurePaths) {
  auto pool = std::make_shared<HotQueryCache>(); Registry registry(pool);
  const auto initial = pool->stats().resident_bytes;
  bool observed = false;
  auto failed = registry.insert(1, 4096, [&] {
    observed = true;
    EXPECT_EQ(registry.stats().resident_entries, 1U);
    EXPECT_EQ(pool->stats().resident_bytes, initial + 4096);
    return BuildResult::failure(QueryStatus::failure(QueryError::InvalidQuery,
      QueryPhase::Validation, "invalid selector"));
  });
  EXPECT_TRUE(observed); EXPECT_EQ(failed.status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_EQ(pool->stats().resident_bytes, initial);
  EXPECT_EQ(registry.insert(1, 4096, []() -> BuildResult { throw std::bad_alloc(); })
    .status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(registry.insert(1, 4096, []() -> BuildResult { throw std::runtime_error("x"); })
    .status().code(), QueryError::Internal);
  EXPECT_FALSE(registry.insert(1, 4096, [] { return BuildResult::success({}); }).ok());
  EXPECT_FALSE(registry.insert(1, 4096, {}).ok());
  EXPECT_EQ(pool->stats().resident_bytes, initial);
}

TEST(PreparedSelectorRegistry, EnforcesLocalGlobalAndSharedPoolCapsWithoutCallingBuilder) {
  auto pool = std::make_shared<HotQueryCache>();
  Registry::Config config; config.max_entries = 2; config.max_entries_per_uid = 1;
  config.max_bytes = 4096; config.max_item_bytes = 2048;
  Registry registry(pool, config); unsigned calls = 0;
  auto build = [&] { ++calls; return value(); };
  EXPECT_FALSE(registry.insert(1, 1023, build).ok());
  EXPECT_FALSE(registry.insert(1, 2049, build).ok());
  EXPECT_EQ(calls, 0U);
  EXPECT_TRUE(registry.insert(1, 2048, build).ok());
  EXPECT_FALSE(registry.insert(1, 2048, build).ok());
  EXPECT_TRUE(registry.insert(2, 2048, build).ok());
  EXPECT_FALSE(registry.insert(3, 1024, build).ok()); EXPECT_EQ(calls, 2U);
  registry.clear();
  auto occupied = pool->reserve(HotQueryCache::Kind::Auxiliary,
    HotQueryCache::maximum_bytes - pool->stats().resident_bytes - 1023);
  ASSERT_TRUE(occupied);
  EXPECT_FALSE(registry.insert(1, 1024, build).ok()); EXPECT_EQ(calls, 2U);
}

TEST(PreparedSelectorRegistry, CollisionAndTokenFailureDoNotReplaceOrLeak) {
  auto pool = std::make_shared<HotQueryCache>(); Registry::Config config;
  config.token = [] { return std::string(48, 'a'); };
  Registry registry(pool, config);
  auto first = registry.insert(1, 2048, builder()); ASSERT_TRUE(first.ok());
  auto second = registry.insert(1, 4096, [] { return value(9); });
  EXPECT_FALSE(second.ok()); EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_EQ(registry.stats().resident_bytes, 2048U);
  EXPECT_EQ(*static_cast<const int*>(registry.acquire(1, first.value()).value().get()), 7);
  config.token = []() -> std::string { throw std::bad_alloc(); };
  Registry broken(pool, config);
  EXPECT_EQ(broken.insert(1, 2048, builder()).status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(broken.stats().resident_entries, 0U);
  config.token = [] { return "not opaque"; }; Registry invalid(pool, config);
  EXPECT_FALSE(invalid.insert(1, 2048, builder()).ok());
  EXPECT_EQ(invalid.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, PayloadDestroyedOutsideLocksBeforeRefund) {
  auto pool = std::make_shared<HotQueryCache>(); Registry registry(pool);
  bool destroyed = false;
  auto inserted = registry.insert(1, 2048, [&] {
    Payload p(new int(7), [&](const void* x) {
      EXPECT_EQ(registry.stats().resident_bytes, 2048U);
      EXPECT_EQ(pool->stats().resident_bytes,
        4096U + Registry::registry_overhead_bytes + 2048U);
      EXPECT_FALSE(registry.erase(1, "absent")); // Would deadlock under registry lock.
      destroyed = true; delete static_cast<const int*>(x);
    }); return BuildResult::success(std::move(p));
  });
  ASSERT_TRUE(inserted.ok()); registry.clear();
  EXPECT_TRUE(destroyed); EXPECT_EQ(registry.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, AccountingOutlivesRegistryUntilLastPayloadLease) {
  auto pool = std::make_shared<HotQueryCache>(); Payload lease;
  {
    Registry registry(pool); auto id = registry.insert(1, 2048, builder()); ASSERT_TRUE(id.ok());
    lease = registry.acquire(1, id.value()).value();
  }
  EXPECT_EQ(pool->stats().resident_bytes,
    4096U + Registry::registry_overhead_bytes + 2048U);
  lease.reset(); EXPECT_EQ(pool->stats().resident_bytes, 4096U);
}

TEST(PreparedSelectorRegistry, PendingBuilderUsesQuotaAndClearCancelsPublication) {
  auto pool = std::make_shared<HotQueryCache>(); Registry::Config config;
  config.max_entries = 1; Registry registry(pool, config);
  std::promise<void> entered, resume; auto ready = resume.get_future();
  auto pending = std::async(std::launch::async, [&] {
    return registry.insert(1, 2048, [&] {
      entered.set_value(); ready.wait(); return value();
    });
  });
  entered.get_future().wait();
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_FALSE(registry.insert(2, 2048, builder()).ok());
  registry.clear();
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  resume.set_value(); auto result = pending.get();
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
  EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_TRUE(registry.insert(1, 2048, builder()).ok());
}

TEST(PreparedSelectorRegistry, ConcurrentAcquireReleaseAndClearRetainOwnedPayloads) {
  auto pool = std::make_shared<HotQueryCache>(); Registry registry(pool);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  std::atomic<unsigned> valid{0};
  std::vector<std::thread> workers;
  for (unsigned i = 0; i < 8; ++i) workers.emplace_back([&] {
    for (unsigned n = 0; n < 200; ++n) {
      auto acquired = registry.acquire(1, inserted.value());
      if (acquired.ok()) {
        if (*static_cast<const int*>(acquired.value().get()) == 7) ++valid;
      } else EXPECT_EQ(acquired.status().code(), QueryError::SourceNotFound);
    }
  });
  registry.clear(); for (auto& worker : workers) worker.join();
  EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_EQ(pool->stats().resident_bytes, 4096U + Registry::registry_overhead_bytes);
}

TEST(PreparedSelectorRegistry, ReapRetiresExpiredEntriesButKeepsLeasesAndUidLimit) {
  auto pool = std::make_shared<HotQueryCache>(); auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 1ms;
  config.max_entries_per_uid = 1; Registry registry(pool, config);
  auto id = registry.insert(1, 2048, builder()); ASSERT_TRUE(id.ok());
  auto lease = registry.acquire(1, id.value()); ASSERT_TRUE(lease.ok());
  now += 1ms; registry.reap();
  EXPECT_FALSE(registry.acquire(1, id.value()).ok());
  EXPECT_FALSE(registry.insert(1, 2048, builder()).ok());
  lease.value().reset(); EXPECT_TRUE(registry.insert(1, 2048, builder()).ok());
}

TEST(PreparedSelectorRegistry, GlobalPoolHandleCountAndByteCapsSpanRegistries) {
  auto pool = std::make_shared<HotQueryCache>();
  Registry one(pool), two(pool);
  for (unsigned i = 0; i < 64; ++i)
    ASSERT_TRUE(one.insert(static_cast<uid_t>(i / 16), 1024, builder()).ok());
  EXPECT_EQ(pool->stats().handles, 64U);
  EXPECT_FALSE(two.insert(99, 1024, builder()).ok());
  one.clear(); EXPECT_EQ(pool->stats().handles, 0U);
  for (unsigned i = 0; i < 8; ++i)
    ASSERT_TRUE(one.insert(1, 1024ULL * 1024, builder()).ok());
  EXPECT_EQ(pool->stats().handle_bytes, 8ULL * 1024 * 1024);
  EXPECT_FALSE(two.insert(2, 1024, builder()).ok());
  one.clear(); EXPECT_TRUE(two.insert(2, 1024, builder()).ok());
}

TEST(PreparedSelectorRegistry, ConstructorOverheadRequiresSharedPoolCapacity) {
  auto pool = std::make_shared<HotQueryCache>();
  auto occupied = pool->reserve(HotQueryCache::Kind::Auxiliary,
    HotQueryCache::maximum_bytes - pool->stats().resident_bytes - 4095);
  ASSERT_TRUE(occupied);
  EXPECT_THROW(Registry registry(pool), std::bad_alloc);
  EXPECT_EQ(pool->stats().resident_bytes, HotQueryCache::maximum_bytes - 4095);
}

TEST(PreparedSelectorRegistry, ConditionalEraseRequiresExactObjectAndLeaseOwnership) {
  auto pool = std::make_shared<HotQueryCache>(); Registry registry(pool);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  auto acquired = registry.acquire(1, inserted.value()); ASSERT_TRUE(acquired.ok());
  auto lease = acquired.value();
  EXPECT_FALSE(registry.erase(1, inserted.value(), Payload{}));
  EXPECT_FALSE(registry.erase(2, inserted.value(), lease));
  EXPECT_FALSE(registry.erase(1, "missing", lease));
  EXPECT_FALSE(registry.erase(1, inserted.value(), std::make_shared<const int>(7)));
  // A borrowed pointer with a different control block is not the registry lease.
  Payload same_address(lease.get(), [](const void*) {});
  EXPECT_FALSE(registry.erase(1, inserted.value(), same_address));
  // Shared ownership alone is insufficient: an alias can name another object.
  Payload different_object(lease, static_cast<const int*>(lease.get()) + 1);
  EXPECT_FALSE(registry.erase(1, inserted.value(), different_object));
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_TRUE(registry.acquire(1, inserted.value()).ok());
  Payload exact_alias(lease, lease.get());
  EXPECT_TRUE(registry.erase(1, inserted.value(), exact_alias));
  EXPECT_FALSE(registry.erase(1, inserted.value(), lease));
  EXPECT_FALSE(registry.acquire(1, inserted.value()).ok());
  EXPECT_EQ(*static_cast<const int*>(lease.get()), 7);
}

TEST(PreparedSelectorRegistry, ConditionalEraseCannotRemoveReusedIdAfterExpiry) {
  auto pool = std::make_shared<HotQueryCache>(); auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 1ms;
  config.token = [] { return std::string(48, 'a'); };
  Registry registry(pool, config);
  auto first = registry.insert(1, 2048, [] { return value(7); }); ASSERT_TRUE(first.ok());
  auto old = registry.acquire(1, first.value()); ASSERT_TRUE(old.ok());
  now += 1ms;
  // insert reaps the old entry, but its held lease still owns its quota.
  auto replacement = registry.insert(1, 2048, [] { return value(9); }); ASSERT_TRUE(replacement.ok());
  ASSERT_EQ(replacement.value(), first.value());
  EXPECT_EQ(registry.stats().resident_entries, 2U);
  EXPECT_FALSE(registry.erase(1, first.value(), old.value()));
  auto current = registry.acquire(1, replacement.value()); ASSERT_TRUE(current.ok());
  EXPECT_EQ(*static_cast<const int*>(current.value().get()), 9);
  EXPECT_TRUE(registry.erase(1, replacement.value(), current.value()));
  EXPECT_EQ(registry.stats().resident_entries, 2U);
  old.value().reset(); EXPECT_EQ(registry.stats().resident_entries, 1U);
  current.value().reset(); EXPECT_EQ(registry.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, FailedConditionalEraseDoesNotTouchTtl) {
  auto pool = std::make_shared<HotQueryCache>(); auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  auto lease = registry.acquire(1, inserted.value()); ASSERT_TRUE(lease.ok());
  now += 9ms;
  EXPECT_FALSE(registry.erase(1, inserted.value(), std::make_shared<const int>(7)));
  now += 1ms;
  EXPECT_FALSE(registry.acquire(1, inserted.value()).ok());
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  lease.value().reset(); EXPECT_EQ(registry.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, ConcurrentConditionalEraseHasOneWinnerAndRetainsQuota) {
  auto pool = std::make_shared<HotQueryCache>(); Registry::Config config;
  config.max_entries_per_uid = 1; Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  auto acquired = registry.acquire(1, inserted.value()); ASSERT_TRUE(acquired.ok());
  auto lease = acquired.value(); acquired.value().reset();
  std::promise<void> start; auto ready = start.get_future().share();
  std::atomic<unsigned> winners{0}; std::vector<std::thread> workers;
  for (unsigned i = 0; i < 8; ++i) workers.emplace_back([&, ready] {
    ready.wait();
    if (registry.erase(1, inserted.value(), lease)) ++winners;
  });
  start.set_value(); for (auto& worker : workers) worker.join();
  EXPECT_EQ(winners.load(), 1U);
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_FALSE(registry.insert(1, 2048, builder()).ok());
  EXPECT_EQ(*static_cast<const int*>(lease.get()), 7);
  lease.reset(); EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_TRUE(registry.insert(1, 2048, builder()).ok());
}

TEST(PreparedSelectorRegistry, InspectIsConstAndHidesMissingCrossUidAndExpiredHandles) {
  auto pool = std::make_shared<HotQueryCache>();
  auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  Registry registry(pool, config);
  const Registry& observation = registry;
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  const auto bytes = pool->stats().resident_bytes;
  EXPECT_FALSE(observation.inspect(1, "missing"));
  EXPECT_FALSE(observation.inspect(2, inserted.value()));
  auto observed = observation.inspect(1, inserted.value()); ASSERT_TRUE(observed);
  EXPECT_EQ(*static_cast<const int*>(observed.get()), 7);
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_EQ(pool->stats().resident_bytes, bytes);
  now += 10ms;
  EXPECT_FALSE(observation.inspect(1, inserted.value()));
  EXPECT_FALSE(observation.inspect(2, inserted.value()));
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_EQ(pool->stats().resident_bytes, bytes);
}

TEST(PreparedSelectorRegistry, InspectDoesNotRenewTtlAndAcquireStillEnforcesExpiry) {
  auto pool = std::make_shared<HotQueryCache>();
  auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  now += 9ms;
  auto observed = registry.inspect(1, inserted.value()); ASSERT_TRUE(observed);
  now += 1ms;
  EXPECT_FALSE(registry.inspect(1, inserted.value()));
  auto acquired = registry.acquire(1, inserted.value());
  EXPECT_EQ(acquired.status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(*static_cast<const int*>(observed.get()), 7);
  // Observation owns immutable bytes, but it does not keep the ID executable.
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  observed.reset();
  EXPECT_EQ(registry.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, InspectDoesNotReapExpiredEntriesOrRefundTheirBudget) {
  for (const bool use_acquire : {false, true}) {
    SCOPED_TRACE(use_acquire);
    auto pool = std::make_shared<HotQueryCache>();
    auto now = Registry::Clock::time_point{};
    Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
    Registry registry(pool, config);
    auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
    const auto bytes = pool->stats().resident_bytes;
    now += 10ms;
    EXPECT_FALSE(registry.inspect(1, inserted.value()));
    EXPECT_FALSE(registry.inspect(1, inserted.value()));
    EXPECT_EQ(registry.stats().resident_entries, 1U);
    EXPECT_EQ(registry.stats().resident_bytes, 2048U);
    EXPECT_EQ(pool->stats().resident_bytes, bytes);
    if (use_acquire)
      EXPECT_EQ(registry.acquire(1, inserted.value()).status().code(), QueryError::SourceNotFound);
    else registry.reap();
    EXPECT_EQ(registry.stats().resident_entries, 0U);
    EXPECT_EQ(pool->stats().resident_bytes, bytes - 2048U);
  }
}

TEST(PreparedSelectorRegistry, InspectedPayloadSurvivesRetirementWithoutAuthorizingAcquire) {
  auto pool = std::make_shared<HotQueryCache>(); Registry registry(pool);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  auto observed = registry.inspect(1, inserted.value()); ASSERT_TRUE(observed);
  EXPECT_TRUE(registry.erase(1, inserted.value()));
  EXPECT_FALSE(registry.inspect(1, inserted.value()));
  EXPECT_EQ(registry.acquire(1, inserted.value()).status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(*static_cast<const int*>(observed.get()), 7);
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  observed.reset();
  EXPECT_EQ(registry.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, OldInspectionCannotErasePayloadPublishedUnderReusedId) {
  auto pool = std::make_shared<HotQueryCache>();
  auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  config.token = [] { return std::string(48, 'b'); };
  Registry registry(pool, config);
  auto first = registry.insert(1, 2048, [] { return value(7); }); ASSERT_TRUE(first.ok());
  auto observed = registry.inspect(1, first.value()); ASSERT_TRUE(observed);
  now += 10ms;
  auto replacement = registry.insert(1, 2048, [] { return value(9); });
  ASSERT_TRUE(replacement.ok());
  ASSERT_EQ(first.value(), replacement.value());
  EXPECT_EQ(registry.stats().resident_entries, 2U);
  EXPECT_FALSE(registry.erase(1, first.value(), observed));
  auto current = registry.acquire(1, replacement.value()); ASSERT_TRUE(current.ok());
  EXPECT_EQ(*static_cast<const int*>(observed.get()), 7);
  EXPECT_EQ(*static_cast<const int*>(current.value().get()), 9);
  observed.reset();
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_TRUE(registry.inspect(1, replacement.value()));
  EXPECT_TRUE(registry.erase(1, replacement.value(), current.value()));
  current.value().reset();
  EXPECT_EQ(registry.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, MatchingWeakObservationAcquiresAndRenewsTtl) {
  auto pool = std::make_shared<HotQueryCache>();
  auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  std::weak_ptr<const void> expected = registry.inspect(1, inserted.value());
  now += 9ms;
  auto acquired = registry.acquire(1, inserted.value(), &expected); ASSERT_TRUE(acquired.ok());
  EXPECT_EQ(*static_cast<const int*>(acquired.value().get()), 7);
  acquired.value().reset();
  now += 1ms;
  EXPECT_TRUE(registry.inspect(1, inserted.value()));
  now += 9ms;
  EXPECT_EQ(registry.acquire(1, inserted.value(), &expected).status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_TRUE(expected.expired());
}

TEST(PreparedSelectorRegistry, MismatchedWeakObservationDoesNotRenewTtl) {
  auto pool = std::make_shared<HotQueryCache>();
  auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  const Payload unrelated = std::make_shared<const int>(7);
  const std::weak_ptr<const void> expected = unrelated;
  now += 9ms;
  EXPECT_THROW(registry.acquire(1, inserted.value(), &expected), Registry::ObservationChanged);
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  now += 1ms;
  auto expired = registry.acquire(1, inserted.value(), &expected);
  EXPECT_EQ(expired.status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(registry.stats().resident_entries, 0U);
}

TEST(PreparedSelectorRegistry, WeakObservationDoesNotPinRetiredBudgetAndDetectsReusedId) {
  auto pool = std::make_shared<HotQueryCache>(); Registry::Config config;
  config.token = [] { return std::string(48, 'c'); };
  Registry registry(pool, config);
  const auto base_bytes = pool->stats().resident_bytes;
  auto first = registry.insert(1, 2048, [] { return value(7); }); ASSERT_TRUE(first.ok());
  const std::weak_ptr<const void> old = registry.inspect(1, first.value());
  EXPECT_FALSE(old.expired());
  EXPECT_TRUE(registry.erase(1, first.value()));
  EXPECT_TRUE(old.expired());
  EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_EQ(pool->stats().resident_bytes, base_bytes);
  auto replacement = registry.insert(1, 2048, [] { return value(9); });
  ASSERT_TRUE(replacement.ok());
  ASSERT_EQ(first.value(), replacement.value());
  EXPECT_THROW(registry.acquire(1, replacement.value(), &old), Registry::ObservationChanged);
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  const std::weak_ptr<const void> refreshed = registry.inspect(1, replacement.value());
  auto acquired = registry.acquire(1, replacement.value(), &refreshed); ASSERT_TRUE(acquired.ok());
  EXPECT_EQ(*static_cast<const int*>(acquired.value().get()), 9);
  EXPECT_FALSE(registry.erase(1, replacement.value(), old.lock()));
  acquired.value().reset();
  registry.clear();
  EXPECT_TRUE(refreshed.expired());
  EXPECT_EQ(pool->stats().resident_bytes, base_bytes);
}

TEST(PreparedSelectorRegistry, EmptyWeakObservationDetectsNewlyPublishedHandle) {
  auto pool = std::make_shared<HotQueryCache>(); Registry::Config config;
  const std::string id(48, 'd'); config.token = [&] { return id; };
  Registry registry(pool, config);
  const std::weak_ptr<const void> absent = registry.inspect(1, id);
  EXPECT_TRUE(absent.expired());
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  EXPECT_THROW(registry.acquire(1, id, &absent), Registry::ObservationChanged);
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  EXPECT_TRUE(registry.acquire(1, id).ok());
}

TEST(PreparedSelectorRegistry, WeakObservationKeepsOriginalMissingUidAndExpiryErrors) {
  auto pool = std::make_shared<HotQueryCache>();
  auto now = Registry::Clock::time_point{};
  Registry::Config config; config.clock = [&] { return now; }; config.ttl = 10ms;
  Registry registry(pool, config);
  auto inserted = registry.insert(1, 2048, builder()); ASSERT_TRUE(inserted.ok());
  const std::weak_ptr<const void> expected = registry.inspect(1, inserted.value());
  const auto missing = registry.acquire(1, "missing", &expected);
  const auto other = registry.acquire(2, inserted.value(), &expected);
  EXPECT_EQ(missing.status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(other.status().message(), missing.status().message());
  EXPECT_EQ(registry.stats().resident_entries, 1U);
  now += 10ms;
  const auto expired = registry.acquire(1, inserted.value(), &expected);
  EXPECT_EQ(expired.status().message(), missing.status().message());
  EXPECT_EQ(registry.stats().resident_entries, 0U);
  EXPECT_TRUE(expected.expired());
}

class ObservationTaskGate {
public:
  void arrive_and_wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    entered_ = true; changed_.notify_all();
    changed_.wait(lock, [&] { return open_; });
  }
  bool wait_for_entry() {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, 5s, [&] { return entered_; });
  }
  void open() {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = true; changed_.notify_all();
  }
private:
  std::mutex mutex_;
  std::condition_variable changed_;
  bool entered_{false}, open_{false};
};

TEST(PreparedSelectorRegistry, ReusedArrowIdReclassifiesToHdfAndParksWhileArrowContinues) {
  for (const bool expire_while_waiting : {false, true}) {
    SCOPED_TRACE(expire_while_waiting);
    auto pool = std::make_shared<HotQueryCache>();
    std::atomic<std::int64_t> now_ms{0};
    Registry::Config registry_config;
    registry_config.clock = [&] {
      return Registry::Clock::time_point{} + std::chrono::milliseconds(now_ms.load());
    };
    registry_config.ttl = 10ms;
    registry_config.token = [] { return std::string(48, 'e'); };
    Registry registry(pool, registry_config);
    auto first = registry.insert(1, 2048, [] { return value(7); }); ASSERT_TRUE(first.ok());
    std::weak_ptr<const void> expected = registry.inspect(1, first.value());
    QueryTaskSchedulerConfig config;
    config.worker_threads = config.max_batch_size = config.max_queued_tasks = 1;
    QueryTaskScheduler scheduler(config);
    auto hdf_resource = std::make_shared<QueryTaskResource>(1);
    auto external_hdf = hdf_resource->try_acquire();
    ObservationTaskGate blocker_gate, reclassification_gate;
    std::atomic<unsigned> payload_executions{0}, arrow_executions{0};
    std::atomic<int> executed_payload{0};
    struct Cleanup {
      QueryTaskScheduler& scheduler;
      QueryTaskResource::Lease& external;
      ObservationTaskGate& blocker;
      ObservationTaskGate& reclassification;
      ~Cleanup() {
        external = {}; blocker.open(); reclassification.open(); scheduler.shutdown();
      }
    } cleanup{scheduler, external_hdf, blocker_gate, reclassification_gate};
    auto blocker = scheduler.submit({[&](const CancellationToken&) {
      blocker_gate.arrive_and_wait();
      return QueryStatus::success();
    }});
    ASSERT_TRUE(blocker.ok());
    ASSERT_TRUE(blocker_gate.wait_for_entry());
    auto original = scheduler.submit({[&, expected](const CancellationToken&) mutable {
      try {
        auto acquired = registry.acquire(1, first.value(), &expected);
        if (!acquired.ok()) return acquired.status();
        executed_payload = *static_cast<const int*>(acquired.value().get());
        ++payload_executions;
        return QueryStatus::success();
      } catch (const Registry::ObservationChanged&) {
        auto observed = registry.inspect(1, first.value());
        expected = observed;
        std::vector<QueryTaskScheduler::ResourceRequirement> resources;
        if (observed && *static_cast<const int*>(observed.get()) == 9)
          resources.push_back({hdf_resource, 1});
        observed.reset(); // Scheduling observations do not pin payload budgets.
        reclassification_gate.arrive_and_wait();
        throw QueryTaskScheduler::ResourcesChanged{std::move(resources)};
      }
    }});
    ASSERT_TRUE(original.ok());
    EXPECT_EQ(scheduler.stats().queued, 1U);
    EXPECT_TRUE(registry.erase(1, first.value()));
    EXPECT_TRUE(expected.expired());
    EXPECT_EQ(registry.stats().resident_entries, 0U);
    auto replacement = registry.insert(1, 2048, [] { return value(9); });
    ASSERT_TRUE(replacement.ok());
    ASSERT_EQ(first.value(), replacement.value());
    now_ms = 9;
    blocker_gate.open();
    EXPECT_TRUE(blocker.value().get().ok());
    ASSERT_TRUE(reclassification_gate.wait_for_entry());
    auto arrow = scheduler.submit({[&](const CancellationToken&) {
      ++arrow_executions;
      return QueryStatus::success();
    }});
    ASSERT_TRUE(arrow.ok());
    EXPECT_EQ(scheduler.stats().queued, 1U);
    reclassification_gate.open();
    ASSERT_EQ(arrow.value().wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(arrow.value().get().ok());
    EXPECT_EQ(arrow_executions.load(), 1U);
    EXPECT_EQ(payload_executions.load(), 0U);
    EXPECT_EQ(original.value().wait_for(0s), std::future_status::timeout);
    EXPECT_EQ(scheduler.stats().resource_requeues, 1U);
    EXPECT_EQ(scheduler.stats().active, 0U);
    EXPECT_LE(scheduler.stats().queued, 1U);
    EXPECT_EQ(registry.stats().resident_entries, 1U);
    if (expire_while_waiting) {
      now_ms = 10;
      // The mismatch and resource park must not have renewed expiry to 19 ms.
      EXPECT_FALSE(registry.inspect(1, replacement.value()));
      EXPECT_EQ(registry.stats().resident_entries, 1U);
    }
    external_hdf = {};
    ASSERT_EQ(original.value().wait_for(5s), std::future_status::ready);
    const auto status = original.value().get();
    if (expire_while_waiting) {
      EXPECT_EQ(status.code(), QueryError::SourceNotFound);
      EXPECT_EQ(payload_executions.load(), 0U);
      EXPECT_EQ(registry.stats().resident_entries, 0U);
    } else {
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(payload_executions.load(), 1U);
      EXPECT_EQ(executed_payload.load(), 9);
    }
    scheduler.shutdown();
    EXPECT_EQ(scheduler.stats().submitted, 3U);
    EXPECT_EQ(scheduler.stats().completed, 3U);
    EXPECT_EQ(scheduler.stats().resource_requeues, 1U);
    EXPECT_EQ(scheduler.stats().queued, 0U);
    EXPECT_EQ(scheduler.stats().parked, 0U);
    EXPECT_EQ(hdf_resource->in_use(), 0U);
  }
}
} // namespace
