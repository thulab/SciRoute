#include "iotdbfs/query/core/QueryDomainCache.h"
#include <gtest/gtest.h>
#include <chrono>
#include <future>
#include <stdexcept>
#include <vector>

using namespace iotdbfs;
namespace {
using namespace std::chrono_literals;
struct Accounting {
  explicit Accounting(std::uint64_t maximum) : maximum(maximum) {}
  bool grow(std::uint64_t count) {
    std::lock_guard<std::mutex> lock(mutex);
    if (count > maximum - bytes) return false;
    bytes += count; return true;
  }
  void shrink(std::uint64_t count) {
    std::lock_guard<std::mutex> lock(mutex);
    if (count > bytes) {underflow = true; return;}
    bytes -= count;
  }
  std::uint64_t used() const {
    std::lock_guard<std::mutex> lock(mutex); return bytes;
  }
  std::uint64_t available() const {
    std::lock_guard<std::mutex> lock(mutex); return maximum - bytes;
  }
  mutable std::mutex mutex;
  const std::uint64_t maximum;
  std::uint64_t bytes{0};
  bool underflow{false};
};
struct State final : QueryDomainState {
  State(std::uint64_t bytes, std::shared_ptr<Accounting> accounting,
        std::shared_ptr<std::uint64_t> destruction_charge = {})
      : bytes(bytes), accounting(std::move(accounting)), destruction_charge(std::move(destruction_charge)) {}
  ~State() override {
    if (destruction_charge) *destruction_charge = accounting->used();
  }
  std::uint64_t retained_bytes() const override {return bytes;}
  const std::uint64_t bytes;
  std::shared_ptr<Accounting> accounting;
  std::shared_ptr<std::uint64_t> destruction_charge;
};
struct RetainedSlot final : QueryDomainRetention {
  explicit RetainedSlot(std::shared_ptr<const void> value) : value(std::move(value)) {}
  std::shared_ptr<const void> acquire() noexcept override {
    std::function<void()> callback;
    std::shared_ptr<const void> result;
    {
      std::lock_guard<std::mutex> lock(mutex);
      callback.swap(on_acquire);
      result = value;
    }
    if(callback) callback();
    return result;
  }
  void clear() {
    std::shared_ptr<const void> retired;
    {std::lock_guard<std::mutex> lock(mutex); retired = std::move(value);}
  }
  std::mutex mutex;
  std::shared_ptr<const void> value;
  std::function<void()> on_acquire;
};
std::shared_ptr<QueryDomainCache> cache_for(const std::shared_ptr<Accounting>& accounting,
                                          QueryDomainCache::Retain retain = {}) {
  return std::make_shared<QueryDomainCache>(
    [accounting](std::uint64_t n) {return accounting->grow(n);},
    [accounting](std::uint64_t n) {accounting->shrink(n);},
    [accounting] {return accounting->available();}, std::move(retain));
}
}

TEST(QueryDomainCache, ConcurrentWaitersShareOnePublishedImmutableState) {
  const auto accounting = std::make_shared<Accounting>(128);
  const auto cache = cache_for(accounting);
  auto producer = cache->claim(); ASSERT_TRUE(producer->building());
  ASSERT_TRUE(producer->grow(64)); EXPECT_EQ(producer->available(), 64U);
  const auto state = std::make_shared<const State>(64, accounting);
  std::vector<std::future<std::shared_ptr<QueryDomainCache::Claim>>> pending;
  std::vector<std::future<void>> entered;
  for (unsigned i = 0; i < 8; ++i) {
    auto started = std::make_shared<std::promise<void>>();
    entered.push_back(started->get_future());
    pending.push_back(std::async(std::launch::async, [cache, started] {
      started->set_value(); return cache->claim();
    }));
  }
  for (auto& start : entered) start.wait();
  producer->publish(state);
  // Publication is not builder completion; no waiter can observe partial work.
  for (auto& future : pending) EXPECT_EQ(future.wait_for(1ms), std::future_status::timeout);
  producer.reset();
  for (auto& future : pending) {
    EXPECT_EQ(future.wait_for(2s), std::future_status::ready);
    const auto hit = future.get();
    EXPECT_FALSE(hit->building()); EXPECT_EQ(hit->value.get(), state.get());
    EXPECT_FALSE(hit->grow(1));
  }
  EXPECT_EQ(accounting->used(), 64U);
  EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, AbandonedBuilderWakesWaiterAndRefundsAllPartialReservations) {
  const auto accounting = std::make_shared<Accounting>(64);
  const auto cache = cache_for(accounting);
  auto producer = cache->claim(); ASSERT_TRUE(producer->building());
  ASSERT_TRUE(producer->grow(13)); ASSERT_TRUE(producer->grow(19));
  auto entered = std::make_shared<std::promise<void>>();
  auto started = entered->get_future();
  auto waiting = std::async(std::launch::async, [cache, entered] {
    entered->set_value(); return cache->claim();
  });
  started.wait(); EXPECT_EQ(waiting.wait_for(5ms), std::future_status::timeout);
  producer.reset();
  EXPECT_EQ(waiting.wait_for(2s), std::future_status::ready);
  auto retry = waiting.get(); ASSERT_TRUE(retry->building()); EXPECT_FALSE(retry->value);
  EXPECT_EQ(accounting->used(), 0U);
  ASSERT_TRUE(retry->grow(64));
  retry.reset(); EXPECT_EQ(accounting->used(), 0U);
  EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, StateAliasPinsChargeAfterDomainCacheOwnerRetires) {
  const auto accounting = std::make_shared<Accounting>(128);
  auto cache = cache_for(accounting);
  auto observed_destructor_charge = std::make_shared<std::uint64_t>(0);
  auto producer = cache->claim(); ASSERT_TRUE(producer->grow(96));
  auto state = std::make_shared<const State>(96, accounting, observed_destructor_charge);
  std::weak_ptr<const QueryDomainState> weak_state = state;
  producer->publish(state);
  ASSERT_EQ(producer->value.get(), state.get());
  auto cold_pin = producer->value;
  state.reset(); producer.reset();
  auto hit = cache->claim(); ASSERT_FALSE(hit->building()); ASSERT_TRUE(hit->value);
  auto pinned = hit->value; hit.reset();
  std::weak_ptr<QueryDomainCache> weak_cache = cache;
  cache.reset();
  EXPECT_TRUE(weak_cache.expired()); EXPECT_FALSE(weak_state.expired());
  EXPECT_EQ(accounting->used(), 96U);
  cold_pin.reset(); EXPECT_EQ(accounting->used(), 96U);
  pinned.reset();
  EXPECT_TRUE(weak_state.expired()); EXPECT_EQ(accounting->used(), 0U);
  EXPECT_EQ(*observed_destructor_charge, 96U);
  EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, LastAliasControlsRefundAcrossMultipleHitClaims) {
  const auto accounting = std::make_shared<Accounting>(128);
  auto cache = cache_for(accounting);
  auto producer = cache->claim(); ASSERT_TRUE(producer->grow(80));
  producer->publish(std::make_shared<const State>(80, accounting)); producer.reset();
  auto first = cache->claim(); auto second = cache->claim();
  EXPECT_EQ(first->value.get(), second->value.get());
  cache.reset(); first.reset(); EXPECT_EQ(accounting->used(), 80U);
  second.reset(); EXPECT_EQ(accounting->used(), 0U);
  EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, OptionalGrowthFailureDoesNotLeakOrPreventLaterBuild) {
  const auto accounting = std::make_shared<Accounting>(48);
  auto cache = cache_for(accounting);
  auto declined = cache->claim(); ASSERT_TRUE(declined->building());
  EXPECT_FALSE(declined->grow(49)); EXPECT_EQ(accounting->used(), 0U);
  ASSERT_TRUE(declined->grow(16)); EXPECT_FALSE(declined->grow(33));
  EXPECT_EQ(accounting->used(), 16U);
  declined.reset(); EXPECT_EQ(accounting->used(), 0U);
  auto retry = cache->claim(); ASSERT_TRUE(retry->building());
  ASSERT_TRUE(retry->grow(48));
  retry->publish(std::make_shared<const State>(48, accounting)); retry.reset();
  auto hit = cache->claim(); ASSERT_TRUE(hit->value); EXPECT_FALSE(hit->building());
  EXPECT_EQ(hit->value->retained_bytes(), 48U);
  hit.reset(); cache.reset(); EXPECT_EQ(accounting->used(), 0U);
  EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, PublishReportsSuccessOnlyForTheFirstActualPublication) {
  const auto accounting = std::make_shared<Accounting>(128);
  const auto cache = cache_for(accounting);
  auto producer = cache->claim(); ASSERT_TRUE(producer->grow(64));
  auto original = std::make_shared<const State>(64, accounting);
  EXPECT_FALSE(producer->publish(nullptr));
  EXPECT_TRUE(producer->publish(original));
  EXPECT_FALSE(producer->publish(std::make_shared<const State>(64, accounting)));
  EXPECT_EQ(producer->value.get(), original.get());
  producer.reset();
  auto hit = cache->claim(); ASSERT_TRUE(hit->value);
  EXPECT_FALSE(hit->publish(std::make_shared<const State>(64, accounting)));
  EXPECT_EQ(hit->value.get(), original.get());
  EXPECT_EQ(accounting->used(), 64U);
}

TEST(QueryDomainCache, RejectedPublicationDestroysPayloadOutsideTheCacheLock) {
  const auto accounting = std::make_shared<Accounting>(128);
  const auto cache = cache_for(accounting);
  auto producer = cache->claim(); ASSERT_TRUE(producer->grow(64));
  ASSERT_TRUE(producer->publish(std::make_shared<const State>(64, accounting)));
  // A destructor that calls back into claim() would wait for this producer;
  // first drop it, then test rejection through a non-producing hit claim.
  producer.reset();
  auto hit = cache->claim(); ASSERT_TRUE(hit->value);
  bool destroyed = false;
  auto rejected = std::shared_ptr<const QueryDomainState>(new State(0, accounting),
    [&](const QueryDomainState* state) {
      auto reentrant = cache->claim(); EXPECT_TRUE(reentrant->value);
      destroyed = true; delete state;
    });
  EXPECT_FALSE(hit->publish(std::move(rejected)));
  EXPECT_TRUE(destroyed); EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, ClearedRetentionRebuildsWhileOldAliasesKeepTheirOwnCharges) {
  const auto accounting = std::make_shared<Accounting>(160);
  std::vector<std::shared_ptr<RetainedSlot>> slots;
  auto cache = cache_for(accounting, [&](std::shared_ptr<const void> entry) {
    auto slot = std::make_shared<RetainedSlot>(std::move(entry));
    slots.push_back(slot); return slot;
  });
  auto destruction_charge = std::make_shared<std::uint64_t>(0);
  auto producer = cache->claim(); ASSERT_TRUE(producer->grow(96));
  auto first = std::make_shared<const State>(96, accounting, destruction_charge);
  std::weak_ptr<const QueryDomainState> weak_first = first;
  ASSERT_TRUE(producer->publish(first));
  auto cold_alias = producer->value;
  first.reset(); producer.reset();
  auto hit = cache->claim(); ASSERT_FALSE(hit->building());
  auto warm_alias = hit->value; hit.reset();
  ASSERT_EQ(slots.size(), 1U); slots.front()->clear();
  EXPECT_FALSE(weak_first.expired()); EXPECT_EQ(accounting->used(), 96U);

  auto rebuild = cache->claim(); ASSERT_TRUE(rebuild->building());
  ASSERT_TRUE(rebuild->grow(64));
  ASSERT_TRUE(rebuild->publish(std::make_shared<const State>(64, accounting)));
  EXPECT_NE(rebuild->value.get(), cold_alias.get());
  rebuild.reset();
  EXPECT_EQ(accounting->used(), 160U);
  cold_alias.reset(); EXPECT_EQ(accounting->used(), 160U);
  warm_alias.reset();
  EXPECT_TRUE(weak_first.expired()); EXPECT_EQ(accounting->used(), 64U);
  EXPECT_EQ(*destruction_charge, 160U);
  slots.back()->clear(); cache.reset();
  EXPECT_EQ(accounting->used(), 0U); EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, OptionalRetentionRefusalAndExceptionAllowLaterBuild) {
  const auto accounting = std::make_shared<Accounting>(96);
  unsigned attempts = 0;
  std::shared_ptr<RetainedSlot> slot;
  auto cache = cache_for(accounting,
    [&](std::shared_ptr<const void> entry) -> std::shared_ptr<QueryDomainRetention> {
      if(++attempts == 1) return {};
      if(attempts == 2) throw std::runtime_error("optional retention failure");
      slot = std::make_shared<RetainedSlot>(std::move(entry)); return slot;
    });
  for(unsigned i = 0; i < 2; ++i) {
    auto builder = cache->claim(); ASSERT_TRUE(builder->building());
    ASSERT_TRUE(builder->grow(96));
    EXPECT_FALSE(builder->publish(std::make_shared<const State>(96, accounting)));
    EXPECT_FALSE(builder->value);
    builder.reset(); EXPECT_EQ(accounting->used(), 0U);
  }
  auto builder = cache->claim(); ASSERT_TRUE(builder->grow(96));
  ASSERT_TRUE(builder->publish(std::make_shared<const State>(96, accounting)));
  builder.reset();
  auto hit = cache->claim(); ASSERT_TRUE(hit->value); EXPECT_FALSE(hit->building());
  slot->clear(); hit.reset(); cache.reset();
  EXPECT_EQ(accounting->used(), 0U); EXPECT_FALSE(accounting->underflow);
}

TEST(QueryDomainCache, RetainedPublicationAndClearingPreserveBuilderCompletionBarrier) {
  const auto accounting = std::make_shared<Accounting>(128);
  auto entered = std::make_shared<std::promise<void>>();
  auto admission_started = entered->get_future();
  auto release = std::make_shared<std::promise<void>>();
  auto admission_released = release->get_future().share();
  std::shared_ptr<RetainedSlot> slot;
  auto cache = cache_for(accounting, [&](std::shared_ptr<const void> entry) {
    entered->set_value(); admission_released.wait();
    slot = std::make_shared<RetainedSlot>(std::move(entry)); return slot;
  });
  auto builder = cache->claim(); ASSERT_TRUE(builder->grow(64));
  auto publication = std::async(std::launch::async, [builder, accounting] {
    return builder->publish(std::make_shared<const State>(64, accounting));
  });
  admission_started.wait();
  auto waiting = std::async(std::launch::async, [cache] {return cache->claim();});
  EXPECT_EQ(waiting.wait_for(5ms), std::future_status::timeout);
  release->set_value(); ASSERT_TRUE(publication.get());
  EXPECT_EQ(waiting.wait_for(5ms), std::future_status::timeout);
  // Even eviction during publication cannot expose partially built state or
  // allow this producer to publish a second, differently accounted artifact.
  slot->clear();
  EXPECT_FALSE(builder->publish(std::make_shared<const State>(64, accounting)));
  builder.reset();
  ASSERT_EQ(waiting.wait_for(2s), std::future_status::ready);
  auto rebuild = waiting.get(); EXPECT_TRUE(rebuild->building()); EXPECT_FALSE(rebuild->value);
  EXPECT_EQ(accounting->used(), 0U);
}

TEST(QueryDomainCache, RetentionAcquisitionAndLastPayloadDestructionRunOutsideCacheLock) {
  const auto accounting = std::make_shared<Accounting>(96);
  std::shared_ptr<RetainedSlot> slot;
  auto cache = cache_for(accounting, [&](std::shared_ptr<const void> entry) {
    slot = std::make_shared<RetainedSlot>(std::move(entry)); return slot;
  });
  bool destroyed = false;
  auto builder = cache->claim(); ASSERT_TRUE(builder->grow(96));
  auto state = std::shared_ptr<const QueryDomainState>(new State(96, accounting),
    [&](const QueryDomainState* value) {
      auto reentrant = cache->claim(); EXPECT_TRUE(reentrant->building());
      EXPECT_EQ(accounting->used(), 96U);
      destroyed = true; delete value;
    });
  ASSERT_TRUE(builder->publish(std::move(state))); builder.reset();
  slot->on_acquire = [&] {
    auto reentrant = cache->claim(); EXPECT_TRUE(reentrant->value);
    EXPECT_FALSE(reentrant->building());
  };
  auto hit = cache->claim(); ASSERT_TRUE(hit->value);
  slot->clear(); EXPECT_FALSE(destroyed);
  hit.reset(); EXPECT_TRUE(destroyed);
  EXPECT_EQ(accounting->used(), 0U); EXPECT_FALSE(accounting->underflow);
}
