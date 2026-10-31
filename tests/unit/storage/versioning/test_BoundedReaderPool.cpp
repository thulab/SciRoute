#include "iotdbfs/storage/versioning/detail/BoundedReaderPool.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

using namespace std::chrono_literals;
using iotdbfs::versioning::detail::BoundedReaderPool;
namespace {
struct Reader {
  int id;
  std::shared_ptr<std::atomic<int>> closed;
  ~Reader() { ++*closed; }
};
struct Factory {
  std::atomic<int> next{0};
  std::shared_ptr<std::atomic<int>> closed = std::make_shared<std::atomic<int>>(0);
};
// Reader has a destructor, so constructing a temporary would count a false
// close. Allocate its aggregate directly instead.
auto make_reader(Factory& factory) {
  return [&factory] { return std::unique_ptr<Reader>(new Reader{++factory.next, factory.closed}); };
}
void checkpoint() {}
}

TEST(BoundedReaderPool, ReusesOnlyExplicitlyValidatedReaders) {
  Factory factory;
  BoundedReaderPool<Reader> pool(1, 1s);
  int first;
  {
    auto lease = pool.acquire(make_reader(factory), checkpoint);
    first = lease->id;
    EXPECT_EQ(factory.closed->load(), 0);
    lease.reuse();
  }
  {
    auto lease = pool.acquire(make_reader(factory), checkpoint);
    EXPECT_EQ(lease->id, first);
    // A failed fetch, visitor, close or final validation unwinds this lease.
  }
  EXPECT_EQ(factory.closed->load(), 1);
  {
    auto lease = pool.acquire(make_reader(factory), checkpoint);
    EXPECT_NE(lease->id, first);
    lease.reuse();
  }
  EXPECT_EQ(pool.stats().active, 0u);
  EXPECT_EQ(pool.stats().created, 2u);
  EXPECT_EQ(pool.stats().discarded, 1u);
}

TEST(BoundedReaderPool, ConcurrentLeasesHaveIndependentReadersAndRespectCapacity) {
  for (const std::size_t capacity : {1u, 2u, 4u}) {
    Factory factory;
    BoundedReaderPool<Reader> pool(capacity, 2s);
    std::mutex mutex;
    std::condition_variable cv;
    std::size_t arrived = 0;
    bool release = false;
    std::vector<int> ids(capacity);
    std::vector<std::future<void>> tasks;
    for (std::size_t i = 0; i < capacity; ++i) {
      tasks.push_back(std::async(std::launch::async, [&, i] {
        auto lease = pool.acquire(make_reader(factory), checkpoint);
        std::unique_lock<std::mutex> lock(mutex);
        ids[i] = lease->id;
        ++arrived;
        cv.notify_all();
        cv.wait(lock, [&] { return release; });
        lease.reuse();
      }));
    }
    {
      std::unique_lock<std::mutex> lock(mutex);
      const auto ready = cv.wait_for(lock, 2s, [&] { return arrived == capacity; });
      EXPECT_TRUE(ready);
      EXPECT_EQ(pool.stats().active, capacity);
      EXPECT_EQ(pool.stats().peak, capacity);
      // Capacity, rather than available CPU cores, is the hard lease bound.
      EXPECT_EQ(factory.next.load(), static_cast<int>(capacity));
      release = true;
      cv.notify_all();
    }
    for (auto& task : tasks) task.get();
    std::sort(ids.begin(), ids.end());
    EXPECT_EQ(std::adjacent_find(ids.begin(), ids.end()), ids.end());
    EXPECT_EQ(pool.stats().active, 0u);
  }
}

TEST(BoundedReaderPool, SaturatedPoolTimesOutWithoutOpeningAnotherSession) {
  Factory factory;
  BoundedReaderPool<Reader> pool(1, 30ms);
  auto held = pool.acquire(make_reader(factory), checkpoint);
  EXPECT_THROW(pool.acquire(make_reader(factory), checkpoint), std::exception);
  EXPECT_EQ(factory.next.load(), 1);
  EXPECT_EQ(pool.stats().active, 1u);
  EXPECT_EQ(pool.stats().acquire_timeouts, 1u);
  held.reuse();
}

TEST(BoundedReaderPool, WaitingCancellationDoesNotDisturbAnActiveReader) {
  Factory factory;
  BoundedReaderPool<Reader> pool(1, 2s);
  auto held = pool.acquire(make_reader(factory), checkpoint);
  const auto original = held->id;
  std::promise<void> waiting;
  std::atomic<bool> cancel{false};
  std::atomic<bool> announced{false};
  auto task = std::async(std::launch::async, [&] {
    EXPECT_THROW(pool.acquire(make_reader(factory), [&] {
      if (!announced.exchange(true)) waiting.set_value();
      if (cancel.load()) throw std::runtime_error("request cancelled");
    }), std::runtime_error);
  });
  EXPECT_EQ(waiting.get_future().wait_for(1s), std::future_status::ready);
  cancel = true;
  EXPECT_EQ(task.wait_for(1s), std::future_status::ready);
  task.get();
  EXPECT_EQ(factory.next.load(), 1);
  EXPECT_EQ(held->id, original);
  EXPECT_EQ(pool.stats().active, 1u);
  EXPECT_EQ(pool.stats().acquire_timeouts, 0u);
  held.reuse();
}

TEST(BoundedReaderPool, CancelledRequestDoesNotConsumeAnIdleReader) {
  Factory factory;
  BoundedReaderPool<Reader> pool(1, 1s);
  EXPECT_THROW(pool.acquire(make_reader(factory), [] {
    throw std::runtime_error("cancelled before admission");
  }), std::runtime_error);
  EXPECT_EQ(factory.next.load(), 0);
  EXPECT_EQ(pool.stats().active, 0u);
}

TEST(BoundedReaderPool, FailedConnectionReleasesItsCapacityWithoutRetrying) {
  Factory factory;
  BoundedReaderPool<Reader> pool(1, 1s);
  int attempts = 0;
  EXPECT_THROW(pool.acquire([&]() -> std::unique_ptr<Reader> {
    ++attempts;
    throw std::runtime_error("connection failure");
  }, checkpoint), std::runtime_error);
  EXPECT_EQ(attempts, 1);
  EXPECT_EQ(pool.stats().active, 0u);
  auto lease = pool.acquire(make_reader(factory), checkpoint);
  EXPECT_NE(lease.get(), nullptr);
  lease.reuse();
}

TEST(BoundedReaderPool, MovedLeaseReleasesAndClosesExactlyOnce) {
  Factory factory;
  BoundedReaderPool<Reader> pool(1, 1s);
  {
    auto first = pool.acquire(make_reader(factory), checkpoint);
    auto second = std::move(first);
    EXPECT_EQ(pool.stats().active, 1u);
    EXPECT_NE(second.get(), nullptr);
  }
  EXPECT_EQ(pool.stats().active, 0u);
  EXPECT_EQ(factory.closed->load(), 1);
}

TEST(BoundedReaderPool, CancellationAfterConnectionOpenDiscardsTheNewReader) {
  Factory factory;
  BoundedReaderPool<Reader> pool(1, 1s);
  int calls = 0;
  EXPECT_THROW(pool.acquire(make_reader(factory), [&] {
    if (++calls == 2) throw std::runtime_error("deadline expired while connecting");
  }), std::runtime_error);
  EXPECT_EQ(factory.next.load(), 1);
  EXPECT_EQ(factory.closed->load(), 1);
  EXPECT_EQ(pool.stats().active, 0u);
  EXPECT_EQ(pool.stats().discarded, 1u);
}

TEST(BoundedReaderPool, AnOutstandingLeaseRetainsItsStateAfterPoolDestruction) {
  Factory factory;
  {
    auto pool = std::make_unique<BoundedReaderPool<Reader>>(1, 1s);
    auto lease = pool->acquire(make_reader(factory), checkpoint);
    pool.reset();
    EXPECT_EQ(factory.closed->load(), 0);
    EXPECT_NE(lease.get(), nullptr);
    lease.reuse();
  }
  EXPECT_EQ(factory.closed->load(), 1);
}

TEST(BoundedReaderPool, InvalidLimitsAreRejectedBeforeOpeningConnections) {
  EXPECT_THROW((BoundedReaderPool<Reader>(0, 1s)), std::exception);
  EXPECT_THROW((BoundedReaderPool<Reader>(257, 1s)), std::exception);
  EXPECT_THROW((BoundedReaderPool<Reader>(1, 0ms)), std::exception);
}
