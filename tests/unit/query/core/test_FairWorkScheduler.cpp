#include "iotdbfs/query/core/FairWorkScheduler.h"

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {
using namespace iotdbfs;

bool wait_for_queued(FairWorkScheduler& scheduler, std::uint64_t count) {
  const auto deadline = std::chrono::steady_clock::now() +
    std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < deadline) {
    if (scheduler.stats().queued_groups == count) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return false;
}

TEST(FairWorkSchedulerTest, RotatesQueuedWorkloadsBetweenBoundedGroups) {
  QueryRuntimeConfig config;
  config.max_active_work_groups = 1;
  config.max_requests_per_workload = 4;
  config.wait_timeout_ms = 3000;
  FairWorkScheduler scheduler(config);
  auto initial = scheduler.begin_request("A", 10);
  auto a1 = scheduler.begin_request("A", 10);
  auto a2 = scheduler.begin_request("A", 10);
  auto b = scheduler.begin_request("B", 10);
  ASSERT_TRUE(initial.ok());
  ASSERT_TRUE(a1.ok());
  ASSERT_TRUE(a2.ok());
  ASSERT_TRUE(b.ok());
  auto occupied = scheduler.acquire_group(initial.value());
  ASSERT_TRUE(occupied.ok());

  std::mutex mutex;
  std::vector<std::string> order;
  std::vector<QueryError> errors;
  auto run = [&](const char* name, const FairWorkScheduler::RequestLease& request) {
    auto turn = scheduler.acquire_group(request);
    std::lock_guard<std::mutex> lock(mutex);
    if (turn.ok()) order.emplace_back(name);
    else errors.push_back(turn.status().code());
  };
  std::thread first(run, "A1", std::cref(a1.value()));
  const bool first_queued = wait_for_queued(scheduler, 1);
  std::thread second(run, "B", std::cref(b.value()));
  const bool second_queued = wait_for_queued(scheduler, 2);
  std::thread third(run, "A2", std::cref(a2.value()));
  const bool third_queued = wait_for_queued(scheduler, 3);
  occupied.value() = FairWorkScheduler::GroupLease{};
  first.join(); second.join(); third.join();
  EXPECT_TRUE(first_queued);
  EXPECT_TRUE(second_queued);
  EXPECT_TRUE(third_queued);
  EXPECT_TRUE(errors.empty());
  EXPECT_EQ(order, (std::vector<std::string>{"A1", "B", "A2"}));
  EXPECT_EQ(scheduler.stats().peak_active_groups, 1);
}

TEST(FairWorkSchedulerTest, PerWorkloadReservationsAndCancellationAreIsolated) {
  QueryRuntimeConfig config;
  config.max_requests_per_workload = 1;
  config.max_output_bytes_per_workload = 100;
  config.wait_timeout_ms = 1000;
  FairWorkScheduler scheduler(config);
  auto a = scheduler.begin_request("A", 60);
  ASSERT_TRUE(a.ok());
  EXPECT_EQ(scheduler.begin_request("A", 101).status().code(),
    QueryError::ResourceExhausted);
  auto b = scheduler.begin_request("B", 60);
  ASSERT_TRUE(b.ok());

  CancellationToken cancellation;
  QueryError result = QueryError::None;
  std::thread blocked([&] {
    result = scheduler.begin_request("A", 60, cancellation).status().code();
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  cancellation.cancel();
  blocked.join();
  EXPECT_EQ(result, QueryError::Cancelled);
  auto b_turn = scheduler.acquire_group(b.value());
  EXPECT_TRUE(b_turn.ok());
}
} // namespace
