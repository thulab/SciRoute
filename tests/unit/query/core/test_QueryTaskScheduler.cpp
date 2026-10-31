#include "iotdbfs/query/core/QueryTaskScheduler.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryRuntime.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace {
using namespace iotdbfs;
using Scheduler = QueryTaskScheduler;
using namespace std::chrono_literals;

// Arrivals prove concurrent task execution, independently of task durations.
class TaskGate {
public:
  void arrive_and_wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    ++arrivals_;
    changed_.notify_all();
    changed_.wait(lock, [&] { return open_; });
  }
  bool wait_for_arrivals(std::size_t count) {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, 5s, [&] { return arrivals_ >= count; });
  }
  void open() {
    std::lock_guard<std::mutex> lock(mutex_);
    open_ = true;
    changed_.notify_all();
  }
private:
  std::mutex mutex_;
  std::condition_variable changed_;
  std::size_t arrivals_{0};
  bool open_{false};
};

// Release blocked tasks even when an ASSERT exits a test before normal cleanup.
struct OpenGateOnExit {
  std::shared_ptr<TaskGate> gate;
  Scheduler* scheduler;
  ~OpenGateOnExit() {
    gate->open();
    scheduler->shutdown();
  }
};

Scheduler::Submission success_task() {
  return {[](const CancellationToken&) { return QueryStatus::success(); }};
}

Scheduler::Submission blocking_task(const std::shared_ptr<TaskGate>& gate) {
  return {[gate](const CancellationToken&) {
    gate->arrive_and_wait();
    return QueryStatus::success();
  }};
}

TEST(QueryTaskSchedulerTest, ExecutesOneTwoAndFourTasksOnActualWorkerThreads) {
  const auto caller = std::this_thread::get_id();
  for (const std::size_t workers : {1U, 2U, 4U}) {
    SCOPED_TRACE(workers);
    QueryTaskSchedulerConfig config;
    config.worker_threads = workers;
    config.max_batch_size = workers;
    Scheduler scheduler(config);
    auto gate = std::make_shared<TaskGate>();
    std::atomic<std::size_t> on_worker{0};
    OpenGateOnExit cleanup{gate, &scheduler};
    std::vector<Scheduler::Submission> tasks;
    for (std::size_t i = 0; i < workers; ++i) {
      tasks.push_back({[&, gate](const CancellationToken&) {
        if (std::this_thread::get_id() != caller) ++on_worker;
        gate->arrive_and_wait();
        return QueryStatus::success();
      }});
    }
    auto batch = scheduler.submit_batch(std::move(tasks));
    ASSERT_TRUE(batch.ok());
    EXPECT_TRUE(gate->wait_for_arrivals(workers));
    EXPECT_EQ(on_worker.load(), workers);
    EXPECT_EQ(scheduler.stats().active, workers);
    EXPECT_EQ(scheduler.stats().peak_active, workers);
    gate->open();
    for (auto& future : batch.value()) EXPECT_TRUE(future.get().ok());
    scheduler.shutdown();
    const auto stats = scheduler.stats();
    EXPECT_EQ(stats.submitted, workers);
    EXPECT_EQ(stats.completed, workers);
    EXPECT_EQ(stats.dispatched_tasks, workers);
    EXPECT_EQ(stats.active, 0U);
    EXPECT_EQ(stats.queued, 0U);
  }
}

TEST(QueryTaskSchedulerTest, DispatchesAnIncompleteBatchWithoutWaitingForMoreTasks) {
  Scheduler scheduler;
  auto submitted = scheduler.submit(success_task());
  ASSERT_TRUE(submitted.ok());
  ASSERT_EQ(submitted.value().wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(submitted.value().get().ok());
  scheduler.shutdown();
  EXPECT_EQ(scheduler.stats().max_dispatch_batch, 1U);
}

TEST(QueryTaskSchedulerTest, ReusesTheOnlyFreeWorkerWhileThreeWorkersRemainBlocked) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 4;
  Scheduler scheduler(config);
  auto occupied = std::make_shared<TaskGate>();
  OpenGateOnExit cleanup{occupied, &scheduler};
  auto pinned = scheduler.submit_batch(std::vector<Scheduler::Submission>(
    3, blocking_task(occupied)));
  ASSERT_TRUE(pinned.ok());
  ASSERT_TRUE(occupied->wait_for_arrivals(3));
  std::thread::id free_worker;
  // Each next arrival happens only after the preceding completion is visible.
  // The other three workers cannot consume a notification for this free slot.
  for (unsigned round = 0; round < 32; ++round) {
    SCOPED_TRACE(round);
    auto next = scheduler.submit({[&, round](const CancellationToken&) {
      if (round == 0) free_worker = std::this_thread::get_id();
      EXPECT_EQ(std::this_thread::get_id(), free_worker);
      return QueryStatus::success();
    }});
    ASSERT_TRUE(next.ok());
    ASSERT_EQ(next.value().wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(next.value().get().ok());
    EXPECT_EQ(scheduler.stats().active, 3U);
  }
  occupied->open();
  for (auto& future : pinned.value()) EXPECT_TRUE(future.get().ok());
  scheduler.shutdown();
  EXPECT_EQ(scheduler.stats().completed, 35U);
  EXPECT_EQ(scheduler.stats().active, 0U);
}

TEST(QueryTaskSchedulerTest, ReusesAllFourWorkersAcrossSuccessiveReadyBatches) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 4;
  Scheduler scheduler(config);
  std::vector<std::shared_ptr<TaskGate>> gates;
  for (unsigned round = 0; round < 8; ++round)
    gates.push_back(std::make_shared<TaskGate>());
  struct Cleanup {
    Scheduler& scheduler;
    std::vector<std::shared_ptr<TaskGate>>& gates;
    ~Cleanup() {
      for (const auto& gate : gates) gate->open();
      scheduler.shutdown();
    }
  } cleanup{scheduler, gates};
  for (unsigned round = 0; round < gates.size(); ++round) {
    SCOPED_TRACE(round);
    auto batch = scheduler.submit_batch(std::vector<Scheduler::Submission>(
      4, blocking_task(gates[round])));
    ASSERT_TRUE(batch.ok());
    ASSERT_TRUE(gates[round]->wait_for_arrivals(4));
    EXPECT_EQ(scheduler.stats().active, 4U);
    gates[round]->open();
    for (auto& future : batch.value()) {
      ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
      EXPECT_TRUE(future.get().ok());
    }
    EXPECT_EQ(scheduler.stats().active, 0U);
  }
  scheduler.shutdown();
  EXPECT_EQ(scheduler.stats().completed, 32U);
  EXPECT_EQ(scheduler.stats().dispatched_tasks, 32U);
}

TEST(QueryTaskSchedulerTest, ShutdownWakesEveryIdleWorkerIncludingDuringStartup) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 4;
  for (unsigned attempt = 0; attempt < 8; ++attempt) {
    SCOPED_TRACE(attempt);
    Scheduler scheduler(config);
    // No submitted work is needed to make the dispatcher finish its drain.
    scheduler.shutdown();
    scheduler.shutdown();
    const auto stats = scheduler.stats();
    EXPECT_EQ(stats.submitted, 0U);
    EXPECT_EQ(stats.completed, 0U);
    EXPECT_EQ(stats.active, 0U);
    EXPECT_EQ(scheduler.submit(success_task()).status().code(),
      QueryError::ResourceExhausted);
  }
}

TEST(QueryTaskSchedulerTest, DispatchesAtomicReadyBatchWithinConfiguredBatchLimit) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = 4;
  config.max_batch_size = 2;
  Scheduler scheduler(config);
  auto gate = std::make_shared<TaskGate>();
  OpenGateOnExit cleanup{gate, &scheduler};
  std::vector<Scheduler::Submission> tasks(4, blocking_task(gate));
  auto batch = scheduler.submit_batch(std::move(tasks));
  ASSERT_TRUE(batch.ok());
  EXPECT_TRUE(gate->wait_for_arrivals(4));
  const auto busy = scheduler.stats();
  EXPECT_EQ(busy.dispatched_tasks, 4U);
  EXPECT_EQ(busy.dispatch_batches, 2U);
  EXPECT_EQ(busy.max_dispatch_batch, 2U);
  EXPECT_EQ(busy.active, 4U);
  gate->open();
  for (auto& future : batch.value()) EXPECT_TRUE(future.get().ok());
}

TEST(QueryTaskSchedulerTest, BoundsPendingQueueAndRejectsTheWholeExcessBatch) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  config.max_queued_tasks = 2;
  Scheduler scheduler(config);
  auto gate = std::make_shared<TaskGate>();
  std::atomic<unsigned> executed{0};
  OpenGateOnExit cleanup{gate, &scheduler};
  auto running = scheduler.submit(blocking_task(gate));
  ASSERT_TRUE(running.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  const Scheduler::Submission counted{[&](const CancellationToken&) {
    ++executed;
    return QueryStatus::success();
  }};
  auto queued = scheduler.submit(counted);
  ASSERT_TRUE(queued.ok());
  auto rejected = scheduler.submit_batch({counted, counted});
  EXPECT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(scheduler.stats().queued, 1U);
  EXPECT_EQ(scheduler.stats().submitted, 2U);
  // The failed batch must leave its available queue position usable.
  auto last = scheduler.submit(counted);
  ASSERT_TRUE(last.ok());
  EXPECT_EQ(scheduler.stats().queued, 2U);
  auto overflow = scheduler.submit(counted);
  EXPECT_EQ(overflow.status().code(), QueryError::ResourceExhausted);
  gate->open();
  EXPECT_TRUE(running.value().get().ok());
  EXPECT_TRUE(queued.value().get().ok());
  EXPECT_TRUE(last.value().get().ok());
  scheduler.shutdown();
  EXPECT_EQ(executed.load(), 2U);
  EXPECT_EQ(scheduler.stats().rejected, 3U);
}

TEST(QueryTaskSchedulerTest, RejectsBatchLargerThanQueueEvenWhenAllWorkersAreIdle) {
  QueryTaskSchedulerConfig config;
  config.max_queued_tasks = 1;
  Scheduler scheduler(config);
  auto rejected = scheduler.submit_batch({success_task(), success_task()});
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  scheduler.shutdown();
  EXPECT_EQ(scheduler.stats().submitted, 0U);
  EXPECT_EQ(scheduler.stats().dispatched_tasks, 0U);
  EXPECT_EQ(scheduler.stats().rejected, 2U);
}

TEST(QueryTaskSchedulerTest, PreservesFifoOrderWithOneExecutionSlot) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  Scheduler scheduler(config);
  auto gate = std::make_shared<TaskGate>();
  std::vector<unsigned> order;
  OpenGateOnExit cleanup{gate, &scheduler};
  auto running = scheduler.submit(blocking_task(gate));
  ASSERT_TRUE(running.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  std::vector<Scheduler::Submission> tasks;
  for (unsigned i = 0; i < 8; ++i) {
    tasks.push_back({[&, i](const CancellationToken&) {
      order.push_back(i);
      return QueryStatus::success();
    }});
  }
  auto queued = scheduler.submit_batch(std::move(tasks));
  ASSERT_TRUE(queued.ok());
  gate->open();
  EXPECT_TRUE(running.value().get().ok());
  for (auto& future : queued.value()) EXPECT_TRUE(future.get().ok());
  EXPECT_EQ(order, (std::vector<unsigned>{0, 1, 2, 3, 4, 5, 6, 7}));
}

TEST(QueryTaskSchedulerTest, SkipsQueuedCancelledAndExpiredTasksWithoutCallingThem) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  Scheduler scheduler(config);
  auto gate = std::make_shared<TaskGate>();
  std::atomic<unsigned> invoked{0};
  OpenGateOnExit cleanup{gate, &scheduler};
  auto running = scheduler.submit(blocking_task(gate));
  ASSERT_TRUE(running.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  Scheduler::Submission cancelled{[&](const CancellationToken&) {
    ++invoked;
    return QueryStatus::success();
  }};
  auto token = cancelled.cancellation;
  Scheduler::Submission expired = cancelled;
  expired.cancellation = CancellationToken{};
  expired.deadline = Scheduler::Clock::now() - 1s;
  auto queued = scheduler.submit_batch({cancelled, expired});
  ASSERT_TRUE(queued.ok());
  token.cancel();
  gate->open();
  EXPECT_TRUE(running.value().get().ok());
  EXPECT_EQ(queued.value()[0].get().code(), QueryError::Cancelled);
  EXPECT_EQ(queued.value()[1].get().code(), QueryError::DeadlineExceeded);
  scheduler.shutdown();
  EXPECT_EQ(invoked.load(), 0U);
  const auto stats = scheduler.stats();
  EXPECT_EQ(stats.completed, 3U);
  EXPECT_EQ(stats.failed, 2U);
  EXPECT_EQ(stats.cancelled, 1U);
  EXPECT_EQ(stats.expired, 1U);
}

TEST(QueryTaskSchedulerTest, RunningTaskReceivesSharedCancellationSignal) {
  Scheduler scheduler;
  auto gate = std::make_shared<TaskGate>();
  OpenGateOnExit cleanup{gate, &scheduler};
  Scheduler::Submission task{[gate](const CancellationToken& cancellation) {
    gate->arrive_and_wait();
    return cancellation.cancelled()
      ? QueryStatus::failure(QueryError::Cancelled, QueryPhase::Storage,
          "cancelled in request storage scope")
      : QueryStatus::success();
  }};
  const auto cancellation = task.cancellation;
  auto submitted = scheduler.submit(std::move(task));
  ASSERT_TRUE(submitted.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  cancellation.cancel();
  gate->open();
  const auto result = submitted.value().get();
  EXPECT_EQ(result.code(), QueryError::Cancelled);
  EXPECT_EQ(result.phase(), QueryPhase::Storage);
  EXPECT_EQ(result.message(), "cancelled in request storage scope");
}

TEST(QueryTaskSchedulerTest, DoesNotRewriteSuccessfulRunningTaskAfterCancellation) {
  Scheduler scheduler;
  auto gate = std::make_shared<TaskGate>();
  OpenGateOnExit cleanup{gate, &scheduler};
  auto task = blocking_task(gate);
  const auto cancellation = task.cancellation;
  auto submitted = scheduler.submit(std::move(task));
  ASSERT_TRUE(submitted.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  cancellation.cancel();
  gate->open();
  EXPECT_TRUE(submitted.value().get().ok());
  scheduler.shutdown();
  EXPECT_EQ(scheduler.stats().cancelled, 0U);
  EXPECT_EQ(scheduler.stats().failed, 0U);
}

TEST(QueryTaskSchedulerTest, PreservesQueryErrorDetails) {
  Scheduler scheduler;
  QueryErrorDetail detail;
  detail.source = QuerySourceReference{"/science/data", 42, "arrow"};
  detail.field_id = "temperature";
  detail.operation = "decode";
  detail.retriable = true;
  const auto expected = QueryStatus::failure(QueryError::CorruptPayload,
    QueryPhase::Storage, "payload digest mismatch", detail);
  auto submitted = scheduler.submit({[expected](const CancellationToken&) {
    return expected;
  }});
  ASSERT_TRUE(submitted.ok());
  const auto actual = submitted.value().get();
  EXPECT_EQ(actual.code(), expected.code());
  EXPECT_EQ(actual.phase(), expected.phase());
  EXPECT_EQ(actual.message(), expected.message());
  ASSERT_TRUE(actual.detail().source.has_value());
  EXPECT_EQ(actual.detail().source->path, "/science/data");
  EXPECT_EQ(actual.detail().source->version, 42);
  EXPECT_EQ(actual.detail().source->format_id, "arrow");
  EXPECT_EQ(actual.detail().field_id, detail.field_id);
  EXPECT_EQ(actual.detail().operation, detail.operation);
  EXPECT_TRUE(actual.detail().retriable);
}

TEST(QueryTaskSchedulerTest, ConvertsExceptionsAndContinuesServingOtherRequests) {
  Scheduler scheduler;
  auto submitted = scheduler.submit_batch({
    {[](const CancellationToken&) -> QueryStatus {
      throw std::runtime_error("decode exception");
    }},
    {[](const CancellationToken&) -> QueryStatus { throw 7; }},
    success_task()});
  ASSERT_TRUE(submitted.ok());
  const auto standard = submitted.value()[0].get();
  EXPECT_EQ(standard.code(), QueryError::Internal);
  EXPECT_EQ(standard.phase(), QueryPhase::Execution);
  EXPECT_EQ(standard.message(), "decode exception");
  EXPECT_EQ(submitted.value()[1].get().code(), QueryError::Internal);
  EXPECT_TRUE(submitted.value()[2].get().ok());
  scheduler.shutdown();
  EXPECT_EQ(scheduler.stats().completed, 3U);
  EXPECT_EQ(scheduler.stats().failed, 2U);
}

TEST(QueryTaskSchedulerTest, RejectsAnEntireBatchContainingAnEmptyTask) {
  std::atomic<unsigned> invoked{0};
  Scheduler scheduler;
  Scheduler::Submission valid{[&](const CancellationToken&) {
    ++invoked;
    return QueryStatus::success();
  }};
  auto submitted = scheduler.submit_batch({valid, Scheduler::Submission{}});
  EXPECT_EQ(submitted.status().code(), QueryError::InvalidQuery);
  scheduler.shutdown();
  EXPECT_EQ(invoked.load(), 0U);
  EXPECT_EQ(scheduler.stats().submitted, 0U);
}

TEST(QueryTaskSchedulerTest, ReleasesRequestCapturesBeforeCompletionBecomesVisible) {
  Scheduler scheduler;
  auto lease = std::make_shared<int>(42);
  const std::weak_ptr<int> observed = lease;
  auto submitted = scheduler.submit({[lease](const CancellationToken&) {
    return *lease == 42 ? QueryStatus::success() :
      QueryStatus::failure(QueryError::Internal, QueryPhase::Execution,
        "request lease was not retained");
  }});
  ASSERT_TRUE(submitted.ok());
  lease.reset();
  EXPECT_TRUE(submitted.value().get().ok());
  EXPECT_TRUE(observed.expired());
}

TEST(QueryTaskSchedulerTest, ShutdownDrainsAcceptedTasksAndRejectsNewRequests) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  Scheduler scheduler(config);
  auto gate = std::make_shared<TaskGate>();
  OpenGateOnExit cleanup{gate, &scheduler};
  auto running = scheduler.submit(blocking_task(gate));
  ASSERT_TRUE(running.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  auto queued = scheduler.submit_batch({success_task(), success_task()});
  ASSERT_TRUE(queued.ok());
  std::thread stop([&] { scheduler.shutdown(); });
  gate->open();
  stop.join();
  EXPECT_TRUE(running.value().get().ok());
  for (auto& future : queued.value()) {
    ASSERT_EQ(future.wait_for(0s), std::future_status::ready);
    EXPECT_TRUE(future.get().ok());
  }
  EXPECT_EQ(scheduler.stats().completed, 3U);
  EXPECT_EQ(scheduler.stats().active, 0U);
  EXPECT_EQ(scheduler.stats().queued, 0U);
  EXPECT_EQ(scheduler.submit(success_task()).status().code(),
    QueryError::ResourceExhausted);
  scheduler.shutdown();
}

TEST(QueryTaskSchedulerTest, DestructorDrainsAcceptedTasks) {
  std::atomic<unsigned> completed{0};
  std::vector<std::future<QueryStatus>> futures;
  {
    Scheduler scheduler;
    for (unsigned i = 0; i < 12; ++i) {
      auto task = scheduler.submit({[&](const CancellationToken&) {
        ++completed;
        return QueryStatus::success();
      }});
      ASSERT_TRUE(task.ok());
      futures.push_back(std::move(task.value()));
    }
  }
  EXPECT_EQ(completed.load(), 12U);
  for (auto& future : futures) {
    ASSERT_EQ(future.wait_for(0s), std::future_status::ready);
    EXPECT_TRUE(future.get().ok());
  }
}

TEST(QueryTaskSchedulerTest, DoesNotInheritCallerProfileAndRestoresWorkerScopes) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  QueryProfileRecord caller_profile;
  QueryProfileRecord request_profile;
  ScopedQueryProfile caller_scope(&caller_profile);
  Scheduler scheduler(config);
  auto submitted = scheduler.submit({[&](const CancellationToken&) {
    if (active_query_profile != nullptr)
      return QueryStatus::failure(QueryError::Internal,
        QueryPhase::Execution, "caller profile leaked into worker");
    {
      ScopedQueryProfile request_scope(&request_profile);
      QueryProfileTimer timer("task");
    }
    return active_query_profile == nullptr ? QueryStatus::success() :
      QueryStatus::failure(QueryError::Internal, QueryPhase::Execution,
        "request scope leaked into worker");
  }});
  ASSERT_TRUE(submitted.ok());
  EXPECT_TRUE(submitted.value().get().ok());
  auto next = scheduler.submit({[](const CancellationToken&) {
    return active_query_profile == nullptr ? QueryStatus::success() :
      QueryStatus::failure(QueryError::Internal, QueryPhase::Execution,
        "profile leaked between requests");
  }});
  ASSERT_TRUE(next.ok());
  EXPECT_TRUE(next.value().get().ok());
  EXPECT_TRUE(caller_profile.nanoseconds.empty());
  EXPECT_EQ(request_profile.nanoseconds.count("task"), 1U);
  EXPECT_EQ(active_query_profile, &caller_profile);
}

TEST(QueryTaskSchedulerTest, RuntimeOwnsOneSharedPoolWithIndependentCacheWorkerLimit) {
  QueryRuntimeConfig config;
  config.worker_threads = 1;  // Decoded fragment producers are a separate pool.
  config.query_tasks.worker_threads = config.query_tasks.max_batch_size = 2;
  QueryRuntime runtime(config);
  auto gate = std::make_shared<TaskGate>();
  OpenGateOnExit cleanup{gate, &runtime.query_tasks()};
  auto& first_client = runtime.query_tasks();
  auto& second_client = runtime.query_tasks();
  EXPECT_EQ(&first_client, &second_client);
  auto first = first_client.submit(blocking_task(gate));
  auto second = second_client.submit(blocking_task(gate));
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  EXPECT_TRUE(gate->wait_for_arrivals(2));
  EXPECT_EQ(runtime.query_tasks().stats().active, 2U);
  EXPECT_EQ(runtime.config().worker_threads, 1U);
  gate->open();
  EXPECT_TRUE(first.value().get().ok());
  EXPECT_TRUE(second.value().get().ok());
}

TEST(QueryTaskSchedulerTest, RejectsInvalidConfiguration) {
  for (const std::size_t workers : {0U, 257U}) {
    QueryTaskSchedulerConfig config;
    config.worker_threads = workers;
    EXPECT_THROW(Scheduler scheduler(config), std::invalid_argument);
  }
  for (const std::size_t queued : {0U, 65537U}) {
    QueryTaskSchedulerConfig config;
    config.max_queued_tasks = queued;
    EXPECT_THROW(Scheduler scheduler(config), std::invalid_argument);
  }
  for (const std::size_t batch : {0U, 5U}) {
    QueryTaskSchedulerConfig config;
    config.max_batch_size = batch;
    EXPECT_THROW(Scheduler scheduler(config), std::invalid_argument);
  }
}

template <class Predicate>
bool wait_until(Predicate predicate) {
  const auto deadline = Scheduler::Clock::now() + 5s;
  while (!predicate()) {
    if (Scheduler::Clock::now() >= deadline) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

Scheduler::Submission resource_task(
    Scheduler::Submission task, const std::shared_ptr<QueryTaskResource>& resource,
    std::size_t units = 1) {
  task.resources.push_back({resource, units});
  return task;
}

// A failed assertion must release external resource holders before draining.
struct ResourceCleanup {
  Scheduler* scheduler;
  std::shared_ptr<TaskGate> gate;
  QueryTaskResource::Lease* external{nullptr};
  std::shared_ptr<TaskGate> second_gate;
  ~ResourceCleanup() {
    if (external) *external = {};
    if (gate) gate->open();
    if (second_gate) second_gate->open();
    scheduler->shutdown();
  }
};

TEST(QueryTaskResourceTest, CountsCapacityAndTransfersLeaseAcrossThreads) {
  static_assert(!std::is_copy_constructible<QueryTaskResource::Lease>::value,
    "resource leases must have one owner");
  auto resource = std::make_shared<QueryTaskResource>(3);
  EXPECT_EQ(resource->capacity(), 3U);
  EXPECT_EQ(resource->in_use(), 0U);
  auto first = resource->try_acquire(2);
  ASSERT_TRUE(first);
  EXPECT_EQ(resource->in_use(), 2U);
  EXPECT_FALSE(resource->try_acquire(2));
  auto second = resource->try_acquire();
  ASSERT_TRUE(second);
  EXPECT_EQ(resource->in_use(), 3U);
  std::thread release([lease = std::move(first)]() mutable { lease = {}; });
  release.join();
  EXPECT_FALSE(first);
  EXPECT_EQ(resource->in_use(), 1U);
  second = {};
  EXPECT_EQ(resource->in_use(), 0U);
  auto all = resource->try_acquire(3);
  ASSERT_TRUE(all);
}

TEST(QueryTaskResourceTest, LeaseKeepsAccountingAliveAfterResourceOwnerIsDestroyed) {
  QueryTaskResource::Lease lease;
  {
    QueryTaskResource resource(1);
    lease = resource.try_acquire();
    ASSERT_TRUE(lease);
  }
  std::thread release([held = std::move(lease)]() mutable { held = {}; });
  release.join();
  EXPECT_FALSE(lease);
}

TEST(QueryTaskSchedulerTest, ResourceBlockedHdfTasksLeaveArrowRunnableWithOneTwoAndFourSlots) {
  for (const std::size_t workers : {1U, 2U, 4U}) {
    SCOPED_TRACE(workers);
    QueryTaskSchedulerConfig config;
    config.worker_threads = config.max_batch_size = workers;
    Scheduler scheduler(config);
    auto hdf_resource = std::make_shared<QueryTaskResource>(1);
    auto external_hdf_lock = hdf_resource->try_acquire();
    ASSERT_TRUE(external_hdf_lock);
    auto hdf_gate = std::make_shared<TaskGate>();
    ResourceCleanup cleanup{&scheduler, hdf_gate, &external_hdf_lock};
    std::atomic<unsigned> hdf_entered{0}, arrow_completed{0};
    std::vector<Scheduler::Submission> tasks;
    for (unsigned i = 0; i < 4; ++i) {
      tasks.push_back(resource_task({[&, hdf_gate](const CancellationToken&) {
        ++hdf_entered;
        hdf_gate->arrive_and_wait();
        return QueryStatus::success();
      }}, hdf_resource));
    }
    tasks.push_back({[&](const CancellationToken&) {
      ++arrow_completed;
      return QueryStatus::success();
    }});
    auto batch = scheduler.submit_batch(std::move(tasks));
    ASSERT_TRUE(batch.ok());
    ASSERT_EQ(batch.value()[4].wait_for(5s), std::future_status::ready);
    EXPECT_TRUE(batch.value()[4].get().ok());
    EXPECT_EQ(arrow_completed.load(), 1U);
    EXPECT_EQ(hdf_entered.load(), 0U);
    ASSERT_TRUE(wait_until([&] { return scheduler.stats().resource_blocked == 4; }));
    const auto waiting = scheduler.stats();
    EXPECT_EQ(waiting.active, 0U);
    EXPECT_EQ(waiting.queued, 4U);
    EXPECT_EQ(waiting.resource_waits, 4U);
    EXPECT_EQ(waiting.resource_bypasses, 1U);
    external_hdf_lock = {};
    ASSERT_TRUE(hdf_gate->wait_for_arrivals(1));
    EXPECT_EQ(hdf_resource->in_use(), 1U);
    EXPECT_EQ(hdf_entered.load(), 1U);
    hdf_gate->open();
    for (unsigned i = 0; i < 4; ++i) EXPECT_TRUE(batch.value()[i].get().ok());
    scheduler.shutdown();
    EXPECT_EQ(hdf_resource->in_use(), 0U);
    EXPECT_EQ(scheduler.stats().resource_blocked, 0U);
    EXPECT_EQ(scheduler.stats().completed, 5U);
  }
}

TEST(QueryTaskSchedulerTest, CapacityLimitsConcurrentTasksWithoutOccupyingOtherSlots) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 4;
  Scheduler scheduler(config);
  auto resource = std::make_shared<QueryTaskResource>(2);
  auto gate = std::make_shared<TaskGate>();
  ResourceCleanup cleanup{&scheduler, gate};
  std::atomic<unsigned> executing{0}, peak_executing{0};
  std::vector<Scheduler::Submission> tasks;
  for (unsigned i = 0; i < 8; ++i) {
    tasks.push_back(resource_task({[&, gate](const CancellationToken&) {
      const auto active = ++executing;
      auto previous = peak_executing.load();
      while (previous < active && !peak_executing.compare_exchange_weak(previous, active)) {}
      gate->arrive_and_wait();
      --executing;
      return QueryStatus::success();
    }}, resource));
  }
  auto batch = scheduler.submit_batch(std::move(tasks));
  ASSERT_TRUE(batch.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(2));
  ASSERT_TRUE(wait_until([&] { return scheduler.stats().resource_blocked == 6; }));
  EXPECT_EQ(resource->in_use(), 2U);
  EXPECT_EQ(scheduler.stats().active, 2U);
  EXPECT_EQ(scheduler.stats().queued, 6U);
  gate->open();
  for (auto& future : batch.value()) EXPECT_TRUE(future.get().ok());
  scheduler.shutdown();
  EXPECT_EQ(resource->in_use(), 0U);
  EXPECT_EQ(peak_executing.load(), 2U);
}

TEST(QueryTaskSchedulerTest, IndependentResourcesRunConcurrently) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 2;
  Scheduler scheduler(config);
  auto first = std::make_shared<QueryTaskResource>(1);
  auto second = std::make_shared<QueryTaskResource>(1);
  auto gate = std::make_shared<TaskGate>();
  ResourceCleanup cleanup{&scheduler, gate};
  auto batch = scheduler.submit_batch({resource_task(blocking_task(gate), first),
    resource_task(blocking_task(gate), second)});
  ASSERT_TRUE(batch.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(2));
  EXPECT_EQ(first->in_use(), 1U);
  EXPECT_EQ(second->in_use(), 1U);
  EXPECT_EQ(scheduler.stats().active, 2U);
  gate->open();
  for (auto& future : batch.value()) EXPECT_TRUE(future.get().ok());
  scheduler.shutdown();
  EXPECT_EQ(first->in_use(), 0U);
  EXPECT_EQ(second->in_use(), 0U);
}

TEST(QueryTaskSchedulerTest, FailedMultiResourceAcquisitionRollsBackEveryEarlierLease) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  Scheduler scheduler(config);
  auto first = std::make_shared<QueryTaskResource>(1);
  auto second = std::make_shared<QueryTaskResource>(1);
  auto external = second->try_acquire();
  auto gate = std::make_shared<TaskGate>();
  ResourceCleanup cleanup{&scheduler, gate, &external};
  auto both = resource_task(resource_task(success_task(), first), second);
  auto first_only = resource_task(blocking_task(gate), first);
  auto batch = scheduler.submit_batch({both, first_only});
  ASSERT_TRUE(batch.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  EXPECT_EQ(first->in_use(), 1U);
  EXPECT_EQ(batch.value()[0].wait_for(0s), std::future_status::timeout);
  EXPECT_EQ(scheduler.stats().resource_bypasses, 1U);
  gate->open();
  EXPECT_TRUE(batch.value()[1].get().ok());
  external = {};
  ASSERT_EQ(batch.value()[0].wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(batch.value()[0].get().ok());
  scheduler.shutdown();
  EXPECT_EQ(first->in_use(), 0U);
  EXPECT_EQ(second->in_use(), 0U);
}

TEST(QueryTaskSchedulerTest, ResourceLeasesAreReturnedBeforeExceptionalCompletionIsVisible) {
  Scheduler scheduler;
  auto resource = std::make_shared<QueryTaskResource>(1);
  auto throwing = resource_task({[](const CancellationToken&) -> QueryStatus {
    throw std::runtime_error("resource guarded failure");
  }}, resource);
  auto failed = scheduler.submit(std::move(throwing));
  ASSERT_TRUE(failed.ok());
  const auto status = failed.value().get();
  EXPECT_EQ(status.code(), QueryError::Internal);
  EXPECT_EQ(status.message(), "resource guarded failure");
  EXPECT_EQ(resource->in_use(), 0U);
  auto next = scheduler.submit(resource_task(success_task(), resource));
  ASSERT_TRUE(next.ok());
  EXPECT_TRUE(next.value().get().ok());
  EXPECT_EQ(resource->in_use(), 0U);
}

TEST(QueryTaskSchedulerTest, ReleasesResourceProbeCapturesBeforeCompletionBecomesVisible) {
  Scheduler scheduler;
  auto request_lease = std::make_shared<int>(42);
  const std::weak_ptr<int> observed = request_lease;
  auto resource = std::make_shared<QueryTaskResource>(1,
    [request_lease] { return *request_lease == 42; });
  auto submitted = scheduler.submit(resource_task(success_task(), resource));
  ASSERT_TRUE(submitted.ok());
  request_lease.reset();
  resource.reset();
  EXPECT_TRUE(submitted.value().get().ok());
  EXPECT_TRUE(observed.expired());
}

TEST(QueryTaskSchedulerTest, RunningCancellationReturnsItsResourceAndPreservesTaskError) {
  Scheduler scheduler;
  auto resource = std::make_shared<QueryTaskResource>(1);
  auto gate = std::make_shared<TaskGate>();
  ResourceCleanup cleanup{&scheduler, gate};
  auto task = resource_task({[gate](const CancellationToken& token) {
    gate->arrive_and_wait();
    return token.cancelled() ? QueryStatus::failure(QueryError::Cancelled,
      QueryPhase::Storage, "resource read cancelled") : QueryStatus::success();
  }}, resource);
  const auto token = task.cancellation;
  auto submitted = scheduler.submit(std::move(task));
  ASSERT_TRUE(submitted.ok());
  ASSERT_TRUE(gate->wait_for_arrivals(1));
  EXPECT_EQ(resource->in_use(), 1U);
  token.cancel();
  gate->open();
  const auto status = submitted.value().get();
  EXPECT_EQ(status.code(), QueryError::Cancelled);
  EXPECT_EQ(status.phase(), QueryPhase::Storage);
  EXPECT_EQ(status.message(), "resource read cancelled");
  EXPECT_EQ(resource->in_use(), 0U);
}

TEST(QueryTaskSchedulerTest, ResourceBlockedCancelledAndExpiredTasksCompleteWithoutResources) {
  Scheduler scheduler;
  auto resource = std::make_shared<QueryTaskResource>(1);
  auto external = resource->try_acquire();
  ResourceCleanup cleanup{&scheduler, {}, &external};
  std::atomic<unsigned> invoked{0};
  auto cancelled = resource_task({[&](const CancellationToken&) {
    ++invoked;
    return QueryStatus::success();
  }}, resource);
  auto token = cancelled.cancellation;
  auto expired = cancelled;
  expired.cancellation = CancellationToken{};
  expired.deadline = Scheduler::Clock::now() + 100ms;
  auto batch = scheduler.submit_batch({cancelled, expired});
  ASSERT_TRUE(batch.ok());
  ASSERT_TRUE(wait_until([&] { return scheduler.stats().resource_waits == 2; }));
  token.cancel();
  ASSERT_EQ(batch.value()[0].wait_for(5s), std::future_status::ready);
  EXPECT_EQ(batch.value()[0].get().code(), QueryError::Cancelled);
  ASSERT_EQ(batch.value()[1].wait_for(5s), std::future_status::ready);
  EXPECT_EQ(batch.value()[1].get().code(), QueryError::DeadlineExceeded);
  EXPECT_EQ(invoked.load(), 0U);
  EXPECT_EQ(resource->in_use(), 1U);
  EXPECT_EQ(scheduler.stats().active, 0U);
  EXPECT_EQ(scheduler.stats().queued, 0U);
  EXPECT_EQ(scheduler.stats().resource_blocked, 0U);
  EXPECT_EQ(scheduler.stats().cancelled, 1U);
  EXPECT_EQ(scheduler.stats().expired, 1U);
}

TEST(QueryTaskSchedulerTest, ResourceBlockedTasksStillConsumeBoundedQueueCapacity) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  config.max_queued_tasks = 2;
  Scheduler scheduler(config);
  auto resource = std::make_shared<QueryTaskResource>(1);
  auto external = resource->try_acquire();
  ResourceCleanup cleanup{&scheduler, {}, &external};
  auto waiting = scheduler.submit(resource_task(success_task(), resource));
  ASSERT_TRUE(waiting.ok());
  ASSERT_TRUE(wait_until([&] { return scheduler.stats().resource_waits == 1; }));
  std::atomic<unsigned> rejected_body{0};
  Scheduler::Submission counted{[&](const CancellationToken&) {
    ++rejected_body;
    return QueryStatus::success();
  }};
  auto rejected = scheduler.submit_batch({counted, counted});
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(scheduler.stats().queued, 1U);
  EXPECT_EQ(scheduler.stats().submitted, 1U);
  auto runnable = scheduler.submit(success_task());
  ASSERT_TRUE(runnable.ok());
  ASSERT_EQ(runnable.value().wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(runnable.value().get().ok());
  external = {};
  EXPECT_TRUE(waiting.value().get().ok());
  scheduler.shutdown();
  EXPECT_EQ(rejected_body.load(), 0U);
  EXPECT_EQ(scheduler.stats().rejected, 2U);
}

TEST(QueryTaskSchedulerTest, ShutdownDrainsResourceWaitersAfterExternalRelease) {
  Scheduler scheduler;
  auto resource = std::make_shared<QueryTaskResource>(1);
  auto external = resource->try_acquire();
  ResourceCleanup cleanup{&scheduler, {}, &external};
  auto batch = scheduler.submit_batch({resource_task(success_task(), resource),
    resource_task(success_task(), resource)});
  ASSERT_TRUE(batch.ok());
  ASSERT_TRUE(wait_until([&] { return scheduler.stats().resource_waits == 2; }));
  std::thread stop([&] { scheduler.shutdown(); });
  external = {};
  stop.join();
  for (auto& future : batch.value()) {
    ASSERT_EQ(future.wait_for(0s), std::future_status::ready);
    EXPECT_TRUE(future.get().ok());
  }
  EXPECT_EQ(resource->in_use(), 0U);
  EXPECT_EQ(scheduler.stats().queued, 0U);
  EXPECT_EQ(scheduler.stats().resource_blocked, 0U);
  EXPECT_EQ(scheduler.stats().completed, 2U);
}

TEST(QueryTaskSchedulerTest, AvailabilityProbeDefersTasksAndRecoversAfterProbeException) {
  Scheduler scheduler;
  std::atomic<bool> available{false}, throw_probe{true};
  std::atomic<unsigned> probes{0}, invoked{0};
  auto resource = std::make_shared<QueryTaskResource>(1, [&] {
    ++probes;
    if (throw_probe.load()) throw std::runtime_error("temporary native lock probe failure");
    return available.load();
  });
  // Ensure every exit allows drain even while the external availability probe is false.
  struct ProbeCleanup {
    std::atomic<bool>& available;
    std::atomic<bool>& throwing;
    Scheduler& scheduler;
    ~ProbeCleanup() { throwing = false; available = true; scheduler.shutdown(); }
  } cleanup{available, throw_probe, scheduler};
  auto waiting = scheduler.submit(resource_task({[&](const CancellationToken&) {
    ++invoked;
    return QueryStatus::success();
  }}, resource));
  ASSERT_TRUE(waiting.ok());
  auto independent = scheduler.submit(success_task());
  ASSERT_TRUE(independent.ok());
  ASSERT_EQ(independent.value().wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(independent.value().get().ok());
  ASSERT_TRUE(wait_until([&] { return probes.load() > 0; }));
  EXPECT_EQ(invoked.load(), 0U);
  EXPECT_EQ(resource->in_use(), 0U);
  throw_probe = false;
  EXPECT_FALSE(resource->try_acquire());
  available = true;
  ASSERT_EQ(waiting.value().wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(waiting.value().get().ok());
  EXPECT_EQ(invoked.load(), 1U);
  EXPECT_EQ(resource->in_use(), 0U);
  EXPECT_EQ(scheduler.stats().resource_waits, 1U);
}

TEST(QueryTaskSchedulerTest, InvalidResourceRequirementRejectsWholeBatchBeforeExecution) {
  for (unsigned invalid = 0; invalid < 5; ++invalid) {
    SCOPED_TRACE(invalid);
    Scheduler scheduler;
    auto resource = std::make_shared<QueryTaskResource>(2);
    std::atomic<unsigned> invoked{0};
    Scheduler::Submission valid{[&](const CancellationToken&) {
      ++invoked;
      return QueryStatus::success();
    }};
    auto bad = valid;
    switch (invalid) {
      case 0: bad.resources = {{nullptr, 1}}; break;
      case 1: bad.resources = {{resource, 0}}; break;
      case 2: bad.resources = {{resource, 3}}; break;
      case 3: bad.resources = {{resource, 1}, {resource, 1}}; break;
      case 4:
        for (unsigned i = 0; i < 33; ++i)
          bad.resources.push_back({std::make_shared<QueryTaskResource>(1), 1});
        break;
    }
    auto rejected = scheduler.submit_batch({valid, bad});
    EXPECT_EQ(rejected.status().code(), QueryError::InvalidQuery);
    scheduler.shutdown();
    EXPECT_EQ(invoked.load(), 0U);
    EXPECT_EQ(resource->in_use(), 0U);
    EXPECT_EQ(scheduler.stats().submitted, 0U);
    EXPECT_EQ(scheduler.stats().dispatched_tasks, 0U);
  }
}

TEST(QueryTaskSchedulerTest, ReclassifiedTaskParksAtFullQueueAndLetsArrowUseItsOnlySlot) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = config.max_queued_tasks = 1;
  Scheduler scheduler(config);
  auto old_resource = std::make_shared<QueryTaskResource>(1);
  auto new_resource = std::make_shared<QueryTaskResource>(1);
  auto external = new_resource->try_acquire();
  auto first_attempt = std::make_shared<TaskGate>();
  auto arrow_gate = std::make_shared<TaskGate>();
  ResourceCleanup cleanup{&scheduler, first_attempt, &external, arrow_gate};
  auto request_capture = std::make_shared<int>(42);
  const std::weak_ptr<int> capture_lifetime = request_capture;
  std::atomic<unsigned> attempts{0}, arrow_calls{0};
  auto original = scheduler.submit(resource_task(
    {[&, request_capture, first_attempt, new_resource](const CancellationToken&) {
      if (++attempts == 1) {
        first_attempt->arrive_and_wait();
        throw Scheduler::ResourcesChanged{{{new_resource, 1}}};
      }
      EXPECT_EQ(*request_capture, 42);
      EXPECT_EQ(new_resource->in_use(), 1U);
      return QueryStatus::success();
    }}, old_resource));
  ASSERT_TRUE(original.ok());
  request_capture.reset();
  ASSERT_TRUE(first_attempt->wait_for_arrivals(1));
  EXPECT_EQ(old_resource->in_use(), 1U);
  auto arrow = scheduler.submit({[&, arrow_gate](const CancellationToken&) {
    ++arrow_calls;
    arrow_gate->arrive_and_wait();
    return QueryStatus::success();
  }});
  ASSERT_TRUE(arrow.ok());
  EXPECT_EQ(scheduler.stats().queued, 1U);
  first_attempt->open();
  ASSERT_TRUE(arrow_gate->wait_for_arrivals(1));
  const auto bypassed = scheduler.stats();
  EXPECT_EQ(bypassed.queued, 1U);
  EXPECT_EQ(bypassed.parked, 0U);
  EXPECT_EQ(bypassed.active, 1U);
  EXPECT_EQ(bypassed.resource_requeues, 1U);
  EXPECT_EQ(old_resource->in_use(), 0U);
  EXPECT_EQ(new_resource->in_use(), 1U); // Still held by the external operation.
  EXPECT_EQ(attempts.load(), 1U);
  EXPECT_EQ(arrow_calls.load(), 1U);
  EXPECT_FALSE(capture_lifetime.expired());
  EXPECT_EQ(original.value().wait_for(0s), std::future_status::timeout);
  external = {};
  arrow_gate->open();
  EXPECT_TRUE(arrow.value().get().ok());
  ASSERT_EQ(original.value().wait_for(5s), std::future_status::ready);
  EXPECT_TRUE(original.value().get().ok());
  EXPECT_EQ(attempts.load(), 2U);
  EXPECT_TRUE(capture_lifetime.expired());
  scheduler.shutdown();
  EXPECT_EQ(scheduler.stats().submitted, 2U);
  EXPECT_EQ(scheduler.stats().completed, 2U);
  EXPECT_EQ(scheduler.stats().resource_requeues, 1U);
  EXPECT_EQ(scheduler.stats().queued, 0U);
  EXPECT_EQ(scheduler.stats().parked, 0U);
  EXPECT_EQ(old_resource->in_use(), 0U);
  EXPECT_EQ(new_resource->in_use(), 0U);
}

TEST(QueryTaskSchedulerTest, ReclassifiedParkedCancellationFinishesWithoutCallingBodyAgain) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  Scheduler scheduler(config);
  auto old_resource = std::make_shared<QueryTaskResource>(1);
  auto new_resource = std::make_shared<QueryTaskResource>(1);
  auto external = new_resource->try_acquire();
  ResourceCleanup cleanup{&scheduler, {}, &external};
  std::atomic<unsigned> attempts{0};
  auto task = resource_task({[&](const CancellationToken&) -> QueryStatus {
    ++attempts;
    throw Scheduler::ResourcesChanged{{{new_resource, 1}}};
  }}, old_resource);
  const auto cancellation = task.cancellation;
  auto submitted = scheduler.submit(std::move(task));
  ASSERT_TRUE(submitted.ok());
  ASSERT_TRUE(wait_until([&] { return scheduler.stats().parked == 1; }));
  EXPECT_EQ(scheduler.stats().active, 0U);
  EXPECT_EQ(scheduler.stats().queued, 0U);
  EXPECT_EQ(old_resource->in_use(), 0U);
  cancellation.cancel();
  ASSERT_EQ(submitted.value().wait_for(5s), std::future_status::ready);
  EXPECT_EQ(submitted.value().get().code(), QueryError::Cancelled);
  EXPECT_EQ(attempts.load(), 1U);
  EXPECT_EQ(new_resource->in_use(), 1U);
  EXPECT_EQ(scheduler.stats().completed, 1U);
  EXPECT_EQ(scheduler.stats().cancelled, 1U);
  EXPECT_EQ(scheduler.stats().parked, 0U);
}

TEST(QueryTaskSchedulerTest, ReclassifiedParkedDeadlineFinishesWithoutCallingBodyAgain) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  Scheduler scheduler(config);
  auto old_resource = std::make_shared<QueryTaskResource>(1);
  auto new_resource = std::make_shared<QueryTaskResource>(1);
  auto external = new_resource->try_acquire();
  ResourceCleanup cleanup{&scheduler, {}, &external};
  std::atomic<unsigned> attempts{0};
  auto task = resource_task({[&](const CancellationToken&) -> QueryStatus {
    ++attempts;
    throw Scheduler::ResourcesChanged{{{new_resource, 1}}};
  }}, old_resource);
  task.deadline = Scheduler::Clock::now() + 500ms;
  auto submitted = scheduler.submit(std::move(task));
  ASSERT_TRUE(submitted.ok());
  ASSERT_TRUE(wait_until([&] { return scheduler.stats().parked == 1; }));
  ASSERT_EQ(submitted.value().wait_for(5s), std::future_status::ready);
  EXPECT_EQ(submitted.value().get().code(), QueryError::DeadlineExceeded);
  EXPECT_EQ(attempts.load(), 1U);
  EXPECT_EQ(old_resource->in_use(), 0U);
  EXPECT_EQ(new_resource->in_use(), 1U);
  EXPECT_EQ(scheduler.stats().expired, 1U);
  EXPECT_EQ(scheduler.stats().parked, 0U);
}

TEST(QueryTaskSchedulerTest, ShutdownDrainsParkedReclassificationAfterNewResourceIsReleased) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 1;
  Scheduler scheduler(config);
  auto old_resource = std::make_shared<QueryTaskResource>(1);
  auto new_resource = std::make_shared<QueryTaskResource>(1);
  auto external = new_resource->try_acquire();
  ResourceCleanup cleanup{&scheduler, {}, &external};
  std::atomic<unsigned> attempts{0};
  auto submitted = scheduler.submit(resource_task({[&](const CancellationToken&) {
    if (++attempts == 1) throw Scheduler::ResourcesChanged{{{new_resource, 1}}};
    return QueryStatus::success();
  }}, old_resource));
  ASSERT_TRUE(submitted.ok());
  ASSERT_TRUE(wait_until([&] { return scheduler.stats().parked == 1; }));
  std::thread stop([&] { scheduler.shutdown(); });
  external = {};
  stop.join();
  ASSERT_EQ(submitted.value().wait_for(0s), std::future_status::ready);
  EXPECT_TRUE(submitted.value().get().ok());
  EXPECT_EQ(attempts.load(), 2U);
  EXPECT_EQ(scheduler.stats().completed, 1U);
  EXPECT_EQ(scheduler.stats().resource_requeues, 1U);
  EXPECT_EQ(scheduler.stats().active, 0U);
  EXPECT_EQ(scheduler.stats().parked, 0U);
  EXPECT_EQ(scheduler.stats().queued, 0U);
  EXPECT_EQ(old_resource->in_use(), 0U);
  EXPECT_EQ(new_resource->in_use(), 0U);
}

TEST(QueryTaskSchedulerTest, FourWorkerShutdownDrainsRunningParkedAndResourceWaitingTasks) {
  QueryTaskSchedulerConfig config;
  config.worker_threads = config.max_batch_size = 4;
  Scheduler scheduler(config);
  auto old_resource = std::make_shared<QueryTaskResource>(1);
  auto new_resource = std::make_shared<QueryTaskResource>(1);
  auto external = new_resource->try_acquire();
  auto first_attempt = std::make_shared<TaskGate>();
  auto running = std::make_shared<TaskGate>();
  ResourceCleanup cleanup{&scheduler, first_attempt, &external, running};
  auto capture = std::make_shared<int>(42);
  const std::weak_ptr<int> capture_lifetime = capture;
  std::atomic<unsigned> attempts{0};
  std::vector<Scheduler::Submission> tasks;
  tasks.push_back(resource_task({[&, capture](const CancellationToken&) {
    if (++attempts == 1) {
      first_attempt->arrive_and_wait();
      throw Scheduler::ResourcesChanged{{{new_resource, 1}}};
    }
    EXPECT_EQ(*capture, 42);
    EXPECT_EQ(old_resource->in_use(), 0U);
    EXPECT_EQ(new_resource->in_use(), 1U);
    return QueryStatus::success();
  }}, old_resource));
  for (unsigned i = 0; i < 3; ++i) tasks.push_back(blocking_task(running));
  tasks.push_back(resource_task(success_task(), new_resource));
  auto batch = scheduler.submit_batch(std::move(tasks));
  ASSERT_TRUE(batch.ok());
  capture.reset();
  ASSERT_TRUE(first_attempt->wait_for_arrivals(1));
  ASSERT_TRUE(running->wait_for_arrivals(3));
  first_attempt->open();
  ASSERT_TRUE(wait_until([&] {
    const auto stats = scheduler.stats();
    return stats.parked == 1 && stats.resource_blocked == 2;
  }));
  EXPECT_EQ(scheduler.stats().active, 3U);
  EXPECT_EQ(scheduler.stats().queued, 1U);
  EXPECT_EQ(old_resource->in_use(), 0U);
  EXPECT_FALSE(capture_lifetime.expired());

  std::thread stop([&] { scheduler.shutdown(); });
  external = {};
  running->open();
  stop.join();
  for (auto& future : batch.value()) {
    ASSERT_EQ(future.wait_for(0s), std::future_status::ready);
    EXPECT_TRUE(future.get().ok());
  }
  EXPECT_EQ(attempts.load(), 2U);
  EXPECT_TRUE(capture_lifetime.expired());
  EXPECT_EQ(old_resource->in_use(), 0U);
  EXPECT_EQ(new_resource->in_use(), 0U);
  const auto stats = scheduler.stats();
  EXPECT_EQ(stats.completed, 5U);
  EXPECT_EQ(stats.resource_requeues, 1U);
  EXPECT_EQ(stats.active, 0U);
  EXPECT_EQ(stats.queued, 0U);
  EXPECT_EQ(stats.parked, 0U);
  EXPECT_EQ(stats.resource_blocked, 0U);
}

TEST(QueryTaskSchedulerTest, InvalidReclassifiedRequirementsFailWithoutReplayingTask) {
  for (unsigned invalid = 0; invalid < 5; ++invalid) {
    SCOPED_TRACE(invalid);
    Scheduler scheduler;
    auto old_resource = std::make_shared<QueryTaskResource>(1);
    auto new_resource = std::make_shared<QueryTaskResource>(2);
    std::vector<Scheduler::ResourceRequirement> requirements;
    switch (invalid) {
      case 0: requirements = {{nullptr, 1}}; break;
      case 1: requirements = {{new_resource, 0}}; break;
      case 2: requirements = {{new_resource, 3}}; break;
      case 3: requirements = {{new_resource, 1}, {new_resource, 1}}; break;
      case 4:
        for (unsigned i = 0; i < 33; ++i)
          requirements.push_back({std::make_shared<QueryTaskResource>(1), 1});
        break;
    }
    std::atomic<unsigned> attempts{0};
    auto submitted = scheduler.submit(resource_task({[&](const CancellationToken&) -> QueryStatus {
      ++attempts;
      throw Scheduler::ResourcesChanged{requirements};
    }}, old_resource));
    ASSERT_TRUE(submitted.ok());
    ASSERT_EQ(submitted.value().wait_for(5s), std::future_status::ready);
    EXPECT_EQ(submitted.value().get().code(), QueryError::InvalidQuery);
    EXPECT_EQ(attempts.load(), 1U);
    EXPECT_EQ(old_resource->in_use(), 0U);
    EXPECT_EQ(new_resource->in_use(), 0U);
    scheduler.shutdown();
    EXPECT_EQ(scheduler.stats().completed, 1U);
    EXPECT_EQ(scheduler.stats().failed, 1U);
    EXPECT_EQ(scheduler.stats().parked, 0U);
  }
}
} // namespace
