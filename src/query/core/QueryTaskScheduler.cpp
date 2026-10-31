#include "iotdbfs/query/core/QueryTaskScheduler.h"

#include <algorithm>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <utility>

namespace iotdbfs {
namespace {
QueryStatus failure(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}
QueryStatus validate_resources(const std::vector<QueryTaskScheduler::ResourceRequirement>& resources) {
  if (resources.size() > 32)
    return failure(QueryError::InvalidQuery, "too many query task resource requirements");
  for (std::size_t i = 0; i < resources.size(); ++i) {
    const auto& need = resources[i];
    if (!need.resource || !need.units || need.units > need.resource->capacity())
      return failure(QueryError::InvalidQuery, "invalid query task resource requirement");
    for (std::size_t previous = 0; previous < i; ++previous)
      if (resources[previous].resource == need.resource)
        return failure(QueryError::InvalidQuery, "duplicate query task resource requirement");
  }
  return QueryStatus::success();
}
}

struct QueryTaskResource::State {
  State(std::size_t count, std::function<bool()> probe)
    : capacity(count), available(std::move(probe)) {}
  const std::size_t capacity;
  const std::function<bool()> available;
  mutable std::mutex mutex;
  std::size_t used{0};
};

QueryTaskResource::QueryTaskResource(std::size_t capacity, std::function<bool()> available) {
  if (capacity == 0) throw std::invalid_argument("query task resource capacity must be positive");
  state_ = std::make_shared<State>(capacity, std::move(available));
}
QueryTaskResource::Lease::Lease(std::shared_ptr<State> state, std::size_t units)
  : state_(std::move(state)), units_(units) {}
QueryTaskResource::Lease::Lease(Lease&& other) noexcept
  : state_(std::move(other.state_)), units_(std::exchange(other.units_, 0)) {}
QueryTaskResource::Lease& QueryTaskResource::Lease::operator=(Lease&& other) noexcept {
  if (this != &other) {
    release();
    state_ = std::move(other.state_);
    units_ = std::exchange(other.units_, 0);
  }
  return *this;
}
QueryTaskResource::Lease::~Lease() { release(); }
void QueryTaskResource::Lease::release() noexcept {
  if (!state_) return;
  auto state = std::move(state_);
  std::lock_guard<std::mutex> lock(state->mutex);
  state->used -= units_;
  units_ = 0;
}
QueryTaskResource::Lease QueryTaskResource::try_acquire(std::size_t units) {
  if (units == 0 || units > state_->capacity)
    throw std::invalid_argument("invalid query task resource units");
  std::lock_guard<std::mutex> lock(state_->mutex);
  if (units > state_->capacity - state_->used) return {};
  try { if (state_->available && !state_->available()) return {}; }
  catch (...) { return {}; } // An optional availability probe cannot run the task unsafely.
  state_->used += units;
  return Lease(state_, units);
}
std::size_t QueryTaskResource::capacity() const { return state_->capacity; }
std::size_t QueryTaskResource::in_use() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->used;
}

struct QueryTaskScheduler::State {
  struct Job {
    Submission submission;
    std::promise<QueryStatus> completion;
    std::vector<QueryTaskResource::Lease> reservations;
    bool resource_waiting{false};
    bool observed_resource_wait{false};
  };
  struct Slot {
    enum Phase { Idle, Ready, Running, Parked };
    std::unique_ptr<Job> job;
    Phase phase{Idle};
    // Exactly one worker waits on this slot; the fixed-size slots vector is
    // constructed before any thread starts and never relocated.
    std::condition_variable ready;
  };
  explicit State(QueryTaskSchedulerConfig value) : config(value), slots(value.worker_threads) {}
  QueryTaskSchedulerConfig config;
  mutable std::mutex mutex;
  std::mutex shutdown_mutex;
  std::condition_variable dispatcher_ready;
  std::deque<std::unique_ptr<Job>> pending;
  std::vector<Slot> slots;
  std::vector<std::thread> workers;
  std::thread dispatcher;
  QueryTaskSchedulerStats counters;
  bool accepting{true};
  bool stopping{false};
  bool dispatch_finished{false};

  void notify_workers_for_shutdown() {
    for (auto& slot : slots) slot.ready.notify_one();
  }

  bool reserve(Job& job) {
    if (job.submission.cancellation.cancelled() || Clock::now() >= job.submission.deadline)
      return true; // Terminal tasks run only their existing pre-execution check.
    for (const auto& need : job.submission.resources) {
      auto lease = need.resource->try_acquire(need.units);
      if (!lease) {
        job.reservations.clear(); // No partial reservation while waiting.
        job.resource_waiting = true;
        if (!job.observed_resource_wait) {
          job.observed_resource_wait = true;
          ++counters.resource_waits;
        }
        return false;
      }
      job.reservations.push_back(std::move(lease));
    }
    return true;
  }

  void dispatch() {
    std::unique_lock<std::mutex> lock(mutex);
    for (;;) {
      dispatcher_ready.wait(lock, [&] {
        return (stopping && pending.empty() && !counters.active && !counters.parked) ||
          ((!pending.empty() || counters.parked) && std::any_of(slots.begin(), slots.end(),
            [](const auto& slot) { return slot.phase == Slot::Idle || slot.phase == Slot::Parked; }));
      });
      if (stopping && pending.empty() && !counters.active && !counters.parked) {
        dispatch_finished = true;
        notify_workers_for_shutdown();
        return;
      }
      std::size_t count = 0;
      bool skipped = false;
      auto next = pending.begin();
      for (auto& slot : slots) {
        if ((slot.phase != Slot::Idle && slot.phase != Slot::Parked) ||
            count == config.max_batch_size) continue;
        if (slot.phase == Slot::Parked) {
          if (reserve(*slot.job)) {
            --counters.parked;
            slot.job->resource_waiting = false;
            slot.phase = Slot::Ready;
            ++count;
            slot.ready.notify_one();
            continue;
          }
          skipped = true;
        }
        while (next != pending.end() && !reserve(**next)) { skipped = true; ++next; }
        if (next == pending.end()) continue;
        if (skipped) ++counters.resource_bypasses;
        (*next)->resource_waiting = false;
        if (slot.phase == Slot::Parked) {
          // Reuse this admission slot for runnable work, exchanging the old
          // observation retry into the vacated queue cell. Pending capacity
          // and the total accepted-task bound never grow during a retry.
          slot.job.swap(*next);
          ++next;
          --counters.parked;
        } else {
          slot.job = std::move(*next);
          next = pending.erase(next);
        }
        slot.phase = Slot::Ready;
        ++count;
        slot.ready.notify_one();
      }
      counters.queued = pending.size();
      counters.resource_blocked = std::count_if(pending.begin(), pending.end(),
        [](const auto& job) { return job->resource_waiting; });
      counters.resource_blocked += std::count_if(slots.begin(), slots.end(),
        [](const auto& slot) { return slot.phase == Slot::Parked && slot.job->resource_waiting; });
      counters.active += count;
      counters.peak_active = std::max(counters.peak_active, counters.active);
      if (count) ++counters.dispatch_batches;
      counters.dispatched_tasks += count;
      counters.max_dispatch_batch = std::max<std::uint64_t>(counters.max_dispatch_batch, count);
      // Releases from this pool notify immediately. An external reservation or
      // availability probe has no scheduler dependency; poll it at a bounded
      // interval, also observing queued cancellation/deadline changes.
      if (!count) dispatcher_ready.wait_for(lock, std::chrono::milliseconds(10));
    }
  }

  void work(std::size_t index) {
    for (;;) {
      Job* job;
      {
        std::unique_lock<std::mutex> lock(mutex);
        slots[index].ready.wait(lock, [&] {
          return slots[index].phase == Slot::Ready || dispatch_finished;
        });
        if (slots[index].phase != Slot::Ready) return;
        // Keep the occupied slot visible until task and all captures are gone.
        job = slots[index].job.get();
        slots[index].phase = Slot::Running;
      }
      QueryStatus status;
      bool retry = false;
      try {
        const auto& submission = job->submission;
        if (submission.cancellation.cancelled())
          status = failure(QueryError::Cancelled, "query task cancelled before execution");
        else if (Clock::now() >= submission.deadline)
          status = failure(QueryError::DeadlineExceeded, "query task expired before execution");
        else status = submission.task(submission.cancellation);
      } catch (ResourcesChanged& changed) {
        try {
          status = validate_resources(changed.resources);
          if (status.ok()) {
            job->reservations.clear();
            job->reservations.reserve(changed.resources.size());
            job->submission.resources = std::move(changed.resources);
            retry = true;
          }
        } catch (...) {
          status = failure(QueryError::ResourceExhausted, "query task resource refresh allocation failed");
        }
      } catch (const std::exception& error) {
        status = QueryStatus::failure(QueryError::Internal, QueryPhase::Execution, error.what());
      } catch (...) {
        status = failure(QueryError::Internal, "query task threw an unknown exception");
      }
      if (retry) {
        std::lock_guard<std::mutex> lock(mutex);
        slots[index].phase = Slot::Parked;
        job->resource_waiting = true;
        --counters.active;
        ++counters.parked;
        ++counters.resource_requeues;
        dispatcher_ready.notify_one();
        continue;
      }
      // Destroy request captures outside the scheduler mutex, before making
      // completion visible. Destructors can release other service resources.
      job->submission.task = {};
      job->reservations.clear();
      job->submission.resources.clear();
      std::unique_ptr<Job> finished;
      {
        std::lock_guard<std::mutex> lock(mutex);
        ++counters.completed;
        if (!status.ok()) ++counters.failed;
        if (status.code() == QueryError::Cancelled) ++counters.cancelled;
        if (status.code() == QueryError::DeadlineExceeded) ++counters.expired;
        finished = std::move(slots[index].job);
        slots[index].phase = Slot::Idle;
        --counters.active;
      }
      finished->completion.set_value(std::move(status));
      dispatcher_ready.notify_one();
    }
  }
};

QueryTaskScheduler::QueryTaskScheduler(QueryTaskSchedulerConfig config) {
  if (config.worker_threads == 0 || config.worker_threads > 256 ||
      config.max_queued_tasks == 0 || config.max_queued_tasks > 65536 ||
      config.max_batch_size == 0 || config.max_batch_size > config.worker_threads)
    throw std::invalid_argument("invalid query task scheduler limits");
  state_ = std::make_unique<State>(config);
  try {
    for (std::size_t i = 0; i < config.worker_threads; ++i)
      state_->workers.emplace_back([this, i] { state_->work(i); });
    state_->dispatcher = std::thread([this] { state_->dispatch(); });
  } catch (...) {
    shutdown();
    throw;
  }
}

QueryTaskScheduler::~QueryTaskScheduler() { shutdown(); }

QueryResult<std::future<QueryStatus>> QueryTaskScheduler::submit(Submission submission) {
  std::vector<Submission> batch;
  batch.push_back(std::move(submission));
  auto result = submit_batch(std::move(batch));
  if (!result.ok()) return QueryResult<std::future<QueryStatus>>::failure(result.status());
  return QueryResult<std::future<QueryStatus>>::success(std::move(result.value().front()));
}

QueryResult<std::vector<std::future<QueryStatus>>> QueryTaskScheduler::submit_batch(
    std::vector<Submission> submissions) {
  using Result = QueryResult<std::vector<std::future<QueryStatus>>>;
  if (submissions.size() > state_->config.max_queued_tasks) {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->counters.rejected += submissions.size();
    return Result::failure(failure(QueryError::ResourceExhausted,
      "query task batch exceeds queue capacity"));
  }
  for (const auto& submission : submissions)
    if (!submission.task) return Result::failure(failure(QueryError::InvalidQuery, "empty query task"));
  for (const auto& submission : submissions) {
    auto valid = validate_resources(submission.resources);
    if (!valid.ok()) return Result::failure(std::move(valid));
  }
  // Allocate outside the lock. The queue is changed only after every future
  // exists, so failed admission never executes a prefix of a batch.
  std::vector<std::unique_ptr<State::Job>> jobs;
  std::vector<std::future<QueryStatus>> futures;
  jobs.reserve(submissions.size());
  futures.reserve(submissions.size());
  for (auto& submission : submissions) {
    auto job = std::make_unique<State::Job>();
    job->submission = std::move(submission);
    job->reservations.reserve(job->submission.resources.size());
    futures.push_back(job->completion.get_future());
    jobs.push_back(std::move(job));
  }
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (!state_->accepting || jobs.size() > state_->config.max_queued_tasks - state_->pending.size()) {
      state_->counters.rejected += jobs.size();
      return Result::failure(failure(QueryError::ResourceExhausted,
        "query task scheduler stopped or queue capacity exhausted"));
    }
    // deque insertion can allocate. Roll back the entire admitted batch on an
    // allocation failure while the dispatcher is excluded by the same lock.
    std::size_t inserted = 0;
    try {
      for (auto& job : jobs) { state_->pending.push_back(std::move(job)); ++inserted; }
    } catch (...) {
      while (inserted > 0) {
        jobs[--inserted] = std::move(state_->pending.back());
        state_->pending.pop_back();
      }
      throw;
    }
    state_->counters.submitted += jobs.size();
    state_->counters.queued = state_->pending.size();
  }
  state_->dispatcher_ready.notify_one();
  return Result::success(std::move(futures));
}

void QueryTaskScheduler::shutdown() {
  if (!state_) return;
  std::lock_guard<std::mutex> shutdown_lock(state_->shutdown_mutex);
  bool finish_without_dispatcher = false;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->accepting = false;
    state_->stopping = true;
    if (!state_->dispatcher.joinable() && state_->pending.empty() &&
        !state_->counters.active && !state_->counters.parked) {
      state_->dispatch_finished = true; // Partial constructor failure.
      finish_without_dispatcher = true;
    }
  }
  state_->dispatcher_ready.notify_one();
  if (finish_without_dispatcher) state_->notify_workers_for_shutdown();
  if (state_->dispatcher.joinable()) state_->dispatcher.join();
  for (auto& worker : state_->workers) if (worker.joinable()) worker.join();
}

QueryTaskSchedulerStats QueryTaskScheduler::stats() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return state_->counters;
}
} // namespace iotdbfs
