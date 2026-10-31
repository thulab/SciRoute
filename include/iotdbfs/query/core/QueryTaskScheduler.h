#pragma once

#include "iotdbfs/query/core/DecodedFragmentCache.h"

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <vector>

namespace iotdbfs {

// Logical admission resource. A lease can be released on a different thread;
// it is never a transferred std::mutex lock. Optional availability probes must
// be nonblocking and must not reenter this resource or its scheduler.
class QueryTaskResource {
  struct State;
public:
  class Lease {
  public:
    Lease() = default;
    Lease(Lease&& other) noexcept;
    Lease& operator=(Lease&& other) noexcept;
    Lease(const Lease&) = delete;
    Lease& operator=(const Lease&) = delete;
    ~Lease();
    explicit operator bool() const { return static_cast<bool>(state_); }
  private:
    friend class QueryTaskResource;
    Lease(std::shared_ptr<State> state, std::size_t units);
    void release() noexcept;
    std::shared_ptr<State> state_;
    std::size_t units_{0};
  };
  explicit QueryTaskResource(std::size_t capacity, std::function<bool()> available = {});
  QueryTaskResource(const QueryTaskResource&) = delete;
  QueryTaskResource& operator=(const QueryTaskResource&) = delete;
  Lease try_acquire(std::size_t units = 1);
  std::size_t capacity() const;
  std::size_t in_use() const;
private:
  std::shared_ptr<State> state_;
};

struct QueryTaskSchedulerStats {
  std::uint64_t submitted{0}, completed{0}, failed{0}, cancelled{0}, expired{0}, rejected{0};
  std::uint64_t dispatch_batches{0}, dispatched_tasks{0}, max_dispatch_batch{0};
  std::uint64_t queued{0}, active{0}, peak_active{0};
  std::uint64_t resource_waits{0}, resource_bypasses{0}, resource_blocked{0};
  std::uint64_t parked{0}, resource_requeues{0};
};

// Service-wide executor for independent requests, not adapter subtasks. Tasks
// establish request-local budget/profile/read scopes on the executing thread.
// No task may synchronously wait for children submitted to this same pool.
class QueryTaskScheduler {
public:
  using Clock = std::chrono::steady_clock;
  using Task = std::function<QueryStatus(const CancellationToken&)>;
  struct ResourceRequirement {
    std::shared_ptr<QueryTaskResource> resource;
    std::size_t units{1};
  };
  // A pre-execution observation changed. Throw only before query I/O,
  // accounting, publication or other externally visible side effects. The
  // same task/future is retained and retried with these resources.
  struct ResourcesChanged { std::vector<ResourceRequirement> resources; };
  struct Submission {
    Task task;
    CancellationToken cancellation;
    Clock::time_point deadline{Clock::time_point::max()};
    std::vector<ResourceRequirement> resources;
  };

  explicit QueryTaskScheduler(QueryTaskSchedulerConfig config = {});
  ~QueryTaskScheduler();
  QueryTaskScheduler(const QueryTaskScheduler&) = delete;
  QueryTaskScheduler& operator=(const QueryTaskScheduler&) = delete;

  QueryResult<std::future<QueryStatus>> submit(Submission submission);
  // Atomic FIFO admission. This groups independent tasks for dispatch only;
  // it does not imply shared budgets, atomic publication, or query splitting.
  QueryResult<std::vector<std::future<QueryStatus>>> submit_batch(
    std::vector<Submission> submissions);
  // Stop admission and drain accepted requests. Call from outside the pool.
  // Running tasks are cooperative; shutdown never abandons their resources.
  void shutdown();
  QueryTaskSchedulerStats stats() const;

private:
  struct State;
  std::unique_ptr<State> state_;
};

} // namespace iotdbfs
