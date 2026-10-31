#include "iotdbfs/query/core/FairWorkScheduler.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>

namespace iotdbfs {
namespace {
QueryStatus failure(QueryError code, const char* message) {
  return QueryStatus::failure(code, QueryPhase::Execution, message);
}
using Clock = std::chrono::steady_clock;
}

struct FairWorkScheduler::State {
  struct Workload {
    std::size_t requests{0};
    std::uint64_t output_bytes{0};
  };
  struct Waiter { bool granted{false}; };

  explicit State(QueryRuntimeConfig limits) : config(std::move(limits)) {}

  void grant_locked() {
    while (active_groups < config.max_active_work_groups && !rotation.empty()) {
      auto workload = std::move(rotation.front());
      rotation.pop_front();
      auto found = pending.find(workload);
      if (found == pending.end() || found->second.empty()) continue;
      auto waiter = found->second.front();
      found->second.pop_front();
      if (found->second.empty()) pending.erase(found);
      else rotation.push_back(workload);
      waiter->granted = true;
      ++active_groups;
      --queued_groups;
      ++granted_groups;
      peak_active_groups = std::max(peak_active_groups, active_groups);
    }
    cv.notify_all();
  }

  void remove_locked(const std::string& workload,
      const std::shared_ptr<Waiter>& waiter) {
    auto found = pending.find(workload);
    if (found == pending.end()) return;
    auto& queue = found->second;
    auto item = std::find(queue.begin(), queue.end(), waiter);
    if (item == queue.end()) return;
    queue.erase(item);
    --queued_groups;
    if (queue.empty()) {
      pending.erase(found);
      auto position = std::find(rotation.begin(), rotation.end(), workload);
      if (position != rotation.end()) rotation.erase(position);
    }
  }

  QueryRuntimeConfig config;
  mutable std::mutex mutex;
  std::condition_variable cv;
  std::map<std::string, Workload> workloads;
  std::map<std::string, std::deque<std::shared_ptr<Waiter>>> pending;
  std::deque<std::string> rotation;
  std::size_t waiting_requests{0};
  std::uint64_t admitted_requests{0};
  std::uint64_t granted_groups{0};
  std::uint64_t active_groups{0};
  std::uint64_t peak_active_groups{0};
  std::uint64_t queued_groups{0};
};

FairWorkScheduler::RequestLease::RequestLease(std::shared_ptr<State> state,
    std::string workload, std::uint64_t output_bytes)
  : state_(std::move(state)), workload_(std::move(workload)),
    output_bytes_(output_bytes) {}

FairWorkScheduler::RequestLease::RequestLease(RequestLease&& other) noexcept
  : state_(std::move(other.state_)), workload_(std::move(other.workload_)),
    output_bytes_(std::exchange(other.output_bytes_, 0)) {}

FairWorkScheduler::RequestLease& FairWorkScheduler::RequestLease::operator=(
    RequestLease&& other) noexcept {
  if (this != &other) {
    release();
    state_ = std::move(other.state_);
    workload_ = std::move(other.workload_);
    output_bytes_ = std::exchange(other.output_bytes_, 0);
  }
  return *this;
}

FairWorkScheduler::RequestLease::~RequestLease() { release(); }

void FairWorkScheduler::RequestLease::release() {
  if (!state_) return;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    auto found = state_->workloads.find(workload_);
    if (found != state_->workloads.end()) {
      --found->second.requests;
      found->second.output_bytes -= output_bytes_;
      if (!found->second.requests) state_->workloads.erase(found);
    }
  }
  state_->cv.notify_all();
  state_.reset();
}

FairWorkScheduler::GroupLease::GroupLease(std::shared_ptr<State> state)
  : state_(std::move(state)) {}

FairWorkScheduler::GroupLease::GroupLease(GroupLease&& other) noexcept
  : state_(std::move(other.state_)) {}

FairWorkScheduler::GroupLease& FairWorkScheduler::GroupLease::operator=(
    GroupLease&& other) noexcept {
  if (this != &other) { release(); state_ = std::move(other.state_); }
  return *this;
}

FairWorkScheduler::GroupLease::~GroupLease() { release(); }

void FairWorkScheduler::GroupLease::release() {
  if (!state_) return;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    --state_->active_groups;
    state_->grant_locked();
  }
  state_.reset();
}

FairWorkScheduler::FairWorkScheduler(QueryRuntimeConfig config)
  : state_(std::make_shared<State>(std::move(config))) {
  const auto& limits = state_->config;
  if (!limits.max_active_work_groups || !limits.max_requests_per_workload ||
      !limits.max_output_bytes_per_workload || !limits.max_queued_work_groups ||
      !limits.wait_timeout_ms)
    throw std::invalid_argument("invalid fair work scheduler limits");
}

QueryResult<FairWorkScheduler::RequestLease> FairWorkScheduler::begin_request(
    const std::string& workload, std::uint64_t output_reservation,
    CancellationToken cancellation) {
  using Result = QueryResult<RequestLease>;
  if (workload.empty() || !output_reservation)
    return Result::failure(failure(QueryError::InvalidQuery,
      "workload identity and output reservation are required"));
  auto state = state_;
  if (output_reservation > state->config.max_output_bytes_per_workload)
    return Result::failure(failure(QueryError::ResourceExhausted,
      "request output reservation exceeds workload limit"));
  const auto deadline = Clock::now() +
    std::chrono::milliseconds(state->config.wait_timeout_ms);
  std::unique_lock<std::mutex> lock(state->mutex);
  if (state->waiting_requests >= state->config.max_queued_work_groups)
    return Result::failure(failure(QueryError::ResourceExhausted,
      "work request queue is full"));
  ++state->waiting_requests;
  while (true) {
    if (cancellation.cancelled()) {
      --state->waiting_requests;
      return Result::failure(failure(QueryError::Cancelled,
        "work request cancelled before admission"));
    }
    const auto found = state->workloads.find(workload);
    const auto count = found == state->workloads.end() ? 0 : found->second.requests;
    const auto bytes = found == state->workloads.end() ? 0 : found->second.output_bytes;
    if (count < state->config.max_requests_per_workload &&
        output_reservation <= state->config.max_output_bytes_per_workload - bytes) {
      auto& entry = state->workloads[workload];
      ++entry.requests;
      entry.output_bytes += output_reservation;
      ++state->admitted_requests;
      --state->waiting_requests;
      return Result::success(RequestLease(std::move(state), workload,
        output_reservation));
    }
    const auto now = Clock::now();
    if (now >= deadline) {
      --state->waiting_requests;
      return Result::failure(failure(QueryError::ResourceExhausted,
        "workload request admission timed out"));
    }
    state->cv.wait_until(lock, std::min(deadline, now +
      std::chrono::milliseconds(10)));
  }
}

QueryResult<FairWorkScheduler::GroupLease> FairWorkScheduler::acquire_group(
    const RequestLease& request, CancellationToken cancellation) {
  using Result = QueryResult<GroupLease>;
  auto state = state_;
  if (!request.state_ || request.state_ != state)
    return Result::failure(failure(QueryError::InvalidQuery,
      "group requires an active request on this scheduler"));
  const auto deadline = Clock::now() +
    std::chrono::milliseconds(state->config.wait_timeout_ms);
  std::unique_lock<std::mutex> lock(state->mutex);
  if (cancellation.cancelled())
    return Result::failure(failure(QueryError::Cancelled,
      "work group cancelled before admission"));
  if (state->queued_groups >= state->config.max_queued_work_groups)
    return Result::failure(failure(QueryError::ResourceExhausted,
      "work group queue is full"));
  auto waiter = std::make_shared<State::Waiter>();
  auto& queue = state->pending[request.workload_];
  if (queue.empty()) state->rotation.push_back(request.workload_);
  queue.push_back(waiter);
  ++state->queued_groups;
  state->grant_locked();
  while (!waiter->granted) {
    if (cancellation.cancelled() || Clock::now() >= deadline) {
      state->remove_locked(request.workload_, waiter);
      return Result::failure(failure(cancellation.cancelled()
        ? QueryError::Cancelled : QueryError::ResourceExhausted,
        cancellation.cancelled() ? "work group cancelled before admission"
                                 : "work group admission timed out"));
    }
    state->cv.wait_until(lock, std::min(deadline, Clock::now() +
      std::chrono::milliseconds(10)));
  }
  if (cancellation.cancelled()) {
    --state->active_groups;
    state->grant_locked();
    return Result::failure(failure(QueryError::Cancelled,
      "work group cancelled before admission"));
  }
  return Result::success(GroupLease(std::move(state)));
}

FairWorkStats FairWorkScheduler::stats() const {
  std::lock_guard<std::mutex> lock(state_->mutex);
  return {state_->admitted_requests, state_->granted_groups,
    state_->active_groups, state_->peak_active_groups,
    state_->queued_groups};
}

} // namespace iotdbfs
