#pragma once

#include "iotdbfs/query/core/QueryPlanning.h"

#include <algorithm>
#include <memory>
#include <mutex>

namespace iotdbfs {

// One synchronous typed batch shares the ordinary query ceilings. This is a
// request lifetime, not a cache or an authorization proof. Capturing the shared
// object lets request source readers account safely from execution workers.
class QueryBatchBudget {
public:
  explicit QueryBatchBudget(QueryLimits limits = {}) : limits_(limits) {}

  QueryStatus reserve_read(bool layout, std::uint64_t bytes) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!status_.ok()) return status_;
    auto& used = layout ? layout_bytes_ : payload_bytes_;
    const auto maximum = layout ? limits_.max_layout_bytes : limits_.max_payload_bytes;
    if (ranges_ >= limits_.max_ranges || bytes > maximum - used)
      return fail("typed batch read budget exhausted");
    ++ranges_; used += bytes;
    return status_;
  }
  QueryStatus reserve_rows(std::uint64_t rows) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!status_.ok()) return status_;
    if (rows > limits_.max_result_rows - rows_)
      return fail("typed batch result row budget exhausted");
    rows_ += rows;
    return status_;
  }
  std::uint64_t remaining_encoded(std::uint64_t ceiling) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return encoded_bytes_ >= ceiling ? 0 : ceiling - encoded_bytes_;
  }
  QueryStatus reserve_encoded(std::uint64_t bytes, std::uint64_t ceiling) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!status_.ok()) return status_;
    if (encoded_bytes_ > ceiling || bytes > ceiling - encoded_bytes_)
      return fail("typed batch encoded result budget exhausted");
    encoded_bytes_ += bytes;
    return status_;
  }
private:
  QueryStatus fail(const char* message) {
    status_ = QueryStatus::failure(QueryError::ResourceExhausted,
      QueryPhase::Execution, message);
    return status_;
  }
  const QueryLimits limits_;
  mutable std::mutex mutex_;
  QueryStatus status_;
  std::uint64_t ranges_{0}, layout_bytes_{0}, payload_bytes_{0}, rows_{0}, encoded_bytes_{0};
};

inline thread_local std::shared_ptr<QueryBatchBudget> active_query_batch_budget;
class ScopedQueryBatchBudget {
public:
  explicit ScopedQueryBatchBudget(std::shared_ptr<QueryBatchBudget> budget)
    : previous_(std::move(active_query_batch_budget)) {
    active_query_batch_budget = std::move(budget);
  }
  ~ScopedQueryBatchBudget() { active_query_batch_budget = std::move(previous_); }
  ScopedQueryBatchBudget(const ScopedQueryBatchBudget&) = delete;
  ScopedQueryBatchBudget& operator=(const ScopedQueryBatchBudget&) = delete;
private:
  std::shared_ptr<QueryBatchBudget> previous_;
};

} // namespace iotdbfs
