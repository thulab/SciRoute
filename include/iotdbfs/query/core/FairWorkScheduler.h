#pragma once

#include "iotdbfs/query/core/DecodedFragmentCache.h"

#include <cstdint>
#include <memory>
#include <string>

namespace iotdbfs {

struct FairWorkStats {
  std::uint64_t admitted_requests{0};
  std::uint64_t granted_groups{0};
  std::uint64_t active_groups{0};
  std::uint64_t peak_active_groups{0};
  std::uint64_t queued_groups{0};
};

// Request admission bounds each workload's reserved output and concurrency.
// Group admission rotates among workloads with queued work. It controls only
// internal execution order; callers retain their own result ordering.
class FairWorkScheduler {
  struct State;
public:
  class RequestLease {
  public:
    RequestLease() = default;
    RequestLease(RequestLease&& other) noexcept;
    RequestLease& operator=(RequestLease&& other) noexcept;
    RequestLease(const RequestLease&) = delete;
    RequestLease& operator=(const RequestLease&) = delete;
    ~RequestLease();
    explicit operator bool() const { return static_cast<bool>(state_); }
  private:
    friend class FairWorkScheduler;
    RequestLease(std::shared_ptr<State> state, std::string workload,
        std::uint64_t output_bytes);
    void release();
    std::shared_ptr<State> state_;
    std::string workload_;
    std::uint64_t output_bytes_{0};
  };

  class GroupLease {
  public:
    GroupLease() = default;
    GroupLease(GroupLease&& other) noexcept;
    GroupLease& operator=(GroupLease&& other) noexcept;
    GroupLease(const GroupLease&) = delete;
    GroupLease& operator=(const GroupLease&) = delete;
    ~GroupLease();
    explicit operator bool() const { return static_cast<bool>(state_); }
  private:
    friend class FairWorkScheduler;
    explicit GroupLease(std::shared_ptr<State> state);
    void release();
    std::shared_ptr<State> state_;
  };

  explicit FairWorkScheduler(QueryRuntimeConfig config = {});
  QueryResult<RequestLease> begin_request(const std::string& workload,
    std::uint64_t output_reservation, CancellationToken cancellation = {});
  QueryResult<GroupLease> acquire_group(const RequestLease& request,
    CancellationToken cancellation = {});
  FairWorkStats stats() const;

private:
  std::shared_ptr<State> state_;
};

} // namespace iotdbfs
