#pragma once
#include "iotdbfs/StructuredBlobStorage.h"
#include "iotdbfs/query/core/QueryModel.h"
#include <memory>
#include <utility>
#include <sys/types.h>
#include <vector>

namespace iotdbfs {
class PreparedQueryMetadataTrace;
struct QueryPrincipal {
  uid_t uid{};
  gid_t gid{};
  std::vector<gid_t> supplementary_groups;
};
// Execution pins are distinct from registrations and materialized IPC handles.
class QueryExecutionLease {
public:
  virtual ~QueryExecutionLease() = default;
};
// Jobs copy this token when they are queued, and release it after their actual
// source work ends. A cancelled waiter must not unpin an unfinished producer.
inline thread_local std::shared_ptr<QueryExecutionLease> active_query_execution_lease;
inline std::shared_ptr<QueryExecutionLease> current_query_execution_lease() {
  return active_query_execution_lease;
}
class ScopedQueryExecutionLease {
public:
  explicit ScopedQueryExecutionLease(std::shared_ptr<QueryExecutionLease> lease)
    : previous_(active_query_execution_lease) {
    active_query_execution_lease = std::move(lease);
  }
  ~ScopedQueryExecutionLease() { active_query_execution_lease = std::move(previous_); }
  ScopedQueryExecutionLease(const ScopedQueryExecutionLease&) = delete;
  ScopedQueryExecutionLease& operator=(const ScopedQueryExecutionLease&) = delete;
private:
  std::shared_ptr<QueryExecutionLease> previous_;
};
// A short commit-serialization gate. Never execute I/O, decode or encode while
// retaining it; use execution_lease() then release the gate for that work.
class QueryAdmission {
public:
  virtual ~QueryAdmission() = default;
  virtual std::shared_ptr<QueryExecutionLease> execution_lease() = 0;
};
// Owner-issued, process-local facts. No wire/API input can supply this object.
// A selected context retains ALL frozen candidates, including predicate misses.
class QueryReadContext {
public:
  virtual ~QueryReadContext() = default;
  virtual QueryResult<std::shared_ptr<const QueryReadContext>> select(
    const std::vector<StructuredBlobManifest>& candidates) const = 0;
  virtual const std::vector<QuerySourceReference>& sources() const = 0;
  virtual QueryResult<std::shared_ptr<QueryAdmission>> admit(
    const QueryPrincipal& principal) const = 0;
  // Pure immutable content access after frontend authorization; this is not
  // an authorization grant. Each execute/publish/new open must still admit.
  virtual QueryResult<StructuredBlobManifest> manifest(
    const StructuredBlobVersionKey& key) const = 0;
  // Optional independently accounted evidence for a completed result's future
  // opens. It must preserve ALL frozen candidates and the same admit/fee/error
  // contract, while dropping execution/planning-only residency. Build outside
  // the publication gate; allocation/quota refusal returns null and the caller
  // retains this context. This is never an authorization grant.
  virtual std::shared_ptr<const QueryReadContext> result_reference() const noexcept {
    return {};
  }
  // Optional immutable derived-domain fee evidence. The producer has already
  // reserved its batch and residency pin; no identities are copied here. This
  // carries neither a request ledger nor authorization. Context owners may
  // combine it with their own selection/principal proof evidence.
  virtual void bind_domain_metadata_fees(
      std::shared_ptr<const PreparedQueryMetadataTrace>) const noexcept {}
  // Request-local arming only. Real admission and original replay remain
  // mandatory; refusal/collectors retain the ordinary observation path.
  virtual void arm_domain_metadata_summary() const noexcept {}
};
} // namespace iotdbfs
