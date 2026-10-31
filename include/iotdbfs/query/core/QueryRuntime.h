#pragma once

#include "iotdbfs/query/core/DecodedFragmentCache.h"
#include "iotdbfs/query/core/FairWorkScheduler.h"
#include "iotdbfs/query/core/QueryTaskScheduler.h"

namespace iotdbfs {

// Share one runtime between queries that should share decoding and the service
// memory budget. Storage namespace is part of each adapter's canonical key.
class QueryRuntime {
public:
  explicit QueryRuntime(QueryRuntimeConfig config = {});
  DecodedFragmentCache& cache() { return cache_; }
  const DecodedFragmentCache& cache() const { return cache_; }
  const QueryRuntimeConfig& config() const { return cache_.config(); }
  QueryRuntimeStats stats() const { return cache_.stats(); }
  FairWorkScheduler& scheduler() { return scheduler_; }
  const FairWorkScheduler& scheduler() const { return scheduler_; }
  QueryTaskScheduler& query_tasks() { return query_tasks_; }
  const QueryTaskScheduler& query_tasks() const { return query_tasks_; }
private:
  DecodedFragmentCache cache_;
  FairWorkScheduler scheduler_;
  // Destroy first: whole requests drain while their cache and admission
  // services still exist.
  QueryTaskScheduler query_tasks_;
};

} // namespace iotdbfs
