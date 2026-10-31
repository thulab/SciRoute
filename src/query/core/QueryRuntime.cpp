#include "iotdbfs/query/core/QueryRuntime.h"

namespace iotdbfs {
QueryRuntime::QueryRuntime(QueryRuntimeConfig config)
  : cache_(config), scheduler_(config), query_tasks_(config.query_tasks) {}
} // namespace iotdbfs
