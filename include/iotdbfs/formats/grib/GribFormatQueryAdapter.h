#pragma once

#include "iotdbfs/formats/grib/GribQueryKernel.h"
#include "iotdbfs/query/core/QueryRuntime.h"

#include <cstddef>
#include <memory>

namespace iotdbfs {

// The Adapter retains a query-independent typed parse of each immutable File
// Version. The byte budget controls retention; the count is a high safety cap,
// not a small working-set target. Either zero limit disables retention.
struct GribParsedIndexCacheConfig {
  std::size_t max_versions{65536};
  std::size_t memory_budget_bytes{64 * 1024 * 1024};
};

// Native Float64 fields share identical encoded content within one storage
// namespace. The byte budget includes retained fields, bounded exact-source
// aliases/fee traces, 2Q history, and pending cache bookkeeping. Native entry
// count includes pending flights; decoded temporaries and cursor output retain
// their separate execution allocation model. Zero disables retention.
struct GribDecodedFieldCacheConfig {
  std::size_t max_fields{16};
  std::size_t memory_budget_bytes{64 * 1024 * 1024};
};

class GribParsedIndexCache;
class GribDecodedFieldCache;
class GribDecodeExecutor;

// Typed GRIB Adapter at the FormatQueryAdapter Seam. The public Interface is
// format-neutral; GRIB layout records, message ranges, and ecCodes operations
// stay inside the Implementation.
class GribFormatQueryAdapter final : public FormatQueryAdapter {
public:
  GribFormatQueryAdapter();
  explicit GribFormatQueryAdapter(
    GribParsedIndexCacheConfig index_cache_config,
    GribDecodedFieldCacheConfig decoded_cache_config = {},
    std::shared_ptr<QueryRuntime> runtime = nullptr);

  const std::string& id() const override;
  bool supports_unit_catalog(const QueryFragment& fragment) const override;

  QueryResult<QueryCapabilities> capabilities(
    const QueryPlanningContext& context, const QueryFragment& fragment) const override;
  QueryResult<AdapterPlan> prepare(const QueryPlanningContext& context,
    const QueryFragment& fragment) const override;
  bool supports_domain_index(const QueryFragment& fragment) const override;
  QueryResult<std::shared_ptr<const AdapterDomainIndex>> build_domain_index(
    const std::vector<std::shared_ptr<const QuerySource>>& sources,
    const QueryFragment& fragment, const QueryLimits& limits,
    std::uint64_t max_retained_bytes) const override;
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(
    const QueryExecutionContext& context,
    std::shared_ptr<const AdapterExecutionPlan> plan,
    const QueryExecutionOptions& options) const override;

private:
  std::shared_ptr<GribParsedIndexCache> parsed_index_cache_;
  std::shared_ptr<GribDecodedFieldCache> decoded_field_cache_;
  std::shared_ptr<GribDecodeExecutor> decode_executor_;
  // BinaryView and numeric-grid encoded reads share the bounded service.
  std::shared_ptr<QueryRuntime> runtime_;
};

} // namespace iotdbfs
