#pragma once

#include "iotdbfs/formats/hdf5/Hdf5QueryKernel.h"

namespace iotdbfs {
struct Hdf5LayoutCacheConfig {
  std::uint64_t max_bytes{64ULL * 1024 * 1024};
  std::size_t max_entries{4096};
};
class Hdf5ValidatedLayoutCache;
// Format-local, bounded cache that may be shared by adapters using distinct
// semantic bindings. Source namespace and exact version are part of its keys.
std::shared_ptr<Hdf5ValidatedLayoutCache> make_hdf5_validated_layout_cache(
  Hdf5LayoutCacheConfig config = {});
// One descriptor per prepared query, not a repeated column per array value.
// Object tokens and storage_namespace are meaningful only with this source.
struct Hdf5QueryIdentity {
  StructuredBlobManifest source;
  std::uint64_t storage_namespace{0};
  Hdf5LayoutDigest layout_checksum{};
  std::string dataset_id, dataset_path;
  LogicalTypeId value_type{LogicalTypeId::Float32};
  std::vector<std::uint64_t> dataset_shape;
  std::string view{"physical-ordinal-c-order/1"};
  std::optional<Hdf5GpsTimeAxis> gps_axis;
};
QueryResult<Hdf5QueryIdentity> hdf5_query_identity(
  std::shared_ptr<const AdapterExecutionPlan> plan);

// Rank 1..4 Float32/64 and Int16 arrays, with explicit physical/scientific views.
// Both engines use the same sealed dataset metadata and exact source identity.
// Indexed execution uses layout chunk pages; native execution delegates file
// indexing and decoding to libhdf5 over the source-backed read-only VFD.
class Hdf5FormatQueryAdapter final : public FormatQueryAdapter {
public:
  explicit Hdf5FormatQueryAdapter(Hdf5QueryKernelConfig config = {},
    std::shared_ptr<QueryRuntime> runtime = {}, Hdf5SemanticConfig semantics = {},
    Hdf5LayoutCacheConfig layout_cache = {},
    std::vector<std::shared_ptr<const Hdf5SemanticProfile>> profiles = {},
    std::shared_ptr<Hdf5ValidatedLayoutCache> shared_layout_cache = {},
    Hdf5QueryMode mode = Hdf5QueryMode::IoTDBIndexed);
  const std::string& id() const override;
  QueryResult<QueryCapabilities> capabilities(const QueryPlanningContext&,
    const QueryFragment&) const override;
  QueryResult<AdapterPlan> prepare(const QueryPlanningContext&,
    const QueryFragment&) const override;
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(
    const QueryExecutionContext&, std::shared_ptr<const AdapterExecutionPlan>,
    const QueryExecutionOptions&) const override;
private:
  Hdf5QueryKernelConfig config_;
  const Hdf5QueryMode mode_;
  std::shared_ptr<QueryRuntime> runtime_;
  const Hdf5SemanticConfig semantics_;
  const std::vector<std::shared_ptr<const Hdf5SemanticProfile>> profiles_;
  std::string semantics_key_;
  // Shared across adapters when a mount supplies the same cache. Either zero
  // limit disables retention.
  std::shared_ptr<Hdf5ValidatedLayoutCache> layout_cache_;
};
} // namespace iotdbfs
