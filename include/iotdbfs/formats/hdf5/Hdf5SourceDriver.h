#pragma once

#include "iotdbfs/FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5Layout.h"

#include <functional>
#include <memory>

namespace iotdbfs {
class QueryTaskResource;
// Process-wide logical admission gate for independent native HDF5 queries.
// The existing library mutex remains responsible for native handle safety.
std::shared_ptr<QueryTaskResource> hdf5_query_task_resource();

struct Hdf5DimensionAssociation;
QueryResult<std::vector<Hdf5DimensionAssociation>>
inspect_hdf5_dimension_associations_native(const FormatSource &, std::uint64_t);

struct Hdf5SourceOptions {
  std::size_t max_read_calls{65536};
  std::uint64_t max_read_bytes{64ULL * 1024 * 1024};
  std::uint64_t max_single_read_bytes{8ULL * 1024 * 1024};
  std::uint64_t max_result_bytes{64ULL * 1024 * 1024};
  // Declared logical chunk bytes, not a hard bound on native filter scratch.
  std::uint64_t max_decoded_chunk_bytes{64ULL * 1024 * 1024};
  std::uint64_t metadata_cache_bytes{8ULL * 1024 * 1024};
  std::uint64_t raw_chunk_cache_bytes{0};
  std::function<bool()> cancelled;
  // Trusted adapter's exact dataset/selection/slab workflow. Pool reuse is
  // disabled without this scope so logical demand never crosses ROI queries.
  std::string native_selection_identity;
  // Trusted internal worker disables recursive semantic discovery.
  bool enable_cf_inventory{true};
  std::vector<std::shared_ptr<const Hdf5SemanticProfile>> semantic_profiles;
};

struct Hdf5SourceRead {
  std::uint64_t offset{0}, length{0};
  // Native VFD H5FD_MEM_DRAW tag; some global-heap metadata also uses it.
  // This flag alone does not prove that a range contains tensor values.
  bool raw_data{false}, success{false};
  bool logical_charged{false};
};
struct Hdf5SourceTrace {
  std::vector<Hdf5SourceRead> reads;
  std::uint64_t metadata_cache_bytes{0}, raw_chunk_cache_bytes{0};
  std::uint64_t sieve_buffer_bytes{0}, conversion_buffer_bytes{0};
  // Charged source-read bytes, including failed/short source-read attempts.
  std::uint64_t metadata_bytes{0}, raw_data_bytes{0};
};
struct Hdf5NativeSubset {
  Hdf5DatasetDescriptor descriptor;
  std::vector<std::uint64_t> origin, shape;
  LogicalArray values;
};

// Read-only native session. The typed engine opens a managed filesystem file;
// metadata/diagnostic tools may explicitly open a source-backed session.
// Library handles stay private. It uses HDF5's
// own file indexes and H5Dread, independently of the indexed fragment cache.
// The first HDF5 entry disables dynamic plugin loading for the process
// lifetime; closing this driver does not restore the old plugin mask.
// Driver/VOL environment overrides are rejected before library initialization.
// This is a daemon-owned HDF5 runtime: unrelated code registering filters,
// changing the plugin mask, or reconfiguring HDF5 concurrently is outside the
// contract.
// Adapter-owned native HDF5 state. Requests exclusively lease entries; idle
// entries retain immutable file/library state, never a QuerySource or callback.
class Hdf5NativeSessionPool {
public:
  struct Stats {
    std::size_t files{0}, datasets{0}, busy{0};
    std::uint64_t accounted_bytes{0};
  };
  Hdf5NativeSessionPool();
  ~Hdf5NativeSessionPool();
  Stats stats() const;
private:
  struct Impl;
  std::shared_ptr<Impl> impl_;
  friend class Hdf5SourceDriver;
};

class Hdf5SourceDriver final : public Hdf5DatasetProvider {
public:
  static QueryResult<std::unique_ptr<Hdf5SourceDriver>>
  open(std::shared_ptr<const QuerySource> source,
       Hdf5SourceOptions options = {});
  // Resolve the exact version's managed filesystem capability. Reads use local
  // pread through the bounded VFD and charge the request's logical I/O budget;
  // there is no database-payload or FUSE fallback.
  static QueryResult<std::unique_ptr<Hdf5SourceDriver>>
  open_local(std::shared_ptr<const QuerySource> source,
             Hdf5SourceOptions options = {},
             std::shared_ptr<Hdf5NativeSessionPool> pool = {});
  ~Hdf5SourceDriver() override;
  Hdf5SourceDriver(const Hdf5SourceDriver &) = delete;
  Hdf5SourceDriver &operator=(const Hdf5SourceDriver &) = delete;
  QueryResult<Hdf5DatasetDescriptor>
  describe(const std::string &path) const override;
  QueryResult<Hdf5NativeSubset>
  read(const std::string &path, const std::vector<std::uint64_t> &origin,
       const std::vector<std::uint64_t> &shape) const;
  // Metadata-only hard-link graph and dataset storage inventory. A successful
  // result can explicitly report an unavailable index after a resource limit;
  // source I/O failure, corruption and cancellation remain failures.
  QueryResult<Hdf5LayoutDocument>
  inventory(const Hdf5LayoutLimits &limits = {}) const;
  Hdf5SourceTrace trace() const;
  // Diagnostic boundary probe through the same VFD read callback. It cannot
  // bypass source bounds, cancellation or cumulative session I/O budgets.
  QueryResult<StructuredBlobBytes>
  diagnostic_read_range(std::uint64_t offset, std::uint64_t length,
                        bool raw_data = true) const;

private:
  friend QueryResult<std::vector<Hdf5DimensionAssociation>>
  inspect_hdf5_dimension_associations_native(const FormatSource &,
                                             std::uint64_t);
  struct Impl;
  explicit Hdf5SourceDriver(std::shared_ptr<Impl> implementation);
  std::shared_ptr<Impl> impl_;
  std::shared_ptr<Hdf5NativeSessionPool> pool_;
  friend class Hdf5NativeSessionPool;
  friend QueryStatus with_hdf5_ingestion_source(
      const FormatSource &, Hdf5SourceOptions,
      const std::function<QueryStatus(const Hdf5SourceDriver &)> &);
};

// The borrowed callback/source never escapes this synchronous call. The caller
// must not retain the driver reference. Close completes before this returns.
QueryStatus with_hdf5_ingestion_source(
    const FormatSource &source, Hdf5SourceOptions options,
    const std::function<QueryStatus(const Hdf5SourceDriver &)> &operation);

} // namespace iotdbfs
