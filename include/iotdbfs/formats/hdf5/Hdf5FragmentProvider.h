#pragma once

#include "iotdbfs/formats/hdf5/Hdf5Layout.h"
#include "iotdbfs/query/core/QueryRuntime.h"
#include <array>
#include <memory>
#include <functional>

namespace iotdbfs {
class Hdf5ChunkDecoder;
class Hdf5NativeSessionPool;
struct Hdf5PreparedSelection {
  std::uint64_t working_bytes{0};
  std::shared_ptr<const Hdf5ChunkDecoder> decoder;
  std::array<std::uint64_t, 8> config_signature{};
};
struct Hdf5FragmentMetrics {
  std::uint64_t payload_bytes{0}, payload_ranges{0}, decoded_bytes{0};
  std::uint64_t useful_bytes{0}, inflated_bytes{0}, fill_bytes{0}, cache_hits{0}, decode_count{0};
};
struct Hdf5QueryKernelConfig {
  bool native_session_cache_enabled{false};
  // Populated once by the adapter. It contains no request or authorization state.
  std::shared_ptr<Hdf5NativeSessionPool> native_session_pool;
  std::uint64_t contiguous_tile_elements{4096};
  // Bounds output, resident fragment capacities and active codec working memory.
  std::uint64_t max_buffer_bytes{64ULL * 1024 * 1024};
  std::uint64_t max_decoded_chunk_bytes{64ULL * 1024 * 1024};
  std::uint64_t max_encoded_chunk_bytes{64ULL * 1024 * 1024};
  std::uint64_t max_zlib_scratch_bytes{1024ULL * 1024};
  std::function<void(const Hdf5FragmentMetrics&)> metrics_observer;
  std::size_t read_parallelism{1}; // Per producer; runtime workers bound concurrent producers.
  std::size_t max_batch_fragments{64};
  std::uint64_t max_batch_encoded_bytes{8ULL * 1024 * 1024};
  // Optional successful payload-decode observations (not layout or fill).
  // Complete cache identity is supplied; observers bound/synchronize their own
  // distinct-key set and keep captured state alive through producer completion,
  // which can outlive a cancelled caller. Hits do not invoke it; exceptions are ignored.
  std::function<void(const std::string&, std::uint64_t)> decoded_fragment_observer;
};
std::array<std::uint64_t, 8> hdf5_selection_config_signature(
  const Hdf5QueryKernelConfig&);
QueryStatus validate_hdf5_selection(
  const Hdf5DatasetLayout& dataset, const Hdf5QueryBinding& binding,
  Hdf5QueryKernelConfig config = {}, PayloadReadCapabilities transport = {});
QueryResult<std::uint64_t> hdf5_selection_working_bytes(
  const Hdf5DatasetLayout&, const Hdf5QueryBinding&, Hdf5QueryKernelConfig = {}, PayloadReadCapabilities transport = {});
// Validates a bound selection once and retains its compiled chunk pipeline.
QueryResult<Hdf5PreparedSelection> prepare_hdf5_selection(
  const Hdf5DatasetLayout&, const Hdf5QueryBinding&, Hdf5QueryKernelConfig = {}, PayloadReadCapabilities transport = {});
// Request-local leases over the common runtime decoded-fragment service.
// Fragments are complete native chunks or fixed logical contiguous tiles.
class Hdf5FragmentProvider {
public:
  static QueryResult<std::unique_ptr<Hdf5FragmentProvider>> create(
    std::shared_ptr<const QuerySource> source, Hdf5DatasetLayout dataset,
    const Hdf5QueryBinding& binding, QueryExecutionOptions options,
    Hdf5QueryKernelConfig config, CancellationToken cancelled,
    std::shared_ptr<QueryRuntime> runtime, std::string identity,
    Hdf5PreparedSelection prepared);
  ~Hdf5FragmentProvider();
  // Retains only bounded fragments intersecting the next chunked slab.
  QueryStatus reserve_output(std::uint64_t bytes);
  void begin_slab(const std::vector<std::uint64_t>& origin,
    const std::vector<std::uint64_t>& shape);
  // Publish this slab's aggregate copy count to the shared runtime once.
  void finish_slab();
  // Copy an innermost-axis run into native-endian Float32/64 or raw Int16 storage.
  // The cursor expands Int16 to logical Int32 or applies the CF science view.
  QueryStatus copy_run(const std::vector<std::uint64_t>& coordinate,
    std::uint64_t count, void* destination, std::uint64_t output_bytes);
  // Scatter each native chunk's slab intersection into row-major output.
  // Native values remain packed within each output row until the cursor's
  // in-place logical/scientific conversion runs.
  QueryStatus copy_chunked_slab(void* destination, std::uint64_t value_bytes,
    std::uint64_t logical_element_bytes, std::uint64_t output_bytes);
  const Hdf5FragmentMetrics& metrics() const;
  void clear();
private:
  struct Impl;
  explicit Hdf5FragmentProvider(std::unique_ptr<Impl>);
  std::unique_ptr<Impl> impl_;
};
} // namespace iotdbfs
