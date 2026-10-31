#pragma once

#include "iotdbfs/formats/arrow/ArrowFragmentProvider.h"
#include "iotdbfs/query/training/arrow/WindowPlanner.h"

#include <memory>

namespace iotdbfs {

// A request-local assembler. It opens only shards reached by planned windows,
// maps declared step spans through Arrow offsets, and copies bounded output
// from immutable runtime-cached physical fragments.
class WindowAssembler {
public:
  static QueryResult<WindowAssembler> create(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime,
    TrainingViewSpec view,
    TrainingQueryLimits limits = {},
    QueryExecutionOptions options = {},
    ArrowDecodeConfig config = {},
    CancellationToken cancellation = {},
    std::uint64_t read_parallelism = 1);

  WindowAssembler(WindowAssembler&&) noexcept;
  WindowAssembler& operator=(WindowAssembler&&) noexcept;
  ~WindowAssembler();
  WindowAssembler(const WindowAssembler&) = delete;
  WindowAssembler& operator=(const WindowAssembler&) = delete;

  // Empty success means RejectWindow excluded a candidate after leaf validity
  // was decoded. The caller may continue lazy enumeration for the next sample.
  QueryResult<std::optional<WindowSample>> assemble(PlannedWindow planned);
  // Claims the exact union of physical tiles for a bounded lookahead and
  // fetches cache misses in multi-range batches. Large groups are retried one
  // window at a time; a large single window uses bounded tile waves.
  QueryStatus prepare_lookahead(const std::vector<PlannedWindow>& windows);
  bool lookahead_complete() const;
  void release_lookahead();
  void cancel();

private:
  friend class TrainingQueryModule;
  // Privately compiled from exact immutable source revisions. Only the owning
  // prepared training module can reuse this context across requests.
  struct PreparedContext;
  static QueryResult<std::shared_ptr<const PreparedContext>> prepare_context(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::uint64_t max_metadata_bytes);
  static std::uint64_t prepared_metadata_bytes(
    const std::shared_ptr<const PreparedContext>& context) noexcept;
  static QueryResult<WindowAssembler> create_prepared(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime, TrainingViewSpec view,
    TrainingQueryLimits limits, QueryExecutionOptions options,
    ArrowDecodeConfig config, CancellationToken cancellation,
    std::uint64_t read_parallelism,
    std::shared_ptr<const PreparedContext> context);
  static QueryResult<WindowAssembler> create_impl(
    const DatasetSnapshot& snapshot,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime, TrainingViewSpec view,
    TrainingQueryLimits limits, QueryExecutionOptions options,
    ArrowDecodeConfig config, CancellationToken cancellation,
    std::uint64_t read_parallelism, bool prepared,
    std::shared_ptr<const PreparedContext> context = {});
  struct Impl;
  explicit WindowAssembler(std::unique_ptr<Impl> impl);
  std::unique_ptr<Impl> impl_;
};

} // namespace iotdbfs
