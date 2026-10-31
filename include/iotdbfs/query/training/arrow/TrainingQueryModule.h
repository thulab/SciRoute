#pragma once

#include "iotdbfs/query/training/arrow/WindowAssembler.h"

namespace iotdbfs {

struct TrainingExecutionOptions {
  // One turn of the shared runtime scheduler owns at most this many windows.
  std::uint64_t window_group_size{8};
  // False reads fragments only as each window is assembled.
  bool prefetch{true};
  // Bounded concurrent read waves for a fully resident lookahead (1..4).
  // Each wave uses independent provider state and the shared fragment cache.
  std::uint64_t read_parallelism{1};
  // Compile immutable channel statistics once; false keeps the reference
  // transform implementation for controlled performance ablations.
  bool compiled_transform{true};
  // Reuse immutable validation and compiled planner/layout state from prepare(). False
  // retains the public factories' complete validation for controlled ablation.
  bool reuse_prepared_validation{true};
};

// A prepared training view pins one DatasetSnapshot and coverage index.
// Calls to execute enumerate only the requested minibatch; different prepared
// views may share one QueryRuntime and its immutable Arrow fragments.
class TrainingQueryModule {
public:
  // Always copy declarations, including rvalue arguments. Mutable element
  // references retained by the caller must never alias the frozen module.
  static QueryResult<std::shared_ptr<TrainingQueryModule>> prepare(
    const DatasetSnapshot& snapshot,
    const DatasetCoverageIndex& index,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime,
    const TrainingViewSpec& view,
    TrainingQueryLimits limits = {},
    QueryExecutionOptions options = {},
    ArrowDecodeConfig config = {},
    TrainingExecutionOptions execution = {});

  // Copies of cancellation share a thread-safe signal. A caller may cancel a
  // running execute from another thread without owning its assembler.
  QueryResult<WindowBatch> execute(WindowRequest request,
    CancellationToken cancellation = {}) const;
  const DatasetSnapshot& snapshot() const { return *snapshot_; }
  const TrainingViewSpec& view() const { return view_; }
  // Conservative charge for this immutable module, snapshot, coverage index,
  // and view (including dynamic capacity). This is not RSS and excludes the
  // shared storage/runtime/decoded cache. Overflow saturates to UINT64_MAX.
  std::uint64_t estimated_metadata_bytes() const noexcept;

private:
  TrainingQueryModule(std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime, TrainingViewSpec view,
    TrainingQueryLimits limits, QueryExecutionOptions options,
    ArrowDecodeConfig config, TrainingExecutionOptions execution,
    std::optional<CompiledTrainingTransform> transform,
    std::shared_ptr<const WindowPlanner::PreparedContext> planner_context,
    std::shared_ptr<const WindowAssembler::PreparedContext> assembler_context);

  std::shared_ptr<const DatasetSnapshot> snapshot_;
  std::shared_ptr<const DatasetCoverageIndex> index_;
  std::shared_ptr<const StructuredBlobStorage> storage_;
  std::shared_ptr<QueryRuntime> runtime_;
  const TrainingViewSpec view_;
  TrainingQueryLimits limits_;
  QueryExecutionOptions options_;
  ArrowDecodeConfig config_;
  TrainingExecutionOptions execution_;
  std::optional<CompiledTrainingTransform> transform_;
  std::shared_ptr<const WindowPlanner::PreparedContext> planner_context_;
  std::shared_ptr<const WindowAssembler::PreparedContext> assembler_context_;
  // Runtime cache/scheduler threads cannot be inherited by forked children.
  const std::int64_t prepared_process_id_;
};

} // namespace iotdbfs
