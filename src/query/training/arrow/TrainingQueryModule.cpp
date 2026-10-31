#include "iotdbfs/query/training/arrow/TrainingQueryModule.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <algorithm>
#include <limits>
#include <type_traits>
#include <unistd.h>

namespace iotdbfs {
namespace {
QueryStatus invalid(const char* message) {
  return QueryStatus::failure(QueryError::InvalidQuery,
    QueryPhase::Validation, message);
}
QueryStatus cancelled() {
  return QueryStatus::failure(QueryError::Cancelled,
    QueryPhase::Execution, "training window query cancelled");
}

// Registry charge estimate: capacity, plus conservative fixed allocator and
// tree-node allowances. Counting SSO strings as allocations intentionally
// overcharges them. This does not claim allocator-independent RSS precision.
class MetadataCharge {
public:
  static constexpr std::uint64_t allocation = 64;
  static constexpr std::uint64_t tree_node = allocation + 4 * sizeof(void*);
  void add(std::uint64_t amount) noexcept {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    bytes_ = amount > maximum - bytes_ ? maximum : bytes_ + amount;
  }
  void array(std::uint64_t count, std::uint64_t width) noexcept {
    const auto maximum = std::numeric_limits<std::uint64_t>::max();
    add(count && width > maximum / count ? maximum : count * width);
  }
  template<class T> void vector(const std::vector<T>& values) noexcept {
    array(values.capacity(), sizeof(T));
    if (values.capacity()) add(allocation);
  }
  void string(const std::string& value) noexcept {
    add(value.capacity()); add(1); add(allocation);
  }
  void metadata(const MetadataMap& values) noexcept {
    array(values.size(), sizeof(MetadataMap::value_type) + tree_node);
    for (const auto& entry : values) {
      string(entry.first);
      if (entry.second.value().valueless_by_exception()) {
        add(std::numeric_limits<std::uint64_t>::max());
        continue;
      }
      std::visit([&](const auto& value) noexcept {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) string(value);
        else if constexpr (!std::is_arithmetic_v<T>) {
          array(value.size(), sizeof(typename T::value_type) + tree_node);
          if constexpr (std::is_same_v<T, MetadataStringSet>)
            for (const auto& item : value) string(item);
        }
      }, entry.second.value());
    }
  }
  std::uint64_t bytes() const noexcept { return bytes_; }
private:
  std::uint64_t bytes_{0};
};
} // namespace

TrainingQueryModule::TrainingQueryModule(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime, TrainingViewSpec view,
    TrainingQueryLimits limits, QueryExecutionOptions options,
    ArrowDecodeConfig config, TrainingExecutionOptions execution,
    std::optional<CompiledTrainingTransform> transform,
    std::shared_ptr<const WindowPlanner::PreparedContext> planner_context,
    std::shared_ptr<const WindowAssembler::PreparedContext> assembler_context)
  : snapshot_(std::move(snapshot)), index_(std::move(index)),
    storage_(std::move(storage)), runtime_(std::move(runtime)),
    view_(std::move(view)), limits_(limits), options_(options),
    config_(config), execution_(execution), transform_(std::move(transform)),
    planner_context_(std::move(planner_context)),
    assembler_context_(std::move(assembler_context)),
    prepared_process_id_(static_cast<std::int64_t>(::getpid())) {}

std::uint64_t TrainingQueryModule::estimated_metadata_bytes() const noexcept {
  MetadataCharge charge;
  charge.add(sizeof(*this));
  // Module, snapshot and index each have shared ownership bookkeeping.
  charge.array(3, MetadataCharge::allocation + 4 * sizeof(void*));
  charge.add(sizeof(DatasetSnapshot));
  charge.string(snapshot_->dataset_id);
  charge.string(snapshot_->fingerprint);
  charge.string(snapshot_->process_identity);
  charge.vector(snapshot_->shards);
  for (const auto& shard : snapshot_->shards) {
    charge.string(shard.filename); charge.string(shard.key.path);
  }
  charge.vector(snapshot_->manifests);
  for (const auto& manifest : snapshot_->manifests) {
    charge.string(manifest.key.path);
    if (manifest.format_id) charge.string(*manifest.format_id);
    charge.metadata(manifest.metadata);
  }
  charge.vector(snapshot_->rows);
  for (const auto& row : snapshot_->rows) {
    charge.string(row.series_id); charge.string(row.entity_id);
    charge.string(row.target_field); charge.string(row.time.timezone);
    charge.vector(row.time.timestamps_ns);
  }
  charge.add(snapshot_channel_metadata_bytes(*snapshot_));
  if (planner_context_) charge.add(planner_context_->estimated_metadata_bytes());
  charge.add(WindowAssembler::prepared_metadata_bytes(assembler_context_));
  charge.add(index_->estimated_metadata_bytes());
  charge.string(view_.view_id); charge.string(view_.experiment_id);
  charge.string(view_.snapshot_fingerprint); charge.string(view_.split.split_id);
  for (const auto* channels : {&view_.input_channels, &view_.target_channels}) {
    charge.vector(*channels);
    for (const auto& channel : *channels) {
      charge.string(channel.field_id); charge.string(channel.label);
    }
  }
  charge.string(view_.transform.artifact_id);
  charge.vector(view_.transform.statistics);
  for (const auto& statistic : view_.transform.statistics)
    charge.string(statistic.field_id);
  if (transform_) charge.add(transform_->estimated_dynamic_bytes());
  return charge.bytes();
}

QueryResult<std::shared_ptr<TrainingQueryModule>> TrainingQueryModule::prepare(
    const DatasetSnapshot& snapshot, const DatasetCoverageIndex& index,
    std::shared_ptr<const StructuredBlobStorage> storage,
    std::shared_ptr<QueryRuntime> runtime, const TrainingViewSpec& view,
    TrainingQueryLimits limits, QueryExecutionOptions options,
    ArrowDecodeConfig config, TrainingExecutionOptions execution) {
  using Result = QueryResult<std::shared_ptr<TrainingQueryModule>>;
  if (!storage || !runtime)
    return Result::failure(invalid("training storage and runtime are required"));
  if (!execution.window_group_size || execution.window_group_size > 64)
    return Result::failure(invalid("window group size must be between 1 and 64"));
  if (!execution.read_parallelism || execution.read_parallelism > 4)
    return Result::failure(invalid("training read parallelism must be between 1 and 4"));
  // Preparation can now precede any concrete window request. Reuse the shared
  // request validator's limit contract with an empty, one-slot selection.
  WindowRequest limit_probe;
  limit_probe.max_windows = 1;
  auto limits_status = validate_window_request(limit_probe, limits);
  if (!limits_status.ok()) return Result::failure(limits_status);
  auto runtime_status = validate_dataset_snapshot_runtime(snapshot, storage);
  if (!runtime_status.ok()) return Result::failure(runtime_status);
  auto view_status = validate_snapshot_view(snapshot, index, view);
  if (!view_status.ok()) return Result::failure(view_status);
  std::optional<CompiledTrainingTransform> transform;
  if (execution.compiled_transform) {
    auto compiled = CompiledTrainingTransform::compile(view);
    if (!compiled.ok()) return Result::failure(compiled.status());
    transform = std::move(compiled.value());
  } else {
    view_status = validate_training_transform(view);
    if (!view_status.ok()) return Result::failure(view_status);
  }
  if (view.experiment_id.empty() || view.split.split_id.empty() ||
      view.split.permitted_input.begin > view.split.permitted_input.end ||
      view.split.permitted_target.begin > view.split.permitted_target.end ||
      (view.target_length == 0) != view.target_channels.empty())
    return Result::failure(invalid("training view has an invalid split or target shape"));
  // Copy even rvalue arguments: a caller may retain references to vector
  // elements before std::move. Moving those buffers into a const owner would
  // not revoke the caller's mutable aliases and would invalidate trusted reuse.
  auto owned_snapshot = std::make_shared<const DatasetSnapshot>(
    deep_copy_dataset_snapshot(snapshot));
  auto owned_index = std::make_shared<const DatasetCoverageIndex>(index);
  std::shared_ptr<const WindowPlanner::PreparedContext> planner_context;
  std::shared_ptr<const WindowAssembler::PreparedContext> assembler_context;
  if (execution.reuse_prepared_validation) {
    auto planner = WindowPlanner::compile_prepared(owned_snapshot, owned_index,
      view, limits);
    if (!planner.ok()) return Result::failure(planner.status());
    planner_context = std::move(planner.value());
    auto assembler = WindowAssembler::prepare_context(*owned_snapshot, storage,
      options.max_layout_bytes);
    if (!assembler.ok()) return Result::failure(assembler.status());
    assembler_context = std::move(assembler.value());
  }
  return Result::success(std::shared_ptr<TrainingQueryModule>(
    new TrainingQueryModule(std::move(owned_snapshot), std::move(owned_index),
      std::move(storage), std::move(runtime), view, limits,
      options, config, execution, std::move(transform),
      std::move(planner_context), std::move(assembler_context))));
}

QueryResult<WindowBatch> TrainingQueryModule::execute(WindowRequest request,
    CancellationToken cancellation) const {
  using Result = QueryResult<WindowBatch>;
  if (prepared_process_id_ != static_cast<std::int64_t>(::getpid()))
    return Result::failure(QueryStatus::failure(QueryError::StalePlan,
      QueryPhase::Validation, "prepared training query belongs to another process"));
  if (cancellation.cancelled()) return Result::failure(cancelled());
  auto planner = [&] {
    QueryProfileTimer timer("window_planner_create");
    if (execution_.reuse_prepared_validation)
      return WindowPlanner::create_prepared(snapshot_, index_, view_, request,
        limits_, cancellation, planner_context_);
    return WindowPlanner::create(snapshot_, index_, view_, request,
      limits_, cancellation);
  }();
  if (!planner.ok()) return Result::failure(planner.status());
  auto assembler = [&] {
    QueryProfileTimer timer("window_assembler_create");
    if (execution_.reuse_prepared_validation)
      return WindowAssembler::create_prepared(*snapshot_, storage_, runtime_, view_,
        limits_, options_, config_, cancellation, execution_.read_parallelism,
        assembler_context_);
    return WindowAssembler::create(*snapshot_, storage_, runtime_, view_,
      limits_, options_, config_, cancellation, execution_.read_parallelism);
  }();
  if (!assembler.ok()) return Result::failure(assembler.status());
  auto admitted = [&] {
    QueryProfileTimer timer("scheduler_admit");
    return runtime_->scheduler().begin_request(view_.experiment_id,
      limits_.max_result_bytes, cancellation);
  }();
  if (!admitted.ok()) return Result::failure(admitted.status());
  std::vector<WindowSample> samples;
  samples.reserve(static_cast<std::size_t>(request.max_windows));
  bool exhausted = false;
  while (samples.size() < request.max_windows) {
    if (cancellation.cancelled()) {
      assembler.value().cancel();
      return Result::failure(cancelled());
    }
    std::vector<PlannedWindow> lookahead;
    const auto count = std::min<std::uint64_t>(execution_.window_group_size,
      request.max_windows - samples.size());
    lookahead.reserve(static_cast<std::size_t>(count));
    while (lookahead.size() < count) {
      if (cancellation.cancelled()) {
        assembler.value().cancel();
        return Result::failure(cancelled());
      }
      auto planned = [&] {
        QueryProfileTimer timer("window_plan");
        return planner.value().next();
      }();
      if (!planned.ok()) return Result::failure(planned.status());
      if (!planned.value()) {
        exhausted = !planner.value().candidate_limit_reached();
        break;
      }
      lookahead.push_back(std::move(*planned.value()));
    }
    if (lookahead.empty()) break;
    auto turn = [&] {
      QueryProfileTimer timer("scheduler_wait");
      return runtime_->scheduler().acquire_group(admitted.value(), cancellation);
    }();
    if (!turn.ok()) return Result::failure(turn.status());
    if (execution_.prefetch) {
      QueryProfileTimer timer("window_prefetch");
      auto prefetched = assembler.value().prepare_lookahead(lookahead);
      if (!prefetched.ok()) return Result::failure(prefetched);
    }
    const bool group_prefetched = assembler.value().lookahead_complete();
    for (auto& planned : lookahead) {
      if (cancellation.cancelled()) {
        assembler.value().cancel();
        return Result::failure(cancelled());
      }
      if (!group_prefetched) {
        QueryProfileTimer timer("window_prefetch");
        auto prefetched = assembler.value().prepare_lookahead({planned});
        if (!prefetched.ok()) return Result::failure(prefetched);
      }
      auto assembled = [&] {
        QueryProfileTimer timer("window_assemble");
        return assembler.value().assemble(std::move(planned));
      }();
      if (!group_prefetched) assembler.value().release_lookahead();
      if (!assembled.ok()) return Result::failure(assembled.status());
      if (!assembled.value()) {
        if (!request.coordinates.empty())
          return Result::failure(invalid("explicit window is ineligible after value decoding"));
        continue;
      }
      auto transformed = [&] {
        QueryProfileTimer timer("window_transform");
        return transform_ ? transform_->apply(*assembled.value())
          : apply_training_view_transform(view_, *assembled.value());
      }();
      if (!transformed.ok()) return Result::failure(transformed);
      samples.push_back(std::move(*assembled.value()));
    }
    assembler.value().release_lookahead();
    if (exhausted || planner.value().candidate_limit_reached()) break;
  }
  if (cancellation.cancelled()) {
    assembler.value().cancel();
    return Result::failure(cancelled());
  }
  QueryProfileTimer timer("window_batch_finalize");
  return WindowBatch::create(view_, std::move(samples), limits_,
    exhausted ? std::nullopt : planner.value().continuation());
}

} // namespace iotdbfs
