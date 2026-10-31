#pragma once

#include "iotdbfs/query/core/QueryModel.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iotdbfs {

// Every interval is half-open and measured in sample positions, never elapsed
// time. Absolute timestamps are a separate binding on those same positions.
struct StepInterval {
  std::uint64_t begin{0};
  std::uint64_t end{0};
};

enum class TrainingTimeKind { Step, RegularNanoseconds, ExplicitNanoseconds };

struct TrainingTimeAxis {
  TrainingTimeKind kind{TrainingTimeKind::Step};
  std::int64_t start_ns{0};
  std::int64_t cadence_ns{0};
  std::string timezone;
  std::vector<std::int64_t> timestamps_ns;
};

// A source row is one independent series. A07 may replace this one-row
// binding with an explicitly verified multi-source coverage index.
struct TrainingSeriesSource {
  QuerySourceReference source;
  std::optional<std::uint64_t> record_batch_index;
  std::optional<std::uint64_t> row_index;
  std::string entity_id;
};

struct TrainingChannel {
  std::string field_id;
  std::uint64_t ordinal{0};
  std::string label;
  std::optional<std::string> unit;
  StepInterval coverage;
  bool available{true};
};

struct TrainingSeries {
  std::string series_id;
  TrainingSeriesSource binding;
  StepInterval steps;
  TrainingTimeAxis time;
  std::vector<TrainingChannel> channels;
  bool valid{true};
};

struct DatasetSpec {
  std::string dataset_id;
  std::string snapshot_fingerprint;
  std::vector<TrainingSeries> series;
};

struct TrainingChannelRef {
  std::string field_id;
  std::uint64_t ordinal{0};
  std::string label;
};

enum class MissingTreatment { RejectWindow, Mask };

struct TrainingMissingPolicy {
  MissingTreatment null_series{MissingTreatment::RejectWindow};
  MissingTreatment missing_channel{MissingTreatment::RejectWindow};
  MissingTreatment ragged_channel{MissingTreatment::RejectWindow};
  MissingTreatment null_value{MissingTreatment::RejectWindow};
};

struct TrainingSplit {
  std::string split_id;
  StepInterval permitted_input;
  StepInterval permitted_target;
};

enum class TrainingTransformKind { Identity, Standardize };

struct TrainingChannelStatistics {
  std::string field_id;
  std::uint64_t ordinal{0};
  double mean{0};
  double stddev{1};
};

struct TrainingTransformSpec {
  TrainingTransformKind kind{TrainingTransformKind::Identity};
  // Names the immutable training-statistics artifact. Its contents are also
  // hashed into the stable view identity, so an edited artifact cannot reuse
  // sample IDs or a paging continuation.
  std::string artifact_id;
  std::vector<TrainingChannelStatistics> statistics;
};

struct TrainingViewSpec {
  std::string view_id;
  // Scheduling identity only. It does not change sample or view identity.
  std::string experiment_id;
  std::string snapshot_fingerprint;
  std::vector<TrainingChannelRef> input_channels;
  std::vector<TrainingChannelRef> target_channels;
  std::uint64_t input_length{0}; // L > 0
  std::uint64_t gap{0};          // G >= 0
  std::uint64_t target_length{0}; // H >= 0; zero means input-only
  std::uint64_t stride{0};       // S > 0
  TrainingSplit split;
  TrainingMissingPolicy missing;
  TrainingTransformSpec transform;
};

struct WindowCoordinate {
  std::string series_id;
  std::uint64_t anchor{0}; // First target position.
  std::optional<std::string> expected_sample_id;
};

struct TrainingQueryLimits {
  std::uint64_t max_series{65536};
  std::uint64_t max_channels{4096};
  std::uint64_t max_steps_per_window{1048576};
  std::uint64_t max_windows_per_batch{4096};
  // Per-worker nominal cap. A sharded snapshot scan may inspect up to this
  // many candidates per worker before yielding a partial page.
  std::uint64_t max_candidate_windows{65536};
  std::uint64_t max_result_bytes{256ULL * 1024 * 1024};
  std::uint64_t max_provenance_spans{65536};
};

// Opaque-to-callers paging state for an anchor-range request. The selection
// identity binds the stable snapshot, view and request selection, excluding
// max_windows so the caller may change page size.
struct WindowContinuation {
  std::string selection_identity;
  std::uint64_t series_index{0};
  std::uint64_t next_anchor{0};
  std::uint64_t eligible_count{0}; // Before worker sharding and value decoding.
};

struct WindowRequest {
  // Choose one: a lazy anchor range for every selected series, or exact
  // coordinates in caller order. Empty coordinates with no range mean empty.
  std::optional<StepInterval> anchors;
  std::vector<WindowCoordinate> coordinates;
  std::uint64_t max_windows{0};
  std::uint64_t epoch{0};
  std::uint64_t seed{0};
  std::uint64_t shard_index{0};
  std::uint64_t shard_count{1};
  std::optional<WindowContinuation> continuation;
};

enum class WindowRole { Input, Target };

struct WindowSourceSpan {
  WindowRole role{WindowRole::Input};
  QuerySourceReference source;
  std::string field_id;
  std::optional<std::uint64_t> record_batch_index;
  std::optional<std::uint64_t> row_index;
  std::uint64_t channel_ordinal{0};
  StepInterval steps;
  // Empty until A08 resolves physical fragments. Source provenance is already
  // exact in A06; a fragment key is never invented from a semantic channel.
  std::string fragment_key;
};

struct WindowChannelSpan {
  TrainingChannelRef channel;
  std::optional<StepInterval> available_steps;
  // A08 can retain disjoint coverage without pretending its bounding range
  // is fully available. When nonempty this supersedes available_steps.
  std::vector<StepInterval> available_ranges;
};

struct WindowDescriptor {
  std::string sample_id;
  std::string view_id;
  std::string series_id;
  std::uint64_t anchor{0};
  std::uint64_t epoch{0};
  std::uint64_t seed{0};
  std::uint64_t shard_index{0};
  StepInterval input;
  StepInterval target;
  std::vector<WindowChannelSpan> input_channels;
  std::vector<WindowChannelSpan> target_channels;
  // Empty for step-only data. These are indexed by position and preserve
  // irregular time without treating elapsed duration as a sample count.
  std::vector<std::int64_t> input_times_ns;
  std::vector<std::int64_t> target_times_ns;
  std::string timezone;
  std::vector<WindowSourceSpan> source_spans;
};

// A06 only enumerates descriptors. It does not open sources or assemble
// values; A08 will consume these descriptors and the A05 fragment cache.
class WindowCursor {
public:
  static QueryResult<WindowCursor> create(
    std::shared_ptr<const DatasetSpec> dataset, TrainingViewSpec view,
    WindowRequest request, TrainingQueryLimits limits = {});
  QueryResult<std::optional<WindowDescriptor>> next();

private:
  WindowCursor(std::shared_ptr<const DatasetSpec> dataset, TrainingViewSpec view,
    WindowRequest request, TrainingQueryLimits limits);
  std::shared_ptr<const DatasetSpec> dataset_;
  TrainingViewSpec view_;
  WindowRequest request_;
  TrainingQueryLimits limits_;
  std::size_t series_index_{0};
  std::size_t coordinate_index_{0};
  std::uint64_t next_anchor_{0};
  std::uint64_t eligible_index_{0};
  std::uint64_t emitted_{0};
  std::string view_identity_;
};

// One bounded final copy per sample. Byte-per-element validity avoids
// interpreting a stored zero as a valid observation; no implicit fill occurs.
struct WindowSample {
  WindowDescriptor descriptor;
  std::vector<float> input_values;
  std::vector<std::uint8_t> input_validity;
  std::vector<float> target_values;
  std::vector<std::uint8_t> target_validity;
};

class WindowBatch {
public:
  static QueryResult<WindowBatch> create(
    const TrainingViewSpec& view, std::vector<WindowSample> samples,
    TrainingQueryLimits limits = {},
    std::optional<WindowContinuation> continuation = std::nullopt);
  const std::string& view_id() const { return view_id_; }
  const std::vector<WindowSample>& samples() const { return samples_; }
  const std::optional<WindowContinuation>& continuation() const {
    return continuation_;
  }

private:
  WindowBatch(std::string view_id, std::vector<WindowSample> samples,
      std::optional<WindowContinuation> continuation)
    : view_id_(std::move(view_id)), samples_(std::move(samples)),
      continuation_(std::move(continuation)) {}
  std::string view_id_;
  std::vector<WindowSample> samples_;
  std::optional<WindowContinuation> continuation_;
};

QueryStatus validate_dataset_spec(const DatasetSpec& dataset,
  const TrainingQueryLimits& limits = {});
QueryStatus validate_training_view(const DatasetSpec& dataset,
  const TrainingViewSpec& view, const TrainingQueryLimits& limits = {});
QueryStatus validate_training_transform(const TrainingViewSpec& view);
QueryStatus validate_window_request(const WindowRequest& request,
  const TrainingQueryLimits& limits = {});

// Stable semantic identity. Different experiments may share one view, while
// channel order, window shape, split, missing policy and statistics cannot.
std::string training_view_identity(const TrainingViewSpec& view);
QueryStatus apply_training_view_transform(const TrainingViewSpec& view,
  WindowSample& sample);

// Immutable, validated numeric statistics in each role's channel order. The
// prepared view owns this plan; applying it neither looks up field strings nor
// mutates decoded base fragments. Failures leave both sample roles untouched.
class CompiledTrainingTransform {
public:
  static QueryResult<CompiledTrainingTransform> compile(const TrainingViewSpec& view);
  QueryStatus apply(WindowSample& sample) const;
  // Dynamic capacity only; the owner's sizeof already includes this object.
  std::uint64_t estimated_dynamic_bytes() const noexcept;
private:
  struct Statistic { double mean; double stddev; };
  TrainingTransformKind kind_{TrainingTransformKind::Identity};
  std::vector<Statistic> input_;
  std::vector<Statistic> target_;
};

std::string training_sample_id(const std::string& snapshot_fingerprint,
  const std::string& view_id, const std::string& series_id,
  std::uint64_t anchor);

} // namespace iotdbfs
