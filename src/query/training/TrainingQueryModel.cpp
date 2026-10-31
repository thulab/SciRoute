#include "iotdbfs/query/training/TrainingQueryModel.h"

#include <openssl/sha.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>
#include <set>
#include <unordered_set>

namespace iotdbfs {
namespace {

QueryStatus invalid(const char* message) {
  return QueryStatus::failure(QueryError::InvalidQuery, QueryPhase::Validation,
    message);
}

QueryStatus exhausted(const char* message) {
  return QueryStatus::failure(QueryError::ResourceExhausted,
    QueryPhase::Validation, message);
}

bool valid_interval(StepInterval value) { return value.begin <= value.end; }
bool contains(StepInterval outer, StepInterval inner) {
  return outer.begin <= inner.begin && inner.end <= outer.end;
}
std::optional<StepInterval> intersection(StepInterval a, StepInterval b) {
  const StepInterval result{std::max(a.begin, b.begin), std::min(a.end, b.end)};
  if (result.begin >= result.end) return std::nullopt;
  return result;
}
bool add_checked(std::uint64_t a, std::uint64_t b, std::uint64_t& result) {
  if (a > std::numeric_limits<std::uint64_t>::max() - b) return false;
  result = a + b;
  return true;
}
std::uint64_t saturating_add(std::uint64_t a, std::uint64_t b) {
  std::uint64_t result;
  return add_checked(a, b, result)
    ? result : std::numeric_limits<std::uint64_t>::max();
}
bool multiply_checked(std::uint64_t a, std::uint64_t b,
  std::uint64_t& result) {
  if (b && a > std::numeric_limits<std::uint64_t>::max() / b) return false;
  result = a * b;
  return true;
}
bool accumulate_checked(std::uint64_t amount, std::uint64_t& total) {
  return add_checked(total, amount, total);
}
std::string channel_key(const std::string& field_id, std::uint64_t ordinal) {
  return std::to_string(field_id.size()) + ":" + field_id +
    std::to_string(ordinal);
}
std::string sample_key(const DatasetSpec& dataset, const std::string& view_identity,
  const TrainingSeries& series, std::uint64_t anchor) {
  return training_sample_id(dataset.snapshot_fingerprint, view_identity,
    series.series_id, anchor);
}

class IdentityFields {
public:
  void u64(std::uint64_t value) {
    for (unsigned index = 0; index < 8; ++index)
      bytes_.push_back(static_cast<char>((value >> (index * 8)) & 0xff));
  }
  void str(const std::string& value) {
    u64(value.size());
    bytes_.append(value);
  }
  void interval(StepInterval value) { u64(value.begin); u64(value.end); }
  void channels(const std::vector<TrainingChannelRef>& channels) {
    u64(channels.size());
    for (const auto& channel : channels) {
      str(channel.field_id); u64(channel.ordinal); str(channel.label);
    }
  }
  void floating(double value) {
    std::uint64_t bits;
    static_assert(sizeof(bits) == sizeof(value), "double must be 64 bits");
    std::memcpy(&bits, &value, sizeof(bits));
    u64(bits);
  }
  std::string digest() const {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(bytes_.data()),
      bytes_.size(), hash);
    constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(2 * SHA256_DIGEST_LENGTH);
    for (auto byte : hash) {
      result.push_back(digits[byte >> 4]);
      result.push_back(digits[byte & 15]);
    }
    return result;
  }
private:
  std::string bytes_;
};

const TrainingChannel* find_channel(const TrainingSeries& series,
  const TrainingChannelRef& reference) {
  for (const auto& channel : series.channels)
    if (channel.field_id == reference.field_id &&
        channel.ordinal == reference.ordinal &&
        channel.label == reference.label)
      return &channel;
  return nullptr;
}

QueryResult<std::int64_t> time_at(const TrainingSeries& series,
  std::uint64_t step) {
  const auto& axis = series.time;
  if (axis.kind == TrainingTimeKind::ExplicitNanoseconds)
    return QueryResult<std::int64_t>::success(
      axis.timestamps_ns[static_cast<std::size_t>(step - series.steps.begin)]);
  const auto offset = static_cast<__int128>(step - series.steps.begin);
  const auto value = static_cast<__int128>(axis.start_ns) +
    offset * static_cast<__int128>(axis.cadence_ns);
  if (value < std::numeric_limits<std::int64_t>::min() ||
      value > std::numeric_limits<std::int64_t>::max())
    return QueryResult<std::int64_t>::failure(
      invalid("training timestamp overflows int64 nanoseconds"));
  return QueryResult<std::int64_t>::success(static_cast<std::int64_t>(value));
}

QueryStatus validate_time_axis(const TrainingSeries& series) {
  const auto& axis = series.time;
  switch (axis.kind) {
    case TrainingTimeKind::Step:
      if (axis.start_ns || axis.cadence_ns || !axis.timezone.empty() ||
          !axis.timestamps_ns.empty())
        return invalid("step-only series cannot declare absolute time");
      return QueryStatus::success();
    case TrainingTimeKind::RegularNanoseconds:
      if (axis.cadence_ns <= 0 || axis.timezone.empty() ||
          !axis.timestamps_ns.empty())
        return invalid("regular time requires cadence and timezone");
      if (series.steps.begin < series.steps.end) {
        auto last = time_at(series, series.steps.end - 1);
        if (!last.ok()) return last.status();
      }
      return QueryStatus::success();
    case TrainingTimeKind::ExplicitNanoseconds:
      if (axis.start_ns || axis.cadence_ns || axis.timezone.empty() ||
          series.steps.end - series.steps.begin != axis.timestamps_ns.size())
        return invalid("explicit time must match series length and timezone");
      for (std::size_t i = 1; i < axis.timestamps_ns.size(); ++i)
        if (axis.timestamps_ns[i] <= axis.timestamps_ns[i - 1])
          return invalid("explicit timestamps must be strictly increasing");
      return QueryStatus::success();
  }
  return invalid("unknown training time kind");
}

QueryStatus validate_limits(const TrainingQueryLimits& limits) {
  if (!limits.max_series || !limits.max_channels ||
      !limits.max_steps_per_window || !limits.max_windows_per_batch ||
      !limits.max_candidate_windows || !limits.max_result_bytes ||
      !limits.max_provenance_spans)
    return invalid("training query limits must be positive");
  return QueryStatus::success();
}

QueryStatus validate_refs(const std::vector<TrainingChannelRef>& channels,
  std::uint64_t cap) {
  if (channels.size() > cap) return exhausted("too many selected channels");
  std::unordered_set<std::string> seen;
  for (const auto& channel : channels) {
    if (channel.field_id.empty() || channel.label.empty())
      return invalid("selected channel requires field and label");
    if (!seen.insert(channel_key(channel.field_id, channel.ordinal)).second)
      return invalid("duplicate selected channel");
  }
  return QueryStatus::success();
}

QueryStatus validate_treatment(MissingTreatment value) {
  if (value != MissingTreatment::RejectWindow && value != MissingTreatment::Mask)
    return invalid("unknown training missing-data treatment");
  return QueryStatus::success();
}

// Bound candidate enumeration before visiting anchors. Without this, a tiny
// series and an enormous requested range could require billions of rejected
// positions before next() reaches end-of-stream.
std::optional<std::pair<std::uint64_t, std::uint64_t>> anchor_bounds(
  const TrainingSeries& series, const TrainingViewSpec& view,
  StepInterval requested) {
  if (requested.begin >= requested.end ||
      (!series.valid && view.missing.null_series == MissingTreatment::RejectWindow))
    return std::nullopt;
  auto low = requested.begin;
  auto high = requested.end - 1;
  std::uint64_t history;
  if (!add_checked(view.input_length, view.gap, history))
    return std::nullopt;
  auto constrain_input = [&](StepInterval coverage) {
    std::uint64_t start;
    if (!add_checked(coverage.begin, history, start)) return false;
    low = std::max(low, start);
    high = std::min(high, saturating_add(coverage.end, view.gap));
    return low <= high;
  };
  auto constrain_target = [&](StepInterval coverage) {
    if (view.target_length == 0) return true;
    if (coverage.end < view.target_length) return false;
    low = std::max(low, coverage.begin);
    high = std::min(high, coverage.end - view.target_length);
    return low <= high;
  };
  if (!constrain_input(series.steps) ||
      !constrain_input(view.split.permitted_input) ||
      !constrain_target(series.steps) ||
      !constrain_target(view.split.permitted_target))
    return std::nullopt;
  if (!series.valid) return std::make_pair(low, high);
  for (const auto& reference : view.input_channels) {
    const auto* channel = find_channel(series, reference);
    if (!channel || !channel->available) {
      if (view.missing.missing_channel == MissingTreatment::RejectWindow)
        return std::nullopt;
    } else if (view.missing.ragged_channel == MissingTreatment::RejectWindow &&
               !constrain_input(channel->coverage)) {
      return std::nullopt;
    }
  }
  for (const auto& reference : view.target_channels) {
    const auto* channel = find_channel(series, reference);
    if (!channel || !channel->available) {
      if (view.missing.missing_channel == MissingTreatment::RejectWindow)
        return std::nullopt;
    } else if (view.missing.ragged_channel == MissingTreatment::RejectWindow &&
               !constrain_target(channel->coverage)) {
      return std::nullopt;
    }
  }
  if (low > high) return std::nullopt;
  return std::make_pair(low, high);
}

QueryResult<std::optional<WindowDescriptor>> describe(
  const DatasetSpec& dataset, const TrainingViewSpec& view,
  const std::string& view_identity,
  const WindowRequest& request, const TrainingQueryLimits& limits,
  const TrainingSeries& series, std::uint64_t anchor) {
  using Result = QueryResult<std::optional<WindowDescriptor>>;
  std::uint64_t input_end, target_end;
  if (anchor < view.gap || anchor - view.gap < view.input_length ||
      !add_checked(anchor, view.target_length, target_end))
    return Result::success(std::nullopt);
  input_end = anchor - view.gap;
  const StepInterval input{input_end - view.input_length, input_end};
  const StepInterval target{anchor, target_end};
  if (!contains(series.steps, input) ||
      (view.target_length && !contains(series.steps, target)) ||
      !contains(view.split.permitted_input, input) ||
      (view.target_length && !contains(view.split.permitted_target, target)))
    return Result::success(std::nullopt);
  if (!series.valid && view.missing.null_series == MissingTreatment::RejectWindow)
    return Result::success(std::nullopt);

  WindowDescriptor descriptor;
  descriptor.sample_id = sample_key(dataset, view_identity, series, anchor);
  descriptor.view_id = view.view_id;
  descriptor.series_id = series.series_id;
  descriptor.anchor = anchor;
  descriptor.epoch = request.epoch;
  descriptor.seed = request.seed;
  descriptor.shard_index = request.shard_index;
  descriptor.input = input;
  descriptor.target = target;
  descriptor.timezone = series.time.timezone;

  auto add_channels = [&](const std::vector<TrainingChannelRef>& selected,
      StepInterval interval, WindowRole role,
      std::vector<WindowChannelSpan>& output) -> bool {
    for (const auto& reference : selected) {
      const auto* channel = find_channel(series, reference);
      const bool available = series.valid && channel && channel->available;
      if (series.valid && !available &&
          view.missing.missing_channel == MissingTreatment::RejectWindow)
        return false;
      auto coverage = available
        ? intersection(channel->coverage, interval) : std::nullopt;
      if (available && (!coverage || !contains(channel->coverage, interval)) &&
          view.missing.ragged_channel == MissingTreatment::RejectWindow)
        return false;
      output.push_back({reference, coverage});
      if (coverage) {
        descriptor.source_spans.push_back({role, series.binding.source,
          reference.field_id, series.binding.record_batch_index,
          series.binding.row_index, reference.ordinal, *coverage, {}});
      }
    }
    return true;
  };
  if (!add_channels(view.input_channels, input, WindowRole::Input,
        descriptor.input_channels) ||
      !add_channels(view.target_channels, target, WindowRole::Target,
        descriptor.target_channels))
    return Result::success(std::nullopt);
  if (descriptor.source_spans.size() > limits.max_provenance_spans)
    return Result::failure(exhausted("too many window source spans"));

  if (series.time.kind != TrainingTimeKind::Step) {
    for (auto step = input.begin; step < input.end; ++step) {
      auto time = time_at(series, step);
      if (!time.ok()) return Result::failure(time.status());
      descriptor.input_times_ns.push_back(time.value());
    }
    for (auto step = target.begin; step < target.end; ++step) {
      auto time = time_at(series, step);
      if (!time.ok()) return Result::failure(time.status());
      descriptor.target_times_ns.push_back(time.value());
    }
  }
  return Result::success(std::move(descriptor));
}

bool valid_mask(const std::vector<std::uint8_t>& validity,
  const std::vector<WindowChannelSpan>& channels, StepInterval interval,
  MissingTreatment null_value) {
  std::vector<std::size_t> range_positions(channels.size(), 0);
  for (const auto& channel : channels) {
    std::uint64_t last = interval.begin;
    for (const auto& range : channel.available_ranges) {
      if (range.begin < last || range.begin >= range.end ||
          range.end > interval.end) return false;
      last = range.end;
    }
  }
  for (std::size_t step = 0; step < interval.end - interval.begin; ++step) {
    for (std::size_t channel = 0; channel < channels.size(); ++channel) {
      const auto value = validity[step * channels.size() + channel];
      if (value > 1) return false;
      const auto& coverage = channels[channel].available_steps;
      const auto position = interval.begin + step;
      bool covered = false;
      if (!channels[channel].available_ranges.empty()) {
        const auto& ranges = channels[channel].available_ranges;
        auto& ordinal = range_positions[channel];
        while (ordinal < ranges.size() && ranges[ordinal].end <= position)
          ++ordinal;
        covered = ordinal < ranges.size() &&
          ranges[ordinal].begin <= position;
      } else covered = coverage && coverage->begin <= position && position < coverage->end;
      if (!covered && value != 0) return false;
      if (covered && value == 0 && null_value == MissingTreatment::RejectWindow)
        return false;
    }
  }
  return true;
}

} // namespace

QueryStatus validate_training_transform(const TrainingViewSpec& view) {
  const auto& transform = view.transform;
  if (transform.kind == TrainingTransformKind::Identity) {
    if (!transform.artifact_id.empty() || !transform.statistics.empty())
      return invalid("identity transform cannot declare training statistics");
    return QueryStatus::success();
  }
  if (transform.kind != TrainingTransformKind::Standardize)
    return invalid("unknown training transform");
  if (transform.artifact_id.empty())
    return invalid("standardization requires a named statistics artifact");
  std::set<std::pair<std::string, std::uint64_t>> selected;
  for (const auto* channels : {&view.input_channels, &view.target_channels})
    for (const auto& channel : *channels)
      selected.emplace(channel.field_id, channel.ordinal);
  if (transform.statistics.size() != selected.size())
    return invalid("standardization statistics must cover exactly the selected channels");
  std::set<std::pair<std::string, std::uint64_t>> covered;
  for (const auto& statistic : transform.statistics) {
    const auto key = std::make_pair(statistic.field_id, statistic.ordinal);
    if (statistic.field_id.empty() || !std::isfinite(statistic.mean) ||
        !std::isfinite(statistic.stddev) || statistic.stddev <= 0 ||
        selected.count(key) == 0 || !covered.insert(key).second)
      return invalid("invalid or duplicate training channel statistics");
  }
  return QueryStatus::success();
}

std::string training_view_identity(const TrainingViewSpec& view) {
  IdentityFields fields;
  fields.str("iotdbfs-training-view/1");
  fields.str(view.view_id);
  fields.str(view.snapshot_fingerprint);
  fields.channels(view.input_channels);
  fields.channels(view.target_channels);
  fields.u64(view.input_length); fields.u64(view.gap);
  fields.u64(view.target_length); fields.u64(view.stride);
  fields.str(view.split.split_id);
  fields.interval(view.split.permitted_input);
  fields.interval(view.split.permitted_target);
  fields.u64(static_cast<std::uint64_t>(view.missing.null_series));
  fields.u64(static_cast<std::uint64_t>(view.missing.missing_channel));
  fields.u64(static_cast<std::uint64_t>(view.missing.ragged_channel));
  fields.u64(static_cast<std::uint64_t>(view.missing.null_value));
  fields.u64(static_cast<std::uint64_t>(view.transform.kind));
  fields.str(view.transform.artifact_id);
  const auto& statistics = view.transform.statistics;
  std::map<std::pair<std::string, std::uint64_t>,
    std::pair<double, double>> ordered;
  for (const auto& statistic : statistics)
    ordered[{statistic.field_id, statistic.ordinal}] =
      {statistic.mean, statistic.stddev};
  fields.u64(ordered.size());
  for (const auto& entry : ordered) {
    fields.str(entry.first.first); fields.u64(entry.first.second);
    fields.floating(entry.second.first); fields.floating(entry.second.second);
  }
  return fields.digest();
}

QueryStatus apply_training_view_transform(const TrainingViewSpec& view,
    WindowSample& sample) {
  auto status = validate_training_transform(view);
  if (!status.ok()) return status;
  if (view.transform.kind == TrainingTransformKind::Identity)
    return QueryStatus::success();
  std::map<std::pair<std::string, std::uint64_t>,
    std::pair<double, double>> statistics;
  for (const auto& entry : view.transform.statistics)
    statistics[{entry.field_id, entry.ordinal}] = {entry.mean, entry.stddev};
  auto transform_role = [&](const std::vector<TrainingChannelRef>& channels,
      const std::vector<float>& values,
      const std::vector<std::uint8_t>& validity,
      std::vector<float>& output) -> QueryStatus {
    if (values.size() != validity.size() ||
        (channels.empty() ? !values.empty() : values.size() % channels.size() != 0))
      return invalid("training transform received an invalid sample shape");
    output = values;
    for (std::size_t offset = 0; offset < values.size(); ++offset) {
      if (validity[offset] > 1)
        return invalid("training transform received an invalid value mask");
      if (!validity[offset]) continue;
      const auto& channel = channels[offset % channels.size()];
      const auto& statistic = statistics.at({channel.field_id, channel.ordinal});
      const double value = (static_cast<double>(values[offset]) -
        statistic.first) / statistic.second;
      if (!std::isfinite(value) ||
          value > std::numeric_limits<float>::max() ||
          value < -std::numeric_limits<float>::max())
        return QueryStatus::failure(QueryError::CorruptPayload,
          QueryPhase::Execution, "standardized training value is not finite Float32");
      output[offset] = static_cast<float>(value);
    }
    return QueryStatus::success();
  };
  std::vector<float> input;
  status = transform_role(view.input_channels, sample.input_values,
    sample.input_validity, input);
  if (!status.ok()) return status;
  std::vector<float> target;
  status = transform_role(view.target_channels, sample.target_values,
    sample.target_validity, target);
  if (!status.ok()) return status;
  sample.input_values.swap(input);
  sample.target_values.swap(target);
  return QueryStatus::success();
}

QueryResult<CompiledTrainingTransform> CompiledTrainingTransform::compile(
    const TrainingViewSpec& view) {
  using Result = QueryResult<CompiledTrainingTransform>;
  const auto status = validate_training_transform(view);
  if (!status.ok()) return Result::failure(status);
  CompiledTrainingTransform plan;
  plan.kind_ = view.transform.kind;
  if (plan.kind_ == TrainingTransformKind::Identity)
    return Result::success(std::move(plan));
  std::map<std::pair<std::string, std::uint64_t>, Statistic> statistics;
  for (const auto& entry : view.transform.statistics)
    statistics.emplace(std::make_pair(entry.field_id, entry.ordinal),
      Statistic{entry.mean, entry.stddev});
  auto role = [&](const std::vector<TrainingChannelRef>& channels,
      std::vector<Statistic>& output) {
    output.reserve(channels.size());
    for (const auto& channel : channels)
      output.push_back(statistics.at({channel.field_id, channel.ordinal}));
  };
  role(view.input_channels, plan.input_);
  role(view.target_channels, plan.target_);
  return Result::success(std::move(plan));
}

QueryStatus CompiledTrainingTransform::apply(WindowSample& sample) const {
  // Match the public reference function's identity fast path, including its
  // lack of sample-shape/mask checks when no transform is requested.
  if (kind_ == TrainingTransformKind::Identity) return QueryStatus::success();
  auto transform_role = [](const std::vector<Statistic>& statistics,
      const std::vector<float>& values,
      const std::vector<std::uint8_t>& validity,
      std::vector<float>& output) -> QueryStatus {
    if (values.size() != validity.size() ||
        (statistics.empty() ? !values.empty() : values.size() % statistics.size() != 0))
      return invalid("training transform received an invalid sample shape");
    output = values;
    // Step-major iteration preserves the reference error order and double
    // subtraction/division while removing per-value modulo and map lookup.
    for (std::size_t step = 0; step < values.size(); step += statistics.size()) {
      for (std::size_t channel = 0; channel < statistics.size(); ++channel) {
        const auto offset = step + channel;
        if (validity[offset] > 1)
          return invalid("training transform received an invalid value mask");
        if (!validity[offset]) continue;
        const auto& statistic = statistics[channel];
        const double value = (static_cast<double>(values[offset]) -
          statistic.mean) / statistic.stddev;
        if (!std::isfinite(value) ||
            value > std::numeric_limits<float>::max() ||
            value < -std::numeric_limits<float>::max())
          return QueryStatus::failure(QueryError::CorruptPayload,
            QueryPhase::Execution, "standardized training value is not finite Float32");
        output[offset] = static_cast<float>(value);
      }
    }
    return QueryStatus::success();
  };
  std::vector<float> input, target;
  auto status = transform_role(input_, sample.input_values, sample.input_validity, input);
  if (!status.ok()) return status;
  status = transform_role(target_, sample.target_values, sample.target_validity, target);
  if (!status.ok()) return status;
  sample.input_values.swap(input);
  sample.target_values.swap(target);
  return QueryStatus::success();
}

std::uint64_t CompiledTrainingTransform::estimated_dynamic_bytes() const noexcept {
  // Same conservative allocator allowance as the prepared registry charge.
  const auto maximum = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t result = 0;
  for (const auto* role : {&input_, &target_}) {
    const auto count = role->capacity();
    if (!count) continue;
    if (count > (maximum - 64) / sizeof(Statistic)) return maximum;
    const auto bytes = count * sizeof(Statistic) + 64;
    if (bytes > maximum - result) return maximum;
    result += bytes;
  }
  return result;
}

std::string training_sample_id(const std::string& snapshot_fingerprint,
  const std::string& view_id, const std::string& series_id,
  std::uint64_t anchor) {
  auto part = [](const std::string& value) {
    return std::to_string(value.size()) + ":" + value;
  };
  return part(snapshot_fingerprint) + part(view_id) +
    part(series_id) + std::to_string(anchor);
}

QueryStatus validate_dataset_spec(const DatasetSpec& dataset,
  const TrainingQueryLimits& limits) {
  auto status = validate_limits(limits);
  if (!status.ok()) return status;
  if (dataset.dataset_id.empty() || dataset.snapshot_fingerprint.empty())
    return invalid("dataset and snapshot identities are required");
  if (dataset.series.size() > limits.max_series)
    return exhausted("too many training series");
  std::unordered_set<std::string> series_ids;
  for (const auto& series : dataset.series) {
    if (series.series_id.empty() || !series_ids.insert(series.series_id).second)
      return invalid("training series ids must be unique and nonempty");
    if (!valid_interval(series.steps))
      return invalid("invalid series step interval");
    const auto& binding = series.binding;
    if (binding.source.path.empty() || binding.source.version < 0 ||
        binding.source.format_id.empty() ||
        binding.record_batch_index.has_value() != binding.row_index.has_value() ||
        (!binding.row_index && binding.entity_id.empty()))
      return invalid("series requires an exact source and valid row binding");
    if (series.channels.size() > limits.max_channels)
      return exhausted("too many training channels");
    std::unordered_set<std::string> channel_ids;
    for (const auto& channel : series.channels) {
      if (channel.field_id.empty() || channel.label.empty() ||
          !valid_interval(channel.coverage) ||
          !contains(series.steps, channel.coverage) ||
          !channel_ids.insert(channel_key(channel.field_id, channel.ordinal)).second)
        return invalid("invalid or duplicate training channel binding");
      if (!channel.available && channel.coverage.begin != channel.coverage.end)
        return invalid("unavailable channel cannot claim step coverage");
    }
    status = validate_time_axis(series);
    if (!status.ok()) return status;
  }
  return QueryStatus::success();
}

QueryStatus validate_training_view(const DatasetSpec& dataset,
  const TrainingViewSpec& view, const TrainingQueryLimits& limits) {
  auto status = validate_dataset_spec(dataset, limits);
  if (!status.ok()) return status;
  if (view.view_id.empty() || view.experiment_id.empty() ||
      view.snapshot_fingerprint != dataset.snapshot_fingerprint ||
      view.split.split_id.empty())
    return invalid("view must bind an exact snapshot and named split");
  if (!view.input_length || !view.stride || view.input_channels.empty() ||
      (view.target_length == 0) != view.target_channels.empty())
    return invalid("invalid training window lengths or channel roles");
  std::uint64_t total_steps;
  if (!add_checked(view.input_length, view.gap, total_steps) ||
      !add_checked(total_steps, view.target_length, total_steps))
    return invalid("training window span overflows");
  if (total_steps > limits.max_steps_per_window)
    return exhausted("training window exceeds step limit");
  if (!valid_interval(view.split.permitted_input) ||
      !valid_interval(view.split.permitted_target))
    return invalid("invalid training split interval");
  status = validate_refs(view.input_channels, limits.max_channels);
  if (!status.ok()) return status;
  status = validate_refs(view.target_channels, limits.max_channels);
  if (!status.ok()) return status;
  for (auto treatment : {view.missing.null_series,
         view.missing.missing_channel, view.missing.ragged_channel,
         view.missing.null_value}) {
    status = validate_treatment(treatment);
    if (!status.ok()) return status;
  }
  status = validate_training_transform(view);
  if (!status.ok()) return status;
  // A selected channel must exist in at least one series. A heterogeneous
  // series may lack it only under the declared missing-channel policy.
  for (const auto* selected : {&view.input_channels, &view.target_channels})
    for (const auto& reference : *selected) {
      bool found = false;
      for (const auto& series : dataset.series)
        if (find_channel(series, reference)) { found = true; break; }
      if (!found && !dataset.series.empty())
        return QueryStatus::failure(QueryError::UnknownField,
          QueryPhase::Validation, "selected training channel is not bound");
    }
  return QueryStatus::success();
}

QueryStatus validate_window_request(const WindowRequest& request,
  const TrainingQueryLimits& limits) {
  auto status = validate_limits(limits);
  if (!status.ok()) return status;
  if (!request.max_windows || request.max_windows > limits.max_windows_per_batch)
    return exhausted("window request exceeds batch limit");
  if (request.max_windows > limits.max_candidate_windows)
    return exhausted("window request exceeds candidate limit");
  if (!request.shard_count || request.shard_index >= request.shard_count)
    return invalid("invalid window worker shard");
  if (request.anchors && (!valid_interval(*request.anchors) ||
      !request.coordinates.empty()))
    return invalid("window request needs one sample selection mode");
  if (request.continuation &&
      (!request.anchors ||
       request.continuation->selection_identity.size() != 64))
    return invalid("window continuation requires an anchor range and selection identity");
  if (!request.coordinates.empty() && request.shard_count != 1)
    return invalid("explicit coordinates cannot be sharded again");
  if (request.coordinates.size() > limits.max_windows_per_batch)
    return exhausted("too many explicit sample coordinates");
  if (request.coordinates.size() > request.max_windows)
    return invalid("explicit samples exceed requested window count");
  std::set<std::pair<std::string, std::uint64_t>> seen;
  for (const auto& coordinate : request.coordinates)
    if (coordinate.series_id.empty() ||
        (coordinate.expected_sample_id &&
          coordinate.expected_sample_id->empty()) ||
        !seen.emplace(coordinate.series_id, coordinate.anchor).second)
      return invalid("explicit window coordinates must be unique");
  return QueryStatus::success();
}

WindowCursor::WindowCursor(std::shared_ptr<const DatasetSpec> dataset,
  TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits)
  : dataset_(std::move(dataset)), view_(std::move(view)),
    request_(std::move(request)), limits_(limits),
    view_identity_(training_view_identity(view_)) {
  if (request_.anchors) next_anchor_ = request_.anchors->begin;
}

QueryResult<WindowCursor> WindowCursor::create(
  std::shared_ptr<const DatasetSpec> dataset, TrainingViewSpec view,
  WindowRequest request, TrainingQueryLimits limits) {
  if (!dataset)
    return QueryResult<WindowCursor>::failure(invalid("dataset is required"));
  auto status = validate_training_view(*dataset, view, limits);
  if (!status.ok()) return QueryResult<WindowCursor>::failure(status);
  status = validate_window_request(request, limits);
  if (!status.ok()) return QueryResult<WindowCursor>::failure(status);
  if (request.continuation)
    return QueryResult<WindowCursor>::failure(
      invalid("descriptor-only cursor does not support window continuation"));
  return QueryResult<WindowCursor>::success(WindowCursor(
    std::move(dataset), std::move(view), std::move(request), limits));
}

QueryResult<std::optional<WindowDescriptor>> WindowCursor::next() {
  using Result = QueryResult<std::optional<WindowDescriptor>>;
  if (emitted_ >= request_.max_windows) return Result::success(std::nullopt);
  if (!request_.coordinates.empty()) {
    while (coordinate_index_ < request_.coordinates.size()) {
      const auto& coordinate = request_.coordinates[coordinate_index_++];
      const TrainingSeries* series = nullptr;
      for (const auto& candidate : dataset_->series)
        if (candidate.series_id == coordinate.series_id) {
          series = &candidate;
          break;
        }
      if (!series)
        return Result::failure(invalid("explicit window series was not found"));
      auto result = describe(*dataset_, view_, view_identity_, request_, limits_,
        *series, coordinate.anchor);
      if (!result.ok()) return result;
      if (!result.value())
        return Result::failure(invalid("explicit window is ineligible"));
      if (coordinate.expected_sample_id &&
          *coordinate.expected_sample_id != result.value()->sample_id)
        return Result::failure(invalid("explicit sample id does not match"));
      ++emitted_;
      return result;
    }
    return Result::success(std::nullopt);
  }
  if (!request_.anchors) return Result::success(std::nullopt);
  while (series_index_ < dataset_->series.size()) {
    const auto bounds = anchor_bounds(dataset_->series[series_index_],
      view_, *request_.anchors);
    if (!bounds || next_anchor_ > bounds->second) {
      ++series_index_;
      next_anchor_ = request_.anchors->begin;
      continue;
    }
    if (next_anchor_ < bounds->first) {
      const auto delta = bounds->first - request_.anchors->begin;
      const auto full_strides = delta / view_.stride;
      const auto strides = full_strides + (delta % view_.stride != 0);
      std::uint64_t offset;
      if (!multiply_checked(strides, view_.stride, offset) ||
          !add_checked(request_.anchors->begin, offset, next_anchor_)) {
        ++series_index_;
        next_anchor_ = request_.anchors->begin;
        continue;
      }
      if (next_anchor_ > bounds->second) {
        ++series_index_;
        next_anchor_ = request_.anchors->begin;
        continue;
      }
    }
    const auto anchor = next_anchor_;
    if (!add_checked(next_anchor_, view_.stride, next_anchor_))
      next_anchor_ = request_.anchors->end;
    auto result = describe(*dataset_, view_, view_identity_, request_, limits_,
      dataset_->series[series_index_], anchor);
    if (!result.ok()) return result;
    if (!result.value()) continue;
    const auto ordinal = eligible_index_++;
    if (ordinal % request_.shard_count != request_.shard_index) continue;
    ++emitted_;
    return result;
  }
  return Result::success(std::nullopt);
}

QueryResult<WindowBatch> WindowBatch::create(const TrainingViewSpec& view,
  std::vector<WindowSample> samples, TrainingQueryLimits limits,
  std::optional<WindowContinuation> continuation) {
  auto status = validate_limits(limits);
  if (!status.ok()) return QueryResult<WindowBatch>::failure(status);
  status = validate_training_transform(view);
  if (!status.ok()) return QueryResult<WindowBatch>::failure(status);
  if (view.view_id.empty() || view.experiment_id.empty() ||
      !view.input_length || !view.stride ||
      view.input_channels.empty() ||
      (view.target_length == 0) != view.target_channels.empty() ||
      view.input_channels.size() > limits.max_channels ||
      view.target_channels.size() > limits.max_channels)
    return QueryResult<WindowBatch>::failure(
      invalid("invalid window batch view"));
  std::uint64_t total_steps;
  if (!add_checked(view.input_length, view.gap, total_steps) ||
      !add_checked(total_steps, view.target_length, total_steps) ||
      total_steps > limits.max_steps_per_window)
    return QueryResult<WindowBatch>::failure(
      exhausted("window batch span exceeds step limit"));
  if (samples.size() > limits.max_windows_per_batch)
    return QueryResult<WindowBatch>::failure(
      exhausted("too many windows in result batch"));
  std::uint64_t input_elements, target_elements;
  if (!multiply_checked(view.input_length, view.input_channels.size(),
        input_elements) ||
      !multiply_checked(view.target_length, view.target_channels.size(),
        target_elements))
    return QueryResult<WindowBatch>::failure(
      exhausted("window result shape overflows"));
  std::uint64_t charged_bytes = 0, spans = 0;
  std::uint64_t total_elements;
  if (!add_checked(input_elements, target_elements, total_elements))
    return QueryResult<WindowBatch>::failure(
      exhausted("window result shape overflows"));
  std::unordered_set<std::string> ids;
  for (const auto& sample : samples) {
    const auto& descriptor = sample.descriptor;
    std::uint64_t expected_target_end;
    const bool valid_window_math =
      descriptor.anchor >= view.gap &&
      descriptor.anchor - view.gap >= view.input_length &&
      add_checked(descriptor.anchor, view.target_length,
        expected_target_end) &&
      descriptor.input.end == descriptor.anchor - view.gap &&
      descriptor.input.begin == descriptor.input.end - view.input_length &&
      descriptor.target.begin == descriptor.anchor &&
      descriptor.target.end == expected_target_end;
    if (descriptor.view_id != view.view_id || descriptor.sample_id.empty() ||
        !ids.insert(descriptor.sample_id).second ||
        !valid_window_math ||
        descriptor.input_channels.size() != view.input_channels.size() ||
        descriptor.target_channels.size() != view.target_channels.size() ||
        sample.input_values.size() != input_elements ||
        sample.input_validity.size() != input_elements ||
        sample.target_values.size() != target_elements ||
        sample.target_validity.size() != target_elements)
      return QueryResult<WindowBatch>::failure(
        invalid("window sample shape or identity is inconsistent"));
    for (std::size_t i = 0; i < view.input_channels.size(); ++i)
      if (descriptor.input_channels[i].channel.field_id != view.input_channels[i].field_id ||
          descriptor.input_channels[i].channel.ordinal != view.input_channels[i].ordinal ||
          descriptor.input_channels[i].channel.label != view.input_channels[i].label)
        return QueryResult<WindowBatch>::failure(
          invalid("input channel order differs from view"));
    for (std::size_t i = 0; i < view.target_channels.size(); ++i)
      if (descriptor.target_channels[i].channel.field_id != view.target_channels[i].field_id ||
          descriptor.target_channels[i].channel.ordinal != view.target_channels[i].ordinal ||
          descriptor.target_channels[i].channel.label != view.target_channels[i].label)
        return QueryResult<WindowBatch>::failure(
          invalid("target channel order differs from view"));
    if ((!descriptor.input_times_ns.empty() &&
          descriptor.input_times_ns.size() != view.input_length) ||
        (!descriptor.target_times_ns.empty() &&
          descriptor.target_times_ns.size() != view.target_length) ||
        (!descriptor.input_times_ns.empty() !=
          !descriptor.target_times_ns.empty() && view.target_length != 0) ||
        (descriptor.input_times_ns.empty() != descriptor.timezone.empty()) ||
        !valid_mask(sample.input_validity, descriptor.input_channels,
          descriptor.input, view.missing.null_value) ||
        !valid_mask(sample.target_validity, descriptor.target_channels,
          descriptor.target, view.missing.null_value))
      return QueryResult<WindowBatch>::failure(
        invalid("window time or validity does not match its coverage"));
    for (const auto& span : descriptor.source_spans) {
      const auto& selected = span.role == WindowRole::Input
        ? view.input_channels : view.target_channels;
      bool selected_channel = false;
      for (const auto& channel : selected)
        if (channel.field_id == span.field_id &&
            channel.ordinal == span.channel_ordinal) {
          selected_channel = true;
          break;
        }
      if (!valid_interval(span.steps) ||
          !selected_channel || span.source.path.empty() ||
          !contains(span.role == WindowRole::Input
            ? descriptor.input : descriptor.target, span.steps))
        return QueryResult<WindowBatch>::failure(
          invalid("window provenance lies outside the sample"));
    }
    if (!accumulate_checked(descriptor.source_spans.size(), spans) ||
        spans > limits.max_provenance_spans)
      return QueryResult<WindowBatch>::failure(
        exhausted("too many window provenance spans"));
    std::uint64_t values_bytes, times_bytes;
    auto charge = [&](std::uint64_t bytes) {
      return accumulate_checked(bytes, charged_bytes) &&
        charged_bytes <= limits.max_result_bytes;
    };
    if (!multiply_checked(total_elements, 5, values_bytes) ||
        !multiply_checked(descriptor.input_times_ns.size() +
          descriptor.target_times_ns.size(), 8, times_bytes) ||
        !charge(values_bytes) || !charge(times_bytes) ||
        !charge(sizeof(WindowSample)) ||
        !charge(descriptor.sample_id.size()) ||
        !charge(descriptor.view_id.size()) ||
        !charge(descriptor.series_id.size()) ||
        !charge(descriptor.timezone.size()))
      return QueryResult<WindowBatch>::failure(
        exhausted("window batch exceeds result byte limit"));
    for (const auto& channel : descriptor.input_channels)
      if (!charge(sizeof(WindowChannelSpan)) ||
          !charge(channel.channel.field_id.size()) ||
          !charge(channel.channel.label.size()) ||
          !charge(channel.available_ranges.size() * sizeof(StepInterval)))
        return QueryResult<WindowBatch>::failure(
          exhausted("window batch exceeds result byte limit"));
    for (const auto& channel : descriptor.target_channels)
      if (!charge(sizeof(WindowChannelSpan)) ||
          !charge(channel.channel.field_id.size()) ||
          !charge(channel.channel.label.size()) ||
          !charge(channel.available_ranges.size() * sizeof(StepInterval)))
        return QueryResult<WindowBatch>::failure(
          exhausted("window batch exceeds result byte limit"));
    for (const auto& span : descriptor.source_spans)
      if (!charge(sizeof(WindowSourceSpan)) ||
          !charge(span.source.path.size()) ||
          !charge(span.source.format_id.size()) ||
          !charge(span.field_id.size()) ||
          !charge(span.fragment_key.size()))
        return QueryResult<WindowBatch>::failure(
          exhausted("window batch exceeds result byte limit"));
  }
  return QueryResult<WindowBatch>::success(
    WindowBatch(view.view_id, std::move(samples), std::move(continuation)));
}

} // namespace iotdbfs
