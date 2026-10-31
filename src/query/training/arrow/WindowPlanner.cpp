#include "iotdbfs/query/training/arrow/WindowPlanner.h"
#include "iotdbfs/query/core/QueryProfile.h"

#include <openssl/sha.h>

#include <algorithm>
#include <limits>
#include <map>
#include <tuple>

namespace iotdbfs {
namespace {
QueryStatus invalid(const char* message) {
  return QueryStatus::failure(QueryError::InvalidQuery, QueryPhase::Planning, message);
}
QueryStatus exhausted(const char* message) {
  return QueryStatus::failure(QueryError::ResourceExhausted,
    QueryPhase::Planning, message);
}
QueryStatus cancelled() {
  return QueryStatus::failure(QueryError::Cancelled, QueryPhase::Planning,
    "training window planning cancelled");
}
bool contains(StepInterval outer, StepInterval inner) {
  return outer.begin <= inner.begin && inner.end <= outer.end;
}
bool add(std::uint64_t a, std::uint64_t b, std::uint64_t& out) {
  if (a > std::numeric_limits<std::uint64_t>::max() - b) return false;
  out = a + b;
  return true;
}
std::uint64_t saturating_add(std::uint64_t a, std::uint64_t b) {
  std::uint64_t out;
  return add(a, b, out) ? out : std::numeric_limits<std::uint64_t>::max();
}
void append_range(std::vector<StepInterval>& ranges, StepInterval next) {
  if (!ranges.empty() && ranges.back().end == next.begin)
    ranges.back().end = next.end;
  else ranges.push_back(next);
}

class PagingFields {
public:
  void u64(std::uint64_t value) {
    for (unsigned i = 0; i < 8; ++i)
      bytes_.push_back(static_cast<char>(value >> (i * 8)));
  }
  void str(const std::string& value) {
    u64(value.size()); bytes_.append(value);
  }
  void interval(StepInterval value) { u64(value.begin); u64(value.end); }
  void channels(const std::vector<TrainingChannelRef>& selected) {
    u64(selected.size());
    for (const auto& channel : selected) {
      str(channel.field_id); u64(channel.ordinal); str(channel.label);
    }
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

std::string paging_selection_identity(const DatasetSnapshot& snapshot,
    const TrainingViewSpec& view, const WindowRequest& request) {
  PagingFields fields;
  fields.str("iotdbfs-window-paging/1");
  fields.str(snapshot.fingerprint);
  fields.str(view.view_id); fields.str(view.snapshot_fingerprint);
  fields.channels(view.input_channels); fields.channels(view.target_channels);
  fields.u64(view.input_length); fields.u64(view.gap);
  fields.u64(view.target_length); fields.u64(view.stride);
  fields.str(view.split.split_id);
  fields.interval(view.split.permitted_input);
  fields.interval(view.split.permitted_target);
  fields.u64(static_cast<std::uint64_t>(view.missing.null_series));
  fields.u64(static_cast<std::uint64_t>(view.missing.missing_channel));
  fields.u64(static_cast<std::uint64_t>(view.missing.ragged_channel));
  fields.u64(static_cast<std::uint64_t>(view.missing.null_value));
  fields.str(training_view_identity(view));
  fields.interval(*request.anchors);
  fields.u64(request.epoch); fields.u64(request.seed);
  fields.u64(request.shard_index); fields.u64(request.shard_count);
  return fields.digest();
}
} // namespace

WindowPlanner::WindowPlanner(std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits,
    std::shared_ptr<const PreparedContext> context, CancellationToken cancellation)
  : snapshot_(std::move(snapshot)), index_(std::move(index)),
    view_(std::move(view)), request_(std::move(request)), limits_(limits),
    context_(std::move(context)),
    view_identity_(context_->view_identity),
    cancellation_(std::move(cancellation)) {
  candidate_budget_ = request_.shard_count >
      std::numeric_limits<std::uint64_t>::max() / limits_.max_candidate_windows
    ? std::numeric_limits<std::uint64_t>::max()
    : request_.shard_count * limits_.max_candidate_windows;
  if (request_.anchors) next_anchor_ = request_.anchors->begin;
}

QueryResult<WindowPlanner> WindowPlanner::create(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits,
    CancellationToken cancellation) {
  return create_impl(std::move(snapshot), std::move(index), std::move(view),
    std::move(request), limits, std::move(cancellation), nullptr);
}

QueryResult<WindowPlanner> WindowPlanner::create_prepared(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits,
    CancellationToken cancellation,
    std::shared_ptr<const PreparedContext> context) {
  return create_impl(std::move(snapshot), std::move(index), std::move(view),
    std::move(request), limits, std::move(cancellation), std::move(context));
}

QueryResult<WindowPlanner> WindowPlanner::create_impl(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits,
    CancellationToken cancellation,
    std::shared_ptr<const PreparedContext> context) {
  using Result = QueryResult<WindowPlanner>;
  if (cancellation.cancelled()) return Result::failure(cancelled());
  if (!snapshot || !index)
    return Result::failure(invalid("snapshot and coverage index are required"));
  if (!context) {
    auto status = validate_snapshot_view(*snapshot, *index, view);
    if (!status.ok()) return Result::failure(status);
    status = validate_training_transform(view);
    if (!status.ok()) return Result::failure(status);
  }
  if (view.experiment_id.empty())
    return Result::failure(invalid("training view needs an experiment identity"));
  auto status = validate_window_request(request, limits);
  if (!status.ok()) return Result::failure(status);
  if (view.split.split_id.empty() ||
      view.split.permitted_input.begin > view.split.permitted_input.end ||
      view.split.permitted_target.begin > view.split.permitted_target.end ||
      (view.target_length == 0) != view.target_channels.empty())
    return Result::failure(invalid("invalid training view or split"));
  std::uint64_t extent;
  if (!add(view.input_length, view.gap, extent) ||
      !add(extent, view.target_length, extent) ||
      extent > limits.max_steps_per_window ||
      view.input_channels.size() > limits.max_channels ||
      view.target_channels.size() > limits.max_channels)
    return Result::failure(exhausted("training window exceeds limits"));

  if (!context) {
    auto compiled = compile_prepared(snapshot, index, view, limits, cancellation);
    if (!compiled.ok()) return Result::failure(compiled.status());
    context = std::move(compiled.value());
  }
  const auto& series = context->series;
  const auto selection_identity = request.anchors ?
    paging_selection_identity(*snapshot, view, request) : std::string{};
  if (request.continuation) {
    const auto& cursor = *request.continuation;
    if (cursor.selection_identity != selection_identity ||
        cursor.series_index >= series.size() ||
        cursor.next_anchor < request.anchors->begin ||
        (cursor.next_anchor != request.anchors->end &&
         (cursor.next_anchor - request.anchors->begin) % view.stride != 0))
      return Result::failure(invalid("window continuation does not match this selection"));
  }
  WindowPlanner planner(std::move(snapshot), std::move(index),
    std::move(view), std::move(request), limits, std::move(context),
    std::move(cancellation));
  planner.selection_identity_ = selection_identity;
  if (planner.request_.continuation) {
    const auto& cursor = *planner.request_.continuation;
    planner.series_index_ = static_cast<std::size_t>(cursor.series_index);
    planner.next_anchor_ = cursor.next_anchor;
    planner.eligible_index_ = cursor.eligible_count;
  }
  return Result::success(std::move(planner));
}

QueryResult<std::shared_ptr<const WindowPlanner::PreparedContext>>
WindowPlanner::compile_prepared(std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    const TrainingViewSpec& view, TrainingQueryLimits limits,
    CancellationToken cancellation) {
  using Result = QueryResult<std::shared_ptr<const PreparedContext>>;
  QueryProfileTimer timer("window_planner_compile");
  auto context = std::make_shared<PreparedContext>();
  auto& series = context->series;
  auto& positions = context->positions;
  context->view_identity = training_view_identity(view);
  for (std::size_t i = 0; i < snapshot->rows.size(); ++i) {
    if (cancellation.cancelled()) return Result::failure(cancelled());
    const auto& row = snapshot->rows[i];
    auto found = positions.emplace(row.series_id, series.size());
    if (found.second) series.push_back(Series{row.series_id, row.steps});
    auto& group = series[found.first->second];
    group.steps.begin = std::min(group.steps.begin, row.steps.begin);
    group.steps.end = std::max(group.steps.end, row.steps.end);
    group.time_rows.push_back(i);
    for (const auto& channel : row.channel_bindings())
      group.channels.emplace(channel.field_id, channel.original_ordinal);
  }
  if (series.size() > limits.max_series)
    return Result::failure(exhausted("too many training series"));
  for (auto& group : series) {
    if (group.steps.begin >= group.steps.end)
      return Result::failure(invalid("invalid series step coverage"));
    std::sort(group.time_rows.begin(), group.time_rows.end(),
      [&](std::size_t a, std::size_t b) {
        const auto& left = snapshot->rows[a].steps;
        const auto& right = snapshot->rows[b].steps;
        return std::tie(left.begin, left.end, a) <
          std::tie(right.begin, right.end, b);
      });
    group.time_rows.erase(std::unique(group.time_rows.begin(),
      group.time_rows.end(), [&](std::size_t a, std::size_t b) {
        const auto& left = snapshot->rows[a].steps;
        const auto& right = snapshot->rows[b].steps;
        return left.begin == right.begin && left.end == right.end;
      }), group.time_rows.end());
  }
  // A view may mask a channel absent from some series, but not a channel that
  // has no binding anywhere in this snapshot.
  for (const auto* selected : {&view.input_channels, &view.target_channels})
    for (const auto& channel : *selected) {
      bool found = false;
      for (const auto& group : series)
        if (group.channels.count({channel.field_id, channel.ordinal})) {
          found = true; break;
        }
      if (!found && !series.empty())
        return Result::failure(QueryStatus::failure(QueryError::UnknownField,
          QueryPhase::Planning, "selected channel has no snapshot binding"));
    }
  WindowRequest all_anchors;
  all_anchors.max_windows = 1;
  all_anchors.anchors = StepInterval{0, std::numeric_limits<std::uint64_t>::max()};
  WindowPlanner compiler(snapshot, index, view, all_anchors, limits, context,
    cancellation);
  for (auto& group : series) {
    if (cancellation.cancelled()) return Result::failure(cancelled());
    auto status = compiler.build_anchor_ranges(group);
    if (!status.ok()) return Result::failure(status);
  }
  return Result::success(std::move(context));
}

std::uint64_t WindowPlanner::PreparedContext::estimated_metadata_bytes() const noexcept {
  std::uint64_t bytes = sizeof(*this) + 96;
  auto add_bytes = [&](std::uint64_t value) { bytes = saturating_add(bytes, value); };
  add_bytes(series.capacity() * sizeof(Series));
  add_bytes(view_identity.capacity() + 65);
  for (const auto& group : series) {
    add_bytes(group.id.capacity() + 65);
    add_bytes(group.time_rows.capacity() * sizeof(std::size_t) + 64);
    add_bytes(group.anchor_ranges.capacity() * sizeof(group.anchor_ranges[0]) + 64);
    for (const auto& channel : group.channels)
      add_bytes(sizeof(channel) + 96 + channel.first.capacity() + 65);
  }
  for (const auto& position : positions)
    add_bytes(sizeof(position) + 96 + position.first.capacity() + 65);
  return bytes;
}

std::optional<WindowContinuation> WindowPlanner::continuation() const {
  if (!request_.anchors || series_index_ >= context_->series.size())
    return std::nullopt;
  return WindowContinuation{selection_identity_,
    static_cast<std::uint64_t>(series_index_), next_anchor_, eligible_index_};
}

std::optional<std::pair<std::uint64_t, std::uint64_t>>
WindowPlanner::anchor_bounds(const Series& series) const {
  if (!request_.anchors || request_.anchors->begin >= request_.anchors->end)
    return std::nullopt;
  auto low = request_.anchors->begin;
  auto high = request_.anchors->end - 1;
  std::uint64_t history;
  if (!add(view_.input_length, view_.gap, history)) return std::nullopt;
  std::uint64_t input_start, split_start;
  if (!add(series.steps.begin, history, input_start) ||
      !add(view_.split.permitted_input.begin, history, split_start))
    return std::nullopt;
  low = std::max({low, input_start, split_start});
  high = std::min({high, saturating_add(series.steps.end, view_.gap),
    saturating_add(view_.split.permitted_input.end, view_.gap)});
  if (view_.target_length) {
    if (series.steps.end < view_.target_length ||
        view_.split.permitted_target.end < view_.target_length)
      return std::nullopt;
    low = std::max({low, series.steps.begin,
      view_.split.permitted_target.begin});
    high = std::min({high, series.steps.end - view_.target_length,
      view_.split.permitted_target.end - view_.target_length});
  }
  if (low > high) return std::nullopt;
  return std::make_pair(low, high);
}

QueryStatus WindowPlanner::build_anchor_ranges(Series& series) const {
  auto bounds = anchor_bounds(series);
  if (!bounds) return QueryStatus::success();
  series.anchor_ranges.push_back(*bounds);
  // In the all-Reject case, derive compact eligible anchor ranges from the
  // declared physical coverage. A sparse series cannot force next() to visit
  // every stride position in a large uncovered gap.
  if (view_.missing.null_series != MissingTreatment::RejectWindow ||
      view_.missing.missing_channel != MissingTreatment::RejectWindow ||
      view_.missing.ragged_channel != MissingTreatment::RejectWindow ||
      view_.missing.null_value != MissingTreatment::RejectWindow)
    return QueryStatus::success();
  std::uint64_t history;
  if (!add(view_.input_length, view_.gap, history))
    return exhausted("training window span overflows");
  auto intersect = [](const std::vector<std::pair<std::uint64_t, std::uint64_t>>& a,
      const std::vector<std::pair<std::uint64_t, std::uint64_t>>& b) {
    std::vector<std::pair<std::uint64_t, std::uint64_t>> result;
    std::size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
      const auto low = std::max(a[i].first, b[j].first);
      const auto high = std::min(a[i].second, b[j].second);
      if (low <= high) result.emplace_back(low, high);
      if (a[i].second < b[j].second) ++i; else ++j;
    }
    return result;
  };
  auto restrict_role = [&](const std::vector<TrainingChannelRef>& selected,
      WindowRole role) -> QueryStatus {
    for (const auto& ref : selected) {
      auto spans = index_->lookup_overlaps(series.id, ref.field_id,
        ref.ordinal, series.steps);
      if (!spans.ok()) return spans.status();
      std::vector<StepInterval> runs;
      for (const auto& span : spans.value())
        if (!span.null_series && !span.null_channel && !span.missing_channel)
          append_range(runs, span.steps);
      std::vector<std::pair<std::uint64_t, std::uint64_t>> permitted;
      for (const auto& run : runs) {
        std::uint64_t low, high;
        if (role == WindowRole::Input) {
          if (run.end - run.begin < view_.input_length ||
              !add(run.begin, history, low)) continue;
          high = saturating_add(run.end, view_.gap);
        } else {
          if (run.end - run.begin < view_.target_length) continue;
          low = run.begin;
          high = run.end - view_.target_length;
        }
        permitted.emplace_back(low, high);
      }
      series.anchor_ranges = intersect(series.anchor_ranges, permitted);
      if (series.anchor_ranges.empty()) break;
    }
    return QueryStatus::success();
  };
  auto status = restrict_role(view_.input_channels, WindowRole::Input);
  if (!status.ok() || series.anchor_ranges.empty()) return status;
  if (view_.target_length)
    return restrict_role(view_.target_channels, WindowRole::Target);
  return QueryStatus::success();
}

const SnapshotRowBinding* WindowPlanner::time_row_at(const Series& series,
    std::uint64_t step) const {
  auto upper = std::upper_bound(series.time_rows.begin(),
    series.time_rows.end(), step, [&](std::uint64_t position,
      std::size_t row_index) {
      return position < snapshot_->rows[row_index].steps.begin;
    });
  if (upper == series.time_rows.begin()) return nullptr;
  const auto& row = snapshot_->rows[*std::prev(upper)];
  return step < row.steps.end ? &row : nullptr;
}

QueryResult<std::int64_t> WindowPlanner::time_at(const Series& series,
    std::uint64_t step) const {
  const auto* row = time_row_at(series, step);
  if (!row || row->time.kind == TrainingTimeKind::Step)
    return QueryResult<std::int64_t>::failure(
      QueryStatus::failure(QueryError::InvalidDomainSlice,
        QueryPhase::Planning, "timestamp is unavailable for this step"));
  if (row->time.kind == TrainingTimeKind::RegularNanoseconds) {
    const auto value = static_cast<__int128>(row->time.start_ns) +
      (static_cast<__int128>(step) - row->steps.begin) * row->time.cadence_ns;
    if (value < std::numeric_limits<std::int64_t>::min() ||
        value > std::numeric_limits<std::int64_t>::max())
      return QueryResult<std::int64_t>::failure(invalid("training timestamp overflows"));
    return QueryResult<std::int64_t>::success(static_cast<std::int64_t>(value));
  }
  return QueryResult<std::int64_t>::success(
    row->time.timestamps_ns[static_cast<std::size_t>(step - row->steps.begin)]);
}

QueryResult<std::optional<PlannedWindow>> WindowPlanner::describe(
    const Series& series, std::uint64_t anchor) const {
  using Result = QueryResult<std::optional<PlannedWindow>>;
  if (cancellation_.cancelled()) return Result::failure(cancelled());
  std::uint64_t history, target_end;
  if (!add(view_.input_length, view_.gap, history) ||
      anchor < history || !add(anchor, view_.target_length, target_end))
    return Result::success(std::nullopt);
  const StepInterval input{anchor - history, anchor - view_.gap};
  const StepInterval target{anchor, target_end};
  if (!contains(series.steps, input) ||
      (view_.target_length && !contains(series.steps, target)) ||
      !contains(view_.split.permitted_input, input) ||
      (view_.target_length && !contains(view_.split.permitted_target, target)))
    return Result::success(std::nullopt);

  PlannedWindow planned;
  auto& descriptor = planned.descriptor;
  descriptor.sample_id = training_sample_id(snapshot_->fingerprint,
    view_identity_, series.id, anchor);
  descriptor.view_id = view_.view_id;
  descriptor.series_id = series.id;
  descriptor.anchor = anchor;
  descriptor.epoch = request_.epoch;
  descriptor.seed = request_.seed;
  descriptor.shard_index = request_.shard_index;
  descriptor.input = input;
  descriptor.target = target;

  auto add_channels = [&](const std::vector<TrainingChannelRef>& selected,
      StepInterval interval, WindowRole role,
      std::vector<WindowChannelSpan>& output) -> QueryResult<bool> {
    for (std::size_t channel_index = 0; channel_index < selected.size(); ++channel_index) {
      const auto& ref = selected[channel_index];
      const bool declared = series.channels.count({ref.field_id, ref.ordinal}) != 0;
      if (!declared && view_.missing.missing_channel == MissingTreatment::RejectWindow)
        return QueryResult<bool>::success(false);
      auto overlaps = index_->lookup_overlaps(series.id, ref.field_id,
        ref.ordinal, interval);
      if (!overlaps.ok()) return QueryResult<bool>::failure(overlaps.status());
      WindowChannelSpan channel{ref, std::nullopt, {}};
      std::uint64_t cursor = interval.begin;
      for (const auto& span : overlaps.value()) {
        if (span.steps.begin > cursor &&
            view_.missing.ragged_channel == MissingTreatment::RejectWindow)
          return QueryResult<bool>::success(false);
        cursor = span.steps.end;
        if (span.null_series) {
          if (view_.missing.null_series == MissingTreatment::RejectWindow)
            return QueryResult<bool>::success(false);
        } else if (span.missing_channel) {
          if (view_.missing.missing_channel == MissingTreatment::RejectWindow)
            return QueryResult<bool>::success(false);
        } else if (span.null_channel &&
                   view_.missing.null_value == MissingTreatment::RejectWindow) {
          return QueryResult<bool>::success(false);
        }
        if (!span.null_series && !span.missing_channel && !span.null_channel)
          append_range(channel.available_ranges, span.steps);
        planned.spans.push_back({role, channel_index, span});
      }
      if (cursor < interval.end && declared &&
          view_.missing.ragged_channel == MissingTreatment::RejectWindow)
        return QueryResult<bool>::success(false);
      if (channel.available_ranges.size() == 1)
        channel.available_steps = channel.available_ranges.front();
      output.push_back(std::move(channel));
    }
    return QueryResult<bool>::success(true);
  };
  auto inputs = add_channels(view_.input_channels, input, WindowRole::Input,
    descriptor.input_channels);
  if (!inputs.ok()) return Result::failure(inputs.status());
  if (!inputs.value()) return Result::success(std::nullopt);
  auto targets = add_channels(view_.target_channels, target, WindowRole::Target,
    descriptor.target_channels);
  if (!targets.ok()) return Result::failure(targets.status());
  if (!targets.value()) return Result::success(std::nullopt);
  if (planned.spans.size() > limits_.max_provenance_spans)
    return Result::failure(exhausted("too many window coverage spans"));

  const SnapshotRowBinding* time_binding = nullptr;
  auto consistent_time = [&](StepInterval interval) {
    if (interval.begin == interval.end) return true;
    auto upper = std::upper_bound(series.time_rows.begin(),
      series.time_rows.end(), interval.begin, [&](std::uint64_t position,
        std::size_t row_index) {
        return position < snapshot_->rows[row_index].steps.begin;
      });
    auto current = upper == series.time_rows.begin() ? upper : std::prev(upper);
    for (; current != series.time_rows.end(); ++current) {
      const auto& row = snapshot_->rows[*current];
      if (row.steps.begin >= interval.end) break;
      if (row.steps.end <= interval.begin) continue;
      if (!time_binding) time_binding = &row;
      else if (row.time.kind != time_binding->time.kind ||
               row.time.timezone != time_binding->time.timezone)
        return false;
    }
    return true;
  };
  if (!consistent_time(input) || !consistent_time(target))
    return Result::success(std::nullopt);
  if (!time_binding) return Result::success(std::nullopt);
  const auto kind = time_binding->time.kind;
  descriptor.timezone = time_binding->time.timezone;
  if (kind != TrainingTimeKind::Step) {
    for (auto step = input.begin; step < input.end; ++step) {
      if (cancellation_.cancelled()) return Result::failure(cancelled());
      auto time = time_at(series, step);
      if (!time.ok()) {
        if (time.status().code() == QueryError::InvalidDomainSlice)
          return Result::success(std::nullopt);
        return Result::failure(time.status());
      }
      descriptor.input_times_ns.push_back(time.value());
    }
    for (auto step = target.begin; step < target.end; ++step) {
      if (cancellation_.cancelled()) return Result::failure(cancelled());
      auto time = time_at(series, step);
      if (!time.ok()) {
        if (time.status().code() == QueryError::InvalidDomainSlice)
          return Result::success(std::nullopt);
        return Result::failure(time.status());
      }
      descriptor.target_times_ns.push_back(time.value());
    }
  }
  return Result::success(std::move(planned));
}

QueryResult<std::optional<PlannedWindow>> WindowPlanner::next() {
  using Result = QueryResult<std::optional<PlannedWindow>>;
  if (cancellation_.cancelled()) return Result::failure(cancelled());
  candidate_limit_reached_ = false;
  if (!request_.coordinates.empty()) {
    if (coordinate_index_ >= request_.coordinates.size())
      return Result::success(std::nullopt);
    if (candidate_count_ >= candidate_budget_)
      return Result::failure(exhausted("explicit window candidate limit exhausted"));
    const auto& coordinate = request_.coordinates[coordinate_index_++];
    auto found = context_->positions.find(coordinate.series_id);
    if (found == context_->positions.end())
      return Result::failure(invalid("explicit window series was not found"));
    ++candidate_count_;
    auto result = describe(context_->series[found->second], coordinate.anchor);
    if (!result.ok()) return result;
    if (!result.value())
      return Result::failure(invalid("explicit window is ineligible"));
    if (coordinate.expected_sample_id &&
        *coordinate.expected_sample_id != result.value()->descriptor.sample_id)
      return Result::failure(invalid("explicit sample id does not match"));
    return result;
  }
  if (!request_.anchors) return Result::success(std::nullopt);
  while (series_index_ < context_->series.size()) {
    if (cancellation_.cancelled()) return Result::failure(cancelled());
    const auto& series = context_->series[series_index_];
    const auto bounds = anchor_bounds(series);
    if (!bounds || next_anchor_ > bounds->second) {
      ++series_index_; next_anchor_ = request_.anchors->begin; continue;
    }
    const auto lower = std::max(next_anchor_, bounds->first);
    auto range = std::lower_bound(series.anchor_ranges.begin(),
      series.anchor_ranges.end(), lower, [](const auto& allowed, auto anchor) {
        return allowed.second < anchor;
      });
    if (range == series.anchor_ranges.end() || range->first > bounds->second) {
      ++series_index_; next_anchor_ = request_.anchors->begin; continue;
    }
    const auto range_begin = std::max(range->first, bounds->first);
    if (next_anchor_ < range_begin) {
      const auto distance = range_begin - request_.anchors->begin;
      const auto strides = distance / view_.stride +
        (distance % view_.stride != 0);
      std::uint64_t offset;
      if (strides > std::numeric_limits<std::uint64_t>::max() / view_.stride ||
          !add(request_.anchors->begin, strides * view_.stride, offset)) {
        ++series_index_; next_anchor_ = request_.anchors->begin; continue;
      }
      next_anchor_ = offset;
    }
    if (next_anchor_ > range->second || next_anchor_ > bounds->second) continue;
    if (candidate_count_ >= candidate_budget_) {
      candidate_limit_reached_ = true;
      return Result::success(std::nullopt);
    }
    const auto anchor = next_anchor_;
    if (!add(next_anchor_, view_.stride, next_anchor_))
      next_anchor_ = request_.anchors->end;
    ++candidate_count_;
    auto result = describe(context_->series[series_index_], anchor);
    if (!result.ok()) return result;
    if (!result.value()) continue;
    const auto ordinal = eligible_index_++;
    if (ordinal % request_.shard_count != request_.shard_index) continue;
    return result;
  }
  return Result::success(std::nullopt);
}

} // namespace iotdbfs
