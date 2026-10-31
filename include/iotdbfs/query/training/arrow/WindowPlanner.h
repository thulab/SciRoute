#pragma once

#include "iotdbfs/query/training/arrow/DatasetSnapshot.h"
#include "iotdbfs/query/core/DecodedFragmentCache.h"

#include <memory>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace iotdbfs {

struct PlannedWindowSpan {
  WindowRole role{WindowRole::Input};
  std::size_t channel_index{0};
  DatasetCoverageSpan coverage;
};

struct PlannedWindow {
  WindowDescriptor descriptor;
  std::vector<PlannedWindowSpan> spans;
};

// Enumerates at most one materialized window at a time. Coverage is resolved
// from the pinned snapshot/index, never inferred from shard order.
class WindowPlanner {
public:
  static QueryResult<WindowPlanner> create(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request,
    TrainingQueryLimits limits = {}, CancellationToken cancellation = {});
  QueryResult<std::optional<PlannedWindow>> next();
  std::optional<WindowContinuation> continuation() const;
  bool candidate_limit_reached() const { return candidate_limit_reached_; }

private:
  friend class TrainingQueryModule;
  // Only TrainingQueryModule owns frozen copies that were fully validated at
  // preparation. The public factory must validate arbitrary mutable inputs.
  struct PreparedContext;
  static QueryResult<std::shared_ptr<const PreparedContext>> compile_prepared(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    const TrainingViewSpec& view, TrainingQueryLimits limits,
    CancellationToken cancellation = {});
  static QueryResult<WindowPlanner> create_prepared(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits,
    CancellationToken cancellation,
    std::shared_ptr<const PreparedContext> context);
  static QueryResult<WindowPlanner> create_impl(
    std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits,
    CancellationToken cancellation,
    std::shared_ptr<const PreparedContext> context);
  struct Series {
    std::string id;
    StepInterval steps;
    // Sorted, non-overlapping time segments; duplicate variable partitions
    // have identical time declarations and need only one lookup entry.
    std::vector<std::size_t> time_rows;
    std::set<std::pair<std::string, std::uint64_t>> channels;
    std::vector<std::pair<std::uint64_t, std::uint64_t>> anchor_ranges;
  };
  struct PreparedContext {
    std::vector<Series> series;
    std::map<std::string, std::size_t> positions;
    std::string view_identity;
    std::uint64_t estimated_metadata_bytes() const noexcept;
  };
  WindowPlanner(std::shared_ptr<const DatasetSnapshot> snapshot,
    std::shared_ptr<const DatasetCoverageIndex> index,
    TrainingViewSpec view, WindowRequest request, TrainingQueryLimits limits,
    std::shared_ptr<const PreparedContext> context, CancellationToken cancellation);
  QueryResult<std::optional<PlannedWindow>> describe(
    const Series& series, std::uint64_t anchor) const;
  std::optional<std::pair<std::uint64_t, std::uint64_t>>
    anchor_bounds(const Series& series) const;
  QueryStatus build_anchor_ranges(Series& series) const;
  QueryResult<std::int64_t> time_at(const Series& series,
    std::uint64_t step) const;
  const SnapshotRowBinding* time_row_at(const Series& series,
    std::uint64_t step) const;

  std::shared_ptr<const DatasetSnapshot> snapshot_;
  std::shared_ptr<const DatasetCoverageIndex> index_;
  TrainingViewSpec view_;
  WindowRequest request_;
  TrainingQueryLimits limits_;
  std::shared_ptr<const PreparedContext> context_;
  std::size_t series_index_{0};
  std::size_t coordinate_index_{0};
  std::uint64_t next_anchor_{0};
  std::uint64_t eligible_index_{0};
  std::uint64_t candidate_count_{0};
  std::uint64_t candidate_budget_{0};
  bool candidate_limit_reached_{false};
  std::string view_identity_;
  std::string selection_identity_;
  CancellationToken cancellation_;
};

} // namespace iotdbfs
