#include "iotdbfs/query/training/TrainingQueryModel.h"

#include <gtest/gtest.h>

#include <limits>

namespace {
using namespace iotdbfs;

DatasetSpec dataset(bool timed = false) {
  TrainingSeries series;
  series.series_id = "sensor-A";
  series.binding.source = {"/dataset/a.arrow", 7, "arrow-ipc-stream"};
  series.binding.record_batch_index = 0;
  series.binding.row_index = 2;
  series.steps = {0, 12};
  if (timed) {
    series.time.kind = TrainingTimeKind::RegularNanoseconds;
    series.time.start_ns = 1000;
    series.time.cadence_ns = 10;
    series.time.timezone = "UTC";
  }
  series.channels = {
    {"target", 0, "temperature", std::nullopt, {0, 12}, true},
    {"target", 1, "humidity", std::nullopt, {0, 12}, true},
  };
  return {"weather", "snapshot-7", {series}};
}

TrainingViewSpec view() {
  TrainingViewSpec result;
  result.view_id = "forecast";
  result.experiment_id = "experiment";
  result.snapshot_fingerprint = "snapshot-7";
  result.input_channels = {{"target", 1, "humidity"},
    {"target", 0, "temperature"}};
  result.target_channels = {{"target", 0, "temperature"}};
  result.input_length = 3;
  result.gap = 1;
  result.target_length = 2;
  result.stride = 2;
  result.split = {"train", {0, 10}, {4, 10}};
  return result;
}

WindowRequest anchors() {
  WindowRequest request;
  request.anchors = StepInterval{0, 13};
  request.max_windows = 10;
  return request;
}

std::vector<WindowDescriptor> collect(WindowCursor& cursor) {
  std::vector<WindowDescriptor> output;
  for (;;) {
    auto next = cursor.next();
    EXPECT_TRUE(next.ok()) << next.status().message();
    if (!next.ok() || !next.value()) break;
    output.push_back(std::move(*next.value()));
  }
  return output;
}

TEST(TrainingQueryModel, EnumeratesHalfOpenInputGapTargetAndOrderedChannels) {
  auto source = std::make_shared<const DatasetSpec>(dataset(true));
  auto cursor_result = WindowCursor::create(source, view(), anchors());
  ASSERT_TRUE(cursor_result.ok()) << cursor_result.status().message();
  auto windows = collect(cursor_result.value());
  ASSERT_EQ(windows.size(), 3u);
  EXPECT_EQ(windows[0].anchor, 4u);
  EXPECT_EQ(windows[1].anchor, 6u);
  EXPECT_EQ(windows[2].anchor, 8u);
  EXPECT_EQ(windows[0].input.begin, 0u);
  EXPECT_EQ(windows[0].input.end, 3u);
  EXPECT_EQ(windows[0].target.begin, 4u);
  EXPECT_EQ(windows[0].target.end, 6u);
  ASSERT_EQ(windows[0].input_channels.size(), 2u);
  EXPECT_EQ(windows[0].input_channels[0].channel.label, "humidity");
  EXPECT_EQ(windows[0].input_channels[1].channel.label, "temperature");
  EXPECT_EQ(windows[0].input_times_ns,
    (std::vector<std::int64_t>{1000, 1010, 1020}));
  EXPECT_EQ(windows[0].target_times_ns,
    (std::vector<std::int64_t>{1040, 1050}));
  ASSERT_EQ(windows[0].source_spans.size(), 3u);
  EXPECT_EQ(windows[0].source_spans[0].row_index, 2u);
  EXPECT_EQ(windows[0].source_spans[0].channel_ordinal, 1u);
  EXPECT_EQ(windows[0].source_spans[2].role, WindowRole::Target);
}

TEST(TrainingQueryModel, SplitAndInputOnlyEndWindowPreservePositions) {
  auto source = std::make_shared<const DatasetSpec>(dataset());
  auto selected = view();
  selected.split.permitted_input = {2, 12};
  selected.split.permitted_target = {0, 9};
  auto cursor = WindowCursor::create(source, selected, anchors());
  ASSERT_TRUE(cursor.ok());
  auto windows = collect(cursor.value());
  ASSERT_EQ(windows.size(), 1u);
  EXPECT_EQ(windows[0].anchor, 6u);
  EXPECT_TRUE(windows[0].input_times_ns.empty());

  selected.target_length = 0;
  selected.target_channels.clear();
  selected.split.permitted_input = {0, 12};
  WindowRequest last;
  last.anchors = StepInterval{12, 13};
  last.max_windows = 1;
  auto input_only = WindowCursor::create(source, selected, last);
  ASSERT_TRUE(input_only.ok());
  auto final_window = collect(input_only.value());
  ASSERT_EQ(final_window.size(), 1u);
  EXPECT_EQ(final_window[0].input.begin, 8u);
  EXPECT_EQ(final_window[0].input.end, 11u);
  EXPECT_EQ(final_window[0].target.begin, 12u);
  EXPECT_EQ(final_window[0].target.end, 12u);
}

TEST(TrainingQueryModel, ShardsAreDeterministicAndDoNotChangeSampleIdentity) {
  auto source = std::make_shared<const DatasetSpec>(dataset());
  auto base = WindowCursor::create(source, view(), anchors());
  ASSERT_TRUE(base.ok());
  const auto all = collect(base.value());

  auto request = anchors();
  request.shard_count = 2;
  request.shard_index = 0;
  request.epoch = 3;
  request.seed = 91;
  auto left = WindowCursor::create(source, view(), request);
  ASSERT_TRUE(left.ok());
  auto left_windows = collect(left.value());
  request.shard_index = 1;
  auto right = WindowCursor::create(source, view(), request);
  ASSERT_TRUE(right.ok());
  auto right_windows = collect(right.value());
  ASSERT_EQ(left_windows.size(), 2u);
  ASSERT_EQ(right_windows.size(), 1u);
  EXPECT_EQ(left_windows[0].sample_id, all[0].sample_id);
  EXPECT_EQ(right_windows[0].sample_id, all[1].sample_id);
  EXPECT_EQ(left_windows[1].sample_id, all[2].sample_id);
  EXPECT_EQ(left_windows[0].epoch, 3u);
  EXPECT_EQ(left_windows[0].seed, 91u);
}

TEST(TrainingQueryModel, DescriptorOnlyCursorRejectsSnapshotPagingToken) {
  auto request = anchors();
  request.continuation = WindowContinuation{std::string(64, '0'), 0, 4, 0};
  auto cursor = WindowCursor::create(
    std::make_shared<const DatasetSpec>(dataset()), view(), request);
  EXPECT_FALSE(cursor.ok());
  EXPECT_EQ(cursor.status().code(), QueryError::InvalidQuery);
}

TEST(TrainingQueryModel, ExplicitCoordinatesPreserveOrderAndRejectIneligible) {
  auto source = std::make_shared<const DatasetSpec>(dataset());
  WindowRequest request;
  request.coordinates = {{"sensor-A", 8}, {"sensor-A", 4}};
  request.max_windows = 2;
  auto cursor = WindowCursor::create(source, view(), request);
  ASSERT_TRUE(cursor.ok());
  auto windows = collect(cursor.value());
  ASSERT_EQ(windows.size(), 2u);
  EXPECT_EQ(windows[0].anchor, 8u);
  EXPECT_EQ(windows[1].anchor, 4u);

  request.coordinates[0].expected_sample_id = windows[0].sample_id;
  auto identified = WindowCursor::create(source, view(), request);
  ASSERT_TRUE(identified.ok());
  EXPECT_TRUE(identified.value().next().ok());
  request.coordinates[0].expected_sample_id = "wrong-sample";
  auto mismatched = WindowCursor::create(source, view(), request);
  ASSERT_TRUE(mismatched.ok());
  EXPECT_FALSE(mismatched.value().next().ok());

  request.coordinates = {{"sensor-A", 3}};
  auto invalid = WindowCursor::create(source, view(), request);
  ASSERT_TRUE(invalid.ok());
  auto next = invalid.value().next();
  EXPECT_FALSE(next.ok());
  EXPECT_EQ(next.status().code(), QueryError::InvalidQuery);
  request.coordinates = {{"sensor-A", 4}, {"sensor-A", 6}};
  request.max_windows = 1;
  EXPECT_FALSE(validate_window_request(request).ok());
}

TEST(TrainingQueryModel, RaggedMaskKeepsStepAndValueValidityDistinct) {
  auto specification = dataset();
  specification.series[0].channels[1].coverage = {2, 8};
  auto selected = view();
  auto source = std::make_shared<const DatasetSpec>(specification);
  auto rejected = WindowCursor::create(source, selected, anchors());
  ASSERT_TRUE(rejected.ok());
  auto eligible = collect(rejected.value());
  ASSERT_EQ(eligible.size(), 2u);
  EXPECT_EQ(eligible[0].anchor, 6u);

  selected.missing.ragged_channel = MissingTreatment::Mask;
  selected.missing.null_value = MissingTreatment::Mask;
  auto masked = WindowCursor::create(source, selected, anchors());
  ASSERT_TRUE(masked.ok());
  auto windows = collect(masked.value());
  ASSERT_EQ(windows.size(), 3u);
  ASSERT_TRUE(windows[0].input_channels[0].available_steps);
  EXPECT_EQ(windows[0].input_channels[0].available_steps->begin, 2u);
  EXPECT_EQ(windows[0].input_channels[0].available_steps->end, 3u);

  WindowSample sample;
  sample.descriptor = windows[0];
  sample.input_values = {0, 10, 0, 11, 12, 12};
  sample.input_validity = {0, 1, 0, 1, 1, 1};
  sample.target_values = {14, 15};
  sample.target_validity = {1, 1};
  auto batch = WindowBatch::create(selected, {sample});
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  EXPECT_EQ(batch.value().samples().size(), 1u);
  EXPECT_EQ(batch.value().samples()[0].descriptor.source_spans.size(), 3u);

  sample.input_validity[0] = 1; // A value outside coverage cannot be valid.
  auto bad_mask = WindowBatch::create(selected, {sample});
  EXPECT_FALSE(bad_mask.ok());
  sample.input_validity[0] = 0;
  TrainingQueryLimits bounded;
  bounded.max_result_bytes = 4096;
  EXPECT_TRUE(WindowBatch::create(selected, {sample}, bounded).ok());
  sample.descriptor.source_spans[0].fragment_key.assign(8192, 'x');
  auto oversized_metadata = WindowBatch::create(selected, {sample}, bounded);
  EXPECT_EQ(oversized_metadata.status().code(), QueryError::ResourceExhausted);
}

TEST(TrainingQueryModel, EmptySeriesNullSeriesAndTimeValidation) {
  auto specification = dataset();
  specification.series[0].valid = false;
  auto selected = view();
  auto source = std::make_shared<const DatasetSpec>(specification);
  auto empty = WindowCursor::create(source, selected, anchors());
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(collect(empty.value()).empty());
  selected.missing.null_series = MissingTreatment::Mask;
  auto masked = WindowCursor::create(source, selected, anchors());
  ASSERT_TRUE(masked.ok());
  auto windows = collect(masked.value());
  ASSERT_EQ(windows.size(), 3u);
  EXPECT_TRUE(windows[0].source_spans.empty());
  EXPECT_FALSE(windows[0].input_channels[0].available_steps);

  specification.series[0].time.kind = TrainingTimeKind::ExplicitNanoseconds;
  specification.series[0].time.timezone = "UTC";
  specification.series[0].time.timestamps_ns = {1, 2, 3};
  EXPECT_FALSE(validate_dataset_spec(specification).ok());
  specification.series[0].time.timestamps_ns.resize(12);
  for (std::size_t i = 0; i < 12; ++i)
    specification.series[0].time.timestamps_ns[i] = static_cast<std::int64_t>(i);
  EXPECT_TRUE(validate_dataset_spec(specification).ok());
  specification.series[0].time.timestamps_ns[2] = 1;
  EXPECT_FALSE(validate_dataset_spec(specification).ok());
}

TEST(TrainingQueryModel, HugeAnchorRangeSkipsOutsideSeriesWithoutEnumeration) {
  auto source = std::make_shared<const DatasetSpec>(dataset());
  auto request = anchors();
  request.anchors = StepInterval{0,
    std::numeric_limits<std::uint64_t>::max()};
  auto cursor = WindowCursor::create(source, view(), request);
  ASSERT_TRUE(cursor.ok());
  auto windows = collect(cursor.value());
  ASSERT_EQ(windows.size(), 3u);
  EXPECT_EQ(windows.back().anchor, 8u);
}

TEST(TrainingQueryModel, IndependentRowsAndIrregularTimeStaySeparate) {
  auto specification = dataset();
  auto& first = specification.series[0];
  first.time.kind = TrainingTimeKind::ExplicitNanoseconds;
  first.time.timezone = "UTC";
  first.time.timestamps_ns = {0, 2, 5, 9, 14, 20, 27, 35, 44, 54, 65, 77};
  auto second = first;
  second.series_id = "sensor-B";
  second.binding.row_index = 3;
  specification.series.push_back(second);
  auto source = std::make_shared<const DatasetSpec>(specification);
  auto cursor = WindowCursor::create(source, view(), anchors());
  ASSERT_TRUE(cursor.ok());
  auto windows = collect(cursor.value());
  ASSERT_EQ(windows.size(), 6u);
  EXPECT_EQ(windows[0].series_id, "sensor-A");
  EXPECT_EQ(windows[3].series_id, "sensor-B");
  EXPECT_NE(windows[0].sample_id, windows[3].sample_id);
  EXPECT_EQ(windows[0].input_times_ns,
    (std::vector<std::int64_t>{0, 2, 5}));
  EXPECT_EQ(windows[0].target_times_ns,
    (std::vector<std::int64_t>{14, 20}));
  EXPECT_EQ(windows[3].source_spans[0].row_index, 3u);
}

TEST(TrainingQueryModel, TargetFieldCanDifferAndEmptyDatasetIsEmpty) {
  auto specification = dataset();
  specification.series[0].channels.push_back(
    {"future", 0, "load", std::nullopt, {0, 12}, true});
  auto selected = view();
  selected.target_channels = {{"future", 0, "load"}};
  auto source = std::make_shared<const DatasetSpec>(specification);
  auto cursor = WindowCursor::create(source, selected, anchors());
  ASSERT_TRUE(cursor.ok());
  auto windows = collect(cursor.value());
  ASSERT_EQ(windows.size(), 3u);
  EXPECT_EQ(windows[0].source_spans.back().field_id, "future");
  EXPECT_EQ(windows[0].target_channels[0].channel.label, "load");

  specification.series.clear();
  auto empty_source = std::make_shared<const DatasetSpec>(specification);
  auto empty = WindowCursor::create(empty_source, selected, anchors());
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(collect(empty.value()).empty());
}

TEST(TrainingQueryModel, OverflowDuplicateAndBatchBudgetAreRejected) {
  auto specification = dataset();
  specification.series[0].binding.source.version = -1;
  EXPECT_FALSE(validate_dataset_spec(specification).ok());
  specification.series[0].binding.source.version = 7;
  auto selected = view();
  selected.gap = std::numeric_limits<std::uint64_t>::max();
  EXPECT_FALSE(validate_training_view(specification, selected).ok());
  selected = view();
  selected.input_channels.push_back(selected.input_channels[0]);
  EXPECT_FALSE(validate_training_view(specification, selected).ok());
  selected = view();
  auto request = anchors();
  request.max_windows = 0;
  EXPECT_FALSE(validate_window_request(request).ok());

  auto source = std::make_shared<const DatasetSpec>(specification);
  auto cursor = WindowCursor::create(source, selected, anchors());
  ASSERT_TRUE(cursor.ok());
  auto first = cursor.value().next();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(first.value());
  WindowSample sample;
  sample.descriptor = *first.value();
  sample.input_values.resize(6);
  sample.input_validity.assign(6, 1);
  sample.target_values.resize(2);
  sample.target_validity.assign(2, 1);
  TrainingQueryLimits limits;
  limits.max_result_bytes = 20;
  auto oversized = WindowBatch::create(selected, {sample}, limits);
  EXPECT_FALSE(oversized.ok());
  EXPECT_EQ(oversized.status().code(), QueryError::ResourceExhausted);
}

} // namespace
