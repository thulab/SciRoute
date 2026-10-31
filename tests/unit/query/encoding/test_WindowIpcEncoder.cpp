#include "iotdbfs/query/encoding/WindowIpcEncoder.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include <memory>
#include <cstdlib>
#include <fstream>
#include <initializer_list>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using namespace iotdbfs;

template<class T> T checked(arrow::Result<T> value) {
  if (!value.ok()) throw std::runtime_error(value.status().ToString());
  return std::move(value).ValueOrDie();
}

TrainingViewSpec view() {
  TrainingViewSpec result;
  result.view_id = "flat-view";
  result.experiment_id = "experiment";
  result.snapshot_fingerprint = "snapshot";
  result.input_channels = {{"target", 0, "x"}, {"target", 1, "y"}};
  result.target_channels = {{"target", 0, "x"}};
  result.input_length = 2;
  result.target_length = 1;
  result.stride = 1;
  result.split = {"train", {0, 8}, {0, 8}};
  result.missing.null_value = MissingTreatment::Mask;
  return result;
}

DatasetSpec dataset() {
  TrainingSeries series;
  series.series_id = "series";
  series.binding.source = {"/training/source", 7, "arrow"};
  series.binding.record_batch_index = 0;
  series.binding.row_index = 0;
  series.steps = {0, 8};
  series.time.kind = TrainingTimeKind::RegularNanoseconds;
  series.time.start_ns = 1000;
  series.time.cadence_ns = 10;
  series.time.timezone = "UTC";
  series.channels = {
    {"target", 0, "x", std::nullopt, {0, 8}, true},
    {"target", 1, "y", std::nullopt, {0, 8}, true},
  };
  return {"dataset", "snapshot", {series}};
}

WindowBatch sample_batch(const TrainingViewSpec& selected) {
  WindowRequest request;
  request.coordinates = {{"series", 2}};
  request.max_windows = 1;
  auto cursor = WindowCursor::create(
    std::make_shared<const DatasetSpec>(dataset()), selected, request);
  if (!cursor.ok()) throw std::runtime_error(cursor.status().message());
  auto next = cursor.value().next();
  if (!next.ok() || !next.value()) throw std::runtime_error("missing sample");
  WindowSample sample;
  sample.descriptor = *next.value();
  sample.input_values = {0, 100, 1, 101};
  sample.input_validity = {1, 1, 0, 1};
  sample.target_values = {2};
  sample.target_validity = {1};
  auto transformed = apply_training_view_transform(selected, sample);
  if (!transformed.ok()) throw std::runtime_error(transformed.message());
  auto batch = WindowBatch::create(selected, {std::move(sample)});
  if (!batch.ok()) throw std::runtime_error(batch.status().message());
  return std::move(batch.value());
}

std::pair<TrainingViewSpec, WindowBatch> long_single_channel_batch(
    std::initializer_list<std::uint64_t> anchors = {512}) {
  auto selected = view();
  selected.snapshot_fingerprint = std::string(64, 'a');
  selected.input_channels.resize(1);
  selected.input_length = 512;
  selected.target_length = 96;
  selected.split = {"train", {0, 1024}, {0, 1024}};
  auto source = dataset();
  source.snapshot_fingerprint = selected.snapshot_fingerprint;
  source.series.front().steps.end = 1024;
  source.series.front().channels.resize(1);
  source.series.front().channels.front().coverage.end = 1024;
  WindowRequest request;
  for (const auto anchor : anchors)
    request.coordinates.push_back({"series", anchor});
  request.max_windows = anchors.size();
  auto cursor = WindowCursor::create(
    std::make_shared<const DatasetSpec>(std::move(source)), selected, request);
  if (!cursor.ok()) throw std::runtime_error(cursor.status().message());
  std::vector<WindowSample> samples;
  for (std::size_t i = 0; i < anchors.size(); ++i) {
    auto next = cursor.value().next();
    if (!next.ok() || !next.value()) throw std::runtime_error("missing long sample");
    WindowSample sample;
    sample.descriptor = *next.value();
    sample.input_values.assign(512, 1.0f);
    sample.input_validity.assign(512, 1);
    sample.target_values.assign(96, 2.0f);
    sample.target_validity.assign(96, 1);
    samples.push_back(std::move(sample));
  }
  auto batch = WindowBatch::create(selected, std::move(samples));
  if (!batch.ok()) throw std::runtime_error(batch.status().message());
  return {std::move(selected), std::move(batch.value())};
}

std::shared_ptr<arrow::ipc::RecordBatchReader> reader(const BinaryValue& bytes) {
  auto buffer = std::make_shared<arrow::Buffer>(bytes.data(), bytes.size());
  auto source = std::make_shared<arrow::io::BufferReader>(buffer);
  return checked(arrow::ipc::RecordBatchStreamReader::Open(source));
}
} // namespace

TEST(WindowIpcEncoder, FixedFlatSchemaPreservesRoleMaskTimeAndProvenance) {
  const auto selected = view();
  auto native = sample_batch(selected);
  auto encoded = encode_window_batch_ipc(selected, native, 1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  if (const char* directory = std::getenv("IOTDBFS_A10_ORACLE_DIR")) {
    std::ofstream output(std::string(directory) + "/window.ipc", std::ios::binary);
    ASSERT_TRUE(output.good());
    output.write(reinterpret_cast<const char*>(encoded.value().data()),
      encoded.value().size());
    ASSERT_TRUE(output.good());
  }
  auto stream = reader(encoded.value());
  auto schema = stream->schema();
  ASSERT_EQ(schema->num_fields(), 9);
  ASSERT_NE(schema->metadata(), nullptr);
  EXPECT_EQ(schema->metadata()->Get("iotdbfs.protocol").ValueOr(""),
    "iotdbfs-window-flat/3");
  EXPECT_EQ(schema->metadata()->Get("iotdbfs.transform.kind").ValueOr(""),
    "identity");
  EXPECT_EQ(schema->metadata()->Get("iotdbfs.transform.artifact_id").ValueOr(""),
    "");
  EXPECT_EQ(schema->metadata()->Get("iotdbfs.transform.statistics.count").ValueOr(""),
    "0");
  EXPECT_TRUE(schema->field(0)->nullable());
  EXPECT_EQ(schema->metadata()->Get("iotdbfs.sample_id_location").ValueOr(""),
    "first_row_per_sample");
  EXPECT_EQ(schema->metadata()->Get("iotdbfs.input.0.field_id").ValueOr(""),
    "target");
  EXPECT_EQ(schema->metadata()->Get("iotdbfs.input.1.ordinal").ValueOr(""), "1");
  std::shared_ptr<arrow::RecordBatch> rows;
  ASSERT_TRUE(stream->ReadNext(&rows).ok());
  ASSERT_NE(rows, nullptr);
  ASSERT_EQ(rows->num_rows(), 3);
  auto role = std::static_pointer_cast<arrow::StringArray>(rows->column(2));
  auto ids = std::static_pointer_cast<arrow::StringArray>(rows->column(0));
  auto step = std::static_pointer_cast<arrow::UInt64Array>(rows->column(3));
  auto time = std::static_pointer_cast<arrow::Int64Array>(rows->column(4));
  auto descriptor = std::static_pointer_cast<arrow::LargeStringArray>(rows->column(5));
  auto input_x = std::static_pointer_cast<arrow::FloatArray>(rows->column(6));
  auto input_y = std::static_pointer_cast<arrow::FloatArray>(rows->column(7));
  auto target_x = std::static_pointer_cast<arrow::FloatArray>(rows->column(8));
  EXPECT_EQ(role->GetString(0), "input");
  EXPECT_EQ(role->GetString(2), "target");
  EXPECT_EQ(ids->GetString(0), native.samples()[0].descriptor.sample_id);
  EXPECT_TRUE(ids->IsNull(1));
  EXPECT_TRUE(ids->IsNull(2));
  EXPECT_EQ(step->Value(0), 0u);
  EXPECT_EQ(step->Value(2), 2u);
  EXPECT_EQ(time->Value(0), 1000);
  EXPECT_EQ(time->Value(2), 1020);
  EXPECT_NE(descriptor->GetString(0).find("/training/source"),
    std::string::npos);
  EXPECT_NE(descriptor->GetString(0).find("source_spans"),
    std::string::npos);
  EXPECT_TRUE(descriptor->IsNull(1));
  EXPECT_EQ(input_x->Value(0), 0);
  EXPECT_TRUE(input_x->IsNull(1)); // Missing data in an active role.
  EXPECT_EQ(input_y->Value(1), 101);
  EXPECT_TRUE(input_x->IsNull(2)); // Opposite role is inapplicable.
  EXPECT_EQ(target_x->Value(2), 2);
  ASSERT_TRUE(stream->ReadNext(&rows).ok());
  EXPECT_EQ(rows, nullptr);
}

TEST(WindowIpcEncoder, SchemaOnlyAndEncodedByteLimit) {
  const auto selected = view();
  auto empty = WindowBatch::create(selected, {});
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  auto encoded = encode_window_batch_ipc(selected, empty.value(), 1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto stream = reader(encoded.value());
  ASSERT_NE(stream->schema(), nullptr);
  std::shared_ptr<arrow::RecordBatch> rows;
  ASSERT_TRUE(stream->ReadNext(&rows).ok());
  EXPECT_EQ(rows, nullptr);
  auto limited = encode_window_batch_ipc(selected, sample_batch(selected), 64);
  EXPECT_EQ(limited.status().code(), QueryError::ResourceExhausted);
  auto complete = encode_window_batch_ipc(selected, sample_batch(selected), 1 << 20);
  ASSERT_TRUE(complete.ok()) << complete.status().message();
  auto one_byte_short = encode_window_batch_ipc(selected, sample_batch(selected),
    complete.value().size() - 1);
  EXPECT_EQ(one_byte_short.status().code(), QueryError::ResourceExhausted);
  auto exact = encode_window_batch_ipc(selected, sample_batch(selected),
    complete.value().size());
  ASSERT_TRUE(exact.ok()) << exact.status().message();
  EXPECT_EQ(exact.value(), complete.value());
}

TEST(WindowIpcEncoder, StandardizedValuesCarryCompleteTrainingStatistics) {
  auto selected = view();
  selected.transform.kind = TrainingTransformKind::Standardize;
  selected.transform.artifact_id = "train-statistics-v1";
  selected.transform.statistics = {
    {"target", 1, 100.0, 1.0},
    {"target", 0, -1.0000000000000002, 2.0}};
  auto native = sample_batch(selected);
  auto encoded = encode_window_batch_ipc(selected, native, 1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  if (const char* directory = std::getenv("IOTDBFS_A10_ORACLE_DIR")) {
    std::ofstream output(std::string(directory) + "/window-standardized.ipc",
      std::ios::binary);
    ASSERT_TRUE(output.good());
    output.write(reinterpret_cast<const char*>(encoded.value().data()),
      encoded.value().size());
    ASSERT_TRUE(output.good());
  }
  auto stream = reader(encoded.value());
  auto metadata = stream->schema()->metadata();
  ASSERT_NE(metadata, nullptr);
  EXPECT_EQ(metadata->Get("iotdbfs.protocol").ValueOr(""),
    "iotdbfs-window-flat/3");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.kind").ValueOr(""), "standardize");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.artifact_id").ValueOr(""),
    "train-statistics-v1");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.count").ValueOr(""), "2");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.0.field_id").ValueOr(""),
    "target");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.0.ordinal").ValueOr(""),
    "0");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.0.mean").ValueOr(""),
    "-1.0000000000000002");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.0.stddev").ValueOr(""),
    "2");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.1.ordinal").ValueOr(""),
    "1");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.1.mean").ValueOr(""),
    "100");
  EXPECT_EQ(metadata->Get("iotdbfs.transform.statistics.1.stddev").ValueOr(""),
    "1");
  std::shared_ptr<arrow::RecordBatch> rows;
  ASSERT_TRUE(stream->ReadNext(&rows).ok());
  ASSERT_NE(rows, nullptr);
  auto input_x = std::static_pointer_cast<arrow::FloatArray>(rows->column(6));
  auto input_y = std::static_pointer_cast<arrow::FloatArray>(rows->column(7));
  auto target_x = std::static_pointer_cast<arrow::FloatArray>(rows->column(8));
  EXPECT_FLOAT_EQ(input_x->Value(0), 0.5f);
  EXPECT_TRUE(input_x->IsNull(1));
  EXPECT_FLOAT_EQ(input_y->Value(0), 0.0f);
  EXPECT_FLOAT_EQ(input_y->Value(1), 1.0f);
  EXPECT_FLOAT_EQ(target_x->Value(2), 1.5f);
}

TEST(WindowIpcEncoder, LongWindowDoesNotRepeatSampleIdentityOnEveryStep) {
  auto [selected, batch] = long_single_channel_batch();
  auto encoded = encode_window_batch_ipc(selected, batch, 1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  EXPECT_LT(encoded.value().size(), 40960u);
  auto stream = reader(encoded.value());
  std::shared_ptr<arrow::RecordBatch> rows;
  ASSERT_TRUE(stream->ReadNext(&rows).ok());
  ASSERT_NE(rows, nullptr);
  ASSERT_EQ(rows->num_rows(), 608);
  auto ids = std::static_pointer_cast<arrow::StringArray>(rows->column(0));
  EXPECT_FALSE(ids->IsNull(0));
  EXPECT_TRUE(ids->IsNull(1));
  EXPECT_TRUE(ids->IsNull(511));
  EXPECT_TRUE(ids->IsNull(512));
  EXPECT_EQ(ids->GetString(0), batch.samples()[0].descriptor.sample_id);
}

TEST(WindowIpcEncoder, SampleIndexConnectsRowsAcrossRecordBatches) {
  auto [selected, native] = long_single_channel_batch({512, 513});
  auto encoded = encode_window_batch_ipc(selected, native, 1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto stream = reader(encoded.value());
  std::shared_ptr<arrow::RecordBatch> first, second;
  ASSERT_TRUE(stream->ReadNext(&first).ok());
  ASSERT_TRUE(stream->ReadNext(&second).ok());
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  ASSERT_EQ(first->num_rows(), 1024);
  ASSERT_EQ(second->num_rows(), 192);
  auto first_ids = std::static_pointer_cast<arrow::StringArray>(first->column(0));
  auto first_indices = std::static_pointer_cast<arrow::UInt32Array>(first->column(1));
  auto second_ids = std::static_pointer_cast<arrow::StringArray>(second->column(0));
  auto second_indices = std::static_pointer_cast<arrow::UInt32Array>(second->column(1));
  EXPECT_EQ(first_ids->GetString(0), native.samples()[0].descriptor.sample_id);
  EXPECT_EQ(first_ids->GetString(608), native.samples()[1].descriptor.sample_id);
  EXPECT_TRUE(first_ids->IsNull(1023));
  EXPECT_EQ(first_indices->Value(0), 0u);
  EXPECT_EQ(first_indices->Value(608), 1u);
  EXPECT_EQ(first_indices->Value(1023), 1u);
  EXPECT_TRUE(second_ids->IsNull(0));
  EXPECT_EQ(second_indices->Value(0), 1u);
  EXPECT_EQ(second_indices->Value(191), 1u);
  std::shared_ptr<arrow::RecordBatch> end;
  ASSERT_TRUE(stream->ReadNext(&end).ok());
  EXPECT_EQ(end, nullptr);
}
