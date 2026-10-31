#include "iotdbfs/query/training/arrow/TrainingQueryModule.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/query/frontend/TypedQueryService.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/encoding/WindowIpcEncoder.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/compression.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <ctime>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <sys/wait.h>
#include <unistd.h>

namespace {
using namespace iotdbfs;

template<class T> T checked(arrow::Result<T> result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}
void checked(arrow::Status status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

// One physical row per file. Logical step position is supplied separately by
// SnapshotRowBinding; an adjacent file does not imply an adjacent series.
BinaryValue arrow_stream(std::uint64_t length, float first_step,
    std::uint64_t physical_channels = 2,
    std::optional<std::pair<std::uint64_t, std::uint64_t>> null_leaf = std::nullopt,
    std::optional<std::uint64_t> null_channel = std::nullopt,
    bool null_row = false, bool single = false) {
  auto leaf = std::make_shared<arrow::FloatBuilder>();
  auto channels = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), leaf);
  auto rows = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), channels);
  std::shared_ptr<arrow::ArrayBuilder> root = single
    ? std::static_pointer_cast<arrow::ArrayBuilder>(channels)
    : std::static_pointer_cast<arrow::ArrayBuilder>(rows);
  if (null_row) checked(root->AppendNull());
  else if (single) {
    checked(channels->Append());
    for (std::uint64_t position = 0; position < length; ++position) {
      if (null_leaf == std::make_pair(std::uint64_t{0}, position))
        checked(leaf->AppendNull());
      else checked(leaf->Append(first_step + static_cast<float>(position)));
    }
  }
  else {
    checked(rows->Append());
    for (std::uint64_t channel = 0; channel < physical_channels; ++channel) {
      if (null_channel == channel) { checked(channels->AppendNull()); continue; }
      checked(channels->Append());
      for (std::uint64_t position = 0; position < length; ++position) {
        if (null_leaf == std::make_pair(channel, position)) checked(leaf->AppendNull());
        else checked(leaf->Append(first_step + static_cast<float>(position) +
          static_cast<float>(channel * 100)));
      }
    }
  }
  std::shared_ptr<arrow::Array> array;
  checked(root->Finish(&array));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({arrow::field("target", array->type())}), 1, {array});
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  checked(writer->WriteRecordBatch(*batch));
  checked(writer->Close());
  auto buffer = checked(sink->Finish());
  return {buffer->data(), buffer->data() + buffer->size()};
}

BinaryValue arrow_layout(const BinaryValue& payload) {
  FormatSource source{payload.size(), [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<BinaryValue> {
    if (offset > payload.size() || length > payload.size() - offset) return std::nullopt;
    return BinaryValue(payload.begin() + offset, payload.begin() + offset + length);
  }};
  ArrowFormatAdapter adapter;
  auto session = adapter.begin_ingestion(source);
  if (!session) throw std::runtime_error("Arrow format detection failed");
  auto consumed = session->consume_chunk(0, payload.data(), payload.size());
  if (!consumed.ok()) throw std::runtime_error(consumed.error());
  auto finished = session->finish();
  if (!finished.ok()) throw std::runtime_error(finished.error());
  return finished.value().layout;
}

class Backend final : public StructuredBlobStorageBackend {
public:
  struct Entry { StructuredBlobManifest manifest; BinaryValue payload, layout; };
  std::map<StructuredBlobVersionKey, Entry> entries;
  std::function<void()> payload_read_hook;

  void put(const std::string& path, std::int64_t version, std::uint64_t length,
      float first_step, std::uint64_t physical_channels = 2,
      std::optional<std::pair<std::uint64_t, std::uint64_t>> null_leaf = std::nullopt,
      std::optional<std::uint64_t> null_channel = std::nullopt,
      bool null_row = false, bool single = false) {
    auto payload = arrow_stream(length, first_step, physical_channels,
      null_leaf, null_channel, null_row, single);
    auto layout = arrow_layout(payload);
    StructuredBlobManifest manifest;
    manifest.key = {path, version};
    manifest.format_id = "arrow";
    manifest.payload_size = payload.size();
    manifest.layout_size = layout.size();
    manifest.phase = StructuredBlobPhase::Published;
    manifest.published = true;
    manifest.revision = 5;
    entries[manifest.key] = {manifest, std::move(payload), std::move(layout)};
  }

  int layout_reads(const StructuredBlobVersionKey& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = layout_reads_.find(key);
    return found == layout_reads_.end() ? 0 : found->second;
  }
  int payload_reads(const StructuredBlobVersionKey& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = payload_reads_.find(key);
    return found == payload_reads_.end() ? 0 : found->second;
  }
  std::vector<ArrowBufferSpan> payload_ranges(const StructuredBlobVersionKey& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = payload_ranges_.find(key);
    return found == payload_ranges_.end() ? std::vector<ArrowBufferSpan>{} : found->second;
  }
  int payload_batches(const StructuredBlobVersionKey& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = payload_batches_.find(key);
    return found == payload_batches_.end() ? 0 : found->second;
  }

  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return {}; }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(
      const StructuredBlobVersionKey& key) const override {
    auto found = entries.find(key);
    if (found == entries.end()) return StructuredBlobResult<StructuredBlobManifest>::failure(
      StructuredBlobError::NotFound, "missing test source");
    return StructuredBlobResult<StructuredBlobManifest>::success(found->second.manifest);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobManifest&) override { return {}; }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&, std::uint64_t,
      const BinaryValue&) override { return {}; }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,
      const BinaryValue&) override { return {}; }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,
      const MetadataMap&) override { return {}; }
  StructuredBlobResult<BinaryValue> read_payload_range(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length) const override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++payload_reads_[key];
      payload_ranges_[key].push_back({offset, length});
    }
    if (payload_read_hook) payload_read_hook();
    return read(key, offset, length, false);
  }
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobVersionKey& key,
      const StructuredBlobPayloadRanges& ranges) const override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++payload_batches_[key];
      payload_reads_[key] += static_cast<int>(ranges.size());
      for (const auto& range : ranges)
        payload_ranges_[key].push_back({range.offset, range.length});
    }
    if (payload_read_hook) payload_read_hook();
    StructuredBlobPayloadBatch result;
    result.reserve(ranges.size());
    for (const auto& range : ranges) {
      auto part = read(key, range.offset, range.length, false);
      if (!part.ok())
        return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
          part.status().code(), part.status().message());
      result.push_back(std::move(part.value()));
    }
    return StructuredBlobResult<StructuredBlobPayloadBatch>::success(std::move(result));
  }
  StructuredBlobResult<BinaryValue> read_layout_range(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length) const override {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++layout_reads_[key];
    }
    return read(key, offset, length, true);
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey& key, std::uint64_t) override {
    entries.erase(key); return {};
  }

private:
  StructuredBlobResult<BinaryValue> read(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length, bool layout) const {
    auto found = entries.find(key);
    if (found == entries.end()) return StructuredBlobResult<BinaryValue>::failure(
      StructuredBlobError::NotFound, "missing test source");
    const auto& bytes = layout ? found->second.layout : found->second.payload;
    if (offset > bytes.size() || length > bytes.size() - offset)
      return StructuredBlobResult<BinaryValue>::failure(
        StructuredBlobError::RangeError, "invalid test range");
    return StructuredBlobResult<BinaryValue>::success(
      {bytes.begin() + offset, bytes.begin() + offset + length});
  }
  mutable std::mutex mutex_;
  mutable std::map<StructuredBlobVersionKey, int> payload_reads_;
  mutable std::map<StructuredBlobVersionKey, int> layout_reads_;
  mutable std::map<StructuredBlobVersionKey, int> payload_batches_;
  mutable std::map<StructuredBlobVersionKey, std::vector<ArrowBufferSpan>> payload_ranges_;
};

SnapshotRowBinding row(std::uint64_t logical, std::size_t shard,
    std::string series, std::uint64_t begin, std::uint64_t length = 4,
    std::optional<std::uint64_t> physical_0 = 0,
    std::optional<std::uint64_t> physical_1 = 1) {
  SnapshotRowBinding result;
  result.logical_row = logical;
  result.shard_index = shard;
  result.series_id = std::move(series);
  result.steps = {begin, begin + length};
  result.declared_n_channels = 2;
  result.declared_length = length;
  result.channels = { {"target", 0, physical_0, "x", std::nullopt, length},
                      {"target", 1, physical_1, "y", std::nullopt, length} };
  return result;
}

struct Fixture {
  enum class Shape {
    Continuous, IndependentLast, VariablePartition, MissingAndNull,
    NullParent, NullChild, Timed, StepGap, SingleList,
    ChangedTimeAcrossGap, MixedTimeAcrossGap, ExplicitTimeAcrossGap
  };
  std::shared_ptr<Backend> backend = std::make_shared<Backend>();
  std::shared_ptr<const StructuredBlobStorage> storage;
  std::shared_ptr<QueryRuntime> runtime = std::make_shared<QueryRuntime>();
  DatasetSnapshot snapshot;
  DatasetCoverageIndex index;

  explicit Fixture(Shape shape = Shape::Continuous) {
    const bool partition = shape == Shape::VariablePartition;
    const bool missing = shape == Shape::MissingAndNull;
    const bool gap = shape == Shape::StepGap ||
      shape == Shape::ChangedTimeAcrossGap ||
      shape == Shape::MixedTimeAcrossGap ||
      shape == Shape::ExplicitTimeAcrossGap;
    const bool single = shape == Shape::SingleList;
    backend->put("/dataset/a", 1, 4, 0, single ? 1 : 2,
      std::nullopt, std::nullopt, false, single);
    backend->put("/dataset/b", 1, 4, gap ? 6 : 4,
      partition || missing || single ? 1 : 2,
      missing ? std::optional<std::pair<std::uint64_t, std::uint64_t>>{{0, 2}}
              : std::nullopt,
      shape == Shape::NullChild ? std::optional<std::uint64_t>{1} : std::nullopt,
      shape == Shape::NullParent, single);
    if (partition) backend->put("/dataset/d", 1, 4, 104, 1);
    backend->put("/dataset/c", 1, 4, gap ? 10 : 8,
      single ? 1 : 2, std::nullopt, std::nullopt, false, single);
    storage = std::make_shared<StructuredBlobStorage>(backend);
    DatasetSnapshotRequest request;
    request.dataset_id = "training-fixture";
    request.selector.paths = {{PathScopeKind::Subtree, "/dataset"}};
    request.selector.versions = VersionSelection::all();
    request.selector.formats.adapters = AdapterSelectionKind::Listed;
    request.selector.formats.adapter_ids = {"arrow"};
    request.selector.formats.include_byte_passthrough = false;
    request.shards = {{"a.arrow", {"/dataset/a", 1}},
      {"b.arrow", {"/dataset/b", 1}}};
    if (partition) request.shards.push_back({"d.arrow", {"/dataset/d", 1}});
    request.shards.push_back({"c.arrow", {"/dataset/c", 1}});
    request.rows = {row(0, 0, "series", 0),
      row(1, 1, "series", gap ? 6 : 4, 4, 0, missing ? std::nullopt
        : std::optional<std::uint64_t>{1})};
    if (missing) request.rows[1].declared_n_channels = 1;
    if (partition) {
      request.rows[1].declared_n_channels = 1;
      request.rows[1].channels.resize(1);
      auto other = row(2, 2, "series", 4, 4, 0, 0);
      other.declared_n_channels = 1;
      other.channels.erase(other.channels.begin());
      request.rows.push_back(std::move(other));
    }
    request.rows.push_back(row(request.rows.size(), request.shards.size() - 1,
      shape == Shape::IndependentLast ? "other" : "series",
      shape == Shape::ExplicitTimeAcrossGap ? 12 : gap ? 10 : 8));
    if (single) for (auto& binding : request.rows) {
      binding.declared_n_channels = 1;
      binding.channels.resize(1);
    }
    if (shape == Shape::Timed) {
      for (auto& binding : request.rows) {
        binding.time.kind = TrainingTimeKind::RegularNanoseconds;
        binding.time.start_ns = 1000 + static_cast<std::int64_t>(binding.steps.begin) * 10;
        binding.time.cadence_ns = 10;
        binding.time.timezone = "UTC";
      }
    }
    if (shape == Shape::ChangedTimeAcrossGap ||
        shape == Shape::MixedTimeAcrossGap ||
        shape == Shape::ExplicitTimeAcrossGap) {
      if (shape == Shape::ChangedTimeAcrossGap) {
        request.rows[0].time = {TrainingTimeKind::RegularNanoseconds,
          1000, 10, "UTC", {}};
        request.rows[1].time = {TrainingTimeKind::RegularNanoseconds,
          5000, 20, "Asia/Shanghai", {}};
        request.rows[2].time = {TrainingTimeKind::RegularNanoseconds,
          5080, 20, "Asia/Shanghai", {}};
      } else if (shape == Shape::MixedTimeAcrossGap) {
        request.rows[1].time = {TrainingTimeKind::RegularNanoseconds,
          5000, 20, "UTC", {}};
        request.rows[2].time = {TrainingTimeKind::RegularNanoseconds,
          5080, 20, "UTC", {}};
      } else {
        request.rows[1].time = {TrainingTimeKind::ExplicitNanoseconds,
          0, 0, "UTC", {6010, 6020, 6030, 6040}};
        request.rows[2].time = {TrainingTimeKind::ExplicitNanoseconds,
          0, 0, "UTC", {7010, 7020, 7030, 7040}};
      }
    }
    std::vector<StructuredBlobManifest> manifests;
    for (const auto& entry : backend->entries) manifests.push_back(entry.second.manifest);
    auto catalog = make_in_memory_published_version_catalog(std::move(manifests));
    auto prepared = prepare_dataset_snapshot(request, *catalog, storage);
    if (!prepared.ok()) throw std::runtime_error(prepared.status().message());
    snapshot = std::move(prepared.value());
    auto covered = prepare_dataset_coverage_index(snapshot, storage);
    if (!covered.ok()) throw std::runtime_error(covered.status().message());
    index = std::move(covered.value());
  }
};

struct LargeFixture {
  std::shared_ptr<Backend> backend = std::make_shared<Backend>();
  std::shared_ptr<const StructuredBlobStorage> storage;
  std::shared_ptr<QueryRuntime> runtime = std::make_shared<QueryRuntime>();
  DatasetSnapshot snapshot;
  DatasetCoverageIndex index;

  explicit LargeFixture(std::uint64_t length = 2048,
      std::optional<std::pair<std::uint64_t, std::uint64_t>> null_leaf =
        std::nullopt, std::uint64_t physical_channels = 1,
      bool explicit_timestamps = false, MetadataMap metadata = {}) {
    backend->put("/dataset/large", 1, length, 0, physical_channels, null_leaf);
    backend->entries.begin()->second.manifest.metadata = std::move(metadata);
    storage = std::make_shared<StructuredBlobStorage>(backend);
    DatasetSnapshotRequest request;
    request.dataset_id = "sparse-demand";
    request.selector.paths = {{PathScopeKind::Exact, "/dataset/large"}};
    request.selector.versions = VersionSelection::all();
    request.selector.formats.adapters = AdapterSelectionKind::Listed;
    request.selector.formats.adapter_ids = {"arrow"};
    request.selector.formats.include_byte_passthrough = false;
    request.shards = {{"large.arrow", {"/dataset/large", 1}}};
    request.rows = {row(0, 0, "series", 0, length, 0, std::nullopt)};
    if (explicit_timestamps) {
      request.rows[0].time.kind = TrainingTimeKind::ExplicitNanoseconds;
      request.rows[0].time.timezone = "UTC";
      for (std::uint64_t i = 0; i < length; ++i)
        request.rows[0].time.timestamps_ns.push_back(static_cast<std::int64_t>(i));
    }
    request.rows[0].declared_n_channels = physical_channels;
    request.rows[0].channels.clear();
    for (std::uint64_t channel = 0; channel < physical_channels; ++channel)
      request.rows[0].channels.push_back({"target", channel, channel,
        channel == 0 ? "x" : "c" + std::to_string(channel),
        std::nullopt, length});
    auto catalog = make_in_memory_published_version_catalog(
      std::vector<StructuredBlobManifest>{backend->entries.begin()->second.manifest});
    auto prepared = prepare_dataset_snapshot(request, *catalog, storage);
    if (!prepared.ok()) throw std::runtime_error(prepared.status().message());
    snapshot = std::move(prepared.value());
    auto covered = prepare_dataset_coverage_index(snapshot, storage);
    if (!covered.ok()) throw std::runtime_error(covered.status().message());
    index = std::move(covered.value());
  }
};

TrainingViewSpec view(const DatasetSnapshot& snapshot) {
  TrainingViewSpec selected;
  selected.view_id = "forecast";
  selected.experiment_id = "experiment";
  selected.snapshot_fingerprint = snapshot.fingerprint;
  selected.input_channels = {{"target", 1, "y"}, {"target", 0, "x"}};
  selected.target_channels = {{"target", 0, "x"}};
  selected.input_length = 3;
  selected.gap = 0;
  selected.target_length = 2;
  selected.stride = 1;
  selected.split = {"train", {0, 12}, {0, 12}};
  return selected;
}

WindowRequest coordinates(std::initializer_list<std::uint64_t> anchors,
    const std::string& series = "series") {
  WindowRequest request;
  for (auto anchor : anchors) request.coordinates.push_back({series, anchor});
  request.max_windows = anchors.size();
  return request;
}

std::shared_ptr<TrainingQueryModule> module(Fixture& fixture,
    TrainingViewSpec selected, ArrowDecodeConfig config = {}) {
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, std::move(selected), {}, {}, config);
  if (!prepared.ok()) throw std::runtime_error(prepared.status().message());
  return std::move(prepared.value());
}

} // namespace

TEST(TrainingQueryModule, CrossFileWindowHasOrderedValuesSplitAndExactProvenance) {
  Fixture fixture;
  ArrowDecodeConfig config; config.tile_elements = 2;
  auto query = module(fixture, view(fixture.snapshot), config);
  auto result = query->execute(coordinates({4}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 1u);
  const auto& sample = result.value().samples()[0];
  EXPECT_EQ(sample.descriptor.series_id, "series");
  EXPECT_EQ(sample.descriptor.input.begin, 1u);
  EXPECT_EQ(sample.descriptor.input.end, 4u);
  EXPECT_EQ(sample.descriptor.target.begin, 4u);
  EXPECT_EQ(sample.descriptor.target.end, 6u);
  EXPECT_EQ(sample.input_values, (std::vector<float>{101, 1, 102, 2, 103, 3}));
  EXPECT_EQ(sample.input_validity, (std::vector<std::uint8_t>{1, 1, 1, 1, 1, 1}));
  EXPECT_EQ(sample.target_values, (std::vector<float>{4, 5}));
  EXPECT_EQ(sample.target_validity, (std::vector<std::uint8_t>{1, 1}));
  EXPECT_EQ(sample.descriptor.sample_id, training_sample_id(
    fixture.snapshot.fingerprint,
    training_view_identity(view(fixture.snapshot)), "series", 4));
  ASSERT_GE(sample.descriptor.source_spans.size(), 3u);
  bool saw_input_a = false, saw_target_b = false;
  for (const auto& span : sample.descriptor.source_spans) {
    EXPECT_FALSE(span.fragment_key.empty());
    EXPECT_EQ(span.source.version, 1);
    if (span.role == WindowRole::Input && span.source.path == "/dataset/a")
      saw_input_a = true;
    if (span.role == WindowRole::Target && span.source.path == "/dataset/b")
      saw_target_b = true;
  }
  EXPECT_TRUE(saw_input_a);
  EXPECT_TRUE(saw_target_b);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
}

TEST(TrainingQueryModule, PreparedValidationReusePreservesCompletePagedResults) {
  Fixture fixture;
  const auto selected = view(fixture.snapshot);
  std::vector<BinaryValue> reference_pages;
  for (const bool reuse : {false, true}) {
    TrainingExecutionOptions execution;
    execution.reuse_prepared_validation = reuse;
    auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, selected, {}, {}, {}, execution);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    WindowRequest request;
    request.anchors = StepInterval{0, 12};
    request.max_windows = 2;
    std::size_t page = 0;
    QueryProfileRecord profile;
    do {
      ScopedQueryProfile scope(&profile);
      auto batch = prepared.value()->execute(request);
      ASSERT_TRUE(batch.ok()) << batch.status().message();
      auto ipc = encode_window_batch_ipc(selected, batch.value(), 1 << 20);
      ASSERT_TRUE(ipc.ok()) << ipc.status().message();
      if (!reuse) reference_pages.push_back(ipc.value());
      else {
        ASSERT_LT(page, reference_pages.size());
        EXPECT_EQ(ipc.value(), reference_pages[page]);
      }
      request.continuation = batch.value().continuation();
      ++page;
      ASSERT_LT(page, 20u);
    } while (request.continuation);
    EXPECT_EQ(page, reference_pages.size());
    // Diagnose actual reuse, not merely equality of an optimized no-op arm.
    EXPECT_EQ(profile.nanoseconds.count("snapshot_fingerprint"), reuse ? 0u : 1u);
    EXPECT_EQ(profile.nanoseconds.count("snapshot_view_validate"), reuse ? 0u : 1u);
  }
}

TEST(TrainingQueryModule, PublicFactoriesStillRejectEditedSnapshotAndView) {
  Fixture fixture;
  auto snapshot = std::make_shared<DatasetSnapshot>(fixture.snapshot);
  auto index = std::make_shared<DatasetCoverageIndex>(fixture.index);
  const auto selected = view(*snapshot);
  snapshot->rows.front().steps.end += 1;
  auto planner = WindowPlanner::create(snapshot, index, selected, coordinates({4}));
  ASSERT_FALSE(planner.ok());
  EXPECT_EQ(planner.status().code(), QueryError::StalePlan);
  auto assembler = WindowAssembler::create(*snapshot, fixture.storage,
    fixture.runtime, selected);
  ASSERT_FALSE(assembler.ok());
  EXPECT_EQ(assembler.status().code(), QueryError::StalePlan);
  auto rejected = TrainingQueryModule::prepare(*snapshot, *index, fixture.storage,
    fixture.runtime, selected);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::StalePlan);

  *snapshot = fixture.snapshot;
  auto edited_view = selected;
  edited_view.input_channels.front().label = "wrong-channel-label";
  EXPECT_FALSE(WindowPlanner::create(snapshot, index, edited_view,
    coordinates({4})).ok());
  EXPECT_FALSE(TrainingQueryModule::prepare(*snapshot, *index, fixture.storage,
    fixture.runtime, edited_view).ok());
}

TEST(TrainingQueryModule, PreparedValidationOwnsInputsAndRetainsRequestAndSourceChecks) {
  for (const bool reuse : {false, true}) {
    Fixture fixture;
    auto selected = view(fixture.snapshot);
    TrainingExecutionOptions execution;
    execution.reuse_prepared_validation = reuse;
    TrainingQueryLimits limits;
    limits.max_windows_per_batch = 1;
    auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, selected, limits, {}, {}, execution);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    // Callers retain mutable originals; the prepared object must own copies.
    fixture.snapshot.rows.front().steps.end += 100;
    selected.input_channels.front().ordinal = 999;
    auto good = prepared.value()->execute(coordinates({4}));
    ASSERT_TRUE(good.ok()) << good.status().message();
    EXPECT_EQ(good.value().samples()[0].input_values,
      (std::vector<float>{101, 1, 102, 2, 103, 3}));
    auto excessive = prepared.value()->execute(coordinates({4, 5}));
    EXPECT_FALSE(excessive.ok());
    CancellationToken cancellation;
    cancellation.cancel();
    auto cancelled = prepared.value()->execute(coordinates({4}), cancellation);
    ASSERT_FALSE(cancelled.ok());
    EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
    fixture.backend->entries.erase({"/dataset/c", 1});
    auto removed = prepared.value()->execute(coordinates({10}));
    ASSERT_FALSE(removed.ok());
    EXPECT_EQ(removed.status().code(), QueryError::SourceNotFound);
  }
}

TEST(TrainingQueryModule, PreparedOwnershipDoesNotStealMutableRvalueAliases) {
  for (const bool reuse : {false, true}) {
    Fixture fixture;
    auto selected = view(fixture.snapshot);
    selected.transform.kind = TrainingTransformKind::Standardize;
    selected.transform.artifact_id = "move-alias-statistics";
    selected.transform.statistics = {{"target", 0, 0.0, 1.0},
                                    {"target", 1, 100.0, 1.0}};
    auto& row_alias = fixture.snapshot.rows.front();
    auto& channel_alias = selected.input_channels.front();
    auto& statistic_alias = selected.transform.statistics.front();
    // The original index is non-const, so a retained mutable reference is
    // legal even though its accessor normally exposes read-only spans.
    auto& coverage_alias = const_cast<DatasetCoverageSpan&>(fixture.index.spans().front());
    TrainingExecutionOptions execution;
    execution.reuse_prepared_validation = reuse;
    auto prepared = TrainingQueryModule::prepare(std::move(fixture.snapshot),
      std::move(fixture.index), fixture.storage, fixture.runtime,
      std::move(selected), {}, {}, {}, execution);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    EXPECT_NE(&row_alias, &prepared.value()->snapshot().rows.front());
    EXPECT_NE(&channel_alias, &prepared.value()->view().input_channels.front());
    EXPECT_NE(&statistic_alias, &prepared.value()->view().transform.statistics.front());
    row_alias.steps.end += 100;
    channel_alias.ordinal = 999;
    statistic_alias.mean = 9999.0;
    coverage_alias.source_step_begin += 100;
    auto batch = prepared.value()->execute(coordinates({4}));
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    ASSERT_EQ(batch.value().samples().size(), 1u);
    EXPECT_EQ(batch.value().samples()[0].input_values,
      (std::vector<float>{1, 1, 2, 2, 3, 3}));
    EXPECT_EQ(batch.value().samples()[0].target_values, (std::vector<float>{4, 5}));
    EXPECT_EQ(prepared.value()->view().transform.statistics.front().mean, 0.0);
  }
}

TEST(TrainingQueryModule, ForkedChildRejectsPreparedModuleBeforeRuntimeUse) {
  for (const bool reuse : {false, true}) {
    Fixture fixture;
    TrainingExecutionOptions execution;
    execution.reuse_prepared_validation = reuse;
    auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, view(fixture.snapshot), {}, {}, {}, execution);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    const auto request = coordinates({4});
    const auto pid = ::fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
      // Inherited runtime worker threads no longer exist in this child. A
      // missing guard must fail boundedly instead of hanging the test runner.
      ::alarm(3);
      QueryProfileRecord profile;
      ScopedQueryProfile scope(&profile);
      auto result = prepared.value()->execute(request);
      const bool rejected = !result.ok() &&
        result.status().code() == QueryError::StalePlan &&
        result.status().phase() == QueryPhase::Validation &&
        profile.nanoseconds.empty();
      // Do not destroy or join the inherited runtime in the fork child.
      ::_exit(rejected ? 0 : 1);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
    auto parent = prepared.value()->execute(request);
    ASSERT_TRUE(parent.ok()) << parent.status().message();
  }
}

TEST(TrainingQueryModule, SingleLevelListContinuesAcrossFiles) {
  Fixture fixture(Fixture::Shape::SingleList);
  auto selected = view(fixture.snapshot);
  selected.input_channels = {{"target", 0, "x"}};
  selected.target_channels = {{"target", 0, "x"}};
  auto query = module(fixture, selected);
  auto result = query->execute(coordinates({4, 5}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 2u);
  EXPECT_EQ(result.value().samples()[0].input_values,
    (std::vector<float>{1, 2, 3}));
  EXPECT_EQ(result.value().samples()[0].target_values,
    (std::vector<float>{4, 5}));
  EXPECT_EQ(result.value().samples()[1].input_values,
    (std::vector<float>{2, 3, 4}));
  EXPECT_EQ(result.value().samples()[1].target_values,
    (std::vector<float>{5, 6}));
  EXPECT_EQ(result.value().samples()[0].input_validity,
    (std::vector<std::uint8_t>{1, 1, 1}));
  EXPECT_EQ(result.value().samples()[0].target_validity,
    (std::vector<std::uint8_t>{1, 1}));
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
}

TEST(TrainingQueryModule, OverlappingWindowsReusePhysicalFragmentsAcrossRequests) {
  Fixture fixture;
  ArrowDecodeConfig config; config.tile_elements = 2;
  auto query = module(fixture, view(fixture.snapshot), config);
  auto first = query->execute(coordinates({4, 5, 6}));
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_EQ(first.value().samples().size(), 3u);
  EXPECT_EQ(first.value().samples()[0].input_values[0], 101);
  EXPECT_EQ(first.value().samples()[1].input_values[0], 102);
  EXPECT_EQ(first.value().samples()[2].input_values[0], 103);
  const auto decoded = fixture.runtime->stats().decode_count;
  const auto read_a = fixture.backend->payload_reads({"/dataset/a", 1});
  const auto read_b = fixture.backend->payload_reads({"/dataset/b", 1});
  EXPECT_GT(decoded, 0u);
  auto second = query->execute(coordinates({5, 4}));
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second.value().samples().size(), 2u);
  EXPECT_EQ(second.value().samples()[0].descriptor.anchor, 5u);
  EXPECT_EQ(second.value().samples()[1].descriptor.anchor, 4u);
  EXPECT_EQ(fixture.runtime->stats().decode_count, decoded);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/a", 1}), read_a);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/b", 1}), read_b);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
}

TEST(TrainingQueryModule, OneBatchCoalescesOverlapWithoutReadingSparseGap) {
  LargeFixture fixture;
  TrainingViewSpec selected;
  selected.view_id = "sparse";
  selected.experiment_id = "experiment";
  selected.snapshot_fingerprint = fixture.snapshot.fingerprint;
  selected.input_channels = {{"target", 0, "x"}};
  selected.target_channels = {{"target", 0, "x"}};
  selected.input_length = 2;
  selected.target_length = 1;
  selected.stride = 1;
  selected.split = {"train", {0, 2048}, {0, 2048}};
  ArrowDecodeConfig config; config.tile_elements = 16;
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected, {}, {}, config);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto result = prepared.value()->execute(coordinates({4, 5, 1996, 1997}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 4u);
  EXPECT_EQ(result.value().samples()[0].input_values, (std::vector<float>{2, 3}));
  EXPECT_EQ(result.value().samples()[1].target_values, (std::vector<float>{5}));
  EXPECT_EQ(result.value().samples()[2].input_values,
    (std::vector<float>{1994, 1995}));
  EXPECT_EQ(result.value().samples()[3].target_values,
    (std::vector<float>{1997}));

  const StructuredBlobVersionKey key{"/dataset/large", 1};
  const auto ranges = fixture.backend->payload_ranges(key);
  EXPECT_EQ(fixture.runtime->stats().decode_count, 2u);
  ASSERT_EQ(ranges.size(), 2u);
  const auto& entry = fixture.backend->entries.at(key);
  auto parsed = parse_arrow_layout(entry.layout, entry.payload.size());
  ASSERT_TRUE(parsed.ok()) << parsed.status().message();
  ASSERT_EQ(parsed.value().batches.size(), 1u);
  ASSERT_EQ(parsed.value().batches[0].arrays.size(), 1u);
  const auto values_begin = parsed.value().batches[0].arrays[0].leaf_values.offset;
  const auto gap_begin = values_begin + 16 * sizeof(float);
  const auto gap_end = values_begin + 1984 * sizeof(float);
  for (const auto& range : ranges) {
    EXPECT_LE(values_begin, range.offset);
    EXPECT_FALSE(range.offset < gap_end && gap_begin < range.offset + range.length)
      << "read entered the unrequested interior of the large Arrow List";
  }
}

TEST(TrainingQueryModule, ManyChannelsBatchMissingTilesAcrossOverlappingWindows) {
  for (const std::size_t max_entries : {std::size_t{1024}, std::size_t{400}}) {
    LargeFixture fixture(32, std::nullopt, 300);
    QueryRuntimeConfig runtime_config;
    runtime_config.max_entries = max_entries;
    fixture.runtime = std::make_shared<QueryRuntime>(runtime_config);
    TrainingViewSpec selected;
    selected.view_id = "many-channels";
    selected.experiment_id = "experiment";
    selected.snapshot_fingerprint = fixture.snapshot.fingerprint;
    selected.input_length = 4;
    selected.target_length = 1;
    selected.stride = 1;
    selected.split = {"train", {0, 32}, {0, 32}};
    for (std::uint64_t channel = 0; channel < 300; ++channel) {
      TrainingChannelRef ref{"target", channel,
        channel == 0 ? "x" : "c" + std::to_string(channel)};
      selected.input_channels.push_back(ref);
      selected.target_channels.push_back(std::move(ref));
    }
    ArrowDecodeConfig config;
    config.tile_elements = 4;
    auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, selected, {}, {}, config);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    auto result = prepared.value()->execute(coordinates({5, 6}));
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.value().samples().size(), 2u);
    for (std::size_t i = 0; i < 2; ++i) {
      const auto& sample = result.value().samples()[i];
      EXPECT_EQ(sample.descriptor.anchor, 5u + i);
      ASSERT_EQ(sample.input_values.size(), 1200u);
      ASSERT_EQ(sample.target_values.size(), 300u);
      EXPECT_EQ(sample.input_values[10], 1001.0f + static_cast<float>(i));
      EXPECT_EQ(sample.target_values[10], 1005.0f + static_cast<float>(i));
    }
    const StructuredBlobVersionKey key{"/dataset/large", 1};
    // The 600 canonical tiles remain independent cache units. Each physical
    // batch transports many of their ranges, including when a 400-entry cache
    // forces the assembler to advance through bounded waves.
    EXPECT_GT(fixture.backend->payload_batches(key), 0);
    EXPECT_LT(fixture.backend->payload_batches(key), 100);
    EXPECT_GT(fixture.runtime->stats().decode_count, 0u);
    if (max_entries == 1024)
      EXPECT_EQ(fixture.runtime->stats().decode_count, 600u);
  }
}

namespace {
TrainingViewSpec parallel_view(const LargeFixture& fixture) {
  auto selected = view(fixture.snapshot);
  selected.input_length = 4;
  selected.target_length = 1;
  selected.split = {"train", {0, 32}, {0, 32}};
  selected.input_channels.clear();
  selected.target_channels.clear();
  for (std::uint64_t channel = 0; channel < 300; ++channel) {
    TrainingChannelRef ref{"target", channel,
      channel == 0 ? "x" : "c" + std::to_string(channel)};
    selected.input_channels.push_back(ref);
    selected.target_channels.push_back(std::move(ref));
  }
  return selected;
}

std::shared_ptr<TrainingQueryModule> parallel_module(LargeFixture& fixture,
    std::uint64_t parallelism, std::size_t max_entries = 16384,
    std::size_t max_inflight = 256, QueryExecutionOptions options = {}) {
  QueryRuntimeConfig runtime_config;
  runtime_config.max_entries = max_entries;
  runtime_config.max_inflight = max_inflight;
  fixture.runtime = std::make_shared<QueryRuntime>(runtime_config);
  ArrowDecodeConfig config; config.tile_elements = 4;
  TrainingExecutionOptions execution;
  execution.read_parallelism = parallelism;
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, parallel_view(fixture), {}, options,
    config, execution);
  if (!prepared.ok()) throw std::runtime_error(prepared.status().message());
  return std::move(prepared.value());
}
} // namespace

TEST(TrainingQueryModule, ParallelReadWavesAreBoundedAndPreserveValuesAndProvenance) {
  LargeFixture fixture(32, std::nullopt, 300);
  auto serial = parallel_module(fixture, 1)->execute(coordinates({5, 6}));
  ASSERT_TRUE(serial.ok()) << serial.status().message();
  for (const std::size_t lanes : {1u, 2u, 4u}) {
    auto query = parallel_module(fixture, lanes);
    std::mutex mutex;
    std::condition_variable ready;
    std::size_t active = 0, peak = 0, entered = 0;
    bool barrier_reached = true;
    fixture.backend->payload_read_hook = [&] {
      std::unique_lock<std::mutex> lock(mutex);
      ++active;
      peak = std::max(peak, active);
      ++entered;
      ready.notify_all();
      if (!ready.wait_for(lock, std::chrono::seconds(2),
          [&] { return entered >= lanes; })) barrier_reached = false;
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      lock.lock();
      --active;
    };
    CancellationToken token;
    auto result = query->execute(coordinates({5, 6}), token);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_FALSE(token.cancelled());
    EXPECT_TRUE(barrier_reached);
    EXPECT_EQ(active, 0u);
    EXPECT_EQ(peak, lanes);
    EXPECT_EQ(fixture.runtime->stats().decode_count, 600u);
    ASSERT_EQ(result.value().samples().size(), serial.value().samples().size());
    for (std::size_t i = 0; i < serial.value().samples().size(); ++i) {
      const auto& expected = serial.value().samples()[i];
      const auto& actual = result.value().samples()[i];
      EXPECT_EQ(actual.input_values, expected.input_values);
      EXPECT_EQ(actual.target_values, expected.target_values);
      EXPECT_EQ(actual.input_validity, expected.input_validity);
      EXPECT_EQ(actual.target_validity, expected.target_validity);
      EXPECT_EQ(actual.descriptor.sample_id, expected.descriptor.sample_id);
      const auto& spans = actual.descriptor.source_spans;
      const auto& expected_spans = expected.descriptor.source_spans;
      ASSERT_EQ(spans.size(), expected_spans.size());
      for (std::size_t j = 0; j < spans.size(); ++j) {
        EXPECT_EQ(spans[j].fragment_key, expected_spans[j].fragment_key);
        EXPECT_EQ(spans[j].steps.begin, expected_spans[j].steps.begin);
        EXPECT_EQ(spans[j].steps.end, expected_spans[j].steps.end);
        EXPECT_EQ(spans[j].source.path, expected_spans[j].source.path);
      }
    }
    const auto reads = fixture.runtime->stats().payload_read_calls;
    auto warm = query->execute(coordinates({5, 6}));
    ASSERT_TRUE(warm.ok()) << warm.status().message();
    EXPECT_EQ(fixture.runtime->stats().payload_read_calls, reads);
    fixture.backend->payload_read_hook = {};
  }
}

TEST(TrainingQueryModule, ParallelReadWavesRetainSequentialNonresidentFallback) {
  LargeFixture fixture(32, std::nullopt, 300);
  auto query = parallel_module(fixture, 4, 400);
  std::atomic<std::size_t> active{0}, peak{0};
  fixture.backend->payload_read_hook = [&] {
    const auto count = ++active;
    auto old = peak.load();
    while (old < count && !peak.compare_exchange_weak(old, count)) {}
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    --active;
  };
  auto result = query->execute(coordinates({5, 6}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value().samples().size(), 2u);
  EXPECT_EQ(active.load(), 0u);
  EXPECT_EQ(peak.load(), 1u);
}

TEST(TrainingQueryModule, ParallelReadWavesRespectSmallInflightAndLogicalBudgets) {
  LargeFixture fixture(32, std::nullopt, 300);
  auto query = parallel_module(fixture, 4, 16384, 4);
  auto result = query->execute(coordinates({5, 6}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(fixture.runtime->stats().decode_count, 600u);
  for (const std::size_t lanes : {1u, 4u}) {
    QueryExecutionOptions options;
    options.max_payload_bytes = 2999; // One window needs 300 * 5 * 4 bytes.
    auto limited = parallel_module(fixture, lanes, 16384, 256, options);
    auto rejected = limited->execute(coordinates({5, 6}));
    ASSERT_FALSE(rejected.ok());
    EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  }
}

TEST(TrainingQueryModule, ParallelReadCancellationAndFailureLeaveRuntimeReusable) {
  for (const bool cancel : {false, true}) {
    LargeFixture fixture(32, std::nullopt, 300);
    auto query = parallel_module(fixture, 4);
    CancellationToken token;
    std::mutex mutex;
    std::condition_variable ready;
    std::size_t active = 0, entered = 0;
    fixture.backend->payload_read_hook = [&] {
      std::unique_lock<std::mutex> lock(mutex);
      ++active;
      ++entered;
      ready.notify_all();
      ready.wait_for(lock, std::chrono::seconds(2), [&] { return entered >= 4; });
      if (cancel) token.cancel();
      --active;
      ready.notify_all();
      if (!cancel) throw std::runtime_error("injected backend batch failure");
    };
    auto result = query->execute(coordinates({5, 6}), token);
    ASSERT_FALSE(result.ok());
    if (cancel) EXPECT_EQ(result.status().code(), QueryError::Cancelled);
    else EXPECT_FALSE(token.cancelled());
    // Cancelled cache callers can leave a producer finishing independently;
    // wait for backend callbacks before replacing their test hook.
    {
      std::unique_lock<std::mutex> lock(mutex);
      ASSERT_TRUE(ready.wait_for(lock, std::chrono::seconds(2),
        [&] { return active == 0 && entered >= 4; }));
    }
    fixture.backend->payload_read_hook = {};
    auto retry = query->execute(coordinates({5, 6}));
    ASSERT_TRUE(retry.ok()) << retry.status().message();
    EXPECT_EQ(retry.value().samples().size(), 2u);
  }
}

TEST(TrainingQueryModule, RejectsInvalidReadParallelism) {
  Fixture fixture;
  for (const std::uint64_t lanes : {0u, 5u}) {
    TrainingExecutionOptions execution;
    execution.read_parallelism = lanes;
    auto result = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, view(fixture.snapshot), {}, {}, {}, execution);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::InvalidQuery);
  }
}

TEST(TrainingQueryModule, PrepareRejectsInvalidLimitsWithoutAnExecuteRequest) {
  Fixture fixture;
  using LimitMember = std::uint64_t TrainingQueryLimits::*;
  const LimitMember members[] = {
    &TrainingQueryLimits::max_series,
    &TrainingQueryLimits::max_channels,
    &TrainingQueryLimits::max_steps_per_window,
    &TrainingQueryLimits::max_windows_per_batch,
    &TrainingQueryLimits::max_candidate_windows,
    &TrainingQueryLimits::max_result_bytes,
    &TrainingQueryLimits::max_provenance_spans,
  };
  for (const auto member : members) {
    TrainingQueryLimits limits;
    limits.*member = 0;
    auto result = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, view(fixture.snapshot), limits);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::InvalidQuery);
  }
}

TEST(TrainingQueryModule, MetadataChargeGrowsWithCoverageChannelsAndExplicitTime) {
  auto prepare = [](LargeFixture& fixture) {
    auto selected = view(fixture.snapshot);
    selected.input_channels = {{"target", 0, "x"}};
    selected.target_channels = selected.input_channels;
    selected.split = {"train", fixture.snapshot.rows.front().steps,
      fixture.snapshot.rows.front().steps};
    return TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, std::move(selected));
  };
  LargeFixture small(4096);
  LargeFixture channels(4096, std::nullopt, 100);
  LargeFixture timestamps(4096, std::nullopt, 1, true);
  auto small_module = prepare(small);
  auto channel_module = prepare(channels);
  auto time_module = prepare(timestamps);
  ASSERT_TRUE(small_module.ok()) << small_module.status().message();
  ASSERT_TRUE(channel_module.ok()) << channel_module.status().message();
  ASSERT_TRUE(time_module.ok()) << time_module.status().message();
  const auto base = small_module.value()->estimated_metadata_bytes();
  EXPECT_GT(base, 0u);
  EXPECT_GT(channels.index.estimated_metadata_bytes(),
    small.index.estimated_metadata_bytes());
  EXPECT_GT(channel_module.value()->estimated_metadata_bytes(), base);
  EXPECT_GT(time_module.value()->estimated_metadata_bytes(), base);
  const auto before_decode = small_module.value()->estimated_metadata_bytes();
  auto windows = small_module.value()->execute(coordinates({5, 6}));
  ASSERT_TRUE(windows.ok()) << windows.status().message();
  EXPECT_GT(small.runtime->stats().decode_count, 0u);
  // Shared decoded-cache residency and transient output are separate budgets.
  EXPECT_EQ(small_module.value()->estimated_metadata_bytes(), before_decode);
}

TEST(TrainingQueryModule, MetadataChargeIncludesManifestContainersAndViewStatistics) {
  auto prepare = [](LargeFixture& fixture, bool standardize) {
    auto selected = view(fixture.snapshot);
    selected.input_channels = {{"target", 0, "x"}};
    selected.target_channels = selected.input_channels;
    selected.split = {"train", {0, 32}, {0, 32}};
    if (standardize) {
      selected.transform.kind = TrainingTransformKind::Standardize;
      selected.transform.artifact_id = std::string(4096, 'a');
      selected.transform.statistics = {{"target", 0, 2.0, 3.0}};
    }
    return TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, std::move(selected));
  };
  LargeFixture plain(32);
  MetadataMap metadata;
  metadata.emplace("description", MetadataValue(std::string(4096, 'd')));
  metadata.emplace("labels", MetadataValue(MetadataStringSet{
    std::string(4096, 'a'), std::string(4096, 'b')}));
  metadata.emplace("ordinals", MetadataValue(MetadataInt64Set{1, 2, 3, 4}));
  metadata.emplace("scales", MetadataValue(MetadataDoubleSet{1.0, 2.0}));
  metadata.emplace("flags", MetadataValue(MetadataBoolSet{false, true}));
  LargeFixture declared(32, std::nullopt, 1, false, std::move(metadata));
  auto base = prepare(plain, false);
  auto with_metadata = prepare(declared, false);
  auto transformed = prepare(declared, true);
  ASSERT_TRUE(base.ok()) << base.status().message();
  ASSERT_TRUE(with_metadata.ok()) << with_metadata.status().message();
  ASSERT_TRUE(transformed.ok()) << transformed.status().message();
  EXPECT_GT(with_metadata.value()->estimated_metadata_bytes(),
    base.value()->estimated_metadata_bytes());
  EXPECT_GT(transformed.value()->estimated_metadata_bytes(),
    with_metadata.value()->estimated_metadata_bytes());
}

TEST(TrainingQueryModule, CompiledTransformMatchesReferenceAndIsChargedToRegistry) {
  Fixture fixture;
  auto selected = view(fixture.snapshot);
  selected.transform.kind = TrainingTransformKind::Standardize;
  selected.transform.artifact_id = "training-statistics";
  selected.transform.statistics = {{"target", 0, 1.0, 3.0},
    {"target", 1, 100.0, 7.0}};
  TrainingExecutionOptions execution;
  execution.compiled_transform = false;
  auto reference = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected, {}, {}, {}, execution);
  execution.compiled_transform = true;
  auto optimized = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected, {}, {}, {}, execution);
  ASSERT_TRUE(reference.ok()) << reference.status().message();
  ASSERT_TRUE(optimized.ok()) << optimized.status().message();
  const auto plan = CompiledTrainingTransform::compile(selected);
  ASSERT_TRUE(plan.ok());
  EXPECT_EQ(optimized.value()->estimated_metadata_bytes(),
    reference.value()->estimated_metadata_bytes() + plan.value().estimated_dynamic_bytes());
  auto first = reference.value()->execute(coordinates({4, 5}));
  auto second = optimized.value()->execute(coordinates({4, 5}));
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(first.value().samples().size(), second.value().samples().size());
  for (std::size_t i = 0; i < first.value().samples().size(); ++i) {
    const auto& left = first.value().samples()[i];
    const auto& right = second.value().samples()[i];
    EXPECT_EQ(left.input_values, right.input_values);
    EXPECT_EQ(left.target_values, right.target_values);
    EXPECT_EQ(left.input_validity, right.input_validity);
    EXPECT_EQ(left.target_validity, right.target_validity);
    EXPECT_EQ(left.descriptor.sample_id, right.descriptor.sample_id);
  }
}

TEST(TrainingQueryModule, LogicalPayloadAndRangeBudgetsAccumulateAcrossShards) {
  for (bool limit_ranges : {false, true}) {
    Fixture fixture;
    QueryExecutionOptions options;
    if (limit_ranges) options.max_ranges = 4;
    else options.max_payload_bytes = 24;
    ArrowDecodeConfig config; config.tile_elements = 4;
    auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, view(fixture.snapshot), {}, options, config);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    auto result = prepared.value()->execute(coordinates({4}));
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted)
      << result.status().message();
    EXPECT_GT(fixture.backend->payload_reads({"/dataset/a", 1}), 0);
    EXPECT_EQ(fixture.backend->payload_reads({"/dataset/b", 1}), 0);
  }
}

TEST(TrainingQueryModule, RejectedLeafWindowsRefundLogicalBudget) {
  for (const bool limit_ranges : {false, true}) {
    LargeFixture fixture(100,
      std::make_pair(std::uint64_t{0}, std::uint64_t{3}));
    QueryRuntimeConfig runtime_config;
    runtime_config.max_entries = 4; // Use streaming to expose each window charge.
    fixture.runtime = std::make_shared<QueryRuntime>(runtime_config);
    TrainingViewSpec selected;
    selected.view_id = "null-budget";
    selected.experiment_id = "experiment";
    selected.snapshot_fingerprint = fixture.snapshot.fingerprint;
    selected.input_channels = {{"target", 0, "x"}};
    selected.target_channels = {{"target", 0, "x"}};
    selected.input_length = 1;
    selected.target_length = 1;
    selected.stride = 1;
    selected.split = {"train", {0, 100}, {0, 100}};
    QueryExecutionOptions options;
    if (limit_ranges) options.max_ranges = 9; // Layout + 4 accepted × 2.
    else options.max_payload_bytes = 40; // 4 accepted × 2 × (4B + bitmap).
    ArrowDecodeConfig config;
    config.tile_elements = 2;
    auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, selected, {}, options, config);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    WindowRequest request;
    request.anchors = StepInterval{1, 100};
    request.max_windows = 4;
    auto result = prepared.value()->execute(request);
    ASSERT_TRUE(result.ok()) << result.status().message();
    std::vector<std::uint64_t> anchors;
    for (const auto& sample : result.value().samples())
      anchors.push_back(sample.descriptor.anchor);
    EXPECT_EQ(anchors, (std::vector<std::uint64_t>{1, 2, 5, 6}));
  }
}

TEST(TrainingQueryModule, RejectedLookaheadWindowsDoNotReserveLogicalBudget) {
  for (const bool limit_ranges : {false, true}) {
    LargeFixture fixture(100,
      std::make_pair(std::uint64_t{0}, std::uint64_t{3}));
    TrainingViewSpec selected;
    selected.view_id = "lookahead-null-budget";
    selected.experiment_id = "experiment";
    selected.snapshot_fingerprint = fixture.snapshot.fingerprint;
    selected.input_channels = {{"target", 0, "x"}};
    selected.target_channels = {{"target", 0, "x"}};
    selected.input_length = 1;
    selected.target_length = 1;
    selected.stride = 1;
    selected.split = {"train", {0, 100}, {0, 100}};
    QueryExecutionOptions options;
    if (limit_ranges) options.max_ranges = 3; // Layout + accepted input/target.
    else options.max_payload_bytes = 10; // Accepted input/target with bitmaps.
    auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
      fixture.storage, fixture.runtime, selected, {}, options);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    WindowRequest request;
    request.anchors = StepInterval{3, 100};
    request.max_windows = 1;
    auto result = prepared.value()->execute(request);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.value().samples().size(), 1u);
    EXPECT_EQ(result.value().samples()[0].descriptor.anchor, 5u);
  }
}

TEST(TrainingQueryModule, OutputBudgetRejectsBeforeReadingAnyPayload) {
  Fixture fixture;
  TrainingQueryLimits limits;
  limits.max_result_bytes = 20; // Eight Float32/mask cells already require 40 B.
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, view(fixture.snapshot), limits);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto result = prepared.value()->execute(coordinates({4}));
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/a", 1}), 0);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/b", 1}), 0);
  EXPECT_EQ(fixture.runtime->stats().decode_count, 0u);
}

TEST(TrainingQueryModule, LookaheadLeasesEndWithTheExecution) {
  Fixture fixture;
  ArrowDecodeConfig config; config.tile_elements = 2;
  auto query = module(fixture, view(fixture.snapshot), config);
  auto result = query->execute(coordinates({4, 5, 6}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 3u);
  const auto before = fixture.runtime->stats();
  EXPECT_GT(before.cache_owned_bytes, 0u);
  EXPECT_EQ(before.pinned_bytes, 0u);
  fixture.runtime->cache().evict_all();
  const auto after = fixture.runtime->stats();
  EXPECT_EQ(after.pinned_bytes, 0u);
  EXPECT_EQ(after.live_bytes, 0u);
}

TEST(TrainingQueryModule, IndependentSeriesNeverStitchAcrossAdjacentFiles) {
  Fixture fixture(Fixture::Shape::IndependentLast);
  auto query = module(fixture, view(fixture.snapshot));
  auto request = coordinates({8}); // target would have to cross into "other".
  auto attempted = query->execute(request);
  EXPECT_FALSE(attempted.ok());
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);

  auto good = query->execute(coordinates({4}));
  ASSERT_TRUE(good.ok()) << good.status().message();
  ASSERT_EQ(good.value().samples().size(), 1u);
  EXPECT_EQ(good.value().samples()[0].descriptor.series_id, "series");
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
}

TEST(TrainingQueryModule, UnusedDeletedShardIsOpenedOnlyWhenDemanded) {
  Fixture fixture;
  fixture.backend->entries.erase({"/dataset/c", 1});
  auto query = module(fixture, view(fixture.snapshot));
  auto early = query->execute(coordinates({4}));
  ASSERT_TRUE(early.ok()) << early.status().message();
  ASSERT_EQ(early.value().samples().size(), 1u);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
  auto late = query->execute(coordinates({10}));
  EXPECT_FALSE(late.ok());
  EXPECT_EQ(late.status().code(), QueryError::SourceNotFound);
}

TEST(TrainingQueryModule, ExplicitVariablePartitionJoinsBySeriesStepAndChannel) {
  Fixture fixture(Fixture::Shape::VariablePartition);
  auto selected = view(fixture.snapshot);
  selected.target_channels = {{"target", 1, "y"}};
  auto query = module(fixture, selected);
  auto result = query->execute(coordinates({4}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 1u);
  const auto& sample = result.value().samples()[0];
  EXPECT_EQ(sample.input_values, (std::vector<float>{101, 1, 102, 2, 103, 3}));
  EXPECT_EQ(sample.target_values, (std::vector<float>{104, 105}));
  ASSERT_EQ(sample.target_validity, (std::vector<std::uint8_t>{1, 1}));
  bool target_from_partition = false;
  for (const auto& span : sample.descriptor.source_spans)
    if (span.role == WindowRole::Target && span.channel_ordinal == 1 &&
        span.source.path == "/dataset/d" && span.steps.begin == 4 &&
        span.steps.end == 6) target_from_partition = true;
  EXPECT_TRUE(target_from_partition);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/b", 1}), 0);
  EXPECT_GT(fixture.backend->payload_reads({"/dataset/d", 1}), 0);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
}

TEST(TrainingQueryModule, MissingChannelAndNullLeafHaveSeparateMaskPolicies) {
  Fixture fixture(Fixture::Shape::MissingAndNull);
  auto selected = view(fixture.snapshot);
  selected.missing.missing_channel = MissingTreatment::Mask;
  selected.missing.null_value = MissingTreatment::Mask;
  auto query = module(fixture, selected);
  auto result = query->execute(coordinates({6}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 1u);
  const auto& sample = result.value().samples()[0];
  EXPECT_EQ(sample.input_validity,
    (std::vector<std::uint8_t>{1, 1, 0, 1, 0, 1}));
  EXPECT_EQ(sample.input_values[0], 103);
  EXPECT_EQ(sample.input_values[1], 3);
  EXPECT_EQ(sample.input_values[3], 4);
  EXPECT_EQ(sample.input_values[5], 5);
  EXPECT_EQ(sample.target_validity, (std::vector<std::uint8_t>{0, 1}));
  EXPECT_EQ(sample.target_values[1], 7);

  auto reject_missing = selected;
  reject_missing.missing.missing_channel = MissingTreatment::RejectWindow;
  auto missing_query = module(fixture, reject_missing);
  EXPECT_FALSE(missing_query->execute(coordinates({6})).ok());

  auto reject_null = selected;
  reject_null.missing.null_value = MissingTreatment::RejectWindow;
  auto null_query = module(fixture, reject_null);
  EXPECT_FALSE(null_query->execute(coordinates({6})).ok());
}

TEST(TrainingQueryModule, NullParentAndNullChildUseDifferentPolicies) {
  Fixture parent(Fixture::Shape::NullParent);
  auto parent_view = view(parent.snapshot);
  parent_view.missing.null_series = MissingTreatment::Mask;
  auto parent_query = module(parent, parent_view);
  auto parent_result = parent_query->execute(coordinates({6}));
  ASSERT_TRUE(parent_result.ok()) << parent_result.status().message();
  ASSERT_EQ(parent_result.value().samples().size(), 1u);
  const auto& parent_sample = parent_result.value().samples()[0];
  EXPECT_EQ(parent_sample.input_validity,
    (std::vector<std::uint8_t>{1, 1, 0, 0, 0, 0}));
  EXPECT_EQ(parent_sample.target_validity,
    (std::vector<std::uint8_t>{0, 0}));
  auto reject_parent = view(parent.snapshot);
  auto parent_reject_query = module(parent, reject_parent);
  EXPECT_FALSE(parent_reject_query->execute(coordinates({6})).ok());

  Fixture child(Fixture::Shape::NullChild);
  auto child_view = view(child.snapshot);
  child_view.missing.null_value = MissingTreatment::Mask;
  auto child_query = module(child, child_view);
  auto child_result = child_query->execute(coordinates({6}));
  ASSERT_TRUE(child_result.ok()) << child_result.status().message();
  ASSERT_EQ(child_result.value().samples().size(), 1u);
  EXPECT_EQ(child_result.value().samples()[0].input_validity,
    (std::vector<std::uint8_t>{1, 1, 0, 1, 0, 1}));
  EXPECT_EQ(child_result.value().samples()[0].target_validity,
    (std::vector<std::uint8_t>{1, 1}));
  auto child_reject_query = module(child, view(child.snapshot));
  EXPECT_FALSE(child_reject_query->execute(coordinates({6})).ok());
}

TEST(TrainingQueryModule, RaggedMaskPreservesAnInteriorStepGap) {
  Fixture fixture(Fixture::Shape::StepGap);
  auto selected = view(fixture.snapshot);
  selected.split = {"train", {0, 14}, {0, 14}};
  selected.missing.ragged_channel = MissingTreatment::Mask;
  auto query = module(fixture, selected);
  auto result = query->execute(coordinates({6}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 1u);
  const auto& sample = result.value().samples()[0];
  EXPECT_EQ(sample.input_validity,
    (std::vector<std::uint8_t>{1, 1, 0, 0, 0, 0}));
  EXPECT_EQ(sample.input_values[0], 103);
  EXPECT_EQ(sample.input_values[1], 3);
  EXPECT_EQ(sample.target_values, (std::vector<float>{6, 7}));
  EXPECT_EQ(sample.target_validity, (std::vector<std::uint8_t>{1, 1}));
  auto reject = selected;
  reject.missing.ragged_channel = MissingTreatment::RejectWindow;
  auto reject_query = module(fixture, reject);
  EXPECT_FALSE(reject_query->execute(coordinates({6})).ok());
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
}

TEST(TrainingQueryModule, SplitAndBoundedGroupStopBeforeUnneededShard) {
  Fixture fixture;
  auto selected = view(fixture.snapshot);
  selected.split.permitted_target = {0, 6};
  auto query = module(fixture, selected);
  WindowRequest request;
  request.anchors = StepInterval{4, 12};
  request.max_windows = 1;
  auto result = query->execute(request);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 1u);
  EXPECT_EQ(result.value().samples()[0].descriptor.anchor, 4u);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
  EXPECT_FALSE(query->execute(coordinates({5})).ok());
}

TEST(TrainingQueryModule, AnchorRangePagesAcrossSeriesWithoutRepeatsOrGaps) {
  Fixture fixture(Fixture::Shape::IndependentLast);
  auto selected = view(fixture.snapshot);
  selected.input_length = 1;
  selected.target_length = 1;
  auto query = module(fixture, selected);
  WindowRequest request;
  request.anchors = StepInterval{1, 12};
  request.max_windows = 3;
  auto whole = request;
  whole.max_windows = 10;
  auto expected = query->execute(whole);
  ASSERT_TRUE(expected.ok()) << expected.status().message();
  ASSERT_EQ(expected.value().samples().size(), 10u);
  std::vector<std::string> actual_ids;
  std::vector<std::pair<std::string, std::uint64_t>> actual_coordinates;
  bool crossed_series = false;
  bool finished = false;
  for (unsigned page = 0; page < 6; ++page) {
    auto result = query->execute(request);
    ASSERT_TRUE(result.ok()) << result.status().message();
    for (const auto& sample : result.value().samples()) {
      actual_ids.push_back(sample.descriptor.sample_id);
      actual_coordinates.emplace_back(sample.descriptor.series_id,
        sample.descriptor.anchor);
      crossed_series |= sample.descriptor.series_id == "other";
    }
    if (!result.value().continuation()) { finished = true; break; }
    request.continuation = result.value().continuation();
    if (page == 0) {
      EXPECT_EQ(request.continuation->eligible_count, 3u);
      EXPECT_EQ(request.continuation->series_index, 0u);
      request.max_windows = 2; // Page size is not part of the selection identity.
      auto changed_range = request;
      changed_range.anchors = StepInterval{2, 12};
      auto rejected = query->execute(changed_range);
      EXPECT_FALSE(rejected.ok());
      EXPECT_EQ(rejected.status().code(), QueryError::InvalidQuery);
      auto changed_view = selected;
      changed_view.stride = 2;
      auto changed_query = module(fixture, changed_view);
      auto rejected_view = changed_query->execute(request);
      EXPECT_FALSE(rejected_view.ok());
      EXPECT_EQ(rejected_view.status().code(), QueryError::InvalidQuery);
    }
  }
  EXPECT_TRUE(finished);
  EXPECT_TRUE(crossed_series);
  std::vector<std::string> expected_ids;
  for (const auto& sample : expected.value().samples())
    expected_ids.push_back(sample.descriptor.sample_id);
  EXPECT_EQ(actual_ids, expected_ids);
  ASSERT_EQ(actual_coordinates.size(), 10u);
  EXPECT_EQ(actual_coordinates.front(),
    (std::pair<std::string, std::uint64_t>{"series", 1}));
  EXPECT_EQ(actual_coordinates.back(),
    (std::pair<std::string, std::uint64_t>{"other", 11}));
}

TEST(TrainingQueryModule, PagingPreservesGlobalWorkerShardOrdinal) {
  Fixture fixture(Fixture::Shape::IndependentLast);
  auto selected = view(fixture.snapshot);
  selected.input_length = 1;
  selected.target_length = 1;
  auto query = module(fixture, selected);
  WindowRequest request;
  request.anchors = StepInterval{1, 12};
  request.max_windows = 2;
  request.shard_count = 2;
  request.shard_index = 1;
  auto whole = request;
  whole.max_windows = 5;
  auto expected = query->execute(whole);
  ASSERT_TRUE(expected.ok()) << expected.status().message();
  ASSERT_EQ(expected.value().samples().size(), 5u);
  std::vector<std::string> actual;
  bool finished = false;
  for (unsigned page = 0; page < 5; ++page) {
    auto result = query->execute(request);
    ASSERT_TRUE(result.ok()) << result.status().message();
    for (const auto& sample : result.value().samples())
      actual.push_back(sample.descriptor.sample_id);
    if (!result.value().continuation()) { finished = true; break; }
    request.continuation = result.value().continuation();
    if (page == 0) EXPECT_EQ(request.continuation->eligible_count, 4u);
  }
  EXPECT_TRUE(finished);
  std::vector<std::string> expected_ids;
  for (const auto& sample : expected.value().samples())
    expected_ids.push_back(sample.descriptor.sample_id);
  EXPECT_EQ(actual, expected_ids);
}

TEST(TrainingQueryModule, EpochPagesPastMaximumBatchSize) {
  LargeFixture fixture(5000);
  TrainingViewSpec selected;
  selected.view_id = "large-epoch";
  selected.experiment_id = "experiment";
  selected.snapshot_fingerprint = fixture.snapshot.fingerprint;
  selected.input_channels = {{"target", 0, "x"}};
  selected.target_channels = {{"target", 0, "x"}};
  selected.input_length = 1;
  selected.target_length = 1;
  selected.stride = 1;
  selected.split = {"train", {0, 5000}, {0, 5000}};
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  WindowRequest request;
  request.anchors = StepInterval{1, 5000};
  request.max_windows = 4096;
  auto first = prepared.value()->execute(request);
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_EQ(first.value().samples().size(), 4096u);
  ASSERT_TRUE(first.value().continuation());
  EXPECT_EQ(first.value().samples().front().descriptor.anchor, 1u);
  EXPECT_EQ(first.value().samples().back().descriptor.anchor, 4096u);
  EXPECT_EQ(first.value().continuation()->eligible_count, 4096u);
  request.continuation = first.value().continuation();
  auto second = prepared.value()->execute(request);
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second.value().samples().size(), 903u);
  EXPECT_EQ(second.value().samples().front().descriptor.anchor, 4097u);
  EXPECT_EQ(second.value().samples().back().descriptor.anchor, 4999u);
  EXPECT_FALSE(second.value().continuation());
}

TEST(TrainingQueryModule, ThirtyTwoWorkersCanFillAFullPage) {
  LargeFixture fixture(140000);
  TrainingViewSpec selected;
  selected.view_id = "sharded-epoch";
  selected.experiment_id = "experiment";
  selected.snapshot_fingerprint = fixture.snapshot.fingerprint;
  selected.input_channels = {{"target", 0, "x"}};
  selected.target_channels = {{"target", 0, "x"}};
  selected.input_length = 1;
  selected.target_length = 1;
  selected.stride = 1;
  selected.split = {"train", {0, 140000}, {0, 140000}};
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  WindowRequest request;
  request.anchors = StepInterval{1, 140000};
  request.max_windows = 4096;
  request.shard_count = 32;
  request.shard_index = 31;
  auto result = prepared.value()->execute(request);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 4096u);
  EXPECT_EQ(result.value().samples().front().descriptor.anchor, 32u);
  EXPECT_EQ(result.value().samples().back().descriptor.anchor, 131072u);
  ASSERT_TRUE(result.value().continuation());
  EXPECT_EQ(result.value().continuation()->eligible_count, 131072u);
}

TEST(TrainingQueryModule, CandidateBudgetReturnsPartialBatchAndContinuation) {
  LargeFixture fixture(100, std::make_pair(std::uint64_t{0}, std::uint64_t{3}));
  TrainingViewSpec selected;
  selected.view_id = "partial-epoch";
  selected.experiment_id = "experiment";
  selected.snapshot_fingerprint = fixture.snapshot.fingerprint;
  selected.input_channels = {{"target", 0, "x"}};
  selected.target_channels = {{"target", 0, "x"}};
  selected.input_length = 1;
  selected.target_length = 1;
  selected.stride = 1;
  selected.split = {"train", {0, 100}, {0, 100}};
  TrainingQueryLimits limits;
  limits.max_candidate_windows = 8;
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected, limits);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  WindowRequest request;
  request.anchors = StepInterval{1, 100};
  request.max_windows = 8;
  auto first = prepared.value()->execute(request);
  ASSERT_TRUE(first.ok()) << first.status().message();
  std::vector<std::uint64_t> anchors;
  for (const auto& sample : first.value().samples())
    anchors.push_back(sample.descriptor.anchor);
  EXPECT_EQ(anchors, (std::vector<std::uint64_t>{1, 2, 5, 6, 7, 8}));
  ASSERT_TRUE(first.value().continuation());
  EXPECT_EQ(first.value().continuation()->next_anchor, 9u);
  EXPECT_EQ(first.value().continuation()->eligible_count, 8u);
  request.continuation = first.value().continuation();
  auto second = prepared.value()->execute(request);
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second.value().samples().size(), 8u);
  EXPECT_EQ(second.value().samples().front().descriptor.anchor, 9u);
  EXPECT_EQ(second.value().samples().back().descriptor.anchor, 16u);
}

TEST(TrainingQueryModule, PagingAdvancesAcrossValueRejectedWindows) {
  Fixture fixture(Fixture::Shape::MissingAndNull);
  auto selected = view(fixture.snapshot);
  selected.missing.missing_channel = MissingTreatment::Mask;
  auto query = module(fixture, selected);
  WindowRequest request;
  request.anchors = StepInterval{3, 11};
  request.max_windows = 2;
  auto whole = request;
  whole.max_windows = 3;
  auto expected = query->execute(whole);
  ASSERT_TRUE(expected.ok()) << expected.status().message();
  ASSERT_EQ(expected.value().samples().size(), 3u);
  std::vector<std::uint64_t> actual;
  for (unsigned page = 0; page < 4; ++page) {
    auto result = query->execute(request);
    ASSERT_TRUE(result.ok()) << result.status().message();
    for (const auto& sample : result.value().samples())
      actual.push_back(sample.descriptor.anchor);
    if (!result.value().continuation()) break;
    request.continuation = result.value().continuation();
  }
  std::vector<std::uint64_t> expected_anchors;
  for (const auto& sample : expected.value().samples())
    expected_anchors.push_back(sample.descriptor.anchor);
  EXPECT_EQ(actual, expected_anchors);
  EXPECT_EQ(actual, (std::vector<std::uint64_t>{3, 4, 10}));
}

TEST(TrainingQueryModule, PublishedNewVersionDoesNotChangePreparedSamples) {
  Fixture fixture;
  auto query = module(fixture, view(fixture.snapshot));
  auto first = query->execute(coordinates({4}));
  ASSERT_TRUE(first.ok()) << first.status().message();
  const auto id = first.value().samples()[0].descriptor.sample_id;
  fixture.backend->put("/dataset/a", 2, 4, 1000);
  auto second = query->execute(coordinates({4}));
  ASSERT_TRUE(second.ok()) << second.status().message();
  ASSERT_EQ(second.value().samples().size(), 1u);
  EXPECT_EQ(second.value().samples()[0].descriptor.sample_id, id);
  EXPECT_EQ(second.value().samples()[0].input_values,
    (std::vector<float>{101, 1, 102, 2, 103, 3}));
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/a", 2}), 0);
}

TEST(TrainingQueryModule, CrossFileRegularTimePreservesStepPositions) {
  Fixture fixture(Fixture::Shape::Timed);
  auto query = module(fixture, view(fixture.snapshot));
  auto result = query->execute(coordinates({4}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 1u);
  const auto& descriptor = result.value().samples()[0].descriptor;
  EXPECT_EQ(descriptor.input_times_ns,
    (std::vector<std::int64_t>{1010, 1020, 1030}));
  EXPECT_EQ(descriptor.target_times_ns,
    (std::vector<std::int64_t>{1040, 1050}));
  EXPECT_EQ(descriptor.timezone, "UTC");
}

TEST(TrainingQueryModule, TimeDefinitionsAcrossGapsAreBoundPerWindow) {
  Fixture fixture(Fixture::Shape::ChangedTimeAcrossGap);
  auto selected = view(fixture.snapshot);
  selected.input_length = 2;
  selected.target_length = 1;
  selected.split.permitted_input = {0, 14};
  selected.split.permitted_target = {0, 14};
  auto query = module(fixture, selected);
  auto early = query->execute(coordinates({2}));
  ASSERT_TRUE(early.ok()) << early.status().message();
  ASSERT_EQ(early.value().samples().size(), 1u);
  EXPECT_EQ(early.value().samples()[0].descriptor.input_times_ns,
    (std::vector<std::int64_t>{1000, 1010}));
  EXPECT_EQ(early.value().samples()[0].descriptor.timezone, "UTC");
  auto late = query->execute(coordinates({8}));
  ASSERT_TRUE(late.ok()) << late.status().message();
  ASSERT_EQ(late.value().samples().size(), 1u);
  EXPECT_EQ(late.value().samples()[0].descriptor.input_times_ns,
    (std::vector<std::int64_t>{5000, 5020}));
  EXPECT_EQ(late.value().samples()[0].descriptor.target_times_ns,
    (std::vector<std::int64_t>{5040}));
  EXPECT_EQ(late.value().samples()[0].descriptor.timezone, "Asia/Shanghai");
}

TEST(TrainingQueryModule, MixedTimeModesAcrossGapSkipOnlyCrossingWindow) {
  Fixture fixture(Fixture::Shape::MixedTimeAcrossGap);
  auto selected = view(fixture.snapshot);
  selected.input_length = 2;
  selected.target_length = 1;
  selected.missing.ragged_channel = MissingTreatment::Mask;
  selected.split.permitted_input = {0, 14};
  selected.split.permitted_target = {0, 14};
  auto query = module(fixture, selected);
  WindowRequest request;
  request.anchors = StepInterval{2, 10};
  request.max_windows = 16;
  auto result = query->execute(request);
  ASSERT_TRUE(result.ok()) << result.status().message();
  bool saw_step = false, saw_timed = false;
  for (const auto& sample : result.value().samples()) {
    if (sample.descriptor.anchor == 2) {
      saw_step = true;
      EXPECT_TRUE(sample.descriptor.input_times_ns.empty());
    }
    if (sample.descriptor.anchor == 8) {
      saw_timed = true;
      EXPECT_EQ(sample.descriptor.input_times_ns,
        (std::vector<std::int64_t>{5000, 5020}));
    }
    EXPECT_NE(sample.descriptor.anchor, 6u);
  }
  EXPECT_TRUE(saw_step);
  EXPECT_TRUE(saw_timed);
}

TEST(TrainingQueryModule, ExplicitTimeInLaterSegmentUsesItsOwnTimestamps) {
  Fixture fixture(Fixture::Shape::ExplicitTimeAcrossGap);
  auto selected = view(fixture.snapshot);
  selected.input_length = 2;
  selected.target_length = 1;
  selected.split.permitted_input = {0, 16};
  selected.split.permitted_target = {0, 16};
  auto query = module(fixture, selected);
  auto result = query->execute(coordinates({8}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().samples().size(), 1u);
  EXPECT_EQ(result.value().samples()[0].descriptor.input_times_ns,
    (std::vector<std::int64_t>{6010, 6020}));
  EXPECT_EQ(result.value().samples()[0].descriptor.target_times_ns,
    (std::vector<std::int64_t>{6030}));
}

TEST(TrainingQueryModule, RunningExecuteCanBeCancelledDuringPayloadRead) {
  Fixture fixture;
  auto query = module(fixture, view(fixture.snapshot));
  CancellationToken cancellation;
  fixture.backend->payload_read_hook = [cancellation] { cancellation.cancel(); };
  auto result = query->execute(coordinates({4}), cancellation);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
}

TEST(TrainingQueryModule, CallerCancelsRunningExecuteFromAnotherThread) {
  Fixture fixture;
  auto query = module(fixture, view(fixture.snapshot));
  CancellationToken cancellation;
  std::atomic<bool> read_started{false};
  fixture.backend->payload_read_hook = [&] {
    read_started.store(true, std::memory_order_release);
    while (!cancellation.cancelled())
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
  };
  std::thread stopper([&] {
    for (int i = 0; i < 2000 &&
        !read_started.load(std::memory_order_acquire); ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    cancellation.cancel();
  });
  auto result = query->execute(coordinates({4}), cancellation);
  stopper.join();
  EXPECT_TRUE(read_started.load(std::memory_order_acquire));
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
}

TEST(TrainingQueryModule, CancellationSignalIsNotSetBySuccessfulExecute) {
  Fixture fixture;
  auto query = module(fixture, view(fixture.snapshot));
  CancellationToken cancellation;
  auto result = query->execute(coordinates({4}), cancellation);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_FALSE(cancellation.cancelled());
}

TEST(TrainingQueryModule, EmptySelectionDoesNotReadPayload) {
  Fixture fixture;
  auto query = module(fixture, view(fixture.snapshot));
  WindowRequest request;
  request.max_windows = 1;
  auto result = query->execute(request);
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_TRUE(result.value().samples().empty());
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/a", 1}), 0);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/b", 1}), 0);
  EXPECT_EQ(fixture.backend->payload_reads({"/dataset/c", 1}), 0);
}

TEST(TrainingQueryModule, DifferentExperimentViewsShareDecodedBaseWithoutOutputAliasing) {
  Fixture fixture;
  ArrowDecodeConfig config; config.tile_elements = 2;
  auto standardized = view(fixture.snapshot);
  standardized.experiment_id = "experiment-standardized";
  standardized.transform.kind = TrainingTransformKind::Standardize;
  standardized.transform.artifact_id = "train-stats-v1";
  standardized.transform.statistics = {
    {"target", 0, 1.0, 2.0}, {"target", 1, 101.0, 2.0}};
  auto reordered = view(fixture.snapshot);
  reordered.experiment_id = "experiment-reordered";
  reordered.view_id = "shorter";
  reordered.input_channels = {{"target", 0, "x"}, {"target", 1, "y"}};
  reordered.input_length = 2;
  reordered.gap = 1;
  reordered.target_length = 1;
  reordered.stride = 2;
  auto first = module(fixture, standardized, config);
  auto second = module(fixture, reordered, config);

  const auto cpu_start = std::clock();
  auto normalized = first->execute(coordinates({4}));
  ASSERT_TRUE(normalized.ok()) << normalized.status().message();
  const auto after_first = fixture.runtime->stats();
  auto identity = second->execute(coordinates({4}));
  ASSERT_TRUE(identity.ok()) << identity.status().message();
  const auto after_second = fixture.runtime->stats();
  ASSERT_EQ(normalized.value().samples().size(), 1u);
  ASSERT_EQ(identity.value().samples().size(), 1u);
  EXPECT_EQ(normalized.value().samples()[0].input_values,
    (std::vector<float>{0, 0, 0.5f, 0.5f, 1, 1}));
  EXPECT_EQ(identity.value().samples()[0].input_values,
    (std::vector<float>{1, 101, 2, 102}));
  EXPECT_EQ(normalized.value().samples()[0].target_values,
    (std::vector<float>{1.5f, 2}));
  EXPECT_EQ(after_second.decode_count, after_first.decode_count);
  EXPECT_NE(normalized.value().samples()[0].descriptor.sample_id,
    identity.value().samples()[0].descriptor.sample_id);
  EXPECT_EQ(normalized.value().samples()[0].input_values.front(), 0);
  const auto cpu_two_views = std::clock() - cpu_start;
  auto different_target = reordered;
  different_target.experiment_id = "experiment-different-target";
  different_target.view_id = "target-y";
  different_target.target_channels = {{"target", 1, "y"}};
  auto third = module(fixture, different_target, config);
  auto y_target = third->execute(coordinates({4}));
  ASSERT_TRUE(y_target.ok()) << y_target.status().message();
  ASSERT_EQ(y_target.value().samples().size(), 1u);
  EXPECT_EQ(y_target.value().samples()[0].target_values,
    (std::vector<float>{104}));
  RecordProperty("a09_decode_count_after_first", after_first.decode_count);
  RecordProperty("a09_decode_count_after_second", after_second.decode_count);
  RecordProperty("a09_peak_runtime_bytes", after_second.peak_bytes);
  RecordProperty("a09_cpu_ticks_two_views", cpu_two_views);
}

TEST(TrainingQueryModule, DemandOnlyAndBoundedPrefetchPreserveSampleOrderAndShards) {
  Fixture fixture;
  auto selected = view(fixture.snapshot);
  selected.experiment_id = "prefetch-experiment";
  auto bounded = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected, {}, {}, {}, {8, true});
  auto demand = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, selected, {}, {}, {}, {1, false});
  ASSERT_TRUE(bounded.ok()) << bounded.status().message();
  ASSERT_TRUE(demand.ok()) << demand.status().message();
  auto request = coordinates({4, 5, 6});
  auto prefetched = bounded.value()->execute(request);
  auto decoded = demand.value()->execute(request);
  ASSERT_TRUE(prefetched.ok()) << prefetched.status().message();
  ASSERT_TRUE(decoded.ok()) << decoded.status().message();
  ASSERT_EQ(prefetched.value().samples().size(), decoded.value().samples().size());
  for (std::size_t i = 0; i < prefetched.value().samples().size(); ++i) {
    const auto& left = prefetched.value().samples()[i];
    const auto& right = decoded.value().samples()[i];
    EXPECT_EQ(left.descriptor.sample_id, right.descriptor.sample_id);
    EXPECT_EQ(left.input_values, right.input_values);
    EXPECT_EQ(left.target_values, right.target_values);
  }
  EXPECT_GT(fixture.runtime->scheduler().stats().granted_groups, 1u);
}

TEST(TrainingQueryModule, CancelledExperimentDoesNotCancelAnotherView) {
  Fixture fixture;
  auto first_view = view(fixture.snapshot);
  first_view.experiment_id = "cancelled-experiment";
  auto second_view = view(fixture.snapshot);
  second_view.experiment_id = "other-experiment";
  auto first = module(fixture, first_view);
  auto second = module(fixture, second_view);
  CancellationToken cancellation;
  fixture.backend->payload_read_hook = [cancellation] { cancellation.cancel(); };
  auto stopped = first->execute(coordinates({4}), cancellation);
  ASSERT_FALSE(stopped.ok());
  EXPECT_EQ(stopped.status().code(), QueryError::Cancelled);
  auto completed = second->execute(coordinates({4}));
  ASSERT_TRUE(completed.ok()) << completed.status().message();
  ASSERT_EQ(completed.value().samples().size(), 1u);
  EXPECT_EQ(completed.value().samples()[0].input_values,
    (std::vector<float>{101, 1, 102, 2, 103, 3}));
}

TEST(TrainingQueryModule, ExperimentExecutionOrderDoesNotChangeSamples) {
  Fixture first_fixture;
  Fixture second_fixture;
  auto make_views = [](const DatasetSnapshot& snapshot) {
    auto a = view(snapshot);
    a.experiment_id = "experiment-a";
    auto b = view(snapshot);
    b.experiment_id = "experiment-b";
    b.input_channels = {{"target", 0, "x"}, {"target", 1, "y"}};
    return std::make_pair(a, b);
  };
  auto first_views = make_views(first_fixture.snapshot);
  auto first_a = module(first_fixture, first_views.first);
  auto first_b = module(first_fixture, first_views.second);
  auto a_then = first_a->execute(coordinates({4}));
  auto b_then = first_b->execute(coordinates({4}));
  auto second_views = make_views(second_fixture.snapshot);
  auto second_a = module(second_fixture, second_views.first);
  auto second_b = module(second_fixture, second_views.second);
  auto b_first = second_b->execute(coordinates({4}));
  auto a_second = second_a->execute(coordinates({4}));
  ASSERT_TRUE(a_then.ok()) << a_then.status().message();
  ASSERT_TRUE(b_then.ok()) << b_then.status().message();
  ASSERT_TRUE(b_first.ok()) << b_first.status().message();
  ASSERT_TRUE(a_second.ok()) << a_second.status().message();
  ASSERT_EQ(a_then.value().samples().size(), 1u);
  ASSERT_EQ(b_then.value().samples().size(), 1u);
  ASSERT_EQ(b_first.value().samples().size(), 1u);
  ASSERT_EQ(a_second.value().samples().size(), 1u);
  EXPECT_EQ(a_then.value().samples()[0].descriptor.sample_id,
    a_second.value().samples()[0].descriptor.sample_id);
  EXPECT_EQ(b_then.value().samples()[0].descriptor.sample_id,
    b_first.value().samples()[0].descriptor.sample_id);
  EXPECT_EQ(a_then.value().samples()[0].input_values,
    a_second.value().samples()[0].input_values);
  EXPECT_EQ(b_then.value().samples()[0].input_values,
    b_first.value().samples()[0].input_values);
}

TEST(TrainingQueryModule, RegisteredWindowRoutePublishesOnlyCompleteIpc) {
  Fixture fixture;
  auto query = module(fixture, view(fixture.snapshot));
  auto frontend = std::make_shared<TypedQueryFrontend>();
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto route = frontend->register_windows(query, coordinates({4}));
  ASSERT_TRUE(route.ok()) << route.status().message();

  TypedQueryService too_small(nullptr, frontend, results, 64);
  auto rejected = too_small.materialize(route.value());
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(results->stats().registered_entries, 0u);

  for (const bool result_limit : {true, false}) {
    TypedResultStore::Config constrained;
    constrained.max_result_bytes = result_limit ? 256 : 1 << 20;
    constrained.max_resident_bytes = result_limit ? 1 << 20 : 256;
    auto limited_store = std::make_shared<TypedResultStore>(constrained);
    TypedQueryService limited(nullptr, frontend, limited_store, 1 << 20);
    auto oversized = limited.materialize(route.value());
    ASSERT_FALSE(oversized.ok());
    EXPECT_EQ(oversized.status().code(), QueryError::ResourceExhausted);
    EXPECT_EQ(oversized.status().phase(), QueryPhase::Encoding);
    EXPECT_EQ(limited_store->stats().registered_entries, 0u);
  }

  TypedQueryService service(nullptr, frontend, results, 1 << 20);
  auto id = service.materialize(route.value());
  ASSERT_TRUE(id.ok()) << id.status().message();
  auto handle = service.open_result(id.value());
  ASSERT_TRUE(handle.ok()) << handle.status().message();
  auto bytes = handle.value().read(0, handle.value().size());
  ASSERT_TRUE(bytes.ok());
  auto buffer = std::make_shared<arrow::Buffer>(bytes.value().data(),
    bytes.value().size());
  auto stream = checked(arrow::ipc::RecordBatchStreamReader::Open(
    std::make_shared<arrow::io::BufferReader>(buffer)));
  EXPECT_EQ(stream->schema()->metadata()->Get("iotdbfs.protocol").ValueOr(""),
    "iotdbfs-window-flat/3");
  auto batch = checked(stream->ReadNext()).batch;
  ASSERT_NE(batch, nullptr);
  ASSERT_EQ(batch->num_rows(), 5);
  auto roles = std::static_pointer_cast<arrow::StringArray>(batch->column(2));
  auto input_y = std::static_pointer_cast<arrow::FloatArray>(batch->column(6));
  auto input_x = std::static_pointer_cast<arrow::FloatArray>(batch->column(7));
  auto target_x = std::static_pointer_cast<arrow::FloatArray>(batch->column(8));
  EXPECT_EQ(roles->GetString(0), "input");
  EXPECT_EQ(roles->GetString(3), "target");
  EXPECT_FLOAT_EQ(input_y->Value(0), 101);
  EXPECT_FLOAT_EQ(input_x->Value(2), 3);
  EXPECT_TRUE(input_x->IsNull(3));
  EXPECT_FLOAT_EQ(target_x->Value(3), 4);
  EXPECT_FLOAT_EQ(target_x->Value(4), 5);
  EXPECT_EQ(roles->GetString(4), "target");
  EXPECT_EQ(checked(stream->ReadNext()).batch, nullptr);
}

TEST(TrainingQueryModule, ExpiredWindowRouteReleasesPinnedModule) {
  Fixture fixture;
  auto now = TypedQueryFrontend::Clock::time_point{};
  TypedQueryFrontend frontend(1, std::chrono::milliseconds(10),
    [&] { return now; });
  auto query = module(fixture, view(fixture.snapshot));
  std::weak_ptr<const TrainingQueryModule> weak = query;
  auto route = frontend.register_windows(query, coordinates({4}));
  ASSERT_TRUE(route.ok()) << route.status().message();
  {
    auto resolved = frontend.resolve(route.value());
    ASSERT_TRUE(resolved.ok()) << resolved.status().message();
    query.reset();
    now += std::chrono::milliseconds(10);
    EXPECT_EQ(frontend.size(), 0u);
    EXPECT_EQ(frontend.resolve(route.value()).status().code(),
      QueryError::SourceNotFound);
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(std::get<WindowExecutionRequest>(resolved.value()).module
      ->snapshot().fingerprint, fixture.snapshot.fingerprint);
  }
  EXPECT_TRUE(weak.expired());
}


TEST(TrainingQueryModule, CompiledPlannerConcurrentRequestsKeepIndependentPaging) {
  Fixture fixture;
  QueryProfileRecord preparation;
  std::shared_ptr<TrainingQueryModule> query;
  {
    ScopedQueryProfile scope(&preparation);
    query = module(fixture, view(fixture.snapshot));
  }
  EXPECT_EQ(preparation.nanoseconds.count("window_planner_compile"), 1u);
  const auto metadata_before = query->estimated_metadata_bytes();
  std::vector<std::uint64_t> anchors[2];
  std::string errors[2];
  QueryProfileRecord profiles[2];
  auto run = [&](std::size_t worker) {
    ScopedQueryProfile scope(&profiles[worker]);
    WindowRequest request;
    request.anchors = worker == 0 ? StepInterval{3, 8} : StepInterval{6, 11};
    request.max_windows = worker + 1;
    for (std::size_t page = 0; page < 10; ++page) {
      auto batch = query->execute(request);
      if (!batch.ok()) { errors[worker] = batch.status().message(); return; }
      for (const auto& sample : batch.value().samples())
        anchors[worker].push_back(sample.descriptor.anchor);
      request.continuation = batch.value().continuation();
      if (!request.continuation) return;
    }
    errors[worker] = "paging did not terminate";
  };
  std::thread first(run, 0), second(run, 1);
  first.join(); second.join();
  EXPECT_TRUE(errors[0].empty()) << errors[0];
  EXPECT_TRUE(errors[1].empty()) << errors[1];
  EXPECT_EQ(anchors[0], (std::vector<std::uint64_t>{3, 4, 5, 6, 7}));
  EXPECT_EQ(anchors[1], (std::vector<std::uint64_t>{6, 7, 8, 9, 10}));
  for (const auto& profile : profiles)
    EXPECT_EQ(profile.nanoseconds.count("window_planner_compile"), 0u);
  EXPECT_EQ(query->estimated_metadata_bytes(), metadata_before);
  auto bad = coordinates({4});
  bad.max_windows = 0;
  EXPECT_FALSE(query->execute(bad).ok());
  EXPECT_TRUE(query->execute(coordinates({4})).ok());
}

TEST(TrainingQueryModule, PreparedContextOwnsSharedChannelTables) {
  Fixture fixture;
  auto aliases = std::make_shared<std::vector<SnapshotChannelBinding>>(
    fixture.snapshot.rows.front().channel_bindings());
  for (auto& binding : fixture.snapshot.rows) {
    binding.channel_table = aliases;
    binding.channels.clear();
  }
  auto query = module(fixture, view(fixture.snapshot));
  ASSERT_NE(query->snapshot().rows.front().channel_table.get(), aliases.get());
  EXPECT_EQ(query->snapshot().rows.front().channel_table.get(),
    query->snapshot().rows.back().channel_table.get());
  aliases->front().physical_ordinal = 999;
  aliases->front().length = 999;
  auto batch = query->execute(coordinates({4}));
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  EXPECT_EQ(batch.value().samples()[0].input_values,
    (std::vector<float>{101, 1, 102, 2, 103, 3}));
  CancellationToken cancelled;
  cancelled.cancel();
  EXPECT_EQ(query->execute(coordinates({4}), cancelled).status().code(),
    QueryError::Cancelled);
  EXPECT_TRUE(query->execute(coordinates({4})).ok());
  ++fixture.backend->entries.at({"/dataset/a", 1}).manifest.revision;
  auto stale = query->execute(coordinates({4}));
  ASSERT_FALSE(stale.ok());
  EXPECT_EQ(stale.status().code(), QueryError::StalePlan);
}

namespace {
struct TrainingBlocksFixture {
  static constexpr std::uint64_t block_count = 3;
  std::shared_ptr<Backend> backend = std::make_shared<Backend>();
  std::shared_ptr<const StructuredBlobStorage> storage;
  std::shared_ptr<QueryRuntime> runtime = std::make_shared<QueryRuntime>();
  DatasetSnapshot snapshot;
  DatasetCoverageIndex index;
  std::uint64_t block_steps;
  std::uint64_t channels;
  StructuredBlobVersionKey key{"/dataset/blocks", 1};

  TrainingBlocksFixture(std::uint64_t steps, std::uint64_t width, bool flat = true,
      std::optional<std::pair<std::uint64_t, std::uint64_t>> null_leaf = {},
      arrow::Compression::type codec = arrow::Compression::UNCOMPRESSED,
      std::optional<std::uint64_t> null_block = {})
      : block_steps(steps), channels(width) {
    auto sink = checked(arrow::io::BufferOutputStream::Create());
    std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
    for (std::uint64_t b = 0; b < block_count; ++b) {
      auto leaf = std::make_shared<arrow::FloatBuilder>();
      auto inner = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), leaf);
      auto outer = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), inner);
      auto root = flat ? std::static_pointer_cast<arrow::ArrayBuilder>(inner)
        : std::static_pointer_cast<arrow::ArrayBuilder>(outer);
      auto append_value = [&](std::uint64_t t, std::uint64_t c) {
        const auto global_step = b * steps + t;
        if (null_leaf == std::make_pair(c, global_step)) checked(leaf->AppendNull());
        else checked(leaf->Append(static_cast<float>(global_step + c * 100)));
      };
      if (null_block == b) checked(root->AppendNull());
      else if (flat) {
        checked(inner->Append());
        for (std::uint64_t t = 0; t < steps; ++t)
          for (std::uint64_t c = 0; c < width; ++c) append_value(t, c);
      } else {
        checked(outer->Append());
        for (std::uint64_t c = 0; c < width; ++c) {
          checked(inner->Append());
          for (std::uint64_t t = 0; t < steps; ++t) append_value(t, c);
        }
      }
      std::shared_ptr<arrow::Array> array;
      checked(root->Finish(&array));
      auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("target", array->type())}), 1, {array});
      if (!writer) {
        auto options = arrow::ipc::IpcWriteOptions::Defaults();
        if (codec != arrow::Compression::UNCOMPRESSED)
          options.codec = checked(arrow::util::Codec::Create(codec));
        writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema(), options));
      }
      checked(writer->WriteRecordBatch(*batch));
    }
    checked(writer->Close());
    auto buffer = checked(sink->Finish());
    BinaryValue payload(buffer->data(), buffer->data() + buffer->size());
    auto layout = arrow_layout(payload);
    StructuredBlobManifest manifest;
    manifest.key = key;
    manifest.format_id = "arrow";
    manifest.payload_size = payload.size();
    manifest.layout_size = layout.size();
    manifest.phase = StructuredBlobPhase::Published;
    manifest.published = true;
    manifest.revision = 5;
    backend->entries.emplace(key, Backend::Entry{manifest, std::move(payload), std::move(layout)});
    storage = std::make_shared<StructuredBlobStorage>(backend);
    DatasetSnapshotRequest request;
    request.dataset_id = "training-blocks";
    request.selector.paths = {{PathScopeKind::Exact, key.path}};
    request.selector.versions = VersionSelection::all();
    request.selector.formats.adapters = AdapterSelectionKind::Listed;
    request.selector.formats.adapter_ids = {"arrow"};
    request.selector.formats.include_byte_passthrough = false;
    request.shards = {{"blocks.arrow", key}};
    auto table = std::make_shared<std::vector<SnapshotChannelBinding>>();
    for (std::uint64_t c = 0; c < width; ++c)
      table->push_back({"target", c, c, "c" + std::to_string(c), {}, steps});
    for (std::uint64_t b = 0; b < block_count; ++b) {
      auto binding = row(b, 0, "series", b * steps, steps);
      binding.batch_index = b;
      binding.declared_n_channels = width;
      binding.value_layout = flat ? SnapshotValueLayout::StepMajorFlat
        : SnapshotValueLayout::ChannelMajorLists;
      binding.channels.clear();
      binding.channel_table = table;
      request.rows.push_back(std::move(binding));
    }
    auto catalog = make_in_memory_published_version_catalog({manifest});
    auto prepared = prepare_dataset_snapshot(request, *catalog, storage);
    if (!prepared.ok()) throw std::runtime_error(prepared.status().message());
    snapshot = std::move(prepared.value());
    auto covered = prepare_dataset_coverage_index(snapshot, storage);
    if (!covered.ok()) throw std::runtime_error(covered.status().message());
    index = std::move(covered.value());
  }

  TrainingViewSpec selected(std::vector<std::uint64_t> channel_order = {}) const {
    auto result = view(snapshot);
    result.input_channels.clear(); result.target_channels.clear();
    if (channel_order.empty())
      for (std::uint64_t c = 0; c < channels; ++c) channel_order.push_back(c);
    for (auto c : channel_order) {
      TrainingChannelRef channel{"target", c, "c" + std::to_string(c)};
      result.input_channels.push_back(channel);
      result.target_channels.push_back(channel);
    }
    result.input_length = 8;
    result.target_length = 4;
    result.split = {"train", {0, block_steps * block_count}, {0, block_steps * block_count}};
    return result;
  }

  QueryResult<std::shared_ptr<TrainingQueryModule>> prepare(TrainingViewSpec selected,
      QueryExecutionOptions options = {}, TrainingQueryLimits limits = {}) const {
    ArrowDecodeConfig config;
    config.tile_elements = 4; // The training block must override ordinary tiny tiles.
    return TrainingQueryModule::prepare(snapshot, index, storage, runtime,
      std::move(selected), limits, options, config);
  }
};
} // namespace

TEST(TrainingQueryModule, StepMajor64And128BlocksMatchChannelMajorAndShareAcrossViews) {
  for (const std::uint64_t steps : {64u, 128u}) {
    TrainingBlocksFixture blocks(steps, 6), reference(steps, 6, false);
    for (const auto order : {std::vector<std::uint64_t>{0, 1, 2, 3, 4, 5},
                            std::vector<std::uint64_t>{4, 1, 0}}) {
      auto query = blocks.prepare(blocks.selected(order));
      auto original = reference.prepare(reference.selected(order));
      ASSERT_TRUE(query.ok()) << query.status().message();
      ASSERT_TRUE(original.ok()) << original.status().message();
      const auto layout_reads = blocks.backend->layout_reads(blocks.key);
      auto result = query.value()->execute(coordinates({steps + 2, steps + 3}));
      auto expected = original.value()->execute(coordinates({steps + 2, steps + 3}));
      ASSERT_TRUE(result.ok()) << result.status().message();
      ASSERT_TRUE(expected.ok()) << expected.status().message();
      ASSERT_EQ(result.value().samples().size(), expected.value().samples().size());
      for (std::size_t i = 0; i < result.value().samples().size(); ++i) {
        const auto& sample = result.value().samples()[i];
        const auto& native = expected.value().samples()[i];
        EXPECT_EQ(sample.input_values, native.input_values);
        EXPECT_EQ(sample.target_values, native.target_values);
        EXPECT_EQ(sample.input_validity, native.input_validity);
        EXPECT_EQ(sample.target_validity, native.target_validity);
        EXPECT_EQ(sample.descriptor.source_spans.size(), order.size() * 3);
        std::set<std::string> fragments;
        for (const auto& span : sample.descriptor.source_spans) fragments.insert(span.fragment_key);
        EXPECT_EQ(fragments.size(), 2u);
      }
      EXPECT_EQ(blocks.runtime->stats().decode_count, 2u);
      EXPECT_EQ(blocks.backend->payload_batches(blocks.key), 1);
      EXPECT_EQ(blocks.backend->layout_reads(blocks.key), layout_reads);
      auto warm = query.value()->execute(coordinates({steps + 3}));
      ASSERT_TRUE(warm.ok()) << warm.status().message();
      EXPECT_EQ(blocks.runtime->stats().decode_count, 2u);
      EXPECT_EQ(blocks.backend->layout_reads(blocks.key), layout_reads);
    }
  }
}

TEST(TrainingQueryModule, StepMajorCompressedWideBlocksAreDecodedOnceAndBatched) {
  for (const auto codec : {arrow::Compression::UNCOMPRESSED,
                          arrow::Compression::LZ4_FRAME, arrow::Compression::ZSTD}) {
    TrainingBlocksFixture blocks(128, 370, true, {}, codec);
    auto query = blocks.prepare(blocks.selected());
    ASSERT_TRUE(query.ok()) << query.status().message();
    auto result = query.value()->execute(coordinates({130, 131}));
    ASSERT_TRUE(result.ok()) << result.status().message();
    const auto& sample = result.value().samples()[0];
    ASSERT_EQ(sample.input_values.size(), 8u * 370u);
    EXPECT_EQ(sample.input_values[0], 122);
    EXPECT_EQ(sample.input_values[369], 37022);
    EXPECT_EQ(sample.input_values[370], 123);
    EXPECT_EQ(sample.target_values[0], 130);
    EXPECT_EQ(blocks.runtime->stats().decode_count, 2u);
    EXPECT_EQ(blocks.backend->payload_batches(blocks.key), 1);
    const auto reads = blocks.backend->payload_reads(blocks.key);
    auto warm = query.value()->execute(coordinates({131, 130}));
    ASSERT_TRUE(warm.ok()) << warm.status().message();
    EXPECT_EQ(blocks.backend->payload_reads(blocks.key), reads);
    EXPECT_EQ(blocks.runtime->stats().decode_count, 2u);
  }
}

TEST(TrainingQueryModule, StepMajorNullMaskRejectRefundAndCancellationRemainRequestLocal) {
  TrainingBlocksFixture blocks(64, 3, true, std::make_pair(std::uint64_t{0}, std::uint64_t{3}));
  auto selected = blocks.selected({0});
  selected.input_length = selected.target_length = 1;
  selected.missing.null_value = MissingTreatment::Mask;
  auto masked = blocks.prepare(selected);
  ASSERT_TRUE(masked.ok()) << masked.status().message();
  auto null = masked.value()->execute(coordinates({3}));
  ASSERT_TRUE(null.ok()) << null.status().message();
  EXPECT_EQ(null.value().samples()[0].input_validity, (std::vector<std::uint8_t>{1}));
  EXPECT_EQ(null.value().samples()[0].target_validity, (std::vector<std::uint8_t>{0}));
  selected.missing.null_value = MissingTreatment::RejectWindow;
  for (const bool range_limit : {false, true}) {
    QueryExecutionOptions options;
    if (range_limit) options.max_ranges = 3; // One logical layout + accepted roles.
    else options.max_payload_bytes = 10; // Each role: float + bitmap byte.
    auto rejecting = blocks.prepare(selected, options);
    ASSERT_TRUE(rejecting.ok()) << rejecting.status().message();
    WindowRequest request; request.anchors = StepInterval{3, 10}; request.max_windows = 1;
    auto result = rejecting.value()->execute(request);
    ASSERT_TRUE(result.ok()) << result.status().message();
    ASSERT_EQ(result.value().samples().size(), 1u);
    EXPECT_EQ(result.value().samples()[0].descriptor.anchor, 5u);
  }
  TrainingBlocksFixture cold(128, 3);
  auto query = cold.prepare(cold.selected());
  ASSERT_TRUE(query.ok()) << query.status().message();
  CancellationToken cancellation;
  cold.backend->payload_read_hook = [&] { cancellation.cancel(); };
  auto cancelled = query.value()->execute(coordinates({130}), cancellation);
  ASSERT_FALSE(cancelled.ok());
  EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  cold.backend->payload_read_hook = {};
  EXPECT_TRUE(query.value()->execute(coordinates({130})).ok());
}

TEST(TrainingQueryModule, StepMajorNullParentMasksOnlyCoveredBlockAndOutputBudgetStopsReads) {
  TrainingBlocksFixture blocks(64, 3, true, {}, arrow::Compression::UNCOMPRESSED, 1);
  auto selected = blocks.selected({2, 0});
  selected.missing.null_series = MissingTreatment::Mask;
  auto query = blocks.prepare(selected);
  ASSERT_TRUE(query.ok()) << query.status().message();
  auto result = query.value()->execute(coordinates({66}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  const auto& sample = result.value().samples()[0];
  EXPECT_EQ(sample.input_validity, (std::vector<std::uint8_t>{1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0}));
  EXPECT_EQ(sample.target_validity, std::vector<std::uint8_t>(8, 0));
  EXPECT_EQ(blocks.runtime->stats().decode_count, 1u);
  TrainingBlocksFixture limited(64, 3);
  TrainingQueryLimits limits; limits.max_result_bytes = 1;
  auto rejected = limited.prepare(limited.selected(), {}, limits);
  ASSERT_TRUE(rejected.ok()) << rejected.status().message();
  EXPECT_FALSE(rejected.value()->execute(coordinates({66})).ok());
  EXPECT_EQ(limited.backend->payload_reads(limited.key), 0);
}

TEST(TrainingQueryModule, PreparedLayoutBudgetFallsBackToLazyReadsWithoutRejectingWindows) {
  Fixture fixture;
  QueryExecutionOptions options;
  options.max_layout_bytes = 0;
  for (const auto& manifest : fixture.snapshot.manifests)
    options.max_layout_bytes += manifest.layout_size;
  const auto before_a = fixture.backend->layout_reads({"/dataset/a", 1});
  const auto before_b = fixture.backend->layout_reads({"/dataset/b", 1});
  const auto before_c = fixture.backend->layout_reads({"/dataset/c", 1});
  auto prepared = TrainingQueryModule::prepare(fixture.snapshot, fixture.index,
    fixture.storage, fixture.runtime, view(fixture.snapshot), {}, options);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  // Wire bytes fit the request budget, but decoded retained metadata does not.
  // Preparation must not allocate/read a context that it cannot retain.
  EXPECT_EQ(fixture.backend->layout_reads({"/dataset/a", 1}), before_a);
  EXPECT_EQ(fixture.backend->layout_reads({"/dataset/b", 1}), before_b);
  EXPECT_EQ(fixture.backend->layout_reads({"/dataset/c", 1}), before_c);
  auto result = prepared.value()->execute(coordinates({4}));
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value().samples()[0].input_values,
    (std::vector<float>{101, 1, 102, 2, 103, 3}));
  EXPECT_EQ(fixture.backend->layout_reads({"/dataset/a", 1}), before_a + 1);
  EXPECT_EQ(fixture.backend->layout_reads({"/dataset/b", 1}), before_b + 1);
  EXPECT_EQ(fixture.backend->layout_reads({"/dataset/c", 1}), before_c);
}
