#include "iotdbfs/query/training/arrow/DatasetSnapshot.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>
#include <openssl/sha.h>

#include <atomic>
#include <thread>
#include <cstdio>
#include <fstream>
#include <map>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
using namespace iotdbfs;

template<class T> T checked(arrow::Result<T> r) {
  if (!r.ok()) throw std::runtime_error(r.status().ToString());
  return std::move(r).ValueOrDie();
}
void checked(arrow::Status s) {
  if (!s.ok()) throw std::runtime_error(s.ToString());
}

std::string read_file(const std::string& path) {
  std::ifstream stream(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>()};
}

void write_file(const std::string& path, const std::string& bytes) {
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  if (!stream) throw std::runtime_error("cannot write test sidecar");
}

void refresh_sidecar_checksum(std::string& bytes) {
  if (bytes.size() < 64) throw std::runtime_error("short test sidecar");
  bytes.resize(bytes.size() - 64);
  unsigned char digest[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(bytes.data()), bytes.size(), digest);
  constexpr char digits[] = "0123456789abcdef";
  for (auto byte : digest) {
    bytes.push_back(digits[byte >> 4]);
    bytes.push_back(digits[byte & 15]);
  }
}

std::uint64_t read_u64(const std::string& bytes, std::size_t& pos) {
  if (pos > bytes.size() || bytes.size() - pos < 8)
    throw std::runtime_error("truncated test sidecar");
  std::uint64_t value = 0;
  for (unsigned i = 0; i < 8; ++i)
    value |= std::uint64_t(static_cast<unsigned char>(bytes[pos++])) << (8 * i);
  return value;
}

void skip_string(const std::string& bytes, std::size_t& pos) {
  const auto size = read_u64(bytes, pos);
  if (size > bytes.size() - pos) throw std::runtime_error("bad test sidecar string");
  pos += static_cast<std::size_t>(size);
}

void omit_last_sidecar_span(std::string& bytes) {
  if (bytes.size() < 64) throw std::runtime_error("short test sidecar");
  std::string payload = bytes.substr(0, bytes.size() - 64);
  std::size_t pos = 0;
  skip_string(payload, pos); skip_string(payload, pos);
  const auto count_pos = pos;
  const auto count = read_u64(payload, pos);
  if (count < 2) throw std::runtime_error("too few test spans");
  std::size_t last_begin = 0;
  for (std::uint64_t i = 0; i < count; ++i) {
    last_begin = pos;
    skip_string(payload, pos); skip_string(payload, pos);
    for (int j = 0; j < 5; ++j) read_u64(payload, pos);
    if (read_u64(payload, pos)) read_u64(payload, pos);
    for (int j = 0; j < 6; ++j) read_u64(payload, pos);
  }
  if (pos != payload.size()) throw std::runtime_error("unexpected test sidecar tail");
  payload.resize(last_begin);
  for (unsigned i = 0; i < 8; ++i)
    payload[count_pos + i] = static_cast<char>((count - 1) >> (8 * i));
  bytes = payload + std::string(64, '0');
  refresh_sidecar_checksum(bytes);
}

BinaryValue stream_bytes(int n = 4, bool single = false,
    bool null_child = false, bool null_row = false, int channels = 2) {
  auto leaf = std::make_shared<arrow::FloatBuilder>();
  auto inner = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), leaf);
  auto outer = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), inner);
  std::shared_ptr<arrow::ArrayBuilder> root = single ?
    std::static_pointer_cast<arrow::ArrayBuilder>(inner) :
    std::static_pointer_cast<arrow::ArrayBuilder>(outer);
  if (null_row) checked(root->AppendNull());
  else if (single) {
    checked(inner->Append());
    for (int i = 0; i < n; ++i) checked(leaf->Append(static_cast<float>(i)));
  } else {
    checked(outer->Append());
    for (int channel = 0; channel < channels; ++channel) {
      if (null_child && channel == 0) checked(inner->AppendNull());
      else {
        checked(inner->Append());
        for (int i = 0; i < n; ++i)
          checked(leaf->Append(static_cast<float>(channel * 100 + i)));
      }
    }
  }
  std::shared_ptr<arrow::Array> array;
  checked(root->Finish(&array));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({arrow::field("target", array->type())}), 1, {array});
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  checked(writer->WriteRecordBatch(*batch)); checked(writer->Close());
  auto buffer = checked(sink->Finish());
  return {buffer->data(), buffer->data() + buffer->size()};
}

BinaryValue layout_bytes(const BinaryValue& payload) {
  ArrowFormatAdapter adapter;
  FormatSource source{payload.size(), [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<BinaryValue> {
    if (offset > payload.size() || length > payload.size() - offset) return std::nullopt;
    return BinaryValue(payload.begin() + offset, payload.begin() + offset + length);
  }};
  auto session = adapter.begin_ingestion(source);
  if (!session) throw std::runtime_error("Arrow ingest failed");
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
  mutable int layout_reads{0};
  mutable int payload_reads{0};
  void put(const std::string& path, std::int64_t version, int length = 4,
      bool single = false, bool null_child = false, bool null_row = false,
      int channels = 2) {
    auto payload = stream_bytes(length, single, null_child, null_row, channels);
    auto layout = layout_bytes(payload);
    StructuredBlobManifest m;
    m.key = {path, version}; m.format_id = "arrow";
    m.payload_size = payload.size(); m.layout_size = layout.size();
    m.phase = StructuredBlobPhase::Published; m.published = true; m.revision = 5;
    entries[m.key] = {m, std::move(payload), std::move(layout)};
  }
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return {}; }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& k) const override {
    auto it = entries.find(k);
    if (it == entries.end()) return StructuredBlobResult<StructuredBlobManifest>::failure(StructuredBlobError::NotFound, "missing");
    return StructuredBlobResult<StructuredBlobManifest>::success(it->second.manifest);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobManifest&) override { return {}; }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&, std::uint64_t,
      const BinaryValue&) override { return {}; }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&, const BinaryValue&) override { return {}; }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&, const MetadataMap&) override { return {}; }
  StructuredBlobResult<BinaryValue> read_payload_range(const StructuredBlobVersionKey& k,
      std::uint64_t off, std::uint64_t len) const override {
    ++payload_reads; return read(k, off, len, false);
  }
  StructuredBlobResult<BinaryValue> read_layout_range(const StructuredBlobVersionKey& k,
      std::uint64_t off, std::uint64_t len) const override {
    ++layout_reads; return read(k, off, len, true);
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey& k, std::uint64_t) override {
    entries.erase(k); return {};
  }
private:
  StructuredBlobResult<BinaryValue> read(const StructuredBlobVersionKey& k,
      std::uint64_t off, std::uint64_t len, bool layout) const {
    auto it = entries.find(k);
    if (it == entries.end()) return StructuredBlobResult<BinaryValue>::failure(StructuredBlobError::NotFound, "missing");
    const auto& bytes = layout ? it->second.layout : it->second.payload;
    if (off > bytes.size() || len > bytes.size() - off)
      return StructuredBlobResult<BinaryValue>::failure(StructuredBlobError::RangeError, "range");
    return StructuredBlobResult<BinaryValue>::success(
      BinaryValue(bytes.begin() + off, bytes.begin() + off + len));
  }
};

DatasetSnapshotRequest request(const std::shared_ptr<Backend>& backend) {
  DatasetSnapshotRequest q;
  q.dataset_id = "fixture";
  q.selector.paths = {{PathScopeKind::Subtree, "/dataset"}};
  q.selector.versions = VersionSelection::all();
  q.selector.formats.adapters = AdapterSelectionKind::Listed;
  q.selector.formats.adapter_ids = {"arrow"};
  q.selector.formats.include_byte_passthrough = false;
  q.shards = {{"part-b.arrow", {"/dataset/b", 1}},
              {"part-a.arrow", {"/dataset/a", 1}}};
  q.hf_state_json = R"({"_data_files":[{"filename":"part-b.arrow"},{"filename":"part-a.arrow"}],"_fingerprint":"hf"})";
  auto make_row = [&](std::uint64_t logical, std::size_t shard, std::string series,
      std::uint64_t begin) {
    SnapshotRowBinding row;
    row.logical_row = logical; row.shard_index = shard;
    row.series_id = std::move(series); row.steps = {begin, begin + 4};
    row.declared_n_channels = 2; row.declared_length = 4;
    row.channels = {{"target", 0, 0, "x", std::nullopt, 4},
                    {"target", 1, 1, "y", std::nullopt, 4}};
    return row;
  };
  q.rows = {make_row(0, 0, "s", 0), make_row(1, 1, "s", 4)};
  (void)backend;
  return q;
}

std::shared_ptr<const PublishedVersionCatalog> catalog(const std::shared_ptr<Backend>& backend) {
  std::vector<StructuredBlobManifest> manifests;
  for (const auto& entry : backend->entries) manifests.push_back(entry.second.manifest);
  return make_in_memory_published_version_catalog(std::move(manifests));
}
} // namespace

TEST(DatasetSnapshot, ExplicitCrossFileCoverageUsesOnlyLayouts) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().message();
  EXPECT_EQ(backend->layout_reads, 0);
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  auto spans = index.value().lookup("s", "target", 1, {3, 6});
  ASSERT_TRUE(spans.ok()) << spans.status().message();
  ASSERT_EQ(spans.value().size(), 2u);
  EXPECT_EQ(spans.value()[0].shard_index, 0u);
  EXPECT_EQ(spans.value()[1].shard_index, 1u);
  EXPECT_EQ(spans.value()[0].steps.begin, 3u);
  EXPECT_EQ(spans.value()[0].source_step_begin, 0u);
  EXPECT_EQ(spans.value()[1].source_step_begin, 4u);
  EXPECT_EQ(spans.value()[1].steps.end, 6u);
  EXPECT_EQ(backend->payload_reads, 0);
  EXPECT_EQ(backend->layout_reads, 2);
}

TEST(DatasetSnapshot, IndependentRowsAndBadDeclarationsDoNotJoin) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend); q.rows[1].series_id = "other";
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok());
  EXPECT_FALSE(index.value().lookup("s", "target", 0, {3, 6}).ok());
  q.rows[0].declared_n_channels = 3;
  snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  EXPECT_FALSE(prepare_dataset_coverage_index(snapshot.value(), storage).ok());
}

TEST(DatasetSnapshot, SidecarReusedAndCorruptionRebuilt) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  std::string path = "/tmp/iotdbfs-a07-" + snapshot.value().fingerprint + ".coverage";
  std::remove(path.c_str());
  auto first = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(first.ok()) << first.status().message();
  EXPECT_EQ(backend->layout_reads, 2);
  auto second = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(second.ok()) << second.status().message();
  EXPECT_EQ(backend->layout_reads, 2);
  { std::ofstream bad(path, std::ios::trunc); bad << "corrupt"; }
  auto rebuilt = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(rebuilt.ok()) << rebuilt.status().message();
  EXPECT_EQ(backend->layout_reads, 4);
  std::remove(path.c_str());
}

TEST(DatasetSnapshot, NewVersionDoesNotAlterFrozenSnapshotAndDeletionFailsExact) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  auto old = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(old.ok());
  backend->put("/dataset/a", 2);
  auto q2 = q; q2.shards[1].key.version = 2;
  q2.selector.versions = VersionSelection::latest();
  auto next = prepare_dataset_snapshot(q2, *catalog(backend), storage);
  ASSERT_TRUE(next.ok()) << next.status().message();
  EXPECT_NE(old.value().fingerprint, next.value().fingerprint);
  EXPECT_TRUE(verify_dataset_snapshot_sources(old.value(), storage).ok());
  backend->entries.erase({"/dataset/a", 1});
  EXPECT_EQ(verify_dataset_snapshot_sources(old.value(), storage).code(), QueryError::SourceNotFound);
  EXPECT_TRUE(verify_dataset_snapshot_sources(next.value(), storage).ok());
}

TEST(DatasetSnapshot, VariablePartitionsUseOriginalChannelIdentity) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.rows[1].steps = {0, 4};
  q.rows[1].channels = {{"target", 2, 0, "z", std::string("unit"), 4}};
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().message();
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  auto z = index.value().lookup("s", "target", 2, {1, 3});
  ASSERT_TRUE(z.ok()) << z.status().message();
  ASSERT_EQ(z.value().size(), 1u);
  EXPECT_EQ(z.value()[0].physical_ordinal, 0u);
  EXPECT_EQ(z.value()[0].shard_index, 1u);
  auto x = index.value().lookup("s", "target", 0, {1, 3});
  ASSERT_TRUE(x.ok()); EXPECT_EQ(x.value()[0].shard_index, 0u);
  q.rows[1].channels[0].original_ordinal = 0;
  auto overlapping = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(overlapping.ok());
  EXPECT_FALSE(prepare_dataset_coverage_index(overlapping.value(), storage).ok());
}

TEST(DatasetSnapshot, RejectsBadHFOrderAndDiscontinuousTime) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  std::swap(q.shards[0].filename, q.shards[1].filename);
  EXPECT_FALSE(prepare_dataset_snapshot(q, *catalog(backend), storage).ok());
  q = request(backend);
  q.rows[0].time = {TrainingTimeKind::RegularNanoseconds, 100, 10, "UTC", {}};
  q.rows[1].time = {TrainingTimeKind::RegularNanoseconds, 150, 10, "UTC", {}};
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  EXPECT_FALSE(prepare_dataset_coverage_index(snapshot.value(), storage).ok());
  q.rows[1].time.start_ns = 140;
  snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  EXPECT_TRUE(prepare_dataset_coverage_index(snapshot.value(), storage).ok());
}

TEST(DatasetSnapshot, SingleLevelAndNullLayersStayDistinct) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/b", 1, 4, true);
  backend->put("/dataset/a", 1, 4, false, true);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.rows[0].declared_n_channels = 1;
  q.rows[0].channels = {{"target", 0, 0, "x", std::nullopt, 4},
                        {"target", 1, std::nullopt, "y", std::nullopt, 4}};
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  auto null_child = index.value().lookup("s", "target", 0, {4, 8});
  ASSERT_TRUE(null_child.ok());
  ASSERT_EQ(null_child.value().size(), 1u);
  EXPECT_TRUE(null_child.value()[0].null_channel);
  EXPECT_FALSE(null_child.value()[0].null_series);
  EXPECT_FALSE(null_child.value()[0].missing_channel);
  auto missing = index.value().lookup("s", "target", 1, {0, 4});
  ASSERT_TRUE(missing.ok());
  EXPECT_TRUE(missing.value()[0].missing_channel);
  EXPECT_EQ(backend->payload_reads, 0);
}

TEST(DatasetSnapshot, NullSeriesIsNotAnEmptyChannel) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/b", 1);
  backend->put("/dataset/a", 1, 4, false, false, true);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  auto spans = index.value().lookup("s", "target", 0, {4, 8});
  ASSERT_TRUE(spans.ok());
  ASSERT_EQ(spans.value().size(), 1u);
  EXPECT_TRUE(spans.value()[0].null_series);
  EXPECT_FALSE(spans.value()[0].null_channel);
  EXPECT_FALSE(spans.value()[0].missing_channel);
  EXPECT_EQ(backend->payload_reads, 0);
}

TEST(DatasetSnapshot, ViewPreflightChecksSelectedChannelIdentity) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok());
  TrainingViewSpec view;
  view.view_id = "v"; view.snapshot_fingerprint = snapshot.value().fingerprint;
  view.experiment_id = "experiment";
  view.input_length = 2; view.stride = 1;
  view.input_channels = {{"target", 0, "x"}};
  EXPECT_TRUE(validate_snapshot_view(snapshot.value(), index.value(), view).ok());
  view.input_channels[0].label = "wrong";
  EXPECT_FALSE(validate_snapshot_view(snapshot.value(), index.value(), view).ok());
  view.input_channels = {{"target", 8, "missing"}};
  EXPECT_FALSE(validate_snapshot_view(snapshot.value(), index.value(), view).ok());
  view.missing.missing_channel = MissingTreatment::Mask;
  EXPECT_TRUE(validate_snapshot_view(snapshot.value(), index.value(), view).ok());
}

TEST(DatasetSnapshot, OmittedChannelInLaterSegmentFailsDuringIndexPreparation) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/b", 1, 4, false, false, false, 3);
  backend->put("/dataset/a", 1, 4, false, false, false, 2);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.rows[0].declared_n_channels = 3;
  q.rows[0].channels.push_back({"target", 2, 2, "z", std::nullopt, 4});
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().message();
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  EXPECT_FALSE(index.ok());
  EXPECT_EQ(index.status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(backend->payload_reads, 0);
}

TEST(DatasetSnapshot, ExplicitlyMissingChannelKeepsCoverageForMaskPolicy) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/b", 1, 4, false, false, false, 3);
  backend->put("/dataset/a", 1, 4, false, false, false, 2);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.rows[0].declared_n_channels = 3;
  q.rows[0].channels.push_back({"target", 2, 2, "z", std::nullopt, 4});
  q.rows[1].channels.push_back({"target", 2, std::nullopt, "z", std::nullopt, 4});
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().message();
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  auto spans = index.value().lookup("s", "target", 2, {3, 5});
  ASSERT_TRUE(spans.ok()) << spans.status().message();
  ASSERT_EQ(spans.value().size(), 2u);
  EXPECT_FALSE(spans.value()[0].missing_channel);
  EXPECT_TRUE(spans.value()[1].missing_channel);
  TrainingViewSpec view;
  view.view_id = "masked"; view.snapshot_fingerprint = snapshot.value().fingerprint;
  view.experiment_id = "experiment";
  view.input_length = 2; view.stride = 1;
  view.input_channels = {{"target", 2, "z"}};
  view.missing.missing_channel = MissingTreatment::Mask;
  EXPECT_TRUE(validate_snapshot_view(snapshot.value(), index.value(), view).ok());
  auto broken = snapshot.value();
  broken.rows[1].channels.pop_back();
  EXPECT_FALSE(validate_snapshot_view(broken, index.value(), view).ok());
  EXPECT_EQ(backend->payload_reads, 0);
}

TEST(DatasetSnapshot, CachedSidecarCannotHideOmittedChannelDeclaration) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/b", 1, 4, false, false, false, 3);
  backend->put("/dataset/a", 1, 4, false, false, false, 2);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.rows[0].declared_n_channels = 3;
  q.rows[0].channels.push_back({"target", 2, 2, "z", std::nullopt, 4});
  q.rows[1].channels.push_back({"target", 2, std::nullopt, "z", std::nullopt, 4});
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  const auto path = "/tmp/iotdbfs-a07-channels-" +
    snapshot.value().fingerprint + ".coverage";
  std::remove(path.c_str());
  auto prepared = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  const auto layout_reads = backend->layout_reads;
  auto broken = snapshot.value();
  broken.rows[1].channels.pop_back();
  auto rejected = prepare_dataset_coverage_index(broken, storage, path);
  EXPECT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::StalePlan);
  EXPECT_EQ(backend->layout_reads, layout_reads);
  std::remove(path.c_str());
}

TEST(DatasetSnapshot, EditedStepBindingCannotReuseOrOverwriteOriginalSidecar) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  const auto path = "/tmp/iotdbfs-a07-fingerprint-" +
    snapshot.value().fingerprint + ".coverage";
  std::remove(path.c_str());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(index.ok()) << index.status().message();
  std::ifstream before_stream(path, std::ios::binary);
  const std::string before((std::istreambuf_iterator<char>(before_stream)),
    std::istreambuf_iterator<char>());
  ASSERT_FALSE(before.empty());
  const auto layout_reads = backend->layout_reads;

  auto edited = snapshot.value();
  edited.rows[1].steps = {100, 104};
  auto rejected = prepare_dataset_coverage_index(edited, storage, path);
  EXPECT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::StalePlan);
  EXPECT_EQ(backend->layout_reads, layout_reads);
  std::ifstream after_stream(path, std::ios::binary);
  const std::string after((std::istreambuf_iterator<char>(after_stream)),
    std::istreambuf_iterator<char>());
  EXPECT_EQ(after, before);

  TrainingViewSpec view;
  view.view_id = "v"; view.snapshot_fingerprint = snapshot.value().fingerprint;
  view.experiment_id = "experiment";
  view.input_length = 2; view.stride = 1;
  view.input_channels = {{"target", 0, "x"}};
  EXPECT_TRUE(validate_snapshot_view(snapshot.value(), index.value(), view).ok());
  EXPECT_EQ(validate_snapshot_view(edited, index.value(), view).code(),
    QueryError::StalePlan);
  std::remove(path.c_str());
}

TEST(DatasetSnapshot, EditedChannelBindingCannotKeepOriginalFingerprint) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok());
  auto edited = snapshot.value();
  edited.rows[0].channels[0].label = "renamed";
  edited.rows[1].channels[0].label = "renamed";
  EXPECT_EQ(prepare_dataset_coverage_index(edited, storage).status().code(),
    QueryError::StalePlan);
  TrainingViewSpec view;
  view.view_id = "v"; view.snapshot_fingerprint = edited.fingerprint;
  view.experiment_id = "experiment";
  view.input_length = 2; view.stride = 1;
  view.input_channels = {{"target", 0, "renamed"}};
  EXPECT_EQ(validate_snapshot_view(edited, index.value(), view).code(),
    QueryError::StalePlan);
}

TEST(DatasetSnapshot, EditedShardOrManifestCannotKeepOriginalFingerprint) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto edited = snapshot.value();
  edited.shards[0].filename = "renamed.arrow";
  EXPECT_EQ(verify_dataset_snapshot_sources(edited, storage).code(),
    QueryError::StalePlan);
  edited = snapshot.value();
  edited.manifests[0].metadata.emplace("tag", MetadataValue("edited"));
  EXPECT_EQ(prepare_dataset_coverage_index(edited, storage).status().code(),
    QueryError::StalePlan);
}

TEST(DatasetSnapshot, ChecksummedIncompleteSidecarIsRebuilt) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/b", 1, 4, false, false, false, 2);
  backend->put("/dataset/a", 1, 4, false, false, false, 3);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.rows[1].steps = {0, 4};
  q.rows[1].declared_n_channels = 3;
  q.rows[1].channels = {{"target", 2, 0, "z", std::nullopt, 4},
                        {"target", 3, 1, "u", std::nullopt, 4},
                        {"target", 4, 2, "v", std::nullopt, 4}};
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  const auto path = "/tmp/iotdbfs-a07-incomplete-" +
    snapshot.value().fingerprint + ".coverage";
  std::remove(path.c_str());
  auto first = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(first.ok()) << first.status().message();
  ASSERT_EQ(first.value().spans().size(), 5u);
  auto bytes = read_file(path);
  omit_last_sidecar_span(bytes);
  write_file(path, bytes);
  const auto before_reads = backend->layout_reads;
  auto rebuilt = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(rebuilt.ok()) << rebuilt.status().message();
  EXPECT_EQ(rebuilt.value().spans().size(), 5u);
  EXPECT_EQ(backend->layout_reads, before_reads + 2);
  EXPECT_NE(read_file(path), bytes);
  EXPECT_EQ(backend->payload_reads, 0);
  std::remove(path.c_str());
}

TEST(DatasetSnapshot, LegacyCoverageProtocolIsRebuilt) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  const auto path = "/tmp/iotdbfs-a07-protocol-" +
    snapshot.value().fingerprint + ".coverage";
  std::remove(path.c_str());
  auto first = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto bytes = read_file(path);
  const auto marker = bytes.find("iotdbfs-arrow-coverage/");
  ASSERT_NE(marker, std::string::npos);
  const auto version = marker + std::string("iotdbfs-arrow-coverage/").size();
  ASSERT_LT(version, bytes.size());
  bytes[version] = '1';
  refresh_sidecar_checksum(bytes);
  write_file(path, bytes);
  const auto before_reads = backend->layout_reads;
  auto rebuilt = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(rebuilt.ok()) << rebuilt.status().message();
  EXPECT_EQ(backend->layout_reads, before_reads + 2);
  EXPECT_NE(read_file(path), bytes);
  EXPECT_EQ(backend->payload_reads, 0);
  std::remove(path.c_str());
}

TEST(DatasetSnapshot, HFStateAcceptsStandardEscapesInNamesAndSkippedFields) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.hf_state_json = R"({"_note":"\u4e2d\n\t\b\f\r\/","unused":{"emoji":"\ud83d\ude00"},"_data_files":[{"filename":"part-\u0062.arrow"},{"filename":"part-a.arrow"}]})";
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().message();
}

TEST(DatasetSnapshot, HFStateRejectsTrailingCommasAndInvalidEscapes) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  for (const auto& json : {
      R"({"_data_files":[{"filename":"part-b.arrow"},{"filename":"part-a.arrow"}],})",
      R"({"_data_files":[{"filename":"part-b.arrow"},{"filename":"part-a.arrow"},]})",
      R"({"_data_files":[{"filename":"part-b.arrow",},{"filename":"part-a.arrow"}]})",
      R"({"unused":{"x":1,},"_data_files":[{"filename":"part-b.arrow"},{"filename":"part-a.arrow"}]})",
      R"({"unused":"\ud800","_data_files":[{"filename":"part-b.arrow"},{"filename":"part-a.arrow"}]})"}) {
    q.hf_state_json = json;
    EXPECT_FALSE(prepare_dataset_snapshot(q, *catalog(backend), storage).ok()) << json;
  }
}

TEST(DatasetSnapshot, HFIndicesRequireAValidatedMapping) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  q.hf_state_json = R"({"_data_files":[{"filename":"part-b.arrow"},{"filename":"part-a.arrow"}],"_indices_data_files":[{"filename":"indices.arrow"}]})";
  auto rejected = prepare_dataset_snapshot(q, *catalog(backend), storage);
  EXPECT_FALSE(rejected.ok());
  EXPECT_EQ(rejected.status().code(), QueryError::InvalidQuery);
  q.hf_state_json = R"({"_data_files":[{"filename":"part-b.arrow"},{"filename":"part-a.arrow"}],"_indices_data_files":[]})";
  EXPECT_TRUE(prepare_dataset_snapshot(q, *catalog(backend), storage).ok());
}

TEST(DatasetSnapshot, SidecarPublishFailureDoesNotFailCoverage) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage,
    "/dev/null/coverage-sidecar");
  ASSERT_TRUE(index.ok()) << index.status().message();
  EXPECT_EQ(index.value().spans().size(), 4u);
  EXPECT_EQ(backend->payload_reads, 0);
}

TEST(DatasetSnapshot, PublishedSidecarCanBeReadByAnotherUser) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto snapshot = prepare_dataset_snapshot(request(backend), *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  const auto path = "/tmp/iotdbfs-a07-mode-" + snapshot.value().fingerprint + ".coverage";
  std::remove(path.c_str());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage, path);
  ASSERT_TRUE(index.ok()) << index.status().message();
  struct stat info {};
  ASSERT_EQ(::stat(path.c_str(), &info), 0);
  EXPECT_EQ(info.st_mode & 0777, 0644);
  std::remove(path.c_str());
}

TEST(DatasetSnapshot, ContentIdentityIsStableAcrossStorageNamespaces) {
  auto first = std::make_shared<Backend>();
  auto second = std::make_shared<Backend>();
  for (const auto& backend : {first, second}) {
    backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  }
  auto first_storage = std::make_shared<StructuredBlobStorage>(first);
  auto second_storage = std::make_shared<StructuredBlobStorage>(second);
  auto first_snapshot = prepare_dataset_snapshot(request(first), *catalog(first), first_storage);
  auto second_snapshot = prepare_dataset_snapshot(request(second), *catalog(second), second_storage);
  ASSERT_TRUE(first_snapshot.ok());
  ASSERT_TRUE(second_snapshot.ok());
  EXPECT_EQ(first_snapshot.value().fingerprint, second_snapshot.value().fingerprint);
  EXPECT_EQ(training_sample_id(first_snapshot.value().fingerprint, "view", "s", 3),
    training_sample_id(second_snapshot.value().fingerprint, "view", "s", 3));
  EXPECT_EQ(validate_dataset_snapshot_runtime(first_snapshot.value(), second_storage).code(),
    QueryError::StalePlan);
  EXPECT_EQ(verify_dataset_snapshot_sources(first_snapshot.value(), second_storage).code(),
    QueryError::StalePlan);
  EXPECT_EQ(prepare_dataset_coverage_index(first_snapshot.value(), second_storage).status().code(),
    QueryError::StalePlan);
  const auto path = "/tmp/iotdbfs-a07-runtime-" +
    first_snapshot.value().fingerprint + ".coverage";
  std::remove(path.c_str());
  auto first_index = prepare_dataset_coverage_index(first_snapshot.value(), first_storage, path);
  ASSERT_TRUE(first_index.ok()) << first_index.status().message();
  EXPECT_EQ(first->layout_reads, 2);
  auto second_index = prepare_dataset_coverage_index(second_snapshot.value(), second_storage, path);
  ASSERT_TRUE(second_index.ok()) << second_index.status().message();
  EXPECT_EQ(second->layout_reads, 2); // Same content, different backend: rebuild.
  auto second_hit = prepare_dataset_coverage_index(second_snapshot.value(), second_storage, path);
  ASSERT_TRUE(second_hit.ok()) << second_hit.status().message();
  EXPECT_EQ(second->layout_reads, 2); // Same runtime binding: sidecar hit.
  TrainingViewSpec view;
  view.view_id = "view";
  view.experiment_id = "experiment";
  view.snapshot_fingerprint = first_snapshot.value().fingerprint;
  view.input_length = 2; view.stride = 1;
  view.input_channels = {{"target", 0, "x"}};
  EXPECT_TRUE(validate_snapshot_view(second_snapshot.value(), second_index.value(), view).ok());
  EXPECT_EQ(validate_snapshot_view(first_snapshot.value(), second_index.value(), view).code(),
    QueryError::StalePlan);
  auto wrong_process = second_snapshot.value();
  wrong_process.process_identity = "another-process";
  EXPECT_EQ(validate_dataset_snapshot_runtime(wrong_process, second_storage).code(),
    QueryError::StalePlan);
  EXPECT_EQ(verify_dataset_snapshot_sources(wrong_process, second_storage).code(),
    QueryError::StalePlan);
  EXPECT_EQ(validate_snapshot_view(wrong_process, second_index.value(), view).code(),
    QueryError::StalePlan);
  std::remove(path.c_str());
}

TEST(DatasetSnapshot, ContentIdentitySurvivesAnotherProcess) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  auto versions = catalog(backend);
  auto parent = prepare_dataset_snapshot(q, *versions, storage);
  ASSERT_TRUE(parent.ok()) << parent.status().message();
  int channel[2];
  ASSERT_EQ(::pipe(channel), 0);
  const auto pid = ::fork();
  ASSERT_GE(pid, 0);
  if (pid == 0) {
    ::close(channel[0]);
    auto child = prepare_dataset_snapshot(q, *versions, storage);
    if (!child.ok()) ::_exit(1);
    const auto identities = child.value().fingerprint + child.value().process_identity;
    std::size_t sent = 0;
    while (sent < identities.size()) {
      const auto n = ::write(channel[1], identities.data() + sent,
        identities.size() - sent);
      if (n <= 0) ::_exit(2);
      sent += static_cast<std::size_t>(n);
    }
    ::close(channel[1]);
    ::_exit(0);
  }
  ::close(channel[1]);
  std::string identities(128, '\0');
  std::size_t received = 0;
  while (received < identities.size()) {
    const auto n = ::read(channel[0], &identities[received],
      identities.size() - received);
    if (n <= 0) break;
    received += static_cast<std::size_t>(n);
  }
  ::close(channel[0]);
  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);
  ASSERT_EQ(received, identities.size());
  EXPECT_EQ(identities.substr(0, 64), parent.value().fingerprint);
  EXPECT_NE(identities.substr(64), parent.value().process_identity);
}

TEST(DatasetSnapshot, SharedChannelTableMatchesExpandedIdentityAndSeversMutableAliases) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  auto expanded = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(expanded.ok());
  auto table = std::make_shared<std::vector<SnapshotChannelBinding>>(q.rows[0].channels);
  for (auto& row : q.rows) { row.channels.clear(); row.channel_table = table; }
  auto compact = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(compact.ok()) << compact.status().message();
  EXPECT_EQ(expanded.value().fingerprint, compact.value().fingerprint);
  EXPECT_EQ(compact.value().rows[0].channel_table, compact.value().rows[1].channel_table);
  EXPECT_NE(compact.value().rows[0].channel_table.get(), table.get());
  auto index = prepare_dataset_coverage_index(compact.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  (*table)[0].label = "changed";
  EXPECT_TRUE(validate_dataset_snapshot_runtime(compact.value(), storage).ok());
  auto copied = deep_copy_dataset_snapshot(compact.value());
  EXPECT_NE(copied.rows[0].channel_table, compact.value().rows[0].channel_table);
  auto spans = index.value().lookup("s", "target", 1, {3, 6});
  ASSERT_TRUE(spans.ok()); ASSERT_EQ(spans.value().size(), 2u);
  EXPECT_EQ(spans.value()[0].logical_row, 0u);
  EXPECT_EQ(spans.value()[1].logical_row, 1u);
  EXPECT_EQ(index.value().spans().size(), 4u);
  auto edited = std::make_shared<std::vector<SnapshotChannelBinding>>(*copied.rows[0].channel_table);
  (*edited)[0].label = "edited";
  copied.rows[0].channel_table = edited;
  EXPECT_FALSE(validate_dataset_snapshot_runtime(copied, storage).ok());
  q.rows[0].channels = expanded.value().rows[0].channels;
  EXPECT_FALSE(prepare_dataset_snapshot(q, *catalog(backend), storage).ok());
}

TEST(DatasetSnapshot, StepMajorFlatDimensionsAndNullParentAreExplicit) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1, 8, true); backend->put("/dataset/b", 1, 8, true);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  for (auto& row : q.rows) row.value_layout = SnapshotValueLayout::StepMajorFlat;
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  auto spans = index.value().lookup("s", "target", 1, {2, 6});
  ASSERT_TRUE(spans.ok()); EXPECT_EQ(spans.value()[0].physical_ordinal, 1u);
  for (auto& row : q.rows) row.declared_n_channels = 3;
  snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok()); EXPECT_FALSE(prepare_dataset_coverage_index(snapshot.value(), storage).ok());
  for (auto& row : q.rows) row.declared_n_channels = 2;
  backend->put("/dataset/a", 1, 8, true, false, true);
  snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok()); index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  spans = index.value().lookup("s", "target", 1, {4, 8});
  ASSERT_TRUE(spans.ok()); EXPECT_TRUE(spans.value()[0].null_series);
}

TEST(DatasetSnapshot, ElectricityShared64And128StepBlocksRetainCompactCoverage) {
  for (const unsigned block : {64, 128}) {
    auto backend = std::make_shared<Backend>();
    backend->put("/dataset/a", 1, block * 370, true);
    backend->put("/dataset/b", 1, block * 370, true);
    auto storage = std::make_shared<StructuredBlobStorage>(backend);
    auto q = request(backend);
    auto table = std::make_shared<std::vector<SnapshotChannelBinding>>();
    for (unsigned c = 0; c < 370; ++c)
      table->push_back({"target", c, c, "channel-" + std::to_string(c), std::nullopt, block});
    auto row = q.rows.front(); row.value_layout = SnapshotValueLayout::StepMajorFlat;
    row.declared_n_channels = 370; row.declared_length = block;
    row.channels.clear(); row.channel_table = table;
    q.rows.clear();
    // Around 140k steps, matching Electricity; each declared row deliberately
    // aliases an identical physical test row with a different logical interval.
    for (unsigned i = 0; i < 140000 / block; ++i) {
      row.logical_row = i; row.shard_index = i % 2;
      row.steps = {std::uint64_t(i) * block, std::uint64_t(i + 1) * block};
      q.rows.push_back(row);
    }
    auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().message();
    EXPECT_LT(snapshot_channel_metadata_bytes(snapshot.value()), 1024u * 1024u);
    auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
    ASSERT_TRUE(index.ok()) << index.status().message();
    EXPECT_LT(index.value().estimated_metadata_bytes(), 16u * 1024u * 1024u);
    auto spans = index.value().lookup("s", "target", 369, {block - 1, 2 * block + 1});
    ASSERT_TRUE(spans.ok()); ASSERT_EQ(spans.value().size(), 3u);
    EXPECT_EQ(spans.value().back().logical_row, 2u);
  }
}

TEST(DatasetSnapshot, CompactCoverageKeepsPerChannelNullAndMissingFlags) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1, 4, false, true);
  backend->put("/dataset/b", 1, 4, false, true);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  auto table = std::make_shared<std::vector<SnapshotChannelBinding>>(q.rows[0].channels);
  table->push_back({"target", 2, std::nullopt, "absent", std::nullopt, 4});
  for (auto& row : q.rows) { row.channels.clear(); row.channel_table = table; }
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto index = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(index.ok()) << index.status().message();
  for (unsigned c = 0; c < 3; ++c) {
    auto spans = index.value().lookup("s", "target", c, {0, 8});
    ASSERT_TRUE(spans.ok()); ASSERT_EQ(spans.value().size(), 2u);
    for (const auto& span : spans.value()) {
      EXPECT_EQ(span.null_channel, c == 0);
      EXPECT_EQ(span.missing_channel, c == 2);
      EXPECT_FALSE(span.null_series);
    }
  }
}

TEST(DatasetSnapshot, CompactDiagnosticExpansionCanRaceIndexCopies) {
  auto backend = std::make_shared<Backend>();
  backend->put("/dataset/a", 1); backend->put("/dataset/b", 1);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto q = request(backend);
  auto table = std::make_shared<const std::vector<SnapshotChannelBinding>>(q.rows[0].channels);
  for (auto& row : q.rows) { row.channels.clear(); row.channel_table = table; }
  auto snapshot = prepare_dataset_snapshot(q, *catalog(backend), storage);
  ASSERT_TRUE(snapshot.ok());
  auto prepared = prepare_dataset_coverage_index(snapshot.value(), storage);
  ASSERT_TRUE(prepared.ok());
  for (unsigned iteration = 0; iteration < 32; ++iteration) {
    auto index = prepared.value(); // A fresh compact index, before lazy expansion.
    std::atomic<bool> start{false};
    std::thread diagnostic([&] {
      while (!start.load(std::memory_order_acquire)) std::this_thread::yield();
      EXPECT_EQ(index.spans().size(), 4u);
    });
    start.store(true, std::memory_order_release);
    DatasetCoverageIndex copy(index);
    DatasetCoverageIndex assigned;
    assigned = index;
    diagnostic.join();
    EXPECT_EQ(copy.spans().size(), 4u);
    auto coverage = assigned.lookup("s", "target", 1, {3, 6});
    ASSERT_TRUE(coverage.ok()); EXPECT_EQ(coverage.value().size(), 2u);
    EXPECT_EQ(assigned.snapshot_fingerprint(), snapshot.value().fingerprint);
  }
}
