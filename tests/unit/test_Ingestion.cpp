#include "iotdbfs/Ingestion.h"
#include "iotdbfs/PublicationProfile.h"
#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include "../support/InMemoryDatasetCatalog.h"

#include <gtest/gtest.h>
#include <eccodes.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace iotdbfs;

class MemoryBackend final : public StructuredBlobStorageBackend {
public:
  enum class Fault { None, PayloadWrite, Seal, LayoutWrite, MetadataWrite,
    Publish };

  struct Record {
    StructuredBlobManifest manifest;
    StructuredBlobBytes payload;
    StructuredBlobBytes layout;
  };

  StructuredBlobStatus create_manifest(
    const StructuredBlobManifest& manifest) override {
    if (records_.count(manifest.key)) return failure("already exists");
    records_.emplace(manifest.key, Record{ manifest, {}, {} });
    return StructuredBlobStatus::success();
  }

  StructuredBlobResult<StructuredBlobManifest> load_manifest(
    const StructuredBlobVersionKey& key) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    return StructuredBlobResult<StructuredBlobManifest>::success(
      found->second.manifest);
  }

  StructuredBlobStatus replace_manifest(
    const StructuredBlobVersionKey& key, std::uint64_t expected_revision,
    const StructuredBlobManifest& next) override {
    auto found = records_.find(key);
    if (found == records_.end() ||
        found->second.manifest.revision != expected_revision ||
        next.revision != expected_revision + 1) {
      return failure("invalid manifest transition");
    }
    if (fault_ == Fault::Seal &&
        next.phase == StructuredBlobPhase::PayloadSealed)
      return failure("seal failed");
    if (fault_ == Fault::Publish && next.published)
      return failure("publish failed");
    if (next.phase == StructuredBlobPhase::PayloadOpen &&
        next.payload_size > found->second.manifest.payload_size)
      payload_frontiers_.push_back(next.payload_size);
    found->second.manifest = next;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes) override {
    auto found = records_.find(key);
    if (found == records_.end()) return failure("not found");
    if (fault_ == Fault::PayloadWrite) return failure("payload write failed");
    const auto end = static_cast<std::size_t>(offset) + bytes.size();
    if (found->second.payload.size() < end)
      found->second.payload.resize(end);
    std::copy(bytes.begin(), bytes.end(),
      found->second.payload.begin() + static_cast<std::size_t>(offset));
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_layout(const StructuredBlobVersionKey& key,
    const StructuredBlobBytes& layout) override {
    auto found = records_.find(key);
    if (found == records_.end()) return failure("not found");
    if (fault_ == Fault::LayoutWrite) return failure("layout write failed");
    found->second.layout = layout;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey& key,
    const MetadataMap&) override {
    if (fault_ == Fault::MetadataWrite)
      return failure("metadata write failed");
    return records_.count(key) ? StructuredBlobStatus::success() :
      failure("not found");
  }

  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& a) override {
    return catalogs_.write(key, a);
  }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
      const StructuredBlobVersionKey& key) const override {
    return catalogs_.read(key);
  }

  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    const auto begin = found->second.payload.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      StructuredBlobBytes(begin, begin + static_cast<std::size_t>(length)));
  }

  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      const StructuredBlobVersionKey& key, std::uint64_t offset,
      std::uint64_t length) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    const auto begin = found->second.layout.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }

  StructuredBlobStatus erase(const StructuredBlobVersionKey& key,
    std::uint64_t expected_revision) override {
    const auto found = records_.find(key);
    if (found == records_.end() ||
        found->second.manifest.revision != expected_revision)
      return failure("erase failed");
    catalogs_.erase(key);
    records_.erase(found);
    return StructuredBlobStatus::success();
  }

  const Record* peek(const StructuredBlobVersionKey& key) const {
    const auto found = records_.find(key);
    return found == records_.end() ? nullptr : &found->second;
  }

  void fail_publish() { fault_ = Fault::Publish; }
  void set_fault(Fault fault) { fault_ = fault; }
  const std::vector<std::uint64_t>& payload_frontiers() const {
    return payload_frontiers_;
  }

private:
  static StructuredBlobStatus failure(const char* message) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::BackendFailure, message);
  }

  std::map<StructuredBlobVersionKey, Record> records_;
  std::vector<std::uint64_t> payload_frontiers_;
  Fault fault_{ Fault::None };
  iotdbfs::test::InMemoryDatasetCatalog catalogs_;
};

struct AdapterState {
  std::vector<std::uint64_t> offsets;
  std::vector<std::size_t> lengths;
  std::uint64_t byte_digest{ 0 };
  bool consume_failed{ false };
  bool finish_failed{ false };
  std::optional<DatasetCatalogArtifact> dataset_catalog;
};

class RecordingSession final : public FormatIngestionSession {
public:
  explicit RecordingSession(std::shared_ptr<AdapterState> state)
    : state_(std::move(state)) {}

  FormatStatus consume_chunk(std::uint64_t offset, const std::uint8_t* data,
    std::size_t length) override {
    if (state_->consume_failed)
      return FormatStatus::failure("injected adapter consume failure");
    state_->offsets.push_back(offset);
    state_->lengths.push_back(length);
    for (std::size_t index = 0; index < length; ++index)
      state_->byte_digest = state_->byte_digest * 131 + data[index];
    return FormatStatus::success();
  }

  FormatResult<FormatIngestionArtifacts> finish() override {
    if (state_->finish_failed) {
      return FormatResult<FormatIngestionArtifacts>::failure(
        "injected adapter failure");
    }
    FormatIngestionArtifacts artifacts;
    artifacts.layout = { 9, 0, 8 };
    artifacts.metadata.emplace("kind", MetadataValue("recording"));
    artifacts.metadata.emplace("count", MetadataValue(std::int64_t{ 3 }));
    artifacts.dataset_catalog = state_->dataset_catalog;
    return FormatResult<FormatIngestionArtifacts>::success(
      std::move(artifacts));
  }

private:
  std::shared_ptr<AdapterState> state_;
};

class RecordingAdapter final : public FormatAdapter {
public:
  explicit RecordingAdapter(std::shared_ptr<AdapterState> state)
    : state_(std::move(state)) {}

  const std::string& id() const override {
    static const std::string value = "recording";
    return value;
  }
  const std::vector<std::string>& extensions() const override {
    static const std::vector<std::string> value;
    return value;
  }
  bool matches(const FormatSource& source) const override {
    if (source.size == 0 || !source.read) return false;
    auto prefix = source.read(0, 1);
    return prefix && *prefix == StructuredBlobBytes{ 0xa0 };
  }
  std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource&) const override {
    return std::make_unique<RecordingSession>(state_);
  }

private:
  std::shared_ptr<AdapterState> state_;
};

class HeaderReadingAdapter final : public FormatAdapter {
public:
  explicit HeaderReadingAdapter(bool claim_after_failed_read = false)
    : claim_after_failed_read_(claim_after_failed_read) {}

  const std::string& id() const override {
    static const std::string value = "header-reader";
    return value;
  }
  const std::vector<std::string>& extensions() const override {
    static const std::vector<std::string> value;
    return value;
  }
  bool matches(const FormatSource& source) const override {
    auto header = source.read(0, 8);
    return claim_after_failed_read_ ||
      (header && *header == StructuredBlobBytes{ 1, 2, 3, 4, 5, 6, 7, 8 });
  }
  std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource&) const override {
    return nullptr;
  }

private:
  bool claim_after_failed_read_{false};
};

struct StreamState {
  std::vector<std::pair<std::uint64_t, std::uint64_t>> finish_reads;
  std::function<std::optional<StructuredBlobBytes>(
    std::uint64_t, std::uint64_t)> escaped_read_for_lifetime_test;
  bool request_out_of_range{ false };
  bool destroyed{ false };
};

// A small IPC-shaped stream fake. Framing and metadata can cross any chunk;
// batch bodies are skipped rather than copied. The index is completed using
// bounded reads of body offsets from the original source in finish().
class StreamIndexSession final : public FormatIngestionSession {
public:
  StreamIndexSession(FormatSource source, std::shared_ptr<StreamState> state)
    : source_(std::move(source)), state_(std::move(state)) {
    state_->escaped_read_for_lifetime_test = source_.read;
  }
  ~StreamIndexSession() override { state_->destroyed = true; }

  FormatStatus consume_chunk(std::uint64_t offset, const std::uint8_t* data,
    std::size_t length) override {
    if (offset != position_)
      return FormatStatus::failure("non-sequential stream chunk");
    for (std::size_t index = 0; index < length; ++index) {
      const auto byte = data[index];
      if (stage_ == Stage::Done)
        return FormatStatus::failure("bytes after stream EOS");
      if (stage_ == Stage::Header) {
        header_[used_++] = byte;
        if (used_ == header_.size()) {
          if (word(header_.data()) != 0xffffffffu)
            return FormatStatus::failure("invalid stream continuation");
          const auto metadata_length = word(header_.data() + 4);
          used_ = 0;
          if (metadata_length == 0) stage_ = Stage::Done;
          else if (metadata_length == metadata_.size()) stage_ = Stage::Metadata;
          else return FormatStatus::failure("unexpected metadata length");
        }
      }
      else if (stage_ == Stage::Metadata) {
        metadata_[used_++] = byte;
        if (used_ == metadata_.size()) {
          remaining_body_ = word(metadata_.data());
          if (remaining_body_ < 8)
            return FormatStatus::failure("batch body is too short");
          body_offsets_.push_back(position_ + 1);
          used_ = 0;
          stage_ = Stage::Body;
        }
      }
      else {
        if (--remaining_body_ == 0) stage_ = Stage::Header;
      }
      ++position_;
    }
    return FormatStatus::success();
  }

  FormatResult<FormatIngestionArtifacts> finish() override {
    if (stage_ != Stage::Done || position_ != source_.size)
      return FormatResult<FormatIngestionArtifacts>::failure(
        "incomplete stream");
    if (state_->request_out_of_range)
      (void)source_.read(source_.size, 1);
    FormatIngestionArtifacts artifacts;
    artifacts.layout.push_back(static_cast<std::uint8_t>(body_offsets_.size()));
    for (const auto body_offset : body_offsets_) {
      const auto offset = body_offset + 4;
      state_->finish_reads.emplace_back(offset, 4);
      auto bytes = source_.read(offset, 4);
      if (!bytes || bytes->size() != 4)
        return FormatResult<FormatIngestionArtifacts>::failure(
          "cannot read nested offset");
      for (unsigned shift = 0; shift < 64; shift += 8)
        artifacts.layout.push_back(
          static_cast<std::uint8_t>(body_offset >> shift));
      artifacts.layout.insert(artifacts.layout.end(),
        bytes->begin(), bytes->end());
    }
    artifacts.metadata.emplace("kind", MetadataValue("stream-fake"));
    return FormatResult<FormatIngestionArtifacts>::success(
      std::move(artifacts));
  }

private:
  enum class Stage { Header, Metadata, Body, Done };
  static std::uint32_t word(const std::uint8_t* bytes) {
    return static_cast<std::uint32_t>(bytes[0]) |
      (static_cast<std::uint32_t>(bytes[1]) << 8) |
      (static_cast<std::uint32_t>(bytes[2]) << 16) |
      (static_cast<std::uint32_t>(bytes[3]) << 24);
  }

  FormatSource source_;
  std::shared_ptr<StreamState> state_;
  Stage stage_{ Stage::Header };
  std::array<std::uint8_t, 8> header_{};
  std::array<std::uint8_t, 8> metadata_{};
  std::size_t used_{ 0 };
  std::uint32_t remaining_body_{ 0 };
  std::uint64_t position_{ 0 };
  std::vector<std::uint64_t> body_offsets_;
};

class StreamIndexAdapter final : public FormatAdapter {
public:
  explicit StreamIndexAdapter(std::shared_ptr<StreamState> state)
    : state_(std::move(state)) {}
  const std::string& id() const override {
    static const std::string value = "stream-fake";
    return value;
  }
  const std::vector<std::string>& extensions() const override {
    static const std::vector<std::string> value;
    return value;
  }
  bool matches(const FormatSource& source) const override {
    auto bytes = source.read(0, 4);
    return bytes && *bytes == StructuredBlobBytes{ 0xff, 0xff, 0xff, 0xff };
  }
  std::unique_ptr<FormatIngestionSession> begin_ingestion(
    const FormatSource& source) const override {
    return std::make_unique<StreamIndexSession>(source, state_);
  }

private:
  std::shared_ptr<StreamState> state_;
};

StructuredBlobBytes fake_stream() {
  StructuredBlobBytes bytes;
  const auto append_word = [&](std::uint32_t value) {
    for (unsigned shift = 0; shift < 32; shift += 8)
      bytes.push_back(static_cast<std::uint8_t>(value >> shift));
  };
  for (std::uint32_t index = 0; index < 2; ++index) {
    append_word(0xffffffffu);
    append_word(8);
    append_word(24); // body length in fake metadata
    append_word(index); // batch ordinal in fake metadata
    for (std::uint32_t position = 0; position < 24; ++position)
      bytes.push_back(position >= 4 && position < 8 ?
        static_cast<std::uint8_t>((index + 3) >> ((position - 4) * 8)) :
        static_cast<std::uint8_t>(position + index));
  }
  append_word(0xffffffffu);
  append_word(0); // stream EOS
  return bytes;
}

StructuredBlobBytes sample_grib2() {
  codes_handle* handle = codes_grib_handle_new_from_samples(nullptr, "GRIB2");
  if (!handle) return {};
  const void* message = nullptr;
  std::size_t size = 0;
  const int error = codes_get_message(handle, &message, &size);
  StructuredBlobBytes bytes;
  if (error == 0 && message && size > 0) {
    const auto* begin = static_cast<const std::uint8_t*>(message);
    bytes.assign(begin, begin + size);
  }
  codes_handle_delete(handle);
  return bytes;
}

FormatSource source_for(const StructuredBlobBytes& bytes) {
  return { bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length)
      -> std::optional<StructuredBlobBytes> {
    if (offset > bytes.size() || length > bytes.size() - offset)
      return std::nullopt;
    return StructuredBlobBytes(bytes.begin() + offset,
      bytes.begin() + offset + length);
  } };
}

} // namespace

TEST(IngestionModule, BytePassThroughIsTheBaselineWithoutAnAdapter) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  IngestionModule ingestion(storage, formats, { 2 });
  const StructuredBlobVersionKey key{ "/raw.bin", 1 };
  const StructuredBlobBytes bytes{ 1, 2, 3, 4, 5 };
  bool prepared = false;

  auto result = ingestion.ingest(key, source_for(bytes),
    [&](const IngestionOutcome& outcome) {
      prepared = true;
      EXPECT_FALSE(outcome.format_id);
      EXPECT_TRUE(outcome.metadata.empty());
      EXPECT_EQ(outcome.payload_size, bytes.size());
      EXPECT_EQ(outcome.layout_size, 0u);
      EXPECT_EQ(storage.load_manifest(key).status().code(),
        StructuredBlobError::NotFound);
      return true;
    });

  ASSERT_TRUE(result.ok()) << result.message();
  EXPECT_TRUE(prepared);
  const auto manifest = storage.load_manifest(key);
  ASSERT_TRUE(manifest.ok());
  EXPECT_FALSE(manifest.value().format_id);
  EXPECT_TRUE(manifest.value().metadata.empty());
  EXPECT_EQ(manifest.value().layout_size, 0u);
  const auto payload = storage.read_payload_range(key, 0, bytes.size());
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), bytes);
}

TEST(IngestionModule, AdapterOnlyDerivesTypedMetadataAndPrivateLayout) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  auto state = std::make_shared<AdapterState>();
  ASSERT_TRUE(formats.register_adapter(
    std::make_shared<RecordingAdapter>(state)));
  IngestionModule ingestion(storage, formats, { 2 });
  const StructuredBlobVersionKey key{ "/science.data", 2 };
  const StructuredBlobBytes bytes{ 0xa0, 1, 2, 3, 4 };

  auto result = ingestion.ingest(key, source_for(bytes),
    [](const IngestionOutcome& outcome) {
      return outcome.format_id == std::optional<std::string>("recording") &&
        outcome.layout_size == 3 && outcome.metadata.size() == 2;
    });

  ASSERT_TRUE(result.ok()) << result.message();
  EXPECT_EQ(state->offsets, (std::vector<std::uint64_t>{ 0, 2, 4 }));
  EXPECT_EQ(state->lengths, (std::vector<std::size_t>{ 2, 2, 1 }));
  std::uint64_t expected_digest = 0;
  for (auto byte : bytes) expected_digest = expected_digest * 131 + byte;
  EXPECT_EQ(state->byte_digest, expected_digest);
  const auto manifest = storage.load_manifest(key);
  ASSERT_TRUE(manifest.ok());
  ASSERT_TRUE(manifest.value().format_id);
  EXPECT_EQ(*manifest.value().format_id, "recording");
  EXPECT_EQ(manifest.value().metadata.size(), 2u);
  const auto layout = storage.load_layout(key);
  ASSERT_TRUE(layout.ok());
  EXPECT_EQ(layout.value(), (StructuredBlobBytes{ 9, 0, 8 }));
}

TEST(IngestionModule, PublicationCallbackRetainsDurableDatasetIndexSeal) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  auto state = std::make_shared<AdapterState>();
  DatasetCatalogArtifact catalog;
  catalog.format_id = "recording";
  catalog.layout_protocol = "recording-layout/1";
  catalog.layout_digest = std::string(64, 'a');
  DatasetCatalogRecord record;
  record.object_id = "dataset-1";
  record.canonical_path = "/temperature";
  catalog.records.push_back(record);
  state->dataset_catalog = catalog;
  ASSERT_TRUE(formats.register_adapter(
      std::make_shared<RecordingAdapter>(state)));
  IngestionModule ingestion(storage, formats, {2});
  const StructuredBlobVersionKey key{"/indexed.bin", 41};
  const StructuredBlobBytes bytes{0xa0, 1, 2, 3};
  MetadataMap callback_metadata;
  bool prepared = false;
  auto result = ingestion.ingest(key, source_for(bytes),
      [&](const IngestionOutcome& outcome) {
        prepared = true;
        callback_metadata = outcome.metadata;
        const auto* durable = backend->peek(key);
        if (!durable) {
          ADD_FAILURE() << "missing described manifest";
          return false;
        }
        EXPECT_FALSE(durable->manifest.published);
        EXPECT_EQ(durable->manifest.phase, StructuredBlobPhase::Described);
        auto from_outcome = durable->manifest;
        from_outcome.metadata = outcome.metadata;
        EXPECT_TRUE(validate_dataset_substructure_index(from_outcome, catalog).ok());
        EXPECT_TRUE(validate_dataset_substructure_index(durable->manifest, catalog).ok());
        for (const auto* suffix : {"protocol", "sha256", "bytes", "records", "attributes"}) {
          const auto name = std::string("__iotdbfs.dataset_index.") + suffix;
          const auto actual = outcome.metadata.find(name);
          const auto persisted = durable->manifest.metadata.find(name);
          if (actual == outcome.metadata.end() || persisted == durable->manifest.metadata.end()) {
            ADD_FAILURE() << "seal field missing from publication callback: " << name;
            return false;
          }
          EXPECT_EQ(actual->second.value(), persisted->second.value());
        }
        return true;
      });
  ASSERT_TRUE(result.ok()) << result.message();
  EXPECT_TRUE(prepared);
  const auto visible = storage.load_manifest(key);
  ASSERT_TRUE(visible.ok());
  EXPECT_TRUE(validate_dataset_substructure_index(visible.value(), catalog).ok());
  EXPECT_EQ(result.outcome().metadata.at("__iotdbfs.dataset_index.sha256").value(),
            callback_metadata.at("__iotdbfs.dataset_index.sha256").value());
  EXPECT_EQ(visible.value().metadata.at("__iotdbfs.dataset_index.sha256").value(),
            callback_metadata.at("__iotdbfs.dataset_index.sha256").value());
}

TEST(IngestionModule, BatchesFrontiersWhilePreservingAdapterChunkBoundaries) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  auto state = std::make_shared<AdapterState>();
  ASSERT_TRUE(formats.register_adapter(std::make_shared<RecordingAdapter>(state)));
  IngestionModule ingestion(storage, formats, {2, 4});
  const StructuredBlobVersionKey key{"/batched", 1};
  const StructuredBlobBytes bytes{0xa0, 1, 2, 3, 4, 5, 6, 7, 8};
  const auto result = ingestion.ingest(key, source_for(bytes),
    [](const IngestionOutcome&) { return true; });
  ASSERT_TRUE(result.ok()) << result.message();
  EXPECT_EQ(backend->payload_frontiers(),
    (std::vector<std::uint64_t>{4, 8, 9}));
  EXPECT_EQ(state->offsets, (std::vector<std::uint64_t>{0, 2, 4, 6, 8}));
  EXPECT_EQ(state->lengths, (std::vector<std::size_t>{2, 2, 2, 2, 1}));
  const auto payload = storage.read_payload_range(key, 0, bytes.size());
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), bytes);
}

TEST(IngestionModule, BatchesHaveAChunkCountBoundAndFlushTheTail) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  IngestionModule ingestion(storage, formats, {1, 128});
  const StructuredBlobVersionKey key{"/many-small-chunks", 1};
  StructuredBlobBytes bytes(130);
  for (std::size_t i = 0; i < bytes.size(); ++i) bytes[i] = i;
  const auto result = ingestion.ingest(key, source_for(bytes),
    [](const IngestionOutcome&) { return true; });
  ASSERT_TRUE(result.ok()) << result.message();
  EXPECT_EQ(backend->payload_frontiers(),
    (std::vector<std::uint64_t>{64, 128, 130}));
  const auto payload = storage.read_payload_range(key, 0, bytes.size());
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), bytes);
}

TEST(IngestionModule, BatchBudgetIsValidatedBeforeBeginning) {
  for (const auto config : {IngestionModule::Config{4, 3},
        IngestionModule::Config{4, 8 * 1024 * 1024 + 1}}) {
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    IngestionModule ingestion(storage, formats, config);
    const StructuredBlobVersionKey key{"/bad-budget", 1};
    const auto result = ingestion.ingest(key, source_for(StructuredBlobBytes{}),
      [](const IngestionOutcome&) { return true; });
    EXPECT_EQ(result.error(), IngestionError::InvalidSource);
    EXPECT_EQ(backend->peek(key), nullptr);
  }
}

TEST(IngestionModule, SourceFailureAfterACheckpointAbortsTheEntireVersion) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  IngestionModule ingestion(storage, formats, {2, 4});
  const StructuredBlobVersionKey key{"/failed-batch", 1};
  const StructuredBlobBytes bytes{1, 2, 3, 4, 5, 6, 7};
  auto source = source_for(bytes);
  const auto exact_read = source.read;
  source.read = [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<StructuredBlobBytes> {
    if (offset == 6) {
      EXPECT_EQ(backend->payload_frontiers(), (std::vector<std::uint64_t>{4}));
      return std::nullopt;
    }
    return exact_read(offset, length);
  };
  bool prepared = false;
  const auto result = ingestion.ingest(key, source,
    [&](const IngestionOutcome&) { prepared = true; return true; });
  EXPECT_EQ(result.error(), IngestionError::SourceReadFailure);
  EXPECT_FALSE(prepared);
  EXPECT_EQ(backend->peek(key), nullptr);
}

TEST(IngestionModule, StreamIndexUsesStableSourceAcrossEveryChunkSplit) {
  const auto bytes = fake_stream();
  StructuredBlobBytes expected_layout;
  for (std::size_t chunk_size : { 1u, 2u, 3u, 4u, 5u, 7u, 8u, 11u, 64u }) {
    SCOPED_TRACE(chunk_size);
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    auto state = std::make_shared<StreamState>();
    ASSERT_TRUE(formats.register_adapter(
      std::make_shared<StreamIndexAdapter>(state)));
    IngestionModule ingestion(storage, formats, { chunk_size });
    const StructuredBlobVersionKey key{ "/stream.arrows", 50 };
    std::vector<std::pair<std::uint64_t, std::uint64_t>> reads;
    FormatSource source{ bytes.size(), [&](std::uint64_t offset,
        std::uint64_t length) -> std::optional<StructuredBlobBytes> {
      reads.emplace_back(offset, length);
      if (offset > bytes.size() || length > bytes.size() - offset)
        return std::nullopt;
      if (!state->finish_reads.empty() &&
          state->finish_reads.back() ==
            std::pair<std::uint64_t, std::uint64_t>{ offset, length }) {
        EXPECT_EQ(storage.load_manifest(key).status().code(),
          StructuredBlobError::NotFound);
        const auto* pending = backend->peek(key);
        EXPECT_NE(pending, nullptr);
        if (pending) {
          EXPECT_EQ(pending->manifest.phase, StructuredBlobPhase::PayloadOpen);
          EXPECT_EQ(pending->manifest.payload_size, bytes.size());
        }
      }
      return StructuredBlobBytes(bytes.begin() + offset,
        bytes.begin() + offset + length);
    } };
    const auto result = ingestion.ingest(key, source,
      [&](const IngestionOutcome& outcome) {
        EXPECT_EQ(storage.load_manifest(key).status().code(),
          StructuredBlobError::NotFound);
        const auto* pending = backend->peek(key);
        EXPECT_NE(pending, nullptr);
        if (pending) {
          EXPECT_EQ(pending->manifest.phase, StructuredBlobPhase::Described);
          EXPECT_EQ(pending->manifest.layout_size, 25u);
          EXPECT_EQ(pending->manifest.payload_size, bytes.size());
        }
        return outcome.format_id == std::optional<std::string>("stream-fake") &&
          outcome.layout_size == 25;
      });
    ASSERT_TRUE(result.ok()) << result.message();
    EXPECT_TRUE(state->destroyed);
    ASSERT_TRUE(state->escaped_read_for_lifetime_test);
    EXPECT_FALSE(state->escaped_read_for_lifetime_test(0, 1));
    EXPECT_EQ(state->finish_reads,
      (std::vector<std::pair<std::uint64_t, std::uint64_t>>{
        { 20, 4 }, { 60, 4 } }));
    EXPECT_NE(std::find(reads.begin(), reads.end(),
      std::pair<std::uint64_t, std::uint64_t>{ 20, 4 }), reads.end());
    const auto layout = storage.load_layout(key);
    ASSERT_TRUE(layout.ok());
    if (expected_layout.empty()) expected_layout = layout.value();
    EXPECT_EQ(layout.value(), expected_layout);
    const auto payload = storage.read_payload_range(key, 0, bytes.size());
    ASSERT_TRUE(payload.ok());
    EXPECT_EQ(payload.value(), bytes);
  }
}

TEST(IngestionModule, ProbeFailureCannotSilentlyBecomePassThrough) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  auto state = std::make_shared<StreamState>();
  ASSERT_TRUE(formats.register_adapter(
    std::make_shared<StreamIndexAdapter>(state)));
  IngestionModule ingestion(storage, formats, { 3 });
  const auto bytes = fake_stream();
  const StructuredBlobVersionKey key{ "/stream.arrows", 51 };
  auto source = source_for(bytes);
  const auto exact_read = source.read;
  int calls = 0;
  source.read = [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<StructuredBlobBytes> {
    if (++calls == 1) return StructuredBlobBytes{ 0xff };
    return exact_read(offset, length);
  };
  const auto result = ingestion.ingest(key, source,
    [](const IngestionOutcome&) { return true; });
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error(), IngestionError::SourceReadFailure);
  EXPECT_EQ(calls, 1);
  EXPECT_EQ(backend->peek(key), nullptr);
}

TEST(IngestionModule, FinishSourceFailuresAndInvalidRangesAbort) {
  const auto bytes = fake_stream();
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    auto state = std::make_shared<StreamState>();
    state->request_out_of_range = mode == 2;
    ASSERT_TRUE(formats.register_adapter(
      std::make_shared<StreamIndexAdapter>(state)));
    IngestionModule ingestion(storage, formats, { 1 });
    const StructuredBlobVersionKey key{ "/stream.arrows", 52 + mode };
    auto source = source_for(bytes);
    const auto exact_read = source.read;
    source.read = [&](std::uint64_t offset, std::uint64_t length)
        -> std::optional<StructuredBlobBytes> {
      if (offset == 20 && length == 4) {
        if (mode == 0) return std::nullopt;
        if (mode == 1) return StructuredBlobBytes{ 1, 2 };
      }
      return exact_read(offset, length);
    };
    const auto result = ingestion.ingest(key, source,
      [](const IngestionOutcome&) { return true; });
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error(), mode == 2 ? IngestionError::AdapterFailure :
      IngestionError::SourceReadFailure);
    EXPECT_EQ(backend->peek(key), nullptr);
    EXPECT_EQ(storage.load_manifest(key).status().code(),
      StructuredBlobError::NotFound);
    EXPECT_TRUE(state->destroyed);
  }
}

TEST(IngestionModule, GribIngestionStillPublishesDerivedLayout) {
  const auto bytes = sample_grib2();
  ASSERT_FALSE(bytes.empty());
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  GribFormatAdapter::Config config;
  config.ingestion.chunk_size = 7;
  config.ingestion.detect_threshold_bytes = 16;
  ASSERT_TRUE(formats.register_adapter(
    std::make_shared<GribFormatAdapter>(std::move(config))));
  IngestionModule ingestion(storage, formats, { 7 });
  const StructuredBlobVersionKey key{ "/sample.grib2", 58 };
  const auto result = ingestion.ingest(key, source_for(bytes),
    [](const IngestionOutcome& outcome) {
      return outcome.format_id == std::optional<std::string>("grib") &&
        outcome.layout_size > 0 && !outcome.metadata.empty();
    });
  ASSERT_TRUE(result.ok()) << result.message();
  const auto layout = storage.load_layout(key);
  ASSERT_TRUE(layout.ok());
  EXPECT_FALSE(layout.value().empty());
  const auto payload = storage.read_payload_range(key, 0, bytes.size());
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), bytes);
}

TEST(IngestionModule, EmptyFileUsesTheSameBaselineLifecycle) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  IngestionModule ingestion(storage, formats, { 4 });
  const StructuredBlobVersionKey key{ "/empty", 3 };

  auto result = ingestion.ingest(key, {},
    [](const IngestionOutcome& outcome) {
      return outcome.payload_size == 0 && !outcome.format_id;
    });
  ASSERT_TRUE(result.ok()) << result.message();
  ASSERT_TRUE(storage.load_manifest(key).ok());
}

TEST(IngestionModule, ShortFilesSkipOutOfRangeFormatProbes) {
  for (const auto& bytes : {StructuredBlobBytes{},
        StructuredBlobBytes{'t', 'x', 't'},
        StructuredBlobBytes{0xa0, 1, 2},
        StructuredBlobBytes(16, 0)}) {
    SCOPED_TRACE(bytes.size());
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    // A detector that claims a match after its invalid read must still be
    // skipped; the following detector gets an independent chance to match.
    ASSERT_TRUE(formats.register_adapter(
      std::make_shared<HeaderReadingAdapter>(bytes.size() < 8)));
    auto state = std::make_shared<AdapterState>();
    ASSERT_TRUE(formats.register_adapter(
      std::make_shared<RecordingAdapter>(state)));
    IngestionModule ingestion(storage, formats, {2});
    const StructuredBlobVersionKey key{"/short", 1};
    const auto source = bytes.empty() ? FormatSource{} : source_for(bytes);
    const auto result = ingestion.ingest(key, source,
      [](const IngestionOutcome&) { return true; });
    ASSERT_TRUE(result.ok()) << result.message();
    const auto manifest = storage.load_manifest(key);
    ASSERT_TRUE(manifest.ok());
    if (!bytes.empty() && bytes.front() == 0xa0)
      EXPECT_EQ(manifest.value().format_id,
        std::optional<std::string>("recording"));
    else
      EXPECT_FALSE(manifest.value().format_id);
    const auto payload = storage.read_payload_range(key, 0, bytes.size());
    ASSERT_TRUE(payload.ok());
    EXPECT_EQ(payload.value(), bytes);
  }
}

TEST(IngestionModule, EarlierProbeOverreadDoesNotHideLaterReadFailure) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(
    std::make_shared<HeaderReadingAdapter>()));
  ASSERT_TRUE(formats.register_adapter(
    std::make_shared<RecordingAdapter>(std::make_shared<AdapterState>())));
  IngestionModule ingestion(storage, formats, {2});
  const StructuredBlobVersionKey key{"/short", 2};
  const StructuredBlobBytes bytes{0xa0, 1, 2};
  auto source = source_for(bytes);
  source.read = [](std::uint64_t, std::uint64_t)
      -> std::optional<StructuredBlobBytes> {
    return StructuredBlobBytes{};
  };

  const auto result = ingestion.ingest(key, source,
    [](const IngestionOutcome&) { return true; });
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error(), IngestionError::SourceReadFailure);
  EXPECT_EQ(backend->peek(key), nullptr);
}

TEST(IngestionModule, ReadAdapterAndPreparationFailuresAbortTheVersion) {
  for (const int failure : { 0, 1, 2, 3 }) {
    SCOPED_TRACE(failure);
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    auto state = std::make_shared<AdapterState>();
    if (failure == 1) state->consume_failed = true;
    if (failure == 2) state->finish_failed = true;
    ASSERT_TRUE(formats.register_adapter(
      std::make_shared<RecordingAdapter>(state)));
    IngestionModule ingestion(storage, formats, { 2 });
    const StructuredBlobVersionKey key{ "/failed", failure + 10 };
    const StructuredBlobBytes bytes{ 0xa0, 1, 2 };
    auto source = source_for(bytes);
    if (failure == 0) {
      const auto exact_read = source.read;
      source.read = [&, exact_read](std::uint64_t offset,
          std::uint64_t length) -> std::optional<StructuredBlobBytes> {
        if (offset == 2 && length == 1) {
          EXPECT_NE(backend->peek(key), nullptr);
          return std::nullopt;
        }
        return exact_read(offset, length);
      };
    }
    const auto result = ingestion.ingest(key, source,
      [failure](const IngestionOutcome&) { return failure != 3; });
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(backend->peek(key), nullptr);
    EXPECT_EQ(storage.load_manifest(key).status().code(),
      StructuredBlobError::NotFound);
  }
}

TEST(IngestionModule, PublishFailureDoesNotLeaveAVisibleVersion) {
  auto backend = std::make_shared<MemoryBackend>();
  backend->fail_publish();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  IngestionModule ingestion(storage, formats, { 4 });
  const StructuredBlobVersionKey key{ "/publish-fails", 20 };
  const StructuredBlobBytes bytes{ 1, 2 };

  const auto result = ingestion.ingest(key, source_for(bytes),
    [](const IngestionOutcome&) { return true; });
  EXPECT_FALSE(result.ok());
  EXPECT_EQ(result.error(), IngestionError::StorageFailure);
  EXPECT_EQ(backend->peek(key), nullptr);
}

TEST(IngestionModule, EveryStoragePhaseFailureAbortsAndAllowsRetry) {
  for (auto fault : { MemoryBackend::Fault::PayloadWrite,
        MemoryBackend::Fault::Seal, MemoryBackend::Fault::LayoutWrite,
        MemoryBackend::Fault::MetadataWrite, MemoryBackend::Fault::Publish }) {
    SCOPED_TRACE(static_cast<int>(fault));
    auto backend = std::make_shared<MemoryBackend>();
    backend->set_fault(fault);
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    auto state = std::make_shared<AdapterState>();
    ASSERT_TRUE(formats.register_adapter(
      std::make_shared<RecordingAdapter>(state)));
    IngestionModule ingestion(storage, formats, { 2 });
    const StructuredBlobVersionKey key{ "/retry.data", 30 };
    const StructuredBlobBytes bytes{ 0xa0, 1, 2, 3 };
    int publication_calls = 0;
    const auto prepare = [&](const IngestionOutcome&) {
      ++publication_calls;
      EXPECT_EQ(storage.load_manifest(key).status().code(),
        StructuredBlobError::NotFound);
      return true;
    };
    const auto failed = ingestion.ingest(key, source_for(bytes), prepare);
    EXPECT_FALSE(failed.ok());
    EXPECT_EQ(failed.error(), IngestionError::StorageFailure);
    EXPECT_EQ(backend->peek(key), nullptr);
    EXPECT_EQ(storage.load_manifest(key).status().code(),
      StructuredBlobError::NotFound);

    backend->set_fault(MemoryBackend::Fault::None);
    const auto retried = ingestion.ingest(key, source_for(bytes), prepare);
    ASSERT_TRUE(retried.ok()) << retried.message();
    EXPECT_TRUE(storage.load_manifest(key).ok());
    EXPECT_EQ(publication_calls,
      fault == MemoryBackend::Fault::Publish ? 2 : 1);
  }
}

TEST(IngestionModule, PayloadProfilingExcludesDetectionAndPreservesChunkBoundaries) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  auto state = std::make_shared<AdapterState>();
  ASSERT_TRUE(formats.register_adapter(std::make_shared<RecordingAdapter>(state)));
  IngestionModule ingestion(storage, formats, {2, 4});
  const StructuredBlobBytes bytes{0xa0, 1, 2, 3, 4};
  const StructuredBlobVersionKey key{"/profiled", 110};
  auto record = std::make_shared<PublicationProfileRecord>();
  {
    PublicationProfileScope scope(record);
    const auto result = ingestion.ingest(key, source_for(bytes),
      [](const IngestionOutcome&) { return true; });
    ASSERT_TRUE(result.ok()) << result.message();
  }
  const auto payload = record->snapshot().at("payload");
  EXPECT_EQ(payload.details.at("payload_source_read").calls, 3);
  EXPECT_EQ(payload.details.at("payload_source_read").failures, 0);
  EXPECT_EQ(payload.details.at("payload_adapter_consume").calls, 3);
  EXPECT_EQ(payload.details.at("payload_adapter_consume").failures, 0);
  EXPECT_EQ(state->offsets, (std::vector<std::uint64_t>{0, 2, 4}));
  EXPECT_EQ(backend->payload_frontiers(), (std::vector<std::uint64_t>{4, 5}));
  const auto copied = storage.read_payload_range(key, 0, bytes.size());
  ASSERT_TRUE(copied.ok());
  EXPECT_EQ(copied.value(), bytes);
}

TEST(IngestionModule, PayloadReadAndConsumeFailuresAreProfiledAndStillAbort) {
  for (const bool fail_read : {false, true}) {
    SCOPED_TRACE(fail_read);
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    auto state = std::make_shared<AdapterState>();
    state->consume_failed = !fail_read;
    ASSERT_TRUE(formats.register_adapter(std::make_shared<RecordingAdapter>(state)));
    IngestionModule ingestion(storage, formats, {2, 2});
    const StructuredBlobBytes bytes{0xa0, 1, 2};
    const StructuredBlobVersionKey key{"/profile-failure", 111};
    auto source = source_for(bytes);
    if (fail_read) {
      const auto reader = source.read;
      source.read = [reader](std::uint64_t offset, std::uint64_t length)
          -> std::optional<StructuredBlobBytes> {
        if (offset == 2) return std::nullopt;
        return reader(offset, length);
      };
    }
    auto record = std::make_shared<PublicationProfileRecord>();
    {
      PublicationProfileScope scope(record);
      const auto result = ingestion.ingest(key, source,
        [](const IngestionOutcome&) { return true; });
      EXPECT_FALSE(result.ok());
      EXPECT_EQ(result.error(), fail_read ? IngestionError::SourceReadFailure :
        IngestionError::AdapterFailure);
    }
    const auto payload = record->snapshot().at("payload");
    EXPECT_EQ(payload.details.at("payload_source_read").failures, fail_read ? 1 : 0);
    EXPECT_EQ(payload.details.at("payload_adapter_consume").failures, fail_read ? 0 : 1);
    EXPECT_EQ(backend->peek(key), nullptr);
  }
}
