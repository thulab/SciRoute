#include "iotdbfs/StructuredBlobStorage.h"
#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/storage/versioning/PayloadArtifacts.h"
#include "../support/InMemoryDatasetCatalog.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <limits>
#include <set>
#include <string>
#include <tuple>
#include <utility>

namespace {

using namespace iotdbfs;

class MemoryStructuredBlobBackend final
  : public StructuredBlobStorageBackend {
public:
  std::shared_ptr<const StructuredBlobPayloadReader> pinned_payload_reader;
  mutable std::size_t bind_calls{0};
  bool reject_payload_binding{false};
  bool shared_payload{false};
  std::function<void()> on_load_manifest, on_bind_payload, on_read_payload;
  bool uses_shared_payload() const override { return shared_payload; }
  StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
  bind_published_payload(const StructuredBlobManifest&) const override {
    ++bind_calls;
    if (on_bind_payload) on_bind_payload();
    if (reject_payload_binding)
      return StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>::failure(
        StructuredBlobError::BackendFailure, "injected binding mismatch");
    return StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>::success(pinned_payload_reader);
  }
  struct Record {
    StructuredBlobManifest manifest;
    StructuredBlobBytes payload;
    StructuredBlobBytes layout;
    MetadataMap metadata;
  };

  StructuredBlobStatus create_manifest(
    const StructuredBlobManifest& manifest) override {
    if (should_fail("create")) return failure();
    if (records_.count(manifest.key) != 0 ||
        retired_.count(manifest.key) != 0) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::AlreadyExists, "already exists");
    }
    records_.emplace(manifest.key, Record{ manifest, {}, {}, {} });
    return StructuredBlobStatus::success();
  }

  StructuredBlobResult<StructuredBlobManifest> load_manifest(
    const StructuredBlobVersionKey& key) const override {
    ++manifest_load_count_;
    if (on_load_manifest) on_load_manifest();
    if (should_fail("load")) {
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::BackendFailure, "injected backend failure");
    }
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
    ++manifest_replace_count_;
    if (should_fail("replace")) return failure();
    auto found = records_.find(key);
    const auto checked = check_transition(found, expected_revision, next);
    if (!checked.ok()) return checked;
    found->second.manifest = next;
    if (should_fail("replace_ack")) return failure();
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes) override {
    ++payload_write_count_;
    payload_offsets_.push_back(offset);
    payload_inputs_.push_back(&bytes);
    if (should_fail("payload")) return failure();
    if (payload_failure_countdown_) {
      if (*payload_failure_countdown_ == 0) {
        payload_failure_countdown_.reset();
        return failure();
      }
      --*payload_failure_countdown_;
    }
    auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::NotFound, "not found");
    }
    auto& payload = found->second.payload;
    const auto end = static_cast<std::size_t>(offset) + bytes.size();
    if (payload.size() < end) payload.resize(end);
    std::copy(bytes.begin(), bytes.end(),
      payload.begin() + static_cast<std::size_t>(offset));
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_layout(
    const StructuredBlobVersionKey& key,
    const StructuredBlobBytes& layout) override {
    if (should_fail("layout")) return failure();
    auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::NotFound, "not found");
    }
    ++layout_write_count_;
    found->second.layout = layout;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& a) override {
    if (should_fail("catalog")) return failure();
    return catalogs.write(key,a);
  }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(const StructuredBlobVersionKey& key) const override {
    if (should_fail("read_catalog")) return StructuredBlobResult<DatasetCatalogArtifact>::failure(StructuredBlobError::BackendFailure,"injected catalog read failure");
    return catalogs.read(key);
  }
  iotdbfs::test::InMemoryDatasetCatalog catalogs;
  StructuredBlobStatus write_metadata(
    const StructuredBlobVersionKey& key,
    const MetadataMap& metadata) override {
    if (should_fail("metadata")) return failure();
    auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::NotFound, "not found");
    }
    found->second.metadata = metadata;
    return StructuredBlobStatus::success();
  }

  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const override {
    if (on_read_payload) on_read_payload();
    if (should_fail("read_payload")) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::BackendFailure, "injected backend failure");
    }
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
    ++layout_read_count_;
    if (should_fail("read_layout")) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::BackendFailure, "injected backend failure");
    }
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
    if (should_fail("erase")) return failure();
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::NotFound, "not found");
    }
    if (found->second.manifest.revision != expected_revision) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::InvalidState, "revision mismatch");
    }
    if (found->second.manifest.published)
      retired_.insert(key);
    catalogs.erase(key);
    records_.erase(found);
    return StructuredBlobStatus::success();
  }

  void fail_once(std::string operation) const {
    fail_operation_ = std::move(operation);
  }

  const Record* peek(const StructuredBlobVersionKey& key) const {
    const auto found = records_.find(key);
    return found == records_.end() ? nullptr : &found->second;
  }

  Record* peek(const StructuredBlobVersionKey& key) {
    const auto found = records_.find(key);
    return found == records_.end() ? nullptr : &found->second;
  }

  std::size_t layout_write_count() const { return layout_write_count_; }
  std::size_t layout_read_count() const { return layout_read_count_; }
  std::size_t manifest_load_count() const { return manifest_load_count_; }
  std::size_t manifest_replace_count() const { return manifest_replace_count_; }
  std::size_t payload_write_count() const { return payload_write_count_; }
  const auto& payload_offsets() const { return payload_offsets_; }
  const auto& payload_inputs() const { return payload_inputs_; }
  void fail_payload_after(std::size_t successful_chunks) {
    payload_failure_countdown_ = successful_chunks;
  }
  using StructuredBlobStorageBackend::plan_payload_write;

private:
  using Iterator = std::map<StructuredBlobVersionKey, Record>::iterator;

  StructuredBlobStatus check_transition(const Iterator& found,
    std::uint64_t expected_revision,
    const StructuredBlobManifest& next) const {
    if (found == records_.end()) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::NotFound, "not found");
    }
    if (found->second.manifest.revision != expected_revision ||
        next.revision != expected_revision + 1 ||
        !(next.key == found->second.manifest.key)) {
      return StructuredBlobStatus::failure(
        StructuredBlobError::InvalidState, "invalid transition");
    }
    return StructuredBlobStatus::success();
  }

  bool should_fail(const std::string& operation) const {
    if (fail_operation_ != operation) return false;
    fail_operation_.clear();
    return true;
  }

  static StructuredBlobStatus failure() {
    return StructuredBlobStatus::failure(
      StructuredBlobError::BackendFailure, "injected backend failure");
  }

  std::map<StructuredBlobVersionKey, Record> records_;
  std::set<StructuredBlobVersionKey> retired_;
  mutable std::string fail_operation_;
  std::size_t layout_write_count_{ 0 };
  mutable std::size_t layout_read_count_{ 0 };
  mutable std::size_t manifest_load_count_{0};
  std::size_t manifest_replace_count_{0}, payload_write_count_{0};
  std::optional<std::size_t> payload_failure_countdown_;
  std::vector<std::uint64_t> payload_offsets_;
  std::vector<const StructuredBlobBytes*> payload_inputs_;
};

StructuredBlobVersionKey key(std::int64_t version = 7) {
  return { "/science/run.arrow", version };
}

void expect_code(const StructuredBlobStatus& status,
  StructuredBlobError expected) {
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), expected);
}

void ingest_through_layout(StructuredBlobStorage& storage,
  const StructuredBlobVersionKey& version,
  const StructuredBlobBytes& payload = { 1, 2, 3 },
  const StructuredBlobBytes& layout = { 8, 9 }) {
  ASSERT_TRUE(storage.begin(version).ok());
  ASSERT_TRUE(storage.write_payload_range(version, 0, payload).ok());
  ASSERT_TRUE(storage.seal_payload(version).ok());
  ASSERT_TRUE(storage.write_layout(version, layout).ok());
}

} // namespace

TEST(StructuredBlobStorage, PublishesTypedManifestAndSeparateArtifacts) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key();

  ASSERT_TRUE(storage.begin(version).ok());
  ASSERT_TRUE(storage.write_payload_range(version, 0, { 1, 2, 3 }).ok());
  ASSERT_TRUE(storage.write_payload_range(version, 1, { 7, 8, 9 }).ok());
  ASSERT_TRUE(storage.seal_payload(version).ok());
  ASSERT_TRUE(storage.write_layout(version, { 20, 21 }).ok());
  MetadataMap metadata{
    { "variable", MetadataValue("temperature") },
    { "level", MetadataValue(std::int64_t{ 850 }) },
    { "forecast", MetadataValue(true) },
  };
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("test-format"), std::move(metadata)).ok());

  EXPECT_EQ(storage.load_manifest(version).status().code(),
    StructuredBlobError::NotFound);
  ASSERT_TRUE(storage.publish(version).ok());

  const auto manifest = storage.load_manifest(version);
  ASSERT_TRUE(manifest.ok()) << manifest.status().message();
  ASSERT_TRUE(manifest.value().format_id);
  EXPECT_EQ(*manifest.value().format_id, "test-format");
  EXPECT_EQ(manifest.value().payload_size, 4u);
  EXPECT_EQ(manifest.value().layout_size, 2u);
  EXPECT_EQ(manifest.value().phase, StructuredBlobPhase::Published);
  EXPECT_TRUE(manifest.value().published);
  ASSERT_EQ(manifest.value().metadata.size(), 3u);
  EXPECT_EQ(std::get<std::string>(
    manifest.value().metadata.at("variable").value()), "temperature");
  EXPECT_EQ(std::get<std::int64_t>(
    manifest.value().metadata.at("level").value()), 850);

  const auto layout = storage.load_layout(version);
  ASSERT_TRUE(layout.ok());
  EXPECT_EQ(layout.value(), (StructuredBlobBytes{ 20, 21 }));
  const auto layout_part = storage.read_layout_range(version, 1, 1);
  ASSERT_TRUE(layout_part.ok());
  EXPECT_EQ(layout_part.value(), (StructuredBlobBytes{ 21 }));
  const auto empty_layout_part = storage.read_layout_range(version, 2, 0);
  ASSERT_TRUE(empty_layout_part.ok());
  EXPECT_TRUE(empty_layout_part.value().empty());
  const auto payload = storage.read_payload_range(version, 1, 3);
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), (StructuredBlobBytes{ 7, 8, 9 }));
  const auto empty = storage.read_payload_range(version, 4, 0);
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty.value().empty());
}

TEST(StructuredBlobStorage, EnforcesLifecycleOrderAndPayloadCoverage) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key();
  ASSERT_TRUE(storage.begin(version).ok());

  expect_code(storage.write_layout(version, {}),
    StructuredBlobError::InvalidState);
  expect_code(storage.write_metadata_and_format_identity(version,
    std::string("grib"), {}), StructuredBlobError::InvalidState);
  expect_code(storage.publish(version), StructuredBlobError::InvalidState);
  expect_code(storage.write_payload_range(version, 1, { 1 }),
    StructuredBlobError::RangeError);

  ASSERT_TRUE(storage.write_payload_range(version, 0, { 1 }).ok());
  ASSERT_TRUE(storage.seal_payload(version).ok());
  expect_code(storage.write_payload_range(version, 1, { 2 }),
    StructuredBlobError::InvalidState);
  ASSERT_TRUE(storage.write_layout(version, {}).ok());
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("grib"), {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());

  expect_code(storage.publish(version), StructuredBlobError::InvalidState);
  expect_code(storage.abort(version), StructuredBlobError::InvalidState);
  expect_code(storage.seal_payload(version), StructuredBlobError::InvalidState);
}

TEST(StructuredBlobStorage, BytePassThroughHasNoSyntheticFormatState) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto rejected = key(1);
  ingest_through_layout(storage, rejected, { 1 }, { 9 });
  expect_code(storage.write_metadata_and_format_identity(
    rejected, std::nullopt, {}), StructuredBlobError::InvalidArgument);
  ASSERT_TRUE(storage.abort(rejected).ok());
  EXPECT_EQ(storage.load_manifest(rejected).status().code(),
    StructuredBlobError::NotFound);
  EXPECT_EQ(backend->peek(rejected), nullptr);

  const auto rejected_metadata = key(2);
  ingest_through_layout(storage, rejected_metadata, { 1 }, {});
  expect_code(storage.write_metadata_and_format_identity(rejected_metadata,
    std::nullopt, { { "invented", MetadataValue(true) } }),
    StructuredBlobError::InvalidArgument);

  const auto baseline = key(3);
  ingest_through_layout(storage, baseline, { 4, 5 }, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(
    baseline, std::nullopt, {}).ok());
  ASSERT_TRUE(storage.publish(baseline).ok());
  const auto manifest = storage.load_manifest(baseline);
  ASSERT_TRUE(manifest.ok());
  EXPECT_FALSE(manifest.value().format_id);
  EXPECT_TRUE(manifest.value().metadata.empty());
  EXPECT_EQ(manifest.value().layout_size, 0u);
  const auto layout = storage.load_layout(baseline);
  ASSERT_TRUE(layout.ok());
  EXPECT_TRUE(layout.value().empty());
  EXPECT_EQ(backend->layout_write_count(), 1u);
  EXPECT_EQ(backend->layout_read_count(), 0u);
}

TEST(StructuredBlobStorage, BackendFailureDoesNotAdvanceLifecycle) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key();
  ASSERT_TRUE(storage.begin(version).ok());

  backend->fail_once("payload");
  expect_code(storage.write_payload_range(version, 0, { 1, 2 }),
    StructuredBlobError::BackendFailure);
  ASSERT_NE(backend->peek(version), nullptr);
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 0u);
  EXPECT_TRUE(backend->peek(version)->payload.empty());

  ASSERT_TRUE(storage.write_payload_range(version, 0, { 1, 2 }).ok());
  ASSERT_TRUE(storage.seal_payload(version).ok());
  backend->fail_once("layout");
  expect_code(storage.write_layout(version, { 3 }),
    StructuredBlobError::BackendFailure);
  EXPECT_EQ(backend->peek(version)->manifest.phase,
    StructuredBlobPhase::PayloadSealed);
  EXPECT_TRUE(backend->peek(version)->layout.empty());

  ASSERT_TRUE(storage.write_layout(version, { 3 }).ok());
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("test-format"), {}).ok());
  backend->fail_once("replace");
  expect_code(storage.publish(version), StructuredBlobError::BackendFailure);
  EXPECT_FALSE(backend->peek(version)->manifest.published);
  EXPECT_EQ(storage.load_manifest(version).status().code(),
    StructuredBlobError::NotFound);
  ASSERT_TRUE(storage.publish(version).ok());
  EXPECT_TRUE(storage.load_manifest(version).ok());
}

TEST(StructuredBlobStorage, ManifestCasFailureLeavesOnlyInvisibleRetryableArtifact) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key();
  ASSERT_TRUE(storage.begin(version).ok());

  backend->fail_once("replace");
  expect_code(storage.write_payload_range(version, 0, { 4, 5 }),
    StructuredBlobError::BackendFailure);
  ASSERT_NE(backend->peek(version), nullptr);
  EXPECT_EQ(backend->peek(version)->payload,
    (StructuredBlobBytes{ 4, 5 }));
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 0u);
  EXPECT_EQ(backend->peek(version)->manifest.phase,
    StructuredBlobPhase::PayloadOpen);
  EXPECT_EQ(storage.load_manifest(version).status().code(),
    StructuredBlobError::NotFound);
  expect_code(storage.publish(version), StructuredBlobError::InvalidState);

  ASSERT_TRUE(storage.write_payload_range(version, 0, { 4, 5 }).ok());
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 2u);
  ASSERT_TRUE(storage.seal_payload(version).ok());
  backend->fail_once("replace");
  expect_code(storage.write_layout(version, { 8, 9 }),
    StructuredBlobError::BackendFailure);
  EXPECT_EQ(backend->peek(version)->layout,
    (StructuredBlobBytes{ 8, 9 }));
  EXPECT_EQ(backend->peek(version)->manifest.phase,
    StructuredBlobPhase::PayloadSealed);
  EXPECT_EQ(backend->peek(version)->manifest.layout_size, 0u);

  ASSERT_TRUE(storage.write_layout(version, { 8, 9 }).ok());
  MetadataMap metadata{
    { "variable", MetadataValue("temperature") },
  };
  backend->fail_once("replace");
  expect_code(storage.write_metadata_and_format_identity(version,
    std::string("test-format"), metadata), StructuredBlobError::BackendFailure);
  ASSERT_EQ(backend->peek(version)->metadata.size(), 1u);
  EXPECT_TRUE(backend->peek(version)->manifest.metadata.empty());
  EXPECT_FALSE(backend->peek(version)->manifest.format_id);
  EXPECT_EQ(backend->peek(version)->manifest.phase,
    StructuredBlobPhase::LayoutStored);
  expect_code(storage.publish(version), StructuredBlobError::InvalidState);

  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("test-format"), std::move(metadata)).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  EXPECT_TRUE(storage.load_manifest(version).ok());
}

TEST(StructuredBlobStorage, RejectsInvalidReadRangesAndPropagatesReadFailure) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key();
  ingest_through_layout(storage, version, { 1, 2, 3 }, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("test-format"), {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());

  EXPECT_EQ(storage.read_payload_range(version, 4, 0).status().code(),
    StructuredBlobError::RangeError);
  EXPECT_EQ(storage.read_payload_range(version, 2, 2).status().code(),
    StructuredBlobError::RangeError);
  EXPECT_EQ(storage.read_layout_range(version, 3, 0).status().code(),
    StructuredBlobError::RangeError);
  EXPECT_EQ(storage.read_layout_range(version, 1, 2).status().code(),
    StructuredBlobError::RangeError);
  backend->fail_once("read_payload");
  EXPECT_EQ(storage.read_payload_range(version, 0, 1).status().code(),
    StructuredBlobError::BackendFailure);
}

TEST(StructuredBlobStorage, RejectsDuplicateOrInvalidVersionKeys) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key();
  ASSERT_TRUE(storage.begin(version).ok());
  expect_code(storage.begin(version), StructuredBlobError::AlreadyExists);
  expect_code(storage.begin({ "", 1 }),
    StructuredBlobError::InvalidArgument);
  expect_code(storage.begin({ "/data", -1 }),
    StructuredBlobError::InvalidArgument);
}

TEST(StructuredBlobStorage, RemoveDeletesPublishedVersionAndAllArtifacts) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key();
  ingest_through_layout(storage, version, { 1, 2 }, { 3, 4 });
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("test-format"), {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  ASSERT_NE(backend->peek(version), nullptr);

  ASSERT_TRUE(storage.remove(version).ok());
  EXPECT_EQ(backend->peek(version), nullptr);
  EXPECT_EQ(storage.load_manifest(version).status().code(),
    StructuredBlobError::NotFound);
  EXPECT_EQ(storage.load_layout(version).status().code(),
    StructuredBlobError::NotFound);
  EXPECT_EQ(storage.read_payload_range(version, 0, 0).status().code(),
    StructuredBlobError::NotFound);
}

TEST(StructuredBlobStorage, PublishedVersionIdentityCannotBeReused) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(71);
  ingest_through_layout(storage, version, { 1, 2 }, { 3, 4 });
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("test-format"), {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  ASSERT_TRUE(storage.remove(version).ok());

  expect_code(storage.begin(version), StructuredBlobError::AlreadyExists);
}

TEST(StructuredBlobStorage, IdenticalVersionsKeepIndependentDeletionAndReadIdentity) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto old = key(71);
  const auto current = key(72);
  for (const auto& version : {old, current}) {
    ingest_through_layout(storage, version, {1, 2, 3}, {8, 9});
    ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
      std::string("test-format"), {}).ok());
    ASSERT_TRUE(storage.publish(version).ok());
  }
  const auto old_manifest = storage.load_manifest(old);
  ASSERT_TRUE(old_manifest.ok());
  auto old_reader = storage.open_catalog_version(old, old_manifest.value(),
    storage.storage_namespace());
  ASSERT_TRUE(old_reader.ok());
  ASSERT_TRUE(storage.remove(old).ok());
  EXPECT_EQ(old_reader.value().read_payload_range(0, 3).status().code(),
    StructuredBlobError::NotFound);
  EXPECT_EQ(old_reader.value().manifest().key.version, 71);
  expect_code(storage.begin(old), StructuredBlobError::AlreadyExists);

  const auto surviving = storage.load_manifest(current);
  ASSERT_TRUE(surviving.ok());
  EXPECT_EQ(surviving.value().key.version, 72);
  const auto payload = storage.read_payload_range(current, 0, 3);
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), (StructuredBlobBytes{1, 2, 3}));
  const auto layout = storage.load_layout(current);
  ASSERT_TRUE(layout.ok());
  EXPECT_EQ(layout.value(), (StructuredBlobBytes{8, 9}));
}

TEST(StructuredBlobStorage, AbortedUnpublishedVersionIdentityCanBeRetried) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(72);
  ASSERT_TRUE(storage.begin(version).ok());
  ASSERT_TRUE(storage.abort(version).ok());

  EXPECT_TRUE(storage.begin(version).ok());
}

TEST(StructuredBlobStorage, DatasetCatalogPublicationAndAbortAreGated) {
  auto backend=std::make_shared<MemoryStructuredBlobBackend>();StructuredBlobStorage storage(backend);
  DatasetCatalogArtifact a; a.format_id="hdf5";a.layout_protocol="/4";a.layout_digest=std::string(64,'a');
  auto k=key(901);ingest_through_layout(storage,k,{1},{2});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(k,std::string("hdf5"),{},a).ok());
  EXPECT_EQ(storage.load_dataset_catalog(k).status().code(),StructuredBlobError::NotFound);
  ASSERT_TRUE(storage.publish(k).ok());ASSERT_TRUE(storage.load_dataset_catalog(k).ok());
  EXPECT_FALSE(storage.write_metadata_and_format_identity(k,std::string("hdf5"),{},a).ok());
  ASSERT_TRUE(storage.remove(k).ok());EXPECT_FALSE(backend->catalogs.read(k).ok());EXPECT_FALSE(storage.begin(k).ok());
}
TEST(StructuredBlobStorage, DatasetCatalogFailuresNeverPublish) {
  auto backend=std::make_shared<MemoryStructuredBlobBackend>();StructuredBlobStorage storage(backend);
  DatasetCatalogArtifact a;a.format_id="hdf5";a.layout_protocol="/4";a.layout_digest=std::string(64,'a');
  auto k=key(902);ingest_through_layout(storage,k,{1},{2});backend->fail_once("catalog");
  EXPECT_FALSE(storage.write_metadata_and_format_identity(k,std::string("hdf5"),{},a).ok());EXPECT_FALSE(storage.publish(k).ok());
  ASSERT_TRUE(storage.write_metadata_and_format_identity(k,std::string("hdf5"),{},a).ok());backend->catalogs.erase(k);
  EXPECT_FALSE(storage.publish(k).ok());ASSERT_TRUE(storage.abort(k).ok());EXPECT_FALSE(storage.load_dataset_catalog(k).ok());
}
TEST(StructuredBlobStorage, DatasetCatalogMarkerCannotBeCopiedWithoutArtifact) {
  auto backend=std::make_shared<MemoryStructuredBlobBackend>();StructuredBlobStorage storage(backend);
  auto k=key(903);ingest_through_layout(storage,k,{1},{2});
  EXPECT_FALSE(storage.write_metadata_and_format_identity(k,std::string("hdf5"),{{"__iotdbfs.dataset_catalog.protocol",MetadataValue("iotdbfs-dataset-catalog/1")}}).ok());
}

TEST(StructuredBlobStorage, DatasetSubstructureSealIsDurableBeforePublication) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(911);
  ingest_through_layout(storage, version, {1, 2, 3}, {8, 9});
  DatasetCatalogArtifact a;
  a.format_id = "hdf5";
  a.layout_protocol = "iotdbfs-hdf5-layout/5";
  a.layout_digest = std::string(64, 'a');
  DatasetCatalogRecord record;
  record.object_id = "dataset-1";
  record.canonical_path = "/temperature";
  record.attributes.push_back({"units", DatasetAttributeState::Complete,
      DatasetAttributeValue(std::string("K")), {}, {}});
  a.records.push_back(record);
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
      std::string("hdf5"), {{"note", MetadataValue("retained")}}, a).ok());
  const auto* durable = backend->peek(version);
  ASSERT_NE(durable, nullptr);
  EXPECT_EQ(durable->manifest.phase, StructuredBlobPhase::Described);
  EXPECT_FALSE(durable->manifest.published);
  EXPECT_TRUE(read_dataset_substructure_seal(durable->manifest).ok());
  EXPECT_TRUE(validate_dataset_substructure_index(durable->manifest, a).ok());
  EXPECT_EQ(durable->metadata.count("__iotdbfs.dataset_index.sha256"), 1u);
  EXPECT_FALSE(storage.load_manifest(version).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  const auto visible = storage.load_manifest(version);
  ASSERT_TRUE(visible.ok());
  EXPECT_TRUE(validate_dataset_substructure_index(visible.value(), a).ok());
  EXPECT_EQ(std::get<std::string>(visible.value().metadata.at("note").value()),
            "retained");
}

TEST(StructuredBlobStorage, SameCountCatalogCorruptionPreventsPublication) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(912);
  ingest_through_layout(storage, version);
  DatasetCatalogArtifact a;
  a.format_id = "hdf5";
  a.layout_protocol = "iotdbfs-hdf5-layout/5";
  a.layout_digest = std::string(64, 'a');
  DatasetCatalogRecord r;
  r.object_id = "dataset-1";
  r.canonical_path = "/temperature";
  r.attributes.push_back({"units", DatasetAttributeState::Complete,
      DatasetAttributeValue(std::string("K")), {}, {}});
  a.records.push_back(r);
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
      std::string("hdf5"), {}, a).ok());
  auto corrupt = a;
  corrupt.records[0].attributes[0].value = std::string("C");
  ASSERT_TRUE(backend->catalogs.write(version, corrupt).ok());
  EXPECT_FALSE(storage.publish(version).ok());
  EXPECT_FALSE(backend->peek(version)->manifest.published);
  EXPECT_FALSE(storage.load_manifest(version).ok());
  // Repairing the exact artifact makes the original publication retry safe.
  ASSERT_TRUE(backend->catalogs.write(version, a).ok());
  EXPECT_TRUE(storage.publish(version).ok());
}

TEST(StructuredBlobStorage, MissingSealAndCatalogReadFailureNeverPublish) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(913);
  ingest_through_layout(storage, version);
  DatasetCatalogArtifact a;
  a.format_id = "hdf5";
  a.layout_protocol = "iotdbfs-hdf5-layout/5";
  a.layout_digest = std::string(64, 'a');
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
      std::string("hdf5"), {}, a).ok());
  backend->fail_once("read_catalog");
  EXPECT_FALSE(storage.publish(version).ok());
  EXPECT_FALSE(backend->peek(version)->manifest.published);
  backend->peek(version)->manifest.metadata.erase(
      "__iotdbfs.dataset_index.sha256");
  EXPECT_FALSE(storage.publish(version).ok());
  EXPECT_FALSE(backend->peek(version)->manifest.published);
  EXPECT_FALSE(storage.load_dataset_catalog(version).ok());
}

TEST(StructuredBlobStorage, CallerCannotInjectSealWithoutCatalog) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(914);
  ingest_through_layout(storage, version);
  EXPECT_FALSE(storage.write_metadata_and_format_identity(version,
      std::string("hdf5"), {{"__iotdbfs.dataset_index.protocol",
      MetadataValue("iotdbfs-dataset-substructure-index/1")}}).ok());
  EXPECT_FALSE(storage.publish(version).ok());
}

TEST(StructuredBlobStorage, OrphanedSealCannotPublishWithoutCatalogMarker) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(915);
  ingest_through_layout(storage, version);
  DatasetCatalogArtifact artifact;
  artifact.format_id = "hdf5";
  artifact.layout_protocol = "iotdbfs-hdf5-layout/5";
  artifact.layout_digest = std::string(64, 'a');
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
      std::string("hdf5"), {}, artifact).ok());
  auto& metadata = backend->peek(version)->manifest.metadata;
  metadata.erase("__iotdbfs.dataset_catalog.protocol");
  metadata.erase("__iotdbfs.dataset_catalog.layout_digest");
  EXPECT_EQ(storage.publish(version).code(), StructuredBlobError::InvalidState);
  EXPECT_FALSE(backend->peek(version)->manifest.published);
}

TEST(StructuredBlobStorage, FreshCatalogManifestOpensWithoutReloadAndChecksIdentity) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(904);
  ingest_through_layout(storage, version, {1, 2, 3}, {8, 9});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version,
    std::string("hdf5"), {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  auto visible = storage.load_manifest(version);
  ASSERT_TRUE(visible.ok());
  backend->fail_once("load");
  auto reader = storage.open_catalog_version(version, visible.value(),
    storage.storage_namespace());
  ASSERT_TRUE(reader.ok()) << reader.status().message();
  EXPECT_EQ(reader.value().manifest().revision, visible.value().revision);
  EXPECT_EQ(reader.value().storage_namespace(), storage.storage_namespace());
  auto payload = reader.value().read_payload_range(1, 2);
  ASSERT_TRUE(payload.ok());
  EXPECT_EQ(payload.value(), (StructuredBlobBytes{2, 3}));
  // The injected load failure remains pending: opening used no backend load.
  EXPECT_FALSE(storage.load_manifest(version).ok());

  EXPECT_FALSE(storage.open_catalog_version(key(905), visible.value(),
    storage.storage_namespace()).ok());
  EXPECT_FALSE(storage.open_catalog_version(version, visible.value(), 0).ok());
  auto foreign = std::make_shared<MemoryStructuredBlobBackend>();
  EXPECT_FALSE(storage.open_catalog_version(version, visible.value(),
    foreign->storage_namespace()).ok());
  auto unpublished = visible.value();
  unpublished.published = false;
  EXPECT_FALSE(storage.open_catalog_version(version, unpublished,
    storage.storage_namespace()).ok());
  auto malformed = visible.value();
  malformed.format_id.reset();
  EXPECT_FALSE(storage.open_catalog_version(version, malformed,
    storage.storage_namespace()).ok());
}

TEST(StructuredBlobStorage, CatalogSkeletonReloadsExactVersionAndMaterializesMetadata) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(920);
  ingest_through_layout(storage, version);
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, std::string("hdf5"),
    {{"station", MetadataValue("A")}}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  auto full = storage.load_manifest(version);
  ASSERT_TRUE(full.ok());
  auto skeleton = full.value();
  skeleton.metadata.clear(); skeleton.metadata_materialized = false;
  const auto loads = backend->manifest_load_count();
  auto reader = storage.open_catalog_version(version, skeleton, storage.storage_namespace());
  ASSERT_TRUE(reader.ok()) << reader.status().message();
  EXPECT_EQ(backend->manifest_load_count(), loads + 1);
  EXPECT_TRUE(reader.value().manifest().metadata_materialized);
  EXPECT_EQ(std::get<std::string>(reader.value().manifest().metadata.at("station").value()), "A");
  EXPECT_EQ(reader.value().manifest().key, version);
  EXPECT_EQ(reader.value().manifest().revision, full.value().revision);
  backend->fail_once("load");
  EXPECT_FALSE(storage.open_catalog_version(version, skeleton, storage.storage_namespace()).ok());
}

TEST(StructuredBlobStorage, CatalogSkeletonRejectsChangedExactFormatSizesRevisionOrUnmaterializedReload) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(921);
  ingest_through_layout(storage, version);
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, std::string("hdf5"), {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  auto full = storage.load_manifest(version);
  ASSERT_TRUE(full.ok());
  for (int field = 0; field != 5; ++field) {
    SCOPED_TRACE(field);
    auto skeleton = full.value();
    skeleton.metadata.clear(); skeleton.metadata_materialized = false;
    if (field == 0) skeleton.format_id = "other-format";
    if (field == 1) ++skeleton.payload_size;
    if (field == 2) ++skeleton.layout_size;
    if (field == 3) ++skeleton.revision;
    if (field == 4) backend->peek(version)->manifest.metadata_materialized = false;
    const auto loads = backend->manifest_load_count();
    auto reader = storage.open_catalog_version(version, skeleton, storage.storage_namespace());
    EXPECT_FALSE(reader.ok());
    EXPECT_EQ(reader.status().code(), StructuredBlobError::BackendFailure);
    EXPECT_EQ(backend->manifest_load_count(), loads + 1);
  }
}

TEST(StructuredBlobStorage, PayloadBatchBorrowsChunksAndCommitsOneFrontier) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(910);
  ASSERT_TRUE(storage.begin(version).ok());
  std::vector<StructuredBlobBytes> chunks{{1, 2}, {}, {3, 4, 5}};
  ASSERT_TRUE(storage.write_payload_batch(version, 0, chunks).ok());
  EXPECT_EQ(backend->manifest_load_count(), 1);
  EXPECT_EQ(backend->manifest_replace_count(), 1);
  EXPECT_EQ(backend->payload_write_count(), 2);
  EXPECT_EQ(backend->payload_offsets(), (std::vector<std::uint64_t>{0, 2}));
  EXPECT_EQ(backend->payload_inputs()[0], &chunks[0]);
  EXPECT_EQ(backend->payload_inputs()[1], &chunks[2]);
  EXPECT_EQ(backend->peek(version)->payload, (StructuredBlobBytes{1, 2, 3, 4, 5}));
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 5);
  EXPECT_EQ(backend->peek(version)->manifest.revision, 1);
  ASSERT_TRUE(storage.write_payload_batch(version, 1, {{8}, {9}}).ok());
  EXPECT_EQ(backend->peek(version)->payload, (StructuredBlobBytes{1, 8, 9, 4, 5}));
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 5);
  EXPECT_EQ(backend->peek(version)->manifest.revision, 2);
}

TEST(StructuredBlobStorage, PayloadBatchPartialFailureKeepsFrontierAndRetriesSameOffsets) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(911);
  ASSERT_TRUE(storage.begin(version).ok());
  std::vector<StructuredBlobBytes> chunks{{1, 2}, {3, 4}, {5}};
  backend->fail_payload_after(1);
  expect_code(storage.write_payload_batch(version, 0, chunks), StructuredBlobError::BackendFailure);
  EXPECT_EQ(backend->payload_write_count(), 2);
  EXPECT_EQ(backend->manifest_replace_count(), 0);
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 0);
  EXPECT_EQ(backend->peek(version)->manifest.revision, 0);
  EXPECT_EQ(backend->peek(version)->payload, (StructuredBlobBytes{1, 2}));
  EXPECT_FALSE(storage.load_manifest(version).ok());
  backend->fail_once("load");
  expect_code(storage.write_payload_batch(version, 0, chunks), StructuredBlobError::BackendFailure);
  EXPECT_EQ(backend->payload_write_count(), 2);
  ASSERT_TRUE(storage.write_payload_batch(version, 0, chunks).ok());
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 5);
  EXPECT_EQ(backend->peek(version)->manifest.revision, 1);
  EXPECT_EQ(backend->peek(version)->payload, (StructuredBlobBytes{1, 2, 3, 4, 5}));
}

TEST(StructuredBlobStorage, PayloadBatchCasFailureIsRetryableAndAbortClearsArtifacts) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(912);
  ASSERT_TRUE(storage.begin(version).ok());
  backend->fail_once("replace");
  expect_code(storage.write_payload_batch(version, 0, {{1}, {2}}), StructuredBlobError::BackendFailure);
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 0);
  EXPECT_EQ(backend->peek(version)->manifest.revision, 0);
  EXPECT_EQ(backend->peek(version)->payload, (StructuredBlobBytes{1, 2}));
  ASSERT_TRUE(storage.write_payload_batch(version, 0, {{1}, {2}}).ok());
  EXPECT_EQ(backend->peek(version)->manifest.revision, 1);
  ASSERT_TRUE(storage.abort(version).ok());
  EXPECT_EQ(backend->peek(version), nullptr);
  ASSERT_TRUE(storage.begin(version).ok());
  EXPECT_TRUE(backend->peek(version)->payload.empty());
}

TEST(StructuredBlobStorage, PayloadBatchLostCommitAcknowledgementRetriesFreshFrontier) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(917);
  ASSERT_TRUE(storage.begin(version).ok());
  backend->fail_once("replace_ack");
  expect_code(storage.write_payload_batch(version, 0, {{1}, {2}}), StructuredBlobError::BackendFailure);
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 2);
  EXPECT_EQ(backend->peek(version)->manifest.revision, 1);
  EXPECT_FALSE(storage.load_manifest(version).ok());
  ASSERT_TRUE(storage.write_payload_batch(version, 0, {{1}, {2}}).ok());
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, 2);
  EXPECT_EQ(backend->peek(version)->manifest.revision, 2);
  EXPECT_EQ(backend->peek(version)->payload, (StructuredBlobBytes{1, 2}));
}

TEST(StructuredBlobStorage, PayloadBatchBoundsAdmitExactlyEightMiBAndSixtyFourChunks) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(913);
  ASSERT_TRUE(storage.begin(version).ok());
  std::vector<StructuredBlobBytes> chunks(8, StructuredBlobBytes(1024 * 1024, 7));
  ASSERT_TRUE(storage.write_payload_batch(version, 0, chunks).ok());
  EXPECT_EQ(backend->payload_write_count(), 8);
  EXPECT_EQ(backend->manifest_replace_count(), 1);
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, kMaxPayloadWriteBytesPerBatch);
  const auto loads = backend->manifest_load_count();
  chunks.back().push_back(1);
  expect_code(storage.write_payload_batch(version, 0, chunks), StructuredBlobError::RangeError);
  expect_code(storage.write_payload_batch(version, 0, std::vector<StructuredBlobBytes>(65, {1})),
              StructuredBlobError::RangeError);
  EXPECT_EQ(backend->manifest_load_count(), loads);
  EXPECT_EQ(backend->payload_write_count(), 8);
  ASSERT_TRUE(storage.write_payload_batch(version, kMaxPayloadWriteBytesPerBatch,
    std::vector<StructuredBlobBytes>(64, {9})).ok());
  EXPECT_EQ(backend->peek(version)->manifest.revision, 2);
}

TEST(StructuredBlobStorage, PayloadScalarAndEmptyBatchKeepExistingBehaviorWithoutCopy) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(914);
  ASSERT_TRUE(storage.begin(version).ok());
  StructuredBlobBytes bytes{1, 2};
  ASSERT_TRUE(storage.write_payload_range(version, 0, bytes).ok());
  EXPECT_EQ(backend->manifest_load_count(), 1);
  EXPECT_EQ(backend->payload_inputs()[0], &bytes);
  ASSERT_TRUE(storage.write_payload_range(version, 2, {}).ok());
  ASSERT_TRUE(storage.write_payload_batch(version, 2, {}).ok());
  ASSERT_TRUE(storage.write_payload_batch(version, 2, {{}, {}}).ok());
  EXPECT_EQ(backend->peek(version)->manifest.revision, 1);
  EXPECT_EQ(backend->manifest_replace_count(), 1);
  EXPECT_EQ(backend->payload_write_count(), 1);
  expect_code(storage.write_payload_batch(version, 3, {}), StructuredBlobError::RangeError);
  ASSERT_TRUE(storage.seal_payload(version).ok());
  expect_code(storage.write_payload_batch(version, 2, {}), StructuredBlobError::InvalidState);
}

TEST(StructuredBlobStorage, PayloadPlanningRejectsWrongKeyOverflowAndExhaustedRevision) {
  auto current = StructuredBlobManifest{};
  current.key = key(915);
  StructuredBlobBytes one{1};
  const StructuredBlobPayloadWriteChunks chunks{std::cref(one)};
  EXPECT_EQ(MemoryStructuredBlobBackend::plan_payload_write(current, key(916), 0, chunks)
    .status().code(), StructuredBlobError::BackendFailure);
  current.payload_size = std::numeric_limits<std::uint64_t>::max();
  EXPECT_EQ(MemoryStructuredBlobBackend::plan_payload_write(current, current.key,
    current.payload_size, chunks).status().code(), StructuredBlobError::RangeError);
  current.revision = std::numeric_limits<std::uint64_t>::max();
  EXPECT_EQ(MemoryStructuredBlobBackend::plan_payload_write(current, current.key,
    0, chunks).status().code(), StructuredBlobError::InvalidState);
  EXPECT_TRUE(MemoryStructuredBlobBackend::plan_payload_write(current, current.key, 0, {})
    .ok());
  current.published = true;
  EXPECT_EQ(MemoryStructuredBlobBackend::plan_payload_write(current, current.key,
    0, chunks).status().code(), StructuredBlobError::InvalidState);
}

TEST(StructuredBlobStorage, ScalarPayloadKeepsItsExistingUnboundedTransferContract) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(918);
  ASSERT_TRUE(storage.begin(version).ok());
  const StructuredBlobBytes bytes(kMaxPayloadWriteBytesPerBatch + 1, 4);
  ASSERT_TRUE(storage.write_payload_range(version, 0, bytes).ok());
  EXPECT_EQ(backend->payload_inputs()[0], &bytes);
  EXPECT_EQ(backend->payload_write_count(), 1);
  EXPECT_EQ(backend->peek(version)->manifest.payload_size, bytes.size());
  EXPECT_EQ(backend->peek(version)->manifest.revision, 1);
}

namespace {
class TestPinnedPayloadReader final : public StructuredBlobPayloadReader {
public:
  StructuredBlobBytes payload, layout;
  mutable std::vector<StructuredBlobPayloadRanges> batches;
  bool short_read{false};
  std::function<void()> on_read;
  std::function<StructuredBlobResult<StructuredBlobPayloadBatch>(
    const StructuredBlobPayloadRanges&)> read_override;
  mutable const std::uint8_t* last_output_data{nullptr};
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobPayloadRanges& ranges) const override {
    if (on_read) on_read();
    batches.push_back(ranges);
    if (read_override) return read_override(ranges);
    StructuredBlobPayloadBatch result;
    for (const auto& range : ranges)
      result.emplace_back(payload.begin() + range.offset, payload.begin() + range.offset + range.length);
    if (short_read && !result.empty() && !result[0].empty()) result[0].pop_back();
    last_output_data = result.empty() ? nullptr : result[0].data();
    return StructuredBlobResult<StructuredBlobPayloadBatch>::success(std::move(result));
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(std::uint64_t offset,
      std::uint64_t length) const override {
    return StructuredBlobResult<StructuredBlobBytes>::success(
      StructuredBlobBytes(layout.begin() + offset, layout.begin() + offset + length));
  }
  StructuredBlobResult<std::optional<std::string>> verified_layout_identity() const override {
    return StructuredBlobResult<std::optional<std::string>>::success("verified/pinned/layout");
  }
};
} // namespace

TEST(StructuredBlobStorage, BoundPayloadHandleRetainsExactRootAfterDeletionButNewOpenIsRejected) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(919);
  ingest_through_layout(storage, version, {1, 2, 3}, {8, 9});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, std::string("fake"), {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  auto cap = std::make_shared<TestPinnedPayloadReader>();
  cap->payload = {1, 2, 3}; cap->layout = {8, 9}; backend->pinned_payload_reader = cap;
  auto opened = storage.open_published_version(version);
  ASSERT_TRUE(opened.ok());
  ASSERT_EQ(backend->bind_calls, 1u);
  ASSERT_TRUE(storage.remove(version).ok());
  const auto after_delete = backend->manifest_load_count();
  EXPECT_EQ(opened.value().read_payload_range(1, 2).value(), (StructuredBlobBytes{2, 3}));
  EXPECT_EQ(opened.value().read_payload_ranges({{2, 1}, {0, 2}}).value(),
    (StructuredBlobPayloadBatch{{3}, {1, 2}}));
  EXPECT_EQ(opened.value().read_layout_range(0, 2).value(), (StructuredBlobBytes{8, 9}));
  EXPECT_EQ(opened.value().verified_layout_identity().value(), std::optional<std::string>("verified/pinned/layout"));
  EXPECT_EQ(backend->manifest_load_count(), after_delete);
  EXPECT_EQ(backend->bind_calls, 1u);
  EXPECT_EQ(storage.open_published_version(version).status().code(), StructuredBlobError::NotFound);
  EXPECT_EQ(backend->bind_calls, 1u);
}

TEST(StructuredBlobStorage, BoundScalarSplitsBatchesAndRejectsShortCapOutputAndBindingFailure) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(920);
  const StructuredBlobBytes data(kMaxPayloadBytesPerBatch + 1, 7);
  ingest_through_layout(storage, version, data, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  auto cap = std::make_shared<TestPinnedPayloadReader>(); cap->payload = data;
  backend->pinned_payload_reader = cap;
  backend->reject_payload_binding = true;
  EXPECT_EQ(storage.open_published_version(version).status().code(), StructuredBlobError::BackendFailure);
  backend->reject_payload_binding = false;
  auto opened = storage.open_published_version(version);
  ASSERT_TRUE(opened.ok());
  auto result = opened.value().read_payload_range(0, data.size());
  ASSERT_TRUE(result.ok()); EXPECT_EQ(result.value(), data);
  ASSERT_EQ(cap->batches.size(), 2u);
  EXPECT_EQ(cap->batches[0][0].length, kMaxPayloadBytesPerBatch);
  EXPECT_EQ(cap->batches[1][0].offset, kMaxPayloadBytesPerBatch);
  EXPECT_EQ(cap->batches[1][0].length, 1u);
  EXPECT_TRUE(opened.value().read_payload_range(data.size(), 0).value().empty());
  EXPECT_EQ(cap->batches.size(), 2u);
  cap->short_read = true;
  EXPECT_EQ(opened.value().read_payload_range(0, 1).status().code(), StructuredBlobError::BackendFailure);
  EXPECT_EQ(opened.value().read_payload_ranges({{0, 1}}).status().code(), StructuredBlobError::BackendFailure);
}

TEST(StructuredBlobStorage, BoundScalarSingleBatchTransfersBufferAtTheBatchBoundary) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(924);
  auto cap = std::make_shared<TestPinnedPayloadReader>();
  cap->payload.resize(kMaxPayloadBytesPerBatch + 1);
  for (std::size_t i = 0; i < cap->payload.size(); ++i)
    cap->payload[i] = static_cast<std::uint8_t>(i % 251);
  ingest_through_layout(storage, version, cap->payload, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  backend->pinned_payload_reader = cap;
  auto opened = storage.open_published_version(version);
  ASSERT_TRUE(opened.ok());

  for (const auto length : {std::uint64_t{1}, std::uint64_t{37},
      kMaxPayloadBytesPerBatch}) {
    auto result = opened.value().read_payload_range(1, length);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.value().data(), cap->last_output_data);
    EXPECT_EQ(result.value(), (StructuredBlobBytes(cap->payload.begin() + 1,
      cap->payload.begin() + 1 + length)));
    ASSERT_EQ(cap->batches.back().size(), 1u);
    EXPECT_EQ(cap->batches.back()[0].offset, 1u);
    EXPECT_EQ(cap->batches.back()[0].length, length);
  }
  const auto reads = cap->batches.size();
  auto empty = opened.value().read_payload_range(cap->payload.size(), 0);
  ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty.value().empty());
  EXPECT_EQ(cap->batches.size(), reads);
}

TEST(StructuredBlobStorage, BoundScalarSingleBatchValidatesShapeAndPreservesReaderError) {
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(925);
  ingest_through_layout(storage, version, {1, 2, 3}, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  auto cap = std::make_shared<TestPinnedPayloadReader>();
  backend->pinned_payload_reader = cap;
  auto opened = storage.open_published_version(version);
  ASSERT_TRUE(opened.ok());

  for (const StructuredBlobPayloadBatch malformed : {
      StructuredBlobPayloadBatch{}, StructuredBlobPayloadBatch{{1}, {2}},
      StructuredBlobPayloadBatch{{}}, StructuredBlobPayloadBatch{{1, 2}}}) {
    cap->read_override = [malformed](const StructuredBlobPayloadRanges&) {
      return StructuredBlobResult<StructuredBlobPayloadBatch>::success(malformed);
    };
    auto result = opened.value().read_payload_range(0, 1);
    EXPECT_EQ(result.status().code(), StructuredBlobError::BackendFailure);
    EXPECT_EQ(result.status().message(),
      "pinned payload reader returned an unexpected byte count");
  }
  cap->read_override = [](const StructuredBlobPayloadRanges&) {
    return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
      StructuredBlobError::NotFound, "injected exact-root read failure");
  };
  auto failed = opened.value().read_payload_range(0, 1);
  EXPECT_EQ(failed.status().code(), StructuredBlobError::NotFound);
  EXPECT_EQ(failed.status().message(), "injected exact-root read failure");
}

TEST(StructuredBlobStorage, BoundScalarMultiBatchKeepsOneLedgerAndPropagatesCancellation) {
  constexpr std::uint64_t mib = 1024 * 1024;
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(926);
  auto cap = std::make_shared<TestPinnedPayloadReader>();
  cap->payload.assign(kMaxPayloadBytesPerBatch + 1, 9);
  ingest_through_layout(storage, version, cap->payload, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  backend->pinned_payload_reader = cap;
  auto opened = storage.open_published_version(version);
  ASSERT_TRUE(opened.ok());

  unsigned reads = 0;
  std::weak_ptr<QueryMetadataBudget> ledger;
  cap->on_read = [&] {
    ++reads;
    auto current = current_query_metadata_budget();
    ASSERT_TRUE(current);
    if (reads == 1) ledger = current;
    EXPECT_EQ(current, ledger.lock());
    require_query_metadata("scalar/slice/" + std::to_string(reads), 33 * mib);
  };
  EXPECT_THROW(opened.value().read_payload_range(0, cap->payload.size()),
    QueryMetadataBudgetExceeded);
  EXPECT_EQ(reads, 2u);
  EXPECT_TRUE(ledger.expired());
  EXPECT_FALSE(current_query_metadata_budget());

  reads = 0;
  cap->batches.clear();
  cap->on_read = [&] {
    ++reads;
    storage_read_checkpoint();
  };
  ScopedQueryStorageReadControl control([&] {
    if (reads == 2)
      throw QueryStorageReadInterrupted(QueryStatus::failure(
        QueryError::Cancelled, QueryPhase::Storage, "cancelled second scalar slice"));
  }, {});
  try {
    (void)opened.value().read_payload_range(0, cap->payload.size());
    FAIL() << "expected reader cancellation";
  } catch (const QueryStorageReadInterrupted& error) {
    EXPECT_EQ(error.status().code(), QueryError::Cancelled);
    EXPECT_EQ(error.status().message(), "cancelled second scalar slice");
  }
  EXPECT_EQ(reads, 2u);
  ASSERT_EQ(cap->batches.size(), 1u);
  EXPECT_EQ(cap->batches[0][0].length, kMaxPayloadBytesPerBatch);
  EXPECT_FALSE(current_query_metadata_budget());
}

namespace {
// Exercise the production scalar adapter and real immutable block/map reader,
// rather than implementing block reuse inside the test's pinned reader.
class ScalarReuseArtifactStore final : public versioning::Store {
public:
  using ObjectKey = std::tuple<std::string, std::string, std::string,
                               std::string, versioning::ContentId>;
  std::optional<versioning::Bytes> head;
  std::map<ObjectKey, versioning::Bytes> objects;
  std::vector<std::vector<versioning::ObjectReadRequest>> payload_batches;
  static ObjectKey object_key(const versioning::NamespaceIdentity& identity,
                              const versioning::ObjectRef& ref) {
    return {identity.namespace_id, identity.incarnation, ref.type, ref.protocol, ref.id};
  }
  std::optional<versioning::Bytes> read_head() override { return head; }
  versioning::Bytes get_object(const versioning::NamespaceIdentity& identity,
                              const versioning::ObjectRef& ref) override {
    return objects.at(object_key(identity, ref));
  }
  std::vector<versioning::Bytes> get_objects(
      const versioning::NamespaceIdentity& identity,
      const std::vector<versioning::ObjectReadRequest>& requests) override {
    if (!requests.empty() && requests.front().ref.type == "payload-block")
      payload_batches.push_back(requests);
    std::vector<versioning::Bytes> result;
    for (const auto& request : requests) result.push_back(get_object(identity, request.ref));
    return result;
  }
  void put_object(const versioning::NamespaceIdentity& identity,
      const versioning::ObjectRef& ref, const versioning::Bytes& bytes) override {
    objects.emplace(object_key(identity, ref), bytes);
  }
  versioning::HeadWriteResult write_head(const versioning::Bytes& bytes) override {
    head = bytes;
    return versioning::HeadWriteResult::Acknowledged;
  }
};
class ScalarReuseArtifactJournal final : public versioning::Journal {
public:
  std::optional<versioning::JournalState> state;
  std::optional<versioning::JournalState> load() override { return state; }
  void save(const versioning::JournalState& value) override { state = value; }
};
class ScalarReuseFixture {
public:
  static constexpr std::uint64_t mib = 1024 * 1024;
  explicit ScalarReuseFixture(bool enabled, bool boundary_in_first_batch = false)
    : backend(std::make_shared<MemoryStructuredBlobBackend>()), storage(backend),
      store(std::make_shared<ScalarReuseArtifactStore>()),
      journal(std::make_shared<ScalarReuseArtifactJournal>()) {
    versioning::OwnerConfig config;
    config.coalesce_payload_reads = enabled;
    config.payload_cache_bytes = 0; // Scalar request reuse, independently of cross-request residency.
    owner = std::make_shared<versioning::Owner>(store, journal, config);
    owner->initialize({"123e4567-e89b-42d3-a456-426614174000",
                       "123e4567-e89b-42d3-a456-426614174001"});
    payload = std::make_shared<versioning::PayloadArtifacts>(owner);
    // All seventeen block identities differ, including the shared boundary.
    bytes.resize(17 * mib);
    for (std::size_t block = 0; block < 17; ++block)
      std::fill(bytes.begin() + block * mib, bytes.begin() + (block + 1) * mib,
                static_cast<std::uint8_t>(11 + block));
    if (boundary_in_first_batch) {
      // Nine physical blocks exceed the eight-MiB retention cap. Place the
      // cross-slice boundary inside the first content-ID-sorted native batch
      // so this case tests an admitted reusable block, not an over-cap promise.
      std::size_t minimum = 0;
      versioning::ContentId minimum_id;
      for (std::size_t block = 0; block < 9; ++block) {
        const auto id = versioning::object_ref("payload-block", versioning::Bytes(
          bytes.begin() + block * mib, bytes.begin() + (block + 1) * mib)).id;
        if (!block || id < minimum_id) { minimum = block; minimum_id = id; }
      }
      if (minimum != 8)
        std::swap_ranges(bytes.begin() + minimum * mib, bytes.begin() + (minimum + 1) * mib,
                         bytes.begin() + 8 * mib);
    }
    auto tx = owner->begin("scalar-reuse-integration-fixture");
    root = payload->build(*tx, bytes.size(), [&](std::uint64_t offset, std::uint64_t size) {
      return versioning::Bytes(bytes.begin() + offset, bytes.begin() + offset + size);
    });
    tx->commit();
  }
  StructuredBlobResult<StructuredBlobReadHandle> open() {
    ingest_through_layout(storage, version, bytes, {});
    EXPECT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
    EXPECT_TRUE(storage.publish(version).ok());
    auto pinned = std::make_shared<TestPinnedPayloadReader>();
    pinned->read_override = [this](const StructuredBlobPayloadRanges& ranges) {
      return StructuredBlobResult<StructuredBlobPayloadBatch>::success(
        payload->read_ranges(root, bytes.size(), ranges, "scalar-fixture-source"));
    };
    backend->pinned_payload_reader = pinned;
    owner->clear_read_cache();
    store->payload_batches.clear();
    return storage.open_published_version(version);
  }
  std::uint64_t physical_bytes() const {
    std::uint64_t result = 0;
    for (const auto& batch : store->payload_batches)
      for (const auto& request : batch) result += request.size;
    return result;
  }
  std::shared_ptr<MemoryStructuredBlobBackend> backend;
  StructuredBlobStorage storage;
  std::shared_ptr<ScalarReuseArtifactStore> store;
  std::shared_ptr<ScalarReuseArtifactJournal> journal;
  std::shared_ptr<versioning::Owner> owner;
  std::shared_ptr<versioning::PayloadArtifacts> payload;
  StructuredBlobBytes bytes;
  versioning::ContentId root;
  const StructuredBlobVersionKey version = key(940);
};
} // namespace

TEST(StructuredBlobStorage, ActualScalarRequestReusesUnalignedBoundaryAcrossNativeBatches) {
  for (bool enabled : {false, true}) {
    SCOPED_TRACE(enabled);
    ScalarReuseFixture fixture(enabled, true);
    auto opened = fixture.open();
    ASSERT_TRUE(opened.ok());
    const auto result = opened.value().read_payload_range(1, 16 * ScalarReuseFixture::mib);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(result.value(), (StructuredBlobBytes(fixture.bytes.begin() + 1,
      fixture.bytes.begin() + 1 + 16 * ScalarReuseFixture::mib)));
    EXPECT_EQ(fixture.physical_bytes(), (enabled ? 17u : 18u) * ScalarReuseFixture::mib);
    EXPECT_EQ(fixture.owner->stats().payload_coalesce_active_flights, 0u);
    EXPECT_EQ(fixture.owner->stats().payload_coalesce_retained_bytes, 0u);
    // A later scalar is a new request, even through the same pinned handle.
    const auto before = fixture.physical_bytes();
    ASSERT_TRUE(opened.value().read_payload_range(8 * ScalarReuseFixture::mib, 1).ok());
    EXPECT_EQ(fixture.physical_bytes() - before, ScalarReuseFixture::mib);
  }
}

TEST(StructuredBlobStorage, ActualScalarOptionalReuseYieldsBeforeTightTemporaryBudgetFails) {
  for (bool enabled : {false, true}) {
    SCOPED_TRACE(enabled);
    ScalarReuseFixture fixture(enabled);
    auto opened = fixture.open();
    ASSERT_TRUE(opened.ok());
    std::uint64_t live = 0, peak = 0, logical_bytes = 0, logical_ranges = 0;
    unsigned reclaims = 0;
    bool sticky_failure = false;
    constexpr auto limit = 40 * ScalarReuseFixture::mib; // 8 MiB output + 4*8 MiB physical batch.
    ScopedQueryStorageReadControl control([] {}, [&](std::uint64_t bytes) -> std::shared_ptr<void> {
      if (bytes > limit - live) {
        ++reclaims;
        reclaim_storage_read_optional();
      }
      if (bytes > limit - live) {
        sticky_failure = true;
        throw QueryStorageReadInterrupted(QueryStatus::failure(QueryError::ResourceExhausted,
          QueryPhase::Storage, "test temporary budget exhausted"));
      }
      live += bytes;
      peak = std::max(peak, live);
      return std::shared_ptr<void>(new std::uint64_t(bytes), [&live](void* pointer) {
        auto* charged = static_cast<std::uint64_t*>(pointer);
        live -= *charged;
        delete charged;
      });
    }, [&](std::uint64_t bytes, std::uint64_t ranges) {
      logical_bytes += bytes;
      logical_ranges += ranges;
    });
    const auto result = opened.value().read_payload_range(1, 16 * ScalarReuseFixture::mib);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(result.value(), (StructuredBlobBytes(fixture.bytes.begin() + 1,
      fixture.bytes.begin() + 1 + 16 * ScalarReuseFixture::mib)));
    EXPECT_FALSE(sticky_failure);
    EXPECT_LE(peak, limit);
    EXPECT_EQ(live, 0u);
    EXPECT_EQ(logical_bytes, 18 * ScalarReuseFixture::mib);
    EXPECT_EQ(logical_ranges, 18u);
    EXPECT_LE(fixture.physical_bytes(), 18 * ScalarReuseFixture::mib);
    EXPECT_EQ(fixture.owner->stats().payload_coalesce_retained_bytes, 0u);
    if (enabled) EXPECT_GT(reclaims, 0u);
  }
}


TEST(StructuredBlobStorage, OrdinarySharedReadKeepsManifestBindingAndPayloadOnOneRequestLedger) {
  constexpr std::uint64_t mib = 1024 * 1024;
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(921);
  ingest_through_layout(storage, version, {1, 2, 3}, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  backend->shared_payload = true;
  auto cap = std::make_shared<TestPinnedPayloadReader>(); cap->payload = {1, 2, 3};
  backend->pinned_payload_reader = cap;
  std::weak_ptr<QueryMetadataBudget> first_ledger;
  backend->on_load_manifest = [&] {
    auto ledger = current_query_metadata_budget();
    ASSERT_TRUE(ledger);
    first_ledger = ledger;
    require_query_metadata("manifest/exact", 40 * mib);
  };
  cap->on_read = [&] {
    EXPECT_EQ(current_query_metadata_budget(), first_ledger.lock());
    require_query_metadata("payload/map", 25 * mib);
  };
  EXPECT_THROW(storage.read_payload_range(version, 0, 1), QueryMetadataBudgetExceeded);
  EXPECT_TRUE(first_ledger.expired());
  EXPECT_FALSE(current_query_metadata_budget());

  // Opening is itself a single request: initial manifest and cap binding cannot reset the ledger.
  backend->on_bind_payload = [&] {
    EXPECT_EQ(current_query_metadata_budget(), first_ledger.lock());
    require_query_metadata("payload/binding", 25 * mib);
  };
  EXPECT_THROW(storage.open_published_version(version), QueryMetadataBudgetExceeded);
  EXPECT_TRUE(first_ledger.expired());
  backend->on_bind_payload = {};
  cap->on_read = [&] {
    EXPECT_EQ(current_query_metadata_budget(), first_ledger.lock());
    require_query_metadata("payload/map", 24 * mib);
  };
  // Exactly 64 MiB succeeds on each independent request; no failed ledger escapes its scope.
  for (unsigned i = 0; i < 2; ++i) {
    auto loaded = storage.read_payload_range(version, 0, 1);
    ASSERT_TRUE(loaded.ok()); EXPECT_EQ(loaded.value(), (StructuredBlobBytes{1}));
    EXPECT_TRUE(first_ledger.expired());
  }
  {
    ScopedQueryMetadataBudget outer;
    require_query_metadata("existing-request", mib);
    EXPECT_THROW(storage.read_payload_range(version, 0, 1), QueryMetadataBudgetExceeded);
    EXPECT_EQ(first_ledger.lock(), outer.budget());
    EXPECT_EQ(outer.status().code(), QueryError::ResourceExhausted);
  }
  EXPECT_TRUE(first_ledger.expired());
}

TEST(StructuredBlobStorage, StrictOrdinaryReadsDoNotCreateSharedMetadataLedgers) {
  constexpr std::uint64_t mib = 1024 * 1024;
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(922);
  ingest_through_layout(storage, version, {4, 5, 6}, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  ASSERT_FALSE(backend->uses_shared_payload());
  unsigned manifests = 0, payloads = 0;
  backend->on_load_manifest = [&] {
    ++manifests;
    EXPECT_FALSE(current_query_metadata_budget());
    require_query_metadata("strict/manifest", 40 * mib);
  };
  backend->on_read_payload = [&] {
    ++payloads;
    EXPECT_FALSE(current_query_metadata_budget());
    require_query_metadata("strict/payload", 25 * mib);
  };
  auto loaded = storage.read_payload_range(version, 0, 1);
  ASSERT_TRUE(loaded.ok()); EXPECT_EQ(loaded.value(), (StructuredBlobBytes{4}));
  EXPECT_EQ(manifests, 1u); EXPECT_EQ(payloads, 1u);
  EXPECT_FALSE(current_query_metadata_budget());
}

TEST(StructuredBlobStorage, NeutralSharedProducerCapturesBothReadFeesWithoutBorrowingALedger) {
  constexpr std::uint64_t mib = 1024 * 1024;
  auto backend = std::make_shared<MemoryStructuredBlobBackend>();
  StructuredBlobStorage storage(backend);
  const auto version = key(923);
  ingest_through_layout(storage, version, {7, 8, 9}, {});
  ASSERT_TRUE(storage.write_metadata_and_format_identity(version, {}, {}).ok());
  ASSERT_TRUE(storage.publish(version).ok());
  backend->shared_payload = true;
  auto cap = std::make_shared<TestPinnedPayloadReader>(); cap->payload = {7, 8, 9};
  backend->pinned_payload_reader = cap;
  backend->on_load_manifest = [&] {
    EXPECT_FALSE(current_query_metadata_budget());
    require_query_metadata("producer/manifest", 40 * mib);
  };
  cap->on_read = [&] {
    EXPECT_FALSE(current_query_metadata_budget());
    require_query_metadata("producer/payload", 25 * mib);
  };
  QueryMetadataTrace fees;
  {
    ScopedQueryMetadataProducer producer;
    ScopedQueryMetadataTrace trace("");
    auto loaded = storage.read_payload_range(version, 0, 1);
    ASSERT_TRUE(loaded.ok()); EXPECT_EQ(loaded.value(), (StructuredBlobBytes{7}));
    EXPECT_FALSE(current_query_metadata_budget());
    fees = trace.release();
  }
  EXPECT_EQ(fees.size(), 2u);
  EXPECT_EQ(fees.at("producer/manifest"), 40 * mib);
  EXPECT_EQ(fees.at("producer/payload"), 25 * mib);
  ScopedQueryMetadataBudget waiter;
  EXPECT_THROW(replay_query_metadata_trace(fees), QueryMetadataBudgetExceeded);
  EXPECT_EQ(waiter.status().code(), QueryError::ResourceExhausted);
}
