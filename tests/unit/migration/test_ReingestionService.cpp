#include "iotdbfs/migration/ReingestionService.h"
#include "iotdbfs/migration/ArrowReingestionPolicy.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/arrow/ArrowLayout.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using namespace iotdbfs;

template <class T> T checked(arrow::Result<T> value) {
  if (!value.ok()) throw std::runtime_error(value.status().ToString());
  return std::move(value).ValueOrDie();
}

void checked(const arrow::Status& status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

StructuredBlobBytes arrow_stream() {
  auto leaves = std::make_shared<arrow::FloatBuilder>();
  arrow::ListBuilder values(arrow::default_memory_pool(), leaves);
  checked(values.Append());
  checked(leaves->AppendValues({1.0F, 2.0F, 3.0F}));
  checked(values.Append());
  checked(leaves->AppendValues({4.0F, 5.0F}));
  std::shared_ptr<arrow::Array> column;
  checked(values.Finish(&column));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({arrow::field("target", column->type())}), 2, {column});
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  checked(writer->WriteRecordBatch(*batch));
  checked(writer->Close());
  auto buffer = checked(sink->Finish());
  return {buffer->data(), buffer->data() + buffer->size()};
}

class MemoryBackend final : public StructuredBlobStorageBackend {
public:
  struct Record {
    StructuredBlobManifest manifest;
    StructuredBlobBytes payload;
    StructuredBlobBytes layout;
  };
  std::map<StructuredBlobVersionKey, Record> records;

  StructuredBlobStatus create_manifest(const StructuredBlobManifest& manifest) override {
    if (!records.emplace(manifest.key, Record{manifest, {}, {}}).second)
      return error(StructuredBlobError::AlreadyExists);
    return StructuredBlobStatus::success();
  }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(
      const StructuredBlobVersionKey& key) const override {
    auto found = records.find(key);
    if (found == records.end())
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::NotFound, "missing manifest");
    return StructuredBlobResult<StructuredBlobManifest>::success(found->second.manifest);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey& key,
      std::uint64_t revision, const StructuredBlobManifest& next) override {
    auto found = records.find(key);
    if (found == records.end() || found->second.manifest.revision != revision ||
        next.revision != revision + 1)
      return error(StructuredBlobError::InvalidState);
    found->second.manifest = next;
    return StructuredBlobStatus::success();
  }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey& key,
      std::uint64_t offset, const StructuredBlobBytes& bytes) override {
    auto found = records.find(key);
    if (found == records.end()) return error(StructuredBlobError::NotFound);
    auto& payload = found->second.payload;
    if (offset > payload.size()) return error(StructuredBlobError::RangeError);
    if (bytes.size() > payload.size() - static_cast<std::size_t>(offset))
      payload.resize(static_cast<std::size_t>(offset) + bytes.size());
    std::copy(bytes.begin(), bytes.end(), payload.begin() + static_cast<std::size_t>(offset));
    return StructuredBlobStatus::success();
  }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey& key,
      const StructuredBlobBytes& layout) override {
    auto found = records.find(key);
    if (found == records.end()) return error(StructuredBlobError::NotFound);
    found->second.layout = layout;
    return StructuredBlobStatus::success();
  }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey& key,
      const MetadataMap&) override {
    return records.count(key) ? StructuredBlobStatus::success() :
      error(StructuredBlobError::NotFound);
  }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
      const StructuredBlobVersionKey& key, std::uint64_t offset,
      std::uint64_t length) const override {
    return read(key, offset, length, false);
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      const StructuredBlobVersionKey& key, std::uint64_t offset,
      std::uint64_t length) const override {
    return read(key, offset, length, true);
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey& key,
      std::uint64_t revision) override {
    auto found = records.find(key);
    if (found == records.end() || found->second.manifest.revision != revision)
      return error(StructuredBlobError::InvalidState);
    records.erase(found);
    return StructuredBlobStatus::success();
  }

private:
  static StructuredBlobStatus error(StructuredBlobError code) {
    return StructuredBlobStatus::failure(code, "memory backend error");
  }
  StructuredBlobResult<StructuredBlobBytes> read(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length, bool layout) const {
    auto found = records.find(key);
    if (found == records.end())
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "missing record");
    const auto& bytes = layout ? found->second.layout : found->second.payload;
    if (offset > bytes.size() || length > bytes.size() - offset)
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::RangeError, "read exceeds record");
    const auto begin = bytes.begin() + static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }
};

class ReingestionServiceTest : public ::testing::Test {
protected:
  std::shared_ptr<MemoryBackend> backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage{backend};
  FormatRegistry formats;
  std::map<StructuredBlobVersionKey, Inode> inodes;

  ReingestionServiceTest() {
    EXPECT_TRUE(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()));
  }

  void seed(const StructuredBlobVersionKey& key,
      const StructuredBlobBytes& bytes, bool indexed = false) {
    FormatRegistry empty;
    auto& registry = indexed ? formats : empty;
    IngestionModule ingestion(storage, registry, {});
    FormatSource source{bytes.size(), [&bytes](std::uint64_t offset,
        std::uint64_t length) -> std::optional<StructuredBlobBytes> {
      if (offset > bytes.size() || length > bytes.size() - offset)
        return std::nullopt;
      const auto begin = bytes.begin() + static_cast<std::size_t>(offset);
      return StructuredBlobBytes(begin, begin + static_cast<std::size_t>(length));
    }};
    auto result = ingestion.ingest(key, source, [&](const IngestionOutcome& outcome) {
      Inode inode;
      inode.path = key.path;
      inode.row_time = key.version;
      inode.size = static_cast<long long>(outcome.payload_size);
      inode.format_id = outcome.format_id.value_or("");
      inodes[key] = inode;
      return true;
    });
    ASSERT_TRUE(result.ok()) << result.message();
  }

  ReingestionNamespace name_space() {
    ReingestionNamespace ns;
    ns.published_manifests = [this]() {
      std::vector<StructuredBlobManifest> result;
      for (const auto& [key, record] : backend->records)
        if (record.manifest.published) result.push_back(record.manifest);
      return StructuredBlobResult<std::vector<StructuredBlobManifest>>::success(
        std::move(result));
    };
    ns.latest_inode = [this](const std::string& path) -> std::optional<Inode> {
      std::optional<Inode> result;
      for (const auto& [key, inode] : inodes) {
        auto manifest = backend->records.find(key);
        if (key.path == path && manifest != backend->records.end() &&
            manifest->second.manifest.published &&
            (!result || inode.row_time > result->row_time)) result = inode;
      }
      return result;
    };
    ns.exact_inode = [this](const StructuredBlobVersionKey& key)
        -> std::optional<Inode> {
      auto found = inodes.find(key);
      return found == inodes.end() ? std::nullopt : std::optional<Inode>(found->second);
    };
    ns.prepare_inode = [this](const Inode& inode) {
      inodes[{inode.path, inode.row_time}] = inode;
      return true;
    };
    ns.next_version = [](std::int64_t latest) { return latest + 1; };
    return ns;
  }
};

TEST_F(ReingestionServiceTest, InventoryRecognizesContentWithoutTrustingExtension) {
  const auto stream = arrow_stream();
  seed({"/actual.bin", 10}, stream);
  seed({"/misleading.arrow", 20}, {1, 2, 3, 4});
  seed({"/truncated.ipc", 25}, {0xff, 0xff, 0xff, 0xff, 8});
  seed({"/indexed", 30}, stream, true);
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory();
  ASSERT_TRUE(inventory.ok()) << inventory.status().message();
  ASSERT_EQ(inventory.value().size(), 4U);
  std::map<std::string, MigrationInventoryEntry> by_path;
  for (const auto& item : inventory.value()) by_path.emplace(item.source.path, item);
  EXPECT_EQ(by_path.at("/actual.bin").kind, MigrationKind::Candidate);
  EXPECT_EQ(by_path.at("/actual.bin").bytes, stream.size());
  EXPECT_FALSE(by_path.at("/actual.bin").sha256.empty());
  EXPECT_EQ(by_path.at("/misleading.arrow").kind, MigrationKind::NotFormat);
  EXPECT_EQ(by_path.at("/truncated.ipc").kind,
    MigrationKind::Unsupported);
  EXPECT_EQ(by_path.at("/indexed").kind, MigrationKind::AlreadyIndexed);
}

TEST_F(ReingestionServiceTest, InventoryListsHistoricalVersionsWithoutMigratingThem) {
  const auto stream = arrow_stream();
  seed({"/series.arrow", 10}, stream);
  seed({"/series.arrow", 20}, stream);
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(std::string("/series.arrow"));
  ASSERT_TRUE(inventory.ok()) << inventory.status().message();
  ASSERT_EQ(inventory.value().size(), 2U);
  EXPECT_EQ(inventory.value()[0].source.version, 10);
  EXPECT_EQ(inventory.value()[0].kind, MigrationKind::HistoricalVersion);
  EXPECT_EQ(inventory.value()[1].source.version, 20);
  EXPECT_EQ(inventory.value()[1].kind, MigrationKind::Candidate);
}

TEST_F(ReingestionServiceTest, MigrationKeepsOldExactVersionAndVerifiesNewBytes) {
  const auto bytes = arrow_stream();
  const StructuredBlobVersionKey old{"/source.bin", 100};
  seed(old, bytes);
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(old.path);
  ASSERT_TRUE(inventory.ok()) << inventory.status().message();
  ASSERT_EQ(inventory.value().size(), 1U);
  ASSERT_EQ(inventory.value().front().kind, MigrationKind::Candidate);
  std::optional<MigrationResult> journal;
  auto migrated = service.migrate(inventory.value().front(),
    [&](const MigrationResult& result) { journal = result; return true; });
  ASSERT_TRUE(journal.has_value());
  ASSERT_TRUE(migrated.ok()) << migrated.error;
  EXPECT_EQ(migrated.source, old);
  EXPECT_NE(migrated.destination.version, old.version);
  EXPECT_EQ(migrated.source_sha256, migrated.destination_sha256);
  auto old_manifest = storage.load_manifest(old);
  auto new_manifest = storage.load_manifest(migrated.destination);
  ASSERT_TRUE(old_manifest.ok());
  ASSERT_TRUE(new_manifest.ok());
  EXPECT_FALSE(old_manifest.value().format_id.has_value());
  ASSERT_TRUE(new_manifest.value().format_id.has_value());
  EXPECT_EQ(*new_manifest.value().format_id, "arrow");
  EXPECT_GT(new_manifest.value().layout_size, 0U);
  auto old_bytes = storage.read_payload_range(old, 0, bytes.size());
  auto new_bytes = storage.read_payload_range(migrated.destination, 0, bytes.size());
  ASSERT_TRUE(old_bytes.ok());
  ASSERT_TRUE(new_bytes.ok());
  EXPECT_EQ(old_bytes.value(), bytes);
  EXPECT_EQ(new_bytes.value(), bytes);
  auto reconciled = service.reconcile(*journal);
  EXPECT_TRUE(reconciled.ok()) << reconciled.error;
}

TEST_F(ReingestionServiceTest, LatestChangeAfterInventoryRejectsMigration) {
  const auto bytes = arrow_stream();
  const StructuredBlobVersionKey old{"/source.bin", 100};
  seed(old, bytes);
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(old.path);
  ASSERT_TRUE(inventory.ok());
  ASSERT_EQ(inventory.value().size(), 1U);
  seed({old.path, 101}, {7, 8, 9});
  bool journaled = false;
  auto migrated = service.migrate(inventory.value().front(),
    [&](const MigrationResult&) { journaled = true; return true; });
  EXPECT_FALSE(migrated.ok());
  EXPECT_FALSE(journaled);
  auto old_manifest = storage.load_manifest(old);
  ASSERT_TRUE(old_manifest.ok());
  EXPECT_FALSE(old_manifest.value().format_id.has_value());
}

TEST_F(ReingestionServiceTest, LatestChangeDuringPublicationReportsConflict) {
  const auto bytes = arrow_stream();
  const StructuredBlobVersionKey old{"/source.bin", 100};
  seed(old, bytes);
  ReingestionService inventory_service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = inventory_service.inventory(old.path);
  ASSERT_TRUE(inventory.ok());
  ASSERT_EQ(inventory.value().size(), 1U);
  auto ns = name_space();
  auto actual_latest = ns.latest_inode;
  int checks = 0;
  ns.latest_inode = [actual_latest, &checks](const std::string& path) {
    auto inode = actual_latest(path);
    if (++checks == 3 && inode) inode->row_time = 101;
    return inode;
  };
  ReingestionService service(storage, formats, std::move(ns), make_arrow_reingestion_policy());
  auto migrated = service.migrate(inventory.value().front(),
    [](const MigrationResult&) { return true; });
  EXPECT_EQ(migrated.state, "conflict");
  EXPECT_FALSE(storage.load_manifest(migrated.destination).ok());
  EXPECT_TRUE(storage.load_manifest(old).ok());
}

TEST_F(ReingestionServiceTest, ResumeRechecksPublishedDestinationBytes) {
  const StructuredBlobVersionKey old{"/source.bin", 100};
  seed(old, arrow_stream());
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(old.path);
  ASSERT_TRUE(inventory.ok());
  ASSERT_EQ(inventory.value().size(), 1U);
  std::optional<MigrationResult> journal;
  auto migrated = service.migrate(inventory.value().front(),
    [&](const MigrationResult& result) { journal = result; return true; });
  ASSERT_TRUE(migrated.ok()) << migrated.error;
  ASSERT_TRUE(journal.has_value());
  auto found = backend->records.find(migrated.destination);
  ASSERT_NE(found, backend->records.end());
  ASSERT_FALSE(found->second.payload.empty());
  found->second.payload[0] ^= 1U;
  auto resumed = service.reconcile(*journal);
  EXPECT_FALSE(resumed.ok());
  EXPECT_EQ(resumed.state, "published_unverified");
  EXPECT_NE(resumed.error.find("SHA-256"), std::string::npos);
}

TEST_F(ReingestionServiceTest, FailedDurableJournalPreventsAllDestinationWrites) {
  seed({"/journal.bin", 100}, arrow_stream());
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(); ASSERT_TRUE(inventory.ok());
  auto result = service.migrate(inventory.value().front(), [](const MigrationResult&) {return false;});
  EXPECT_EQ(result.state, "failed"); EXPECT_EQ(backend->records.size(), 1u);
}

TEST_F(ReingestionServiceTest, MutationDuringJournalPauseIsCaughtBeforeIngestion) {
  seed({"/source.bin", 100}, arrow_stream());
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(); ASSERT_TRUE(inventory.ok());
  auto result = service.migrate(inventory.value().front(), [&](const MigrationResult&) {
    backend->records.at({"/source.bin", 100}).payload[0] ^= 1;
    return true;
  });
  EXPECT_EQ(result.state, "conflict"); EXPECT_EQ(backend->records.size(), 1u);
}

TEST_F(ReingestionServiceTest, ResumeRejectsValidButSubstitutedLayout) {
  seed({"/source.bin", 100}, arrow_stream());
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(); ASSERT_TRUE(inventory.ok());
  auto result = service.migrate(inventory.value().front(), [](const MigrationResult&) {return true;});
  ASSERT_TRUE(result.ok()) << result.error;
  auto& target = backend->records.at(result.destination);
  auto parsed = parse_arrow_layout(target.layout, result.bytes); ASSERT_TRUE(parsed.ok());
  // Alter a structurally valid List boundary while leaving the native schema,
  // total leaf count and payload untouched. Renaming only the field would be
  // rejected by the layout parser before recovery can test its digest check.
  auto& offsets = parsed.value().batches[0].arrays[0].levels[0].offsets;
  ASSERT_EQ(offsets.size(), 3U);
  offsets[1] = 2;
  auto substituted = serialize_arrow_layout(parsed.value()); ASSERT_TRUE(substituted.ok());
  target.layout = std::move(substituted.value()); target.manifest.layout_size = target.layout.size();
  ASSERT_TRUE(parse_arrow_layout(target.layout, result.bytes).ok());
  auto resumed = service.reconcile(result);
  EXPECT_EQ(resumed.state, "published_unverified");
  EXPECT_NE(resumed.error.find("layout"), std::string::npos);
}

TEST_F(ReingestionServiceTest, ResumeRejectsDestinationInodeMetadataMismatch) {
  seed({"/source.bin", 100}, arrow_stream());
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(); ASSERT_TRUE(inventory.ok());
  auto result = service.migrate(inventory.value().front(), [](const MigrationResult&) {return true;});
  ASSERT_TRUE(result.ok()) << result.error;
  inodes.at(result.destination).metadata = "{}";
  EXPECT_FALSE(service.reconcile(result).ok());
}

TEST_F(ReingestionServiceTest, ResumeRejectsWrongPolicyAndSourceInode) {
  seed({"/source.bin", 100}, arrow_stream());
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(); ASSERT_TRUE(inventory.ok());
  auto result = service.migrate(inventory.value().front(), [](const MigrationResult&) {return true;});
  ASSERT_TRUE(result.ok()) << result.error;
  auto wrong = result; wrong.adapter_id = "hdf5";
  EXPECT_FALSE(service.reconcile(wrong).ok());
  inodes.erase(result.source);
  EXPECT_FALSE(service.reconcile(result).ok());
}

TEST_F(ReingestionServiceTest, ResumeRejectsConsistentlyCorruptedArrowSummary) {
  seed({"/summary.bin", 100}, arrow_stream());
  ReingestionService service(storage, formats, name_space(), make_arrow_reingestion_policy());
  auto inventory = service.inventory(); ASSERT_TRUE(inventory.ok());
  auto migrated = service.migrate(inventory.value().front(), [](const MigrationResult&) { return true; });
  ASSERT_TRUE(migrated.ok()) << migrated.error;
  auto& target = backend->records.at(migrated.destination);
  const auto valid_metadata = target.manifest.metadata;
  for (const char* key : {"arrow.rows", "arrow.record_batches", "arrow.fields"}) {
    SCOPED_TRACE(key);
    for (int variant = 0; variant < 4; ++variant) {
      SCOPED_TRACE(variant);
      target.manifest.metadata = valid_metadata;
      if (variant == 0) target.manifest.metadata[key] = MetadataValue(std::int64_t(999));
      if (variant == 1) target.manifest.metadata.erase(key);
      if (variant == 2) target.manifest.metadata[key] = MetadataValue(1.0);
      if (variant == 3) target.manifest.metadata[key] = MetadataValue(std::int64_t(-1));
      auto encoded = serialize_metadata(target.manifest.metadata, 65536); ASSERT_TRUE(encoded.ok());
      // IoTDB stores the inode and manifest metadata in the same row. Corrupt
      // both representations, so equality alone cannot detect the bad summary.
      inodes.at(migrated.destination).metadata = encoded.value();
      auto opened = storage.open_published_version(migrated.destination); ASSERT_TRUE(opened.ok());
      auto inspected = make_arrow_reingestion_policy()->inspect(opened.value(), formats);
      EXPECT_EQ(inspected.kind, MigrationKind::Error);
      EXPECT_NE(inspected.detail.find("summary"), std::string::npos);
      auto resumed = service.reconcile(migrated);
      EXPECT_FALSE(resumed.ok());
      EXPECT_EQ(resumed.state, "published_unverified");
    }
  }
  target.manifest.metadata = valid_metadata;
  auto encoded = serialize_metadata(valid_metadata, 65536); ASSERT_TRUE(encoded.ok());
  inodes.at(migrated.destination).metadata = encoded.value();
  EXPECT_TRUE(service.reconcile(migrated).ok());
}

} // namespace
