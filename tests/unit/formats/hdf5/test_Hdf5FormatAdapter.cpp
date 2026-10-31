#include "../../../support/InMemoryDatasetCatalog.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/MetadataSerialization.h"

#include <gtest/gtest.h>
#include <hdf5.h>
#include <openssl/sha.h>

#include <algorithm>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <stdexcept>
#include <unistd.h>

namespace {
using namespace iotdbfs;

void checked(herr_t result, const char* message) {
  if (result < 0) throw std::runtime_error(message);
}
struct H5Id {
  H5Id(hid_t value, herr_t (*close)(hid_t)) : value(value), close(close) {
    if (value < 0) throw std::runtime_error("cannot create H03 fixture");
  }
  ~H5Id() { close(value); }
  H5Id(const H5Id&) = delete;
  operator hid_t() const { return value; }
  hid_t value;
  herr_t (*close)(hid_t);
};
struct TempFile {
  TempFile() {
    char path[] = "/tmp/iotdbfs-h03-XXXXXX";
    const auto fd = ::mkstemp(path);
    if (fd < 0) throw std::runtime_error("cannot create H03 temporary fixture");
    ::close(fd);
    name = path;
  }
  ~TempFile() { ::unlink(name.c_str()); }
  std::string name;
};

BinaryValue hdf_bytes(hsize_t user_block = 0, bool only_unsupported = false,
    bool checksum = false, std::uint64_t* first_chunk = nullptr) {
  TempFile temporary;
  {
    H5Id access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    checked(H5Pset_fapl_sec2(access), "configure native fixture driver");
    H5Id creation(H5Pcreate(H5P_FILE_CREATE), H5Pclose);
    if (user_block) checked(H5Pset_userblock(creation, user_block), "configure user block");
    H5Id file(H5Fcreate(temporary.name.c_str(), H5F_ACC_TRUNC, creation, access), H5Fclose);
    if (!only_unsupported) {
      const hsize_t shape[]{3, 5}, chunks[]{2, 3};
      H5Id space(H5Screate_simple(2, shape, nullptr), H5Sclose);
      H5Id properties(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
      checked(H5Pset_chunk(properties, 2, chunks), "configure chunks");
      if (checksum) checked(H5Pset_fletcher32(properties), "configure Fletcher32");
      H5Id values(H5Dcreate2(file, "/values", H5T_IEEE_F32LE, space,
        H5P_DEFAULT, properties, H5P_DEFAULT), H5Dclose);
      std::vector<float> data;
      for (std::size_t row = 0; row < 3; ++row)
        for (std::size_t column = 0; column < 5; ++column)
          data.push_back(static_cast<float>(100*row + column));
      checked(H5Dwrite(values, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL,
        H5P_DEFAULT, data.data()), "write values");
      checked(H5Lcreate_hard(file, "/values", file, "/values_alias",
        H5P_DEFAULT, H5P_DEFAULT), "create hard-link alias");
      if (first_chunk) {
        const hsize_t where[]{0, 0};
        unsigned mask = 0;
        haddr_t address = HADDR_UNDEF;
        hsize_t size = 0;
        checked(H5Dget_chunk_info_by_coord(values, where, &mask, &address, &size),
          "locate Fletcher32 chunk");
        if (address == HADDR_UNDEF || size == 0) throw std::runtime_error("missing written chunk");
        *first_chunk = address;
      }
    }
    const hsize_t shape[]{2};
    H5Id space(H5Screate_simple(1, shape, nullptr), H5Sclose);
    H5Id integers(H5Dcreate2(file, "/integer", H5T_STD_I32LE, space,
      H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    const int data[]{7, 8};
    checked(H5Dwrite(integers, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
      H5P_DEFAULT, data), "write unsupported integer dataset");
  }
  std::ifstream input(temporary.name, std::ios::binary);
  if (!input) throw std::runtime_error("cannot reopen fixture");
  return BinaryValue(std::istreambuf_iterator<char>(input), {});
}

FormatSource source_for(const BinaryValue& bytes) {
  return {bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length)
      -> std::optional<BinaryValue> {
    if (offset > bytes.size() || length > bytes.size() - offset) return std::nullopt;
    return BinaryValue(bytes.begin() + offset, bytes.begin() + offset + length);
  }};
}
FormatResult<FormatIngestionArtifacts> ingest(const BinaryValue& bytes,
    Hdf5LayoutLimits limits = {}, std::optional<FormatSource> source = std::nullopt,
    Hdf5SourceOptions options = {}) {
  Hdf5FormatAdapter adapter(limits, options);
  auto session = adapter.begin_ingestion(source ? *source : source_for(bytes));
  for (std::size_t offset = 0; offset < bytes.size(); offset += 17) {
    auto status = session->consume_chunk(offset, bytes.data() + offset,
      std::min<std::size_t>(17, bytes.size() - offset));
    if (!status.ok()) return FormatResult<FormatIngestionArtifacts>::failure(status.error());
  }
  return session->finish();
}
QueryResult<Hdf5LayoutDirectory> directory(const BinaryValue& layout, std::uint64_t source_size) {
  return read_hdf5_layout_directory([&](std::uint64_t offset, std::uint64_t length) {
    if (offset > layout.size() || length > layout.size() - offset)
      return QueryResult<BinaryValue>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage, "bad fixture layout read"));
    return QueryResult<BinaryValue>::success(BinaryValue(layout.begin() + offset,
      layout.begin() + offset + length));
  }, layout.size(), source_size);
}
BinaryValue digest(const BinaryValue& bytes) {
  BinaryValue result(SHA256_DIGEST_LENGTH);
  SHA256(bytes.data(), bytes.size(), result.data());
  return result;
}

class MemoryBackend final : public StructuredBlobStorageBackend {
public:
  iotdbfs::test::InMemoryDatasetCatalog dataset_catalogs;
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& value) override { return dataset_catalogs.write(key, value); }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
      const StructuredBlobVersionKey& key) const override { return dataset_catalogs.read(key); }

  std::uint64_t payload_bytes_written{0};
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
    found->second.manifest = next;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes) override {
    auto found = records_.find(key);
    if (found == records_.end()) return failure("not found");
    if (fault_ == Fault::PayloadWrite) return failure("payload write failed");
    payload_bytes_written += bytes.size();
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
    dataset_catalogs.erase(key);
    records_.erase(found);
    return StructuredBlobStatus::success();
  }

  const Record* peek(const StructuredBlobVersionKey& key) const {
    const auto found = records_.find(key);
    return found == records_.end() ? nullptr : &found->second;
  }

  void fail_publish() { fault_ = Fault::Publish; }
  void set_fault(Fault fault) { fault_ = fault; }

private:
  static StructuredBlobStatus failure(const char* message) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::BackendFailure, message);
  }

  std::map<StructuredBlobVersionKey, Record> records_;
  Fault fault_{ Fault::None };
};

} // namespace

TEST(Hdf5FormatAdapter, DetectsContentAtLegalUserBlocksAndBoundsEveryProbe) {
  Hdf5FormatAdapter adapter;
  for (const hsize_t block : {0, 512, 1024, 4096}) {
    const auto bytes = hdf_bytes(block);
    auto source = source_for(bytes);
    auto read = source.read;
    std::uint64_t last = 0;
    source.read = [&](std::uint64_t offset, std::uint64_t length) {
      EXPECT_EQ(length, 8u);
      EXPECT_LE(offset + length, bytes.size());
      last = offset;
      return read(offset, length);
    };
    EXPECT_TRUE(adapter.matches(source));
    EXPECT_EQ(last, block);
  }
  for (const BinaryValue bytes : {BinaryValue{}, BinaryValue{'a','b','c'},
      BinaryValue{'n','o','t','h','d','f','5','!'}, BinaryValue{0x0e,3,0x13,1}})
    EXPECT_FALSE(adapter.matches(source_for(bytes)));
  BinaryValue misplaced(1024);
  const BinaryValue signature{0x89,'H','D','F','\r','\n',0x1a,'\n'};
  std::copy(signature.begin(), signature.end(), misplaced.begin() + 256);
  EXPECT_FALSE(adapter.matches(source_for(misplaced)));
  std::size_t calls = 0;
  FormatSource huge{std::numeric_limits<std::uint64_t>::max(),
    [&](std::uint64_t offset, std::uint64_t length) -> std::optional<BinaryValue> {
      ++calls;
      EXPECT_LE(length, std::numeric_limits<std::uint64_t>::max() - offset);
      return BinaryValue(length);
    }};
  EXPECT_FALSE(adapter.matches(huge));
  EXPECT_LE(calls, 64u);
}

TEST(Hdf5FormatAdapter, SequentialSessionDoesNotReadPayloadAndRejectsInvalidLifecycle) {
  const auto bytes = hdf_bytes();
  auto source = source_for(bytes);
  auto read = source.read;
  std::size_t reads = 0;
  source.read = [&](std::uint64_t offset, std::uint64_t length) {
    ++reads; return read(offset, length);
  };
  Hdf5FormatAdapter adapter;
  auto session = adapter.begin_ingestion(source);
  ASSERT_TRUE(session->consume_chunk(0, bytes.data(), bytes.size()).ok());
  EXPECT_EQ(reads, 0u);
  ASSERT_TRUE(session->finish().ok());
  EXPECT_GT(reads, 0u);
  EXPECT_FALSE(session->finish().ok());
  EXPECT_FALSE(session->consume_chunk(bytes.size(), nullptr, 0).ok());
  session = adapter.begin_ingestion(source);
  ASSERT_TRUE(session->consume_chunk(0, bytes.data(), 8).ok());
  EXPECT_FALSE(session->finish().ok());
  session = adapter.begin_ingestion(source);
  EXPECT_FALSE(session->consume_chunk(1, bytes.data(), 8).ok());
  EXPECT_FALSE(session->finish().ok());
  session = adapter.begin_ingestion(source);
  EXPECT_FALSE(session->consume_chunk(0, nullptr, 1).ok());
}

TEST(Hdf5FormatAdapter, MixedAndUnsupportedDatasetsKeepBoundedCountsAndHardLinkIdentity) {
  for (const bool unsupported_only : {false, true}) {
    const auto bytes = hdf_bytes(0, unsupported_only);
    auto result = ingest(bytes);
    ASSERT_TRUE(result.ok()) << result.error();
    auto parsed = directory(result.value().layout, bytes.size());
    ASSERT_TRUE(parsed.ok()) << parsed.status().message();
    EXPECT_TRUE(parsed.value().document().directory_complete);
    const auto& metadata = result.value().metadata;
    EXPECT_EQ(std::get<std::int64_t>(metadata.at("hdf5.datasets").value()), unsupported_only ? 1 : 2);
    EXPECT_EQ(std::get<std::int64_t>(metadata.at("hdf5.queryable_datasets").value()), unsupported_only ? 0 : 1);
    // H07 adds seven bounded summaries, never one Sigma entry per attribute.
    EXPECT_LE(metadata.size(), 16u);
    EXPECT_EQ(std::get<std::int64_t>(metadata.at("hdf5.layout_version").value()), 5);
    EXPECT_TRUE(std::get<bool>(metadata.at("hdf5.attribute_counts_known").value()));
    EXPECT_TRUE(std::get<bool>(metadata.at("hdf5.attributes_complete").value()));
    EXPECT_TRUE(std::get<bool>(metadata.at("hdf5.attribute_values_complete").value()));
    EXPECT_EQ(std::get<std::int64_t>(metadata.at("hdf5.attributes_total").value()), 0);
    auto integer = resolve_hdf5_layout_path(parsed.value(), "/integer");
    ASSERT_TRUE(integer.ok());
    bool found = false;
    for (const auto& object : parsed.value().document().objects) {
      if (object.id != integer.value()) continue;
      ASSERT_TRUE(object.dataset);
      EXPECT_FALSE(object.dataset->descriptor.supported);
      EXPECT_FALSE(object.dataset->descriptor.unsupported_reason.empty());
      found = true;
    }
    EXPECT_TRUE(found);
    if (!unsupported_only) {
      auto values = resolve_hdf5_layout_path(parsed.value(), "/values");
      auto alias = resolve_hdf5_layout_path(parsed.value(), "/values_alias");
      ASSERT_TRUE(values.ok()); ASSERT_TRUE(alias.ok());
      EXPECT_EQ(values.value(), alias.value());
    }
  }
}

TEST(Hdf5FormatAdapter, InventoryDoesNotValidateFletcher32ValueChecksum) {
  std::uint64_t offset = 0;
  auto bytes = hdf_bytes(0, false, true, &offset);
  ASSERT_LT(offset, bytes.size());
  bytes[offset] ^= 0x40;
  // H03 inventories structure; a value decoder must detect this corruption later.
  const auto result = ingest(bytes);
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_TRUE(directory(result.value().layout, bytes.size()).ok());
}

TEST(Hdf5FormatAdapter, ArtifactProofValidatesEveryChunkPageWithoutPayloadReads) {
  const auto bytes = hdf_bytes();
  Hdf5LayoutLimits limits;
  limits.chunks_per_page = 1;
  std::size_t payload_reads = 0;
  auto source = source_for(bytes);
  const auto original_read = source.read;
  source.read = [&](std::uint64_t offset, std::uint64_t length) {
    ++payload_reads;
    return original_read(offset, length);
  };
  auto result = ingest(bytes, limits, source);
  ASSERT_TRUE(result.ok()) << result.error();
  const auto reads_after_ingestion = payload_reads;
  Hdf5FormatAdapter adapter(limits);
  EXPECT_TRUE(adapter.validate_layout_artifact(result.value().layout, bytes.size()).ok());
  EXPECT_EQ(payload_reads, reads_after_ingestion);
  const auto parsed = directory(result.value().layout, bytes.size());
  ASSERT_TRUE(parsed.ok()) << parsed.status().message();
  ASSERT_GT(parsed.value().pages().size(), 1u);
  auto corrupt = result.value().layout;
  const auto& final_page = parsed.value().pages().back();
  ASSERT_GT(final_page.length, 0u);
  corrupt[final_page.offset + final_page.length - 1] ^= 1;
  // The directory itself remains valid; the full proof must visit this page.
  ASSERT_TRUE(directory(corrupt, bytes.size()).ok());
  EXPECT_FALSE(adapter.validate_layout_artifact(corrupt, bytes.size()).ok());
  EXPECT_EQ(payload_reads, reads_after_ingestion);
}

TEST(Hdf5FormatAdapter, ArtifactProofRejectsWrongSizeFramingAndLimits) {
  const auto bytes = hdf_bytes();
  const auto result = ingest(bytes);
  ASSERT_TRUE(result.ok()) << result.error();
  Hdf5FormatAdapter adapter;
  EXPECT_FALSE(adapter.validate_layout_artifact(result.value().layout, bytes.size() + 1).ok());
  auto truncated = result.value().layout;
  truncated.pop_back();
  EXPECT_FALSE(adapter.validate_layout_artifact(truncated, bytes.size()).ok());
  auto trailing = result.value().layout;
  trailing.push_back(0);
  EXPECT_FALSE(adapter.validate_layout_artifact(trailing, bytes.size()).ok());
  auto previous_protocol = result.value().layout;
  previous_protocol[sizeof(kHdf5LayoutMagic) - 3] = '4';
  EXPECT_FALSE(adapter.validate_layout_artifact(previous_protocol, bytes.size()).ok());
  Hdf5LayoutLimits limits;
  limits.max_layout_bytes = result.value().layout.size() - 1;
  EXPECT_FALSE(Hdf5FormatAdapter(limits).validate_layout_artifact(
    result.value().layout, bytes.size()).ok());
}

TEST(Hdf5FormatAdapter, ArtifactProofRetainsExplicitUnavailableDirectory) {
  Hdf5LayoutDocument marker;
  marker.source_size = 1234;
  marker.directory_complete = false;
  marker.index_unavailable_reason = "resource_limit";
  auto encoded = serialize_hdf5_layout(marker);
  ASSERT_TRUE(encoded.ok());
  EXPECT_TRUE(Hdf5FormatAdapter().validate_layout_artifact(encoded.value(), 1234).ok());
  auto parsed = directory(encoded.value(), 1234);
  ASSERT_TRUE(parsed.ok());
  EXPECT_FALSE(parsed.value().document().directory_complete);
}

TEST(Hdf5FormatAdapter, IndexCapsYieldUnavailableWithinTheSameHardBudget) {
  const auto bytes = hdf_bytes();
  Hdf5LayoutDocument marker;
  marker.source_size = bytes.size();
  marker.directory_complete = false;
  marker.index_unavailable_reason = "resource_limit";
  auto encoded = serialize_hdf5_layout(marker);
  ASSERT_TRUE(encoded.ok());
  for (const bool byte_cap : {false, true}) {
    Hdf5LayoutLimits limits;
    if (byte_cap) limits.max_layout_bytes = encoded.value().size();
    else limits.max_objects = 1;
    const auto result = ingest(bytes, limits);
    ASSERT_TRUE(result.ok()) << result.error();
    EXPECT_LE(result.value().layout.size(), limits.max_layout_bytes);
    auto parsed = directory(result.value().layout, bytes.size());
    ASSERT_TRUE(parsed.ok()) << parsed.status().message();
    EXPECT_FALSE(parsed.value().document().directory_complete);
    EXPECT_TRUE(parsed.value().document().objects.empty());
    EXPECT_TRUE(parsed.value().document().links.empty());
    EXPECT_FALSE(parsed.value().document().index_unavailable_reason.empty());
    EXPECT_FALSE(std::get<bool>(result.value().metadata.at("hdf5.directory_complete").value()));
    EXPECT_EQ(result.value().metadata.count("hdf5.datasets"), 0u);
    EXPECT_EQ(result.value().metadata.count("hdf5.queryable_datasets"), 0u);
  }
  Hdf5LayoutLimits impossible;
  impossible.max_layout_bytes = 1;
  EXPECT_THROW(Hdf5FormatAdapter adapter(impossible), std::invalid_argument);
}

TEST(Hdf5FormatAdapter, PerDatasetChunkCapDoesNotClaimQueryableCoverage) {
  const auto bytes = hdf_bytes();
  Hdf5LayoutLimits limits;
  limits.max_chunks = 1;
  const auto result = ingest(bytes, limits);
  ASSERT_TRUE(result.ok()) << result.error();
  auto parsed = directory(result.value().layout, bytes.size());
  ASSERT_TRUE(parsed.ok());
  EXPECT_TRUE(parsed.value().document().directory_complete);
  EXPECT_EQ(std::get<std::int64_t>(result.value().metadata.at("hdf5.datasets").value()), 2);
  EXPECT_EQ(std::get<std::int64_t>(result.value().metadata.at("hdf5.queryable_datasets").value()), 0);
  EXPECT_FALSE(std::get<bool>(result.value().metadata.at("hdf5.index_complete").value()));
}

TEST(Hdf5FormatAdapter, OpeningBudgetFallbackCountsReadsBeforeInventoryCallback) {
  const auto bytes = hdf_bytes();
  auto source = source_for(bytes);
  auto read = source.read;
  std::uint64_t actual = 0;
  source.read = [&](std::uint64_t offset, std::uint64_t length) {
    auto result = read(offset, length);
    if (result) actual += result->size();
    return result;
  };
  Hdf5SourceOptions options;
  options.max_read_bytes = 8;
  const auto result = ingest(bytes, {}, source, options);
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_GT(actual, 0u);
  EXPECT_EQ(std::get<std::int64_t>(result.value().metadata.at("hdf5.inventory_read_bytes").value()), actual);
  EXPECT_FALSE(std::get<bool>(result.value().metadata.at("hdf5.directory_complete").value()));
}

TEST(Hdf5FormatAdapter, PublishesExactBytesOnceAndIdentitySurvivesWrongSuffixAndRename) {
  auto backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage(backend);
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()));
  ASSERT_TRUE(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()));
  IngestionModule ingestion(storage, formats, {113});
  const auto bytes = hdf_bytes(512);
  const StructuredBlobVersionKey key{"/scientific/arbitrary.arrow", 7};
  bool prepared = false;
  auto result = ingestion.ingest(key, source_for(bytes), [&](const IngestionOutcome& outcome) {
    const auto* pending = backend->peek(key);
    if (!pending) return false;
    EXPECT_FALSE(pending->manifest.published);
    EXPECT_EQ(pending->payload, bytes);
    EXPECT_FALSE(pending->layout.empty());
    EXPECT_EQ(outcome.format_id, std::optional<std::string>("hdf5"));
    prepared = true;
    return true;
  });
  ASSERT_TRUE(result.ok()) << result.message();
  EXPECT_TRUE(prepared);
  const auto* record = backend->peek(key);
  ASSERT_NE(record, nullptr);
  EXPECT_TRUE(record->manifest.published);
  EXPECT_EQ(record->manifest.format_id, std::optional<std::string>("hdf5"));
  EXPECT_EQ(record->payload, bytes);
  EXPECT_EQ(digest(record->payload), digest(bytes));
  EXPECT_EQ(backend->payload_bytes_written, bytes.size());
  EXPECT_TRUE(directory(record->layout, bytes.size()).ok());
  ASSERT_NE(formats.resolve_by_id("hdf5"), nullptr);
  auto metadata = serialize_metadata(result.outcome().metadata, 65536);
  ASSERT_TRUE(metadata.ok());
  Inode inode;
  inode.format_id = "hdf5";
  inode.metadata = metadata.value();
  Hdf5FormatAdapter adapter;
  EXPECT_EQ(adapter.get_xattr(7, "/renamed.no_extension", inode, "user.hdf5.container"), "hdf5");
  EXPECT_EQ(adapter.get_xattr(7, "/renamed.no_extension", inode, "user.hdf5.datasets"), "2");
  EXPECT_FALSE(adapter.get_xattr(7, "/renamed.h5", inode, "user.hdf5.dataset_paths"));
  inode.format_id.clear();
  EXPECT_TRUE(adapter.list_xattrs(7, "/renamed.h5", inode).empty());
}

TEST(Hdf5FormatAdapter, TinyAndMisleadingExtensionFilesRemainOrdinaryBytes) {
  std::uint64_t version = 1;
  for (const BinaryValue bytes : {BinaryValue{}, BinaryValue{'a','b','c'},
      BinaryValue{'R','E','A','D','M','E',' ','t','e','x','t'}}) {
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    ASSERT_TRUE(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()));
    ASSERT_TRUE(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()));
    IngestionModule ingestion(storage, formats, {3});
    const StructuredBlobVersionKey key{"/misleading.h5", static_cast<long long>(version++)};
    const auto result = ingestion.ingest(key, source_for(bytes), [](const IngestionOutcome&) { return true; });
    ASSERT_TRUE(result.ok()) << result.message();
    EXPECT_FALSE(result.outcome().format_id);
    ASSERT_NE(backend->peek(key), nullptr);
    EXPECT_EQ(backend->peek(key)->payload, bytes);
  }
}

TEST(Hdf5FormatAdapter, ShortAndFailedDetectionReadsRemainFatal) {
  const auto bytes = hdf_bytes();
  for (const bool short_read : {false, true}) {
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    ASSERT_TRUE(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()));
    IngestionModule ingestion(storage, formats, {19});
    auto source = source_for(bytes);
    source.read = [short_read](std::uint64_t, std::uint64_t length) -> std::optional<BinaryValue> {
      if (short_read) return BinaryValue(length - 1);
      return std::nullopt;
    };
    const StructuredBlobVersionKey key{"/failed.h5", 1};
    const auto result = ingestion.ingest(key, source, [](const IngestionOutcome&) { return true; });
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error(), IngestionError::SourceReadFailure);
    EXPECT_EQ(backend->peek(key), nullptr);
  }
}

TEST(Hdf5FormatAdapter, StructuralCorruptionAndInventoryIoFailureCannotPublish) {
  for (const bool corrupt : {false, true}) {
    auto bytes = hdf_bytes();
    if (corrupt) bytes[8] = 0xff; // Invalid superblock version, signature remains valid.
    auto backend = std::make_shared<MemoryBackend>();
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    ASSERT_TRUE(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()));
    IngestionModule ingestion(storage, formats, {19});
    auto source = source_for(bytes);
    const auto read = source.read;
    unsigned signatures = 0;
    if (!corrupt) source.read = [&](std::uint64_t offset, std::uint64_t length) -> std::optional<BinaryValue> {
      if (offset == 0 && length == 8 && ++signatures > 1) return std::nullopt;
      return read(offset, length);
    };
    const StructuredBlobVersionKey key{"/failed.h5", 1};
    bool prepared = false;
    const auto result = ingestion.ingest(key, source, [&](const IngestionOutcome&) { prepared = true; return true; });
    EXPECT_FALSE(result.ok());
    EXPECT_FALSE(prepared);
    EXPECT_EQ(backend->peek(key), nullptr);
    if (!corrupt) EXPECT_EQ(result.error(), IngestionError::SourceReadFailure);
  }
}

TEST(Hdf5FormatAdapter, StorageAndPublicationFailuresRemoveUnpublishedArtifacts) {
  const auto bytes = hdf_bytes();
  for (const auto fault : {MemoryBackend::Fault::PayloadWrite, MemoryBackend::Fault::Seal,
      MemoryBackend::Fault::LayoutWrite, MemoryBackend::Fault::MetadataWrite,
      MemoryBackend::Fault::Publish, MemoryBackend::Fault::None}) {
    auto backend = std::make_shared<MemoryBackend>();
    backend->set_fault(fault);
    StructuredBlobStorage storage(backend);
    FormatRegistry formats;
    ASSERT_TRUE(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()));
    IngestionModule ingestion(storage, formats, {19});
    const StructuredBlobVersionKey key{"/failed.h5", 1};
    const auto result = ingestion.ingest(key, source_for(bytes), [&](const IngestionOutcome&) {
      return fault != MemoryBackend::Fault::None;
    });
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(backend->peek(key), nullptr);
  }
}

TEST(Hdf5FormatAdapter, NativeProfileMatchesBoundedSourceObservations) {
  const auto bytes = hdf_bytes();
  auto source = source_for(bytes);
  const auto reader = source.read;
  std::uint64_t calls = 0, requested = 0;
  source.read = [&](std::uint64_t offset, std::uint64_t length) {
    ++calls; requested += length;
    return reader(offset, length);
  };
  QueryProfileRecord profile;
  {
    ScopedQueryProfile scope(&profile);
    auto artifacts = ingest(bytes, {}, source);
    ASSERT_TRUE(artifacts.ok()) << artifacts.error();
  }
  EXPECT_EQ(profile.counters["hdf5.native.source.calls"], calls);
  EXPECT_EQ(profile.counters["hdf5.native.source.bytes"], requested);
  EXPECT_GT(profile.nanoseconds["hdf5.native.lock_wait"], 0u);
  EXPECT_GT(profile.nanoseconds["hdf5.native.setup"], 0u);
  EXPECT_GT(profile.nanoseconds["hdf5.native.inventory"], 0u);
  EXPECT_GT(profile.nanoseconds["hdf5.native.source.read"], 0u);
}
