#include "../../support/InMemoryDatasetCatalog.h"
#include "iotdbfs/migration/Hdf5ReingestionPolicy.h"
#include "iotdbfs/migration/ReingestionService.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/Ingestion.h"
#include <gtest/gtest.h>
#include <hdf5.h>
#include <algorithm>
#include <fstream>
#include <iterator>
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
    bool checksum = false, std::uint64_t* first_chunk = nullptr, bool noncanonical_pipeline = false) {
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
      if (noncanonical_pipeline) {
        checked(H5Pset_deflate(properties, 1), "configure deflate");
        checked(H5Pset_shuffle(properties), "configure unsupported filter order");
      }
      H5Id values(H5Dcreate2(file, "/values", H5T_IEEE_F32LE, space,
        H5P_DEFAULT, properties, H5P_DEFAULT), H5Dclose);
      H5Id scalar(H5Screate(H5S_SCALAR), H5Sclose);
      H5Id attribute(H5Acreate2(values, "calibration", H5T_IEEE_F32LE, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
      const float calibration = 2;
      checked(H5Awrite(attribute, H5T_NATIVE_FLOAT, &calibration), "write optional attribute");
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

class MemoryBackend final : public StructuredBlobStorageBackend {
public:
  iotdbfs::test::InMemoryDatasetCatalog dataset_catalogs;
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& value) override { return dataset_catalogs.write(key, value); }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
      const StructuredBlobVersionKey& key) const override { return dataset_catalogs.read(key); }

  struct Record {
    StructuredBlobManifest manifest;
    StructuredBlobBytes payload;
    StructuredBlobBytes layout;
  };
  std::map<StructuredBlobVersionKey, Record> records;
  bool fail_reads{false};

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
    dataset_catalogs.erase(key);
    records.erase(found);
    return StructuredBlobStatus::success();
  }

private:
  static StructuredBlobStatus error(StructuredBlobError code) {
    return StructuredBlobStatus::failure(code, "memory backend error");
  }
  StructuredBlobResult<StructuredBlobBytes> read(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length, bool layout) const {
    if (fail_reads) return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::BackendFailure, "injected read failure");
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


class Hdf5ReingestionPolicyTest : public ::testing::Test {
protected:
  std::shared_ptr<MemoryBackend> backend = std::make_shared<MemoryBackend>();
  StructuredBlobStorage storage{backend};
  FormatRegistry formats;
  const std::shared_ptr<const ReingestionPolicy> policy = make_hdf5_reingestion_policy();
  const StructuredBlobVersionKey key{"/not-an-hdf5-extension.bin", 10};
  Hdf5ReingestionPolicyTest() { formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()); }
  void seed(const BinaryValue& bytes, bool indexed = false) {
    FormatRegistry empty;
    IngestionModule ingestion(storage, indexed ? formats : empty, {});
    FormatSource source{bytes.size(), [&bytes](std::uint64_t at, std::uint64_t size) -> std::optional<BinaryValue> {
      if (at > bytes.size() || size > bytes.size() - at) return std::nullopt;
      return BinaryValue(bytes.begin() + at, bytes.begin() + at + size);
    }};
    auto outcome = ingestion.ingest(key, source, [](const IngestionOutcome&) { return true; });
    ASSERT_TRUE(outcome.ok()) << outcome.message();
  }
  MigrationInspection inspect() {
    auto opened = storage.open_published_version(key);
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    return policy->inspect(opened.value(), formats);
  }
};

TEST_F(Hdf5ReingestionPolicyTest, ContentUserBlockAndPartialProfilesHaveDeterministicLayout) {
  seed(hdf_bytes(512, false, true));
  const auto first = inspect();
  ASSERT_EQ(first.kind, MigrationKind::Candidate) << first.detail;
  EXPECT_TRUE(first.partially_queryable);
  EXPECT_EQ(first.layout_sha256.size(), 64U);
  EXPECT_NE(first.profile_json.find("\"queryable_dataset_count\":1"), std::string::npos);
  EXPECT_NE(first.profile_json.find("/integer"), std::string::npos);
  EXPECT_NE(first.profile_json.find("\"id\":3"), std::string::npos);
  EXPECT_EQ(inspect().layout_sha256, first.layout_sha256);
  const auto bytes = backend->records.at(key).payload;
  const StructuredBlobVersionKey target{key.path, 20};
  IngestionModule ingestion(storage, formats, {});
  FormatSource source{bytes.size(), [&bytes](std::uint64_t at, std::uint64_t size) -> std::optional<BinaryValue> {
    return BinaryValue(bytes.begin()+at, bytes.begin()+at+size);
  }};
  ASSERT_TRUE(ingestion.ingest(target, source, [](const IngestionOutcome&) { return true; }).ok());
  auto opened = storage.open_published_version(target);
  ASSERT_TRUE(opened.ok());
  auto indexed = policy->inspect(opened.value(), formats);
  ASSERT_EQ(indexed.kind, MigrationKind::AlreadyIndexed) << indexed.detail;
  EXPECT_EQ(indexed.layout_sha256, first.layout_sha256);
  EXPECT_EQ(indexed.profile_json, first.profile_json);
}

TEST_F(Hdf5ReingestionPolicyTest, UnsupportedDatasetsAreNotCandidates) {
  seed(hdf_bytes(0, true));
  const auto result = inspect();
  EXPECT_EQ(result.kind, MigrationKind::Unsupported) << result.detail;
  EXPECT_FALSE(result.partially_queryable);
  EXPECT_NE(result.profile_json.find("\"queryable_dataset_count\":0"), std::string::npos);
}

TEST_F(Hdf5ReingestionPolicyTest, FilterWhitelistAloneDoesNotClaimExecutableSupport) {
  seed(hdf_bytes(0, false, false, nullptr, true));
  auto result = inspect();
  EXPECT_EQ(result.kind, MigrationKind::Unsupported) << result.detail;
  EXPECT_NE(result.profile_json.find("unsupported HDF5 filter order"), std::string::npos);
}

TEST_F(Hdf5ReingestionPolicyTest, ShortAndMisleadingContentIsNotHdf5) {
  seed({'h','5'});
  EXPECT_EQ(inspect().kind, MigrationKind::NotFormat);
}

TEST_F(Hdf5ReingestionPolicyTest, DetectionReadFailureIsAnError) {
  seed(hdf_bytes());
  backend->fail_reads = true;
  EXPECT_EQ(inspect().kind, MigrationKind::Error);
}

TEST_F(Hdf5ReingestionPolicyTest, SignatureWithCorruptNativeContentsIsAnError) {
  seed({0x89,'H','D','F','\r','\n',0x1a,'\n',0,0,0,0});
  EXPECT_EQ(inspect().kind, MigrationKind::Error);
}

TEST_F(Hdf5ReingestionPolicyTest, VerifiesChunkPagesNotOnlyDirectory) {
  seed(hdf_bytes(), true);
  ASSERT_EQ(inspect().kind, MigrationKind::AlreadyIndexed);
  auto& layout = backend->records.at(key).layout;
  ASSERT_FALSE(layout.empty());
  layout.back() ^= 1;
  auto result = inspect();
  EXPECT_EQ(result.kind, MigrationKind::Error);
  EXPECT_NE(result.detail.find("checksum"), std::string::npos);
}

TEST_F(Hdf5ReingestionPolicyTest, ManifestSummaryCannotContradictLayout) {
  seed(hdf_bytes(), true);
  backend->records.at(key).manifest.metadata["hdf5.datasets"] = MetadataValue(std::int64_t(999));
  auto result = inspect();
  EXPECT_EQ(result.kind, MigrationKind::Error);
  EXPECT_NE(result.detail.find("summary"), std::string::npos);
}

TEST_F(Hdf5ReingestionPolicyTest, ObsoleteLayoutsRebuildFromOriginalBytesWithoutChangingSource) {
  seed(hdf_bytes(), true);
  auto& record = backend->records.at(key);
  const auto original = record.layout;
  const auto payload = record.payload;
  const auto expected = inspect().layout_sha256;
  for (std::int64_t version : {1, 2, 3, 4}) {
    record.layout = original;
    const std::string magic = "IOTDBFS-HDF5-LAYOUT/" + std::to_string(version) + "\n";
    std::copy(magic.begin(), magic.end(), record.layout.begin());
    record.manifest.metadata["hdf5.layout_version"] = MetadataValue(version);
    auto result = inspect();
    EXPECT_EQ(result.kind, MigrationKind::Candidate) << result.detail;
    EXPECT_EQ(result.layout_sha256, expected);
    EXPECT_EQ(record.payload, payload);
    EXPECT_EQ(std::string(record.layout.begin(), record.layout.begin() + magic.size()), magic);
  }
  record.manifest.metadata["hdf5.layout_version"] = MetadataValue(std::int64_t(5));
  EXPECT_EQ(inspect().kind, MigrationKind::Error);
}

TEST_F(Hdf5ReingestionPolicyTest, MigratesLatestIndexedSourceToNewVersionAndReconciles) {
  seed(hdf_bytes(), true);
  auto& old = backend->records.at(key);
  const std::string old_magic = "IOTDBFS-HDF5-LAYOUT/3\n";
  std::copy(old_magic.begin(), old_magic.end(), old.layout.begin());
  old.manifest.metadata["hdf5.layout_version"] = MetadataValue(std::int64_t(3));
  const auto old_layout = old.layout;
  const auto old_payload = old.payload;
  std::map<StructuredBlobVersionKey, Inode> inodes;
  Inode inode;
  inode.path = key.path; inode.row_time = key.version;
  inode.size = static_cast<long long>(old.payload.size()); inode.format_id = "hdf5";
  inodes[key] = inode;
  ReingestionNamespace ns;
  ns.published_manifests = [&] {
    std::vector<StructuredBlobManifest> manifests;
    for (const auto& item : backend->records)
      if (item.second.manifest.published) manifests.push_back(item.second.manifest);
    return StructuredBlobResult<std::vector<StructuredBlobManifest>>::success(std::move(manifests));
  };
  ns.latest_inode = [&](const std::string& path) -> std::optional<Inode> {
    std::optional<Inode> result;
    for (const auto& item : inodes)
      if (item.first.path == path && backend->records.at(item.first).manifest.published &&
          (!result || item.first.version > result->row_time)) result = item.second;
    return result;
  };
  ns.exact_inode = [&](const StructuredBlobVersionKey& at) -> std::optional<Inode> {
    const auto found = inodes.find(at);
    return found == inodes.end() ? std::nullopt : std::optional<Inode>(found->second);
  };
  ns.prepare_inode = [&](const Inode& value) {
    inodes[{value.path, value.row_time}] = value; return true;
  };
  ns.next_version = [](std::int64_t value) { return value + 1; };
  ReingestionService service(storage, formats, ns, policy);
  auto inventory = service.inventory(key.path);
  ASSERT_TRUE(inventory.ok());
  ASSERT_EQ(inventory.value().size(), 1U);
  ASSERT_EQ(inventory.value().front().kind, MigrationKind::Candidate);
  auto result = service.migrate(inventory.value().front(), [](const MigrationResult&) { return true; });
  ASSERT_EQ(result.state, "verified") << result.error;
  EXPECT_EQ(result.destination.version, key.version + 1);
  EXPECT_EQ(backend->records.at(key).payload, old_payload);
  EXPECT_EQ(backend->records.at(key).layout, old_layout);
  EXPECT_EQ(backend->records.at(result.destination).payload, old_payload);
  EXPECT_EQ(service.reconcile(result).state, "verified");
  auto after = service.inventory(key.path);
  ASSERT_TRUE(after.ok());
  ASSERT_EQ(after.value().size(), 2U);
  EXPECT_EQ(after.value()[0].kind, MigrationKind::HistoricalVersion);
  EXPECT_EQ(after.value()[1].kind, MigrationKind::AlreadyIndexed);
}

TEST_F(Hdf5ReingestionPolicyTest, OptionalMetadataLimitsDoNotRemoveCandidate) {
  formats = FormatRegistry{};
  Hdf5LayoutLimits limits;
  limits.max_attributes = 0;
  formats.register_adapter(std::make_shared<Hdf5FormatAdapter>(limits));
  seed(hdf_bytes());
  EXPECT_EQ(inspect().kind, MigrationKind::Candidate);
}

TEST_F(Hdf5ReingestionPolicyTest, PhysicalIndexLimitsAreExplicitlyUnqueryable) {
  formats = FormatRegistry{};
  Hdf5LayoutLimits limits;
  limits.max_chunks = 0;
  formats.register_adapter(std::make_shared<Hdf5FormatAdapter>(limits));
  seed(hdf_bytes());
  auto result = inspect();
  EXPECT_EQ(result.kind, MigrationKind::Unsupported) << result.detail;
  EXPECT_NE(result.profile_json.find("queryable_dataset_count\":0"), std::string::npos);
  EXPECT_FALSE(result.layout_sha256.empty());
}

TEST_F(Hdf5ReingestionPolicyTest, UnavailableDirectoryRemainsIndexedWithExplicitUnknownSummary) {
  formats = FormatRegistry{};
  Hdf5LayoutLimits limits;
  limits.max_objects = 1;
  formats.register_adapter(std::make_shared<Hdf5FormatAdapter>(limits));
  seed(hdf_bytes(), true);
  auto result = inspect();
  ASSERT_EQ(result.kind, MigrationKind::AlreadyIndexed) << result.detail;
  EXPECT_NE(result.profile_json.find("\"directory_complete\":false"), std::string::npos);
  backend->records.at(key).manifest.metadata["hdf5.attributes_complete"] = MetadataValue(true);
  EXPECT_EQ(inspect().kind, MigrationKind::Error);
}

TEST_F(Hdf5ReingestionPolicyTest, RegisteredOtherFormatDoesNotGetReinterpreted) {
  seed(hdf_bytes(), true);
  backend->records.at(key).manifest.format_id = "other";
  EXPECT_EQ(inspect().kind, MigrationKind::OtherFormat);
}

TEST_F(Hdf5ReingestionPolicyTest, RefusesOversizedPersistedLayoutBeforeReadingIt) {
  seed(hdf_bytes(), true);
  backend->records.at(key).manifest.layout_size = Hdf5LayoutLimits{}.max_layout_bytes + 1;
  auto result = inspect();
  EXPECT_EQ(result.kind, MigrationKind::Error);
  EXPECT_NE(result.detail.find("byte limit"), std::string::npos);
}
} // namespace
