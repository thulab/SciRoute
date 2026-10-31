#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/IoTDBDatasetIndexValidation.h"
#include "iotdbfs/IoTDBDatasetCatalogCache.h"

#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <vector>

namespace {
using namespace iotdbfs;

DatasetCatalogArtifact catalog() {
  DatasetCatalogArtifact a;
  a.format_id = "hdf5";
  a.layout_protocol = "iotdbfs-hdf5-layout/5";
  a.layout_digest = std::string(64, 'a');
  DatasetCatalogRecord r;
  r.object_id = "dataset-1";
  r.canonical_path = "/data/temperature";
  r.attributes.push_back({"units", DatasetAttributeState::Complete,
                          DatasetAttributeValue(std::string("K")), {}, {}});
  r.attributes.push_back({"level", DatasetAttributeState::Complete,
                          DatasetAttributeValue(std::int64_t(850)), {}, {}});
  a.records.push_back(std::move(r));
  return a;
}

StructuredBlobManifest manifest() {
  StructuredBlobManifest m;
  m.key = {"/science/run.h5", 42};
  m.format_id = "hdf5";
  m.payload_size = 4096;
  m.layout_size = 256;
  m.phase = StructuredBlobPhase::Described;
  m.revision = 4;
  return m;
}
} // namespace

TEST(DatasetSubstructureIndex, SealIsDeterministicAndDescribesCanonicalBytes) {
  auto a = catalog();
  auto first = manifest();
  auto second = manifest();
  ASSERT_TRUE(seal_dataset_substructure_index(first, a).ok());
  ASSERT_TRUE(seal_dataset_substructure_index(second, a).ok());
  auto one = read_dataset_substructure_seal(first);
  auto two = read_dataset_substructure_seal(second);
  ASSERT_TRUE(one.ok());
  ASSERT_TRUE(two.ok());
  EXPECT_EQ(one.value().digest, two.value().digest);
  EXPECT_EQ(one.value().digest.size(), 64u);
  EXPECT_EQ(one.value().digest.find_first_not_of("0123456789abcdef"),
            std::string::npos);
  auto bytes = serialize_dataset_catalog(a);
  ASSERT_TRUE(bytes.ok());
  EXPECT_EQ(one.value().bytes, bytes.value().size());
  EXPECT_EQ(one.value().records, 1u);
  EXPECT_EQ(one.value().attributes, 2u);
  EXPECT_TRUE(validate_dataset_substructure_index(first, a).ok());
}

TEST(DatasetSubstructureIndex, LifecycleAndPosixChangesPreserveSeal) {
  auto a = catalog();
  auto m = manifest();
  ASSERT_TRUE(seal_dataset_substructure_index(m, a).ok());
  m.revision += 3;
  m.phase = StructuredBlobPhase::Published;
  m.published = true;
  m.metadata["mode"] = MetadataValue(std::int64_t(0640));
  m.metadata["uid"] = MetadataValue(std::int64_t(1000));
  m.metadata["gid"] = MetadataValue(std::int64_t(1000));
  m.metadata["description"] = MetadataValue("unrelated metadata");
  EXPECT_TRUE(validate_dataset_substructure_index(m, a).ok());
}

TEST(DatasetSubstructureIndex, BindsExactSourceFormatAndSizes) {
  const auto a = catalog();
  auto sealed = manifest();
  ASSERT_TRUE(seal_dataset_substructure_index(sealed, a).ok());
  const std::vector<std::function<void(StructuredBlobManifest&)>> edits{
      [](auto& m) { m.key.path = "/science/other.h5"; },
      [](auto& m) { ++m.key.version; },
      [](auto& m) { m.format_id = "arrow"; },
      [](auto& m) { m.format_id.reset(); },
      [](auto& m) { ++m.payload_size; },
      [](auto& m) { ++m.layout_size; }};
  for (std::size_t i = 0; i < edits.size(); ++i) {
    auto changed = sealed;
    edits[i](changed);
    EXPECT_FALSE(validate_dataset_substructure_index(changed, a).ok());
  }
}

TEST(DatasetSubstructureIndex, RejectsCatalogEditsEvenWhenCountsStayEqual) {
  auto sealed = manifest();
  const auto a = catalog();
  ASSERT_TRUE(seal_dataset_substructure_index(sealed, a).ok());
  const std::vector<std::function<void(DatasetCatalogArtifact&)>> edits{
      [](auto& c) { c.records[0].canonical_path = "/data/pressure"; },
      [](auto& c) { c.records[0].object_id = "dataset-2"; },
      [](auto& c) { c.records[0].attributes[0].value = std::string("C"); },
      [](auto& c) { c.records[0].attributes[1].value = std::uint64_t(850); },
      [](auto& c) { c.records[0].attributes.pop_back(); },
      [](auto& c) {
        auto r = c.records[0];
        r.object_id = "dataset-2";
        r.canonical_path = "/data/pressure";
        c.records.push_back(r);
      },
      [](auto& c) { c.records.clear(); },
      [](auto& c) { c.layout_digest[0] = 'b'; },
      [](auto& c) { c.layout_protocol = "iotdbfs-hdf5-layout/4"; },
      [](auto& c) { c.directory_complete = false; },
      [](auto& c) { c.records[0].attributes_complete = false; }};
  for (std::size_t i = 0; i < edits.size(); ++i) {
    auto changed = a;
    edits[i](changed);
    EXPECT_FALSE(validate_dataset_substructure_index(sealed, changed).ok());
  }
}

TEST(DatasetSubstructureIndex, MissingMalformedAndUnsupportedSealsFailClosed) {
  const auto a = catalog();
  auto sealed = manifest();
  EXPECT_FALSE(read_dataset_substructure_seal(sealed).ok());
  EXPECT_FALSE(validate_dataset_substructure_index(sealed, a).ok());
  ASSERT_TRUE(seal_dataset_substructure_index(sealed, a).ok());
  for (const std::string suffix : {"protocol", "sha256", "bytes", "records",
                                    "attributes"}) {
    auto missing = sealed;
    missing.metadata.erase("__iotdbfs.dataset_index." + suffix);
    EXPECT_FALSE(read_dataset_substructure_seal(missing).ok());
    EXPECT_FALSE(validate_dataset_substructure_index(missing, a).ok());
    auto wrong_type = sealed;
    wrong_type.metadata["__iotdbfs.dataset_index." + suffix] = MetadataValue(true);
    EXPECT_FALSE(read_dataset_substructure_seal(wrong_type).ok());
  }
  auto wrong_protocol = sealed;
  wrong_protocol.metadata["__iotdbfs.dataset_index.protocol"] =
      MetadataValue("iotdbfs-dataset-substructure-index/999");
  EXPECT_FALSE(read_dataset_substructure_seal(wrong_protocol).ok());
  auto wrong_digest = sealed;
  wrong_digest.metadata["__iotdbfs.dataset_index.sha256"] =
      MetadataValue(std::string(64, 'g'));
  EXPECT_FALSE(read_dataset_substructure_seal(wrong_digest).ok());
  for (const std::string suffix : {"bytes", "records", "attributes"}) {
    auto changed_count = sealed;
    changed_count.metadata["__iotdbfs.dataset_index." + suffix] =
        MetadataValue(std::int64_t(999));
    EXPECT_FALSE(validate_dataset_substructure_index(changed_count, a).ok());
    auto negative = sealed;
    negative.metadata["__iotdbfs.dataset_index." + suffix] =
        MetadataValue(std::int64_t(-1));
    EXPECT_FALSE(read_dataset_substructure_seal(negative).ok());
  }
}

TEST(DatasetSubstructureIndex, SealDoesNotClaimIncompleteDirectoryIsComplete) {
  auto a = catalog();
  a.directory_complete = false;
  a.records[0].attributes_complete = false;
  auto m = manifest();
  ASSERT_TRUE(seal_dataset_substructure_index(m, a).ok());
  EXPECT_TRUE(validate_dataset_substructure_index(m, a).ok());
  // The seal proves these exact declarations, not completeness. Discovery must
  // still reject directory_complete=false and unavailable predicate evidence.
  EXPECT_FALSE(a.directory_complete);
  EXPECT_FALSE(a.records[0].attributes_complete);
}

TEST(DatasetIndexPublication, ExactTypedRowsPassInAnyDatabaseOrder) {
  const auto a = catalog();
  std::vector<DatasetIndexEntryRow> entries{
      {"dataset-1", "/data/temperature", true}};
  std::vector<DatasetIndexAttributeRow> attributes{
      {"dataset-1", "level", DatasetAttributeState::Complete,
       DatasetAttributeValue(std::int64_t(850))},
      {"dataset-1", "units", DatasetAttributeState::Complete,
       DatasetAttributeValue(std::string("K"))}};
  EXPECT_TRUE(validate_dataset_index_rows(a, entries, attributes).ok());
}

TEST(DatasetIndexPublication, RejectsEqualCountSubstitutionsAndDuplicates) {
  auto a = catalog();
  auto other = a.records[0];
  other.object_id = "dataset-2";
  other.canonical_path = "/data/pressure";
  a.records.push_back(other);
  const std::vector<DatasetIndexEntryRow> entries{
      {"dataset-1", "/data/temperature", true},
      {"dataset-2", "/data/pressure", true}};
  const std::vector<DatasetIndexAttributeRow> attributes{
      {"dataset-1", "units", DatasetAttributeState::Complete,
       DatasetAttributeValue(std::string("K"))},
      {"dataset-1", "level", DatasetAttributeState::Complete,
       DatasetAttributeValue(std::int64_t(850))},
      {"dataset-2", "units", DatasetAttributeState::Complete,
       DatasetAttributeValue(std::string("K"))},
      {"dataset-2", "level", DatasetAttributeState::Complete,
       DatasetAttributeValue(std::int64_t(850))}};
  ASSERT_TRUE(validate_dataset_index_rows(a, entries, attributes).ok());
  auto wrong_value = attributes;
  wrong_value[0].value = std::string("C");
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, wrong_value).ok());
  auto wrong_type = attributes;
  wrong_type[1].value = std::uint64_t(850);
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, wrong_type).ok());
  auto null_value = attributes;
  null_value[1].value.reset();
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, null_value).ok());
  auto duplicate = attributes;
  duplicate[1] = duplicate[0];
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, duplicate).ok());
  auto wrong_object = attributes;
  wrong_object[0].object_id = "dataset-2";
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, wrong_object).ok());
  auto missing = attributes;
  missing.pop_back();
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, missing).ok());
  auto duplicate_entry = entries;
  duplicate_entry[1] = duplicate_entry[0];
  EXPECT_FALSE(validate_dataset_index_rows(a, duplicate_entry, attributes).ok());
  auto renamed = entries;
  renamed[0].dataset_path = "/data/other";
  EXPECT_FALSE(validate_dataset_index_rows(a, renamed, attributes).ok());
  auto incomplete = entries;
  incomplete[0].attributes_complete = false;
  EXPECT_FALSE(validate_dataset_index_rows(a, incomplete, attributes).ok());
}

TEST(DatasetIndexPublication, UnavailableAttributesRequireExactStateAndNullValue) {
  auto a = catalog();
  auto& unavailable = a.records[0].attributes[0];
  unavailable.state = DatasetAttributeState::TooLarge;
  unavailable.value.reset();
  const std::vector<DatasetIndexEntryRow> entries{
      {"dataset-1", "/data/temperature", true}};
  std::vector<DatasetIndexAttributeRow> attributes{
      {"dataset-1", "units", DatasetAttributeState::TooLarge, std::nullopt},
      {"dataset-1", "level", DatasetAttributeState::Complete,
       DatasetAttributeValue(std::int64_t(850))}};
  ASSERT_TRUE(validate_dataset_index_rows(a, entries, attributes).ok());
  attributes[0].state = DatasetAttributeState::Unsupported;
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, attributes).ok());
  attributes[0].state = DatasetAttributeState::TooLarge;
  attributes[0].value = std::string("hidden unexpected value");
  EXPECT_FALSE(validate_dataset_index_rows(a, entries, attributes).ok());
}

TEST(DatasetSubstructureIndex, ParsedCacheSeparatesSealsNamespacesAndVersions) {
  const auto m = manifest();
  const auto a = std::make_shared<const DatasetCatalogArtifact>(catalog());
  DatasetCatalogMarker marker;
  marker.key = m.key;
  marker.protocol = a->protocol;
  marker.layout_protocol = a->layout_protocol;
  marker.layout_digest = a->layout_digest;
  marker.record_count = a->records.size();
  marker.attribute_count = 2;
  auto bytes = serialize_dataset_catalog(*a);
  ASSERT_TRUE(bytes.ok());
  marker.size = bytes.value().size();
  const auto sealed = make_dataset_catalog_cache_identity(1, m, marker,
      std::string(64, 'a'));
  const auto other_seal = make_dataset_catalog_cache_identity(1, m, marker,
      std::string(64, 'b'));
  const auto other_namespace = make_dataset_catalog_cache_identity(2, m, marker,
      std::string(64, 'a'));
  auto other_marker = marker;
  ++other_marker.key.version;
  auto other_manifest = m;
  ++other_manifest.key.version;
  const auto other_version = make_dataset_catalog_cache_identity(1,
      other_manifest, other_marker, std::string(64, 'a'));
  DatasetCatalogParsedCache cache;
  EXPECT_FALSE(cache.get(sealed));
  ASSERT_TRUE(cache.put_validated(sealed, a));
  EXPECT_EQ(cache.get(sealed), a);
  EXPECT_FALSE(cache.get(other_seal));
  EXPECT_FALSE(cache.get(other_namespace));
  EXPECT_FALSE(cache.get(other_version));
  EXPECT_EQ(cache.size(), 1u);
}

TEST(DatasetSubstructureIndex, ParsedCacheBoundsResidentBytesAndKeepsBorrowedArtifactAlive) {
  const auto m = manifest();
  const auto a = std::make_shared<const DatasetCatalogArtifact>(catalog());
  DatasetCatalogMarker marker;
  marker.key = m.key;
  marker.protocol = a->protocol;
  marker.layout_protocol = a->layout_protocol;
  marker.layout_digest = a->layout_digest;
  const auto first = make_dataset_catalog_cache_identity(1, m, marker,
      std::string(64, 'a'));
  const auto second = make_dataset_catalog_cache_identity(2, m, marker,
      std::string(64, 'a'));
  const auto charge = estimate_dataset_catalog_cache_bytes(first, *a);
  DatasetCatalogParsedCache cache(100, charge);
  ASSERT_TRUE(cache.put_validated(first, a));
  auto borrowed = cache.get(first);
  ASSERT_TRUE(borrowed);
  ASSERT_TRUE(cache.put_validated(second, a));
  EXPECT_FALSE(cache.get(first));
  EXPECT_TRUE(cache.get(second));
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_LE(cache.memory_bytes(), charge);
  EXPECT_EQ(cache.evictions(), 1u);
  EXPECT_EQ(borrowed->records[0].canonical_path, "/data/temperature");
  DatasetCatalogParsedCache too_small(100, charge - 1);
  EXPECT_FALSE(too_small.put_validated(first, a));
  EXPECT_EQ(too_small.size(), 0u);
  EXPECT_EQ(too_small.memory_bytes(), 0u);
}
