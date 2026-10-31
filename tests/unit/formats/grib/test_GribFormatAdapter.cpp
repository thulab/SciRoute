#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include "iotdbfs/MetadataSerialization.h"

#include <gtest/gtest.h>

#include <eccodes.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace iotdbfs;

std::vector<std::uint8_t> make_valid_grib2_message() {
  codes_handle* handle = codes_grib_handle_new_from_samples(nullptr, "GRIB2");
  if (!handle) {
    ADD_FAILURE() << "ecCodes GRIB2 sample unavailable";
    return {};
  }
  const void* message = nullptr;
  std::size_t size = 0;
  const int error = codes_get_message(handle, &message, &size);
  std::vector<std::uint8_t> bytes;
  if (error == 0 && message && size > 0) {
    const auto* begin = static_cast<const std::uint8_t*>(message);
    bytes.assign(begin, begin + size);
  }
  else {
    ADD_FAILURE() << "failed to get ecCodes GRIB2 sample message";
  }
  codes_handle_delete(handle);
  return bytes;
}

FormatSource source_for(const std::vector<std::uint8_t>& bytes) {
  return { bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length)
      -> std::optional<std::vector<std::uint8_t>> {
    if (offset > bytes.size() || length > bytes.size() - offset)
      return std::nullopt;
    return std::vector<std::uint8_t>(bytes.begin() + offset,
      bytes.begin() + offset + length);
  } };
}

GribFormatAdapter::Config test_config() {
  GribFormatAdapter::Config config;
  config.ingestion.chunk_size = 16;
  config.ingestion.process_threshold_bytes = 16;
  config.ingestion.process_retry_bytes = 8;
  config.ingestion.window_reserve_bytes = 128;
  config.ingestion.detect_threshold_bytes = 16;
  return config;
}

} // namespace

TEST(GribFormatAdapter, DeclaresFormatIdentityAndGenericQueryViews) {
  GribFormatAdapter adapter(test_config());
  EXPECT_EQ(adapter.id(), "grib");
  EXPECT_TRUE(adapter.can_handle_virtual_query("forecast.grib2"));
  EXPECT_TRUE(adapter.can_handle_virtual_query("renamed.bin"));

  auto range = adapter.parse_query(
    "function=read&type=binary&shortName=t");
  ASSERT_TRUE(range);
  EXPECT_EQ(range->type, FormatViewType::Range);
  auto materialized = adapter.parse_query(
    "function=read&type=data&shortName=t");
  ASSERT_TRUE(materialized);
  EXPECT_EQ(materialized->type, FormatViewType::Materialized);
  EXPECT_FALSE(adapter.parse_query("function=sum&shortName=t"));
}

TEST(GribFormatAdapter, DetectsContentAndDerivesTypedArtifacts) {
  auto config = test_config();
  const auto metadata_config = MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.edition.source=edition\n"
    "metadata.field.edition.type=int64\n");
  ASSERT_TRUE(metadata_config.ok()) << metadata_config.error();
  config.ingestion.metadata_config =
    std::make_shared<const MetadataConfig>(metadata_config.value());
  GribFormatAdapter adapter(std::move(config));
  const auto message = make_valid_grib2_message();
  ASSERT_FALSE(message.empty());
  EXPECT_TRUE(adapter.matches(source_for(message)));
  const std::vector<std::uint8_t> unrecognized{
    'n', 'o', 't', '-', 'g', 'r', 'i', 'b'
  };
  EXPECT_FALSE(adapter.matches(source_for(unrecognized)));

  const auto source = source_for(message);
  auto session = adapter.begin_ingestion(source);
  ASSERT_TRUE(session);
  ASSERT_TRUE(session->consume_chunk(
    0, message.data(), message.size()).ok());
  auto derived = session->finish();
  ASSERT_TRUE(derived.ok()) << derived.error();
  const std::string layout(derived.value().layout.begin(),
    derived.value().layout.end());
  EXPECT_NE(layout.find("\"ed\":2"), std::string::npos);
  EXPECT_TRUE(adapter.validate_layout_artifact(derived.value().layout, message.size()).ok());

  const auto& metadata = derived.value().metadata;
  for (const char* key : {"shortName", "typeOfProcessedData",
      "typeOfGeneratingProcess", "typeOfLevel", "centre",
      "generatingProcessIdentifier", "discipline", "parameterCategory",
      "parameterNumber", "unit", "latitudeOfFirstGridPoint",
      "longitudeOfFirstGridPoint", "latitudeOfLastGridPoint",
      "longitudeOfLastGridPoint", "globalDomain", "iDirectionIncrement",
      "jDirectionIncrement", "Ni", "Nj", "numberOfDataPoints"}) {
    EXPECT_NE(metadata.find(key), metadata.end()) << key;
  }
  const auto points = metadata.find("numberOfDataPoints");
  ASSERT_NE(points, metadata.end());
  EXPECT_EQ(points->second.type(), MetadataValueType::Int64Set);
  const auto& point_values =
    std::get<MetadataInt64Set>(points->second.value());
  ASSERT_EQ(point_values.size(), 1u);
  EXPECT_GT(*point_values.begin(), 0);
  EXPECT_NE(metadata.find("shortName"), metadata.end());
  EXPECT_NE(metadata.find("typeOfLevel"), metadata.end());
  const auto edition = metadata.find("edition");
  ASSERT_NE(edition, metadata.end());
  EXPECT_EQ(edition->second.type(), MetadataValueType::Int64Set);
  EXPECT_EQ(std::get<MetadataInt64Set>(edition->second.value()),
    MetadataInt64Set({2}));
}

TEST(GribFormatAdapter, OwnsGribXattrNames) {
  GribFormatAdapter adapter(test_config());
  Inode inode;
  inode.format_id = "grib";
  inode.metadata = R"({"numberOfDataPoints":{"type":"int64_set","value":[42]},"shortName":{"type":"string_set","value":["t","u"]}})";
  const auto attrs = adapter.list_xattrs(1, "forecast.grib2", inode);
  EXPECT_FALSE(attrs.empty());
  auto short_name = adapter.get_xattr(1, "forecast.grib2", inode,
    "user.grib.shortName");
  ASSERT_TRUE(short_name);
  EXPECT_EQ(*short_name, "[\"t\",\"u\"]");
  auto renamed = adapter.get_xattr(1, "renamed.bin", inode,
    "user.grib.shortName");
  ASSERT_TRUE(renamed);
  EXPECT_EQ(*renamed, "[\"t\",\"u\"]");
}

TEST(GribFormatAdapter, ValidatesCompleteLayoutWithoutReadingPayload) {
  GribFormatAdapter adapter(test_config());
  auto validate = [&](const std::string& records, std::uint64_t size = 16) {
    const auto text = std::string("IOTDBFS-GRIB-LAYOUT/1\n") + records;
    return adapter.validate_layout_artifact({text.begin(), text.end()}, size);
  };
  EXPECT_TRUE(validate(R"({"of":0,"ln":16,"ed":2,"lv":-1,"pi":12,"vd":20261016,"vt":600})").ok());
  EXPECT_TRUE(validate(R"({"offset":0,"totalLength":16,"edition":2,"shortName":"t"})").ok());
  for (const auto* record : {
      R"({"of":0,"ln":16,"ed":2)" ,
      R"({"of":0,"ln":16,"ed":2}trailing)",
      R"({"of":0,"ln":16,"ed":2.5})",
      R"({"of":0,"ln":16,"ed":"2"})",
      R"({"of":0,"ln":16,"ed":2,"lv":"bad"})",
      R"({"of":0,"ln":16,"ed":2,"pi":9223372036854775808})",
      R"({"of":0,"ln":16,"ed":2147483648})",
      R"({"of":0,"ln":16,"ed":2,"ed":1})",
      R"({"of":0,"ln":16,"ed":2,"edition":2})",
      R"({"of":0,"ln":16,"ed":2,"sn":"bad\x"})",
      R"({"of":0,"ln":16,"ed":2,"sn":"\ud800"})",
      R"({"of":0,"ln":16,"ed":2,"ni":"bad"})",
      R"({"of":0,"ln":16,"ed":2,"np":1e2})",
      R"({"of":0,"ln":16,"ed":2,"vd":null})",
      R"({"of":0,"ln":16,"ed":2,"vt":true})",
      R"({"of":0,"ln":16})",
      R"({"of":-1,"ln":16,"ed":2})",
      R"({"of":1,"ln":16,"ed":2})",
      R"({"of":0,"ln":0,"ed":2})",
      R"({"of":00,"ln":16,"ed":2})",
      R"({"of":0,"ln":16,"ed" :2})",
      R"({"of":0,"ln":16,"\u0065d":2})"}) {
    EXPECT_FALSE(validate(record).ok()) << record;
  }
  EXPECT_FALSE(validate("{\"of\":0,\"ln\":16,\"ed\":2}\nmalformed\n").ok());
  const std::vector<std::uint8_t> no_header{'{','}'};
  EXPECT_FALSE(adapter.validate_layout_artifact(no_header, 16).ok());
}
