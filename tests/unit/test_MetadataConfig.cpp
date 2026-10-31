#include "iotdbfs/MetadataConfig.h"

#include <gtest/gtest.h>

#include <map>
#include <limits>
#include <fstream>
#include <cstdlib>
#include "iotdbfs/Filesystem.h"
#include <string>
#include "TestSupport.h"

using namespace iotdbfs;

TEST(MetadataConfig, ParsesTypedFieldsInDeterministicOrder) {
  const std::string text =
    "metadata.schema_version=1\n"
    "metadata.field.experimentId.source=experimentId\n"
    "metadata.field.experimentId.type=string\n"
    "metadata.field.experimentId.xattr=user.grib.experimentId\n"
    "metadata.field.experimentId.queryable=true\n"
    "metadata.field.quality.source=quality\n"
    "metadata.field.quality.type=double\n";
  auto result = MetadataConfig::parse(text);
  ASSERT_TRUE(result.ok()) << result.error();
  ASSERT_EQ(result.value().fields().size(), 2u);
  EXPECT_EQ(result.value().fields()[0].id, "experimentId");
  EXPECT_EQ(result.value().fields()[1].id, "quality");
  EXPECT_EQ(result.value().fields()[0].type, MetadataValueType::String);
  EXPECT_TRUE(result.value().fields()[0].queryable);
}

TEST(MetadataConfig, RejectsDuplicateAndInvalidDefinitions) {
  auto duplicate = MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.a.source=x\nmetadata.field.a.type=string\n"
    "metadata.field.a.source=y\nmetadata.field.a.type=string\n");
  EXPECT_FALSE(duplicate.ok());

  auto invalid = MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.bad-id.source=x\nmetadata.field.bad-id.type=wat\n"
    "metadata.field.bad-id.xattr=user.grib.bad-id\n");
  EXPECT_FALSE(invalid.ok());
}

TEST(MetadataConfig, AppliesTypedDefaultsRegardlessOfPropertyOrder) {
  auto result = MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.flag.default=true\n"
    "metadata.field.flag.required=true\n"
    "metadata.field.flag.source=flag\n"
    "metadata.field.flag.type=bool\n");
  ASSERT_TRUE(result.ok()) << result.error();
  ASSERT_EQ(result.value().fields().size(), 1u);
  ASSERT_TRUE(result.value().fields()[0].default_value.has_value());
  EXPECT_EQ(result.value().fields()[0].default_value->type(), MetadataValueType::Bool);
}

TEST(MetadataValue, SerializesCanonicalJsonAndRejectsNonFinite) {
  MetadataMap values;
  values.emplace("quality", MetadataValue(1.25));
  values.emplace("experimentId", MetadataValue("alpha\n\"beta"));
  const auto json = serialize_metadata(values, 4096);
  ASSERT_TRUE(json.ok()) << json.error();
  EXPECT_EQ(json.value(),
    "{\"experimentId\":{\"type\":\"string\",\"value\":\"alpha\\n\\\"beta\"},"
    "\"quality\":{\"type\":\"double\",\"value\":1.25}}");

  auto bad = MetadataValue(std::numeric_limits<double>::quiet_NaN());
  values["bad"] = bad;
  EXPECT_FALSE(serialize_metadata(values, 4096).ok());
}

TEST(MetadataValue, EnforcesSizeCap) {
  MetadataMap values;
  values.emplace("x", MetadataValue(std::string(64, 'a')));
  auto result = serialize_metadata(values, 8);
  EXPECT_FALSE(result.ok());
}

TEST(MetadataConfig, RejectsUnsafeOrIncompleteSchema) {
  const std::string prefix = "metadata.schema_version=1\n";
  const std::vector<std::string> invalid = {
    "metadata.field.x.source=edition\n", // no type
    "metadata.field.x.source=edition\nmetadata.field.x.type=wat\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=int64\nmetadata.field.x.default=12junk\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=double\nmetadata.field.x.default=nan\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=double\nmetadata.field.x.scale=1junk\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=string\nmetadata.field.x.scale=2\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=string\nmetadata.field.x.xattr=user.bad name\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=string\nmetadata.field.x.xattr=user.grib.index.keys\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=int64\nmetadata.field.x.queryable=true\n",
    "metadata.field.x.source=bad;key\nmetadata.field.x.type=int64\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=int64\nmetadata.field.x.aliases=edition,,centre\n",
    "metadata.unknown=true\n",
    "metadata.field.x.source=edition\nmetadata.field.x.type=int64\nmetadata.field.x.xattr=user.grib.same\n"
    "metadata.field.y.source=centre\nmetadata.field.y.type=int64\nmetadata.field.y.xattr=user.grib.same\n"
  };
  for (const auto& text : invalid)
    EXPECT_FALSE(MetadataConfig::parse(prefix + text).ok()) << text;
}

namespace {
struct ConfigDirectory {
  std::string path;
  std::optional<std::string> old_override;
  ConfigDirectory() {
    if (const char* value = std::getenv("IOTDB_METADATA_CONFIG_FILE")) old_override = value;
    unsetenv("IOTDB_METADATA_CONFIG_FILE");
    std::string pattern = (fs::temp_directory_path() / "iotdbfs-metadata-XXXXXX").string();
    auto* created = mkdtemp(pattern.data());
    if (!created) throw std::runtime_error("mkdtemp failed");
    path = created;
  }
  ~ConfigDirectory() {
    std::error_code ec;
    fs::remove_all(path, ec);
    if (old_override) setenv("IOTDB_METADATA_CONFIG_FILE", old_override->c_str(), 1);
    else unsetenv("IOTDB_METADATA_CONFIG_FILE");
  }
  void write(const std::string& name, const std::string& text) {
    std::ofstream out(path + "/" + name); out << text;
  }
};
}

TEST(MetadataConfig, ResolvesFilesRelativeToMainConfigAndEnvironmentOverride) {
  ConfigDirectory dir;
  const auto main = dir.path + "/config.properties";
  EXPECT_TRUE(load_metadata_config(main).ok());
  dir.write("metadata.properties", "metadata.schema_version=invalid\n");
  EXPECT_FALSE(load_metadata_config(main).ok());
  dir.write("custom.properties", "metadata.schema_version=1\nmetadata.field.ed.source=edition\nmetadata.field.ed.type=int64\n");
  dir.write("config.properties", "metadata.config = custom.properties\n");
  auto relative = load_metadata_config(main);
  ASSERT_TRUE(relative.ok()) << relative.error();
  ASSERT_EQ(relative.value().fields().size(), 1u);
  EXPECT_EQ(relative.value().fields()[0].id, "ed");
  setenv("IOTDB_METADATA_CONFIG_FILE", (dir.path + "/missing").c_str(), 1);
  EXPECT_FALSE(load_metadata_config(main).ok());
  setenv("IOTDB_METADATA_CONFIG_FILE", (dir.path + "/custom.properties").c_str(), 1);
  dir.write("config.properties", "metadata.config=missing\n");
  EXPECT_TRUE(load_metadata_config(main).ok());
}

TEST(MetadataValue, WriterRejectsAnythingTheReaderCannotRepresent) {
  MetadataMap values;
  values.emplace("x", MetadataValue(std::string(4097, 'a')));
  EXPECT_FALSE(serialize_metadata(values, 1000000).ok());
  values["x"] = MetadataValue(std::string("\xff"));
  EXPECT_FALSE(serialize_metadata(values, 1000000).ok());
  values.clear();
  values.emplace(std::string(65, 'k'), MetadataValue(true));
  EXPECT_FALSE(serialize_metadata(values, 1000000).ok());
  values.clear();
  for (int i = 0; i < 129; ++i)
    values.emplace("k" + std::to_string(i), MetadataValue(true));
  EXPECT_FALSE(serialize_metadata(values, 1000000).ok());
  values.clear();
  for (int i = 0; i < 20; ++i)
    values.emplace("k" + std::to_string(i), MetadataValue(std::string(4000, 'a')));
  EXPECT_FALSE(serialize_metadata(values, 1000000).ok());
}

TEST(MetadataConfig, ConfigurationByteCapAcceptsExactBoundary) {
  const std::string header = "metadata.schema_version=1\n#";
  const auto exact = header + std::string(65536 - header.size(), 'x');
  EXPECT_TRUE(MetadataConfig::parse(exact).ok());
  EXPECT_FALSE(MetadataConfig::parse(exact + "x").ok());
  test_support::TempDirectory dir;
  EXPECT_TRUE(MetadataConfig::load_file(dir.write("exact", exact)).ok());
  EXPECT_FALSE(MetadataConfig::load_file(dir.write("large", exact + "x")).ok());
}

TEST(MetadataConfig, RejectsNulInvalidUtf8AndMalformedTopLevelProperties) {
  for (const std::string& text : {std::string("metadata.schema_version=1\n\0", 27),
      std::string("metadata.schema_version=1\n#\xff"), std::string(""),
      std::string("metadata.schema_version=2\n"),
      std::string("metadata.schema_version=1\nmissing_equals\n"),
      std::string("metadata.schema_version=1\nmetadata.field.a=bad\n")}) {
    SCOPED_TRACE(::testing::PrintToString(text));
    EXPECT_FALSE(MetadataConfig::parse(text).ok());
  }
}

TEST(MetadataConfig, FieldCountBoundaryAccepts128AndRejects129) {
  std::string text = "metadata.schema_version=1\n";
  for (int i = 0; i < 128; ++i) {
    const auto prefix = "metadata.field.f" + std::to_string(i);
    text += prefix + ".source=edition\n" + prefix + ".type=int64\n";
  }
  const auto result = MetadataConfig::parse(text);
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_EQ(result.value().fields().size(), 128u);
  text += "metadata.field.extra.source=edition\nmetadata.field.extra.type=int64\n";
  EXPECT_FALSE(MetadataConfig::parse(text).ok());
}

TEST(MetadataConfig, IdentifierAndSourceLengthsEnforceExactBoundaries) {
  const auto make = [](const std::string& id, const std::string& source) {
    const auto prefix = "metadata.field." + id;
    return "metadata.schema_version=1\n" + prefix + ".source=" + source + "\n" + prefix + ".type=string\n";
  };
  EXPECT_TRUE(MetadataConfig::parse(make(std::string(64, 'a'), std::string(128, 'b'))).ok());
  EXPECT_FALSE(MetadataConfig::parse(make(std::string(65, 'a'), "b")).ok());
  EXPECT_FALSE(MetadataConfig::parse(make("a", std::string(129, 'b'))).ok());
  EXPECT_TRUE(MetadataConfig::parse(make("a", "_internal")).ok());
  EXPECT_FALSE(MetadataConfig::parse(make("_internal", "a")).ok());
}

TEST(MetadataConfig, AliasLimitAcceptsEightAndRejectsDuplicateOrNinth) {
  const std::string prefix = "metadata.schema_version=1\nmetadata.field.a.source=edition\n"
    "metadata.field.a.type=int64\nmetadata.field.a.aliases=";
  const auto result = MetadataConfig::parse(prefix + "a,b,c,d,e,f,g,h\n");
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_EQ(result.value().fields()[0].aliases.size(), 8u);
  for (const auto* aliases : {"a,b,c,d,e,f,g,h,i", "a,a", "edition", "", "a,", ",a"}) {
    SCOPED_TRACE(aliases);
    EXPECT_FALSE(MetadataConfig::parse(prefix + aliases).ok());
  }
}

TEST(MetadataConfig, RejectsCaseCollisionsInIdsAndXattrs) {
  EXPECT_FALSE(MetadataConfig::parse(
    "metadata.schema_version=1\nmetadata.field.A.source=a\nmetadata.field.A.type=string\n"
    "metadata.field.a.source=b\nmetadata.field.a.type=string\n").ok());
  EXPECT_FALSE(MetadataConfig::parse(
    "metadata.schema_version=1\nmetadata.field.a.source=a\nmetadata.field.a.type=string\n"
    "metadata.field.a.xattr=user.grib.VALUE\nmetadata.field.b.source=b\n"
    "metadata.field.b.type=string\nmetadata.field.b.xattr=user.grib.value\n").ok());
}

TEST(MetadataConfig, ParsesExactTypedDefaultsAndScale) {
  const auto result = MetadataConfig::parse(
    "metadata.schema_version=1\nmetadata.field.a.source=a\nmetadata.field.a.type=int64\n"
    "metadata.field.a.default=-9223372036854775808\n"
    "metadata.field.b.source=b\nmetadata.field.b.type=double\nmetadata.field.b.default=-1.25e2\n"
    "metadata.field.b.scale=0.01\nmetadata.field.c.source=c\nmetadata.field.c.type=bool\n"
    "metadata.field.c.default=no\nmetadata.field.c.required=yes\n"
    "metadata.field.d.source=d\nmetadata.field.d.type=string\nmetadata.field.d.default=天气\n");
  ASSERT_TRUE(result.ok()) << result.error();
  const auto& fields = result.value().fields();
  ASSERT_EQ(fields.size(), 4u);
  for (const auto& field : fields) ASSERT_TRUE(field.default_value);
  EXPECT_EQ(std::get<std::int64_t>(fields[0].default_value->value()), std::numeric_limits<std::int64_t>::min());
  EXPECT_DOUBLE_EQ(std::get<double>(fields[1].default_value->value()), -125.0);
  EXPECT_DOUBLE_EQ(fields[1].scale, 0.01);
  EXPECT_FALSE(std::get<bool>(fields[2].default_value->value()));
  EXPECT_TRUE(fields[2].required);
  EXPECT_EQ(std::get<std::string>(fields[3].default_value->value()), "天气");
}

TEST(MetadataConfig, RejectsOverflowDefaultsInvalidBooleansAndOversizedStrings) {
  for (const auto& pair : std::vector<std::pair<std::string, std::string>>{
      {"int64", "9223372036854775808"}, {"int64", "-9223372036854775809"},
      {"bool", "maybe"}, {"double", "1e999"}, {"double", "inf"},
      {"string", std::string(4097, 'x')}}) {
    SCOPED_TRACE(pair.first);
    EXPECT_FALSE(MetadataConfig::parse("metadata.schema_version=1\nmetadata.field.a.source=a\n"
      "metadata.field.a.type=" + pair.first + "\nmetadata.field.a.default=" + pair.second).ok());
  }
  const std::string valid = "metadata.schema_version=1\nmetadata.field.a.source=a\nmetadata.field.a.type=string\n";
  EXPECT_FALSE(MetadataConfig::parse(valid + "metadata.field.a.required=maybe").ok());
  EXPECT_FALSE(MetadataConfig::parse(valid + "metadata.field.a.queryable=maybe").ok());
  EXPECT_TRUE(MetadataConfig::parse(valid + "metadata.field.a.default=" + std::string(4096, 'x')).ok());
  EXPECT_FALSE(MetadataConfig::parse(valid + "metadata.field.a.xattr=user." + std::string(251, 'x')).ok());
  EXPECT_TRUE(MetadataConfig::parse(valid + "metadata.field.a.xattr=user." + std::string(250, 'x')).ok());
}

TEST(MetadataConfig, RejectsExplicitEmptyOrMissingFilesAndLoadsAbsolutePath) {
  ConfigDirectory dir;
  const auto main = dir.path + "/config.properties";
  dir.write("config.properties", "metadata.config=\n");
  EXPECT_FALSE(load_metadata_config(main).ok());
  dir.write("config.properties", "metadata.config=missing.properties\n");
  EXPECT_FALSE(load_metadata_config(main).ok());
  dir.write("absolute.properties", "metadata.schema_version=1\n");
  dir.write("config.properties", "metadata.config=" + dir.path + "/absolute.properties\n");
  auto result = load_metadata_config(main);
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_TRUE(result.value().fields().empty());
}
