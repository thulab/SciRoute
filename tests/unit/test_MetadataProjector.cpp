#include "iotdbfs/MetadataProjector.h"
#include "iotdbfs/MetadataSerialization.h"

#include <algorithm>
#include <gtest/gtest.h>
#include <limits>

using namespace iotdbfs;

TEST(MetadataProjector, ProjectsConfiguredValuesToXattrs) {
  const auto parsed = MetadataConfig::parse(
      "metadata.schema_version=1\n"
      "metadata.field.name.source=shortName\n"
      "metadata.field.name.type=string\n"
      "metadata.field.name.xattr=user.weather.name\n"
      "metadata.field.count.source=count\n"
      "metadata.field.count.type=int64\n"
      "metadata.field.count.xattr=user.weather.count\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataMap values{{"name", MetadataValue("temperature")},
                     {"count", MetadataValue(std::int64_t{3})},
                     {"unknown", MetadataValue("secret")}};

  MetadataProjector projector(parsed.value());
  const auto attrs = projector.to_xattrs(values);
  ASSERT_EQ(attrs.size(), 2u);
  auto find = [&](const std::string& key) {
    return std::find_if(attrs.begin(), attrs.end(), [&](const auto& item) {
      return item.first == key;
    });
  };
  ASSERT_NE(find("user.weather.name"), attrs.end());
  EXPECT_EQ(find("user.weather.name")->second, "temperature");
  ASSERT_NE(find("user.weather.count"), attrs.end());
  EXPECT_EQ(find("user.weather.count")->second, "3");
}

TEST(MetadataProjector, ProjectsCanonicalFieldOnlyWhenConfigured) {
  const auto parsed = MetadataConfig::parse(
      "metadata.schema_version=1\n"
      "metadata.field.name.source=shortName\n"
      "metadata.field.name.type=string\n"
      "metadata.field.name.xattr=user.grib.shortName\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataMap values{{"name", MetadataValue("new")},
                     {"shortName", MetadataValue("implicit")}};
  MetadataProjector projector(parsed.value());
  const auto attrs = projector.to_xattrs(values);
  ASSERT_EQ(attrs.size(), 1u);
  EXPECT_EQ(attrs.front().second, "new");
}

TEST(MetadataProjector, WebProjectionIncludesOnlyConfiguredValues) {
  const auto parsed = MetadataConfig::parse(
      "metadata.schema_version=1\n"
      "metadata.field.name.source=shortName\n"
      "metadata.field.name.type=string\n"
      "metadata.field.name.queryable=true\n"
      "metadata.field.name.xattr=user.weather.name\n"
      "metadata.field.hidden.source=hidden\n"
      "metadata.field.hidden.type=string\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataMap values{{"name", MetadataValue("x")}, {"other", MetadataValue("y")}};
  MetadataProjector projector(parsed.value());
  const auto web = projector.to_web(values);
  ASSERT_EQ(web.size(), 1u);
  EXPECT_EQ(web.front().id, "name");
  EXPECT_EQ(web.front().value, "x");
}

TEST(MetadataProjector, MalformedPersistedDocumentProducesNoAttributes) {
  const auto parsed = MetadataConfig::parse(
      "metadata.schema_version=1\n"
      "metadata.field.name.source=shortName\n"
      "metadata.field.name.type=string\n"
      "metadata.field.name.xattr=user.weather.name\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataProjector projector(parsed.value());
  for (const std::string malformed : {
      "{", "{\"name\":{\"type\":\"string\",\"value\":\"secret\"}}junk",
      "{\"name\":{\"type\":\"int64\",\"value\":1e999}}"}) {
    EXPECT_TRUE(projector.to_xattrs_json(malformed).empty());
  }
}

TEST(MetadataProjector, MalformedJsonCannotFallBackToFormatFields) {
  const auto parsed = MetadataConfig::parse(
      "metadata.schema_version=1\n"
      "metadata.field.name.source=shortName\n"
      "metadata.field.name.type=string\n"
      "metadata.field.name.xattr=user.weather.name\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataProjector projector(parsed.value());
  EXPECT_TRUE(projector.to_xattrs_json("{malformed").empty());
}

TEST(MetadataProjector, JsonXattrAndWebUseTheSameTypedValue) {
  const auto parsed = MetadataConfig::parse(
      "metadata.schema_version=1\n"
      "metadata.field.level.source=level\n"
      "metadata.field.level.type=double\n"
      "metadata.field.level.xattr=user.weather.level\n"
      "metadata.field.level.queryable=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataProjector projector(parsed.value());
  MetadataMap values{{"level", MetadataValue(850.25)}};
  const auto json = projector.to_json(values);
  ASSERT_TRUE(json.ok()) << json.error();
  const auto decoded = deserialize_metadata(json.value());
  ASSERT_TRUE(decoded.ok()) << decoded.error();
  const auto attrs = projector.to_xattrs(decoded.value());
  ASSERT_EQ(attrs.size(), 1u);
  EXPECT_EQ(attrs.front().second, "850.25");
  const auto web = projector.to_web(decoded.value());
  ASSERT_EQ(web.size(), 1u);
  EXPECT_EQ(web.front().id, "level");
  EXPECT_EQ(web.front().value, attrs.front().second);
}

TEST(MetadataProjector, InvalidUtf8IsExcludedFromViews) {
  const auto parsed = MetadataConfig::parse(
      "metadata.schema_version=1\n"
      "metadata.field.name.source=name\n"
      "metadata.field.name.type=string\n"
      "metadata.field.name.xattr=user.weather.name\n"
      "metadata.field.name.queryable=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataMap values{{"name", MetadataValue(std::string("bad", 3) + char(0xff))}};
  MetadataProjector projector(parsed.value());
  EXPECT_TRUE(projector.to_xattrs(values).empty());
  EXPECT_TRUE(projector.to_web(values).empty());
  EXPECT_FALSE(projector.to_json(values).ok());
}

TEST(MetadataProjector, OmitsWrongTypesMissingFieldsAndNonQueryableWebValues) {
  const auto parsed = MetadataConfig::parse(
    "metadata.schema_version=1\nmetadata.field.a.source=a\nmetadata.field.a.type=int64\n"
    "metadata.field.a.xattr=user.a\nmetadata.field.a.queryable=true\n"
    "metadata.field.b.source=b\nmetadata.field.b.type=string\nmetadata.field.b.xattr=user.b\n"
    "metadata.field.c.source=c\nmetadata.field.c.type=bool\nmetadata.field.c.xattr=user.c\n"
    "metadata.field.c.queryable=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataProjector projector(parsed.value());
  const MetadataMap values{{"a", MetadataValue("wrong type")}, {"b", MetadataValue("private")},
    {"unknown", MetadataValue(true)}};
  EXPECT_TRUE(projector.to_web(values).empty());
  EXPECT_EQ(projector.to_xattrs(values), (MetadataXattrList{{"user.b", "private"}}));
  const auto json = projector.to_json(values);
  ASSERT_TRUE(json.ok()) << json.error();
  EXPECT_EQ(json.value(), R"({"b":{"type":"string","value":"private"}})");
  EXPECT_FALSE(projector.to_json(values, json.value().size() - 1).ok());
  EXPECT_TRUE(projector.to_json(values, json.value().size()).ok());
}

TEST(MetadataProjector, NonFiniteValuesCannotEscapeThroughAnyView) {
  const auto parsed = MetadataConfig::parse(
    "metadata.schema_version=1\nmetadata.field.a.source=a\nmetadata.field.a.type=double\n"
    "metadata.field.a.xattr=user.a\nmetadata.field.a.queryable=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  MetadataProjector projector(parsed.value());
  for (const double value : {std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
    MetadataMap values{{"a", MetadataValue(value)}};
    EXPECT_TRUE(projector.to_xattrs(values).empty());
    EXPECT_TRUE(projector.to_web(values).empty());
    EXPECT_FALSE(projector.to_json(values).ok());
  }
}
