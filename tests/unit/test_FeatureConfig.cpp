#include "iotdbfs/FeatureConfig.h"

#include <gtest/gtest.h>

#include <map>
#include <unordered_map>
#include "TestSupport.h"

TEST(FeatureConfigTest, ParsesBooleanValues) {
  EXPECT_TRUE(iotdbfs::parse_bool_feature_value("true", false));
  EXPECT_TRUE(iotdbfs::parse_bool_feature_value(" YES ", false));
  EXPECT_TRUE(iotdbfs::parse_bool_feature_value("enabled", false));
  EXPECT_FALSE(iotdbfs::parse_bool_feature_value("false", true));
  EXPECT_FALSE(iotdbfs::parse_bool_feature_value(" off ", true));
  EXPECT_FALSE(iotdbfs::parse_bool_feature_value("disabled", true));
  EXPECT_TRUE(iotdbfs::parse_bool_feature_value("unexpected", true));
  EXPECT_FALSE(iotdbfs::parse_bool_feature_value("unexpected", false));
}

TEST(FeatureConfigTest, ReadsCanonicalAndLegacyPropertyKeys) {
  std::map<std::string, std::string> canonical{
    { iotdbfs::kGribExtensionEnabledKey, "false" }
  };
  EXPECT_FALSE(iotdbfs::grib_extension_enabled_from_properties(canonical, true));

  std::unordered_map<std::string, std::string> legacy{
    { "grib.enabled", "true" }
  };
  EXPECT_TRUE(iotdbfs::grib_extension_enabled_from_properties(legacy, false));

  std::map<std::string, std::string> missing;
  EXPECT_TRUE(iotdbfs::grib_extension_enabled_from_properties(missing, true));
  EXPECT_FALSE(iotdbfs::grib_extension_enabled_from_properties(missing, false));
}

TEST(FeatureConfigTest, CombinesEnabledFlagAndPathExtension) {
  EXPECT_TRUE(iotdbfs::grib_enabled_for_path(true, "/data/a.grib2"));
  EXPECT_TRUE(iotdbfs::grib_enabled_for_path(true, "/data/a.grib"));
  EXPECT_TRUE(iotdbfs::grib_enabled_for_path(true, "/data/a.grb2"));
  EXPECT_FALSE(iotdbfs::grib_enabled_for_path(false, "/data/a.grib2"));
  EXPECT_FALSE(iotdbfs::grib_enabled_for_path(true, "/data/a.txt"));
}

TEST(FeatureConfigTest, ParsesEveryBooleanSpellingAndPreservesFallback) {
  for (const auto* value : {"1", "TRUE", " Yes\t", "On", "ENABLED"}) {
    SCOPED_TRACE(value);
    EXPECT_TRUE(iotdbfs::parse_bool_feature_value(value, false));
  }
  for (const auto* value : {"0", "FALSE", " No\n", "Off", "DISABLED"}) {
    SCOPED_TRACE(value);
    EXPECT_FALSE(iotdbfs::parse_bool_feature_value(value, true));
  }
  for (const auto* value : {"", " \t\r\n", "2", "truejunk", "-1"}) {
    SCOPED_TRACE(value);
    for (const bool fallback : {false, true})
      EXPECT_EQ(iotdbfs::parse_bool_feature_value(value, fallback), fallback);
  }
}

TEST(FeatureConfigTest, CanonicalPropertyTakesPrecedenceEvenWhenInvalid) {
  std::map<std::string, std::string> properties{
    {iotdbfs::kGribExtensionEnabledKey, "false"}, {"grib.enabled", "true"}};
  EXPECT_FALSE(iotdbfs::grib_extension_enabled_from_properties(properties));
  properties[iotdbfs::kGribExtensionEnabledKey] = "invalid";
  EXPECT_FALSE(iotdbfs::grib_extension_enabled_from_properties(properties, false));
  EXPECT_TRUE(iotdbfs::grib_extension_enabled_from_properties(properties, true));
}

TEST(FeatureConfigTest, PropertiesHandleCommentsCrLfDuplicatesAndEmbeddedEquals) {
  test_support::TempDirectory dir;
  const auto path = dir.write("config.properties",
    "\r\n # comment\r\n; comment\n // comment\ninvalid line\n=ignored\n"
    " duplicate = first\n duplicate=last\n empty=\n token=a=b=c\n"
    " value = text # retained\n");
  bool loaded = false;
  const auto values = iotdbfs::load_simple_properties(path, &loaded);
  EXPECT_TRUE(loaded);
  const std::unordered_map<std::string, std::string> expected{
    {"duplicate", "last"}, {"empty", ""}, {"token", "a=b=c"},
    {"value", "text # retained"}};
  EXPECT_EQ(values, expected);
  EXPECT_EQ(iotdbfs::load_simple_properties(path), expected);
}

TEST(FeatureConfigTest, MissingAndEmptyFilesReportDifferentLoadStates) {
  test_support::TempDirectory dir;
  bool loaded = true;
  EXPECT_TRUE(iotdbfs::load_simple_properties(dir.file("missing"), &loaded).empty());
  EXPECT_FALSE(loaded);
  EXPECT_TRUE(iotdbfs::load_simple_properties(dir.write("empty", ""), &loaded).empty());
  EXPECT_TRUE(loaded);
}

TEST(FeatureConfigTest, EnvironmentOverridesFileAndInvalidValuesUseFileFallback) {
  test_support::ScopedEnvironment enabled(iotdbfs::kGribExtensionEnabledEnv);
  test_support::TempDirectory dir;
  const auto path = dir.write("config.properties", "grib.extension.enabled=false\n");
  enabled.unset();
  EXPECT_FALSE(iotdbfs::load_grib_extension_enabled_from_config_file(path));
  enabled.set("TRUE");
  EXPECT_TRUE(iotdbfs::load_grib_extension_enabled_from_config_file(path));
  enabled.set("invalid");
  EXPECT_FALSE(iotdbfs::load_grib_extension_enabled_from_config_file(path));
  enabled.set("");
  EXPECT_FALSE(iotdbfs::load_grib_extension_enabled_from_config_file(path));
  enabled.unset();
  for (const bool fallback : {false, true})
    EXPECT_EQ(iotdbfs::load_grib_extension_enabled_from_config_file(
      dir.file("missing"), fallback), fallback);
}

TEST(FeatureConfigTest, DefaultConfigPathHonorsFileThenDirectoryThenDefault) {
  test_support::ScopedEnvironment config("IOTDB_CONFIG_FILE");
  test_support::ScopedEnvironment directory("IOTDB_CONF_DIR");
  test_support::ScopedEnvironment enabled(iotdbfs::kGribExtensionEnabledEnv);
  enabled.unset();
  config.unset();
  directory.unset();
  EXPECT_EQ(iotdbfs::default_config_path(), "resources/config.properties");
  test_support::TempDirectory dir;
  const auto path = dir.write("config.properties", "grib.extension.enabled=false\n");
  directory.set(dir.file(""));
  EXPECT_EQ(fs::path(iotdbfs::default_config_path()).lexically_normal(), fs::path(path));
  EXPECT_FALSE(iotdbfs::load_grib_extension_enabled_from_default_config());
  const auto override_path = dir.write("override.properties", "grib.extension.enabled=true\n");
  config.set(override_path);
  EXPECT_EQ(iotdbfs::default_config_path(), override_path);
  EXPECT_TRUE(iotdbfs::load_grib_extension_enabled_from_default_config(false));
  config.set("");
  EXPECT_FALSE(iotdbfs::load_grib_extension_enabled_from_default_config());
  directory.set("");
  EXPECT_EQ(iotdbfs::default_config_path(), "resources/config.properties");
}

TEST(FeatureConfigTest, LoadedPropertiesUseFallbackWhenFeatureKeyIsAbsent) {
  test_support::TempDirectory dir;
  test_support::ScopedEnvironment enabled(iotdbfs::kGribExtensionEnabledEnv);
  enabled.unset();
  for (const std::string& contents : {std::string("unrelated=value\n"),
      std::string("grib.enabled=false\n")}) {
    const auto path = dir.write("properties", contents);
    EXPECT_FALSE(iotdbfs::load_grib_extension_enabled_from_config_file(path, false));
  }
  const auto path = dir.write("properties", "unrelated=value\n");
  EXPECT_TRUE(iotdbfs::load_grib_extension_enabled_from_config_file(path, true));
}
