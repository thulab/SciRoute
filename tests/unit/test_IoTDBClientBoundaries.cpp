#include "iotdbfs/IoTDBClientPolicy.h"
#include "iotdbfs/IoTDBProfile.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <limits>
#include <string>
#include <vector>

TEST(IoTDBClientBoundaries, UsesDefaultPoolSizeForZero) {
  EXPECT_EQ(iotdbfs::normalize_iotdb_pool_size(0),
    iotdbfs::kDefaultIoTDBPoolSize);
}

TEST(IoTDBClientBoundaries, RejectsPoolSizesAboveMaximum) {
  EXPECT_THROW(
    iotdbfs::normalize_iotdb_pool_size(iotdbfs::kMaxIoTDBPoolSize + 1),
    std::invalid_argument);
  EXPECT_EQ(iotdbfs::normalize_iotdb_pool_size(iotdbfs::kMaxIoTDBPoolSize),
    iotdbfs::kMaxIoTDBPoolSize);
}

TEST(IoTDBClientBoundaries, ValidatesDatabaseIdentifiers) {
  EXPECT_TRUE(iotdbfs::is_valid_database_name("dfs"));
  EXPECT_TRUE(iotdbfs::is_valid_database_name("weather_2026"));
  EXPECT_FALSE(iotdbfs::is_valid_database_name(""));
  EXPECT_FALSE(iotdbfs::is_valid_database_name("1dfs"));
  EXPECT_FALSE(iotdbfs::is_valid_database_name("weather-db"));
  EXPECT_FALSE(iotdbfs::is_valid_database_name("weather db"));
  EXPECT_FALSE(iotdbfs::is_valid_database_name("weather;DROP"));
}

TEST(IoTDBClientBoundaries, RedactsSqlStringLiterals) {
  const std::string sql =
    "INSERT INTO inode(time,path) VALUES(1,'/secret/O''Reilly.grib')";
  const std::string redacted = iotdbfs::redact_sql_for_log(sql);
  EXPECT_EQ(redacted,
    "INSERT INTO inode(time,path) VALUES(1,'?')");
  EXPECT_EQ(redacted.find("secret"), std::string::npos);
  EXPECT_EQ(redacted.find("Reilly"), std::string::npos);
}

TEST(IoTDBClientBoundaries, PreservesSqlStructureOutsideLiterals) {
  EXPECT_EQ(iotdbfs::redact_sql_for_log(
    "SELECT * FROM inode WHERE time=42 AND path='/tmp/a' LIMIT 1"),
    "SELECT * FROM inode WHERE time=42 AND path='?' LIMIT 1");
}

TEST(IoTDBClientBoundaries, RedactsEmptyAndEscapedSqlLiterals) {
  EXPECT_EQ(iotdbfs::redact_sql_for_log("SELECT '' AS empty, 'a''b' AS value"),
    "SELECT '?' AS empty, '?' AS value");
}

TEST(IoTDBClientBoundaries, PreservesEverySupportedExplicitPoolSize) {
  for (std::size_t requested = 1; requested <= iotdbfs::kMaxIoTDBPoolSize; ++requested)
    EXPECT_EQ(iotdbfs::normalize_iotdb_pool_size(requested), requested);
  EXPECT_THROW(iotdbfs::normalize_iotdb_pool_size(std::numeric_limits<std::size_t>::max()),
    std::invalid_argument);
}

TEST(IoTDBClientBoundaries, EnforcesDatabaseIdentifierAsciiAndLengthLimits) {
  for (const std::string valid : std::vector<std::string>{"_", "A", "_Weather_2026", std::string(128, 'a')})
    EXPECT_TRUE(iotdbfs::is_valid_database_name(valid)) << valid;
  for (const std::string invalid : std::vector<std::string>{
      std::string(129, 'a'), " weather", "weather\n", "weather.db", "`weather`",
      "weather'", "weather/dir", "weather\\dir", u8"天气", std::string("a\0b", 3),
      std::string("a\xff", 2)})
    EXPECT_FALSE(iotdbfs::is_valid_database_name(invalid));
}

TEST(IoTDBClientBoundaries, RedactsBackslashEscapesAndMultiplePrivateValues) {
  const std::string sql = R"(INSERT INTO inode(path,name) VALUES('C:\private\file','O\'Reilly'))";
  EXPECT_EQ(iotdbfs::redact_sql_for_log(sql),
    "INSERT INTO inode(path,name) VALUES('?','?')");
  EXPECT_EQ(iotdbfs::redact_sql_for_log("SELECT 'line\nsecret\tvalue', 'second-secret'"),
    "SELECT '?', '?'");
  EXPECT_EQ(iotdbfs::redact_sql_for_log(std::string("SELECT 'a\0private'", 18)),
    "SELECT '?'");
}

TEST(IoTDBClientBoundaries, TruncatedLiteralDoesNotExposeRemainingPrivateBytes) {
  for (const std::string sql : {"SELECT 'secret", "SELECT 'secret\\", "SELECT 'secret\\'suffix"}) {
    const auto redacted = iotdbfs::redact_sql_for_log(sql);
    EXPECT_EQ(redacted, "SELECT '?");
    EXPECT_EQ(redacted.find("secret"), std::string::npos);
    EXPECT_EQ(redacted.find("suffix"), std::string::npos);
  }
}

TEST(IoTDBClientBoundaries, SqlWithoutStringLiteralsIsPreservedByteForByte) {
  for (const std::string sql : {"", "SELECT 1", "\nSELECT path FROM inode WHERE time=42\t", "SELECT \"column\" FROM inode"})
    EXPECT_EQ(iotdbfs::redact_sql_for_log(sql), sql);
}

TEST(IoTDBClientBoundaries, ProfileSeparatesAttemptedAndSuccessfulBackendBytes) {
  const auto before = iotdbfs::iotdb_profile_snapshot();
  { iotdbfs::IoTDBReadObservation failed(true, 3, 120); }
  { iotdbfs::IoTDBReadObservation successful(true, 2, 80); successful.success(64); }
  { iotdbfs::IoTDBReadObservation layout(false, 1, 32); layout.success(32); }
  const auto after = iotdbfs::iotdb_profile_snapshot();
  EXPECT_EQ(after.payload_calls - before.payload_calls, 2u);
  EXPECT_EQ(after.payload_ranges - before.payload_ranges, 5u);
  EXPECT_EQ(after.payload_requested_bytes - before.payload_requested_bytes, 200u);
  EXPECT_EQ(after.payload_returned_bytes - before.payload_returned_bytes, 64u);
  EXPECT_EQ(after.payload_failures - before.payload_failures, 1u);
  EXPECT_EQ(after.layout_calls - before.layout_calls, 1u);
  EXPECT_EQ(after.layout_requested_bytes - before.layout_requested_bytes, 32u);
  EXPECT_EQ(after.layout_returned_bytes - before.layout_returned_bytes, 32u);
  EXPECT_EQ(after.layout_failures, before.layout_failures);
}
