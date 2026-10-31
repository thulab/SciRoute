#include "iotdbfs/EnvironmentPreflight.h"

#include <gtest/gtest.h>

using iotdbfs::preflight::CheckResult;
using iotdbfs::preflight::CheckStatus;

TEST(EnvironmentPreflight, JsonEscapesControlCharactersAndSensitiveValues) {
  const CheckResult result{CheckStatus::Fail, "credentials", "AUTH_FAILED",
    "password=should-not-leak\nquoted \"value\""};
  const std::string json = iotdbfs::preflight::to_json(result);
  EXPECT_EQ(json,
    R"({"check":"credentials","status":"fail","code":"AUTH_FAILED","message":"password=<redacted>\nquoted \"value\""})");
  EXPECT_EQ(json.find("should-not-leak"), std::string::npos);
}

TEST(EnvironmentPreflight, ClassifiesSchemaRegionErrors) {
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(
    "TSStatus code 906: SchemaRegionGroup is not ready"),
    "IOTDB_SCHEMA_REGION_UNAVAILABLE");
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(
    "CREATE TABLE probe failed; schema region may be unready"),
    "IOTDB_SCHEMA_REGION_UNAVAILABLE");
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(
    "authentication failed for user"), "IOTDB_AUTH_FAILED");
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(
    "Unauthorized: permission denied"), "IOTDB_AUTH_FAILED");
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(
    "connection reset by peer"), "IOTDB_SESSION_FAILED");
}

TEST(EnvironmentPreflight, ValidatesDatabaseIdentifiers) {
  EXPECT_TRUE(iotdbfs::preflight::is_valid_identifier("weather_2026"));
  EXPECT_TRUE(iotdbfs::preflight::is_valid_identifier("A1"));
  EXPECT_FALSE(iotdbfs::preflight::is_valid_identifier(""));
  EXPECT_FALSE(iotdbfs::preflight::is_valid_identifier("1weather"));
  EXPECT_FALSE(iotdbfs::preflight::is_valid_identifier("weather-db"));
  EXPECT_FALSE(iotdbfs::preflight::is_valid_identifier("weather;DROP"));
}

TEST(EnvironmentPreflight, StatusAndExitSemanticsAreStable) {
  EXPECT_EQ(iotdbfs::preflight::status_name(CheckStatus::Pass), "pass");
  EXPECT_EQ(iotdbfs::preflight::status_name(CheckStatus::Fail), "fail");
  EXPECT_EQ(iotdbfs::preflight::status_name(CheckStatus::Skip), "skip");
}

TEST(EnvironmentPreflight, JsonEscapesEveryAsciiControlAndPreservesUtf8) {
  std::string controls;
  for (char c = 0; c < 32; ++c) controls += c;
  const auto json = iotdbfs::preflight::to_json(
    {CheckStatus::Pass, "check\"\\", "code\n", controls + "气象"});
  const std::string escaped =
    R"(\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000b\f\r\u000e\u000f)"
    R"(\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001a\u001b\u001c\u001d\u001e\u001f)";
  EXPECT_EQ(json,
    R"({"check":"check\"\\","status":"pass","code":"code\n","message":")" +
    escaped + "气象\"}");
}

TEST(EnvironmentPreflight, RedactsAllCredentialKeysWithDelimitersAndCase) {
  const auto json = iotdbfs::preflight::to_json({CheckStatus::Fail, "db", "auth",
    "PASSWORD = alpha,passwd:beta;secret=gamma) token = delta] credential:epsilon\npassword=zeta"});
  EXPECT_EQ(json,
    R"({"check":"db","status":"fail","code":"auth","message":"PASSWORD = <redacted>,passwd:<redacted>;secret=<redacted>) token = <redacted>] credential:<redacted>\npassword=<redacted>"})");
}

TEST(EnvironmentPreflight, DoesNotRedactOrdinaryWordsOrNonAssignmentKeys) {
  const std::string message = "mypassword=visible password policy tokenized=true token missing";
  EXPECT_EQ(iotdbfs::preflight::to_json({CheckStatus::Skip, "db", "skip", message}),
    R"({"check":"db","status":"skip","code":"skip","message":")" + message + "\"}");
}

TEST(EnvironmentPreflight, ClassifiesAllDocumentedErrorFamiliesAndPrecedence) {
  for (const auto* message : {"LOGIN rejected", "wrong Password", "permission denied"}) {
    SCOPED_TRACE(message);
    EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(message), "IOTDB_AUTH_FAILED");
  }
  for (const auto* message : {"SOCKET closed", "NETWORK failure", "reset by peer"}) {
    SCOPED_TRACE(message);
    EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(message), "IOTDB_SESSION_FAILED");
  }
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error("SchemaRegion auth connection"),
    "IOTDB_SCHEMA_REGION_UNAVAILABLE");
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error("authentication connection failure"),
    "IOTDB_AUTH_FAILED");
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error("unknown"), "IOTDB_CHECK_FAILED");
  EXPECT_EQ(iotdbfs::preflight::classify_iotdb_error(""), "IOTDB_CHECK_FAILED");
  EXPECT_EQ(iotdbfs::preflight::status_name(static_cast<CheckStatus>(99)), "fail");
}

TEST(EnvironmentPreflight, RejectsIdentifierWhitespaceUnicodeAndEmbeddedNul) {
  for (const std::string& value : {std::string(" a"), std::string("a "),
      std::string("a\nb"), std::string("_a"), std::string("a.b"),
      std::string("天气"), std::string("a\0b", 3), std::string("a\xff")}) {
    SCOPED_TRACE(::testing::PrintToString(value));
    EXPECT_FALSE(iotdbfs::preflight::is_valid_identifier(value));
  }
}
