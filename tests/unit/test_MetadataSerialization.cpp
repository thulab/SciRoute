#include "iotdbfs/MetadataSerialization.h"

#include <gtest/gtest.h>

#include <limits>
#include <cmath>
#include <locale>
#include <set>
#include <stdexcept>
#include <vector>

using namespace iotdbfs;

TEST(MetadataSerialization, LoadsStringMetadataFromPersistedJson) {
  const auto result = deserialize_metadata(
    R"({"experimentId":{"type":"string","value":"analysis"}})");
  ASSERT_TRUE(result.ok()) << result.error();
  ASSERT_EQ(result.value().size(), 1u);
  EXPECT_EQ(std::get<std::string>(result.value().at("experimentId").value()),
    "analysis");
}

TEST(MetadataSerialization, PreservesTypesLimitsAndArbitraryPropertyOrder) {
  const auto result = deserialize_metadata(R"({
    "min":{"value":-9223372036854775808,"type":"int64"},
    "max":{"type":"int64","value":9223372036854775807},
    "real":{"type":"double","value":-1.25e2},
    "whole":{"value":1,"type":"double"},
    "yes":{"type":"bool","value":true},
    "no":{"value":false,"type":"bool"}
  })");
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_EQ(std::get<std::int64_t>(result.value().at("min").value()),
    std::numeric_limits<std::int64_t>::min());
  EXPECT_EQ(std::get<std::int64_t>(result.value().at("max").value()),
    std::numeric_limits<std::int64_t>::max());
  EXPECT_DOUBLE_EQ(std::get<double>(result.value().at("real").value()), -125.0);
  EXPECT_DOUBLE_EQ(std::get<double>(result.value().at("whole").value()), 1.0);
  EXPECT_TRUE(std::get<bool>(result.value().at("yes").value()));
  EXPECT_FALSE(std::get<bool>(result.value().at("no").value()));
}

TEST(MetadataSerialization, AcceptsLegacyEmptyMetadataAndEnforcesReadBudget) {
  for (const std::string json : { "", "{}", " \n\t{}\r" }) {
    const auto result = deserialize_metadata(json);
    ASSERT_TRUE(result.ok()) << result.error();
    EXPECT_TRUE(result.value().empty());
  }
  EXPECT_FALSE(deserialize_metadata("{}", 1).ok());
  EXPECT_FALSE(deserialize_metadata(std::string(65537, ' '), 100000).ok());
  const std::string prefix = R"({"name":{"type":"string","value":")";
  EXPECT_TRUE(deserialize_metadata(prefix + std::string(4096, 'a') + "\"}}").ok());
  EXPECT_FALSE(deserialize_metadata(prefix + std::string(4097, 'a') + "\"}}").ok());
  EXPECT_TRUE(deserialize_metadata("{\"" + std::string(64, 'a') +
    R"(":{"type":"bool","value":true}})").ok());
  EXPECT_FALSE(deserialize_metadata("{\"" + std::string(65, 'a') +
    R"(":{"type":"bool","value":true}})").ok());
  std::string many = "{";
  for (int index = 0; index < 128; ++index) {
    if (index) many += ',';
    many += "\"k" + std::to_string(index) + R"(":{"type":"bool","value":false})";
  }
  EXPECT_TRUE(deserialize_metadata(many + "}").ok());
  EXPECT_FALSE(deserialize_metadata(many + R"(,"extra":{"type":"bool","value":true}})").ok());
}

TEST(MetadataSerialization, DecodesJsonEscapesAndUnicodeWithoutLosingBytes) {
  const auto result = deserialize_metadata(
    R"({"label":{"type":"string","value":"\"\\\/\b\f\n\r\t\u0000\u4e2d\ud83d\ude80中文"}})");
  ASSERT_TRUE(result.ok()) << result.error();
  std::string expected = "\"\\/\b\f\n\r\t";
  expected.push_back('\0');
  expected += u8"中🚀中文";
  EXPECT_EQ(std::get<std::string>(result.value().at("label").value()), expected);
}

TEST(MetadataSerialization, RejectsInvalidUtf8AndSurrogates) {
  const std::string prefix = R"({"label":{"type":"string","value":")";
  for (const std::string invalid : {
      "\x80", "\xc0\xaf", "\xc2", "\xe0\x80\xaf", "\xed\xa0\x80",
      "\xf0\x80\x80\xaf", "\xf4\x90\x80\x80", "\xff", "\xc2x" }) {
    EXPECT_FALSE(deserialize_metadata(prefix + invalid + "\"}}").ok());
  }
  for (const std::string invalid : {
      R"(\ud800)", R"(\ud800\u0041)", R"(\udc00)", R"(\uZZZZ)", R"(\x20)" }) {
    EXPECT_FALSE(deserialize_metadata(prefix + invalid + "\"}}").ok());
  }
}

TEST(MetadataSerialization, FormatsValuesForExtendedAttributes) {
  EXPECT_EQ(metadata_value_text(MetadataValue("alpha")), "alpha");
  EXPECT_EQ(metadata_value_text(MetadataValue(std::int64_t(-9))), "-9");
  EXPECT_EQ(metadata_value_text(MetadataValue(1.25)), "1.25");
  EXPECT_EQ(metadata_value_text(MetadataValue(true)), "true");
  EXPECT_FALSE(valid_metadata_utf8(std::string("\xf0\x28\x8c\xbc")));
}

TEST(MetadataSerialization, RejectsWrongShapeAndNonFiniteNumbers) {
  const std::vector<std::string> invalid = {
    R"({"a":{"type":"string","value":null}})",
    R"({"a":{"type":"string","value":[]}})",
    R"({"a":{"type":"float","value":1}})",
    R"({"a":{"type":"double","value":1e999}})",
    R"({"a":{"type":"int64","value":9223372036854775808}})",
    R"({"a":{"type":"bool","value":0}})",
    R"({"a":{"type":"string","value":"x","value":"y"}})",
    R"({"a":{"type":"string"}})",
    R"({"a":{"value":"x"}})",
    R"({"a":{"type":"string","value":"x","extra":1}})"
  };
  for (const auto& json : invalid) EXPECT_FALSE(deserialize_metadata(json).ok()) << json;
}

TEST(MetadataSerialization, RoundTripsAllTypesAndPreservesDoublePrecision) {
  MetadataMap values{
    {"min", MetadataValue(std::numeric_limits<std::int64_t>::min())},
    {"max", MetadataValue(std::numeric_limits<std::int64_t>::max())},
    {"real", MetadataValue(std::nextafter(1.0, 2.0))},
    {"tiny", MetadataValue(std::numeric_limits<double>::min())},
    {"large", MetadataValue(std::numeric_limits<double>::max())},
    {"negativeZero", MetadataValue(-0.0)},
    {"false", MetadataValue(false)},
    {"true", MetadataValue(true)},
    {"text", MetadataValue(std::string("line\n\t\r\b\f\"\\\0tail", 16) + u8"中文🚀")}
  };
  const auto encoded = serialize_metadata(values, 65536);
  ASSERT_TRUE(encoded.ok()) << encoded.error();
  const auto decoded = deserialize_metadata(encoded.value());
  ASSERT_TRUE(decoded.ok()) << decoded.error();
  ASSERT_EQ(decoded.value().size(), values.size());
  for (const auto& [key, expected] : values) {
    ASSERT_NE(decoded.value().find(key), decoded.value().end()) << key;
    EXPECT_EQ(decoded.value().at(key).type(), expected.type()) << key;
    EXPECT_EQ(decoded.value().at(key).value(), expected.value()) << key;
  }
  EXPECT_TRUE(std::signbit(std::get<double>(decoded.value().at("negativeZero").value())));
  const auto second_encoding = serialize_metadata(decoded.value(), 65536);
  ASSERT_TRUE(second_encoding.ok()) << second_encoding.error();
  EXPECT_EQ(second_encoding.value(), encoded.value());
}

TEST(MetadataSerialization, RoundTripsTypedSetsDeterministically) {
  MetadataMap values{
    {"names", MetadataValue(MetadataStringSet{"u", "t", "t"})},
    {"levels", MetadataValue(MetadataInt64Set{850, 500, 850})},
    {"increments", MetadataValue(MetadataDoubleSet{0.5, 0.25})},
    {"flags", MetadataValue(MetadataBoolSet{true, false})},
  };
  const auto encoded = serialize_metadata(values, 65536);
  ASSERT_TRUE(encoded.ok()) << encoded.error();
  EXPECT_NE(encoded.value().find(
    R"("names":{"type":"string_set","value":["t","u"]})"),
    std::string::npos);
  const auto decoded = deserialize_metadata(encoded.value());
  ASSERT_TRUE(decoded.ok()) << decoded.error();
  EXPECT_EQ(decoded.value().at("names").value(), values.at("names").value());
  EXPECT_EQ(decoded.value().at("levels").value(), values.at("levels").value());
  EXPECT_EQ(decoded.value().at("increments").value(),
    values.at("increments").value());
  EXPECT_EQ(decoded.value().at("flags").value(), values.at("flags").value());
  EXPECT_EQ(metadata_value_text(decoded.value().at("names")),
    R"(["t","u"])");
}

TEST(MetadataSerialization, RejectsEveryTruncatedDocumentAndTrailingContent) {
  const std::string json = R"({"a":{"type":"string","value":"hello\n\u4e2d"},"b":{"type":"double","value":-1.25e+2}})";
  ASSERT_TRUE(deserialize_metadata(json).ok());
  for (std::size_t bytes = 1; bytes < json.size(); ++bytes) {
    const auto result = deserialize_metadata(json.substr(0, bytes));
    EXPECT_FALSE(result.ok()) << "truncated at byte " << bytes;
    EXPECT_FALSE(result.error().empty()) << "truncated at byte " << bytes;
  }
  for (const std::string suffix : {"x", "{}", ",", " true"})
    EXPECT_FALSE(deserialize_metadata(json + suffix).ok()) << suffix;
}

TEST(MetadataSerialization, RejectsInvalidNumberGrammarAndMismatchedScalars) {
  const std::vector<std::string> invalid_numbers{
    "+1", "01", "-01", "-", ".5", "1.", "1e", "1e+", "1e-",
    "NaN", "Infinity", "0x10", "1 2"
  };
  for (const auto& raw : invalid_numbers) {
    EXPECT_FALSE(deserialize_metadata("{\"a\":{\"type\":\"double\",\"value\":" + raw + "}}").ok()) << raw;
  }
  for (const std::string json : {
      R"({"a":{"type":"int64","value":1.0}})",
      R"({"a":{"type":"int64","value":1e2}})",
      R"({"a":{"type":"int64","value":-9223372036854775809}})",
      R"({"a":{"type":"double","value":"1.0"}})",
      R"({"a":{"type":"string","value":true}})",
      R"({"a":{"type":"bool","value":"true"}})",
      R"({"a":{"type":"bool","value":truex}})",
      R"({"a":{"type":"bool","value":{}}})"})
    EXPECT_FALSE(deserialize_metadata(json).ok()) << json;
}

TEST(MetadataSerialization, RejectsDuplicateIdsTypesAndInvalidObjectSyntax) {
  for (const std::string json : {
      "[]", "null", " \t\n", "{,}", "{\"a\":{}}",
      R"({"a":{"type":"bool","value":true},})",
      R"({"a":{"type":"bool","value":true,}})",
      R"({"a":{"type":"bool","type":"bool","value":true}})",
      R"({"a":{"type":"bool","value":true},"a":{"type":"bool","value":false}})",
      R"({"a":{"type":"bool","value":true},"\u0061":{"type":"bool","value":false}})"})
    EXPECT_FALSE(deserialize_metadata(json).ok()) << json;
}

TEST(MetadataSerialization, AppliesLimitsToDecodedUnicodeBytes) {
  const std::string prefix = R"({"a":{"type":"string","value":")";
  std::string escaped;
  for (int i = 0; i < 1024; ++i) escaped += R"(\ud83d\ude80)";
  const auto valid = deserialize_metadata(prefix + escaped + "\"}}");
  ASSERT_TRUE(valid.ok()) << valid.error();
  EXPECT_EQ(std::get<std::string>(valid.value().at("a").value()).size(), 4096u);
  EXPECT_FALSE(deserialize_metadata(prefix + escaped + "a\"}}").ok());
  const auto maximum = deserialize_metadata(prefix + R"(\udbff\udfff"}})");
  ASSERT_TRUE(maximum.ok()) << maximum.error();
  EXPECT_EQ(std::get<std::string>(maximum.value().at("a").value()), "\xf4\x8f\xbf\xbf");
  const auto boundaries = deserialize_metadata(prefix + R"(\u007f\u0080\u07ff\u0800\uD7FF\uE000\uFFFF"}})");
  ASSERT_TRUE(boundaries.ok()) << boundaries.error();
  EXPECT_EQ(std::get<std::string>(boundaries.value().at("a").value()),
    "\x7f\xc2\x80\xdf\xbf\xe0\xa0\x80\xed\x9f\xbf\xee\x80\x80\xef\xbf\xbf");
  for (const std::string escaped_invalid : {R"(\u0)", R"(\ud800\u)", R"(\ud800\ud800)"})
    EXPECT_FALSE(deserialize_metadata(prefix + escaped_invalid + "\"}}").ok());
}

TEST(MetadataSerialization, WriterAndReaderUseExactByteCapsIncludingBraces) {
  const MetadataMap values{{"flag", MetadataValue(true)}};
  const auto encoded = serialize_metadata(values, 65536);
  ASSERT_TRUE(encoded.ok()) << encoded.error();
  EXPECT_TRUE(serialize_metadata(values, encoded.value().size()).ok());
  EXPECT_FALSE(serialize_metadata(values, encoded.value().size() - 1).ok());
  EXPECT_TRUE(deserialize_metadata(encoded.value(), encoded.value().size()).ok());
  EXPECT_FALSE(deserialize_metadata(encoded.value(), encoded.value().size() - 1).ok());
  EXPECT_TRUE(serialize_metadata({}, 2).ok());
  EXPECT_FALSE(serialize_metadata({}, 1).ok());
  EXPECT_FALSE(serialize_metadata({}, 0).ok());
  EXPECT_TRUE(deserialize_metadata("", 0).ok());
  std::string exact = "{}" + std::string(65534, ' ');
  EXPECT_TRUE(deserialize_metadata(exact, 1000000).ok());
  EXPECT_FALSE(deserialize_metadata(exact + ' ', 1000000).ok());
}

TEST(MetadataSerialization, RejectsInvalidUtf8KeysAndRawControlBytes) {
  const MetadataMap invalid{{std::string("\xff"), MetadataValue(true)}};
  EXPECT_FALSE(serialize_metadata(invalid, 65536).ok());
  EXPECT_FALSE(deserialize_metadata(std::string("{\"\xff\":{\"type\":\"bool\",\"value\":true}}")).ok());
  for (unsigned int value = 0; value < 0x20; ++value) {
    const std::string raw = R"({"a":{"type":"string","value":")" +
      std::string(1, static_cast<char>(value)) + "\"}}";
    EXPECT_FALSE(deserialize_metadata(raw).ok()) << value;
    MetadataMap values{{"a", MetadataValue(std::string(1, static_cast<char>(value)))}};
    const auto escaped = serialize_metadata(values, 65536);
    ASSERT_TRUE(escaped.ok()) << escaped.error();
    const auto decoded = deserialize_metadata(escaped.value());
    ASSERT_TRUE(decoded.ok()) << decoded.error();
    EXPECT_EQ(decoded.value().at("a").value(), values.at("a").value());
  }
}

TEST(MetadataSerialization, TextFormattingRejectsNonFiniteAndInvalidUtf8) {
  for (const double value : {std::numeric_limits<double>::quiet_NaN(),
      std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity()}) {
    EXPECT_THROW(metadata_value_text(MetadataValue(value)), std::invalid_argument);
    EXPECT_FALSE(serialize_metadata({{"a", MetadataValue(value)}}, 65536).ok());
  }
  EXPECT_THROW(metadata_value_text(MetadataValue(std::string("\xff"))), std::invalid_argument);
  EXPECT_EQ(metadata_value_text(MetadataValue(false)), "false");
  EXPECT_EQ(metadata_value_text(MetadataValue(std::numeric_limits<std::int64_t>::min())), "-9223372036854775808");
  EXPECT_TRUE(valid_metadata_utf8(""));
  EXPECT_TRUE(valid_metadata_utf8(std::string("\0", 1)));
  EXPECT_TRUE(valid_metadata_utf8("\xc2\x80\xe0\xa0\x80\xf0\x90\x80\x80\xf4\x8f\xbf\xbf"));
}

TEST(MetadataSerialization, NumericSerializationIgnoresTheGlobalCppLocale) {
  struct CommaDecimal : std::numpunct<char> {
    char do_decimal_point() const override { return ','; }
    char do_thousands_sep() const override { return '_'; }
    std::string do_grouping() const override { return "\3"; }
  };
  struct RestoreLocale {
    std::locale saved = std::locale();
    ~RestoreLocale() { std::locale::global(saved); }
  } restore;
  std::locale::global(std::locale(std::locale::classic(), new CommaDecimal));
  const MetadataMap values{{"real", MetadataValue(1234.5)},
                          {"integer", MetadataValue(std::int64_t{12345})}};
  const auto encoded = serialize_metadata(values, 65536);
  ASSERT_TRUE(encoded.ok()) << encoded.error();
  EXPECT_EQ(encoded.value(),
    R"({"integer":{"type":"int64","value":12345},"real":{"type":"double","value":1234.5}})");
  const auto decoded = deserialize_metadata(encoded.value());
  ASSERT_TRUE(decoded.ok()) << decoded.error();
  EXPECT_EQ(decoded.value().at("real").value(), values.at("real").value());
  EXPECT_EQ(metadata_value_text(values.at("real")), "1234.5");
  EXPECT_EQ(metadata_value_text(values.at("integer")), "12345");
}
