#include <gtest/gtest.h>
#include "iotdbfs/formats/grib/gribUtils.h"
#include "iotdbfs/MetadataConfig.h"

#include <memory>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstring>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <limits>

using namespace grib_utils;

TEST(GribConfiguredMetadata, ExtractsTypedFieldsWithAliasAndDefault) {
  codes_handle* handle = codes_grib_handle_new_from_samples(nullptr, "GRIB2");
  ASSERT_NE(handle, nullptr);
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.edition.source=edition\n"
    "metadata.field.edition.type=int64\n"
    "metadata.field.name.source=shortName\n"
    "metadata.field.name.type=string\n"
    "metadata.field.alias.source=missingPrimary\n"
    "metadata.field.alias.aliases=shortName\n"
    "metadata.field.alias.type=string\n"
    "metadata.field.missing.source=missingKey\n"
    "metadata.field.missing.type=bool\n"
    "metadata.field.missing.default=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  const auto values = extract_configured_metadata(handle, parsed.value());
  ASSERT_TRUE(values.ok()) << values.error();
  ASSERT_TRUE(values.value().count("edition"));
  EXPECT_EQ(values.value().at("edition").type(), iotdbfs::MetadataValueType::Int64);
  EXPECT_EQ(std::get<std::int64_t>(values.value().at("edition").value()), 2);
  ASSERT_TRUE(values.value().count("name"));
  EXPECT_EQ(values.value().at("name").type(), iotdbfs::MetadataValueType::String);
  EXPECT_EQ(std::get<std::string>(values.value().at("name").value()), "t");
  ASSERT_TRUE(values.value().count("missing"));
  EXPECT_EQ(values.value().at("missing").type(), iotdbfs::MetadataValueType::Bool);
  EXPECT_TRUE(std::get<bool>(values.value().at("missing").value()));
  ASSERT_TRUE(values.value().count("alias"));
  EXPECT_EQ(values.value().at("alias").type(), iotdbfs::MetadataValueType::String);
  EXPECT_EQ(std::get<std::string>(values.value().at("alias").value()), "t");
  codes_handle_delete(handle);
}

TEST(GribConfiguredMetadata, RejectsMissingRequiredField) {
  codes_handle* handle = codes_grib_handle_new_from_samples(nullptr, "GRIB2");
  ASSERT_NE(handle, nullptr);
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.required.source=missingKey\n"
    "metadata.field.required.type=string\n"
    "metadata.field.required.required=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  const auto values = extract_configured_metadata(handle, parsed.value());
  EXPECT_FALSE(values.ok());
  codes_handle_delete(handle);
}

TEST(GribConfiguredMetadata, DoesNotTruncateFloatingPointKeysToIntegers) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "GRIB2"), codes_handle_delete);
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfFirstGridPointInDegrees",
    12.5), CODES_SUCCESS);
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.latitude.source=latitudeOfFirstGridPointInDegrees\n"
    "metadata.field.latitude.type=int64\n"
    "metadata.field.latitude.required=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  const auto values = extract_configured_metadata(handle.get(), parsed.value());
  EXPECT_FALSE(values.ok());
  EXPECT_NE(values.error().find("latitude"), std::string::npos);
}

TEST(GribConfiguredMetadata, DoesNotTreatFractionalValuesAsBooleans) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "GRIB2"), codes_handle_delete);
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfFirstGridPointInDegrees",
    0.5), CODES_SUCCESS);
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.flag.source=latitudeOfFirstGridPointInDegrees\n"
    "metadata.field.flag.type=bool\n"
    "metadata.field.flag.required=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  const auto values = extract_configured_metadata(handle.get(), parsed.value());
  EXPECT_FALSE(values.ok());
}

TEST(GribConfiguredMetadata, TriesAliasesWhenPrimaryValueHasWrongType) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "GRIB2"), codes_handle_delete);
  ASSERT_NE(handle, nullptr);
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.flag.source=edition\n"
    "metadata.field.flag.aliases=notPresent,bitmapPresent\n"
    "metadata.field.flag.type=bool\n"
    "metadata.field.flag.required=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  const auto values = extract_configured_metadata(handle.get(), parsed.value());
  ASSERT_TRUE(values.ok()) << values.error();
  ASSERT_EQ(values.value().size(), 1u);
  EXPECT_FALSE(std::get<bool>(values.value().at("flag").value()));
}

TEST(GribConfiguredMetadata, SkipsMissingSentinelsAndArraysAndScalesDoubles) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "GRIB2"), codes_handle_delete);
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfFirstGridPointInDegrees",
    12.5), CODES_SUCCESS);
  ASSERT_EQ(codes_set_missing(handle.get(), "scaleFactorOfSecondFixedSurface"),
    CODES_SUCCESS);
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.latitude.source=latitudeOfFirstGridPointInDegrees\n"
    "metadata.field.latitude.type=double\n"
    "metadata.field.latitude.scale=0.1\n"
    "metadata.field.array.source=values\n"
    "metadata.field.array.type=double\n"
    "metadata.field.sentinel.source=scaleFactorOfSecondFixedSurface\n"
    "metadata.field.sentinel.type=int64\n"
    "metadata.field.invalidBool.source=edition\n"
    "metadata.field.invalidBool.type=bool\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  const auto values = extract_configured_metadata(handle.get(), parsed.value());
  ASSERT_TRUE(values.ok()) << values.error();
  ASSERT_EQ(values.value().size(), 1u);
  EXPECT_DOUBLE_EQ(std::get<double>(values.value().at("latitude").value()), 1.25);
}

TEST(GribConfiguredMetadata, RejectsScaleOverflowAndNullHandle) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "GRIB2"), codes_handle_delete);
  ASSERT_NE(handle, nullptr);
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.latitude.source=latitudeOfFirstGridPointInDegrees\n"
    "metadata.field.latitude.type=double\n"
    "metadata.field.latitude.scale=1e308\n"
    "metadata.field.latitude.required=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  EXPECT_FALSE(extract_configured_metadata(handle.get(), parsed.value()).ok());
  EXPECT_FALSE(extract_configured_metadata(nullptr, parsed.value()).ok());
}

static std::vector<uint8_t> make_grib1(size_t total_len) {
  std::vector<uint8_t> buf(total_len, 0);
  buf[0] = 'G'; buf[1] = 'R'; buf[2] = 'I'; buf[3] = 'B';
  buf[4] = static_cast<uint8_t>((total_len >> 16) & 0xFF);
  buf[5] = static_cast<uint8_t>((total_len >> 8) & 0xFF);
  buf[6] = static_cast<uint8_t>(total_len & 0xFF);
  buf[7] = 1;
  return buf;
}

static std::vector<uint8_t> make_grib2(size_t total_len) {
  std::vector<uint8_t> buf(total_len, 0);
  buf[0] = 'G'; buf[1] = 'R'; buf[2] = 'I'; buf[3] = 'B';
  buf[4] = 0; buf[5] = 0; buf[6] = 0; buf[7] = 2;
  buf[8] = static_cast<uint8_t>((total_len >> 56) & 0xFF);
  buf[9] = static_cast<uint8_t>((total_len >> 48) & 0xFF);
  buf[10] = static_cast<uint8_t>((total_len >> 40) & 0xFF);
  buf[11] = static_cast<uint8_t>((total_len >> 32) & 0xFF);
  buf[12] = static_cast<uint8_t>((total_len >> 24) & 0xFF);
  buf[13] = static_cast<uint8_t>((total_len >> 16) & 0xFF);
  buf[14] = static_cast<uint8_t>((total_len >> 8) & 0xFF);
  buf[15] = static_cast<uint8_t>(total_len & 0xFF);
  return buf;
}

TEST(GribFindMessages, NullData) {
  auto msgs = find_messages(nullptr, 100);
  EXPECT_TRUE(msgs.empty());
}

TEST(GribFindMessages, ZeroLength) {
  std::vector<uint8_t> buf(100, 0);
  auto msgs = find_messages(buf.data(), 0);
  EXPECT_TRUE(msgs.empty());
}

TEST(GribFindMessages, NoMagic) {
  std::vector<uint8_t> buf(100, 0x55);
  auto msgs = find_messages(buf.data(), buf.size());
  EXPECT_TRUE(msgs.empty());
}

TEST(GribFindMessages, SingleGrib1) {
  auto buf = make_grib1(32);
  auto msgs = find_messages(buf.data(), buf.size());
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].pos, 0);
  EXPECT_EQ(msgs[0].len, 32);
  EXPECT_EQ(msgs[0].edition, 1);
}

TEST(GribFindMessages, SingleGrib2) {
  auto buf = make_grib2(64);
  auto msgs = find_messages(buf.data(), buf.size());
  EXPECT_EQ(msgs.size(), 1);
  EXPECT_EQ(msgs[0].pos, 0);
  EXPECT_EQ(msgs[0].len, 64);
  EXPECT_EQ(msgs[0].edition, 2);
}

TEST(GribFindMessages, TwoMessages) {
  auto grib1 = make_grib1(32);
  auto grib2 = make_grib2(64);
  std::vector<uint8_t> buf;
  buf.insert(buf.end(), grib1.begin(), grib1.end());
  buf.insert(buf.end(), grib2.begin(), grib2.end());
  auto msgs = find_messages(buf.data(), buf.size());
  EXPECT_EQ(msgs.size(), 2);
  EXPECT_EQ(msgs[0].len, 32);
  EXPECT_EQ(msgs[1].len, 64);
}

TEST(GribFindMessages, IncompleteTail) {
  auto buf = make_grib2(200);
  buf.resize(100);
  auto msgs = find_messages(buf.data(), buf.size());
  EXPECT_TRUE(msgs.empty());
}

TEST(GribFindMessages, IncrementalScan_Complete) {
  auto buf = make_grib1(32);
  auto result = find_messages_incremental(buf.data(), buf.size(), 0);
  EXPECT_FALSE(result.incomplete_tail);
  EXPECT_EQ(result.messages.size(), 1);
}

TEST(GribFindMessages, IncrementalScan_Offset) {
  auto grib1 = make_grib1(32);
  std::vector<uint8_t> buf(50, 0x55);
  std::copy(grib1.begin(), grib1.end(), buf.begin() + 10);
  auto result = find_messages_incremental(buf.data(), buf.size(), 10);
  EXPECT_FALSE(result.incomplete_tail);
  EXPECT_EQ(result.messages.size(), 1);
}

TEST(GribFindMsgInfo, EmptyIndex) {
  std::string index_text = "";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto mi = find_msg_info(index_text, kv);
  EXPECT_FALSE(mi.ok);
}

TEST(GribFindMsgInfo, SingleMatch) {
  std::string index_text = R"({"sn":"t","of":100,"ln":200,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto mi = find_msg_info(index_text, kv);
  EXPECT_TRUE(mi.ok);
  EXPECT_EQ(mi.off, 100);
  EXPECT_EQ(mi.len, 200);
}

TEST(GribFindMsgInfo, NoMatch) {
  std::string index_text = R"({"sn":"t","of":100,"ln":200,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "x"}};
  auto mi = find_msg_info(index_text, kv);
  EXPECT_FALSE(mi.ok);
}

TEST(GribFindMsgInfo, MatchWithLevel) {
  std::string index_text = R"({"sn":"t","lv":500,"of":100,"ln":200,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}, {"level", "500"}};
  auto mi = find_msg_info(index_text, kv);
  EXPECT_TRUE(mi.ok);
}

TEST(GribFindMsgInfo, WrongLevel) {
  std::string index_text = R"({"sn":"t","lv":500,"of":100,"ln":200,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}, {"level", "850"}};
  auto mi = find_msg_info(index_text, kv);
  EXPECT_FALSE(mi.ok);
}

TEST(GribFindMsgInfo, MultipleMatchesFailUniqueLookup) {
  std::string index_text = R"({"sn":"t","lv":500,"of":100,"ln":200,"ed":2}
{"sn":"t","lv":850,"of":300,"ln":400,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto mi = find_msg_info(index_text, kv);
  EXPECT_FALSE(mi.ok);
}

TEST(GribFindMsgInfo, ExtractsGridDims) {
  std::string index_text = R"({"sn":"t","ni":100,"nj":80,"np":8000,"of":100,"ln":200,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto mi = find_msg_info(index_text, kv);
  EXPECT_TRUE(mi.ok);
  EXPECT_EQ(mi.ni, 100);
  EXPECT_EQ(mi.nj, 80);
  EXPECT_EQ(mi.numberOfDataPoints, 8000);
}

TEST(GribFindAllMsgMatches, Basic) {
  std::string index_text = R"({"sn":"t","lv":500,"of":100,"ln":200,"ed":2}
{"sn":"t","lv":850,"of":300,"ln":400,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto matches = find_all_msg_matches(index_text, kv);
  EXPECT_EQ(matches.size(), 2);
}

TEST(GribFindAllMsgMatches, WithLimit) {
  std::string index_text = R"({"sn":"t","lv":500,"of":100,"ln":200,"ed":2}
{"sn":"t","lv":850,"of":300,"ln":400,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto matches = find_all_msg_matches(index_text, kv, 1);
  EXPECT_EQ(matches.size(), 1);
}

TEST(GribFindAllMsgMatches, NoMatches) {
  std::string index_text = R"({"sn":"x","of":100,"ln":200,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto matches = find_all_msg_matches(index_text, kv);
  EXPECT_TRUE(matches.empty());
}

TEST(GribFindAllMsgMatches, ExtractsLabels) {
  std::string index_text = R"({"sn":"t","tl":"isobaricInhPa","sr":"0-12","u":"K","of":100,"ln":200,"ed":2})";
  std::map<std::string, std::string> kv = {{"shortName", "t"}};
  auto matches = find_all_msg_matches(index_text, kv);
  ASSERT_EQ(matches.size(), 1);
  EXPECT_EQ(matches[0].labels["shortName"], "t");
  EXPECT_EQ(matches[0].labels["typeOfLevel"], "isobaricInhPa");
  EXPECT_EQ(matches[0].labels["stepRange"], "0-12");
  EXPECT_EQ(matches[0].labels["unit"], "K");
}

TEST(GribFindMsgInfo, LongFormIndexSupportsAllSelectionFieldsAndLabels) {
  const std::string index = R"({"shortName":"t","typeOfLevel":"isobaricInhPa","stepRange":"0-6","unit":"K","globalDomain":"g","level":-5,"step":6,"ni":4,"nj":3,"numberOfDataPoints":12,"offset":37,"totalLength":100,"edition":2})";
  const std::map<std::string, std::string> query{
    {"shortName", "t"}, {"typeOfLevel", "isobaricInhPa"}, {"stepRange", "0-6"},
    {"unit", "K"}, {"globalDomain", "g"}, {"level", "-5"}, {"step", "6"},
    {"ni", "4"}, {"nj", "3"}, {"numberOfDataPoints", "12"}, {"edition", "2"}};
  const auto match = find_msg_info(index, query);
  ASSERT_TRUE(match.ok);
  EXPECT_EQ(match.off, 37u);
  EXPECT_EQ(match.len, 100u);
  EXPECT_EQ(match.ni, 4);
  EXPECT_EQ(match.nj, 3);
  EXPECT_EQ(match.numberOfDataPoints, 12);
  const auto all = find_all_msg_matches(index, query);
  ASSERT_EQ(all.size(), 1u);
  EXPECT_EQ(all.front().labels.at("globalDomain"), "g");
  EXPECT_EQ(all.front().labels.at("step"), "6");
  EXPECT_EQ(all.front().labels.at("level"), "-5");
}

TEST(GribFindMsgInfo, CustomStringAndNumericFieldsUseExactTypedMatches) {
  const std::string index = R"({"experiment":"control","member":-7,"of":0,"ln":100})";
  for (const auto& query : std::vector<std::map<std::string, std::string>>{
       {{"experiment", "control"}}, {{"member", "-7"}},
       {{"experiment", "control"}, {"member", "-7"}}}) {
    EXPECT_TRUE(find_msg_info(index, query).ok);
    EXPECT_EQ(find_all_msg_matches(index, query).size(), 1u);
  }
  for (const auto& query : std::vector<std::map<std::string, std::string>>{
       {{"experiment", "forecast"}}, {{"member", "7"}}, {{"member", ""}},
       {{"member", "-7invalid"}}, {{"missingField", "0"}}, {{"level", "0"}}}) {
    EXPECT_FALSE(find_msg_info(index, query).ok);
    EXPECT_TRUE(find_all_msg_matches(index, query).empty());
  }
}

TEST(GribFindMsgInfo, EscapedQuotesAndBackslashesMatchDecodedStrings) {
  const std::string index = R"({"sn": "a\"b\\c","of": 0,"ln": 100})";
  const std::map<std::string, std::string> query{{"shortName", "a\"b\\c"}};
  EXPECT_TRUE(find_msg_info(index, query).ok);
  const auto matches = find_all_msg_matches(index, query);
  ASSERT_EQ(matches.size(), 1u);
  EXPECT_EQ(matches.front().labels.at("shortName"), "a\"b\\c");
}

TEST(GribFindMsgInfo, SkipsEmptyLinesAndIncompleteOrNegativeSliceLocations) {
  const std::string index =
    "\n{\"sn\":\"t\",\"ln\":100}\n"
    "{\"sn\":\"t\",\"of\":0}\n"
    "{\"sn\":\"t\",\"of\":-1,\"ln\":100}\n"
    "{\"sn\":\"t\",\"of\":0,\"ln\":-1}\n"
    "{\"sn\":\"t\",\"of\":37,\"ln\":100}\n\n";
  const std::map<std::string, std::string> query{{"shortName", "t"}};
  const auto first = find_msg_info(index, query);
  ASSERT_TRUE(first.ok);
  EXPECT_EQ(first.off, 37u);
  const auto all = find_all_msg_matches(index, query);
  ASSERT_EQ(all.size(), 1u);
  EXPECT_EQ(all.front().info.off, 37u);
  EXPECT_TRUE(find_all_msg_matches("", query).empty());
}

TEST(GribFindMsgInfo, MalformedScalarValuesCannotSatisfySelection) {
  for (const auto& index : std::vector<std::string>{
       "{\"sn\":", "{\"sn\": ", "{\"sn\":123}", "{\"sn\":\"unterminated"}) {
    SCOPED_TRACE(index);
    EXPECT_FALSE(find_msg_info(index, {{"shortName", "t"}}).ok);
  }
  for (const auto& value : {"", " ", "-", "null", "\"500\""}) {
    const auto index = std::string("{\"lv\":") + value + ",\"of\":0,\"ln\":100}";
    SCOPED_TRACE(index);
    EXPECT_FALSE(find_msg_info(index, {{"level", "500"}}).ok);
  }
  EXPECT_FALSE(find_msg_info("{\"lv\":", {{"level", "500"}}).ok);
  EXPECT_FALSE(find_msg_info("{\"lv\":-", {{"level", "500"}}).ok);
}

TEST(GribFileMetadata, NullHandleDoesNotReplaceExistingMetadata) {
  FileMeta metadata;
  metadata.shortName = "existing";
  EXPECT_FALSE(extract_file_meta(nullptr, metadata));
  EXPECT_FALSE(metadata.has);
  EXPECT_EQ(metadata.shortName, "existing");
}

TEST(GribParseDtype, DefaultIsF32) {
  std::map<std::string, std::string> kv = {};
  EXPECT_EQ(parse_dtype_from_kv(kv), DataDType::F32);
}

TEST(GribParseDtype, F64Variant) {
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "f64"}}), DataDType::F64);
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "float64"}}), DataDType::F64);
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "double"}}), DataDType::F64);
}

TEST(GribParseDtype, F64CaseInsensitive) {
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "F64"}}), DataDType::F64);
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "FLOAT64"}}), DataDType::F64);
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "Double"}}), DataDType::F64);
}

TEST(GribParseDtype, F32Variant) {
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "f32"}}), DataDType::F32);
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "float32"}}), DataDType::F32);
}

TEST(GribParseDtype, UnknownDefaultsToF32) {
  EXPECT_EQ(parse_dtype_from_kv({{"dtype", "other"}}), DataDType::F32);
}

TEST(GribDtypeBytes, F32) {
  EXPECT_EQ(dtype_bytes(DataDType::F32), 4);
}

TEST(GribDtypeBytes, F64) {
  EXPECT_EQ(dtype_bytes(DataDType::F64), 8);
}

TEST(GribParseCropParams, Disabled) {
  auto cp = parse_crop_params_from_kv({});
  EXPECT_FALSE(cp.enabled);
}

TEST(GribParseCropParams, PointCrop) {
  auto cp = parse_crop_params_from_kv({{"lat0", "30"}, {"lon0", "100"}});
  EXPECT_TRUE(cp.enabled);
  EXPECT_DOUBLE_EQ(cp.lat0, 30.0);
  EXPECT_DOUBLE_EQ(cp.lon0, 100.0);
  EXPECT_DOUBLE_EQ(cp.lat1, 30.0);
  EXPECT_DOUBLE_EQ(cp.lon1, 100.0);
}

TEST(GribParseCropParams, FullCrop) {
  auto cp = parse_crop_params_from_kv({
    {"lat0", "30"}, {"lon0", "100"},
    {"lat1", "10"}, {"lon1", "140"}
  });
  EXPECT_TRUE(cp.enabled);
  EXPECT_DOUBLE_EQ(cp.lat0, 30.0);
  EXPECT_DOUBLE_EQ(cp.lon0, 100.0);
  EXPECT_DOUBLE_EQ(cp.lat1, 10.0);
  EXPECT_DOUBLE_EQ(cp.lon1, 140.0);
}

TEST(GribParseCropParams, OnlyLat0) {
  auto cp = parse_crop_params_from_kv({{"lat0", "30"}});
  EXPECT_FALSE(cp.enabled);
}

TEST(GribParseCropParams, OnlyLon0) {
  auto cp = parse_crop_params_from_kv({{"lon0", "100"}});
  EXPECT_FALSE(cp.enabled);
}

TEST(GribParseInterpParams, Disabled) {
  auto ip = parse_interp_params_from_kv({});
  EXPECT_FALSE(ip.enabled);
}

TEST(GribParseInterpParams, Bilinear) {
  auto ip = parse_interp_params_from_kv({{"interp", "bilinear"}});
  EXPECT_TRUE(ip.enabled);
}

TEST(GribParseInterpParams, BilinearCaseInsensitive) {
  auto ip = parse_interp_params_from_kv({{"interp", "BILINEAR"}});
  EXPECT_TRUE(ip.enabled);
}

TEST(GribParseInterpParams, NotBilinear) {
  auto ip = parse_interp_params_from_kv({{"interp", "nearest"}});
  EXPECT_FALSE(ip.enabled);
}

TEST(GribParseInterpParams, OutNiNj) {
  auto ip = parse_interp_params_from_kv({
    {"interp", "bilinear"},
    {"outNi", "100"},
    {"outNj", "80"}
  });
  EXPECT_TRUE(ip.enabled);
  EXPECT_EQ(ip.outNi, 100);
  EXPECT_EQ(ip.outNj, 80);
}

TEST(GribParseInterpParams, DlonDlat) {
  auto ip = parse_interp_params_from_kv({
    {"interp", "bilinear"},
    {"dlon", "0.25"},
    {"dlat", "0.25"}
  });
  EXPECT_TRUE(ip.enabled);
  EXPECT_DOUBLE_EQ(ip.dlon, 0.25);
  EXPECT_DOUBLE_EQ(ip.dlat, 0.25);
}

TEST(GribParseInterpParams, BorderClamp) {
  auto ip = parse_interp_params_from_kv({{"border", "clamp"}});
  EXPECT_EQ(ip.border, InterpParams::Border::CLAMP);
}

TEST(GribParseInterpParams, BorderNan) {
  auto ip = parse_interp_params_from_kv({{"border", "nan"}});
  EXPECT_EQ(ip.border, InterpParams::Border::AS_NAN);
}

TEST(GribParseInterpParams, BorderNanCaseInsensitive) {
  auto ip = parse_interp_params_from_kv({{"border", "NaN"}});
  EXPECT_EQ(ip.border, InterpParams::Border::AS_NAN);
}

TEST(GribParseInterpParams, BorderDefaultIsClamp) {
  auto ip = parse_interp_params_from_kv({});
  EXPECT_EQ(ip.border, InterpParams::Border::CLAMP);
}

TEST(GribStripCropKeys, WithCropKeys) {
  std::map<std::string, std::string> kv = {
    {"lat0", "30"}, {"lon0", "100"}, {"shortName", "t"}
  };
  strip_crop_keys(kv);
  EXPECT_EQ(kv.size(), 1);
  EXPECT_EQ(kv["shortName"], "t");
}

TEST(GribStripCropKeys, WithoutCropKeys) {
  std::map<std::string, std::string> kv = {
    {"shortName", "t"}, {"level", "500"}
  };
  strip_crop_keys(kv);
  EXPECT_EQ(kv.size(), 2);
}

TEST(GribStripCropKeys, Empty) {
  std::map<std::string, std::string> kv = {};
  strip_crop_keys(kv);
  EXPECT_TRUE(kv.empty());
}

TEST(GribStripCropKeys, PartialCropKeys) {
  std::map<std::string, std::string> kv = {
    {"lat0", "30"}, {"shortName", "t"}
  };
  strip_crop_keys(kv);
  EXPECT_EQ(kv.size(), 1);
  EXPECT_EQ(kv["shortName"], "t");
}

namespace {

// A small, nonconstant regular grid provides independently calculable answers:
// longitude grows by 10 per column and latitude contributes 100 per row.
class GribRegularGrid : public ::testing::TestWithParam<int> {
protected:
  using Handle = std::unique_ptr<codes_handle, decltype(&codes_handle_delete)>;
  Handle handle{nullptr, codes_handle_delete};
  std::vector<uint8_t> message;
  const std::vector<double> expected{
    0, 10, 20, 30, 100, 110, 120, 130, 200, 210, 220, 230};
  MsgInfo info;
  size_t reads{0};

  void SetUp() override {
    const std::string sample = "regular_ll_sfc_grib" + std::to_string(GetParam());
    handle.reset(codes_grib_handle_new_from_samples(nullptr, sample.c_str()));
    ASSERT_NE(handle, nullptr);
    ASSERT_EQ(codes_set_long(handle.get(), "Ni", 4), CODES_SUCCESS);
    ASSERT_EQ(codes_set_long(handle.get(), "Nj", 3), CODES_SUCCESS);
    ASSERT_EQ(codes_set_long(handle.get(), "jScansPositively", 0), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfFirstGridPointInDegrees", 2), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfFirstGridPointInDegrees", 0), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfLastGridPointInDegrees", 0), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfLastGridPointInDegrees", 3), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "iDirectionIncrementInDegrees", 1), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double(handle.get(), "jDirectionIncrementInDegrees", 1), CODES_SUCCESS);
    ASSERT_EQ(codes_set_double_array(handle.get(), "values", expected.data(), expected.size()), CODES_SUCCESS);
    const void* bytes = nullptr;
    size_t size = 0;
    ASSERT_EQ(codes_get_message(handle.get(), &bytes, &size), CODES_SUCCESS);
    const auto* first = static_cast<const uint8_t*>(bytes);
    message.assign(first, first + size);
    info.off = 37;
    info.len = size;
    info.ok = true;
  }

  ReadObjectPartFunc reader() {
    return [this](uint64_t offset, uint64_t length)
      -> std::optional<std::vector<uint8_t>> {
      ++reads;
      EXPECT_EQ(offset, info.off);
      EXPECT_EQ(length, info.len);
      return message;
    };
  }

  template <typename T>
  std::vector<T> unpack(const std::optional<std::vector<uint8_t>>& bytes) {
    if (!bytes) {
      ADD_FAILURE() << "GRIB operation failed";
      return {};
    }
    if (bytes->size() % sizeof(T) != 0) {
      ADD_FAILURE() << "decoded buffer is not aligned to its element size";
      return {};
    }
    std::vector<T> values(bytes->size() / sizeof(T));
    if (!bytes->empty()) std::memcpy(values.data(), bytes->data(), bytes->size());
    return values;
  }

  std::string cache_token() const {
    static std::atomic<unsigned> serial{0};
    return "grib-ut-" + std::to_string(++serial);
  }

  void refresh_message() {
    const void* bytes = nullptr;
    size_t size = 0;
    ASSERT_EQ(codes_get_message(handle.get(), &bytes, &size), CODES_SUCCESS);
    const auto* first = static_cast<const uint8_t*>(bytes);
    message.assign(first, first + size);
    info.len = size;
  }
};

TEST_P(GribRegularGrid, DetectsRealMessageWithJunkPrefixAndSuffix) {
  std::vector<uint8_t> bytes(13, 0x55);
  bytes.insert(bytes.end(), message.begin(), message.end());
  bytes.insert(bytes.end(), 19, 0x55);
  auto found = find_messages(bytes.data(), bytes.size());
  ASSERT_EQ(found.size(), 1u);
  EXPECT_EQ(found.front().pos, 13u);
  EXPECT_EQ(found.front().len, message.size());
  EXPECT_EQ(found.front().edition, GetParam());
}

TEST_P(GribRegularGrid, ResumesIncompleteMessageWithoutLosingPrefixOffset) {
  std::vector<uint8_t> bytes(23, 0x55);
  bytes.insert(bytes.end(), message.begin(), message.end());
  const auto partial = find_messages_incremental(bytes.data(), 23 + 32, 0);
  EXPECT_TRUE(partial.incomplete_tail);
  EXPECT_TRUE(partial.messages.empty());
  EXPECT_EQ(partial.next_offset, 23u);
  const auto complete = find_messages_incremental(bytes.data(), bytes.size(), partial.next_offset);
  EXPECT_FALSE(complete.incomplete_tail);
  ASSERT_EQ(complete.messages.size(), 1u);
  EXPECT_EQ(complete.messages.front().pos, 23u);
  EXPECT_EQ(complete.messages.front().len, message.size());
}

TEST_P(GribRegularGrid, CopiedHandleOwnsBytesAfterSourceIsReleased) {
  auto source = message;
  Handle copy(new_handle_from_message_copy_guarded(source.data(), source.size()), codes_handle_delete);
  ASSERT_NE(copy, nullptr);
  source.assign(source.size(), 0);
  source.clear();
  source.shrink_to_fit();
  std::vector<double> values(expected.size());
  size_t count = values.size();
  ASSERT_EQ(codes_get_double_array(copy.get(), "values", values.data(), &count), CODES_SUCCESS);
  EXPECT_EQ(values, expected);
}

TEST_P(GribRegularGrid, IndexLineRoundTripsDimensionsOffsetsAndLabels) {
  const auto line = build_index_line(handle.get(), info.off, message.size(), GetParam());
  auto matches = find_all_msg_matches(line, {{"edition", std::to_string(GetParam())}});
  ASSERT_EQ(matches.size(), 1u) << line;
  EXPECT_TRUE(matches[0].info.ok);
  EXPECT_EQ(matches[0].info.off, info.off);
  EXPECT_EQ(matches[0].info.len, info.len);
  EXPECT_EQ(matches[0].info.ni, 4);
  EXPECT_EQ(matches[0].info.nj, 3);
  EXPECT_EQ(matches[0].info.numberOfDataPoints, 12);
  EXPECT_EQ(matches[0].labels.at("unit"), "K");
  EXPECT_EQ(line.back(), '\n');
}

TEST_P(GribRegularGrid, ExtractsMetadataInDegrees) {
  FileMeta metadata;
  ASSERT_TRUE(extract_file_meta(handle.get(), metadata));
  EXPECT_TRUE(metadata.has);
  EXPECT_EQ(metadata.Ni, 4);
  EXPECT_EQ(metadata.Nj, 3);
  EXPECT_EQ(metadata.numberOfDataPoints, 12);
  EXPECT_DOUBLE_EQ(metadata.latitudeOfFirstGridPoint, 2);
  EXPECT_DOUBLE_EQ(metadata.longitudeOfLastGridPoint, 3);
  EXPECT_DOUBLE_EQ(metadata.iDirectionIncrement, 1);
  EXPECT_DOUBLE_EQ(metadata.jDirectionIncrement, 1);
  EXPECT_EQ(metadata.unit, "K");
}

TEST_P(GribRegularGrid, CountFallsBackToRealMessageAndForwardsSlice) {
  EXPECT_EQ(get_values_count_from_message_bytes(message), 12u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader()), 12u);
  EXPECT_EQ(reads, 1u);
}

TEST_P(GribRegularGrid, IndexDimensionsAndPointCountAvoidObjectRead) {
  info.ni = 4;
  info.nj = 3;
  EXPECT_EQ(compute_values_count_for_msg(info, reader()), 12u);
  info.ni = 0;
  info.numberOfDataPoints = 12;
  EXPECT_EQ(compute_values_count_for_msg(info, reader()), 12u);
  EXPECT_EQ(reads, 0u);
}

TEST_P(GribRegularGrid, DecodesAllValuesAsFloat32AndFloat64) {
  EXPECT_EQ(unpack<double>(decode_values_as_dtype(info, DataDType::F64, reader())), expected);
  const std::vector<float> floats(expected.begin(), expected.end());
  EXPECT_EQ(unpack<float>(decode_values_as_dtype(info, DataDType::F32, reader())), floats);
  EXPECT_EQ(reads, 2u);
}

TEST_P(GribRegularGrid, DisabledCropPreservesEntireGridAndDtype) {
  EXPECT_EQ(compute_cropped_value_count(info, CropParams{}, reader()), 12u);
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64, {}, reader())), expected);
  const std::vector<float> floats(expected.begin(), expected.end());
  EXPECT_EQ(unpack<float>(decode_and_maybe_crop_values(info, DataDType::F32, {}, reader())), floats);
}

TEST_P(GribRegularGrid, RectangularCropIsInclusiveAndPreservesRowOrder) {
  const CropParams crop{true, 2, 1, 1, 2};
  EXPECT_EQ(compute_cropped_value_count(info, crop, reader()), 4u);
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader())),
    (std::vector<double>{10, 20, 110, 120}));
  EXPECT_EQ(unpack<float>(decode_and_maybe_crop_values(info, DataDType::F32, crop, reader())),
    (std::vector<float>{10, 20, 110, 120}));
}

TEST_P(GribRegularGrid, ReversedCropCornersSelectTheSameRectangle) {
  const CropParams crop{true, 1, 2, 2, 1};
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader())),
    (std::vector<double>{10, 20, 110, 120}));
}

TEST_P(GribRegularGrid, StartOnlyCropExtendsToLastGridPoint) {
  const auto crop = parse_crop_params_from_kv({{"lat0", "1"}, {"lon0", "2"}});
  EXPECT_EQ(compute_cropped_value_count(info, crop, reader()), 4u);
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader())),
    (std::vector<double>{120, 130, 220, 230}));
}

TEST_P(GribRegularGrid, CropClampsLatitudeToGridBoundary) {
  const CropParams crop{true, 10, 0, -10, 3};
  EXPECT_EQ(compute_cropped_value_count(info, crop, reader()), expected.size());
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader())), expected);
}

TEST_P(GribRegularGrid, BilinearUpsamplingReproducesLinearField) {
  InterpParams interp;
  interp.enabled = true;
  interp.outNi = 7;
  interp.outNj = 5;
  std::vector<double> interpolated;
  for (int row = 0; row < 5; ++row)
    for (int col = 0; col < 7; ++col)
      interpolated.push_back(50.0 * row + 5.0 * col);
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64, {}, interp, reader())), interpolated);
  const std::vector<float> floats(interpolated.begin(), interpolated.end());
  EXPECT_EQ(unpack<float>(decode_crop_and_bilinear_resample(info, DataDType::F32, {}, interp, reader())), floats);
}

TEST_P(GribRegularGrid, BilinearResamplingUsesSelectedCropDomain) {
  // The selected 2x2 crop must retain its coordinates when resampled.
  const CropParams crop{true, 2, 1, 1, 2};
  const InterpParams interp{true, 2, 2};
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(
    info, DataDType::F64, crop, interp, reader())),
    (std::vector<double>{10, 20, 110, 120}));
}

TEST_P(GribRegularGrid, RegionalLongitudeOutsideGridDoesNotWrapByRegionWidth) {
  // A 0..10 degree request covers the whole regional 0..3 degree grid.
  const CropParams crop{true, 2, 0, 0, 10};
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(
    info, DataDType::F64, crop, reader())), expected);
}

TEST_P(GribRegularGrid, EquivalentLongitudesSelectTheSameRegionalColumns) {
  for (const auto& longitude : std::vector<std::pair<double, double>>{
      {1, 2}, {361, 362}, {-359, -358}, {721, 722},
      {360000001, 360000002}, {-359999999, -359999998}}) {
    SCOPED_TRACE(longitude.first);
    const CropParams crop{true, 2, longitude.first, 1, longitude.second};
    EXPECT_EQ(compute_cropped_value_count(info, crop, reader()), 4u);
    EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64,
      crop, reader())), (std::vector<double>{10, 20, 110, 120}));
    EXPECT_EQ(unpack<float>(decode_crop_and_bilinear_resample(info, DataDType::F32,
      crop, InterpParams{true, 2, 2}, reader())),
      (std::vector<float>{10, 20, 110, 120}));
  }
}

TEST_P(GribRegularGrid, LongitudeRangesOutsideRegionClampToClosestEdge) {
  const CropParams west{true, 2, -10, 0, -1};
  const CropParams east{true, 2, 10, 0, 20};
  EXPECT_EQ(compute_cropped_value_count(info, west, reader()), 3u);
  EXPECT_EQ(compute_cropped_value_count(info, east, reader()), 3u);
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64,
    west, reader())), (std::vector<double>{0, 100, 200}));
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64,
    east, reader())), (std::vector<double>{30, 130, 230}));
  const CropParams overlapping{true, 2, -1, 0, 1};
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64,
    overlapping, reader())), (std::vector<double>{0, 10, 100, 110, 200, 210}));
}

TEST_P(GribRegularGrid, NegativeLongitudesSelectRegionNear360Degrees) {
  ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfFirstGridPointInDegrees", 350), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfLastGridPointInDegrees", 353), CODES_SUCCESS);
  refresh_message();
  const CropParams crop{true, 2, -9, 1, -8};
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64,
    crop, reader())), (std::vector<double>{10, 20, 110, 120}));
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    crop, InterpParams{true, 2, 2}, reader())),
    (std::vector<double>{10, 20, 110, 120}));
}

TEST_P(GribRegularGrid, CropAcrossZeroLongitudeSelectsBothGridEdges) {
  constexpr long ni = 1440;
  ASSERT_EQ(codes_set_long(handle.get(), "Ni", ni), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(),
    "longitudeOfLastGridPointInDegrees", 359.75), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(),
    "iDirectionIncrementInDegrees", 0.25), CODES_SUCCESS);
  std::vector<double> global_values(3 * ni);
  for (long j = 0; j < 3; ++j)
    for (long i = 0; i < ni; ++i)
      global_values[static_cast<std::size_t>(j * ni + i)] = j * 2000 + i;
  ASSERT_EQ(codes_set_double_array(handle.get(), "values",
    global_values.data(), global_values.size()), CODES_SUCCESS);
  refresh_message();

  const CropParams crop{true, 2, -10, 1, 10};
  EXPECT_EQ(compute_cropped_value_count(info, crop, reader()), 162u);
  const auto cropped = unpack<double>(decode_and_maybe_crop_values(
    info, DataDType::F64, crop, reader()));
  std::vector<double> expected_crop;
  for (long j = 0; j < 2; ++j) {
    for (long i = 1400; i < ni; ++i)
      expected_crop.push_back(j * 2000 + i);
    for (long i = 0; i <= 40; ++i)
      expected_crop.push_back(j * 2000 + i);
  }
  EXPECT_EQ(cropped, expected_crop);

  const auto grid = describe_output_grid(info, crop, {}, reader(),
    cache_token());
  ASSERT_TRUE(grid);
  EXPECT_EQ(grid->ni, 81u);
  EXPECT_EQ(grid->nj, 2u);
  EXPECT_DOUBLE_EQ(grid->longitude_axis.front(), 350);
  EXPECT_DOUBLE_EQ(grid->longitude_axis[39], 359.75);
  EXPECT_DOUBLE_EQ(grid->longitude_axis[40], 0);
  EXPECT_DOUBLE_EQ(grid->longitude_axis[80], 10);
}

TEST_P(GribRegularGrid, BilinearInterpolationCrossesLongitudeSeam) {
  ASSERT_EQ(codes_set_long(handle.get(), "Ni", 360), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(),
    "longitudeOfLastGridPointInDegrees", 359), CODES_SUCCESS);
  std::vector<double> periodic_values(3 * 360);
  for (long j = 0; j < 3; ++j) {
    for (long i = 0; i < 360; ++i) {
      periodic_values[static_cast<std::size_t>(j * 360 + i)] =
        i <= 180 ? i : i - 360;
    }
  }
  ASSERT_EQ(codes_set_double_array(handle.get(), "values",
    periodic_values.data(), periodic_values.size()), CODES_SUCCESS);
  refresh_message();

  const CropParams crop{true, 2, -1, 2, 1, false};
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info,
    DataDType::F64, crop, InterpParams{true, 5, 1}, reader())),
    (std::vector<double>{-1, -0.5, 0, 0.5, 1}));
}

TEST_P(GribRegularGrid, CropInterpolationResolutionUsesSelectedSpan) {
  const CropParams crop{true, 2, 1, 1, 2};
  InterpParams interpolation;
  interpolation.enabled = true;
  interpolation.dlon = 0.5;
  interpolation.dlat = 0.5;
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    crop, interpolation, reader())),
    (std::vector<double>{10, 15, 20, 60, 65, 70, 110, 115, 120}));
  interpolation.dlon = 0;
  interpolation.dlat = 0;
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    crop, interpolation, reader())),
    (std::vector<double>{10, 20, 110, 120}));
}

TEST_P(GribRegularGrid, CropInterpolationUsesRoundedGridWindowCoordinates) {
  const CropParams crop{true, 1.8, 0.7, 0.8, 2.2};
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    crop, InterpParams{true, 2, 2}, reader())),
    (std::vector<double>{10, 20, 110, 120}));
}

TEST_P(GribRegularGrid, CropInterpolationPreservesNorthToSouthOutputForAscendingInput) {
  ASSERT_EQ(codes_set_long(handle.get(), "jScansPositively", 1), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfFirstGridPointInDegrees", 0), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfLastGridPointInDegrees", 2), CODES_SUCCESS);
  refresh_message();
  const CropParams crop{true, 0, 1, 1, 2};
  const InterpParams interpolation{true, 2, 2};
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    crop, interpolation, reader())),
    (std::vector<double>{110, 120, 10, 20}));
  EXPECT_EQ(unpack<float>(decode_crop_and_bilinear_resample(info, DataDType::F32,
    crop, interpolation, reader())),
    (std::vector<float>{110, 120, 10, 20}));
  // Whole-field interpolation keeps its established north-to-south output.
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    {}, InterpParams{true, 4, 3}, reader())),
    (std::vector<double>{200, 210, 220, 230, 100, 110, 120, 130, 0, 10, 20, 30}));
}

TEST_P(GribRegularGrid, SingleColumnCropInterpolatesOnlyItsSelectedLatitudeSpan) {
  ASSERT_EQ(codes_set_long(handle.get(), "Ni", 1), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfLastGridPointInDegrees", 0), CODES_SUCCESS);
  const std::vector<double> column{0, 100, 200};
  ASSERT_EQ(codes_set_double_array(handle.get(), "values", column.data(), column.size()), CODES_SUCCESS);
  refresh_message();
  const CropParams crop{true, 2, 360, 1, 720};
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64,
    crop, reader())), (std::vector<double>{0, 100}));
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    crop, InterpParams{true, 1, 3}, reader())),
    (std::vector<double>{0, 50, 100}));
}

TEST_P(GribRegularGrid, NonFiniteCropCoordinatesReturnFailure) {
  for (const double invalid : {std::numeric_limits<double>::infinity(),
      -std::numeric_limits<double>::infinity(),
      std::numeric_limits<double>::quiet_NaN()}) {
    for (const auto& crop : std::vector<CropParams>{
        {true, invalid, 1, 1, 2}, {true, 2, invalid, 1, 2},
        {true, 2, 1, invalid, 2}, {true, 2, 1, 1, invalid}}) {
      EXPECT_FALSE(compute_cropped_value_count(info, crop, reader()));
      EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader()));
      EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64,
        crop, InterpParams{true, 2, 2}, reader()));
    }
  }
}

TEST_P(GribRegularGrid, BilinearResolutionDeterminesOutputSizeAndValues) {
  InterpParams interp;
  interp.enabled = true;
  interp.dlon = 1.5;
  interp.dlat = 1;
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64, {}, interp, reader())),
    (std::vector<double>{0, 15, 30, 100, 115, 130, 200, 215, 230}));
}

TEST_P(GribRegularGrid, BilinearOutsideGridUsesSelectedBorderPolicy) {
  InterpParams interp;
  interp.enabled = true;
  interp.outNi = 3;
  interp.outNj = 2;
  interp.dlon = 2;
  interp.dlat = 3;
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64, {}, interp, reader())),
    (std::vector<double>{0, 20, 30, 200, 220, 230}));
  interp.border = InterpParams::Border::AS_NAN;
  const auto nan_border = unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64, {}, interp, reader()));
  ASSERT_EQ(nan_border.size(), 6u);
  EXPECT_DOUBLE_EQ(nan_border[0], 0);
  EXPECT_DOUBLE_EQ(nan_border[1], 20);
  for (size_t i = 2; i < nan_border.size(); ++i)
    EXPECT_TRUE(std::isnan(nan_border[i])) << i;
}

TEST_P(GribRegularGrid, OnePointResamplingDoesNotDivideByZero) {
  InterpParams interp;
  interp.enabled = true;
  interp.outNi = 1;
  interp.outNj = 1;
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64, {}, interp, reader())),
    (std::vector<double>{0}));
}

TEST_P(GribRegularGrid, ReadFailuresPropagateAcrossCountCropAndDecode) {
  const ReadObjectPartFunc unavailable = [](uint64_t, uint64_t)
    -> std::optional<std::vector<uint8_t>> { return std::nullopt; };
  const CropParams crop{true, 2, 1, 1, 2};
  const InterpParams interp{true, 7, 5};
  for (const auto& failing_reader : {ReadObjectPartFunc{}, unavailable}) {
    EXPECT_FALSE(compute_values_count_for_msg(info, failing_reader));
    EXPECT_FALSE(compute_cropped_value_count(info, crop, failing_reader));
    EXPECT_FALSE(decode_values_as_dtype(info, DataDType::F64, failing_reader));
    EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F32, crop, failing_reader));
    EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64, crop, interp, failing_reader));
  }
}

TEST_P(GribRegularGrid, CacheReusesReadAcrossCountDecodeAndCrop) {
  const auto token = cache_token();
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), token), 12u);
  EXPECT_EQ(unpack<double>(decode_values_as_dtype(info, DataDType::F64, reader(), token)), expected);
  EXPECT_EQ(compute_cropped_value_count(info, CropParams{true, 2, 1, 1, 2}, reader(), token), 4u);
  EXPECT_EQ(reads, 1u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), cache_token()), 12u);
  EXPECT_EQ(reads, 2u);
}

TEST_P(GribRegularGrid, FailedReadIsNotCachedAndCanBeRetried) {
  const auto token = cache_token();
  const ReadObjectPartFunc unavailable = [](uint64_t, uint64_t)
    -> std::optional<std::vector<uint8_t>> { return std::nullopt; };
  EXPECT_FALSE(compute_values_count_for_msg(info, unavailable, token));
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), token), 12u);
  EXPECT_EQ(reads, 1u);
}

TEST_P(GribRegularGrid, EmptyReadIsNotCachedAndCanBeRetried) {
  const auto token = cache_token();
  bool first_read = true;
  size_t attempts = 0;
  const ReadObjectPartFunc retrying_reader = [&](uint64_t offset, uint64_t length) {
    EXPECT_EQ(offset, info.off);
    EXPECT_EQ(length, info.len);
    ++attempts;
    if (first_read) {
      first_read = false;
      return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{});
    }
    return std::optional<std::vector<uint8_t>>(message);
  };
  EXPECT_FALSE(compute_values_count_for_msg(info, retrying_reader, token));
  EXPECT_EQ(compute_values_count_for_msg(info, retrying_reader, token), 12u);
  EXPECT_EQ(unpack<double>(decode_values_as_dtype(info, DataDType::F64,
    retrying_reader, token)), expected);
  EXPECT_EQ(attempts, 2u);
}

TEST_P(GribRegularGrid, CacheEvictsLeastRecentlyUsedEntryAndHonorsShrinking) {
  struct RestoreCapacity {
    ~RestoreCapacity() { configure_message_cache(32); }
  } restore;
  configure_message_cache(2);
  const auto first = cache_token();
  const auto second = cache_token();
  const auto third = cache_token();
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), first), 12u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), second), 12u);
  EXPECT_EQ(reads, 2u);
  // Refresh the first entry, making the second the eviction candidate.
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), first), 12u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), third), 12u);
  EXPECT_EQ(reads, 3u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), first), 12u);
  EXPECT_EQ(reads, 3u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), second), 12u);
  EXPECT_EQ(reads, 4u);
  configure_message_cache(0);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), second), 12u);
  EXPECT_EQ(reads, 4u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), first), 12u);
  EXPECT_EQ(reads, 5u);
}

TEST_P(GribRegularGrid, ConcurrentCacheMissesPublishOneReusableMessage) {
  const auto token = cache_token();
  std::mutex mutex;
  std::condition_variable both_reading;
  size_t readers_entered = 0;
  ReadObjectPartFunc simultaneous_reader = [&](uint64_t offset, uint64_t length)
    -> std::optional<std::vector<uint8_t>> {
    EXPECT_EQ(offset, info.off);
    EXPECT_EQ(length, info.len);
    std::unique_lock<std::mutex> lock(mutex);
    ++readers_entered;
    both_reading.notify_all();
    // Both reads must pass the cache lookup before either publishes its data.
    EXPECT_TRUE(both_reading.wait_for(lock, std::chrono::seconds(2), [&] {
      return readers_entered == 2;
    }));
    return message;
  };
  std::optional<size_t> first_count;
  std::optional<size_t> second_count;
  std::thread first([&] {
    first_count = compute_values_count_for_msg(info, simultaneous_reader, token);
  });
  std::thread second([&] {
    second_count = compute_values_count_for_msg(info, simultaneous_reader, token);
  });
  first.join();
  second.join();
  EXPECT_EQ(first_count, 12u);
  EXPECT_EQ(second_count, 12u);
  EXPECT_EQ(readers_entered, 2u);
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), token), 12u);
  EXPECT_EQ(reads, 0u);
}

TEST_P(GribRegularGrid, CacheKeysDistinguishMessageOffsetsAndLengths) {
  const auto token = cache_token();
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), token), 12u);
  ++info.off;
  EXPECT_EQ(compute_values_count_for_msg(info, reader(), token), 12u);
  const std::vector<double> replacement(12, 3.5);
  ASSERT_EQ(codes_set_double_array(handle.get(), "values", replacement.data(), replacement.size()), CODES_SUCCESS);
  const auto original_length = info.len;
  refresh_message();
  ASSERT_NE(info.len, original_length);
  EXPECT_EQ(unpack<double>(decode_values_as_dtype(info, DataDType::F64, reader(), token)), replacement);
  EXPECT_EQ(reads, 3u);
}

TEST_P(GribRegularGrid, ZeroGridDimensionRejectsSpatialOperations) {
  ASSERT_EQ(codes_set_long(handle.get(), "Ni", 0), CODES_SUCCESS);
  refresh_message();
  const CropParams crop{true, 2, 1, 1, 2};
  EXPECT_FALSE(compute_cropped_value_count(info, crop, reader()));
  EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader()));
  EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64, crop,
    InterpParams{true, 2, 2}, reader()));
}

TEST_P(GribRegularGrid, SingleLongitudeGridCanBeCroppedWithoutWrapping) {
  ASSERT_EQ(codes_set_long(handle.get(), "Ni", 1), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(), "longitudeOfLastGridPointInDegrees", 0), CODES_SUCCESS);
  const std::vector<double> column{0, 100, 200};
  ASSERT_EQ(codes_set_double_array(handle.get(), "values", column.data(), column.size()), CODES_SUCCESS);
  refresh_message();
  const CropParams crop{true, 2, 0, 0, 0};
  EXPECT_EQ(compute_cropped_value_count(info, crop, reader()), 3u);
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader())), column);
}

TEST_P(GribRegularGrid, SouthToNorthGridCropUsesStorageRowOrder) {
  ASSERT_EQ(codes_set_long(handle.get(), "jScansPositively", 1), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfFirstGridPointInDegrees", 0), CODES_SUCCESS);
  ASSERT_EQ(codes_set_double(handle.get(), "latitudeOfLastGridPointInDegrees", 2), CODES_SUCCESS);
  refresh_message();
  const CropParams crop{true, 0, 1, 1, 2};
  EXPECT_EQ(compute_cropped_value_count(info, crop, reader()), 4u);
  EXPECT_EQ(unpack<double>(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader())),
    (std::vector<double>{10, 20, 110, 120}));
}

TEST_P(GribRegularGrid, InterpolationDefaultsToNativeGridDimensions) {
  InterpParams interpolation;
  interpolation.enabled = true;
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    {}, interpolation, reader())), expected);
}

TEST_P(GribRegularGrid, ConstantFieldsRemainConstantAfterInterpolation) {
  const std::vector<double> constant(12, 273.125);
  ASSERT_EQ(codes_set_double_array(handle.get(), "values", constant.data(), constant.size()), CODES_SUCCESS);
  refresh_message();
  const InterpParams interpolation{true, 7, 5};
  EXPECT_EQ(unpack<double>(decode_crop_and_bilinear_resample(info, DataDType::F64,
    {}, interpolation, reader())), std::vector<double>(35, 273.125));
}

TEST_P(GribRegularGrid, SpectralMessagesCanBeDecodedButCannotUseRectangularCrop) {
  const auto sample = "sh_sfc_grib" + std::to_string(GetParam());
  handle.reset(codes_grib_handle_new_from_samples(nullptr, sample.c_str()));
  ASSERT_NE(handle, nullptr);
  refresh_message();
  const auto index = build_index_line(handle.get(), info.off, info.len, GetParam());
  const auto indexed = find_msg_info(index, {{"shortName", "t"}});
  ASSERT_TRUE(indexed.ok);
  EXPECT_EQ(indexed.ni, 0);
  EXPECT_EQ(indexed.nj, 0);
  FileMeta metadata;
  ASSERT_TRUE(extract_file_meta(handle.get(), metadata));
  EXPECT_TRUE(metadata.has);
  EXPECT_EQ(metadata.shortName, "t");
  EXPECT_EQ(metadata.unit, "K");
  EXPECT_EQ(metadata.Ni, 0);
  EXPECT_EQ(metadata.Nj, 0);
  EXPECT_DOUBLE_EQ(metadata.latitudeOfFirstGridPoint, 0);
  EXPECT_DOUBLE_EQ(metadata.longitudeOfFirstGridPoint, 0);
  EXPECT_DOUBLE_EQ(metadata.latitudeOfLastGridPoint, 0);
  EXPECT_DOUBLE_EQ(metadata.longitudeOfLastGridPoint, 0);
  const auto count = get_values_count_from_message_bytes(message);
  ASSERT_TRUE(count);
  EXPECT_GT(*count, 0u);
  const auto bytes = decode_values_as_dtype(info, DataDType::F64, reader());
  ASSERT_TRUE(bytes);
  EXPECT_EQ(bytes->size(), *count * sizeof(double));
  const CropParams crop{true, 2, 1, 1, 2};
  EXPECT_FALSE(compute_cropped_value_count(info, crop, reader()));
  EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader()));
  EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64, crop,
    InterpParams{true, 2, 2}, reader()));
}

TEST_P(GribRegularGrid, ProjectedGridRejectsLatitudeLongitudeCrop) {
  const auto sample = "polar_stereographic_sfc_grib" + std::to_string(GetParam());
  handle.reset(codes_grib_handle_new_from_samples(nullptr, sample.c_str()));
  ASSERT_NE(handle, nullptr);
  refresh_message();
  const auto count = get_values_count_from_message_bytes(message);
  ASSERT_TRUE(count);
  EXPECT_GT(*count, 0u);
  const CropParams crop{true, 2, 1, 1, 2};
  EXPECT_FALSE(compute_cropped_value_count(info, crop, reader()));
  EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader()));
  EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64, crop,
    InterpParams{true, 2, 2}, reader()));
}

TEST(GribInvalidMessage, RejectsNonGribBytesForCountDecodeCropAndInterpolation) {
  const std::vector<uint8_t> bytes(64, 'x');
  const ReadObjectPartFunc reader = [&](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(bytes);
  };
  MsgInfo info;
  info.len = bytes.size();
  info.ok = true;
  const CropParams crop{true, 2, 1, 1, 2};
  EXPECT_FALSE(get_values_count_from_message_bytes(bytes));
  EXPECT_FALSE(compute_cropped_value_count(info, crop, reader));
  EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F64, crop, reader));
  EXPECT_FALSE(decode_values_as_dtype(info, DataDType::F32, reader));
  EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64, crop,
    InterpParams{true, 2, 2}, reader));
}

TEST(GribInvalidMessage, EmptyBuffersFailBeforeCallingEcCodes) {
  uint8_t byte = 0;
  EXPECT_EQ(new_handle_from_message_guarded(nullptr, 0), nullptr);
  EXPECT_EQ(new_handle_from_message_guarded(nullptr, 32), nullptr);
  EXPECT_EQ(new_handle_from_message_guarded(&byte, 0), nullptr);
  EXPECT_EQ(new_handle_from_message_copy_guarded(nullptr, 0), nullptr);
  EXPECT_EQ(new_handle_from_message_copy_guarded(nullptr, 32), nullptr);
  EXPECT_EQ(new_handle_from_message_copy_guarded(&byte, 0), nullptr);
  EXPECT_FALSE(get_values_count_from_message_bytes({}));

  const ReadObjectPartFunc reader = [](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{});
  };
  const CropParams crop{true, 2, 1, 1, 2};
  for (const std::string token : {"", "empty-message-ut"}) {
    EXPECT_FALSE(compute_values_count_for_msg({}, reader, token));
    EXPECT_FALSE(compute_cropped_value_count({}, crop, reader, token));
    EXPECT_FALSE(decode_values_as_dtype({}, DataDType::F64, reader, token));
    EXPECT_FALSE(decode_and_maybe_crop_values({}, DataDType::F32, crop, reader, token));
    EXPECT_FALSE(decode_crop_and_bilinear_resample({}, DataDType::F64, crop,
      InterpParams{true, 2, 2}, reader, token));
  }
}

TEST(GribInvalidGeometry, ZeroGridIncrementRejectsCropAndInterpolation) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "regular_ll_sfc_grib2"), codes_handle_delete);
  ASSERT_NE(handle, nullptr);
  ASSERT_EQ(codes_set_double(handle.get(), "iDirectionIncrementInDegrees", 0), CODES_SUCCESS);
  const void* data = nullptr;
  size_t length = 0;
  ASSERT_EQ(codes_get_message(handle.get(), &data, &length), CODES_SUCCESS);
  const auto* first = static_cast<const uint8_t*>(data);
  const std::vector<uint8_t> bytes(first, first + length);
  const ReadObjectPartFunc reader = [&](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(bytes);
  };
  MsgInfo info;
  info.len = length;
  info.ok = true;
  const CropParams crop{true, 2, 1, 1, 2};
  EXPECT_FALSE(compute_cropped_value_count(info, crop, reader));
  EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F32, crop, reader));
  EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64, crop,
    InterpParams{true, 2, 2}, reader));
  const auto count = get_values_count_from_message_bytes(bytes);
  ASSERT_TRUE(count);
  const auto decoded = decode_values_as_dtype(info, DataDType::F64, reader);
  ASSERT_TRUE(decoded);
  EXPECT_EQ(decoded->size(), *count * sizeof(double));
}

TEST(GribInvalidMessage, UnsupportedIeeePrecisionPropagatesArrayDecodeFailure) {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "regular_ll_sfc_grib2"), codes_handle_delete);
  ASSERT_NE(handle, nullptr);
  const char packing[] = "grid_ieee";
  size_t packing_length = sizeof(packing) - 1;
  ASSERT_EQ(codes_set_string(handle.get(), "packingType", packing, &packing_length), CODES_SUCCESS);
  // IEEE precision code 3 is unsupported. The framing and value count remain
  // readable, but decoding the payload must fail through every public path.
  ASSERT_EQ(codes_set_long(handle.get(), "precision", 3), CODES_SUCCESS);
  const void* data = nullptr;
  size_t length = 0;
  ASSERT_EQ(codes_get_message(handle.get(), &data, &length), CODES_SUCCESS);
  const auto* first = static_cast<const uint8_t*>(data);
  const std::vector<uint8_t> bytes(first, first + length);
  const ReadObjectPartFunc reader = [&](uint64_t, uint64_t) {
    return std::optional<std::vector<uint8_t>>(bytes);
  };
  MsgInfo info;
  info.len = length;
  info.ok = true;
  const auto count = get_values_count_from_message_bytes(bytes);
  ASSERT_TRUE(count);
  EXPECT_GT(*count, 0u);
  EXPECT_FALSE(decode_values_as_dtype(info, DataDType::F32, reader));
  EXPECT_FALSE(decode_values_as_dtype(info, DataDType::F64, reader));
  EXPECT_FALSE(decode_and_maybe_crop_values(info, DataDType::F64,
    CropParams{true, 2, 1, 1, 2}, reader));
  EXPECT_FALSE(decode_crop_and_bilinear_resample(info, DataDType::F64, {},
    InterpParams{true, 2, 2}, reader));
}

INSTANTIATE_TEST_SUITE_P(GribEditions, GribRegularGrid, ::testing::Values(1, 2));

} // namespace
