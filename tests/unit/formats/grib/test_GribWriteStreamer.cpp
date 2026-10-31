#include <gtest/gtest.h>

#include "iotdbfs/formats/grib/GribLayout.h"
#include "iotdbfs/formats/grib/GribWriteStreamer.h"
#include "iotdbfs/MetadataConfig.h"

#include <eccodes.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace {

using iotdbfs::GribWriteStreamer;

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

struct SummaryMessage {
  std::vector<std::uint8_t> bytes;
  std::string short_name;
  std::int64_t level{0};
  std::int64_t step{0};
};

SummaryMessage make_summary_message(long param_id, long level, long step) {
  SummaryMessage result;
  codes_handle* handle = codes_grib_handle_new_from_samples(nullptr, "GRIB2");
  if (!handle) {
    ADD_FAILURE() << "ecCodes GRIB2 sample unavailable";
    return result;
  }
  const char* type_of_level = "isobaricInhPa";
  std::size_t type_of_level_size = std::strlen(type_of_level);
  if (codes_set_string(handle, "typeOfLevel", type_of_level,
        &type_of_level_size) != 0 ||
      codes_set_long(handle, "paramId", param_id) != 0 ||
      codes_set_long(handle, "level", level) != 0 ||
      codes_set_long(handle, "step", step) != 0) {
    ADD_FAILURE() << "failed to configure GRIB summary fixture";
    codes_handle_delete(handle);
    return result;
  }
  std::vector<char> name(128, '\0');
  std::size_t name_size = name.size();
  if (codes_get_string(handle, "shortName", name.data(), &name_size) != 0 ||
      name_size < 2) {
    ADD_FAILURE() << "failed to read fixture shortName";
    codes_handle_delete(handle);
    return result;
  }
  result.short_name.assign(name.data(), name_size - 1);
  result.level = level;
  result.step = step;
  const void* message = nullptr;
  std::size_t size = 0;
  if (codes_get_message(handle, &message, &size) != 0 || !message || !size) {
    ADD_FAILURE() << "failed to encode GRIB summary fixture";
  } else {
    const auto* begin = static_cast<const std::uint8_t*>(message);
    result.bytes.assign(begin, begin + size);
  }
  codes_handle_delete(handle);
  return result;
}

std::vector<std::uint8_t> make_invalid_grib2_message() {
  std::vector<std::uint8_t> bytes(32, 0);
  std::copy_n("GRIB", 4, bytes.begin());
  bytes[7] = 2;
  bytes[15] = static_cast<std::uint8_t>(bytes.size());
  return bytes;
}

GribWriteStreamer::Config config() {
  GribWriteStreamer::Config value;
  value.chunk_size = 16;
  value.process_threshold_bytes = 16;
  value.process_retry_bytes = 8;
  value.window_reserve_bytes = 128;
  value.detect_threshold_bytes = 16;
  return value;
}

std::size_t layout_records(const std::string& layout) {
  const auto lines = static_cast<std::size_t>(
    std::count(layout.begin(), layout.end(), '\n'));
  return layout.rfind(iotdbfs::grib_layout_header(), 0) == 0
    ? lines - 1
    : lines;
}

} // namespace

TEST(GribWriteStreamer, NonGribProducesNoDerivedArtifacts) {
  GribWriteStreamer streamer(config());
  const std::vector<std::uint8_t> bytes{
    'n', 'o', 't', '-', 'g', 'r', 'i', 'b'
  };
  streamer.feed_chunk(bytes.data(), bytes.size());
  streamer.finalize();

  EXPECT_TRUE(streamer.type_decided());
  EXPECT_FALSE(streamer.is_grib());
  EXPECT_FALSE(streamer.failed());
  EXPECT_TRUE(streamer.layout().empty());
  EXPECT_FALSE(streamer.summary());
}

TEST(GribWriteStreamer, ValidMessageProducesPrivateLayoutAndMetadata) {
  GribWriteStreamer streamer(config());
  const auto message = make_valid_grib2_message();
  ASSERT_FALSE(message.empty());
  streamer.feed_chunk(message.data(), message.size());
  streamer.finalize();

  EXPECT_TRUE(streamer.is_grib());
  EXPECT_FALSE(streamer.failed());
  EXPECT_EQ(streamer.layout().rfind(iotdbfs::grib_layout_header(), 0), 0u);
  EXPECT_EQ(layout_records(streamer.layout()), 1u);
  EXPECT_NE(streamer.layout().find("\"of\":0"), std::string::npos);
  EXPECT_NE(streamer.layout().find(
    "\"ln\":" + std::to_string(message.size())), std::string::npos);
  EXPECT_NE(streamer.layout().find("\"ed\":2"), std::string::npos);
  ASSERT_TRUE(streamer.summary());
  const auto short_names = streamer.summary()->find("shortName");
  ASSERT_NE(short_names, streamer.summary()->end());
  EXPECT_FALSE(std::get<iotdbfs::MetadataStringSet>(
    short_names->second.value()).empty());
  EXPECT_EQ(streamer.stats().messages_found, 1u);
}

TEST(GribWriteStreamer, EmitsConfiguredTypedMetadata) {
  auto cfg = config();
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.edition.source=edition\n"
    "metadata.field.edition.type=int64\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  cfg.metadata_config =
    std::make_shared<const iotdbfs::MetadataConfig>(parsed.value());
  GribWriteStreamer streamer(cfg);
  const auto message = make_valid_grib2_message();
  streamer.feed_chunk(message.data(), message.size());
  streamer.finalize();

  ASSERT_TRUE(streamer.summary());
  const auto& summary = *streamer.summary();
  ASSERT_EQ(summary.count("edition"), 1u);
  EXPECT_EQ(std::get<iotdbfs::MetadataInt64Set>(
    summary.at("edition").value()), (iotdbfs::MetadataInt64Set{2}));
}

TEST(GribWriteStreamer, RequiredMetadataFailureRejectsAllArtifacts) {
  auto cfg = config();
  const auto parsed = iotdbfs::MetadataConfig::parse(
    "metadata.schema_version=1\n"
    "metadata.field.required.source=missingKey\n"
    "metadata.field.required.type=string\n"
    "metadata.field.required.required=true\n");
  ASSERT_TRUE(parsed.ok()) << parsed.error();
  cfg.metadata_config =
    std::make_shared<const iotdbfs::MetadataConfig>(parsed.value());
  GribWriteStreamer streamer(cfg);
  const auto message = make_valid_grib2_message();
  streamer.feed_chunk(message.data(), message.size());
  streamer.finalize();

  EXPECT_TRUE(streamer.failed());
  EXPECT_NE(streamer.error().find("required"), std::string::npos);
  EXPECT_TRUE(streamer.layout().empty());
  EXPECT_FALSE(streamer.summary());
}

TEST(GribWriteStreamer, InvalidSpanPreservesFollowingAbsoluteOffset) {
  GribWriteStreamer streamer(config());
  const auto valid = make_valid_grib2_message();
  const auto invalid = make_invalid_grib2_message();
  ASSERT_FALSE(valid.empty());

  streamer.feed_chunk(valid.data(), valid.size());
  streamer.process_complete_messages();
  streamer.feed_chunk(invalid.data(), invalid.size());
  streamer.process_complete_messages();
  streamer.feed_chunk(valid.data(), valid.size());
  streamer.finalize();

  ASSERT_EQ(layout_records(streamer.layout()), 2u);
  EXPECT_NE(streamer.layout().find("\"of\":" +
    std::to_string(valid.size() + invalid.size())), std::string::npos);
  EXPECT_FALSE(streamer.failed());
}

TEST(GribWriteStreamer, TruncatedTailDoesNotCreateLayoutRecord) {
  GribWriteStreamer streamer(config());
  const auto valid = make_valid_grib2_message();
  ASSERT_GT(valid.size(), 32u);
  streamer.feed_chunk(valid.data(), valid.size());
  streamer.process_complete_messages();
  streamer.feed_chunk(valid.data(), 32);
  streamer.finalize();

  EXPECT_EQ(layout_records(streamer.layout()), 1u);
  EXPECT_FALSE(streamer.failed());
}

TEST(GribWriteStreamer, ChunkingDoesNotChangeLayoutOrMetadata) {
  const auto valid = make_valid_grib2_message();
  ASSERT_FALSE(valid.empty());
  auto bytes = valid;
  bytes.insert(bytes.end(), valid.begin(), valid.end());

  std::string expected_layout;
  for (const std::size_t chunk : { 1u, 7u, 16u, 31u, 4096u }) {
    SCOPED_TRACE(chunk);
    GribWriteStreamer streamer(config());
    for (std::size_t offset = 0; offset < bytes.size(); offset += chunk) {
      const auto count = std::min(chunk, bytes.size() - offset);
      streamer.feed_chunk(bytes.data() + offset, count);
      streamer.process_complete_messages();
    }
    streamer.finalize();
    ASSERT_FALSE(streamer.failed()) << streamer.error();
    ASSERT_TRUE(streamer.summary());
    EXPECT_EQ(layout_records(streamer.layout()), 2u);
    if (expected_layout.empty()) expected_layout = streamer.layout();
    else EXPECT_EQ(streamer.layout(), expected_layout);
  }
}

TEST(GribWriteStreamer, SummarizesEveryMessageAsSetsAndRanges) {
  const auto first = make_summary_message(130, 500, 1);
  const auto second = make_summary_message(131, 850, 6);
  ASSERT_FALSE(first.bytes.empty());
  ASSERT_FALSE(second.bytes.empty());
  ASSERT_NE(first.short_name, second.short_name);
  auto bytes = first.bytes;
  bytes.insert(bytes.end(), second.bytes.begin(), second.bytes.end());

  GribWriteStreamer streamer(config());
  streamer.feed_chunk(bytes.data(), bytes.size());
  streamer.finalize();

  ASSERT_FALSE(streamer.failed()) << streamer.error();
  ASSERT_TRUE(streamer.summary());
  const auto& summary = *streamer.summary();
  EXPECT_EQ(std::get<iotdbfs::MetadataStringSet>(
    summary.at("shortName").value()),
    (iotdbfs::MetadataStringSet{first.short_name, second.short_name}));
  EXPECT_EQ(std::get<iotdbfs::MetadataInt64Set>(summary.at("level").value()),
    (iotdbfs::MetadataInt64Set{500, 850}));
  EXPECT_EQ(std::get<iotdbfs::MetadataInt64Set>(summary.at("step").value()),
    (iotdbfs::MetadataInt64Set{1, 6}));
  EXPECT_EQ(std::get<std::int64_t>(summary.at("levelMin").value()), 500);
  EXPECT_EQ(std::get<std::int64_t>(summary.at("levelMax").value()), 850);
  EXPECT_EQ(std::get<std::int64_t>(summary.at("stepMin").value()), 1);
  EXPECT_EQ(std::get<std::int64_t>(summary.at("stepMax").value()), 6);
}

TEST(GribWriteStreamer, ResetClearsDerivedStateAndOffsets) {
  GribWriteStreamer streamer(config());
  const auto valid = make_valid_grib2_message();
  streamer.feed_chunk(valid.data(), valid.size());
  streamer.finalize();
  ASSERT_FALSE(streamer.layout().empty());
  ASSERT_TRUE(streamer.summary());
  ASSERT_GT(streamer.stats().scan_calls, 0u);

  streamer.reset_state();
  EXPECT_FALSE(streamer.failed());
  EXPECT_FALSE(streamer.type_decided());
  EXPECT_TRUE(streamer.layout().empty());
  EXPECT_FALSE(streamer.summary());
  EXPECT_EQ(streamer.stats().scan_calls, 0u);

  streamer.feed_chunk(valid.data(), valid.size());
  streamer.finalize();
  EXPECT_NE(streamer.layout().find("\"of\":0"), std::string::npos);
}

TEST(GribWriteStreamer, DetectionUsesLargerThreshold) {
  for (const bool detection_is_larger : { false, true }) {
    auto cfg = config();
    cfg.detect_threshold_bytes = detection_is_larger ? 40 : 16;
    cfg.process_threshold_bytes = detection_is_larger ? 16 : 40;
    GribWriteStreamer streamer(cfg);
    const std::vector<std::uint8_t> bytes(40, 'x');
    streamer.feed_chunk(bytes.data(), 39);
    streamer.process_complete_messages();
    EXPECT_EQ(streamer.stats().scan_calls, 0u);
    streamer.feed_chunk(bytes.data() + 39, 1);
    streamer.process_complete_messages();
    EXPECT_EQ(streamer.stats().scan_calls, 1u);
    EXPECT_TRUE(streamer.type_decided());
  }
}
