#include "iotdbfs/formats/grib/GribLayout.h"

#include <charconv>
#include <string_view>

namespace iotdbfs {
namespace {

constexpr std::string_view kPrefix = "IOTDBFS-GRIB-LAYOUT/";

QueryStatus layout_failure(QueryError error, std::string message) {
  return QueryStatus::failure(error, QueryPhase::Planning,
    std::move(message), QueryErrorDetail{std::nullopt, std::nullopt,
      std::string("parse-grib-layout"), false});
}

} // namespace

std::string grib_layout_header() {
  return std::string(kPrefix) + std::to_string(kGribLayoutVersion) + "\n";
}

QueryResult<GribLayoutDocument> parse_grib_layout(
    const BinaryValue& bytes) {
  if (bytes.empty())
    return QueryResult<GribLayoutDocument>::failure(layout_failure(
      QueryError::LayoutUnavailable, "GRIB Layout Artifact is empty"));
  const std::string_view text(reinterpret_cast<const char*>(bytes.data()),
    bytes.size());
  const auto newline = text.find('\n');
  if (newline == std::string_view::npos ||
      text.substr(0, kPrefix.size()) != kPrefix)
    return QueryResult<GribLayoutDocument>::failure(layout_failure(
      QueryError::CorruptLayout,
      "GRIB Layout Artifact has no version header"));
  const auto version_text = text.substr(kPrefix.size(),
    newline - kPrefix.size());
  std::uint32_t version = 0;
  const auto parsed = std::from_chars(version_text.data(),
    version_text.data() + version_text.size(), version);
  if (version_text.empty() || parsed.ec != std::errc{} ||
      parsed.ptr != version_text.data() + version_text.size())
    return QueryResult<GribLayoutDocument>::failure(layout_failure(
      QueryError::CorruptLayout,
      "GRIB Layout Artifact has an invalid version"));
  if (version != kGribLayoutVersion)
    return QueryResult<GribLayoutDocument>::failure(layout_failure(
      QueryError::UnsupportedLayoutVersion,
      "GRIB Layout Artifact version is not supported"));
  GribLayoutDocument document;
  document.version = version;
  document.records.assign(text.substr(newline + 1));
  return QueryResult<GribLayoutDocument>::success(std::move(document));
}

} // namespace iotdbfs
