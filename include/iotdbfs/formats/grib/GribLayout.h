#pragma once

#include "iotdbfs/query/core/QueryModel.h"

#include <cstdint>
#include <string>

namespace iotdbfs {

inline constexpr std::uint32_t kGribLayoutVersion = 1;

struct GribLayoutDocument {
  std::uint32_t version{ 0 };
  std::string records;
};

// GRIB Layout Artifacts use a non-JSON first line so legacy record scanners
// safely ignore it while typed readers can reject corrupt or unknown versions.
std::string grib_layout_header();
QueryResult<GribLayoutDocument> parse_grib_layout(
  const BinaryValue& bytes);

} // namespace iotdbfs
