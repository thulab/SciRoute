#pragma once

#include "iotdbfs/MetadataConfig.h"
#include <unordered_map>

namespace iotdbfs {

// Both modes use the same cross-file catalog and admission protocol. This
// Native uses an explicitly bound immutable filesystem file; indexed uses
// database payload fragments. Native never downloads/falls back to the latter.
enum class Hdf5QueryMode { NativeHdf5, IoTDBIndexed };

inline MetadataResult<Hdf5QueryMode> parse_hdf5_query_mode(const std::string& value) {
  if (value == "native-hdf5")
    return MetadataResult<Hdf5QueryMode>::success(Hdf5QueryMode::NativeHdf5);
  if (value == "iotdb-indexed")
    return MetadataResult<Hdf5QueryMode>::success(Hdf5QueryMode::IoTDBIndexed);
  return MetadataResult<Hdf5QueryMode>::failure(
    "query.hdf5.mode must be native-hdf5 or iotdb-indexed");
}

inline const char* hdf5_query_mode_name(Hdf5QueryMode mode) {
  switch (mode) {
  case Hdf5QueryMode::NativeHdf5: return "native-hdf5";
  case Hdf5QueryMode::IoTDBIndexed: return "iotdb-indexed";
  }
  return "invalid";
}

inline MetadataResult<Hdf5QueryMode> parse_hdf5_query_configuration(
    const std::unordered_map<std::string, std::string>& properties) {
  if (properties.count("query.hdf5.catalog_mode"))
    return MetadataResult<Hdf5QueryMode>::failure(
      "query.hdf5.catalog_mode was removed; use query.hdf5.mode="
      "native-hdf5 or iotdb-indexed (no legacy aliases)");
  const auto found = properties.find("query.hdf5.mode");
  return found == properties.end()
    ? MetadataResult<Hdf5QueryMode>::success(Hdf5QueryMode::IoTDBIndexed)
    : parse_hdf5_query_mode(found->second);
}

} // namespace iotdbfs
