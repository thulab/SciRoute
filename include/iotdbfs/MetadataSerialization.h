#pragma once

#include "iotdbfs/MetadataConfig.h"

#include <cstddef>
#include <string>

namespace iotdbfs {

// Decode persisted, typed metadata. An empty string represents an empty typed
// document. Invalid documents return an error and no values.
MetadataResult<MetadataMap> deserialize_metadata(const std::string& json,
  std::size_t max_bytes = 65536);

// Text representation used by extended attributes (without JSON quoting).
std::string metadata_value_text(const MetadataValue& value);

bool valid_metadata_utf8(const std::string& text);

} // namespace iotdbfs
