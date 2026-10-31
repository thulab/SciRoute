#pragma once

#include "iotdbfs/MetadataConfig.h"

#include <string>
#include <utility>
#include <vector>

namespace iotdbfs {

struct WebMetadataField {
  std::string id;
  std::string value;
};

using MetadataXattrList = std::vector<std::pair<std::string, std::string>>;

// Projects dynamic metadata into the views consumed by filesystem and web
// clients. The projector is deliberately side-effect free and only emits
// fields declared by MetadataConfig.
class MetadataProjector {
public:
  explicit MetadataProjector(const MetadataConfig& config) : config_(config) {}

  MetadataXattrList to_xattrs(const MetadataMap& values) const;
  // Decode a persisted metadata document and project it to xattrs. Invalid
  // documents are treated as absent metadata and produce no dynamic attrs.
  MetadataXattrList to_xattrs_json(const std::string& json) const;
  // Serialize only fields declared in this projector's schema.
  MetadataResult<std::string> to_json(const MetadataMap& values,
    std::size_t max_bytes = 65536) const;
  std::vector<WebMetadataField> to_web(const MetadataMap& values) const;

private:
  const MetadataConfig& config_;
};

} // namespace iotdbfs
