#pragma once

#include "iotdbfs/formats/grib/GribTypes.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/MetadataCache.h"

#include <cstddef>
#include <map>
#include <memory>
#include <string>

namespace iotdbfs {

void configure_grib_message_cache(std::size_t max_entries);

class GribExtensionService {
public:
  struct Dependencies {
    ReadObjectPart read_object_part;
    ReadLayoutText read_layout_text;
    IsPathInWriting is_path_in_writing;
    std::shared_ptr<const MetadataConfig> metadata_config;
  };

  GribExtensionService(bool enabled, Dependencies deps);

  bool enabled_for_path(const std::string& path) const;
  GribQuerySpec parse_query_spec(const std::string& query) const;
  bool has_non_type_params(const std::map<std::string, std::string>& kv) const;
  GribXattrList build_inode_xattrs(const Inode& inode) const;
  void append_index_xattrs(const std::string& index_text,
    GribXattrList& attrs) const;
  void append_index_xattr_names(GribXattrList& attrs) const;
  bool can_handle_virtual_query(const std::string& path) const;
  GribVirtualAttr get_virtual_attr(long long row_time,
    const std::string& path, const GribQuerySpec& spec,
    const std::string& raw_query) const;
  GribVirtualOpenResult open_virtual(long long row_time,
    const std::string& path, const GribQuerySpec& spec,
    const std::string& raw_query) const;

private:
  bool enabled_{ false };
  Dependencies deps_;
  mutable MetadataCache metadata_cache_;
};

} // namespace iotdbfs
