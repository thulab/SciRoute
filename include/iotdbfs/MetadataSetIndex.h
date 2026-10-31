#pragma once

#include "iotdbfs/MetadataConfig.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/QueryUtils.h"

#include <string>
#include <string_view>
#include <vector>

namespace iotdbfs {

inline constexpr char kMetadataSetIndexTable[] = "blob_metadata_set";

struct MetadataSetMember {
  std::string field_id;
  std::string member_value;
  std::string member_type;

  bool operator==(const MetadataSetMember& other) const {
    return field_id == other.field_id && member_value == other.member_value &&
      member_type == other.member_type;
  }
};

inline std::vector<MetadataSetMember> metadata_set_members(
    const MetadataMap& metadata) {
  std::vector<MetadataSetMember> members;
  for (const auto& [field_id, value] : metadata) {
    switch (value.type()) {
      case MetadataValueType::StringSet:
        for (const auto& member : std::get<MetadataStringSet>(value.value()))
          members.push_back({field_id, member, "string"});
        break;
      case MetadataValueType::Int64Set:
        for (const auto member : std::get<MetadataInt64Set>(value.value()))
          members.push_back({field_id, std::to_string(member), "int64"});
        break;
      case MetadataValueType::DoubleSet:
        for (const auto member : std::get<MetadataDoubleSet>(value.value()))
          members.push_back({field_id,
            metadata_value_text(MetadataValue(member)), "double"});
        break;
      case MetadataValueType::BoolSet:
        for (const auto member : std::get<MetadataBoolSet>(value.value()))
          members.push_back({field_id, member ? "true" : "false", "bool"});
        break;
      case MetadataValueType::String:
      case MetadataValueType::Int64:
      case MetadataValueType::Double:
      case MetadataValueType::Bool:
        break;
    }
  }
  return members;
}

inline std::string build_grib_cross_file_inode_sql(bool,
    std::string_view short_name) {
  if (short_name.empty()) {
    return "SELECT time,path FROM inode WHERE published=true AND "
      "is_dir=false AND format_id='grib' ORDER BY time DESC";
  }
  return std::string(
    "SELECT table1.time,table1.path FROM inode table1 JOIN "
    "blob_metadata_set set1 ON table1.time=set1.time AND "
    "table1.path=set1.path WHERE table1.published=true AND "
    "table1.is_dir=false AND table1.format_id='grib' AND "
    "set1.present=true AND set1.field_id='shortName' AND "
    "set1.member_type='string' AND set1.member_value=") +
    query_utils::sql_literal(short_name) + " ORDER BY table1.time DESC";
}

} // namespace iotdbfs
