#include "iotdbfs/MetadataProjector.h"

#include "iotdbfs/MetadataSerialization.h"

#include <algorithm>

namespace iotdbfs {
namespace {

const MetadataValue* value_for(const MetadataMap& values,
  const MetadataFieldDefinition& field) {
  const auto it = values.find(field.id);
  return it == values.end() ? nullptr : &it->second;
}

bool valid_value(const MetadataValue* value, MetadataValueType type) {
  if (!value) return false;
  if (value->type() == type) return true;
  return (type == MetadataValueType::String &&
      value->type() == MetadataValueType::StringSet) ||
    (type == MetadataValueType::Int64 &&
      value->type() == MetadataValueType::Int64Set) ||
    (type == MetadataValueType::Double &&
      value->type() == MetadataValueType::DoubleSet) ||
    (type == MetadataValueType::Bool &&
      value->type() == MetadataValueType::BoolSet);
}

MetadataMap configured_values(const MetadataMap& values,
  const MetadataConfig& config) {
  MetadataMap filtered;
  for (const auto& field : config.fields()) {
    const auto it = values.find(field.id);
    if (it != values.end() && valid_value(&it->second, field.type))
      filtered.emplace(field.id, it->second);
  }
  return filtered;
}

} // namespace

MetadataXattrList MetadataProjector::to_xattrs(const MetadataMap& values) const {
  MetadataXattrList attrs;
  for (const auto& field : config_.fields()) {
    if (field.xattr.empty()) continue;
    const auto* value = value_for(values, field);
    if (!valid_value(value, field.type)) continue;
    std::string text;
    try {
      text = metadata_value_text(*value);
    } catch (...) {
      continue;
    }
    const auto existing = std::find_if(attrs.begin(), attrs.end(),
      [&](const auto& item) { return item.first == field.xattr; });
    if (existing == attrs.end()) attrs.emplace_back(field.xattr, text);
    else existing->second = text;
  }
  return attrs;
}

MetadataXattrList MetadataProjector::to_xattrs_json(const std::string& json) const {
  const auto parsed = deserialize_metadata(json);
  if (!parsed.ok()) return {};
  return to_xattrs(parsed.value());
}

MetadataResult<std::string> MetadataProjector::to_json(const MetadataMap& values,
  std::size_t max_bytes) const {
  return serialize_metadata(configured_values(values, config_), max_bytes);
}

std::vector<WebMetadataField> MetadataProjector::to_web(const MetadataMap& values) const {
  std::vector<WebMetadataField> result;
  for (const auto& field : config_.fields()) {
    if (!field.queryable) continue;
    const auto* value = value_for(values, field);
    if (!valid_value(value, field.type)) continue;
    try {
      result.push_back({field.id, metadata_value_text(*value)});
    } catch (...) {
      // Invalid values cannot be represented safely in a web response.
    }
  }
  return result;
}

} // namespace iotdbfs
