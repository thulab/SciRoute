#pragma once

#include "iotdbfs/StructuredBlobStorage.h"

#include <map>
#include <set>

namespace iotdbfs {

struct DatasetIndexEntryRow {
  std::string object_id, dataset_path;
  bool attributes_complete{false};
};

struct DatasetIndexAttributeRow {
  std::string object_id, name;
  DatasetAttributeState state{DatasetAttributeState::Invalid};
  std::optional<DatasetAttributeValue> value;
};

// Publication compares every stored identity and typed value, not only COUNT.
// Equal-count replacement, duplicates and values attached to another object
// must all fail before the inode can become visible.
inline StructuredBlobStatus validate_dataset_index_rows(
    const DatasetCatalogArtifact& artifact,
    const std::vector<DatasetIndexEntryRow>& entries,
    const std::vector<DatasetIndexAttributeRow>& attributes) {
  const auto invalid = [] {
    return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,
      "dataset secondary index differs from its immutable catalog");
  };
  std::map<std::string, const DatasetCatalogRecord*> expected_entries;
  std::map<std::pair<std::string, std::string>, const DatasetCatalogAttribute*> expected_attrs;
  for (const auto& record : artifact.records) {
    if (!expected_entries.emplace(record.object_id, &record).second) return invalid();
    for (const auto& attribute : record.attributes)
      if (!expected_attrs.emplace(std::make_pair(record.object_id, attribute.name), &attribute).second)
        return invalid();
  }
  if (entries.size() != expected_entries.size() || attributes.size() != expected_attrs.size())
    return invalid();
  std::set<std::string> seen_entries;
  for (const auto& row : entries) {
    const auto found = expected_entries.find(row.object_id);
    if (found == expected_entries.end() || !seen_entries.insert(row.object_id).second ||
        row.dataset_path != found->second->canonical_path ||
        row.attributes_complete != found->second->attributes_complete) return invalid();
  }
  std::set<std::pair<std::string, std::string>> seen_attrs;
  for (const auto& row : attributes) {
    const auto key = std::make_pair(row.object_id, row.name);
    const auto found = expected_attrs.find(key);
    if (found == expected_attrs.end() || !seen_attrs.insert(key).second ||
        row.state != found->second->state || row.value != found->second->value)
      return invalid();
  }
  return StructuredBlobStatus::success();
}

} // namespace iotdbfs
