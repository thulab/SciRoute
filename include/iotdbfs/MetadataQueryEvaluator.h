#pragma once

#include "iotdbfs/MetadataConfig.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace iotdbfs {

// A parsed equality predicate against a queryable metadata field.
struct MetadataQueryPredicate {
  std::string field_id; // canonical configured field id
  MetadataValue expected;
};

// Parses and evaluates bounded metadata equality queries. Query keys may be a
// field id or one of its configured aliases; only queryable fields are
// accepted. The evaluator does not access IoTDB and is safe to reuse across
// requests.
class MetadataQueryEvaluator {
public:
  explicit MetadataQueryEvaluator(const MetadataConfig& config);

  MetadataResult<std::vector<MetadataQueryPredicate>> parse(
    const std::string& expression) const;

  bool matches(const MetadataMap& values,
    const std::vector<MetadataQueryPredicate>& predicates) const;

  MetadataResult<bool> evaluate(const std::string& expression,
    const MetadataMap& values) const;

private:
  const MetadataConfig& config_;
  std::unordered_map<std::string, const MetadataFieldDefinition*> fields_;
};

} // namespace iotdbfs
