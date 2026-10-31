#include "iotdbfs/MetadataQueryEvaluator.h"

#include "iotdbfs/MetadataSerialization.h"

#include <charconv>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <sstream>
#include <type_traits>
#include <variant>

namespace iotdbfs {
namespace {

std::string trim(std::string s) {
  const auto first = s.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return {};
  const auto last = s.find_last_not_of(" \t\r\n");
  return s.substr(first, last - first + 1);
}

std::string lower(const std::string& s) {
  std::string out = s;
  for (char& c : out)
    if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
  return out;
}

MetadataResult<MetadataValue> parse_value(const std::string& text,
  MetadataValueType type) {
  try {
    switch (type) {
    case MetadataValueType::String:
      if (text.size() > 4096 || !valid_metadata_utf8(text))
        return MetadataResult<MetadataValue>::failure("invalid metadata string");
      return MetadataResult<MetadataValue>::success(MetadataValue(text));
    case MetadataValueType::Int64: {
      std::int64_t value = 0;
      const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
      if (result.ec != std::errc{} || result.ptr != text.data() + text.size())
        return MetadataResult<MetadataValue>::failure("invalid int64 query value");
      return MetadataResult<MetadataValue>::success(MetadataValue(value));
    }
    case MetadataValueType::Double: {
      char* end = nullptr;
      const double value = std::strtod(text.c_str(), &end);
      if (end != text.c_str() + text.size() || !std::isfinite(value))
        return MetadataResult<MetadataValue>::failure("invalid double query value");
      return MetadataResult<MetadataValue>::success(MetadataValue(value));
    }
    case MetadataValueType::Bool: {
      const auto normalized = lower(text);
      if (normalized == "true" || normalized == "1")
        return MetadataResult<MetadataValue>::success(MetadataValue(true));
      if (normalized == "false" || normalized == "0")
        return MetadataResult<MetadataValue>::success(MetadataValue(false));
      return MetadataResult<MetadataValue>::failure("invalid bool query value");
    }
    case MetadataValueType::StringSet:
    case MetadataValueType::Int64Set:
    case MetadataValueType::DoubleSet:
    case MetadataValueType::BoolSet:
      return MetadataResult<MetadataValue>::failure(
        "metadata queries require scalar field types");
    }
  } catch (...) {
    return MetadataResult<MetadataValue>::failure("invalid metadata query value");
  }
  return MetadataResult<MetadataValue>::failure("unsupported metadata type");
}

bool equal_value(const MetadataValue& lhs, const MetadataValue& rhs) {
  if (lhs.type() == rhs.type()) {
    if (lhs.type() == MetadataValueType::String)
      return lower(std::get<std::string>(lhs.value())) ==
        lower(std::get<std::string>(rhs.value()));
    return lhs.value() == rhs.value();
  }
  if (lhs.type() == MetadataValueType::StringSet &&
      rhs.type() == MetadataValueType::String) {
    const auto expected = lower(std::get<std::string>(rhs.value()));
    for (const auto& member : std::get<MetadataStringSet>(lhs.value()))
      if (lower(member) == expected) return true;
    return false;
  }
  if (lhs.type() == MetadataValueType::Int64Set &&
      rhs.type() == MetadataValueType::Int64)
    return std::get<MetadataInt64Set>(lhs.value()).count(
      std::get<std::int64_t>(rhs.value())) != 0;
  if (lhs.type() == MetadataValueType::DoubleSet &&
      rhs.type() == MetadataValueType::Double)
    return std::get<MetadataDoubleSet>(lhs.value()).count(
      std::get<double>(rhs.value())) != 0;
  if (lhs.type() == MetadataValueType::BoolSet &&
      rhs.type() == MetadataValueType::Bool)
    return std::get<MetadataBoolSet>(lhs.value()).count(
      std::get<bool>(rhs.value())) != 0;
  return false;
}

} // namespace

MetadataQueryEvaluator::MetadataQueryEvaluator(const MetadataConfig& config)
  : config_(config) {
  auto register_name = [this](const std::string& name,
      const MetadataFieldDefinition* field) {
    const auto key = lower(name);
    const auto it = fields_.find(key);
    if (it == fields_.end()) {
      fields_.emplace(key, field);
    } else if (it->second != field) {
      // Ambiguous aliases are deliberately disabled rather than selecting a
      // field nondeterministically.
      it->second = nullptr;
    }
  };
  for (const auto& field : config_.fields()) {
    if (!field.queryable) continue;
    register_name(field.id, &field);
    for (const auto& alias : field.aliases)
      register_name(alias, &field);
  }
}

MetadataResult<std::vector<MetadataQueryPredicate>> MetadataQueryEvaluator::parse(
  const std::string& expression) const {
  if (expression.empty())
    return MetadataResult<std::vector<MetadataQueryPredicate>>::failure("query is required");
  if (expression.size() > 8192)
    return MetadataResult<std::vector<MetadataQueryPredicate>>::failure("query exceeds size cap");

  std::vector<MetadataQueryPredicate> predicates;
  std::size_t begin = 0;
  while (begin <= expression.size()) {
    const auto comma = expression.find(',', begin);
    const auto segment = trim(expression.substr(begin,
      comma == std::string::npos ? std::string::npos : comma - begin));
    if (segment.empty())
      return MetadataResult<std::vector<MetadataQueryPredicate>>::failure("empty query term");
    const auto eq = segment.find('=');
    if (eq == std::string::npos || segment.find('=', eq + 1) != std::string::npos)
      return MetadataResult<std::vector<MetadataQueryPredicate>>::failure("query term must be key=value");
    const auto key = lower(trim(segment.substr(0, eq)));
    const auto raw_value = trim(segment.substr(eq + 1));
    if (key.empty() || raw_value.empty())
      return MetadataResult<std::vector<MetadataQueryPredicate>>::failure("query key and value are required");
    const auto it = fields_.find(key);
    if (it == fields_.end() || it->second == nullptr)
      return MetadataResult<std::vector<MetadataQueryPredicate>>::failure("metadata field is not queryable: " + key);
    const auto parsed = parse_value(raw_value, it->second->type);
    if (!parsed.ok()) return MetadataResult<std::vector<MetadataQueryPredicate>>::failure(parsed.error());
    predicates.push_back({it->second->id, parsed.value()});
    if (predicates.size() > 32)
      return MetadataResult<std::vector<MetadataQueryPredicate>>::failure("too many query terms");
    if (comma == std::string::npos) break;
    begin = comma + 1;
  }
  return MetadataResult<std::vector<MetadataQueryPredicate>>::success(std::move(predicates));
}

bool MetadataQueryEvaluator::matches(const MetadataMap& values,
  const std::vector<MetadataQueryPredicate>& predicates) const {
  if (predicates.empty() || predicates.size() > 32) return false;
  for (const auto& predicate : predicates) {
    const auto field_it = fields_.find(lower(predicate.field_id));
    if (field_it == fields_.end() || field_it->second == nullptr ||
        field_it->second->id != predicate.field_id ||
        field_it->second->type != predicate.expected.type())
      return false;
    const auto it = values.find(predicate.field_id);
    if (it == values.end() || !equal_value(it->second, predicate.expected)) return false;
  }
  return true;
}

MetadataResult<bool> MetadataQueryEvaluator::evaluate(const std::string& expression,
  const MetadataMap& values) const {
  const auto parsed = parse(expression);
  if (!parsed.ok()) return MetadataResult<bool>::failure(parsed.error());
  return MetadataResult<bool>::success(matches(values, parsed.value()));
}

} // namespace iotdbfs
