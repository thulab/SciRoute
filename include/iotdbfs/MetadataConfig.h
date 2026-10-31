#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <variant>
#include <vector>

namespace iotdbfs {

using MetadataStringSet = std::set<std::string>;
using MetadataInt64Set = std::set<std::int64_t>;
using MetadataDoubleSet = std::set<double>;
using MetadataBoolSet = std::set<bool>;

enum class MetadataValueType {
  String,
  Int64,
  Double,
  Bool,
  StringSet,
  Int64Set,
  DoubleSet,
  BoolSet,
};

using MetadataValueStorage = std::variant<std::string, std::int64_t, double,
  bool, MetadataStringSet, MetadataInt64Set, MetadataDoubleSet,
  MetadataBoolSet>;

class MetadataValue {
public:
  MetadataValue() : value_(std::string()) {}
  explicit MetadataValue(std::string value) : value_(std::move(value)) {}
  explicit MetadataValue(const char* value) : value_(std::string(value)) {}
  explicit MetadataValue(std::int64_t value) : value_(value) {}
  explicit MetadataValue(double value) : value_(value) {}
  explicit MetadataValue(bool value) : value_(value) {}
  explicit MetadataValue(MetadataStringSet value) : value_(std::move(value)) {}
  explicit MetadataValue(MetadataInt64Set value) : value_(std::move(value)) {}
  explicit MetadataValue(MetadataDoubleSet value) : value_(std::move(value)) {}
  explicit MetadataValue(MetadataBoolSet value) : value_(std::move(value)) {}

  MetadataValueType type() const;
  const MetadataValueStorage& value() const {
    return value_;
  }
  MetadataValueStorage& value() { return value_; }

private:
  MetadataValueStorage value_;
};

using MetadataMap = std::map<std::string, MetadataValue>;

template<typename T>
class MetadataResult {
public:
  static MetadataResult success(T value) {
    MetadataResult result;
    result.value_ = std::move(value);
    return result;
  }
  static MetadataResult failure(std::string error) {
    MetadataResult result;
    result.error_ = std::move(error);
    return result;
  }
  bool ok() const { return error_.empty(); }
  const std::string& error() const { return error_; }
  const T& value() const { return *value_; }
  T& value() { return *value_; }

private:
  std::string error_;
  std::optional<T> value_;
};

struct MetadataFieldDefinition {
  std::string id;
  std::string source;
  std::vector<std::string> aliases;
  std::optional<MetadataValue> default_value;
  MetadataValueType type{ MetadataValueType::String };
  std::string xattr;
  bool queryable{ false };
  bool required{ false };
  double scale{ 1.0 };
};

class MetadataConfig {
public:
  static MetadataResult<MetadataConfig> parse(const std::string& text);
  static MetadataResult<MetadataConfig> load_file(const std::string& path);
  const std::vector<MetadataFieldDefinition>& fields() const { return fields_; }

private:
  std::vector<MetadataFieldDefinition> fields_;
};

MetadataResult<std::string> serialize_metadata(const MetadataMap& values,
  std::size_t max_bytes);

// Load the metadata schema referenced by the main properties file. A missing
// metadata config returns an empty extension schema; canonical adapter fields
// remain available in the typed document.
MetadataResult<MetadataConfig> load_metadata_config(const std::string& path);

} // namespace iotdbfs
