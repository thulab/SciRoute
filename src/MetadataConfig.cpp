#include "iotdbfs/MetadataConfig.h"
#include "iotdbfs/MetadataSerialization.h"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <optional>
#include <regex>
#include <set>
#include <stdexcept>
#include <sstream>
#include <fstream>
#include <charconv>
#include <locale>
#include "iotdbfs/FeatureConfig.h"
#include "iotdbfs/Filesystem.h"

namespace iotdbfs {
namespace {

std::string trim(std::string value) {
  const auto first = value.find_first_not_of(" \t\r\n");
  if (first == std::string::npos)
    return {};
  const auto last = value.find_last_not_of(" \t\r\n");
  return value.substr(first, last - first + 1);
}

bool parse_bool(const std::string& value, bool& out) {
  const auto v = trim(value);
  if (v == "true" || v == "1" || v == "yes") { out = true; return true; }
  if (v == "false" || v == "0" || v == "no") { out = false; return true; }
  return false;
}

std::string json_escape(const std::string& value) {
  std::ostringstream out;
  for (unsigned char c : value) {
    switch (c) {
    case '"': out << "\\\""; break;
    case '\\': out << "\\\\"; break;
    case '\b': out << "\\b"; break;
    case '\f': out << "\\f"; break;
    case '\n': out << "\\n"; break;
    case '\r': out << "\\r"; break;
    case '\t': out << "\\t"; break;
    default:
      if (c < 0x20) {
        out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
            << static_cast<int>(c) << std::dec;
      } else out << static_cast<char>(c);
    }
  }
  return out.str();
}

const char* type_name(MetadataValueType type) {
  switch (type) {
  case MetadataValueType::String: return "string";
  case MetadataValueType::Int64: return "int64";
  case MetadataValueType::Double: return "double";
  case MetadataValueType::Bool: return "bool";
  case MetadataValueType::StringSet: return "string_set";
  case MetadataValueType::Int64Set: return "int64_set";
  case MetadataValueType::DoubleSet: return "double_set";
  case MetadataValueType::BoolSet: return "bool_set";
  }
  return "unknown";
}

} // namespace

MetadataValueType MetadataValue::type() const {
  switch (value_.index()) {
  case 0: return MetadataValueType::String;
  case 1: return MetadataValueType::Int64;
  case 2: return MetadataValueType::Double;
  case 3: return MetadataValueType::Bool;
  case 4: return MetadataValueType::StringSet;
  case 5: return MetadataValueType::Int64Set;
  case 6: return MetadataValueType::DoubleSet;
  case 7: return MetadataValueType::BoolSet;
  }
  return MetadataValueType::String;
}

MetadataResult<MetadataConfig> MetadataConfig::parse(const std::string& text) {
  using Result = MetadataResult<MetadataConfig>;
  if (text.size() > 65536 || text.find('\0') != std::string::npos || !valid_metadata_utf8(text))
    return Result::failure("metadata configuration exceeds 65536 bytes or contains NUL");
  static const std::regex identifier("[A-Za-z][A-Za-z0-9_]{0,63}");
  static const std::regex source_key("[A-Za-z_][A-Za-z0-9_]{0,127}");
  static const std::regex xattr_name("user\\.[A-Za-z0-9_]+(\\.[A-Za-z0-9_]+)*");
  auto number = [](const std::string& raw, double& value) {
    std::istringstream stream(raw);
    stream.imbue(std::locale::classic());
    stream >> std::noskipws >> value;
    return !stream.fail() && stream.eof() && std::isfinite(value);
  };
  std::map<std::string, std::string> props;
  std::istringstream input(text);
  std::string line;
  while (std::getline(input, line)) {
    line = trim(line);
    if (line.empty() || line[0] == '#' || line[0] == ';' || line.rfind("//", 0) == 0)
      continue;
    const auto eq = line.find('=');
    if (eq == std::string::npos) return Result::failure("invalid metadata property line");
    const auto key = trim(line.substr(0, eq));
    if (!props.emplace(key, trim(line.substr(eq + 1))).second)
      return Result::failure("duplicate metadata property: " + key);
  }
  if (props["metadata.schema_version"] != "1")
    return Result::failure("metadata.schema_version must be 1");

  const std::string prefix = "metadata.field.";
  std::map<std::string, std::map<std::string, std::string>> definitions;
  const std::set<std::string> allowed = {
    "source", "type", "aliases", "xattr", "required", "queryable", "scale", "default"
  };
  for (const auto& property : props) {
    if (property.first == "metadata.schema_version") continue;
    if (property.first.rfind(prefix, 0) != 0)
      return Result::failure("unknown metadata property: " + property.first);
    const auto rest = property.first.substr(prefix.size());
    const auto dot = rest.find('.');
    const auto id = rest.substr(0, dot);
    if (dot == std::string::npos || !std::regex_match(id, identifier))
      return Result::failure("invalid metadata field id");
    const auto name = rest.substr(dot + 1);
    if (!allowed.count(name)) return Result::failure("unknown metadata field property: " + name);
    definitions[id][name] = property.second;
    if (definitions.size() > 128) return Result::failure("metadata supports at most 128 fields");
  }

  MetadataConfig config;
  std::set<std::string> xattrs;
  std::set<std::string> ids;
  for (const auto& item : definitions) {
    MetadataFieldDefinition field;
    field.id = item.first;
    if (!ids.insert(lower_feature_value(field.id)).second)
      return Result::failure("metadata field IDs must be distinct ignoring case");
    auto properties = item.second;
    field.source = properties["source"];
    if (!std::regex_match(field.source, source_key))
      return Result::failure("invalid metadata source: " + field.id);
    const auto type = properties["type"];
    if (type == "string") field.type = MetadataValueType::String;
    else if (type == "int64") field.type = MetadataValueType::Int64;
    else if (type == "double") field.type = MetadataValueType::Double;
    else if (type == "bool") field.type = MetadataValueType::Bool;
    else return Result::failure("metadata type must be string, int64, double or bool: " + field.id);
    for (const auto& property : {"required", "queryable"}) {
      auto it = properties.find(property);
      bool& flag = std::string(property) == "required" ? field.required : field.queryable;
      if (it != properties.end() && !parse_bool(it->second, flag))
        return Result::failure("invalid metadata boolean: " + field.id);
    }
    field.xattr = properties["xattr"];
    if (!field.xattr.empty()) {
      if (field.xattr.size() > 255 || !std::regex_match(field.xattr, xattr_name) ||
          field.xattr.rfind("user.grib.index.", 0) == 0 ||
          !xattrs.insert(lower_feature_value(field.xattr)).second)
        return Result::failure("invalid, reserved or duplicate metadata xattr: " + field.id);
    } else if (field.queryable) {
      return Result::failure("queryable metadata requires an xattr: " + field.id);
    }
    if (properties.count("aliases")) {
      auto remaining = properties["aliases"];
      std::set<std::string> seen{field.source};
      do {
        auto comma = remaining.find(',');
        auto alias = trim(remaining.substr(0, comma));
        if (!std::regex_match(alias, source_key) || !seen.insert(alias).second ||
            field.aliases.size() >= 8)
          return Result::failure("invalid metadata aliases: " + field.id);
        field.aliases.push_back(alias);
        if (comma == std::string::npos) break;
        remaining.erase(0, comma + 1);
      } while (true);
    }
    if (properties.count("scale")) {
      if (field.type != MetadataValueType::Double || !number(properties["scale"], field.scale))
        return Result::failure("scale must be finite and requires double type: " + field.id);
    }
    if (properties.count("default")) {
      const auto& raw = properties["default"];
      if (field.type == MetadataValueType::String) {
        if (raw.size() > 4096) return Result::failure("metadata default exceeds 4096 bytes");
        field.default_value = MetadataValue(raw);
      } else if (field.type == MetadataValueType::Int64) {
        std::int64_t value = 0;
        const auto parsed = std::from_chars(raw.data(), raw.data() + raw.size(), value);
        if (parsed.ec != std::errc{} || parsed.ptr != raw.data() + raw.size())
          return Result::failure("invalid int64 metadata default: " + field.id);
        field.default_value = MetadataValue(value);
      } else if (field.type == MetadataValueType::Double) {
        double value = 0;
        if (!number(raw, value)) return Result::failure("invalid double metadata default: " + field.id);
        field.default_value = MetadataValue(value);
      } else {
        bool value = false;
        if (!parse_bool(raw, value)) return Result::failure("invalid bool metadata default: " + field.id);
        field.default_value = MetadataValue(value);
      }
    }
    config.fields_.push_back(std::move(field));
  }
  return Result::success(std::move(config));
}

MetadataResult<MetadataConfig> MetadataConfig::load_file(const std::string& path) {
  std::ifstream input(path);
  if (!input)
    return MetadataResult<MetadataConfig>::failure("unable to open metadata config: " + path);
  std::string text(65537, '\0');
  input.read(text.data(), static_cast<std::streamsize>(text.size()));
  text.resize(static_cast<size_t>(input.gcount()));
  if (input.bad()) return MetadataResult<MetadataConfig>::failure("unable to read metadata config: " + path);
  return parse(text);
}

MetadataResult<std::string> serialize_metadata(const MetadataMap& values,
  std::size_t max_bytes) {
  max_bytes = std::min<std::size_t>(max_bytes, 65536);
  if (values.size() > 128)
    return MetadataResult<std::string>::failure("too many metadata fields");
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << '{';
  bool first = true;
  for (const auto& item : values) {
    if (item.first.size() > 64 || !valid_metadata_utf8(item.first))
      return MetadataResult<std::string>::failure("invalid metadata key");
    if (!first) out << ',';
    first = false;
    out << '"' << json_escape(item.first) << "\":{\"type\":\""
        << type_name(item.second.type()) << "\",\"value\":";
    const auto& value = item.second.value();
    if (std::holds_alternative<std::string>(value)) {
      const auto& text = std::get<std::string>(value);
      if (text.size() > 4096 || !valid_metadata_utf8(text))
        return MetadataResult<std::string>::failure("invalid or oversized metadata string");
      out << '"' << json_escape(text) << '"';
    }
    else if (std::holds_alternative<std::int64_t>(value)) out << std::get<std::int64_t>(value);
    else if (std::holds_alternative<double>(value)) {
      const double number = std::get<double>(value);
      if (!std::isfinite(number)) return MetadataResult<std::string>::failure("metadata contains non-finite number");
      out << std::setprecision(17) << number;
    } else if (std::holds_alternative<bool>(value)) {
      out << (std::get<bool>(value) ? "true" : "false");
    } else if (std::holds_alternative<MetadataStringSet>(value)) {
      out << '[';
      bool first_member = true;
      for (const auto& member : std::get<MetadataStringSet>(value)) {
        if (member.size() > 4096 || !valid_metadata_utf8(member))
          return MetadataResult<std::string>::failure(
            "invalid or oversized metadata set string");
        if (!first_member) out << ',';
        first_member = false;
        out << '"' << json_escape(member) << '"';
      }
      out << ']';
    } else if (std::holds_alternative<MetadataInt64Set>(value)) {
      out << '[';
      bool first_member = true;
      for (const auto member : std::get<MetadataInt64Set>(value)) {
        if (!first_member) out << ',';
        first_member = false;
        out << member;
      }
      out << ']';
    } else if (std::holds_alternative<MetadataDoubleSet>(value)) {
      out << '[';
      bool first_member = true;
      for (const auto member : std::get<MetadataDoubleSet>(value)) {
        if (!std::isfinite(member))
          return MetadataResult<std::string>::failure(
            "metadata set contains non-finite number");
        if (!first_member) out << ',';
        first_member = false;
        out << std::setprecision(17) << member;
      }
      out << ']';
    } else {
      out << '[';
      bool first_member = true;
      for (const auto member : std::get<MetadataBoolSet>(value)) {
        if (!first_member) out << ',';
        first_member = false;
        out << (member ? "true" : "false");
      }
      out << ']';
    }
    out << '}';
    if (out.tellp() > static_cast<std::streamoff>(max_bytes))
      return MetadataResult<std::string>::failure("metadata exceeds size cap");
  }
  out << '}';
  const auto result = out.str();
  if (result.size() > max_bytes) return MetadataResult<std::string>::failure("metadata exceeds size cap");
  return MetadataResult<std::string>::success(result);
}

MetadataResult<MetadataConfig> load_metadata_config(const std::string& path) {
  const fs::path parent = fs::path(path).parent_path();
  if (const char* override_path = std::getenv("IOTDB_METADATA_CONFIG_FILE")) {
    if (*override_path) return MetadataConfig::load_file(override_path);
  }
  const auto properties = load_simple_properties(path);
  const auto configured = properties.find("metadata.config");
  if (configured != properties.end()) {
    if (configured->second.empty())
      return MetadataResult<MetadataConfig>::failure("metadata.config cannot be empty");
    auto resolved = fs::path(configured->second);
    if (resolved.is_relative()) resolved = parent / resolved;
    return MetadataConfig::load_file(resolved.string());
  }
  const auto sibling = parent / "metadata.properties";
  std::error_code ec;
  const bool exists = fs::exists(sibling, ec);
  if (!exists && !ec) return MetadataResult<MetadataConfig>::success(MetadataConfig{});
  return MetadataConfig::load_file(sibling.string());
}

} // namespace iotdbfs
