#pragma once

#include "iotdbfs/PathUtils.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unordered_map>

namespace iotdbfs {

static constexpr const char* kGribExtensionEnabledKey =
  "grib.extension.enabled";
static constexpr const char* kGribExtensionEnabledEnv =
  "IOTDB_GRIB_EXTENSION_ENABLED";

inline std::string trim_feature_value(const std::string& s) {
  size_t begin = 0;
  while (begin < s.size() &&
    std::isspace(static_cast<unsigned char>(s[begin]))) {
    ++begin;
  }
  size_t end = s.size();
  while (end > begin &&
    std::isspace(static_cast<unsigned char>(s[end - 1]))) {
    --end;
  }
  return s.substr(begin, end - begin);
}

inline std::string lower_feature_value(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
    [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

inline bool parse_bool_feature_value(const std::string& raw, bool fallback) {
  const std::string value = lower_feature_value(trim_feature_value(raw));
  if (value.empty())
    return fallback;
  if (value == "1" || value == "true" || value == "yes" ||
    value == "on" || value == "enabled") {
    return true;
  }
  if (value == "0" || value == "false" || value == "no" ||
    value == "off" || value == "disabled") {
    return false;
  }
  return fallback;
}

template<typename PropertyMap>
inline bool grib_extension_enabled_from_properties(const PropertyMap& props,
  bool fallback = true) {
  auto it = props.find(kGribExtensionEnabledKey);
  if (it != props.end())
    return parse_bool_feature_value(it->second, fallback);

  it = props.find("grib.enabled");
  if (it != props.end())
    return parse_bool_feature_value(it->second, fallback);

  return fallback;
}

inline bool grib_extension_enabled_from_env(bool fallback) {
  const char* env = std::getenv(kGribExtensionEnabledEnv);
  if (!env || !*env)
    return fallback;
  return parse_bool_feature_value(env, fallback);
}

inline std::string default_config_path() {
  const char* config_file_env = std::getenv("IOTDB_CONFIG_FILE");
  if (config_file_env && *config_file_env)
    return config_file_env;

  const char* conf_dir_env = std::getenv("IOTDB_CONF_DIR");
  const std::string conf_dir =
    (conf_dir_env && *conf_dir_env) ? conf_dir_env : "resources";
  return conf_dir + "/config.properties";
}

inline std::unordered_map<std::string, std::string>
load_simple_properties(const std::string& path, bool* loaded = nullptr) {
  std::unordered_map<std::string, std::string> values;
  if (loaded)
    *loaded = false;

  std::ifstream in(path);
  if (!in)
    return values;
  if (loaded)
    *loaded = true;

  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    const std::string trimmed = trim_feature_value(line);
    if (trimmed.empty() || trimmed[0] == '#' || trimmed[0] == ';' ||
      (trimmed.size() >= 2 && trimmed[0] == '/' && trimmed[1] == '/')) {
      continue;
    }
    const size_t eq = trimmed.find('=');
    if (eq == std::string::npos)
      continue;
    const std::string key = trim_feature_value(trimmed.substr(0, eq));
    const std::string value = trim_feature_value(trimmed.substr(eq + 1));
    if (!key.empty())
      values[key] = value;
  }
  return values;
}

inline bool load_grib_extension_enabled_from_config_file(
  const std::string& path, bool fallback = true) {
  bool loaded = false;
  const auto props = load_simple_properties(path, &loaded);
  bool enabled = loaded
    ? grib_extension_enabled_from_properties(props, fallback)
    : fallback;
  return grib_extension_enabled_from_env(enabled);
}

inline bool load_grib_extension_enabled_from_default_config(
  bool fallback = true) {
  return load_grib_extension_enabled_from_config_file(default_config_path(),
    fallback);
}

inline bool grib_enabled_for_path(bool extension_enabled,
  const std::string& path) {
  return extension_enabled && path_utils::has_grib_ext(path);
}

} // namespace iotdbfs
