#pragma once

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>

namespace iotdbfs {

inline constexpr size_t kDefaultIoTDBPoolSize = 8;
inline constexpr size_t kMaxIoTDBPoolSize = 256;

inline bool is_valid_database_name(std::string_view name) {
  if (name.empty() || name.size() > 128)
    return false;
  const auto is_letter = [](char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
  };
  const auto is_digit = [](char c) { return c >= '0' && c <= '9'; };
  if (!is_letter(name.front()) && name.front() != '_')
    return false;
  for (size_t i = 1; i < name.size(); ++i) {
    if (!is_letter(name[i]) && !is_digit(name[i]) && name[i] != '_')
      return false;
  }
  return true;
}

inline size_t normalize_iotdb_pool_size(size_t requested) {
  const size_t normalized = requested == 0 ? kDefaultIoTDBPoolSize : requested;
  if (normalized > kMaxIoTDBPoolSize)
    throw std::invalid_argument("IoTDB session pool size exceeds maximum " +
      std::to_string(kMaxIoTDBPoolSize));
  return normalized;
}

inline std::string redact_sql_for_log(std::string_view sql) {
  std::string out;
  out.reserve(sql.size());
  bool in_literal = false;
  bool literal_has_content = false;
  for (size_t i = 0; i < sql.size(); ++i) {
    const char c = sql[i];
    if (!in_literal) {
      if (c == '\'') {
        out.push_back('\'');
        out.push_back('?');
        in_literal = true;
        literal_has_content = false;
      }
      else {
        out.push_back(c);
      }
      continue;
    }

    if (c == '\\' && i + 1 < sql.size()) {
      ++i;
      continue;
    }
    if (c == '\'') {
      if (literal_has_content && i + 1 < sql.size() && sql[i + 1] == '\'') {
        ++i;
        continue;
      }
      out.push_back('\'');
      in_literal = false;
      literal_has_content = false;
      continue;
    }
    literal_has_content = true;
  }
  return out;
}

} // namespace iotdbfs
