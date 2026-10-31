#pragma once

#include "iotdbfs/PathUtils.h"

#include <cctype>
#include <map>
#include <string>
#include <string_view>

namespace query_utils {

inline std::string sql_escape_literal(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size() + 2);
  escaped.push_back('\'');
  for (char ch : value) {
    if (ch == '\'')
      escaped += "''";
    else
      escaped.push_back(ch);
  }
  escaped.push_back('\'');
  return escaped;
}

inline std::string sql_literal(std::string_view value) {
  return sql_escape_literal(value);
}

inline std::string sql_escape(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size() + 8);
  for (char ch : value) {
    if (ch == '\'')
      escaped += "''";
    else
      escaped.push_back(ch);
  }
  return escaped;
}

inline std::string like_pattern(std::string_view value) {
  std::string escaped;
  escaped.reserve(value.size() + 8);
  for (char ch : value) {
    if (ch == '!' || ch == '%' || ch == '_')
      escaped.push_back('!');
    escaped.push_back(ch);
  }
  return escaped;
}

inline std::string like_subtree_predicate(std::string_view directory) {
  const std::string pattern = like_pattern(directory) + "/%";
  return "path LIKE '" + sql_escape(pattern) + "' ESCAPE '!'";
}

inline bool is_safe_identifier(std::string_view value) {
  if (value.empty() || value.size() > 128)
    return false;
  for (size_t i = 0; i < value.size(); ++i) {
    const unsigned char ch = static_cast<unsigned char>(value[i]);
    if (i == 0) {
      if (!(std::isalpha(ch) || ch == '_'))
        return false;
    }
    else if (!(std::isalnum(ch) || ch == '_')) {
      return false;
    }
  }
  return true;
}

inline std::map<std::string, std::string>
parse_kv_query(const std::string& query) {
  // Parse key=value pairs into an ordered map.
  // Supported separators: ',' '&' ';'
  std::map<std::string, std::string> kv;
  size_t i = 0;
  while (i < query.size()) {
    size_t j = i;
    while (j < query.size() && query[j] != ',' && query[j] != '&' &&
      query[j] != ';')
      ++j;
    std::string pair =
      (j >= query.size()) ? query.substr(i) : query.substr(i, j - i);
    i = (j >= query.size()) ? query.size() : j + 1;
    if (pair.empty())
      continue;
    size_t eq = pair.find('=');
    std::string k = (eq == std::string::npos) ? pair : pair.substr(0, eq);
    std::string v = (eq == std::string::npos) ? std::string()
      : pair.substr(eq + 1);
    auto trim = [](std::string& s) {
      size_t a = 0;
      while (a < s.size() && std::isspace(static_cast<unsigned char>(s[a])))
        ++a;
      size_t b = s.size();
      while (b > a && std::isspace(static_cast<unsigned char>(s[b - 1])))
        --b;
      s = (a < b) ? s.substr(a, b - a) : std::string();
      };
    trim(k);
    trim(v);
    if (!k.empty())
      kv[k] = v;
  }
  return kv;
}

inline bool is_ctrl_key(const std::string& k) {
  std::string kk = path_utils::to_lower(k);
  // Control keys affect decoding rather than message selection.
  return (kk == "type" || kk == "function" || kk == "dtype" || kk == "endian" || kk == "interp" ||
    kk == "outni" || kk == "outnj" || kk == "dlon" || kk == "dlat" ||
    kk == "border" || kk == "lat0" || kk == "lon0" || kk == "lat1" ||
    kk == "lon1");
}

inline std::map<std::string, std::string>
filter_index_match_kv(const std::map<std::string, std::string>& kv) {
  std::map<std::string, std::string> out;
  for (auto& it : kv)
    // Only retain keys needed to match a GRIB message index.
    if (!is_ctrl_key(it.first))
      out.insert(it);
  return out;
}

} // namespace query_utils
