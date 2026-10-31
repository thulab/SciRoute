#pragma once

#include <cctype>
#include <cstring>
#include <string>
#include <string_view>

namespace path_utils {

inline std::string norm(const std::string& p) {
  if (p.empty())
    return "/";
  return p[0] == '/' ? p : std::string("/") + p;
}

// Return the final component of a path (after the last slash).
inline std::string base(const std::string& p) {
  if (p == "/")
    return "/";
  auto pos = p.find_last_of('/');
  return (pos == std::string::npos) ? p : p.substr(pos + 1);
}

// Return the directory portion of a path, defaulting to root.
inline std::string parent_of(const std::string& p) {
  if (p == "/")
    return "/";
  auto pos = p.find_last_of('/');
  if (pos == std::string::npos || pos == 0)
    return "/";
  return p.substr(0, pos);
}

inline std::string to_lower(std::string s) {
  for (auto& c : s)
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

// Check whether the path points to a file with a recognised GRIB extension.
inline bool has_grib_ext(const std::string& p) {
  std::string b = to_lower(base(p));
  auto ends_with = [](const std::string& s, std::string_view suf) {
    return s.size() >= suf.size() &&
      s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
    };
  return ends_with(b, ".grib") || ends_with(b, ".grib2") ||
    ends_with(b, ".grb") || ends_with(b, ".grb2");
}

inline bool ends_with(const std::string& s, std::string_view suf) {
    return s.size() >= suf.size() &&
      s.compare(s.size() - suf.size(), suf.size(), suf) == 0;
}

// Split a path into path component and query string (following '?').
inline void split_path_and_query(const std::string& p,
  std::string& base_path, std::string& query) {
  auto pos = p.find('?');
  if (pos == std::string::npos) {
    base_path = p;
    query.clear();
  }
  else {
    base_path = p.substr(0, pos);
    query = p.substr(pos + 1);
  }
  if (base_path.empty())
    base_path = "/";
}

} // namespace path_utils
