#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/xattr.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include "iotdbfs/FeatureConfig.h"
#include "iotdbfs/Filesystem.h"
#include "iotdbfs/MetadataConfig.h"
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

// namespace fs definition handled by Filesystem.h

namespace {

struct PropertiesResult {
  bool loaded{ false };
  std::map<std::string, std::string> values;
};

struct AppConfig {
  std::string host{ "127.0.0.1" };
  int port{ 8080 };
  fs::path root{ "/mnt/dfs" };
  int workers{ 0 };
  std::string config_path;
  bool config_loaded{ false };
  bool grib_extension_enabled{ true };
  std::string auth_token;
  bool require_auth_all{ false };
  size_t max_header_bytes{ 64 * 1024 };
  size_t max_body_bytes{ 256 * 1024 * 1024 };
  size_t max_upload_bytes{ 64 * 1024 * 1024 };
  int socket_timeout_ms{ 30000 };
  size_t max_queue{ 256 };
  std::string config_error;
  std::optional<iotdbfs::MetadataConfig> metadata_config;
  std::string metadata_config_error;
};

constexpr size_t kDefaultMaxHttpHeaderBytes = 64 * 1024;
constexpr size_t kDefaultMaxHttpBodyBytes = 256 * 1024 * 1024;
constexpr size_t kDefaultMaxUploadBytes = 64 * 1024 * 1024;
constexpr int kDefaultSocketTimeoutMs = 30000;
constexpr size_t kDefaultMaxQueuedClients = 256;
constexpr size_t kAbsoluteMaxHeaderBytes = 4 * 1024 * 1024;
constexpr size_t kAbsoluteMaxBodyBytes = 2ull * 1024 * 1024 * 1024;
constexpr int kMaxSocketTimeoutMs = 10 * 60 * 1000;
constexpr size_t kMaxQueuedClientsLimit = 4096;

struct EntryInfo {
  std::string name;
  std::string web_path;
  std::string kind;
  std::string extension;
  std::uintmax_t size{ 0 };
  bool size_known{ false };
  long long mtime_ms{ 0 };
  bool hidden{ false };
};

struct HttpRequest {
  std::string method;
  std::string target;
  std::string route;
  std::map<std::string, std::string> query;
  std::map<std::string, std::string> headers;
  size_t content_length{ 0 };
  std::string body_prefix;
};

struct XattrInfo {
  std::string name;
  std::string value;
  bool binary{ false };
};

struct SearchHit {
  EntryInfo entry;
  std::vector<XattrInfo> attributes;
};

struct MetadataQueryTerm {
  std::string key;
  std::string value;
  std::string key_lower;
  std::string value_lower;
};

struct BatchDownloadRequestItem {
  int index{ 0 };
  std::string web_path;
  std::string read_query;
};

std::string request_read_query(const HttpRequest& req);
std::string build_target_native_path(const fs::path& resolved,
  const std::string& read_query);
bool stat_native_path(const std::string& native, struct stat& st);
std::string filename_for_download(const fs::path& p);
std::string query_result_filename(const fs::path& p,
  const std::string& read_query);
std::string sanitize_upload_name(std::string name);
bool stream_request_body_to_file(int fd, const HttpRequest& req,
  const fs::path& target, std::uintmax_t& bytes_written, std::string& error);
std::vector<XattrInfo> read_extended_attributes(const fs::path& path,
  std::string& error);

std::string trim_copy(const std::string& s) {
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

bool starts_with(const std::string& s, const char* prefix) {
  const size_t n = std::strlen(prefix);
  return s.size() >= n && std::equal(prefix, prefix + n, s.begin());
}

bool should_expose_metadata_attr(const std::string& name) {
  return name == "user.grib.shortName" ||
    name == "user.grib.typeOfProcessedData" ||
    name == "user.grib.typeOfGeneratingProcess" ||
    name == "user.grib.typeOfLevel" ||
    name == "user.grib.centre" ||
    name == "user.grib.generatingProcessIdentifier" ||
    name == "user.grib.discipline" ||
    name == "user.grib.parameterCategory" ||
    name == "user.grib.parameterNumber" ||
    name == "user.grib.globalDomain" ||
    name == "user.grib.latitudeOfFirstGridPoint" ||
    name == "user.grib.longitudeOfFirstGridPoint" ||
    name == "user.grib.latitudeOfLastGridPoint" ||
    name == "user.grib.longitudeOfLastGridPoint" ||
    name == "user.grib.iDirectionIncrement" ||
    name == "user.grib.jDirectionIncrement" ||
    name == "user.grib.Ni" || name == "user.grib.Nj" ||
    name == "user.grib.numberOfDataPoints" ||
    name == "user.grib.index.keys" || name == "user.grib.index.values" ||
    name == "user.grib.unit";
}

std::string simplify_metadata_attr_name(const std::string& name) {
  if (starts_with(name, "user.grib."))
    return name.substr(std::strlen("user.grib."));
  if (starts_with(name, "user."))
    return name.substr(std::strlen("user."));
  return name;
}

std::string to_lower_copy(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
    [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

const iotdbfs::MetadataFieldDefinition* configured_field_for_xattr(
  const AppConfig& cfg, const std::string& name) {
  if (!cfg.metadata_config) return nullptr;
  for (const auto& field : cfg.metadata_config->fields())
    if (field.xattr == name) return &field;
  return nullptr;
}

std::vector<XattrInfo> filter_exposed_metadata_attrs(
  const AppConfig& cfg, const std::vector<XattrInfo>& attrs) {
  std::vector<XattrInfo> filtered;
  filtered.reserve(attrs.size());
  for (const auto& attr : attrs) {
    if (should_expose_metadata_attr(attr.name) || configured_field_for_xattr(cfg, attr.name))
      filtered.push_back(attr);
  }
  return filtered;
}

bool parse_metadata_query(const std::string& raw_query,
  std::vector<MetadataQueryTerm>& terms, std::string& error) {
  terms.clear();
  error.clear();

  std::stringstream ss(raw_query);
  std::string segment;
  while (std::getline(ss, segment, ',')) {
    segment = trim_copy(segment);
    if (segment.empty()) {
      error = "Invalid metadata query: empty segment in query";
      return false;
    }

    const size_t eq = segment.find('=');
    if (eq == std::string::npos) {
      error = "Invalid metadata query segment \"" + segment +
        "\": expected key=value";
      return false;
    }

    MetadataQueryTerm term;
    term.key = trim_copy(segment.substr(0, eq));
    term.value = trim_copy(segment.substr(eq + 1));
    if (term.key.empty()) {
      error = "Invalid metadata query segment \"" + segment +
        "\": key cannot be empty";
      return false;
    }
    term.key_lower = to_lower_copy(term.key);
    term.value_lower = to_lower_copy(term.value);
    terms.push_back(std::move(term));
  }

  if (terms.empty()) {
    error = "Search query is required";
    return false;
  }
  return true;
}

bool metadata_attr_matches_query_key(const AppConfig& cfg,
  const std::string& attr_name, const std::string& query_key_lower) {
  if (const auto* field = configured_field_for_xattr(cfg, attr_name)) {
    if (!field->queryable) return false;
    return to_lower_copy(field->id) == query_key_lower ||
      to_lower_copy(field->xattr) == query_key_lower ||
      to_lower_copy(simplify_metadata_attr_name(field->xattr)) == query_key_lower;
  }
  if (!should_expose_metadata_attr(attr_name)) return false;
  const std::string attr_name_lower = to_lower_copy(attr_name);
  if (attr_name_lower == query_key_lower)
    return true;
  return to_lower_copy(simplify_metadata_attr_name(attr_name)) == query_key_lower;
}

std::string format_metadata_query(
  const std::vector<MetadataQueryTerm>& terms) {
  std::ostringstream os;
  for (size_t i = 0; i < terms.size(); ++i) {
    if (i)
      os << ",";
    os << simplify_metadata_attr_name(terms[i].key) << "=" << terms[i].value;
  }
  return os.str();
}

PropertiesResult load_properties_file(const std::string& path) {
  PropertiesResult res;
  std::ifstream in(path);
  if (!in)
    return res;
  res.loaded = true;
  std::string line;
  while (std::getline(in, line)) {
    if (!line.empty() && line.back() == '\r')
      line.pop_back();
    const std::string trimmed = trim_copy(line);
    if (trimmed.empty() || trimmed[0] == '#' || trimmed[0] == ';' ||
      starts_with(trimmed, "//")) {
      continue;
    }
    const size_t eq = trimmed.find('=');
    if (eq == std::string::npos)
      continue;
    const std::string key = trim_copy(trimmed.substr(0, eq));
    const std::string value = trim_copy(trimmed.substr(eq + 1));
    if (!key.empty())
      res.values[key] = value;
  }
  return res;
}

std::string env_or(const char* key, const std::string& fallback) {
  const char* value = std::getenv(key);
  return (value && *value) ? std::string(value) : fallback;
}

int default_worker_count() {
  const unsigned int hc = std::thread::hardware_concurrency();
  if (hc == 0)
    return 1;
  return static_cast<int>(std::max(1u, hc / 4));
}

int parse_int_or(const std::string& s, int fallback) {
  if (s.empty())
    return fallback;
  try {
    return std::stoi(s);
  }
  catch (...) {
    return fallback;
  }
}

bool parse_content_length(const std::string& s, size_t& out) {
  if (s.empty())
    return false;
  if (s[0] == '+' || s[0] == '-')
    return false;
  try {
    size_t pos = 0;
    unsigned long long value = std::stoull(s, &pos, 10);
    if (pos != s.size())
      return false;
    if (value > static_cast<unsigned long long>(std::numeric_limits<size_t>::max()))
      return false;
    out = static_cast<size_t>(value);
    return true;
  }
  catch (...) {
    return false;
  }
}

bool parse_bounded_size(const std::string& raw, size_t fallback,
  size_t minimum, size_t maximum, size_t& out) {
  const std::string value = trim_copy(raw);
  if (value.empty()) {
    out = fallback;
    return true;
  }
  try {
    size_t pos = 0;
    const unsigned long long parsed = std::stoull(value, &pos, 10);
    if (pos != value.size() || parsed < minimum || parsed > maximum ||
      parsed > static_cast<unsigned long long>(std::numeric_limits<size_t>::max()))
      return false;
    out = static_cast<size_t>(parsed);
    return true;
  }
  catch (...) {
    return false;
  }
}

bool parse_bounded_int(const std::string& raw, int fallback,
  int minimum, int maximum, int& out) {
  const std::string value = trim_copy(raw);
  if (value.empty()) {
    out = fallback;
    return true;
  }
  try {
    size_t pos = 0;
    const long parsed = std::stol(value, &pos, 10);
    if (pos != value.size() || parsed < minimum || parsed > maximum)
      return false;
    out = static_cast<int>(parsed);
    return true;
  }
  catch (...) {
    return false;
  }
}

bool parse_bool_value(const std::string& raw, bool& out) {
  const std::string value = to_lower_copy(trim_copy(raw));
  if (value == "1" || value == "true" || value == "yes" || value == "on") {
    out = true;
    return true;
  }
  if (value == "0" || value == "false" || value == "no" || value == "off") {
    out = false;
    return true;
  }
  return false;
}

bool is_loopback_host(const std::string& host) {
  in_addr address{};
  if (::inet_pton(AF_INET, host.c_str(), &address) != 1)
    return host == "localhost";
  const uint32_t value = ntohl(address.s_addr);
  return (value >> 24) == 127;
}

std::string url_decode(std::string_view input) {
  std::string out;
  out.reserve(input.size());
  for (size_t i = 0; i < input.size(); ++i) {
    const char c = input[i];
    if (c == '+') {
      out.push_back(' ');
      continue;
    }
    if (c == '%' && i + 2 < input.size()) {
      auto hex = [](char ch) -> int {
        if (ch >= '0' && ch <= '9')
          return ch - '0';
        if (ch >= 'a' && ch <= 'f')
          return ch - 'a' + 10;
        if (ch >= 'A' && ch <= 'F')
          return ch - 'A' + 10;
        return -1;
        };
      const int hi = hex(input[i + 1]);
      const int lo = hex(input[i + 2]);
      if (hi >= 0 && lo >= 0) {
        out.push_back(static_cast<char>((hi << 4) | lo));
        i += 2;
        continue;
      }
    }
    out.push_back(c);
  }
  return out;
}

std::map<std::string, std::string> parse_query_string(
  const std::string& query) {
  std::map<std::string, std::string> values;
  size_t begin = 0;
  while (begin <= query.size()) {
    const size_t amp = query.find('&', begin);
    const std::string part =
      query.substr(begin, amp == std::string::npos ? std::string::npos : amp - begin);
    if (!part.empty()) {
      const size_t eq = part.find('=');
      const std::string raw_key = part.substr(0, eq);
      const std::string raw_value =
        eq == std::string::npos ? std::string() : part.substr(eq + 1);
      values[url_decode(raw_key)] = url_decode(raw_value);
    }
    if (amp == std::string::npos)
      break;
    begin = amp + 1;
  }
  return values;
}

std::string json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 16);
  for (unsigned char c : s) {
    switch (c) {
    case '\\':
      out += "\\\\";
      break;
    case '"':
      out += "\\\"";
      break;
    case '\b':
      out += "\\b";
      break;
    case '\f':
      out += "\\f";
      break;
    case '\n':
      out += "\\n";
      break;
    case '\r':
      out += "\\r";
      break;
    case '\t':
      out += "\\t";
      break;
    default:
      if (c < 0x20) {
        char buf[7];
        std::snprintf(buf, sizeof(buf), "\\u%04x", c);
        out += buf;
      }
      else {
        out.push_back(static_cast<char>(c));
      }
      break;
    }
  }
  return out;
}

std::string http_date_now() {
  const auto now = std::chrono::system_clock::now();
  const std::time_t tt = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  gmtime_r(&tt, &tm);
  std::ostringstream os;
  os << std::put_time(&tm, "%a, %d %b %Y %H:%M:%S GMT");
  return os.str();
}

std::string compact_timestamp_now() {
  const auto now = std::chrono::system_clock::now();
  const std::time_t tt = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  localtime_r(&tt, &tm);
  std::ostringstream os;
  os << std::put_time(&tm, "%Y%m%d-%H%M%S");
  return os.str();
}

void log_line(const char* level, const std::string& message) {
  const auto now = std::chrono::system_clock::now();
  const std::time_t tt = std::chrono::system_clock::to_time_t(now);
  std::tm tm{};
  localtime_r(&tt, &tm);
  std::cerr << "[" << std::put_time(&tm, "%F %T") << "]"
            << "[" << level << "] " << message << "\n";
}

void replace_all(std::string& text, const std::string& from,
  const std::string& to) {
  if (from.empty())
    return;
  size_t pos = 0;
  while ((pos = text.find(from, pos)) != std::string::npos) {
    text.replace(pos, from.size(), to);
    pos += to.size();
  }
}

std::string normalize_web_path(const std::string& raw_path) {
  std::vector<std::string> parts;
  std::string path = raw_path.empty() ? "/" : raw_path;
  if (path[0] != '/')
    path.insert(path.begin(), '/');
  size_t begin = 0;
  while (begin <= path.size()) {
    size_t slash = path.find('/', begin);
    if (slash == std::string::npos)
      slash = path.size();
    std::string part = path.substr(begin, slash - begin);
    begin = slash + 1;
    if (part.empty())
      continue;
    if (part == ".")
      continue;
    if (part == "..")
      return {};
    parts.push_back(std::move(part));
  }
  if (parts.empty())
    return "/";
  std::string out;
  for (const auto& part : parts) {
    out.push_back('/');
    out += part;
  }
  return out;
}

bool path_has_prefix(const fs::path& path, const fs::path& root) {
  auto pit = path.begin();
  auto rit = root.begin();
  for (; rit != root.end(); ++rit, ++pit) {
    if (pit == path.end() || *pit != *rit)
      return false;
  }
  return true;
}

fs::path canonical_or_absolute(const fs::path& p) {
  std::error_code ec;
  fs::path resolved = fs::weakly_canonical(p, ec);
  if (!ec)
    return resolved;
  ec.clear();
  resolved = fs::absolute(p, ec);
  if (!ec)
    return resolved;
  return p;
}

std::optional<fs::path> resolve_web_path(const AppConfig& cfg,
  const std::string& web_path) {
  const std::string normalized = normalize_web_path(web_path);
  if (normalized.empty())
    return std::nullopt;

  fs::path relative;
  if (normalized != "/") {
    size_t begin = 1;
    while (begin <= normalized.size()) {
      size_t slash = normalized.find('/', begin);
      if (slash == std::string::npos)
        slash = normalized.size();
      const std::string part = normalized.substr(begin, slash - begin);
      if (!part.empty())
        relative /= part;
      begin = slash + 1;
    }
  }

  const fs::path root_real = canonical_or_absolute(cfg.root);
  const fs::path candidate = root_real / relative;
  const fs::path candidate_real = canonical_or_absolute(candidate);
  if (!path_has_prefix(candidate_real, root_real))
    return std::nullopt;
  return candidate_real;
}

long long file_time_to_unix_ms(fs::file_time_type ft) {
  const auto sys_now = std::chrono::system_clock::now();
  const auto fs_now = fs::file_time_type::clock::now();
  const auto adjusted = ft - fs_now + sys_now;
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    adjusted.time_since_epoch());
  return static_cast<long long>(ms.count());
}

std::string extension_of(const std::string& name) {
  const auto pos = name.find_last_of('.');
  if (pos == std::string::npos || pos + 1 >= name.size())
    return "";
  std::string ext = name.substr(pos + 1);
  std::transform(ext.begin(), ext.end(), ext.begin(),
    [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return ext;
}

bool is_grib2_extension(const std::string& ext) {
  return ext == "grib2" || ext == "grib" || ext == "grb2" || ext == "grb";
}

bool is_grib2_entry(const AppConfig& cfg, const EntryInfo& entry) {
  return cfg.grib_extension_enabled && entry.kind == "file" &&
    is_grib2_extension(entry.extension);
}

bool should_expose_extended_metadata_for_path(const AppConfig& cfg,
  const fs::path& path) {
  if (!cfg.grib_extension_enabled)
    return false;
  std::error_code ec;
  return fs::is_regular_file(path, ec) &&
    is_grib2_extension(extension_of(path.filename().string()));
}

std::string upper_ascii_copy(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
    [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}

std::string human_file_type_label(const fs::path& p) {
  const std::string ext = extension_of(p.filename().string());
  if (ext.empty())
    return "File";
  return upper_ascii_copy(ext) + " File";
}

std::string join_web_path(const std::string& parent, const std::string& name) {
  if (parent.empty() || parent == "/")
    return "/" + name;
  return parent + "/" + name;
}

std::string web_path_from_native(const fs::path& root_real, const fs::path& native) {
  std::error_code ec;
  fs::path rel = fs::relative(native, root_real, ec);
  if (ec || rel.empty() || rel == ".")
    return "/";
  const std::string rel_text = rel.generic_string();
  return rel_text.empty() ? "/" : "/" + rel_text;
}

EntryInfo make_entry_info(const fs::path& root_real, const fs::directory_entry& entry) {
  EntryInfo info;
  info.name = entry.path().filename().string();
  info.web_path = web_path_from_native(root_real, entry.path());
  info.hidden = !info.name.empty() && info.name[0] == '.';

  std::error_code local_ec;
  const fs::file_status st = entry.symlink_status(local_ec);
  if (!local_ec && fs::is_directory(st)) {
    info.kind = "directory";
  }
  else if (!local_ec && fs::is_symlink(st)) {
    std::error_code target_ec;
    const fs::file_status target_st = fs::status(entry.path(), target_ec);
    if (!target_ec && fs::is_directory(target_st))
      info.kind = "directory";
    else
      info.kind = "file";
  }
  else {
    info.kind = "file";
  }

  std::error_code size_ec;
  if (info.kind == "file") {
    info.size = entry.file_size(size_ec);
    info.size_known = !size_ec;
  }

  std::error_code time_ec;
  const auto write_time = entry.last_write_time(time_ec);
  if (!time_ec)
    info.mtime_ms = file_time_to_unix_ms(write_time);

  info.extension = info.kind == "file" ? extension_of(info.name) : "";
  return info;
}

std::optional<EntryInfo> make_entry_info_for_path(const fs::path& root_real,
  const fs::path& path) {
  std::error_code ec;
  fs::directory_entry entry(path, ec);
  if (ec)
    return std::nullopt;
  return make_entry_info(root_real, entry);
}

std::optional<std::vector<EntryInfo>> list_directory(const AppConfig& cfg,
  const std::string& web_path, std::string& error) {
  const auto resolved = resolve_web_path(cfg, web_path);
  if (!resolved) {
    error = "Invalid path";
    return std::nullopt;
  }

  std::error_code ec;
  if (!fs::exists(*resolved, ec)) {
    error = "Directory does not exist";
    return std::nullopt;
  }
  if (!fs::is_directory(*resolved, ec)) {
    error = "Path is not a directory";
    return std::nullopt;
  }

  std::vector<EntryInfo> entries;
  const fs::path root_real = canonical_or_absolute(cfg.root);
  for (fs::directory_iterator it(*resolved, ec), end; !ec && it != end;
       it.increment(ec)) {
    entries.push_back(make_entry_info(root_real, *it));
  }

  if (ec) {
    error = "Failed to read directory: " + ec.message();
    return std::nullopt;
  }

  return entries;
}

bool entry_matches_xattr_query(const AppConfig& cfg,
  const std::vector<XattrInfo>& attrs,
  const std::vector<MetadataQueryTerm>& terms) {
  if (terms.empty())
    return false;

  for (const auto& term : terms) {
    bool matched = false;
    for (const auto& attr : attrs) {
      if (!metadata_attr_matches_query_key(cfg, attr.name, term.key_lower))
        continue;
      if (to_lower_copy(attr.value) != term.value_lower)
        continue;
      matched = true;
      break;
    }
    if (!matched)
      return false;
  }
  return true;
}

std::optional<std::vector<SearchHit>> recursive_search_by_xattr(
  const AppConfig& cfg, const std::string& web_path,
  const std::vector<MetadataQueryTerm>& query_terms,
  bool show_hidden, std::string& error) {
  const auto resolved = resolve_web_path(cfg, web_path);
  if (!resolved) {
    error = "Invalid path";
    return std::nullopt;
  }

  std::error_code ec;
  if (!fs::exists(*resolved, ec)) {
    error = "Directory does not exist";
    return std::nullopt;
  }
  if (!fs::is_directory(*resolved, ec)) {
    error = "Path is not a directory";
    return std::nullopt;
  }

  const fs::path root_real = canonical_or_absolute(cfg.root);
  std::vector<SearchHit> hits;
  std::deque<fs::path> pending;
  pending.push_back(*resolved);

  while (!pending.empty()) {
    const fs::path current = pending.back();
    pending.pop_back();

    const auto info_opt = make_entry_info_for_path(root_real, current);
    if (!info_opt)
      continue;

    const EntryInfo info = *info_opt;
    const bool is_root = current == *resolved;
    if (!show_hidden && info.hidden && !is_root)
      continue;

    if (info.kind == "directory") {
      std::error_code iter_ec;
      fs::directory_iterator it(current,
        fs::directory_options::skip_permission_denied, iter_ec);
      fs::directory_iterator end;
      for (; !iter_ec && it != end; it.increment(iter_ec)) {
        pending.push_back(it->path());
      }
      if (iter_ec) {
        error = "Metadata search failed: " + iter_ec.message();
        return std::nullopt;
      }
    }

    if (!is_grib2_entry(cfg, info))
      continue;

    std::string xattr_error;
    const auto attrs = read_extended_attributes(current, xattr_error);
    if (!entry_matches_xattr_query(cfg, attrs, query_terms))
      continue;

    SearchHit hit;
    hit.entry = info;
    hit.attributes = filter_exposed_metadata_attrs(cfg, attrs);
    hits.push_back(std::move(hit));
  }
  return hits;
}

bool is_mostly_text(const std::string& data) {
  if (data.empty())
    return true;
  size_t suspicious = 0;
  for (unsigned char c : data) {
    if (c == 0)
      return false;
    if ((c < 0x20 && c != '\n' && c != '\r' && c != '\t') || c == 0x7f)
      ++suspicious;
  }
  return suspicious * 20 < data.size();
}

std::string bytes_to_hex_preview(const std::string& data, size_t limit = 64) {
  static const char* hex = "0123456789abcdef";
  const size_t n = std::min(limit, data.size());
  std::string out;
  out.reserve(n * 3 + 16);
  for (size_t i = 0; i < n; ++i) {
    if (i)
      out.push_back(' ');
    const unsigned char c = static_cast<unsigned char>(data[i]);
    out.push_back(hex[c >> 4]);
    out.push_back(hex[c & 0x0f]);
  }
  if (data.size() > limit)
    out += " ...";
  return out;
}

std::vector<XattrInfo> read_extended_attributes(const fs::path& path,
  std::string& error) {
  std::vector<XattrInfo> attrs;
  errno = 0;
  const std::string native = path.string();
#ifdef __APPLE__
  const ssize_t names_size = ::listxattr(native.c_str(), nullptr, 0, 0);
#else
  const ssize_t names_size = ::listxattr(native.c_str(), nullptr, 0);
#endif
  if (names_size < 0) {
    if (errno == ENOTSUP || errno == EOPNOTSUPP) {
      error = "Extended attributes are not supported on this path.";
      return attrs;
    }
    error = std::string("Failed to list extended attributes: ") +
      std::strerror(errno);
    return attrs;
  }
  if (names_size == 0)
    return attrs;

  std::string names(static_cast<size_t>(names_size), '\0');
#ifdef __APPLE__
  const ssize_t filled = ::listxattr(native.c_str(), names.data(), names.size(), 0);
#else
  const ssize_t filled = ::listxattr(native.c_str(), names.data(), names.size());
#endif
  if (filled < 0) {
    error = std::string("Failed to read attribute names: ") +
      std::strerror(errno);
    return attrs;
  }

  for (size_t pos = 0; pos < static_cast<size_t>(filled);) {
    const char* raw_name = names.data() + pos;
    const size_t len = std::strlen(raw_name);
    pos += len + 1;
    if (len == 0)
      continue;

    XattrInfo info;
    info.name.assign(raw_name, len);

    errno = 0;
#ifdef __APPLE__
    const ssize_t value_size = ::getxattr(native.c_str(), info.name.c_str(),
      nullptr, 0, 0, 0);
#else
    const ssize_t value_size = ::getxattr(native.c_str(), info.name.c_str(),
      nullptr, 0);
#endif
    if (value_size < 0) {
      info.value = std::string("<error: ") + std::strerror(errno) + ">";
      attrs.push_back(std::move(info));
      continue;
    }
    if (value_size == 0) {
      info.value = "";
      attrs.push_back(std::move(info));
      continue;
    }

    std::string value(static_cast<size_t>(value_size), '\0');
#ifdef __APPLE__
    const ssize_t got = ::getxattr(native.c_str(), info.name.c_str(),
      value.data(), value.size(), 0, 0);
#else
    const ssize_t got = ::getxattr(native.c_str(), info.name.c_str(),
      value.data(), value.size());
#endif
    if (got < 0) {
      info.value = std::string("<error: ") + std::strerror(errno) + ">";
      attrs.push_back(std::move(info));
      continue;
    }
    value.resize(static_cast<size_t>(got));
    info.binary = !is_mostly_text(value);
    info.value = info.binary ? bytes_to_hex_preview(value) : value;
    attrs.push_back(std::move(info));
  }

  std::sort(attrs.begin(), attrs.end(),
    [](const XattrInfo& a, const XattrInfo& b) {
      const bool a_grib = starts_with(a.name, "user.grib.");
      const bool b_grib = starts_with(b.name, "user.grib.");
      if (a_grib != b_grib)
        return a_grib > b_grib;
      return a.name < b.name;
    });
  return attrs;
}

std::string mime_type_for_extension(const std::string& ext) {
  if (ext == "html" || ext == "htm")
    return "text/html; charset=utf-8";
  if (ext == "txt" || ext == "log" || ext == "cfg" || ext == "conf" ||
    ext == "md" || ext == "csv") {
    return "text/plain; charset=utf-8";
  }
  if (ext == "json" || ext == "jsonl")
    return "application/json; charset=utf-8";
  if (ext == "xml")
    return "application/xml; charset=utf-8";
  return "application/octet-stream";
}

bool send_all(int fd, const void* data, size_t len) {
  const char* ptr = static_cast<const char*>(data);
  size_t sent = 0;
  while (sent < len) {
    const ssize_t n = ::send(fd, ptr + sent, len - sent, 0);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      return false;
    sent += static_cast<size_t>(n);
  }
  return true;
}

bool send_all(int fd, const std::string& data) {
  return send_all(fd, data.data(), data.size());
}

void send_response(int fd, int status, const std::string& reason,
  const std::string& content_type, const std::string& body,
  const std::vector<std::pair<std::string, std::string>>& extra_headers = {},
  bool send_body = true) {
  std::ostringstream os;
  os << "HTTP/1.1 " << status << " " << reason << "\r\n";
  os << "Date: " << http_date_now() << "\r\n";
  os << "Server: iotdb-fs-web\r\n";
  os << "Connection: close\r\n";
  os << "Cache-Control: no-store\r\n";
  os << "Content-Type: " << content_type << "\r\n";
  os << "Content-Length: " << body.size() << "\r\n";
  for (const auto& header : extra_headers)
    os << header.first << ": " << header.second << "\r\n";
  os << "\r\n";
  const std::string head = os.str();
  send_all(fd, head);
  if (send_body)
    send_all(fd, body);
}

void send_json_error(int fd, int status, const std::string& reason,
  const std::string& message, bool send_body = true) {
  std::string body = std::string("{\"ok\":false,\"error\":\"") +
    json_escape(message) + "\"}";
  send_response(fd, status, reason, "application/json; charset=utf-8", body, {},
    send_body);
}

bool read_http_request(int fd, const AppConfig& cfg, HttpRequest& req,
  int& failure_status) {
  failure_status = 400;
  req = HttpRequest{};
  std::string raw;
  raw.reserve(8192);
  char buf[2048];
  while (raw.find("\r\n\r\n") == std::string::npos &&
    raw.size() <= cfg.max_header_bytes) {
    const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      failure_status = 408;
      return false;
    }
    if (n <= 0)
      return false;
    raw.append(buf, static_cast<size_t>(n));
  }
  const size_t header_end = raw.find("\r\n\r\n");
  if (header_end == std::string::npos || header_end > cfg.max_header_bytes) {
    failure_status = 431;
    return false;
  }
  const size_t line_end = raw.find("\r\n");
  if (line_end == std::string::npos)
    return false;
  std::istringstream iss(raw.substr(0, line_end));
  std::string version;
  if (!(iss >> req.method >> req.target >> version))
    return false;

  size_t pos = line_end + 2;
  while (pos < header_end) {
    const size_t next = raw.find("\r\n", pos);
    if (next == std::string::npos || next > header_end)
      break;
    const std::string line = raw.substr(pos, next - pos);
    pos = next + 2;
    const size_t colon = line.find(':');
    if (colon == std::string::npos)
      continue;
    const std::string key = to_lower_copy(trim_copy(line.substr(0, colon)));
    const std::string value = trim_copy(line.substr(colon + 1));
    if (!key.empty()) {
      auto existing = req.headers.find(key);
      if (existing != req.headers.end() && existing->second != value) {
        failure_status = 400;
        return false;
      }
      req.headers[key] = value;
    }
  }

  if (req.headers.find("transfer-encoding") != req.headers.end()) {
    failure_status = 400;
    return false;
  }

  auto cl_it = req.headers.find("content-length");
  if (cl_it != req.headers.end()) {
    if (!parse_content_length(cl_it->second, req.content_length))
      return false;
    if (req.content_length > cfg.max_body_bytes) {
      failure_status = 413;
      return false;
    }
  }
  req.body_prefix = raw.substr(header_end + 4);

  const size_t q = req.target.find('?');
  req.route = req.target.substr(0, q);
  if (q != std::string::npos)
    req.query = parse_query_string(req.target.substr(q + 1));
  return true;
}

bool constant_time_equal(const std::string& lhs, const std::string& rhs) {
  if (lhs.size() != rhs.size())
    return false;
  unsigned char diff = 0;
  for (size_t i = 0; i < lhs.size(); ++i)
    diff |= static_cast<unsigned char>(lhs[i] ^ rhs[i]);
  return diff == 0;
}

bool request_is_authenticated(const HttpRequest& req, const AppConfig& cfg) {
  if (cfg.auth_token.empty())
    return !cfg.require_auth_all;
  auto it = req.headers.find("authorization");
  std::string supplied;
  if (it != req.headers.end() && starts_with(it->second, "Bearer "))
    supplied = trim_copy(it->second.substr(7));
  if (supplied.empty()) {
    it = req.headers.find("x-iotdb-fs-token");
    if (it != req.headers.end())
      supplied = trim_copy(it->second);
  }
  return constant_time_equal(supplied, cfg.auth_token);
}

bool request_origin_is_allowed(const HttpRequest& req, const AppConfig& cfg) {
  const auto origin_it = req.headers.find("origin");
  if (origin_it == req.headers.end() || origin_it->second.empty())
    return true;
  const auto host_it = req.headers.find("host");
  const std::string host = host_it == req.headers.end() ?
    cfg.host + ":" + std::to_string(cfg.port) : trim_copy(host_it->second);
  const std::string expected = "http://" + host;
  return origin_it->second == expected;
}

std::string make_json_listing(const AppConfig& cfg, const std::string& web_path,
  const std::vector<EntryInfo>& entries) {
  size_t dir_count = 0;
  size_t file_count = 0;
  for (const auto& entry : entries) {
    if (entry.kind == "directory")
      ++dir_count;
    else
      ++file_count;
  }

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"root\":\"" << json_escape(canonical_or_absolute(cfg.root).string()) << "\",";
  os << "\"current\":\"" << json_escape(web_path) << "\",";
  os << "\"parent\":";
  if (web_path == "/")
    os << "null,";
  else {
    const size_t slash = web_path.find_last_of('/');
    const std::string parent =
      (slash == std::string::npos || slash == 0) ? "/" : web_path.substr(0, slash);
    os << "\"" << json_escape(parent) << "\",";
  }
  os << "\"dirCount\":" << dir_count << ",";
  os << "\"fileCount\":" << file_count << ",";
  os << "\"entries\":[";
  for (size_t i = 0; i < entries.size(); ++i) {
    const auto& entry = entries[i];
    if (i)
      os << ",";
    os << "{";
    os << "\"name\":\"" << json_escape(entry.name) << "\",";
    os << "\"path\":\"" << json_escape(entry.web_path) << "\",";
    os << "\"kind\":\"" << json_escape(entry.kind) << "\",";
    os << "\"extension\":\"" << json_escape(entry.extension) << "\",";
    os << "\"size\":";
    if (entry.size_known)
      os << entry.size;
    else
      os << "null";
    os << ",";
    os << "\"mtimeMs\":" << entry.mtime_ms << ",";
    os << "\"hidden\":" << (entry.hidden ? "true" : "false");
    os << "}";
  }
  os << "]";
  os << "}";
  return os.str();
}

void append_json_attributes(std::ostringstream& os, const AppConfig& cfg,
  const std::vector<XattrInfo>& attrs, bool exposed_only = true) {
  std::vector<XattrInfo> filtered;
  const std::vector<XattrInfo>* items = &attrs;
  if (exposed_only) {
    filtered = filter_exposed_metadata_attrs(cfg, attrs);
    items = &filtered;
  }
  os << "\"attributes\":[";
  for (size_t i = 0; i < items->size(); ++i) {
    if (i)
      os << ",";
    os << "{";
    os << "\"name\":\"" << json_escape((*items)[i].name) << "\",";
    os << "\"value\":\"" << json_escape((*items)[i].value) << "\",";
    os << "\"binary\":" << ((*items)[i].binary ? "true" : "false");
    os << "}";
  }
  os << "]";
}

std::string make_json_search_results(const AppConfig& cfg,
  const std::string& web_path, const std::string& query,
  const std::vector<SearchHit>& hits) {
  size_t dir_count = 0;
  size_t file_count = 0;
  for (const auto& hit : hits) {
    if (hit.entry.kind == "directory")
      ++dir_count;
    else
      ++file_count;
  }

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"root\":\"" << json_escape(canonical_or_absolute(cfg.root).string()) << "\",";
  os << "\"current\":\"" << json_escape(web_path) << "\",";
  os << "\"query\":\"" << json_escape(query) << "\",";
  os << "\"dirCount\":" << dir_count << ",";
  os << "\"fileCount\":" << file_count << ",";
  os << "\"entries\":[";
  for (size_t i = 0; i < hits.size(); ++i) {
    const auto& hit = hits[i];
    if (i)
      os << ",";
    os << "{";
    os << "\"name\":\"" << json_escape(hit.entry.name) << "\",";
    os << "\"path\":\"" << json_escape(hit.entry.web_path) << "\",";
    os << "\"kind\":\"" << json_escape(hit.entry.kind) << "\",";
    os << "\"extension\":\"" << json_escape(hit.entry.extension) << "\",";
    os << "\"size\":";
    if (hit.entry.size_known)
      os << hit.entry.size;
    else
      os << "null";
    os << ",";
    os << "\"mtimeMs\":" << hit.entry.mtime_ms << ",";
    os << "\"hidden\":" << (hit.entry.hidden ? "true" : "false") << ",";
    append_json_attributes(os, cfg, hit.attributes);
    os << "}";
  }
  os << "]";
  os << "}";
  return os.str();
}

void handle_list_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  const auto it = req.query.find("path");
  const std::string web_path =
    normalize_web_path(it == req.query.end() ? "/" : it->second);
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  std::string error;
  auto entries = list_directory(cfg, web_path, error);
  if (!entries) {
    send_json_error(fd, 404, "Not Found", error, !head_only);
    return;
  }

  const std::string body = make_json_listing(cfg, web_path, *entries);
  send_response(fd, 200, "OK", "application/json; charset=utf-8", body, {},
    !head_only);
}

void handle_search_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  if (!cfg.grib_extension_enabled) {
    send_json_error(fd, 400, "Bad Request", "GRIB extension is disabled",
      !head_only);
    return;
  }

  const auto path_it = req.query.find("path");
  const std::string web_path =
    normalize_web_path(path_it == req.query.end() ? "/" : path_it->second);
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  const auto query_it = req.query.find("query");
  const std::string query =
    query_it == req.query.end() ? std::string() : trim_copy(query_it->second);
  if (query.empty()) {
    send_json_error(fd, 400, "Bad Request", "Search query is required",
      !head_only);
    return;
  }

  std::vector<MetadataQueryTerm> query_terms;
  std::string parse_error;
  if (!parse_metadata_query(query, query_terms, parse_error)) {
    send_json_error(fd, 400, "Bad Request", parse_error, !head_only);
    return;
  }
  const std::string normalized_query = format_metadata_query(query_terms);

  const auto hidden_it = req.query.find("hidden");
  const bool show_hidden =
    hidden_it != req.query.end() &&
    (hidden_it->second == "1" || to_lower_copy(hidden_it->second) == "true");

  std::string error;
  auto hits = recursive_search_by_xattr(cfg, web_path, query_terms, show_hidden,
    error);
  if (!hits) {
    send_json_error(fd, 404, "Not Found", error, !head_only);
    return;
  }

  const std::string body =
    make_json_search_results(cfg, web_path, normalized_query, *hits);
  send_response(fd, 200, "OK", "application/json; charset=utf-8", body, {},
    !head_only);
}

void handle_preview_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  const auto it = req.query.find("path");
  const std::string read_query = request_read_query(req);
  const std::string web_path =
    normalize_web_path(it == req.query.end() ? "" : it->second);
  if (!read_query.empty() && !cfg.grib_extension_enabled) {
    send_json_error(fd, 400, "Bad Request", "GRIB extension is disabled",
      !head_only);
    return;
  }
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  const auto resolved = resolve_web_path(cfg, web_path);
  if (!resolved) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  std::error_code ec;
  if (!fs::exists(*resolved, ec)) {
    send_json_error(fd, 404, "Not Found", "File does not exist", !head_only);
    return;
  }
  if (fs::is_directory(*resolved, ec)) {
    send_json_error(fd, 400, "Bad Request", "Directories cannot be previewed",
      !head_only);
    return;
  }

  const std::string target_native = build_target_native_path(*resolved, read_query);

  size_t limit = 32768;
  auto lim_it = req.query.find("limit");
  if (lim_it != req.query.end()) {
    limit = static_cast<size_t>(parse_int_or(lim_it->second, 32768));
    if (limit == 0)
      limit = 32768;
    if (limit > 262144)
      limit = 262144;
  }

  std::ifstream in(target_native, std::ios::binary);
  if (!in) {
    send_json_error(fd, 500, "Internal Server Error",
      "Failed to open file", !head_only);
    return;
  }

  std::string content(limit, '\0');
  in.read(&content[0], static_cast<std::streamsize>(limit));
  content.resize(static_cast<size_t>(in.gcount()));
  const bool binary = !is_mostly_text(content);

  struct stat st {};
  const bool have_stat = stat_native_path(target_native, st);
  const bool truncated = have_stat &&
    static_cast<std::uintmax_t>(st.st_size) > content.size();

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"path\":\"" << json_escape(web_path) << "\",";
  os << "\"readQuery\":\"" << json_escape(read_query) << "\",";
  os << "\"binary\":" << (binary ? "true" : "false") << ",";
  os << "\"truncated\":" << (truncated ? "true" : "false") << ",";
  if (binary) {
    os << "\"content\":\"" << json_escape(bytes_to_hex_preview(content, 256)) << "\",";
    os << "\"message\":\"Binary result shown as a hex preview. Use download to save the raw output.\"";
  }
  else {
    os << "\"content\":\"" << json_escape(content) << "\",";
    os << "\"message\":\"\"";
  }
  os << "}";
  send_response(fd, 200, "OK", "application/json; charset=utf-8", os.str(), {},
    !head_only);
}

void handle_viewer_info_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  const auto it = req.query.find("path");
  const std::string read_query = request_read_query(req);
  const std::string web_path =
    normalize_web_path(it == req.query.end() ? "" : it->second);
  if (!read_query.empty() && !cfg.grib_extension_enabled) {
    send_json_error(fd, 400, "Bad Request", "GRIB extension is disabled",
      !head_only);
    return;
  }
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  const auto resolved = resolve_web_path(cfg, web_path);
  if (!resolved) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  std::error_code ec;
  if (!fs::exists(*resolved, ec)) {
    send_json_error(fd, 404, "Not Found", "File does not exist", !head_only);
    return;
  }
  if (fs::is_directory(*resolved, ec)) {
    send_json_error(fd, 400, "Bad Request", "Directories cannot be viewed",
      !head_only);
    return;
  }

  const bool partial = !read_query.empty();
  const std::string target_native = build_target_native_path(*resolved, read_query);
  struct stat st {};
  if (!stat_native_path(target_native, st)) {
    send_json_error(fd, 500, "Internal Server Error", "Failed to stat file",
      !head_only);
    return;
  }

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"path\":\"" << json_escape(web_path) << "\",";
  os << "\"readQuery\":\"" << json_escape(read_query) << "\",";
  os << "\"partial\":" << (partial ? "true" : "false") << ",";
  os << "\"size\":" << static_cast<std::uintmax_t>(st.st_size) << ",";

  if (partial) {
    const std::string derived_path = web_path + "?" + read_query;
    os << "\"name\":\"" <<
      json_escape(query_result_filename(*resolved, read_query)) << "\",";
    os << "\"displayPath\":\"" << json_escape(derived_path) << "\",";
    os << "\"showMetadata\":false,";
    os << "\"attributes\":[]";
  }
  else {
    const bool show_metadata =
      should_expose_extended_metadata_for_path(cfg, *resolved);
    std::vector<XattrInfo> attrs;
    std::string metadata_error;
    if (show_metadata)
      attrs = read_extended_attributes(*resolved, metadata_error);
    os << "\"name\":\"" << json_escape(filename_for_download(*resolved)) << "\",";
    os << "\"displayPath\":\"" << json_escape(web_path) << "\",";
    os << "\"fileType\":\"" << json_escape(human_file_type_label(*resolved)) << "\",";
    os << "\"showMetadata\":" << (show_metadata ? "true" : "false") << ",";
    os << "\"metadataSupported\":"
      << ((show_metadata && metadata_error.empty()) ? "true" : "false") << ",";
    os << "\"metadataMessage\":\"" << json_escape(metadata_error) << "\",";
    append_json_attributes(os, cfg, attrs, true);
  }
  os << "}";
  send_response(fd, 200, "OK", "application/json; charset=utf-8", os.str(), {},
    !head_only);
}

void handle_metadata_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  const auto it = req.query.find("path");
  const std::string web_path =
    normalize_web_path(it == req.query.end() ? "" : it->second);
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  const auto resolved = resolve_web_path(cfg, web_path);
  if (!resolved) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  std::error_code ec;
  if (!fs::exists(*resolved, ec)) {
    send_json_error(fd, 404, "Not Found", "Path does not exist", !head_only);
    return;
  }

  if (!should_expose_extended_metadata_for_path(cfg, *resolved)) {
    send_response(fd, 200, "OK", "application/json; charset=utf-8",
      "{\"ok\":true,\"path\":\"" + json_escape(web_path) +
        "\",\"visible\":false,\"supported\":false,"
        "\"message\":\"\","
        "\"attributes\":[]}",
      {}, !head_only);
    return;
  }

  std::string error;
  const auto attrs = filter_exposed_metadata_attrs(cfg,
    read_extended_attributes(*resolved, error));

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"path\":\"" << json_escape(web_path) << "\",";
  os << "\"visible\":true,";
  os << "\"supported\":" << (error.empty() ? "true" : "false") << ",";
  os << "\"message\":\"" << json_escape(error) << "\",";
  os << "\"attributes\":[";
  for (size_t i = 0; i < attrs.size(); ++i) {
    if (i)
      os << ",";
    os << "{";
    os << "\"name\":\"" << json_escape(attrs[i].name) << "\",";
    os << "\"value\":\"" << json_escape(attrs[i].value) << "\",";
    os << "\"binary\":" << (attrs[i].binary ? "true" : "false");
    os << "}";
  }
  os << "]";
  os << "}";
  send_response(fd, 200, "OK", "application/json; charset=utf-8", os.str(), {},
    !head_only);
}

void handle_upload_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  if (head_only) {
    send_response(fd, 200, "OK", "application/json; charset=utf-8",
      "{\"ok\":true}", {}, false);
    return;
  }

  const auto path_it = req.query.find("path");
  const std::string web_path =
    normalize_web_path(path_it == req.query.end() ? "/" : path_it->second);
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid directory path");
    return;
  }

  const auto name_it = req.query.find("name");
  const std::string upload_name = sanitize_upload_name(
    name_it == req.query.end() ? std::string() : name_it->second);
  if (upload_name.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid upload file name");
    return;
  }

  const auto resolved_dir = resolve_web_path(cfg, web_path);
  if (!resolved_dir) {
    send_json_error(fd, 400, "Bad Request", "Invalid directory path");
    return;
  }

  std::error_code ec;
  if (!fs::exists(*resolved_dir, ec) || !fs::is_directory(*resolved_dir, ec)) {
    send_json_error(fd, 404, "Not Found", "Target directory does not exist");
    return;
  }

  const fs::path target = *resolved_dir / upload_name;
  struct stat target_stat {};
  if (::lstat(target.c_str(), &target_stat) == 0 &&
    S_ISLNK(target_stat.st_mode)) {
    send_json_error(fd, 409, "Conflict", "Refusing to replace a symbolic link");
    return;
  }
  if (req.headers.find("content-length") == req.headers.end()) {
    send_json_error(fd, 411, "Length Required", "Upload requires Content-Length");
    return;
  }
  if (req.content_length > cfg.max_upload_bytes) {
    send_json_error(fd, 413, "Payload Too Large", "Upload exceeds configured size limit");
    return;
  }
  std::uintmax_t bytes_written = 0;
  std::string error;
  if (!stream_request_body_to_file(fd, req, target, bytes_written, error)) {
    send_json_error(fd, 500, "Internal Server Error", error);
    return;
  }

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"path\":\"" << json_escape(join_web_path(web_path, upload_name)) << "\",";
  os << "\"name\":\"" << json_escape(upload_name) << "\",";
  os << "\"bytes\":" << bytes_written;
  os << "}";
  send_response(fd, 200, "OK", "application/json; charset=utf-8", os.str());
}

void handle_mkdir_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  if (head_only) {
    send_response(fd, 200, "OK", "application/json; charset=utf-8",
      "{\"ok\":true}", {}, false);
    return;
  }

  const auto path_it = req.query.find("path");
  const std::string web_path =
    normalize_web_path(path_it == req.query.end() ? "/" : path_it->second);
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid directory path");
    return;
  }

  const auto name_it = req.query.find("name");
  const std::string folder_name = sanitize_upload_name(
    name_it == req.query.end() ? std::string() : name_it->second);
  if (folder_name.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid folder name");
    return;
  }

  const auto resolved_dir = resolve_web_path(cfg, web_path);
  if (!resolved_dir) {
    send_json_error(fd, 400, "Bad Request", "Invalid directory path");
    return;
  }

  std::error_code ec;
  if (!fs::exists(*resolved_dir, ec) || !fs::is_directory(*resolved_dir, ec)) {
    send_json_error(fd, 404, "Not Found", "Target directory does not exist");
    return;
  }

  const fs::path target = *resolved_dir / folder_name;
  if (fs::exists(target, ec)) {
    send_json_error(fd, 409, "Conflict", "A file or folder with this name already exists");
    return;
  }

  if (!fs::create_directory(target, ec)) {
    const std::string message = ec
      ? "Failed to create folder: " + ec.message()
      : "Failed to create folder";
    send_json_error(fd, 500, "Internal Server Error", message);
    return;
  }

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"path\":\"" << json_escape(join_web_path(web_path, folder_name)) << "\",";
  os << "\"name\":\"" << json_escape(folder_name) << "\"";
  os << "}";
  send_response(fd, 200, "OK", "application/json; charset=utf-8", os.str());
}

std::string filename_for_download(const fs::path& p) {
  const std::string name = p.filename().string();
  return name.empty() ? "download.bin" : name;
}

std::string sanitize_upload_name(std::string name) {
  if (name.empty())
    return {};
  std::replace(name.begin(), name.end(), '\\', '/');
  const size_t slash = name.find_last_of('/');
  if (slash != std::string::npos)
    name = name.substr(slash + 1);
  if (name == "." || name == "..")
    return {};
  std::string out;
  out.reserve(name.size());
  for (char ch : name) {
    const unsigned char u = static_cast<unsigned char>(ch);
    if (u < 0x20 || ch == '/' || ch == '\0')
      continue;
    out.push_back(ch);
  }
  return out;
}

std::string request_read_query(const HttpRequest& req) {
  const auto it = req.query.find("read_query");
  return it == req.query.end() ? std::string() : it->second;
}

std::string build_target_native_path(const fs::path& resolved,
  const std::string& read_query) {
  std::string native = resolved.string();
  if (!read_query.empty())
    native += "?" + read_query;
  return native;
}

bool stat_native_path(const std::string& native, struct stat& st) {
  return ::stat(native.c_str(), &st) == 0;
}

bool stream_request_body_to_file(int fd, const HttpRequest& req,
  const fs::path& target, std::uintmax_t& bytes_written, std::string& error) {
  const fs::path parent = target.parent_path();
  std::string template_path = (parent / ".iotdb-upload-XXXXXX").string();
  std::vector<char> temp_name(template_path.begin(), template_path.end());
  temp_name.push_back('\0');
  int temp_fd = ::mkstemp(temp_name.data());
  if (temp_fd < 0) {
    error = std::string("Failed to create temporary upload file: ") +
      std::strerror(errno);
    return false;
  }
  const fs::path temp_path(temp_name.data());
  auto cleanup = [&]() {
    if (temp_fd >= 0) {
      ::close(temp_fd);
      temp_fd = -1;
    }
    std::error_code ignored;
    fs::remove(temp_path, ignored);
  };
  ::fchmod(temp_fd, 0600);

  auto write_bytes = [&](const char* data, size_t len) -> bool {
    size_t written = 0;
    while (written < len) {
      const ssize_t n = ::write(temp_fd, data + written, len - written);
      if (n < 0 && errno == EINTR)
        continue;
      if (n <= 0)
        return false;
      written += static_cast<size_t>(n);
    }
    return true;
  };

  bytes_written = 0;
  size_t remaining = req.content_length;

  if (!req.body_prefix.empty()) {
    const size_t prefix_size = std::min(remaining, req.body_prefix.size());
    if (!write_bytes(req.body_prefix.data(), prefix_size)) {
      error = "Failed to write file";
      cleanup();
      return false;
    }
    bytes_written += prefix_size;
    remaining -= prefix_size;
  }

  char buf[65536];
  while (remaining > 0) {
    const size_t want = std::min(remaining, sizeof(buf));
    const ssize_t n = ::recv(fd, buf, want, 0);
    if (n <= 0) {
      error = "Upload stream ended unexpectedly";
      cleanup();
      return false;
    }
    if (!write_bytes(buf, static_cast<size_t>(n))) {
      error = "Failed to write file";
      cleanup();
      return false;
    }
    bytes_written += static_cast<size_t>(n);
    remaining -= static_cast<size_t>(n);
  }
  if (::fsync(temp_fd) != 0) {
    error = std::string("Failed to sync uploaded file: ") + std::strerror(errno);
    cleanup();
    return false;
  }
  if (::close(temp_fd) != 0) {
    error = std::string("Failed to close uploaded file: ") + std::strerror(errno);
    temp_fd = -1;
    std::error_code ignored;
    fs::remove(temp_path, ignored);
    return false;
  }
  temp_fd = -1;
  std::error_code rename_ec;
  fs::rename(temp_path, target, rename_ec);
  if (rename_ec) {
    error = "Failed to atomically publish uploaded file: " + rename_ec.message();
    std::error_code ignored;
    fs::remove(temp_path, ignored);
    return false;
  }
#ifdef O_DIRECTORY
  int dir_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY);
#else
  int dir_fd = ::open(parent.c_str(), O_RDONLY);
#endif
  if (dir_fd >= 0) {
    // Directory fsync is unavailable on some filesystems. The file itself is
    // already durable; do not report failure after the atomic rename.
    (void)::fsync(dir_fd);
    ::close(dir_fd);
  }
  return true;
}

std::string query_result_filename(const fs::path& p,
  const std::string& read_query) {
  std::string name = filename_for_download(p);
  if (read_query.empty())
    return name;
  std::string suffix = read_query;
  for (char& ch : suffix) {
    if (!(std::isalnum(static_cast<unsigned char>(ch)) || ch == '-' || ch == '_'))
      ch = '_';
  }
  if (suffix.size() > 48)
    suffix.resize(48);
  return name + "__" + suffix + ".bin";
}

std::string header_safe_filename(std::string name) {
  if (name.empty())
    return "download.bin";
  for (char& ch : name) {
    if (ch == '"' || ch == '\\' || ch == '\r' || ch == '\n')
      ch = '_';
  }
  return name;
}

std::vector<BatchDownloadRequestItem> collect_batch_download_items(
  const HttpRequest& req) {
  std::map<int, BatchDownloadRequestItem> ordered;
  for (const auto& item : req.query) {
    if (!starts_with(item.first, "path") && !starts_with(item.first, "read_query"))
      continue;

    int index = 0;
    const bool is_path = starts_with(item.first, "path");
    const std::string suffix = item.first.substr(is_path ? 4 : 10);
    if (!suffix.empty()) {
      if (!std::all_of(suffix.begin(), suffix.end(),
          [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
        continue;
      }
      index = parse_int_or(suffix, 0);
    }
    BatchDownloadRequestItem& slot = ordered[index];
    slot.index = index;
    if (is_path)
      slot.web_path = item.second;
    else
      slot.read_query = item.second;
  }

  std::vector<BatchDownloadRequestItem> items;
  items.reserve(ordered.size());
  for (auto& [index, item] : ordered) {
    const std::string normalized = normalize_web_path(item.web_path);
    if (normalized.empty())
      continue;
    item.web_path = normalized;
    items.push_back(std::move(item));
  }
  return items;
}

std::vector<std::string> collect_batch_delete_paths(const HttpRequest& req) {
  std::map<int, std::string> ordered;
  for (const auto& item : req.query) {
    if (!starts_with(item.first, "path"))
      continue;

    int index = 0;
    const std::string suffix = item.first.substr(4);
    if (!suffix.empty()) {
      if (!std::all_of(suffix.begin(), suffix.end(),
          [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
        continue;
      }
      index = parse_int_or(suffix, 0);
    }
    ordered[index] = item.second;
  }

  std::vector<std::string> paths;
  std::set<std::string> seen;
  paths.reserve(ordered.size());
  for (const auto& [index, raw_path] : ordered) {
    (void)index;
    const std::string normalized = normalize_web_path(raw_path);
    if (normalized.empty())
      continue;
    if (seen.insert(normalized).second)
      paths.push_back(normalized);
  }
  return paths;
}

std::string tar_entry_name_for_web_path(const std::string& web_path) {
  if (web_path.empty() || web_path == "/")
    return "download.bin";
  return web_path[0] == '/' ? web_path.substr(1) : web_path;
}

std::string tar_entry_name_for_download(const std::string& web_path,
  const fs::path& resolved, const std::string& read_query) {
  if (read_query.empty())
    return tar_entry_name_for_web_path(web_path);

  std::string parent;
  if (web_path != "/") {
    const size_t slash = web_path.find_last_of('/');
    if (slash != std::string::npos && slash > 0)
      parent = web_path.substr(1, slash - 1);
  }
  const std::string file_name = query_result_filename(resolved, read_query);
  if (parent.empty())
    return file_name;
  return parent + "/" + file_name;
}

bool split_tar_entry_name(const std::string& entry_name, std::string& name,
  std::string& prefix) {
  if (entry_name.size() <= 100) {
    name = entry_name;
    prefix.clear();
    return true;
  }

  size_t slash = entry_name.size();
  while (slash > 0) {
    slash = entry_name.rfind('/', slash - 1);
    if (slash == std::string::npos)
      break;
    const size_t prefix_len = slash;
    const size_t name_len = entry_name.size() - slash - 1;
    if (prefix_len <= 155 && name_len <= 100) {
      prefix = entry_name.substr(0, slash);
      name = entry_name.substr(slash + 1);
      return true;
    }
  }
  return false;
}

void write_tar_octal(char* field, size_t len, std::uintmax_t value) {
  if (!field || len == 0)
    return;
  std::ostringstream os;
  os << std::oct << value;
  std::string digits = os.str();
  if (digits.size() + 1 > len) {
    digits = std::string(len > 1 ? len - 1 : 0, '7');
  }
  const size_t pad = len > digits.size() + 1 ? len - digits.size() - 1 : 0;
  std::memset(field, '0', len);
  std::memcpy(field + pad, digits.data(), std::min(digits.size(), len));
  field[len - 1] = '\0';
}

bool make_tar_header(const std::string& entry_name, std::uintmax_t size,
  std::uintmax_t mtime, std::array<char, 512>& header) {
  header.fill('\0');

  std::string name;
  std::string prefix;
  if (!split_tar_entry_name(entry_name, name, prefix))
    return false;

  std::memcpy(header.data(), name.data(), name.size());
  if (!prefix.empty())
    std::memcpy(header.data() + 345, prefix.data(), prefix.size());

  write_tar_octal(header.data() + 100, 8, 0644);
  write_tar_octal(header.data() + 108, 8, 0);
  write_tar_octal(header.data() + 116, 8, 0);
  write_tar_octal(header.data() + 124, 12, size);
  write_tar_octal(header.data() + 136, 12, mtime);
  std::memset(header.data() + 148, ' ', 8);
  header[156] = '0';
  std::memcpy(header.data() + 257, "ustar", 5);
  std::memcpy(header.data() + 263, "00", 2);

  unsigned int checksum = 0;
  for (unsigned char ch : header)
    checksum += ch;

  std::ostringstream os;
  os << std::oct << checksum;
  std::string digits = os.str();
  if (digits.size() > 6)
    digits = digits.substr(digits.size() - 6);
  const std::string padded(6 - digits.size(), '0');
  std::memcpy(header.data() + 148, padded.data(), padded.size());
  std::memcpy(header.data() + 148 + padded.size(), digits.data(), digits.size());
  header[154] = '\0';
  header[155] = ' ';
  return true;
}

void handle_batch_download_api(int fd, const AppConfig& cfg,
  const HttpRequest& req, bool head_only) {
  const auto request_items = collect_batch_download_items(req);
  if (request_items.empty()) {
    send_json_error(fd, 400, "Bad Request", "At least one file path is required",
      !head_only);
    return;
  }

  struct BatchItem {
    std::string archive_name;
    std::string target_native;
    fs::path resolved;
    std::uintmax_t size{ 0 };
    std::uintmax_t mtime{ 0 };
  };

  std::vector<BatchItem> items;
  items.reserve(request_items.size());
  std::uintmax_t total_size = 1024;

  for (const auto& request_item : request_items) {
    if (!request_item.read_query.empty() && !cfg.grib_extension_enabled) {
      send_json_error(fd, 400, "Bad Request", "GRIB extension is disabled",
        !head_only);
      return;
    }
    const std::string& web_path = request_item.web_path;
    const auto resolved = resolve_web_path(cfg, web_path);
    if (!resolved) {
      send_json_error(fd, 400, "Bad Request", "Invalid path: " + web_path,
        !head_only);
      return;
    }

    std::error_code ec;
    if (!fs::exists(*resolved, ec)) {
      send_json_error(fd, 404, "Not Found", "File does not exist: " + web_path,
        !head_only);
      return;
    }
    if (fs::is_directory(*resolved, ec)) {
      send_json_error(fd, 400, "Bad Request",
        "Directories cannot be batch-downloaded: " + web_path, !head_only);
      return;
    }

    const std::string target_native =
      build_target_native_path(*resolved, request_item.read_query);
    struct stat st {};
    if (!stat_native_path(target_native, st)) {
      send_json_error(fd, 500, "Internal Server Error",
        "Failed to stat file: " + web_path, !head_only);
      return;
    }

    BatchItem item;
    item.archive_name =
      tar_entry_name_for_download(web_path, *resolved, request_item.read_query);
    item.target_native = target_native;
    item.resolved = *resolved;
    item.size = static_cast<std::uintmax_t>(st.st_size);
    item.mtime = static_cast<std::uintmax_t>(st.st_mtime);

    std::array<char, 512> header {};
    if (!make_tar_header(item.archive_name, item.size, item.mtime, header)) {
      send_json_error(fd, 400, "Bad Request",
        "Path is too long for tar archive: " + web_path, !head_only);
      return;
    }

    const std::uintmax_t padding = (512 - (item.size % 512)) % 512;
    total_size += 512 + item.size + padding;
    items.push_back(std::move(item));
  }

  const std::string filename = header_safe_filename(
    "iotdb-fs-selection-" + compact_timestamp_now() + ".tar");
  std::ostringstream headers;
  headers << "HTTP/1.1 200 OK\r\n";
  headers << "Date: " << http_date_now() << "\r\n";
  headers << "Server: iotdb-fs-web\r\n";
  headers << "Connection: close\r\n";
  headers << "Cache-Control: no-store\r\n";
  headers << "Content-Type: application/x-tar\r\n";
  headers << "Content-Disposition: attachment; filename=\"" <<
    filename << "\"\r\n";
  headers << "Content-Length: " << total_size << "\r\n";
  headers << "\r\n";

  if (!send_all(fd, headers.str()))
    return;
  if (head_only)
    return;

  const std::array<char, 512> zero_block {};
  char buf[65536];
  for (const auto& item : items) {
    std::array<char, 512> header {};
    if (!make_tar_header(item.archive_name, item.size, item.mtime, header))
      return;
    if (!send_all(fd, header.data(), header.size()))
      return;

    std::ifstream in(item.target_native, std::ios::binary);
    if (!in)
      return;

    while (in) {
      in.read(buf, sizeof(buf));
      const std::streamsize n = in.gcount();
      if (n <= 0)
        break;
      if (!send_all(fd, buf, static_cast<size_t>(n)))
        return;
    }

    const std::uintmax_t padding = (512 - (item.size % 512)) % 512;
    if (padding && !send_all(fd, zero_block.data(),
        static_cast<size_t>(padding))) {
      return;
    }
  }

  send_all(fd, zero_block.data(), zero_block.size());
  send_all(fd, zero_block.data(), zero_block.size());
}

void handle_download_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  const auto it = req.query.find("path");
  const std::string read_query = request_read_query(req);
  const std::string web_path =
    normalize_web_path(it == req.query.end() ? "" : it->second);
  if (!read_query.empty() && !cfg.grib_extension_enabled) {
    send_json_error(fd, 400, "Bad Request", "GRIB extension is disabled",
      !head_only);
    return;
  }
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  const auto resolved = resolve_web_path(cfg, web_path);
  if (!resolved) {
    send_json_error(fd, 400, "Bad Request", "Invalid path", !head_only);
    return;
  }

  std::error_code ec;
  if (!fs::exists(*resolved, ec)) {
    send_json_error(fd, 404, "Not Found", "File does not exist", !head_only);
    return;
  }
  if (fs::is_directory(*resolved, ec)) {
    send_json_error(fd, 400, "Bad Request", "Directories cannot be downloaded",
      !head_only);
    return;
  }

  const std::string target_native = build_target_native_path(*resolved, read_query);

  std::ifstream in(target_native, std::ios::binary);
  if (!in) {
    send_json_error(fd, 500, "Internal Server Error", "Failed to open file",
      !head_only);
    return;
  }

  struct stat st {};
  const bool have_stat = stat_native_path(target_native, st);
  const std::string ext = extension_of(resolved->filename().string());
  const std::string mime = mime_type_for_extension(ext);
  const std::string filename = header_safe_filename(
    query_result_filename(*resolved, read_query));

  std::ostringstream headers;
  headers << "HTTP/1.1 200 OK\r\n";
  headers << "Date: " << http_date_now() << "\r\n";
  headers << "Server: iotdb-fs-web\r\n";
  headers << "Connection: close\r\n";
  headers << "Cache-Control: no-store\r\n";
  headers << "Content-Type: " << mime << "\r\n";
  headers << "Content-Disposition: attachment; filename=\"" <<
    filename << "\"\r\n";
  if (have_stat)
    headers << "Content-Length: " << st.st_size << "\r\n";
  headers << "\r\n";

  if (!send_all(fd, headers.str()))
    return;
  if (head_only)
    return;

  char buf[65536];
  while (in) {
    in.read(buf, sizeof(buf));
    const std::streamsize n = in.gcount();
    if (n <= 0)
      break;
    if (!send_all(fd, buf, static_cast<size_t>(n)))
      break;
  }
}

void handle_delete_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  if (head_only) {
    send_response(fd, 200, "OK", "application/json; charset=utf-8",
      "{\"ok\":true}", {}, false);
    return;
  }

  const auto it = req.query.find("path");
  const std::string web_path =
    normalize_web_path(it == req.query.end() ? "" : it->second);
  if (web_path.empty()) {
    send_json_error(fd, 400, "Bad Request", "Invalid path");
    return;
  }

  const auto resolved = resolve_web_path(cfg, web_path);
  if (!resolved) {
    send_json_error(fd, 400, "Bad Request", "Invalid path");
    return;
  }
  if (web_path == "/") {
    send_json_error(fd, 400, "Bad Request", "The root directory cannot be deleted");
    return;
  }

  std::error_code ec;
  if (!fs::exists(*resolved, ec)) {
    send_json_error(fd, 404, "Not Found", "Path does not exist");
    return;
  }
  const bool is_directory = fs::is_directory(*resolved, ec);
  const std::uintmax_t removed = fs::remove_all(*resolved, ec);
  if (removed == 0 || ec) {
    const std::string message = ec
      ? "Failed to delete item: " + ec.message()
      : "Item could not be deleted";
    send_json_error(fd, 500, "Internal Server Error", message);
    return;
  }

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"path\":\"" << json_escape(web_path) << "\",";
  os << "\"kind\":\"" << (is_directory ? "directory" : "file") << "\",";
  os << "\"removedCount\":" << removed;
  os << "}";
  send_response(fd, 200, "OK", "application/json; charset=utf-8", os.str());
}

void handle_batch_delete_api(int fd, const AppConfig& cfg, const HttpRequest& req,
  bool head_only) {
  if (head_only) {
    send_response(fd, 200, "OK", "application/json; charset=utf-8",
      "{\"ok\":true}", {}, false);
    return;
  }

  const auto request_paths = collect_batch_delete_paths(req);
  if (request_paths.empty()) {
    send_json_error(fd, 400, "Bad Request", "At least one file path is required");
    return;
  }

  struct DeleteItem {
    std::string web_path;
    fs::path resolved;
  };

  std::vector<DeleteItem> items;
  items.reserve(request_paths.size());
  for (const auto& web_path : request_paths) {
    if (web_path == "/") {
      send_json_error(fd, 400, "Bad Request", "The root directory cannot be deleted");
      return;
    }
    const auto resolved = resolve_web_path(cfg, web_path);
    if (!resolved) {
      send_json_error(fd, 400, "Bad Request", "Invalid path: " + web_path);
      return;
    }

    std::error_code ec;
    if (!fs::exists(*resolved, ec)) {
      send_json_error(fd, 404, "Not Found", "Path does not exist: " + web_path);
      return;
    }

    items.push_back(DeleteItem{ web_path, *resolved });
  }

  std::uintmax_t removed_count = 0;
  for (const auto& item : items) {
    std::error_code ec;
    const std::uintmax_t removed = fs::remove_all(item.resolved, ec);
    if (removed == 0 || ec) {
      const std::string message = ec
        ? "Failed to delete item: " + item.web_path + ": " + ec.message()
        : "Item could not be deleted: " + item.web_path;
      send_json_error(fd, 500, "Internal Server Error", message);
      return;
    }
    removed_count += removed;
  }

  std::ostringstream os;
  os << "{";
  os << "\"ok\":true,";
  os << "\"deletedCount\":" << items.size() << ",";
  os << "\"removedCount\":" << removed_count << ",";
  os << "\"deleted\":[";
  for (size_t i = 0; i < items.size(); ++i) {
    if (i)
      os << ",";
    os << "\""<< json_escape(items[i].web_path) << "\"";
  }
  os << "]";
  os << "}";
  send_response(fd, 200, "OK", "application/json; charset=utf-8", os.str());
}

const char* kIndexHtml = R"HTML(
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>IoTDB-FS Explorer</title>
  <style>
    :root {
      --bg: #d8d8dd;
      --window: rgba(241, 242, 246, 0.9);
      --panel: rgba(255, 255, 255, 0.78);
      --panel-strong: rgba(255, 255, 255, 0.92);
      --line: rgba(83, 88, 101, 0.12);
      --text: #1f232c;
      --muted: #667085;
      --accent: #2d6cdf;
      --accent-soft: rgba(45, 108, 223, 0.1);
      --warm: #f59e0b;
      --danger: #b45309;
      --shadow: 0 18px 44px rgba(29, 34, 45, 0.16);
      --radius: 18px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", sans-serif;
      color: var(--text);
      background: linear-gradient(180deg, #d9dbe1 0%, #cfd2d9 100%);
    }

    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background: radial-gradient(circle at top, rgba(255,255,255,0.34), transparent 45%);
      opacity: 0.85;
    }

    .shell {
      position: relative;
      max-width: 1460px;
      margin: 18px auto;
      padding: 14px;
      background: var(--window);
      border: 1px solid rgba(255,255,255,0.62);
      border-radius: 26px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(20px);
    }

    .shell::before {
      content: "";
      position: absolute;
      top: 14px;
      left: 18px;
      width: 52px;
      height: 12px;
      border-radius: 999px;
      background:
        radial-gradient(circle at 6px 6px, #ff5f57 0 5px, transparent 5.5px),
        radial-gradient(circle at 26px 6px, #febc2e 0 5px, transparent 5.5px),
        radial-gradient(circle at 46px 6px, #28c840 0 5px, transparent 5.5px);
    }

    .hero {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(320px, 0.8fr);
      gap: 14px;
      margin: 18px 0 14px;
    }

    .hero-card, .status-card, .pane, .details-card {
      background: var(--panel);
      backdrop-filter: blur(14px);
      border: 1px solid rgba(255, 255, 255, 0.7);
      border-radius: var(--radius);
      box-shadow: 0 8px 24px rgba(26, 31, 42, 0.08);
    }

    .hero-card {
      padding: 18px 18px 16px;
      display: flex;
      flex-direction: column;
      gap: 10px;
      animation: rise .45s ease-out both;
    }

    .status-card {
      padding: 16px 18px;
      display: grid;
      gap: 10px;
      animation: rise .55s ease-out both;
    }

    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      width: fit-content;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 11px;
      letter-spacing: .12em;
      text-transform: uppercase;
      font-weight: 700;
    }

    h1 {
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", sans-serif;
      font-size: clamp(24px, 3vw, 32px);
      line-height: 1.08;
      letter-spacing: -0.03em;
    }

    .subtitle {
      margin: 0;
      color: var(--muted);
      max-width: 760px;
      line-height: 1.55;
      font-size: 14px;
    }

    .root-chip {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      width: fit-content;
      padding: 8px 12px;
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.68);
      color: var(--text);
      font-size: 13px;
      border: 1px solid var(--line);
      word-break: break-all;
    }

    .hero-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }

    .metric {
      padding: 12px;
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      border: 1px solid rgba(255,255,255,0.9);
    }

    .metric-label {
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: .12em;
      color: var(--muted);
      margin-bottom: 8px;
    }

    .metric-value {
      font-size: 22px;
      font-weight: 700;
      letter-spacing: -0.03em;
    }

    .status-row {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 11px 0;
      border-bottom: 1px solid var(--line);
    }

    .status-row:last-child {
      border-bottom: 0;
      padding-bottom: 0;
    }

    .status-label {
      color: var(--muted);
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: .12em;
    }

    .status-value {
      font-size: 14px;
      font-weight: 700;
      text-align: right;
      word-break: break-all;
    }

    .toolbar {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto auto auto auto auto;
      gap: 10px;
      align-items: center;
      margin-bottom: 12px;
      animation: rise .65s ease-out both;
    }

    .crumbs {
      min-height: 46px;
      padding: 6px;
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      background: var(--panel-strong);
      border: 1px solid var(--line);
      border-radius: 14px;
    }

    .crumb {
      border: 0;
      border-radius: 10px;
      background: transparent;
      color: var(--text);
      padding: 8px 10px;
      font: inherit;
      cursor: pointer;
      transition: background .18s ease, color .18s ease, transform .18s ease;
    }

    .crumb:hover {
      background: rgba(15, 118, 110, 0.12);
      color: var(--accent);
      transform: translateY(-1px);
    }

    .crumb.current {
      background: rgba(194, 109, 50, 0.12);
      color: var(--warm);
      font-weight: 700;
    }

    .control, .button {
      min-height: 46px;
      border-radius: 14px;
      border: 1px solid var(--line);
      font: inherit;
    }

    .control {
      padding: 0 14px;
      background: rgba(255, 255, 255, 0.88);
      color: var(--text);
      min-width: 170px;
    }

    .button {
      padding: 0 16px;
      background: linear-gradient(180deg, #ffffff 0%, #eceef2 100%);
      color: var(--text);
      font-weight: 700;
      cursor: pointer;
      transition: transform .18s ease, box-shadow .18s ease, background .18s ease;
    }

    .toggle {
      min-height: 46px;
      padding: 0 14px;
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.88);
      font-size: 13px;
      color: var(--text);
      white-space: nowrap;
    }

    .toggle input {
      width: 16px;
      height: 16px;
      accent-color: var(--accent);
    }

    .button:hover {
      transform: translateY(-1px);
      background: linear-gradient(180deg, #ffffff 0%, #e4e7ee 100%);
      box-shadow: 0 8px 18px rgba(30, 35, 46, 0.12);
    }

    .button:disabled {
      cursor: not-allowed;
      color: var(--muted);
      background: linear-gradient(180deg, #f4f5f7 0%, #eceef2 100%);
      box-shadow: none;
      transform: none;
      opacity: 0.7;
    }

    .button.danger,
    .details-action.danger {
      color: #8a3412;
      background: linear-gradient(180deg, #fff7ed 0%, #ffedd5 100%);
      border-color: rgba(180, 83, 9, 0.18);
    }

    .button.danger:hover,
    .details-action.danger:hover {
      background: linear-gradient(180deg, #fffaf3 0%, #ffe2bf 100%);
      box-shadow: 0 8px 18px rgba(180, 83, 9, 0.12);
    }

    .workspace {
      display: grid;
      grid-template-columns: minmax(0, 1fr);
      gap: 14px;
    }

    .pane {
      padding: 14px;
      animation: rise .75s ease-out both;
      transition: border-color .18s ease, background .18s ease, box-shadow .18s ease;
    }

    .pane.dragover {
      border-color: rgba(45, 108, 223, 0.42);
      background: rgba(236, 243, 255, 0.94);
      box-shadow: inset 0 0 0 1px rgba(45, 108, 223, 0.18);
    }

    .pane-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 12px;
      margin-bottom: 10px;
    }

    .pane-tools {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }

    .pane-title {
      margin: 0;
      font-size: 18px;
      letter-spacing: -0.03em;
    }

    .pane-meta {
      color: var(--muted);
      font-size: 13px;
    }

    .list {
      display: grid;
      gap: 8px;
    }

    .upload-status {
      margin-bottom: 12px;
      padding: 10px 12px;
      border-radius: 12px;
      border: 1px solid rgba(27,36,48,0.08);
      background: rgba(255,255,255,0.82);
      display: grid;
      gap: 8px;
    }

    .upload-status[hidden] {
      display: none;
    }

    .upload-hint {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }

    .upload-list {
      display: grid;
      gap: 8px;
    }

    .upload-item {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      align-items: center;
      padding: 8px 10px;
      border-radius: 12px;
      background: rgba(255,255,255,0.88);
      border: 1px solid rgba(27,36,48,0.08);
      font-size: 12px;
    }

    .upload-name {
      font-weight: 700;
      word-break: break-all;
    }

    .upload-state {
      color: var(--muted);
      white-space: nowrap;
    }

    .entry {
      display: grid;
      grid-template-columns: 30px minmax(0, 2.15fr) 132px minmax(0, 1.55fr) 108px 112px 78px;
      gap: 10px;
      align-items: center;
      padding: 9px 11px;
      border-radius: 12px;
      border: 1px solid rgba(27, 36, 48, 0.06);
      background: rgba(255,255,255,0.72);
      transition: transform .16s ease, border-color .16s ease, background .16s ease;
      cursor: pointer;
    }

    .entry:hover {
      transform: translateY(-1px);
      border-color: rgba(45, 108, 223, 0.16);
      background: rgba(255,255,255,0.94);
    }

    .entry.active {
      border-color: rgba(45, 108, 223, 0.24);
      background: rgba(231, 239, 255, 0.98);
    }

    .entry.marked {
      border-color: rgba(45, 108, 223, 0.18);
      background: rgba(239, 245, 255, 0.94);
    }

    .entry-select {
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .entry-select input {
      width: 16px;
      height: 16px;
      accent-color: var(--accent);
      cursor: pointer;
    }

    .entry-name {
      display: flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
    }

    .entry-badge {
      flex: 0 0 34px;
      height: 34px;
      border-radius: 11px;
      display: grid;
      place-items: center;
      font-size: 9px;
      font-weight: 800;
      letter-spacing: .06em;
      text-transform: uppercase;
      color: #fff;
    }

    .entry-badge.dir {
      background: linear-gradient(180deg, #8bc4ff 0%, #4f8ff8 100%);
    }

    .entry-badge.file {
      background: linear-gradient(180deg, #b8bcc8 0%, #8f95a6 100%);
    }

    .entry-text {
      min-width: 0;
    }

    .entry-title {
      font-weight: 700;
      font-size: 14px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .entry-subtitle {
      margin-top: 2px;
      color: var(--muted);
      font-size: 11px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .entry-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 5px;
      min-width: 0;
      align-items: center;
    }

    .meta-chip {
      max-width: 100%;
      padding: 3px 7px;
      border-radius: 999px;
      background: rgba(45, 108, 223, 0.08);
      color: #36598f;
      font-size: 10px;
      line-height: 1.35;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      border: 1px solid rgba(45, 108, 223, 0.08);
    }

    .meta-chip.dim {
      background: rgba(148, 163, 184, 0.12);
      color: var(--muted);
      border-color: rgba(148, 163, 184, 0.12);
    }

    .entry-cell {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }

    .entry-action {
      justify-self: end;
      min-height: 34px;
      padding: 7px 10px;
      border-radius: 11px;
      border: 0;
      font-size: 12px;
      line-height: 1;
      font-weight: 700;
      color: #36598f;
      background: rgba(45, 108, 223, 0.1);
      cursor: pointer;
    }

    .empty, .error-box {
      padding: 20px;
      border-radius: 18px;
      border: 1px dashed rgba(27,36,48,0.18);
      color: var(--muted);
      background: rgba(255,255,255,0.52);
      line-height: 1.7;
    }

    .entry-details-host {
      margin: -2px 0 6px 40px;
    }

    .entry-details-host .details-card {
      border-radius: 20px;
    }

    .error-box {
      color: #8a3412;
      border-style: solid;
      border-color: rgba(180, 83, 9, 0.22);
      background: rgba(255, 247, 237, 0.92);
    }

    .details-card {
      padding: 18px;
      display: grid;
      gap: 12px;
      animation: rise .85s ease-out both;
    }

    .details-card[hidden] {
      display: none;
    }

    .details-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }

    .details-action {
      min-height: 40px;
      padding: 0 14px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, #ffffff 0%, #eef1f6 100%);
      color: var(--text);
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }

    .details-action:disabled {
      cursor: not-allowed;
      opacity: 0.72;
    }

    .modal-overlay {
      position: fixed;
      inset: 0;
      display: grid;
      place-items: center;
      padding: 20px;
      background: rgba(22, 27, 36, 0.32);
      backdrop-filter: blur(6px);
      z-index: 30;
    }

    .modal-overlay[hidden] {
      display: none;
    }

    .modal-card {
      width: min(520px, 100%);
      padding: 20px;
      border-radius: 22px;
      background: rgba(241, 242, 246, 0.98);
      border: 1px solid rgba(255,255,255,0.74);
      box-shadow: 0 22px 50px rgba(29, 34, 45, 0.22);
      display: grid;
      gap: 14px;
    }

    .modal-card.wide {
      width: min(920px, 100%);
    }

    .modal-title {
      margin: 0;
      font-size: 22px;
      line-height: 1.1;
      letter-spacing: -0.03em;
      color: var(--text);
    }

    .modal-input {
      width: 100%;
      min-height: 44px;
      padding: 0 14px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.94);
      color: var(--text);
      font: inherit;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
    }

    .modal-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }

    .download-table-wrap {
      border-radius: 14px;
      border: 1px solid rgba(27,36,48,0.08);
      background: rgba(255,255,255,0.82);
      overflow: auto;
    }

    .download-table {
      width: 100%;
      border-collapse: collapse;
      min-width: 520px;
    }

    .download-table th,
    .download-table td {
      padding: 12px 14px;
      text-align: left;
      vertical-align: middle;
      border-bottom: 1px solid rgba(27,36,48,0.08);
    }

    .download-table tr:last-child td {
      border-bottom: none;
    }

    .download-table th {
      font-size: 12px;
      letter-spacing: .08em;
      text-transform: uppercase;
      color: var(--muted);
      background: rgba(255,255,255,0.7);
    }

    .download-name {
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      color: var(--text);
      word-break: break-word;
    }

    .download-query-input {
      width: 100%;
      min-height: 38px;
      padding: 0 12px;
      border-radius: 10px;
      border: 1px solid rgba(27,36,48,0.12);
      background: rgba(255,255,255,0.96);
      color: var(--text);
      font: inherit;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
    }

    .download-query-placeholder {
      color: var(--muted);
      font-size: 13px;
    }

    .modal-note {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
    }

    .meta-panel {
      padding: 14px;
      border-radius: 16px;
      background: rgba(255,255,255,0.8);
      border: 1px solid rgba(27,36,48,0.08);
      display: grid;
      gap: 10px;
    }

    .meta-panel[hidden] {
      display: none;
    }

    .meta-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
    }

    .meta-title {
      font-size: 13px;
      letter-spacing: .12em;
      text-transform: uppercase;
      color: var(--muted);
      font-weight: 700;
    }

    .meta-state {
      color: var(--muted);
      font-size: 12px;
    }

    .meta-list {
      display: flex;
      gap: 10px;
      overflow-x: auto;
      overflow-y: hidden;
      padding-bottom: 4px;
      scroll-snap-type: x proximity;
    }

    .meta-empty {
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(255,255,255,0.66);
      color: var(--muted);
      font-size: 13px;
      line-height: 1.6;
      border: 1px dashed rgba(27,36,48,0.14);
    }

    .meta-item {
      flex: 0 0 280px;
      max-width: min(280px, 72vw);
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      border: 1px solid rgba(27,36,48,0.08);
      display: grid;
      gap: 6px;
      scroll-snap-align: start;
    }

    .meta-key {
      font-size: 12px;
      line-height: 1.4;
      color: #36598f;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      word-break: break-all;
    }

    .meta-value {
      font-size: 12px;
      line-height: 1.6;
      color: var(--text);
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
    }

    .meta-value.collapsed {
      max-height: calc(1.6em * 3);
      overflow: hidden;
    }

    .meta-toggle {
      justify-self: start;
      min-height: 30px;
      padding: 0 10px;
      border-radius: 10px;
      border: 1px solid rgba(27,36,48,0.08);
      background: rgba(45, 108, 223, 0.08);
      color: #36598f;
      font: inherit;
      font-size: 11px;
      font-weight: 700;
      cursor: pointer;
    }

    @keyframes rise {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @media (max-width: 1120px) {
      .hero,
      .workspace {
        grid-template-columns: 1fr;
      }
    }

    @media (max-width: 860px) {
      .toolbar {
        grid-template-columns: 1fr;
      }

      .hero-grid,
      .details-grid {
        grid-template-columns: 1fr;
      }

      .entry {
        grid-template-columns: minmax(0, 1fr);
      }

      .entry-action {
        justify-self: start;
      }

      .entry-details-host {
        margin-left: 0;
      }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section class="hero">
      <div class="hero-card">
        <h1>IoTDB-FS File Manager</h1>
        <p class="subtitle">A browser for mounted IoTDB-FS directories.</p>
        <div class="root-chip">Browsing Root: <strong id="rootPath"></strong></div>
        <div class="hero-grid">
          <div class="metric">
            <div class="metric-label">Folders</div>
            <div class="metric-value" id="folderCount">0</div>
          </div>
          <div class="metric">
            <div class="metric-label">Files</div>
            <div class="metric-value" id="fileCount">0</div>
          </div>
          <div class="metric">
            <div class="metric-label">Current Path</div>
            <div class="metric-value" id="currentDepth">/</div>
          </div>
        </div>
      </div>

      <div class="status-card">
        <div class="status-row">
          <div class="status-label">Mounted Root</div>
          <div class="status-value" id="statusRoot"></div>
        </div>
        <div class="status-row">
          <div class="status-label">Selected Item</div>
          <div class="status-value" id="statusSelection">None</div>
        </div>
        <div class="status-row">
          <div class="status-label">Sort Mode</div>
          <div class="status-value" id="statusSort">Name A-Z</div>
        </div>
        <div class="status-row">
          <div class="status-label">Explorer State</div>
          <div class="status-value" id="statusState">Ready</div>
        </div>
      </div>
    </section>

    <section class="toolbar">
      <div class="crumbs" id="breadcrumbs"></div>
      <button class="button" id="metadataSearchBtn" type="button">Search Metadata</button>
      <input class="control" id="searchInput" type="search" placeholder="Filter files in this folder">
      <label class="toggle">
        <input id="hiddenToggle" type="checkbox">
        <span>Show Hidden Files</span>
      </label>
      <select class="control" id="sortSelect">
        <option value="name-asc">Name A-Z</option>
        <option value="name-desc">Name Z-A</option>
        <option value="mtime-desc">Recently Modified</option>
        <option value="size-desc">Largest First</option>
      </select>
      <button class="button" id="refreshBtn" type="button">Refresh</button>
    </section>

    <section class="workspace" id="workspace">
      <div class="pane" id="contentsPane">
        <div class="pane-head">
          <div>
            <h2 class="pane-title">Contents</h2>
            <div class="pane-meta" id="paneMeta">Loading...</div>
          </div>
          <div class="pane-tools">
            <input id="fileInput" type="file" multiple hidden>
            <button class="button" id="pickFilesBtn" type="button">Upload File</button>
            <button class="button" id="newFolderBtn" type="button">New Folder</button>
            <button class="button" id="querySelectedBtn" type="button" hidden disabled>Query Selected</button>
            <button class="button" id="downloadSelectedBtn" type="button" hidden disabled>Download Selected</button>
            <button class="button danger" id="deleteSelectedBtn" type="button" hidden disabled>Delete Selected</button>
          </div>
        </div>
        <div class="upload-status" id="uploadStatus" hidden>
          <div class="upload-hint" id="uploadHint">Drop files anywhere in the Contents pane to write them into the current directory through IoTDB-FS, matching a local copy into the mount point.</div>
          <div class="upload-list" id="uploadList"></div>
        </div>
        <div id="errorBox" class="error-box" hidden></div>
        <div id="entryList" class="list"></div>
        <div id="detailsDock" hidden>
          <aside class="details-card" id="detailsCard" hidden>
            <div class="meta-panel" hidden>
              <div class="meta-head">
                <span class="meta-title">Extended Metadata</span>
                <span class="meta-state" id="metadataState">No Selection</span>
              </div>
              <div class="meta-list" id="metadataList">
                <div class="meta-empty">Select a file or folder to inspect IoTDB-FS extended attributes.</div>
              </div>
            </div>

            <div class="details-actions">
              <button class="details-action" id="openBtn" type="button">Open in New Window</button>
              <button class="details-action" id="downloadBtn" type="button">Download File</button>
              <button class="details-action" id="partialReadBtn" type="button" disabled>Partial Read</button>
              <button class="details-action danger" id="deleteFileBtn" type="button">Delete File</button>
            </div>
          </aside>
        </div>
      </div>
    </section>
  </div>

  <div class="modal-overlay" id="partialReadModal" hidden>
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="partialReadTitle">
      <h2 class="modal-title" id="partialReadTitle">Partial Read</h2>
      <input
        class="modal-input"
        id="partialReadInput"
        type="text"
        placeholder="Enter partial read path/query"
        autocomplete="off">
      <div class="modal-actions">
        <button class="details-action" id="partialReadOpenBtn" type="button">Open In New Window</button>
        <button class="details-action" id="partialReadDownloadBtn" type="button">Download</button>
      </div>
    </div>
  </div>

  <div class="modal-overlay" id="batchQueryModal" hidden>
    <div class="modal-card wide" role="dialog" aria-modal="true" aria-labelledby="batchQueryTitle">
      <h2 class="modal-title" id="batchQueryTitle">Query Selected GRIB2 Files</h2>
      <div class="download-table-wrap">
        <table class="download-table">
          <thead>
            <tr>
              <th>Relative Path</th>
              <th>Partial Read</th>
            </tr>
          </thead>
          <tbody id="batchQueryTableBody"></tbody>
        </table>
      </div>
      <div class="modal-note" id="batchQueryNote">Review each .grib2 file and adjust its partial read query before opening query results in a single new tab.</div>
      <div class="modal-actions">
        <button class="details-action" id="batchQueryCancelBtn" type="button">Cancel</button>
        <button class="details-action" id="batchQueryOkBtn" type="button">Open Results</button>
      </div>
    </div>
  </div>

  <div class="modal-overlay" id="metadataSearchModal" hidden>
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="metadataSearchTitle">
      <h2 class="modal-title" id="metadataSearchTitle">Search Metadata</h2>
      <input
        class="modal-input"
        id="metadataSearchInput"
        type="text"
        placeholder="Enter metadata filters as k1=v1,k2=v2"
        autocomplete="off">
      <div class="modal-actions">
        <button class="details-action" id="metadataSearchCancelBtn" type="button">Cancel</button>
        <button class="details-action" id="metadataSearchOkBtn" type="button">OK</button>
      </div>
    </div>
  </div>

  <div class="modal-overlay" id="newFolderModal" hidden>
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="newFolderTitle">
      <h2 class="modal-title" id="newFolderTitle">New Folder</h2>
      <input
        class="modal-input"
        id="newFolderInput"
        type="text"
        placeholder="Enter folder name"
        autocomplete="off">
      <div class="modal-note" id="newFolderNote">Create a new folder inside the current directory.</div>
      <div class="modal-actions">
        <button class="details-action" id="newFolderCancelBtn" type="button">Cancel</button>
        <button class="details-action" id="newFolderOkBtn" type="button">Create</button>
      </div>
    </div>
  </div>

  <div class="modal-overlay" id="deleteModal" hidden>
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="deleteTitle">
      <h2 class="modal-title" id="deleteTitle">Delete</h2>
      <div class="modal-note" id="deleteModalNote">This action cannot be undone.</div>
      <div class="modal-actions">
        <button class="details-action" id="deleteCancelBtn" type="button">Cancel</button>
        <button class="details-action danger" id="deleteOkBtn" type="button">Delete</button>
      </div>
    </div>
  </div>

  <div class="modal-overlay" id="downloadModal" hidden>
    <div class="modal-card wide" role="dialog" aria-modal="true" aria-labelledby="downloadTitle">
      <h2 class="modal-title" id="downloadTitle">Download Files</h2>
      <div class="download-table-wrap">
        <table class="download-table">
          <thead>
            <tr>
              <th>File Name</th>
              <th>Partial Read</th>
            </tr>
          </thead>
          <tbody id="downloadTableBody"></tbody>
        </table>
      </div>
      <div class="modal-note" id="downloadModalNote">Review file names and optional partial read parameters for .grib2 files, then click OK to download using the browser's default download directory.</div>
      <div class="modal-actions">
        <button class="details-action" id="downloadCancelBtn" type="button">Cancel</button>
        <button class="details-action" id="downloadOkBtn" type="button">OK</button>
      </div>
    </div>
  </div>

  <script>
    function apiFetch(input, init = {}) {
      const options = { ...init };
      const headers = new Headers(options.headers || {});
      const token = window.sessionStorage.getItem("iotdb-fs-auth-token");
      if (token && (options.method || "GET").toUpperCase() === "POST") {
        headers.set("Authorization", `Bearer ${token}`);
      }
      options.headers = headers;
      return fetch(input, options);
    }
    window.__IOTDB_FS_BOOT__ = {
      root: "__ROOT__",
      gribExtensionEnabled: __GRIB_EXTENSION_ENABLED__,
      metadataFields: __METADATA_FIELDS__
    };

    const state = {
      currentPath: "/",
      rootPath: window.__IOTDB_FS_BOOT__.root,
      data: null,
      searchResults: null,
      filter: "",
      metadataSearchQuery: "",
      readQuery: "function=read&level=500&dtype=f64",
      sortMode: "name-asc",
      selected: null,
      selectedPaths: new Set(),
      showHidden: false,
      metadataCache: Object.create(null),
      batchQueryEntries: [],
      downloadDialogEntries: [],
      deleteDialogEntries: [],
      searchSeq: 0
    };

    const rootPathEl = document.getElementById("rootPath");
    const statusRootEl = document.getElementById("statusRoot");
    const breadcrumbsEl = document.getElementById("breadcrumbs");
    const entryListEl = document.getElementById("entryList");
    const errorBoxEl = document.getElementById("errorBox");
    const paneMetaEl = document.getElementById("paneMeta");
    const searchInputEl = document.getElementById("searchInput");
    const folderCountEl = document.getElementById("folderCount");
    const fileCountEl = document.getElementById("fileCount");
    const currentDepthEl = document.getElementById("currentDepth");
    const statusSelectionEl = document.getElementById("statusSelection");
    const statusSortEl = document.getElementById("statusSort");
    const statusStateEl = document.getElementById("statusState");
    const contentsPaneEl = document.getElementById("contentsPane");
    const detailsDockEl = document.getElementById("detailsDock");
    const detailsCardEl = document.getElementById("detailsCard");
    const metadataPanelEl = detailsCardEl.querySelector(".meta-panel");
    const metadataStateEl = document.getElementById("metadataState");
    const metadataListEl = document.getElementById("metadataList");
    const hiddenToggleEl = document.getElementById("hiddenToggle");
    const metadataSearchBtnEl = document.getElementById("metadataSearchBtn");
    const newFolderBtnEl = document.getElementById("newFolderBtn");
    const querySelectedBtnEl = document.getElementById("querySelectedBtn");
    const downloadSelectedBtnEl = document.getElementById("downloadSelectedBtn");
    const deleteSelectedBtnEl = document.getElementById("deleteSelectedBtn");
    const partialReadBtnEl = document.getElementById("partialReadBtn");
    const deleteFileBtnEl = document.getElementById("deleteFileBtn");
    const partialReadModalEl = document.getElementById("partialReadModal");
    const partialReadInputEl = document.getElementById("partialReadInput");
    const partialReadOpenBtnEl = document.getElementById("partialReadOpenBtn");
    const partialReadDownloadBtnEl = document.getElementById("partialReadDownloadBtn");
    const batchQueryModalEl = document.getElementById("batchQueryModal");
    const batchQueryTableBodyEl = document.getElementById("batchQueryTableBody");
    const batchQueryNoteEl = document.getElementById("batchQueryNote");
    const batchQueryCancelBtnEl = document.getElementById("batchQueryCancelBtn");
    const batchQueryOkBtnEl = document.getElementById("batchQueryOkBtn");
    const downloadModalEl = document.getElementById("downloadModal");
    const downloadTableBodyEl = document.getElementById("downloadTableBody");
    const downloadModalNoteEl = document.getElementById("downloadModalNote");
    const downloadCancelBtnEl = document.getElementById("downloadCancelBtn");
    const downloadOkBtnEl = document.getElementById("downloadOkBtn");
    const metadataSearchModalEl = document.getElementById("metadataSearchModal");
    const metadataSearchInputEl = document.getElementById("metadataSearchInput");
    const metadataSearchCancelBtnEl = document.getElementById("metadataSearchCancelBtn");
    const metadataSearchOkBtnEl = document.getElementById("metadataSearchOkBtn");
    const newFolderModalEl = document.getElementById("newFolderModal");
    const newFolderInputEl = document.getElementById("newFolderInput");
    const newFolderNoteEl = document.getElementById("newFolderNote");
    const newFolderCancelBtnEl = document.getElementById("newFolderCancelBtn");
    const newFolderOkBtnEl = document.getElementById("newFolderOkBtn");
    const deleteModalEl = document.getElementById("deleteModal");
    const deleteModalNoteEl = document.getElementById("deleteModalNote");
    const deleteCancelBtnEl = document.getElementById("deleteCancelBtn");
    const deleteOkBtnEl = document.getElementById("deleteOkBtn");
    const uploadStatusEl = document.getElementById("uploadStatus");
    const uploadHintEl = document.getElementById("uploadHint");
    const uploadListEl = document.getElementById("uploadList");
    const fileInputEl = document.getElementById("fileInput");
    const uploadDefaultHint = "Drop files anywhere in the Contents pane to write them into the current directory through IoTDB-FS, matching a local copy into the mount point.";

    rootPathEl.textContent = state.rootPath;
    statusRootEl.textContent = state.rootPath;

    function renderUploadItems(items) {
      uploadStatusEl.hidden = !items.length;
      uploadListEl.innerHTML = "";
      items.forEach((item) => {
        const row = document.createElement("div");
        row.className = "upload-item";

        const name = document.createElement("div");
        name.className = "upload-name";
        name.textContent = item.name;

        const status = document.createElement("div");
        status.className = "upload-state";
        status.textContent = item.status;

        row.append(name, status);
        uploadListEl.appendChild(row);
      });
    }

    async function uploadFiles(fileList) {
      const files = Array.from(fileList || []);
      if (!files.length) {
        return;
      }
      uploadStatusEl.hidden = false;
      uploadHintEl.textContent = `Uploading ${files.length} file(s) to ${state.currentPath}...`;
      const items = files.map((file) => ({ name: file.name, status: "Queued..." }));
      renderUploadItems(items);

      for (let i = 0; i < files.length; ++i) {
        items[i].status = "Uploading...";
        renderUploadItems(items);
        try {
          const url = new URL("/api/upload", window.location.origin);
          url.searchParams.set("path", state.currentPath);
          url.searchParams.set("name", files[i].name);
          const response = await apiFetch(url.toString(), {
            method: "POST",
            headers: {
              "Content-Type": "application/octet-stream"
            },
            body: files[i]
          });
          const payload = await response.json();
          if (!response.ok || !payload.ok) {
            throw new Error(payload.error || "Upload failed");
          }
          items[i].status = `Done ${formatSize(payload.bytes)}`;
        } catch (error) {
          items[i].status = `Failed: ${error.message}`;
        }
        renderUploadItems(items);
      }

      uploadHintEl.textContent = "Upload processing finished. The current directory has been refreshed.";
      await loadCurrentView(state.currentPath);
    }

    function normalizePath(raw) {
      const input = raw && raw.length ? raw : "/";
      const parts = input.split("/").filter(Boolean);
      const output = [];
      for (const part of parts) {
        if (part === ".") {
          continue;
        }
        if (part === "..") {
          return "/";
        }
        output.push(part);
      }
      return output.length ? "/" + output.join("/") : "/";
    }

    function relativePathForWebPath(webPath) {
      const normalized = normalizePath(webPath);
      if (normalized === "/") {
        return ".";
      }
      return normalized.replace(/^\/+/, "");
    }

    function formatSize(value) {
      if (value === null || value === undefined) {
        return "-";
      }
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = Number(value);
      let idx = 0;
      while (size >= 1024 && idx < units.length - 1) {
        size /= 1024;
        idx += 1;
      }
      return `${size.toFixed(size >= 10 || idx === 0 ? 0 : 1)} ${units[idx]}`;
    }

    function formatTime(ms) {
      if (!ms) {
        return "-";
      }
      return new Intl.DateTimeFormat("zh-CN", {
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit"
      }).format(new Date(ms));
    }

    function describeSort(mode) {
      const labels = {
        "name-asc": "Name A-Z",
        "name-desc": "Name Z-A",
        "mtime-desc": "Recently Modified",
        "size-desc": "Largest First"
      };
      return labels[mode] || mode;
    }

    function isSearchMode() {
      return Boolean(state.metadataSearchQuery.trim());
    }

    function updateMetadataSearchButton() {
      metadataSearchBtnEl.textContent = isSearchMode()
        ? "Clear Metadata Search"
        : "Search Metadata";
    }

    function describeMetadataSearchQuery(query) {
      return query.trim() || "(none)";
    }

    function buildBreadcrumbs(path) {
      breadcrumbsEl.innerHTML = "";
      const parts = path === "/" ? [] : path.slice(1).split("/");
      const nodes = [{ label: "Root", path: "/" }];
      let cursor = "";
      for (const part of parts) {
        cursor += `/${part}`;
        nodes.push({ label: part, path: cursor });
      }
      nodes.forEach((node, index) => {
        const button = document.createElement("button");
        button.className = "crumb" + (index === nodes.length - 1 ? " current" : "");
        button.textContent = node.label;
        button.addEventListener("click", () => navigate(node.path));
        breadcrumbsEl.appendChild(button);
      });
    }

    function sortEntries(entries) {
      const items = [...entries];
      items.sort((a, b) => {
        if (a.kind !== b.kind) {
          return a.kind === "directory" ? -1 : 1;
        }
        if (state.sortMode === "name-desc") {
          return b.name.localeCompare(a.name, "zh-CN");
        }
        if (state.sortMode === "mtime-desc") {
          return (b.mtimeMs || 0) - (a.mtimeMs || 0) || a.name.localeCompare(b.name, "zh-CN");
        }
        if (state.sortMode === "size-desc") {
          return (Number(b.size) || 0) - (Number(a.size) || 0) || a.name.localeCompare(b.name, "zh-CN");
        }
        return a.name.localeCompare(b.name, "zh-CN");
      });
      return items;
    }

    function currentEntries() {
      if (isSearchMode()) {
        return state.searchResults ? state.searchResults.entries || [] : [];
      }
      return state.data ? state.data.entries || [] : [];
    }

    function parentPathOf(path) {
      const normalized = normalizePath(path);
      if (normalized === "/") {
        return "/";
      }
      const slash = normalized.lastIndexOf("/");
      return slash <= 0 ? "/" : normalized.slice(0, slash);
    }

    function parentEntry() {
      return {
        name: "..",
        path: parentPathOf(state.currentPath),
        kind: "directory",
        extension: "",
        size: null,
        mtimeMs: 0,
        hidden: false,
        virtualParent: true
      };
    }

    function selectedFileEntries() {
      return currentEntries().filter((item) => item.kind !== "directory" && state.selectedPaths.has(item.path));
    }

    function selectedGrib2Entries() {
      return selectedFileEntries().filter((item) => isGrib2Entry(item));
    }

    function selectedDeleteEntries() {
      return currentEntries().filter((item) => state.selectedPaths.has(item.path));
    }

    function updateSelectedActionButtons() {
      const queryCount = selectedGrib2Entries().length;
      const downloadCount = selectedFileEntries().length;
      const deleteCount = selectedDeleteEntries().length;
      querySelectedBtnEl.hidden = queryCount === 0;
      querySelectedBtnEl.disabled = queryCount === 0;
      querySelectedBtnEl.textContent = queryCount
        ? `Query Selected (${queryCount})`
        : "Query Selected";
      downloadSelectedBtnEl.hidden = downloadCount === 0;
      downloadSelectedBtnEl.disabled = downloadCount === 0;
      downloadSelectedBtnEl.textContent = downloadCount
        ? `Download Selected (${downloadCount})`
        : "Download Selected";
      deleteSelectedBtnEl.hidden = deleteCount === 0;
      deleteSelectedBtnEl.disabled = deleteCount === 0;
      deleteSelectedBtnEl.textContent = deleteCount
        ? `Delete Selected (${deleteCount})`
        : "Delete Selected";
    }

    function pruneSelectedPaths(entries) {
      const allowed = new Set(
        (entries || [])
          .filter((item) => !item.virtualParent)
          .map((item) => item.path)
      );
      state.selectedPaths.forEach((path) => {
        if (!allowed.has(path)) {
          state.selectedPaths.delete(path);
        }
      });
      updateSelectedActionButtons();
    }

    function visibleEntries() {
      const entries = currentEntries();
      const baseEntries = !isSearchMode() && state.currentPath !== "/"
        ? [parentEntry(), ...entries]
        : entries;
      if (!baseEntries.length) {
        return [];
      }
      return baseEntries.filter((item) => state.showHidden || !item.hidden);
    }

    function filteredEntries() {
      let entries = visibleEntries();
      const query = state.filter.trim().toLowerCase();
      if (!isSearchMode() && query) {
        entries = entries.filter((item) =>
          item.virtualParent || item.name.toLowerCase().includes(query));
      }
      return sortEntries(entries);
    }

    function entryBadgeLabel(entry) {
      if (entry.virtualParent) {
        return "UP";
      }
      if (entry.kind === "directory") {
        return "DIR";
      }
      const ext = entry.extension || "FILE";
      return ext.slice(0, 4).toUpperCase();
    }

    function simplifyMetadataKey(name) {
      const field = window.__IOTDB_FS_BOOT__.metadataFields.find((item) => item.xattr === name);
      if (field) return field.id;
      return name
        .replace(/^user\.grib\./, "")
        .replace(/^user\./, "");
    }

    function summarizeMetadataValue(value) {
      if (!value) {
        return "(empty)";
      }
      return value.length > 20 ? `${value.slice(0, 20)}…` : value;
    }

    function isSummaryMetadataAttr(name) {
      return name === "user.grib.shortName"
        || name === "user.grib.typeOfProcessedData"
        || name === "user.grib.typeOfLevel"
        || name === "user.grib.centre"
        || name === "user.grib.globalDomain"
        || name === "user.grib.unit"
        || window.__IOTDB_FS_BOOT__.metadataFields.some((field) => field.xattr === name);
    }

    function fillEntryMetadataSummary(container, entry) {
      container.innerHTML = "";
      if (entry.virtualParent) {
        const chip = document.createElement("span");
        chip.className = "meta-chip dim";
        chip.textContent = "Double-click to go back";
        container.appendChild(chip);
        return;
      }
      if (!isGrib2Entry(entry)) {
        return;
      }
      const cached = state.metadataCache[entry.path];

      if (!cached || cached.loading) {
        const chip = document.createElement("span");
        chip.className = "meta-chip dim";
        chip.textContent = "Loading metadata...";
        container.appendChild(chip);
        return;
      }

      if (cached.error) {
        const chip = document.createElement("span");
        chip.className = "meta-chip dim";
        chip.textContent = "Metadata error";
        container.appendChild(chip);
        return;
      }

      if (!cached.attributes || !cached.attributes.length) {
        const chip = document.createElement("span");
        chip.className = "meta-chip dim";
        chip.textContent = "No attributes";
        container.appendChild(chip);
        return;
      }

      const summaryAttrs = cached.attributes.filter((attr) => isSummaryMetadataAttr(attr.name));
      if (!summaryAttrs.length) {
        const chip = document.createElement("span");
        chip.className = "meta-chip dim";
        chip.textContent = "No summary attributes";
        container.appendChild(chip);
        return;
      }

      summaryAttrs.forEach((attr) => {
        const chip = document.createElement("span");
        chip.className = "meta-chip";
        chip.textContent = `${simplifyMetadataKey(attr.name)}=${summarizeMetadataValue(attr.value)}`;
        container.appendChild(chip);
      });
    }

    function refreshEntryMetadataSummary(path) {
      entryListEl.querySelectorAll(".entry-meta").forEach((node) => {
        if (node.dataset.path !== path) {
          return;
        }
        const entry = filteredEntries().find((item) => item.path === path);
        if (entry) {
          fillEntryMetadataSummary(node, entry);
        }
      });
    }

    function isGrib2Entry(entry) {
      return Boolean(window.__IOTDB_FS_BOOT__.gribExtensionEnabled &&
        entry && entry.kind !== "directory" &&
        ["grib2", "grib", "grb2", "grb"].includes(entry.extension));
    }

    async function ensureEntryMetadata(entry) {
      if (entry.virtualParent) {
        return;
      }
      if (!isGrib2Entry(entry)) {
        return;
      }
      const cached = state.metadataCache[entry.path];
      if (cached && (cached.loading || cached.loaded)) {
        return;
      }

      state.metadataCache[entry.path] = { loading: true, loaded: false, attributes: [] };
      refreshEntryMetadataSummary(entry.path);

      try {
        const response = await fetch(`/api/metadata?path=${encodeURIComponent(entry.path)}`);
        const payload = await response.json();
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || "Failed to read extended metadata");
        }
        state.metadataCache[entry.path] = {
          loading: false,
          loaded: true,
          attributes: payload.attributes || [],
          supported: payload.supported,
          message: payload.message || ""
        };
      } catch (error) {
        state.metadataCache[entry.path] = {
          loading: false,
          loaded: true,
          error: error.message,
          attributes: []
        };
      }

      refreshEntryMetadataSummary(entry.path);
      if (!isSearchMode()) {
        renderEntries();
      }
    }

    function cacheSearchMetadata(entries) {
      entries.forEach((entry) => {
        state.metadataCache[entry.path] = {
          loading: false,
          loaded: true,
          attributes: entry.attributes || [],
          supported: true,
          message: ""
        };
      });
    }

    function renderEntries() {
      entryListEl.innerHTML = "";
      const entries = filteredEntries();
      const selectedPath = state.selected ? state.selected.path : "";
      const filterText = state.filter.trim();
      const metadataQuery = state.metadataSearchQuery.trim();
      if (metadataQuery) {
        paneMetaEl.textContent = `${entries.length} item(s) matched metadata filters: ${describeMetadataSearchQuery(metadataQuery)}`;
      }
      else if (filterText) {
        paneMetaEl.textContent = `${entries.length} item(s) matched file name "${filterText}" in this folder.`;
      }
      else {
        paneMetaEl.textContent = `${entries.length} item(s). Double-click folders to open, or files to view in a new window.`;
      }

      if (!entries.length) {
        dockDetailsCard(null);
        detailsCardEl.hidden = true;
        const empty = document.createElement("div");
        empty.className = "empty";
        if (metadataQuery) {
          empty.textContent = "No items matched the current metadata filters.";
        }
        else if (filterText) {
          empty.textContent = "No files or folders in this directory matched the current file name filter.";
        }
        else {
          empty.textContent = "This folder is currently empty.";
        }
        entryListEl.appendChild(empty);
        return;
      }

      entries.forEach((entry) => {
        const item = document.createElement("div");
        item.className = "entry"
          + (state.selected && state.selected.path === entry.path ? " active" : "")
          + (state.selectedPaths.has(entry.path) ? " marked" : "");
        item.addEventListener("click", () => selectEntry(entry));
        item.addEventListener("dblclick", () => activateEntry(entry));

        const select = document.createElement("div");
        select.className = "entry-select";
        if (!entry.virtualParent) {
          const checkbox = document.createElement("input");
          checkbox.type = "checkbox";
          checkbox.checked = state.selectedPaths.has(entry.path);
          checkbox.addEventListener("click", (event) => {
            event.stopPropagation();
          });
          checkbox.addEventListener("change", (event) => {
            if (event.target.checked) {
              state.selectedPaths.add(entry.path);
            }
            else {
              state.selectedPaths.delete(entry.path);
            }
            updateSelectedActionButtons();
            renderEntries();
          });
          select.appendChild(checkbox);
        }

        const name = document.createElement("div");
        name.className = "entry-name";
        name.innerHTML = `
          <div class="entry-badge ${entry.kind === "directory" ? "dir" : "file"}">${entryBadgeLabel(entry)}</div>
          <div class="entry-text">
            <div class="entry-title"></div>
            <div class="entry-subtitle"></div>
          </div>
        `;
        name.querySelector(".entry-title").textContent = entry.name;
        name.querySelector(".entry-subtitle").textContent = entry.path;

        const meta = document.createElement("div");
        meta.className = "entry-meta";
        meta.dataset.path = entry.path;
        fillEntryMetadataSummary(meta, entry);

        const modified = document.createElement("div");
        modified.className = "entry-cell";
        modified.textContent = formatTime(entry.mtimeMs);

        const kind = document.createElement("div");
        kind.className = "entry-cell";
        kind.textContent = entry.kind === "directory" ? "Folder" : (entry.extension || "File");

        const size = document.createElement("div");
        size.className = "entry-cell";
        size.textContent = entry.kind === "directory" ? "--" : formatSize(entry.size);

        const action = document.createElement("button");
        action.className = "entry-action";
        action.type = "button";
        action.textContent = entry.kind === "directory" ? "Open" : "View";
        action.addEventListener("click", (event) => {
          event.stopPropagation();
          activateEntry(entry);
        });

        item.append(select, name, modified, meta, kind, size, action);
        entryListEl.appendChild(item);

        if (selectedPath && selectedPath === entry.path) {
          const host = document.createElement("div");
          host.className = "entry-details-host";
          entryListEl.appendChild(host);
          dockDetailsCard(host);
          detailsCardEl.hidden = false;
        }
      });

      if (!selectedPath || !entries.some((entry) => entry.path === selectedPath)) {
        dockDetailsCard(null);
        detailsCardEl.hidden = true;
      }

      entries.forEach((entry) => ensureEntryMetadata(entry));
    }

    function renderMetadata(attributes, message) {
      metadataListEl.innerHTML = "";
      if (message) {
        const empty = document.createElement("div");
        empty.className = "meta-empty";
        empty.textContent = message;
        metadataListEl.appendChild(empty);
        return;
      }
      if (!attributes || !attributes.length) {
        const empty = document.createElement("div");
        empty.className = "meta-empty";
        empty.textContent = "No readable extended attributes were found for this item.";
        metadataListEl.appendChild(empty);
        return;
      }

      function formatMetadataDisplayValue(attr) {
        if (attr.binary) {
          return `${attr.value}  (binary)`;
        }
        const raw = attr.value || "(empty)";
        if (attr.name !== "user.grib.index.values" || !attr.value) {
          return raw;
        }
        try {
          const parsed = JSON.parse(attr.value);
          return JSON.stringify(parsed, null, 2);
        } catch (error) {
          return raw;
        }
      }

      function configureExpandableMetadataValue(value, toggle) {
        const naturalHeight = value.getBoundingClientRect().height;
        value.classList.add("collapsed");
        const collapsedHeight = value.getBoundingClientRect().height;
        const expandable = naturalHeight > collapsedHeight + 2;
        if (!expandable) {
          value.classList.remove("collapsed");
          toggle.hidden = true;
          toggle.textContent = "View All";
          return;
        }
        toggle.hidden = false;
        toggle.textContent = "View All";
      }

      attributes.forEach((attr) => {
        const item = document.createElement("div");
        item.className = "meta-item";

        const key = document.createElement("div");
        key.className = "meta-key";
        key.textContent = attr.name;

        const value = document.createElement("div");
        value.className = "meta-value";
        value.textContent = formatMetadataDisplayValue(attr);

        const toggle = document.createElement("button");
        toggle.className = "meta-toggle";
        toggle.type = "button";
        toggle.textContent = "View All";
        toggle.hidden = true;
        toggle.addEventListener("click", () => {
          const expanded = value.classList.toggle("collapsed") === false;
          toggle.textContent = expanded ? "Collapse" : "View All";
        });

        item.append(key, value, toggle);
        metadataListEl.appendChild(item);
        configureExpandableMetadataValue(value, toggle);
      });
    }

    async function loadMetadata(entry) {
      if (!entry) {
        metadataPanelEl.hidden = true;
        metadataStateEl.textContent = "No Selection";
        renderMetadata([], "Select a file or folder to inspect IoTDB-FS extended attributes.");
        return;
      }
      if (!isGrib2Entry(entry)) {
        metadataPanelEl.hidden = true;
        metadataStateEl.textContent = "";
        metadataListEl.innerHTML = "";
        return;
      }

      metadataPanelEl.hidden = false;
      metadataStateEl.textContent = "Loading...";
      renderMetadata([], "Reading extended metadata...");
      try {
        const response = await fetch(`/api/metadata?path=${encodeURIComponent(entry.path)}`);
        const payload = await response.json();
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || "Failed to read extended metadata");
        }
        metadataStateEl.textContent = payload.attributes.length
          ? `${payload.attributes.length} field(s)`
          : "No Attributes";
        if (!payload.supported && payload.message) {
          renderMetadata([], payload.message);
          return;
        }
        renderMetadata(payload.attributes, payload.message || "");
      } catch (error) {
        metadataStateEl.textContent = "Read Error";
        renderMetadata([], error.message);
      }
    }

    function currentReadQuery() {
      return state.readQuery.trim();
    }

    function fileNameFromPath(path) {
      const normalized = normalizePath(path);
      const slash = normalized.lastIndexOf("/");
      return slash >= 0 ? normalized.slice(slash + 1) : normalized;
    }

    function openViewer(path, readQuery) {
      const url = new URL("/viewer", window.location.origin);
      url.searchParams.set("path", path);
      if (readQuery) {
        url.searchParams.set("read_query", readQuery);
      }
      window.open(url.toString(), "_blank");
    }

    function openBatchViewer(entries) {
      const url = new URL("/viewer-batch", window.location.origin);
      entries.forEach((entry, index) => {
        url.searchParams.set(`path${index}`, entry.path);
        url.searchParams.set(`read_query${index}`, entry.readQuery);
      });
      window.open(url.toString(), "_blank");
    }

    function buildBatchQueryEntries(entries, options = {}) {
      const prefillReadQuery = Object.prototype.hasOwnProperty.call(options, "prefillReadQuery")
        ? options.prefillReadQuery
        : currentReadQuery();
      return (entries || [])
        .filter((entry) => isGrib2Entry(entry))
        .map((entry) => ({
          path: entry.path,
          name: entry.name || fileNameFromPath(entry.path),
          displayPath: relativePathForWebPath(entry.path),
          readQuery: prefillReadQuery || ""
        }));
    }

    function renderBatchQueryTable() {
      batchQueryTableBodyEl.innerHTML = "";
      state.batchQueryEntries.forEach((entry, index) => {
        const row = document.createElement("tr");

        const nameCell = document.createElement("td");
        nameCell.className = "download-name";
        nameCell.textContent = entry.displayPath || entry.name;

        const queryCell = document.createElement("td");
        const input = document.createElement("input");
        input.className = "download-query-input";
        input.type = "text";
        input.value = entry.readQuery || "";
        input.placeholder = "Required partial read query";
        input.addEventListener("input", (event) => {
          state.batchQueryEntries[index].readQuery = event.target.value;
          batchQueryNoteEl.textContent = "Review each .grib2 file and adjust its partial read query before opening query results in a single new tab.";
        });
        queryCell.appendChild(input);

        row.append(nameCell, queryCell);
        batchQueryTableBodyEl.appendChild(row);
      });
    }

    function closeBatchQueryModal() {
      batchQueryModalEl.hidden = true;
      batchQueryNoteEl.textContent = "Review each .grib2 file and adjust its partial read query before opening query results in a single new tab.";
      batchQueryOkBtnEl.disabled = false;
      batchQueryCancelBtnEl.disabled = false;
    }

    function openBatchQueryModal(entries, options = {}) {
      const dialogEntries = buildBatchQueryEntries(entries, options);
      if (!dialogEntries.length) {
        return;
      }
      state.batchQueryEntries = dialogEntries;
      renderBatchQueryTable();
      batchQueryModalEl.hidden = false;
    }

    function buildDownloadDialogEntries(entries, options = {}) {
      return (entries || []).map((entry) => ({
        path: entry.path,
        name: entry.name || fileNameFromPath(entry.path),
        extension: entry.extension || extensionOfName(entry.name || fileNameFromPath(entry.path)),
        readQuery: isGrib2Entry(entry) ? (options.prefillReadQuery || "") : ""
      }));
    }

    function renderDownloadTable() {
      downloadTableBodyEl.innerHTML = "";
      state.downloadDialogEntries.forEach((entry, index) => {
        const row = document.createElement("tr");

        const nameCell = document.createElement("td");
        nameCell.className = "download-name";
        nameCell.textContent = entry.name;

        const queryCell = document.createElement("td");
        if (entry.extension === "grib2") {
          const input = document.createElement("input");
          input.className = "download-query-input";
          input.type = "text";
          input.value = entry.readQuery || "";
          input.placeholder = "Optional partial read query";
          input.addEventListener("input", (event) => {
            state.downloadDialogEntries[index].readQuery = event.target.value;
          });
          queryCell.appendChild(input);
        }
        else {
          const placeholder = document.createElement("div");
          placeholder.className = "download-query-placeholder";
          placeholder.textContent = "--";
          queryCell.appendChild(placeholder);
        }

        row.append(nameCell, queryCell);
        downloadTableBodyEl.appendChild(row);
      });
    }

    function closeDownloadModal() {
      downloadModalEl.hidden = true;
      downloadModalNoteEl.textContent = "Review file names and optional partial read parameters for .grib2 files, then click OK to download using the browser's default download directory.";
      downloadOkBtnEl.disabled = false;
      downloadCancelBtnEl.disabled = false;
    }

    function openDownloadModal(entries, options = {}) {
      if (!entries || !entries.length) {
        return;
      }
      state.downloadDialogEntries = buildDownloadDialogEntries(entries, options);
      renderDownloadTable();
      downloadModalEl.hidden = false;
    }

    function dockDetailsCard(host) {
      if (host) {
        host.appendChild(detailsCardEl);
        return;
      }
      detailsDockEl.appendChild(detailsCardEl);
    }

    function closeMetadataSearchModal() {
      metadataSearchModalEl.hidden = true;
    }

    function openMetadataSearchModal() {
      metadataSearchInputEl.value = state.metadataSearchQuery.trim();
      metadataSearchModalEl.hidden = false;
      metadataSearchInputEl.focus();
      metadataSearchInputEl.select();
    }

    function closeNewFolderModal() {
      newFolderModalEl.hidden = true;
      newFolderInputEl.value = "";
      newFolderNoteEl.textContent = "Create a new folder inside the current directory.";
      newFolderOkBtnEl.disabled = false;
      newFolderCancelBtnEl.disabled = false;
    }

    function openNewFolderModal() {
      newFolderInputEl.value = "";
      newFolderNoteEl.textContent = `Create a new folder inside ${state.currentPath}.`;
      newFolderModalEl.hidden = false;
      newFolderInputEl.focus();
      newFolderInputEl.select();
    }

    async function submitMetadataSearch() {
      const query = metadataSearchInputEl.value.trim();
      if (!query) {
        metadataSearchInputEl.focus();
        return;
      }
      closeMetadataSearchModal();
      await runRecursiveSearch(state.currentPath, query);
    }

    async function submitNewFolder() {
      const name = newFolderInputEl.value.trim();
      if (!name) {
        newFolderInputEl.focus();
        return;
      }

      newFolderOkBtnEl.disabled = true;
      newFolderCancelBtnEl.disabled = true;
      newFolderNoteEl.textContent = `Creating "${name}"...`;
      statusStateEl.textContent = "Creating Folder";
      errorBoxEl.hidden = true;

      try {
        const url = new URL("/api/mkdir", window.location.origin);
        url.searchParams.set("path", state.currentPath);
        url.searchParams.set("name", name);
        const response = await apiFetch(url.toString(), { method: "POST" });
        const payload = await response.json();
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || "Create folder failed");
        }
        closeNewFolderModal();
        await loadCurrentView(state.currentPath);
      } catch (error) {
        newFolderNoteEl.textContent = error.message;
        newFolderOkBtnEl.disabled = false;
        newFolderCancelBtnEl.disabled = false;
        errorBoxEl.hidden = false;
        errorBoxEl.textContent = error.message;
        statusStateEl.textContent = "Error";
      }
    }

    function closePartialReadModal() {
      partialReadModalEl.hidden = true;
    }

    function extensionOfName(name) {
      const dot = (name || "").lastIndexOf(".");
      if (dot < 0 || dot + 1 >= name.length) {
        return "";
      }
      return name.slice(dot + 1).toLowerCase();
    }

    function openPartialReadModal() {
      if (!state.selected || state.selected.kind === "directory") {
        return;
      }
      partialReadInputEl.value = currentReadQuery();
      partialReadModalEl.hidden = false;
      partialReadInputEl.focus();
      partialReadInputEl.select();
    }

    function submitPartialRead(action) {
      if (!state.selected || state.selected.kind === "directory") {
        closePartialReadModal();
        return;
      }
      const query = partialReadInputEl.value.trim();
      if (!query) {
        partialReadInputEl.focus();
        return;
      }
      state.readQuery = query;
      updateQueryPanel(state.selected);
      closePartialReadModal();
      if (action === "download") {
        openDownloadModal([state.selected], { prefillReadQuery: query });
        return;
      }
      openViewer(state.selected.path, query);
    }

    function submitBatchQueryModal() {
      batchQueryOkBtnEl.disabled = true;
      batchQueryCancelBtnEl.disabled = true;
      const entries = state.batchQueryEntries.map((entry) => ({
        ...entry,
        readQuery: (entry.readQuery || "").trim()
      }));
      const firstEmptyIndex = entries.findIndex((entry) => !entry.readQuery);
      if (firstEmptyIndex >= 0) {
        const label = entries[firstEmptyIndex].displayPath || entries[firstEmptyIndex].path || entries[firstEmptyIndex].name;
        batchQueryNoteEl.textContent = `Partial read query is required for ${label}.`;
        batchQueryOkBtnEl.disabled = false;
        batchQueryCancelBtnEl.disabled = false;
        const targetInput = batchQueryTableBodyEl.querySelectorAll("input")[firstEmptyIndex];
        if (targetInput) {
          targetInput.focus();
          targetInput.select();
        }
        return;
      }

      try {
        batchQueryNoteEl.textContent = `Opening ${entries.length} query result(s) in a new tab...`;
        openBatchViewer(entries);
        closeBatchQueryModal();
      } catch (error) {
        batchQueryNoteEl.textContent = error.message;
        batchQueryOkBtnEl.disabled = false;
        batchQueryCancelBtnEl.disabled = false;
      }
    }

    function triggerBatchDownload(entries) {
      const url = new URL("/api/download-batch", window.location.origin);
      entries.forEach((entry, index) => {
        url.searchParams.set(`path${index}`, entry.path);
        const readQuery = (entry.readQuery || "").trim();
        if (readQuery) {
          url.searchParams.set(`read_query${index}`, readQuery);
        }
      });
      const anchor = document.createElement("a");
      anchor.href = url.toString();
      anchor.rel = "noopener";
      anchor.style.display = "none";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
    }

    function buildDeleteConfirmationMessage(entries) {
      if (!entries || !entries.length) {
        return "";
      }
      if (entries.length === 1) {
        const label = entries[0].kind === "directory" ? "folder" : "file";
        const detail = entries[0].kind === "directory"
          ? "This will delete the folder and all contents. This action cannot be undone."
          : "This action cannot be undone.";
        return `Delete ${label} "${entries[0].name}"?\n\n${detail}`;
      }
      return `Delete ${entries.length} selected items?\n\nFolders will be deleted with all contents. This action cannot be undone.`;
    }

    function closeDeleteModal() {
      deleteModalEl.hidden = true;
      deleteModalNoteEl.textContent = "This action cannot be undone.";
      deleteCancelBtnEl.disabled = false;
      deleteOkBtnEl.disabled = false;
      state.deleteDialogEntries = [];
    }

    function openDeleteModal(entries) {
      if (!entries || !entries.length) {
        return;
      }
      state.deleteDialogEntries = entries.map((entry) => ({ ...entry }));
      deleteModalNoteEl.textContent = buildDeleteConfirmationMessage(entries);
      deleteModalEl.hidden = false;
    }

    async function submitDeleteModal() {
      const entries = state.deleteDialogEntries || [];
      if (!entries || !entries.length) {
        closeDeleteModal();
        return;
      }

      deleteOkBtnEl.disabled = true;
      deleteCancelBtnEl.disabled = true;
      statusStateEl.textContent = "Deleting";
      errorBoxEl.hidden = true;

      try {
        if (entries.length === 1) {
          const url = new URL("/api/delete", window.location.origin);
          url.searchParams.set("path", entries[0].path);
          const response = await apiFetch(url.toString(), { method: "POST" });
          const payload = await response.json();
          if (!response.ok || !payload.ok) {
            throw new Error(payload.error || "Delete failed");
          }
        }
        else {
          const url = new URL("/api/delete-batch", window.location.origin);
          entries.forEach((entry, index) => {
            url.searchParams.set(`path${index}`, entry.path);
          });
          const response = await apiFetch(url.toString(), { method: "POST" });
          const payload = await response.json();
          if (!response.ok || !payload.ok) {
            throw new Error(payload.error || "Delete failed");
          }
        }

        closeDeleteModal();
        entries.forEach((entry) => {
          state.selectedPaths.delete(entry.path);
        });
        if (state.selected && entries.some((entry) => entry.path === state.selected.path)) {
          state.selected = null;
        }
        updateSelectedActionButtons();
        await loadCurrentView(state.currentPath);
      } catch (error) {
        deleteModalNoteEl.textContent = error.message;
        deleteOkBtnEl.disabled = false;
        deleteCancelBtnEl.disabled = false;
        errorBoxEl.hidden = false;
        errorBoxEl.textContent = error.message;
        statusStateEl.textContent = "Error";
      }
    }

    function submitDownloadModal() {
      downloadOkBtnEl.disabled = true;
      downloadCancelBtnEl.disabled = true;
      const entries = state.downloadDialogEntries.map((entry) => ({
        ...entry,
        readQuery: (entry.readQuery || "").trim()
      }));
      try {
        downloadModalNoteEl.textContent = `Starting archive download for ${entries.length} file(s)...`;
        triggerBatchDownload(entries);
        closeDownloadModal();
      } catch (error) {
        downloadModalNoteEl.textContent = error.message;
        downloadOkBtnEl.disabled = false;
        downloadCancelBtnEl.disabled = false;
      }
    }

    function updateQueryPanel(entry) {
      const selectedGrib2File = isGrib2Entry(entry);
      partialReadBtnEl.hidden = !selectedGrib2File;
      partialReadBtnEl.disabled = !selectedGrib2File;
      partialReadBtnEl.title = selectedGrib2File
        ? `Partial Read (${currentReadQuery() || "empty query"})`
        : "";
    }

    function updateDetailsVisibility(entry) {
      const showMetadata = isGrib2Entry(entry);
      metadataPanelEl.hidden = !showMetadata;
    }

    function updateDetails(entry) {
      if (!entry) {
        detailsCardEl.hidden = true;
        statusSelectionEl.textContent = "None";
        metadataStateEl.textContent = "No Selection";
        renderMetadata([], "Select a file or folder to inspect IoTDB-FS extended attributes.");
        updateDetailsVisibility(null);
        updateQueryPanel(null);
        dockDetailsCard(null);
        return;
      }

      detailsCardEl.hidden = false;
      statusSelectionEl.textContent = entry.name;
      document.getElementById("openBtn").textContent = entry.virtualParent
        ? "Go Up"
        : (entry.kind === "directory" ? "Open Folder" : "Open in New Window");
      document.getElementById("downloadBtn").hidden = entry.kind === "directory";
      deleteFileBtnEl.textContent =
        entry.kind === "directory" ? "Delete Folder" : "Delete File";
      deleteFileBtnEl.disabled = Boolean(entry.virtualParent);
      updateDetailsVisibility(entry);
      loadMetadata(entry);
      updateQueryPanel(entry);
    }

    function selectEntry(entry) {
      if (state.selected && state.selected.path === entry.path) {
        state.selected = null;
        renderEntries();
        updateDetails(null);
        return;
      }
      state.selected = entry;
      renderEntries();
      updateDetails(entry);
    }

    function activateEntry(entry) {
      if (entry.kind === "directory") {
        navigate(entry.path);
        return;
      }
      openViewer(entry.path, "");
    }

    function updateCounters() {
      const entries = filteredEntries();
      if (!entries.length && !state.data && !state.searchResults) {
        folderCountEl.textContent = "0";
        fileCountEl.textContent = "0";
        currentDepthEl.textContent = "/";
        return;
      }
      folderCountEl.textContent = String(entries.filter((item) => item.kind === "directory").length);
      fileCountEl.textContent = String(entries.filter((item) => item.kind !== "directory").length);
      currentDepthEl.textContent = state.currentPath;
      statusSortEl.textContent = describeSort(state.sortMode);
    }

    async function loadDirectory(path) {
      const normalized = normalizePath(path);
      state.searchSeq += 1;
      state.currentPath = normalized;
      state.selected = null;
      state.searchResults = null;
      statusStateEl.textContent = "Loading";
      errorBoxEl.hidden = true;
      buildBreadcrumbs(normalized);
      updateDetails(null);
      try {
        const response = await fetch(`/api/list?path=${encodeURIComponent(normalized)}`);
        const payload = await response.json();
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || "Directory load failed");
        }
        state.data = payload;
        pruneSelectedPaths(payload.entries || []);
        updateCounters();
        renderEntries();
        statusStateEl.textContent = "Ready";
      } catch (error) {
        state.data = { entries: [], dirCount: 0, fileCount: 0 };
        state.selectedPaths.clear();
        updateSelectedActionButtons();
        updateCounters();
        renderEntries();
        errorBoxEl.hidden = false;
        errorBoxEl.textContent = error.message;
        statusStateEl.textContent = "Error";
      }
    }

    function clearMetadataSearch() {
      state.metadataSearchQuery = "";
      state.searchResults = null;
      state.searchSeq += 1;
      updateMetadataSearchButton();
    }

    async function runRecursiveSearch(path, query) {
      const normalized = normalizePath(path);
      const trimmed = query.trim();
      const requestId = ++state.searchSeq;
      state.currentPath = normalized;
      state.selected = null;
      statusStateEl.textContent = "Searching";
      errorBoxEl.hidden = true;
      buildBreadcrumbs(normalized);
      updateDetails(null);
      paneMetaEl.textContent = `Searching metadata under ${normalized} with filters: ${describeMetadataSearchQuery(trimmed)}...`;
      try {
        const url = new URL("/api/search", window.location.origin);
        url.searchParams.set("path", normalized);
        url.searchParams.set("query", trimmed);
        url.searchParams.set("hidden", state.showHidden ? "1" : "0");
        const response = await fetch(url.toString());
        const payload = await response.json();
        if (requestId !== state.searchSeq) {
          return;
        }
        if (!response.ok || !payload.ok) {
          throw new Error(payload.error || "Metadata search failed");
        }
        state.metadataSearchQuery = (payload.query || trimmed).trim();
        state.searchResults = payload;
        cacheSearchMetadata(payload.entries || []);
        pruneSelectedPaths(payload.entries || []);
        updateMetadataSearchButton();
        updateCounters();
        renderEntries();
        statusStateEl.textContent = "Ready";
      } catch (error) {
        if (requestId !== state.searchSeq) {
          return;
        }
        state.searchResults = { entries: [], dirCount: 0, fileCount: 0 };
        state.selectedPaths.clear();
        updateSelectedActionButtons();
        updateCounters();
        renderEntries();
        errorBoxEl.hidden = false;
        errorBoxEl.textContent = error.message;
        statusStateEl.textContent = "Error";
      }
    }

    async function loadCurrentView(path) {
      if (isSearchMode()) {
        await runRecursiveSearch(path, state.metadataSearchQuery);
        return;
      }
      await loadDirectory(path);
    }

    function navigate(path) {
      const normalized = normalizePath(path);
      if (isSearchMode()) {
        clearMetadataSearch();
      }
      if (location.hash.slice(1) === normalized) {
        loadCurrentView(normalized);
        return;
      }
      location.hash = normalized;
    }

    searchInputEl.addEventListener("input", (event) => {
      state.filter = event.target.value;
      updateCounters();
      renderEntries();
    });

    document.getElementById("sortSelect").addEventListener("change", (event) => {
      state.sortMode = event.target.value;
      statusSortEl.textContent = describeSort(state.sortMode);
      renderEntries();
    });

    hiddenToggleEl.addEventListener("change", (event) => {
      state.showHidden = event.target.checked;
      loadCurrentView(state.currentPath);
    });

    metadataSearchBtnEl.addEventListener("click", async () => {
      if (isSearchMode()) {
        clearMetadataSearch();
        await loadDirectory(state.currentPath);
        return;
      }
      openMetadataSearchModal();
    });

    newFolderBtnEl.addEventListener("click", openNewFolderModal);

    document.getElementById("refreshBtn").addEventListener("click", () => {
      loadCurrentView(state.currentPath);
    });

    querySelectedBtnEl.addEventListener("click", () => {
      openBatchQueryModal(selectedGrib2Entries());
    });

    downloadSelectedBtnEl.addEventListener("click", () => {
      openDownloadModal(selectedFileEntries());
    });

    deleteSelectedBtnEl.addEventListener("click", async () => {
      openDeleteModal(selectedDeleteEntries());
    });

    document.getElementById("openBtn").addEventListener("click", () => {
      if (!state.selected) {
        navigate(state.currentPath);
        return;
      }
      activateEntry(state.selected);
    });

    document.getElementById("downloadBtn").addEventListener("click", () => {
      if (!state.selected || state.selected.kind === "directory") {
        return;
      }
      openDownloadModal([state.selected]);
    });

    deleteFileBtnEl.addEventListener("click", async () => {
      if (!state.selected || state.selected.virtualParent) {
        return;
      }
      openDeleteModal([state.selected]);
    });

    partialReadBtnEl.addEventListener("click", openPartialReadModal);
    partialReadOpenBtnEl.addEventListener("click", () => submitPartialRead("open"));
    partialReadDownloadBtnEl.addEventListener("click", () => submitPartialRead("download"));
    partialReadModalEl.addEventListener("click", (event) => {
      if (event.target === partialReadModalEl) {
        closePartialReadModal();
      }
    });
    batchQueryModalEl.addEventListener("click", (event) => {
      if (event.target === batchQueryModalEl) {
        closeBatchQueryModal();
      }
    });
    metadataSearchModalEl.addEventListener("click", (event) => {
      if (event.target === metadataSearchModalEl) {
        closeMetadataSearchModal();
      }
    });
    newFolderModalEl.addEventListener("click", (event) => {
      if (event.target === newFolderModalEl) {
        closeNewFolderModal();
      }
    });
    deleteModalEl.addEventListener("click", (event) => {
      if (event.target === deleteModalEl) {
        closeDeleteModal();
      }
    });
    downloadModalEl.addEventListener("click", (event) => {
      if (event.target === downloadModalEl) {
        closeDownloadModal();
      }
    });
    partialReadInputEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        submitPartialRead("open");
      }
      else if (event.key === "Escape") {
        closePartialReadModal();
      }
    });
    batchQueryModalEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        submitBatchQueryModal();
      }
      else if (event.key === "Escape") {
        closeBatchQueryModal();
      }
    });
    metadataSearchInputEl.addEventListener("keydown", async (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        await submitMetadataSearch();
      }
      else if (event.key === "Escape") {
        closeMetadataSearchModal();
      }
    });
    newFolderInputEl.addEventListener("keydown", async (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        await submitNewFolder();
      }
      else if (event.key === "Escape") {
        closeNewFolderModal();
      }
    });
    deleteCancelBtnEl.addEventListener("click", closeDeleteModal);
    deleteOkBtnEl.addEventListener("click", submitDeleteModal);
    batchQueryCancelBtnEl.addEventListener("click", closeBatchQueryModal);
    batchQueryOkBtnEl.addEventListener("click", submitBatchQueryModal);
    metadataSearchCancelBtnEl.addEventListener("click", closeMetadataSearchModal);
    metadataSearchOkBtnEl.addEventListener("click", submitMetadataSearch);
    newFolderCancelBtnEl.addEventListener("click", closeNewFolderModal);
    newFolderOkBtnEl.addEventListener("click", submitNewFolder);
    downloadCancelBtnEl.addEventListener("click", closeDownloadModal);
    downloadOkBtnEl.addEventListener("click", submitDownloadModal);
    window.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !partialReadModalEl.hidden) {
        closePartialReadModal();
        return;
      }
      if (event.key === "Escape" && !batchQueryModalEl.hidden) {
        closeBatchQueryModal();
        return;
      }
      if (event.key === "Escape" && !metadataSearchModalEl.hidden) {
        closeMetadataSearchModal();
        return;
      }
      if (event.key === "Escape" && !newFolderModalEl.hidden) {
        closeNewFolderModal();
        return;
      }
      if (event.key === "Escape" && !deleteModalEl.hidden) {
        closeDeleteModal();
        return;
      }
      if (event.key === "Escape" && !downloadModalEl.hidden) {
        closeDownloadModal();
      }
    });

    document.getElementById("pickFilesBtn").addEventListener("click", () => {
      fileInputEl.click();
    });

    fileInputEl.addEventListener("change", async (event) => {
      await uploadFiles(event.target.files);
      fileInputEl.value = "";
    });

    contentsPaneEl.addEventListener("dragover", (event) => {
      event.preventDefault();
      contentsPaneEl.classList.add("dragover");
      uploadStatusEl.hidden = false;
      uploadHintEl.textContent = `Drop now to write into ${state.currentPath} through IoTDB-FS`;
    });

    contentsPaneEl.addEventListener("dragleave", (event) => {
      if (event.currentTarget.contains(event.relatedTarget)) {
        return;
      }
      contentsPaneEl.classList.remove("dragover");
      if (!uploadListEl.childElementCount) {
        uploadStatusEl.hidden = true;
      }
      uploadHintEl.textContent = uploadDefaultHint;
    });

    contentsPaneEl.addEventListener("drop", async (event) => {
      event.preventDefault();
      contentsPaneEl.classList.remove("dragover");
      uploadStatusEl.hidden = false;
      await uploadFiles(event.dataTransfer.files);
    });

    window.addEventListener("hashchange", () => {
      loadCurrentView(location.hash.slice(1) || "/");
    });

    updateMetadataSearchButton();
    updateSelectedActionButtons();
    loadCurrentView(location.hash.slice(1) || "/");
  </script>
</body>
</html>
)HTML";

const char* kViewerHtml = R"HTML(
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Viewer</title>
  <style>
    :root {
      --bg: #d9dbe1;
      --panel: rgba(255,255,255,0.86);
      --line: rgba(80, 86, 98, 0.14);
      --text: #1f232c;
      --muted: #667085;
      --accent: #2d6cdf;
      --shadow: 0 18px 44px rgba(29, 34, 45, 0.16);
      --preview-height: calc(1.65em * 7 + 32px);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      background: linear-gradient(180deg, #d9dbe1 0%, #cfd2d9 100%);
      font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", "Segoe UI", sans-serif;
      color: var(--text);
      padding: 18px;
    }

    .window {
      max-width: 1180px;
      margin: 0 auto;
      background: rgba(241, 242, 246, 0.92);
      border-radius: 24px;
      border: 1px solid rgba(255,255,255,0.65);
      box-shadow: var(--shadow);
      backdrop-filter: blur(18px);
      padding: 18px;
    }

    .window::before {
      content: "";
      display: block;
      width: 52px;
      height: 12px;
      border-radius: 999px;
      margin-bottom: 14px;
      background:
        radial-gradient(circle at 6px 6px, #ff5f57 0 5px, transparent 5.5px),
        radial-gradient(circle at 26px 6px, #febc2e 0 5px, transparent 5.5px),
        radial-gradient(circle at 46px 6px, #28c840 0 5px, transparent 5.5px);
    }

    .footer-toolbar {
      padding: 12px 14px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      align-items: center;
      margin-top: 12px;
    }

    .button {
      min-height: 40px;
      padding: 0 14px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, #ffffff 0%, #eef1f6 100%);
      color: var(--text);
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }

    .status {
      margin-left: auto;
      color: var(--muted);
      font-size: 13px;
    }

    .viewer-title, .summary, .content, .footer-toolbar {
      padding: 14px;
      background: var(--panel);
      border: 1px solid rgba(255,255,255,0.72);
      border-radius: 18px;
      box-shadow: 0 8px 20px rgba(29, 34, 45, 0.08);
    }

    .viewer-title {
      margin-bottom: 12px;
    }

    .viewer-title-text {
      margin: 0;
      font-size: 24px;
      line-height: 1.12;
      letter-spacing: -0.03em;
      color: var(--text);
    }

    .summary {
      display: grid;
      gap: 14px;
      margin-bottom: 12px;
    }

    .section-title {
      margin: 0;
      font-size: 14px;
      letter-spacing: .12em;
      text-transform: uppercase;
      color: var(--muted);
    }

    .info-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 10px;
    }

    .info-item {
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      border: 1px solid rgba(80, 86, 98, 0.1);
      display: grid;
      gap: 6px;
    }

    .info-label {
      font-size: 12px;
      color: var(--muted);
      letter-spacing: .08em;
      text-transform: uppercase;
    }

    .info-value {
      font-size: 14px;
      line-height: 1.6;
      color: var(--text);
      word-break: break-word;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      max-height: calc(1.6em * 6);
      overflow: auto;
    }

    .metadata-panel {
      display: grid;
      gap: 10px;
    }

    .metadata-panel[hidden] {
      display: none;
    }

    .metadata-list {
      display: flex;
      gap: 10px;
      overflow-x: auto;
      overflow-y: hidden;
      padding-bottom: 4px;
      scroll-snap-type: x proximity;
    }

    .metadata-item {
      flex: 0 0 320px;
      max-width: min(320px, 78vw);
      padding: 12px 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.82);
      border: 1px solid rgba(80, 86, 98, 0.1);
      display: grid;
      gap: 6px;
      scroll-snap-align: start;
    }

    .metadata-key {
      font-size: 12px;
      line-height: 1.5;
      color: #36598f;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      word-break: break-all;
    }

    .metadata-value {
      font-size: 13px;
      line-height: 1.7;
      color: var(--text);
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
    }

    .metadata-value.collapsed {
      max-height: calc(1.7em * 10);
      overflow: hidden;
    }

    .metadata-toggle {
      justify-self: start;
      min-height: 30px;
      padding: 0 10px;
      border-radius: 10px;
      border: 1px solid rgba(27,36,48,0.08);
      background: rgba(45, 108, 223, 0.08);
      color: #36598f;
      font: inherit;
      font-size: 11px;
      font-weight: 700;
      cursor: pointer;
    }

    .modal-overlay {
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.38);
      display: grid;
      place-items: center;
      padding: 16px;
      z-index: 20;
    }

    .modal-overlay[hidden] {
      display: none;
    }

    .modal-card {
      width: min(560px, 100%);
      padding: 18px;
      border-radius: 20px;
      background: rgba(246, 247, 250, 0.98);
      border: 1px solid rgba(255,255,255,0.72);
      box-shadow: 0 24px 60px rgba(15, 23, 42, 0.22);
      display: grid;
      gap: 12px;
    }

    .modal-title {
      margin: 0;
      font-size: 18px;
      color: var(--text);
    }

    .modal-note {
      font-size: 13px;
      line-height: 1.6;
      color: var(--muted);
    }

    .modal-input {
      width: 100%;
      min-height: 112px;
      padding: 12px 14px;
      resize: vertical;
      border-radius: 14px;
      border: 1px solid rgba(80, 86, 98, 0.14);
      background: rgba(255,255,255,0.88);
      color: var(--text);
      font: inherit;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      line-height: 1.6;
    }

    .modal-actions {
      display: flex;
      gap: 10px;
      justify-content: flex-end;
      flex-wrap: wrap;
    }

    .content {
      padding: 14px;
      margin-bottom: 0;
    }

    pre {
      margin: 0;
      min-height: var(--preview-height);
      max-height: var(--preview-height);
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-size: 13px;
      line-height: 1.65;
      font-family: "IBM Plex Mono", "SFMono-Regular", monospace;
      background: linear-gradient(180deg, #272a31 0%, #1e2027 100%);
      color: #eef2ff;
      border-radius: 16px;
      padding: 16px;
    }

    .empty {
      padding: 24px;
      border-radius: 16px;
      border: 1px dashed var(--line);
      color: var(--muted);
      background: rgba(255,255,255,0.78);
      line-height: 1.7;
    }

    @media (max-width: 760px) {
      body { padding: 10px; }
      .window { padding: 12px; }
      .status { margin-left: 0; width: 100%; }
    }
  </style>
</head>
<body>
  <div class="window">
    <section class="viewer-title">
      <h1 class="viewer-title-text" id="viewerTitle">File Viewer</h1>
    </section>

    <section class="summary">
      <h2 class="section-title" id="summaryTitle">Basic Info</h2>
      <div class="info-grid" id="infoGrid">
        <div class="empty">Loading file information...</div>
      </div>
      <div class="metadata-panel" id="metadataPanel" hidden>
        <h2 class="section-title">Extended Metadata</h2>
        <div class="metadata-list" id="metadataList"></div>
      </div>
    </section>

    <section class="content">
      <pre id="content">Loading content...</pre>
    </section>

    <section class="footer-toolbar">
      <button class="button" id="downloadBtn" type="button">Download Raw Result</button>
      <button class="button" id="partialReadBtn" type="button" hidden>Partial Read</button>
      <button class="button" id="reloadBtn" type="button">Reload</button>
      <div class="status" id="status">Ready</div>
    </section>
  </div>

  <div class="modal-overlay" id="partialReadModal" hidden>
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="partialReadTitle">
      <h2 class="modal-title" id="partialReadTitle">Partial Read</h2>
      <div class="modal-note">Enter a partial read query for this `.grib2` file. The current viewer can open the query result directly or download it.</div>
      <textarea
        class="modal-input"
        id="partialReadInput"
        placeholder="function=read&level=500&dtype=f64"
      ></textarea>
      <div class="modal-actions">
        <button class="button" id="partialReadCancelBtn" type="button">Cancel</button>
        <button class="button" id="partialReadDownloadBtn" type="button">Download</button>
        <button class="button" id="partialReadOpenBtn" type="button">Open Query Result</button>
      </div>
    </div>
  </div>

  <script>
    const defaultReadQuery = "function=read&level=500&dtype=f64";
    const partialReadBtnEl = document.getElementById("partialReadBtn");
    const partialReadModalEl = document.getElementById("partialReadModal");
    const partialReadInputEl = document.getElementById("partialReadInput");
    const partialReadCancelBtnEl = document.getElementById("partialReadCancelBtn");
    const partialReadDownloadBtnEl = document.getElementById("partialReadDownloadBtn");
    const partialReadOpenBtnEl = document.getElementById("partialReadOpenBtn");

    function parseSearch() {
      const params = new URLSearchParams(location.search);
      return {
        path: params.get("path") || "",
        readQuery: params.get("read_query") || ""
      };
    }

    function fileNameFromPath(path) {
      const normalized = (path || "").replace(/\/+$/, "");
      const slash = normalized.lastIndexOf("/");
      return slash >= 0 ? normalized.slice(slash + 1) : normalized;
    }

    function extensionOfName(name) {
      const dot = (name || "").lastIndexOf(".");
      if (dot < 0 || dot + 1 >= name.length) {
        return "";
      }
      return name.slice(dot + 1).toLowerCase();
    }

    function canPartialRead(path) {
      return extensionOfName(fileNameFromPath(path)) === "grib2";
    }

    function updatePartialReadButton(path, readQuery) {
      const enabled = canPartialRead(path) && !readQuery;
      partialReadBtnEl.hidden = !enabled;
      partialReadBtnEl.disabled = !enabled;
      partialReadBtnEl.title = enabled
        ? `Partial Read (${readQuery || defaultReadQuery})`
        : "";
    }

    function closePartialReadModal() {
      partialReadModalEl.hidden = true;
    }

    function openPartialReadModal() {
      const { path, readQuery } = parseSearch();
      if (!canPartialRead(path)) {
        return;
      }
      partialReadInputEl.value = readQuery || defaultReadQuery;
      partialReadModalEl.hidden = false;
      partialReadInputEl.focus();
      partialReadInputEl.select();
    }

    function submitPartialRead(action) {
      const { path } = parseSearch();
      if (!canPartialRead(path)) {
        closePartialReadModal();
        return;
      }
      const query = partialReadInputEl.value.trim();
      if (!query) {
        partialReadInputEl.focus();
        return;
      }

      if (action === "download") {
        const downloadUrl = new URL("/api/download", location.origin);
        downloadUrl.searchParams.set("path", path);
        downloadUrl.searchParams.set("read_query", query);
        closePartialReadModal();
        window.open(downloadUrl.toString(), "_blank");
        return;
      }

      const url = new URL(location.href);
      url.searchParams.set("path", path);
      url.searchParams.set("read_query", query);
      closePartialReadModal();
      location.assign(url.toString());
    }

    function setStatus(text) {
      document.getElementById("status").textContent = text;
    }

    function formatSize(value) {
      if (value === null || value === undefined) {
        return "-";
      }
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = Number(value);
      let idx = 0;
      while (size >= 1024 && idx < units.length - 1) {
        size /= 1024;
        idx += 1;
      }
      return `${size.toFixed(size >= 10 || idx === 0 ? 0 : 1)} ${units[idx]}`;
    }

    function renderInfo(payload) {
      const infoGridEl = document.getElementById("infoGrid");
      const metadataPanelEl = document.getElementById("metadataPanel");
      const metadataListEl = document.getElementById("metadataList");
      const summaryTitleEl = document.getElementById("summaryTitle");

      const items = payload.partial
        ? [
            { label: "Extended File Name", value: payload.name || "-" },
            { label: "Extended File Path", value: payload.displayPath || "-" },
            { label: "File Size", value: formatSize(payload.size) }
          ]
        : [
            { label: "File Name", value: payload.name || "-" },
            { label: "File Path", value: payload.displayPath || "-" },
            { label: "File Size", value: formatSize(payload.size) },
            { label: "File Type", value: payload.fileType || "File" }
          ];

      summaryTitleEl.textContent = payload.partial ? "Query Result Info" : "Basic Info";
      infoGridEl.innerHTML = "";
      items.forEach((item) => {
        const box = document.createElement("div");
        box.className = "info-item";

        const label = document.createElement("div");
        label.className = "info-label";
        label.textContent = item.label;

        const value = document.createElement("div");
        value.className = "info-value";
        value.textContent = item.value;

        box.append(label, value);
        infoGridEl.appendChild(box);
      });

      if (payload.partial) {
        metadataPanelEl.hidden = true;
        metadataListEl.innerHTML = "";
        return;
      }

      metadataListEl.innerHTML = "";
      if (!payload.showMetadata) {
        metadataPanelEl.hidden = true;
        return;
      }

      metadataPanelEl.hidden = false;
      const attributes = payload.attributes || [];
      if (!payload.metadataSupported && payload.metadataMessage) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = payload.metadataMessage;
        metadataListEl.appendChild(empty);
        return;
      }
      if (!attributes.length) {
        const empty = document.createElement("div");
        empty.className = "empty";
        empty.textContent = "No extended metadata was found for this file.";
        metadataListEl.appendChild(empty);
        return;
      }

      function configureExpandableMetadataValue(value, toggle) {
        const naturalHeight = value.getBoundingClientRect().height;
        value.classList.add("collapsed");
        const collapsedHeight = value.getBoundingClientRect().height;
        const expandable = naturalHeight > collapsedHeight + 2;
        if (!expandable) {
          value.classList.remove("collapsed");
          toggle.hidden = true;
          toggle.textContent = "View All";
          return;
        }
        toggle.hidden = false;
        toggle.textContent = "View All";
      }

      attributes.forEach((attr) => {
        const item = document.createElement("div");
        item.className = "metadata-item";

        const key = document.createElement("div");
        key.className = "metadata-key";
        key.textContent = attr.name;

        const value = document.createElement("div");
        value.className = "metadata-value";
        value.textContent = attr.binary
          ? `${attr.value}  (binary)`
          : (attr.value || "(empty)");

        const toggle = document.createElement("button");
        toggle.className = "metadata-toggle";
        toggle.type = "button";
        toggle.textContent = "View All";
        toggle.hidden = true;
        toggle.addEventListener("click", () => {
          const expanded = value.classList.toggle("collapsed") === false;
          toggle.textContent = expanded ? "Collapse" : "View All";
        });

        item.append(key, value, toggle);
        metadataListEl.appendChild(item);
        configureExpandableMetadataValue(value, toggle);
      });
    }

    function compactDisplayText(text) {
      return (text || "").replace(/\n{3,}/g, "\n\n").trim();
    }

    function compactBinaryDisplayText(text, maxLines = 10) {
      const compact = compactDisplayText(text)
        .replace(/[ \t]+\n/g, "\n")
        .replace(/\n[ \t]+/g, "\n");
      const lines = compact.split("\n");
      if (lines.length <= maxLines) {
        return compact;
      }
      return `${lines.slice(0, maxLines).join("\n")}\n\n... binary preview truncated ...`;
    }

    function renderContent(payload) {
      const suffix = payload.message ? `\n${payload.message}` : "";
      const rawText = `${payload.content || ""}${suffix}`;
      document.getElementById("content").textContent =
        (payload.binary
          ? compactBinaryDisplayText(rawText)
          : compactDisplayText(rawText)) || "(empty file)";
      setStatus(payload.binary ? "Showing binary hex preview" : "Loaded");
    }

    async function loadContent() {
      const { path, readQuery } = parseSearch();
      updatePartialReadButton(path, readQuery);
      document.querySelector(".window").classList.toggle("query-result", Boolean(readQuery));
      document.getElementById("viewerTitle").textContent =
        readQuery ? "Query Result Viewer" : "File Viewer";
      document.title = "Viewer";
      document.getElementById("infoGrid").innerHTML = `<div class="empty">Loading file information...</div>`;
      document.getElementById("metadataList").innerHTML = "";
      document.getElementById("metadataPanel").hidden = true;
      document.getElementById("content").textContent = "Loading content...";
      setStatus("Loading...");

      try {
        const infoUrl = new URL("/api/viewer-info", location.origin);
        infoUrl.searchParams.set("path", path);
        if (readQuery) {
          infoUrl.searchParams.set("read_query", readQuery);
        }
        const previewUrl = new URL("/api/preview", location.origin);
        previewUrl.searchParams.set("path", path);
        if (readQuery) {
          previewUrl.searchParams.set("read_query", readQuery);
        }

        const [infoResponse, previewResponse] = await Promise.all([
          fetch(infoUrl),
          fetch(previewUrl)
        ]);
        const infoPayload = await infoResponse.json();
        const previewPayload = await previewResponse.json();
        if (!infoResponse.ok || !infoPayload.ok) {
          throw new Error(infoPayload.error || "Failed to load file information");
        }
        if (!previewResponse.ok || !previewPayload.ok) {
          throw new Error(previewPayload.error || "Read failed");
        }
        renderInfo(infoPayload);
        renderContent(previewPayload);
      } catch (error) {
        document.getElementById("infoGrid").innerHTML = `<div class="empty">${error.message}</div>`;
        document.getElementById("content").textContent = error.message;
        setStatus("Read failed");
      }
    }

    function downloadCurrent() {
      const { path, readQuery } = parseSearch();
      const url = new URL("/api/download", location.origin);
      url.searchParams.set("path", path);
      if (readQuery) {
        url.searchParams.set("read_query", readQuery);
      }
      window.open(url.toString(), "_blank");
    }

    document.getElementById("downloadBtn").addEventListener("click", downloadCurrent);
    partialReadBtnEl.addEventListener("click", openPartialReadModal);
    partialReadCancelBtnEl.addEventListener("click", closePartialReadModal);
    partialReadOpenBtnEl.addEventListener("click", () => submitPartialRead("open"));
    partialReadDownloadBtnEl.addEventListener("click", () => submitPartialRead("download"));
    partialReadModalEl.addEventListener("click", (event) => {
      if (event.target === partialReadModalEl) {
        closePartialReadModal();
      }
    });
    partialReadInputEl.addEventListener("keydown", (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        submitPartialRead("open");
      }
      if (event.key === "Escape") {
        event.preventDefault();
        closePartialReadModal();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !partialReadModalEl.hidden) {
        closePartialReadModal();
      }
    });
    document.getElementById("reloadBtn").addEventListener("click", loadContent);
    loadContent();
  </script>
</body>
</html>
)HTML";

const char* kBatchViewerHtml = R"HTML(
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Batch Query Results</title>
  <style>
    :root {
      --bg: #d9dbe1;
      --panel: rgba(255,255,255,0.86);
      --line: rgba(80, 86, 98, 0.14);
      --text: #1f232c;
      --muted: #667085;
      --accent: #2d6cdf;
      --shadow: 0 18px 44px rgba(29, 34, 45, 0.16);
      --danger: #c83f3f;
      --preview-height: calc(1.65em * 7 + 32px);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Segoe UI", "PingFang SC", sans-serif;
      background:
        radial-gradient(circle at top right, rgba(45,108,223,0.18), transparent 42%),
        linear-gradient(180deg, #edf0f6 0%, #d9dbe1 100%);
      color: var(--text);
      padding: 18px;
    }

    .window {
      max-width: 1180px;
      margin: 0 auto;
      padding: 20px;
      border-radius: 24px;
      background: var(--panel);
      box-shadow: var(--shadow);
      backdrop-filter: blur(16px);
    }

    .hero {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
      margin-bottom: 18px;
    }

    .hero h1 {
      margin: 0;
      font-size: 28px;
    }

    .hero p {
      margin: 6px 0 0;
      color: var(--muted);
    }

    .toolbar {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 18px;
      padding: 14px 16px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: rgba(255,255,255,0.74);
    }

    .status {
      color: var(--muted);
      font-size: 14px;
    }

    .button {
      border: none;
      border-radius: 999px;
      padding: 10px 16px;
      font: inherit;
      color: white;
      background: var(--accent);
      cursor: pointer;
    }

    .button.secondary {
      background: #4b5567;
    }

    .results {
      display: flex;
      flex-direction: column;
      gap: 18px;
    }

    .result-card {
      border: 1px solid var(--line);
      border-radius: 20px;
      background: rgba(255,255,255,0.78);
      overflow: hidden;
    }

    .result-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: flex-start;
      padding: 16px 18px 12px;
      border-bottom: 1px solid var(--line);
    }

    .result-title {
      margin: 0;
      font-size: 18px;
    }

    .result-subtitle {
      margin: 6px 0 0;
      color: var(--muted);
      word-break: break-all;
      line-height: 1.6;
      font-size: 14px;
    }

    .result-meta {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      padding: 16px 18px 0;
    }

    .meta-item {
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.68);
    }

    .meta-label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }

    .meta-value {
      font-size: 14px;
      word-break: break-all;
      line-height: 1.6;
    }

    .result-content {
      padding: 16px 18px 18px;
    }

    pre {
      margin: 0;
      padding: 16px;
      border-radius: 16px;
      background: #1d2230;
      color: #eff3ff;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      line-height: 1.6;
      font-family: "SFMono-Regular", Consolas, monospace;
      font-size: 13px;
      min-height: var(--preview-height);
      max-height: var(--preview-height);
    }

    .empty,
    .error {
      padding: 24px;
      border-radius: 16px;
      border: 1px dashed var(--line);
      color: var(--muted);
      background: rgba(255,255,255,0.78);
      line-height: 1.7;
    }

    .error {
      color: var(--danger);
      border-style: solid;
    }

    @media (max-width: 760px) {
      body { padding: 10px; }
      .window { padding: 14px; }
      .hero,
      .toolbar,
      .result-head {
        flex-direction: column;
        align-items: stretch;
      }
    }
  </style>
</head>
<body>
  <div class="window">
    <section class="hero">
      <div>
        <h1>Batch Query Results</h1>
        <p id="summary">Loading selected query results...</p>
      </div>
      <div style="display:flex; gap:12px; flex-wrap:wrap;">
        <button class="button" id="downloadAllBtn" type="button">Download All Results</button>
        <button class="button secondary" id="reloadBtn" type="button">Reload</button>
      </div>
    </section>

    <section class="toolbar">
      <div class="status" id="status">Ready</div>
    </section>

    <section class="results" id="results">
      <div class="empty">Loading query results...</div>
    </section>
  </div>

  <script>
    function parseEntries() {
      const params = new URLSearchParams(location.search);
      const indexed = [];
      for (const [key, value] of params.entries()) {
        const match = key.match(/^path(\d+)$/);
        if (!match || !value) {
          continue;
        }
        const index = Number(match[1]);
        indexed.push({
          index,
          path: value,
          readQuery: params.get(`read_query${index}`) || ""
        });
      }
      indexed.sort((a, b) => a.index - b.index);
      if (indexed.length) {
        return indexed.map((entry) => ({
          path: entry.path,
          readQuery: entry.readQuery
        }));
      }

      const path = params.get("path") || "";
      if (!path) {
        return [];
      }
      return [{
        path,
        readQuery: params.get("read_query") || ""
      }];
    }

    function setStatus(text) {
      document.getElementById("status").textContent = text;
    }

    function formatSize(value) {
      if (value === null || value === undefined) {
        return "-";
      }
      const units = ["B", "KB", "MB", "GB", "TB"];
      let size = Number(value);
      let idx = 0;
      while (size >= 1024 && idx < units.length - 1) {
        size /= 1024;
        idx += 1;
      }
      return `${size.toFixed(size >= 10 || idx === 0 ? 0 : 1)} ${units[idx]}`;
    }

    function compactDisplayText(text) {
      return (text || "").replace(/\n{3,}/g, "\n\n").trim();
    }

    function compactBinaryDisplayText(text, maxLines = 10) {
      const compact = compactDisplayText(text)
        .replace(/[ \t]+\n/g, "\n")
        .replace(/\n[ \t]+/g, "\n");
      const lines = compact.split("\n");
      if (lines.length <= maxLines) {
        return compact;
      }
      return `${lines.slice(0, maxLines).join("\n")}\n\n... binary preview truncated ...`;
    }

    function fileNameFromPath(path) {
      const slash = (path || "").lastIndexOf("/");
      return slash >= 0 ? path.slice(slash + 1) : path;
    }

    function buildDownloadUrl(entry) {
      const url = new URL("/api/download", location.origin);
      url.searchParams.set("path", entry.path);
      if (entry.readQuery) {
        url.searchParams.set("read_query", entry.readQuery);
      }
      return url.toString();
    }

    function buildBatchDownloadUrl(entries) {
      const url = new URL("/api/download-batch", location.origin);
      (entries || []).forEach((entry, index) => {
        url.searchParams.set(`path${index}`, entry.path);
        if (entry.readQuery) {
          url.searchParams.set(`read_query${index}`, entry.readQuery);
        }
      });
      return url.toString();
    }

    function createMetaItem(label, value) {
      const item = document.createElement("div");
      item.className = "meta-item";

      const labelEl = document.createElement("div");
      labelEl.className = "meta-label";
      labelEl.textContent = label;

      const valueEl = document.createElement("div");
      valueEl.className = "meta-value";
      valueEl.textContent = value;

      item.append(labelEl, valueEl);
      return item;
    }

    function createResultCard(entry, index) {
      const card = document.createElement("article");
      card.className = "result-card";
      card.innerHTML = `
        <div class="result-head">
          <div>
            <h2 class="result-title">Result ${index + 1}: ${fileNameFromPath(entry.path) || "(unknown file)"}</h2>
            <div class="result-subtitle">Path: ${entry.path || "-"}\nQuery: ${entry.readQuery || "(empty)"}</div>
          </div>
          <button class="button" type="button">Download Raw Result</button>
        </div>
        <div class="result-meta"></div>
        <div class="result-content">
          <pre>Loading...</pre>
        </div>
      `;
      card.querySelector("button").addEventListener("click", () => {
        window.open(buildDownloadUrl(entry), "_blank");
      });
      return card;
    }

    function renderError(card, message) {
      const metaEl = card.querySelector(".result-meta");
      const contentEl = card.querySelector(".result-content");
      metaEl.innerHTML = "";
      contentEl.innerHTML = "";
      const errorEl = document.createElement("div");
      errorEl.className = "error";
      errorEl.textContent = message;
      contentEl.appendChild(errorEl);
    }

    function renderResult(card, entry, infoPayload, previewPayload) {
      const metaEl = card.querySelector(".result-meta");
      metaEl.innerHTML = "";
      metaEl.append(
        createMetaItem("File Name", infoPayload.name || fileNameFromPath(entry.path) || "-"),
        createMetaItem("Extended Path", infoPayload.displayPath || entry.path || "-"),
        createMetaItem("Result Size", formatSize(infoPayload.size)),
        createMetaItem("Result Type", previewPayload.binary ? "Binary Preview" : "Text")
      );

      const contentText = `${previewPayload.content || ""}${previewPayload.message ? `\n${previewPayload.message}` : ""}`;
      card.querySelector("pre").textContent =
        (previewPayload.binary
          ? compactBinaryDisplayText(contentText)
          : compactDisplayText(contentText)) || "(empty file)";
    }

    async function loadResults() {
      const entries = parseEntries();
      const resultsEl = document.getElementById("results");
      const summaryEl = document.getElementById("summary");
      const downloadAllBtnEl = document.getElementById("downloadAllBtn");
      resultsEl.innerHTML = "";
      downloadAllBtnEl.disabled = !entries.length;

      if (!entries.length) {
        summaryEl.textContent = "No query request was provided.";
        resultsEl.innerHTML = `<div class="error">No file path was provided for batch query rendering.</div>`;
        setStatus("No data");
        return;
      }

      summaryEl.textContent = `${entries.length} query result(s) will be rendered from top to bottom in this page.`;
      document.title = `Batch Query Results (${entries.length})`;
      setStatus("Loading...");

      let completed = 0;
      for (let index = 0; index < entries.length; ++index) {
        const entry = entries[index];
        const card = createResultCard(entry, index);
        resultsEl.appendChild(card);
        try {
          const infoUrl = new URL("/api/viewer-info", location.origin);
          infoUrl.searchParams.set("path", entry.path);
          if (entry.readQuery) {
            infoUrl.searchParams.set("read_query", entry.readQuery);
          }

          const previewUrl = new URL("/api/preview", location.origin);
          previewUrl.searchParams.set("path", entry.path);
          if (entry.readQuery) {
            previewUrl.searchParams.set("read_query", entry.readQuery);
          }

          const [infoResponse, previewResponse] = await Promise.all([
            fetch(infoUrl),
            fetch(previewUrl)
          ]);
          const infoPayload = await infoResponse.json();
          const previewPayload = await previewResponse.json();
          if (!infoResponse.ok || !infoPayload.ok) {
            throw new Error(infoPayload.error || "Failed to load file information");
          }
          if (!previewResponse.ok || !previewPayload.ok) {
            throw new Error(previewPayload.error || "Read failed");
          }
          renderResult(card, entry, infoPayload, previewPayload);
        } catch (error) {
          renderError(card, error.message);
        }
        completed += 1;
        setStatus(`Loaded ${completed}/${entries.length}`);
      }

      setStatus(`Completed ${completed}/${entries.length}`);
    }

    document.getElementById("downloadAllBtn").addEventListener("click", () => {
      const entries = parseEntries();
      if (!entries.length) {
        return;
      }
      window.open(buildBatchDownloadUrl(entries), "_blank");
    });
    document.getElementById("reloadBtn").addEventListener("click", loadResults);
    loadResults();
  </script>
</body>
</html>
)HTML";

std::string build_index_html(const AppConfig& cfg) {
  std::string html = kIndexHtml;
  replace_all(html, "__ROOT__", json_escape(canonical_or_absolute(cfg.root).string()));
  replace_all(html, "__GRIB_EXTENSION_ENABLED__",
    cfg.grib_extension_enabled ? "true" : "false");
  std::ostringstream fields;
  fields << '[';
  bool first = true;
  if (cfg.metadata_config) {
    for (const auto& field : cfg.metadata_config->fields()) {
      if (field.xattr.empty()) continue;
      if (!first) fields << ',';
      first = false;
      fields << "{\"id\":\"" << json_escape(field.id)
        << "\",\"xattr\":\"" << json_escape(field.xattr)
        << "\",\"queryable\":" << (field.queryable ? "true" : "false") << '}';
    }
  }
  fields << ']';
  std::string descriptors = fields.str();
  replace_all(descriptors, "<", "\\u003c");
  replace_all(html, "__METADATA_FIELDS__", descriptors);
  return html;
}

std::string build_viewer_html() {
  return kViewerHtml;
}

std::string build_batch_viewer_html() {
  return kBatchViewerHtml;
}

void handle_client(int fd, const AppConfig& cfg) {
  HttpRequest req;
  int failure_status = 400;
  if (!read_http_request(fd, cfg, req, failure_status)) {
    const char* reason = failure_status == 408 ? "Request Timeout" :
      (failure_status == 413 ? "Payload Too Large" :
      (failure_status == 431 ? "Request Header Fields Too Large" : "Bad Request"));
    send_response(fd, failure_status, reason, "text/plain; charset=utf-8",
      reason);
    return;
  }

  const bool head_only = req.method == "HEAD";
  const bool is_get_like = req.method == "GET" || head_only;
  const bool is_upload_post = req.method == "POST" && req.route == "/api/upload";
  const bool is_mkdir_post = req.method == "POST" && req.route == "/api/mkdir";
  const bool is_delete_post = req.method == "POST" &&
    (req.route == "/api/delete" || req.route == "/api/delete-batch");
  if (!is_get_like && !is_upload_post && !is_mkdir_post && !is_delete_post) {
    send_response(fd, 405, "Method Not Allowed", "text/plain; charset=utf-8",
      "Only GET, HEAD, POST /api/upload, POST /api/mkdir and POST delete endpoints are supported",
      { { "Allow", "GET, HEAD, POST" } });
    return;
  }

  const bool mutating = is_upload_post || is_mkdir_post || is_delete_post;
  if ((cfg.require_auth_all || mutating) && !request_is_authenticated(req, cfg)) {
    send_response(fd, 401, "Unauthorized", "text/plain; charset=utf-8",
      "Authentication required", { { "WWW-Authenticate", "Bearer" } });
    return;
  }
  if (mutating && !request_origin_is_allowed(req, cfg)) {
    send_json_error(fd, 403, "Forbidden", "Cross-origin write requests are not allowed");
    return;
  }

  if (req.route == "/" || req.route == "/index.html") {
    const std::string html = build_index_html(cfg);
    send_response(fd, 200, "OK", "text/html; charset=utf-8", html, {},
      !head_only);
    return;
  }
  if (req.route == "/viewer") {
    const std::string html = build_viewer_html();
    send_response(fd, 200, "OK", "text/html; charset=utf-8", html, {},
      !head_only);
    return;
  }
  if (req.route == "/viewer-batch") {
    const std::string html = build_batch_viewer_html();
    send_response(fd, 200, "OK", "text/html; charset=utf-8", html, {},
      !head_only);
    return;
  }
  if (req.route == "/api/list") {
    handle_list_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/search") {
    handle_search_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/preview") {
    handle_preview_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/viewer-info") {
    handle_viewer_info_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/metadata") {
    handle_metadata_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/upload") {
    handle_upload_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/mkdir") {
    handle_mkdir_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/delete") {
    handle_delete_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/delete-batch") {
    handle_batch_delete_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/download") {
    handle_download_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/api/download-batch") {
    handle_batch_download_api(fd, cfg, req, head_only);
    return;
  }
  if (req.route == "/favicon.ico") {
    send_response(fd, 204, "No Content", "text/plain; charset=utf-8", "", {},
      !head_only);
    return;
  }

  send_response(fd, 404, "Not Found", "text/plain; charset=utf-8", "Not Found");
}

void print_help(const char* argv0) {
  std::cout
    << "Usage: " << argv0 <<
      " [--root PATH] [--host HOST] [--port PORT] [--workers N] [--auth-token TOKEN]\n"
    << "\n"
    << "Start a lightweight web file explorer for the IoTDB-FS mount point.\n"
    << "\n"
    << "Options:\n"
    << "  --root PATH   Browse this directory instead of mount.point\n"
    << "  --host HOST   Bind address, default 127.0.0.1\n"
    << "  --port PORT   Listen port, default 8080\n"
    << "  --workers N   Worker threads for request handling, default CPU count / 4\n"
    << "  --auth-token TOKEN  Require Bearer token for mutating API requests\n"
    << "  --require-auth-all BOOL  Require token for read and write requests\n"
    << "  --max-header-bytes N  Maximum HTTP header bytes\n"
    << "  --max-body-bytes N  Maximum HTTP request body bytes\n"
    << "  --max-upload-bytes N  Maximum upload bytes\n"
    << "  --socket-timeout-ms N  Client socket timeout\n"
    << "  --max-queue N  Maximum queued clients\n"
    << "  --help        Show this message\n";
}

void load_configuration(AppConfig& cfg) {
  const char* config_file_env = std::getenv("IOTDB_CONFIG_FILE");
  fs::path config_path;
  if (config_file_env && *config_file_env) {
    config_path = fs::path(config_file_env);
  }
  else {
    const char* conf_dir_env = std::getenv("IOTDB_CONF_DIR");
    const std::string conf_dir =
      (conf_dir_env && *conf_dir_env) ? conf_dir_env : "resources";
    config_path = fs::path(conf_dir) / "config.properties";
  }

  const PropertiesResult props = load_properties_file(config_path.string());
  cfg.config_path = config_path.string();
  cfg.config_loaded = props.loaded;
  cfg.grib_extension_enabled = iotdbfs::grib_extension_enabled_from_env(
    iotdbfs::grib_extension_enabled_from_properties(props.values, true));
  const auto metadata_result = iotdbfs::load_metadata_config(config_path.string());
  if (!metadata_result.ok()) {
    cfg.metadata_config_error = metadata_result.error();
  } else {
    cfg.metadata_config = metadata_result.value();
  }

  auto get_prop = [&](const std::string& key) -> std::string {
    auto it = props.values.find(key);
    return it == props.values.end() ? std::string() : it->second;
    };

  std::string root = get_prop("mount.point");
  if (root.empty())
    root = env_or("IOTDB_MOUNT_POINT", "/mnt/dfs");
  cfg.root = root;

  std::string host = get_prop("web.host");
  if (host.empty())
    host = env_or("IOTDB_WEB_HOST", "127.0.0.1");
  if (host == "localhost")
    host = "127.0.0.1";
  cfg.host = host;

  std::string port = get_prop("web.port");
  if (port.empty())
    port = env_or("IOTDB_WEB_PORT", "8080");
  cfg.port = parse_int_or(port, 8080);
  if (cfg.port <= 0)
    cfg.port = 8080;

  std::string workers = get_prop("web.workers");
  if (workers.empty())
    workers = env_or("IOTDB_WEB_WORKERS", "");
  cfg.workers = workers.empty()
    ? default_worker_count()
    : parse_int_or(workers, default_worker_count());
  if (cfg.workers <= 0)
    cfg.workers = default_worker_count();
  cfg.workers = std::min(cfg.workers, 128);

  auto value_or_env = [&](const std::string& key, const char* env_key) {
    const std::string value = get_prop(key);
    return value.empty() ? env_or(env_key, "") : value;
  };
  const std::string max_header = value_or_env("web.max_header_bytes",
    "IOTDB_WEB_MAX_HEADER_BYTES");
  if (!max_header.empty() && !parse_bounded_size(max_header,
    kDefaultMaxHttpHeaderBytes, 1024, kAbsoluteMaxHeaderBytes,
    cfg.max_header_bytes)) {
    cfg.config_error = "Invalid web.max_header_bytes; using default";
    cfg.max_header_bytes = kDefaultMaxHttpHeaderBytes;
  }
  const std::string max_body = value_or_env("web.max_body_bytes",
    "IOTDB_WEB_MAX_BODY_BYTES");
  if (!max_body.empty() && !parse_bounded_size(max_body,
    kDefaultMaxHttpBodyBytes, 1, kAbsoluteMaxBodyBytes, cfg.max_body_bytes)) {
    cfg.config_error = "Invalid web.max_body_bytes; using default";
    cfg.max_body_bytes = kDefaultMaxHttpBodyBytes;
  }
  const std::string max_upload = value_or_env("web.max_upload_bytes",
    "IOTDB_WEB_MAX_UPLOAD_BYTES");
  if (!max_upload.empty() && !parse_bounded_size(max_upload,
    kDefaultMaxUploadBytes, 1, kAbsoluteMaxBodyBytes, cfg.max_upload_bytes)) {
    cfg.config_error = "Invalid web.max_upload_bytes; using default";
    cfg.max_upload_bytes = kDefaultMaxUploadBytes;
  }
  cfg.max_upload_bytes = std::min(cfg.max_upload_bytes, cfg.max_body_bytes);
  const std::string timeout = value_or_env("web.socket_timeout_ms",
    "IOTDB_WEB_SOCKET_TIMEOUT_MS");
  if (!timeout.empty() && !parse_bounded_int(timeout, kDefaultSocketTimeoutMs,
    1000, kMaxSocketTimeoutMs, cfg.socket_timeout_ms)) {
    cfg.config_error = "Invalid web.socket_timeout_ms; using default";
    cfg.socket_timeout_ms = kDefaultSocketTimeoutMs;
  }
  const std::string max_queue = value_or_env("web.max_queue",
    "IOTDB_WEB_MAX_QUEUE");
  if (!max_queue.empty() && !parse_bounded_size(max_queue,
    kDefaultMaxQueuedClients, 1, kMaxQueuedClientsLimit, cfg.max_queue)) {
    cfg.config_error = "Invalid web.max_queue; using default";
    cfg.max_queue = kDefaultMaxQueuedClients;
  }

  const std::string require_auth = value_or_env("web.require_auth_all",
    "IOTDB_WEB_REQUIRE_AUTH_ALL");
  if (!require_auth.empty() && !parse_bool_value(require_auth,
    cfg.require_auth_all)) {
    cfg.config_error = "Invalid web.require_auth_all; using default";
    cfg.require_auth_all = false;
  }

  cfg.auth_token = get_prop("web.auth_token");
  if (cfg.auth_token.empty())
    cfg.auth_token = env_or("IOTDB_WEB_AUTH_TOKEN", "");
}

bool apply_cli_args(int argc, char* argv[], AppConfig& cfg) {
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--help" || arg == "-h") {
      print_help(argv[0]);
      return false;
    }
    if ((arg == "--root" || arg == "--host" || arg == "--port" ||
      arg == "--workers" || arg == "--auth-token" ||
      arg == "--require-auth-all" || arg == "--max-header-bytes" ||
      arg == "--max-body-bytes" || arg == "--max-upload-bytes" ||
      arg == "--socket-timeout-ms" || arg == "--max-queue") && i + 1 >= argc) {
      std::cerr << arg << " requires a value\n";
      return false;
    }
    if (arg == "--root") {
      cfg.root = argv[++i];
      continue;
    }
    if (arg == "--host") {
      cfg.host = argv[++i];
      if (cfg.host == "localhost")
        cfg.host = "127.0.0.1";
      continue;
    }
    if (arg == "--port") {
      if (!parse_bounded_int(argv[++i], cfg.port, 1, 65535, cfg.port))
        return false;
      continue;
    }
    if (arg == "--workers") {
      if (!parse_bounded_int(argv[++i], cfg.workers, 1, 128, cfg.workers))
        return false;
      continue;
    }
    if (arg == "--auth-token") {
      cfg.auth_token = argv[++i];
      continue;
    }
    if (arg == "--require-auth-all") {
      if (!parse_bool_value(argv[++i], cfg.require_auth_all))
        return false;
      continue;
    }
    if (arg == "--max-header-bytes") {
      if (!parse_bounded_size(argv[++i], cfg.max_header_bytes, 1024,
        kAbsoluteMaxHeaderBytes, cfg.max_header_bytes))
        return false;
      continue;
    }
    if (arg == "--max-body-bytes") {
      if (!parse_bounded_size(argv[++i], cfg.max_body_bytes, 1,
        kAbsoluteMaxBodyBytes, cfg.max_body_bytes))
        return false;
      continue;
    }
    if (arg == "--max-upload-bytes") {
      if (!parse_bounded_size(argv[++i], cfg.max_upload_bytes, 1,
        kAbsoluteMaxBodyBytes, cfg.max_upload_bytes) ||
        cfg.max_upload_bytes > cfg.max_body_bytes)
        return false;
      continue;
    }
    if (arg == "--socket-timeout-ms") {
      if (!parse_bounded_int(argv[++i], cfg.socket_timeout_ms, 100,
        kMaxSocketTimeoutMs, cfg.socket_timeout_ms))
        return false;
      continue;
    }
    if (arg == "--max-queue") {
      if (!parse_bounded_size(argv[++i], cfg.max_queue, 1,
        kMaxQueuedClientsLimit, cfg.max_queue))
        return false;
      continue;
    }
    std::cerr << "Unknown argument: " << arg << "\n";
    print_help(argv[0]);
    return false;
  }
  return true;
}

int open_server_socket(const AppConfig& cfg) {
  sockaddr_in probe{};
  probe.sin_family = AF_INET;
  if (::inet_pton(AF_INET, cfg.host.c_str(), &probe.sin_addr) != 1) {
    log_line("ERROR", "Only IPv4 bind addresses are supported: " + cfg.host);
    return -1;
  }
  const bool loopback = (ntohl(probe.sin_addr.s_addr) >> 24) == 127;
  if ((cfg.require_auth_all || !loopback) && cfg.auth_token.empty()) {
    log_line("ERROR", "Authentication is required but no web auth token is configured");
    return -1;
  }
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    log_line("ERROR", std::string("socket() failed: ") + std::strerror(errno));
    return -1;
  }

  int reuse = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(cfg.port));
  addr.sin_addr = probe.sin_addr;

  if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    log_line("ERROR", std::string("bind() failed: ") + std::strerror(errno));
    ::close(fd);
    return -1;
  }
  if (::listen(fd, 64) < 0) {
    log_line("ERROR", std::string("listen() failed: ") + std::strerror(errno));
    ::close(fd);
    return -1;
  }
  return fd;
}

} // namespace

int main(int argc, char* argv[]) {
  std::signal(SIGPIPE, SIG_IGN);

  AppConfig cfg;
  load_configuration(cfg);
  if (!cfg.metadata_config_error.empty()) {
    log_line("ERROR", "Invalid metadata configuration: " + cfg.metadata_config_error);
    return 1;
  }
  if (!apply_cli_args(argc, argv, cfg))
    return cfg.config_error.empty() ? 0 : 1;

  if (!cfg.config_error.empty()) {
    log_line("ERROR", cfg.config_error);
    return 1;
  }
  if (!is_loopback_host(cfg.host))
    cfg.require_auth_all = true;

  const fs::path root_real = canonical_or_absolute(cfg.root);
  if (cfg.config_loaded) {
    log_line("INFO", "Loaded configuration from " + cfg.config_path);
  }
  else {
    log_line("WARN", "Configuration file not found at " + cfg.config_path +
      ", using defaults/environment variables");
  }
  if (!cfg.config_error.empty())
    log_line("WARN", cfg.config_error);
  log_line("INFO", "Web explorer root: " + root_real.string());
  log_line("INFO", "Binding address: " + cfg.host + ":" + std::to_string(cfg.port));
  log_line("INFO", "Request worker threads: " + std::to_string(cfg.workers));
  if (!fs::exists(root_real)) {
    log_line("WARN", "Root path does not exist yet: " + root_real.string());
  }

  const int server_fd = open_server_socket(cfg);
  if (server_fd < 0)
    return 1;

  log_line("INFO", "Open http://" + cfg.host + ":" + std::to_string(cfg.port) +
    "/ in your browser");

  std::mutex queue_mu;
  std::condition_variable queue_cv;
  std::deque<int> client_queue;
  bool stopping = false;
  std::vector<std::thread> workers;
  workers.reserve(static_cast<size_t>(cfg.workers));

  for (int i = 0; i < cfg.workers; ++i) {
    workers.emplace_back([&]() {
      while (true) {
        int client_fd = -1;
        {
          std::unique_lock<std::mutex> lock(queue_mu);
          queue_cv.wait(lock, [&]() {
            return stopping || !client_queue.empty();
          });
          if (client_queue.empty()) {
            if (stopping)
              return;
            continue;
          }
          client_fd = client_queue.front();
          client_queue.pop_front();
        }

        handle_client(client_fd, cfg);
        ::close(client_fd);
      }
    });
  }

  while (true) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    const int client_fd =
      ::accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
      if (errno == EINTR)
        continue;
      log_line("ERROR", std::string("accept() failed: ") + std::strerror(errno));
      break;
    }
    timeval timeout{};
    timeout.tv_sec = std::max(1, cfg.socket_timeout_ms / 1000);
    ::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    bool queued = false;
    {
      std::lock_guard<std::mutex> lock(queue_mu);
      if (client_queue.size() < cfg.max_queue) {
        client_queue.push_back(client_fd);
        queued = true;
      }
    }
    if (queued) {
      queue_cv.notify_one();
    }
    else {
      const std::string body = "Service Unavailable\n";
      send_response(client_fd, 503, "Service Unavailable",
        "text/plain; charset=utf-8", body);
      ::close(client_fd);
    }
  }

  {
    std::lock_guard<std::mutex> lock(queue_mu);
    stopping = true;
  }
  queue_cv.notify_all();
  for (auto& worker : workers) {
    if (worker.joinable())
      worker.join();
  }

  ::close(server_fd);
  return 0;
}
