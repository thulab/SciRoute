#include "iotdbfs/formats/grib/GribExtensionService.h"

#include "iotdbfs/Logging.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/QueryUtils.h"
#include "iotdbfs/MetadataProjector.h"
#include "iotdbfs/formats/grib/gribUtils.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

namespace iotdbfs {
namespace {

std::vector<std::string> split_query_chain(const std::string& query) {
  std::vector<std::string> parts;
  size_t i = 0;
  while (i <= query.size()) {
    size_t j = query.find('?', i);
    if (j == std::string::npos)
      j = query.size();
    std::string part = query.substr(i, j - i);
    if (!part.empty())
      parts.emplace_back(std::move(part));
    i = j + 1;
    if (j >= query.size())
      break;
  }
  return parts;
}

std::string normalize_index_key(const std::string& key) {
  if (key == "sn")
    return "shortName";
  if (key == "sr")
    return "stepRange";
  if (key == "tl")
    return "typeOfLevel";
  if (key == "lv")
    return "level";
  if (key == "st")
    return "step";
  if (key == "u")
    return "unit";
  if (key == "gd")
    return "globalDomain";
  if (key == "np")
    return "numberOfDataPoints";
  if (key == "of")
    return "offset";
  if (key == "ln")
    return "totalLength";
  if (key == "ed")
    return "edition";
  return key;
}

std::string json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 8);
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

bool parse_index_line_kv(const std::string& line,
  std::vector<std::pair<std::string, std::string>>& out) {
  size_t i = 0;
  auto skip_ws = [&]() {
    while (i < line.size() && std::isspace(static_cast<unsigned char>(line[i])))
      ++i;
  };
  auto parse_string = [&](std::string& s) -> bool {
    if (i >= line.size() || line[i] != '"')
      return false;
    ++i;
    std::string tmp;
    while (i < line.size()) {
      char c = line[i++];
      if (c == '"')
        break;
      if (c == '\\' && i < line.size()) {
        char n = line[i++];
        switch (n) {
        case '"':
        case '\\':
        case '/':
          tmp.push_back(n);
          break;
        case 'b':
          tmp.push_back('\b');
          break;
        case 'f':
          tmp.push_back('\f');
          break;
        case 'n':
          tmp.push_back('\n');
          break;
        case 'r':
          tmp.push_back('\r');
          break;
        case 't':
          tmp.push_back('\t');
          break;
        default:
          tmp.push_back(n);
          break;
        }
      }
      else {
        tmp.push_back(c);
      }
    }
    s = std::move(tmp);
    return true;
  };

  skip_ws();
  if (i >= line.size() || line[i] != '{')
    return false;
  ++i;
  while (i < line.size()) {
    skip_ws();
    if (i < line.size() && line[i] == '}') {
      ++i;
      break;
    }
    std::string key;
    if (!parse_string(key))
      break;
    skip_ws();
    if (i >= line.size() || line[i] != ':')
      break;
    ++i;
    skip_ws();
    std::string value;
    if (i < line.size() && line[i] == '"') {
      if (!parse_string(value))
        break;
    }
    else {
      size_t start = i;
      while (i < line.size() && line[i] != ',' && line[i] != '}')
        ++i;
      size_t end = i;
      while (end > start &&
        std::isspace(static_cast<unsigned char>(line[end - 1])))
        --end;
      value = (end > start) ? line.substr(start, end - start) : std::string();
    }
    out.emplace_back(std::move(key), std::move(value));
    skip_ws();
    if (i < line.size() && line[i] == ',') {
      ++i;
      continue;
    }
    if (i < line.size() && line[i] == '}') {
      ++i;
      break;
    }
  }
  return !out.empty();
}

std::map<std::string, std::set<std::string>>
collect_index_key_values(const std::string& index_text) {
  std::map<std::string, std::set<std::string>> out;
  size_t start = 0;
  while (start < index_text.size()) {
    size_t nl = index_text.find('\n', start);
    std::string line = (nl == std::string::npos)
      ? index_text.substr(start)
      : index_text.substr(start, nl - start);
    start = (nl == std::string::npos) ? index_text.size() : nl + 1;
    if (line.empty())
      continue;
    std::vector<std::pair<std::string, std::string>> kvs;
    if (!parse_index_line_kv(line, kvs))
      continue;
    for (auto& kv : kvs) {
      std::string key = normalize_index_key(kv.first);
      out[key].insert(kv.second);
    }
  }
  return out;
}

std::string make_cache_token(long long row_time,
  const std::string& file_path) {
  return std::to_string(row_time) + "|" + path_utils::norm(file_path);
}

grib_utils::MsgInfo find_msg_info_in_index(const std::string& index_text,
  const std::map<std::string, std::string>& user_kv,
  const std::string& path) {
  auto kv = query_utils::filter_index_match_kv(user_kv);
  if (kv.empty())
    return grib_utils::MsgInfo{};
  auto matches = grib_utils::find_all_msg_matches(index_text, kv, 2);
  if (matches.size() > 1) {
    LOG_WARN("ambiguous GRIB message selection path=" + path +
      "; add step, stepRange, or another message field");
    return grib_utils::MsgInfo{};
  }
  return matches.size() == 1 ? matches.front().info : grib_utils::MsgInfo{};
}

enum class CanonicalType { String, Int64, Double };

struct CanonicalGribField {
  const char* id;
  CanonicalType type;
};

constexpr CanonicalGribField kCanonicalGribFields[] = {
  {"shortName", CanonicalType::String},
  {"typeOfProcessedData", CanonicalType::String},
  {"typeOfGeneratingProcess", CanonicalType::Int64},
  {"typeOfLevel", CanonicalType::String},
  {"centre", CanonicalType::Int64},
  {"generatingProcessIdentifier", CanonicalType::Int64},
  {"discipline", CanonicalType::Int64},
  {"parameterCategory", CanonicalType::Int64},
  {"parameterNumber", CanonicalType::Int64},
  {"unit", CanonicalType::String},
  {"latitudeOfFirstGridPoint", CanonicalType::Double},
  {"longitudeOfFirstGridPoint", CanonicalType::Double},
  {"latitudeOfLastGridPoint", CanonicalType::Double},
  {"longitudeOfLastGridPoint", CanonicalType::Double},
  {"globalDomain", CanonicalType::String},
  {"iDirectionIncrement", CanonicalType::Double},
  {"jDirectionIncrement", CanonicalType::Double},
  {"Ni", CanonicalType::Int64},
  {"Nj", CanonicalType::Int64},
  {"numberOfDataPoints", CanonicalType::Int64},
  {"level", CanonicalType::Int64},
  {"levelMin", CanonicalType::Int64},
  {"levelMax", CanonicalType::Int64},
  {"step", CanonicalType::Int64},
  {"stepMin", CanonicalType::Int64},
  {"stepMax", CanonicalType::Int64},
  {"stepRange", CanonicalType::String},
  {"edition", CanonicalType::Int64},
};

bool has_canonical_type(const MetadataValue& value, CanonicalType type) {
  switch (type) {
    case CanonicalType::String:
      return value.type() == MetadataValueType::String ||
        value.type() == MetadataValueType::StringSet;
    case CanonicalType::Int64:
      return value.type() == MetadataValueType::Int64 ||
        value.type() == MetadataValueType::Int64Set;
    case CanonicalType::Double:
      return value.type() == MetadataValueType::Double ||
        value.type() == MetadataValueType::DoubleSet;
  }
  return false;
}

void append_canonical_xattrs(const MetadataMap& values,
  GribXattrList& attrs) {
  for (const auto& field : kCanonicalGribFields) {
    const auto found = values.find(field.id);
    if (found == values.end() ||
        !has_canonical_type(found->second, field.type))
      continue;
    std::string text;
    try {
      text = metadata_value_text(found->second);
    } catch (...) {
      continue;
    }
    if (field.type == CanonicalType::String && text.empty())
      continue;
    attrs.emplace_back(std::string("user.grib.") + field.id,
      std::move(text));
  }
}

} // namespace

void configure_grib_message_cache(std::size_t max_entries) {
  grib_utils::configure_message_cache(max_entries);
}

GribExtensionService::GribExtensionService(bool enabled, Dependencies deps)
  : enabled_(enabled), deps_(std::move(deps)) {}

bool GribExtensionService::enabled_for_path(const std::string& path) const {
  (void)path;
  // Format identity is established by content at ingestion and the registry
  // dispatches to this service. A later rename must not disable GRIB behavior.
  return enabled_;
}

bool GribExtensionService::has_non_type_params(
  const std::map<std::string, std::string>& kv) const {
  for (const auto& it : kv) {
    if (path_utils::to_lower(it.first) != "type")
      return true;
  }
  return false;
}

GribQuerySpec
GribExtensionService::parse_query_spec(const std::string& query) const {
  GribQuerySpec out;
  if (query.empty()) {
    out.ok = true;
    out.type = GribVirtualType::None;
    return out;
  }

  auto parts = split_query_chain(query);
  bool has_function = false;
  std::vector<std::map<std::string, std::string>> kv_parts;
  kv_parts.reserve(parts.size());
  for (const auto& part : parts) {
    auto kv = query_utils::parse_kv_query(part);
    if (kv.find("function") != kv.end())
      has_function = true;
    kv_parts.emplace_back(std::move(kv));
  }

  if (!has_function) {
    out.ok = false;
    return out;
  }

  out.is_function_chain = true;
  out.type = GribVirtualType::Data;
  std::map<std::string, std::string> merged;
  std::map<std::string, std::string> read_kv;
  bool has_read = false;

  auto erase_if_present = [](std::map<std::string, std::string>& kv,
                              const char* key) {
    auto it = kv.find(key);
    if (it != kv.end())
      kv.erase(it);
  };
  auto erase_crop_keys = [&](std::map<std::string, std::string>& kv) {
    erase_if_present(kv, "lat0");
    erase_if_present(kv, "lon0");
    erase_if_present(kv, "lat1");
    erase_if_present(kv, "lon1");
  };
  auto erase_interp_keys = [&](std::map<std::string, std::string>& kv) {
    erase_if_present(kv, "interp");
    erase_if_present(kv, "outNi");
    erase_if_present(kv, "outNj");
    erase_if_present(kv, "outni");
    erase_if_present(kv, "outnj");
    erase_if_present(kv, "dlon");
    erase_if_present(kv, "dlat");
    erase_if_present(kv, "border");
  };

  for (auto& kv : kv_parts) {
    if (kv.empty())
      continue;
    auto it_fn = kv.find("function");
    if (it_fn == kv.end()) {
      out.ok = false;
      return out;
    }
    std::string fn = path_utils::to_lower(it_fn->second);
    kv.erase(it_fn);

    if (fn == "read") {
      has_read = true;
      auto it_type = kv.find("type");
      if (it_type != kv.end()) {
        std::string v = path_utils::to_lower(it_type->second);
        if (v.empty() || v == "binary")
          out.type = GribVirtualType::Binary;
        else if (v == "data")
          out.type = GribVirtualType::Data;
        else {
          out.ok = false;
          return out;
        }
        kv.erase(it_type);
      }
      erase_crop_keys(kv);
      erase_interp_keys(kv);
      for (const auto& it : kv)
        read_kv[it.first] = it.second;
    }
    else if (fn == "crop") {
      if (kv.find("type") != kv.end()) {
        out.ok = false;
        return out;
      }
      erase_interp_keys(kv);
    }
    else if (fn == "interp") {
      if (kv.find("type") != kv.end()) {
        out.ok = false;
        return out;
      }
      erase_crop_keys(kv);
      if (kv.find("interp") == kv.end())
        kv["interp"] = "bilinear";
    }
    else {
      out.ok = false;
      return out;
    }

    for (const auto& it : kv)
      merged[it.first] = it.second;
  }

  if (!has_read) {
    out.ok = false;
    return out;
  }
  if (query_utils::filter_index_match_kv(read_kv).empty()) {
    out.ok = false;
    return out;
  }

  out.kv_all = std::move(merged);
  out.kv_idx = std::move(read_kv);
  grib_utils::strip_crop_keys(out.kv_idx);
  out.ok = true;
  return out;
}

GribXattrList
GribExtensionService::build_inode_xattrs(const Inode& inode) const {
  if (inode.metadata.empty())
    return {};

  auto parsed = metadata_cache_.get_or_deserialize(
    inode.row_time, inode.path, inode.metadata);
  if (!parsed.ok())
    return {};

  GribXattrList attrs;
  attrs.reserve(20);
  append_canonical_xattrs(parsed.value(), attrs);
  if (deps_.metadata_config) {
    MetadataProjector projector(*deps_.metadata_config);
    for (const auto& dynamic : projector.to_xattrs(parsed.value())) {
      auto existing = std::find_if(attrs.begin(), attrs.end(),
        [&](const auto& attr) { return attr.first == dynamic.first; });
      if (existing == attrs.end()) attrs.push_back(dynamic);
      else existing->second = dynamic.second;
    }
  }
  return attrs;
}

void GribExtensionService::append_index_xattrs(const std::string& index_text,
  GribXattrList& attrs) const {
  static const char kIndexKeysAttr[] = "user.grib.index.keys";
  static const char kIndexValuesAttr[] = "user.grib.index.values";

  auto kv_map = collect_index_key_values(index_text);
  if (kv_map.empty())
    return;

  std::string keys;
  for (auto it = kv_map.begin(); it != kv_map.end(); ++it) {
    if (it->first == "offset" || it->first == "totalLength")
      continue;
    if (it != kv_map.begin())
      keys += ",";
    keys += it->first;
  }

  std::ostringstream os;
  os << "{";
  bool first_key = true;
  for (const auto& kv : kv_map) {
    if (kv.first == "offset" || kv.first == "totalLength")
      continue;
    if (!first_key)
      os << ",";
    first_key = false;
    os << "\"" << json_escape(kv.first) << "\":[";
    bool first_val = true;
    for (const auto& val : kv.second) {
      if (!first_val)
        os << ",";
      first_val = false;
      os << "\"" << json_escape(val) << "\"";
    }
    os << "]";
  }
  os << "}";

  attrs.emplace_back(kIndexKeysAttr, std::move(keys));
  attrs.emplace_back(kIndexValuesAttr, os.str());
}

void GribExtensionService::append_index_xattr_names(GribXattrList& attrs) const {
  static const char kIndexKeysAttr[] = "user.grib.index.keys";
  static const char kIndexValuesAttr[] = "user.grib.index.values";
  attrs.emplace_back(kIndexKeysAttr, std::string());
  attrs.emplace_back(kIndexValuesAttr, std::string());
}

bool GribExtensionService::can_handle_virtual_query(
  const std::string& path) const {
  if (!enabled_for_path(path))
    return false;
  if (deps_.is_path_in_writing && deps_.is_path_in_writing(path))
    return false;
  return true;
}

GribVirtualAttr GribExtensionService::get_virtual_attr(long long row_time,
  const std::string& path, const GribQuerySpec& spec,
  const std::string& raw_query) const {
  (void)raw_query;
  GribVirtualAttr out;
  if (!spec.ok || spec.type == GribVirtualType::None)
    return out;

  if (spec.type == GribVirtualType::Binary) {
    if (!has_non_type_params(spec.kv_all)) {
      out.ok = true;
      out.size = 0;
      return out;
    }
    if (!deps_.read_layout_text)
      return out;
    auto idx = deps_.read_layout_text(row_time, path);
    if (!idx || idx->empty())
      return out;
    auto mi = find_msg_info_in_index(*idx,
      spec.kv_idx.empty() ? spec.kv_all : spec.kv_idx, path);
    if (!mi.ok)
      return out;
    out.ok = true;
    out.size = mi.len;
    return out;
  }

  if (spec.type != GribVirtualType::Data)
    return out;
  if (!has_non_type_params(spec.kv_all)) {
    out.ok = true;
    out.size = 0;
    return out;
  }
  if (!deps_.read_layout_text || !deps_.read_object_part)
    return out;
  auto idx = deps_.read_layout_text(row_time, path);
  if (!idx || idx->empty())
    return out;

  auto kv_idx = spec.kv_idx.empty() ? spec.kv_all : spec.kv_idx;
  auto mi = find_msg_info_in_index(*idx, kv_idx, path);
  if (!mi.ok)
    return out;

  auto dtype = grib_utils::parse_dtype_from_kv(spec.kv_all);
  auto crop = grib_utils::parse_crop_params_from_kv(spec.kv_all);
  auto interp = grib_utils::parse_interp_params_from_kv(spec.kv_all);
  auto cache_token = make_cache_token(row_time, path);
  grib_utils::ReadObjectPartFunc reader =
    [&](std::uint64_t off, std::uint64_t len) {
      return deps_.read_object_part(row_time, path, off, len);
    };

  std::size_t dtype_size = grib_utils::dtype_bytes(dtype);
  if (dtype_size == 0)
    return out;

  if (interp.enabled) {
    auto bytes = grib_utils::decode_crop_and_bilinear_resample(
      mi, dtype, crop, interp, reader, cache_token);
    if (!bytes)
      return out;
    out.ok = true;
    out.size = bytes->size();
    return out;
  }

  std::optional<std::size_t> count;
  if (crop.enabled)
    count = grib_utils::compute_cropped_value_count(
      mi, crop, reader, cache_token);
  else
    count = grib_utils::compute_values_count_for_msg(mi, reader, cache_token);
  if (!count)
    return out;
  out.ok = true;
  out.size = (*count) * dtype_size;
  return out;
}

GribVirtualOpenResult GribExtensionService::open_virtual(long long row_time,
  const std::string& path, const GribQuerySpec& spec,
  const std::string& raw_query) const {
  (void)raw_query;
  GribVirtualOpenResult out;
  if (!spec.ok || spec.type == GribVirtualType::None)
    return out;

  if (spec.type == GribVirtualType::Binary) {
    std::uint64_t off = 0;
    std::uint64_t len = 0;
    if (has_non_type_params(spec.kv_all)) {
      if (!deps_.read_layout_text)
        return out;
      auto idx = deps_.read_layout_text(row_time, path);
      if (!idx || idx->empty())
        return out;
      auto mi = find_msg_info_in_index(*idx,
        spec.kv_idx.empty() ? spec.kv_all : spec.kv_idx, path);
      if (!mi.ok)
        return out;
      off = mi.off;
      len = mi.len;
    }
    out.ok = true;
    out.type = GribVirtualType::Binary;
    out.binary.offset = off;
    out.binary.length = len;
    return out;
  }

  if (spec.type != GribVirtualType::Data)
    return out;
  if (!has_non_type_params(spec.kv_all))
    return out;
  if (!deps_.read_layout_text || !deps_.read_object_part)
    return out;
  auto idx = deps_.read_layout_text(row_time, path);
  if (!idx || idx->empty())
    return out;

  auto kv_idx = spec.kv_idx.empty() ? spec.kv_all : spec.kv_idx;
  auto mi = find_msg_info_in_index(*idx, kv_idx, path);
  if (!mi.ok)
    return out;

  auto dtype = grib_utils::parse_dtype_from_kv(spec.kv_all);
  auto crop = grib_utils::parse_crop_params_from_kv(spec.kv_all);
  auto interp = grib_utils::parse_interp_params_from_kv(spec.kv_all);
  auto cache_token = make_cache_token(row_time, path);
  grib_utils::ReadObjectPartFunc reader =
    [&](std::uint64_t off, std::uint64_t len) {
      return deps_.read_object_part(row_time, path, off, len);
    };

  std::optional<std::vector<std::uint8_t>> bytes;
  if (interp.enabled) {
    bytes = grib_utils::decode_crop_and_bilinear_resample(
      mi, dtype, crop, interp, reader, cache_token);
  }
  else if (crop.enabled) {
    bytes = grib_utils::decode_and_maybe_crop_values(
      mi, dtype, crop, reader, cache_token);
  }
  else {
    bytes = grib_utils::decode_values_as_dtype(
      mi, dtype, reader, cache_token);
  }
  if (!bytes)
    return out;
  out.ok = true;
  out.type = GribVirtualType::Data;
  out.data.bytes = std::move(*bytes);
  return out;
}

} // namespace iotdbfs
