#include "iotdbfs/formats/grib/gribUtils.h"

#include "iotdbfs/Logging.h"
#include "iotdbfs/PathUtils.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <chrono>
#include <memory>

namespace grib_utils {

namespace {

std::recursive_mutex g_eccodes_mutex;
struct EccThreadCtx {
  EccThreadCtx() = default;
  ~EccThreadCtx() = default;
};
thread_local EccThreadCtx g_ecc_tls;

struct HandleHolder {
  codes_handle* handle{ nullptr };
  explicit HandleHolder(codes_handle* h = nullptr)
    : handle(h) {
  }
  HandleHolder(const HandleHolder&) = delete;
  HandleHolder& operator=(const HandleHolder&) = delete;
  ~HandleHolder() {
    if (handle)
      codes_handle_delete(handle);
  }
  codes_handle* get() const { return handle; }
  codes_handle* release() {
    codes_handle* tmp = handle;
    handle = nullptr;
    return tmp;
  }
};

codes_context* get_ecc_ctx() {
  return codes_context_get_default();
}

class MessageCache {
public:
  static MessageCache& instance() {
    static MessageCache inst;
    return inst;
  }

  void set_max_entries(size_t n) {
    std::lock_guard<std::mutex> lk(mu_);
    max_entries_ = std::max<size_t>(1, n);
    while (cache_.size() > max_entries_)
      prune_nolock();
  }

  std::shared_ptr<const std::vector<uint8_t>> fetch(
    const std::string& token, uint64_t off, uint64_t len,
    const ReadObjectPartFunc& reader) {
    if (!reader)
      return nullptr;
    if (token.empty()) {
      auto opt = reader(off, len);
      if (!opt || opt->empty())
        return nullptr;
      auto data = std::make_shared<std::vector<uint8_t>>(std::move(*opt));
      return std::static_pointer_cast<const std::vector<uint8_t>>(data);
    }

    const std::string key = token + "|" + std::to_string(off) + "|" +
      std::to_string(len);
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = cache_.find(key);
      if (it != cache_.end()) {
        it->second.last = Clock::now();
        ++it->second.hits;
        return std::static_pointer_cast<const std::vector<uint8_t>>(it->second.data);
      }
    }

    auto opt = reader(off, len);
    if (!opt || opt->empty())
      return nullptr;
    auto data = std::make_shared<std::vector<uint8_t>>(std::move(*opt));
    std::lock_guard<std::mutex> lk(mu_);
    auto it = cache_.find(key);
    if (it != cache_.end()) {
      it->second.last = Clock::now();
      ++it->second.hits;
      return std::static_pointer_cast<const std::vector<uint8_t>>(it->second.data);
    }
    CacheEntry entry;
    entry.data = data;
    entry.last = Clock::now();
    entry.hits = 1;
    if (cache_.size() >= max_entries_)
      prune_nolock();
    cache_.emplace(key, std::move(entry));
    return std::static_pointer_cast<const std::vector<uint8_t>>(data);
  }

private:
  using Clock = std::chrono::steady_clock;
  struct CacheEntry {
    std::shared_ptr<std::vector<uint8_t>> data;
    Clock::time_point last;
    size_t hits{ 0 };
  };

  void prune_nolock() {
    if (cache_.empty())
      return;
    auto victim = std::min_element(cache_.begin(), cache_.end(),
      [](const auto& a, const auto& b) {
        return a.second.last < b.second.last;
      });
    if (victim != cache_.end())
      cache_.erase(victim);
  }

  std::mutex mu_;
  std::unordered_map<std::string, CacheEntry> cache_;
  size_t max_entries_{ 32 };
};

std::shared_ptr<const std::vector<uint8_t>> fetch_message_bytes(
  const std::string& token, uint64_t off, uint64_t len,
  const ReadObjectPartFunc& reader) {
  return MessageCache::instance().fetch(token, off, len, reader);
}

bool json_get_str(const std::string& line, const std::string& key,
  std::string& out) {
  std::string pat = "\"" + key + "\":";
  size_t pos = line.find(pat);
  if (pos == std::string::npos)
    return false;
  pos += pat.size();
  while (pos < line.size() && isspace(static_cast<unsigned char>(line[pos])))
    ++pos;
  if (pos >= line.size())
    return false;
  if (line[pos] != '"')
    return false;
  ++pos;
  std::string val;
  bool esc = false;
  for (; pos < line.size(); ++pos) {
    char c = line[pos];
    if (esc) {
      val.push_back(c);
      esc = false;
    }
    else if (c == '\\') {
      esc = true;
    }
    else if (c == '"') {
      out = val;
      return true;
    }
    else {
      val.push_back(c);
    }
  }
  return false;
}

bool json_get_ll(const std::string& line, const std::string& key,
  long long& out) {
  std::string pat = "\"" + key + "\":";
  size_t pos = line.find(pat);
  if (pos == std::string::npos)
    return false;
  pos += pat.size();
  while (pos < line.size() && isspace(static_cast<unsigned char>(line[pos])))
    ++pos;
  if (pos >= line.size())
    return false;
  bool neg = false;
  if (line[pos] == '-') {
    neg = true;
    ++pos;
  }
  if (pos >= line.size() || !isdigit(static_cast<unsigned char>(line[pos])))
    return false;
  unsigned long long v = 0;
  while (pos < line.size() && isdigit(static_cast<unsigned char>(line[pos]))) {
    v = v * 10 + (line[pos] - '0');
    ++pos;
  }
  out = neg ? -(long long)v : (long long)v;
  return true;
}

bool json_get_u64(const std::string& line, const std::string& key,
  uint64_t& out) {
  long long tmp = 0;
  if (!json_get_ll(line, key, tmp))
    return false;
  if (tmp < 0)
    return false;
  out = static_cast<uint64_t>(tmp);
  return true;
}

bool index_get_string(const std::string& line, const std::string& logical_key,
  std::string& out) {
  if (logical_key == "shortName")
    return json_get_str(line, "shortName", out) ||
    json_get_str(line, "sn", out);
  if (logical_key == "typeOfLevel")
    return json_get_str(line, "typeOfLevel", out) ||
    json_get_str(line, "tl", out);
  if (logical_key == "stepRange")
    return json_get_str(line, "stepRange", out) ||
    json_get_str(line, "sr", out);
  if (logical_key == "unit")
    return json_get_str(line, "unit", out) ||
    json_get_str(line, "u", out);
  if (logical_key == "globalDomain")
    return json_get_str(line, "globalDomain", out) ||
    json_get_str(line, "gd", out);
  return json_get_str(line, logical_key, out);
}

bool index_get_number(const std::string& line, const std::string& logical_key,
  long long& out) {
  if (logical_key == "level")
    return json_get_ll(line, "level", out) ||
    json_get_ll(line, "lv", out);
  if (logical_key == "step")
    return json_get_ll(line, "step", out) ||
    json_get_ll(line, "st", out);
  if (logical_key == "ni")
    return json_get_ll(line, "ni", out);
  if (logical_key == "nj")
    return json_get_ll(line, "nj", out);
  if (logical_key == "edition")
    return json_get_ll(line, "edition", out) ||
    json_get_ll(line, "ed", out);
  if (logical_key == "numberOfDataPoints")
    return json_get_ll(line, "numberOfDataPoints", out) ||
    json_get_ll(line, "np", out);
  if (logical_key == "paramId")
    return json_get_ll(line, "pi", out);
  if (logical_key == "validityDate")
    return json_get_ll(line, "vd", out);
  if (logical_key == "validityTime")
    return json_get_ll(line, "vt", out);
  return json_get_ll(line, logical_key, out);
}

bool index_get_u64(const std::string& line, const std::string& logical_key,
  uint64_t& out) {
  if (logical_key == "offset")
    return json_get_u64(line, "offset", out) ||
    json_get_u64(line, "of", out);
  if (logical_key == "totalLength")
    return json_get_u64(line, "totalLength", out) ||
    json_get_u64(line, "ln", out);
  return json_get_u64(line, logical_key, out);
}

bool get_long_safe(codes_handle* h, const char* key, long& out) {
  return codes_get_long(h, key, &out) == 0;
}

bool get_double_safe(codes_handle* h, const char* key, double& out) {
  return codes_get_double(h, key, &out) == 0;
}

bool get_string_safe(codes_handle* h, const char* key, std::string& out) {
  size_t need = 0;
  int err = codes_get_length(h, key, &need);
  if (err != 0 || need == 0) {
    out.clear();
    return false;
  }
  std::string buf(need, '\0');
  size_t got = need;
  err = codes_get_string(h, key, buf.data(), &got);
  if (err != 0 || got == 0) {
    out.clear();
    return false;
  }
  out.assign(buf.data(), got > 0 ? got - 1 : 0);
  return true;
}

bool key_missing(codes_handle* h, const std::string& key) {
  int err = 0;
  const int missing = codes_is_missing(h, key.c_str(), &err);
  return err != 0 || missing != 0;
}

bool scalar_key(codes_handle* h, const std::string& key) {
  size_t size = 0;
  return codes_get_size(h, key.c_str(), &size) == 0 && size == 1;
}

std::optional<iotdbfs::MetadataValue> configured_value(codes_handle* h,
  const std::string& key, const iotdbfs::MetadataFieldDefinition& field) {
  if (key.empty() || !scalar_key(h, key) || key_missing(h, key))
    return std::nullopt;
  switch (field.type) {
  case iotdbfs::MetadataValueType::String: {
    // Never allocate from a size supplied by an untrusted GRIB message.
    std::array<char, 4097> buffer{};
    size_t length = buffer.size();
    if (codes_get_string(h, key.c_str(), buffer.data(), &length) != 0 ||
        length == 0 || length > buffer.size() || buffer[length - 1] != '\0')
      return std::nullopt;
    return iotdbfs::MetadataValue(std::string(buffer.data(), length - 1));
  }
  case iotdbfs::MetadataValueType::Int64:
  case iotdbfs::MetadataValueType::Bool: {
    long value = 0;
    int native_type = 0;
    if (codes_get_native_type(h, key.c_str(), &native_type) != 0 ||
        native_type != CODES_TYPE_LONG || !get_long_safe(h, key.c_str(), value) ||
        value == CODES_MISSING_LONG)
      return std::nullopt;
    if (field.type == iotdbfs::MetadataValueType::Int64)
      return iotdbfs::MetadataValue(static_cast<std::int64_t>(value));
    if (value == 0 || value == 1)
      return iotdbfs::MetadataValue(value != 0);
    return std::nullopt;
  }
  case iotdbfs::MetadataValueType::Double: {
    double value = 0.0;
    if (!get_double_safe(h, key.c_str(), value) || value == CODES_MISSING_DOUBLE ||
        !std::isfinite(value) || !std::isfinite(value * field.scale))
      return std::nullopt;
    return iotdbfs::MetadataValue(value * field.scale);
  }
  case iotdbfs::MetadataValueType::StringSet:
  case iotdbfs::MetadataValueType::Int64Set:
  case iotdbfs::MetadataValueType::DoubleSet:
  case iotdbfs::MetadataValueType::BoolSet:
    return std::nullopt;
  }
  return std::nullopt;
}

bool get_units(codes_handle* h, std::string& units) {
  if (get_string_safe(h, "units", units))
    return true;
  return get_string_safe(h, "unit", units);
}

bool get_step_range(codes_handle* h, std::string& sr) {
  if (get_string_safe(h, "stepRange", sr))
    return true;
  long s = 0, e = 0;
  bool hs = get_long_safe(h, "startStep", s);
  bool he = get_long_safe(h, "endStep", e);
  if (hs && he) {
    sr = std::to_string(s) + "-" + std::to_string(e);
    return true;
  }
  if (get_long_safe(h, "step", e)) {
    sr = std::to_string(e);
    return true;
  }
  return false;
}

bool get_ni_nj(codes_handle* h, long& ni, long& nj) {
  bool ok1 = get_long_safe(h, "Ni", ni);
  bool ok2 = get_long_safe(h, "Nj", nj);
  if (ok1 && ok2)
    return true;
  ok1 = get_long_safe(h, "numberOfPointsAlongParallel", ni);
  ok2 = get_long_safe(h, "numberOfPointsAlongMeridian", nj);
  return ok1 && ok2;
}

double to_degrees_from_int_maybe(double v) {
  if (std::fabs(v) > 1000.0)
    return v * 1e-6;
  return v;
}

double normalize_lon(double lon, double min_lon, double max_lon) {
  if (lon >= min_lon && lon <= max_lon)
    return lon;
  // Equivalent longitudes differ by 360 degrees, regardless of region width.
  // Choose the representation nearest this region; indices then clamp points
  // outside the region to its actual edge. remainder also handles large inputs
  // without repeatedly adding or subtracting a full revolution.
  const double center = min_lon + (max_lon - min_lon) / 2.0;
  return center + std::remainder(std::remainder(lon, 360.0) - center, 360.0);
}

bool kv_match_line(const std::string& line,
  const std::map<std::string, std::string>& kv) {
  for (const auto& it : kv) {
    const std::string& k = it.first;
    const std::string& v = it.second;
    if (k == "shortName" || k == "typeOfLevel" || k == "stepRange" ||
      k == "unit" || k == "globalDomain") {
      std::string got;
      if (!index_get_string(line, k, got) || got != v)
        return false;
    }
    else if (k == "level" || k == "step" || k == "ni" || k == "nj" ||
      k == "edition" || k == "numberOfDataPoints") {
      long long got = 0;
      if (!index_get_number(line, k, got))
        return false;
      char* endp = nullptr;
      long long want = std::strtoll(v.c_str(), &endp, 10);
      if (endp == v.c_str() || *endp != '\0' || got != want)
        return false;
    }
    else {
      std::string got;
      if (index_get_string(line, k, got)) {
        if (got != v)
          return false;
      }
      else {
        long long gn = 0;
        if (index_get_number(line, k, gn)) {
          char* endp = nullptr;
          long long want = std::strtoll(v.c_str(), &endp, 10);
          if (endp == v.c_str() || *endp != '\0' || gn != want)
            return false;
        }
        else {
          return false;
        }
      }
    }
  }
  return true;
}

struct CropWindow {
  long i0{ 0 }, i1{ 0 }, j0{ 0 }, j1{ 0 };
  long width{ 0 }, height{ 0 };
  bool wraps_longitude{ false };
};

constexpr double kLongitudePeriod = 360.0;

bool is_cyclic_longitude_grid(long ni, double increment) {
  if (ni <= 1 || !std::isfinite(increment) || increment <= 0.0)
    return false;
  const double circumference = static_cast<double>(ni) * increment;
  const double tolerance = std::max(1e-6, increment * 1e-6);
  return std::fabs(circumference - kLongitudePeriod) <= tolerance;
}

double wrap_longitude_to_grid(double longitude, double first_longitude) {
  double offset = std::fmod(longitude - first_longitude, kLongitudePeriod);
  if (offset < 0.0) offset += kLongitudePeriod;
  return first_longitude + offset;
}

long crop_column(const CropWindow& window, long offset, long ni) {
  const long column = window.i0 + offset;
  return window.wraps_longitude && column >= ni ? column - ni : column;
}

std::optional<CropWindow> compute_crop_window(codes_handle* h,
  const CropParams& cp_in, long Ni, long Nj) {
  if (!h || Ni <= 0 || Nj <= 0)
    return std::nullopt;
  if (!std::isfinite(cp_in.lat0) || !std::isfinite(cp_in.lon0) ||
    !std::isfinite(cp_in.lat1) || !std::isfinite(cp_in.lon1))
    return std::nullopt;
  CropParams cp = cp_in;

  double lat_first = 0, lon_first = 0, lat_last = 0, lon_last = 0;
  double iinc = 0, jinc = 0;

  if (!(get_double_safe(h, "latitudeOfFirstGridPointInDegrees", lat_first) ||
    get_double_safe(h, "latitudeOfFirstGridPoint", lat_first)))
    return std::nullopt;
  if (!(get_double_safe(h, "longitudeOfFirstGridPointInDegrees", lon_first) ||
    get_double_safe(h, "longitudeOfFirstGridPoint", lon_first)))
    return std::nullopt;
  if (!(get_double_safe(h, "latitudeOfLastGridPointInDegrees", lat_last) ||
    get_double_safe(h, "latitudeOfLastGridPoint", lat_last)))
    return std::nullopt;
  if (!(get_double_safe(h, "longitudeOfLastGridPointInDegrees", lon_last) ||
    get_double_safe(h, "longitudeOfLastGridPoint", lon_last)))
    return std::nullopt;

  lat_first = to_degrees_from_int_maybe(lat_first);
  lon_first = to_degrees_from_int_maybe(lon_first);
  lat_last = to_degrees_from_int_maybe(lat_last);
  lon_last = to_degrees_from_int_maybe(lon_last);

  if (!(get_double_safe(h, "iDirectionIncrementInDegrees", iinc) ||
    get_double_safe(h, "iDirectionIncrement", iinc)))
    return std::nullopt;
  if (!(get_double_safe(h, "jDirectionIncrementInDegrees", jinc) ||
    get_double_safe(h, "jDirectionIncrement", jinc)))
    return std::nullopt;

  iinc = std::fabs(to_degrees_from_int_maybe(iinc));
  jinc = std::fabs(to_degrees_from_int_maybe(jinc));
  if (!std::isfinite(lat_first) || !std::isfinite(lon_first) ||
    !std::isfinite(lat_last) || !std::isfinite(lon_last) ||
    !std::isfinite(iinc) || !std::isfinite(jinc) || iinc <= 0 || jinc <= 0)
    return std::nullopt;

  if (cp_in.enabled && cp_in.extend_equal_corners_to_grid_end &&
    (cp_in.lat0 == cp_in.lat1 && cp_in.lon0 == cp_in.lon1)) {
    cp.lat1 = lat_last;
    cp.lon1 = lon_last;
  }

  const double lon_min = std::min(lon_first, lon_last);
  const double lon_max = std::max(lon_first, lon_last);
  const bool cyclic_longitude = is_cyclic_longitude_grid(Ni, iinc);
  const double requested_lon_min = std::min(cp.lon0, cp.lon1);
  const double requested_lon_max = std::max(cp.lon0, cp.lon1);
  const double requested_lon_span = requested_lon_max - requested_lon_min;
  if (!std::isfinite(requested_lon_span)) return std::nullopt;

  // Align the interval as a unit. Normalizing each endpoint independently and
  // sorting the results turns [-10, 10] into [10, 350] on a 0..359 grid.
  // Keeping the original span produces [350, 370], whose upper endpoint wraps
  // back to 10 while preserving the requested 20-degree interval.
  if (cyclic_longitude) {
    cp.lon0 = normalize_lon(requested_lon_min, lon_min, lon_max);
    cp.lon1 = cp.lon0 + requested_lon_span;
  }
  else {
    // Regional grids retain the established endpoint-wise normalization and
    // boundary clamping semantics.
    cp.lon0 = normalize_lon(cp.lon0, lon_min, lon_max);
    cp.lon1 = normalize_lon(cp.lon1, lon_min, lon_max);
  }

  auto lon_to_i = [&](double lon) -> long {
    if (cyclic_longitude)
      lon = wrap_longitude_to_grid(lon, lon_first);
    const double index = std::clamp((lon - lon_first) / iinc,
      0.0, static_cast<double>(Ni - 1));
    return static_cast<long>(std::floor(index + 0.5));
    };

  bool north_to_south = (lat_first >= lat_last);
  auto lat_to_j = [&](double lat) -> long {
    double delta = north_to_south ? (lat_first - lat) : (lat - lat_first);
    const double index = std::clamp(delta / jinc,
      0.0, static_cast<double>(Nj - 1));
    return static_cast<long>(std::floor(index + 0.5));
    };

  long ii0 = 0;
  long ii1 = Ni - 1;
  bool wraps_longitude = false;
  if (!cyclic_longitude || requested_lon_span < kLongitudePeriod) {
    ii0 = lon_to_i(cp.lon0);
    ii1 = lon_to_i(cp.lon1);
    wraps_longitude = cyclic_longitude && ii0 > ii1;
  }
  long jj0 = lat_to_j(std::max(cp.lat0, cp.lat1));
  long jj1 = lat_to_j(std::min(cp.lat0, cp.lat1));

  CropWindow cw;
  cw.i0 = wraps_longitude ? ii0 : std::min(ii0, ii1);
  cw.i1 = wraps_longitude ? ii1 : std::max(ii0, ii1);
  cw.j0 = std::min(jj0, jj1);
  cw.j1 = std::max(jj0, jj1);
  cw.wraps_longitude = wraps_longitude;
  cw.width = wraps_longitude
    ? (Ni - cw.i0) + (cw.i1 + 1)
    : (cw.i1 - cw.i0 + 1);
  cw.height = (cw.j1 - cw.j0 + 1);
  if (cw.width <= 0 || cw.height <= 0)
    return std::nullopt;
  return cw;
}

} // namespace

codes_handle* new_handle_from_message_guarded(const uint8_t* msg, size_t size) {
  if (!msg || size == 0)
    return nullptr;
  std::lock_guard<std::recursive_mutex> lk(g_eccodes_mutex);
  return codes_handle_new_from_message(get_ecc_ctx(), msg, size);
}

codes_handle* new_handle_from_message_copy_guarded(const uint8_t* msg,
  size_t size) {
  if (!msg || size == 0)
    return nullptr;
  std::lock_guard<std::recursive_mutex> lk(g_eccodes_mutex);
  return codes_handle_new_from_message_copy(get_ecc_ctx(), msg, size);
}

struct MessageDecoder::Impl {
  Impl(std::shared_ptr<const std::vector<uint8_t>> message,
      codes_handle* codes)
    : bytes(std::move(message)), handle(codes) {}

  std::shared_ptr<const std::vector<uint8_t>> bytes;
  HandleHolder handle;
};

MessageDecoder::MessageDecoder(std::unique_ptr<Impl> implementation)
  : implementation_(std::move(implementation)) {}

MessageDecoder::~MessageDecoder() = default;
MessageDecoder::MessageDecoder(MessageDecoder&&) noexcept = default;
MessageDecoder& MessageDecoder::operator=(MessageDecoder&&) noexcept = default;

std::optional<MessageDecoder> MessageDecoder::open(const MsgInfo& mi,
    const ReadObjectPartFunc& reader, const std::string& cache_token) {
  auto bytes = fetch_message_bytes(cache_token, mi.off, mi.len, reader);
  if (!bytes) return std::nullopt;
  auto* handle = new_handle_from_message_guarded(bytes->data(), bytes->size());
  if (!handle)
    handle = new_handle_from_message_copy_guarded(bytes->data(), bytes->size());
  if (!handle) return std::nullopt;
  return MessageDecoder(std::make_unique<Impl>(std::move(bytes), handle));
}

std::vector<MsgLoc> find_messages(const uint8_t* data, size_t length) {
  return find_messages_incremental(data, length, 0).messages;
}

MsgScanResult find_messages_incremental(const uint8_t* data, size_t length,
  size_t start_offset) {
  MsgScanResult result;
  if (!data || length == 0) {
    result.next_offset = 0;
    return result;
  }

  size_t start = std::min(start_offset, length);
  if (start > 0) {
    constexpr size_t kHeaderOverlap = 16;
    start -= std::min(start, kHeaderOverlap);
  }

  std::vector<MsgLoc> out;
  for (size_t i = start; i + 8 < length; ++i) {
    if (std::memcmp(data + i, "GRIB", 4) != 0)
      continue;
    int edition = -1;
    size_t msg_len = 0;
    if (i + 16 < length) {
      uint8_t ed = data[i + 7];
      if (ed == 2) {
        uint64_t L = ((uint64_t)data[i + 8] << 56) |
          ((uint64_t)data[i + 9] << 48) |
          ((uint64_t)data[i + 10] << 40) |
          ((uint64_t)data[i + 11] << 32) |
          ((uint64_t)data[i + 12] << 24) |
          ((uint64_t)data[i + 13] << 16) |
          ((uint64_t)data[i + 14] << 8) |
          (uint64_t)data[i + 15];
        if (L > 0 && L <= std::numeric_limits<size_t>::max()) {
          edition = 2;
          msg_len = static_cast<size_t>(L);
        }
      }
    }
    if (edition != 2) {
      if (i + 8 < length && data[i + 7] == 1) {
        uint32_t L =
          ((uint32_t)data[i + 4] << 16) |
          ((uint32_t)data[i + 5] << 8) |
          (uint32_t)data[i + 6];
        if (L > 0) {
          edition = 1;
          msg_len = static_cast<size_t>(L);
        }
      }
    }
    if (edition == -1 || msg_len == 0)
      continue;
    if (i + msg_len <= length) {
      out.push_back(MsgLoc{ i, msg_len, edition });
      i += msg_len - 1;
    }
    else {
      result.next_offset = i;
      result.incomplete_tail = true;
      break;
    }
  }
  if (!result.incomplete_tail) {
    constexpr size_t kTailOverlap = 16;
    result.next_offset = length > kTailOverlap ? length - kTailOverlap : 0;
  }
  result.messages = std::move(out);
  return result;
}

std::string build_index_line(codes_handle* h, uint64_t file_offset_of_msg,
  size_t total_len, int edition) {
  std::string shortName;
  std::string stepRange;
  std::string typeOfLevel;
  std::string unit;
  std::string globalDomain;
  long level = 0;
  long step = 0;
  long Ni = 0;
  long Nj = 0;
  long numberOfDataPoints = 0;
  long paramId = 0;
  long validityDate = 0;
  long validityTime = 0;
  bool has_level = get_long_safe(h, "level", level);
  bool has_step = get_long_safe(h, "step", step);
  bool has_ni_nj = get_ni_nj(h, Ni, Nj);
  bool has_num_points = get_long_safe(h, "numberOfDataPoints", numberOfDataPoints);
  bool has_param_id = get_long_safe(h, "paramId", paramId);
  bool has_validity_date = get_long_safe(h, "validityDate", validityDate);
  bool has_validity_time = get_long_safe(h, "validityTime", validityTime);

  get_string_safe(h, "shortName", shortName);
  get_step_range(h, stepRange);
  get_string_safe(h, "typeOfLevel", typeOfLevel);
  get_units(h, unit);
  get_string_safe(h, "globalDomain", globalDomain);

  std::ostringstream os;
  os << "{";
  bool first = true;
  auto emit_str = [&](const char* k, const std::string& v) {
    if (v.empty())
      return;
    if (!first)
      os << ",";
    first = false;
    os << "\"" << k << "\":\"" << v << "\"";
    };
  auto emit_ll = [&](const char* k, long long v, bool has) {
    if (!has)
      return;
    if (!first)
      os << ",";
    first = false;
    os << "\"" << k << "\":" << v;
    };
  auto emit_u64 = [&](const char* k, uint64_t v) {
    if (!first)
      os << ",";
    first = false;
    os << "\"" << k << "\":" << v;
    };

  emit_str("sn", shortName);
  emit_str("sr", stepRange);
  emit_str("tl", typeOfLevel);
  emit_ll("lv", level, has_level);
  emit_ll("st", step, has_step);
  emit_str("u", unit);
  emit_str("gd", globalDomain);
  emit_ll("ni", Ni, has_ni_nj);
  emit_ll("nj", Nj, has_ni_nj);
  emit_ll("np", numberOfDataPoints, has_num_points);
  emit_ll("pi", paramId, has_param_id);
  emit_ll("vd", validityDate, has_validity_date);
  emit_ll("vt", validityTime, has_validity_time);
  emit_u64("of", file_offset_of_msg);
  emit_u64("ln", static_cast<uint64_t>(total_len));
  emit_ll("ed", edition, true);
  os << "}\n";
  return os.str();
}

bool extract_file_meta(codes_handle* h, FileMeta& meta) {
  if (!h)
    return false;

  long centre = 0, gpi = 0, discipline = 0, parameterCategory = 0,
    parameterNumber = 0, typeOfGeneratingProcess = 0;
  std::string typeOfProcessedData;
  std::string typeOfLevel;
  std::string shortName;
  get_long_safe(h, "centre", centre);
  get_long_safe(h, "generatingProcessIdentifier", gpi);
  get_long_safe(h, "discipline", discipline);
  get_long_safe(h, "parameterCategory", parameterCategory);
  get_long_safe(h, "parameterNumber", parameterNumber);
  get_long_safe(h, "typeOfGeneratingProcess", typeOfGeneratingProcess);
  get_string_safe(h, "typeOfProcessedData", typeOfProcessedData);
  get_string_safe(h, "typeOfLevel", typeOfLevel);
  if (!get_string_safe(h, "shortName", shortName) || shortName.empty())
    (void)get_string_safe(h, "name", shortName);

  meta.shortName = shortName;
  meta.typeOfProcessedData = typeOfProcessedData;
  meta.typeOfGeneratingProcess = typeOfGeneratingProcess;
  meta.typeOfLevel = typeOfLevel;
  meta.centre = centre;
  meta.generatingProcessIdentifier = gpi;
  meta.discipline = discipline;
  meta.parameterCategory = parameterCategory;
  meta.parameterNumber = parameterNumber;

  get_units(h, meta.unit);
  get_string_safe(h, "globalDomain", meta.globalDomain);
  get_ni_nj(h, meta.Ni, meta.Nj);

  long np = 0;
  if (get_long_safe(h, "numberOfDataPoints", np))
    meta.numberOfDataPoints = np;

  double v = 0;
  if (get_double_safe(h, "latitudeOfFirstGridPointInDegrees", v) ||
    get_double_safe(h, "latitudeOfFirstGridPoint", v))
    meta.latitudeOfFirstGridPoint = to_degrees_from_int_maybe(v);
  if (get_double_safe(h, "longitudeOfFirstGridPointInDegrees", v) ||
    get_double_safe(h, "longitudeOfFirstGridPoint", v))
    meta.longitudeOfFirstGridPoint = to_degrees_from_int_maybe(v);
  if (get_double_safe(h, "latitudeOfLastGridPointInDegrees", v) ||
    get_double_safe(h, "latitudeOfLastGridPoint", v))
    meta.latitudeOfLastGridPoint = to_degrees_from_int_maybe(v);
  if (get_double_safe(h, "longitudeOfLastGridPointInDegrees", v) ||
    get_double_safe(h, "longitudeOfLastGridPoint", v))
    meta.longitudeOfLastGridPoint = to_degrees_from_int_maybe(v);

  if (get_double_safe(h, "iDirectionIncrementInDegrees", v) ||
    get_double_safe(h, "iDirectionIncrement", v))
    meta.iDirectionIncrement = to_degrees_from_int_maybe(v);
  if (get_double_safe(h, "jDirectionIncrementInDegrees", v) ||
    get_double_safe(h, "jDirectionIncrement", v))
    meta.jDirectionIncrement = to_degrees_from_int_maybe(v);

  meta.has = true;
  return true;
}

iotdbfs::MetadataResult<iotdbfs::MetadataMap>
extract_message_metadata(codes_handle* h, int edition) {
  if (!h)
    return iotdbfs::MetadataResult<iotdbfs::MetadataMap>::failure(
      "null GRIB handle");
  std::lock_guard<std::recursive_mutex> lk(g_eccodes_mutex);
  iotdbfs::MetadataMap values;
  const auto string_value = [&](const char* id, const std::string& value) {
    if (!value.empty())
      values.insert_or_assign(id, iotdbfs::MetadataValue(value));
  };
  const auto long_value = [&](const char* id, const char* key) {
    long value = 0;
    if (get_long_safe(h, key, value) && value != CODES_MISSING_LONG)
      values.insert_or_assign(id, iotdbfs::MetadataValue(
        static_cast<std::int64_t>(value)));
  };
  const auto double_value = [&](const char* id, const char* primary,
      const char* fallback, bool degrees) {
    double value = 0;
    if ((get_double_safe(h, primary, value) ||
         (fallback && get_double_safe(h, fallback, value))) &&
        value != CODES_MISSING_DOUBLE && std::isfinite(value)) {
      values.insert_or_assign(id, iotdbfs::MetadataValue(
        degrees ? to_degrees_from_int_maybe(value) : value));
    }
  };

  std::string text;
  if (!get_string_safe(h, "shortName", text) || text.empty())
    (void)get_string_safe(h, "name", text);
  string_value("shortName", text);
  text.clear();
  if (get_string_safe(h, "typeOfProcessedData", text))
    string_value("typeOfProcessedData", text);
  text.clear();
  if (get_string_safe(h, "typeOfLevel", text))
    string_value("typeOfLevel", text);
  text.clear();
  if (get_units(h, text)) string_value("unit", text);
  text.clear();
  if (get_string_safe(h, "globalDomain", text))
    string_value("globalDomain", text);
  text.clear();
  if (get_step_range(h, text)) string_value("stepRange", text);

  long_value("typeOfGeneratingProcess", "typeOfGeneratingProcess");
  long_value("centre", "centre");
  long_value("generatingProcessIdentifier", "generatingProcessIdentifier");
  long_value("discipline", "discipline");
  long_value("parameterCategory", "parameterCategory");
  long_value("parameterNumber", "parameterNumber");
  long_value("level", "level");
  long_value("step", "step");
  long_value("Ni", "Ni");
  long_value("Nj", "Nj");
  long_value("numberOfDataPoints", "numberOfDataPoints");
  values.insert_or_assign("edition", iotdbfs::MetadataValue(
    static_cast<std::int64_t>(edition)));

  double_value("latitudeOfFirstGridPoint",
    "latitudeOfFirstGridPointInDegrees", "latitudeOfFirstGridPoint", true);
  double_value("longitudeOfFirstGridPoint",
    "longitudeOfFirstGridPointInDegrees", "longitudeOfFirstGridPoint", true);
  double_value("latitudeOfLastGridPoint",
    "latitudeOfLastGridPointInDegrees", "latitudeOfLastGridPoint", true);
  double_value("longitudeOfLastGridPoint",
    "longitudeOfLastGridPointInDegrees", "longitudeOfLastGridPoint", true);
  double_value("iDirectionIncrement", "iDirectionIncrementInDegrees",
    "iDirectionIncrement", true);
  double_value("jDirectionIncrement", "jDirectionIncrementInDegrees",
    "jDirectionIncrement", true);
  return iotdbfs::MetadataResult<iotdbfs::MetadataMap>::success(
    std::move(values));
}

iotdbfs::MetadataResult<iotdbfs::MetadataMap>
extract_configured_metadata(codes_handle* h,
  const iotdbfs::MetadataConfig& config) {
  if (!h)
    return iotdbfs::MetadataResult<iotdbfs::MetadataMap>::failure(
      "null GRIB handle");
  std::lock_guard<std::recursive_mutex> lk(g_eccodes_mutex);
  iotdbfs::MetadataMap values;
  for (const auto& field : config.fields()) {
    auto value = configured_value(h, field.source, field);
    for (const auto& alias : field.aliases) {
      if (value) break;
      value = configured_value(h, alias, field);
    }
    if (!value) value = field.default_value;
    if (value) {
      values.emplace(field.id, std::move(*value));
    } else if (field.required) {
      return iotdbfs::MetadataResult<iotdbfs::MetadataMap>::failure(
        "required metadata missing or invalid: " + field.id);
    }
  }
  return iotdbfs::MetadataResult<iotdbfs::MetadataMap>::success(std::move(values));
}

MsgInfo find_msg_info(const std::string& index_text,
  const std::map<std::string, std::string>& kv) {
  auto matches = find_all_msg_matches(index_text, kv, 2);
  return matches.size() == 1 ? matches.front().info : MsgInfo{};
}

std::vector<MsgMatch> find_all_msg_matches(const std::string& index_text,
  const std::map<std::string, std::string>& kv, size_t limit) {
  std::vector<MsgMatch> out;
  if (index_text.empty())
    return out;
  size_t start = 0;
  while (start < index_text.size()) {
    size_t nl = index_text.find('\n', start);
    std::string line = (nl == std::string::npos)
      ? index_text.substr(start)
      : index_text.substr(start, nl - start);
    start = (nl == std::string::npos) ? index_text.size() : nl + 1;
    if (line.empty())
      continue;
    if (!kv_match_line(line, kv))
      continue;

    MsgMatch match;
    if (!index_get_u64(line, "offset", match.info.off))
      continue;
    if (!index_get_u64(line, "totalLength", match.info.len))
      continue;
    match.info.ok = true;

    long long v = 0;
    if (index_get_number(line, "ni", v))
      match.info.ni = v;
    if (index_get_number(line, "nj", v))
      match.info.nj = v;
    if (index_get_number(line, "numberOfDataPoints", v))
      match.info.numberOfDataPoints = v;

    std::string text;
    if (index_get_string(line, "shortName", text))
      match.labels["shortName"] = text;
    if (index_get_string(line, "typeOfLevel", text))
      match.labels["typeOfLevel"] = text;
    if (index_get_string(line, "stepRange", text))
      match.labels["stepRange"] = text;
    if (index_get_string(line, "unit", text))
      match.labels["unit"] = text;
    if (index_get_string(line, "globalDomain", text))
      match.labels["globalDomain"] = text;
    if (index_get_number(line, "level", v))
      match.labels["level"] = std::to_string(v);
    if (index_get_number(line, "step", v))
      match.labels["step"] = std::to_string(v);
    if (index_get_number(line, "edition", v))
      match.labels["edition"] = std::to_string(v);
    if (index_get_number(line, "paramId", v))
      match.labels["paramId"] = std::to_string(v);
    if (index_get_number(line, "validityDate", v))
      match.labels["validityDate"] = std::to_string(v);
    if (index_get_number(line, "validityTime", v))
      match.labels["validityTime"] = std::to_string(v);

    out.push_back(std::move(match));
    if (limit > 0 && out.size() >= limit)
      break;
  }
  return out;
}

size_t dtype_bytes(DataDType t) {
  return (t == DataDType::F64) ? 8 : 4;
}

DataDType parse_dtype_from_kv(
  const std::map<std::string, std::string>& kv) {
  auto it = kv.find("dtype");
  if (it == kv.end())
    return DataDType::F32;
  std::string v = path_utils::to_lower(it->second);
  if (v == "f64" || v == "float64" || v == "double")
    return DataDType::F64;
  return DataDType::F32;
}

std::optional<size_t>
get_values_count_from_message_bytes(const std::vector<uint8_t>& msg) {
  HandleHolder holder(new_handle_from_message_guarded(msg.data(), msg.size()));
  if (!holder.get())
    holder.handle = new_handle_from_message_copy_guarded(msg.data(), msg.size());
  if (!holder.get())
    return std::nullopt;
  size_t n = 0;
  int err = codes_get_size(holder.get(), "values", &n);
  if (err != 0 || n == 0)
    return std::nullopt;
  return n;
}

std::optional<size_t>
compute_values_count_for_msg(const MsgInfo& mi,
  const ReadObjectPartFunc& reader) {
  return compute_values_count_for_msg(mi, reader, std::string());
}

std::optional<size_t>
compute_values_count_for_msg(const MsgInfo& mi,
  const ReadObjectPartFunc& reader,
  const std::string& cache_token) {
  if (mi.ni > 0 && mi.nj > 0)
    return static_cast<size_t>(mi.ni * mi.nj);
  if (mi.numberOfDataPoints > 0)
    return static_cast<size_t>(mi.numberOfDataPoints);
  auto bytes = fetch_message_bytes(cache_token, mi.off, mi.len, reader);
  if (!bytes)
    return std::nullopt;
  return get_values_count_from_message_bytes(*bytes);
}

void strip_crop_keys(std::map<std::string, std::string>& kv) {
  kv.erase("lat0");
  kv.erase("lon0");
  kv.erase("lat1");
  kv.erase("lon1");
}

CropParams parse_crop_params_from_kv(
  const std::map<std::string, std::string>& kv) {
  CropParams cp;
  auto it_lat0 = kv.find("lat0");
  auto it_lon0 = kv.find("lon0");
  if (it_lat0 != kv.end() && it_lon0 != kv.end()) {
    cp.enabled = true;
    cp.lat0 = std::atof(it_lat0->second.c_str());
    cp.lon0 = std::atof(it_lon0->second.c_str());
    auto it_lat1 = kv.find("lat1");
    auto it_lon1 = kv.find("lon1");
    if (it_lat1 != kv.end() && it_lon1 != kv.end()) {
      cp.lat1 = std::atof(it_lat1->second.c_str());
      cp.lon1 = std::atof(it_lon1->second.c_str());
    }
    else {
      cp.lat1 = cp.lat0;
      cp.lon1 = cp.lon0;
    }
  }
  return cp;
}

std::optional<size_t>
compute_cropped_value_count(const MsgInfo& mi, const CropParams& cp,
  const ReadObjectPartFunc& reader) {
  return compute_cropped_value_count(mi, cp, reader, std::string());
}

std::optional<size_t>
compute_cropped_value_count(const MsgInfo& mi, const CropParams& cp,
  const ReadObjectPartFunc& reader, const std::string& cache_token) {
  auto bytes = fetch_message_bytes(cache_token, mi.off, mi.len, reader);
  if (!bytes)
    return std::nullopt;
  HandleHolder holder(
    new_handle_from_message_guarded(bytes->data(), bytes->size()));
  if (!holder.get())
    holder.handle = new_handle_from_message_copy_guarded(bytes->data(), bytes->size());
  if (!holder.get())
    return std::nullopt;
  long Ni = 0, Nj = 0;
  if (!get_ni_nj(holder.get(), Ni, Nj)) {
    return std::nullopt;
  }
  if (!cp.enabled) {
    return static_cast<size_t>(Ni * Nj);
  }
  auto cw = compute_crop_window(holder.get(), cp, Ni, Nj);
  if (!cw)
    return std::nullopt;
  return static_cast<size_t>(cw->width) * static_cast<size_t>(cw->height);
}

std::optional<std::vector<uint8_t>> decode_and_maybe_crop_values(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const ReadObjectPartFunc& reader) {
  return decode_and_maybe_crop_values(mi, dtype, cp, reader, std::string());
}

std::optional<std::vector<uint8_t>> decode_and_maybe_crop_values(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const ReadObjectPartFunc& reader, const std::string& cache_token) {
  auto decoded = decode_and_maybe_crop_values_typed(mi, dtype, cp, reader,
    cache_token);
  if (!decoded) return std::nullopt;
  return std::visit([](const auto& values) {
    std::vector<uint8_t> bytes(values.size() * sizeof(values.front()));
    if (!bytes.empty())
      std::memcpy(bytes.data(), values.data(), bytes.size());
    return bytes;
  }, *decoded);
}

namespace {

std::optional<DecodedValues> decode_and_maybe_crop_from_handle(
    codes_handle* handle, DataDType dtype, const CropParams& cp) {
  long Ni = 0, Nj = 0;
  if (!get_ni_nj(handle, Ni, Nj) || Ni <= 0 || Nj <= 0) {
    return std::nullopt;
  }
  size_t n = 0;
  if (codes_get_size(handle, "values", &n) != 0 || n == 0) {
    return std::nullopt;
  }
  std::vector<double> vals(n, 0.0);
  size_t want = n;
  if (codes_get_double_array(handle, "values", vals.data(), &want) != 0 ||
    want != n) {
    return std::nullopt;
  }

  if (!cp.enabled) {
    if (dtype == DataDType::F64) return DecodedValues(std::move(vals));
    std::vector<float> converted;
    converted.reserve(vals.size());
    for (const auto value : vals)
      converted.push_back(static_cast<float>(value));
    return DecodedValues(std::move(converted));
  }

  auto cw = compute_crop_window(handle, cp, Ni, Nj);
  if (!cw) {
    LOG_WARN("decode_and_maybe_crop: compute_crop_window failed");
    return std::nullopt;
  }

  size_t cropped_count =
    static_cast<size_t>(cw->width) * static_cast<size_t>(cw->height);
  if (dtype == DataDType::F64) {
    std::vector<double> out;
    out.reserve(cropped_count);
    for (long jj = cw->j0; jj <= cw->j1; ++jj) {
      size_t row_off = static_cast<size_t>(jj) * static_cast<size_t>(Ni);
      for (long offset = 0; offset < cw->width; ++offset) {
        const long ii = crop_column(*cw, offset, Ni);
        out.push_back(vals[row_off + static_cast<size_t>(ii)]);
      }
    }
    return DecodedValues(std::move(out));
  }
  std::vector<float> out;
  out.reserve(cropped_count);
  for (long jj = cw->j0; jj <= cw->j1; ++jj) {
    size_t row_off = static_cast<size_t>(jj) * static_cast<size_t>(Ni);
    for (long offset = 0; offset < cw->width; ++offset) {
      const long ii = crop_column(*cw, offset, Ni);
      out.push_back(static_cast<float>(
        vals[row_off + static_cast<size_t>(ii)]));
    }
  }
  return DecodedValues(std::move(out));
}

} // namespace

std::optional<DecodedValues> decode_and_maybe_crop_values_typed(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const ReadObjectPartFunc& reader, const std::string& cache_token) {
  auto decoder = MessageDecoder::open(mi, reader, cache_token);
  if (!decoder) {
    LOG_WARN("decode_and_maybe_crop: message decoder failed");
    return std::nullopt;
  }
  return decoder->decode_values(dtype, cp, {});
}

std::optional<std::vector<uint8_t>>
decode_values_as_dtype(const MsgInfo& mi, DataDType dtype,
  const ReadObjectPartFunc& reader) {
  return decode_values_as_dtype(mi, dtype, reader, std::string());
}

std::optional<std::vector<uint8_t>>
decode_values_as_dtype(const MsgInfo& mi, DataDType dtype,
  const ReadObjectPartFunc& reader, const std::string& cache_token) {
  auto decoded = decode_values_typed(mi, dtype, reader, cache_token);
  if (!decoded) return std::nullopt;
  return std::visit([](const auto& values) {
    std::vector<uint8_t> bytes(values.size() * sizeof(values.front()));
    if (!bytes.empty())
      std::memcpy(bytes.data(), values.data(), bytes.size());
    return bytes;
  }, *decoded);
}

namespace {

std::optional<DecodedValues> decode_values_from_handle(codes_handle* handle,
    DataDType dtype) {
  size_t n = 0;
  if (codes_get_size(handle, "values", &n) != 0 || n == 0) {
    LOG_WARN("decode_values: values size=0");
    return std::nullopt;
  }

  if (dtype == DataDType::F64) {
    std::vector<double> vals(n, 0.0);
    size_t want = n;
    int err = codes_get_double_array(handle, "values", vals.data(), &want);
    if (err != 0 || want != n) {
      LOG_WARN("decode_values: get_double_array err");
      return std::nullopt;
    }
    return DecodedValues(std::move(vals));
  }

  std::vector<float> vals_f(n, 0.0f);
  size_t want_f = n;
  int err = codes_get_float_array(handle, "values", vals_f.data(), &want_f);
  if (err == 0 && want_f == n) return DecodedValues(std::move(vals_f));
  std::vector<double> vals_d(n, 0.0);
  size_t want_d = n;
  err = codes_get_double_array(handle, "values", vals_d.data(), &want_d);
  if (err != 0 || want_d != n) {
    LOG_WARN("decode_values: fallback get_double_array err");
    return std::nullopt;
  }
  std::vector<float> out;
  out.reserve(n);
  for (size_t i = 0; i < n; ++i)
    out.push_back(static_cast<float>(vals_d[i]));
  return DecodedValues(std::move(out));
}

} // namespace

std::optional<DecodedValues> decode_values_typed(const MsgInfo& mi,
  DataDType dtype, const ReadObjectPartFunc& reader,
  const std::string& cache_token) {
  auto decoder = MessageDecoder::open(mi, reader, cache_token);
  if (!decoder) {
    LOG_WARN("decode_values: message decoder failed");
    return std::nullopt;
  }
  return decoder->decode_values(dtype, {}, {});
}

InterpParams parse_interp_params_from_kv(
  const std::map<std::string, std::string>& kv) {
  InterpParams ip;
  auto it_interp = kv.find("interp");
  if (it_interp != kv.end() &&
    path_utils::to_lower(it_interp->second) == "bilinear")
    ip.enabled = true;

  auto it_outni = kv.find("outNi");
  if (it_outni != kv.end())
    ip.outNi = std::atol(it_outni->second.c_str());
  auto it_outnj = kv.find("outNj");
  if (it_outnj != kv.end())
    ip.outNj = std::atol(it_outnj->second.c_str());
  auto it_dlon = kv.find("dlon");
  if (it_dlon != kv.end())
    ip.dlon = std::atof(it_dlon->second.c_str());
  auto it_dlat = kv.find("dlat");
  if (it_dlat != kv.end())
    ip.dlat = std::atof(it_dlat->second.c_str());

  auto it_border = kv.find("border");
  if (it_border != kv.end()) {
    std::string v = path_utils::to_lower(it_border->second);
    if (v == "nan")
      ip.border = InterpParams::Border::AS_NAN;
    else
      ip.border = InterpParams::Border::CLAMP;
  }
  return ip;
}

namespace {

std::optional<GridAxesDescription> native_grid_axes_from_handle(
    codes_handle* handle) {
  long ni = 0;
  long nj = 0;
  if (!get_ni_nj(handle, ni, nj) || ni <= 0 || nj <= 0)
    return std::nullopt;
  double lat_first = 0.0;
  double lon_first = 0.0;
  double lat_last = 0.0;
  double iinc = 0.0;
  double jinc = 0.0;
  if (!(get_double_safe(handle,
          "latitudeOfFirstGridPointInDegrees", lat_first) ||
        get_double_safe(handle, "latitudeOfFirstGridPoint", lat_first)) ||
      !(get_double_safe(handle,
          "longitudeOfFirstGridPointInDegrees", lon_first) ||
        get_double_safe(handle, "longitudeOfFirstGridPoint", lon_first)) ||
      !(get_double_safe(handle,
          "latitudeOfLastGridPointInDegrees", lat_last) ||
        get_double_safe(handle, "latitudeOfLastGridPoint", lat_last)) ||
      !(get_double_safe(handle,
          "iDirectionIncrementInDegrees", iinc) ||
        get_double_safe(handle, "iDirectionIncrement", iinc)) ||
      !(get_double_safe(handle,
          "jDirectionIncrementInDegrees", jinc) ||
        get_double_safe(handle, "jDirectionIncrement", jinc)))
    return std::nullopt;
  lat_first = to_degrees_from_int_maybe(lat_first);
  lon_first = to_degrees_from_int_maybe(lon_first);
  lat_last = to_degrees_from_int_maybe(lat_last);
  iinc = std::fabs(to_degrees_from_int_maybe(iinc));
  jinc = std::fabs(to_degrees_from_int_maybe(jinc));
  if (!std::isfinite(lat_first) || !std::isfinite(lon_first) ||
      !std::isfinite(lat_last) || !std::isfinite(iinc) ||
      !std::isfinite(jinc) || iinc <= 0.0 || jinc <= 0.0)
    return std::nullopt;

  GridAxesDescription result;
  result.ni = static_cast<std::uint64_t>(ni);
  result.nj = static_cast<std::uint64_t>(nj);
  result.longitude_increment = iinc;
  result.latitude_increment = jinc;
  result.latitudes.reserve(static_cast<std::size_t>(nj));
  result.longitudes.reserve(static_cast<std::size_t>(ni));
  const double signed_jinc = lat_first >= lat_last ? -jinc : jinc;
  for (long j = 0; j < nj; ++j)
    result.latitudes.push_back(lat_first + static_cast<double>(j) *
      signed_jinc);
  for (long i = 0; i < ni; ++i)
    result.longitudes.push_back(lon_first + static_cast<double>(i) * iinc);
  return result;
}

std::optional<GridDescription> output_grid_from_handle(codes_handle* handle,
    const CropParams& cp, const InterpParams& ip) {
  long ni = 0;
  long nj = 0;
  if (!get_ni_nj(handle, ni, nj) || ni <= 0 || nj <= 0)
    return std::nullopt;
  double lat_first = 0.0;
  double lon_first = 0.0;
  double lat_last = 0.0;
  double iinc = 0.0;
  double jinc = 0.0;
  if (!(get_double_safe(handle,
          "latitudeOfFirstGridPointInDegrees", lat_first) ||
        get_double_safe(handle, "latitudeOfFirstGridPoint", lat_first)) ||
      !(get_double_safe(handle,
          "longitudeOfFirstGridPointInDegrees", lon_first) ||
        get_double_safe(handle, "longitudeOfFirstGridPoint", lon_first)) ||
      !(get_double_safe(handle,
          "latitudeOfLastGridPointInDegrees", lat_last) ||
        get_double_safe(handle, "latitudeOfLastGridPoint", lat_last)) ||
      !(get_double_safe(handle,
          "iDirectionIncrementInDegrees", iinc) ||
        get_double_safe(handle, "iDirectionIncrement", iinc)) ||
      !(get_double_safe(handle,
          "jDirectionIncrementInDegrees", jinc) ||
        get_double_safe(handle, "jDirectionIncrement", jinc)))
    return std::nullopt;
  lat_first = to_degrees_from_int_maybe(lat_first);
  lon_first = to_degrees_from_int_maybe(lon_first);
  lat_last = to_degrees_from_int_maybe(lat_last);
  iinc = std::fabs(to_degrees_from_int_maybe(iinc));
  jinc = std::fabs(to_degrees_from_int_maybe(jinc));
  if (!std::isfinite(lat_first) || !std::isfinite(lon_first) ||
      !std::isfinite(lat_last) || !std::isfinite(iinc) ||
      !std::isfinite(jinc) || iinc <= 0.0 || jinc <= 0.0)
    return std::nullopt;

  auto crop = cp.enabled
    ? compute_crop_window(handle, cp, ni, nj)
    : std::optional<CropWindow>{};
  if (cp.enabled && !crop) return std::nullopt;
  const bool lat_descending = lat_first >= lat_last;
  const double signed_jinc = lat_descending ? -jinc : jinc;
  const long i0 = crop ? crop->i0 : 0;
  const long j0 = crop ? crop->j0 : 0;
  const long j1 = crop ? crop->j1 : nj - 1;

  long out_ni = crop ? crop->width : ni;
  long out_nj = j1 - j0 + 1;
  double lon_min = lon_first + static_cast<double>(i0) * iinc;
  double lon_max = lon_min + static_cast<double>(out_ni - 1) * iinc;
  const double first_lat = lat_first + static_cast<double>(j0) * signed_jinc;
  const double last_lat = lat_first + static_cast<double>(j1) * signed_jinc;
  double lat_min = std::min(first_lat, last_lat);
  double lat_max = std::max(first_lat, last_lat);

  GridDescription description;
  description.origin_i = static_cast<std::uint64_t>(i0);
  description.origin_j = static_cast<std::uint64_t>(j0);
  bool interpolated = ip.enabled;
  if (interpolated) {
    out_ni = ip.outNi;
    out_nj = ip.outNj;
    if (out_ni <= 0 || out_nj <= 0) {
      const double span_lon = std::fabs(lon_max - lon_min);
      const double span_lat = std::fabs(lat_max - lat_min);
      out_ni = ip.dlon > 0.0
        ? std::max(1L,
            static_cast<long>(std::round(span_lon / ip.dlon)) + 1)
        : (crop ? crop->width : ni);
      out_nj = ip.dlat > 0.0
        ? std::max(1L,
            static_cast<long>(std::round(span_lat / ip.dlat)) + 1)
        : j1 - j0 + 1;
    }
    description.origin_i = 0;
    description.origin_j = 0;
  }
  if (out_ni <= 0 || out_nj <= 0 ||
      static_cast<std::uint64_t>(out_ni) >
        std::numeric_limits<std::uint64_t>::max() /
        static_cast<std::uint64_t>(out_nj))
    return std::nullopt;
  description.ni = static_cast<std::uint64_t>(out_ni);
  description.nj = static_cast<std::uint64_t>(out_nj);
  if (description.ni > std::numeric_limits<std::size_t>::max() ||
      description.nj > std::numeric_limits<std::size_t>::max())
    return std::nullopt;
  description.latitude_axis.reserve(
    static_cast<std::size_t>(description.nj));
  description.longitude_axis.reserve(
    static_cast<std::size_t>(description.ni));

  const double lon_step = out_ni > 1
    ? (interpolated && ip.dlon > 0.0
        ? ip.dlon
        : (lon_max - lon_min) / static_cast<double>(out_ni - 1))
    : 0.0;
  const double lat_step = out_nj > 1
    ? (interpolated && ip.dlat > 0.0
        ? ip.dlat
        : (lat_max - lat_min) / static_cast<double>(out_nj - 1))
    : 0.0;
  for (long j = 0; j < out_nj; ++j) {
    const double latitude = interpolated
      ? lat_max - static_cast<double>(j) * lat_step
      : first_lat + static_cast<double>(j) * signed_jinc;
    description.latitude_axis.push_back(latitude);
  }
  for (long i = 0; i < out_ni; ++i) {
    const double longitude = lon_min + static_cast<double>(i) * lon_step;
    description.longitude_axis.push_back(crop && crop->wraps_longitude
      ? wrap_longitude_to_grid(longitude, lon_first)
      : longitude);
  }
  return description;
}

} // namespace

std::optional<GridAxesDescription> MessageDecoder::native_grid_axes() const {
  return native_grid_axes_from_handle(implementation_->handle.get());
}

std::optional<GridDescription> MessageDecoder::output_grid(
    const CropParams& cp, const InterpParams& ip) const {
  return output_grid_from_handle(implementation_->handle.get(), cp, ip);
}

std::optional<GridAxesDescription> describe_native_grid_axes(
    const MsgInfo& mi, const ReadObjectPartFunc& reader,
    const std::string& cache_token) {
  auto decoder = MessageDecoder::open(mi, reader, cache_token);
  return decoder ? decoder->native_grid_axes() : std::nullopt;
}

std::optional<GridDescription> describe_output_grid(const MsgInfo& mi,
    const CropParams& cp, const InterpParams& ip,
    const ReadObjectPartFunc& reader, const std::string& cache_token) {
  auto decoder = MessageDecoder::open(mi, reader, cache_token);
  return decoder ? decoder->output_grid(cp, ip) : std::nullopt;
}

std::optional<std::vector<uint8_t>> decode_crop_and_bilinear_resample(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const InterpParams& ip, const ReadObjectPartFunc& reader) {
  return decode_crop_and_bilinear_resample(mi, dtype, cp, ip, reader,
    std::string());
}

std::optional<std::vector<uint8_t>> decode_crop_and_bilinear_resample(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const InterpParams& ip, const ReadObjectPartFunc& reader,
  const std::string& cache_token) {
  auto decoded = decode_crop_and_bilinear_resample_typed(mi, dtype, cp, ip,
    reader, cache_token);
  if (!decoded) return std::nullopt;
  return std::visit([](const auto& values) {
    std::vector<uint8_t> bytes(values.size() * sizeof(values.front()));
    if (!bytes.empty())
      std::memcpy(bytes.data(), values.data(), bytes.size());
    return bytes;
  }, *decoded);
}

namespace {

std::optional<DecodedValues> decode_bilinear_from_handle(
    codes_handle* handle, DataDType dtype, const CropParams& cp,
    const InterpParams& ip) {
  long Ni = 0, Nj = 0;
  if (!get_ni_nj(handle, Ni, Nj) || Ni <= 0 || Nj <= 0) {
    return std::nullopt;
  }

  std::vector<double> vals;
  size_t n = 0;
  if (codes_get_size(handle, "values", &n) != 0 || n == 0) {
    return std::nullopt;
  }
  vals.resize(n);
  size_t want = n;
  if (codes_get_double_array(handle, "values", vals.data(), &want) != 0 ||
    want != n) {
    return std::nullopt;
  }

  double lat_first = 0, lon_first = 0, lat_last = 0, lon_last = 0;
  double iinc = 0, jinc = 0;
  if (!(get_double_safe(handle, "latitudeOfFirstGridPointInDegrees", lat_first) ||
    get_double_safe(handle, "latitudeOfFirstGridPoint", lat_first)))
    return std::nullopt;
  if (!(get_double_safe(handle, "longitudeOfFirstGridPointInDegrees", lon_first) ||
    get_double_safe(handle, "longitudeOfFirstGridPoint", lon_first)))
    return std::nullopt;
  if (!(get_double_safe(handle, "latitudeOfLastGridPointInDegrees", lat_last) ||
    get_double_safe(handle, "latitudeOfLastGridPoint", lat_last)))
    return std::nullopt;
  if (!(get_double_safe(handle, "longitudeOfLastGridPointInDegrees", lon_last) ||
    get_double_safe(handle, "longitudeOfLastGridPoint", lon_last)))
    return std::nullopt;

  lat_first = to_degrees_from_int_maybe(lat_first);
  lon_first = to_degrees_from_int_maybe(lon_first);
  lat_last = to_degrees_from_int_maybe(lat_last);
  lon_last = to_degrees_from_int_maybe(lon_last);

  if (!(get_double_safe(handle, "iDirectionIncrementInDegrees", iinc) ||
    get_double_safe(handle, "iDirectionIncrement", iinc)))
    return std::nullopt;
  if (!(get_double_safe(handle, "jDirectionIncrementInDegrees", jinc) ||
    get_double_safe(handle, "jDirectionIncrement", jinc)))
    return std::nullopt;

  iinc = std::fabs(to_degrees_from_int_maybe(iinc));
  jinc = std::fabs(to_degrees_from_int_maybe(jinc));
  if (iinc <= 0 || jinc <= 0)
    return std::nullopt;

  auto cw = cp.enabled ? compute_crop_window(handle, cp, Ni, Nj)
                       : std::optional<CropWindow>{};
  if (cp.enabled && !cw)
    return std::nullopt;

  const bool lat_descending = (lat_first >= lat_last);
  const bool cyclic_longitude = is_cyclic_longitude_grid(Ni, iinc);
  double lon_min = lon_first;
  double lon_max = lon_last;
  double lat_min = std::min(lat_first, lat_last);
  double lat_max = std::max(lat_first, lat_last);
  if (cw) {
    lon_min = lon_first + cw->i0 * iinc;
    lon_max = lon_min + (cw->width - 1) * iinc;
    const double signed_jinc = lat_descending ? -jinc : jinc;
    const double first_crop_lat = lat_first + cw->j0 * signed_jinc;
    const double last_crop_lat = lat_first + cw->j1 * signed_jinc;
    lat_min = std::min(first_crop_lat, last_crop_lat);
    lat_max = std::max(first_crop_lat, last_crop_lat);
  }

  const double inv_iinc = 1.0 / iinc;
  const double inv_jinc = 1.0 / jinc;
  const double max_u = static_cast<double>(Ni - 1);
  const double max_v = static_cast<double>(Nj - 1);
  const size_t row_stride = static_cast<size_t>(Ni);
  const bool border_clamp = (ip.border == InterpParams::Border::CLAMP);
  const double nan_val = std::numeric_limits<double>::quiet_NaN();

  auto clampi = [](long v, long lo, long hi) {
    return std::max(lo, std::min(hi, v));
    };

  auto sample = [&](double lat, double lon) -> double {
    double u = (lon - lon_first) * inv_iinc;
    double v = (lat_descending ? (lat_first - lat) : (lat - lat_first)) * inv_jinc;
    if (cyclic_longitude) {
      u = std::fmod(u, static_cast<double>(Ni));
      if (u < 0.0) u += static_cast<double>(Ni);
      if (border_clamp) v = std::clamp(v, 0.0, max_v);
      else if (v < 0.0 || v > max_v) return nan_val;
    }
    else if (border_clamp) {
      u = std::clamp(u, 0.0, max_u);
      v = std::clamp(v, 0.0, max_v);
    }
    else if (u < 0.0 || v < 0.0 || u > max_u || v > max_v) {
      return nan_val;
    }

    const double u_floor = std::floor(u);
    const double v_floor = std::floor(v);
    long iL = clampi(static_cast<long>(u_floor), 0, Ni - 1);
    long jT = clampi(static_cast<long>(v_floor), 0, Nj - 1);
    long iR = cyclic_longitude ? (iL + 1) % Ni
      : clampi(iL + 1, 0, Ni - 1);
    long jB = clampi(jT + 1, 0, Nj - 1);
    const double a = u - u_floor;
    const double b = v - v_floor;

    const size_t idxTL = static_cast<size_t>(jT) * row_stride + static_cast<size_t>(iL);
    const size_t idxTR = static_cast<size_t>(jT) * row_stride + static_cast<size_t>(iR);
    const size_t idxBL = static_cast<size_t>(jB) * row_stride + static_cast<size_t>(iL);
    const size_t idxBR = static_cast<size_t>(jB) * row_stride + static_cast<size_t>(iR);

    const double vTL = vals[idxTL];
    const double vTR = vals[idxTR];
    const double vBL = vals[idxBL];
    const double vBR = vals[idxBR];

    const double top = vTL + a * (vTR - vTL);
    const double bottom = vBL + a * (vBR - vBL);
    return top + b * (bottom - top);
    };

  long outNi = ip.outNi;
  long outNj = ip.outNj;

  if (outNi <= 0 || outNj <= 0) {
    double span_lon = std::fabs(lon_max - lon_min);
    double span_lat = std::fabs(lat_max - lat_min);
    if (ip.dlon > 0)
      outNi = std::max(1L, static_cast<long>(std::round(span_lon / ip.dlon)) + 1);
    else
      outNi = (cw ? (cw->width) : Ni);
    if (ip.dlat > 0)
      outNj = std::max(1L, static_cast<long>(std::round(span_lat / ip.dlat)) + 1);
    else
      outNj = (cw ? (cw->height) : Nj);
  }

  const double lon_step =
    (outNi > 1)
    ? ((ip.dlon > 0)
      ? ip.dlon
      : ((lon_max - lon_min) / static_cast<double>(std::max<long>(1, outNi - 1))))
    : 0.0;
  const double lat_step =
    (outNj > 1)
    ? ((ip.dlat > 0)
      ? ip.dlat
      : ((lat_max - lat_min) / static_cast<double>(std::max<long>(1, outNj - 1))))
    : 0.0;

  if (dtype == DataDType::F64) {
    std::vector<double> out;
    out.reserve(static_cast<size_t>(outNi) * static_cast<size_t>(outNj));
    double lat = lat_max;
    for (long j = 0; j < outNj; ++j, lat -= lat_step) {
      double lon = lon_min;
      for (long i = 0; i < outNi; ++i, lon += lon_step) {
        out.push_back(sample(lat, lon));
      }
    }
    return DecodedValues(std::move(out));
  }
  std::vector<float> out;
  out.reserve(static_cast<size_t>(outNi) * static_cast<size_t>(outNj));
  double lat = lat_max;
  for (long j = 0; j < outNj; ++j, lat -= lat_step) {
    double lon = lon_min;
    for (long i = 0; i < outNi; ++i, lon += lon_step) {
      const double v = sample(lat, lon);
      out.push_back(static_cast<float>(v));
    }
  }
  return DecodedValues(std::move(out));
}

} // namespace

std::optional<DecodedValues> MessageDecoder::decode_values(DataDType dtype,
    const CropParams& cp, const InterpParams& ip) const {
  auto* handle = implementation_->handle.get();
  if (ip.enabled) return decode_bilinear_from_handle(handle, dtype, cp, ip);
  if (cp.enabled) return decode_and_maybe_crop_from_handle(handle, dtype, cp);
  return decode_values_from_handle(handle, dtype);
}

std::optional<DecodedValues> decode_crop_and_bilinear_resample_typed(
  const MsgInfo& mi, DataDType dtype, const CropParams& cp,
  const InterpParams& ip, const ReadObjectPartFunc& reader,
  const std::string& cache_token) {
  auto decoder = MessageDecoder::open(mi, reader, cache_token);
  if (!decoder) {
    LOG_WARN("bilinear: message decoder failed");
    return std::nullopt;
  }
  return decoder->decode_values(dtype, cp, ip);
}

void configure_message_cache(size_t max_entries) {
  MessageCache::instance().set_max_entries(max_entries == 0 ? 1 : max_entries);
}

} // namespace grib_utils
