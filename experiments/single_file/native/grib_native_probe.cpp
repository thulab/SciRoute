// Standalone ecCodes filesystem baseline. No IoTDB, sidecar or external index.
// Discovery uses the public headers-only GRIB API, rather than fetching every
// complete message. All selected payload reads, native unpacking, selection,
// owned output copies and cleanup are charged to the query timer. Digests and
// JSON serialization are outside timers. Coordinates use native scanning order.
#include <eccodes.h>
#include <grib_api.h>
#include <openssl/evp.h>
#include <sys/stat.h>
#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
using Bytes = std::vector<std::uint8_t>;
constexpr std::uint64_t MiB = 1024ULL * 1024;
struct Options {
  std::string file, view{"values"};
  std::optional<std::string> short_name, type_of_level;
  std::optional<long> level, step;
  std::optional<std::pair<double, double>> latitude, longitude;
  std::uint64_t repeat{1}, max_result_bytes{64 * MiB}, max_working_bytes{256 * MiB},
      max_candidates{100000};
  bool bound{false};
};
void require(bool ok, const std::string& message) {
  if (!ok) throw std::runtime_error(message);
}
void check(int error, const std::string& operation) {
  require(error == CODES_SUCCESS, operation + ": " + codes_get_error_message(error));
}
std::uint64_t elapsed(Clock::time_point begin) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - begin).count();
}
std::uint64_t unsigned_integer(const std::string& text) {
  require(!text.empty() && text.find_first_not_of("0123456789") == std::string::npos,
          "expected unsigned decimal integer");
  std::size_t used = 0;
  auto n = std::stoull(text, &used);
  require(used == text.size(), "invalid integer");
  return n;
}
long signed_integer(const std::string& text) {
  std::size_t used = 0;
  auto n = std::stol(text, &used);
  require(!text.empty() && used == text.size(), "invalid signed integer");
  return n;
}
std::pair<double, double> range(const std::string& text) {
  const auto comma = text.find(',');
  require(comma != std::string::npos && text.find(',', comma + 1) == std::string::npos,
          "range must be LOW,HIGH");
  auto number = [](const std::string& value) {
    std::size_t used = 0;
    const double n = std::stod(value, &used);
    require(used == value.size() && std::isfinite(n), "range bound must be finite");
    return n;
  };
  auto low = number(text.substr(0, comma)), high = number(text.substr(comma + 1));
  require(low <= high, "range requires LOW <= HIGH; longitude wrapping is unsupported");
  return {low, high};
}
Options options(int argc, char** argv) {
  Options out;
  std::vector<std::string> seen;
  for (int i = 1; i < argc; ++i) {
    const std::string key = argv[i];
    require(std::find(seen.begin(), seen.end(), key) == seen.end(), "duplicate option " + key);
    seen.push_back(key);
    if (key == "--bound") { out.bound = true; continue; }
    require(i + 1 < argc, "missing option argument for " + key);
    const std::string value = argv[++i];
    if (key == "--file") out.file = value;
    else if (key == "--view") out.view = value;
    else if (key == "--short-name") out.short_name = value;
    else if (key == "--type-of-level") out.type_of_level = value;
    else if (key == "--level") out.level = signed_integer(value);
    else if (key == "--step") out.step = signed_integer(value);
    else if (key == "--lat-range") out.latitude = range(value);
    else if (key == "--lon-range") out.longitude = range(value);
    else if (key == "--repeat") out.repeat = unsigned_integer(value);
    else if (key == "--max-result-bytes") out.max_result_bytes = unsigned_integer(value);
    else if (key == "--max-working-bytes") out.max_working_bytes = unsigned_integer(value);
    else if (key == "--max-candidates") out.max_candidates = unsigned_integer(value);
    else throw std::runtime_error("unknown option " + key);
  }
  require(!out.file.empty(), "--file is required");
  require(out.view == "binary" || out.view == "values", "--view requires binary or values");
  require(out.view == "values" || (!out.latitude && !out.longitude),
          "coordinate slices cannot be applied to encoded message bytes");
  require(out.repeat > 0 && out.repeat <= 10000, "repeat outside [1,10000]");
  require(out.max_result_bytes > 0 && out.max_result_bytes <= 1024 * MiB &&
          out.max_working_bytes > 0 && out.max_working_bytes <= 4096 * MiB &&
          out.max_candidates > 0 && out.max_candidates <= 1000000,
          "limits outside supported bounded range");
  return out;
}
std::string quote(const std::string& text) {
  std::ostringstream out;
  out << '"';
  for (unsigned char c : text) {
    if (c == '"' || c == '\\') out << '\\' << char(c);
    else if (c < 32) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << unsigned(c);
    else out << char(c);
  }
  out << '"';
  return out.str();
}
std::string version() {
  const auto v = codes_get_api_version();
  return std::to_string(v / 10000) + "." + std::to_string((v / 100) % 100) + "." + std::to_string(v % 100);
}
struct Handle {
  codes_handle* ptr;
  explicit Handle(codes_handle* h) : ptr(h) { require(ptr, "cannot construct native GRIB handle"); }
  ~Handle() { if (ptr) codes_handle_delete(ptr); }
  Handle(const Handle&) = delete;
  Handle& operator=(const Handle&) = delete;
  void close() { check(codes_handle_delete(ptr), "delete GRIB handle"); ptr = nullptr; }
};
long get_long(codes_handle* handle, const char* key) {
  long result = 0; check(codes_get_long(handle, key, &result), std::string("read ") + key); return result;
}
double get_double(codes_handle* handle, const char* key) {
  double result = 0; check(codes_get_double(handle, key, &result), std::string("read ") + key); return result;
}
std::string get_string(codes_handle* handle, const char* key) {
  std::array<char, 1024> result{};
  std::size_t n = result.size();
  check(codes_get_string(handle, key, result.data(), &n), std::string("read ") + key);
  return result.data();
}
struct Message { std::uint64_t offset, length; };
struct Field {
  Bytes raw;
  std::vector<double> values;
  long ni{0}, nj{0};
  double lat0{0}, lon0{0}, signed_dj{0}, di{0};
  bool loaded{false};
  std::uint64_t bytes() const { return raw.capacity() + values.capacity() * sizeof(double); }
};
struct Session {
  FILE* file{nullptr};
  std::uint64_t source_bytes{0}, candidates{0};
  std::vector<Message> selected;
  std::vector<Field> cache;
  std::uint64_t resident_bytes{0};
  std::uint64_t metadata_bytes() const {
    return selected.capacity() * sizeof(Message) + cache.capacity() * sizeof(Field);
  }
  ~Session() { if (file) fclose(file); }
  void close() {
    std::vector<Field>().swap(cache); std::vector<Message>().swap(selected);
    resident_bytes = 0;
    auto* f = file; file = nullptr; require(fclose(f) == 0, "fclose failed");
  }
};
struct Timings {
  std::uint64_t open{0}, discovery{0}, payload{0}, decode{0}, selection{0}, copy{0}, close{0}, total{0};
};
void add_charge(std::uint64_t& total, std::uint64_t bytes, std::uint64_t limit, const char* what) {
  require(total <= limit && bytes <= limit - total, std::string(what) + " budget exceeded"); total += bytes;
}
void seek(FILE* f, std::uint64_t offset) {
  require(offset <= static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) &&
          fseeko(f, static_cast<off_t>(offset), SEEK_SET) == 0, "source seek failed");
}
void read_exact(FILE* f, void* data, std::size_t n) {
  require(fread(data, 1, n, f) == n, "short or failed source read");
}
void open(Session& session, const Options& opt, Timings& timing) {
  const auto begin = Clock::now();
  session.file = fopen(opt.file.c_str(), "rb");
  require(session.file, "cannot open source file");
  struct stat st{};
  require(fstat(fileno(session.file), &st) == 0 && S_ISREG(st.st_mode) && st.st_size >= 0,
          "source must be a regular file with a valid size");
  session.source_bytes = static_cast<std::uint64_t>(st.st_size);
  timing.open += elapsed(begin);
}
bool matches(codes_handle* h, const Options& opt) {
  // Evaluate cheap numeric terms before ecCodes concept/string resolution.
  if (opt.level && get_long(h, "level") != *opt.level) return false;
  if (opt.step && get_long(h, "step") != *opt.step) return false;
  if (opt.type_of_level && get_string(h, "typeOfLevel") != *opt.type_of_level) return false;
  if (opt.short_name && get_string(h, "shortName") != *opt.short_name) return false;
  return true;
}
void discover(Session& session, const Options& opt, Timings& timing) {
  const auto begin = Clock::now();
  // Let the native headers-only reader do the GRIB framing/metadata work.
  // No extra per-message indicator/footer reads or duplicate envelope parsing.
  // Offset + totalLength are checked against fstat and contiguous boundaries;
  // native parser errors and premature EOF are never turned into a cache miss.
  std::uint64_t position = 0;
  while (position < session.source_bytes) {
    require(session.candidates < opt.max_candidates, "candidate message limit exhausted");
    int error = CODES_SUCCESS;
    auto* native_header = grib_new_from_file(nullptr, session.file, 1, &error);
    if (!native_header) {
      check(error, "headers-only message discovery");
      throw std::runtime_error("premature EOF or non-GRIB bytes during headers-only discovery");
    }
    Handle header(native_header);
    check(error, "headers-only message discovery");
    off_t offset = -1; check(codes_get_message_offset(header.ptr, &offset), "message offset");
    const auto reported = get_long(header.ptr, "totalLength");
    require(offset >= 0 && std::uint64_t(offset) == position && reported > 0 &&
            std::uint64_t(reported) <= session.source_bytes - position,
            "native header range is non-contiguous, invalid or truncated");
    const auto length = static_cast<std::uint64_t>(reported);
    const auto cursor = ftello(session.file);
    require(cursor >= offset && cursor <= static_cast<off_t>(position + length),
            "native headers-only file position is invalid");
    std::size_t native_header_bytes = 0;
    check(codes_get_message_size(header.ptr, &native_header_bytes), "native header buffer size");
    std::uint64_t working = session.selected.capacity() * sizeof(Message);
    add_charge(working, native_header_bytes, opt.max_working_bytes, "header working memory");
    ++session.candidates;
    if (matches(header.ptr, opt)) {
      require((session.selected.size() + 1) <= opt.max_working_bytes / 2 / sizeof(Message),
              "selected message metadata exceeds working budget");
      session.selected.push_back({position, length});
    }
    header.close();
    position += length;
    // GRIB1 headers-only scanning can leave FILE before the message terminator.
    // Seek only when needed to start the next message at the verified boundary.
    if (position < session.source_bytes && cursor != static_cast<off_t>(position))
      seek(session.file, position);
  }
  timing.discovery += elapsed(begin);
}
struct Output {
  Message source;
  std::uint64_t height{0}, width{0}, rows{0};
  Bytes value, mask, latitudes, longitudes;
};
struct Observation {
  std::vector<Output> messages;
  std::uint64_t payload_bytes{0}, payload_reads{0}, decodes{0}, rows{0}, output_bytes{0},
      cache_hits{0}, cache_misses{0}, cached_payload_bytes{0}, cached_decoded_bytes{0}, cache_resident_bytes{0};
};
template<class T> Bytes bytes_of(const std::vector<T>& values) {
  Bytes result(values.size() * sizeof(T));
  if (!result.empty()) std::memcpy(result.data(), values.data(), result.size());
  return result;
}
bool inside(double n, const std::optional<std::pair<double, double>>& bound) {
  return !bound || (n >= bound->first && n <= bound->second);
}
void validate_single_field(const Bytes& raw) {
  if (raw[7] != 2) return;
  std::size_t position = 16;
  unsigned products = 0, data = 0;
  while (position < raw.size() - 4) {
    require(raw.size() - 4 - position >= 5, "truncated GRIB2 section header");
    std::uint64_t length = 0;
    for (unsigned i = 0; i < 4; ++i) length = (length << 8) | raw[position + i];
    require(length >= 5 && length <= raw.size() - 4 - position, "invalid GRIB2 section length");
    products += raw[position + 4] == 4;
    data += raw[position + 4] == 7;
    position += length;
  }
  require(position == raw.size() - 4 && products == 1 && data == 1,
          "values reader requires exactly one field per GRIB2 message");
}
std::uint64_t resident(const Session& session, const Options& opt, const Field& current) {
  // In a bound session, current raw/values are already included in resident_bytes.
  return session.metadata_bytes() + session.resident_bytes + (opt.bound ? 0 : current.bytes());
}
void load(Session& session, const Options& opt, Message message, Field& field,
          Timings& timing, Observation& observation) {
  if (field.loaded) { ++observation.cache_hits; return; }
  if (opt.bound) ++observation.cache_misses;
  auto begin = Clock::now();
  std::uint64_t working = session.metadata_bytes() + session.resident_bytes;
  add_charge(working, observation.output_bytes, opt.max_working_bytes, "working memory");
  add_charge(working, message.length * 2, opt.max_working_bytes, "working memory");
  seek(session.file, message.offset);
  field.raw.resize(message.length);
  read_exact(session.file, field.raw.data(), field.raw.size());
  if (opt.bound) session.resident_bytes += field.raw.capacity();
  observation.payload_bytes += field.raw.size(); ++observation.payload_reads;
  timing.payload += elapsed(begin);
  if (opt.view == "values") {
    begin = Clock::now();
    validate_single_field(field.raw);
    Handle handle(codes_handle_new_from_message_copy(nullptr, field.raw.data(), field.raw.size()));
    require(get_string(handle.ptr, "gridType") == "regular_ll", "values reader requires regular_ll grid");
    require(get_long(handle.ptr, "iScansNegatively") == 0 && get_long(handle.ptr, "jPointsAreConsecutive") == 0 &&
            get_long(handle.ptr, "alternativeRowScanning") == 0,
            "unsupported non-row-major or reversed-longitude scanning mode");
    field.ni = get_long(handle.ptr, "Ni"); field.nj = get_long(handle.ptr, "Nj");
    require(field.ni > 0 && field.nj > 0 &&
            std::uint64_t(field.ni) <= opt.max_working_bytes / 8 / std::uint64_t(field.nj),
            "invalid or excessive regular grid shape");
    const auto count = std::uint64_t(field.ni) * std::uint64_t(field.nj);
    std::size_t value_count = 0; check(codes_get_size(handle.ptr, "values", &value_count), "values count");
    require(count == value_count, "grid shape disagrees with native values count");
    working = resident(session, opt, field);
    add_charge(working, message.length, opt.max_working_bytes, "native handle working memory");
    add_charge(working, observation.output_bytes, opt.max_working_bytes, "working memory");
    add_charge(working, count * sizeof(double), opt.max_working_bytes, "decoded cache working memory");
    field.values.resize(value_count);
    check(codes_get_double_array(handle.ptr, "values", field.values.data(), &value_count), "native values unpack");
    if (opt.bound) session.resident_bytes += field.values.capacity() * sizeof(double);
    ++observation.decodes;
    field.lat0 = get_double(handle.ptr, "latitudeOfFirstGridPointInDegrees");
    field.lon0 = get_double(handle.ptr, "longitudeOfFirstGridPointInDegrees");
    const double lat_last = get_double(handle.ptr, "latitudeOfLastGridPointInDegrees");
    field.di = get_double(handle.ptr, "iDirectionIncrementInDegrees");
    const double dj = get_double(handle.ptr, "jDirectionIncrementInDegrees");
    require(std::isfinite(field.lat0) && std::isfinite(field.lon0) && std::isfinite(lat_last) &&
            std::isfinite(field.di) && std::isfinite(dj) && field.di > 0 && dj > 0,
            "invalid regular grid geometry");
    field.signed_dj = field.lat0 >= lat_last ? -dj : dj;
    timing.decode += elapsed(begin);
    begin = Clock::now(); handle.close(); timing.close += elapsed(begin);
  }
  field.loaded = true;
}
Output render(const Session& session, const Options& opt, Message message, const Field& field,
              Timings& timing, Observation& observation) {
  Output output; output.source = message;
  auto begin = Clock::now();
  auto working = resident(session, opt, field);
  if (opt.view == "binary") {
    add_charge(observation.output_bytes, field.raw.size() + 1, opt.max_result_bytes, "result");
    add_charge(working, observation.output_bytes, opt.max_working_bytes, "working memory");
    output.rows = 1; output.mask = {1};
    timing.selection += elapsed(begin);
    begin = Clock::now(); output.value = field.raw; timing.copy += elapsed(begin);
  } else {
    require(!opt.longitude || (opt.longitude->first >= field.lon0 && opt.longitude->second <= field.lon0 + 360.0),
            "longitude slice must use native coordinates; wrapping/normalization is unsupported");
    add_charge(working, (std::uint64_t(field.ni) + std::uint64_t(field.nj)) * sizeof(std::size_t),
               opt.max_working_bytes, "selection working memory");
    std::vector<std::size_t> selected_i, selected_j;
    selected_i.reserve(field.ni); selected_j.reserve(field.nj);
    for (long i = 0; i < field.ni; ++i)
      if (inside(field.lon0 + i * field.di, opt.longitude)) selected_i.push_back(i);
    for (long j = 0; j < field.nj; ++j)
      if (inside(field.lat0 + j * field.signed_dj, opt.latitude)) selected_j.push_back(j);
    output.height = selected_j.size(); output.width = selected_i.size();
    if (selected_i.empty() || selected_j.empty()) output.height = output.width = 0;
    output.rows = output.height * output.width;
    const auto prior_output = observation.output_bytes;
    add_charge(observation.output_bytes, output.rows * 21, opt.max_result_bytes, "result");
    // Already returned messages, new crop temporaries and owned output coexist.
    add_charge(working, prior_output, opt.max_working_bytes, "working memory");
    add_charge(working, output.rows * 42, opt.max_working_bytes, "working memory");
    std::vector<float> cropped; std::vector<double> latitudes, longitudes;
    cropped.reserve(output.rows); latitudes.reserve(output.rows); longitudes.reserve(output.rows);
    for (auto j : selected_j) for (auto i : selected_i) {
      cropped.push_back(static_cast<float>(field.values[j * field.ni + i]));
      latitudes.push_back(field.lat0 + j * field.signed_dj); longitudes.push_back(field.lon0 + i * field.di);
    }
    // Physical values match production: bitmap sentinels remain non-null values.
    Bytes mask(output.rows, 1);
    timing.selection += elapsed(begin);
    begin = Clock::now();
    output.value = bytes_of(cropped); output.latitudes = bytes_of(latitudes);
    output.longitudes = bytes_of(longitudes); output.mask = mask;
    timing.copy += elapsed(begin);
    begin = Clock::now();
    std::vector<float>().swap(cropped); std::vector<double>().swap(latitudes);
    std::vector<double>().swap(longitudes); Bytes().swap(mask);
    std::vector<std::size_t>().swap(selected_i); std::vector<std::size_t>().swap(selected_j);
    timing.close += elapsed(begin);
  }
  observation.rows += output.rows;
  return output;
}
Observation execute(Session& session, const Options& opt, Timings& timing) {
  Observation out;
  out.messages.reserve(session.selected.size());
  for (std::size_t i = 0; i < session.selected.size(); ++i) {
    const auto& message = session.selected[i];
    if (opt.bound) {
      auto& field = session.cache[i];
      load(session, opt, message, field, timing, out);
      out.messages.push_back(render(session, opt, message, field, timing, out));
    } else {
      Field field;
      load(session, opt, message, field, timing, out);
      out.messages.push_back(render(session, opt, message, field, timing, out));
      const auto begin = Clock::now();
      Bytes().swap(field.raw); std::vector<double>().swap(field.values);
      timing.close += elapsed(begin);
    }
  }
  if (opt.bound) {
    out.cache_resident_bytes = session.metadata_bytes() + session.resident_bytes;
    for (const auto& field : session.cache) {
      out.cached_payload_bytes += field.raw.capacity();
      out.cached_decoded_bytes += field.values.capacity() * sizeof(double);
    }
  }
  return out;
}
class Digest {
  EVP_MD_CTX* context{EVP_MD_CTX_new()};
public:
  Digest() { require(context && EVP_DigestInit_ex(context, EVP_sha256(), nullptr) == 1, "SHA-256 init failed"); }
  ~Digest() { EVP_MD_CTX_free(context); }
  void add(const Bytes& bytes) { require(EVP_DigestUpdate(context, bytes.data(), bytes.size()) == 1, "SHA-256 update failed"); }
  std::string finish() {
    unsigned char bytes[EVP_MAX_MD_SIZE]; unsigned n = 0;
    require(EVP_DigestFinal_ex(context, bytes, &n) == 1, "SHA-256 finish failed");
    std::ostringstream out; out << std::hex << std::setfill('0');
    for (unsigned i = 0; i < n; ++i) out << std::setw(2) << unsigned(bytes[i]);
    return out.str();
  }
};
std::string digest(const Bytes& bytes) { Digest d; d.add(bytes); return d.finish(); }
std::string serialize(std::uint64_t iteration, const Options& opt, const Session& session,
                      const Observation& result, const Timings& t,
                      std::uint64_t bind_ns, std::uint64_t session_close_ns,
                      std::uint64_t session_open_ns = 0, std::uint64_t session_discovery_ns = 0) {
  Digest values, mask, latitudes, longitudes;
  std::uint64_t value_bytes = 0;
  std::ostringstream messages, offsets, lengths;
  messages << '['; offsets << '['; lengths << '[';
  for (std::size_t i = 0; i < result.messages.size(); ++i) {
    const auto& m = result.messages[i];
    values.add(m.value); mask.add(m.mask); latitudes.add(m.latitudes); longitudes.add(m.longitudes);
    value_bytes += m.value.size();
    if (i) { messages << ','; offsets << ','; lengths << ','; }
    offsets << m.source.offset; lengths << m.source.length;
    messages << "{\"offset\":" << m.source.offset << ",\"length\":" << m.source.length
             << ",\"shape\":" << (opt.view == "binary" ? "[]" : "[" + std::to_string(m.height) + "," + std::to_string(m.width) + "]") << ",\"rows\":" << m.rows
             << ",\"value_sha256\":" << quote(digest(m.value)) << ",\"mask_sha256\":" << quote(digest(m.mask))
             << ",\"latitude_sha256\":" << quote(digest(m.latitudes))
             << ",\"longitude_sha256\":" << quote(digest(m.longitudes)) << '}';
  }
  messages << ']'; offsets << ']'; lengths << ']';
  std::ostringstream out;
  out << "{\"protocol\":\"iotdbfs-single-file-grib-native/1\",\"iteration\":" << iteration
      << ",\"mode\":" << quote(opt.bound ? "bound" : "unbound") << ",\"runtime_eccodes\":" << quote(version())
      << ",\"view\":" << quote(opt.view) << ",\"dtype\":" << quote(opt.view == "binary" ? "bytes" : "<f4")
      << ",\"coordinate_dtype\":\"<f8\",\"mask_dtype\":\"uint8\",\"order\":\"native-row-major\""
      << ",\"discovery_policy\":\"native-headers-only-strict-contiguous-messages\""
      << ",\"metadata_read_bytes\":null,\"metadata_read_bytes_measured\":false"
      << ",\"cache_policy\":" << quote(opt.bound ? "all-selected-encoded-and-native-decoded-bounded" : "none")
      << ",\"cache_hits\":" << result.cache_hits << ",\"cache_misses\":" << result.cache_misses
      << ",\"cached_payload_bytes\":" << result.cached_payload_bytes
      << ",\"cached_decoded_bytes\":" << result.cached_decoded_bytes
      << ",\"cache_resident_bytes\":" << result.cache_resident_bytes
      << ",\"max_working_bytes\":" << opt.max_working_bytes
      << ",\"max_result_bytes\":" << opt.max_result_bytes
      << ",\"working_budget_scope\":\"reader-buffers-and-estimated-native-message-copy-excludes-library-global-allocations\""
      << ",\"source_bytes\":" << session.source_bytes
      << ",\"candidate_messages\":" << session.candidates << ",\"selected_messages\":" << result.messages.size()
      << ",\"rows\":" << result.rows << ",\"null_count\":0,\"value_bytes\":" << value_bytes
      << ",\"output_bytes\":" << result.output_bytes << ",\"actual_payload_read_bytes\":" << result.payload_bytes
      << ",\"payload_read_calls\":" << result.payload_reads << ",\"decoded_messages\":" << result.decodes
      << ",\"message_offsets\":" << offsets.str() << ",\"message_lengths\":" << lengths.str()
      << ",\"messages\":" << messages.str() << ",\"value_sha256\":" << quote(values.finish())
      << ",\"mask_sha256\":" << quote(mask.finish()) << ",\"latitude_sha256\":" << quote(latitudes.finish())
      << ",\"longitude_sha256\":" << quote(longitudes.finish())
      << ",\"open_file_ns\":" << t.open << ",\"discovery_ns\":" << t.discovery
      << ",\"payload_read_ns\":" << t.payload << ",\"decode_ns\":" << t.decode
      << ",\"selection_setup_ns\":" << t.selection << ",\"owned_copy_ns\":" << t.copy
      << ",\"close_ns\":" << t.close << ",\"total_ns\":" << t.total
      << ",\"other_ns\":" << (t.total - t.open - t.discovery - t.payload - t.decode - t.selection - t.copy - t.close)
      << ",\"session_bind_ns\":" << bind_ns << ",\"session_close_ns\":" << session_close_ns
      << ",\"session_open_file_ns\":" << session_open_ns
      << ",\"session_discovery_ns\":" << session_discovery_ns
      << ",\"session_other_ns\":" << (bind_ns - session_open_ns - session_discovery_ns) << '}';
  return out.str();
}
} // namespace
int main(int argc, char** argv) {
  try {
    const std::uint16_t endian = 1;
    require(*reinterpret_cast<const std::uint8_t*>(&endian) == 1, "canonical hashes require little-endian host");
    codes_grib_multi_support_off(nullptr);
    if (argc == 2 && std::string(argv[1]) == "--versions") {
      std::cout << "{\"runtime_eccodes\":" << quote(version())
                << ",\"headers_only_api\":\"grib_new_from_file\",\"multi_field_messages\":false,\"max_result_bytes\":67108864,\"max_working_bytes\":268435456,\"max_candidates\":100000,\"bound_supported\":true,\"bound_cache_policy\":\"all-selected-encoded-and-native-decoded-bounded\",\"working_budget_scope\":\"reader-buffers-and-estimated-native-message-copy-excludes-library-global-allocations\"}\n";
      return 0;
    }
    const auto opt = options(argc, argv);
    if (opt.bound) {
      const auto bind_begin = Clock::now();
      Session session; Timings bind_timing;
      open(session, opt, bind_timing); discover(session, opt, bind_timing);
      std::uint64_t working = session.selected.capacity() * sizeof(Message);
      add_charge(working, session.selected.size() * sizeof(Field), opt.max_working_bytes, "bound metadata cache");
      session.cache.resize(session.selected.size());
      const auto bind_ns = elapsed(bind_begin);
      for (std::uint64_t i = 0; i < opt.repeat; ++i) {
        const auto begin = Clock::now();
        Timings timing;
        auto result = execute(session, opt, timing);
        timing.total = elapsed(begin);
        std::uint64_t session_close_ns = 0;
        if (i + 1 == opt.repeat) {
          const auto close_begin = Clock::now(); session.close(); session_close_ns = elapsed(close_begin);
        }
        std::cout << serialize(i, opt, session, result, timing, i == 0 ? bind_ns : 0, session_close_ns,
                               i == 0 ? bind_timing.open : 0, i == 0 ? bind_timing.discovery : 0) << '\n';
      }
      return 0;
    }
    for (std::uint64_t i = 0; i < opt.repeat; ++i) {
      const auto begin = Clock::now();
      Session session; Timings timing;
      open(session, opt, timing); discover(session, opt, timing);
      auto result = execute(session, opt, timing);
      const auto close_begin = Clock::now(); session.close(); timing.close += elapsed(close_begin);
      timing.total = elapsed(begin);
      std::cout << serialize(i, opt, session, result, timing, 0, 0) << '\n';
    }
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "{\"error\":" << quote(e.what()) << "}\n";
    return 1;
  }
}
