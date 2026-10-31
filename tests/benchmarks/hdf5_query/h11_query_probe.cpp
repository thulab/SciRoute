#include "../../support/InMemoryDatasetCatalog.h"
// H11: strong native A/C and identical production typed reader B/D.
// Production readers are unchanged. All paths return owned contiguous values;
// native H5Dread writes the final vector, while typed batches are concatenated.
// Those real costs are timed; canonical hashing/JSON are outside timing.
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/migration/MigrationPolicy.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include <H5Dpublic.h>
#include <H5Fpublic.h>
#include <H5PLpublic.h>
#include <H5Ppublic.h>
#include <H5Spublic.h>
#include <H5Tpublic.h>
#include <algorithm>
#include <atomic>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <sys/resource.h>
#include <sys/stat.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <variant>

namespace {
using namespace iotdbfs;
using Clock = std::chrono::steady_clock;
using Json = boost::property_tree::ptree;
constexpr std::uint64_t max_bytes = 64ULL * 1024 * 1024;
void need(bool yes, const std::string &why) {
  if (!yes)
    throw std::runtime_error(why);
}
std::uint64_t steady_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             Clock::now().time_since_epoch())
      .count();
}
std::uint64_t ns(Clock::time_point t) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t)
      .count();
}
std::string q(const std::string &s) {
  std::string out = "\"";
  const char *h = "0123456789abcdef";
  for (unsigned char c : s) {
    if (c == '"' || c == '\\') {
      out += '\\';
      out += c;
    } else if (c < 32) {
      out += "\\u00";
      out += h[c >> 4];
      out += h[c & 15];
    } else
      out += c;
  }
  return out + '"';
}
void object(std::ostream &out, const std::map<std::string, std::uint64_t> &m) {
  out << '{';
  bool first = true;
  for (const auto &[k, v] : m) {
    if (!first)
      out << ',';
    first = false;
    out << q(k) << ':' << v;
  }
  out << '}';
}
void numbers(std::ostream &out, const std::vector<std::uint64_t> &v) {
  out << '[';
  for (std::size_t i = 0; i < v.size(); ++i) {
    if (i)
      out << ',';
    out << v[i];
  }
  out << ']';
}
std::uint64_t get_uint(const Json &n, const std::string &key,
                       std::uint64_t fallback) {
  const auto text = n.get<std::string>(key, std::to_string(fallback));
  need(!text.empty() &&
           text.find_first_not_of("0123456789") == std::string::npos,
       "invalid integer " + key);
  return std::stoull(text);
}
std::vector<std::uint64_t> dims(const Json &n, const char *key) {
  std::vector<std::uint64_t> v;
  for (const auto &p : n.get_child(key)) {
    need(p.first.empty(), "dimension must be array");
    Json item;
    item.put("v", p.second.get_value<std::string>());
    v.push_back(get_uint(item, "v", 0));
  }
  need(!v.empty() && v.size() <= 3, "rank must be 1..3");
  return v;
}
struct Fd {
  int value{-1};
  Fd() = default;
  explicit Fd(const std::string &path) {
    value = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    need(value >= 0, "cannot open " + path);
  }
  ~Fd() {
    if (value >= 0)
      ::close(value);
  }
  Fd(const Fd &) = delete;
};
std::uint64_t size(int fd) {
  struct stat st {};
  need(::fstat(fd, &st) == 0 && S_ISREG(st.st_mode) && st.st_size > 0 &&
           static_cast<std::uint64_t>(st.st_size) <= max_bytes,
       "file outside 64 MiB cap");
  return st.st_size;
}
BinaryValue pread_bytes(int fd, std::uint64_t o, std::uint64_t n) {
  need(n <= max_bytes, "oversized pread");
  BinaryValue b(n);
  std::size_t done = 0;
  while (done < b.size()) {
    auto r = ::pread(fd, b.data() + done, b.size() - done, o + done);
    if (r < 0 && errno == EINTR)
      continue;
    need(r > 0, "short/failed pread");
    done += r;
  }
  return b;
}
void save(const std::string &path, const BinaryValue &b) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(reinterpret_cast<const char *>(b.data()), b.size());
  out.close();
  need(bool(out), "cannot save sidecar");
}
struct DecodeTrace {
  std::mutex mutex;
  std::set<std::string> keys;
  std::uint64_t bytes{0}, count{0};
  bool complete{true};
};
struct Source {
  std::string id, local, fuse, layout, sha256;
  StructuredBlobVersionKey key;
};
struct Case {
  std::string id, source, dataset;
  std::vector<std::uint64_t> origin, shape;
  std::uint64_t elements{1};
};
struct Counts {
  std::uint64_t manifests, payload_calls, payload_ranges, payload_bytes,
      layout_calls, layout_bytes;
};
class Backend final : public StructuredBlobStorageBackend {
  struct Local {
    std::shared_ptr<Fd> payload, layout;
    StructuredBlobManifest manifest;
    std::uint64_t written{0};
  };
  std::map<StructuredBlobVersionKey, Local> files_;
  std::shared_ptr<IoTDBStructuredBlobBackend> remote_;
  bool multi_;
  using CacheKey =
      std::tuple<StructuredBlobVersionKey, std::uint64_t, std::uint64_t>;
  struct Encoded {
    BinaryValue bytes;
    std::uint64_t stamp;
  };
  mutable std::map<CacheKey, Encoded> encoded_;
  mutable std::mutex encoded_mu_;
  std::uint64_t encoded_budget_{0};
  mutable std::uint64_t encoded_live_{0}, stamp_{0}, encoded_hits_{0},
      encoded_misses_{0}, encoded_evictions_{0};
  std::optional<BinaryValue> cached(const CacheKey &key) const {
    if (!encoded_budget_)
      return {};
    std::lock_guard<std::mutex> lock(encoded_mu_);
    auto i = encoded_.find(key);
    if (i == encoded_.end()) {
      ++encoded_misses_;
      return {};
    }
    ++encoded_hits_;
    i->second.stamp = ++stamp_;
    return i->second.bytes;
  }
  void retain(const CacheKey &key, const BinaryValue &b) const {
    if (!encoded_budget_ || b.size() > encoded_budget_)
      return;
    std::lock_guard<std::mutex> lock(encoded_mu_);
    if (encoded_.count(key))
      return;
    while (!encoded_.empty() && (encoded_.size() >= 65536 ||
                                 encoded_live_ > encoded_budget_ - b.size())) {
      auto victim = std::min_element(encoded_.begin(), encoded_.end(),
                                     [](const auto &a, const auto &b) {
                                       return a.second.stamp < b.second.stamp;
                                     });
      encoded_live_ -= victim->second.bytes.size();
      encoded_.erase(victim);
      ++encoded_evictions_;
    }
    encoded_.emplace(key, Encoded{b, ++stamp_});
    encoded_live_ += b.size();
  }

  mutable std::atomic<std::uint64_t> manifests_{0}, calls_{0}, ranges_{0},
      bytes_{0}, lcalls_{0}, lbytes_{0};
  static StructuredBlobStatus bad() {
    return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,
                                         "read-only benchmark backend");
  }

public:
  iotdbfs::test::InMemoryDatasetCatalog dataset_catalogs;
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& value) override { return dataset_catalogs.write(key, value); }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
      const StructuredBlobVersionKey& key) const override { return dataset_catalogs.read(key); }

  explicit Backend(std::shared_ptr<IoTDBStructuredBlobBackend> r, bool multi,
                   std::uint64_t encoded_budget = 0)
      : remote_(std::move(r)), multi_(multi), encoded_budget_(encoded_budget) {}
  std::map<std::string, std::uint64_t> encoded_stats() const {
    std::lock_guard<std::mutex> lock(encoded_mu_);
    return {{"encoded_cache_bytes", encoded_live_},
            {"encoded_cache_entries", encoded_.size()},
            {"encoded_cache_hits_cumulative", encoded_hits_},
            {"encoded_cache_misses_cumulative", encoded_misses_},
            {"encoded_cache_evictions_cumulative", encoded_evictions_}};
  }
  void add(const Source &s) {
    Local f;
    f.payload = std::make_shared<Fd>(s.local);
    f.manifest.key = s.key;
    files_.emplace(s.key, std::move(f));
  }
  void seal_layout(const Source &s) {
    files_.at(s.key).layout = std::make_shared<Fd>(s.layout);
  }
  Counts counts() const {
    return {manifests_.load(), calls_.load(),  ranges_.load(),
            bytes_.load(),     lcalls_.load(), lbytes_.load()};
  }
  StructuredBlobStatus
  create_manifest(const StructuredBlobManifest &m) override {
    files_.at(m.key).manifest = m;
    return StructuredBlobStatus::success();
  }
  StructuredBlobResult<StructuredBlobManifest>
  load_manifest(const StructuredBlobVersionKey &k) const override {
    ++manifests_;
    if (remote_)
      return remote_->load_manifest(k);
    auto it = files_.find(k);
    if (it == files_.end())
      return StructuredBlobResult<StructuredBlobManifest>::failure(
          StructuredBlobError::NotFound, "absent");
    return StructuredBlobResult<StructuredBlobManifest>::success(
        it->second.manifest);
  }
  StructuredBlobStatus
  replace_manifest(const StructuredBlobVersionKey &k, std::uint64_t r,
                   const StructuredBlobManifest &m) override {
    auto &f = files_.at(k);
    if (f.manifest.revision != r)
      return bad();
    f.manifest = m;
    return StructuredBlobStatus::success();
  }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey &k,
                                           std::uint64_t o,
                                           const BinaryValue &b) override {
    auto &f = files_.at(k);
    if (o != f.written)
      return bad();
    f.written += b.size();
    return StructuredBlobStatus::success();
  }
  std::map<StructuredBlobVersionKey, std::string> sidecars;
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey &k,
                                    const BinaryValue &b) override {
    save(sidecars.at(k), b);
    return StructuredBlobStatus::success();
  }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey &,
                                      const MetadataMap &) override {
    return StructuredBlobStatus::success();
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey &,
                             std::uint64_t) override {
    return bad();
  }
  StructuredBlobResult<BinaryValue>
  read_payload_range(const StructuredBlobVersionKey &k, std::uint64_t o,
                     std::uint64_t n) const override {
    CacheKey key{k, o, n};
    if (auto hit = cached(key))
      return StructuredBlobResult<BinaryValue>::success(std::move(*hit));
    ++calls_;
    ++ranges_;
    bytes_ += n;
    try {
      auto result = remote_
                        ? remote_->read_payload_range(k, o, n)
                        : StructuredBlobResult<BinaryValue>::success(
                              pread_bytes(files_.at(k).payload->value, o, n));
      if (result.ok())
        retain(key, result.value());
      return result;
    } catch (const std::exception &e) {
      return StructuredBlobResult<BinaryValue>::failure(
          StructuredBlobError::BackendFailure, e.what());
    }
  }
  bool supports_payload_ranges() const override {
    return remote_ && multi_;
  } // FS uses natural pread, never a simulated multi-range RPC.
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobVersionKey &k,
      const StructuredBlobPayloadRanges &ranges) const override {
    StructuredBlobPayloadBatch result(ranges.size());
    StructuredBlobPayloadRanges missing;
    std::vector<std::size_t> indices;
    for (std::size_t i = 0; i < ranges.size(); ++i) {
      const auto &r = ranges[i];
      if (auto hit = cached({k, r.offset, r.length}))
        result[i] = std::move(*hit);
      else {
        indices.push_back(i);
        missing.push_back(r);
      }
    }
    if (!missing.empty()) {
      ++calls_;
      ranges_ += missing.size();
      for (const auto &r : missing)
        bytes_ += r.length;
      auto fetched = remote_->read_payload_ranges(k, missing);
      if (!fetched.ok())
        return fetched;
      need(fetched.value().size() == missing.size(), "bad remote batch");
      for (std::size_t i = 0; i < missing.size(); ++i) {
        result[indices[i]] = std::move(fetched.value()[i]);
        retain({k, missing[i].offset, missing[i].length}, result[indices[i]]);
      }
    }
    return StructuredBlobResult<StructuredBlobPayloadBatch>::success(
        std::move(result));
  }
  StructuredBlobResult<BinaryValue>
  read_layout_range(const StructuredBlobVersionKey &k, std::uint64_t o,
                    std::uint64_t n) const override {
    ++lcalls_;
    lbytes_ += n;
    if (remote_)
      return remote_->read_layout_range(k, o, n);
    try {
      return StructuredBlobResult<BinaryValue>::success(
          pread_bytes(files_.at(k).layout->value, o, n));
    } catch (const std::exception &e) {
      return StructuredBlobResult<BinaryValue>::failure(
          StructuredBlobError::BackendFailure, e.what());
    }
  }
};
void initialize_remote(const std::string &path, unsigned sessions) {
  std::ifstream in(path);
  need(bool(in), "missing config");
  std::map<std::string, std::string> p;
  std::string line;
  auto trim = [](const std::string &s) {
    auto a = s.find_first_not_of(" \r\n\t");
    return a == std::string::npos
               ? std::string{}
               : s.substr(a, s.find_last_not_of(" \r\n\t") - a + 1);
  };
  while (std::getline(in, line)) {
    line = trim(line);
    if (line.empty() || line[0] == '#')
      continue;
    auto i = line.find('=');
    if (i != std::string::npos)
      p[trim(line.substr(0, i))] = trim(line.substr(i + 1));
  }
  auto setting = [&](const char *key, const char *env,
                     const char *fallback = nullptr) {
    auto found = p.find(key);
    if (found != p.end())
      return found->second;
    if (const auto *value = std::getenv(env))
      return std::string(value);
    if (fallback)
      return std::string(fallback);
    throw std::runtime_error(std::string("missing setting ") + key);
  };
  IoTDBConfig c;
  c.host = setting("iotdb.host", "IOTDB_HOST", "127.0.0.1");
  c.port = std::stoi(setting("iotdb.port", "IOTDB_PORT", "6667"));
  c.user = setting("iotdb.user", "IOTDB_USER", "root");
  c.pass = p.count("iotdb.pass") ? p.at("iotdb.pass")
                                 : setting("iotdb.password", "IOTDB_PASS");
  c.db = setting("iotdb.db", "IOTDB_DB");
  IoTDBConnector::instance().initialize(c, sessions);
}
using Values = std::variant<std::vector<float>, std::vector<double>>;
struct Observation {
  std::uint64_t steady_start_ns{0}, steady_end_ns{0};
  Values values;
  std::map<std::string, std::uint64_t> timings;
  QueryProfileRecord profile;
  std::string error;
};
BinaryValue canonical(const Values &values) {
  BinaryValue bytes;
  std::visit(
      [&](const auto &v) {
        using T = typename std::decay_t<decltype(v)>::value_type;
        bytes.reserve(v.size() * sizeof(T));
        for (T x : v) {
          std::conditional_t<sizeof(T) == 4, std::uint32_t, std::uint64_t> bits;
          std::memcpy(&bits, &x, sizeof(T));
          for (std::size_t i = 0; i < sizeof(T); ++i)
            bytes.push_back(bits >> (i * 8));
        }
      },
      values);
  return bytes;
}
class Native {
  struct Dataset {
    hid_t id{-1}, type{-1};
    std::vector<hsize_t> shape;
    std::size_t width{0};
  };
  std::map<std::string, hid_t> files_;
  std::map<std::pair<std::string, std::string>, Dataset> datasets_;
  std::map<std::string, std::string> paths_;
  std::uint64_t cache_;

public:
  Native(const std::vector<Source> &sources, const std::vector<Case> &cases,
         bool fuse, std::uint64_t budget)
      : cache_(budget) {
    need(!std::getenv("HDF5_DRIVER") && !std::getenv("HDF5_VOL_CONNECTOR"),
         "native driver/VOL override forbidden");
    need(H5PLset_loading_state(0) >= 0, "cannot disable plugins");
    for (const auto &s : sources)
      paths_[s.id] = fuse ? s.fuse : s.local;
    open(cases);
  }
  ~Native() { close(); }
  void close() {
    for (auto &[k, d] : datasets_) {
      if (d.type >= 0)
        H5Tclose(d.type);
      if (d.id >= 0)
        H5Dclose(d.id);
    }
    datasets_.clear();
    for (auto &[k, f] : files_)
      H5Fclose(f);
    files_.clear();
  }
  void open(const std::vector<Case> &cases) {
    std::set<std::pair<std::string, std::string>> requested;
    for (const auto &c : cases)
      requested.emplace(c.source, c.dataset);
    const auto per = std::max<std::uint64_t>(1, cache_ / requested.size());
    for (const auto &k : requested) {
      if (!files_.count(k.first)) {
        Fd checked_path(paths_.at(k.first));
        (void)size(checked_path.value);
        auto f =
            H5Fopen(paths_.at(k.first).c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
        need(f >= 0, "native H5Fopen failed");
        files_[k.first] = f;
      }
      hid_t dapl = H5Pcreate(H5P_DATASET_ACCESS);
      need(dapl >= 0 && H5Pset_chunk_cache(dapl, 10007, per, 0.75) >= 0,
           "native chunk cache config failed");
      Dataset d;
      d.id = H5Dopen2(files_.at(k.first), k.second.c_str(), dapl);
      H5Pclose(dapl);
      need(d.id >= 0, "native H5Dopen failed");
      d.type = H5Dget_type(d.id);
      auto space = H5Dget_space(d.id);
      const int rank = H5Sget_simple_extent_ndims(space);
      need(rank >= 1 && rank <= 3, "native unsupported rank");
      d.shape.resize(rank);
      H5Sget_simple_extent_dims(space, d.shape.data(), nullptr);
      H5Sclose(space);
      d.width = H5Tget_size(d.type);
      need(H5Tget_class(d.type) == H5T_FLOAT && (d.width == 4 || d.width == 8),
           "native benchmark requires Float32/64");
      datasets_.emplace(k, std::move(d));
    }
  }
  Observation read(const Case &c) {
    Observation o;
    o.steady_start_ns = steady_ns();
    const auto total = Clock::now();
    auto &d = datasets_.at({c.source, c.dataset});
    need(c.shape.size() == d.shape.size(), "native rank differs");
    std::vector<hsize_t> start(c.origin.begin(), c.origin.end()),
        count(c.shape.begin(), c.shape.end());
    for (std::size_t i = 0; i < count.size(); ++i)
      need(start[i] <= d.shape[i] && count[i] <= d.shape[i] - start[i],
           "native bounds");
    const auto begin = Clock::now();
    auto file_space = H5Dget_space(d.id);
    hid_t memory = -1;
    if (c.elements) {
      need(H5Sselect_hyperslab(file_space, H5S_SELECT_SET, start.data(),
                               nullptr, count.data(), nullptr) >= 0,
           "native selection failed");
      memory = H5Screate_simple(count.size(), count.data(), nullptr);
    } else {
      H5Sselect_none(file_space);
      memory = H5Screate(H5S_NULL);
    }
    o.timings["prepare"] = ns(begin);
    const auto read_start = Clock::now();
    if (d.width == 4) {
      std::vector<float> v(c.elements);
      need(!c.elements || H5Dread(d.id, H5T_NATIVE_FLOAT, memory, file_space,
                                  H5P_DEFAULT, v.data()) >= 0,
           "native read failed");
      o.values = std::move(v);
    } else {
      std::vector<double> v(c.elements);
      need(!c.elements || H5Dread(d.id, H5T_NATIVE_DOUBLE, memory, file_space,
                                  H5P_DEFAULT, v.data()) >= 0,
           "native read failed");
      o.values = std::move(v);
    }
    H5Sclose(memory);
    H5Sclose(file_space);
    o.timings["cursor_consume"] = ns(read_start);
    o.timings["execute_setup"] = 0;
    o.timings["total"] = ns(total);
    o.steady_end_ns = steady_ns();
    return o;
  }
};
QueryRequest request(const StructuredBlobManifest &m, const Case &c,
                     std::uint64_t rows) {
  QueryRequest r;
  r.sources.paths = {{PathScopeKind::Exact, m.key.path}};
  r.sources.versions = VersionSelection::exact_version(m.key.version);
  r.sources.formats = {AdapterSelectionKind::Listed, {"hdf5"}, false};
  r.subset = QueryFragment{};
  auto field = make_hdf5_value_field(c.dataset);
  need(field.ok(), field.status().message());
  r.subset->projection = {field.value()};
  for (std::size_t i = 0; i < c.shape.size(); ++i)
    r.subset->slices.push_back(
        {hdf5_axis_field(i),
         DomainBound{LogicalValue::uint64(c.origin[i]), BoundKind::Inclusive},
         DomainBound{LogicalValue::uint64(c.origin[i] + c.shape[i]),
                     BoundKind::Exclusive}});
  r.limits.target_batch_rows = rows;
  return r;
}
Observation typed(const QueryModule &module, const StructuredBlobManifest &m,
                  const Case &c, std::uint64_t rows) {
  Observation o;
  o.steady_start_ns = steady_ns();
  ScopedQueryProfile scope(&o.profile);
  const auto total = Clock::now();
  auto start = Clock::now();
  auto prepared = module.prepare(request(m, c, rows));
  o.timings["prepare"] = ns(start);
  need(prepared.ok(), prepared.status().message());
  start = Clock::now();
  auto cursor = module.execute(prepared.value());
  o.timings["execute_setup"] = ns(start);
  need(cursor.ok(), cursor.status().message());
  start = Clock::now();
  bool initialized = false;
  std::uint64_t count = 0;
  o.timings["cursor_next"] = 0;
  o.timings["owned_copy"] = 0;
  for (;;) {
    const auto next_start = Clock::now();
    auto b = cursor.value()->next();
    o.timings["cursor_next"] += ns(next_start);
    need(b.ok(), b.status().message());
    if (!b.value())
      break;
    const auto copy_start = Clock::now();
    need(b.value()->columns().size() == 1, "expected one value column");
    const auto &column = b.value()->columns()[0];
    need(column.validity.empty(), "unexpected validity");
    count += b.value()->row_count();
    need(count <= c.elements, "excess rows");
    if (column.type.id == LogicalTypeId::Float32) {
      if (!initialized) {
        o.values = std::vector<float>{};
        std::get<std::vector<float>>(o.values).reserve(c.elements);
      }
      auto &out = std::get<std::vector<float>>(o.values);
      const auto &in = std::get<std::vector<float>>(column.values);
      out.insert(out.end(), in.begin(), in.end());
    } else {
      need(column.type.id == LogicalTypeId::Float64, "unsupported output type");
      if (!initialized) {
        o.values = std::vector<double>{};
        std::get<std::vector<double>>(o.values).reserve(c.elements);
      }
      auto &out = std::get<std::vector<double>>(o.values);
      const auto &in = std::get<std::vector<double>>(column.values);
      out.insert(out.end(), in.begin(), in.end());
    }
    initialized = true;
    o.timings["owned_copy"] += ns(copy_start);
  }
  if (!initialized) {
    need(cursor.value()->schema().fields().size() == 1, "missing empty schema");
    if (cursor.value()->schema().fields()[0].type.id == LogicalTypeId::Float64)
      o.values = std::vector<double>{};
  }
  need(count == c.elements, "result shape mismatch");
  o.timings["cursor_consume"] = ns(start);
  o.timings["total"] = ns(total);
  o.steady_end_ns = steady_ns();
  return o;
}
std::map<std::string, std::uint64_t> delta(const QueryRuntimeStats &a,
                                           const QueryRuntimeStats &b) {
  std::map<std::string, std::uint64_t> m;
#define D(n) m[#n] = b.n - a.n
  D(hits);
  D(misses);
  D(waits);
  D(producer_count);
  D(decode_count);
  D(decoded_bytes);
  D(decompressions);
  D(decode_nanoseconds);
  D(decode_cpu_nanoseconds);
  D(decode_cpu_samples);
  D(read_bytes);
  D(read_ranges);
  D(copy_bytes);
  D(payload_read_calls);
  D(payload_read_nanoseconds);
  D(fragment_load_nanoseconds);
  D(evictions);
  D(redecodes);
  D(payload_read_bytes);
  D(payload_read_ranges);
  D(fragment_queue_nanoseconds);
  D(fragment_admission_nanoseconds);
  D(fragment_memory_wait_nanoseconds);
  D(decode_filter_nanoseconds);
  D(decode_inflate_nanoseconds);
  D(decode_shuffle_nanoseconds);
  D(decode_checksum_nanoseconds);
  D(decode_conversion_nanoseconds);
#undef D
#define G(n) m[#n] = b.n
  G(cache_owned_bytes);
  G(pinned_bytes);
  G(inflight_bytes);
  G(temporary_bytes);
  G(live_bytes);
  G(peak_bytes);
  G(payload_reads_peak);
#undef G
  return m;
}
} // namespace

int main(int argc, char **argv) {
  try {
    need(argc == 3, "usage: h11_query_probe MANIFEST.json OUTPUT.json");
    Json input;
    boost::property_tree::read_json(argv[1], input);
    need(input.get<std::string>("protocol") == "iotdbfs-hdf5-h11-run/1",
         "wrong manifest protocol");
    const auto path = input.get<std::string>("path");
    need(path == "A" || path == "B" || path == "C" || path == "D",
         "path must be A/B/C/D");
    const bool native = path == "A" || path == "C";
    const auto clients = get_uint(input, "clients", 1),
               passes = get_uint(input, "passes", 4),
               budget = get_uint(input, "cache_bytes", max_bytes),
               parallel = get_uint(input, "parallelism", 1),
               ranges = get_uint(input, "max_call_ranges", 32),
               rows = get_uint(input, "target_batch_rows", 65536);
    const auto policy = input.get<std::string>("cache_policy", "retain");
    need((clients == 1 || clients == 2 || clients == 4) &&
             (!native || clients == 1),
         "native clients must be independent processes");
    need(passes && passes <= 32 && budget >= 65536 &&
             budget <= 1024ULL * 1024 * 1024 &&
             (parallel == 1 || parallel == 2 || parallel == 4) &&
             (ranges == 1 || ranges == 32) && rows && rows <= 8 * 1024 * 1024,
         "invalid bounds");
    need(policy == "retain" || policy == "evict_before_request" ||
             (!native && policy == "encoded"),
         "invalid cache policy");
    const auto encoded_budget =
        policy == "encoded" ? get_uint(input, "encoded_cache_bytes", budget / 2)
                            : 0;
    need(encoded_budget <= budget - 65536,
         "encoded split leaves insufficient runtime budget");
    std::vector<Source> sources;
    std::set<std::string> source_ids;
    for (const auto &p : input.get_child("sources")) {
      need(p.first.empty(), "sources must array");
      const auto &n = p.second;
      Source s;
      s.id = n.get<std::string>("id");
      s.local = n.get<std::string>("local_path", "");
      s.fuse = n.get<std::string>("fuse_path", "");
      s.layout = n.get<std::string>("layout_path", "");
      s.sha256 = n.get<std::string>("source_sha256", "");
      s.key = {n.get<std::string>("logical_path"),
               n.get<std::int64_t>("version")};
      need(source_ids.insert(s.id).second, "duplicate source");
      sources.push_back(s);
    }
    need(!sources.empty() && sources.size() <= 64, "source cap");
    std::vector<Case> cases;
    std::set<std::string> ids;
    for (const auto &p : input.get_child("cases")) {
      need(p.first.empty(), "cases must array");
      const auto &n = p.second;
      Case c;
      c.id = n.get<std::string>("id");
      c.source = n.get<std::string>("source");
      c.dataset = n.get<std::string>("dataset");
      c.origin = dims(n, "origin");
      c.shape = dims(n, "shape");
      need(c.origin.size() == c.shape.size() && source_ids.count(c.source) &&
               ids.insert(c.id).second,
           "invalid case");
      for (std::size_t i = 0; i < c.shape.size(); ++i) {
        need(c.shape[i] <= UINT64_MAX - c.origin[i] &&
                 (!c.shape[i] || c.elements <= max_bytes / 8 / c.shape[i]),
             "selection cap");
        c.elements *= c.shape[i];
      }
      cases.push_back(c);
    }
    need(!cases.empty() && cases.size() <= 256, "case cap");
    need(!std::getenv("HDF5_DRIVER") && !std::getenv("HDF5_VOL_CONNECTOR"),
         "native driver/VOL override forbidden");
    unsigned hv_major = 0, hv_minor = 0, hv_release = 0;
    need(H5get_libversion(&hv_major, &hv_minor, &hv_release) >= 0 &&
             hv_major == 1 && hv_minor == 14 && hv_release == 4,
         "benchmark requires HDF5 1.14.4");
    const auto setup_start = Clock::now();
    std::uint64_t index_ns = 0, index_read_bytes = 0, index_read_calls = 0;
    std::shared_ptr<Backend> backend;
    std::shared_ptr<QueryRuntime> runtime;
    std::shared_ptr<const QueryModule> module;
    std::unique_ptr<Native> reader;
    std::map<std::string, StructuredBlobManifest> manifests;
    std::vector<StructuredBlobManifest> catalog;
    auto decoded_trace = std::make_shared<DecodeTrace>();
    if (native)
      reader = std::make_unique<Native>(sources, cases, path == "C", budget);
    else {
      std::shared_ptr<IoTDBStructuredBlobBackend> remote;
      if (path == "D") {
        initialize_remote(input.get<std::string>("config"),
                          std::max<std::uint64_t>(4, clients * parallel));
        remote = std::make_shared<IoTDBStructuredBlobBackend>();
      }
      backend = std::make_shared<Backend>(remote, ranges > 1, encoded_budget);
      auto storage = std::make_shared<StructuredBlobStorage>(backend);
      for (const auto &s : sources) {
        if (path == "B") {
          need(!s.layout.empty(), "B requires layout_path");
          backend->add(s);
          backend->sidecars[s.key] = s.layout;
          Fd fd(s.local);
          FormatSource raw{size(fd.value), {}};
          raw.read = [&](std::uint64_t o,
                         std::uint64_t n) -> std::optional<BinaryValue> {
            if (o > raw.size || n > raw.size - o)
              return std::nullopt;
            ++index_read_calls;
            index_read_bytes += n;
            return pread_bytes(fd.value, o, n);
          };
          FormatRegistry formats;
          need(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()),
               "format registration failed");
          IngestionModule ingestion(*storage, formats, {65536});
          auto start = Clock::now();
          auto result = ingestion.ingest(
              s.key, raw, [](const IngestionOutcome &) { return true; });
          index_ns += ns(start);
          need(result.ok(), result.message());
          backend->seal_layout(s);
        }
        auto m = backend->load_manifest(s.key);
        need(m.ok(), m.status().message());
        need(m.value().published &&
                 m.value().phase == StructuredBlobPhase::Published &&
                 m.value().format_id == std::optional<std::string>{"hdf5"} &&
                 m.value().payload_size <= max_bytes,
             "exact source is not bounded published HDF5");
        manifests[s.id] = m.value();
        catalog.push_back(m.value());
      }
      QueryRuntimeConfig rc;
      rc.memory_budget_bytes = budget - encoded_budget;
      rc.max_entries = 65536;
      rc.max_inflight = 256;
      rc.worker_threads = 4;
      rc.wait_timeout_ms = 30000;
      rc.enable_detailed_profile = true;
      runtime = std::make_shared<QueryRuntime>(rc);
      Hdf5QueryKernelConfig hc;
      hc.read_parallelism = parallel;
      hc.max_batch_fragments = 32;
      hc.decoded_fragment_observer = [decoded_trace](const std::string &key,
                                                     std::uint64_t bytes) {
        std::lock_guard<std::mutex> lock(decoded_trace->mutex);
        ++decoded_trace->count;
        if (decoded_trace->keys.size() < 65536) {
          if (decoded_trace->keys.insert(key).second)
            decoded_trace->bytes += bytes;
        } else if (!decoded_trace->keys.count(key))
          decoded_trace->complete = false;
      };
      auto created = QueryModule::create(
          make_in_memory_published_version_catalog(catalog), storage,
          {std::make_shared<Hdf5FormatQueryAdapter>(hc, runtime)});
      need(created.ok(), created.status().message());
      module = created.value();
    }
    const auto setup_ns = ns(setup_start);
    std::ofstream out(argv[2]);
    need(bool(out), "cannot create output");
    out << "{\"protocol\":\"iotdbfs-hdf5-h11-probe/1\",\"config\":{\"path\":"
        << q(path) << ",\"clients\":" << clients
        << ",\"cache_bytes\":" << budget
        << ",\"encoded_cache_budget_bytes\":" << encoded_budget
        << ",\"decoded_cache_budget_bytes\":" << (budget - encoded_budget)
        << ",\"cache_policy\":" << q(policy) << ",\"parallelism\":" << parallel
        << ",\"max_call_ranges\":" << ranges
        << ",\"target_batch_rows\":" << rows << ",\"session_pool_size\":"
        << (path == "D" ? std::max<std::uint64_t>(4, clients * parallel) : 0)
        << ",\"max_batch_fragments\":32"
        << ",\"native_metadata_cache_in_budget\":false,\"cache_scope\":"
           "\"decoded-payload; native metadata caches "
           "additional\",\"backend_multi_range\":"
        << (!native && backend->supports_payload_ranges() ? "true" : "false")
        << "},\"setup\":{\"wall_ns\":" << setup_ns
        << ",\"index_build_ns\":" << index_ns
        << ",\"index_read_calls\":" << index_read_calls
        << ",\"index_read_bytes\":" << index_read_bytes << ",\"sources\":[";
    for (std::size_t i = 0; i < sources.size(); ++i) {
      if (i)
        out << ',';
      const auto &s = sources[i];
      out << "{\"id\":" << q(s.id) << ",\"logical_path\":" << q(s.key.path)
          << ",\"version\":" << s.key.version
          << ",\"declared_source_sha256\":" << q(s.sha256);
      if (manifests.count(s.id)) {
        const auto &m = manifests.at(s.id);
        out << ",\"revision\":" << m.revision
            << ",\"payload_bytes\":" << m.payload_size
            << ",\"layout_bytes\":" << m.layout_size;
      }
      out << '}';
    }
    out << "]},\"requests\":[";
    bool comma = false;
    std::vector<std::string> pass_records;
    for (std::uint64_t pass = 0; pass < passes; ++pass) {
      const auto barrier = input.get<std::string>("barrier_dir", "");
      const auto pass_start = steady_ns();
      const auto before = runtime ? runtime->stats() : QueryRuntimeStats{};
      const auto counts_before = backend ? backend->counts() : Counts{};
      std::uint64_t wave_ns = 0, dispatch_ns = 0, reset_ns = 0;
      std::size_t case_index = 0;
      for (const auto &c : cases) {
        if (policy == "evict_before_request" || policy == "encoded") {
          auto t = Clock::now();
          if (reader) {
            reader->close();
            reader->open(cases);
          } else
            runtime->cache().evict_all();
          reset_ns += ns(t);
        }

        if (!barrier.empty()) {
          need(native, "barrier only for native processes");
          const auto participant = get_uint(input, "participant", 0);
          const auto participants = get_uint(input, "participants", 1);
          need(participant < participants && participants <= 4,
               "invalid barrier participant");
          const auto prefix = barrier + "/pass-" + std::to_string(pass) +
                              "-case-" + std::to_string(case_index);
          {
            std::ofstream marker(prefix + "-ready-" +
                                 std::to_string(participant));
            need(bool(marker), "cannot create barrier marker");
          }
          const auto deadline = Clock::now() + std::chrono::seconds(120);
          while (!std::filesystem::exists(prefix + "-go")) {
            need(Clock::now() < deadline, "barrier timeout");
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
          }
          // The runner may publish a common future CLOCK_MONOTONIC timestamp.
          // This avoids 1-ms poll skew dominating microsecond native reads.
          std::ifstream go_file(prefix + "-go");
          std::uint64_t target_ns = 0;
          go_file >> target_ns;
          need(!target_ns || target_ns <= steady_ns() + 1000000000ULL,
               "barrier target exceeds one second");
          while (target_ns) {
            const auto now = steady_ns();
            if (now >= target_ns)
              break;
            if (target_ns - now > 200000ULL)
              std::this_thread::sleep_for(std::chrono::microseconds(50));
            else
              std::this_thread::yield();
          }
        }
        std::vector<Observation> observations(clients);
        std::mutex mu;
        std::condition_variable cv;
        std::size_t ready = 0;
        bool go = false;
        std::vector<std::thread> threads;
        if (native) {
          const auto t = Clock::now();
          observations[0] = reader->read(c);
          dispatch_ns += ns(t);
        } else if (clients == 1) {
          const auto t = Clock::now();
          observations[0] = typed(*module, manifests.at(c.source), c, rows);
          dispatch_ns += ns(t);
        } else {
          for (std::size_t client = 0; client < clients; ++client)
            threads.emplace_back([&, client] {
              {
                std::unique_lock<std::mutex> lock(mu);
                ++ready;
                cv.notify_all();
                cv.wait(lock, [&] { return go; });
              }
              try {
                observations[client] =
                    typed(*module, manifests.at(c.source), c, rows);
              } catch (const std::exception &e) {
                observations[client].error = e.what();
              }
            });
          {
            std::unique_lock<std::mutex> lock(mu);
            cv.wait(lock, [&] { return ready == clients; });
          }
          auto t = Clock::now();
          {
            std::lock_guard<std::mutex> lock(mu);
            go = true;
          }
          cv.notify_all();
          for (auto &thread : threads)
            thread.join();
          dispatch_ns += ns(t);
        }
        std::uint64_t first_start = UINT64_MAX, last_end = 0;
        for (const auto &observation : observations) {
          need(observation.error.empty(), observation.error);
          first_start = std::min(first_start, observation.steady_start_ns);
          last_end = std::max(last_end, observation.steady_end_ns);
        }
        need(last_end >= first_start, "invalid owned-result interval");
        wave_ns += last_end - first_start;
        for (std::size_t client = 0; client < clients; ++client) {
          const auto &o = observations[client];
          need(o.error.empty(), o.error);
          const auto bytes = canonical(o.values);
          if (comma)
            out << ',';
          comma = true;
          out << "{\"pass\":" << pass << ",\"client\":" << client
              << ",\"steady_start_ns\":" << o.steady_start_ns
              << ",\"steady_end_ns\":" << o.steady_end_ns
              << ",\"case_id\":" << q(c.id) << ",\"source_id\":" << q(c.source)
              << ",\"dataset\":" << q(c.dataset) << ",\"origin\":";
          numbers(out, c.origin);
          out << ",\"shape\":";
          numbers(out, c.shape);
          out << ",\"dtype\":"
              << q(std::holds_alternative<std::vector<float>>(o.values)
                       ? "float32"
                       : "float64")
              << ",\"values_sha256\":" << q(migration_sha256(bytes))
              << ",\"value_bytes\":" << bytes.size() << ",\"timings_ns\":";
          object(out, o.timings);
          out << ",\"stages_ns\":";
          object(out, o.profile.nanoseconds);
          out << ",\"counters\":";
          object(out, o.profile.counters);
          out << '}';
        }
        ++case_index;
      }
      const auto after = runtime ? runtime->stats() : QueryRuntimeStats{};
      auto metrics = delta(before, after);
      const auto counts_after = backend ? backend->counts() : Counts{};
#define C(k, f) metrics[k] = counts_after.f - counts_before.f
      C("backend_manifest_calls", manifests);
      C("backend_payload_calls", payload_calls);
      C("backend_payload_ranges", payload_ranges);
      C("backend_payload_bytes", payload_bytes);
      C("backend_layout_calls", layout_calls);
      C("backend_layout_bytes", layout_bytes);
#undef C
      if (runtime && !encoded_budget)
        need(metrics["backend_payload_bytes"] ==
                     metrics["payload_read_bytes"] &&
                 metrics["backend_payload_ranges"] ==
                     metrics["payload_read_ranges"],
             "transport/runtime counters differ");
      if (backend) {
        for (const auto &p : backend->encoded_stats())
          metrics[p.first] = p.second;
      }
      {
        std::lock_guard<std::mutex> lock(decoded_trace->mutex);
        metrics["unique_decoded_fragments_cumulative"] =
            decoded_trace->keys.size();
        metrics["unique_decoded_bytes_cumulative"] = decoded_trace->bytes;
        metrics["payload_decode_count_cumulative"] = decoded_trace->count;
        metrics["unique_decode_trace_complete"] = decoded_trace->complete;
      }
      struct rusage usage {};
      if (getrusage(RUSAGE_SELF, &usage) == 0) {
#ifdef __APPLE__
        metrics["process_peak_rss_bytes"] = usage.ru_maxrss;
#else
        metrics["process_peak_rss_bytes"] =
            static_cast<std::uint64_t>(usage.ru_maxrss) * 1024;
#endif
      }
      std::ostringstream record;
      record << "{\"pass\":" << pass << ",\"steady_start_ns\":" << pass_start
             << ",\"steady_end_ns\":" << steady_ns()
             << ",\"wall_ns\":" << wave_ns
             << ",\"dispatch_join_ns\":" << dispatch_ns
             << ",\"cache_reset_ns\":" << reset_ns
             << ",\"runtime_measured\":" << (runtime ? "true" : "false")
             << ",\"runtime_delta\":";
      object(record, metrics);
      record << '}';
      pass_records.push_back(record.str());
    }
    out << "],\"passes\":[";
    for (std::size_t i = 0; i < pass_records.size(); ++i) {
      if (i)
        out << ',';
      out << pass_records[i];
    }
    out << "],\"ok\":true}\n";
    out.close();
    need(bool(out), "output write failed");
    return 0;
  } catch (const std::exception &e) {
    std::cerr << "h11: " << e.what() << '\n';
    return 1;
  }
}
