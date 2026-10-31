// Auxiliary mechanism experiment, not an FM1/IoTDB storage benchmark.
// Production ingestion/adapters/runtime/socket/scheduler/bridge over immutable
// in-memory metadata/Arrow bytes and a managed immutable local HDF5 file.
// One client thread multiplexes eight connections.
#include "../../support/InMemoryDatasetCatalog.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"
#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"
#include "iotdbfs/query/frontend/QueryRegistrationSocket.h"
#include "iotdbfs/storage/NativeFileStore.h"
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <boost/property_tree/json_parser.hpp>
#include <hdf5.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <poll.h>
#include <unistd.h>
#include <atomic>
#include <cerrno>
#include <cmath>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace {
using namespace iotdbfs;
using Clock = std::chrono::steady_clock;
using Tree = boost::property_tree::ptree;
std::filesystem::path owned_output;
std::uint64_t ns() { return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count(); }
void need(bool yes, const std::string& why) { if (!yes) throw std::runtime_error(why); }
std::string quote(const std::string& in) {
  std::string out = "\""; const char hex[] = "0123456789abcdef";
  for (unsigned char c : in) {
    if (c == '"' || c == '\\') { out += '\\'; out += c; }
    else if (c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
    else out += c;
  }
  return out + '"';
}
template<class T> T checked(arrow::Result<T> r) { need(r.ok(), r.status().ToString()); return std::move(r).ValueOrDie(); }
void checked(arrow::Status s) { need(s.ok(), s.ToString()); }
struct HdfId {
  hid_t id; herr_t (*close)(hid_t);
  HdfId(hid_t i, herr_t (*c)(hid_t)) : id(i), close(c) { need(id >= 0, "HDF oracle open failed"); }
  ~HdfId() { close(id); }
  HdfId(const HdfId&) = delete;
  operator hid_t() const { return id; }
};
class Backend final : public StructuredBlobStorageBackend, public std::enable_shared_from_this<Backend> {
public:
  struct Object { StructuredBlobManifest manifest; BinaryValue payload, layout; };
  std::map<StructuredBlobVersionKey, Object> objects;
  test::InMemoryDatasetCatalog catalogs;
  std::shared_ptr<NativeFileStore> native_store;
  std::optional<NativeFileLocation> native_location;
  StructuredBlobStatus create_manifest(const StructuredBlobManifest& m) override {
    need(objects.emplace(m.key, Object{m, {}, {}}).second, "duplicate fixture manifest"); return {};
  }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& key) const override {
    auto f = objects.find(key);
    if (f == objects.end()) return StructuredBlobResult<StructuredBlobManifest>::failure(StructuredBlobError::NotFound, "missing fixture");
    return StructuredBlobResult<StructuredBlobManifest>::success(f->second.manifest);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey& key, std::uint64_t revision,
      const StructuredBlobManifest& m) override {
    auto& o = objects.at(key); need(o.manifest.revision == revision, "fixture CAS mismatch"); o.manifest = m; return {};
  }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey& key, std::uint64_t offset, const BinaryValue& b) override {
    auto& p = objects.at(key).payload; need(offset == p.size(), "fixture nonsequential write"); p.insert(p.end(), b.begin(), b.end()); return {};
  }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey& key, const BinaryValue& b) override { objects.at(key).layout = b; return {}; }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&, const MetadataMap&) override { return {}; }
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key, const DatasetCatalogArtifact& c) override { return catalogs.write(key, c); }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(const StructuredBlobVersionKey& key) const override { return catalogs.read(key); }
  StructuredBlobResult<BinaryValue> read_payload_range(const StructuredBlobVersionKey& key, std::uint64_t o, std::uint64_t n) const override {
    return range(objects.at(key).payload, o, n);
  }
  StructuredBlobResult<BinaryValue> read_layout_range(const StructuredBlobVersionKey& key, std::uint64_t o, std::uint64_t n) const override {
    return range(objects.at(key).layout, o, n);
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey& key, std::uint64_t) override { objects.erase(key); catalogs.erase(key); return {}; }
  StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
  bind_published_payload(const StructuredBlobManifest& manifest) const override {
    using Result = StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>;
    if (!native_location || !(manifest.key == native_location->key))
      return StructuredBlobStorageBackend::bind_published_payload(manifest);
    const auto& object = objects.at(manifest.key);
    if (!native_store || !manifest.published || manifest.phase != StructuredBlobPhase::Published ||
        manifest.format_id != "hdf5" || manifest.revision != object.manifest.revision ||
        manifest.payload_size != native_location->size || manifest.layout_size != object.layout.size())
      return Result::failure(StructuredBlobError::InvalidState, "native fixture manifest binding mismatch");
    class Reader final : public StructuredBlobPayloadReader {
    public:
      explicit Reader(std::shared_ptr<const Backend> backend) : backend_(std::move(backend)) {}
      StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
          const StructuredBlobPayloadRanges& ranges) const override {
        StructuredBlobPayloadBatch result; result.reserve(ranges.size());
        for (const auto& range : ranges) {
          auto bytes = backend_->read_payload_range(backend_->native_location->key, range.offset, range.length);
          if (!bytes.ok()) return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(bytes.status().code(), bytes.status().message());
          result.push_back(std::move(bytes.value()));
        }
        return StructuredBlobResult<StructuredBlobPayloadBatch>::success(std::move(result));
      }
      StructuredBlobResult<StructuredBlobBytes> read_layout_range(std::uint64_t offset, std::uint64_t length) const override {
        return backend_->read_layout_range(backend_->native_location->key, offset, length);
      }
      StructuredBlobResult<std::optional<std::string>> verified_layout_identity() const override {
        return StructuredBlobResult<std::optional<std::string>>::success(std::nullopt);
      }
      StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
        return backend_->native_store->open(*backend_->native_location);
      }
      StructuredBlobResult<std::optional<std::string>> native_file_cache_identity() const override {
        auto identity = backend_->native_store->cache_identity(*backend_->native_location);
        if (!identity.ok()) return StructuredBlobResult<std::optional<std::string>>::failure(identity.status().code(), identity.status().message());
        return StructuredBlobResult<std::optional<std::string>>::success(std::move(identity.value()));
      }
      StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> reuse_native_file(
          const std::shared_ptr<const NativeFileDescriptor>& descriptor) const override {
        return backend_->native_store->reuse(*backend_->native_location, descriptor);
      }
    private:
      std::shared_ptr<const Backend> backend_;
    };
    return Result::success(std::make_shared<Reader>(shared_from_this()));
  }
private:
  static StructuredBlobResult<BinaryValue> range(const BinaryValue& b, std::uint64_t o, std::uint64_t n) {
    if (o > b.size() || n > b.size() - o) return StructuredBlobResult<BinaryValue>::failure(StructuredBlobError::RangeError, "fixture range");
    return StructuredBlobResult<BinaryValue>::success({b.begin() + o, b.begin() + o + n});
  }
};
FormatSource source_for(const BinaryValue& bytes) {
  return {bytes.size(), [&bytes](std::uint64_t o, std::uint64_t n) -> std::optional<BinaryValue> {
    if (o > bytes.size() || n > bytes.size() - o) return std::nullopt;
    return BinaryValue(bytes.begin() + o, bytes.begin() + o + n);
  }};
}
BinaryValue arrow_fixture(std::size_t length) {
  auto values = std::make_shared<arrow::FloatBuilder>();
  auto channels = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), values);
  arrow::ListBuilder rows(arrow::default_memory_pool(), channels);
  checked(rows.Append()); checked(channels->Append());
  for (std::size_t i = 0; i < length; ++i) checked(values->Append(static_cast<float>(i % 4096) * 0.25f));
  std::shared_ptr<arrow::Array> array; checked(rows.Finish(&array));
  auto batch = arrow::RecordBatch::Make(arrow::schema({arrow::field("target", array->type())}), 1, {array});
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  checked(writer->WriteRecordBatch(*batch)); checked(writer->Close());
  auto bytes = checked(sink->Finish()); return {bytes->data(), bytes->data() + bytes->size()};
}
std::string request(const std::string& path, const std::string& format, const std::string& subset) {
  return "{\"kind\":\"typed\",\"request\":{\"sources\":{\"paths\":[{\"kind\":\"exact\",\"path\":" + quote(path) +
    "}],\"versions\":{\"kind\":\"exact\",\"exact\":7},\"formats\":{\"adapters\":\"listed\",\"adapter_ids\":[" + quote(format) +
    "],\"include_byte_passthrough\":false}},\"subset\":" + subset + "}}";
}
void transfer(int fd, void* data, std::size_t length, bool writing) {
  auto* p = static_cast<char*>(data);
  while (length) {
#ifdef MSG_NOSIGNAL
    auto n = writing ? ::send(fd, p, length, MSG_NOSIGNAL) : ::recv(fd, p, length, 0);
#else
    auto n = writing ? ::send(fd, p, length, 0) : ::recv(fd, p, length, 0);
#endif
    if (n < 0 && errno == EINTR) continue;
    need(n > 0, "socket transfer failed: " + std::string(std::strerror(errno)));
    p += n; length -= n;
  }
}
struct Connection {
  int fd{-1}; std::string format, response, route;
  std::uint64_t submit_ns{}, receive_ns{}, owned_ns{}, byte_count{}, rows{}, columns{}, oracle_ns{};
  std::vector<double> values;
  std::vector<std::uint8_t> masks, ordinal_masks;
  std::vector<std::uint64_t> ordinals;
  ~Connection() { if (fd >= 0) ::close(fd); }
  Connection() = default; Connection(const Connection&) = delete;
};
void send_request(Connection& c, const std::string& socket, const std::string& json) {
  c.fd = ::socket(AF_UNIX, SOCK_STREAM, 0); need(c.fd >= 0, "socket create");
  timeval timeout{60, 0};
  need(::setsockopt(c.fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0, "socket timeout");
  sockaddr_un addr{}; addr.sun_family = AF_UNIX;
  need(socket.size() < sizeof(addr.sun_path), "socket path too long");
  std::memcpy(addr.sun_path, socket.c_str(), socket.size() + 1);
  need(::connect(c.fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0, "socket connect");
  auto length = htonl(static_cast<std::uint32_t>(json.size()));
  c.submit_ns = ns(); transfer(c.fd, &length, 4, true);
  transfer(c.fd, const_cast<char*>(json.data()), json.size(), true);
}
void receive(Connection& c) {
  std::uint32_t length; transfer(c.fd, &length, 4, false); length = ntohl(length);
  need(length > 0 && length <= 1024 * 1024, "response length"); c.response.resize(length);
  transfer(c.fd, c.response.data(), length, false); c.receive_ns = ns();
  ::close(c.fd); c.fd = -1;
}
struct Oracle {
  std::vector<double> values; std::string hdf_field;
  Oracle(const std::string& file, const std::string& dataset) {
    HdfId f(H5Fopen(file.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    HdfId d(H5Dopen2(f, dataset.c_str(), H5P_DEFAULT), H5Dclose);
    HdfId space(H5Dget_space(d), H5Sclose);
    const auto points = H5Sget_simple_extent_npoints(space);
    need(points > 0 && points <= 1000000, "oracle dataset must have 1..1000000 values");
    values.resize(points);
    need(H5Dread(d, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, values.data()) >= 0, "oracle native read");
    auto field = make_hdf5_value_field(dataset); need(field.ok(), field.status().message()); hdf_field = field.value().field_id;
  }
};
std::string choose_dataset(hid_t group, const std::string& prefix, unsigned depth = 0) {
  need(depth < 16, "dataset discovery exceeded group-depth limit");
  H5G_info_t info{}; need(H5Gget_info(group, &info) >= 0, "group inventory failed");
  for (hsize_t i = 0; i < info.nlinks; ++i) {
    auto length = H5Lget_name_by_idx(group, ".", H5_INDEX_NAME, H5_ITER_INC, i, nullptr, 0, H5P_DEFAULT);
    need(length >= 0 && length < 65536, "dataset name size");
    std::string name(static_cast<std::size_t>(length) + 1, '\0');
    need(H5Lget_name_by_idx(group, ".", H5_INDEX_NAME, H5_ITER_INC, i, name.data(), name.size(), H5P_DEFAULT) == length, "dataset name read");
    name.resize(length);
    // Do not follow soft/external links while choosing an oracle dataset.
    H5L_info2_t link{};
    need(H5Lget_info2(group, name.c_str(), &link, H5P_DEFAULT) >= 0, "link inventory failed");
    if (link.type != H5L_TYPE_HARD) continue;
    HdfId object(H5Oopen(group, name.c_str(), H5P_DEFAULT), H5Oclose);
    const auto path = prefix + "/" + name;
    if (H5Iget_type(object) == H5I_DATASET) {
      HdfId space(H5Dget_space(object), H5Sclose), type(H5Dget_type(object), H5Tclose);
      if (H5Sget_simple_extent_ndims(space) != 2) continue;
      hsize_t dims[2]{}; need(H5Sget_simple_extent_dims(space, dims, nullptr) == 2, "dataset shape");
      const auto kind = H5Tget_class(type); const auto width = H5Tget_size(type);
      if (dims[0] == 64 && dims[1] == 64 &&
          ((kind == H5T_FLOAT && (width == 4 || width == 8)) ||
           (kind == H5T_INTEGER && width == 2 && H5Tget_sign(type) == H5T_SGN_2))) return path;
    } else if (H5Iget_type(object) == H5I_GROUP) {
      auto nested = choose_dataset(object, path, depth + 1);
      if (!nested.empty()) return nested;
    }
  }
  return {};
}
double scalar(const std::shared_ptr<arrow::Array>& a, std::int64_t i) {
  switch (a->type_id()) {
    case arrow::Type::FLOAT: return std::static_pointer_cast<arrow::FloatArray>(a)->Value(i);
    case arrow::Type::DOUBLE: return std::static_pointer_cast<arrow::DoubleArray>(a)->Value(i);
    case arrow::Type::INT16: return std::static_pointer_cast<arrow::Int16Array>(a)->Value(i);
    default: throw std::runtime_error("unexpected oracle value type: " + a->type()->ToString());
  }
}
std::string own_result(Connection& c, const std::shared_ptr<FuseTypedQueryBridge>& bridge,
    const std::string& hdf_field) {
  Tree response; std::istringstream in(c.response); boost::property_tree::read_json(in, response);
  need(response.get<bool>("ok", false), "query returned error: " + c.response);
  need(response.get<std::string>("kind") == "published_route", "unexpected response kind");
  const auto path = response.get<std::string>("path"); const auto result = path.rfind("&result=");
  const std::string root = "/probe";
  need(path.rfind(root, 0) == 0 && result != std::string::npos, "invalid immutable route");
  auto handle = bridge->acquire_exact(path.substr(root.size(), result - root.size()), path.substr(result + 8), {});
  need(handle.ok(), handle.status().message());
  auto bytes = handle.value()->read(0, handle.value()->size()); need(bytes.ok(), bytes.status().message());
  c.byte_count = bytes.value().size();
  auto buffer = arrow::Buffer::FromVector(std::move(bytes.value()));
  auto reader = checked(arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buffer)));
  auto table = checked(reader->ToTable());
  const bool hdf = c.format == "hdf5";
  auto values = table->GetColumnByName(hdf ? hdf_field : "value.0"); need(bool(values), "missing values");
  c.values.reserve(table->num_rows()); c.masks.reserve(table->num_rows());
  for (const auto& chunk : values->chunks()) for (std::int64_t i = 0; i < chunk->length(); ++i) {
    c.masks.push_back(!chunk->IsNull(i));
    c.values.push_back(chunk->IsNull(i) ? 0.0 : scalar(chunk, i));
  }
  if (!hdf) {
    auto indices = table->GetColumnByName("index"); need(bool(indices), "missing Arrow ordinal");
    c.ordinals.reserve(table->num_rows()); c.ordinal_masks.reserve(table->num_rows());
    for (const auto& chunk : indices->chunks()) {
      need(chunk->type_id() == arrow::Type::UINT64, "ordinal type mismatch");
      auto a = std::static_pointer_cast<arrow::UInt64Array>(chunk);
      for (std::int64_t i = 0; i < a->length(); ++i) {
        c.ordinal_masks.push_back(!a->IsNull(i)); c.ordinals.push_back(a->IsNull(i) ? 0 : a->Value(i));
      }
    }
  }
  c.rows = table->num_rows(); c.columns = table->num_columns(); c.owned_ns = ns();
  return path.substr(root.size(), result - root.size());
}
void verify_owned(Connection& c, const Oracle& oracle, std::size_t arrow_values) {
  const auto begin = ns(); const bool hdf = c.format == "hdf5";
  const auto expected_rows = hdf ? oracle.values.size() : arrow_values;
  need(c.columns == (hdf ? 1u : 2u), "result column count mismatch");
  need(c.rows == expected_rows && c.values.size() == expected_rows && c.masks.size() == expected_rows, "result row count mismatch");
  for (std::size_t row = 0; row < expected_rows; ++row) {
    need(c.masks[row] == 1, "unexpected null mask");
    const double expected = hdf ? oracle.values[row] : static_cast<float>(row % 4096) * 0.25f;
    const double actual = c.values[row];
    need(actual == expected || (std::isnan(actual) && std::isnan(expected)), "value oracle mismatch at " + std::to_string(row));
  }
  if (!hdf) {
    need(c.ordinals.size() == expected_rows && c.ordinal_masks.size() == expected_rows, "ordinal count mismatch");
    for (std::size_t row = 0; row < expected_rows; ++row)
      need(c.ordinal_masks[row] == 1 && c.ordinals[row] == row, "ordinal oracle mismatch");
  }
  c.oracle_ns = ns() - begin;
}
template<class T> std::uint64_t owned_hash(const std::vector<T>& values) {
  // FNV-1a over explicitly little-endian scalar bits, after the timed region.
  std::uint64_t result = 14695981039346656037ULL;
  for (const auto& value : values) {
    std::uint64_t bits = 0; std::memcpy(&bits, &value, sizeof(value));
    for (std::size_t i = 0; i < sizeof(value); ++i) { result ^= (bits >> (i * 8)) & 255; result *= 1099511628211ULL; }
  }
  return result;
}
void stats_json(std::ostream& out, const QueryTaskSchedulerStats& s) {
  out << "{\"submitted\":" << s.submitted << ",\"completed\":" << s.completed << ",\"failed\":" << s.failed
    << ",\"queued\":" << s.queued << ",\"active\":" << s.active << ",\"peak_active\":" << s.peak_active
    << ",\"dispatch_batches\":" << s.dispatch_batches << ",\"dispatched_tasks\":" << s.dispatched_tasks
    << ",\"max_dispatch_batch\":" << s.max_dispatch_batch << ",\"resource_waits\":" << s.resource_waits
    << ",\"resource_bypasses\":" << s.resource_bypasses << ",\"resource_blocked\":" << s.resource_blocked
    << ",\"parked\":" << s.parked << '}';
}
int run(int argc, char** argv) {
  need(argc == 10, "usage: query_task_mixed_probe HDF5_FILE DATASET OUTPUT_DIR WORKERS SCENARIO WARM MEASURED CAMPAIGN ARROW_VALUES\nSCENARIO=hdf5|arrow|mixed|mixed-held");
  const std::string hdf_file = argv[1], scenario = argv[5];
  std::string dataset = argv[2];
  const std::filesystem::path out = argv[3];
  const auto workers = std::stoul(argv[4]), warm = std::stoul(argv[6]), measured = std::stoul(argv[7]);
  const auto arrow_values = std::stoul(argv[9]); const std::string campaign = argv[8];
  need((workers == 1 || workers == 2 || workers == 4) && measured > 0 && warm <= 100 && measured <= 1000, "invalid worker/repetition count");
  need(arrow_values > 0 && arrow_values <= 1000000, "Arrow value count out of range");
  need(scenario == "hdf5" || scenario == "arrow" || scenario == "mixed" || scenario == "mixed-held", "unknown scenario");
  need(!std::filesystem::exists(out), "output directory already exists; preserve prior evidence");
  need(std::filesystem::create_directories(out), "output directory creation failed");
  owned_output = out;
  std::ifstream file(hdf_file, std::ios::binary); need(bool(file), "cannot read source");
  BinaryValue hdf_bytes((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  need(!hdf_bytes.empty() && hdf_bytes.size() <= 64ULL * 1024 * 1024, "HDF5 source must be 1..64 MiB");
  if (dataset == "auto") {
    HdfId f(H5Fopen(hdf_file.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT), H5Fclose);
    dataset = choose_dataset(f, "");
    need(!dataset.empty(), "no supported numeric 64x64 dataset found");
  }
  const Oracle oracle(hdf_file, dataset); const auto arrow_bytes = arrow_fixture(arrow_values);
  auto backend = std::make_shared<Backend>(); auto storage = std::make_shared<StructuredBlobStorage>(backend);
  FormatRegistry formats; need(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()), "HDF ingestion registration");
  need(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()), "Arrow ingestion registration");
  IngestionModule ingestion(*storage, formats, {65536});
  const StructuredBlobVersionKey hdf_key{"/fixture/source.h5", 7}, arrow_key{"/fixture/source.arrow", 7};
  const auto hdf_ingestion = ingestion.ingest(hdf_key, source_for(hdf_bytes), [](const IngestionOutcome&) { return true; });
  need(hdf_ingestion.ok(), hdf_ingestion.message());
  const auto arrow_ingestion = ingestion.ingest(arrow_key, source_for(arrow_bytes), [](const IngestionOutcome&) { return true; });
  need(arrow_ingestion.ok(), arrow_ingestion.message());
  // The native engine deliberately refuses unbound payload range readers.
  // Install a private immutable copy before any query timing, using the real
  // store's hash/fsync/atomic publish and descriptor validation. The original
  // sealed input remains untouched; fixture IDs are not corpus seal claims.
  const auto native_root = std::filesystem::canonical(out) / "private-native-root";
  backend->native_store = std::make_shared<NativeFileStore>(native_root.string());
  NativeFileLocation native_identity;
  native_identity.namespace_identity = {"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"};
  native_identity.key = hdf_key; native_identity.descriptor_id = std::string(64, 'a');
  native_identity.seal_id = std::string(64, 'b'); native_identity.payload_root = std::string(64, 'c');
  native_identity.size = hdf_bytes.size();
  auto installed = backend->native_store->install(native_identity, [&](std::uint64_t offset, std::uint64_t length) {
    return backend->read_payload_range(hdf_key, offset, length);
  });
  need(installed.ok(), installed.status().message()); backend->native_location = installed.value();
  auto native_check = backend->native_store->open(*backend->native_location);
  need(native_check.ok(), native_check.status().message());
  auto native_checked_bytes = native_check.value()->read(0, hdf_bytes.size());
  need(native_checked_bytes.ok() && native_checked_bytes.value() == hdf_bytes, "managed native fixture differs from sealed input bytes");
  std::vector<StructuredBlobManifest> manifests; for (const auto& entry : backend->objects) manifests.push_back(entry.second.manifest);
  auto catalog = make_in_memory_published_version_catalog(manifests);
  QueryRuntimeConfig config; config.query_tasks = {workers, 64, workers}; config.enable_detailed_profile = true;
  auto runtime = std::make_shared<QueryRuntime>(config);
  Hdf5QueryKernelConfig hdf_execution; hdf_execution.native_session_cache_enabled = true;
  auto module = QueryModule::create(catalog, storage, {
    std::make_shared<ArrowFormatQueryAdapter>(ArrowDecodeConfig{}, runtime),
    std::make_shared<Hdf5FormatQueryAdapter>(hdf_execution, runtime, Hdf5SemanticConfig{}, Hdf5LayoutCacheConfig{},
      std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{}, std::shared_ptr<Hdf5ValidatedLayoutCache>{}, Hdf5QueryMode::NativeHdf5)});
  need(module.ok(), module.status().message());
  auto frontend = std::make_shared<TypedQueryFrontend>();
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto service = std::make_shared<TypedQueryService>(module.value(), frontend, results, 256ULL * 1024 * 1024);
  auto bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
  char pattern[] = "/tmp/iotdbfs-mixed-XXXXXX"; auto created = ::mkdtemp(pattern); need(created, "mkdtemp");
  const std::string socket_dir = created, socket = socket_dir + "/c.sock";
  struct SocketCleanup { std::string dir, path; ~SocketCleanup() { ::unlink(path.c_str()); ::unlink((path + ".lock").c_str()); ::rmdir(dir.c_str()); } } cleanup{socket_dir, socket};
  const std::string profile_path = (out / "profile.jsonl").string(); need(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", profile_path.c_str(), 1) == 0, "set profile path");
  QueryRegistrationSocketServer server(socket, "/probe", frontend, catalog, storage, runtime, bridge,
    [](const auto&, const auto&, const auto*) { return QueryStatus::success(); }, 0600,
    TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{}, hdf_execution,
    QueryRegistrationSocketServer::DatasetDiscovery{}, std::shared_ptr<Hdf5ValidatedLayoutCache>{},
    std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{}, QueryRegistrationSocketServer::BeginAuthorizationPublication{},
    false, Hdf5QueryMode::NativeHdf5, module.value());
  const auto started = server.start(); need(started.ok(), started.message());
  const std::string hdf_request = request(hdf_key.path, "hdf5", "{\"projection\":[" + quote(oracle.hdf_field) + "]}");
  const std::string arrow_request = request(arrow_key.path, "arrow", R"({"projection":["index","value.0"],"physical_array":{"field_id":"target","record_batch_index":0,"row_index":0,"values":[{"child_indices":[0]}]}})");
  std::ofstream raw(out / "requests.jsonl"), waves(out / "waves.jsonl"), stats(out / "scheduler.jsonl");
  raw << std::setprecision(17); waves << std::setprecision(17);
  std::ofstream manifest(out / "manifest.json");
  manifest << "{\"schema\":\"query-task-mixed-probe/1\",\"scope\":\"auxiliary-no-iotdb-native-hdf5-memory-metadata-arrow\",\"source_count_per_query\":1,\"client_threads\":1,\"outstanding_per_wave\":8,\"workers\":" << workers
    << ",\"max_queued_tasks\":64,\"max_batch_size\":" << workers << ",\"fragment_workers\":" << config.worker_threads << ",\"cache_max_entries\":" << config.max_entries
    << ",\"cache_memory_bytes\":" << config.memory_budget_bytes << ",\"cache_policy\":\"shared-runtime-warm-retained\",\"native_cache_enabled\":true,\"hdf5_mode\":\"NativeHdf5\",\"hdf5_input\":" << quote(hdf_file)
    << ",\"dataset\":" << quote(dataset) << ",\"hdf5_values\":" << oracle.values.size() << ",\"hdf5_bytes\":" << hdf_bytes.size() << ",\"arrow_values\":" << arrow_values
    << ",\"native_root\":" << quote(native_root.string()) << ",\"native_file_sha256\":" << quote(backend->native_location->sha256)
    << ",\"native_file_identity_kind\":\"auxiliary-fixture-binding-not-corpus-seal\",\"native_file_equal_to_input\":true"
    << ",\"arrow_bytes\":" << arrow_bytes.size() << ",\"scenario\":" << quote(scenario) << ",\"controlled_hold_ms\":" << (scenario == "mixed-held" ? 100 : 0)
    << ",\"warm_waves\":" << warm << ",\"measured_waves\":" << measured << ",\"campaign\":" << quote(campaign) << "}\n"; manifest.close();
  for (std::size_t wave = 0; wave < warm + measured; ++wave) {
    const bool is_warm = wave < warm, held = scenario == "mixed-held";
    const auto before = runtime->query_tasks().stats();
    std::mutex hold_mutex; std::condition_variable hold_changed; bool lock_entered = false;
    std::atomic<std::uint64_t> lock_start{0}, lock_release{0};
    std::future<QueryStatus> external;
    if (held) {
      external = std::async(std::launch::async, [&] {
        return with_hdf5_ingestion_source(source_for(hdf_bytes), {}, [&](const Hdf5SourceDriver&) {
          { std::lock_guard<std::mutex> guard(hold_mutex); lock_start.store(ns()); lock_entered = true; } hold_changed.notify_all();
          // Controlled contention only, never a normal-query execution cost.
          std::this_thread::sleep_for(std::chrono::milliseconds(100)); lock_release.store(ns());
          return QueryStatus::success();
        });
      });
      std::unique_lock<std::mutex> guard(hold_mutex);
      need(hold_changed.wait_for(guard, std::chrono::seconds(5), [&] { return lock_entered; }), "native lock holder did not enter");
    }
    std::atomic<bool> sampling{true}; std::vector<std::pair<std::uint64_t, QueryTaskSchedulerStats>> samples;
    std::thread sampler([&] { while (sampling.load()) { samples.emplace_back(ns(), runtime->query_tasks().stats()); std::this_thread::sleep_for(std::chrono::microseconds(100)); } });
    struct JoinSampler { std::atomic<bool>& running; std::thread& thread; ~JoinSampler() { running = false; if (thread.joinable()) thread.join(); } } join{sampling, sampler};
    Connection connections[8]; const auto wave_start = ns();
    // HDF first: without resource skipping, four unavailable native requests
    // would precede all Arrow requests in the shared FIFO.
    for (int i = 0; i < 8; ++i) {
      const bool hdf = scenario == "hdf5" || ((scenario == "mixed" || held) && i < 4);
      connections[i].format = hdf ? "hdf5" : "arrow";
      send_request(connections[i], socket, hdf ? hdf_request : arrow_request);
      if (held && i == 3) {
        const auto queued_deadline = Clock::now() + std::chrono::milliseconds(50);
        bool all_blocked = false;
        while (Clock::now() < queued_deadline) {
          const auto current = runtime->query_tasks().stats();
          if (current.queued == 4 && current.resource_blocked == 4 && current.active == 0) {
            all_blocked = true; break;
          }
          std::this_thread::yield();
        }
        need(all_blocked && !lock_release.load(), "HDF requests did not all wait outside execution slots before Arrow submission");
      }
    }
    int remaining = 8;
    while (remaining) {
      pollfd descriptors[8]; for (int i = 0; i < 8; ++i) descriptors[i] = {connections[i].fd, POLLIN, 0};
      int available; do { available = ::poll(descriptors, 8, 60000); } while (available < 0 && errno == EINTR);
      need(available > 0, "response poll timeout/error");
      for (int i = 0; i < 8; ++i) if (connections[i].fd >= 0 && (descriptors[i].revents & (POLLIN | POLLHUP | POLLERR))) {
        auto& c = connections[i]; receive(c);
        c.route = own_result(c, bridge, oracle.hdf_field); --remaining;
      }
    }
    std::uint64_t wave_end = 0;
    for (const auto& c : connections) wave_end = std::max(wave_end, c.owned_ns);
    // Oracle comparisons and hashes begin only after all eight results are
    // copied into independent owned arrays; they cannot delay later requests.
    const auto validation_start = ns();
    for (auto& c : connections) verify_owned(c, oracle, arrow_values);
    const auto validation_ns = ns() - validation_start;
    for (const auto& c : connections) bridge->drop(c.route);
    sampling = false; sampler.join();
    if (held) { const auto status = external.get(); need(status.ok(), status.message()); }
    const auto finish_deadline = Clock::now() + std::chrono::seconds(5);
    while (runtime->query_tasks().stats().completed < before.completed + 8 && Clock::now() < finish_deadline) std::this_thread::yield();
    const auto after = runtime->query_tasks().stats(); need(after.completed == before.completed + 8 && after.failed == before.failed, "scheduler completion mismatch");
    std::size_t arrow_before_unlock = 0;
    for (int i = 0; i < 8; ++i) {
      const auto& c = connections[i];
      const bool before_unlock = held && c.format == "arrow" && c.owned_ns < lock_release.load(); if (before_unlock) ++arrow_before_unlock;
      raw << "{\"campaign\":" << quote(campaign) << ",\"workers\":" << workers << ",\"scenario\":" << quote(scenario) << ",\"wave\":" << wave << ",\"warm\":" << (is_warm ? "true" : "false")
        << ",\"request\":" << i << ",\"format\":" << quote(c.format) << ",\"submit_ns\":" << c.submit_ns << ",\"receive_ns\":" << c.receive_ns << ",\"owned_ns\":" << c.owned_ns
        << ",\"response_latency_ns\":" << c.receive_ns - c.submit_ns << ",\"owned_latency_ns\":" << c.owned_ns - c.submit_ns << ",\"rows\":" << c.rows << ",\"result_bytes\":" << c.byte_count
        << ",\"oracle_validation_ns\":" << c.oracle_ns << ",\"values_fnv1a64\":" << quote(std::to_string(owned_hash(c.values)))
        << ",\"masks_fnv1a64\":" << quote(std::to_string(owned_hash(c.masks))) << ",\"ordinals_fnv1a64\":" << quote(std::to_string(owned_hash(c.ordinals)))
        << ",\"ordinal_masks_fnv1a64\":" << quote(std::to_string(owned_hash(c.ordinal_masks)))
        << ",\"values_and_masks_ok\":true,\"arrow_owned_before_unlock\":" << (before_unlock ? "true" : "false") << ",\"response\":" << c.response << "}\n";
    }
    for (const auto& sample : samples) { stats << "{\"wave\":" << wave << ",\"warm\":" << (is_warm ? "true" : "false") << ",\"steady_ns\":" << sample.first << ",\"stats\":"; stats_json(stats, sample.second); stats << "}\n"; }
    waves << "{\"campaign\":" << quote(campaign) << ",\"workers\":" << workers << ",\"scenario\":" << quote(scenario) << ",\"wave\":" << wave << ",\"warm\":" << (is_warm ? "true" : "false")
      << ",\"start_ns\":" << wave_start << ",\"owned_end_ns\":" << wave_end << ",\"wave_ns\":" << wave_end - wave_start << ",\"throughput_qps\":" << 8e9 / static_cast<double>(wave_end - wave_start)
      << ",\"oracle_validation_ns\":" << validation_ns << ",\"native_hold_enter_ns\":" << lock_start.load() << ",\"native_hold_release_marker_ns\":" << lock_release.load() << ",\"arrow_owned_before_unlock\":" << arrow_before_unlock
      << ",\"before\":"; stats_json(waves, before); waves << ",\"after\":"; stats_json(waves, after); waves << "}\n";
    raw.flush(); stats.flush(); waves.flush(); need(bool(raw) && bool(stats) && bool(waves), "evidence write failed");
    if (held) need(arrow_before_unlock == 4, "Arrow did not finish before controlled native lock release");
  }
  server.stop();
  std::cout << "{\"ok\":true,\"output\":" << quote(out.string()) << ",\"waves\":" << warm + measured << ",\"requests\":" << (warm + measured) * 8 << "}\n";
  return 0;
}
} // namespace
int main(int argc, char** argv) {
  try { return run(argc, argv); }
  catch (const std::exception& e) {
    std::cerr << e.what() << '\n';
    // Preserve completed raw rows and the terminal failure instead of silently
    // discarding a slow or unsuccessful repetition.
    if (!owned_output.empty() && std::filesystem::is_directory(owned_output)) {
      std::ofstream failure(owned_output / "failure.json");
      failure << "{\"ok\":false,\"steady_ns\":" << ns() << ",\"message\":" << quote(e.what()) << "}\n";
    }
    return 1;
  }
}
