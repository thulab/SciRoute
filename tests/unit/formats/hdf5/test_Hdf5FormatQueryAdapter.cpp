#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/storage/NativeFileStore.h"
#include "../../../support/InMemoryDatasetCatalog.h"

#include <gtest/gtest.h>
#include <hdf5.h>
#include <algorithm>
#include <fstream>
#include <functional>
#include <iterator>
#include <numeric>
#include <map>
#include <future>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <cmath>
#include <limits>
#include <optional>
#include <filesystem>
#include <unistd.h>

namespace {
using namespace iotdbfs;
void check(herr_t result) { if (result < 0) throw std::runtime_error("H04 fixture failure"); }
struct Id {
  hid_t value; herr_t (*close)(hid_t);
  Id(hid_t value, herr_t (*close)(hid_t)) : value(value), close(close) {
    if (value < 0) throw std::runtime_error("H04 fixture handle failure");
  }
  ~Id() { close(value); }
  operator hid_t() const { return value; }
};
struct Temporary {
  std::string path;
  Temporary() {
    char name[] = "/tmp/iotdbfs-h04-XXXXXX";
    const int fd = mkstemp(name);
    if (fd < 0) throw std::runtime_error("H04 temporary file failure");
    ::close(fd); path = name;
  }
  ~Temporary() { unlink(path.c_str()); }
};
std::uint64_t product(const std::vector<hsize_t>& dimensions) {
  return std::accumulate(dimensions.begin(), dimensions.end(), std::uint64_t{1}, std::multiplies<std::uint64_t>());
}
// Reverse coordinate writes deliberately make file offsets disagree with C order.
BinaryValue fixture(std::vector<hsize_t> shape = {5, 7, 9},
    std::vector<hsize_t> chunks = {}, bool wide = false, bool big = false,
    bool reverse = false, bool filter = false, bool sparse = false, bool special = false) {
  Temporary temporary;
  {
    Id access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    check(H5Pset_fapl_sec2(access));
    Id creation(H5Pcreate(H5P_FILE_CREATE), H5Pclose);
    check(H5Pset_userblock(creation, 512));
    Id file(H5Fcreate(temporary.path.c_str(), H5F_ACC_TRUNC, creation, access), H5Fclose);
    Id space(H5Screate_simple(shape.size(), shape.data(), nullptr), H5Sclose);
    Id props(H5Pcreate(H5P_DATASET_CREATE), H5Pclose);
    if (!chunks.empty()) check(H5Pset_chunk(props, chunks.size(), chunks.data()));
    if (filter) check(H5Pset_fletcher32(props));
    const hid_t type = wide ? (big ? H5T_IEEE_F64BE : H5T_IEEE_F64LE) :
      (big ? H5T_IEEE_F32BE : H5T_IEEE_F32LE);
    Id dataset(H5Dcreate2(file, "/a", type, space, H5P_DEFAULT, props, H5P_DEFAULT), H5Dclose);
    check(H5Lcreate_hard(file, "/a", file, "/alias", H5P_DEFAULT, H5P_DEFAULT));
    std::vector<double> data(product(shape));
    for (std::size_t i = 0; i < data.size(); ++i) data[i] = static_cast<double>(i) + .25;
    if (special) {
      const std::vector<std::uint64_t> bits = wide ?
        std::vector<std::uint64_t>{0, 0x8000000000000000ULL, 0x7ff0000000000000ULL, 0xfff0000000000000ULL, 0x7ff8000000000123ULL, 1} :
        std::vector<std::uint64_t>{0, 0x80000000U, 0x7f800000U, 0xff800000U, 0x7fc00123U, 1};
      const std::size_t width = wide ? 8 : 4;
      BinaryValue native(bits.size() * width);
      for (std::size_t i = 0; i < bits.size(); ++i)
        for (std::size_t byte = 0; byte < width; ++byte)
          native[i*width+byte] = static_cast<std::uint8_t>(bits[i] >> (8*(big ? width-1-byte : byte)));
      check(H5Dwrite(dataset, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, native.data()));
    } else if (!sparse && !reverse) {
      check(H5Dwrite(dataset, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, data.data()));
    } else if (!sparse) {
      std::vector<std::vector<hsize_t>> coordinates;
      std::vector<hsize_t> current(shape.size());
      for (;;) {
        coordinates.push_back(current);
        std::size_t axis = shape.size();
        while (axis) {
          --axis; current[axis] += chunks[axis];
          if (current[axis] < shape[axis]) break;
          current[axis] = 0;
        }
        if (axis == 0 && current[0] == 0) break;
      }
      for (auto it = coordinates.rbegin(); it != coordinates.rend(); ++it) {
        std::vector<hsize_t> extent(shape.size());
        for (std::size_t i = 0; i < shape.size(); ++i) extent[i] = std::min(chunks[i], shape[i] - (*it)[i]);
        Id memory(H5Screate_simple(extent.size(), extent.data(), nullptr), H5Sclose);
        check(H5Sselect_hyperslab(space, H5S_SELECT_SET, it->data(), nullptr, extent.data(), nullptr));
        std::vector<double> tile(product(extent));
        for (std::size_t i = 0; i < tile.size(); ++i) {
          std::size_t left = i, index = 0, stride = 1;
          for (std::size_t d = shape.size(); d-- > 0;) {
            index += ((*it)[d] + left % extent[d]) * stride;
            left /= extent[d]; stride *= shape[d];
          }
          tile[i] = data[index];
        }
        check(H5Dwrite(dataset, H5T_NATIVE_DOUBLE, memory, space, H5P_DEFAULT, tile.data()));
      }
    }
  }
  std::ifstream input(temporary.path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input), {}};
}
class Source final : public QuerySource {
public:
  explicit Source(BinaryValue bytes = fixture(), Hdf5LayoutLimits limits = {}) : payload(std::move(bytes)) {
    FormatSource input{payload.size(), [this](std::uint64_t offset, std::uint64_t length) -> std::optional<BinaryValue> {
      auto result = read(payload, offset, length);
      if (!result.ok()) return std::nullopt;
      return std::move(result.value());
    }};
    Hdf5FormatAdapter adapter(limits);
    auto ingestion = adapter.begin_ingestion(input);
    auto status = ingestion->consume_chunk(0, payload.data(), payload.size());
    if (!status.ok()) throw std::runtime_error(status.error());
    auto result = ingestion->finish();
    if (!result.ok()) throw std::runtime_error(result.error());
    layout = std::move(result.value().layout);
    manifest_.key = {"/hdf5/wrong-extension.bin", 17};
    manifest_.format_id = "hdf5";
    manifest_.payload_size = payload.size(); manifest_.layout_size = layout.size();
    manifest_.revision = 5; manifest_.published = true; manifest_.phase = StructuredBlobPhase::Published;
  }
  ~Source() override {
    native_store.reset();
    if (!native_root.empty()) {
      std::error_code ignored;
      std::filesystem::remove_all(native_root, ignored);
    }
  }
  const StructuredBlobManifest& manifest() const override { return manifest_; }
  QueryResult<std::optional<std::string>> verified_layout_identity() const override {
    ++identity_calls;
    if (identity_failure)
      return QueryResult<std::optional<std::string>>::failure(QueryStatus::failure(
        QueryError::StalePlan, QueryPhase::Storage, "injected frozen layout proof"));
    return QueryResult<std::optional<std::string>>::success(verified_identity);
  }
  QueryResult<BinaryValue> read_payload(std::uint64_t offset, std::uint64_t length) const override {
    payload_reads.push_back({offset, length});
    if (on_read) on_read();
    if (throw_read) throw std::runtime_error("injected source callback exception");
    if (fail) return QueryResult<BinaryValue>::failure(QueryStatus::failure(
      QueryError::ReadFailure, QueryPhase::Storage, "injected H04 read failure"));
    auto result = read(payload, offset, length);
    if (short_read && result.ok() && !result.value().empty()) result.value().pop_back();
    return result;
  }
  QueryResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
    using Result = QueryResult<std::shared_ptr<const NativeFileHandle>>;
    if (missing_native_file)
      return Result::failure(QueryStatus::failure(QueryError::SourceNotFound,
        QueryPhase::Storage, "fixture has no managed file binding"));
    if (fail)
      return Result::failure(QueryStatus::failure(QueryError::ReadFailure,
        QueryPhase::Storage, "injected managed file read failure"));
    if (!native_store) {
      char root[] = "/tmp/iotdbfs-h04-native-XXXXXX";
      if (!mkdtemp(root)) throw std::runtime_error("native fixture directory failure");
      native_root = root;
      native_store = std::make_unique<NativeFileStore>(native_root);
    }
    NativeFileLocation location;
    location.namespace_identity = {"11111111-1111-4111-8111-111111111111",
      "22222222-2222-4222-8222-222222222222"};
    location.key = manifest_.key;
    location.descriptor_id = std::string(64, 'a');
    location.seal_id = std::string(64, 'b');
    location.payload_root = std::string(64, 'c');
    location.size = payload.size();
    auto installed = native_store->install(location, [this](auto offset, auto length) {
      auto bytes = read(payload, offset, length);
      if (!bytes.ok()) return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::BackendFailure, bytes.status().message());
      return StructuredBlobResult<StructuredBlobBytes>::success(std::move(bytes.value()));
    });
    if (!installed.ok()) throw std::runtime_error(installed.status().message());
    auto opened = native_store->open(installed.value());
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    return Result::success(std::move(opened.value()));
  }
  QueryStatus reserve_logical_payload(std::uint64_t length) const override {
    if (length > logical_payload_limit - std::min(logical_payload_bytes, logical_payload_limit))
      return QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Storage,
        "fixture request payload budget exhausted");
    ++native_reads;
    logical_payload_bytes += length;
    return QueryStatus::success();
  }
  QueryResult<BinaryValue> read_layout(std::uint64_t offset, std::uint64_t length) const override {
    layout_reads.push_back({offset, length});
    return read(layout, offset, length);
  }
  static QueryResult<BinaryValue> read(const BinaryValue& bytes, std::uint64_t offset, std::uint64_t length) {
    if (offset > bytes.size() || length > bytes.size() - offset)
      return QueryResult<BinaryValue>::failure(QueryStatus::failure(QueryError::ReadFailure,
        QueryPhase::Storage, "out of range H04 fixture read"));
    return QueryResult<BinaryValue>::success({bytes.begin() + offset, bytes.begin() + offset + length});
  }
  BinaryValue payload, layout;
  StructuredBlobManifest manifest_;
  mutable std::vector<std::pair<std::uint64_t, std::uint64_t>> payload_reads, layout_reads;
  bool fail{false}, short_read{false}, throw_read{false};
  bool identity_failure{false};
  std::optional<std::string> verified_identity;
  mutable std::size_t identity_calls{0};
  bool missing_native_file{false};
  std::uint64_t logical_payload_limit{std::numeric_limits<std::uint64_t>::max()};
  mutable std::uint64_t native_reads{0}, logical_payload_bytes{0};
  mutable std::string native_root;
  mutable std::unique_ptr<NativeFileStore> native_store;
  std::function<void()> on_read;
};
DomainSlice slice(std::size_t axis, std::uint64_t begin, std::uint64_t end) {
  return {hdf5_axis_field(axis), DomainBound{LogicalValue::uint64(begin), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(end), BoundKind::Exclusive}};
}
QueryFragment query(const std::vector<std::uint64_t>& begin = {1, 2, 3},
    const std::vector<std::uint64_t>& end = {5, 6, 8}, bool axes = true,
    const std::string& path = "/a") {
  QueryFragment fragment;
  auto field = make_hdf5_value_field(path);
  if (!field.ok()) throw std::runtime_error(field.status().message());
  fragment.projection.push_back(field.value());
  if (axes) for (std::size_t i = begin.size(); i-- > 0;) fragment.projection.push_back(hdf5_axis_field(i));
  for (std::size_t i = 0; i < begin.size(); ++i) fragment.slices.push_back(slice(i, begin[i], end[i]));
  return fragment;
}
std::vector<QueryBatch> execute(const std::shared_ptr<Source>& source, const QueryFragment& fragment,
    QueryExecutionOptions options = {}, Hdf5QueryMode mode = Hdf5QueryMode::IoTDBIndexed) {
  Hdf5FormatQueryAdapter adapter({}, {}, {}, {}, {}, {}, mode);
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  if (!plan.ok()) throw std::runtime_error(plan.status().message());
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), options);
  if (!cursor.ok()) throw std::runtime_error(cursor.status().message());
  std::vector<QueryBatch> batches;
  for (;;) {
    auto next = cursor.value()->next();
    if (!next.ok()) throw std::runtime_error(next.status().message());
    if (!next.value()) break;
    batches.push_back(std::move(*next.value()));
  }
  return batches;
}
void verify(const std::vector<QueryBatch>& batches, const std::vector<std::uint64_t>& source_shape,
    const std::vector<std::uint64_t>& begin, const std::vector<std::uint64_t>& end, bool wide, bool axes) {
  std::vector<std::uint64_t> position = begin;
  std::uint64_t count = 0, expected = 1;
  for (std::size_t d = 0; d < begin.size(); ++d) expected *= end[d] - begin[d];
  for (const auto& batch : batches) {
    ASSERT_TRUE(batch.domain());
    EXPECT_EQ(batch.domain()->kind, DomainKind::DenseGrid);
    EXPECT_EQ(batch.domain()->origin, position);
    EXPECT_EQ(batch.domain()->shape.size(), begin.size());
    ASSERT_EQ(batch.columns().size(), axes ? begin.size() + 1 : 1);
    for (std::size_t i = 0; i < batch.row_count(); ++i) {
      std::uint64_t index = 0;
      for (std::size_t d = 0; d < begin.size(); ++d) index = index * source_shape[d] + position[d];
      if (wide) EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(batch.columns()[0].values)[i], index + .25);
      else EXPECT_FLOAT_EQ(std::get<std::vector<float>>(batch.columns()[0].values)[i], index + .25);
      if (axes) for (std::size_t d = 0; d < begin.size(); ++d) {
        EXPECT_EQ(batch.schema().fields()[begin.size()-d].field_id, hdf5_axis_field(d).field_id);
        EXPECT_EQ(std::get<std::vector<std::uint64_t>>(batch.columns()[begin.size()-d].values)[i], position[d]);
      }
      ++count;
      for (std::size_t d = begin.size(); d-- > 0;) {
        if (++position[d] < end[d]) break;
        position[d] = begin[d];
      }
    }
  }
  EXPECT_EQ(count, expected);
}
class MemoryBackend final : public StructuredBlobStorageBackend {
public:
  iotdbfs::test::InMemoryDatasetCatalog dataset_catalog;
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key, const DatasetCatalogArtifact& artifact) override {
    return dataset_catalog.write(key, artifact);
  }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(const StructuredBlobVersionKey& key) const override {
    return dataset_catalog.read(key);
  }
  std::uint64_t payload_bytes_written{0};
  std::function<void()> on_payload_read;
  enum class Fault { None, PayloadWrite, Seal, LayoutWrite, MetadataWrite,
    Publish };

  struct Record {
    StructuredBlobManifest manifest;
    StructuredBlobBytes payload;
    StructuredBlobBytes layout;
  };

  StructuredBlobStatus create_manifest(
    const StructuredBlobManifest& manifest) override {
    if (records_.count(manifest.key)) return failure("already exists");
    records_.emplace(manifest.key, Record{ manifest, {}, {} });
    return StructuredBlobStatus::success();
  }

  StructuredBlobResult<StructuredBlobManifest> load_manifest(
    const StructuredBlobVersionKey& key) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    return StructuredBlobResult<StructuredBlobManifest>::success(
      found->second.manifest);
  }

  StructuredBlobStatus replace_manifest(
    const StructuredBlobVersionKey& key, std::uint64_t expected_revision,
    const StructuredBlobManifest& next) override {
    auto found = records_.find(key);
    if (found == records_.end() ||
        found->second.manifest.revision != expected_revision ||
        next.revision != expected_revision + 1) {
      return failure("invalid manifest transition");
    }
    if (fault_ == Fault::Seal &&
        next.phase == StructuredBlobPhase::PayloadSealed)
      return failure("seal failed");
    if (fault_ == Fault::Publish && next.published)
      return failure("publish failed");
    found->second.manifest = next;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    const StructuredBlobBytes& bytes) override {
    auto found = records_.find(key);
    if (found == records_.end()) return failure("not found");
    if (fault_ == Fault::PayloadWrite) return failure("payload write failed");
    payload_bytes_written += bytes.size();
    const auto end = static_cast<std::size_t>(offset) + bytes.size();
    if (found->second.payload.size() < end)
      found->second.payload.resize(end);
    std::copy(bytes.begin(), bytes.end(),
      found->second.payload.begin() + static_cast<std::size_t>(offset));
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_layout(const StructuredBlobVersionKey& key,
    const StructuredBlobBytes& layout) override {
    auto found = records_.find(key);
    if (found == records_.end()) return failure("not found");
    if (fault_ == Fault::LayoutWrite) return failure("layout write failed");
    found->second.layout = layout;
    return StructuredBlobStatus::success();
  }

  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey& key,
    const MetadataMap&) override {
    if (fault_ == Fault::MetadataWrite)
      return failure("metadata write failed");
    return records_.count(key) ? StructuredBlobStatus::success() :
      failure("not found");
  }

  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
    const StructuredBlobVersionKey& key, std::uint64_t offset,
    std::uint64_t length) const override {
    if (on_payload_read) on_payload_read();
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    const auto begin = found->second.payload.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      StructuredBlobBytes(begin, begin + static_cast<std::size_t>(length)));
  }

  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      const StructuredBlobVersionKey& key, std::uint64_t offset,
      std::uint64_t length) const override {
    const auto found = records_.find(key);
    if (found == records_.end()) {
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    }
    const auto begin = found->second.layout.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }

  StructuredBlobStatus erase(const StructuredBlobVersionKey& key,
    std::uint64_t expected_revision) override {
    const auto found = records_.find(key);
    if (found == records_.end() ||
        found->second.manifest.revision != expected_revision)
      return failure("erase failed");
    dataset_catalog.erase(key);
    records_.erase(found);
    return StructuredBlobStatus::success();
  }

  const Record* peek(const StructuredBlobVersionKey& key) const {
    const auto found = records_.find(key);
    return found == records_.end() ? nullptr : &found->second;
  }

  void fail_publish() { fault_ = Fault::Publish; }
  void set_fault(Fault fault) { fault_ = fault; }

private:
  static StructuredBlobStatus failure(const char* message) {
    return StructuredBlobStatus::failure(
      StructuredBlobError::BackendFailure, message);
  }

  std::map<StructuredBlobVersionKey, Record> records_;
  Fault fault_{ Fault::None };
};

// Native HDF5 creates every encoded pipeline. With a skip mask the bytes are
// produced by the corresponding subset pipeline, then copied into the full
// pipeline using the public raw-chunk API. No test codec duplicates production.
struct FilterFixture {
  std::vector<hsize_t> shape{7}, chunks{4};
  bool wide{false}, big{false}, partial_unfiltered{false};
  unsigned filters{7}; // shuffle, deflate, Fletcher32 in native order
  std::optional<unsigned> skip_mask;
  int fill{0}; // default, explicit, undefined, NaN, negative zero
  H5D_fill_time_t fill_time{H5D_FILL_TIME_IFSET};
  int writes{0}; // all, none, first complete chunk
};
BinaryValue filtered_fixture(const FilterFixture& config) {
  Temporary temporary;
  {
    Id file_access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    if (config.partial_unfiltered) check(H5Pset_libver_bounds(file_access, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST));
    Id file(H5Fcreate(temporary.path.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, file_access), H5Fclose);
    Id space(H5Screate_simple(config.shape.size(), config.shape.data(), nullptr), H5Sclose);
    const hid_t type = config.wide ? (config.big ? H5T_IEEE_F64BE : H5T_IEEE_F64LE) :
      (config.big ? H5T_IEEE_F32BE : H5T_IEEE_F32LE);
    const auto make_props = [&](unsigned filters) {
      hid_t props = H5Pcreate(H5P_DATASET_CREATE);
      if (!config.chunks.empty()) check(H5Pset_chunk(props, config.chunks.size(), config.chunks.data()));
      if (config.fill == 2) check(H5Pset_fill_value(props, type, nullptr));
      else if (config.fill) {
        double fill = config.fill == 1 ? 13.5 : (config.fill == 3 ? std::numeric_limits<double>::quiet_NaN() : -0.0);
        check(H5Pset_fill_value(props, H5T_NATIVE_DOUBLE, &fill));
      }
      check(H5Pset_fill_time(props, config.fill_time));
      if (filters & 1) check(H5Pset_shuffle(props));
      if (filters & 2) check(H5Pset_deflate(props, 6));
      if (filters & 4) check(H5Pset_fletcher32(props));
      if (filters & 8) check(H5Pset_filter(props, 65000, H5Z_FLAG_OPTIONAL, 0, nullptr));
      if (config.partial_unfiltered) check(H5Pset_chunk_opts(props, H5D_CHUNK_DONT_FILTER_PARTIAL_CHUNKS));
      return props;
    };
    Id properties(make_props(config.filters), H5Pclose);
    Id destination(H5Dcreate2(file, "/a", type, space, H5P_DEFAULT, properties, H5P_DEFAULT), H5Dclose);
    std::optional<Id> source;
    if (config.skip_mask) {
      Id subset_properties(make_props(config.filters & ~*config.skip_mask), H5Pclose);
      source.emplace(H5Dcreate2(file, "/encoded", type, space, H5P_DEFAULT, subset_properties, H5P_DEFAULT), H5Dclose);
    }
    const hid_t writer = source ? source->value : destination.value;
    if (config.writes != 1) {
      std::vector<hsize_t> write_shape = config.writes == 2 ? config.chunks : config.shape;
      for (std::size_t axis = 0; axis < write_shape.size(); ++axis)
        write_shape[axis] = std::min(write_shape[axis], config.shape[axis]);
      std::vector<hsize_t> origin(config.shape.size());
      check(H5Sselect_hyperslab(space, H5S_SELECT_SET, origin.data(), nullptr, write_shape.data(), nullptr));
      Id memory(H5Screate_simple(write_shape.size(), write_shape.data(), nullptr), H5Sclose);
      std::vector<double> values(product(write_shape));
      for (std::size_t linear = 0; linear < values.size(); ++linear) {
        std::size_t left = linear, source_index = 0, stride = 1;
        for (std::size_t axis = config.shape.size(); axis-- > 0;) {
          source_index += (left % write_shape[axis]) * stride;
          left /= write_shape[axis]; stride *= config.shape[axis];
        }
        values[linear] = source_index + .25;
      }
      check(H5Dwrite(writer, H5T_NATIVE_DOUBLE, memory, space, H5P_DEFAULT, values.data()));
    }
    if (source) {
      check(H5Dflush(*source));
      hsize_t chunks = 0;
      check(H5Dget_num_chunks(*source, H5S_ALL, &chunks));
      for (hsize_t i = 0; i < chunks; ++i) {
        std::vector<hsize_t> origin(config.shape.size()); unsigned original_mask = 0;
        haddr_t address = 0; hsize_t size = 0;
        check(H5Dget_chunk_info(*source, H5S_ALL, i, origin.data(), &original_mask, &address, &size));
        BinaryValue bytes(size);
        check(H5Dread_chunk(*source, H5P_DEFAULT, origin.data(), &original_mask, bytes.data()));
        check(H5Dwrite_chunk(destination, H5P_DEFAULT, *config.skip_mask, origin.data(), bytes.size(), bytes.data()));
      }
      // Prove the raw-mask fixture itself is accepted and decoded by HDF5.
      std::vector<double> oracle(product(config.shape));
      check(H5Dread(destination, H5T_NATIVE_DOUBLE, H5S_ALL, H5S_ALL, H5P_DEFAULT, oracle.data()));
      for (std::size_t i = 0; i < oracle.size(); ++i)
        if (oracle[i] != i + .25) throw std::runtime_error("native filter-mask oracle mismatch");
      check(H5Ldelete(file, "/encoded", H5P_DEFAULT));
    }
  }
  std::ifstream input(temporary.path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input), {}};
}
BinaryValue mutate_first_encoded_chunk(BinaryValue bytes,
    const std::function<void(BinaryValue&)>& mutate, hsize_t index = 0) {
  Temporary temporary;
  { std::ofstream output(temporary.path, std::ios::binary); output.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); }
  {
    Id file(H5Fopen(temporary.path.c_str(), H5F_ACC_RDWR, H5P_DEFAULT), H5Fclose);
    Id dataset(H5Dopen2(file, "/a", H5P_DEFAULT), H5Dclose);
    Id space(H5Dget_space(dataset), H5Sclose);
    std::vector<hsize_t> origin(H5Sget_simple_extent_ndims(space));
    unsigned mask = 0; haddr_t address = 0; hsize_t size = 0;
    check(H5Dget_chunk_info(dataset, H5S_ALL, index, origin.data(), &mask, &address, &size));
    BinaryValue encoded(size);
    check(H5Dread_chunk(dataset, H5P_DEFAULT, origin.data(), &mask, encoded.data()));
    mutate(encoded);
    check(H5Dwrite_chunk(dataset, H5P_DEFAULT, mask, origin.data(), encoded.size(), encoded.data()));
  }
  std::ifstream input(temporary.path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input), {}};
}

} // namespace

TEST(Hdf5FormatQueryAdapter, ReadsRankOneThroughThreeAndBothFloatWidthsAndByteOrders) {
  for (const bool wide : {false, true}) for (const bool big : {false, true})
    for (std::size_t rank = 1; rank <= 3; ++rank) for (const bool chunked : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "wide=" << wide << " big=" << big << " rank=" << rank << " chunked=" << chunked);
      std::vector<hsize_t> shape({5, 7, 9}), chunks({2, 3, 4});
      shape.resize(rank); chunks.resize(rank);
      std::vector<std::uint64_t> begin(rank, 1), end(shape.begin(), shape.end()), dimensions(shape.begin(), shape.end());
      --end.back();
      auto source = std::make_shared<Source>(fixture(shape, chunked ? chunks : std::vector<hsize_t>{}, wide, big));
      verify(execute(source, query(begin, end)), dimensions, begin, end, wide, true);
      for (const auto& range : source->payload_reads) EXPECT_LE(range.first + range.second, source->payload.size());
    }
}

TEST(Hdf5FormatQueryAdapter, ReverseAllocatedChunksAndPartialEdgesStillProduceCOrder) {
  auto source = std::make_shared<Source>(fixture({5, 7, 9}, {2, 3, 4}, false, true, true));
  QueryExecutionOptions options; options.target_batch_rows = 7; options.target_batch_bytes = 196;
  auto batches = execute(source, query(), options);
  EXPECT_GT(batches.size(), 1u);
  for (const auto& batch : batches) EXPECT_LE(batch.row_count(), 7u);
  verify(batches, {5,7,9}, {1,2,3}, {5,6,8}, false, true);
}

TEST(Hdf5FormatQueryAdapter, ValueOnlyProjectionRetainsSlabDomainAndAliasBinding) {
  auto source = std::make_shared<Source>(fixture({5,7,9}, {2,3,4}, true));
  QueryExecutionOptions options; options.target_batch_rows = 3;
  verify(execute(source, query({1,2,3}, {5,6,8}, false, "/alias"), options),
    {5,7,9}, {1,2,3}, {5,6,8}, true, false);
}

TEST(Hdf5FormatQueryAdapter, EmptySelectionRetainsSchemaWithoutPayloadReads) {
  auto source = std::make_shared<Source>(fixture({5,7,9}, {2,3,4}));
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query({1,2,3}, {1,6,8}, false));
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  EXPECT_EQ(cursor.value()->schema().fields().size(), 1u);
  auto next = cursor.value()->next();
  ASSERT_TRUE(next.ok()); EXPECT_FALSE(next.value()); EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, PlanningNeverReadsValuesAndSharesDirectoryWithPrepare) {
  auto source = std::make_shared<Source>(fixture({5,7,9}, {2,3,4}));
  Hdf5FormatQueryAdapter adapter;
  QueryPlanningContext context(*source);
  auto capabilities = adapter.capabilities(context, query());
  ASSERT_TRUE(capabilities.ok()) << capabilities.status().message();
  EXPECT_EQ(adapter.id(), "hdf5");
  EXPECT_TRUE(validate_query_capabilities(capabilities.value()).ok());
  ASSERT_GE(source->layout_reads.size(), 2u);
  const auto directory_reads = source->layout_reads.size();
  auto plan = adapter.prepare(context, query());
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  // Capabilities must not make prepare re-read the directory header/body.
  for (std::size_t i = directory_reads; i < source->layout_reads.size(); ++i)
    EXPECT_NE(source->layout_reads[i].first, 0u);
  EXPECT_TRUE(source->payload_reads.empty());
  const auto planned_reads = source->layout_reads.size();
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  EXPECT_EQ(source->layout_reads.size(), planned_reads);
  EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, CachedPlanAndExecutionDoNotRereadLayout) {
  auto source = std::make_shared<Source>();
  Hdf5FormatQueryAdapter adapter;
  auto first = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(first.ok()) << first.status().message();
  const auto cold_reads = source->layout_reads.size();
  ASSERT_GE(cold_reads, 2u);
  auto warm = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(warm.ok()) << warm.status().message();
  EXPECT_EQ(source->layout_reads.size(), cold_reads);
  auto cursor = adapter.execute(QueryExecutionContext(source),
                                warm.value().implementation(), {});
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  EXPECT_EQ(source->layout_reads.size(), cold_reads);
}

TEST(Hdf5FormatQueryAdapter, PreparedIdentityRejectsOtherNamespaceAndChangedRevision) {
  auto source = std::make_shared<Source>();
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(plan.ok());
  auto other = std::make_shared<Source>(source->payload);
  auto result = adapter.execute(QueryExecutionContext(other), plan.value().implementation(), {});
  EXPECT_FALSE(result.ok()); EXPECT_TRUE(other->payload_reads.empty());
  ++source->manifest_.revision;
  result = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  EXPECT_FALSE(result.ok()); EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, VerifiedLayoutSharesParsingAcrossVersionsButNotPlans) {
  auto first = std::make_shared<Source>(fixture({5,7,9}, {2,3,4}));
  auto second = std::make_shared<Source>(first->payload);
  ASSERT_EQ(first->layout, second->layout);
  second->manifest_.key = {"/renamed/other.h5", 18};
  first->verified_identity = second->verified_identity = "namespace/incarnation/layout/rules";
  Hdf5FormatQueryAdapter adapter;
  auto first_plan = adapter.prepare(QueryPlanningContext(*first), query());
  ASSERT_TRUE(first_plan.ok()) << first_plan.status().message();
  ASSERT_FALSE(first->layout_reads.empty());
  auto second_plan = adapter.prepare(QueryPlanningContext(*second), query());
  ASSERT_TRUE(second_plan.ok()) << second_plan.status().message();
  // Both the directory and all pages selected above are immutable cache hits.
  EXPECT_TRUE(second->layout_reads.empty());
  EXPECT_GT(second->identity_calls, 0u);
  auto wrong_plan = adapter.execute(QueryExecutionContext(second),
    first_plan.value().implementation(), {});
  EXPECT_FALSE(wrong_plan.ok());
  EXPECT_TRUE(second->payload_reads.empty());
  auto own_plan = adapter.execute(QueryExecutionContext(second),
    second_plan.value().implementation(), {});
  EXPECT_TRUE(own_plan.ok());
}

TEST(Hdf5FormatQueryAdapter, UnverifiedAndDifferentVerifiedIdentitiesDoNotShareParsing) {
  Hdf5FormatQueryAdapter adapter;
  auto first = std::make_shared<Source>();
  auto second = std::make_shared<Source>(first->payload);
  ++second->manifest_.key.version;
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*first), query()).ok());
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*second), query()).ok());
  EXPECT_FALSE(first->layout_reads.empty());
  EXPECT_FALSE(second->layout_reads.empty());
  first->layout_reads.clear(); second->layout_reads.clear();
  first->verified_identity = "namespace-A/incarnation/layout/rules";
  second->verified_identity = "namespace-B/incarnation/layout/rules";
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*first), query()).ok());
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*second), query()).ok());
  EXPECT_FALSE(first->layout_reads.empty());
  EXPECT_FALSE(second->layout_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, VerifiedIdentityFailurePrecedesWarmCacheAndRequestState) {
  auto source = std::make_shared<Source>();
  source->verified_identity = "namespace/incarnation/layout/rules";
  Hdf5FormatQueryAdapter adapter;
  QueryPlanningContext context(*source);
  ASSERT_TRUE(adapter.prepare(context, query()).ok());
  auto plan = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(plan.ok());
  source->layout_reads.clear();
  source->identity_failure = true;
  auto reused = adapter.prepare(context, query());
  ASSERT_FALSE(reused.ok());
  EXPECT_EQ(reused.status().code(), QueryError::StalePlan);
  auto cached = adapter.prepare(QueryPlanningContext(*source), query());
  ASSERT_FALSE(cached.ok());
  EXPECT_EQ(cached.status().code(), QueryError::StalePlan);
  EXPECT_TRUE(source->layout_reads.empty());
  EXPECT_TRUE(source->payload_reads.empty());
  auto execution = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_FALSE(execution.ok());
  EXPECT_EQ(execution.status().code(), QueryError::StalePlan);
  EXPECT_TRUE(source->payload_reads.empty());
  source->identity_failure = false;
  source->verified_identity = "";
  EXPECT_FALSE(adapter.prepare(QueryPlanningContext(*source), query()).ok());
}

TEST(Hdf5FormatQueryAdapter, SharedDirectoryHitRetainsRequestLayoutBudget) {
  auto first = std::make_shared<Source>();
  auto second = std::make_shared<Source>(first->payload);
  ++second->manifest_.key.version;
  first->verified_identity = second->verified_identity = "namespace/incarnation/layout/rules";
  Hdf5FormatQueryAdapter adapter;
  ASSERT_TRUE(adapter.prepare(QueryPlanningContext(*first), query()).ok());
  QueryLimits limits;
  limits.max_layout_bytes = kHdf5LayoutHeaderBytes;
  auto result = adapter.prepare(QueryPlanningContext(*second, limits), query());
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(second->layout_reads.empty());
  EXPECT_TRUE(second->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, SharedLayoutNeverSharesDecodedValuesAcrossVersions) {
  auto first = std::make_shared<Source>();
  auto parsed = read_hdf5_layout_directory([&](std::uint64_t offset, std::uint64_t length) {
    return Source::read(first->layout, offset, length);
  }, first->layout.size(), first->payload.size());
  ASSERT_TRUE(parsed.ok()) << parsed.status().message();
  std::optional<std::uint64_t> data_offset;
  for (const auto& object : parsed.value().document().objects)
    if (object.dataset && object.dataset->descriptor.path == "/a")
      data_offset = object.dataset->contiguous_offset;
  ASSERT_TRUE(data_offset);
  auto changed_payload = first->payload;
  // First selected element (1,2,3), explicit little-endian float 999.0f.
  const auto offset = *data_offset + (1 * 7 * 9 + 2 * 9 + 3) * 4;
  ASSERT_LE(offset + 4, changed_payload.size());
  const std::uint8_t replacement[]{0x00, 0xc0, 0x79, 0x44};
  std::copy(std::begin(replacement), std::end(replacement), changed_payload.begin() + offset);
  auto second = std::make_shared<Source>(std::move(changed_payload));
  ASSERT_EQ(first->layout, second->layout);
  ++second->manifest_.key.version;
  first->verified_identity = second->verified_identity = "namespace/incarnation/layout/rules";
  Hdf5FormatQueryAdapter adapter;
  for (const auto& source : {first, second}) {
    auto plan = adapter.prepare(QueryPlanningContext(*source), query());
    ASSERT_TRUE(plan.ok()) << plan.status().message();
    auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    auto next = cursor.value()->next();
    ASSERT_TRUE(next.ok()) << next.status().message();
    ASSERT_TRUE(next.value());
    const auto& values = std::get<std::vector<float>>(next.value()->columns()[0].values);
    ASSERT_FALSE(values.empty());
    EXPECT_FLOAT_EQ(values.front(), source == first ? 84.25f : 999.0f);
  }
  EXPECT_TRUE(second->layout_reads.empty());
  EXPECT_FALSE(second->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, CacheMissRejectsCorruptLayoutHeader) {
  auto source = std::make_shared<Source>();
  ASSERT_GT(source->layout.size(), 64u);
  source->layout[50] ^= 1;
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query());
  EXPECT_FALSE(plan.ok());
  EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, FilteredAndDefaultUnallocatedDatasetsAreQueryable) {
  auto filtered = std::make_shared<Source>(fixture({5,7,9}, {2,3,4}, false, false, false, true));
  verify(execute(filtered, query()), {5,7,9}, {1,2,3}, {5,6,8}, false, true);
  for (const bool chunked : {false, true}) {
    auto sparse = std::make_shared<Source>(fixture({5,7,9}, chunked ? std::vector<hsize_t>{2,3,4} :
      std::vector<hsize_t>{}, false, false, false, false, true));
    auto batches = execute(sparse, query());
    std::size_t count = 0;
    for (const auto& batch : batches) for (const auto value : std::get<std::vector<float>>(batch.columns()[0].values)) {
      EXPECT_EQ(value, 0.f); ++count;
    }
    EXPECT_EQ(count, 80u);
    EXPECT_TRUE(sparse->payload_reads.empty());
  }
}

TEST(Hdf5FormatQueryAdapter, PayloadFailuresAndShortReadsCannotReturnPartialSlabs) {
  for (const bool short_read : {false, true}) {
    auto source = std::make_shared<Source>();
    Hdf5FormatQueryAdapter adapter;
    auto plan = adapter.prepare(QueryPlanningContext(*source), query()); ASSERT_TRUE(plan.ok());
    auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
    ASSERT_TRUE(cursor.ok());
    source->short_read = short_read; source->fail = !short_read;
    auto next = cursor.value()->next();
    ASSERT_FALSE(next.ok()); EXPECT_EQ(next.status().code(), QueryError::ReadFailure);
  }
}

TEST(Hdf5FormatQueryAdapter, CancellationBeforeAndInsideReadIsSafeAndSuppressesResults) {
  for (const bool inside : {false, true}) {
    auto source = std::make_shared<Source>();
    Hdf5FormatQueryAdapter adapter;
    auto plan = adapter.prepare(QueryPlanningContext(*source), query()); ASSERT_TRUE(plan.ok());
    auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
    ASSERT_TRUE(cursor.ok());
    if (inside) source->on_read = [&] { cursor.value()->cancel(); };
    else cursor.value()->cancel();
    auto next = cursor.value()->next();
    ASSERT_FALSE(next.ok()); EXPECT_EQ(next.status().code(), QueryError::Cancelled);
    if (!inside) EXPECT_TRUE(source->payload_reads.empty());
    else EXPECT_EQ(source->payload_reads.size(), 1u);
  }
}

TEST(Hdf5FormatQueryAdapter, PlanningLayoutRangeAndOutputBudgetsRejectBeforeValueReads) {
  Hdf5FormatQueryAdapter adapter;
  for (int kind = 1; kind < 4; ++kind) {
    auto source = std::make_shared<Source>(fixture({5,7,9}, {2,3,4}));
    QueryLimits limits;
    if (kind == 1) limits.max_ranges = 1;
    if (kind == 2) limits.max_result_rows = 1;
    if (kind == 3) limits.max_layout_bytes = 1;
    auto plan = adapter.prepare(QueryPlanningContext(*source, limits), query());
    EXPECT_FALSE(plan.ok()) << "limit case " << kind;
    EXPECT_TRUE(source->payload_reads.empty());
  }
}

TEST(Hdf5FormatQueryAdapter, ExecutionHonorsStricterBudgetsThanPreparedPlan) {
  Hdf5FormatQueryAdapter adapter;
  for (int kind = 0; kind < 4; ++kind) {
    auto source = std::make_shared<Source>(fixture({5,7,9}, {2,3,4}));
    auto plan = adapter.prepare(QueryPlanningContext(*source), query()); ASSERT_TRUE(plan.ok());
    QueryExecutionOptions limits;
    if (kind == 0) limits.max_payload_bytes = 4;
    if (kind == 1) limits.max_ranges = 1;
    if (kind == 2) limits.max_result_rows = 1;
    if (kind == 3) limits.target_batch_bytes = 1;
    auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), limits);
    if (cursor.ok()) {
      auto next = cursor.value()->next();
      EXPECT_FALSE(next.ok()) << "limit case " << kind;
    }
    std::uint64_t bytes = 0;
    for (const auto& range : source->payload_reads) bytes += range.second;
    EXPECT_LE(bytes, limits.max_payload_bytes);
    EXPECT_LE(source->payload_reads.size(), limits.max_ranges);
    if (kind != 1) EXPECT_TRUE(source->payload_reads.empty());
  }
}

TEST(Hdf5FormatQueryAdapter, StructuredStorageAndQueryModuleRetainSourceAndProjectionContract) {
  auto backend = std::make_shared<MemoryBackend>();
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  const auto bytes = fixture({5,7,9}, {2,3,4}, true, true, true);
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()));
  IngestionModule ingestion(*storage, formats, {173});
  const StructuredBlobVersionKey key{"/science/renamed.arrow", 17};
  FormatSource input{bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length) -> std::optional<BinaryValue> {
    auto result = Source::read(bytes, offset, length);
    if (!result.ok()) return std::nullopt;
    return std::move(result.value());
  }};
  auto ingested = ingestion.ingest(key, input, [](const IngestionOutcome&) { return true; });
  ASSERT_TRUE(ingested.ok()) << ingested.message();
  ASSERT_NE(backend->peek(key), nullptr);
  auto catalog = make_in_memory_published_version_catalog({backend->peek(key)->manifest});
  auto module = QueryModule::create(catalog, storage, {std::make_shared<Hdf5FormatQueryAdapter>()});
  ASSERT_TRUE(module.ok()) << module.status().message();
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, key.path}};
  request.sources.versions = VersionSelection::exact_version(key.version);
  request.subset = query(); request.limits.target_batch_rows = 7;
  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  std::vector<QueryBatch> batches;
  for (;;) {
    auto next = cursor.value()->next();
    ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    ASSERT_EQ(next.value()->sources().size(), 1u);
    EXPECT_EQ(next.value()->sources()[0].path, key.path);
    EXPECT_EQ(next.value()->sources()[0].version, key.version);
    batches.push_back(std::move(*next.value()));
  }
  verify(batches, {5,7,9}, {1,2,3}, {5,6,8}, true, true);
  request.sources.versions = VersionSelection::exact_version(18);
  auto missing = module.value()->prepare(request);
  ASSERT_FALSE(missing.ok()); EXPECT_EQ(missing.status().code(), QueryError::SourceNotFound);
  // A retained plan may finish from its validated decoded fragments after
  // deletion, but it cannot switch versions or manufacture other values.
  ASSERT_TRUE(backend->erase(key, backend->peek(key)->manifest.revision).ok());
  request.sources.versions = VersionSelection::exact_version(key.version);
  EXPECT_FALSE(module.value()->prepare(request).ok());
  auto deleted = module.value()->execute(prepared.value());
  if (deleted.ok()) {
    std::vector<QueryBatch> retained;
    bool complete = true;
    for (;;) {
      auto next = deleted.value()->next();
      if (!next.ok()) { complete = false; break; }
      if (!next.value()) break;
      retained.push_back(std::move(*next.value()));
    }
    if (complete)
      verify(retained, {5,7,9}, {1,2,3}, {5,6,8}, true, true);
  }
}

TEST(Hdf5FormatQueryAdapter, ContiguousLogicalTilesReadOnlyTouchedRangesAndReuseAcrossSmallSlabs) {
  auto source = std::make_shared<Source>(fixture({10000}));
  Hdf5QueryKernelConfig config; config.contiguous_tile_elements = 16;
  Hdf5FormatQueryAdapter adapter(config);
  auto plan = adapter.prepare(QueryPlanningContext(*source), query({34}, {59}, false));
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  QueryExecutionOptions options; options.target_batch_rows = 3;
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), options);
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  std::vector<QueryBatch> batches;
  for (;;) {
    auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    batches.push_back(std::move(*next.value()));
  }
  verify(batches, {10000}, {34}, {59}, false, false);
  ASSERT_EQ(source->payload_reads.size(), 2u);
  EXPECT_EQ(source->payload_reads[0].second, 16 * sizeof(float));
  EXPECT_EQ(source->payload_reads[1].second, 16 * sizeof(float));
  EXPECT_EQ(source->payload_reads[1].first, source->payload_reads[0].first + 16 * sizeof(float));
}

TEST(Hdf5FormatQueryAdapter, SmallSelectionLoadsOnlyIntersectingChunkPages) {
  auto source = std::make_shared<Source>(fixture({2048}, {1}));
  Hdf5FormatQueryAdapter adapter;
  QueryPlanningContext context(*source);
  auto capabilities = adapter.capabilities(context, query({1000}, {1001}, false));
  ASSERT_TRUE(capabilities.ok());
  const auto directory_reads = source->layout_reads.size();
  auto plan = adapter.prepare(context, query({1000}, {1001}, false));
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  EXPECT_EQ(directory_reads, 3u);
  EXPECT_EQ(source->layout_reads.size(), directory_reads);
  auto identity = hdf5_query_identity(plan.value().implementation());
  ASSERT_TRUE(identity.ok());
  EXPECT_EQ(identity.value().source.key, source->manifest_.key);
  EXPECT_EQ(identity.value().storage_namespace, source->storage_namespace());
  EXPECT_EQ(identity.value().dataset_shape, (std::vector<std::uint64_t>{2048}));
  EXPECT_EQ(identity.value().dataset_path, "/a");
  EXPECT_FALSE(identity.value().dataset_id.empty());
  EXPECT_EQ(identity.value().value_type, LogicalTypeId::Float32);
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok());
  auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()); ASSERT_TRUE(next.value());
  EXPECT_FLOAT_EQ(std::get<std::vector<float>>(next.value()->columns()[0].values)[0], 1000.25f);
  ASSERT_EQ(source->payload_reads.size(), 1u);
  EXPECT_EQ(source->payload_reads[0].second, sizeof(float));
}

TEST(Hdf5FormatQueryAdapter, RunningQueryModuleCursorCanBeCancelledDuringBackendRead) {
  auto backend = std::make_shared<MemoryBackend>();
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  const auto bytes = fixture({128});
  FormatRegistry formats;
  ASSERT_TRUE(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()));
  IngestionModule ingestion(*storage, formats, {});
  const StructuredBlobVersionKey key{"/science/a.h5", 1};
  FormatSource input{bytes.size(), [&bytes](std::uint64_t offset, std::uint64_t length) -> std::optional<BinaryValue> {
    auto result = Source::read(bytes, offset, length);
    if (!result.ok()) return std::nullopt;
    return std::move(result.value());
  }};
  ASSERT_TRUE(ingestion.ingest(key, input, [](const IngestionOutcome&) { return true; }).ok());
  auto module = QueryModule::create(make_in_memory_published_version_catalog({backend->peek(key)->manifest}),
    storage, {std::make_shared<Hdf5FormatQueryAdapter>()});
  ASSERT_TRUE(module.ok());
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, key.path}};
  request.sources.versions = VersionSelection::exact_version(key.version);
  request.subset = query({4}, {32}, false);
  auto prepared = module.value()->prepare(request); ASSERT_TRUE(prepared.ok());
  auto cursor = module.value()->execute(prepared.value()); ASSERT_TRUE(cursor.ok());
  std::promise<void> entered, release;
  auto entered_future = entered.get_future();
  auto release_future = release.get_future().share();
  backend->on_payload_read = [&] { entered.set_value(); release_future.wait(); };
  auto next = std::async(std::launch::async, [&] { return cursor.value()->next(); });
  const auto status = entered_future.wait_for(std::chrono::seconds(3));
  if (status != std::future_status::ready) {
    release.set_value();
    FAIL() << "cursor did not enter the backend payload read";
  }
  cursor.value()->cancel();
  release.set_value();
  auto result = next.get();
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::Cancelled);
}

TEST(Hdf5FormatQueryAdapter, ThrowingSourceReadBecomesStickyQueryFailure) {
  auto source = std::make_shared<Source>();
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query()); ASSERT_TRUE(plan.ok());
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok());
  source->throw_read = true;
  auto next = cursor.value()->next();
  ASSERT_FALSE(next.ok());
  EXPECT_EQ(next.status().code(), QueryError::ReadFailure);
  const auto calls = source->payload_reads.size();
  source->throw_read = false;
  next = cursor.value()->next();
  EXPECT_FALSE(next.ok());
  EXPECT_EQ(source->payload_reads.size(), calls);
}

TEST(Hdf5FormatQueryAdapter, EndianConversionPreservesSignedZeroInfinityNanPayloadAndSubnormalBits) {
  for (const bool wide : {false, true}) for (const bool big : {false, true})
    for (const bool chunked : {false, true}) for (const bool filtered : {false, true}) {
      if (filtered && !chunked) continue;
      SCOPED_TRACE(::testing::Message() << "wide=" << wide << " big=" << big << " chunked=" << chunked);
      auto source = std::make_shared<Source>(fixture({6}, chunked ? std::vector<hsize_t>{4} : std::vector<hsize_t>{},
        wide, big, false, filtered, false, true));
      auto batches = execute(source, query({0}, {6}, false));
      ASSERT_EQ(batches.size(), 1u);
      const std::vector<std::uint64_t> expected = wide ?
        std::vector<std::uint64_t>{0, 0x8000000000000000ULL, 0x7ff0000000000000ULL, 0xfff0000000000000ULL, 0x7ff8000000000123ULL, 1} :
        std::vector<std::uint64_t>{0, 0x80000000U, 0x7f800000U, 0xff800000U, 0x7fc00123U, 1};
      for (std::size_t i = 0; i < expected.size(); ++i) {
        if (wide) {
          std::uint64_t bits = 0;
          const auto& value = std::get<std::vector<double>>(batches[0].columns()[0].values)[i];
          std::memcpy(&bits, &value, sizeof(bits));
          EXPECT_EQ(bits, expected[i]);
        } else {
          std::uint32_t bits = 0;
          const auto& value = std::get<std::vector<float>>(batches[0].columns()[0].values)[i];
          std::memcpy(&bits, &value, sizeof(bits));
          EXPECT_EQ(bits, expected[i]);
        }
      }
    }
}

TEST(Hdf5FormatQueryAdapter, ValueProjectionCanFollowAxesAndMemoryBudgetReducesSlabs) {
  auto source = std::make_shared<Source>(fixture({100}));
  Hdf5QueryKernelConfig config; config.contiguous_tile_elements = 16; config.max_buffer_bytes = 76;
  Hdf5FormatQueryAdapter adapter(config);
  auto fragment = query({20}, {30});
  std::reverse(fragment.projection.begin(), fragment.projection.end());
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment); ASSERT_TRUE(plan.ok());
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  std::uint64_t row = 20;
  for (;;) {
    auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    ASSERT_EQ(next.value()->row_count(), 1u);
    EXPECT_EQ(next.value()->schema().fields()[0].field_id, "hdf5.axis.0");
    EXPECT_EQ(std::get<std::vector<std::uint64_t>>(next.value()->columns()[0].values)[0], row);
    EXPECT_FLOAT_EQ(std::get<std::vector<float>>(next.value()->columns()[1].values)[0], row + .25f);
    ++row;
  }
  EXPECT_EQ(row, 30u);
  EXPECT_EQ(source->payload_reads.size(), 1u);
  config.max_buffer_bytes = 75;
  auto impossible = Hdf5FormatQueryAdapter(config).prepare(QueryPlanningContext(*source), fragment);
  ASSERT_FALSE(impossible.ok());
  EXPECT_EQ(impossible.status().code(), QueryError::ResourceExhausted);
}

TEST(Hdf5FormatQueryAdapter, NativeFilterPipelinesPreserveEndianWidthsAndPartialEdges) {
  for (unsigned filters = 1; filters <= 7; ++filters)
    for (const bool wide : {false, true}) for (const bool big : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "filters=" << filters << " wide=" << wide << " big=" << big);
      FilterFixture config; config.shape = {5,7,9}; config.chunks = {2,3,4};
      config.filters = filters; config.wide = wide; config.big = big;
      auto source = std::make_shared<Source>(filtered_fixture(config));
      QueryExecutionOptions options; options.target_batch_rows = 7;
      verify(execute(source, query(), options), {5,7,9}, {1,2,3}, {5,6,8}, wide, true);
    }
}

TEST(Hdf5FormatQueryAdapter, EveryNativePerChunkFilterMaskMatchesNativeRead) {
  for (unsigned mask = 0; mask != 8; ++mask)
    for (const bool wide : {false, true}) for (const bool big : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "mask=" << mask << " wide=" << wide << " big=" << big);
      FilterFixture config; config.skip_mask = mask; config.wide = wide; config.big = big;
      auto source = std::make_shared<Source>(filtered_fixture(config));
      verify(execute(source, query({1}, {7}, false)), {7}, {1}, {7}, wide, false);
    }
}

TEST(Hdf5FormatQueryAdapter, NativeUnfilteredPartialEdgeFlagOverridesTheRawChunkMask) {
  for (const bool wide : {false, true}) for (const bool big : {false, true}) {
    FilterFixture config; config.shape = {5,7,9}; config.chunks = {2,3,4};
    config.partial_unfiltered = true; config.wide = wide; config.big = big;
    auto source = std::make_shared<Source>(filtered_fixture(config));
    verify(execute(source, query()), {5,7,9}, {1,2,3}, {5,6,8}, wide, true);
  }
}

TEST(Hdf5FormatQueryAdapter, CompressedChunkIsReadAndDecodedOnceAcrossOutputSlabs) {
  FilterFixture fixture_config; fixture_config.shape = {128}; fixture_config.chunks = {128};
  auto source = std::make_shared<Source>(filtered_fixture(fixture_config));
  Hdf5FragmentMetrics metrics;
  Hdf5QueryKernelConfig config;
  config.metrics_observer = [&](const Hdf5FragmentMetrics& current) { metrics = current; };
  Hdf5FormatQueryAdapter adapter(config);
  auto plan = adapter.prepare(QueryPlanningContext(*source), query({7}, {119}, false));
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  QueryExecutionOptions options; options.target_batch_rows = 3;
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), options);
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  std::vector<QueryBatch> batches;
  for (;;) {
    auto next = cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message();
    if (!next.value()) break;
    batches.push_back(std::move(*next.value()));
  }
  verify(batches, {128}, {7}, {119}, false, false);
  ASSERT_EQ(source->payload_reads.size(), 1u);
  EXPECT_EQ(metrics.payload_ranges, 1u);
  EXPECT_EQ(metrics.payload_bytes, source->payload_reads[0].second);
  EXPECT_EQ(metrics.decode_count, 1u);
  EXPECT_EQ(metrics.decoded_bytes, 128u * sizeof(float));
  EXPECT_EQ(metrics.inflated_bytes, 128u * sizeof(float));
  EXPECT_EQ(metrics.useful_bytes, 112u * sizeof(float));
  EXPECT_GT(metrics.cache_hits, 0u);
}

TEST(Hdf5FormatQueryAdapter, CorruptChecksumTruncatedDeflateAndTrailingDataYieldNoPartialSlab) {
  for (unsigned fault = 0; fault != 3; ++fault) {
    SCOPED_TRACE(fault);
    FilterFixture config; config.shape = {256}; config.chunks = {128};
    config.filters = fault == 0 ? 4 : 2;
    auto bytes = mutate_first_encoded_chunk(filtered_fixture(config), [fault](BinaryValue& encoded) {
      if (fault == 0) encoded.back() ^= 1;
      if (fault == 1) encoded.pop_back();
      if (fault == 2) encoded.push_back(0xa5);
    }, 1);
    auto source = std::make_shared<Source>(std::move(bytes));
    Hdf5FormatQueryAdapter adapter;
    auto plan = adapter.prepare(QueryPlanningContext(*source), query({0}, {256}, false));
    ASSERT_TRUE(plan.ok()) << plan.status().message();
    auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    auto next = cursor.value()->next();
    EXPECT_FALSE(next.ok());
    const auto reads = source->payload_reads.size();
    EXPECT_FALSE(cursor.value()->next().ok());
    EXPECT_EQ(source->payload_reads.size(), reads);
  }
}

TEST(Hdf5FormatQueryAdapter, DeterministicDefaultAndExplicitFillAreValuesWithoutNullMask) {
  for (const bool chunked : {false, true}) for (const bool wide : {false, true})
    for (const bool big : {false, true}) for (int fill : {0,1,3,4}) {
      SCOPED_TRACE(::testing::Message() << "chunked=" << chunked << " wide=" << wide << " big=" << big << " fill=" << fill);
      FilterFixture config; config.writes = 1; config.fill = fill; config.wide = wide; config.big = big;
      if (!chunked) { config.chunks = {}; config.filters = 0; }
      auto source = std::make_shared<Source>(filtered_fixture(config));
      auto batches = execute(source, query({1}, {7}, false));
      ASSERT_EQ(batches.size(), 1u);
      const auto& column = batches[0].columns()[0];
      EXPECT_TRUE(column.validity.empty());
      ASSERT_EQ(batches[0].row_count(), 6u);
      for (std::size_t i = 0; i < batches[0].row_count(); ++i) {
        double value = wide ? std::get<std::vector<double>>(column.values)[i] : std::get<std::vector<float>>(column.values)[i];
        if (fill == 3) EXPECT_TRUE(std::isnan(value));
        else if (fill == 1) EXPECT_EQ(value, 13.5);
        else { EXPECT_EQ(value, 0.); EXPECT_EQ(std::signbit(value), fill == 4); }
      }
      EXPECT_TRUE(source->payload_reads.empty());
    }
}

TEST(Hdf5FormatQueryAdapter, SparseChunkedDatasetCombinesAllocatedDataAndExplicitFill) {
  FilterFixture config; config.writes = 2; config.fill = 1; config.wide = true; config.big = true;
  auto source = std::make_shared<Source>(filtered_fixture(config));
  auto batches = execute(source, query({0}, {7}, false));
  ASSERT_EQ(batches.size(), 1u);
  EXPECT_EQ(std::get<std::vector<double>>(batches[0].columns()[0].values),
    (std::vector<double>{.25,1.25,2.25,3.25,13.5,13.5,13.5}));
  EXPECT_TRUE(batches[0].columns()[0].validity.empty());
  EXPECT_EQ(source->payload_reads.size(), 1u);
}

TEST(Hdf5FormatQueryAdapter, UndefinedAndNeverFillRejectOnlySelectionsThatNeedMissingData) {
  for (const bool chunked : {false, true}) for (const bool never : {false, true}) {
    SCOPED_TRACE(::testing::Message() << "chunked=" << chunked << " never=" << never);
    FilterFixture config; config.writes = chunked ? 2 : 1;
    config.fill = never ? 1 : 2;
    if (never) config.fill_time = H5D_FILL_TIME_NEVER;
    if (!chunked) { config.chunks = {}; config.filters = 0; }
    auto source = std::make_shared<Source>(filtered_fixture(config));
    Hdf5FormatQueryAdapter adapter;
    auto missing = adapter.prepare(QueryPlanningContext(*source), query({4}, {7}, false));
    EXPECT_FALSE(missing.ok());
    if (!missing.ok()) EXPECT_EQ(missing.status().code(), QueryError::UnsupportedCapability);
    EXPECT_TRUE(source->payload_reads.empty());
    if (chunked) verify(execute(source, query({0}, {4}, false)), {7}, {0}, {4}, false, false);
  }
}

TEST(Hdf5FormatQueryAdapter, SizeReservationsRejectBeforePayloadAndCodecScratchIsBounded) {
  FilterFixture fixture_config; fixture_config.shape = {1024}; fixture_config.chunks = {1024};
  for (unsigned limit = 0; limit < 4; ++limit) {
    auto source = std::make_shared<Source>(filtered_fixture(fixture_config));
    Hdf5QueryKernelConfig config;
    if (limit == 0) config.max_decoded_chunk_bytes = 4095;
    if (limit == 1) config.max_encoded_chunk_bytes = 1;
    if (limit == 2) config.max_zlib_scratch_bytes = 1;
    if (limit == 3) config.max_buffer_bytes = 4096;
    Hdf5FormatQueryAdapter adapter(config);
    auto plan = adapter.prepare(QueryPlanningContext(*source), query({0}, {1}, false));
    if (plan.ok()) {
      auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
      if (cursor.ok()) EXPECT_FALSE(cursor.value()->next().ok()) << "limit=" << limit;
    }
    // A tiny codec allocator may fail only when zlib initializes, after its
    // encoded chunk is read. Size and total reservation failures are pre-I/O.
    if (limit == 2) EXPECT_LE(source->payload_reads.size(), 1u);
    else EXPECT_TRUE(source->payload_reads.empty()) << "limit=" << limit;
  }
}

TEST(Hdf5FormatQueryAdapter, UnknownOptionalFilterRemainsExplicitlyUnsupportedEvenWhenSkipped) {
  FilterFixture config; config.filters = 8;
  auto source = std::make_shared<Source>(filtered_fixture(config));
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query({0}, {7}, false));
  ASSERT_FALSE(plan.ok());
  EXPECT_EQ(plan.status().code(), QueryError::UnsupportedCapability);
  EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, DeflateExpansionBeyondDeclaredChunkCannotPublishValues) {
  FilterFixture large; large.shape = {4096}; large.chunks = {4096}; large.filters = 2;
  BinaryValue oversized_encoded;
  (void)mutate_first_encoded_chunk(filtered_fixture(large),
    [&](BinaryValue& bytes) { oversized_encoded = bytes; });
  FilterFixture small; small.shape = {16}; small.chunks = {16}; small.filters = 2;
  auto bytes = mutate_first_encoded_chunk(filtered_fixture(small),
    [&](BinaryValue& encoded) { encoded = oversized_encoded; });
  auto source = std::make_shared<Source>(std::move(bytes));
  Hdf5FormatQueryAdapter adapter;
  auto plan = adapter.prepare(QueryPlanningContext(*source), query({0}, {1}, false));
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  EXPECT_FALSE(cursor.value()->next().ok());
  ASSERT_EQ(source->payload_reads.size(), 1u);
  EXPECT_FALSE(cursor.value()->next().ok());
  EXPECT_EQ(source->payload_reads.size(), 1u);
}

namespace {
void cf_text(hid_t object, const char* name, const std::string& text) {
  Id type(H5Tcopy(H5T_C_S1),H5Tclose); check(H5Tset_size(type,text.size()+1));
  Id space(H5Screate(H5S_SCALAR),H5Sclose);
  Id attr(H5Acreate2(object,name,type,space,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
  check(H5Awrite(attr,type,text.c_str()));
}
void cf_number(hid_t object, const char* name, double value) {
  const hsize_t one=1;
  Id space(H5Screate_simple(1,&one,nullptr),H5Sclose);
  Id attr(H5Acreate2(object,name,H5T_IEEE_F64LE,space,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
  check(H5Awrite(attr,H5T_NATIVE_DOUBLE,&value));
}
BinaryValue packed_cf_fixture(bool big, bool filtered, LogicalTypeId value_type = LogicalTypeId::Int32) {
  Temporary temporary;
  {
    Id access(H5Pcreate(H5P_FILE_ACCESS),H5Pclose); check(H5Pset_fapl_sec2(access));
    Id file(H5Fcreate(temporary.path.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,access),H5Fclose);
    cf_text(file,"Conventions","CF-1.6");
    const hsize_t shape[]{1,1,3,4};
    Id space(H5Screate_simple(4,shape,nullptr),H5Sclose);
    Id props(H5Pcreate(H5P_DATASET_CREATE),H5Pclose);
    if(filtered) { check(H5Pset_chunk(props,4,shape)); check(H5Pset_shuffle(props)); check(H5Pset_deflate(props,1)); check(H5Pset_fletcher32(props)); }
    const std::int16_t fill=-999;
    check(H5Pset_fill_value(props,H5T_NATIVE_SHORT,&fill));
    const hid_t data_type = value_type == LogicalTypeId::Float32 ? (big ? H5T_IEEE_F32BE : H5T_IEEE_F32LE) :
      value_type == LogicalTypeId::Float64 ? (big ? H5T_IEEE_F64BE : H5T_IEEE_F64LE) : (big ? H5T_STD_I16BE : H5T_STD_I16LE);
    Id data(H5Dcreate2(file,"/a",data_type,space,H5P_DEFAULT,props,H5P_DEFAULT),H5Dclose);
    const std::int16_t values[]{-999,-2,0,2,4,6,8,10,12,14,16,18};
    check(H5Dwrite(data,H5T_NATIVE_SHORT,H5S_ALL,H5S_ALL,H5P_DEFAULT,values));
    cf_text(data,"units","degree_C"); cf_text(data,"standard_name","sea_surface_temperature");
    cf_number(data,"scale_factor",.5); cf_number(data,"add_offset",10);
    cf_number(data,"_FillValue",-999); cf_number(data,"valid_min",-2); cf_number(data,"valid_max",14);
    hobj_ref_t refs[2]{};
    for(unsigned i=0;i<2;++i) {
      const char* path=i?"/lon":"/lat";
      hsize_t count=i?4:3; Id axis_space(H5Screate_simple(1,&count,nullptr),H5Sclose);
      Id axis(H5Dcreate2(file,path,H5T_IEEE_F64LE,axis_space,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT),H5Dclose);
      const double lat[]{2,1,0},lon[]{0,90,180,270};
      check(H5Dwrite(axis,H5T_NATIVE_DOUBLE,H5S_ALL,H5S_ALL,H5P_DEFAULT,i?lon:lat));
      cf_text(axis,"CLASS","DIMENSION_SCALE"); cf_text(axis,"units",i?"degrees_east":"degrees_north");
      cf_text(axis,"standard_name",i?"longitude":"latitude");
      check(H5Rcreate(&refs[i],file,path,H5R_OBJECT,H5I_INVALID_HID));
    }
    Id refs_type(H5Tvlen_create(H5T_STD_REF_OBJ),H5Tclose);
    hvl_t lists[4]{{0,nullptr},{0,nullptr},{1,&refs[0]},{1,&refs[1]}};
    const hsize_t rank=4; Id ref_space(H5Screate_simple(1,&rank,nullptr),H5Sclose);
    Id attr(H5Acreate2(data,"DIMENSION_LIST",refs_type,ref_space,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
    check(H5Awrite(attr,refs_type,lists));
  }
  std::ifstream input(temporary.path,std::ios::binary); return {std::istreambuf_iterator<char>(input),{}};
}
}

TEST(Hdf5FormatQueryAdapter, DatasetAttributeMismatchSkipsChunkPagesAndPayload) {
  auto source=std::make_shared<Source>(packed_cf_fixture(false,true));
  Hdf5FormatQueryAdapter adapter;
  auto f=query({0,0,0,0},{1,1,3,4},false);
  f.predicate=make_comparison(make_hdf5_attribute_field("standard_name").value(),ComparisonOperator::Equal,LogicalValue::utf8("air_temperature"));
  auto plan=adapter.prepare(QueryPlanningContext(*source),f); ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto directory=read_hdf5_layout_directory([&](auto o,auto n){return Source::read(source->layout,o,n);},source->layout.size(),source->payload.size());
  ASSERT_TRUE(directory.ok());
  for(const auto& read:source->layout_reads) for(const auto& page:directory.value().pages())
    EXPECT_FALSE(read.first<page.offset+page.length && page.offset<read.first+read.second);
  auto cursor=adapter.execute(QueryExecutionContext(source),plan.value().implementation(),{}); ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto next=cursor.value()->next(); ASSERT_TRUE(next.ok()); EXPECT_FALSE(next.value());
  EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, DatasetAttributeMissingAndUnreadableAreDifferent) {
  auto source=std::make_shared<Source>(packed_cf_fixture(false,false));
  Hdf5FormatQueryAdapter adapter;
  auto f=query({0,0,0,0},{1,1,3,4},false);
  f.predicate=make_is_null(make_hdf5_attribute_field("absent").value());
  auto plan=adapter.prepare(QueryPlanningContext(*source),f); ASSERT_TRUE(plan.ok()) << plan.status().message();
  f.predicate=make_is_null(make_hdf5_attribute_field("DIMENSION_LIST").value());
  plan=adapter.prepare(QueryPlanningContext(*source),f); ASSERT_FALSE(plan.ok());
  EXPECT_EQ(plan.status().code(),QueryError::UnsupportedCapability);
  EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, PackedInt16PhysicalViewPreservesRawSignedValues) {
  for(bool big:{false,true}) for(bool filtered:{false,true}) {
    auto source=std::make_shared<Source>(packed_cf_fixture(big,filtered));
    QueryExecutionOptions options; options.target_batch_rows=5;
    auto batches=execute(source,query({0,0,0,0},{1,1,3,4},true),options);
    std::vector<std::int32_t> values;
    for(const auto& batch:batches) {
      EXPECT_EQ(batch.columns()[0].type.id,LogicalTypeId::Int32);
      EXPECT_TRUE(batch.columns()[0].validity.empty());
      const auto& part=std::get<std::vector<std::int32_t>>(batch.columns()[0].values);
      values.insert(values.end(),part.begin(),part.end());
    }
    EXPECT_EQ(values,(std::vector<std::int32_t>{-999,-2,0,2,4,6,8,10,12,14,16,18}));
  }
}

TEST(Hdf5FormatQueryAdapter, ScientificPackedViewMasksRawDomainAndReusesRawChunks) {
#ifdef __APPLE__
  GTEST_SKIP() << "macOS cannot enforce the helper RLIMIT_AS; bounded CF forward-reference ingestion is Linux-only";
#endif
  auto source=std::make_shared<Source>(packed_cf_fixture(true,true));
  auto runtime=std::make_shared<QueryRuntime>(); Hdf5FormatQueryAdapter adapter({},runtime);
  auto physical=query({0,0,0,0},{1,1,3,4},false);
  auto first=adapter.prepare(QueryPlanningContext(*source),physical); ASSERT_TRUE(first.ok()) << first.status().message();
  auto raw=adapter.execute(QueryExecutionContext(source),first.value().implementation(),{}); ASSERT_TRUE(raw.ok());
  ASSERT_TRUE(raw.value()->next().ok()); ASSERT_TRUE(raw.value()->next().ok());
  const auto reads=source->payload_reads.size(); ASSERT_GT(reads,0u);
  auto science=physical; science.projection={make_hdf5_science_value_field("/a").value(),{kHdf5LatitudeField},{kHdf5LongitudeField}};
  auto plan=adapter.prepare(QueryPlanningContext(*source),science); ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto cursor=adapter.execute(QueryExecutionContext(source),plan.value().implementation(),{}); ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto next=cursor.value()->next(); ASSERT_TRUE(next.ok()) << next.status().message(); ASSERT_TRUE(next.value());
  const auto& batch=*next.value(); ASSERT_EQ(batch.row_count(),12u);
  const auto& values=std::get<std::vector<double>>(batch.columns()[0].values);
  const auto& mask=batch.columns()[0].validity;
  for(std::size_t i=0;i<12;++i) {
    const bool valid=i>0 && i<10; EXPECT_EQ(bool(mask[i/8]&(1U<<(i%8))),valid);
    if(valid) EXPECT_DOUBLE_EQ(values[i],8+static_cast<double>(i));
    EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(batch.columns()[1].values)[i],2-i/4);
    EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(batch.columns()[2].values)[i],90*(i%4));
  }
  EXPECT_EQ(source->payload_reads.size(),reads);
  auto repeat=adapter.execute(QueryExecutionContext(source),first.value().implementation(),{}); ASSERT_TRUE(repeat.ok());
  auto untouched=repeat.value()->next(); ASSERT_TRUE(untouched.ok()); ASSERT_TRUE(untouched.value());
  EXPECT_EQ(std::get<std::vector<std::int32_t>>(untouched.value()->columns()[0].values)[0],-999);
  EXPECT_EQ(source->payload_reads.size(),reads);
  const auto& metadata=plan.value().implementation()->result_metadata();
  EXPECT_NE(std::find(metadata.begin(),metadata.end(),std::make_pair(std::string("transform.mask_domain"),std::string("raw"))),metadata.end());
}

TEST(Hdf5FormatQueryAdapter, NativeEngineReadsRealSourceRoiAcrossLayoutsAndAliases) {
  for (bool wide : {false, true}) for (bool big : {false, true})
    for (bool chunked : {false, true}) {
      auto source = std::make_shared<Source>(fixture({5,7,9},
        chunked ? std::vector<hsize_t>{2,3,4} : std::vector<hsize_t>{},
        wide, big, chunked, chunked));
      QueryExecutionOptions options; options.target_batch_rows = 13;
      auto batches = execute(source, query({1,2,3},{5,6,8},true,"/alias"),
        options, Hdf5QueryMode::NativeHdf5);
      verify(batches, {5,7,9}, {1,2,3}, {5,6,8}, wide, true);
      EXPECT_TRUE(source->payload_reads.empty());
      EXPECT_GT(source->native_reads, 0u);
    }
}

TEST(Hdf5FormatQueryAdapter, NativeEngineAlignedSlabsReadEachChunkOnceAndMovePhysicalValues) {
  for (bool wide : {false,true}) {
    auto source = std::make_shared<Source>(fixture({96,64},{32,32},wide,true,true,true));
    QueryExecutionOptions options; options.target_batch_rows = 48*64;
    options.target_batch_bytes = options.target_batch_rows * (wide ? 24 : 20);
    QueryProfileRecord record; ScopedQueryProfile scope(&record);
    auto batches = execute(source,query({0,0},{96,64},true),options,Hdf5QueryMode::NativeHdf5);
    verify(batches,{96,64},{0,0},{96,64},wide,true);
    ASSERT_EQ(batches.size(),3u);
    for (const auto& batch : batches) {
      ASSERT_TRUE(batch.domain()); EXPECT_EQ(batch.domain()->shape,(std::vector<std::uint64_t>{32,64}));
      EXPECT_LE(batch.row_count(),options.target_batch_rows);
      EXPECT_LE(batch.row_count() * (wide ? 24 : 20),options.target_batch_bytes);
      EXPECT_TRUE(batch.columns()[0].validity.empty());
      ASSERT_EQ(batch.sources().size(),1u);
      EXPECT_EQ(batch.sources()[0].path,source->manifest_.key.path);
      EXPECT_EQ(batch.sources()[0].version,source->manifest_.key.version);
    }
    EXPECT_EQ(record.counters["hdf5.native.chunk_aligned.cursors"],1u);
    EXPECT_EQ(record.counters["hdf5.native.selected_chunk.visits"],6u);
    EXPECT_EQ(record.counters["hdf5.native.output.moved.bytes"],96*64*(wide ? 8u : 4u));
    EXPECT_EQ(record.counters["hdf5.native.output.copied.bytes"],0u);
    EXPECT_EQ(record.counters["hdf5.native.filesystem.bytes"],source->logical_payload_bytes);
    EXPECT_GT(source->logical_payload_bytes,0u); EXPECT_TRUE(source->payload_reads.empty());
  }
}

TEST(Hdf5FormatQueryAdapter, NativeAlignedPhysicalSlabsKeepCancellationAndCumulativeBudgetSticky) {
  for (bool cancel : {false,true}) {
    auto source = std::make_shared<Source>(fixture({96,64},{32,32},false,false,true,true));
    Hdf5FormatQueryAdapter native({}, {}, {}, {}, {}, {}, Hdf5QueryMode::NativeHdf5);
    auto plan = native.prepare(QueryPlanningContext(*source),query({5,0},{96,64},false));
    ASSERT_TRUE(plan.ok()) << plan.status().message();
    QueryExecutionOptions options; options.target_batch_rows = 48*64;
    auto cursor = native.execute(QueryExecutionContext(source),plan.value().implementation(),options);
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    auto first = cursor.value()->next(); ASSERT_TRUE(first.ok()); ASSERT_TRUE(first.value());
    EXPECT_EQ(first.value()->domain()->shape,(std::vector<std::uint64_t>{27,64}));
    const auto calls = source->native_reads;
    if (cancel) cursor.value()->cancel();
    else source->logical_payload_limit = source->logical_payload_bytes;
    auto stopped = cursor.value()->next(); ASSERT_FALSE(stopped.ok());
    EXPECT_EQ(stopped.status().code(),cancel ? QueryError::Cancelled : QueryError::ResourceExhausted);
    auto repeated = cursor.value()->next(); ASSERT_FALSE(repeated.ok());
    EXPECT_EQ(repeated.status().code(),stopped.status().code());
    EXPECT_EQ(source->native_reads,calls); EXPECT_TRUE(source->payload_reads.empty());
    // The returned batch owns its values even after cursor failure/cleanup.
    const auto& values = std::get<std::vector<float>>(first.value()->columns()[0].values);
    ASSERT_EQ(values.size(),27*64u); EXPECT_EQ(values.front(),5*64+.25f);
    EXPECT_EQ(values.back(),32*64-1+.25f);
  }
}

TEST(Hdf5FormatQueryAdapter, NativeEngineDoesNotReadEvenCorruptIndexedChunkPages) {
  auto source = std::make_shared<Source>(fixture({5,7,9},{2,3,4},false,false,true,true));
  auto directory = read_hdf5_layout_directory([&](auto o,auto n){return Source::read(source->layout,o,n);},
    source->layout.size(), source->payload.size());
  ASSERT_TRUE(directory.ok()); ASSERT_FALSE(directory.value().pages().empty());
  for (const auto& page : directory.value().pages()) source->layout[page.offset] ^= 0x7f;
  source->layout_reads.clear();
  auto native = execute(source, query(), {}, Hdf5QueryMode::NativeHdf5);
  verify(native, {5,7,9}, {1,2,3}, {5,6,8}, false, true);
  for (const auto& read : source->layout_reads) for (const auto& page : directory.value().pages())
    EXPECT_FALSE(read.first < page.offset + page.length && page.offset < read.first + read.second);
  Hdf5FormatQueryAdapter indexed;
  auto failed = indexed.prepare(QueryPlanningContext(*source), query());
  EXPECT_FALSE(failed.ok());
}

TEST(Hdf5FormatQueryAdapter, NativeEngineDoesNotRequireAnExportedChunkIndex) {
  Hdf5LayoutLimits limits; limits.max_chunks=1;
  auto source=std::make_shared<Source>(fixture({5,7,9},{2,3,4}),limits);
  Hdf5FormatQueryAdapter indexed;
  auto plan=indexed.prepare(QueryPlanningContext(*source),query());
  ASSERT_FALSE(plan.ok()); EXPECT_EQ(plan.status().code(),QueryError::LayoutUnavailable);
  auto native=execute(source,query(),{},Hdf5QueryMode::NativeHdf5);
  verify(native,{5,7,9},{1,2,3},{5,6,8},false,true);
}

TEST(Hdf5FormatQueryAdapter, NativeEnginePreservesSpecialFloatBitsAndSparseFill) {
  for (bool wide : {false,true}) for (bool big : {false,true}) {
    auto source=std::make_shared<Source>(fixture({6},{3},wide,big,false,true,false,true));
    const auto fragment=query({0},{6},false);
    const auto indexed=execute(source,fragment);
    const auto native=execute(source,fragment,{},Hdf5QueryMode::NativeHdf5);
    ASSERT_EQ(indexed.size(),1u); ASSERT_EQ(native.size(),1u);
    if (wide) {
      const auto& a=std::get<std::vector<double>>(indexed[0].columns()[0].values);
      const auto& b=std::get<std::vector<double>>(native[0].columns()[0].values);
      ASSERT_EQ(a.size(),b.size()); EXPECT_EQ(std::memcmp(a.data(),b.data(),a.size()*sizeof(double)),0);
    } else {
      const auto& a=std::get<std::vector<float>>(indexed[0].columns()[0].values);
      const auto& b=std::get<std::vector<float>>(native[0].columns()[0].values);
      ASSERT_EQ(a.size(),b.size()); EXPECT_EQ(std::memcmp(a.data(),b.data(),a.size()*sizeof(float)),0);
    }
  }
  auto sparse=std::make_shared<Source>(fixture({5,7,9},{2,3,4},false,false,false,false,true));
  for (const auto& batch : execute(sparse,query(),{},Hdf5QueryMode::NativeHdf5)) {
    EXPECT_TRUE(batch.columns()[0].validity.empty());
    for (float value : std::get<std::vector<float>>(batch.columns()[0].values)) EXPECT_EQ(value,0);
  }
}

TEST(Hdf5FormatQueryAdapter, NativeEnginePreservesPackedPhysicalAndScientificOutputs) {
#ifdef __APPLE__
  GTEST_SKIP() << "CF helper resource boundary requires Linux";
#endif
  for (auto type : {LogicalTypeId::Int32,LogicalTypeId::Float32,LogicalTypeId::Float64})
    for (bool big : {false,true}) for (bool filtered : {false,true}) {
    auto source = std::make_shared<Source>(packed_cf_fixture(big,filtered,type));
    for (bool scientific : {false,true}) {
      auto fragment = query({0,0,0,0},{1,1,3,4},true);
      if (scientific) fragment.projection = {make_hdf5_science_value_field("/a").value(),
        {kHdf5LatitudeField},{kHdf5LongitudeField},hdf5_axis_field(3)};
      QueryExecutionOptions options; options.target_batch_rows = 5;
      const auto indexed = execute(source, fragment, options);
      QueryProfileRecord record; ScopedQueryProfile scope(&record);
      const auto native = execute(source, fragment, options, Hdf5QueryMode::NativeHdf5);
      EXPECT_EQ(record.counters["hdf5.native.output.moved.bytes"],
        scientific ? 0u : 12u * (type == LogicalTypeId::Float64 ? 8u : 4u));
      EXPECT_EQ(record.counters["hdf5.native.output.copied.bytes"],
        scientific ? 12u * (type == LogicalTypeId::Int32 ? 2u : type == LogicalTypeId::Float32 ? 4u : 8u) : 0u);
      ASSERT_EQ(indexed.size(),native.size());
      for (std::size_t i=0;i<native.size();++i) {
        ASSERT_EQ(indexed[i].row_count(),native[i].row_count());
        ASSERT_EQ(indexed[i].columns().size(),native[i].columns().size());
        for (std::size_t j=0;j<native[i].columns().size();++j) {
          const auto& expected=indexed[i].columns()[j];
          const auto& actual=native[i].columns()[j];
          EXPECT_EQ(expected.type.id,actual.type.id);
          EXPECT_EQ(expected.validity,actual.validity);
          if (actual.type.id==LogicalTypeId::Float64)
            EXPECT_EQ(std::get<std::vector<double>>(expected.values),std::get<std::vector<double>>(actual.values));
          else if (actual.type.id==LogicalTypeId::Float32)
            EXPECT_EQ(std::get<std::vector<float>>(expected.values),std::get<std::vector<float>>(actual.values));
          else if (actual.type.id==LogicalTypeId::Int32)
            EXPECT_EQ(std::get<std::vector<std::int32_t>>(expected.values),std::get<std::vector<std::int32_t>>(actual.values));
          else
            EXPECT_EQ(std::get<std::vector<std::uint64_t>>(expected.values),std::get<std::vector<std::uint64_t>>(actual.values));
        }
      }
    }
  }
}

TEST(Hdf5FormatQueryAdapter, NativeEngineRejectsOtherEnginePlansAndReportsEngine) {
  auto source=std::make_shared<Source>();
  Hdf5FormatQueryAdapter native({}, {}, {}, {}, {}, {}, Hdf5QueryMode::NativeHdf5), indexed;
  QueryPlanningContext context(*source);
  auto plan=native.prepare(context,query()); ASSERT_TRUE(plan.ok()) << plan.status().message();
  const auto& metadata=plan.value().implementation()->result_metadata();
  EXPECT_NE(std::find(metadata.begin(),metadata.end(),std::make_pair(std::string("hdf5.query_mode"),
    std::string("native-hdf5"))),metadata.end());
  auto wrong=indexed.execute(QueryExecutionContext(source),plan.value().implementation(),{});
  ASSERT_FALSE(wrong.ok()); EXPECT_EQ(wrong.status().code(),QueryError::StalePlan);
  auto replan=indexed.prepare(context,query()); ASSERT_FALSE(replan.ok());
  EXPECT_EQ(replan.status().code(),QueryError::StalePlan);
  QueryProfileRecord record; ScopedQueryProfile scope(&record);
  auto values=execute(source,query(),{},Hdf5QueryMode::NativeHdf5);
  EXPECT_FALSE(values.empty());
  EXPECT_GT(record.counters["hdf5.native.source.calls"],0u);
  EXPECT_EQ(record.counters["hdf5.native.filesystem.opens"],1u);
  EXPECT_EQ(record.counters["hdf5.native.filesystem.calls"],
    record.counters["hdf5.native.source.calls"]);
  EXPECT_EQ(record.counters["hdf5.native.filesystem.bytes"],source->logical_payload_bytes);
  EXPECT_TRUE(source->payload_reads.empty());
  EXPECT_GT(record.counters["hdf5.native.slabs"],0u);
  EXPECT_EQ(record.counters["hdf5.selection.decoder_compiles"],0u);
  EXPECT_GT(record.nanoseconds["hdf5.native.read"],0u);
}

TEST(Hdf5FormatQueryAdapter, NativeEngineRetainsExactGwoscGpsBindingAndOrdinalDomain) {
  Temporary temporary;
  constexpr double start=1126259446.,spacing=1./4096;
  {
    Id access(H5Pcreate(H5P_FILE_ACCESS),H5Pclose);check(H5Pset_fapl_sec2(access));
    Id file(H5Fcreate(temporary.path.c_str(),H5F_ACC_TRUNC,H5P_DEFAULT,access),H5Fclose);
    const hsize_t count=128;
    Id space(H5Screate_simple(1,&count,nullptr),H5Sclose);
    Id data(H5Dcreate2(file,"/a",H5T_IEEE_F64LE,space,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT),H5Dclose);
    std::vector<double> values(count);std::iota(values.begin(),values.end(),.25);
    check(H5Dwrite(data,H5T_NATIVE_DOUBLE,H5S_ALL,H5S_ALL,H5P_DEFAULT,values.data()));
    Id scalar(H5Screate(H5S_SCALAR),H5Sclose);
    {
      Id attr(H5Acreate2(data,"Xspacing",H5T_IEEE_F64LE,scalar,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
      check(H5Awrite(attr,H5T_NATIVE_DOUBLE,&spacing));
    }
    {
      const std::int64_t first=1126259446;
      Id attr(H5Acreate2(data,"Xstart",H5T_STD_I64LE,scalar,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
      check(H5Awrite(attr,H5T_NATIVE_INT64,&first));
    }
    const std::int64_t points=128;
    Id attr(H5Acreate2(data,"Npoints",H5T_STD_I64LE,scalar,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
    check(H5Awrite(attr,H5T_NATIVE_INT64,&points));
    cf_text(data,"Xunits","second");cf_text(data,"Xlabel","GPS time");
  }
  std::ifstream input(temporary.path,std::ios::binary);
  auto source=std::make_shared<Source>(BinaryValue(std::istreambuf_iterator<char>(input),{}));
  Hdf5SemanticConfig semantics;semantics.gwosc_bindings.push_back({source->manifest_.key,"/a",source->storage_namespace()});
  Hdf5FormatQueryAdapter native({}, {}, semantics, {}, {}, {}, Hdf5QueryMode::NativeHdf5);
  QueryFragment fragment;
  fragment.projection={{kHdf5GpsSecondsField},make_hdf5_value_field("/a").value(),hdf5_axis_field(0)};
  fragment.slices={{{kHdf5GpsSecondsField},DomainBound{LogicalValue::float64(start+3*spacing),BoundKind::Inclusive},
    DomainBound{LogicalValue::float64(start+17*spacing),BoundKind::Exclusive}}};
  auto plan=native.prepare(QueryPlanningContext(*source),fragment);ASSERT_TRUE(plan.ok()) << plan.status().message();
  QueryExecutionOptions options;options.target_batch_rows=3;
  auto cursor=native.execute(QueryExecutionContext(source),plan.value().implementation(),options);ASSERT_TRUE(cursor.ok());
  std::uint64_t ordinal=3;
  for (;;) {
    auto next=cursor.value()->next();ASSERT_TRUE(next.ok()) << next.status().message();if (!next.value()) break;
    const auto& batch=*next.value();ASSERT_TRUE(batch.domain());EXPECT_EQ(batch.domain()->origin[0],ordinal);
    for (std::uint64_t i=0;i<batch.row_count();++i,++ordinal) {
      EXPECT_EQ(std::get<std::vector<double>>(batch.columns()[0].values)[i],start+ordinal*spacing);
      EXPECT_EQ(std::get<std::vector<double>>(batch.columns()[1].values)[i],ordinal+.25);
      EXPECT_EQ(std::get<std::vector<std::uint64_t>>(batch.columns()[2].values)[i],ordinal);
    }
  }
  EXPECT_EQ(ordinal,17u);
}

TEST(Hdf5FormatQueryAdapter, NativeEngineEmptyPredicateReadsNoPayload) {
  auto source=std::make_shared<Source>(packed_cf_fixture(false,true));
  auto fragment=query({0,0,0,0},{1,1,3,4},false);
  fragment.predicate=make_comparison(make_hdf5_attribute_field("standard_name").value(),
    ComparisonOperator::Equal,LogicalValue::utf8("air_temperature"));
  auto result=execute(source,fragment,{},Hdf5QueryMode::NativeHdf5);
  EXPECT_TRUE(result.empty()); EXPECT_TRUE(source->payload_reads.empty());
}

TEST(Hdf5FormatQueryAdapter, NativeEngineBudgetAndFailureNeverFallBackToIndexed) {
  auto source=std::make_shared<Source>();
  Hdf5FormatQueryAdapter native({}, {}, {}, {}, {}, {}, Hdf5QueryMode::NativeHdf5);
  auto plan=native.prepare(QueryPlanningContext(*source),query()); ASSERT_TRUE(plan.ok());
  QueryExecutionOptions options; options.max_payload_bytes=1;
  auto budget=native.execute(QueryExecutionContext(source),plan.value().implementation(),options);
  ASSERT_FALSE(budget.ok()); EXPECT_EQ(budget.status().code(),QueryError::ResourceExhausted);
  EXPECT_TRUE(source->payload_reads.empty());
  source->fail=true;
  auto failed=native.execute(QueryExecutionContext(source),plan.value().implementation(),{});
  ASSERT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(),QueryError::ReadFailure);
  source->fail=false;
  source->payload[512]^=0xff; // Actual superblock follows the user block.
  auto corrupt=native.execute(QueryExecutionContext(source),plan.value().implementation(),{});
  EXPECT_FALSE(corrupt.ok());
}

TEST(Hdf5FormatQueryAdapter, NativeEngineMissingFileBindingNeverReadsDatabasePayload) {
  auto source = std::make_shared<Source>();
  source->missing_native_file = true;
  Hdf5FormatQueryAdapter native({}, {}, {}, {}, {}, {}, Hdf5QueryMode::NativeHdf5);
  auto plan = native.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto result = native.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::SourceNotFound);
  EXPECT_TRUE(source->payload_reads.empty());
  EXPECT_EQ(source->native_reads, 0u);
}

TEST(Hdf5FormatQueryAdapter, NativeEngineLocalReadsRetainAggregateRequestBudget) {
  auto source = std::make_shared<Source>();
  source->logical_payload_limit = 1;
  Hdf5FormatQueryAdapter native({}, {}, {}, {}, {}, {}, Hdf5QueryMode::NativeHdf5);
  auto plan = native.prepare(QueryPlanningContext(*source), query());
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  auto result = native.execute(QueryExecutionContext(source), plan.value().implementation(), {});
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(source->payload_reads.empty());
  EXPECT_LE(source->logical_payload_bytes, source->logical_payload_limit);
}

TEST(Hdf5FormatQueryAdapter, NativeEngineCancellationAndDescriptorMismatchAreExplicit) {
  auto source=std::make_shared<Source>();
  Hdf5FormatQueryAdapter native({}, {}, {}, {}, {}, {}, Hdf5QueryMode::NativeHdf5);
  auto plan=native.prepare(QueryPlanningContext(*source),query()); ASSERT_TRUE(plan.ok());
  auto opened=native.execute(QueryExecutionContext(source),plan.value().implementation(),{});
  ASSERT_TRUE(opened.ok()) << opened.status().message();
  const auto reads=source->native_reads;
  opened.value()->cancel();
  auto stopped=opened.value()->next(); ASSERT_FALSE(stopped.ok());
  EXPECT_EQ(stopped.status().code(),QueryError::Cancelled);
  EXPECT_EQ(source->native_reads,reads);
  EXPECT_TRUE(source->payload_reads.empty());
  opened.value().reset();
  // Keep the sealed layout and publication identity; change the native bytes.
  // The native engine must detect disagreement instead of trusting the layout.
  auto replacement=fixture({3,5,7});
  ASSERT_LE(replacement.size(),source->payload.size());
  replacement.resize(source->payload.size());
  source->payload=std::move(replacement);
  auto different=native.prepare(QueryPlanningContext(*source),query({0,0,0},{1,1,1},false));
  ASSERT_TRUE(different.ok());
  auto rejected=native.execute(QueryExecutionContext(source),different.value().implementation(),{});
  ASSERT_FALSE(rejected.ok()); EXPECT_EQ(rejected.status().code(),QueryError::CorruptPayload);
}
