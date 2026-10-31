#include "iotdbfs/query/frontend/QueryRegistrationSocket.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/storage/NativeFileStore.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>
#include <hdf5.h>
#include <boost/property_tree/json_parser.hpp>
#include <fstream>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <future>
#include <limits>
#include <mutex>
#include <sstream>
#include <thread>
#include <filesystem>

namespace {
using namespace iotdbfs;
using namespace std::chrono_literals;

template<class T> T arrow_checked(arrow::Result<T> result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}
Hdf5Attribute number(const std::string& name, LogicalTypeId type, std::uint64_t bits) {
  Hdf5Attribute a; a.name = name; a.type = type; a.state = Hdf5AttributeState::Complete;
  a.native_type_class = type == LogicalTypeId::Float64 ? 1 : 0; a.native_element_bytes = 8;
  for (unsigned n = 0; n < 8; ++n) a.values.push_back(static_cast<std::uint8_t>(bits >> (8*n)));
  return a;
}
Hdf5Attribute text_attribute(const std::string& name, std::string value) {
  Hdf5Attribute a; a.name = name; a.type = LogicalTypeId::Utf8; a.state = Hdf5AttributeState::Complete;
  a.native_type_class = 3; a.native_element_bytes = value.size() + 1; a.strings.push_back(std::move(value)); return a;
}
class HdfBackend final : public StructuredBlobStorageBackend,
    public std::enable_shared_from_this<HdfBackend> {
public:
  StructuredBlobManifest manifest;
  BinaryValue payload, layout;
  mutable std::atomic<std::uint64_t> payload_reads{0}, layout_reads{0};
  mutable std::mutex mutex;
  mutable std::condition_variable ready;
  mutable bool block{false}, entered{false}, released{false};
  std::shared_ptr<const NativeFileHandle> native_handle;
  std::string native_root;
  explicit HdfBackend(bool subnormal = false) {
    for (std::uint32_t i = 0; i < 8; ++i) {
      float value = static_cast<float>(i); std::uint32_t bits; std::memcpy(&bits, &value, 4);
      for (unsigned n = 0; n < 4; ++n) payload.push_back(static_cast<std::uint8_t>(bits >> (8*n)));
    }
    Hdf5LayoutDocument doc; doc.source_size = payload.size(); doc.root_object_id = "root";
    Hdf5LayoutObject root; root.id = "root"; root.kind = Hdf5ObjectKind::Group;
    Hdf5LayoutObject object; object.id = "values"; object.kind = Hdf5ObjectKind::Dataset;
    Hdf5DatasetLayout dataset; dataset.descriptor.path = "/d"; dataset.descriptor.shape = {8};
    dataset.descriptor.value_type = LogicalTypeId::Float32; dataset.descriptor.supported = true;
    dataset.storage = Hdf5StorageLayout::Contiguous; dataset.byte_order = Hdf5ByteOrder::Little;
    dataset.native_type_class = 1; dataset.element_bytes = 4; dataset.index_complete = true;
    dataset.contiguous_offset = 0; dataset.contiguous_size = payload.size(); dataset.allocation_status = 2;
    object.dataset = dataset;
    double spacing = subnormal ? std::numeric_limits<double>::denorm_min() : .25; std::uint64_t bits; std::memcpy(&bits, &spacing, 8);
    object.attributes = {number("Xstart", LogicalTypeId::Int64, subnormal ? 0 : 100), number("Xspacing", LogicalTypeId::Float64, bits),
      number("Npoints", LogicalTypeId::Int64, 8), text_attribute("Xunits", "second"), text_attribute("Xlabel", "GPS time")};
    object.attribute_count = object.attributes.size();
    doc.objects = {root, object}; doc.links = {{"root", "d", Hdf5LinkKind::Hard, "values", {}},
      {"root", "alias", Hdf5LinkKind::Hard, "values", {}}};
    auto encoded = serialize_hdf5_layout(doc);
    if (!encoded.ok()) throw std::runtime_error(encoded.status().message());
    layout = std::move(encoded.value());
    manifest.key = {"/source.h5", 7}; manifest.format_id = "hdf5"; manifest.payload_size = payload.size();
    manifest.layout_size = layout.size(); manifest.phase = StructuredBlobPhase::Published; manifest.published = true; manifest.revision = 1;
  }
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override {return {};}
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& key) const override {
    if (!(key == manifest.key)) return StructuredBlobResult<StructuredBlobManifest>::failure(StructuredBlobError::NotFound,"missing");
    return StructuredBlobResult<StructuredBlobManifest>::success(manifest);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t,const StructuredBlobManifest&) override {return {};}
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&,std::uint64_t,const BinaryValue&) override {return {};}
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,const BinaryValue&) override {return {};}
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,const MetadataMap&) override {return {};}
  StructuredBlobStatus erase(const StructuredBlobVersionKey&,std::uint64_t) override {return {};}
  StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>
  bind_published_payload(const StructuredBlobManifest& selected) const override {
    if (!native_handle) return StructuredBlobStorageBackend::bind_published_payload(selected);
    class Reader final : public StructuredBlobPayloadReader {
    public:
      explicit Reader(std::shared_ptr<const HdfBackend> backend) : backend_(std::move(backend)) {}
      StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
          const StructuredBlobPayloadRanges& ranges) const override {
        return backend_->read_payload_ranges(backend_->manifest.key, ranges);
      }
      StructuredBlobResult<StructuredBlobBytes> read_layout_range(
          std::uint64_t offset, std::uint64_t length) const override {
        return backend_->read_layout_range(backend_->manifest.key, offset, length);
      }
      StructuredBlobResult<std::optional<std::string>> verified_layout_identity() const override {
        return StructuredBlobResult<std::optional<std::string>>::success(std::nullopt);
      }
      StructuredBlobResult<std::shared_ptr<const NativeFileHandle>> open_native_file() const override {
        return StructuredBlobResult<std::shared_ptr<const NativeFileHandle>>::success(backend_->native_handle);
      }
    private:
      std::shared_ptr<const HdfBackend> backend_;
    };
    return StructuredBlobResult<std::shared_ptr<const StructuredBlobPayloadReader>>::success(
      std::make_shared<Reader>(shared_from_this()));
  }
  StructuredBlobResult<BinaryValue> read_payload_range(const StructuredBlobVersionKey& key,std::uint64_t offset,std::uint64_t length) const override {
    ++payload_reads;
    {std::unique_lock<std::mutex> lock(mutex); entered = true; ready.notify_all(); if (block) ready.wait(lock,[&]{return released;});}
    return read(key,payload,offset,length);
  }
  StructuredBlobResult<BinaryValue> read_layout_range(const StructuredBlobVersionKey& key,std::uint64_t offset,std::uint64_t length) const override {
    ++layout_reads; return read(key,layout,offset,length);
  }
  StructuredBlobResult<BinaryValue> read(const StructuredBlobVersionKey& key,const BinaryValue& data,std::uint64_t offset,std::uint64_t length) const {
    if (!(key == manifest.key) || offset > data.size() || length > data.size()-offset)
      return StructuredBlobResult<BinaryValue>::failure(StructuredBlobError::RangeError,"bad range");
    return StructuredBlobResult<BinaryValue>::success({data.begin()+offset,data.begin()+offset+length});
  }
};
// This fixture is a real HDF5 byte image; the older synthetic HdfBackend
// payload is intentionally only suitable for the indexed engine.
BinaryValue native_hdf5_fixture(const std::string& path) {
  struct Id {
    hid_t value;
    herr_t (*close)(hid_t);
    Id(hid_t v, herr_t (*c)(hid_t)) : value(v), close(c) {
      if (v < 0) throw std::runtime_error("native HDF5 fixture handle failed");
    }
    ~Id() { close(value); }
    operator hid_t() const { return value; }
  };
  auto check = [](herr_t value) {
    if (value < 0) throw std::runtime_error("native HDF5 fixture operation failed");
  };
  {
    Id access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    check(H5Pset_fapl_sec2(access));
    Id file(H5Fcreate(path.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT, access), H5Fclose);
    const hsize_t shape[]{8};
    Id space(H5Screate_simple(1, shape, nullptr), H5Sclose);
    Id data(H5Dcreate2(file, "/d", H5T_IEEE_F32LE, space,
      H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
    const float values[]{0,1,2,3,4,5,6,7};
    check(H5Dwrite(data, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, values));
    Id scalar(H5Screate(H5S_SCALAR), H5Sclose);
    auto attribute = [&](const char* name, hid_t type, const void* value) {
      Id attr(H5Acreate2(data, name, type, scalar, H5P_DEFAULT, H5P_DEFAULT), H5Aclose);
      check(H5Awrite(attr, type, value));
    };
    const std::int64_t start = 100, points = 8;
    const double spacing = .25;
    attribute("Xstart", H5T_NATIVE_INT64, &start);
    attribute("Xspacing", H5T_NATIVE_DOUBLE, &spacing);
    attribute("Npoints", H5T_NATIVE_INT64, &points);
    auto text = [&](const char* name, const char* value) {
      Id type(H5Tcopy(H5T_C_S1), H5Tclose);
      check(H5Tset_size(type, std::strlen(value) + 1));
      attribute(name, type, value);
    };
    text("Xunits", "second");
    text("Xlabel", "GPS time");
    check(H5Lcreate_hard(file, "/d", file, "/alias", H5P_DEFAULT, H5P_DEFAULT));
  }
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("native HDF5 fixture read failed");
  BinaryValue bytes{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
  ::unlink(path.c_str());
  return bytes;
}

std::string request(bool gps = false, bool binding = false, bool empty = false, bool alias = false) {
  std::ostringstream out;
  out << R"({"kind":"typed",)";
  if (binding) out << R"("hdf5_binding":{"profile":"gwosc-gps-seconds/1","dataset_path":"/d"},)";
  out << R"("request":{"sources":{"paths":[{"kind":"exact","path":"/source.h5"}],"versions":{"kind":"exact","exact":7}},"subset":{"projection":["hdf5.value:%2F)" << (alias ? "alias" : "d") << R"(",")" << (gps ? "hdf5.coord.gps_seconds" : "hdf5.axis.0") << R"("],"slices":[{"dimension":")" << (gps ? "hdf5.coord.gps_seconds" : "hdf5.axis.0") << R"(","lower":{"kind":"inclusive","value":{"type":")" << (gps ? "float64" : "uint64") << R"(","value":)" << (gps ? "100.5" : "2") << R"(}},"upper":{"kind":"exclusive","value":{"type":")" << (gps ? "float64" : "uint64") << R"(","value":)" << (empty ? (gps ? "100.5" : "2") : (gps ? "101.25" : "5")) << "}}}]}}}";
  return out.str();
}
std::string replace(std::string text,const std::string& from,const std::string& to) {
  const auto p = text.find(from); if (p == std::string::npos) throw std::runtime_error("missing replacement");
  text.replace(p,from.size(),to); return text;
}
class Hdf5Registration : public ::testing::Test {
protected:
  void SetUp() override {
    char pattern[] = "/tmp/iotdbfs-hdfreg-XXXXXX"; const auto path = ::mkdtemp(pattern); ASSERT_NE(path,nullptr);
    directory=path; socket=directory+"/control.sock";
    backend=std::make_shared<HdfBackend>(); storage=std::make_shared<StructuredBlobStorage>(backend);
    catalog=make_in_memory_published_version_catalog({backend->manifest}); runtime=std::make_shared<QueryRuntime>();
    layout_cache=make_hdf5_validated_layout_cache();
    profiles=default_hdf5_semantic_profiles();
    setup();
  }
  void setup(TypedResultStore::Config config = {},std::uint64_t encode_cap=64ULL*1024*1024,
             std::chrono::milliseconds ttl=5min,std::chrono::milliseconds wait=30s) {
    frontend=std::make_shared<TypedQueryFrontend>(1024,ttl);
    auto created=QueryModule::create(catalog,storage,{std::make_shared<Hdf5FormatQueryAdapter>(
      Hdf5QueryKernelConfig{},runtime,Hdf5SemanticConfig{},Hdf5LayoutCacheConfig{},profiles,layout_cache)});
    ASSERT_TRUE(created.ok())<<created.status().message(); module=created.value();
    results=std::make_shared<TypedResultStore>(config); service=std::make_shared<TypedQueryService>(module,frontend,results,encode_cap);
    bridge=std::make_shared<FuseTypedQueryBridge>(frontend,service,64,ttl,FuseTypedQueryBridge::Clock::now,wait);
  }
  void start(bool share_with_unbound = false,
      Hdf5QueryMode mode = Hdf5QueryMode::IoTDBIndexed) {
    server=std::make_unique<QueryRegistrationSocketServer>(socket,"/mounted",frontend,catalog,storage,runtime,bridge,
      [&](const QueryRegistrationPeer&,const std::vector<QuerySourceReference>& sources, const CatalogSelection*) {
        ++authorizations;
        if (sources.size()!=1 || sources.front().path!="/source.h5" || sources.front().version!=7)
          return QueryStatus::failure(QueryError::PermissionDenied,QueryPhase::Validation,"wrong authorization source");
        return allowed ? QueryStatus::success() : QueryStatus::failure(QueryError::PermissionDenied,QueryPhase::Validation,"denied");
      }, 0600, TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{},
      Hdf5QueryKernelConfig{}, QueryRegistrationSocketServer::DatasetDiscovery{},
      share_with_unbound ? layout_cache : nullptr,
      share_with_unbound ? profiles : std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
      QueryRegistrationSocketServer::BeginAuthorizationPublication{}, false, mode);
    auto status = server->start();
    ASSERT_TRUE(status.ok()) << status.message();
  }
  void TearDown() override {
    {std::lock_guard<std::mutex> lock(backend->mutex);backend->released=true;backend->ready.notify_all();}
    server.reset();
    if (!backend->native_root.empty()) {
      std::error_code ignored;
      std::filesystem::remove_all(backend->native_root, ignored);
    }
    ::unlink(socket.c_str());::unlink((socket+".lock").c_str());::rmdir(directory.c_str());
  }
  QueryResult<std::shared_ptr<TypedResultStore::Handle>> handle(const std::string& path,bool permit=true) {
    auto route=path.substr(std::string("/mounted").size()); const auto p=route.rfind("&result=");
    if(p==std::string::npos) throw std::runtime_error("missing resultid");
    auto id=route.substr(p+8);route.resize(p);
    return bridge->acquire_exact(route,id,[=](const auto&){return permit?QueryStatus::success():QueryStatus::failure(QueryError::PermissionDenied,QueryPhase::Validation,"denied cached");});
  }
  std::shared_ptr<arrow::Table> table(const std::string& path) {
    auto h=handle(path);if(!h.ok())throw std::runtime_error(h.status().message());
    auto bytes=h.value()->read(0,h.value()->size());if(!bytes.ok())throw std::runtime_error(bytes.status().message());
    auto buffer=arrow::Buffer::FromVector(std::move(bytes.value()));
    auto reader=arrow_checked(arrow::ipc::RecordBatchStreamReader::Open(std::make_shared<arrow::io::BufferReader>(buffer)));
    return arrow_checked(reader->ToTable());
  }
  std::string directory,socket;
  std::shared_ptr<HdfBackend> backend;
  std::shared_ptr<StructuredBlobStorage> storage;
  std::shared_ptr<const PublishedVersionCatalog> catalog;
  std::shared_ptr<QueryRuntime> runtime;
  std::shared_ptr<Hdf5ValidatedLayoutCache> layout_cache;
  std::vector<std::shared_ptr<const Hdf5SemanticProfile>> profiles;
  std::shared_ptr<TypedQueryFrontend> frontend;
  std::shared_ptr<const QueryModule> module;
  std::shared_ptr<TypedResultStore> results;
  std::shared_ptr<TypedQueryService> service;
  std::shared_ptr<FuseTypedQueryBridge> bridge;
  std::unique_ptr<QueryRegistrationSocketServer> server;
  std::atomic<bool> allowed{true};std::atomic<unsigned> authorizations{0};
};

TEST_F(Hdf5Registration, NativeModeReachesBindingAdapterAndPublishedArrowResult) {
  backend->payload = native_hdf5_fixture(directory + "/native.h5");
  FormatSource source{backend->payload.size(), [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<BinaryValue> {
    if (offset > backend->payload.size() || length > backend->payload.size() - offset) return std::nullopt;
    return BinaryValue(backend->payload.begin()+offset, backend->payload.begin()+offset+length);
  }};
  Hdf5FormatAdapter ingest;
  auto session = ingest.begin_ingestion(source);
  ASSERT_NE(session, nullptr);
  ASSERT_TRUE(session->consume_chunk(0, backend->payload.data(), backend->payload.size()).ok());
  auto ingested = session->finish();
  ASSERT_TRUE(ingested.ok()) << ingested.error();
  backend->layout = std::move(ingested.value().layout);
  backend->manifest.payload_size = backend->payload.size();
  backend->manifest.layout_size = backend->layout.size();
  backend->native_root = directory + "/native-store";
  NativeFileStore files(backend->native_root);
  NativeFileLocation location;
  location.namespace_identity = {"11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222"};
  location.key = backend->manifest.key;
  location.descriptor_id = std::string(64, 'a');
  location.seal_id = std::string(64, 'b');
  location.payload_root = std::string(64, 'c');
  location.size = backend->payload.size();
  auto installed = files.install(location, [&](auto offset, auto length) {
    return backend->read(backend->manifest.key, backend->payload, offset, length);
  });
  ASSERT_TRUE(installed.ok()) << installed.status().message();
  auto local = files.open(installed.value());
  ASSERT_TRUE(local.ok()) << local.status().message();
  backend->native_handle = std::move(local.value());
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  setup(); // The generic module remains indexed: the scoped binding must use the socket mode.
  const auto profile_path = directory + "/native-profile.jsonl";
  const auto* old_profile = ::getenv("IOTDBFS_QUERY_PROFILE_JSONL");
  const bool had_profile = old_profile != nullptr;
  const std::string old_value = old_profile ? old_profile : "";
  ASSERT_EQ(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", profile_path.c_str(), 1), 0);
  start(false, Hdf5QueryMode::NativeHdf5);
  if (had_profile) ::setenv("IOTDBFS_QUERY_PROFILE_JSONL", old_value.c_str(), 1);
  else ::unsetenv("IOTDBFS_QUERY_PROFILE_JSONL");
  auto registered = submit_query_registration(socket, request(true, true));
  ASSERT_TRUE(registered.ok()) << registered.status().message();
  auto result = table(registered.value());
  ASSERT_EQ(result->num_rows(), 3);
  ASSERT_EQ(result->num_columns(), 2);
  EXPECT_FLOAT_EQ(std::static_pointer_cast<arrow::FloatArray>(result->column(0)->chunk(0))->Value(0), 2);
  EXPECT_DOUBLE_EQ(std::static_pointer_cast<arrow::DoubleArray>(result->column(1)->chunk(0))->Value(0), 100.5);
  ASSERT_NE(result->schema()->metadata(), nullptr);
  EXPECT_EQ(arrow_checked(result->schema()->metadata()->Get(
    "iotdbfs.result_descriptor.0.hdf5.query_mode")), "native-hdf5");
  EXPECT_EQ(backend->payload_reads.load(), 0u);
  server->stop();
  boost::property_tree::ptree profile;
  boost::property_tree::read_json(profile_path, profile);
  EXPECT_EQ(profile.get<std::string>("server_hdf5_execution.mode"), "native-hdf5");
  const auto& counters = profile.get_child("counters");
  auto count = [&](const char* name) {
    const auto entry = counters.find(name);
    return entry == counters.not_found() ? std::uint64_t{0} : entry->second.get_value<std::uint64_t>();
  };
  EXPECT_GT(count("hdf5.native.source.calls"), 0u);
  EXPECT_EQ(count("hdf5.native.filesystem.opens"), 1u);
  EXPECT_EQ(count("hdf5.native.filesystem.calls"), count("hdf5.native.source.calls"));
  EXPECT_GT(count("hdf5.native.slabs"), 0u);
  EXPECT_EQ(count("hdf5.selection.decoder_compiles"), 0u);
  ::unlink(profile_path.c_str());
}

TEST_F(Hdf5Registration, PhysicalAndScopedGpsAliasShareDecodedData) {
  start(true);auto physical=submit_query_registration(socket,request());ASSERT_TRUE(physical.ok())<<physical.status().message();
  EXPECT_EQ(authorizations.load(),1u);
  auto p=table(physical.value());ASSERT_EQ(p->num_rows(),3);ASSERT_EQ(p->num_columns(),2);
  EXPECT_FLOAT_EQ(std::static_pointer_cast<arrow::FloatArray>(p->column(0)->chunk(0))->Value(0),2);
  const auto reads=backend->payload_reads.load();ASSERT_GT(reads,0u);
  const auto layout_reads=backend->layout_reads.load();
  EXPECT_EQ(layout_reads,2U);
  auto scientific=submit_query_registration(socket,request(true,true,false,true));ASSERT_TRUE(scientific.ok())<<scientific.status().message();
  auto s=table(scientific.value());ASSERT_EQ(s->num_rows(),3);EXPECT_DOUBLE_EQ(std::static_pointer_cast<arrow::DoubleArray>(s->column(1)->chunk(0))->Value(0),100.5);
  ASSERT_NE(s->schema()->metadata(),nullptr);
  EXPECT_EQ(arrow_checked(s->schema()->metadata()->Get("iotdbfs.result_descriptors")),"1");
  EXPECT_EQ(arrow_checked(s->schema()->metadata()->Get("iotdbfs.result_descriptor.0.protocol")),"iotdbfs-hdf5-subarray/1");
  EXPECT_EQ(arrow_checked(s->schema()->metadata()->Get("iotdbfs.result_descriptor.0.dataset.path")),"/alias");
  EXPECT_EQ(arrow_checked(s->schema()->metadata()->Get("iotdbfs.result_descriptor.0.axis.0.selection_origin")),"2");
  EXPECT_EQ(arrow_checked(s->schema()->metadata()->Get("iotdbfs.result_descriptor.0.axis.0.selection_extent")),"3");
  EXPECT_EQ(arrow_checked(s->schema()->metadata()->Get("iotdbfs.result_descriptor.0.view")),kHdf5GwoscProfile);
  EXPECT_EQ(backend->payload_reads,reads);
  EXPECT_EQ(backend->layout_reads,layout_reads);
  // Decoded data may be shared, but each independent registration authorizes once.
  EXPECT_EQ(authorizations.load(),2u);
  auto unbound=submit_query_registration(socket,request(true));EXPECT_FALSE(unbound.ok());
  if(!unbound.ok())EXPECT_EQ(unbound.status().code(),QueryError::UnsupportedCapability);
  EXPECT_EQ(frontend->size(),2u);
}
TEST_F(Hdf5Registration, RepeatedGpsRegistrationsReuseValidatedLayout) {
  start();
  auto first = submit_query_registration(socket, request(true, true));
  ASSERT_TRUE(first.ok()) << first.status().message();
  auto first_table = table(first.value());
  ASSERT_EQ(first_table->num_rows(), 3);
  const auto cold_reads = backend->layout_reads.load();
  EXPECT_EQ(cold_reads, 2U);

  auto second = submit_query_registration(socket, request(true, true, false, true));
  ASSERT_TRUE(second.ok()) << second.status().message();
  auto second_table = table(second.value());
  ASSERT_EQ(second_table->num_rows(), 3);
  EXPECT_EQ(backend->layout_reads.load(), cold_reads);
  EXPECT_EQ(authorizations.load(), 2U);
}
TEST_F(Hdf5Registration, AuthorizationPrecedesLayoutAndStillAppliesToWarmResults) {
  start();allowed=false;auto denied=submit_query_registration(socket,request(true,true));EXPECT_FALSE(denied.ok());
  EXPECT_EQ(backend->layout_reads,0u);EXPECT_EQ(backend->payload_reads,0u);EXPECT_EQ(frontend->size(),0u);
  allowed=true;auto ok=submit_query_registration(socket,request(true,true));ASSERT_TRUE(ok.ok())<<ok.status().message();
  auto denied_cached=handle(ok.value(),false);EXPECT_FALSE(denied_cached.ok());
  if(!denied_cached.ok())EXPECT_EQ(denied_cached.status().code(),QueryError::PermissionDenied);
}
TEST_F(Hdf5Registration, RejectsUnknownDuplicateAndServerOwnedBindingFields) {
  start();const auto valid=request(true,true);
  const std::vector<std::pair<std::string,std::string>> mutations={
    {R"("kind":"typed",)",R"("kind":"typed","kind":"typed",)"},
    {R"("profile":"gwosc-gps-seconds/1")",R"("profile":"unknown")"},
    {R"("profile":"gwosc-gps-seconds/1")",R"("profile":"gwosc-gps-seconds/1","profile":"gwosc-gps-seconds/1")"},
    {R"("dataset_path":"/d")",R"("dataset_path":"/d","storage_namespace":1)"},
    {R"("dataset_path":"/d")",R"("dataset_path":"/d","version":7)"},
    {R"("dataset_path":"/d")",R"("dataset_path":"relative")"},
    {R"("request":{)",R"("request":{"read_parallelism":4,)"},
    {R"("sources":{)",R"("sources":{"namespace":1,)"},
    {R"("kind":"exact","path")",R"("kind":"exact","stride":2,"path")"},
    {R"("versions":{"kind":"exact","exact":7})",R"("versions":{"kind":"exact","exact":7,"exact":8})"},
    {R"("subset":{)",R"("limits":{"max_rangez":1},"subset":{)"},
    {R"("lower":{"kind")",R"("stride":2,"lower":{"kind")"},
    {R"("kind":"typed",)",R"("kind":"typed","storage_namespace":1,)"}};
  for(const auto& change:mutations){SCOPED_TRACE(change.second);auto result=submit_query_registration(socket,replace(valid,change.first,change.second));EXPECT_FALSE(result.ok());if(!result.ok())EXPECT_EQ(result.status().code(),QueryError::InvalidQuery);}
  EXPECT_EQ(frontend->size(),0u);EXPECT_EQ(backend->payload_reads,0u);
}
TEST_F(Hdf5Registration, BindingCannotEnableLatestOrOtherDatasetsOrUnusedSemantics) {
  start();for(auto text:{replace(request(true,true),R"("kind":"exact","exact":7)",R"("kind":"latest")"),
    replace(request(true,true),R"("dataset_path":"/d")",R"("dataset_path":"/absent")"),request(false,true)}) {
    EXPECT_FALSE(submit_query_registration(socket,text).ok());
  }EXPECT_EQ(frontend->size(),0u);EXPECT_EQ(backend->payload_reads,0u);
}
TEST_F(Hdf5Registration, Float64BoundsAcceptExactSubnormalsAndRejectUnderflowOrNondecimalSyntax) {
  backend=std::make_shared<HdfBackend>(true);storage=std::make_shared<StructuredBlobStorage>(backend);
  catalog=make_in_memory_published_version_catalog({backend->manifest});setup();start();
  auto json=replace(request(true,true),"100.5","9.8813129168249309e-324");
  json=replace(json,"101.25","2.4703282292062327e-323");
  auto response=submit_query_registration(socket,json);ASSERT_TRUE(response.ok())<<response.status().message();
  auto values=table(response.value());ASSERT_EQ(values->num_rows(),3);
  EXPECT_EQ(std::static_pointer_cast<arrow::DoubleArray>(values->column(1)->chunk(0))->Value(0),
    2*std::numeric_limits<double>::denorm_min());
  for(const auto& invalid: {"1e-9999","1e9999", "\"nan\"", "\"+1.5\"", "\" 1.5\"", "\"0x1p0\"", "\"1,5\""}) {
    SCOPED_TRACE(invalid);
    auto rejected=submit_query_registration(socket,replace(json,"9.8813129168249309e-324",invalid));
    EXPECT_FALSE(rejected.ok());if(!rejected.ok())EXPECT_EQ(rejected.status().code(),QueryError::InvalidQuery);
  }
}
TEST_F(Hdf5Registration, EmptySelectionStillPublishesTypedSchema) {
  start();auto response=submit_query_registration(socket,request(true,true,true));ASSERT_TRUE(response.ok())<<response.status().message();
  auto values=table(response.value());EXPECT_EQ(values->num_rows(),0);EXPECT_EQ(values->num_columns(),2);EXPECT_EQ(backend->payload_reads,0u);
  ASSERT_NE(values->schema()->metadata(),nullptr);
  EXPECT_EQ(arrow_checked(values->schema()->metadata()->Get("iotdbfs.result_descriptor.0.axis.0.selection_extent")),"0");
}
TEST_F(Hdf5Registration, EncodeCapUsesStoreLimitAndFailedRoutesAreRemoved) {
  TypedResultStore::Config config;config.max_result_bytes=64;setup(config,1024*1024);start();
  auto result=submit_query_registration(socket,request(true,true));EXPECT_FALSE(result.ok());
  if(!result.ok())EXPECT_EQ(result.status().code(),QueryError::ResourceExhausted);
  EXPECT_EQ(frontend->size(),0u);EXPECT_EQ(bridge->cached_routes(),0u);EXPECT_EQ(results->stats().open_handles,0u);
}
TEST_F(Hdf5Registration, FailedPublicationReleasesRouteAndOnlyKeepsCallerPinnedBytes) {
  TypedResultStore::Config config;config.max_result_bytes=65536;config.max_resident_bytes=65536;
  setup(config);auto published=results->publish(BinaryValue(65536,42));ASSERT_TRUE(published.ok());
  auto pinned=results->open(published.value());ASSERT_TRUE(pinned.ok());start();
  auto response=submit_query_registration(socket,request(true,true));EXPECT_FALSE(response.ok());
  if(!response.ok())EXPECT_EQ(response.status().code(),QueryError::ResourceExhausted);
  EXPECT_EQ(frontend->size(),0u);EXPECT_EQ(bridge->cached_routes(),0u);
  EXPECT_EQ(results->stats().open_handles,1u);pinned.value().release();
  EXPECT_EQ(results->stats().open_handles,0u);
}
TEST_F(Hdf5Registration, RejectsMalformedAndOversizedFramesWithoutRegistration) {
  start();EXPECT_FALSE(submit_query_registration(socket,"{not_json}").ok());
  EXPECT_FALSE(submit_query_registration(socket,std::string(1024*1024+1,'x')).ok());
  EXPECT_EQ(frontend->size(),0u);EXPECT_EQ(backend->payload_reads,0u);
}
TEST_F(Hdf5Registration, MidStreamEncodingLimitCancelsCursorAndReleasesLeases) {
  start(); auto empty = submit_query_registration(socket,request(false,false,true));
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  std::uint64_t empty_bytes = 0;
  {auto pinned = handle(empty.value()); ASSERT_TRUE(pinned.ok()); empty_bytes = pinned.value()->size();}
  server.reset(); TypedResultStore::Config config; config.max_result_bytes = empty_bytes + 16;
  setup(config); start();
  auto failed = submit_query_registration(socket,request()); EXPECT_FALSE(failed.ok());
  if (!failed.ok()) EXPECT_EQ(failed.status().code(),QueryError::ResourceExhausted);
  EXPECT_GT(backend->payload_reads,0u);
  EXPECT_EQ(frontend->size(),0u); EXPECT_EQ(results->stats().open_handles,0u);
  const auto stats = runtime->stats();
  EXPECT_EQ(stats.pinned_bytes,0u); EXPECT_EQ(stats.temporary_bytes,0u);
  EXPECT_EQ(stats.payload_reads_active,0u);
}
TEST_F(Hdf5Registration, ExpiredRouteCannotServeOldResultAndReleasesScopedModule) {
  auto now=TypedQueryFrontend::Clock::now();frontend=std::make_shared<TypedQueryFrontend>(4,10ms,[&]{return now;});
  Hdf5SemanticConfig semantics;semantics.gwosc_bindings.push_back({backend->manifest.key,"/d",storage->storage_namespace()});
  auto scoped=QueryModule::create(catalog,storage,{std::make_shared<Hdf5FormatQueryAdapter>(Hdf5QueryKernelConfig{},runtime,semantics)});
  ASSERT_TRUE(scoped.ok());std::weak_ptr<const QueryModule> weak=scoped.value();
  QueryRequest query;query.sources.paths={{PathScopeKind::Exact,"/source.h5"}};query.sources.versions=VersionSelection::exact_version(7);
  auto route=frontend->register_typed(query,scoped.value());ASSERT_TRUE(route.ok());scoped.value().reset();ASSERT_FALSE(weak.expired());
  now+=11ms;EXPECT_FALSE(frontend->resolve(route.value()).ok());EXPECT_TRUE(weak.expired());EXPECT_EQ(frontend->size(),0u);
}
TEST_F(Hdf5Registration, DisconnectedClientRetiresCompletedRegistration) {
  start();{std::lock_guard<std::mutex> lock(backend->mutex);backend->block=true;}
  int fd=::socket(AF_UNIX,SOCK_STREAM,0);ASSERT_GE(fd,0);sockaddr_un address{};address.sun_family=AF_UNIX;
  std::memcpy(address.sun_path,socket.c_str(),socket.size()+1);ASSERT_EQ(::connect(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address)),0);
  const auto text=request(true,true);auto length=htonl(static_cast<std::uint32_t>(text.size()));
  ASSERT_EQ(::send(fd,&length,4,0),4);ASSERT_EQ(::send(fd,text.data(),text.size(),0),static_cast<ssize_t>(text.size()));
  {std::unique_lock<std::mutex> lock(backend->mutex);ASSERT_TRUE(backend->ready.wait_for(lock,2s,[&]{return backend->entered;}));}
  ::shutdown(fd,SHUT_RDWR);::close(fd);
  {std::lock_guard<std::mutex> lock(backend->mutex);backend->released=true;backend->ready.notify_all();}
  for(int i=0;i<200 && (frontend->size() || bridge->cached_routes() || results->stats().open_handles);++i)std::this_thread::sleep_for(5ms);
  EXPECT_EQ(frontend->size(),0u);EXPECT_EQ(bridge->cached_routes(),0u);EXPECT_EQ(results->stats().open_handles,0u);
}
TEST_F(Hdf5Registration, BridgeWaitTimeoutDoesNotCancelIndependentProducer) {
  setup({},64ULL*1024*1024,5min,20ms);
  QueryRequest query;query.sources.paths={{PathScopeKind::Exact,"/source.h5"}};query.sources.versions=VersionSelection::exact_version(7);
  QueryFragment fragment;fragment.projection={{"hdf5.value:%2Fd"}};query.subset=fragment;
  auto route=frontend->register_typed(query);ASSERT_TRUE(route.ok());
  {std::lock_guard<std::mutex> lock(backend->mutex);backend->block=true;}
  auto producer=std::async(std::launch::async,[&]{return bridge->acquire(route.value(),{});});
  struct ReleaseOnExit {
    std::shared_ptr<HdfBackend> backend;
    ~ReleaseOnExit() {std::lock_guard<std::mutex> lock(backend->mutex); backend->released=true; backend->ready.notify_all();}
  } release{backend};
  {std::unique_lock<std::mutex> lock(backend->mutex);ASSERT_TRUE(backend->ready.wait_for(lock,2s,[&]{return backend->entered;}));}
  auto waiter=bridge->acquire(route.value(),{});EXPECT_FALSE(waiter.ok());if(!waiter.ok())EXPECT_EQ(waiter.status().code(),QueryError::DeadlineExceeded);
  {std::lock_guard<std::mutex> lock(backend->mutex);backend->released=true;backend->ready.notify_all();}
  EXPECT_TRUE(producer.get().ok());
}
} // namespace
