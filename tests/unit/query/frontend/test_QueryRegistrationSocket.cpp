#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryBatchBudget.h"
#include "iotdbfs/query/frontend/TypedAuthorizationCache.h"
#include "iotdbfs/query/frontend/CatalogSourceAuthorization.h"
#include "../../../support/InMemoryDatasetCatalog.h"
#include "iotdbfs/query/frontend/QueryRegistrationSocket.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/arrow/ArrowFormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5SourceDriver.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <gtest/gtest.h>
#include <hdf5.h>
#include <boost/property_tree/json_parser.hpp>

#include <arpa/inet.h>
#include <grp.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <variant>
#include <vector>

namespace {
using namespace iotdbfs;

template<class T> T checked(arrow::Result<T> result) {
  if (!result.ok()) throw std::runtime_error(result.status().ToString());
  return std::move(result).ValueOrDie();
}
void checked(arrow::Status status) {
  if (!status.ok()) throw std::runtime_error(status.ToString());
}

BinaryValue tiny_arrow_payload(bool flat = false, int length = 4) {
  auto leaf = std::make_shared<arrow::FloatBuilder>();
  auto channels = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), leaf);
  auto rows = std::make_shared<arrow::ListBuilder>(arrow::default_memory_pool(), channels);
  if (!flat) checked(rows->Append());
  checked(channels->Append());
  for (int step = 0; step < length; ++step) checked(leaf->Append(static_cast<float>(step)));
  std::shared_ptr<arrow::Array> array;
  if (flat) checked(channels->Finish(&array));
  else checked(rows->Finish(&array));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({arrow::field("target", array->type())}), 1, {array});
  auto sink = checked(arrow::io::BufferOutputStream::Create());
  auto writer = checked(arrow::ipc::MakeStreamWriter(sink, batch->schema()));
  checked(writer->WriteRecordBatch(*batch));
  checked(writer->Close());
  auto buffer = checked(sink->Finish());
  return {buffer->data(), buffer->data() + buffer->size()};
}

BinaryValue tiny_arrow_layout(const BinaryValue& payload) {
  FormatSource source{payload.size(), [&](std::uint64_t offset, std::uint64_t length)
      -> std::optional<BinaryValue> {
    if (offset > payload.size() || length > payload.size() - offset) return std::nullopt;
    return BinaryValue(payload.begin() + offset, payload.begin() + offset + length);
  }};
  ArrowFormatAdapter adapter;
  auto session = adapter.begin_ingestion(source);
  if (!session) throw std::runtime_error("Arrow ingestion did not start");
  auto consumed = session->consume_chunk(0, payload.data(), payload.size());
  if (!consumed.ok()) throw std::runtime_error(consumed.error());
  auto finished = session->finish();
  if (!finished.ok()) throw std::runtime_error(finished.error());
  return finished.value().layout;
}

class TinyBackend final : public StructuredBlobStorageBackend {
public:
  iotdbfs::test::InMemoryDatasetCatalog dataset_catalogs;
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& value) override { return dataset_catalogs.write(key, value); }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
      const StructuredBlobVersionKey& key) const override { return dataset_catalogs.read(key); }

  StructuredBlobManifest manifest;
  BinaryValue payload;
  BinaryValue layout;
  std::function<void()> during_payload;
  mutable std::atomic<std::uint64_t> layout_reads{0};
  mutable std::atomic<std::uint64_t> manifest_reads{0};

  TinyBackend() {
    payload = tiny_arrow_payload();
    layout = tiny_arrow_layout(payload);
    manifest.key = {"/dataset/a.arrow", 7};
    manifest.format_id = "arrow";
    manifest.payload_size = payload.size();
    manifest.layout_size = layout.size();
    manifest.phase = StructuredBlobPhase::Published;
    manifest.published = true;
    manifest.revision = 5;
  }
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return {}; }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(
      const StructuredBlobVersionKey& key) const override {
    ++manifest_reads;
    if (!(key == manifest.key)) return StructuredBlobResult<StructuredBlobManifest>::failure(
      StructuredBlobError::NotFound, "missing test source");
    return StructuredBlobResult<StructuredBlobManifest>::success(manifest);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobManifest&) override { return {}; }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&, std::uint64_t,
      const BinaryValue&) override { return {}; }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&, const BinaryValue&) override { return {}; }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&, const MetadataMap&) override { return {}; }
  StructuredBlobResult<BinaryValue> read_payload_range(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length) const override {
    if (during_payload) during_payload();
    return read(key, payload, offset, length);
  }
  StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(
      const StructuredBlobVersionKey& key,
      const StructuredBlobPayloadRanges& ranges) const override {
    StructuredBlobPayloadBatch result;
    result.reserve(ranges.size());
    for (const auto& range : ranges) {
      auto bytes = read_payload_range(key, range.offset, range.length);
      if (!bytes.ok())
        return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(
          bytes.status().code(), bytes.status().message());
      result.push_back(std::move(bytes.value()));
    }
    return StructuredBlobResult<StructuredBlobPayloadBatch>::success(std::move(result));
  }
  StructuredBlobResult<BinaryValue> read_layout_range(const StructuredBlobVersionKey& key,
      std::uint64_t offset, std::uint64_t length) const override {
    ++layout_reads;
    return read(key, layout, offset, length);
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&, std::uint64_t) override { return {}; }
private:
  StructuredBlobResult<BinaryValue> read(const StructuredBlobVersionKey& key,
      const BinaryValue& bytes, std::uint64_t offset, std::uint64_t length) const {
    if (!(key == manifest.key) || offset > bytes.size() || length > bytes.size() - offset)
      return StructuredBlobResult<BinaryValue>::failure(
        StructuredBlobError::RangeError, "invalid test range");
    return StructuredBlobResult<BinaryValue>::success(
      {bytes.begin() + offset, bytes.begin() + offset + length});
  }
};

class SocketFixture : public ::testing::Test {
protected:
  void SetUp() override {
    char pattern[] = "/tmp/iotdbfs-registration-XXXXXX";
    char* created = ::mkdtemp(pattern);
    ASSERT_NE(created, nullptr);
    directory = created;
    socket = directory + "/control.sock";
    frontend = std::make_shared<TypedQueryFrontend>();
    auto backend = std::make_shared<TinyBackend>();
    catalog = make_in_memory_published_version_catalog({backend->manifest});
    storage = std::make_shared<StructuredBlobStorage>(backend);
    runtime = std::make_shared<QueryRuntime>();
    auto query = QueryModule::create(catalog, storage,
      {std::make_shared<ArrowFormatQueryAdapter>(ArrowDecodeConfig{}, runtime)});
    ASSERT_TRUE(query.ok()) << query.status().message();
    auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
    auto service = std::make_shared<TypedQueryService>(query.value(), frontend,
      results, 64ULL * 1024 * 1024);
    bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
  }
  void TearDown() override {
    ::unlink(socket.c_str());
    ::unlink((socket + ".lock").c_str());
    ::rmdir(directory.c_str());
  }

  std::string directory;
  std::string socket;
  std::shared_ptr<TypedQueryFrontend> frontend;
  std::shared_ptr<const PublishedVersionCatalog> catalog;
  std::shared_ptr<const StructuredBlobStorage> storage;
  std::shared_ptr<QueryRuntime> runtime;
  std::shared_ptr<FuseTypedQueryBridge> bridge;
};

constexpr const char* kTyped = R"({"kind":"typed","request":{"sources":{"paths":[{"kind":"exact","path":"/dataset/a.arrow"}],"versions":{"kind":"exact","exact":7},"formats":{"adapters":"listed","adapter_ids":["arrow"],"include_byte_passthrough":false}}}})";
constexpr const char* kArrowValues = R"({"kind":"typed","request":{
  "sources":{"paths":[{"kind":"exact","path":"/dataset/a.arrow"}],
    "versions":{"kind":"exact","exact":7},
    "formats":{"adapters":"listed","adapter_ids":["arrow"],"include_byte_passthrough":false}},
  "subset":{"projection":["index","value.0"],"physical_array":{
    "field_id":"target","record_batch_index":0,"row_index":0,
    "values":[{"child_indices":[0]}]}}}})";

std::string typed_batch(const std::vector<std::string>& items) {
  std::string result = "{\"kind\":\"typed_batch\",\"items\":[";
  for (std::size_t i = 0; i < items.size(); ++i) {
    if (i) result += ',';
    result += items[i];
  }
  return result + "]}";
}

std::string hdf5_only_typed() {
  return R"({"kind":"typed","request":{
    "sources":{"paths":[{"kind":"exact","path":"/dataset/a.arrow"}],
      "versions":{"kind":"exact","exact":7},
      "formats":{"adapters":"listed","adapter_ids":["hdf5"],"include_byte_passthrough":false}},
    "subset":{"projection":["hdf5.value:%2Fd"]}}})";
}

BinaryValue socket_result_bytes(const std::shared_ptr<FuseTypedQueryBridge>& bridge,
    const std::string& path) {
  const auto marker = path.rfind("&result=");
  if (marker == std::string::npos) throw std::runtime_error("missing result identity");
  auto opened = bridge->acquire_exact(path.substr(9, marker - 9), path.substr(marker + 8), {});
  if (!opened.ok()) throw std::runtime_error(opened.status().message());
  auto bytes = opened.value()->read(0, opened.value()->size());
  if (!bytes.ok()) throw std::runtime_error(bytes.status().message());
  return std::move(bytes.value());
}

void expect_tiny_arrow_values(BinaryValue bytes) {
  auto buffer = arrow::Buffer::FromVector(std::move(bytes));
  auto reader = checked(arrow::ipc::RecordBatchStreamReader::Open(
    std::make_shared<arrow::io::BufferReader>(buffer)));
  auto table = checked(reader->ToTable());
  ASSERT_EQ(table->num_rows(), 4);
  ASSERT_EQ(table->num_columns(), 2);
  auto indices = table->GetColumnByName("index");
  auto values = table->GetColumnByName("value.0");
  ASSERT_NE(indices, nullptr);
  ASSERT_NE(values, nullptr);
  ASSERT_EQ(indices->num_chunks(), 1);
  ASSERT_EQ(values->num_chunks(), 1);
  ASSERT_EQ(indices->type()->id(), arrow::Type::UINT64);
  ASSERT_EQ(values->type()->id(), arrow::Type::FLOAT);
  const auto index = std::static_pointer_cast<arrow::UInt64Array>(indices->chunk(0));
  const auto value = std::static_pointer_cast<arrow::FloatArray>(values->chunk(0));
  for (int i = 0; i < 4; ++i) {
    EXPECT_FALSE(index->IsNull(i));
    EXPECT_FALSE(value->IsNull(i));
    EXPECT_EQ(index->Value(i), static_cast<std::uint64_t>(i));
    EXPECT_FLOAT_EQ(value->Value(i), static_cast<float>(i));
  }
}


// Exercise the binary delivery protocol without the path-only control client.
std::pair<boost::property_tree::ptree, BinaryValue> fetch_result(
    const std::string& socket, const std::string& request) {
  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) throw std::runtime_error("socket failed");
  struct Close {int fd; ~Close() {::close(fd);}} close{fd};
  timeval timeout{3, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  sockaddr_un address{}; address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket.c_str(), socket.size() + 1);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
    throw std::runtime_error("fetch connect failed");
  auto exact = [&](void* data, std::size_t size, bool write) {
    auto* bytes = static_cast<char*>(data);
    for (std::size_t offset = 0; offset < size;) {
      const auto count = write ? ::write(fd, bytes + offset, size - offset)
                               : ::read(fd, bytes + offset, size - offset);
      if (count < 0 && errno == EINTR) continue;
      if (count <= 0) throw std::runtime_error("fetch short IO");
      offset += count;
    }
  };
  auto length = htonl(static_cast<std::uint32_t>(request.size()));
  exact(&length, sizeof(length), true);
  auto payload = request;
  exact(payload.data(), payload.size(), true);
  exact(&length, sizeof(length), false);
  const auto header_size = ntohl(length);
  if (header_size == 0 || header_size > 1048576) throw std::runtime_error("fetch header bound");
  std::string header(header_size, '\0'); exact(header.data(), header.size(), false);
  boost::property_tree::ptree root;
  std::istringstream input(header); boost::property_tree::read_json(input, root);
  BinaryValue bytes;
  if (root.get<bool>("ok")) {
    const auto size = root.get<std::size_t>("size");
    if (size == 0 || size > 64 * 1024 * 1024) throw std::runtime_error("fetch body bound");
    bytes.resize(size); exact(bytes.data(), bytes.size(), false);
  }
  return {std::move(root), std::move(bytes)};
}
std::string fetch_request(const std::string& path) {
  return "{\"kind\":\"result_fetch\",\"version\":1,\"path\":\"" + path + "\"}";
}

TEST_F(SocketFixture, ResultFetchMatchesPublishedIpcAndReauthorizesEveryOpen) {
  std::atomic<unsigned> authorizations{0};
  std::atomic<bool> denied{false};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime,
    bridge, [&](const auto& peer, const auto&, const auto*) {
      EXPECT_EQ(peer.uid, ::geteuid()); ++authorizations;
      return denied ? QueryStatus::failure(QueryError::PermissionDenied,
        QueryPhase::Catalog, "revoked") : QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto published = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(published.ok()) << published.status().message();
  const auto path = published.value().path;
  const auto expected = socket_result_bytes(bridge, path);
  const auto before = authorizations.load();
  for (unsigned i = 0; i < 2; ++i) {
    auto fetched = fetch_result(socket, fetch_request(path));
    EXPECT_TRUE(fetched.first.get<bool>("ok"));
    EXPECT_EQ(fetched.first.get<std::string>("kind"), "result_fetch");
    EXPECT_EQ(fetched.first.get<unsigned>("version"), 1u);
    EXPECT_EQ(fetched.second, expected); expect_tiny_arrow_values(fetched.second);
  }
  EXPECT_EQ(authorizations, before + 2);
  denied = true;
  const auto refused = fetch_result(socket, fetch_request(path));
  EXPECT_FALSE(refused.first.get<bool>("ok"));
  EXPECT_EQ(refused.first.get<unsigned>("code"), static_cast<unsigned>(QueryError::PermissionDenied));
  denied = false;
  EXPECT_EQ(fetch_result(socket, fetch_request(path)).second, expected);
  EXPECT_EQ(frontend->size(), 1u); // Failed open must not retire another consumer's publication.
}

TEST_F(SocketFixture, ResultFetchRejectsWrongResultVersionMountAndOverrides) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto published = submit_query_control(socket, kArrowValues); ASSERT_TRUE(published.ok());
  const auto path = published.value().path;
  auto wrong = path; wrong.back() = wrong.back() == '0' ? '1' : '0';
  EXPECT_FALSE(fetch_result(socket, fetch_request(wrong)).first.get<bool>("ok"));
  for (const auto& request : {
      "{\"kind\":\"result_fetch\",\"version\":2,\"path\":\"" + path + "\"}",
      fetch_request("/mnt/other" + path.substr(9)),
      "{\"kind\":\"result_fetch\",\"version\":1,\"path\":\"" + path + "\",\"workers\":4}",
      "{\"kind\":\"result_fetch\",\"version\":1,\"version\":1,\"path\":\"" + path + "\"}",
      fetch_request(path.substr(0, path.rfind("&result=")))}) {
    EXPECT_FALSE(fetch_result(socket, request).first.get<bool>("ok"));
  }
  EXPECT_EQ(fetch_result(socket, fetch_request(path)).second, socket_result_bytes(bridge, path));
  EXPECT_EQ(frontend->size(), 1u);
}

TEST_F(SocketFixture, TransportClassificationPreservesMalformedAndDuplicateKindErrors) {
  std::atomic<unsigned> authorizations{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      ++authorizations;
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  // A leading fetch kind in a syntactically incomplete frame must still fail
  // at the ordinary registration validation boundary, without a binary body.
  for (const auto& test : std::vector<std::pair<std::string, std::string>>{
      {R"({"kind":"result_fetch",)", "expected key string"},
      {R"({"kind":"typed","request":)", "expected value"},
      {R"({"kind":"typed"} trailing)", "garbage after data"}}) {
    SCOPED_TRACE(test.first);
    auto response = submit_query_control(socket, test.first);
    ASSERT_FALSE(response.ok());
    EXPECT_EQ(response.status().code(), QueryError::InvalidQuery);
    EXPECT_EQ(response.status().phase(), QueryPhase::Validation);
    EXPECT_NE(response.status().message().find(test.second), std::string::npos);
  }
  // Property-tree retains duplicates and chooses the first kind. Reusing its
  // parsed tree must not silently change that to last-kind dispatch or erase
  // the duplicates before the strict result-fetch validator sees them.
  auto unknown_first = submit_query_control(socket,
    R"({"kind":"unknown","kind":"result_fetch","version":1,"path":"/mnt/data/x"})");
  ASSERT_FALSE(unknown_first.ok());
  EXPECT_EQ(unknown_first.status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(unknown_first.status().phase(), QueryPhase::Validation);
  EXPECT_EQ(unknown_first.status().message(), "unknown registration kind");
  auto fetch_first = fetch_result(socket,
    R"({"kind":"result_fetch","kind":"unknown","version":1,"path":"/mnt/data/x"})");
  EXPECT_FALSE(fetch_first.first.get<bool>("ok"));
  EXPECT_EQ(fetch_first.first.get<unsigned>("code"), static_cast<unsigned>(QueryError::InvalidQuery));
  EXPECT_EQ(fetch_first.first.get<unsigned>("phase"), static_cast<unsigned>(QueryPhase::Validation));
  EXPECT_EQ(fetch_first.first.get<std::string>("message"), "duplicate result fetch field: kind");
  EXPECT_TRUE(fetch_first.second.empty());
  EXPECT_EQ(authorizations, 0u);
  EXPECT_EQ(frontend->size(), 0u);
  // A rejected frame must release all per-request pins and keep serving.
  auto valid = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(valid.ok()) << valid.status().message();
  expect_tiny_arrow_values(fetch_result(socket, fetch_request(valid.value().path)).second);
}

TEST_F(SocketFixture, TypedBatchKeepsWholeFrameValidationAndFirstErrorOrder) {
  std::atomic<unsigned> authorizations{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      ++authorizations;
      return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
        "authorization must follow complete batch validation");
    });
  ASSERT_TRUE(server.start().ok());
  const auto duplicate_kind = R"({"kind":"typed","kind":"typed","request":{}})";
  const auto duplicate_request = R"({"kind":"typed","request":{},"request":{}})";
  const auto invalid_kind = R"({"kind":"window"})";
  struct Case { std::string request, message; };
  for (const auto& test : std::vector<Case>{
      {typed_batch({kTyped, duplicate_kind, invalid_kind}), "duplicate typed batch item field: kind"},
      {typed_batch({kTyped, duplicate_request, invalid_kind}), "duplicate typed batch item field: request"},
      {typed_batch({kTyped, invalid_kind, duplicate_kind}), "typed batch accepts typed registrations only"}}) {
    SCOPED_TRACE(test.request);
    auto response = submit_query_control(socket, test.request);
    ASSERT_FALSE(response.ok());
    EXPECT_EQ(response.status().code(), QueryError::InvalidQuery);
    EXPECT_EQ(response.status().phase(), QueryPhase::Validation);
    EXPECT_EQ(response.status().message(), test.message);
    EXPECT_EQ(authorizations, 0u);
    EXPECT_EQ(frontend->size(), 0u);
  }
}

TEST_F(SocketFixture, ResultFetchKeepsStickyMetadataBudgetFailureAndRefundsAdmission) {
  std::atomic<bool> exhausted{false};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime,
    bridge, [&](const auto&, const auto&, const auto*) {
      if (exhausted) {
        try { require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1); }
        catch (const QueryMetadataBudgetExceeded&) {} // Sticky failure survives a swallowing callback.
      }
      return QueryStatus::success();
    }, 0600, {}, {}, {}, {}, {}, {}, {}, true);
  ASSERT_TRUE(server.start().ok());
  auto published = submit_query_control(socket, kArrowValues); ASSERT_TRUE(published.ok());
  exhausted = true;
  for (unsigned i = 0; i < 20; ++i) {
    const auto refused = fetch_result(socket, fetch_request(published.value().path));
    EXPECT_FALSE(refused.first.get<bool>("ok"));
    EXPECT_EQ(refused.first.get<unsigned>("code"), static_cast<unsigned>(QueryError::ResourceExhausted));
  }
  exhausted = false;
  EXPECT_EQ(fetch_result(socket, fetch_request(published.value().path)).second,
    socket_result_bytes(bridge, published.value().path));
}

TEST_F(SocketFixture, Hdf5ResourceWaitersDoNotOccupySlotsOrBlockArrowAndItsBatchBudget) {
  std::mutex mutex;
  std::condition_variable changed;
  bool block_hdf5 = false, release = false;
  std::size_t hdf5_authorizations = 0;
  std::atomic<bool> charge_arrow_metadata{false};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto& sources, const auto*) {
      // Explicit HDF5 selects no Arrow source in this fixture. The existing
      // authorization boundary still executes before adapter planning. This
      // deterministic gate keeps one genuine socket request executing while
      // other HDF5 requests must remain in the scheduler, not on worker locks.
      if (sources.empty()) {
        std::unique_lock<std::mutex> lock(mutex);
        ++hdf5_authorizations;
        changed.notify_all();
        if (block_hdf5) changed.wait(lock, [&] { return release; });
        return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
          "test HDF5 source denied");
      }
      if (charge_arrow_metadata.load()) require_query_metadata_unique(40ULL * 1024 * 1024);
      return QueryStatus::success();
    }, 0600, TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{},
    Hdf5QueryKernelConfig{}, QueryRegistrationSocketServer::DatasetDiscovery{},
    std::shared_ptr<Hdf5ValidatedLayoutCache>{},
    std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
    QueryRegistrationSocketServer::BeginAuthorizationPublication{}, false,
    Hdf5QueryMode::NativeHdf5);
  ASSERT_TRUE(server.start().ok());
  const auto hdf5 = hdf5_only_typed();
  const auto mixed = typed_batch({kArrowValues, hdf5});
  const auto invalid_mixed = typed_batch({hdf5, "{\"kind\":\"typed\",\"request\":{}}"});
  auto expected_hdf5 = submit_query_control(socket, hdf5);
  auto expected_mixed = submit_query_control(socket, mixed);
  auto expected_invalid = submit_query_control(socket, invalid_mixed);
  ASSERT_FALSE(expected_hdf5.ok());
  ASSERT_FALSE(expected_mixed.ok());
  ASSERT_FALSE(expected_invalid.ok());
  ASSERT_EQ(expected_hdf5.status().code(), QueryError::PermissionDenied);
  ASSERT_EQ(frontend->size(), 0u);
  auto serial = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(serial.ok()) << serial.status().message();
  const auto expected_bytes = socket_result_bytes(bridge, serial.value().path);
  expect_tiny_arrow_values(expected_bytes);
  charge_arrow_metadata = true;
  auto expected_budget = submit_query_control(socket, typed_batch({kArrowValues, kArrowValues}));
  charge_arrow_metadata = false;
  ASSERT_FALSE(expected_budget.ok());
  ASSERT_EQ(expected_budget.status().code(), QueryError::ResourceExhausted);
  const auto routes_before = frontend->size();
  std::size_t authorizations_before;
  {
    std::lock_guard<std::mutex> lock(mutex);
    block_hdf5 = true;
    authorizations_before = hdf5_authorizations;
  }
  std::vector<std::future<QueryResult<QueryControlResponse>>> waiters;
  waiters.push_back(std::async(std::launch::async, [&] {
    return submit_query_control(socket, hdf5, std::chrono::seconds(10));
  }));
  bool entered;
  {
    std::unique_lock<std::mutex> lock(mutex);
    entered = changed.wait_for(lock, std::chrono::seconds(3), [&] {
      return hdf5_authorizations == authorizations_before + 1;
    });
  }
  for (int i = 0; i < 3; ++i)
    waiters.push_back(std::async(std::launch::async, [&] {
      return submit_query_control(socket, hdf5, std::chrono::seconds(10));
    }));
  waiters.push_back(std::async(std::launch::async, [&] {
    return submit_query_control(socket, mixed, std::chrono::seconds(10));
  }));
  waiters.push_back(std::async(std::launch::async, [&] {
    return submit_query_control(socket, invalid_mixed, std::chrono::seconds(10));
  }));
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    const auto stats = runtime->query_tasks().stats();
    if (stats.queued == 5 && stats.resource_blocked == 5) break;
    std::this_thread::yield();
  }
  const auto occupied = runtime->query_tasks().stats();
  // These calls must complete while the first HDF5 task still holds the
  // resource. Socket timeouts keep a broken implementation testable.
  auto arrow = submit_query_control(socket, kArrowValues, std::chrono::seconds(2));
  auto arrow_batch = submit_query_control(socket, typed_batch({kArrowValues, kArrowValues}),
    std::chrono::seconds(2));
  charge_arrow_metadata = true;
  auto exhausted = submit_query_control(socket, typed_batch({kArrowValues, kArrowValues}),
    std::chrono::seconds(2));
  charge_arrow_metadata = false;
  std::size_t authorizations_during;
  {
    std::lock_guard<std::mutex> lock(mutex);
    authorizations_during = hdf5_authorizations;
    release = true;
  }
  changed.notify_all();
  // No fatal assertions before releasing the gate: future destructors must
  // never leave a failing test waiting behind its own held resource.
  EXPECT_TRUE(entered);
  EXPECT_EQ(occupied.active, 1u);
  EXPECT_EQ(occupied.queued, 5u);
  EXPECT_GT(occupied.resource_blocked, 0u);
  EXPECT_EQ(authorizations_during, authorizations_before + 1);
  EXPECT_TRUE(arrow.ok()) << arrow.status().message();
  if (arrow.ok()) {
    auto bytes = socket_result_bytes(bridge, arrow.value().path);
    EXPECT_EQ(bytes, expected_bytes);
    expect_tiny_arrow_values(std::move(bytes));
  }
  EXPECT_TRUE(arrow_batch.ok()) << arrow_batch.status().message();
  if (arrow_batch.ok()) {
    EXPECT_EQ(arrow_batch.value().items.size(), 2u);
    for (const auto& item : arrow_batch.value().items)
      EXPECT_EQ(socket_result_bytes(bridge, item.path), expected_bytes);
  }
  EXPECT_FALSE(exhausted.ok());
  EXPECT_EQ(exhausted.status().code(), expected_budget.status().code());
  EXPECT_EQ(exhausted.status().phase(), expected_budget.status().phase());
  EXPECT_EQ(exhausted.status().message(), expected_budget.status().message());
  for (std::size_t i = 0; i < waiters.size(); ++i) {
    auto actual = waiters[i].get();
    const auto& expected = i < 4 ? expected_hdf5 : i == 4 ? expected_mixed : expected_invalid;
    EXPECT_FALSE(actual.ok());
    EXPECT_EQ(actual.status().code(), expected.status().code());
    EXPECT_EQ(actual.status().phase(), expected.status().phase());
    EXPECT_EQ(actual.status().message(), expected.status().message());
  }
  server.stop();
  EXPECT_EQ(frontend->size(), routes_before + (arrow.ok() ? 1u : 0u) +
    (arrow_batch.ok() ? arrow_batch.value().items.size() : 0u));
  EXPECT_EQ(runtime->query_tasks().stats().active, 0u);
  EXPECT_EQ(runtime->query_tasks().stats().queued, 0u);
}

TEST_F(SocketFixture, IndexedHdf5RequestsDoNotReserveTheNativeHdf5Resource) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
        "indexed authorization oracle");
    });
  ASSERT_TRUE(server.start().ok());
  auto expected = submit_query_control(socket, hdf5_only_typed());
  ASSERT_FALSE(expected.ok());
  auto held = hdf5_query_task_resource()->try_acquire();
  ASSERT_TRUE(held);
  auto actual = submit_query_control(socket, hdf5_only_typed(), std::chrono::seconds(2));
  held = {};
  EXPECT_FALSE(actual.ok());
  EXPECT_EQ(actual.status().code(), expected.status().code());
  EXPECT_EQ(actual.status().phase(), expected.status().phase());
  EXPECT_EQ(actual.status().message(), expected.status().message());
  EXPECT_EQ(runtime->query_tasks().stats().resource_waits, 0u);
  server.stop();
}

TEST_F(SocketFixture, ExternalNativeHdf5MutexLeavesSingleWorkerAvailableForArrow) {
  const auto native_image = [&] {
    struct Hdf5Id {
      Hdf5Id(hid_t value, herr_t (*close)(hid_t)) : value(value), close(close) {
        if (value < 0) throw std::runtime_error("cannot create native resource test fixture");
      }
      ~Hdf5Id() { close(value); }
      Hdf5Id(const Hdf5Id&) = delete;
      operator hid_t() const { return value; }
      hid_t value;
      herr_t (*close)(hid_t);
    };
    auto check = [](herr_t status) {
      if (status < 0) throw std::runtime_error("native resource fixture operation failed");
    };
    // Build the valid source image before starting any scheduled HDF5 work.
    // The core driver keeps this fixture entirely in memory.
    Hdf5Id access(H5Pcreate(H5P_FILE_ACCESS), H5Pclose);
    check(H5Pset_fapl_core(access, 4096, false));
    Hdf5Id file(H5Fcreate((directory + "/native-resource.h5").c_str(),
      H5F_ACC_TRUNC, H5P_DEFAULT, access), H5Fclose);
    {
      const hsize_t shape[]{4};
      Hdf5Id space(H5Screate_simple(1, shape, nullptr), H5Sclose);
      Hdf5Id dataset(H5Dcreate2(file, "/d", H5T_STD_U32LE, space,
        H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT), H5Dclose);
      const std::uint32_t values[]{0, 1, 2, 3};
      check(H5Dwrite(dataset, H5T_NATIVE_UINT32, H5S_ALL, H5S_ALL, H5P_DEFAULT, values));
    }
    check(H5Fflush(file, H5F_SCOPE_GLOBAL));
    const auto size = H5Fget_file_image(file, nullptr, 0);
    if (size <= 0) throw std::runtime_error("cannot size native resource fixture image");
    BinaryValue bytes(static_cast<std::size_t>(size));
    if (H5Fget_file_image(file, bytes.data(), bytes.size()) != size)
      throw std::runtime_error("cannot read native resource fixture image");
    return bytes;
  }();
  QueryRuntimeConfig config;
  config.query_tasks = {1, 16, 1};
  auto execution_runtime = std::make_shared<QueryRuntime>(config);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, execution_runtime, bridge, [&](const auto&, const auto& sources, const auto*) {
      return sources.empty() ? QueryStatus::failure(QueryError::PermissionDenied,
        QueryPhase::Catalog, "native HDF5 authorization oracle") : QueryStatus::success();
    }, 0600, TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{},
    Hdf5QueryKernelConfig{}, QueryRegistrationSocketServer::DatasetDiscovery{},
    std::shared_ptr<Hdf5ValidatedLayoutCache>{},
    std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
    QueryRegistrationSocketServer::BeginAuthorizationPublication{}, false,
    Hdf5QueryMode::NativeHdf5);
  ASSERT_TRUE(server.start().ok());
  const auto hdf5 = hdf5_only_typed();
  auto expected_hdf5 = submit_query_control(socket, hdf5);
  ASSERT_FALSE(expected_hdf5.ok());
  auto serial = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(serial.ok()) << serial.status().message();
  const auto expected_bytes = socket_result_bytes(bridge, serial.value().path);
  expect_tiny_arrow_values(expected_bytes);
  std::mutex mutex;
  std::condition_variable changed;
  bool native_entered = false, release = false;
  auto native = std::async(std::launch::async, [&] {
    // This public ingestion boundary holds the real process-wide native
    // library mutex while opening the valid image and throughout operation().
    // The operation then exercises admission against an external mutex owner,
    // independently of the scheduler's logical lease count.
    FormatSource source{native_image.size(), [&](std::uint64_t offset,
        std::uint64_t length) -> std::optional<BinaryValue> {
      if (offset > native_image.size() || length > native_image.size() - offset)
        return std::nullopt;
      return BinaryValue(native_image.begin() + offset, native_image.begin() + offset + length);
    }};
    return with_hdf5_ingestion_source(source, {}, [&](const Hdf5SourceDriver&) {
      std::unique_lock<std::mutex> lock(mutex);
      native_entered = true;
      changed.notify_all();
      changed.wait(lock, [&] { return release; });
      return QueryStatus::success();
    });
  });
  bool held;
  {
    std::unique_lock<std::mutex> lock(mutex);
    held = changed.wait_for(lock, std::chrono::seconds(3), [&] { return native_entered; });
  }
  const auto logical_in_use = hdf5_query_task_resource()->in_use();
  std::vector<std::future<QueryResult<QueryControlResponse>>> waiters;
  for (int i = 0; i < 4; ++i)
    waiters.push_back(std::async(std::launch::async, [&] {
      return submit_query_control(socket, hdf5, std::chrono::seconds(10));
    }));
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    const auto stats = execution_runtime->query_tasks().stats();
    if (stats.queued == 4 && stats.resource_blocked == 4) break;
    std::this_thread::yield();
  }
  const auto occupied = execution_runtime->query_tasks().stats();
  auto arrow = submit_query_control(socket, kArrowValues, std::chrono::seconds(2));
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  changed.notify_all();
  const auto native_status = native.get();
  EXPECT_TRUE(held);
  EXPECT_TRUE(native_status.ok()) << native_status.message();
  EXPECT_EQ(logical_in_use, 0u);
  EXPECT_EQ(occupied.active, 0u);
  EXPECT_EQ(occupied.queued, 4u);
  EXPECT_EQ(occupied.resource_blocked, 4u);
  EXPECT_TRUE(arrow.ok()) << arrow.status().message();
  if (arrow.ok()) {
    auto actual = socket_result_bytes(bridge, arrow.value().path);
    EXPECT_EQ(actual, expected_bytes);
    expect_tiny_arrow_values(std::move(actual));
  }
  for (auto& waiter : waiters) {
    auto actual = waiter.get();
    EXPECT_FALSE(actual.ok());
    EXPECT_EQ(actual.status().code(), expected_hdf5.status().code());
    EXPECT_EQ(actual.status().phase(), expected_hdf5.status().phase());
    EXPECT_EQ(actual.status().message(), expected_hdf5.status().message());
  }
  server.stop();
  EXPECT_EQ(execution_runtime->query_tasks().stats().active, 0u);
  EXPECT_EQ(execution_runtime->query_tasks().stats().queued, 0u);
}

// These clients supply independent requests; all execution concurrency is
// chosen by the server's runtime, not by a client-side query executor.
TEST_F(SocketFixture, CommonPoolBoundsActualRequestConcurrencyAtOneTwoAndFour) {
  std::optional<BinaryValue> serial_bytes;
  for (const std::size_t parallelism : {1u, 2u, 4u}) {
    QueryRuntimeConfig config;
    config.query_tasks = {parallelism, 16, parallelism};
    auto execution_runtime = std::make_shared<QueryRuntime>(config);
    std::mutex mutex;
    std::condition_variable changed;
    std::size_t entered = 0;
    bool release = false;
    QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
      storage, execution_runtime, bridge,
      [&](const auto&, const auto&, const auto*) {
        std::unique_lock<std::mutex> lock(mutex);
        ++entered;
        changed.notify_all();
        changed.wait(lock, [&] { return release; });
        return QueryStatus::success();
      });
    ASSERT_TRUE(server.start().ok());
    std::vector<std::future<QueryResult<std::string>>> clients;
    for (std::size_t i = 0; i < parallelism + 2; ++i)
      clients.push_back(std::async(std::launch::async, [&] {
        return submit_query_registration(socket, kTyped);
      }));
    bool all_slots_running;
    {
      std::unique_lock<std::mutex> lock(mutex);
      all_slots_running = changed.wait_for(lock, std::chrono::seconds(3),
        [&] { return entered == parallelism; });
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (execution_runtime->query_tasks().stats().queued < 2 &&
        std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
    const auto occupied = execution_runtime->query_tasks().stats();
    {
      std::lock_guard<std::mutex> lock(mutex);
      release = true;
    }
    changed.notify_all();
    for (auto& client : clients) {
      auto result = client.get();
      EXPECT_TRUE(result.ok()) << result.status().message();
      if (result.ok()) {
        const auto marker = result.value().rfind("&result=");
        EXPECT_NE(marker, std::string::npos);
        if (marker == std::string::npos) continue;
        const auto route = result.value().substr(std::string("/mnt/data").size(),
          marker - std::string("/mnt/data").size());
        auto opened = bridge->acquire_exact(route, result.value().substr(marker + 8), {});
        EXPECT_TRUE(opened.ok()) << opened.status().message();
        if (opened.ok()) {
          auto bytes = opened.value()->read(0, opened.value()->size());
          EXPECT_TRUE(bytes.ok());
          if (bytes.ok()) {
            EXPECT_FALSE(bytes.value().empty());
            if (!serial_bytes) serial_bytes = bytes.value();
            EXPECT_EQ(bytes.value(), *serial_bytes);
          }
        }
      }
    }
    server.stop();
    EXPECT_TRUE(all_slots_running);
    EXPECT_EQ(occupied.active, parallelism);
    EXPECT_EQ(occupied.queued, 2u);
    const auto final = execution_runtime->query_tasks().stats();
    EXPECT_EQ(final.submitted, parallelism + 2);
    EXPECT_EQ(final.completed, parallelism + 2);
    EXPECT_EQ(final.peak_active, parallelism);
    EXPECT_EQ(final.active, 0u);
    EXPECT_EQ(final.queued, 0u);
  }
}

TEST_F(SocketFixture, CommonPoolFullQueueReturnsStructuredErrorAndDrainsOnStop) {
  QueryRuntimeConfig config;
  config.query_tasks = {1, 1, 1};
  auto execution_runtime = std::make_shared<QueryRuntime>(config);
  std::mutex mutex;
  std::condition_variable changed;
  bool entered = false, release = false;
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, execution_runtime, bridge,
    [&](const auto&, const auto&, const auto*) {
      std::unique_lock<std::mutex> lock(mutex);
      entered = true;
      changed.notify_all();
      changed.wait(lock, [&] { return release; });
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto first = std::async(std::launch::async, [&] { return submit_query_control(socket, kTyped); });
  bool running;
  {
    std::unique_lock<std::mutex> lock(mutex);
    running = changed.wait_for(lock, std::chrono::seconds(3), [&] { return entered; });
  }
  auto second = std::async(std::launch::async, [&] { return submit_query_control(socket, kTyped); });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (execution_runtime->query_tasks().stats().queued != 1 &&
      std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
  const bool queued = execution_runtime->query_tasks().stats().queued == 1;
  auto rejected = submit_query_control(socket, kTyped, std::chrono::seconds(2));
  auto stopped = std::async(std::launch::async, [&] { server.stop(); });
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
  }
  changed.notify_all();
  EXPECT_TRUE(first.get().ok());
  EXPECT_TRUE(second.get().ok());
  stopped.get();
  EXPECT_TRUE(running);
  EXPECT_TRUE(queued);
  EXPECT_EQ(rejected.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(execution_runtime->query_tasks().stats().rejected, 1u);
  // Stopping one socket must leave a shared runtime usable by other callers.
  auto next = execution_runtime->query_tasks().submit({[](const CancellationToken&) {
    return QueryStatus::success();
  }});
  ASSERT_TRUE(next.ok());
  EXPECT_TRUE(next.value().get().ok());
}

TEST(QueryBatchBudget, CumulativeLimitsAreOverflowSafeAndSticky) {
  QueryLimits limits;
  limits.max_ranges = 2;
  limits.max_layout_bytes = 5;
  limits.max_payload_bytes = 8;
  limits.max_result_rows = 3;
  QueryBatchBudget reads(limits);
  EXPECT_TRUE(reads.reserve_read(true, 3).ok());
  EXPECT_TRUE(reads.reserve_read(false, 8).ok());
  EXPECT_EQ(reads.reserve_read(false, 0).code(), QueryError::ResourceExhausted);
  EXPECT_FALSE(reads.reserve_rows(0).ok());
  QueryBatchBudget layout(limits);
  EXPECT_TRUE(layout.reserve_read(true, 3).ok());
  EXPECT_FALSE(layout.reserve_read(true, 3).ok());
  QueryBatchBudget payload(limits);
  EXPECT_FALSE(payload.reserve_read(false, UINT64_MAX).ok());
  QueryBatchBudget rows(limits);
  EXPECT_TRUE(rows.reserve_rows(2).ok());
  EXPECT_FALSE(rows.reserve_rows(2).ok());
  QueryBatchBudget encoded(limits);
  EXPECT_TRUE(encoded.reserve_encoded(4, 5).ok());
  EXPECT_EQ(encoded.remaining_encoded(5), 1u);
  EXPECT_FALSE(encoded.reserve_encoded(2, 5).ok());
}

TEST_F(SocketFixture, TypedBatchReturnsOrderedEquivalentResultsAt64ItemBoundary) {
  std::atomic<std::size_t> checks{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      ++checks; return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto rejected = submit_query_control(socket, typed_batch(std::vector<std::string>(65, kTyped)));
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(checks.load(), 0u);
  EXPECT_EQ(frontend->size(), 0u);
  auto accepted = submit_query_control(socket, typed_batch(std::vector<std::string>(64, kTyped)));
  ASSERT_TRUE(accepted.ok()) << accepted.status().message();
  EXPECT_EQ(accepted.value().kind, QueryControlResponse::Kind::PublishedRoutes);
  ASSERT_EQ(accepted.value().items.size(), 64u);
  EXPECT_EQ(checks.load(), 64u);
  BinaryValue expected;
  for (std::size_t i = 0; i < accepted.value().items.size(); ++i) {
    const auto& item = accepted.value().items[i];
    EXPECT_EQ(item.index, i);
    const auto marker = item.path.rfind("&result=");
    ASSERT_NE(marker, std::string::npos);
    auto opened = bridge->acquire_exact(item.path.substr(9, marker - 9),
      item.path.substr(marker + 8), {});
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    auto bytes = opened.value()->read(0, opened.value()->size());
    ASSERT_TRUE(bytes.ok());
    if (!i) expected = bytes.value();
    else EXPECT_EQ(bytes.value(), expected);
  }
  // The path-only convenience API must reject before creating batch routes.
  EXPECT_FALSE(submit_query_registration(socket, typed_batch({kTyped})).ok());
  EXPECT_EQ(frontend->size(), 64u);
}

TEST_F(SocketFixture, TypedBatchPrevalidatesAllItemsAndRequiresExactVersions) {
  std::atomic<std::size_t> checks{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      ++checks; return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  std::string latest = kTyped;
  latest.replace(latest.find("\"exact\",\"exact\":7"),
    std::string("\"exact\",\"exact\":7").size(), "\"latest\"");
  for (const auto& json : {typed_batch({}), typed_batch({kTyped, "{\"kind\":\"window\"}"}),
      typed_batch({kTyped, latest}), typed_batch({kTyped, "{\"kind\":\"typed\",\"request\":{}}"})}) {
    EXPECT_FALSE(submit_query_control(socket, json).ok());
    EXPECT_EQ(frontend->size(), 0u);
    EXPECT_EQ(bridge->cached_routes(), 0u);
    EXPECT_EQ(checks.load(), 0u);
  }
  auto tree = boost::property_tree::ptree{};
  std::istringstream input(kTyped);
  boost::property_tree::read_json(input, tree);
  boost::property_tree::ptree paths;
  for (int i = 0; i < 1025; ++i) {
    boost::property_tree::ptree path;
    path.put("kind", "exact"); path.put("path", "/source/" + std::to_string(i));
    paths.push_back({"", path});
  }
  tree.put_child("request.sources.paths", paths);
  tree.put("request.limits.max_sources", 2048);
  std::ostringstream large; boost::property_tree::write_json(large, tree, false);
  auto exhausted = submit_query_control(socket, typed_batch({large.str()}));
  ASSERT_FALSE(exhausted.ok());
  EXPECT_EQ(exhausted.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(checks.load(), 0u);
}

TEST_F(SocketFixture, TypedBatchAuthorizationFailureRetiresEarlierItems) {
  std::atomic<std::size_t> checks{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      if (++checks == 2) return QueryStatus::failure(QueryError::PermissionDenied,
        QueryPhase::Catalog, "second item denied");
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto denied = submit_query_control(socket, typed_batch({kTyped, kTyped}));
  ASSERT_FALSE(denied.ok());
  EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(checks.load(), 2u);
  EXPECT_EQ(frontend->size(), 0u);
  EXPECT_EQ(bridge->cached_routes(), 0u);
}

TEST_F(SocketFixture, TypedBatchSharesMetadataLedgerWithoutOwnerMode) {
  std::atomic<std::size_t> checks{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      ++checks;
      require_query_metadata_unique(40ULL * 1024 * 1024);
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto exhausted = submit_query_control(socket, typed_batch({kTyped, kTyped}));
  ASSERT_FALSE(exhausted.ok());
  EXPECT_EQ(exhausted.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(checks.load(), 2u);
  EXPECT_EQ(frontend->size(), 0u);
  EXPECT_EQ(bridge->cached_routes(), 0u);
}

TEST_F(SocketFixture, TypedBatchEncodedCeilingIsSharedAndFailureRetiresEveryRoute) {
  std::uint64_t one_size = 0;
  {
    QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
      storage, runtime, bridge);
    ASSERT_TRUE(server.start().ok());
    auto single = submit_query_control(socket, kTyped);
    ASSERT_TRUE(single.ok()) << single.status().message();
    const auto marker = single.value().path.rfind("&result=");
    const auto route = single.value().path.substr(9, marker - 9);
    auto opened = bridge->acquire_exact(route, single.value().path.substr(marker + 8), {});
    ASSERT_TRUE(opened.ok());
    one_size = opened.value()->size();
    const auto id_marker = route.find("?id=");
    ASSERT_NE(id_marker, std::string::npos);
    EXPECT_TRUE(frontend->erase(route.substr(id_marker + 4, 48)));
    bridge->drop(route);
  }
  auto query = QueryModule::create(catalog, storage,
    {std::make_shared<ArrowFormatQueryAdapter>(ArrowDecodeConfig{}, runtime)});
  ASSERT_TRUE(query.ok());
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto service = std::make_shared<TypedQueryService>(query.value(), frontend,
    results, one_size * 2 - 1);
  bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto failed = submit_query_control(socket, typed_batch({kTyped, kTyped}));
  ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(frontend->size(), 0u);
  EXPECT_EQ(bridge->cached_routes(), 0u);
  EXPECT_EQ(results->stats().open_handles, 0u);
  // No failed-batch state leaks into an independent request.
  auto next = submit_query_control(socket, typed_batch({kTyped}));
  EXPECT_TRUE(next.ok()) << next.status().message();
}

TEST_F(SocketFixture, TypedBatchLostResponseRetiresAllPublishedPaths) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const auto&, const auto&, const auto*) {
      std::this_thread::sleep_for(std::chrono::milliseconds(60));
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto timed_out = submit_query_control(socket, typed_batch({kTyped, kTyped}),
    std::chrono::milliseconds(10));
  EXPECT_FALSE(timed_out.ok());
  server.stop();
  EXPECT_EQ(frontend->size(), 0u);
  EXPECT_EQ(bridge->cached_routes(), 0u);
}

TEST_F(SocketFixture, TypedBatchMatchesSerialAndEmitsOneEnclosingProfile) {
  const auto profile_path = directory + "/batch-profile.jsonl";
  ASSERT_EQ(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", profile_path.c_str(), 1), 0);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  ::unsetenv("IOTDBFS_QUERY_PROFILE_JSONL");
  ASSERT_TRUE(server.start().ok());
  auto serial = submit_query_control(socket, kTyped);
  ASSERT_TRUE(serial.ok());
  auto batch = submit_query_control(socket, typed_batch({kTyped, kTyped}));
  ASSERT_TRUE(batch.ok());
  auto bytes = [&](const std::string& path) {
    const auto marker = path.rfind("&result=");
    auto opened = bridge->acquire_exact(path.substr(9, marker - 9), path.substr(marker + 8), {});
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    return opened.value()->read(0, opened.value()->size()).value();
  };
  const auto expected = bytes(serial.value().path);
  for (const auto& item : batch.value().items) EXPECT_EQ(bytes(item.path), expected);
  server.stop();
  std::ifstream input(profile_path);
  std::string line;
  std::size_t lines = 0, batches = 0;
  while (std::getline(input, line)) {
    ++lines;
    boost::property_tree::ptree profile;
    std::istringstream json(line); boost::property_tree::read_json(json, profile);
    if (profile.get<std::string>("operation") != "typed_batch") continue;
    ++batches;
    EXPECT_EQ(profile.get<std::size_t>("counters.typed_batch_items"), 2u);
    EXPECT_EQ(profile.get_child("items").size(), 2u);
    EXPECT_FALSE(profile.get<bool>("server_hdf5_execution.native_cache_enabled"));
    EXPECT_EQ(profile.get<std::uint64_t>("server_hdf5_execution.native_raw_chunk_bytes_per_dataset"), 0u);
  }
  EXPECT_EQ(lines, 2u);
  EXPECT_EQ(batches, 1u);
  ::unlink(profile_path.c_str());
}

constexpr const char* kWindow = R"({
    "kind":"window",
    "snapshot":{
      "dataset_id":"tiny",
      "selector":{
        "paths":[{"kind":"exact","path":"/dataset/a.arrow"}],
        "versions":{"kind":"exact","exact":7},
        "formats":{"adapters":"listed","adapter_ids":["arrow"],"include_byte_passthrough":false}
      },
      "shards":[{"filename":"a.arrow","path":"/dataset/a.arrow","version":7}],
      "rows":[{
        "logical_row":0,"shard_index":0,"batch_index":0,"row_index":0,
        "series_id":"series","steps":{"begin":0,"end":4},
        "declared_n_channels":1,"declared_length":4,
        "channels":[{"field_id":"target","original_ordinal":0,
          "physical_ordinal":0,"label":"x","length":4}]
      }]
    },
    "view":{
      "view_id":"tiny-view","experiment_id":"test",
      "input_channels":[{"field_id":"target","ordinal":0,"label":"x"}],
      "target_channels":[{"field_id":"target","ordinal":0,"label":"x"}],
      "input_length":2,"gap":0,"target_length":1,"stride":1,
      "split":{"split_id":"train","permitted_input":{"begin":0,"end":4},
        "permitted_target":{"begin":0,"end":4}}
    },
    "request":{"coordinates":[{"series_id":"series","anchor":2}],"max_windows":1}
  })";

boost::property_tree::ptree window_tree(bool prepare = true) {
  std::istringstream input(kWindow);
  boost::property_tree::ptree tree;
  boost::property_tree::read_json(input, tree);
  if (prepare) { tree.put("kind", "window_prepare"); tree.erase("request"); }
  return tree;
}
boost::property_tree::ptree hdf5_typed_tree() {
  std::istringstream input(R"({"kind":"typed","request":{
    "sources":{"paths":[{"kind":"exact","path":"/dataset/a.arrow"}],
      "versions":{"kind":"exact","exact":7}},
    "subset":{"projection":["hdf5.value:%2Fa"],"slices":[{
      "dimension":"hdf5.axis.0",
      "lower":{"kind":"inclusive","value":{"type":"uint64","value":1}},
      "upper":{"kind":"exclusive","value":{"type":"uint64","value":5}}
    }]}}})");
  boost::property_tree::ptree tree;
  boost::property_tree::read_json(input, tree);
  return tree;
}
std::string json_tree(const boost::property_tree::ptree& tree) {
  std::ostringstream output;
  boost::property_tree::write_json(output, tree, false);
  return output.str();
}
std::string execute_prepared(const std::string& id) {
  auto tree = window_tree(false);
  boost::property_tree::ptree execute;
  execute.put("kind", "window_execute");
  execute.put("prepared_id", id);
  execute.add_child("request", tree.get_child("request"));
  return json_tree(execute);
}
std::string release_prepared(const std::string& id, const std::string& path = "") {
  boost::property_tree::ptree tree;
  tree.put("kind", path.empty() ? "window_release" : "window_result_release");
  tree.put("prepared_id", id);
  if (!path.empty()) tree.put("path", path);
  return json_tree(tree);
}
boost::property_tree::ptree compact_window_tree() {
  auto tree = window_tree();
  auto& row = tree.get_child("snapshot.rows").front().second;
  auto channels = row.get_child("channels");
  channels.front().second.erase("length");
  boost::property_tree::ptree definition, definitions;
  definition.put("id", "shared");
  definition.add_child("channels", channels);
  definitions.push_back({"", definition});
  tree.add_child("snapshot.channel_tables", definitions);
  row.erase("channels");
  row.put("channel_table_id", "shared");
  return tree;
}

TEST_F(SocketFixture, RejectsRemovedFrozenTypedExperimentKinds) {
  std::atomic<unsigned> authorizations{0}, publications{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage,
    runtime, bridge, [&](const auto&, const auto&, const auto*) {
      ++authorizations;
      return QueryStatus::success();
    }, 0600, {}, {}, {}, {}, {}, {},
    [&](const auto&, const auto*) {
      ++publications;
      return QueryRegistrationSocketServer::AuthorizationPublicationCommit{};
    });
  ASSERT_TRUE(server.start().ok());
  const std::vector<std::string> requests{
    R"({"kind":"experiment_typed_bind","request":{
      "sources":{"paths":[{"kind":"exact","path":"/dataset/a.arrow"}],
        "versions":{"kind":"exact","exact":7},
        "formats":{"adapters":"listed","adapter_ids":["arrow"],"include_byte_passthrough":false}},
      "subset":{"projection":["index","value.0"],"physical_array":{
        "field_id":"target","record_batch_index":0,"row_index":0,
        "values":[{"child_indices":[0]}]}}}})",
    R"({"kind":"experiment_typed_read","prepared_id":"000000000000000000000000000000000000000000000000"})",
    R"({"kind":"experiment_typed_release","prepared_id":"000000000000000000000000000000000000000000000000"})"
  };
  for (const auto& request : requests) {
    SCOPED_TRACE(request);
    auto result = submit_query_control(socket, request);
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), QueryError::InvalidQuery);
    EXPECT_EQ(frontend->size(), 0u);
    EXPECT_EQ(bridge->cached_routes(), 0u);
  }
  EXPECT_EQ(authorizations.load(), 0u);
  EXPECT_EQ(publications.load(), 0u);
}

TEST_F(SocketFixture, PreparedWindowsReuseMetadataAndReleaseOwnedResults) {
  auto backend = std::make_shared<TinyBackend>();
  storage = std::make_shared<StructuredBlobStorage>(backend);
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  std::atomic<unsigned> authorizations{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer&, const auto&, const CatalogSelection* selected) {
      EXPECT_EQ(selected, nullptr);
      ++authorizations; return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto prepared = submit_query_control(socket, json_tree(window_tree()));
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(prepared.value().kind, QueryControlResponse::Kind::PreparedWindow);
  EXPECT_EQ(prepared.value().prepared_id.size(), 48u);
  EXPECT_EQ(prepared.value().ttl_ms, 300000u);
  EXPECT_TRUE(prepared.value().path.empty());
  auto first = submit_query_control(socket, execute_prepared(prepared.value().prepared_id));
  ASSERT_TRUE(first.ok()) << first.status().message();
  const auto result_marker = first.value().path.rfind("&result=");
  const auto first_route = first.value().path.substr(9, result_marker - 9);
  auto opened = bridge->acquire_exact(first_route, first.value().path.substr(result_marker + 8), {});
  ASSERT_TRUE(opened.ok());
  const auto reads_after_first = backend->layout_reads.load();
  auto second = submit_query_control(socket, execute_prepared(prepared.value().prepared_id));
  ASSERT_TRUE(second.ok()) << second.status().message();
  // The prepared assembler reuses the verified immutable layout. Each
  // execution still validates the source manifest and authorization.
  EXPECT_EQ(backend->layout_reads.load(), reads_after_first);
  EXPECT_EQ(authorizations.load(), 3u); // Prepare plus a fresh check for each execution.
  auto wrong = submit_query_control(socket, release_prepared("another-context", first.value().path));
  ASSERT_TRUE(wrong.ok()); EXPECT_FALSE(wrong.value().released);
  EXPECT_EQ(frontend->size(), 2u);
  auto released = submit_query_control(socket, release_prepared(prepared.value().prepared_id, first.value().path));
  ASSERT_TRUE(released.ok()); EXPECT_TRUE(released.value().released);
  EXPECT_EQ(frontend->size(), 1u);
  EXPECT_TRUE(opened.value()->read(0, 8).ok()); // Open handles survive route retirement.
  EXPECT_FALSE(bridge->acquire_exact(first_route, opened.value()->id(), {}).ok());
  auto closed = submit_query_control(socket, release_prepared(prepared.value().prepared_id));
  ASSERT_TRUE(closed.ok()); EXPECT_TRUE(closed.value().released);
  EXPECT_EQ(frontend->size(), 0u);
  EXPECT_FALSE(submit_query_control(socket, execute_prepared(prepared.value().prepared_id)).ok());
  auto again = submit_query_control(socket, release_prepared(prepared.value().prepared_id));
  ASSERT_TRUE(again.ok()); EXPECT_FALSE(again.value().released);
}

TEST_F(SocketFixture, PreparedWindowsRejectRequestOverridesAndChangedSource) {
  auto backend = std::make_shared<TinyBackend>();
  storage = std::make_shared<StructuredBlobStorage>(backend);
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto prepared = submit_query_control(socket, json_tree(window_tree()));
  ASSERT_TRUE(prepared.ok());
  std::string bad = execute_prepared(prepared.value().prepared_id);
  bad.insert(1, R"("limits":{},)");
  auto rejected = submit_query_control(socket, bad);
  ASSERT_FALSE(rejected.ok()); EXPECT_EQ(rejected.status().code(), QueryError::InvalidQuery);
  ++backend->manifest.revision;
  auto changed = submit_query_control(socket, execute_prepared(prepared.value().prepared_id));
  EXPECT_FALSE(changed.ok());
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, TimedOutPreparedResponseRefundsRegistrySlot) {
  PreparedTrainingRegistry::Config config;
  config.max_entries = 1;
  std::atomic<bool> slow{true};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer&, const auto&, const CatalogSelection*) {
      if (slow.exchange(false)) std::this_thread::sleep_for(std::chrono::milliseconds(80));
      return QueryStatus::success();
    }, 0600, {}, config);
  ASSERT_TRUE(server.start().ok());
  auto timed_out = submit_query_control(socket, json_tree(window_tree()), std::chrono::milliseconds(10));
  ASSERT_FALSE(timed_out.ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(120));
  auto prepared = submit_query_control(socket, json_tree(window_tree()));
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
}

TEST_F(SocketFixture, PathOnlyClientRejectsPreparationBeforeSending) {
  PreparedTrainingRegistry::Config config;
  config.max_entries = 1;
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, {}, 0600, {}, config);
  ASSERT_TRUE(server.start().ok());
  EXPECT_FALSE(submit_query_registration(socket, json_tree(window_tree())).ok());
  EXPECT_TRUE(submit_query_control(socket, json_tree(window_tree())).ok());
}

TEST_F(SocketFixture, PreparedExecuteProfileOmitsRepeatedPreparationStages) {
  const auto profile_path = directory + "/prepared-profile.jsonl";
  const char* previous = ::getenv("IOTDBFS_QUERY_PROFILE_JSONL");
  const bool had_previous = previous != nullptr;
  const std::string previous_value = previous ? previous : "";
  ASSERT_EQ(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", profile_path.c_str(), 1), 0);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
  if (had_previous) ::setenv("IOTDBFS_QUERY_PROFILE_JSONL", previous_value.c_str(), 1);
  else ::unsetenv("IOTDBFS_QUERY_PROFILE_JSONL");
  ASSERT_TRUE(server.start().ok());
  auto prepared = submit_query_control(socket, json_tree(window_tree()));
  ASSERT_TRUE(prepared.ok());
  ASSERT_TRUE(submit_query_control(socket, execute_prepared(prepared.value().prepared_id)).ok());
  ASSERT_TRUE(submit_query_control(socket, release_prepared(prepared.value().prepared_id)).ok());
  server.stop();
  std::ifstream input(profile_path);
  std::string line;
  unsigned seen = 0;
  while (std::getline(input, line)) {
    std::istringstream json(line);
    boost::property_tree::ptree profile;
    boost::property_tree::read_json(json, profile);
    EXPECT_EQ(profile.get<std::string>("prepared_id"), prepared.value().prepared_id);
    const auto operation = profile.get<std::string>("operation");
    if (operation == "window_execute") {
      EXPECT_FALSE(profile.get_child_optional("stages_ns.snapshot_prepare"));
      EXPECT_FALSE(profile.get_child_optional("stages_ns.coverage_index_prepare"));
      EXPECT_FALSE(profile.get<std::string>("path").empty());
    } else EXPECT_FALSE(profile.get_optional<std::string>("path"));
    ++seen;
  }
  EXPECT_EQ(seen, 3u);
  ::unlink(profile_path.c_str());
}

TEST_F(SocketFixture, PreparedRegistryReapsIdleIdsAndRoutesWithoutNewClientTraffic) {
  PreparedTrainingRegistry::Config config;
  config.ttl = std::chrono::milliseconds(30);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, {}, 0600, {}, config);
  ASSERT_TRUE(server.start().ok());
  auto prepared = submit_query_control(socket, json_tree(window_tree()));
  ASSERT_TRUE(prepared.ok());
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  auto expired = submit_query_control(socket, execute_prepared(prepared.value().prepared_id));
  ASSERT_FALSE(expired.ok()); EXPECT_EQ(expired.status().code(), QueryError::SourceNotFound);
}

TEST_F(SocketFixture, CompactChannelTablesMatchExplicitSnapshotAndRejectMalformedTables) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto explicit_form = submit_query_control(socket, json_tree(window_tree()));
  auto compact = submit_query_control(socket, json_tree(compact_window_tree()));
  ASSERT_TRUE(explicit_form.ok()); ASSERT_TRUE(compact.ok()) << compact.status().message();
  EXPECT_EQ(explicit_form.value().snapshot_fingerprint, compact.value().snapshot_fingerprint);
  auto result = submit_query_control(socket, execute_prepared(compact.value().prepared_id));
  ASSERT_TRUE(result.ok()) << result.status().message();
  for (unsigned mode = 0; mode < 7; ++mode) {
    auto tree = compact_window_tree();
    auto& row = tree.get_child("snapshot.rows").front().second;
    auto& tables = tree.get_child("snapshot.channel_tables");
    if (mode == 0) row.put("channel_table_id", "missing");
    if (mode == 6) row.put("value_layout", "unknown");
    if (mode == 1) tables.push_back({"", tables.front().second});
    if (mode == 2) row.erase("declared_length");
    if (mode == 3) row.add_child("channels", boost::property_tree::ptree{});
    if (mode == 4) {
      auto unused = tables.front().second; unused.put("id", "unused");
      unused.get_child("channels").front().second.put("original_ordinal", "bad");
      tables.push_back({"", unused});
    }
    if (mode == 5) {
      auto& channels = tables.front().second.get_child("channels");
      auto channel = channels.front().second;
      channels.clear();
      for (unsigned i = 0; i < 4097; ++i) { channel.put("original_ordinal", i); channels.push_back({"", channel}); }
    }
    auto rejected = submit_query_control(socket, json_tree(tree));
    EXPECT_FALSE(rejected.ok()) << "mode " << mode;
  }
}

TEST_F(SocketFixture, DistinctResolvedChannelTablesRejectStringAmplificationBeforeReadingSources) {
  auto backend = std::make_shared<TinyBackend>();
  storage = std::make_shared<StructuredBlobStorage>(backend);
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto tree = compact_window_tree();
  auto& channel = tree.get_child("snapshot.channel_tables").front().second.get_child("channels").front().second;
  channel.put("label", std::string(128 * 1024, 'x'));
  auto& rows = tree.get_child("snapshot.rows");
  auto row = rows.front().second;
  rows.clear();
  for (unsigned i = 0; i < 520; ++i) {
    row.put("logical_row", i);
    row.put("declared_length", 4 + i);
    rows.push_back({"", row});
  }
  const auto request = json_tree(tree);
  ASSERT_LT(request.size(), 1024u * 1024u);
  auto rejected = submit_query_control(socket, request);
  ASSERT_FALSE(rejected.ok());
  EXPECT_NE(rejected.status().message().find("expanded channel bytes"), std::string::npos);
  EXPECT_EQ(backend->layout_reads.load(), 0u);
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, PreparedResultReleaseAllowsMoreThan1024SequentialBatches) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto prepared = submit_query_control(socket, json_tree(window_tree()));
  ASSERT_TRUE(prepared.ok());
  const auto request = execute_prepared(prepared.value().prepared_id);
  for (unsigned i = 0; i < 1025; ++i) {
    auto result = submit_query_control(socket, request);
    ASSERT_TRUE(result.ok()) << "batch " << i << ": " << result.status().message();
    auto released = submit_query_control(socket, release_prepared(prepared.value().prepared_id, result.value().path));
    ASSERT_TRUE(released.ok()); ASSERT_TRUE(released.value().released);
  }
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, RegistryChargesLeasedMetadataAfterIdRetirementAndIsolatesUids) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto result = submit_query_registration(socket, kWindow);
  ASSERT_TRUE(result.ok());
  const auto end = result.value().rfind("&result=");
  auto resolved = frontend->resolve(result.value().substr(9, end - 9));
  ASSERT_TRUE(resolved.ok());
  auto module = std::get<WindowExecutionRequest>(resolved.value()).module;
  PreparedTrainingRegistry::Config config;
  config.max_entries = 1;
  config.max_entries_per_uid = 1;
  config.max_bytes = module->estimated_metadata_bytes();
  config.max_item_bytes = config.max_bytes;
  PreparedTrainingRegistry registry(config, [](const std::string&) {});
  auto id = registry.insert(100, module);
  ASSERT_TRUE(id.ok());
  EXPECT_FALSE(registry.acquire(id.value(), 200).ok());
  EXPECT_FALSE(registry.erase(id.value(), 200));
  auto lease = registry.acquire(id.value(), 100);
  ASSERT_TRUE(lease.ok());
  EXPECT_TRUE(registry.erase(id.value(), 100));
  EXPECT_EQ(registry.stats().resident_entries, 1u);
  EXPECT_FALSE(registry.insert(200, module).ok());
  lease.value().reset();
  EXPECT_EQ(registry.stats().resident_entries, 0u);
  EXPECT_EQ(registry.stats().resident_bytes, 0u);
  EXPECT_TRUE(registry.insert(200, module).ok());
  config.max_item_bytes = 1;
  PreparedTrainingRegistry too_small(config, [](const std::string&) {});
  EXPECT_FALSE(too_small.insert(100, module).ok());

  auto now = PreparedTrainingRegistry::Clock::now();
  config.max_entries = 2;
  config.max_bytes = module->estimated_metadata_bytes() * 2;
  config.max_item_bytes = module->estimated_metadata_bytes();
  config.clock = [&] { return now; };
  PreparedTrainingRegistry expires(config, [](const std::string&) {});
  auto first = expires.insert(100, module);
  ASSERT_TRUE(first.ok());
  EXPECT_FALSE(expires.insert(100, module).ok()); // Per-UID cap, with a global slot free.
  auto other = expires.insert(200, module);
  ASSERT_TRUE(other.ok());
  auto inflight = expires.acquire(first.value(), 100);
  ASSERT_TRUE(inflight.ok());
  now += config.ttl + std::chrono::milliseconds(1);
  expires.reap();
  EXPECT_FALSE(expires.acquire(first.value(), 100).ok());
  EXPECT_FALSE(expires.track_route(first.value(), 100, "late-result").ok());
  EXPECT_EQ(expires.stats().resident_entries, 1u); // Expiry cannot refund an in-flight lease.
  inflight.value().reset();
  EXPECT_EQ(expires.stats().resident_bytes, 0u);
}

TEST_F(SocketFixture, RegistersTypedDeclarationAndReturnsReadOnlyFusePath) {
  std::vector<QuerySourceReference> authorized;
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer& peer, const std::vector<QuerySourceReference>& refs, const CatalogSelection*) {
      EXPECT_EQ(peer.uid, ::geteuid());
      authorized = refs;
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  struct stat metadata{};
  ASSERT_EQ(::stat(socket.c_str(), &metadata), 0);
  EXPECT_EQ(metadata.st_mode & 0777, 0600);

  auto submitted = submit_query_registration(socket, kTyped);
  ASSERT_TRUE(submitted.ok()) << submitted.status().message();
  EXPECT_EQ(submitted.value().find("/mnt/data/.iotdbfs/query/v1/typed.ipc?id="), 0u);
  EXPECT_NE(submitted.value().find("&version=7&result="), std::string::npos);
  ASSERT_EQ(authorized.size(), 1u);
  EXPECT_EQ(authorized[0].path, "/dataset/a.arrow");
  EXPECT_EQ(authorized[0].version, 7);
  const auto result_marker = submitted.value().rfind("&result=");
  ASSERT_NE(result_marker, std::string::npos);
  const auto route = submitted.value().substr(std::string("/mnt/data").size(),
    result_marker - std::string("/mnt/data").size());
  const auto result_id = submitted.value().substr(result_marker + 8);
  ASSERT_EQ(result_id.size(), 64u);
  auto pinned = bridge->acquire_exact(route, result_id, {});
  ASSERT_TRUE(pinned.ok()) << pinned.status().message();
  EXPECT_EQ(pinned.value()->id(), result_id);
  auto resolved = frontend->resolve(route);
  ASSERT_TRUE(resolved.ok());
  const auto& request = std::get<TypedQueryExecutionRequest>(resolved.value());
  EXPECT_EQ(request.request->sources.formats.adapter_ids,
    std::vector<std::string>({"arrow"}));
  server.stop();
  EXPECT_NE(::stat(socket.c_str(), &metadata), 0);
}

TEST_F(SocketFixture, SharedSocketRequiresSourceAuthorization) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, {}, 0666);
  auto started = server.start();
  ASSERT_FALSE(started.ok());
  EXPECT_EQ(started.code(), QueryError::InvalidQuery);
  struct stat metadata{};
  EXPECT_NE(::lstat(socket.c_str(), &metadata), 0);
}

TEST_F(SocketFixture, RejectsInvalidServerReadParallelismBeforeCreatingSocket) {
  for (const auto parallelism : {0u, 5u}) {
    TrainingExecutionOptions execution;
    execution.read_parallelism = parallelism;
    QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
      storage, runtime, bridge, {}, 0600, execution);
    auto started = server.start();
    ASSERT_FALSE(started.ok());
    EXPECT_EQ(started.code(), QueryError::InvalidQuery);
    struct stat metadata{};
    EXPECT_NE(::lstat(socket.c_str(), &metadata), 0);
  }
}

TEST_F(SocketFixture, RejectsParallelismWithoutEnoughCacheAdmissionOrWorkers) {
  TrainingExecutionOptions execution;
  execution.read_parallelism = 4;
  for (const bool insufficient_workers : {false, true}) {
    QueryRuntimeConfig config;
    config.max_inflight = insufficient_workers ? 256 : 64;
    config.worker_threads = insufficient_workers ? 2 : 4;
    auto configured_runtime = std::make_shared<QueryRuntime>(config);
    QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
      storage, configured_runtime, bridge, {}, 0600, execution);
    EXPECT_EQ(server.start().code(), QueryError::InvalidQuery);
  }
}

TEST_F(SocketFixture, SocketClientCannotOverrideServerReadParallelism) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  for (const auto* setting : {"\"read_parallelism\":4",
      "\"reuse_prepared_validation\":false", "\"compiled_transform\":false"}) {
    std::string declaration(kWindow);
    declaration.insert(declaration.find('{') + 1,
      std::string("\"execution\":{") + setting + "},");
    auto submitted = submit_query_registration(socket, declaration);
    ASSERT_FALSE(submitted.ok());
    EXPECT_EQ(submitted.status().code(), QueryError::InvalidQuery);
    EXPECT_NE(submitted.status().message().find("server-owned"), std::string::npos);
    EXPECT_EQ(frontend->size(), 0u);
  }
}

TEST_F(SocketFixture, ProfileRecordsActualRuntimeAndServerReadConfiguration) {
  QueryRuntimeConfig config;
  config.max_entries = 16384;
  config.max_inflight = 256;
  auto configured_runtime = std::make_shared<QueryRuntime>(config);
  TrainingExecutionOptions execution;
  execution.read_parallelism = 4;
  const auto profile_path = directory + "/profile.jsonl";
  const char* previous = ::getenv("IOTDBFS_QUERY_PROFILE_JSONL");
  const bool had_previous = previous != nullptr;
  const std::string previous_value = previous ? previous : "";
  ASSERT_EQ(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", profile_path.c_str(), 1), 0);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, configured_runtime, bridge, {}, 0600, execution);
  if (had_previous) ::setenv("IOTDBFS_QUERY_PROFILE_JSONL", previous_value.c_str(), 1);
  else ::unsetenv("IOTDBFS_QUERY_PROFILE_JSONL");
  ASSERT_TRUE(server.start().ok());
  auto submitted = submit_query_registration(socket, kWindow);
  ASSERT_TRUE(submitted.ok()) << submitted.status().message();
  server.stop();
  boost::property_tree::ptree profile;
  boost::property_tree::read_json(profile_path, profile);
  EXPECT_EQ(profile.get<std::uint64_t>("counters.registration_attempts"), 1u);
  const auto& counters = profile.get_child("counters");
  EXPECT_EQ(counters.get<std::uint64_t>(boost::property_tree::ptree::path_type(
    "control_json.parse_attempts", '/')), 1u);
  EXPECT_EQ(counters.get<std::uint64_t>(boost::property_tree::ptree::path_type(
    "control_json.reuses", '/')), 1u);
  EXPECT_TRUE(counters.get_optional<std::uint64_t>(boost::property_tree::ptree::path_type(
    "control_json.transport_parse_ns", '/')));
  EXPECT_GT(profile.get<std::uint64_t>("counters.published_bytes"), 0u);
  EXPECT_EQ(profile.get<std::string>("runtime_delta_scope"), "shared-runtime");
  EXPECT_NE(profile.get<std::string>("measurement_semantics.server_total").find("excludes socket"), std::string::npos);
  EXPECT_EQ(profile.get<std::uint64_t>("iotdb_delta.payload_calls"), 0u);
  EXPECT_EQ(profile.get<std::uint64_t>("iotdb_after.session_leases_active"), 0u);
  EXPECT_TRUE(profile.get_optional<std::uint64_t>("runtime_delta.fragment_queue_nanoseconds"));
  EXPECT_TRUE(profile.get_optional<std::uint64_t>("runtime_delta.decode_filter_nanoseconds"));
  EXPECT_TRUE(profile.get_optional<std::uint64_t>("stages_ns.registration_total"));
  EXPECT_EQ(profile.get<std::size_t>("runtime_config.max_entries"), 16384u);
  EXPECT_EQ(profile.get<std::size_t>("runtime_config.max_inflight"), 256u);
  EXPECT_EQ(profile.get<std::size_t>("runtime_config.worker_threads"), 4u);
  EXPECT_EQ(profile.get<std::uint64_t>("runtime_config.memory_budget_bytes"), 1024ULL * 1024 * 1024);
  EXPECT_EQ(profile.get<std::string>("server_hdf5_execution.mode"), "iotdb-indexed");
  EXPECT_EQ(profile.get<unsigned>("server_training_execution.read_parallelism"), 4u);
  EXPECT_TRUE(profile.get<bool>("server_training_execution.reuse_prepared_validation"));
  EXPECT_TRUE(profile.get<bool>("server_training_execution.compiled_transform"));
  EXPECT_EQ(profile.get<unsigned>("runtime_after.payload_reads_active"), 0u);
  EXPECT_GE(profile.get<unsigned>("runtime_after.payload_reads_peak"), 1u);
  ::unlink(profile_path.c_str());
}

TEST_F(SocketFixture, FailedRegistrationStillRecordsProfileAndErrorWithoutPublishing) {
  const auto profile_path = directory + "/failed-profile.jsonl";
  const char* previous = ::getenv("IOTDBFS_QUERY_PROFILE_JSONL");
  const bool had_previous = previous != nullptr;
  const std::string previous_value = previous ? previous : "";
  ASSERT_EQ(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", profile_path.c_str(), 1), 0);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  if (had_previous) ::setenv("IOTDBFS_QUERY_PROFILE_JSONL", previous_value.c_str(), 1);
  else ::unsetenv("IOTDBFS_QUERY_PROFILE_JSONL");
  ASSERT_TRUE(server.start().ok());
  auto submitted = submit_query_registration(socket, R"({"kind":"typed","request":{}})");
  ASSERT_FALSE(submitted.ok());
  server.stop();
  boost::property_tree::ptree profile;
  boost::property_tree::read_json(profile_path, profile);
  EXPECT_FALSE(profile.get<bool>("ok"));
  EXPECT_EQ(profile.get<std::uint64_t>("counters.registration_attempts"), 1u);
  EXPECT_TRUE(profile.get_optional<int>("error_code"));
  EXPECT_TRUE(profile.get_optional<int>("error_phase"));
  EXPECT_TRUE(profile.get_optional<std::uint64_t>("stages_ns.json_parse"));
  EXPECT_TRUE(profile.get_optional<std::uint64_t>("stages_ns.registration_total"));
  EXPECT_FALSE(profile.get_optional<std::uint64_t>("counters.published_results"));
  EXPECT_EQ(frontend->size(), 0u);
  ::unlink(profile_path.c_str());
}

TEST_F(SocketFixture, SharedSocketUsesAuthenticatedPeerOncePerRegistration) {
  std::atomic<std::size_t> checks{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge,
    [&](const QueryRegistrationPeer& peer,
        const std::vector<QuerySourceReference>& refs, const CatalogSelection*) {
      EXPECT_EQ(peer.uid, ::geteuid());
      EXPECT_EQ(peer.gid, ::getegid());
#if defined(__linux__)
      const int count = ::getgroups(0, nullptr);
      EXPECT_GE(count, 0);
      if (count >= 0) {
        std::vector<gid_t> actual(static_cast<std::size_t>(count));
        EXPECT_EQ(::getgroups(count, actual.data()), count);
        actual.erase(std::remove(actual.begin(), actual.end(), peer.gid),
          actual.end());
        std::sort(actual.begin(), actual.end());
        actual.erase(std::unique(actual.begin(), actual.end()), actual.end());
        EXPECT_EQ(peer.supplementary_groups, actual);
      }
#endif
      EXPECT_EQ(refs.size(), 1u);
      ++checks;
      return QueryStatus::success();
    }, 0666);
  ASSERT_TRUE(server.start().ok());
  struct stat metadata{};
  ASSERT_EQ(::stat(socket.c_str(), &metadata), 0);
  EXPECT_EQ(metadata.st_mode & 0777, 0666);
  auto submitted = submit_query_registration(socket, kTyped);
  ASSERT_TRUE(submitted.ok()) << submitted.status().message();
  EXPECT_EQ(checks.load(), 1u);
  // Reuse is scoped to one registration, including on a shared socket.
  // A fresh connection must authenticate and authorize the peer again.
  auto next = submit_query_registration(socket, kTyped);
  ASSERT_TRUE(next.ok()) << next.status().message();
  EXPECT_EQ(checks.load(), 2u);
}

#if defined(__linux__)
TEST_F(SocketFixture, SharedSocketAcceptsAndAuthorizesAnotherUid) {
  if (::geteuid() != 0) GTEST_SKIP() << "requires root to switch client UID";
  ASSERT_EQ(::chmod(directory.c_str(), 0711), 0);
  constexpr uid_t client_uid = 65534;
  constexpr gid_t client_gid = 65534;
  constexpr gid_t client_extra_gid = 23456;
  int gate[2];
  int outcome[2];
  ASSERT_EQ(::pipe(gate), 0);
  ASSERT_EQ(::pipe(outcome), 0);
  const pid_t child = ::fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    ::close(gate[1]);
    ::close(outcome[0]);
    char signal = 0;
    if (::read(gate[0], &signal, 1) != 1) ::_exit(2);
    char result = 2;
    const gid_t extra_group = client_extra_gid;
    if (::setgroups(1, &extra_group) == 0 && ::setgid(client_gid) == 0 &&
        ::setuid(client_uid) == 0)
      result = submit_query_registration(socket, kTyped).ok() ? 1 : 0;
    if (::write(outcome[1], &result, 1) != 1) ::_exit(3);
    ::_exit(0);
  }
  ::close(gate[0]);
  ::close(outcome[1]);
  std::atomic<uid_t> seen_uid{static_cast<uid_t>(-1)};
  std::atomic<bool> seen_extra_group{false};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge,
    [&](const QueryRegistrationPeer& peer,
        const std::vector<QuerySourceReference>&, const CatalogSelection*) {
      seen_uid.store(peer.uid);
      seen_extra_group.store(std::find(peer.supplementary_groups.begin(),
        peer.supplementary_groups.end(), client_extra_gid) !=
        peer.supplementary_groups.end());
      return QueryStatus::success();
    }, 0666);
  const auto started = server.start();
  const char signal = 1;
  EXPECT_EQ(::write(gate[1], &signal, 1), 1);
  ::close(gate[1]);
  char result = 0;
  EXPECT_EQ(::read(outcome[0], &result, 1), 1);
  ::close(outcome[0]);
  int child_status = 0;
  EXPECT_EQ(::waitpid(child, &child_status, 0), child);
  ASSERT_TRUE(started.ok()) << started.message();
  if (result == 2) GTEST_SKIP() << "cannot switch client UID in this environment";
  EXPECT_EQ(result, 1);
  EXPECT_EQ(seen_uid.load(), client_uid);
  EXPECT_TRUE(seen_extra_group.load());
}
#endif

TEST_F(SocketFixture, RestartsAfterAbandonedSocket) {
  const int old_lock = ::open((socket + ".lock").c_str(), O_CREAT | O_RDWR, 0600);
  ASSERT_GE(old_lock, 0);
  ASSERT_EQ(::flock(old_lock, LOCK_EX | LOCK_NB), 0);
  const int abandoned = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(abandoned, 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket.c_str(), socket.size() + 1);
  ASSERT_EQ(::bind(abandoned, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  ::close(abandoned);
  ::close(old_lock);  // A killed daemon releases its advisory lock, not the pathname.

  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto submitted = submit_query_registration(socket, kTyped);
  ASSERT_TRUE(submitted.ok()) << submitted.status().message();
  server.stop();
  struct stat metadata{};
  EXPECT_NE(::lstat(socket.c_str(), &metadata), 0);
}

TEST_F(SocketFixture, DefaultSocketDoesNotRequireWritableMountParent) {
  const auto mount_parent = directory + "/read-only-parent";
  ASSERT_EQ(::mkdir(mount_parent.c_str(), 0500), 0);
  const auto mount_root = mount_parent + "/dfs";
  auto selected = default_query_registration_socket_path(
    mount_root, directory);
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  EXPECT_EQ(selected.value().find(directory + "/iotdbfs-"), 0u);
  EXPECT_EQ(selected.value().find(mount_parent), std::string::npos);

  auto other = default_query_registration_socket_path(
    mount_parent + "/other", directory);
  ASSERT_TRUE(other.ok());
  EXPECT_NE(selected.value(), other.value());
  socket = selected.value();
  QueryRegistrationSocketServer server(socket, mount_root, frontend, catalog,
    storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  EXPECT_TRUE(submit_query_registration(socket, kTyped).ok());
  server.stop();
  ::rmdir(mount_parent.c_str());
}

TEST_F(SocketFixture, IgnoresUnsafeRuntimeDirectory) {
  const auto insecure = directory + "/shared";
  ASSERT_EQ(::mkdir(insecure.c_str(), 0700), 0);
  ASSERT_EQ(::chmod(insecure.c_str(), 0777), 0);
  auto selected = default_query_registration_socket_path("/mnt/dfs", insecure);
  ASSERT_TRUE(selected.ok()) << selected.status().message();
  EXPECT_NE(selected.value().find(insecure + "/"), 0u);
  const auto parent = selected.value().substr(0, selected.value().find_last_of('/'));
  struct stat metadata{};
  ASSERT_EQ(::lstat(parent.c_str(), &metadata), 0);
  EXPECT_TRUE(S_ISDIR(metadata.st_mode));
  EXPECT_EQ(metadata.st_uid, ::geteuid());
  EXPECT_EQ(metadata.st_mode & 0777, 0700);
  ::rmdir(insecure.c_str());
}

TEST_F(SocketFixture, DoesNotReplaceAnActiveRegistrationSocket) {
  QueryRegistrationSocketServer first(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  ASSERT_TRUE(first.start().ok());
  struct stat before{};
  ASSERT_EQ(::lstat(socket.c_str(), &before), 0);

  QueryRegistrationSocketServer second(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  EXPECT_FALSE(second.start().ok());
  struct stat after{};
  ASSERT_EQ(::lstat(socket.c_str(), &after), 0);
  EXPECT_EQ(before.st_dev, after.st_dev);
  EXPECT_EQ(before.st_ino, after.st_ino);
  auto submitted = submit_query_registration(socket, kTyped);
  EXPECT_TRUE(submitted.ok()) << submitted.status().message();

  first.stop();
  ASSERT_TRUE(second.start().ok());
  second.stop();
}

TEST_F(SocketFixture, DoesNotDeleteAnotherListeningUnixSocket) {
  const int listener = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket.c_str(), socket.size() + 1);
  ASSERT_EQ(::bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(::listen(listener, 1), 0);
  struct stat before{};
  ASSERT_EQ(::lstat(socket.c_str(), &before), 0);

  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  EXPECT_FALSE(server.start().ok());
  struct stat after{};
  ASSERT_EQ(::lstat(socket.c_str(), &after), 0);
  EXPECT_EQ(before.st_dev, after.st_dev);
  EXPECT_EQ(before.st_ino, after.st_ino);
  ::close(listener);
}

TEST_F(SocketFixture, DoesNotDeleteAnUnrelatedFileAtSocketPath) {
  const int unrelated = ::open(socket.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
  ASSERT_GE(unrelated, 0);
  ::close(unrelated);

  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  EXPECT_FALSE(server.start().ok());
  struct stat metadata{};
  ASSERT_EQ(::lstat(socket.c_str(), &metadata), 0);
  EXPECT_TRUE(S_ISREG(metadata.st_mode));
}

TEST_F(SocketFixture, PreparesFullWindowDeclarationBeforeRegisteringBoundedSample) {
  auto backend = std::make_shared<TinyBackend>();
  storage = std::make_shared<StructuredBlobStorage>(backend);
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  std::vector<QuerySourceReference> authorized;
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>& refs, const CatalogSelection*) {
      authorized = refs;
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto submitted = submit_query_registration(socket, kWindow);
  ASSERT_TRUE(submitted.ok()) << submitted.status().message();
  EXPECT_EQ(submitted.value().find("/mnt/data/.iotdbfs/query/v1/windows.ipc?id="), 0u);
  ASSERT_EQ(authorized.size(), 1u);
  EXPECT_EQ(authorized[0].path, "/dataset/a.arrow");
  EXPECT_EQ(authorized[0].version, 7);

  const auto result_marker = submitted.value().rfind("&result=");
  ASSERT_NE(result_marker, std::string::npos);
  auto resolved = frontend->resolve(submitted.value().substr(
    std::string("/mnt/data").size(), result_marker - std::string("/mnt/data").size()));
  ASSERT_TRUE(resolved.ok()) << resolved.status().message();
  ASSERT_TRUE(std::holds_alternative<WindowExecutionRequest>(resolved.value()));
  const auto& registration = std::get<WindowExecutionRequest>(resolved.value());
  EXPECT_EQ(registration.module->view().snapshot_fingerprint,
    registration.module->snapshot().fingerprint);
  auto batch = registration.module->execute(*registration.request);
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_EQ(batch.value().samples().size(), 1u);
  EXPECT_EQ(batch.value().samples()[0].input_values,
    std::vector<float>({0.0f, 1.0f}));
  EXPECT_EQ(batch.value().samples()[0].target_values,
    std::vector<float>({2.0f}));
}

TEST_F(SocketFixture, SlowWindowPreparationOutlivesShortSocketTimeout) {
  auto backend = std::make_shared<TinyBackend>();
  storage = std::make_shared<StructuredBlobStorage>(backend);
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>&, const CatalogSelection*) {
      std::this_thread::sleep_for(std::chrono::milliseconds(3200));
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto submitted = submit_query_registration(socket, kWindow);
  ASSERT_TRUE(submitted.ok()) << submitted.status().message();
  EXPECT_EQ(frontend->size(), 1u);
}

TEST_F(SocketFixture, TimedOutWindowRegistrationDoesNotLeaveARoute) {
  auto backend = std::make_shared<TinyBackend>();
  storage = std::make_shared<StructuredBlobStorage>(backend);
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>&, const CatalogSelection*) {
      std::this_thread::sleep_for(std::chrono::milliseconds(250));
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto submitted = submit_query_registration(socket, kWindow,
    std::chrono::milliseconds(40));
  EXPECT_FALSE(submitted.ok());
  server.stop();  // Wait for the in-flight preparation and failed response send.
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, RejectsUnauthorizedAndMalformedDeclarationsWithoutRegistration) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>&, const CatalogSelection*) {
      return QueryStatus::failure(QueryError::SourceNotFound, QueryPhase::Validation,
        "source is not readable by caller");
    });
  ASSERT_TRUE(server.start().ok());
  auto denied = submit_query_registration(socket, kTyped);
  ASSERT_FALSE(denied.ok());
  EXPECT_EQ(denied.status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(frontend->size(), 0u);

  auto malformed = submit_query_registration(socket,
    R"({"kind":"typed","request":{"sources":{"versions":{"kind":"exact","exact":"oops"}}}})");
  ASSERT_FALSE(malformed.ok());
  EXPECT_EQ(malformed.status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, RejectsHdf5UnknownSelectionSyntaxBeforeAuthorization) {
  std::atomic<unsigned> authorizations{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>&, const CatalogSelection*) {
      ++authorizations;
      return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Validation,
        "test authorization stops before materialization");
    });
  ASSERT_TRUE(server.start().ok());
  auto rejected = [&](const boost::property_tree::ptree& tree) {
    auto response = submit_query_registration(socket, json_tree(tree));
    ASSERT_FALSE(response.ok());
    EXPECT_EQ(response.status().code(), QueryError::InvalidQuery);
    EXPECT_NE(response.status().message().find("unexpected HDF5"), std::string::npos)
      << response.status().message();
    EXPECT_EQ(authorizations.load(), 0u);
    EXPECT_EQ(frontend->size(), 0u);
  };
  for (const auto* name : {"dataset", "dataset_subject", "subject", "stride", "block",
                           "points", "union", "selection"}) {
    SCOPED_TRACE(name);
    auto tree = hdf5_typed_tree();
    tree.get_child("request.subset").put(name, 2);
    rejected(tree);
  }
  for (const auto* name : {"stride", "block", "points", "union", "count"}) {
    SCOPED_TRACE(name);
    auto tree = hdf5_typed_tree();
    tree.get_child("request.subset.slices").front().second.put(name, 2);
    rejected(tree);
  }
  for (const auto* field : {"lower.stride", "upper.block", "lower.value.stride"}) {
    SCOPED_TRACE(field);
    auto tree = hdf5_typed_tree();
    tree.get_child("request.subset.slices").front().second.put(field, 2);
    rejected(tree);
  }
  auto tree = hdf5_typed_tree();
  tree.put("request.subset.output.selection", "union");
  rejected(tree);
}

TEST_F(SocketFixture, RejectsHdf5DuplicateKeysAndScalarSlicesWithoutRegistration) {
  std::atomic<unsigned> authorizations{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>&, const CatalogSelection*) {
      ++authorizations;
      return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Validation, "test stop");
    });
  ASSERT_TRUE(server.start().ok());
  auto duplicate = hdf5_typed_tree();
  auto& slice = duplicate.get_child("request.subset.slices").front().second;
  slice.push_back({"upper", slice.get_child("upper")});
  auto response = submit_query_registration(socket, json_tree(duplicate));
  ASSERT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), QueryError::InvalidQuery);
  EXPECT_NE(response.status().message().find("duplicate HDF5"), std::string::npos);

  auto scalar = hdf5_typed_tree();
  scalar.get_child("request.subset").erase("slices");
  scalar.put("request.subset.slices", "ignored");
  response = submit_query_registration(socket, json_tree(scalar));
  ASSERT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), QueryError::InvalidQuery);
  EXPECT_NE(response.status().message().find("HDF5 slices must be an array"), std::string::npos);
  EXPECT_EQ(authorizations.load(), 0u);
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, Hdf5StrictJsonKeysPreserveKnownSyntaxAndOtherFormats) {
  // The fixture has no HDF5 adapter. Reaching this authorizer demonstrates
  // successful parsing/validation independently of dataset materialization.
  std::atomic<unsigned> authorizations{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>&, const CatalogSelection*) {
      ++authorizations;
      return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Validation, "test stop");
    });
  ASSERT_TRUE(server.start().ok());
  auto known = hdf5_typed_tree();
  known.put("request.subset.output.mode", "logical_values");
  auto response = submit_query_registration(socket, json_tree(known));
  ASSERT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), QueryError::PermissionDenied) << response.status().message();

  for (const auto* value : {"value.0", "data.temperature"}) {
    auto legacy = hdf5_typed_tree();
    legacy.get_child("request.subset.projection").front().second.put_value(value);
    auto& slice = legacy.get_child("request.subset.slices").front().second;
    slice.put("dimension", "domain.index");
    slice.put("stride", 2);
    legacy.put("request.subset.subject", "legacy_extension");
    response = submit_query_registration(socket, json_tree(legacy));
    ASSERT_FALSE(response.ok());
    EXPECT_EQ(response.status().code(), QueryError::PermissionDenied) << response.status().message();
  }
  EXPECT_EQ(authorizations.load(), 3u);
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, ClosesOversizedFrameAndKeepsServing) {
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge);
  ASSERT_TRUE(server.start().ok());
  auto oversized = submit_query_registration(socket, std::string(1024 * 1024 + 1, 'x'));
  ASSERT_FALSE(oversized.ok());
  EXPECT_EQ(oversized.status().code(), QueryError::InvalidQuery);

  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket.c_str(), socket.size() + 1);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  const auto size = htonl(1024 * 1024 + 1);
  ASSERT_EQ(::write(fd, &size, sizeof(size)), sizeof(size));
  ::close(fd);

  auto valid = submit_query_registration(socket, kTyped);
  ASSERT_TRUE(valid.ok()) << valid.status().message();
  EXPECT_EQ(frontend->size(), 1u);
}

} // namespace

namespace {
class FetchTraceEnvironment {
public:
  FetchTraceEnvironment(const char* name, const std::string& value, bool enabled = true)
      : name_(name) {
    if (const auto* previous = ::getenv(name)) {
      previous_ = previous;
      had_previous_ = true;
    }
    if (enabled) ::setenv(name, value.c_str(), 1);
    else ::unsetenv(name);
  }
  ~FetchTraceEnvironment() {
    if (had_previous_) ::setenv(name_.c_str(), previous_.c_str(), 1);
    else ::unsetenv(name_.c_str());
  }
private:
  std::string name_, previous_;
  bool had_previous_{false};
};

std::vector<boost::property_tree::ptree> fetch_trace_rows(const std::string& path) {
  std::vector<boost::property_tree::ptree> rows;
  std::ifstream input(path);
  for (std::string line; std::getline(input, line);) {
    boost::property_tree::ptree row;
    std::istringstream json(line);
    boost::property_tree::read_json(json, row);
    rows.push_back(std::move(row));
  }
  return rows;
}

class ResultFetchTimingSocketFixture : public SocketFixture {
protected:
  std::unique_ptr<QueryRegistrationSocketServer> two_sender_server(
      QueryRegistrationSocketServer::Authorization authorize = {}) {
    BoundedResultSender::Config config;
    config.sender_threads = 2;
    return std::make_unique<QueryRegistrationSocketServer>(socket, "/mnt/data",
      frontend, catalog, storage, runtime, bridge, std::move(authorize), 0600,
      TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{},
      Hdf5QueryKernelConfig{}, QueryRegistrationSocketServer::DatasetDiscovery{},
      std::shared_ptr<Hdf5ValidatedLayoutCache>{},
      std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
      QueryRegistrationSocketServer::BeginAuthorizationPublication{}, false,
      Hdf5QueryMode::IoTDBIndexed, std::shared_ptr<const QueryModule>{},
      std::shared_ptr<HotQueryCache>{},
      QueryRegistrationSocketServer::PrepareSelectorBinding{},
      QueryRegistrationSocketServer::SelectSources{}, config);
  }
};

TEST_F(ResultFetchTimingSocketFixture, TwoSendersDeferTraceUntilStopAndPreserveWireBytes) {
  const auto trace_path = directory + "/fetch-timing.jsonl";
  FetchTraceEnvironment trace("IOTDBFS_RESULT_FETCH_TRACE_JSONL", trace_path);
  auto server = two_sender_server();
  ASSERT_TRUE(server->start().ok());
  const auto published = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(published.ok()) << published.status().message();
  const auto path = published.value().path;
  const auto expected = socket_result_bytes(bridge, path);
  std::promise<void> release;
  const auto ready = release.get_future().share();
  std::vector<std::future<std::pair<boost::property_tree::ptree, BinaryValue>>> pending;
  for (unsigned i = 0; i < 8; ++i)
    pending.push_back(std::async(std::launch::async, [&, ready] {
      ready.wait();
      return fetch_result(socket, fetch_request(path));
    }));
  release.set_value();
  for (auto& future : pending) {
    auto response = future.get();
    EXPECT_TRUE(response.first.get<bool>("ok"));
    EXPECT_EQ(response.second, expected);
    expect_tiny_arrow_values(std::move(response.second));
  }
  EXPECT_TRUE(fetch_trace_rows(trace_path).empty());
  server->stop();
  const auto rows = fetch_trace_rows(trace_path);
  ASSERT_EQ(rows.size(), 9u);
  EXPECT_EQ(rows.back().get<std::string>("kind"), "summary");
  EXPECT_EQ(rows.back().get<std::size_t>("records"), 8u);
  EXPECT_EQ(rows.back().get<std::size_t>("dropped"), 0u);
  bool saw_shard[2]{false, false};
  const auto header = "{\"ok\":true,\"kind\":\"result_fetch\",\"version\":1,\"size\":" +
    std::to_string(expected.size()) + "}";
  for (std::size_t i = 0; i < 8; ++i) {
    const auto& row = rows[i];
    EXPECT_EQ(row.get<std::string>("kind"), "result_fetch_timing");
    EXPECT_EQ(row.get<std::string>("path"), path);
    EXPECT_EQ(row.get<unsigned>("sender_threads"), 2u);
    EXPECT_TRUE(row.get<bool>("completed"));
    EXPECT_EQ(row.get<unsigned>("error"), static_cast<unsigned>(QueryError::None));
    EXPECT_EQ(row.get<std::uint64_t>("bytes_sent"), sizeof(std::uint32_t) + header.size() + expected.size());
    const auto shard = row.get<unsigned>("shard");
    ASSERT_LT(shard, 2u); saw_shard[shard] = true;
    std::uint64_t previous = 0;
    for (const auto* field : {"accept_ns", "transport_start_ns", "request_ready_ns",
        "submit_ns", "task_start_ns", "admit_start_ns", "admit_end_ns", "enqueue_ns",
        "dequeue_ns", "first_send_ns", "last_send_ns", "cleanup_ns"}) {
      const auto stamp = row.get<std::uint64_t>(field);
      EXPECT_GT(stamp, 0u) << field;
      EXPECT_GE(stamp, previous) << field;
      previous = stamp;
    }
    const auto& spans = row.get_child("send_spans");
    EXPECT_EQ(spans.size(), row.get<std::size_t>("send_span_count"));
    EXPECT_EQ(spans.size(), row.get<std::size_t>("send_calls"));
    EXPECT_EQ(row.get<std::size_t>("send_spans_dropped"), 0u);
    ASSERT_FALSE(spans.empty());
    // This tiny IPC fits one send budget: the first actual sendmsg must offer
    // framing, JSON and body together, even if the kernel accepts a short write.
    EXPECT_EQ(spans.front().second.get<std::uint64_t>("requested_bytes"),
      sizeof(std::uint32_t) + header.size() + expected.size());
    auto span_end = row.get<std::uint64_t>("dequeue_ns");
    std::uint64_t span_bytes = 0;
    for (const auto& span : spans) {
      const auto begin = span.second.get<std::uint64_t>("begin_ns");
      const auto end = span.second.get<std::uint64_t>("end_ns");
      EXPECT_GE(begin, span_end); EXPECT_GE(end, begin);
      EXPECT_LE(end, row.get<std::uint64_t>("last_send_ns"));
      const auto requested = span.second.get<std::uint64_t>("requested_bytes");
      const auto sent = span.second.get<std::int64_t>("result_bytes");
      EXPECT_GT(requested, 0u); EXPECT_LE(requested, 64u * 1024u);
      EXPECT_LE(sent, static_cast<std::int64_t>(requested));
      if (sent > 0) span_bytes += static_cast<std::uint64_t>(sent);
      else EXPECT_GT(span.second.get<int>("error_no"), 0);
      span_end = end;
    }
    EXPECT_EQ(span_bytes, row.get<std::uint64_t>("bytes_sent"));
    EXPECT_TRUE(row.get_optional<std::uint64_t>("send_cpu_ns"));
    EXPECT_TRUE(row.get_optional<std::uint64_t>("eagain"));
    EXPECT_TRUE(row.get_optional<std::uint64_t>("short_writes"));
  }
  EXPECT_TRUE(saw_shard[0]); EXPECT_TRUE(saw_shard[1]);
  ::unlink(trace_path.c_str());
}

TEST_F(ResultFetchTimingSocketFixture, FailedAuthorizationIsTracedWithoutRetiringPublishedRoute) {
  const auto trace_path = directory + "/failed-fetch-timing.jsonl";
  FetchTraceEnvironment trace("IOTDBFS_RESULT_FETCH_TRACE_JSONL", trace_path);
  std::atomic<bool> denied{false};
  auto server = two_sender_server([&](const auto&, const auto&, const auto*) {
    return denied ? QueryStatus::failure(QueryError::PermissionDenied,
      QueryPhase::Catalog, "revoked") : QueryStatus::success();
  });
  ASSERT_TRUE(server->start().ok());
  auto published = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(published.ok());
  const auto expected = socket_result_bytes(bridge, published.value().path);
  denied = true;
  const auto refused = fetch_result(socket, fetch_request(published.value().path));
  EXPECT_FALSE(refused.first.get<bool>("ok"));
  EXPECT_EQ(refused.first.get<unsigned>("code"), static_cast<unsigned>(QueryError::PermissionDenied));
  EXPECT_TRUE(refused.second.empty());
  denied = false;
  EXPECT_EQ(fetch_result(socket, fetch_request(published.value().path)).second, expected);
  EXPECT_EQ(frontend->size(), 1u);
  EXPECT_EQ(bridge->cached_routes(), 1u);
  EXPECT_TRUE(fetch_trace_rows(trace_path).empty());
  server->stop();
  const auto rows = fetch_trace_rows(trace_path);
  ASSERT_EQ(rows.size(), 3u);
  unsigned failures = 0, successes = 0;
  for (std::size_t i = 0; i < 2; ++i) {
    const auto& row = rows[i];
    EXPECT_EQ(row.get<std::string>("kind"), "result_fetch_timing");
    EXPECT_EQ(row.get<std::string>("path"), published.value().path);
    EXPECT_EQ(row.get<unsigned>("sender_threads"), 2u);
    if (row.get<bool>("completed")) { ++successes; continue; }
    ++failures;
    EXPECT_EQ(row.get<unsigned>("error"), static_cast<unsigned>(QueryError::PermissionDenied));
    EXPECT_GT(row.get<std::uint64_t>("admit_start_ns"), 0u);
    EXPECT_GE(row.get<std::uint64_t>("admit_end_ns"), row.get<std::uint64_t>("admit_start_ns"));
    EXPECT_GE(row.get<std::uint64_t>("cleanup_ns"), row.get<std::uint64_t>("admit_end_ns"));
    EXPECT_EQ(row.get<std::uint64_t>("enqueue_ns"), 0u);
    EXPECT_EQ(row.get<std::uint64_t>("send_calls"), 0u);
    EXPECT_EQ(row.get<std::uint64_t>("bytes_sent"), 0u);
    EXPECT_TRUE(row.get_child("send_spans").empty());
  }
  EXPECT_EQ(failures, 1u); EXPECT_EQ(successes, 1u);
  EXPECT_EQ(rows.back().get<std::string>("kind"), "summary");
  EXPECT_EQ(rows.back().get<unsigned>("records"), 2u);
  EXPECT_EQ(rows.back().get<unsigned>("dropped"), 0u);
  ::unlink(trace_path.c_str());
}

TEST_F(ResultFetchTimingSocketFixture, DisabledFetchTraceDoesNotAddRowsToQueryProfiles) {
  const auto profile_path = directory + "/fetch-query-profile.jsonl";
  FetchTraceEnvironment trace("IOTDBFS_RESULT_FETCH_TRACE_JSONL", "", false);
  FetchTraceEnvironment profile("IOTDBFS_QUERY_PROFILE_JSONL", profile_path);
  auto server = two_sender_server();
  ASSERT_TRUE(server->start().ok());
  const auto published = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(published.ok());
  const auto expected = socket_result_bytes(bridge, published.value().path);
  for (unsigned i = 0; i < 4; ++i)
    EXPECT_EQ(fetch_result(socket, fetch_request(published.value().path)).second, expected);
  server->stop();
  const auto profiles = fetch_trace_rows(profile_path);
  ASSERT_EQ(profiles.size(), 1u);
  EXPECT_EQ(profiles[0].get<std::string>("operation"), "typed");
  EXPECT_FALSE(profiles[0].get_optional<std::string>("kind"));
  ::unlink(profile_path.c_str());
}
} // namespace



TEST_F(SocketFixture, CompactElectricity64And128StepDeclarationsFitPreparedRegistry) {
  for (const unsigned block : {64, 128}) {
    auto backend = std::make_shared<TinyBackend>();
    backend->payload = tiny_arrow_payload(true, block * 370);
    backend->layout = tiny_arrow_layout(backend->payload);
    backend->manifest.payload_size = backend->payload.size();
    backend->manifest.layout_size = backend->layout.size();
    storage = std::make_shared<StructuredBlobStorage>(backend);
    catalog = make_in_memory_published_version_catalog({backend->manifest});
    QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog, storage, runtime, bridge);
    ASSERT_TRUE(server.start().ok());
    auto tree = compact_window_tree();
    auto& table = tree.get_child("snapshot.channel_tables").front().second.get_child("channels");
    auto channel = table.front().second; table.clear();
    for (unsigned c = 0; c < 370; ++c) {
      channel.put("original_ordinal", c); channel.put("physical_ordinal", c);
      channel.put("label", c ? "channel-" + std::to_string(c) : "x");
      table.push_back({"", channel});
    }
    auto& rows = tree.get_child("snapshot.rows");
    auto row = rows.front().second; rows.clear();
    row.put("value_layout", "step_major");
    row.put("declared_n_channels", 370); row.put("declared_length", block);
    for (unsigned i = 0; i < 140000 / block; ++i) {
      row.put("logical_row", i); row.put("steps.begin", i * block);
      row.put("steps.end", (i + 1) * block); rows.push_back({"", row});
    }
    const auto declaration = json_tree(tree);
    ASSERT_LT(declaration.size(), 1024u * 1024u);
    auto prepared = submit_query_control(socket, declaration);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    EXPECT_FALSE(prepared.value().prepared_id.empty());
    EXPECT_TRUE(submit_query_control(socket, release_prepared(prepared.value().prepared_id)).ok());
  }
}

TEST_F(SocketFixture, DatasetDiscoveryReturnsExactIdentitiesAfterPeerAuthorization) {
  std::atomic<int> authorized{0}, discovered{0}, publication_observations{0};
  auto permissions = std::make_shared<SourcePermissionSnapshot>();
  permissions->storage_namespace = storage->storage_namespace();
  permissions->metadata_generation = 9;
  permissions->observed_at = std::chrono::steady_clock::now();
  StructuredBlobManifest manifest;
  manifest.key = {"/science/a.nc", 42};
  manifest.format_id = "hdf5";
  manifest.revision = 5;
  manifest.payload_size = 100;
  manifest.layout_size = 20;
  manifest.phase = StructuredBlobPhase::Published;
  manifest.published = true;
  permissions->sources[manifest.key] = {"hdf5", 5, 100, 20, 0640, 1000, 40077, false};
  CatalogSelection selection{{manifest}, {}, storage->storage_namespace(), permissions};
  QueryRegistrationSocketServer server(socket, "/mount", frontend, catalog, storage, runtime, bridge,
    [&](const QueryRegistrationPeer& peer, const std::vector<QuerySourceReference>& sources, const CatalogSelection* selected) {
      EXPECT_EQ(selected, &selection);
      if (selected) {
        EXPECT_EQ(selected->storage_namespace, storage->storage_namespace());
        EXPECT_EQ(selected->source_permissions.get(), permissions.get());
        EXPECT_EQ(selected->versions().size(), 1u);
        if (!selected->versions().empty()) EXPECT_EQ(selected->versions()[0].key, manifest.key);
      }
      EXPECT_EQ(peer.uid, ::geteuid());
      EXPECT_EQ(sources.size(), 1);
      if (!sources.empty()) {
        EXPECT_EQ(sources[0].path, manifest.key.path);
        EXPECT_EQ(sources[0].version, manifest.key.version);
        EXPECT_EQ(sources[0].format_id, "hdf5");
      }
      ++authorized;
      return QueryStatus::success();
    }, 0600, {}, {}, {},
    [&](const DatasetCatalogRequest& request, const DatasetCatalogAuthorizer& authorize, const std::shared_ptr<HotQueryCache::Binding>&) {
      ++discovered;
      EXPECT_EQ(request.dataset_path, std::optional<std::string>("/sst"));
      EXPECT_TRUE(request.time_range.has_value());
      if (request.time_range) {
        EXPECT_EQ(request.time_range->lower, 42);
        EXPECT_EQ(request.time_range->upper, 43);
      }
      EXPECT_EQ(request.sources.versions.kind, VersionSelectionKind::Latest);
      auto status = authorize({{"/science/a.nc", 42, "hdf5"}}, selection);
      if (!status.ok()) return QueryResult<DatasetCatalogResult>::failure(status);
      DatasetCatalogResult result;
      result.selected_sources = 1; result.time_candidate_sources = 1;
      result.candidate_datasets = 4; result.sql_queries = 1;
      result.datasets.push_back({{"/science/a.nc", 42, "hdf5"}, 5, "object-1", "/sst",
        std::string(64, 'a'), "IOTDBFS-HDF5-LAYOUT/4\n"});
      return QueryResult<DatasetCatalogResult>::success(std::move(result));
    }, {}, {}, [&](const QueryRegistrationPeer&, const CatalogSelection*) {
      ++publication_observations;
      return QueryRegistrationSocketServer::AuthorizationPublicationCommit{};
    });
  ASSERT_TRUE(server.start().ok());
  const auto response = submit_query_control(socket,
    R"({"kind":"dataset_catalog","request":{"sources":{"paths":[{"kind":"subtree","path":"/science"}]},"dataset_path":"/sst","time_range":{"lower_unix_s":42,"upper_unix_s":43},"predicate":{"kind":"compare","field":"hdf5.attr:units","operator":"eq","value":{"type":"utf8","value":"Celsius"}}}})");
  ASSERT_TRUE(response.ok()) << response.status().message();
  EXPECT_EQ(response.value().kind, QueryControlResponse::Kind::DatasetCatalog);
  ASSERT_EQ(response.value().datasets.datasets.size(), 1);
  EXPECT_EQ(response.value().datasets.datasets[0].source.version, 42);
  EXPECT_EQ(response.value().datasets.datasets[0].layout_digest, std::string(64, 'a'));
  EXPECT_EQ(response.value().datasets.sql_queries, 1);
  EXPECT_EQ(response.value().datasets.time_candidate_sources, 1);
  EXPECT_EQ(authorized, 1); EXPECT_EQ(discovered, 1);
  EXPECT_EQ(publication_observations, 0);
  EXPECT_EQ(frontend->size(), 0u);
  server.stop();
}

TEST_F(SocketFixture, DatasetDiscoveryReferenceUsesFreshPermissionHintBeforeArtifactRead) {
  auto backend = std::make_shared<TinyBackend>();
  backend->manifest.key = {"/science/a.h5", 7};
  backend->manifest.format_id = "hdf5";
  const auto digest = std::string(64, 'a');
  backend->manifest.metadata = {
    {"__iotdbfs.dataset_catalog.protocol", MetadataValue("iotdbfs-dataset-catalog/1")},
    {"__iotdbfs.dataset_catalog.layout_digest", MetadataValue(digest)}};
  DatasetCatalogArtifact artifact{"iotdbfs-dataset-catalog/1", "hdf5",
    "IOTDBFS-HDF5-LAYOUT/5\n", digest, true,
    {{"object-1", "/T", true, {}}}};
  ASSERT_TRUE(backend->write_dataset_catalog(backend->manifest.key, artifact).ok());
  storage = std::make_shared<StructuredBlobStorage>(backend);
  auto permissions = std::make_shared<SourcePermissionSnapshot>();
  permissions->storage_namespace = storage->storage_namespace();
  permissions->metadata_generation = 9;
  permissions->observed_at = std::chrono::steady_clock::now();
  permissions->sources[backend->manifest.key] = {"hdf5", backend->manifest.revision,
    backend->manifest.payload_size, backend->manifest.layout_size, 0400, 1000, 2000, false};
  catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
    return QueryResult<PublishedVersionSnapshot>::success({{backend->manifest}, permissions});
  }, storage->storage_namespace());
  std::atomic<unsigned> authorizations{0}, fresh_source_reads{0}, publication_observations{0};
  QueryRegistrationSocketServer server(socket, "/mount", frontend, catalog, storage, runtime, bridge,
    [&](const QueryRegistrationPeer& peer, const auto& sources, const CatalogSelection* selected) {
      ++authorizations;
      EXPECT_EQ(peer.uid, ::geteuid());
      EXPECT_NE(selected, nullptr);
      EXPECT_EQ(backend->manifest_reads, 0u); // Auth must precede artifact source loading.
      if (!selected || sources.size() != 1 || selected->versions().size() != 1)
        return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "missing observation");
      EXPECT_EQ(selected->storage_namespace, storage->storage_namespace());
      EXPECT_TRUE(selected->source_permissions != nullptr);
      if (!selected->source_permissions) return QueryStatus::failure(
        QueryError::PermissionDenied, QueryPhase::Catalog, "missing permissions");
      EXPECT_EQ(selected->source_permissions->sources.size(), 1u);
      auto fact = validated_catalog_source_permission(selected, &selected->versions()[0], sources[0],
        storage->storage_namespace(), 9);
      if (!fact) ++fresh_source_reads;
      // Simulate a non-root user at the same production permission-helper seam.
      const bool allowed = fact && check_query_posix_access(fact->mode, fact->uid, fact->gid,
        R_OK, 1000, 2000) == 0;
      return allowed ? QueryStatus::success() : QueryStatus::failure(
        QueryError::PermissionDenied, QueryPhase::Catalog, "source denied");
    }, 0600, {}, {}, {}, {}, {}, {}, [&](const QueryRegistrationPeer&, const CatalogSelection*) {
      ++publication_observations;
      return QueryRegistrationSocketServer::AuthorizationPublicationCommit{};
    });
  ASSERT_TRUE(server.start().ok());
  const auto response = submit_query_control(socket,
    R"({"kind":"dataset_catalog","request":{"sources":{"paths":[{"kind":"exact","path":"/science/a.h5"}],"versions":{"kind":"exact","exact":7}},"dataset_path":"/T"}})");
  ASSERT_TRUE(response.ok()) << response.status().message();
  ASSERT_EQ(response.value().datasets.datasets.size(), 1u);
  EXPECT_EQ(response.value().datasets.datasets[0].source.path, "/science/a.h5");
  EXPECT_EQ(response.value().datasets.datasets[0].source.version, 7);
  EXPECT_EQ(authorizations, 1u);
  EXPECT_EQ(fresh_source_reads, 0u);
  EXPECT_EQ(publication_observations, 0u);
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, DatasetDiscoveryUnusableHintsFallBackWithoutGrantingPermission) {
  for (const std::string kind : {"absent", "expired", "generation", "namespace", "identity"}) {
    SCOPED_TRACE(kind);
    auto backend = std::make_shared<TinyBackend>();
    backend->manifest.key = {"/science/a.h5", 7};
    backend->manifest.format_id = "hdf5";
    storage = std::make_shared<StructuredBlobStorage>(backend);
    auto permissions = std::make_shared<SourcePermissionSnapshot>();
    permissions->storage_namespace = storage->storage_namespace();
    permissions->metadata_generation = kind == "generation" ? 8 : 9;
    permissions->observed_at = std::chrono::steady_clock::now() -
      (kind == "expired" ? std::chrono::seconds(2) : std::chrono::seconds(0));
    permissions->sources[backend->manifest.key] = {"hdf5", backend->manifest.revision,
      backend->manifest.payload_size, backend->manifest.layout_size, 0444, 1000, 2000, false};
    if (kind == "namespace") ++permissions->storage_namespace;
    if (kind == "identity") ++permissions->sources[backend->manifest.key].manifest_revision;
    catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::success({{backend->manifest},
        kind == "absent" ? nullptr : permissions});
    }, storage->storage_namespace());
    std::atomic<unsigned> authorizations{0}, fresh_source_reads{0};
    QueryRegistrationSocketServer server(socket, "/mount", frontend, catalog, storage, runtime, bridge,
      [&](const QueryRegistrationPeer&, const auto& sources, const CatalogSelection* selected) {
        ++authorizations;
        EXPECT_NE(selected, nullptr);
        const CatalogSourcePermission* fact = selected && selected->versions().size() == 1 && sources.size() == 1 ?
          validated_catalog_source_permission(selected, &selected->versions()[0], sources[0],
            storage->storage_namespace(), 9) : nullptr;
        EXPECT_EQ(fact, nullptr);
        if (!fact) ++fresh_source_reads; // Fresh inode observation denies the simulated user.
        return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "fresh source denied");
      });
    ASSERT_TRUE(server.start().ok());
    // This source has no valid time-coverage marker. Permission denial must win
    // before coverage evaluation, even when the requested range would filter it.
    const auto response = submit_query_control(socket,
      R"({"kind":"dataset_catalog","request":{"sources":{"paths":[{"kind":"exact","path":"/science/a.h5"}]} ,"dataset_path":"/T","time_range":{"lower_unix_s":42,"upper_unix_s":43}}})");
    EXPECT_FALSE(response.ok());
    EXPECT_EQ(response.status().code(), QueryError::PermissionDenied);
    EXPECT_EQ(authorizations, 1u);
    EXPECT_EQ(fresh_source_reads, 1u);
    EXPECT_EQ(backend->manifest_reads, 0u);
    EXPECT_EQ(frontend->size(), 0u);
    server.stop();
  }
}

TEST_F(SocketFixture, DatasetDiscoveryAbsentHintCanUseFreshAuthorization) {
  auto backend = std::make_shared<TinyBackend>();
  backend->manifest.key = {"/science/a.h5", 7};
  backend->manifest.format_id = "hdf5";
  storage = std::make_shared<StructuredBlobStorage>(backend);
  catalog = make_in_memory_published_version_catalog({backend->manifest});
  std::atomic<unsigned> fresh_source_reads{0};
  QueryRegistrationSocketServer server(socket, "/mount", frontend, catalog, storage, runtime, bridge,
    [&](const QueryRegistrationPeer&, const auto& sources, const CatalogSelection* selected) {
      EXPECT_NE(selected, nullptr);
      if (selected) EXPECT_EQ(selected->source_permissions, nullptr);
      EXPECT_EQ(sources.size(), 1u);
      ++fresh_source_reads;
      return QueryStatus::success(); // Fresh source + ancestor authorization allowed.
    });
  ASSERT_TRUE(server.start().ok());
  const auto response = submit_query_control(socket,
    R"({"kind":"dataset_catalog","request":{"sources":{"paths":[{"kind":"exact","path":"/science/a.h5"}]}}})");
  EXPECT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), QueryError::LayoutUnavailable); // Artifact step is reached.
  EXPECT_EQ(fresh_source_reads, 1u);
  EXPECT_GT(backend->manifest_reads, 0u);
  EXPECT_EQ(frontend->size(), 0u);
}

TEST_F(SocketFixture, DatasetDiscoveryRejectsInvalidRequestBeforeBackendAndDoesNotPublish) {
  std::atomic<int> calls{0};
  QueryRegistrationSocketServer server(socket, "/mount", frontend, catalog, storage, runtime, bridge,
    {}, 0600, {}, {}, {}, [&](const DatasetCatalogRequest& request, const DatasetCatalogAuthorizer&, const std::shared_ptr<HotQueryCache::Binding>&) {
      EXPECT_TRUE(request.time_range.has_value());
      EXPECT_FALSE(request.dataset_path.has_value());
      ++calls; return QueryResult<DatasetCatalogResult>::success({});
    });
  ASSERT_TRUE(server.start().ok());
  for (const auto* json : {
      R"({"kind":"dataset_catalog","request":{"sources":{},"max_datasets":0}})",
      R"({"kind":"dataset_catalog","request":{"sources":{},"time_range":{"lower_unix_s":42,"upper_unix_s":43}}})",
      R"({"kind":"dataset_catalog","request":{"sources":{},"dataset_path":"/T","time_range":{"lower_unix_s":43,"upper_unix_s":42}}})",
      R"({"kind":"dataset_catalog","request":{"sources":{},"unexpected":true}})",
      R"({"kind":"dataset_catalog","request":{"sources":{},"predicate":{"kind":"null","field":"hdf5.attr:%00"}}})"}) {
    SCOPED_TRACE(json);
    auto result = submit_query_control(socket, json);
    EXPECT_FALSE(result.ok());
  }
  EXPECT_EQ(calls, 0);
  server.stop();
}

TEST_F(SocketFixture, TypedRegistrationReusesCatalogManifestAndAuthorizationOnlyWithinRequest) {
  auto backend = std::make_shared<TinyBackend>();
  storage = std::make_shared<StructuredBlobStorage>(backend);
  std::atomic<unsigned> scans{0}, authorizations{0};
  std::atomic<bool> allowed{true};
  catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
    ++scans;
    return QueryResult<PublishedVersionSnapshot>::success({{backend->manifest}, {}});
  }, storage->storage_namespace());
  auto query = QueryModule::create(catalog, storage, {});
  ASSERT_TRUE(query.ok());
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto service = std::make_shared<TypedQueryService>(query.value(), frontend, results,
    64ULL * 1024 * 1024);
  bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
  std::vector<std::int64_t> authorized_versions;
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, [&](const QueryRegistrationPeer&, const auto& sources, const CatalogSelection* selected) {
      ++authorizations;
      EXPECT_TRUE(selected != nullptr);
      for (const auto& source : sources) authorized_versions.push_back(source.version);
      return allowed ? QueryStatus::success() : QueryStatus::failure(
        QueryError::PermissionDenied, QueryPhase::Catalog, "permission revoked");
    });
  ASSERT_TRUE(server.start().ok());
  const auto request = R"({"kind":"typed","request":{"sources":{"paths":[{"kind":"exact","path":"/dataset/a.arrow"}],"versions":{"kind":"latest"}}}})";
  auto first = submit_query_registration(socket, request);
  ASSERT_TRUE(first.ok()) << first.status().message();
  EXPECT_EQ(scans, 1u);
  EXPECT_EQ(authorizations, 1u);
  EXPECT_EQ(backend->manifest_reads, 0u);

  // Cached result access uses its caller's authorization, never the registration's.
  auto route = first.value().substr(std::string("/mnt/data").size());
  route.resize(route.rfind("&result="));
  unsigned reopen_checks = 0;
  auto reopened = bridge->acquire(route, [&](const auto&) {
    ++reopen_checks;
    return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
      "permission revoked after registration");
  });
  EXPECT_FALSE(reopened.ok());
  EXPECT_EQ(reopen_checks, 1u);

  allowed = false;
  auto denied = submit_query_registration(socket, request);
  ASSERT_FALSE(denied.ok());
  EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(scans, 2u);
  EXPECT_EQ(authorizations, 2u);

  allowed = true;
  backend->manifest.key.version = 8;
  auto next = submit_query_registration(socket, request);
  ASSERT_TRUE(next.ok()) << next.status().message();
  server.stop();
  EXPECT_EQ(scans, 3u);
  EXPECT_EQ(authorizations, 3u);
  EXPECT_EQ(backend->manifest_reads, 0u);
  EXPECT_EQ(authorized_versions, (std::vector<std::int64_t>{7, 7, 8}));
}

TEST(CatalogSourceAuthorization, RequiresFreshExactBackendAndManifestEvidence) {
  StructuredBlobManifest manifest;
  manifest.key = {"/dataset/a.hdf5", 7};
  manifest.format_id = "hdf5";
  manifest.phase = StructuredBlobPhase::Published;
  manifest.published = true;
  manifest.revision = 3;
  manifest.payload_size = 100;
  manifest.layout_size = 20;
  QuerySourceReference source{manifest.key.path, 7, "hdf5"};
  const auto now = std::chrono::steady_clock::now();
  auto snapshot = std::make_shared<SourcePermissionSnapshot>();
  snapshot->storage_namespace = 9;
  snapshot->metadata_generation = 4;
  snapshot->observed_at = now;
  snapshot->sources[manifest.key] = {"hdf5", 3, 100, 20, 0640, 1000, 40077, false};
  CatalogSelection selected{{manifest}, {}, 9, snapshot};
  auto lookup = [&] {
    return validated_catalog_source_permission(&selected, &manifest, source, 9, 4, now);
  };
  ASSERT_NE(lookup(), nullptr);
  EXPECT_EQ(validated_catalog_source_permission(nullptr, &manifest, source, 9, 4, now), nullptr);
  EXPECT_EQ(validated_catalog_source_permission(&selected, nullptr, source, 9, 4, now), nullptr);
  EXPECT_EQ(validated_catalog_source_permission(&selected, &manifest, source, 8, 4, now), nullptr);
  EXPECT_EQ(validated_catalog_source_permission(&selected, &manifest, source, 9, 5, now), nullptr);
  selected.storage_namespace = 8;
  EXPECT_EQ(lookup(), nullptr);
  selected.storage_namespace = 9;
  snapshot->storage_namespace = 8;
  EXPECT_EQ(lookup(), nullptr);
  snapshot->storage_namespace = 9;
  snapshot->observed_at = now - std::chrono::seconds(2);
  EXPECT_EQ(lookup(), nullptr);
  snapshot->observed_at = now + std::chrono::milliseconds(1);
  EXPECT_EQ(lookup(), nullptr);
  snapshot->observed_at = now;
  source.version = 8;
  EXPECT_EQ(lookup(), nullptr);
  source.version = 7;
  source.path = "/other";
  EXPECT_EQ(lookup(), nullptr);
  source.path = manifest.key.path;
  source.format_id = "grib";
  EXPECT_EQ(lookup(), nullptr);
  source.format_id = "hdf5";
  auto& fact = snapshot->sources[manifest.key];
  ++fact.manifest_revision;
  EXPECT_EQ(lookup(), nullptr);
  --fact.manifest_revision;
  ++fact.payload_size;
  EXPECT_EQ(lookup(), nullptr);
  --fact.payload_size;
  ++fact.layout_size;
  EXPECT_EQ(lookup(), nullptr);
  --fact.layout_size;
  fact.format_id = "grib";
  EXPECT_EQ(lookup(), nullptr);
  fact.format_id = "hdf5";
  fact.uid = -1;
  EXPECT_EQ(lookup(), nullptr);
  fact.uid = 1000;
  fact.gid = -1;
  EXPECT_EQ(lookup(), nullptr);
  fact.gid = 40077;
  fact.mode = S_IFREG | 0640;
  EXPECT_NE(lookup(), nullptr);
  fact.mode = 0200000 | 0640;
  EXPECT_EQ(lookup(), nullptr);
  fact.mode = 0640;
  manifest.published = false;
  EXPECT_EQ(lookup(), nullptr);
  manifest.published = true;
  manifest.phase = StructuredBlobPhase::Described;
  EXPECT_EQ(lookup(), nullptr);
  manifest.phase = StructuredBlobPhase::Published;
  ASSERT_NE(lookup(), nullptr);
  fact.is_dir = true;
  ASSERT_NE(lookup(), nullptr);
  EXPECT_TRUE(lookup()->is_dir); // Caller rejects a directory as a file source.
  snapshot->sources.clear();
  EXPECT_EQ(lookup(), nullptr);
}

TEST(CatalogSourceAuthorization, PreservesOwnerGroupSupplementaryAndDenialRules) {
  EXPECT_EQ(check_query_posix_access(0640, 1000, 40077, R_OK, 1000, 99), 0);
  EXPECT_EQ(check_query_posix_access(S_IFREG | 0640, 1000, 40077, R_OK, 1000, 99), 0);
  EXPECT_EQ(check_query_posix_access(S_IFDIR | 0750, 1000, 40077, X_OK, 65534, 99, {40077}), 0);
  EXPECT_EQ(check_query_posix_access(0640, 1000, 40077, R_OK, 65534, 40077), 0);
  EXPECT_EQ(check_query_posix_access(0640, 1000, 40077, R_OK, 65534, 99, {40077}), 0);
  EXPECT_EQ(check_query_posix_access(0640, 1000, 40077, R_OK, 65534, 99), -EACCES);
  EXPECT_EQ(check_query_posix_access(0600, 1000, 40077, R_OK, 65534, 99, {40077}), -EACCES);
  // Owner precedence must not inherit more generous group/other permissions.
  EXPECT_EQ(check_query_posix_access(0044, 1000, 40077, R_OK, 1000, 40077), -EACCES);
  EXPECT_EQ(check_query_posix_access(0750, 1000, 40077, X_OK, 65534, 99, {40077}), 0);
  EXPECT_EQ(check_query_posix_access(0640, 1000, 40077, X_OK, 65534, 99, {40077}), -EACCES);
  EXPECT_EQ(check_query_posix_access(0000, 1000, 40077, R_OK, 0, 0), 0);
  EXPECT_EQ(check_query_posix_access(-1, 1000, 40077, R_OK, 0, 0), -EACCES);
  EXPECT_EQ(check_query_posix_access(0640, -1, 40077, R_OK, 0, 0), -EACCES);
}

TEST(CatalogSourceAuthorization, MutationRetriesWithoutCatalogEvidenceAndKeepsDenial) {
  std::uint64_t generation = 3;
  unsigned calls = 0;
  auto result = authorize_source_permissions_with_generation(
    [&] { return generation; }, [&](bool use_catalog, std::uint64_t observed) {
      ++calls;
      EXPECT_EQ(observed, generation);
      if (calls == 1) {
        EXPECT_TRUE(use_catalog);
        ++generation; // Same-process chmod revokes the observed read permission.
        return QueryStatus::success();
      }
      EXPECT_FALSE(use_catalog);
      return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
        "fresh source read denied");
    });
  EXPECT_EQ(calls, 2u);
  EXPECT_EQ(result.code(), QueryError::PermissionDenied);
}

TEST(CatalogSourceAuthorization, RepeatedMutationsReturnStalePlan) {
  std::uint64_t generation = 0;
  unsigned calls = 0;
  auto result = authorize_source_permissions_with_generation(
    [&] { return generation; }, [&](bool use_catalog, std::uint64_t) {
      EXPECT_EQ(use_catalog, calls == 0);
      ++calls;
      ++generation;
      return QueryStatus::success();
    });
  EXPECT_EQ(calls, 2u);
  EXPECT_EQ(result.code(), QueryError::StalePlan);
}

TEST(CatalogSourceAuthorization, PermissionGrantRetriesAStaleDenial) {
  std::uint64_t generation = 0;
  unsigned calls = 0;
  auto result = authorize_source_permissions_with_generation(
    [&] { return generation; }, [&](bool use_catalog, std::uint64_t) {
      if (++calls == 1) {
        EXPECT_TRUE(use_catalog);
        ++generation;
        return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
          "permission observation preceded grant");
      }
      EXPECT_FALSE(use_catalog);
      return QueryStatus::success();
    });
  EXPECT_EQ(calls, 2u);
  EXPECT_TRUE(result.ok());
}

TEST(CatalogParentAuthorization, CollectsRootToLeafAncestorPaths) {
  EXPECT_EQ(query_source_parent_paths("/a/b/file.hdf5"),
    (std::vector<std::string>{"/", "/a", "/a/b"}));
  EXPECT_EQ(query_source_parent_paths("/file.hdf5"), (std::vector<std::string>{"/"}));
}

TEST(CatalogParentAuthorization, BatchesSharedAncestorsExactlyOnce) {
  auto first = query_source_parent_paths("/a/b/one.hdf5");
  auto second = query_source_parent_paths("/a/c/two.hdf5");
  first.insert(first.end(), second.begin(), second.end());
  unsigned loads = 0;
  auto result = resolve_query_parent_permissions(nullptr, first, 9, 4,
    [&](const std::vector<std::string>& requested) {
      ++loads;
      EXPECT_EQ(requested, (std::vector<std::string>{"/", "/a", "/a/b", "/a/c"}));
      auto snapshot = std::make_shared<SourcePermissionSnapshot>();
      snapshot->storage_namespace = 9;
      snapshot->metadata_generation = 4;
      snapshot->observed_at = std::chrono::steady_clock::now();
      for (const auto& path : requested)
        snapshot->parents[path] = CatalogParentPermission{17, S_IFDIR | 0750, 1000, 40077, true};
      return QueryResult<std::shared_ptr<const SourcePermissionSnapshot>>::success(snapshot);
    });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(loads, 1u);
  EXPECT_EQ(result.value().observations.size(), 4u);
  EXPECT_EQ(result.value().catalog_reuse, 0u);
  const QuerySourceReference source{"/a/b/one.hdf5", 7, "hdf5"};
  for (const auto& [path, permission] : result.value().observations) {
    EXPECT_TRUE(check_query_parent_permission(permission, source, 65534, 99, {40077}).ok());
    EXPECT_EQ(check_query_parent_permission(permission, source, 65534, 99, {}).code(),
      QueryError::PermissionDenied);
  }
}

TEST(CatalogParentAuthorization, ObservedMissingAndLatestNonDirectoryNeverFallBack) {
  SourcePermissionSnapshot hint;
  hint.storage_namespace = 9;
  hint.metadata_generation = 4;
  hint.observed_at = std::chrono::steady_clock::now();
  hint.parents["/"] = CatalogParentPermission{17, 0755, 0, 0, true};
  hint.parents["/missing"] = std::nullopt;
  hint.parents["/replaced"] = CatalogParentPermission{99, 0755, 0, 0, false};
  unsigned loads = 0;
  auto result = resolve_query_parent_permissions(&hint, {"/", "/missing", "/replaced"}, 9, 4,
    [&](const std::vector<std::string>&) {
      ++loads;
      return QueryResult<std::shared_ptr<const SourcePermissionSnapshot>>::failure(
        QueryStatus::failure(QueryError::Internal, QueryPhase::Catalog, "must not load"));
    });
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(loads, 0u);
  EXPECT_EQ(result.value().catalog_reuse, 3u);
  QuerySourceReference source{"/missing/file.hdf5", 7, "hdf5"};
  auto missing = check_query_parent_permission(result.value().observations.at("/missing"), source, 0, 0, {});
  EXPECT_EQ(missing.code(), QueryError::StalePlan);
  ASSERT_TRUE(missing.detail().source.has_value());
  EXPECT_EQ(missing.detail().source->path, source.path);
  EXPECT_EQ(check_query_parent_permission(result.value().observations.at("/replaced"),
    source, 0, 0, {}).code(), QueryError::StalePlan);
}

TEST(CatalogParentAuthorization, PartialObservationLoadsOnlyUnobservedPaths) {
  SourcePermissionSnapshot hint;
  hint.storage_namespace = 9;
  hint.metadata_generation = 4;
  hint.observed_at = std::chrono::steady_clock::now();
  hint.parents["/"] = CatalogParentPermission{17, 0755, 0, 0, true};
  unsigned loads = 0;
  auto result = resolve_query_parent_permissions(&hint, {"/", "/a", "/a"}, 9, 4,
    [&](const std::vector<std::string>& requested) {
      ++loads;
      EXPECT_EQ(requested, (std::vector<std::string>{"/a"}));
      auto loaded = std::make_shared<SourcePermissionSnapshot>();
      loaded->storage_namespace = 9;
      loaded->metadata_generation = 4;
      loaded->observed_at = std::chrono::steady_clock::now();
      loaded->parents["/a"] = CatalogParentPermission{19, 0755, 0, 0, true};
      return QueryResult<std::shared_ptr<const SourcePermissionSnapshot>>::success(loaded);
    });
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(loads, 1u);
  EXPECT_EQ(result.value().catalog_reuse, 1u);
  EXPECT_EQ(result.value().observations.at("/a")->version, 19);
}

TEST(CatalogParentAuthorization, ForeignExpiredAndChangedGenerationObservationsReload) {
  const auto now = std::chrono::steady_clock::now();
  for (int variant = 0; variant < 4; ++variant) {
    SourcePermissionSnapshot hint;
    hint.storage_namespace = variant == 0 ? 8 : 9;
    hint.metadata_generation = variant == 1 ? 3 : 4;
    hint.observed_at = variant == 2 ? now - std::chrono::seconds(2) :
      variant == 3 ? now + std::chrono::seconds(1) : now;
    hint.parents["/"] = std::nullopt;
    unsigned loads = 0;
    auto result = resolve_query_parent_permissions(&hint, {"/"}, 9, 4,
      [&](const std::vector<std::string>& requested) {
        ++loads;
        auto loaded = std::make_shared<SourcePermissionSnapshot>();
        loaded->storage_namespace = 9;
        loaded->metadata_generation = 4;
        loaded->observed_at = std::chrono::steady_clock::now();
        loaded->parents["/"] = CatalogParentPermission{19, 0755, 0, 0, true};
        return QueryResult<std::shared_ptr<const SourcePermissionSnapshot>>::success(loaded);
      }, now);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(loads, 1u);
    EXPECT_EQ(result.value().catalog_reuse, 0u);
    EXPECT_TRUE(result.value().observations.at("/").has_value());
  }
}

TEST(CatalogParentAuthorization, IncompleteForeignOrChangedGenerationBulkFailsClosed) {
  for (int variant = 0; variant < 3; ++variant) {
    auto result = resolve_query_parent_permissions(nullptr, {"/", "/a"}, 9, 4,
      [&](const std::vector<std::string>&) {
        auto loaded = std::make_shared<SourcePermissionSnapshot>();
        loaded->storage_namespace = variant == 1 ? 8 : 9;
        loaded->metadata_generation = variant == 2 ? 3 : 4;
        loaded->observed_at = std::chrono::steady_clock::now();
        loaded->parents["/"] = CatalogParentPermission{19, 0755, 0, 0, true};
        if (variant != 0)
          loaded->parents["/a"] = CatalogParentPermission{20, 0755, 0, 0, true};
        return QueryResult<std::shared_ptr<const SourcePermissionSnapshot>>::success(loaded);
      });
    ASSERT_FALSE(result.ok());
    EXPECT_EQ(result.status().code(), variant == 0 ? QueryError::ReadFailure : QueryError::StalePlan);
  }
}

TEST(CatalogParentAuthorization, LongSynchronousBulkReadDoesNotInheritHintExpiry) {
  auto result = resolve_query_parent_permissions(nullptr, {"/", "/a"}, 9, 4,
    [&](const std::vector<std::string>& requested) {
      auto loaded = std::make_shared<SourcePermissionSnapshot>();
      loaded->storage_namespace = 9;
      loaded->metadata_generation = 4;
      loaded->observed_at = std::chrono::steady_clock::now() - std::chrono::seconds(2);
      for (const auto& path : requested)
        loaded->parents[path] = CatalogParentPermission{19, 0755, 0, 0, true};
      return QueryResult<std::shared_ptr<const SourcePermissionSnapshot>>::success(loaded);
    });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(result.value().observations.size(), 2u);
  EXPECT_EQ(result.value().catalog_reuse, 0u);
}

TEST(CatalogParentAuthorization, InvalidParentAndUpperAncestorDenialAreAttributedToSource) {
  QuerySourceReference source{"/a/b/c/file.hdf5", 7, "hdf5"};
  EXPECT_EQ(check_query_parent_permission(CatalogParentPermission{-1, 0755, 0, 0, true},
    source, 0, 0, {}).code(), QueryError::StalePlan);
  EXPECT_EQ(check_query_parent_permission(CatalogParentPermission{7, -1, 0, 0, true},
    source, 0, 0, {}).code(), QueryError::StalePlan);
  auto denied = check_query_parent_permission(CatalogParentPermission{7, 0700, 0, 0, true},
    source, 65534, 65534, {});
  EXPECT_EQ(denied.code(), QueryError::PermissionDenied);
  ASSERT_TRUE(denied.detail().source.has_value());
  EXPECT_EQ(denied.detail().source->version, 7);
  EXPECT_EQ(denied.detail().source->path, source.path);
}

TEST(TypedAuthorizationCache, EquivalentSupplementaryGroupsMatchSocketAndFuseKeys) {
  const std::string route = "/.iotdbfs/query/v1/typed.ipc?id=" + std::string(48, 'a') +
    "&result=" + std::string(64, 'b');
  const auto socket_key = typed_authorization_cache_key(65534, 65534, route, {40077, 40088});
  const auto fuse_key = typed_authorization_cache_key(65534, 65534, route,
    {65534, 40088, 40077, 40077, 65534});
  EXPECT_EQ(socket_key, fuse_key);
  EXPECT_EQ(normalized_authorization_groups(65534, {40088, 65534, 40077, 40077}),
    (std::vector<gid_t>{40077, 40088}));
  EXPECT_EQ(typed_authorization_cache_key(0, 0, route, {}),
    typed_authorization_cache_key(0, 0, route, {0, 0}));
}

TEST(TypedAuthorizationCache, DistinctSupplementaryGroupsAndIdentitiesStayIsolated) {
  const std::string route = "/.iotdbfs/query/v1/typed.ipc?id=" + std::string(48, 'a') +
    "&result=" + std::string(64, 'b');
  const auto key = typed_authorization_cache_key(65534, 65534, route, {40077});
  EXPECT_NE(key, typed_authorization_cache_key(65534, 65534, route, {}));
  EXPECT_NE(key, typed_authorization_cache_key(65534, 65534, route, {40088}));
  EXPECT_NE(key, typed_authorization_cache_key(65533, 65534, route, {40077}));
  EXPECT_NE(key, typed_authorization_cache_key(65534, 65533, route, {40077}));
}

TEST(TypedAuthorizationCache, CanonicalAliasesRetainResultAndRegistrationIsolation) {
  const auto id = std::string(48, 'a');
  const auto result = std::string(64, 'b');
  const auto base = "/.iotdbfs/query/v1/typed.ipc?id=" + id + "&result=" + result;
  const auto constrained = "/.iotdbfs/query/v1/typed.ipc?version=7&id=%61" + id.substr(1) + "&result=" + result;
  EXPECT_EQ(typed_authorization_cache_key(100, 99, base, {}),
    typed_authorization_cache_key(100, 99, constrained, {99}));
  EXPECT_NE(typed_authorization_cache_key(100, 99, base, {}),
    typed_authorization_cache_key(100, 99,
      "/.iotdbfs/query/v1/typed.ipc?id=" + std::string(48, 'c') + "&result=" + result, {}));
  EXPECT_NE(typed_authorization_cache_key(100, 99, base, {}),
    typed_authorization_cache_key(100, 99,
      "/.iotdbfs/query/v1/typed.ipc?id=" + id + "&result=" + std::string(64, 'd'), {}));
  EXPECT_TRUE(typed_authorization_cache_key(100, 99, "/other?id=x&result=y", {}).empty());
}

TEST(TypedAuthorizationCache, PrefillExpiresFromBeforeCheckAndNeverExtendsOnPublication) {
  TypedAuthorizationCache cache;
  const auto start = TypedAuthorizationCache::Clock::now();
  auto observed = cache.capture(start);
  ASSERT_TRUE(cache.prefill("valid", observed, start + std::chrono::milliseconds(300)));
  EXPECT_TRUE(cache.contains("valid", observed.generation, start + std::chrono::milliseconds(999)));
  EXPECT_FALSE(cache.contains("valid", observed.generation, start + std::chrono::milliseconds(1000)));
  EXPECT_FALSE(cache.prefill("slow", observed, start + std::chrono::milliseconds(1001)));
  EXPECT_TRUE(cache.store("slow", observed, start + std::chrono::milliseconds(1001)));
  EXPECT_EQ(cache.size(), 0u); // A slow valid check succeeds without extending its observation.
  EXPECT_FALSE(cache.prefill("future", cache.capture(start + std::chrono::seconds(2)), start));
}

TEST(TypedAuthorizationCache, DisabledShortAndCappedTtlAreRespected) {
  TypedAuthorizationCache cache;
  const auto now = TypedAuthorizationCache::Clock::now();
  const auto observed = cache.capture(now);
  cache.ttl = std::chrono::milliseconds(0);
  EXPECT_FALSE(cache.prefill("disabled", observed, now));
  EXPECT_TRUE(cache.store("disabled", observed, now));
  EXPECT_FALSE(cache.contains("disabled", observed.generation, now));
  cache.ttl = std::chrono::milliseconds(100);
  ASSERT_TRUE(cache.prefill("short", observed, now));
  EXPECT_FALSE(cache.contains("short", observed.generation, now + std::chrono::milliseconds(100)));
  cache.ttl = std::chrono::milliseconds(5000);
  ASSERT_TRUE(cache.prefill("capped", observed, now));
  EXPECT_FALSE(cache.contains("capped", observed.generation, now + std::chrono::milliseconds(1000)));
}

TEST(TypedAuthorizationCache, MutationAndBoundsPreventPublicationAdmission) {
  TypedAuthorizationCache cache(2);
  const auto now = TypedAuthorizationCache::Clock::now();
  const auto old = cache.capture(now);
  cache.invalidate();
  EXPECT_FALSE(cache.prefill("mutated", old, now));
  EXPECT_FALSE(cache.store("mutated", old, now));
  const auto current = cache.capture(now);
  EXPECT_FALSE(cache.prefill(std::string(4097, 'x'), current, now));
  EXPECT_FALSE(cache.prefill("", current, now));
  EXPECT_TRUE(cache.prefill("a", current, now));
  EXPECT_TRUE(cache.prefill("b", current, now));
  EXPECT_TRUE(cache.prefill("c", current, now));
  EXPECT_LE(cache.size(), 2u);
  cache.invalidate();
  EXPECT_EQ(cache.size(), 0u);
}

TEST_F(SocketFixture, PublicationCommitsPermissionObservationOnlyAfterSuccessfulAuthorization) {
  TypedAuthorizationCache cache;
  std::atomic<int> phase{0}, commits{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge,
    [&](const QueryRegistrationPeer&, const auto&, const CatalogSelection*) {
      EXPECT_EQ(phase.load(), 1);
      phase = 2;
      return QueryStatus::success();
    }, 0600, {}, {}, {}, {}, {}, {},
    [&](const QueryRegistrationPeer& peer, const CatalogSelection* selected)
        -> QueryRegistrationSocketServer::AuthorizationPublicationCommit {
      EXPECT_EQ(phase.load(), 0);
      EXPECT_NE(selected, nullptr);
      phase = 1;
      const auto observed = cache.capture();
      return [&, peer, observed](std::string_view route) {
        EXPECT_EQ(phase.load(), 2);
        EXPECT_NE(route.find("&result="), std::string_view::npos);
        ++commits;
        return cache.prefill(typed_authorization_cache_key(peer.uid, peer.gid,
          std::string(route), peer.supplementary_groups), observed);
      };
    });
  ASSERT_TRUE(server.start().ok());
  auto published = submit_query_registration(socket, kTyped);
  ASSERT_TRUE(published.ok()) << published.status().message();
  EXPECT_EQ(commits.load(), 1);
  const auto route = published.value().substr(std::string("/mnt/data").size());
  std::vector<gid_t> groups;
#ifdef __linux__
  const int count = ::getgroups(0, nullptr);
  ASSERT_GE(count, 0);
  groups.resize(static_cast<std::size_t>(count));
  ASSERT_EQ(::getgroups(count, groups.data()), count);
#endif
  const auto key = typed_authorization_cache_key(::geteuid(), ::getegid(), route, groups);
  EXPECT_TRUE(cache.contains(key, cache.snapshot()));
  // The bridge checks a route's precondition before permission cache lookup.
  const auto marker = route.rfind("&result=");
  auto wrong_version = route.substr(0, marker);
  const auto version = wrong_version.find("&version=7");
  ASSERT_NE(version, std::string::npos);
  wrong_version.replace(version, std::string("&version=7").size(), "&version=8");
  unsigned checks = 0;
  auto denied = bridge->acquire_exact(wrong_version, route.substr(marker + 8), [&](const auto&) {
    ++checks;
    return QueryStatus::success();
  });
  EXPECT_FALSE(denied.ok());
  EXPECT_EQ(checks, 0u);
  auto invalid = bridge->acquire_exact(wrong_version + "&unexpected=1", route.substr(marker + 8), [&](const auto&) {
    ++checks;
    return QueryStatus::success();
  });
  EXPECT_FALSE(invalid.ok());
  EXPECT_EQ(checks, 0u);
  server.stop();
}

TEST_F(SocketFixture, FailedAuthorizationOrMaterializationNeverCommitsPublicationObservation) {
  for (bool fail_authorization : {true, false}) {
    auto query = QueryModule::create(catalog, storage, {});
    ASSERT_TRUE(query.ok());
    TypedResultStore::Config config;
    config.max_result_bytes = fail_authorization ? 64ULL * 1024 * 1024 : 1;
    auto results = std::make_shared<TypedResultStore>(config);
    auto service = std::make_shared<TypedQueryService>(query.value(), frontend, results,
      64ULL * 1024 * 1024);
    auto limited_bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
    std::atomic<unsigned> commits{0}, captured{0};
    QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
      storage, runtime, limited_bridge,
      [&](const QueryRegistrationPeer&, const auto&, const CatalogSelection*) {
        return fail_authorization ? QueryStatus::failure(QueryError::PermissionDenied,
          QueryPhase::Catalog, "denied") : QueryStatus::success();
      }, 0600, {}, {}, {}, {}, {}, {},
      [&](const QueryRegistrationPeer&, const CatalogSelection*)
          -> QueryRegistrationSocketServer::AuthorizationPublicationCommit {
        ++captured;
        return [&](std::string_view) { ++commits; return true; };
      });
    ASSERT_TRUE(server.start().ok());
    EXPECT_FALSE(submit_query_registration(socket, kTyped).ok());
    EXPECT_EQ(captured.load(), 1u);
    EXPECT_EQ(commits.load(), 0u);
    server.stop();
  }
}

TEST_F(SocketFixture, MutationAfterCheckPreventsPublicationPrefill) {
  TypedAuthorizationCache cache;
  std::atomic<unsigned> commits{0};
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge,
    [&](const QueryRegistrationPeer&, const auto&, const CatalogSelection*) {
      cache.invalidate();
      return QueryStatus::success();
    }, 0600, {}, {}, {}, {}, {}, {},
    [&](const QueryRegistrationPeer&, const CatalogSelection*)
        -> QueryRegistrationSocketServer::AuthorizationPublicationCommit {
      const auto observed = cache.capture();
      return [&, observed](std::string_view route) {
        ++commits;
        return cache.prefill(std::string(route), observed);
      };
    });
  ASSERT_TRUE(server.start().ok());
  EXPECT_TRUE(submit_query_registration(socket, kTyped).ok());
  EXPECT_EQ(commits.load(), 1u);
  EXPECT_EQ(cache.size(), 0u);
  server.stop();
}

namespace {
// Deterministic owner seam: mutation is injected while execution owns a lease,
// after the first admission has released its short gate.
struct OwnerAdmissionState {
  std::recursive_mutex mutex;
  bool revoked{false};
  unsigned gates{0}, leases{0}, admits{0};
  uid_t uid{::getuid()};
};
struct TestExecutionLease final : QueryExecutionLease {
  explicit TestExecutionLease(std::shared_ptr<OwnerAdmissionState> s) : state(std::move(s)) {
    ++state->leases;
  }
  ~TestExecutionLease() override { std::lock_guard<std::recursive_mutex> lock(state->mutex); --state->leases; }
  std::shared_ptr<OwnerAdmissionState> state;
};
struct TestAdmission final : QueryAdmission {
  explicit TestAdmission(std::shared_ptr<OwnerAdmissionState> s)
    : state(std::move(s)), lock(state->mutex) { ++state->gates; }
  ~TestAdmission() override { --state->gates; }
  std::shared_ptr<QueryExecutionLease> execution_lease() override {
    return std::make_shared<TestExecutionLease>(state);
  }
  std::shared_ptr<OwnerAdmissionState> state;
  std::unique_lock<std::recursive_mutex> lock;
};
class TestReadContext : public QueryReadContext,
    public std::enable_shared_from_this<TestReadContext> {
public:
  std::shared_ptr<OwnerAdmissionState> state = std::make_shared<OwnerAdmissionState>();
  std::vector<StructuredBlobManifest> manifests;
  std::vector<QuerySourceReference> candidates;
  std::function<void()> during_manifest, during_admit, during_scan;
  bool expose_context{true};
  QueryResult<std::shared_ptr<const QueryReadContext>> select(
      const std::vector<StructuredBlobManifest>&) const override {
    return QueryResult<std::shared_ptr<const QueryReadContext>>::success(shared_from_this());
  }
  const std::vector<QuerySourceReference>& sources() const override { return candidates; }
  QueryResult<std::shared_ptr<QueryAdmission>> admit(const QueryPrincipal& principal) const override {
    std::lock_guard<std::recursive_mutex> lock(state->mutex);
    ++state->admits;
    if (during_admit) during_admit();
    if (state->revoked || principal.uid != state->uid)
      return QueryResult<std::shared_ptr<QueryAdmission>>::failure(QueryStatus::failure(
        QueryError::PermissionDenied, QueryPhase::Catalog, "owner dependency was revoked"));
    return QueryResult<std::shared_ptr<QueryAdmission>>::success(std::make_shared<TestAdmission>(state));
  }
  QueryResult<StructuredBlobManifest> manifest(const StructuredBlobVersionKey& key) const override {
    EXPECT_EQ(state->gates, 0u);
    EXPECT_GT(state->leases, 0u);
    EXPECT_TRUE(current_query_execution_lease());
    if (during_manifest) during_manifest();
    for (const auto& m : manifests) if (m.key == key)
      return QueryResult<StructuredBlobManifest>::success(m);
    return QueryResult<StructuredBlobManifest>::failure(QueryStatus::failure(
      QueryError::SourceNotFound, QueryPhase::Catalog, "missing frozen source"));
  }
};
class ContextCatalog final : public PublishedVersionCatalog {
public:
  explicit ContextCatalog(std::shared_ptr<TestReadContext> value) : context(std::move(value)) {}
  QueryResult<CatalogSelection> scan(const SourceSelector& selector, std::size_t cap) const override {
    if (context->during_scan) context->during_scan();
    auto catalog = make_in_memory_published_version_catalog(context->manifests);
    auto selected = catalog->scan(selector, cap);
    if (selected.ok() && context->expose_context) selected.value().read_context = context;
    return selected;
  }
  std::shared_ptr<TestReadContext> context;
};
struct OwnerBridgeFixture {
  std::shared_ptr<TinyBackend> backend = std::make_shared<TinyBackend>();
  std::shared_ptr<TestReadContext> context = std::make_shared<TestReadContext>();
  std::shared_ptr<TypedQueryFrontend> frontend = std::make_shared<TypedQueryFrontend>();
  std::shared_ptr<TypedResultStore> results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  std::shared_ptr<ContextCatalog> catalog;
  std::shared_ptr<FuseTypedQueryBridge> bridge;
  std::shared_ptr<TypedQueryService> service;
  QueryRequest request;
  QueryPrincipal principal{::getuid(), ::getgid(), {}};
  explicit OwnerBridgeFixture(std::shared_ptr<TestReadContext> value = {}) {
    if (value) context = std::move(value);
    context->manifests = {backend->manifest};
    context->candidates = {{backend->manifest.key.path, backend->manifest.key.version, "arrow"}};
    auto storage = std::make_shared<StructuredBlobStorage>(backend);
    catalog = std::make_shared<ContextCatalog>(context);
    auto module = QueryModule::create(catalog, storage,
      {std::make_shared<ArrowFormatQueryAdapter>()});
    if (!module.ok()) throw std::runtime_error(module.status().message());
    service = std::make_shared<TypedQueryService>(module.value(), frontend, results, 1024 * 1024);
    bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
    request.sources.versions = VersionSelection::all();
  }
};

TEST(OwnerQueryAdmission, PreparedPublicationRacePreservesOtherUnopenedRoute) {
  OwnerBridgeFixture f;
  auto route_a = f.frontend->register_typed(f.request);
  auto route_b = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route_a.ok());
  ASSERT_TRUE(route_b.ok());
  ASSERT_NE(route_a.value(), route_b.value());
  std::string result_a;
  BinaryValue expected_bytes;
  bool interleaved = false;
  f.context->during_admit = [&] {
    // B has prepared capacity but has not published. Publish identical A
    // during B's final admission to reproduce the competing publication
    // deterministically, without threads, sleeps, sockets or a live database.
    if (interleaved || f.context->state->admits != 2) return;
    interleaved = true;
    auto published_a = f.bridge->acquire(route_a.value(), {}, nullptr, f.principal);
    ASSERT_TRUE(published_a.ok()) << published_a.status().message();
    result_a = published_a.value()->id();
    auto bytes = published_a.value()->read(0, published_a.value()->size());
    ASSERT_TRUE(bytes.ok()) << bytes.status().message();
    expected_bytes = std::move(bytes.value());
    // Only the bridge retains A after this callback, exactly as after socket
    // publication and before the client opens its immutable FUSE route.
  };
  auto published_b = f.bridge->acquire(route_b.value(), {}, nullptr, f.principal);
  ASSERT_TRUE(interleaved);
  ASSERT_FALSE(result_a.empty());
  ASSERT_FALSE(expected_bytes.empty());
  ASSERT_TRUE(published_b.ok()) << published_b.status().message();
  EXPECT_EQ(published_b.value()->id(), result_a);
  EXPECT_EQ(f.results->stats().registered_entries, 1u);
  EXPECT_EQ(f.results->stats().resident_bytes, expected_bytes.size());
  EXPECT_EQ(f.frontend->size(), 2u);
  // A publication race is not capacity pressure. Both registrations must
  // survive; the cache relief callback would erase the otherwise idle A.
  EXPECT_EQ(f.bridge->cached_routes(), 2u);
  auto admits_before_open = f.context->state->admits;
  auto opened_a = f.bridge->acquire_exact(route_a.value(), result_a, {}, f.principal);
  ASSERT_TRUE(opened_a.ok()) << opened_a.status().message();
  EXPECT_EQ(f.context->state->admits, admits_before_open + 1);
  auto opened_b = f.bridge->acquire_exact(route_b.value(), result_a, {}, f.principal);
  ASSERT_TRUE(opened_b.ok()) << opened_b.status().message();
  EXPECT_EQ(f.context->state->admits, admits_before_open + 2);
  EXPECT_EQ(opened_a.value()->read(0, opened_a.value()->size()).value(), expected_bytes);
  EXPECT_EQ(opened_b.value()->read(0, opened_b.value()->size()).value(), expected_bytes);
  auto wrong_principal = f.principal;
  ++wrong_principal.uid;
  EXPECT_EQ(f.bridge->acquire_exact(route_a.value(), result_a, {}, wrong_principal).status().code(),
    QueryError::PermissionDenied);
  f.context->state->revoked = true;
  EXPECT_EQ(f.bridge->acquire_exact(route_b.value(), result_a, {}, f.principal).status().code(),
    QueryError::PermissionDenied);
  EXPECT_EQ(opened_a.value()->read(0, opened_a.value()->size()).value(), expected_bytes);
  EXPECT_EQ(opened_b.value()->read(0, opened_b.value()->size()).value(), expected_bytes);
  EXPECT_EQ(f.context->state->gates, 0u);
  EXPECT_EQ(f.context->state->leases, 0u);
}

TEST(OwnerQueryAdmission, MaterializationBorrowsAllFrozenSourcesAndKeepsThemAlive) {
  std::optional<TypedQueryService::MaterializedResult> held;
  std::weak_ptr<const QueryReadContext> lifetime;
  const QuerySourceReference* references = nullptr;
  {
    OwnerBridgeFixture f;
    auto other = f.backend->manifest;
    other.key.path = "/dataset/predicate-miss.arrow";
    f.context->manifests.push_back(other);
    f.context->candidates.push_back({other.key.path, other.key.version, "arrow"});
    f.request.sources.file_predicate = make_comparison({"manifest.path"},
      ComparisonOperator::Equal, LogicalValue::utf8("/no-match"));
    auto route = f.frontend->register_typed(f.request);
    ASSERT_TRUE(route.ok());
    auto result = f.service->materialize_authorized(route.value(),
      [](const auto&) {
        ADD_FAILURE() << "owner execution must admit its frozen context";
        return QueryStatus::success();
      }, {}, nullptr, f.principal);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_TRUE(result.value().owned_sources.empty());
    EXPECT_EQ(&result.value().sources(), &f.context->sources());
    ASSERT_EQ(result.value().sources().size(), 2u);
    EXPECT_EQ(result.value().sources().back().path, other.key.path);
    EXPECT_EQ(f.context->state->admits, 2u);
    references = f.context->sources().data();
    lifetime = f.context;
    held = std::move(result.value());
    // The caller releases the short publication gate before keeping the result.
    held->publication_admission.reset();
    held->publication_cleanup.reset();
  }
  ASSERT_FALSE(lifetime.expired());
  EXPECT_EQ(held->sources().data(), references);
  EXPECT_EQ(held->sources().back().path, "/dataset/predicate-miss.arrow");
  held.reset();
  EXPECT_TRUE(lifetime.expired());
}

// These fakes exercise the frontend ownership/admission contract only. The
// real Owner integration probe verifies the production compact reference's
// identities, dependency checks and residency accounting.
class ResultReferenceTestContext final : public QueryReadContext {
public:
  std::shared_ptr<OwnerAdmissionState> state;
  std::vector<QuerySourceReference> candidates;
  mutable unsigned admits{0};
  std::function<void()> during_admit;
  QueryResult<std::shared_ptr<const QueryReadContext>> select(
      const std::vector<StructuredBlobManifest>&) const override {
    ADD_FAILURE() << "a completed result reference must not plan a query";
    return QueryResult<std::shared_ptr<const QueryReadContext>>::failure(
      QueryStatus::failure(QueryError::InvalidQuery, QueryPhase::Catalog,
        "result-only reference"));
  }
  const std::vector<QuerySourceReference>& sources() const override { return candidates; }
  QueryResult<std::shared_ptr<QueryAdmission>> admit(const QueryPrincipal& principal) const override {
    std::lock_guard<std::recursive_mutex> lock(state->mutex);
    ++admits;
    ++state->admits;
    if (during_admit) during_admit();
    if (state->revoked || principal.uid != state->uid)
      return QueryResult<std::shared_ptr<QueryAdmission>>::failure(QueryStatus::failure(
        QueryError::PermissionDenied, QueryPhase::Catalog, "owner dependency was revoked"));
    return QueryResult<std::shared_ptr<QueryAdmission>>::success(std::make_shared<TestAdmission>(state));
  }
  QueryResult<StructuredBlobManifest> manifest(const StructuredBlobVersionKey&) const override {
    ADD_FAILURE() << "a completed result reference must not read source content";
    return QueryResult<StructuredBlobManifest>::failure(QueryStatus::failure(
      QueryError::SourceNotFound, QueryPhase::Catalog, "result-only reference"));
  }
};
class DetachingTestReadContext final : public TestReadContext {
public:
  std::function<std::shared_ptr<const QueryReadContext>()> prepare_reference;
  std::function<void()> during_destroy;
  ~DetachingTestReadContext() override { if (during_destroy) during_destroy(); }
  mutable unsigned reference_calls{0};
  std::shared_ptr<const QueryReadContext> result_reference() const noexcept override {
    ++reference_calls;
    return prepare_reference ? prepare_reference() : nullptr;
  }
};
std::shared_ptr<ResultReferenceTestContext> result_reference_for(
    const std::shared_ptr<TestReadContext>& context) {
  auto reference = std::make_shared<ResultReferenceTestContext>();
  reference->state = context->state;
  reference->candidates = context->candidates;
  return reference;
}

TEST(OwnerQueryAdmission, DetachedMaterializationReleasesPlanningContextAndKeepsPredicateMisses) {
  std::optional<TypedQueryService::MaterializedResult> held;
  std::weak_ptr<const QueryReadContext> planning_lifetime;
  std::weak_ptr<const QueryReadContext> result_lifetime;
  {
    auto planning = std::make_shared<DetachingTestReadContext>();
    OwnerBridgeFixture f(planning);
    auto other = f.backend->manifest;
    other.key.path = "/dataset/predicate-miss.arrow";
    f.context->manifests.push_back(other);
    f.context->candidates.push_back({other.key.path, other.key.version, "arrow"});
    f.request.sources.file_predicate = make_comparison({"manifest.path"},
      ComparisonOperator::Equal, LogicalValue::utf8("/no-match"));
    auto reference = result_reference_for(planning);
    planning->prepare_reference = [reference] { return reference; };
    auto route = f.frontend->register_typed(f.request, {}, planning);
    ASSERT_TRUE(route.ok());
    auto result = f.service->materialize_authorized(route.value(), {}, {}, nullptr, f.principal);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(planning->reference_calls, 1u);
    EXPECT_EQ(reference->admits, 0u); // Final publication still admits the execution context.
    EXPECT_EQ(planning->state->admits, 2u);
    EXPECT_EQ(result.value().read_context.get(), reference.get());
    EXPECT_NE(&result.value().sources(), &planning->sources());
    ASSERT_EQ(result.value().sources().size(), 2u);
    EXPECT_EQ(result.value().sources().front().version, f.backend->manifest.key.version);
    EXPECT_EQ(result.value().sources().back().path, other.key.path);
    planning_lifetime = planning;
    result_lifetime = reference;
    held = std::move(result.value());
    held->publication_admission.reset();
    held->publication_cleanup.reset();
    const auto id = route.value().substr(route.value().find("id=") + 3, 48);
    EXPECT_TRUE(f.frontend->erase(id));
  }
  EXPECT_TRUE(planning_lifetime.expired());
  ASSERT_FALSE(result_lifetime.expired());
  EXPECT_EQ(held->sources().back().path, "/dataset/predicate-miss.arrow");
  EXPECT_TRUE(held->handle.read(0, held->handle.size()).ok());
  held.reset();
  EXPECT_TRUE(result_lifetime.expired());
}

TEST(OwnerQueryAdmission, DetachedPlanningCleanupDestroysOnlyAfterPublicationGateReleases) {
  unsigned destroyed = 0;
  auto planning = std::make_shared<DetachingTestReadContext>();
  OwnerBridgeFixture f(planning);
  auto reference = result_reference_for(planning);
  auto state = planning->state;
  planning->prepare_reference = [reference] { return reference; };
  planning->during_destroy = [state, &destroyed] {
    EXPECT_EQ(state->gates, 0u);
    ++destroyed;
  };
  auto route = f.frontend->register_typed(f.request, {}, planning);
  ASSERT_TRUE(route.ok());
  auto result = f.service->materialize_authorized(route.value(), {}, {}, nullptr, f.principal);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().read_context.get(), reference.get());
  const std::weak_ptr<const QueryReadContext> lifetime = planning;
  const auto id = route.value().substr(route.value().find("id=") + 3, 48);
  EXPECT_TRUE(f.frontend->erase(id));
  f.catalog->context.reset();
  f.context.reset();
  planning.reset();
  EXPECT_EQ(state->gates, 1u);
  EXPECT_FALSE(lifetime.expired());
  EXPECT_EQ(destroyed, 0u);
  result.value().publication_admission.reset();
  EXPECT_EQ(state->gates, 0u);
  EXPECT_FALSE(lifetime.expired());
  EXPECT_EQ(destroyed, 0u);
  result.value().publication_cleanup.reset();
  EXPECT_TRUE(lifetime.expired());
  EXPECT_EQ(destroyed, 1u);
  EXPECT_TRUE(result.value().handle.read(0, result.value().handle.size()).ok());
}

TEST(OwnerQueryAdmission, DetachedBridgeOpensReadmitEvidenceAfterPlanningOwnersRelease) {
  auto planning = std::make_shared<DetachingTestReadContext>();
  OwnerBridgeFixture f(planning);
  f.request.sources.file_predicate = make_comparison({"manifest.path"},
    ComparisonOperator::Equal, LogicalValue::utf8("/no-match"));
  auto reference = result_reference_for(planning);
  planning->prepare_reference = [reference] { return reference; };
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  auto published = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_TRUE(published.ok()) << published.status().message();
  const auto bytes = published.value()->read(0, published.value()->size());
  ASSERT_TRUE(bytes.ok());
  const std::weak_ptr<const QueryReadContext> lifetime = planning;
  f.catalog->context.reset();
  f.context.reset();
  planning.reset();
  ASSERT_TRUE(lifetime.expired());

  auto exact = f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal);
  ASSERT_TRUE(exact.ok()) << exact.status().message();
  auto cached = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_TRUE(cached.ok()) << cached.status().message();
  EXPECT_EQ(reference->admits, 2u);
  auto wrong = f.principal;
  ++wrong.uid;
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), published.value()->id(), {}, wrong).status().code(),
    QueryError::PermissionDenied);
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), published.value()->id(), {}).status().code(),
    QueryError::PermissionDenied);
  reference->state->revoked = true;
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal).status().code(),
    QueryError::PermissionDenied);
  EXPECT_EQ(f.bridge->acquire(route.value(), {}, nullptr, f.principal).status().code(),
    QueryError::PermissionDenied);
  EXPECT_EQ(exact.value()->read(0, exact.value()->size()).value(), bytes.value());
  const auto id = route.value().substr(route.value().find("id=") + 3, 48);
  EXPECT_TRUE(f.frontend->erase(id));
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal).status().code(),
    QueryError::SourceNotFound);
  EXPECT_EQ(published.value()->read(0, published.value()->size()).value(), bytes.value());
  EXPECT_EQ(reference->state->leases, 0u);
  EXPECT_EQ(reference->state->gates, 0u);
}

TEST(OwnerQueryAdmission, DetachedReferencePreparationPrecedesOriginalFinalAdmission) {
  auto planning = std::make_shared<DetachingTestReadContext>();
  OwnerBridgeFixture f(planning);
  auto reference = result_reference_for(planning);
  planning->prepare_reference = [&] {
    EXPECT_EQ(planning->state->admits, 1u);
    EXPECT_EQ(planning->state->gates, 0u);
    EXPECT_GT(planning->state->leases, 0u);
    EXPECT_TRUE(current_query_execution_lease());
    EXPECT_EQ(f.results->stats().registered_entries, 0u);
    planning->state->revoked = true;
    return reference;
  };
  planning->during_admit = [&] {
    if (planning->state->admits == 2) EXPECT_EQ(planning->reference_calls, 1u);
  };
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  auto denied = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_FALSE(denied.ok());
  EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(denied.status().phase(), QueryPhase::Catalog);
  EXPECT_EQ(denied.status().message(), "owner dependency was revoked");
  EXPECT_EQ(reference->admits, 0u);
  EXPECT_EQ(planning->state->admits, 2u);
  EXPECT_EQ(planning->state->leases, 0u);
  EXPECT_EQ(planning->state->gates, 0u);
  EXPECT_EQ(f.results->stats().registered_entries, 0u);
  EXPECT_EQ(f.bridge->cached_routes(), 0u);
}

TEST(OwnerQueryAdmission, OptionalReferenceRefusalRetainsOriginalWithoutChangingPublication) {
  std::optional<TypedQueryService::MaterializedResult> held;
  std::weak_ptr<const QueryReadContext> lifetime;
  {
    auto planning = std::make_shared<DetachingTestReadContext>();
    OwnerBridgeFixture f(planning);
    planning->prepare_reference = [] { return std::shared_ptr<const QueryReadContext>{}; };
    auto route = f.frontend->register_typed(f.request);
    ASSERT_TRUE(route.ok());
    auto result = f.service->materialize_authorized(route.value(), {}, {}, nullptr, f.principal);
    ASSERT_TRUE(result.ok()) << result.status().message();
    EXPECT_EQ(result.value().read_context.get(), planning.get());
    EXPECT_EQ(planning->reference_calls, 1u);
    EXPECT_EQ(planning->state->admits, 2u);
    lifetime = planning;
    held = std::move(result.value());
    held->publication_admission.reset();
    held->publication_cleanup.reset();
  }
  EXPECT_FALSE(lifetime.expired());
  EXPECT_TRUE(held->handle.read(0, held->handle.size()).ok());
  held.reset();
  EXPECT_TRUE(lifetime.expired());
}

TEST(OwnerQueryAdmission, DetachedNewOpensKeepFreshAndStickyMetadataBudgets) {
  auto planning = std::make_shared<DetachingTestReadContext>();
  OwnerBridgeFixture f(planning);
  auto reference = result_reference_for(planning);
  planning->prepare_reference = [reference] { return reference; };
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  auto published = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_TRUE(published.ok()) << published.status().message();
  reference->during_admit = [] { require_query_metadata_unique(40ULL * 1024 * 1024); };
  for (unsigned i = 0; i != 3; ++i) {
    EXPECT_TRUE(f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal).ok());
    EXPECT_FALSE(current_query_metadata_budget());
  }
  {
    ScopedQueryMetadataBudget outer;
    require_query_metadata_unique(40ULL * 1024 * 1024);
    EXPECT_EQ(f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal).status().code(),
      QueryError::ResourceExhausted);
  }
  reference->during_admit = [] {
    try { require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1); }
    catch (const QueryMetadataBudgetExceeded&) {}
  };
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal).status().code(),
    QueryError::ResourceExhausted);
  EXPECT_TRUE(published.value()->read(0, published.value()->size()).ok());
  EXPECT_EQ(reference->state->gates, 0u);
  EXPECT_EQ(reference->state->leases, 0u);
}

TEST(OwnerQueryAdmission, StrictMaterializationKeepsOwnedSourceReferences) {
  OwnerBridgeFixture f;
  f.context->expose_context = false;
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  unsigned checks = 0;
  auto result = f.service->materialize_authorized(route.value(),
    [&](const std::vector<QuerySourceReference>& references) {
      ++checks;
      EXPECT_EQ(references.size(), 1u);
      EXPECT_EQ(references.front().path, f.backend->manifest.key.path);
      return QueryStatus::success();
    });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(checks, 1u);
  EXPECT_FALSE(result.value().read_context);
  EXPECT_EQ(&result.value().sources(), &result.value().owned_sources);
  ASSERT_EQ(result.value().sources().size(), 1u);
  EXPECT_EQ(result.value().sources().front().version, f.backend->manifest.key.version);
}

TEST(OwnerQueryAdmission, SharedBudgetReplaysScanAndRejectsStickyFinalAdmission) {
  OwnerBridgeFixture f;
  f.context->during_scan = [] { require_query_metadata("scan", 40ULL * 1024 * 1024); };
  f.context->during_admit = [&] {
    if (f.context->state->admits == 2) {
      // A backend can convert an exception to another status; sticky exhaustion
      // must still prohibit the final publication even if admit succeeds.
      try { require_query_metadata("final", 30ULL * 1024 * 1024); }
      catch (const QueryMetadataBudgetExceeded&) {}
    }
  };
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  auto result = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(f.results->stats().registered_entries, 0u);
  EXPECT_EQ(f.bridge->cached_routes(), 0u);
  EXPECT_FALSE(current_query_metadata_budget());
}

TEST(OwnerQueryAdmission, SwallowedBudgetFailureDuringEncodingStillRejectsPublication) {
  OwnerBridgeFixture f;
  f.request.sources.paths = {{PathScopeKind::Exact, f.backend->manifest.key.path}};
  f.request.sources.versions = VersionSelection::exact_version(f.backend->manifest.key.version);
  f.request.sources.formats.adapters = AdapterSelectionKind::Listed;
  f.request.sources.formats.adapter_ids = {"arrow"};
  f.request.sources.formats.include_byte_passthrough = false;
  f.request.subset = QueryFragment{};
  f.request.subset->physical_array = PhysicalArraySubject{"target", 0, 0, {{{0}}}};
  f.request.subset->projection = {{"index"}, {"value.0"}};
  unsigned reads = 0;
  f.backend->during_payload = [&] {
    ++reads;
    EXPECT_FALSE(current_query_metadata_budget());
    EXPECT_NE(active_query_metadata_trace, nullptr);
    EXPECT_EQ(f.context->state->gates, 0u);
    EXPECT_GT(f.context->state->leases, 0u);
    try { require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1); }
    catch (const QueryMetadataBudgetExceeded&) {}
    // A neutral producer returns valid data; its recorded fee must still
    // reject this consumer before publication, even if a backend swallows errors.
  };
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok()) << route.status().message();
  auto result = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  EXPECT_GT(reads, 0u) << result.status().message();
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(f.results->stats().registered_entries, 0u);
  EXPECT_EQ(f.bridge->cached_routes(), 0u);
}

TEST(OwnerQueryAdmission, IndependentNewOpensHaveFreshBudgets) {
  OwnerBridgeFixture f;
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  auto published = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_TRUE(published.ok());
  f.context->during_admit = [] { require_query_metadata_unique(40ULL * 1024 * 1024); };
  for (unsigned i = 0; i != 3; ++i) {
    auto opened = f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal);
    ASSERT_TRUE(opened.ok()) << opened.status().message();
    EXPECT_FALSE(current_query_metadata_budget());
  }
  {
    ScopedQueryMetadataBudget outer;
    require_query_metadata_unique(40ULL * 1024 * 1024);
    auto denied = f.bridge->acquire_exact(route.value(), published.value()->id(), {}, f.principal);
    EXPECT_EQ(denied.status().code(), QueryError::ResourceExhausted);
  }
  EXPECT_TRUE(published.value()->read(0, published.value()->size()).ok());
}

TEST(OwnerQueryAdmission, StrictServiceDoesNotCreateSharedBudget) {
  OwnerBridgeFixture f;
  f.context->expose_context = false;
  f.context->during_scan = [] { EXPECT_FALSE(current_query_metadata_budget()); };
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  unsigned authorized = 0;
  auto result = f.bridge->acquire(route.value(), [&](const std::vector<QuerySourceReference>&) {
    ++authorized;
    EXPECT_FALSE(current_query_metadata_budget());
    EXPECT_FALSE(current_query_execution_lease());
    return QueryStatus::success();
  });
  ASSERT_TRUE(result.ok()) << result.status().message();
  EXPECT_EQ(authorized, 1u);
  EXPECT_FALSE(current_query_metadata_budget());
}

TEST(OwnerQueryAdmission, MutationDuringExecutionRejectsFinalPublication) {
  OwnerBridgeFixture f;
  f.context->during_manifest = [&] {
    std::lock_guard<std::recursive_mutex> lock(f.context->state->mutex);
    f.context->state->revoked = true;
  };
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  auto result = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(f.results->stats().registered_entries, 0u);
  EXPECT_EQ(f.bridge->cached_routes(), 0u);
  EXPECT_EQ(f.context->state->leases, 0u);
  EXPECT_EQ(f.context->state->gates, 0u);
}

TEST(OwnerQueryAdmission, PredicateMissRetainsDependencyAndOpenedIpcSurvivesRevocation) {
  OwnerBridgeFixture f;
  f.request.sources.file_predicate = make_comparison({"manifest.path"},
    ComparisonOperator::Equal, LogicalValue::utf8("/no-match"));
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  auto published = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_TRUE(published.ok()) << published.status().message();
  const auto id = published.value()->id();
  auto opened = f.bridge->acquire_exact(route.value(), id, {}, f.principal);
  ASSERT_TRUE(opened.ok());
  const auto before = opened.value()->read(0, opened.value()->size());
  ASSERT_TRUE(before.ok());
  auto wrong_principal = f.principal; ++wrong_principal.uid;
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), id, {}, wrong_principal).status().code(),
    QueryError::PermissionDenied);
  f.context->state->revoked = true;
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), id, {}, f.principal).status().code(),
    QueryError::PermissionDenied);
  EXPECT_EQ(f.bridge->acquire(route.value(), {}, nullptr, f.principal).status().code(),
    QueryError::PermissionDenied);
  EXPECT_EQ(opened.value()->read(0, opened.value()->size()).value(), before.value());
  EXPECT_EQ(f.context->state->gates, 0u);
}

TEST(OwnerQueryAdmission, MissingPrincipalFailsEvenForEmptyFrozenSelection) {
  OwnerBridgeFixture f;
  f.context->manifests.clear(); f.context->candidates.clear();
  auto route = f.frontend->register_typed(f.request);
  ASSERT_TRUE(route.ok());
  EXPECT_EQ(f.bridge->acquire(route.value(), {}).status().code(), QueryError::PermissionDenied);
  auto allowed = f.bridge->acquire(route.value(), {}, nullptr, f.principal);
  ASSERT_TRUE(allowed.ok()) << allowed.status().message();
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), allowed.value()->id(), {}).status().code(),
    QueryError::PermissionDenied);
  f.context->state->revoked = true;
  EXPECT_EQ(f.bridge->acquire_exact(route.value(), allowed.value()->id(), {}, f.principal).status().code(),
    QueryError::PermissionDenied);
}
} // namespace

TEST_F(SocketFixture, OwnerRegistrationBorrowsTheCompleteFrozenCandidateArray) {
  auto backend = std::make_shared<TinyBackend>();
  auto context = std::make_shared<TestReadContext>();
  context->manifests = {backend->manifest};
  context->candidates = {{backend->manifest.key.path, backend->manifest.key.version, "arrow"}};
  // The frozen context retains dependencies outside the post-filter selection.
  context->candidates.push_back({"/dataset/predicate-miss.arrow", 8, "arrow"});
  catalog = std::make_shared<ContextCatalog>(context);
  storage = std::make_shared<StructuredBlobStorage>(backend);
  auto module = QueryModule::create(catalog, storage, {});
  ASSERT_TRUE(module.ok());
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto service = std::make_shared<TypedQueryService>(module.value(), frontend, results, 1024 * 1024);
  bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
  unsigned checks = 0;
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge,
    [&](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>& references,
        const CatalogSelection* selected) {
      ++checks;
      EXPECT_EQ(&references, &context->sources());
      EXPECT_EQ(references.size(), 2u);
      EXPECT_TRUE(selected && selected->read_context == context);
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto registered = submit_query_registration(socket, kTyped);
  ASSERT_TRUE(registered.ok()) << registered.status().message();
  EXPECT_EQ(checks, 1u);
  server.stop();
}

TEST_F(SocketFixture, OwnerMutationAfterCheckRejectsPublicationInsteadOfOnlyPrefill) {
  auto backend = std::make_shared<TinyBackend>();
  auto context = std::make_shared<TestReadContext>();
  context->manifests = {backend->manifest};
  context->candidates = {{backend->manifest.key.path, backend->manifest.key.version, "arrow"}};
  catalog = std::make_shared<ContextCatalog>(context);
  storage = std::make_shared<StructuredBlobStorage>(backend);
  auto module = QueryModule::create(catalog, storage, {});
  ASSERT_TRUE(module.ok());
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto service = std::make_shared<TypedQueryService>(module.value(), frontend, results, 1024 * 1024);
  bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge,
    [&](const QueryRegistrationPeer& peer, const auto&, const CatalogSelection* selected) {
      EXPECT_TRUE(selected && selected->read_context);
      auto admitted = selected->read_context->admit({peer.uid, peer.gid, peer.supplementary_groups});
      if (!admitted.ok()) return admitted.status();
      admitted.value().reset();
      std::lock_guard<std::recursive_mutex> lock(context->state->mutex);
      context->state->revoked = true;
      return QueryStatus::success();
    });
  ASSERT_TRUE(server.start().ok());
  auto registered = submit_query_registration(socket, kTyped);
  ASSERT_FALSE(registered.ok());
  EXPECT_EQ(registered.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(frontend->size(), 0u);
  EXPECT_EQ(bridge->cached_routes(), 0u);
  EXPECT_EQ(results->stats().registered_entries, 0u);
  server.stop();
}

TEST_F(SocketFixture, DatasetCatalogFinalSendRechecksEvenEmptyOwnerContext) {
  auto context = std::make_shared<TestReadContext>();
  QueryRegistrationSocketServer server(socket, "/mnt/data", frontend, catalog,
    storage, runtime, bridge, {}, 0600, {}, {}, {},
    [&](const DatasetCatalogRequest&, const DatasetCatalogAuthorizer&, const std::shared_ptr<HotQueryCache::Binding>&) {
      DatasetCatalogResult result;
      result.read_context = context;
      context->state->revoked = true;
      return QueryResult<DatasetCatalogResult>::success(std::move(result));
    });
  ASSERT_TRUE(server.start().ok());
  auto response = submit_query_control(socket,
    R"({"kind":"dataset_catalog","request":{"sources":{}}})");
  ASSERT_FALSE(response.ok());
  EXPECT_EQ(response.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(context->state->admits, 1u);
  EXPECT_EQ(context->state->gates, 0u);
  server.stop();
}

namespace {
// Exercise only the public socket boundary. The fake discovery seam does not
// implement domain selection/caching; backend residency has separate tests.
std::string prepared_catalog_predicate(const std::string& name = "t") {
  return std::string(R"({"kind":"and","operands":[{"kind":"compare","field":"hdf5.attr:shortName","operator":"eq","value":{"type":"utf8","value":")") + name +
    R"("}},{"kind":"compare","field":"hdf5.attr:typeOfLevel","operator":"eq","value":{"type":"utf8","value":"surface"}},{"kind":"compare","field":"hdf5.attr:level","operator":"eq","value":{"type":"int64","value":0}},{"kind":"compare","field":"hdf5.attr:step","operator":"eq","value":{"type":"int64","value":0}},{"kind":"compare","field":"hdf5.attr:validityDate","operator":"eq","value":{"type":"int64","value":20261023}},{"kind":"compare","field":"hdf5.attr:validityTime","operator":"eq","value":{"type":"int64","value":0}}]})";
}
constexpr const char* selector_template = R"({"sources":{"paths":[{"kind":"exact","path":"/science/a.h5"}]},"format_id":"hdf5","dataset_path":"/T","max_sources":1})";
std::string selector_prepare(const std::string& spec = selector_template) {
  return "{\"kind\":\"catalog_selector_prepare\",\"request\":" + spec + "}";
}
std::string selector_execute(const std::string& id, const std::string& name = "t",
    const std::string& extra = {}) {
  return "{\"kind\":\"dataset_catalog_by_selector\",\"selector_id\":\"" + id +
    "\",\"predicate\":" + prepared_catalog_predicate(name) + extra + "}";
}
std::string selector_release(const std::string& id) {
  return "{\"kind\":\"catalog_selector_release\",\"selector_id\":\"" + id + "\"}";
}
class SelectorSocketFixture : public SocketFixture {
protected:
  void SetUp() override {
    SocketFixture::SetUp();
    context = std::make_shared<TestReadContext>();
    context->candidates = {{"/science/a.h5", 42, "hdf5"}};
    pool = std::make_shared<HotQueryCache>();
  }
  std::unique_ptr<QueryRegistrationSocketServer> make_server(bool enable_handles = true) {
    QueryRegistrationSocketServer::Authorization authorize =
      [&](const QueryRegistrationPeer& peer, const auto& references, const CatalogSelection*) {
        ++authorizations;
        EXPECT_EQ(peer.uid, ::geteuid()); EXPECT_EQ(peer.gid, ::getegid());
        EXPECT_EQ(references.size(), 1U);
        return allowed ? QueryStatus::success() : QueryStatus::failure(
          QueryError::PermissionDenied, QueryPhase::Catalog, "current source permission denied");
      };
    QueryRegistrationSocketServer::DatasetDiscovery discover =
      [&](const DatasetCatalogRequest& request, const DatasetCatalogAuthorizer& authorize,
          const std::shared_ptr<HotQueryCache::Binding>& binding) {
        ++discoveries;
        EXPECT_EQ(request.max_sources, 1U);
        EXPECT_EQ(request.dataset_path, std::optional<std::string>("/T"));
        EXPECT_EQ(request.sources.paths.size(), 1U);
        if (binding) ++handle_discoveries;
        if (auto budget = current_query_metadata_budget()) {
          EXPECT_EQ(budget->consumed_bytes(), 0U);
          ledgers.push_back(budget);
          // More than half the cap: sharing one ledger across requests fails.
          require_query_metadata_unique(over_budget ? QueryMetadataBudget::maximum_bytes + 1 :
            QueryMetadataBudget::maximum_bytes / 2 + 1);
        } else ADD_FAILURE() << "owner request budget was not installed";
        CatalogSelection selection;
        auto granted = authorize(context->sources(), selection);
        if (!granted.ok()) return QueryResult<DatasetCatalogResult>::failure(granted);
        const auto& logical = std::get<LogicalExpression>(request.predicate->node);
        const auto& comparison = std::get<ComparisonExpression>(logical.operands[0]->node);
        const auto name = std::get<std::string>(comparison.value.value());
        observed_names.push_back(name);
        observed_limits.push_back({request.max_datasets, request.max_catalog_bytes});
        DatasetCatalogResult result;
        result.selected_sources = 1; result.candidate_datasets = 2;
        result.datasets.push_back({context->sources()[0], 5, "object-" + name, "/T",
          std::string(64, 'a'), "IOTDBFS-HDF5-LAYOUT/5\n"});
        result.read_context = context;
        if (revoke_before_send) context->state->revoked = true;
        return QueryResult<DatasetCatalogResult>::success(std::move(result));
      };
    QueryRegistrationSocketServer::PrepareSelectorBinding prepare = [&] {
      ++bindings;
      if (during_prepare) during_prepare();
      auto binding = std::make_shared<HotQueryCache::Binding>();
      binding->owner = pool;
      return QueryResult<std::shared_ptr<HotQueryCache::Binding>>::success(std::move(binding));
    };
    return std::make_unique<QueryRegistrationSocketServer>(socket, "/mnt/data", frontend,
      catalog, storage, runtime, bridge, std::move(authorize), 0600,
      TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{}, Hdf5QueryKernelConfig{},
      std::move(discover), std::shared_ptr<Hdf5ValidatedLayoutCache>{},
      std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
      QueryRegistrationSocketServer::BeginAuthorizationPublication{}, true,
      Hdf5QueryMode::IoTDBIndexed, std::shared_ptr<const QueryModule>{},
      enable_handles ? pool : nullptr, std::move(prepare));
  }
  std::shared_ptr<TestReadContext> context;
  std::shared_ptr<HotQueryCache> pool;
  std::atomic<unsigned> bindings{0}, discoveries{0}, handle_discoveries{0}, authorizations{0};
  std::atomic<bool> allowed{true}, over_budget{false}, revoke_before_send{false};
  std::vector<std::shared_ptr<QueryMetadataBudget>> ledgers;
  std::vector<std::string> observed_names;
  std::vector<std::pair<std::size_t, std::uint64_t>> observed_limits;
  std::function<void()> during_prepare;
};

TEST_F(SelectorSocketFixture, PrepareIsStaticAndFullAndHandleReturnSameIdentity) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, selector_prepare());
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(prepared.value().kind, QueryControlResponse::Kind::PreparedCatalogSelector);
  EXPECT_EQ(prepared.value().selector_id.size(), 48U);
  EXPECT_EQ(prepared.value().ttl_ms, 300000U);
  EXPECT_EQ(bindings, 1U); EXPECT_EQ(discoveries, 0U); EXPECT_EQ(authorizations, 0U);
  EXPECT_EQ(context->state->admits, 0U); EXPECT_EQ(pool->stats().handles, 1U);
  std::string full_spec(selector_template); full_spec.pop_back();
  full_spec += ",\"predicate\":" + prepared_catalog_predicate() + "}";
  auto full = submit_query_control(socket,
    "{\"kind\":\"dataset_catalog\",\"request\":" + full_spec + "}");
  auto handle = submit_query_control(socket, selector_execute(prepared.value().selector_id));
  ASSERT_TRUE(full.ok()) << full.status().message();
  ASSERT_TRUE(handle.ok()) << handle.status().message();
  ASSERT_EQ(full.value().datasets.datasets.size(), 1U);
  ASSERT_EQ(handle.value().datasets.datasets.size(), 1U);
  const auto& a = full.value().datasets.datasets[0];
  const auto& b = handle.value().datasets.datasets[0];
  EXPECT_EQ(a.source.path, b.source.path); EXPECT_EQ(a.source.version, b.source.version);
  EXPECT_EQ(a.object_id, b.object_id); EXPECT_EQ(a.layout_digest, b.layout_digest);
  EXPECT_EQ(discoveries, 2U); EXPECT_EQ(handle_discoveries, 1U);
  EXPECT_EQ(authorizations, 2U); EXPECT_EQ(context->state->admits, 2U);
  server->stop(); EXPECT_EQ(pool->stats().handles, 0U); EXPECT_EQ(pool->stats().handle_bytes, 0U);
}

TEST_F(SelectorSocketFixture, EveryExecutionUsesCurrentAuthorizationAndNewBudget) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, selector_prepare()); ASSERT_TRUE(prepared.ok());
  const auto id = prepared.value().selector_id;
  EXPECT_TRUE(submit_query_control(socket, selector_execute(id)).ok());
  allowed = false;
  auto denied = submit_query_control(socket, selector_execute(id));
  EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied);
  allowed = true;
  EXPECT_TRUE(submit_query_control(socket, selector_execute(id)).ok());
  EXPECT_EQ(authorizations, 3U); EXPECT_EQ(context->state->admits, 2U);
  ASSERT_EQ(ledgers.size(), 3U);
  EXPECT_NE(ledgers[0].get(), ledgers[1].get()); EXPECT_NE(ledgers[1].get(), ledgers[2].get());
  for (const auto& ledger : ledgers) {
    EXPECT_TRUE(ledger->status().ok());
    EXPECT_EQ(ledger->consumed_bytes(), QueryMetadataBudget::maximum_bytes / 2 + 1);
  }
  server->stop(); EXPECT_EQ(pool->stats().handles, 0U);
}

TEST_F(SelectorSocketFixture, PredicateAndDynamicLimitsAreBoundPerExecution) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, selector_prepare()); ASSERT_TRUE(prepared.ok());
  auto a = submit_query_control(socket, selector_execute(prepared.value().selector_id, "t",
    ",\"max_datasets\":2,\"max_catalog_bytes\":4096"));
  auto b = submit_query_control(socket, selector_execute(prepared.value().selector_id, "u",
    ",\"max_datasets\":3,\"max_catalog_bytes\":8192"));
  ASSERT_TRUE(a.ok()) << a.status().message(); ASSERT_TRUE(b.ok()) << b.status().message();
  EXPECT_EQ(a.value().datasets.datasets[0].object_id, "object-t");
  EXPECT_EQ(b.value().datasets.datasets[0].object_id, "object-u");
  EXPECT_EQ(observed_names, (std::vector<std::string>{"t", "u"}));
  ASSERT_EQ(observed_limits.size(), 2U);
  EXPECT_EQ(observed_limits[0], (std::make_pair<std::size_t, std::uint64_t>(2, 4096)));
  EXPECT_EQ(observed_limits[1], (std::make_pair<std::size_t, std::uint64_t>(3, 8192)));
  for (const auto& extra : {",\"max_sources\":2", ",\"dataset_path\":\"/U\"",
      ",\"sources\":{}", ",\"max_datasets\":0", ",\"max_catalog_bytes\":0"}) {
    auto invalid = submit_query_control(socket, selector_execute(prepared.value().selector_id, "t", extra));
    EXPECT_FALSE(invalid.ok()) << extra;
  }
  EXPECT_EQ(discoveries, 2U); server->stop();
}

TEST_F(SelectorSocketFixture, UnknownReleasedAndUnsupportedPredicatesDoNotDiscover) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, selector_prepare()); ASSERT_TRUE(prepared.ok());
  const auto id = prepared.value().selector_id;
  auto unknown = submit_query_control(socket, selector_execute(std::string(48, 'f')));
  EXPECT_EQ(unknown.status().code(), QueryError::SourceNotFound);
  auto unsupported = submit_query_control(socket,
    "{\"kind\":\"dataset_catalog_by_selector\",\"selector_id\":\"" + id +
    R"(","predicate":{"kind":"null","field":"hdf5.attr:shortName"}})");
  EXPECT_EQ(unsupported.status().code(), QueryError::UnsupportedCapability);
  auto released = submit_query_control(socket, selector_release(id));
  ASSERT_TRUE(released.ok()); EXPECT_TRUE(released.value().released);
  EXPECT_EQ(released.value().kind, QueryControlResponse::Kind::ReleasedCatalogSelector);
  auto twice = submit_query_control(socket, selector_release(id));
  ASSERT_TRUE(twice.ok()); EXPECT_FALSE(twice.value().released);
  auto stale = submit_query_control(socket, selector_execute(id));
  EXPECT_EQ(stale.status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(stale.status().message(), unknown.status().message());
  EXPECT_EQ(discoveries, 0U); EXPECT_EQ(authorizations, 0U);
  EXPECT_EQ(pool->stats().handles, 0U); server->stop();
}

TEST_F(SelectorSocketFixture, FailedRequestBudgetDoesNotPoisonLaterHandleExecution) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, selector_prepare()); ASSERT_TRUE(prepared.ok());
  over_budget = true;
  auto failed = submit_query_control(socket, selector_execute(prepared.value().selector_id));
  EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(pool->stats().handles, 1U);
  over_budget = false;
  auto recovered = submit_query_control(socket, selector_execute(prepared.value().selector_id));
  EXPECT_TRUE(recovered.ok()) << recovered.status().message();
  EXPECT_EQ(authorizations, 1U); ASSERT_EQ(ledgers.size(), 2U);
  EXPECT_FALSE(ledgers[0]->status().ok()); EXPECT_TRUE(ledgers[1]->status().ok());
  server->stop(); EXPECT_EQ(pool->stats().handles, 0U);
}

TEST_F(SelectorSocketFixture, FinalAdmissionRejectsRevocationAfterDiscovery) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, selector_prepare()); ASSERT_TRUE(prepared.ok());
  revoke_before_send = true;
  auto denied = submit_query_control(socket, selector_execute(prepared.value().selector_id));
  EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(authorizations, 1U); EXPECT_EQ(context->state->admits, 1U);
  EXPECT_EQ(context->state->gates, 0U); EXPECT_EQ(pool->stats().handles, 1U);
  revoke_before_send = false;
  { std::lock_guard<std::recursive_mutex> lock(context->state->mutex); context->state->revoked = false; }
  EXPECT_TRUE(submit_query_control(socket, selector_execute(prepared.value().selector_id)).ok());
  server->stop(); EXPECT_EQ(pool->stats().handles, 0U);
}

TEST_F(SelectorSocketFixture, InvalidPrepareAndUnavailableServiceDoNotAllocateHandles) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  for (const auto& spec : {
      R"({"sources":{},"max_sources":0})",
      R"({"sources":{},"time_range":{"lower_unix_s":1,"upper_unix_s":2}})",
      R"({"sources":{"paths":[{"kind":"exact","path":""}]}})",
      R"({"sources":{},"format_id":"grib"})",
      R"({"sources":{"file_predicate":{"kind":"compare","field":"manifest.path","operator":"eq","value":{"type":"utf8","value":"/science/a.h5"}}}})"}) {
    SCOPED_TRACE(spec); auto invalid = submit_query_control(socket, selector_prepare(spec));
    EXPECT_FALSE(invalid.ok()); EXPECT_EQ(pool->stats().handles, 0U);
  }
  EXPECT_EQ(bindings, 0U); EXPECT_EQ(discoveries, 0U); EXPECT_EQ(authorizations, 0U);
  server->stop(); server.reset();
  auto unavailable = make_server(false); ASSERT_TRUE(unavailable->start().ok());
  auto rejected = submit_query_control(socket, selector_prepare());
  EXPECT_EQ(rejected.status().code(), QueryError::UnsupportedCapability);
  unavailable->stop(); EXPECT_EQ(pool->stats().handles, 0U);
}

TEST_F(SelectorSocketFixture, FailedPrepareResponseRetiresTheNewHandle) {
  std::promise<void> entered, resume;
  auto entered_future = entered.get_future();
  auto resume_future = resume.get_future();
  during_prepare = [&] { entered.set_value(); resume_future.wait(); };
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  const auto fd = ::socket(AF_UNIX, SOCK_STREAM, 0); ASSERT_GE(fd, 0);
  sockaddr_un address{}; address.sun_family = AF_UNIX;
  std::strncpy(address.sun_path, socket.c_str(), sizeof(address.sun_path) - 1);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  const auto request = selector_prepare();
  const auto size = htonl(static_cast<std::uint32_t>(request.size()));
  ASSERT_EQ(::send(fd, &size, sizeof(size), 0), static_cast<ssize_t>(sizeof(size)));
  ASSERT_EQ(::send(fd, request.data(), request.size(), 0), static_cast<ssize_t>(request.size()));
  const auto ready = entered_future.wait_for(std::chrono::seconds(2));
  if (ready != std::future_status::ready) {
    resume.set_value(); ::close(fd);
    FAIL() << "prepare did not reach its bound callback";
  }
  EXPECT_EQ(pool->stats().handles, 1U); // Pending builder is already charged.
  ::shutdown(fd, SHUT_RDWR); ::close(fd);
  resume.set_value();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (pool->stats().handles && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  EXPECT_EQ(pool->stats().handles, 0U); EXPECT_EQ(pool->stats().handle_bytes, 0U);
  EXPECT_EQ(discoveries, 0U); EXPECT_EQ(authorizations, 0U);
  server->stop();
}
} // namespace

namespace {
std::string query_template_prepare(const std::string& declaration = kTyped) {
  return "{\"kind\":\"query_template_prepare\",\"declaration\":" + declaration + "}";
}
std::string query_template_execute(const std::string& id, const std::string& extra = {}) {
  return "{\"kind\":\"query_template_execute\",\"template_id\":\"" + id + "\"" + extra + "}";
}
std::string query_template_release(const std::string& id) {
  return "{\"kind\":\"query_template_release\",\"template_id\":\"" + id + "\"}";
}

class QueryTemplateSocketFixture : public SocketFixture {
protected:
  void SetUp() override {
    SocketFixture::SetUp();
    backend = std::make_shared<TinyBackend>();
    storage = std::make_shared<StructuredBlobStorage>(backend);
    catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
      ++scans;
      return QueryResult<PublishedVersionSnapshot>::success({
        removed ? std::vector<StructuredBlobManifest>{} :
          std::vector<StructuredBlobManifest>{backend->manifest}, {}});
    }, storage->storage_namespace());
    auto module = QueryModule::create(catalog, storage,
      {std::make_shared<ArrowFormatQueryAdapter>(ArrowDecodeConfig{}, runtime)});
    ASSERT_TRUE(module.ok());
    results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
    service = std::make_shared<TypedQueryService>(module.value(), frontend, results,
      64ULL * 1024 * 1024);
    bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
    pool = std::make_shared<HotQueryCache>();
  }
  std::unique_ptr<QueryRegistrationSocketServer> make_server(mode_t mode = 0600,
      Hdf5QueryMode hdf5_mode = Hdf5QueryMode::IoTDBIndexed) {
    return std::make_unique<QueryRegistrationSocketServer>(socket, "/mnt/data", frontend,
      catalog, storage, runtime, bridge,
      [&](const QueryRegistrationPeer& peer, const auto& sources, const auto*) {
        ++authorizations;
        seen_peers.push_back(peer);
        if (auto budget = current_query_metadata_budget()) ledgers.push_back(budget);
        if (during_authorize) during_authorize();
        if (!allowed || (deny_call && authorizations == deny_call) ||
            (deny_empty_sources && sources.empty()))
          return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
            "current template source denied");
        if (!sources.empty()) seen_versions.push_back(sources.front().version);
        return QueryStatus::success();
      }, mode, TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{},
      Hdf5QueryKernelConfig{}, QueryRegistrationSocketServer::DatasetDiscovery{},
      std::shared_ptr<Hdf5ValidatedLayoutCache>{},
      std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
      QueryRegistrationSocketServer::BeginAuthorizationPublication{}, true,
      hdf5_mode, std::shared_ptr<const QueryModule>{}, pool);
  }
  BinaryValue bytes(const std::string& path) {
    const auto marker = path.rfind("&result=");
    if (marker == std::string::npos) throw std::runtime_error("missing result identity");
    auto opened = bridge->acquire_exact(path.substr(9, marker - 9), path.substr(marker + 8), {});
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    auto value = opened.value()->read(0, opened.value()->size());
    if (!value.ok()) throw std::runtime_error(value.status().message());
    return value.value();
  }
  void retire(const std::string& path) {
    const auto marker = path.rfind("&result=");
    ASSERT_NE(marker, std::string::npos);
    const auto route = path.substr(9, marker - 9);
    const auto id = route.find("?id=");
    ASSERT_NE(id, std::string::npos);
    EXPECT_TRUE(frontend->erase(route.substr(id + 4, 48)));
    bridge->drop(route);
  }
  std::shared_ptr<TinyBackend> backend;
  std::shared_ptr<TypedQueryService> service;
  std::shared_ptr<TypedResultStore> results;
  std::shared_ptr<HotQueryCache> pool;
  std::atomic<unsigned> scans{0}, authorizations{0};
  std::atomic<bool> allowed{true}, removed{false};
  bool deny_empty_sources{false};
  unsigned deny_call{0};
  std::function<void()> during_authorize;
  std::vector<QueryRegistrationPeer> seen_peers;
  std::vector<std::uint64_t> seen_versions;
  std::vector<std::shared_ptr<QueryMetadataBudget>> ledgers;
};

TEST_F(QueryTemplateSocketFixture, Hdf5WaitQueueDoesNotBlockArrowTemplatesAndControlOperations) {
  deny_empty_sources = true;
  auto server = make_server(0600, Hdf5QueryMode::NativeHdf5);
  ASSERT_TRUE(server->start().ok());
  auto serial = submit_query_control(socket, kArrowValues);
  ASSERT_TRUE(serial.ok()) << serial.status().message();
  const auto expected_bytes = bytes(serial.value().path);
  expect_tiny_arrow_values(expected_bytes);
  auto hdf5_template = submit_query_control(socket, query_template_prepare(hdf5_only_typed()));
  ASSERT_TRUE(hdf5_template.ok()) << hdf5_template.status().message();
  auto expected_hdf5 = submit_query_control(socket,
    query_template_execute(hdf5_template.value().template_id));
  ASSERT_FALSE(expected_hdf5.ok());
  auto resource = hdf5_query_task_resource();
  auto held = resource->try_acquire();
  ASSERT_TRUE(held);
  std::vector<std::future<QueryResult<QueryControlResponse>>> hdf5_waiters;
  for (int i = 0; i < 4; ++i)
    hdf5_waiters.push_back(std::async(std::launch::async, [&] {
      return submit_query_control(socket,
        query_template_execute(hdf5_template.value().template_id), std::chrono::seconds(10));
    }));
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    const auto stats = runtime->query_tasks().stats();
    if (stats.queued == 4 && stats.resource_blocked == 4) break;
    std::this_thread::yield();
  }
  const auto occupied = runtime->query_tasks().stats();
  // Preparing a declaration has no HDF5 execution. Arrow template execution
  // must inspect the stored declaration rather than reserve HDF5 just because
  // the request JSON contains only an opaque template ID.
  auto arrow_template = submit_query_control(socket, query_template_prepare(kArrowValues),
    std::chrono::seconds(2));
  std::optional<QueryResult<QueryControlResponse>> arrow;
  std::optional<QueryResult<QueryControlResponse>> released;
  if (arrow_template.ok()) {
    arrow.emplace(submit_query_control(socket,
      query_template_execute(arrow_template.value().template_id), std::chrono::seconds(2)));
    released.emplace(submit_query_control(socket,
      query_template_release(arrow_template.value().template_id), std::chrono::seconds(2)));
  }
  auto unused_hdf5_template = submit_query_control(socket,
    query_template_prepare(hdf5_only_typed()), std::chrono::seconds(2));
  std::optional<QueryResult<QueryControlResponse>> released_hdf5;
  if (unused_hdf5_template.ok())
    released_hdf5.emplace(submit_query_control(socket,
      query_template_release(unused_hdf5_template.value().template_id), std::chrono::seconds(2)));
  // Release before assertions/future destruction so failures cannot deadlock
  // the fixture's socket shutdown behind its own external resource lease.
  held = {};
  EXPECT_EQ(occupied.active, 0u);
  EXPECT_EQ(occupied.queued, 4u);
  EXPECT_GT(occupied.resource_blocked, 0u);
  EXPECT_TRUE(arrow_template.ok()) << arrow_template.status().message();
  EXPECT_TRUE(arrow.has_value() && arrow->ok());
  if (arrow && arrow->ok()) {
    auto actual = bytes(arrow->value().path);
    EXPECT_EQ(actual, expected_bytes);
    expect_tiny_arrow_values(std::move(actual));
  }
  EXPECT_TRUE(released.has_value() && released->ok() && released->value().released);
  EXPECT_TRUE(unused_hdf5_template.ok()) << unused_hdf5_template.status().message();
  EXPECT_TRUE(released_hdf5.has_value() && released_hdf5->ok() && released_hdf5->value().released);
  for (auto& waiter : hdf5_waiters) {
    auto actual = waiter.get();
    EXPECT_FALSE(actual.ok());
    EXPECT_EQ(actual.status().code(), expected_hdf5.status().code());
    EXPECT_EQ(actual.status().phase(), expected_hdf5.status().phase());
    EXPECT_EQ(actual.status().message(), expected_hdf5.status().message());
  }
  server->stop();
  EXPECT_EQ(runtime->query_tasks().stats().queued, 0u);
  EXPECT_EQ(runtime->query_tasks().stats().active, 0u);
}

TEST_F(QueryTemplateSocketFixture, QueuedParsedRequestsObserveHandleRetirementAndCurrentPermission) {
  auto server = make_server(0600, Hdf5QueryMode::NativeHdf5);
  ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare(hdf5_only_typed()));
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto held = hdf5_query_task_resource()->try_acquire();
  ASSERT_TRUE(held);
  const auto execute = query_template_execute(prepared.value().template_id);
  auto ordinary_waiter = std::async(std::launch::async, [&] {
    return submit_query_control(socket, hdf5_only_typed(), std::chrono::seconds(10));
  });
  auto template_waiter = std::async(std::launch::async, [&] {
    return submit_query_control(socket, execute, std::chrono::seconds(10));
  });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    const auto stats = runtime->query_tasks().stats();
    if (stats.queued == 2 && stats.resource_blocked == 2) break;
    std::this_thread::yield();
  }
  const auto occupied = runtime->query_tasks().stats();
  const auto authorizations_before = authorizations.load();
  // Neither the parsed frame nor the resource-plan observation is permission
  // or handle admission. Both can change while that frame waits in the pool.
  auto released = submit_query_control(socket,
    query_template_release(prepared.value().template_id), std::chrono::seconds(2));
  auto missing = submit_query_control(socket, execute, std::chrono::seconds(2));
  allowed = false;
  held = {}; // Always unblock before assertions or future destruction.
  auto ordinary = ordinary_waiter.get();
  auto templated = template_waiter.get();
  EXPECT_EQ(occupied.active, 0u);
  EXPECT_EQ(occupied.queued, 2u);
  EXPECT_EQ(occupied.resource_blocked, 2u);
  EXPECT_EQ(authorizations_before, 0u);
  EXPECT_TRUE(released.ok() && released.value().released);
  EXPECT_FALSE(missing.ok());
  EXPECT_FALSE(templated.ok());
  EXPECT_EQ(templated.status().code(), missing.status().code());
  EXPECT_EQ(templated.status().phase(), missing.status().phase());
  EXPECT_EQ(templated.status().message(), missing.status().message());
  EXPECT_FALSE(ordinary.ok());
  EXPECT_EQ(ordinary.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(ordinary.status().phase(), QueryPhase::Catalog);
  EXPECT_EQ(ordinary.status().message(), "current template source denied");
  EXPECT_EQ(authorizations, 1u);
  EXPECT_EQ(frontend->size(), 0u);
  server->stop();
  EXPECT_EQ(runtime->query_tasks().stats().queued, 0u);
  EXPECT_EQ(runtime->query_tasks().stats().active, 0u);
}

TEST_F(QueryTemplateSocketFixture, PreparationIsStaticWithoutCatalogBindingAndExecutionMatchesOrdinary) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  const auto stats = pool->stats();
  auto prepared = submit_query_control(socket, query_template_prepare());
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(prepared.value().kind, QueryControlResponse::Kind::PreparedQueryTemplate);
  EXPECT_EQ(prepared.value().template_id.size(), 48U);
  EXPECT_EQ(prepared.value().ttl_ms, 300000U);
  EXPECT_EQ(scans, 0U); EXPECT_EQ(authorizations, 0U);
  EXPECT_EQ(backend->manifest_reads, 0U); EXPECT_EQ(backend->layout_reads, 0U);
  EXPECT_EQ(frontend->size(), 0U); EXPECT_EQ(bridge->cached_routes(), 0U);
  EXPECT_EQ(pool->stats().handles, stats.handles + 1);
  auto ordinary = submit_query_control(socket, kTyped);
  auto reused = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_TRUE(ordinary.ok()) << ordinary.status().message();
  ASSERT_TRUE(reused.ok()) << reused.status().message();
  EXPECT_EQ(bytes(ordinary.value().path), bytes(reused.value().path));
  EXPECT_EQ(authorizations, 2U);
  auto released = submit_query_control(socket, query_template_release(prepared.value().template_id));
  ASSERT_TRUE(released.ok()); EXPECT_TRUE(released.value().released);
  EXPECT_EQ(released.value().kind, QueryControlResponse::Kind::ReleasedQueryTemplate);
  // The route still borrows the template's compiled paths/AST. Retirement
  // revokes the ID immediately, but cannot refund the allocation yet.
  EXPECT_EQ(pool->stats().handles, stats.handles + 1);
  retire(reused.value().path);
  EXPECT_EQ(pool->stats().handles, stats.handles);
  server->stop(); EXPECT_EQ(pool->stats().handle_bytes, 0U);
}

TEST_F(QueryTemplateSocketFixture, ResolvedRequestAlonePinsTemplateQuotaAfterIdRouteAndServerRetire) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare()); ASSERT_TRUE(prepared.ok());
  const auto charged = pool->stats().handle_bytes;
  ASSERT_GT(charged, 0U);
  auto executed = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_TRUE(executed.ok()) << executed.status().message();
  std::shared_ptr<const QueryRequest> request_only;
  {
    const auto marker = executed.value().path.rfind("&result=");
    auto resolved = frontend->resolve(executed.value().path.substr(9, marker - 9));
    ASSERT_TRUE(resolved.ok()) << resolved.status().message();
    request_only = std::get<TypedQueryExecutionRequest>(resolved.value()).request;
  }
  auto release = submit_query_control(socket, query_template_release(prepared.value().template_id));
  ASSERT_TRUE(release.ok()); EXPECT_TRUE(release.value().released);
  EXPECT_FALSE(submit_query_control(socket, query_template_execute(prepared.value().template_id)).ok());
  retire(executed.value().path);
  server->stop(); server.reset();
  EXPECT_EQ(pool->stats().handles, 1U);
  EXPECT_EQ(pool->stats().handle_bytes, charged);
  ASSERT_TRUE(request_only); EXPECT_EQ(request_only->sources.paths.size(), 1U);
  request_only.reset();
  EXPECT_EQ(pool->stats().handles, 0U); EXPECT_EQ(pool->stats().handle_bytes, 0U);
}

TEST_F(QueryTemplateSocketFixture, BatchRoutesShareOneRetiredTemplateChargeUntilLastRoute) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare(typed_batch({kTyped, kTyped})));
  ASSERT_TRUE(prepared.ok()); const auto charged = pool->stats().handle_bytes;
  auto result = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_TRUE(result.ok()) << result.status().message(); ASSERT_EQ(result.value().items.size(), 2U);
  auto released = submit_query_control(socket, query_template_release(prepared.value().template_id));
  ASSERT_TRUE(released.ok()); EXPECT_TRUE(released.value().released);
  EXPECT_EQ(pool->stats().handles, 1U); EXPECT_EQ(pool->stats().handle_bytes, charged);
  retire(result.value().items[0].path);
  EXPECT_EQ(pool->stats().handles, 1U); EXPECT_EQ(pool->stats().handle_bytes, charged);
  retire(result.value().items[1].path);
  EXPECT_EQ(pool->stats().handles, 0U); EXPECT_EQ(pool->stats().handle_bytes, 0U);
}

TEST_F(QueryTemplateSocketFixture, SchemaChecksAndCurrentPermissionRemainAtExecution) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  std::string declaration(kTyped);
  const auto end = declaration.rfind("}}");
  declaration.insert(end, ",\"subset\":{\"projection\":[\"unknown.field\"]}");
  auto prepared = submit_query_control(socket, query_template_prepare(declaration));
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(scans, 0U); EXPECT_EQ(authorizations, 0U);
  allowed = false;
  auto denied = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_FALSE(denied.ok()); EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied);
  allowed = true;
  auto invalid_schema = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  EXPECT_FALSE(invalid_schema.ok());
  EXPECT_NE(invalid_schema.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(frontend->size(), 0U); EXPECT_EQ(bridge->cached_routes(), 0U);
}

TEST_F(QueryTemplateSocketFixture, EveryExecutionUsesFreshBudgetAndActualPeer) {
  during_authorize = [] { require_query_metadata_unique(40ULL * 1024 * 1024); };
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare()); ASSERT_TRUE(prepared.ok());
  for (unsigned i = 0; i < 3; ++i) {
    auto result = submit_query_control(socket, query_template_execute(prepared.value().template_id));
    ASSERT_TRUE(result.ok()) << result.status().message();
  }
  ASSERT_EQ(ledgers.size(), 3U); ASSERT_EQ(seen_peers.size(), 3U);
  for (unsigned i = 0; i < 3; ++i) {
    EXPECT_TRUE(ledgers[i]->status().ok());
    EXPECT_GE(ledgers[i]->consumed_bytes(), 40ULL * 1024 * 1024);
    EXPECT_EQ(seen_peers[i].uid, ::geteuid()); EXPECT_EQ(seen_peers[i].gid, ::getegid());
    if (i) EXPECT_NE(ledgers[i].get(), ledgers[i - 1].get());
  }
}

TEST_F(QueryTemplateSocketFixture, LatestIsResolvedAgainAndExactIsNotRetargeted) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  std::string latest(kTyped);
  const std::string version = "\"exact\",\"exact\":7";
  latest.replace(latest.find(version), version.size(), "\"latest\"");
  auto dynamic = submit_query_control(socket, query_template_prepare(latest)); ASSERT_TRUE(dynamic.ok());
  auto exact = submit_query_control(socket, query_template_prepare()); ASSERT_TRUE(exact.ok());
  EXPECT_TRUE(submit_query_control(socket, query_template_execute(dynamic.value().template_id)).ok());
  backend->manifest.key.version = 8;
  auto fresh = submit_query_control(socket, query_template_execute(dynamic.value().template_id));
  ASSERT_TRUE(fresh.ok()) << fresh.status().message();
  ASSERT_GE(seen_versions.size(), 2U);
  EXPECT_EQ(seen_versions[0], 7U); EXPECT_EQ(seen_versions[1], 8U);
  auto old = submit_query_control(socket, query_template_execute(exact.value().template_id));
  auto ordinary_old = submit_query_control(socket, kTyped);
  ASSERT_EQ(old.ok(), ordinary_old.ok());
  if (old.ok()) {
    EXPECT_EQ(bytes(old.value().path), bytes(ordinary_old.value().path));
    EXPECT_NE(bytes(old.value().path), bytes(fresh.value().path));
  }
  removed = true;
  auto gone = submit_query_control(socket, query_template_execute(dynamic.value().template_id));
  auto ordinary_gone = submit_query_control(socket, latest);
  ASSERT_EQ(gone.ok(), ordinary_gone.ok());
  if (gone.ok()) EXPECT_EQ(bytes(gone.value().path), bytes(ordinary_gone.value().path));
}

TEST_F(QueryTemplateSocketFixture, WholeBatchPreservesDuplicatesOrderAndSharedMetadataFailure) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare(typed_batch({kTyped, kTyped})));
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(authorizations, 0U); EXPECT_EQ(pool->stats().handles, 1U);
  auto batch = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_EQ(batch.value().items.size(), 2U);
  EXPECT_EQ(batch.value().items[0].index, 0U); EXPECT_EQ(batch.value().items[1].index, 1U);
  EXPECT_NE(batch.value().items[0].path, batch.value().items[1].path);
  EXPECT_EQ(bytes(batch.value().items[0].path), bytes(batch.value().items[1].path));
  const auto retained = frontend->size();
  during_authorize = [] { require_query_metadata_unique(40ULL * 1024 * 1024); };
  auto failed = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_FALSE(failed.ok()); EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(frontend->size(), retained); EXPECT_EQ(bridge->cached_routes(), retained);
  during_authorize = {};
  EXPECT_TRUE(submit_query_control(socket, query_template_execute(prepared.value().template_id)).ok());
}

TEST_F(QueryTemplateSocketFixture, BatchFailureRetiresEarlierItemsAndDoesNotExecuteLaterItems) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare(typed_batch({kTyped, kTyped, kTyped})));
  ASSERT_TRUE(prepared.ok());
  deny_call = 2;
  auto result = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_FALSE(result.ok()); EXPECT_EQ(result.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(authorizations, 2U); EXPECT_EQ(frontend->size(), 0U);
  EXPECT_EQ(bridge->cached_routes(), 0U); EXPECT_EQ(results->stats().open_handles, 0U);
}

TEST_F(QueryTemplateSocketFixture, InvalidWholeBatchAndUnsupportedBindingsNeverReadOrAuthorize) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  std::string bound(kTyped); bound.pop_back();
  bound += ",\"hdf5_binding\":{\"profile\":\"CF\",\"dataset_path\":\"/T\"}}";
  std::string latest(kTyped);
  const std::string version = "\"exact\",\"exact\":7";
  latest.replace(latest.find(version), version.size(), "\"latest\"");
  std::string subtree(kTyped);
  subtree.replace(subtree.find("\"kind\":\"exact\""), 14, "\"kind\":\"subtree\"");
  for (const auto& declaration : {typed_batch({}), typed_batch({kTyped, latest}),
      typed_batch({kTyped, "{\"kind\":\"typed\",\"request\":{}}"}),
      typed_batch(std::vector<std::string>(65, kTyped)), bound, subtree}) {
    SCOPED_TRACE(declaration);
    EXPECT_FALSE(submit_query_control(socket, query_template_prepare(declaration)).ok());
    EXPECT_EQ(pool->stats().handles, 0U);
  }
  EXPECT_EQ(scans, 0U); EXPECT_EQ(authorizations, 0U);
  EXPECT_EQ(backend->manifest_reads, 0U); EXPECT_EQ(backend->layout_reads, 0U);
}

TEST_F(QueryTemplateSocketFixture, UnknownReleasedAndWrongKindHandlesCannotBeUsedOrErased) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare()); ASSERT_TRUE(prepared.ok());
  const auto id = prepared.value().template_id;
  auto unknown = submit_query_control(socket, query_template_execute(std::string(48, 'f')));
  ASSERT_FALSE(unknown.ok());
  auto wrong_execute = submit_query_control(socket,
    "{\"kind\":\"dataset_catalog_by_selector\",\"selector_id\":\"" + id +
      "\",\"predicate\":" + prepared_catalog_predicate() + "}");
  EXPECT_FALSE(wrong_execute.ok());
  auto wrong_release = submit_query_control(socket, selector_release(id));
  ASSERT_TRUE(wrong_release.ok()); EXPECT_FALSE(wrong_release.value().released);
  EXPECT_EQ(pool->stats().handles, 1U);
  EXPECT_TRUE(submit_query_control(socket, query_template_execute(id)).ok());
  auto release = submit_query_control(socket, query_template_release(id));
  ASSERT_TRUE(release.ok()); EXPECT_TRUE(release.value().released);
  auto twice = submit_query_control(socket, query_template_release(id));
  ASSERT_TRUE(twice.ok()); EXPECT_FALSE(twice.value().released);
  auto stale = submit_query_control(socket, query_template_execute(id));
  ASSERT_FALSE(stale.ok()); EXPECT_EQ(stale.status().code(), unknown.status().code());
  EXPECT_EQ(stale.status().message(), unknown.status().message());
}

TEST_F(SelectorSocketFixture, CatalogAndQueryTemplatesShareKindsAndUidQuota) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto catalog_id = submit_query_control(socket, selector_prepare()); ASSERT_TRUE(catalog_id.ok());
  auto unknown = submit_query_control(socket, query_template_execute(std::string(48, 'f')));
  auto wrong = submit_query_control(socket, query_template_execute(catalog_id.value().selector_id));
  ASSERT_FALSE(unknown.ok()); ASSERT_FALSE(wrong.ok());
  EXPECT_EQ(wrong.status().code(), unknown.status().code());
  EXPECT_EQ(wrong.status().message(), unknown.status().message());
  auto wrong_release = submit_query_control(socket, query_template_release(catalog_id.value().selector_id));
  ASSERT_TRUE(wrong_release.ok()); EXPECT_FALSE(wrong_release.value().released);
  std::vector<std::string> ids;
  for (unsigned i = 0; i < 15; ++i) {
    auto prepared = submit_query_control(socket, query_template_prepare());
    ASSERT_TRUE(prepared.ok()) << i << ": " << prepared.status().message();
    ids.push_back(prepared.value().template_id);
  }
  auto exhausted = submit_query_control(socket, query_template_prepare());
  ASSERT_FALSE(exhausted.ok()); EXPECT_EQ(exhausted.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(pool->stats().handles, 16U);
  EXPECT_TRUE(submit_query_control(socket, selector_execute(catalog_id.value().selector_id)).ok());
  for (const auto& id : ids) EXPECT_TRUE(submit_query_control(socket, query_template_release(id)).value().released);
  EXPECT_TRUE(submit_query_control(socket, selector_release(catalog_id.value().selector_id)).value().released);
  EXPECT_EQ(pool->stats().handles, 0U);
}

TEST_F(QueryTemplateSocketFixture, ExecuteRejectsOverridesAndPathOnlyClientCannotPrepare) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  EXPECT_FALSE(submit_query_registration(socket, query_template_prepare()).ok());
  EXPECT_EQ(pool->stats().handles, 0U);
  auto prepared = submit_query_control(socket, query_template_prepare()); ASSERT_TRUE(prepared.ok());
  for (const auto& extra : {",\"request\":{}", ",\"limits\":{}", ",\"declaration\":{}",
      ",\"predicate\":{}", ",\"sources\":{}", ",\"output\":{}"})
    EXPECT_FALSE(submit_query_control(socket, query_template_execute(prepared.value().template_id, extra)).ok());
  EXPECT_EQ(scans, 0U); EXPECT_EQ(authorizations, 0U);
}

TEST_F(QueryTemplateSocketFixture, StopRetiresTemplatesAndOldIdsCannotSurviveRestart) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare()); ASSERT_TRUE(prepared.ok());
  const auto id = prepared.value().template_id;
  server->stop(); EXPECT_EQ(pool->stats().handles, 0U); EXPECT_EQ(pool->stats().handle_bytes, 0U);
  server.reset(); server = make_server(); ASSERT_TRUE(server->start().ok());
  EXPECT_FALSE(submit_query_control(socket, query_template_execute(id)).ok());
  EXPECT_EQ(authorizations, 0U);
}

TEST_F(QueryTemplateSocketFixture, LostBatchExecuteResponseRetiresAllRoutesButKeepsTemplate) {
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  auto prepared = submit_query_control(socket, query_template_prepare(typed_batch({kTyped, kTyped})));
  ASSERT_TRUE(prepared.ok());
  during_authorize = [] { std::this_thread::sleep_for(std::chrono::milliseconds(50)); };
  auto timed_out = submit_query_control(socket, query_template_execute(prepared.value().template_id),
    std::chrono::milliseconds(5));
  EXPECT_FALSE(timed_out.ok());
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (authorizations < 2 && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  while (frontend->size() && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  // Keep the server and its template registry alive through response cleanup.
  auto release = submit_query_control(socket, query_template_release(prepared.value().template_id));
  ASSERT_TRUE(release.ok()); EXPECT_TRUE(release.value().released);
  EXPECT_EQ(frontend->size(), 0U); EXPECT_EQ(bridge->cached_routes(), 0U);
}

TEST_F(QueryTemplateSocketFixture, FullPlannerStillRunsButSocketDeclarationIsNotRecompiled) {
  const auto profile_path = directory + "/template-profile.jsonl";
  ASSERT_EQ(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", profile_path.c_str(), 1), 0);
  auto server = make_server(); ::unsetenv("IOTDBFS_QUERY_PROFILE_JSONL");
  ASSERT_TRUE(server->start().ok());
  auto declaration = typed_batch({kTyped, kTyped});
  auto prepared = submit_query_control(socket, query_template_prepare(declaration));
  ASSERT_TRUE(prepared.ok());
  auto ordinary = submit_query_control(socket, declaration); ASSERT_TRUE(ordinary.ok());
  auto reused = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_TRUE(reused.ok());
  server->stop();
  unsigned old_profiles = 0, template_profiles = 0;
  std::ifstream input(profile_path);
  std::string line;
  while (std::getline(input, line)) {
    boost::property_tree::ptree profile;
    std::istringstream json(line); boost::property_tree::read_json(json, profile);
    const auto operation = profile.get<std::string>("operation");
    if (operation != "typed_batch" && operation != "query_template_execute") continue;
    const auto& counters = profile.get_child("counters");
    const auto compiles = counters.find("selector.compiles");
    ASSERT_NE(compiles, counters.not_found());
    if (operation == "typed_batch") {
      ++old_profiles; EXPECT_EQ(compiles->second.get_value<unsigned>(), 4U);
    } else {
      ++template_profiles; EXPECT_EQ(compiles->second.get_value<unsigned>(), 0U);
    }
    // Both executions still materialize two independent results through the
    // original planner; no frozen plan/result is retained by the template.
    EXPECT_EQ(profile.get<std::size_t>("counters.typed_batch_items"), 2U);
    EXPECT_EQ(profile.get_child("items").size(), 2U);
  }
  EXPECT_EQ(old_profiles, 1U); EXPECT_EQ(template_profiles, 1U);
  ::unlink(profile_path.c_str());
}

TEST_F(QueryTemplateSocketFixture, NestedAndLateCollectorsMatchOrdinaryChargesAndFailures) {
  std::vector<QueryMetadataTrace> outer_traces, inner_traces;
  during_authorize = [&] {
    // These collectors are installed after catalog selection, at the same
    // runtime callback in either endpoint. All effects must reach them again.
    ScopedQueryMetadataTrace outer("template-test/");
    require_query_metadata("template-test/a", 7);
    {
      ScopedQueryMetadataTrace inner("template-test/");
      require_query_metadata("template-test/a", 11);
      require_query_metadata("template-test/b", 13);
      inner_traces.push_back(inner.release());
    }
    outer_traces.push_back(outer.release());
    require_query_metadata_unique(40ULL * 1024 * 1024);
  };
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  const auto declaration = typed_batch({kTyped, kTyped});
  auto prepared = submit_query_control(socket, query_template_prepare(declaration));
  ASSERT_TRUE(prepared.ok());
  auto ordinary = submit_query_control(socket, declaration);
  auto reused = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_FALSE(ordinary.ok()); ASSERT_FALSE(reused.ok());
  EXPECT_EQ(reused.status().code(), ordinary.status().code());
  EXPECT_EQ(reused.status().phase(), ordinary.status().phase());
  EXPECT_EQ(reused.status().message(), ordinary.status().message());
  ASSERT_EQ(outer_traces.size(), 4U); ASSERT_EQ(inner_traces.size(), 4U);
  EXPECT_EQ(outer_traces[0], outer_traces[2]); EXPECT_EQ(outer_traces[1], outer_traces[3]);
  EXPECT_EQ(inner_traces[0], inner_traces[2]); EXPECT_EQ(inner_traces[1], inner_traces[3]);
  ASSERT_EQ(ledgers.size(), 4U);
  EXPECT_EQ(ledgers[0].get(), ledgers[1].get());
  EXPECT_EQ(ledgers[2].get(), ledgers[3].get());
  EXPECT_NE(ledgers[0].get(), ledgers[2].get());
  EXPECT_EQ(ledgers[0]->consumed_bytes(), ledgers[2]->consumed_bytes());
  EXPECT_EQ(frontend->size(), 0U); EXPECT_EQ(bridge->cached_routes(), 0U);
}

TEST_F(QueryTemplateSocketFixture, EncodedBatchBudgetAndStickyFailureAreEquivalent) {
  std::uint64_t one_size = 0;
  {
    auto server = make_server(); ASSERT_TRUE(server->start().ok());
    auto result = submit_query_control(socket, kTyped); ASSERT_TRUE(result.ok());
    one_size = bytes(result.value().path).size();
  }
  frontend = std::make_shared<TypedQueryFrontend>();
  auto module = QueryModule::create(catalog, storage,
    {std::make_shared<ArrowFormatQueryAdapter>(ArrowDecodeConfig{}, runtime)});
  ASSERT_TRUE(module.ok());
  results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  service = std::make_shared<TypedQueryService>(module.value(), frontend, results, one_size * 2 - 1);
  bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
  auto server = make_server(); ASSERT_TRUE(server->start().ok());
  const auto declaration = typed_batch({kTyped, kTyped});
  auto prepared = submit_query_control(socket, query_template_prepare(declaration)); ASSERT_TRUE(prepared.ok());
  auto a = submit_query_control(socket, declaration);
  auto b = submit_query_control(socket, query_template_execute(prepared.value().template_id));
  ASSERT_FALSE(a.ok()); ASSERT_FALSE(b.ok());
  EXPECT_EQ(a.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(b.status().code(), a.status().code()); EXPECT_EQ(b.status().phase(), a.status().phase());
  EXPECT_EQ(frontend->size(), 0U); EXPECT_EQ(bridge->cached_routes(), 0U);
  EXPECT_EQ(results->stats().open_handles, 0U);
  auto single = submit_query_control(socket, query_template_prepare()); ASSERT_TRUE(single.ok());
  EXPECT_TRUE(submit_query_control(socket, query_template_execute(single.value().template_id)).ok());
}

#ifdef __linux__
TEST_F(QueryTemplateSocketFixture, ActualSocketPeerCannotUseAnotherUidsTemplate) {
  if (::geteuid() != 0) GTEST_SKIP() << "requires root to switch client UID";
  ASSERT_EQ(::chmod(directory.c_str(), 0711), 0);
  int gate[2], outcome[2]; ASSERT_EQ(::pipe(gate), 0); ASSERT_EQ(::pipe(outcome), 0);
  const auto child = ::fork(); ASSERT_GE(child, 0);
  if (!child) {
    ::close(gate[1]); ::close(outcome[0]);
    std::string id(48, '\0');
    char success = 0;
    if (::read(gate[0], id.data(), id.size()) == static_cast<ssize_t>(id.size())) {
      const gid_t group = 23456;
      if (::setgroups(1, &group) != 0 || ::setgid(65534) != 0 || ::setuid(65534) != 0)
        success = 2;
      else {
        auto denied = submit_query_control(socket, query_template_execute(id));
        auto released = submit_query_control(socket, query_template_release(id));
        auto own = submit_query_control(socket, query_template_prepare());
        bool own_ok = false;
        if (own.ok()) {
          own_ok = submit_query_control(socket, query_template_execute(own.value().template_id)).ok();
          auto cleanup = submit_query_control(socket, query_template_release(own.value().template_id));
          own_ok = own_ok && cleanup.ok() && cleanup.value().released;
        }
        success = !denied.ok() && denied.status().code() == QueryError::SourceNotFound &&
          released.ok() && !released.value().released && own_ok;
      }
    }
    if (::write(outcome[1], &success, 1) != 1) ::_exit(3);
    ::_exit(0);
  }
  ::close(gate[0]); ::close(outcome[1]);
  auto server = make_server(0666);
  auto started = server->start();
  auto prepared = started.ok() ? submit_query_control(socket, query_template_prepare()) :
    QueryResult<QueryControlResponse>::failure(started);
  const std::string id = prepared.ok() ? prepared.value().template_id : std::string(48, 'f');
  EXPECT_EQ(::write(gate[1], id.data(), id.size()), static_cast<ssize_t>(id.size())); ::close(gate[1]);
  char success = 0; EXPECT_EQ(::read(outcome[0], &success, 1), 1); ::close(outcome[0]);
  int status = 0; EXPECT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(started.ok()) << started.message(); ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  if (success == 2) GTEST_SKIP() << "cannot switch client UID in this environment";
  EXPECT_EQ(success, 1); ASSERT_EQ(seen_peers.size(), 1U);
  EXPECT_EQ(seen_peers[0].uid, 65534U); EXPECT_EQ(seen_peers[0].gid, 65534U);
  EXPECT_NE(std::find(seen_peers[0].supplementary_groups.begin(), seen_peers[0].supplementary_groups.end(),
    23456), seen_peers[0].supplementary_groups.end());
  EXPECT_TRUE(submit_query_control(socket, query_template_execute(id)).ok());
}

TEST_F(QueryTemplateSocketFixture, SameUidTemplateExecutionObservesChangedGroups) {
  if (::geteuid() != 0) GTEST_SKIP() << "requires root to switch client groups";
  int gate[2], outcome[2]; ASSERT_EQ(::pipe(gate), 0); ASSERT_EQ(::pipe(outcome), 0);
  const auto child = ::fork(); ASSERT_GE(child, 0);
  if (!child) {
    ::close(gate[1]); ::close(outcome[0]);
    std::string id(48, '\0'); char success = 0;
    if (::read(gate[0], id.data(), id.size()) == static_cast<ssize_t>(id.size())) {
      success = 1;
      for (const gid_t group : {23456, 23457}) {
        // Keep the primary group fixed: the peer reader deliberately removes
        // the primary GID from its normalized supplementary group list.
        if (::setgroups(1, &group)) { success = 2; break; }
        if (!submit_query_control(socket, query_template_execute(id)).ok()) { success = 0; break; }
      }
    }
    if (::write(outcome[1], &success, 1) != 1) ::_exit(3);
    ::_exit(0);
  }
  ::close(gate[0]); ::close(outcome[1]);
  auto server = make_server(); auto started = server->start();
  auto prepared = started.ok() ? submit_query_control(socket, query_template_prepare()) :
    QueryResult<QueryControlResponse>::failure(started);
  const std::string id = prepared.ok() ? prepared.value().template_id : std::string(48, 'f');
  EXPECT_EQ(::write(gate[1], id.data(), id.size()), static_cast<ssize_t>(id.size())); ::close(gate[1]);
  char success = 0; EXPECT_EQ(::read(outcome[0], &success, 1), 1); ::close(outcome[0]);
  int status = 0; EXPECT_EQ(::waitpid(child, &status, 0), child);
  ASSERT_TRUE(started.ok()) << started.message(); ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  if (success == 2) GTEST_SKIP() << "cannot switch client groups in this environment";
  EXPECT_EQ(success, 1); ASSERT_EQ(seen_peers.size(), 2U);
  EXPECT_EQ(seen_peers[0].uid, ::geteuid()); EXPECT_EQ(seen_peers[1].uid, ::geteuid());
  EXPECT_EQ(seen_peers[0].gid, ::getegid()); EXPECT_EQ(seen_peers[1].gid, ::getegid());
  EXPECT_NE(seen_peers[0].supplementary_groups, seen_peers[1].supplementary_groups);
  EXPECT_NE(std::find(seen_peers[0].supplementary_groups.begin(), seen_peers[0].supplementary_groups.end(),
    23456), seen_peers[0].supplementary_groups.end());
  EXPECT_NE(std::find(seen_peers[1].supplementary_groups.begin(), seen_peers[1].supplementary_groups.end(),
    23457), seen_peers[1].supplementary_groups.end());
}
#endif
} // namespace

namespace {
class OwnerResultFetchSocketFixture : public SocketFixture {
protected:
  std::unique_ptr<QueryRegistrationSocketServer> owner_server() {
    return std::make_unique<QueryRegistrationSocketServer>(socket, "/mnt/data",
      owner.frontend, std::make_shared<ContextCatalog>(owner.context),
      std::make_shared<StructuredBlobStorage>(owner.backend), runtime, owner.bridge,
      [](const QueryRegistrationPeer&, const auto&, const auto*) {
        ADD_FAILURE() << "owner result fetch must not fall back to strict authorization";
        return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog,
          "strict authorization is not an owner admission");
      }, 0600, TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{},
      Hdf5QueryKernelConfig{}, QueryRegistrationSocketServer::DatasetDiscovery{},
      std::shared_ptr<Hdf5ValidatedLayoutCache>{},
      std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
      QueryRegistrationSocketServer::BeginAuthorizationPublication{}, true);
  }
  std::pair<std::string, std::shared_ptr<TypedResultStore::Handle>> publish() {
    auto route = owner.frontend->register_typed(owner.request);
    if (!route.ok()) throw std::runtime_error(route.status().message());
    auto opened = owner.bridge->acquire(route.value(), {}, nullptr, owner.principal);
    if (!opened.ok()) throw std::runtime_error(opened.status().message());
    return {"/mnt/data" + route.value() + "&result=" + opened.value()->id(), opened.value()};
  }
  OwnerBridgeFixture owner;
};

TEST_F(OwnerResultFetchSocketFixture, ResultFetchAdmitsCurrentSocketPrincipalAndAllOwnerDependencies) {
  // The second frozen candidate misses the predicate, but remains a dependency
  // of the published result. Every new socket open must re-admit that context.
  auto other = owner.backend->manifest;
  other.key.path = "/dataset/predicate-miss.arrow";
  owner.context->manifests.push_back(other);
  owner.context->candidates.push_back({other.key.path, other.key.version, "arrow"});
  owner.request.sources.file_predicate = make_comparison({"manifest.path"},
    ComparisonOperator::Equal, LogicalValue::utf8("/no-match"));
  auto published = publish();
  auto expected = published.second->read(0, published.second->size());
  ASSERT_TRUE(expected.ok());
  auto server = owner_server(); ASSERT_TRUE(server->start().ok());
  const auto before = owner.context->state->admits;
  for (unsigned i = 0; i < 2; ++i) {
    auto opened = fetch_result(socket, fetch_request(published.first));
    ASSERT_TRUE(opened.first.get<bool>("ok"));
    EXPECT_EQ(opened.second, expected.value());
  }
  EXPECT_EQ(owner.context->state->admits, before + 2);
  {
    std::lock_guard<std::recursive_mutex> lock(owner.context->state->mutex);
    owner.context->state->uid = ::geteuid() + 1;
  }
  auto wrong_uid = fetch_result(socket, fetch_request(published.first));
  EXPECT_FALSE(wrong_uid.first.get<bool>("ok"));
  EXPECT_EQ(wrong_uid.first.get<unsigned>("code"), static_cast<unsigned>(QueryError::PermissionDenied));
  {
    std::lock_guard<std::recursive_mutex> lock(owner.context->state->mutex);
    owner.context->state->uid = ::geteuid();
    owner.context->state->revoked = true;
  }
  auto revoked = fetch_result(socket, fetch_request(published.first));
  EXPECT_FALSE(revoked.first.get<bool>("ok"));
  EXPECT_EQ(revoked.first.get<unsigned>("code"), static_cast<unsigned>(QueryError::PermissionDenied));
  EXPECT_EQ(owner.frontend->size(), 1u);
  EXPECT_EQ(owner.bridge->cached_routes(), 1u);
  EXPECT_EQ(published.second->read(0, published.second->size()).value(), expected.value());
  {
    std::lock_guard<std::recursive_mutex> lock(owner.context->state->mutex);
    owner.context->state->revoked = false;
  }
  EXPECT_EQ(fetch_result(socket, fetch_request(published.first)).second, expected.value());
}

TEST_F(OwnerResultFetchSocketFixture, ResultFetchReplaysOwnerFeesWithFreshAndStickyRequestLedgers) {
  auto published = publish();
  std::vector<std::shared_ptr<QueryMetadataBudget>> ledgers;
  std::atomic<bool> exhaust{false};
  owner.context->during_admit = [&] {
    auto ledger = current_query_metadata_budget();
    EXPECT_TRUE(ledger);
    ledgers.push_back(ledger);
    require_query_metadata_unique(40ULL * 1024 * 1024);
    if (exhaust) {
      try { require_query_metadata_unique(30ULL * 1024 * 1024); }
      catch (const QueryMetadataBudgetExceeded&) {} // Swallowed, but failure stays sticky.
    }
  };
  auto server = owner_server(); ASSERT_TRUE(server->start().ok());
  for (unsigned i = 0; i < 3; ++i)
    ASSERT_TRUE(fetch_result(socket, fetch_request(published.first)).first.get<bool>("ok"));
  exhaust = true;
  for (unsigned i = 0; i < 20; ++i) {
    auto refused = fetch_result(socket, fetch_request(published.first));
    EXPECT_FALSE(refused.first.get<bool>("ok"));
    EXPECT_EQ(refused.first.get<unsigned>("code"), static_cast<unsigned>(QueryError::ResourceExhausted));
  }
  exhaust = false;
  EXPECT_TRUE(fetch_result(socket, fetch_request(published.first)).first.get<bool>("ok"));
  server->stop();
  ASSERT_EQ(ledgers.size(), 24u);
  for (unsigned i = 0; i < ledgers.size(); ++i) {
    ASSERT_TRUE(ledgers[i]);
    EXPECT_EQ(ledgers[i]->status().ok(), i < 3 || i == 23);
    EXPECT_GE(ledgers[i]->consumed_bytes(), 40ULL * 1024 * 1024);
    for (unsigned j = 0; j < i; ++j) EXPECT_NE(ledgers[i].get(), ledgers[j].get());
  }
  EXPECT_FALSE(current_query_metadata_budget());
  EXPECT_EQ(owner.frontend->size(), 1u);
}

TEST_F(OwnerResultFetchSocketFixture, StartedTransferOutlivesRouteAndPermissionWithoutProducerLease) {
  // Larger than the Unix socket send buffer: reading only the header leaves
  // an actual in-flight delivery while the route and dependency are retired.
  owner.backend->payload = tiny_arrow_payload(false, 1024 * 1024);
  owner.backend->layout = tiny_arrow_layout(owner.backend->payload);
  owner.backend->manifest.payload_size = owner.backend->payload.size();
  owner.backend->manifest.layout_size = owner.backend->layout.size();
  owner.context->manifests = {owner.backend->manifest};
  owner.request.sources.paths = {{PathScopeKind::Exact, owner.backend->manifest.key.path}};
  owner.request.sources.versions = VersionSelection::exact_version(owner.backend->manifest.key.version);
  owner.request.sources.formats.adapters = AdapterSelectionKind::Listed;
  owner.request.sources.formats.adapter_ids = {"arrow"};
  owner.request.sources.formats.include_byte_passthrough = false;
  owner.request.subset = QueryFragment{};
  owner.request.subset->physical_array = PhysicalArraySubject{"target", 0, 0, {{{0}}}};
  owner.request.subset->projection = {{"index"}, {"value.0"}};
  auto module = QueryModule::create(std::make_shared<ContextCatalog>(owner.context),
    std::make_shared<StructuredBlobStorage>(owner.backend),
    {std::make_shared<ArrowFormatQueryAdapter>()});
  ASSERT_TRUE(module.ok());
  owner.service = std::make_shared<TypedQueryService>(module.value(), owner.frontend,
    owner.results, 16ULL * 1024 * 1024);
  owner.bridge = std::make_shared<FuseTypedQueryBridge>(owner.frontend, owner.service);
  auto published = publish();
  auto expected = published.second->read(0, published.second->size());
  ASSERT_TRUE(expected.ok()); ASSERT_GT(expected.value().size(), 1024u * 1024u);
  published.second.reset(); // The delivery must obtain its own immutable pin.
  owner.context->during_admit = [&] {
    EXPECT_EQ(owner.context->state->gates, 0u);
    EXPECT_EQ(owner.context->state->leases, 0u);
    EXPECT_FALSE(current_query_execution_lease());
  };
  auto server = owner_server(); ASSERT_TRUE(server->start().ok());
  const int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);
  struct Close { int fd; ~Close() { ::close(fd); } } close{fd};
  timeval timeout{3, 0};
  ASSERT_EQ(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
  sockaddr_un address{}; address.sun_family = AF_UNIX;
  std::memcpy(address.sun_path, socket.c_str(), socket.size() + 1);
  ASSERT_EQ(::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  auto exact = [&](void* data, std::size_t size, bool writing) {
    auto* bytes = static_cast<char*>(data);
    for (std::size_t offset = 0; offset < size;) {
      const auto count = writing ? ::write(fd, bytes + offset, size - offset)
                                 : ::read(fd, bytes + offset, size - offset);
      if (count < 0 && errno == EINTR) continue;
      if (count <= 0) throw std::runtime_error("owner fetch short IO");
      offset += static_cast<std::size_t>(count);
    }
  };
  auto request = fetch_request(published.first);
  auto length = htonl(static_cast<std::uint32_t>(request.size()));
  exact(&length, sizeof(length), true); exact(request.data(), request.size(), true);
  exact(&length, sizeof(length), false);
  const auto header_size = ntohl(length); ASSERT_GT(header_size, 0u); ASSERT_LE(header_size, 4096u);
  std::string header(header_size, '\0'); exact(header.data(), header.size(), false);
  boost::property_tree::ptree response;
  std::istringstream input(header); boost::property_tree::read_json(input, response);
  ASSERT_TRUE(response.get<bool>("ok"));
  ASSERT_EQ(response.get<std::size_t>("size"), expected.value().size());
  {
    std::lock_guard<std::recursive_mutex> lock(owner.context->state->mutex);
    EXPECT_EQ(owner.context->state->gates, 0u);
    EXPECT_EQ(owner.context->state->leases, 0u);
    owner.context->state->revoked = true;
  }
  const auto marker = published.first.rfind("&result=");
  const auto route = published.first.substr(9, marker - 9);
  auto registration = owner.frontend->resolve_with_identity(route);
  ASSERT_TRUE(registration.ok());
  EXPECT_TRUE(owner.frontend->erase(registration.value().registration_id));
  owner.bridge->drop(route);
  EXPECT_EQ(owner.frontend->size(), 0u); EXPECT_EQ(owner.bridge->cached_routes(), 0u);
  BinaryValue bytes(expected.value().size()); exact(bytes.data(), bytes.size(), false);
  EXPECT_EQ(bytes, expected.value());
  EXPECT_FALSE(fetch_result(socket, fetch_request(published.first)).first.get<bool>("ok"));
  server->stop();
  EXPECT_EQ(owner.results->stats().open_handles, 0u);
  EXPECT_EQ(owner.context->state->gates, 0u); EXPECT_EQ(owner.context->state->leases, 0u);
}
} // namespace
