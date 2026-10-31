#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/frontend/QueryRegistrationSocket.h"
#include <boost/property_tree/json_parser.hpp>

#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <functional>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace {
using namespace iotdbfs;

LogicalSchema catalog_test_schema() {
  return LogicalSchema::create({{"meta.offset", "Offset",
    {LogicalTypeId::UInt64, false, std::nullopt}, FieldRole::Attribute, std::nullopt}}).value();
}

class CatalogBackend final : public StructuredBlobStorageBackend {
public:
  CatalogBackend() {
    value.key = {"/dataset/a.locator", 7}; value.format_id = "locator-test";
    value.payload_size = 64; value.revision = 9;
    value.published = true; value.phase = StructuredBlobPhase::Published;
  }
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return {}; }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& key) const override {
    ++manifest_reads;
    if (!(value.key == key)) return StructuredBlobResult<StructuredBlobManifest>::failure(
      StructuredBlobError::NotFound, "missing source");
    return StructuredBlobResult<StructuredBlobManifest>::success(value);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobManifest&) override { return {}; }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobBytes&) override { return {}; }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&, const StructuredBlobBytes&) override { return {}; }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&, const MetadataMap&) override { return {}; }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(const StructuredBlobVersionKey&,
      std::uint64_t, std::uint64_t) const override {
    ++payload_reads;
    return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::NotFound, "must not read payload");
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(const StructuredBlobVersionKey&,
      std::uint64_t, std::uint64_t) const override {
    ++layout_reads;
    return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::NotFound, "metadata plan has no layout");
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&, std::uint64_t) override { return {}; }
  StructuredBlobManifest value;
  mutable std::atomic<unsigned> manifest_reads{0}, payload_reads{0}, layout_reads{0};
};

struct CatalogOwnerState {
  std::recursive_mutex mutex;
  bool revoked{false};
  unsigned admits{0}, gates{0}, leases{0};
  std::function<void(unsigned)> on_admit;
};
class CatalogLease final : public QueryExecutionLease {
public:
  explicit CatalogLease(std::shared_ptr<CatalogOwnerState> value) : state(std::move(value)) { ++state->leases; }
  ~CatalogLease() override { std::lock_guard<std::recursive_mutex> lock(state->mutex); --state->leases; }
private:
  std::shared_ptr<CatalogOwnerState> state;
};
class CatalogAdmission final : public QueryAdmission {
public:
  explicit CatalogAdmission(std::shared_ptr<CatalogOwnerState> value)
    : state(std::move(value)), lock(state->mutex) { ++state->gates; }
  ~CatalogAdmission() override { --state->gates; }
  std::shared_ptr<QueryExecutionLease> execution_lease() override { return std::make_shared<CatalogLease>(state); }
private:
  std::shared_ptr<CatalogOwnerState> state;
  std::unique_lock<std::recursive_mutex> lock;
};
class CatalogContext final : public QueryReadContext,
    public std::enable_shared_from_this<CatalogContext> {
public:
  std::shared_ptr<CatalogOwnerState> state = std::make_shared<CatalogOwnerState>();
  std::vector<StructuredBlobManifest> manifests;
  std::vector<QuerySourceReference> references;
  QueryResult<std::shared_ptr<const QueryReadContext>> select(
      const std::vector<StructuredBlobManifest>&) const override {
    // Keep all frozen dependencies, including candidates that will miss.
    return QueryResult<std::shared_ptr<const QueryReadContext>>::success(shared_from_this());
  }
  const std::vector<QuerySourceReference>& sources() const override { return references; }
  QueryResult<std::shared_ptr<QueryAdmission>> admit(const QueryPrincipal& principal) const override {
    std::lock_guard<std::recursive_mutex> lock(state->mutex);
    ++state->admits;
    if (state->on_admit) state->on_admit(state->admits);
    if (state->revoked || principal.uid != ::getuid())
      return QueryResult<std::shared_ptr<QueryAdmission>>::failure(QueryStatus::failure(
        QueryError::PermissionDenied, QueryPhase::Catalog, "revoked source dependency"));
    return QueryResult<std::shared_ptr<QueryAdmission>>::success(std::make_shared<CatalogAdmission>(state));
  }
  QueryResult<StructuredBlobManifest> manifest(const StructuredBlobVersionKey& key) const override {
    EXPECT_EQ(state->gates, 0u); EXPECT_GT(state->leases, 0u);
    EXPECT_TRUE(current_query_execution_lease());
    for (const auto& item : manifests) if (item.key == key)
      return QueryResult<StructuredBlobManifest>::success(item);
    return QueryResult<StructuredBlobManifest>::failure(QueryStatus::failure(
      QueryError::SourceNotFound, QueryPhase::Catalog, "missing frozen source"));
  }
};
class CatalogSelectionFixture final : public PublishedVersionCatalog {
public:
  explicit CatalogSelectionFixture(std::shared_ptr<CatalogContext> value) : context(std::move(value)) {}
  QueryResult<CatalogSelection> scan(const SourceSelector& selector, std::size_t limit) const override {
    auto selected = make_in_memory_published_version_catalog(context->manifests)->scan(selector, limit);
    if (selected.ok()) selected.value().read_context = context;
    return selected;
  }
private:
  std::shared_ptr<CatalogContext> context;
};

struct CatalogObservations {
  unsigned authorizations{0}, preparations{0}, executions{0}, visits{0};
  unsigned selector_bindings{0}, selector_scans{0};
  std::vector<std::uint64_t> predicates;
  std::function<void()> on_visit;
  QueryStatus terminal;
  std::vector<FormatUnitLocator> units{{"message:0", 0, 16}, {"message:1", 48, 16}};
};
class CatalogMetadataPlan final : public AdapterExecutionPlan {
public:
  explicit CatalogMetadataPlan(std::shared_ptr<CatalogObservations> value,
      std::optional<std::uint64_t> offset = {}) : observed(std::move(value)), offset(offset) {}
  QueryStatus visit_unit_locators(const std::function<QueryStatus(const FormatUnitLocator&)>& visitor) const override {
    ++observed->visits;
    if (observed->on_visit) observed->on_visit();
    for (const auto& unit : observed->units) {
      if (offset && unit.offset != *offset) continue;
      auto status = visitor(unit); if (!status.ok()) return status;
    }
    return observed->terminal;
  }
private:
  std::shared_ptr<CatalogObservations> observed;
  std::optional<std::uint64_t> offset;
};
class CatalogMetadataAdapter final : public FormatQueryAdapter {
public:
  explicit CatalogMetadataAdapter(std::shared_ptr<CatalogObservations> value) : observed(std::move(value)) {}
  const std::string& id() const override { static const std::string value = "locator-test"; return value; }
  bool supports_unit_catalog(const QueryFragment&) const override { return true; }
  QueryResult<QueryCapabilities> capabilities(const QueryPlanningContext&, const QueryFragment&) const override {
    EXPECT_GT(observed->authorizations, 0u);
    EXPECT_TRUE(current_query_execution_lease());
    FieldQueryCapability field;
    field.field = {"meta.offset"}; field.projectable = true;
    field.predicate_operators = {ComparisonOperator::Equal};
    field.predicate_pushdown = PushdownLevel::Exact;
    return QueryResult<QueryCapabilities>::success(QueryCapabilities(1, id(), "test-1",
      catalog_test_schema(), {field}, {DomainKind::Rows}, true, true, OrderingGuarantee::Source));
  }
  QueryResult<AdapterPlan> prepare(const QueryPlanningContext&, const QueryFragment& fragment) const override {
    ++observed->preparations;
    std::optional<std::uint64_t> offset;
    if (fragment.predicate) {
      const auto* compare = std::get_if<ComparisonExpression>(&fragment.predicate->node);
      if (compare && compare->field.field_id == "meta.offset" && compare->operation == ComparisonOperator::Equal)
        if (const auto* number = std::get_if<std::uint64_t>(&compare->value.value())) {
          offset = *number; observed->predicates.push_back(*number);
        }
    }
    return QueryResult<AdapterPlan>::success(AdapterPlan(std::make_shared<CatalogMetadataPlan>(observed, offset),
      catalog_test_schema(), offset ? nullptr : fragment.predicate, fragment.slices, fragment.projection));
  }
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(const QueryExecutionContext&,
      std::shared_ptr<const AdapterExecutionPlan>, const QueryExecutionOptions&) const override {
    ++observed->executions;
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Execution, "discovery must not execute"));
  }
private:
  std::shared_ptr<CatalogObservations> observed;
};

const char* kFormatCatalog = R"({"kind":"format_catalog","request":{"sources":{"paths":[{"kind":"exact","path":"/dataset/a.locator"}],"versions":{"kind":"exact","exact":7},"formats":{"adapters":"listed","adapter_ids":["locator-test"],"include_byte_passthrough":false}},"subset":{"projection":["meta.offset"]}}})";

class FormatCatalogSocket : public ::testing::Test {
protected:
  void SetUp() override {
    char pattern[] = "/tmp/iotdbfs-format-catalog-XXXXXX";
    auto created = ::mkdtemp(pattern); ASSERT_NE(created, nullptr);
    directory = created; socket = directory + "/control.sock";
    context->manifests = {backend->value};
    context->references = {{backend->value.key.path, backend->value.key.version, "locator-test"}};
    catalog = std::make_shared<CatalogSelectionFixture>(context);
    storage = std::make_shared<StructuredBlobStorage>(backend);
    auto created_module = QueryModule::create(catalog, storage,
      {std::make_shared<CatalogMetadataAdapter>(observed)});
    ASSERT_TRUE(created_module.ok()) << created_module.status().message(); module = created_module.value();
    auto service = std::make_shared<TypedQueryService>(module, frontend, results, 1024 * 1024);
    bridge = std::make_shared<FuseTypedQueryBridge>(frontend, service);
    observed->on_visit = [&] {
      EXPECT_GT(observed->authorizations, 0u);
      EXPECT_EQ(context->state->gates, 0u); EXPECT_GT(context->state->leases, 0u);
      EXPECT_TRUE(current_query_execution_lease());
    };
  }
  void TearDown() override {
    server.reset();
    ::unlink(socket.c_str()); ::unlink((socket + ".lock").c_str()); ::rmdir(directory.c_str());
  }
  void start(QueryRegistrationSocketServer::Authorization custom = {}, bool handles = false) {
    auto authorization = custom ? std::move(custom) : QueryRegistrationSocketServer::Authorization(
      [&](const QueryRegistrationPeer&, const std::vector<QuerySourceReference>& refs,
          const CatalogSelection* selection) {
        ++observed->authorizations;
        EXPECT_NE(selection, nullptr);
        if (selection) EXPECT_EQ(refs.size(), selection->versions().size());
        return QueryStatus::success();
      });
    server = std::make_unique<QueryRegistrationSocketServer>(socket, "/mnt/data", frontend,
      catalog, storage, runtime, bridge, std::move(authorization), 0600,
      TrainingExecutionOptions{}, PreparedTrainingRegistry::Config{}, Hdf5QueryKernelConfig{},
      QueryRegistrationSocketServer::DatasetDiscovery{}, nullptr,
      std::vector<std::shared_ptr<const Hdf5SemanticProfile>>{},
      QueryRegistrationSocketServer::BeginAuthorizationPublication{}, true,
      Hdf5QueryMode::IoTDBIndexed, module, handles ? pool : nullptr,
      [&] {
        ++observed->selector_bindings;
        auto binding = std::make_shared<HotQueryCache::Binding>(); binding->owner = pool;
        return QueryResult<std::shared_ptr<HotQueryCache::Binding>>::success(std::move(binding));
      },
      [&](const SourceSelector& selector, std::size_t max_sources,
          const std::shared_ptr<HotQueryCache::Binding>& binding) {
        ++observed->selector_scans; EXPECT_TRUE(binding);
        return catalog->scan(selector, max_sources);
      });
    ASSERT_TRUE(server->start().ok());
  }
  void expect_no_results() {
    EXPECT_EQ(observed->executions, 0u); EXPECT_EQ(backend->payload_reads, 0u);
    EXPECT_EQ(backend->layout_reads, 0u); EXPECT_EQ(frontend->size(), 0u);
    EXPECT_EQ(bridge->cached_routes(), 0u); EXPECT_EQ(results->stats().registered_entries, 0u);
    EXPECT_EQ(context->state->gates, 0u); EXPECT_EQ(context->state->leases, 0u);
  }
  std::string directory, socket;
  std::shared_ptr<CatalogBackend> backend = std::make_shared<CatalogBackend>();
  std::shared_ptr<CatalogContext> context = std::make_shared<CatalogContext>();
  std::shared_ptr<CatalogObservations> observed = std::make_shared<CatalogObservations>();
  std::shared_ptr<TypedQueryFrontend> frontend = std::make_shared<TypedQueryFrontend>();
  std::shared_ptr<TypedResultStore> results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  std::shared_ptr<QueryRuntime> runtime = std::make_shared<QueryRuntime>();
  std::shared_ptr<const PublishedVersionCatalog> catalog;
  std::shared_ptr<const StructuredBlobStorage> storage;
  std::shared_ptr<const QueryModule> module;
  std::shared_ptr<HotQueryCache> pool = std::make_shared<HotQueryCache>();
  std::shared_ptr<FuseTypedQueryBridge> bridge;
  std::unique_ptr<QueryRegistrationSocketServer> server;
};

TEST_F(FormatCatalogSocket, RoundTripsMetadataWithoutRoutesOrPayloadAndReadmitsEveryRequest) {
  start();
  for (unsigned request = 0; request != 2; ++request) {
    const auto before = context->state->admits;
    auto response = submit_query_control(socket, kFormatCatalog);
    ASSERT_TRUE(response.ok()) << response.status().message();
    EXPECT_EQ(response.value().kind, QueryControlResponse::Kind::FormatCatalog);
    ASSERT_EQ(response.value().format_units.units.size(), 2u);
    const auto& first = response.value().format_units.units.front();
    EXPECT_EQ(first.source.path, backend->value.key.path); EXPECT_EQ(first.source.version, 7);
    EXPECT_EQ(first.source.format_id, "locator-test"); EXPECT_EQ(first.revision, 9u);
    EXPECT_EQ(first.unit.unit_id, "message:0"); EXPECT_EQ(first.unit.length, 16u);
    EXPECT_GE(context->state->admits - before, 2u);
    EXPECT_TRUE(response.value().path.empty());
    expect_no_results();
  }
  EXPECT_EQ(observed->authorizations, 2u);
}

TEST_F(FormatCatalogSocket, DeniedAuthorizationPrecedesAdapterMetadataInspection) {
  start([&](const auto&, const auto&, const auto*) {
    ++observed->authorizations;
    return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "denied");
  });
  auto response = submit_query_control(socket, kFormatCatalog);
  ASSERT_FALSE(response.ok()); EXPECT_EQ(response.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(observed->preparations, 0u); EXPECT_EQ(observed->visits, 0u);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, AuthorizesEveryFrozenCandidateBeforeAnyPlanIsVisited) {
  auto other = backend->value; other.key.path = "/dataset/unreadable.locator";
  context->manifests.push_back(other);
  context->references.push_back({other.key.path, other.key.version, "locator-test"});
  start([&](const auto&, const std::vector<QuerySourceReference>& refs, const CatalogSelection* selected) {
    ++observed->authorizations;
    EXPECT_EQ(&refs, &context->references);
    EXPECT_EQ(refs.size(), 2u); EXPECT_EQ(selected->versions().size(), 2u);
    EXPECT_EQ(selected->read_context->sources().size(), 2u);
    return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "second candidate denied");
  });
  auto response = submit_query_control(socket,
    R"({"kind":"format_catalog","request":{"sources":{"paths":[{"kind":"subtree","path":"/dataset"}],"versions":{"kind":"all"},"formats":{"adapters":"listed","adapter_ids":["locator-test"],"include_byte_passthrough":false}},"subset":{"projection":["meta.offset"]}}})");
  ASSERT_FALSE(response.ok()); EXPECT_EQ(response.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(observed->preparations, 0u); EXPECT_EQ(observed->visits, 0u);
  EXPECT_EQ(backend->manifest_reads, 0u);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, RevocationAfterMetadataVisitRejectsFinalSend) {
  const auto check_lease = observed->on_visit;
  observed->on_visit = [&, check_lease] { check_lease(); context->state->revoked = true; };
  start();
  auto response = submit_query_control(socket, kFormatCatalog);
  ASSERT_FALSE(response.ok()); EXPECT_EQ(response.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(observed->visits, 1u); EXPECT_GE(context->state->admits, 2u);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, EmptyOwnerSelectionStillRequiresFinalAdmission) {
  context->manifests.clear(); context->references.clear();
  context->state->on_admit = [&](unsigned ordinal) { if (ordinal == 2) context->state->revoked = true; };
  start();
  auto response = submit_query_control(socket, kFormatCatalog);
  ASSERT_FALSE(response.ok()); EXPECT_EQ(response.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(observed->preparations, 0u); EXPECT_EQ(context->state->admits, 2u);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, EmptySuccessKeepsTheProtocolAndBothAdmissionBoundaries) {
  context->manifests.clear(); context->references.clear();
  start();
  auto response = submit_query_control(socket, kFormatCatalog);
  ASSERT_TRUE(response.ok()) << response.status().message();
  EXPECT_EQ(response.value().kind, QueryControlResponse::Kind::FormatCatalog);
  EXPECT_TRUE(response.value().format_units.units.empty());
  EXPECT_EQ(context->state->admits, 2u); EXPECT_EQ(observed->authorizations, 1u);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, SwallowedFinalBudgetFailureRejectsMetadataSend) {
  context->state->on_admit = [&](unsigned ordinal) {
    if (ordinal == 2) {
      try { require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1); }
      catch (const QueryMetadataBudgetExceeded&) {}
    }
  };
  start();
  auto response = submit_query_control(socket, kFormatCatalog);
  ASSERT_FALSE(response.ok()); EXPECT_EQ(response.status().code(), QueryError::ResourceExhausted);
  expect_no_results();
  context->state->on_admit = {};
  auto next_request = submit_query_control(socket, kFormatCatalog);
  ASSERT_TRUE(next_request.ok()) << next_request.status().message();
  EXPECT_EQ(next_request.value().format_units.units.size(), 2u);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, CancelledMetadataVisitorDoesNotLeakPartialUnits) {
  observed->terminal = QueryStatus::failure(QueryError::Cancelled, QueryPhase::Planning, "cancelled");
  start();
  auto response = submit_query_control(socket, kFormatCatalog);
  ASSERT_FALSE(response.ok()); EXPECT_EQ(response.status().code(), QueryError::Cancelled);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, RowAndFrameLimitsRejectMetadataWithoutRegisteringAnything) {
  start();
  std::string request = kFormatCatalog;
  const auto end = request.rfind("}}"); ASSERT_NE(end, std::string::npos);
  request.insert(end, ",\"limits\":{\"max_result_rows\":1}");
  auto rows = submit_query_control(socket, request);
  ASSERT_FALSE(rows.ok()); EXPECT_EQ(rows.status().code(), QueryError::ResourceExhausted);
  observed->units = {{std::string(200000, '\"'), 0, 1}};
  auto frame = submit_query_control(socket, kFormatCatalog);
  ASSERT_FALSE(frame.ok()); EXPECT_EQ(frame.status().code(), QueryError::ResourceExhausted);
  expect_no_results();
}

TEST_F(FormatCatalogSocket, UnknownAndDuplicateProtocolMembersFailBeforePlanning) {
  start();
  for (const auto& extra : {std::string(",\"unexpected\":true"),
       std::string(",\"kind\":\"format_catalog\"")}) {
    std::string request = kFormatCatalog;
    request.insert(request.size() - 1, extra);
    auto response = submit_query_control(socket, request);
    ASSERT_FALSE(response.ok()); EXPECT_EQ(response.status().code(), QueryError::InvalidQuery);
  }
  EXPECT_EQ(observed->preparations, 0u); EXPECT_EQ(observed->authorizations, 0u);
  expect_no_results();
}

std::string format_selector_prepare(bool latest = false) {
  std::string request(kFormatCatalog);
  request.replace(request.find("format_catalog"), std::string("format_catalog").size(), "catalog_selector_prepare");
  const auto begin = request.find("\"request\":{");
  request.insert(begin + std::string("\"request\":{").size(), "\"format_id\":\"locator-test\",");
  if (latest) {
    const std::string exact = "\"versions\":{\"kind\":\"exact\",\"exact\":7}";
    request.replace(request.find(exact), exact.size(), "\"versions\":{\"kind\":\"latest\"}");
  }
  return request;
}
std::string format_selector_execute(const std::string& id, std::uint64_t offset = 0,
    const std::string& extra = {}) {
  return "{\"kind\":\"format_catalog_by_selector\",\"selector_id\":\"" + id +
    "\",\"predicate\":{\"kind\":\"compare\",\"field\":\"meta.offset\",\"operator\":\"eq\","
    "\"value\":{\"type\":\"uint64\",\"value\":" + std::to_string(offset) + "}}" + extra + "}";
}
std::string format_selector_release(const std::string& id) {
  return "{\"kind\":\"catalog_selector_release\",\"selector_id\":\"" + id + "\"}";
}

TEST_F(FormatCatalogSocket, PublicSelectorPreparesStaticMetadataWithoutSelectionOrAuthorization) {
  start({}, true);
  auto prepared = submit_query_control(socket, format_selector_prepare());
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(prepared.value().kind, QueryControlResponse::Kind::PreparedCatalogSelector);
  EXPECT_EQ(prepared.value().selector_id.size(), 48U);
  EXPECT_EQ(prepared.value().ttl_ms, 300000U);
  EXPECT_EQ(observed->selector_bindings, 1U); EXPECT_EQ(observed->selector_scans, 0U);
  EXPECT_EQ(observed->authorizations, 0U); EXPECT_EQ(observed->preparations, 0U);
  EXPECT_EQ(observed->visits, 0U); EXPECT_EQ(context->state->admits, 0U);
  EXPECT_EQ(pool->stats().handles, 1U); expect_no_results();
  auto full = std::string(kFormatCatalog);
  const auto subset = full.find("\"projection\":[\"meta.offset\"]");
  full.insert(subset, "\"predicate\":{\"kind\":\"compare\",\"field\":\"meta.offset\",\"operator\":\"eq\",\"value\":{\"type\":\"uint64\",\"value\":0}},");
  auto ordinary = submit_query_control(socket, full);
  auto handle = submit_query_control(socket, format_selector_execute(prepared.value().selector_id));
  ASSERT_TRUE(ordinary.ok()) << ordinary.status().message();
  ASSERT_TRUE(handle.ok()) << handle.status().message();
  ASSERT_EQ(ordinary.value().format_units.units.size(), 1U);
  ASSERT_EQ(handle.value().format_units.units.size(), 1U);
  const auto& a = ordinary.value().format_units.units[0]; const auto& b = handle.value().format_units.units[0];
  EXPECT_EQ(a.source.path, b.source.path); EXPECT_EQ(a.source.version, b.source.version);
  EXPECT_EQ(a.revision, b.revision); EXPECT_EQ(a.unit.unit_id, b.unit.unit_id);
  EXPECT_EQ(a.unit.offset, b.unit.offset); EXPECT_EQ(a.unit.length, b.unit.length);
  EXPECT_EQ(observed->selector_scans, 1U); EXPECT_EQ(observed->authorizations, 2U);
  EXPECT_EQ(context->state->admits, 4U); expect_no_results();
  server->stop(); EXPECT_EQ(pool->stats().handles, 0U);
}

TEST_F(FormatCatalogSocket, PublicSelectorCreatesFreshPredicatePlansAndPreservesEmptyMatches) {
  start({}, true); auto prepared = submit_query_control(socket, format_selector_prepare());
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  const auto id = prepared.value().selector_id;
  for (const auto offset : {0U, 48U, 999U}) {
    auto found = submit_query_control(socket, format_selector_execute(id, offset));
    ASSERT_TRUE(found.ok()) << found.status().message();
    if (offset == 999) EXPECT_TRUE(found.value().format_units.units.empty());
    else {
      ASSERT_EQ(found.value().format_units.units.size(), 1U);
      EXPECT_EQ(found.value().format_units.units[0].unit.offset, offset);
    }
  }
  EXPECT_EQ(observed->predicates, (std::vector<std::uint64_t>{0, 48, 999}));
  EXPECT_EQ(observed->preparations, 3U); EXPECT_EQ(observed->selector_scans, 3U);
  EXPECT_EQ(context->state->admits, 6U); expect_no_results();
}

TEST_F(FormatCatalogSocket, PublicSelectorLatestSelectsCurrentVersionsRatherThanFreezingPreparedPlan) {
  start({}, true); auto prepared = submit_query_control(socket, format_selector_prepare(true));
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  const auto id = prepared.value().selector_id;
  auto before = submit_query_control(socket, format_selector_execute(id)); ASSERT_TRUE(before.ok());
  ASSERT_EQ(before.value().format_units.units.size(), 1U);
  EXPECT_EQ(before.value().format_units.units[0].source.version, 7);
  backend->value.key.version = 8; context->manifests = {backend->value};
  context->references = {{backend->value.key.path, 8, "locator-test"}};
  auto after = submit_query_control(socket, format_selector_execute(id));
  ASSERT_TRUE(after.ok()) << after.status().message(); ASSERT_EQ(after.value().format_units.units.size(), 1U);
  EXPECT_EQ(after.value().format_units.units[0].source.version, 8);
  EXPECT_EQ(observed->selector_bindings, 1U); EXPECT_EQ(observed->selector_scans, 2U);
  EXPECT_EQ(observed->preparations, 2U); expect_no_results();
}

TEST_F(FormatCatalogSocket, PublicSelectorKeepsAuthorizationAdmissionLeaseAndFinalSendChecks) {
  bool allowed = true;
  start([&](const QueryRegistrationPeer& peer, const auto&, const CatalogSelection* selection) {
    ++observed->authorizations; EXPECT_EQ(peer.uid, ::geteuid()); EXPECT_EQ(peer.gid, ::getegid());
    if (!allowed) return QueryStatus::failure(QueryError::PermissionDenied, QueryPhase::Catalog, "revoked");
    auto admitted = selection->read_context->admit({peer.uid, peer.gid, peer.supplementary_groups});
    return admitted.ok() ? QueryStatus::success() : admitted.status();
  }, true);
  auto prepared = submit_query_control(socket, format_selector_prepare()); ASSERT_TRUE(prepared.ok());
  const auto id = prepared.value().selector_id;
  auto first = submit_query_control(socket, format_selector_execute(id)); ASSERT_TRUE(first.ok());
  EXPECT_EQ(context->state->admits, 3U); // Includes the production-like authorizer admission.
  allowed = false;
  auto denied = submit_query_control(socket, format_selector_execute(id));
  EXPECT_EQ(denied.status().code(), QueryError::PermissionDenied); EXPECT_EQ(observed->preparations, 1U);
  allowed = true;
  const auto check_lease = observed->on_visit;
  observed->on_visit = [&, check_lease] { check_lease(); context->state->revoked = true; };
  auto raced = submit_query_control(socket, format_selector_execute(id));
  EXPECT_EQ(raced.status().code(), QueryError::PermissionDenied);
  EXPECT_EQ(context->state->admits, 6U); EXPECT_EQ(pool->stats().handles, 1U); expect_no_results();
}

TEST_F(FormatCatalogSocket, PublicSelectorBudgetIsIndependentAndFailureCannotPoisonLaterRequest) {
  start({}, true); auto prepared = submit_query_control(socket, format_selector_prepare()); ASSERT_TRUE(prepared.ok());
  const auto id = prepared.value().selector_id;
  bool fail = true;
  const auto check_lease = observed->on_visit;
  observed->on_visit = [&, check_lease] {
    check_lease(); auto budget = current_query_metadata_budget(); EXPECT_TRUE(budget);
    if (fail) {
      try { require_query_metadata_unique(QueryMetadataBudget::maximum_bytes + 1); }
      catch (const QueryMetadataBudgetExceeded&) {}
    } else require_query_metadata_unique(QueryMetadataBudget::maximum_bytes / 2 + 1);
  };
  auto failed = submit_query_control(socket, format_selector_execute(id));
  EXPECT_EQ(failed.status().code(), QueryError::ResourceExhausted);
  fail = false;
  for (unsigned i = 0; i < 2; ++i)
    EXPECT_TRUE(submit_query_control(socket, format_selector_execute(id)).ok());
  EXPECT_EQ(pool->stats().handles, 1U); expect_no_results();
}

TEST_F(FormatCatalogSocket, PublicSelectorRejectsWrongEndpointOverridesUnknownAndReleasedIds) {
  start({}, true); auto prepared = submit_query_control(socket, format_selector_prepare()); ASSERT_TRUE(prepared.ok());
  const auto id = prepared.value().selector_id;
  for (const auto& extra : {",\"limits\":{}", ",\"sources\":{}", ",\"subset\":{}", ",\"max_sources\":2"}) {
    auto invalid = submit_query_control(socket, format_selector_execute(id, 0, extra));
    EXPECT_EQ(invalid.status().code(), QueryError::InvalidQuery);
  }
  auto wrong_kind = submit_query_control(socket,
    "{\"kind\":\"dataset_catalog_by_selector\",\"selector_id\":\"" + id +
    "\",\"predicate\":{\"kind\":\"null\",\"field\":\"hdf5.attr:shortName\"}}");
  EXPECT_EQ(wrong_kind.status().code(), QueryError::InvalidQuery);
  auto dataset = submit_query_control(socket,
    R"({"kind":"catalog_selector_prepare","request":{"format_id":"hdf5","sources":{}}})");
  ASSERT_TRUE(dataset.ok()) << dataset.status().message();
  auto wrong_format = submit_query_control(socket, format_selector_execute(dataset.value().selector_id));
  EXPECT_EQ(wrong_format.status().code(), QueryError::InvalidQuery);
  EXPECT_TRUE(submit_query_control(socket, format_selector_release(dataset.value().selector_id)).ok());
  auto unknown = submit_query_control(socket, format_selector_execute(std::string(48, 'f')));
  EXPECT_EQ(unknown.status().code(), QueryError::SourceNotFound);
  auto release = submit_query_control(socket, format_selector_release(id)); ASSERT_TRUE(release.ok());
  EXPECT_TRUE(release.value().released); EXPECT_EQ(pool->stats().handles, 0U);
  auto stale = submit_query_control(socket, format_selector_execute(id));
  EXPECT_EQ(stale.status().code(), QueryError::SourceNotFound);
  EXPECT_EQ(stale.status().message(), unknown.status().message());
  EXPECT_EQ(observed->selector_scans, 0U); EXPECT_EQ(observed->authorizations, 0U); expect_no_results();
}

TEST_F(FormatCatalogSocket, PublicSelectorRejectsDynamicFieldsInPrepareAndMismatchedFormat) {
  start({}, true);
  auto dynamic = format_selector_prepare();
  dynamic.insert(dynamic.find("\"projection\""), "\"predicate\":{\"kind\":\"null\",\"field\":\"meta.offset\"},");
  EXPECT_EQ(submit_query_control(socket, dynamic).status().code(), QueryError::InvalidQuery);
  auto mismatch = format_selector_prepare();
  mismatch.replace(mismatch.find("\"format_id\":\"locator-test\""),
    std::string("\"format_id\":\"locator-test\"").size(), "\"format_id\":\"grib\"");
  EXPECT_EQ(submit_query_control(socket, mismatch).status().code(), QueryError::InvalidQuery);
  EXPECT_EQ(pool->stats().handles, 0U); EXPECT_EQ(observed->selector_bindings, 0U);
  EXPECT_EQ(observed->preparations, 0U); expect_no_results();
}

TEST_F(FormatCatalogSocket, PublicSelectorProfileCountsActualFeeReplayAcrossStagesAndFinalAdmission) {
  const auto path = directory + "/selector-profile.jsonl";
  ASSERT_EQ(::setenv("IOTDBFS_QUERY_PROFILE_JSONL", path.c_str(), 1), 0);
  start({}, true);
  ::unsetenv("IOTDBFS_QUERY_PROFILE_JSONL");
  auto prepared = submit_query_control(socket, format_selector_prepare()); ASSERT_TRUE(prepared.ok());
  const auto check_lease = observed->on_visit;
  observed->on_visit = [check_lease] {
    check_lease();
    replay_query_metadata_trace(std::make_shared<const PreparedQueryMetadataTrace>(
      QueryMetadataTrace{{"format-unit-fee/a", 7}, {"format-unit-fee/b", 11}}));
  };
  context->state->on_admit = [&](unsigned ordinal) {
    if (ordinal == 2) replay_query_metadata_trace(std::make_shared<const PreparedQueryMetadataTrace>(
      QueryMetadataTrace{{"publication-fee", 13}}));
  };
  auto result = submit_query_control(socket, format_selector_execute(prepared.value().selector_id));
  ASSERT_TRUE(result.ok()) << result.status().message();
  server->stop();
  std::ifstream file(path); std::string line; boost::property_tree::ptree record;
  while (std::getline(file, line)) {
    std::istringstream input(line); boost::property_tree::ptree parsed;
    boost::property_tree::read_json(input, parsed);
    if (parsed.get<std::string>("operation") == "format_catalog_by_selector") record = std::move(parsed);
  }
  ASSERT_FALSE(record.empty());
  const auto& counts = record.get_child("counters");
  auto count = [&](const char* name) {
    const auto found = counts.find(name);
    EXPECT_NE(found, counts.not_found()) << name;
    return found == counts.not_found() ? std::uint64_t{999} : found->second.get_value<std::uint64_t>();
  };
  EXPECT_EQ(count("request_metadata.fee_entries_visited"), 3U);
  EXPECT_EQ(count("format_catalog.locators_metadata.fee_entries_visited"), 2U);
  EXPECT_EQ(count("publication.admit_metadata.fee_entries_visited"), 1U);
  EXPECT_EQ(count("request_metadata.fee_entries_skipped"), 0U);
  EXPECT_EQ(count("selector.compiles"), 0U);
  ::unlink(path.c_str());
}
} // namespace
