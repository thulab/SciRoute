#include "iotdbfs/query/core/QueryModule.h"

#include <gtest/gtest.h>

#include <functional>
#include <limits>
#include <memory>
#include <vector>

namespace {
using namespace iotdbfs;

LogicalSchema locator_schema() {
  return LogicalSchema::create({{"meta.offset", "Offset",
    {LogicalTypeId::UInt64, false, std::nullopt}, FieldRole::Dimension,
    std::nullopt}}).value();
}

class LocatorSource final : public QuerySource {
public:
  explicit LocatorSource(std::string path = "/source.bin", std::int64_t version = 7) {
    value.key = {std::move(path), version};
    value.format_id = "locator-test";
    value.payload_size = 64;
    value.revision = 13;
    value.published = true;
    value.phase = StructuredBlobPhase::Published;
  }
  const StructuredBlobManifest& manifest() const override { return value; }
  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t, std::uint64_t) const override {
    ++payload_reads;
    return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Storage, "catalog must not read payload"));
  }
  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t, std::uint64_t) const override {
    ++layout_reads;
    return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Storage, "prepared catalog must not read layout"));
  }
  StructuredBlobManifest value;
  mutable unsigned payload_reads{0}, layout_reads{0};
};

class LocatorPlan final : public AdapterExecutionPlan {
public:
  std::vector<FormatUnitLocator> units{{"first", 0, 16}, {"second", 48, 16}};
  QueryStatus terminal;
  bool supported{true};
  mutable unsigned visits{0};
  QueryStatus visit_unit_locators(
      const std::function<QueryStatus(const FormatUnitLocator&)>& visitor) const override {
    ++visits;
    if (!supported) return AdapterExecutionPlan::visit_unit_locators(visitor);
    for (const auto& unit : units) {
      auto status = visitor(unit);
      if (!status.ok()) return status;
    }
    return terminal;
  }
};

class LocatorAdapter final : public FormatQueryAdapter {
public:
  const std::string& id() const override { static const std::string value = "locator-test"; return value; }
  bool supports_unit_catalog(const QueryFragment&) const override { return true; }
  QueryResult<QueryCapabilities> capabilities(const QueryPlanningContext&,
      const QueryFragment&) const override {
    return QueryResult<QueryCapabilities>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Planning, "already prepared"));
  }
  QueryResult<AdapterPlan> prepare(const QueryPlanningContext&,
      const QueryFragment&) const override {
    return QueryResult<AdapterPlan>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Planning, "already prepared"));
  }
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(const QueryExecutionContext&,
      std::shared_ptr<const AdapterExecutionPlan>, const QueryExecutionOptions&) const override {
    ++executions;
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Execution, "catalog must not execute"));
  }
  mutable unsigned executions{0};
};

// QueryModule::format_catalog only visits already-prepared metadata, so it
// needs a valid storage owner but must not invoke any storage operation.
class NoReadBackend final : public StructuredBlobStorageBackend {
public:
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return {}; }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey&) const override {
    ADD_FAILURE() << "catalog unexpectedly loaded a manifest";
    return StructuredBlobResult<StructuredBlobManifest>::failure(StructuredBlobError::NotFound, "unused");
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobManifest&) override { return {}; }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&, std::uint64_t,
      const StructuredBlobBytes&) override { return {}; }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&, const StructuredBlobBytes&) override { return {}; }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&, const MetadataMap&) override { return {}; }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(const StructuredBlobVersionKey&,
      std::uint64_t, std::uint64_t) const override {
    ADD_FAILURE() << "catalog unexpectedly read payload";
    return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::NotFound, "unused");
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(const StructuredBlobVersionKey&,
      std::uint64_t, std::uint64_t) const override {
    ADD_FAILURE() << "catalog unexpectedly read layout";
    return StructuredBlobResult<StructuredBlobBytes>::failure(StructuredBlobError::NotFound, "unused");
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&, std::uint64_t) override { return {}; }
};

class FormatCatalogCore : public ::testing::Test {
protected:
  void SetUp() override {
    request.sources.versions = VersionSelection::all();
    request.sources.formats.adapters = AdapterSelectionKind::Listed;
    request.sources.formats.adapter_ids = {"locator-test"};
    request.sources.formats.include_byte_passthrough = false;
    request.subset = QueryFragment{};
    request.subset->projection = {{"meta.offset"}};
    auto result = QueryModule::create(make_in_memory_published_version_catalog({}),
      std::make_shared<StructuredBlobStorage>(std::make_shared<NoReadBackend>()), {adapter});
    ASSERT_TRUE(result.ok()) << result.status().message();
    module = result.value();
  }
  QueryResult<PreparedQuery> prepared(QueryExpressionPtr residual = {},
      std::vector<DomainSlice> slices = {}) {
    return PreparedQuery::create(request, locator_schema(), {{source, adapter,
      AdapterPlan(plan, locator_schema(), std::move(residual), std::move(slices),
        {{"meta.offset"}})}});
  }
  QueryRequest request;
  std::shared_ptr<LocatorSource> source = std::make_shared<LocatorSource>();
  std::shared_ptr<LocatorAdapter> adapter = std::make_shared<LocatorAdapter>();
  std::shared_ptr<LocatorPlan> plan = std::make_shared<LocatorPlan>();
  std::shared_ptr<const QueryModule> module;
};

TEST_F(FormatCatalogCore, ReturnsExactVersionAndRevisionWithoutExecutionOrReads) {
  auto query = prepared(); ASSERT_TRUE(query.ok()) << query.status().message();
  auto result = module->format_catalog(query.value(), 4096);
  ASSERT_TRUE(result.ok()) << result.status().message();
  ASSERT_EQ(result.value().units.size(), 2u);
  const auto& first = result.value().units.front();
  EXPECT_EQ(first.source.path, "/source.bin"); EXPECT_EQ(first.source.version, 7);
  EXPECT_EQ(first.source.format_id, "locator-test"); EXPECT_EQ(first.revision, 13u);
  EXPECT_EQ(first.unit.unit_id, "first"); EXPECT_EQ(first.unit.offset, 0u);
  EXPECT_EQ(result.value().units.back().unit.offset + result.value().units.back().unit.length, 64u);
  EXPECT_EQ(plan->visits, 1u); EXPECT_EQ(adapter->executions, 0u);
  EXPECT_EQ(source->payload_reads, 0u); EXPECT_EQ(source->layout_reads, 0u);
}

TEST_F(FormatCatalogCore, EnforcesCumulativeRowsAcrossUnitsAndSources) {
  request.limits.max_result_rows = 2;
  auto query = prepared(); ASSERT_TRUE(query.ok());
  EXPECT_TRUE(module->format_catalog(query.value(), 4096).ok());
  plan->units.push_back({"third", 16, 1});
  auto exhausted = module->format_catalog(query.value(), 4096);
  ASSERT_FALSE(exhausted.ok()); EXPECT_EQ(exhausted.status().code(), QueryError::ResourceExhausted);
  auto second = std::make_shared<LocatorSource>("/another.bin", 9);
  plan->units = {{"first", 0, 16}, {"second", 48, 16}};
  auto many = PreparedQuery::create(request, locator_schema(), {
    {source, adapter, AdapterPlan(plan, locator_schema(), {}, {}, {{"meta.offset"}})},
    {second, adapter, AdapterPlan(plan, locator_schema(), {}, {}, {{"meta.offset"}})}});
  ASSERT_TRUE(many.ok());
  EXPECT_EQ(module->format_catalog(many.value(), 8192).status().code(), QueryError::ResourceExhausted);
}

TEST_F(FormatCatalogCore, ResponseBudgetIsDeterministicAndEscapeSafe) {
  plan->units = {{std::string(20, '\"'), 0, 1}};
  source->value.key.path = "/quoted/\"\\\n";
  auto query = prepared(); ASSERT_TRUE(query.ok());
  std::uint64_t first_success = 0;
  for (std::uint64_t bytes = 64; bytes < 4096; ++bytes) {
    if (module->format_catalog(query.value(), bytes).ok()) { first_success = bytes; break; }
  }
  ASSERT_GT(first_success, 64u);
  EXPECT_EQ(module->format_catalog(query.value(), first_success - 1).status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(module->format_catalog(query.value(), first_success).ok());
}

TEST_F(FormatCatalogCore, RejectsBadOrOverflowingRangesAndInvalidIdentity) {
  const std::vector<FormatUnitLocator> invalid{{"zero", 0, 0}, {"past", 65, 1},
    {"cross", 63, 2}, {"overflow", 1, std::numeric_limits<std::uint64_t>::max()},
    {"huge-offset", std::numeric_limits<std::uint64_t>::max(), 1}, {"", 0, 1},
    {std::string("a\0b", 3), 0, 1}};
  auto query = prepared(); ASSERT_TRUE(query.ok());
  for (const auto& unit : invalid) {
    SCOPED_TRACE(unit.unit_id); plan->units = {unit};
    auto result = module->format_catalog(query.value(), 4096);
    ASSERT_FALSE(result.ok()); EXPECT_EQ(result.status().code(), QueryError::CorruptLayout);
    ASSERT_TRUE(result.status().detail().source); EXPECT_EQ(result.status().detail().source->version, 7);
  }
}

TEST_F(FormatCatalogCore, RejectsDuplicateUnitWithinSourceButNotAcrossVersions) {
  plan->units = {{"same", 0, 1}, {"same", 1, 1}};
  auto query = prepared(); ASSERT_TRUE(query.ok());
  EXPECT_EQ(module->format_catalog(query.value(), 4096).status().code(), QueryError::CorruptLayout);
  plan->units = {{"same", 0, 1}};
  auto other_version = std::make_shared<LocatorSource>("/source.bin", 8);
  auto many = PreparedQuery::create(request, locator_schema(), {
    {source, adapter, AdapterPlan(plan, locator_schema(), {}, {}, {{"meta.offset"}})},
    {other_version, adapter, AdapterPlan(plan, locator_schema(), {}, {}, {{"meta.offset"}})}});
  ASSERT_TRUE(many.ok());
  auto result = module->format_catalog(many.value(), 4096);
  ASSERT_TRUE(result.ok()); EXPECT_EQ(result.value().units.size(), 2u);
}

TEST_F(FormatCatalogCore, RefusesResidualPredicateInsteadOfSilentlyDroppingIt) {
  auto query = prepared(make_comparison({"meta.offset"}, ComparisonOperator::Equal,
    LogicalValue::uint64(0)));
  ASSERT_TRUE(query.ok()) << query.status().message();
  EXPECT_EQ(module->format_catalog(query.value(), 4096).status().code(), QueryError::UnsupportedCapability);
  EXPECT_EQ(plan->visits, 0u);
}

TEST_F(FormatCatalogCore, RefusesResidualSliceInsteadOfReturningWholeUnits) {
  auto query = prepared({}, {{{"meta.offset"},
    DomainBound{LogicalValue::uint64(1), BoundKind::Inclusive}, std::nullopt}});
  ASSERT_TRUE(query.ok()) << query.status().message();
  EXPECT_EQ(module->format_catalog(query.value(), 4096).status().code(), QueryError::UnsupportedCapability);
  EXPECT_EQ(plan->visits, 0u);
}

TEST_F(FormatCatalogCore, PreservesUnsupportedAndCancellationErrorsWithoutPartialResult) {
  auto query = prepared(); ASSERT_TRUE(query.ok());
  plan->supported = false;
  EXPECT_EQ(module->format_catalog(query.value(), 4096).status().code(), QueryError::UnsupportedCapability);
  plan->supported = true;
  plan->terminal = QueryStatus::failure(QueryError::Cancelled, QueryPhase::Planning, "cancelled after first page");
  auto cancelled = module->format_catalog(query.value(), 4096);
  EXPECT_FALSE(cancelled.ok()); EXPECT_EQ(cancelled.status().code(), QueryError::Cancelled);
  EXPECT_EQ(adapter->executions, 0u);
}

TEST_F(FormatCatalogCore, EmptyMetadataDoesNotNeedAResultRoute) {
  plan->units.clear(); auto query = prepared(); ASSERT_TRUE(query.ok());
  auto result = module->format_catalog(query.value(), 64);
  ASSERT_TRUE(result.ok()); EXPECT_TRUE(result.value().units.empty());
  EXPECT_EQ(adapter->executions, 0u);
}

TEST_F(FormatCatalogCore, EmptySelectionCannotBypassExplicitMetadataAdapterContract) {
  auto empty = PreparedQuery::create(request, std::nullopt, {});
  ASSERT_TRUE(empty.ok()); EXPECT_TRUE(module->format_catalog(empty.value(), 64).ok());
  request.sources.formats.adapter_ids = {"unregistered"};
  empty = PreparedQuery::create(request, std::nullopt, {});
  ASSERT_TRUE(empty.ok());
  EXPECT_EQ(module->format_catalog(empty.value(), 64).status().code(), QueryError::UnsupportedCapability);
  request.sources.formats.adapter_ids = {"locator-test"};
  request.sources.formats.include_byte_passthrough = true;
  empty = PreparedQuery::create(request, std::nullopt, {});
  ASSERT_TRUE(empty.ok());
  EXPECT_EQ(module->format_catalog(empty.value(), 64).status().code(), QueryError::UnsupportedCapability);
}
} // namespace
