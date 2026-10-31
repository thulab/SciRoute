#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include <gtest/gtest.h>
#include <map>

using namespace iotdbfs;
namespace {
StructuredBlobManifest item(unsigned i) {
  StructuredBlobManifest m;
  m.key = {"/domain/source" + std::to_string(i), 10 + i};
  m.format_id = "domain-test"; m.payload_size = 1; m.layout_size = 1;
  m.published = true; m.phase = StructuredBlobPhase::Published; m.revision = 1;
  return m;
}
class Backend final : public StructuredBlobStorageBackend {
public:
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override {return denied();}
  StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& key) const override {
    for (unsigned i = 0; i < 3; ++i) if (item(i).key == key)
      return StructuredBlobResult<StructuredBlobManifest>::success(item(i));
    return StructuredBlobResult<StructuredBlobManifest>::failure(StructuredBlobError::NotFound, "missing");
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&, std::uint64_t, const StructuredBlobManifest&) override {return denied();}
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&, std::uint64_t, const StructuredBlobBytes&) override {return denied();}
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&, const StructuredBlobBytes&) override {return denied();}
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&, const MetadataMap&) override {return denied();}
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(const StructuredBlobVersionKey&, std::uint64_t, std::uint64_t n) const override {
    return StructuredBlobResult<StructuredBlobBytes>::success(StructuredBlobBytes(n, 1));
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(const StructuredBlobVersionKey&, std::uint64_t, std::uint64_t n) const override {
    return StructuredBlobResult<StructuredBlobBytes>::success(StructuredBlobBytes(n, 1));
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&, std::uint64_t) override {return denied();}
private:
  static StructuredBlobStatus denied() {return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure, "read-only");}
};
class Catalog final : public PublishedVersionCatalog {
public:
  Catalog(std::uint64_t storage_namespace, std::shared_ptr<QueryDomainCache> cache)
      : storage_namespace(storage_namespace), cache(std::move(cache)) {}
  QueryResult<CatalogSelection> scan(const SourceSelector&, std::size_t maximum) const override {
    if (maximum < 3) return QueryResult<CatalogSelection>::failure(QueryStatus::failure(
      QueryError::ResourceExhausted, QueryPhase::Catalog, "candidate cap"));
    CatalogSelection result; result.storage_namespace = storage_namespace; result.hot_state = cache;
    std::vector<StructuredBlobManifest> versions;
    for (unsigned i = 0; i < 3; ++i) versions.push_back(item(i));
    if (duplicate) versions.push_back(item(0));
    result.set_versions(std::move(versions));
    result.read_context = context;
    return QueryResult<CatalogSelection>::success(std::move(result));
  }
  std::uint64_t storage_namespace;
  std::shared_ptr<QueryDomainCache> cache;
  bool duplicate{false};
  std::shared_ptr<const QueryReadContext> context;
};
LogicalSchema schema() {
  return LogicalSchema::create({
    {"m.value", "Value", {LogicalTypeId::Int64, false, {}}, FieldRole::Measure, {}},
    {"m.tag", "Tag", {LogicalTypeId::Int64, false, {}}, FieldRole::Attribute, {}}}).value();
}
class Plan final : public AdapterExecutionPlan {};
class SourceHint final : public AdapterPlanningState {
public:
  explicit SourceHint(unsigned ordinal) : ordinal(ordinal) {}
  const unsigned ordinal;
};
class Index final : public AdapterDomainIndex {
public:
  std::uint64_t retained_bytes() const override {return 64;}
  QueryResult<std::optional<AdapterDomainSelection>> select(const QueryFragment& fragment, const QueryLimits& limits) const override {
    // Deterministic stand-ins for a real index's certification refusal/error.
    if (limits.max_ranges == 1) return QueryResult<std::optional<AdapterDomainSelection>>::success(std::nullopt);
    if (limits.max_ranges == 2) return QueryResult<std::optional<AdapterDomainSelection>>::failure(
      QueryStatus::failure(QueryError::ResourceExhausted, QueryPhase::Planning, "index limit failure"));
    if (fragment.predicate && std::get<std::int64_t>(std::get<ComparisonExpression>(fragment.predicate->node).value.value()) == 0)
      return QueryResult<std::optional<AdapterDomainSelection>>::success(AdapterDomainSelection{});
    return QueryResult<std::optional<AdapterDomainSelection>>::success(AdapterDomainSelection{{0, std::make_shared<const SourceHint>(0)},
      {2, std::make_shared<const SourceHint>(2)}});
  }
};
class Adapter final : public FormatQueryAdapter {
public:
  mutable unsigned preparations{0}, builds{0}, unhinted_preparations{0};
  mutable std::vector<unsigned> capability_hint_ordinals, preparation_hint_ordinals;
  const std::string& id() const override {static const std::string value = "domain-test"; return value;}
  bool supports_domain_index(const QueryFragment& fragment) const override {return bool(fragment.predicate);}
  QueryResult<std::shared_ptr<const AdapterDomainIndex>> build_domain_index(
      const std::vector<std::shared_ptr<const QuerySource>>& sources,
      const QueryFragment&, const QueryLimits&, std::uint64_t maximum) const override {
    ++builds; EXPECT_EQ(sources.size(), 3U);
    if (maximum < 64) return QueryResult<std::shared_ptr<const AdapterDomainIndex>>::success(nullptr);
    return QueryResult<std::shared_ptr<const AdapterDomainIndex>>::success(std::make_shared<const Index>());
  }
  QueryResult<QueryCapabilities> capabilities(const QueryPlanningContext& context, const QueryFragment&) const override {
    if (context.state()) {
      const auto hint = std::dynamic_pointer_cast<const SourceHint>(context.state());
      EXPECT_NE(hint, nullptr);
      if (hint) {
        EXPECT_EQ(context.source().manifest().key, item(hint->ordinal).key);
        capability_hint_ordinals.push_back(hint->ordinal);
      }
    }
    return QueryResult<QueryCapabilities>::success(QueryCapabilities(1, id(), "1", schema(), {
      {{"m.value"}, true, {}, PushdownLevel::None},
      {{"m.tag"}, true, {ComparisonOperator::Equal}, PushdownLevel::Candidate}},
      {}, true, false, OrderingGuarantee::Source));
  }
  QueryResult<AdapterPlan> prepare(const QueryPlanningContext& context, const QueryFragment& fragment) const override {
    ++preparations;
    if (context.state()) {
      const auto hint = std::dynamic_pointer_cast<const SourceHint>(context.state());
      EXPECT_NE(hint, nullptr);
      if (hint) {
        EXPECT_EQ(context.source().manifest().key, item(hint->ordinal).key);
        preparation_hint_ordinals.push_back(hint->ordinal);
      }
    } else ++unhinted_preparations;
    require_query_metadata("test-domain-fee/" + context.source().manifest().key.path, 10);
    return QueryResult<AdapterPlan>::success(AdapterPlan(std::make_shared<const Plan>(), schema(), nullptr, {}, fragment.projection));
  }
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(const QueryExecutionContext&,
      std::shared_ptr<const AdapterExecutionPlan>, const QueryExecutionOptions&) const override {
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(QueryStatus::failure(
      QueryError::UnsupportedCapability, QueryPhase::Execution, "planning-only fixture"));
  }
};
struct Fixture {
  std::shared_ptr<std::uint64_t> resident = std::make_shared<std::uint64_t>(0);
  std::shared_ptr<QueryDomainCache> cache = std::make_shared<QueryDomainCache>(
    [n = resident](std::uint64_t bytes) {if (bytes > (1ULL << 20) - *n) return false; *n += bytes; return true;},
    [n = resident](std::uint64_t bytes) {*n -= bytes;}, [n = resident] {return (1ULL << 20) - *n;});
  std::shared_ptr<Backend> backend = std::make_shared<Backend>();
  std::shared_ptr<StructuredBlobStorage> storage = std::make_shared<StructuredBlobStorage>(backend);
  std::shared_ptr<Catalog> catalog = std::make_shared<Catalog>(storage->storage_namespace(), cache);
  std::shared_ptr<Adapter> adapter = std::make_shared<Adapter>();
  std::shared_ptr<const QueryModule> module = QueryModule::create(catalog, storage, {adapter}).value();
  QueryRequest request() const {
    QueryRequest r; r.sources.paths = {{PathScopeKind::Subtree, "/domain"}};
    r.sources.formats = {AdapterSelectionKind::Listed, {"domain-test"}, false};
    r.sources.versions = VersionSelection::latest();
    r.subset = QueryFragment{}; r.subset->projection = {{"m.value"}};
    r.subset->predicate = make_comparison({"m.tag"}, ComparisonOperator::Equal, LogicalValue::int64(1));
    return r;
  }
};

// The common module only publishes immutable evidence and asks the owner to arm
// its optional summary. This mock has no permissions, mutable request ledger or
// format knowledge; production owner admission is covered by backend tests.
class MetadataBridgeContext final : public QueryReadContext,
    public std::enable_shared_from_this<MetadataBridgeContext> {
public:
  using Batch = std::shared_ptr<const PreparedQueryMetadataTrace>;
  mutable Batch fees;
  mutable std::shared_ptr<const PreparedQueryMetadataStageSummary> summary;
  mutable unsigned bindings{0}, arms{0};
  std::vector<QuerySourceReference> references;
  QueryResult<std::shared_ptr<const QueryReadContext>> select(
      const std::vector<StructuredBlobManifest>&) const override {
    return QueryResult<std::shared_ptr<const QueryReadContext>>::success(shared_from_this());
  }
  const std::vector<QuerySourceReference>& sources() const override { return references; }
  QueryResult<std::shared_ptr<QueryAdmission>> admit(const QueryPrincipal&) const override {
    return QueryResult<std::shared_ptr<QueryAdmission>>::failure(QueryStatus::failure(
      QueryError::UnsupportedCapability, QueryPhase::Catalog, "planning-only mock context"));
  }
  QueryResult<StructuredBlobManifest> manifest(const StructuredBlobVersionKey& key) const override {
    for (unsigned i = 0; i < 3; ++i) if (item(i).key == key)
      return QueryResult<StructuredBlobManifest>::success(item(i));
    return QueryResult<StructuredBlobManifest>::failure(QueryStatus::failure(
      QueryError::SourceNotFound, QueryPhase::Catalog, "missing fixture source"));
  }
  void bind_domain_metadata_fees(Batch batch) const noexcept override {
    ++bindings;
    if (!fees) fees = std::move(batch);
  }
  void arm_domain_metadata_summary() const noexcept override {
    ++arms;
    if (summary) (void)arm_query_metadata_stage_summary(summary);
  }
  bool compile_summary(const std::shared_ptr<std::uint64_t>& resident) const {
    struct Charge {
      std::shared_ptr<std::uint64_t> resident;
      std::uint64_t bytes{0};
      ~Charge() { *resident -= bytes; }
      void grow(std::uint64_t n) {
        if (n > (1ULL << 20) - *resident) throw std::bad_alloc();
        *resident += n; bytes += n;
      }
      void shrink(std::uint64_t n) { *resident -= n; bytes -= n; }
    };
    auto charge = std::make_shared<Charge>(); charge->resident = resident;
    summary = PreparedQueryMetadataStageSummary::compile({fees},
      [charge](std::uint64_t n) { charge->grow(n); },
      [charge](std::uint64_t n) { charge->shrink(n); }, charge);
    return bool(summary);
  }
};

std::shared_ptr<MetadataBridgeContext> bridge_for(Fixture& f) {
  auto context = std::make_shared<MetadataBridgeContext>();
  for (unsigned i = 0; i < 3; ++i)
    context->references.push_back({item(i).key.path, item(i).key.version, "domain-test"});
  f.catalog->context = context;
  return context;
}
}

TEST(QueryModuleDomainCache, FullCandidateColdPlanAndWarmIndexedPlanKeepSourceOrder) {
  Fixture f; const auto request = f.request();
  auto cold = f.module->prepare(request); ASSERT_TRUE(cold.ok());
  EXPECT_EQ(cold.value().sources().size(), 3U); EXPECT_EQ(f.adapter->builds, 1U);
  auto warm = f.module->prepare(request); ASSERT_TRUE(warm.ok());
  ASSERT_EQ(warm.value().sources().size(), 2U);
  EXPECT_EQ(warm.value().sources()[0].source->manifest().key, item(0).key);
  EXPECT_EQ(warm.value().sources()[1].source->manifest().key, item(2).key);
  EXPECT_EQ(f.adapter->preparations, 5U); EXPECT_EQ(f.adapter->builds, 1U);
  EXPECT_EQ(f.module->select(request).value().versions().size(), 3U);
}

TEST(QueryModuleDomainCache, OpaqueHintsBindExactSourceForCapabilitiesAndPrepareButNotEmptyProbe) {
  Fixture f; auto request = f.request();
  auto cold = f.module->prepare(request); ASSERT_TRUE(cold.ok());
  EXPECT_EQ(f.adapter->unhinted_preparations, 3U);
  EXPECT_TRUE(f.adapter->capability_hint_ordinals.empty());
  EXPECT_TRUE(f.adapter->preparation_hint_ordinals.empty());
  auto warm = f.module->prepare(request); ASSERT_TRUE(warm.ok());
  EXPECT_EQ(f.adapter->capability_hint_ordinals, (std::vector<unsigned>{0, 2}));
  EXPECT_EQ(f.adapter->preparation_hint_ordinals, (std::vector<unsigned>{0, 2}));
  EXPECT_EQ(f.adapter->unhinted_preparations, 3U);
  request.subset->predicate = make_comparison({"m.tag"}, ComparisonOperator::Equal,
    LogicalValue::int64(0));
  auto empty = f.module->prepare(request); ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty.value().sources().empty());
  EXPECT_EQ(f.adapter->unhinted_preparations, 4U);
  EXPECT_EQ(f.adapter->capability_hint_ordinals, (std::vector<unsigned>{0, 2}));
  EXPECT_EQ(f.adapter->preparation_hint_ordinals, (std::vector<unsigned>{0, 2}));
}

TEST(QueryModuleDomainCache, WarmBatchReplaysFullCandidateFeesIntoIndependentRequestLedger) {
  Fixture f; const auto request = f.request(); std::uint64_t cold_bytes = 0;
  {ScopedQueryMetadataBudget budget; auto value = f.module->prepare(request); ASSERT_TRUE(value.ok()); cold_bytes = budget.budget()->consumed_bytes();}
  EXPECT_GE(cold_bytes, 30U);
  {ScopedQueryMetadataBudget budget; auto value = f.module->prepare(request); ASSERT_TRUE(value.ok()); EXPECT_EQ(budget.budget()->consumed_bytes(), cold_bytes);}
}

TEST(QueryModuleDomainCache, UnsupportedFragmentStillUsesOriginalPlannerAndCandidateLimit) {
  Fixture f; auto request = f.request(); ASSERT_TRUE(f.module->prepare(request).ok());
  request.subset->predicate.reset();
  auto fallback = f.module->prepare(request); ASSERT_TRUE(fallback.ok());
  EXPECT_EQ(fallback.value().sources().size(), 3U); EXPECT_EQ(f.adapter->preparations, 6U);
  request.limits.max_sources = 2;
  auto bounded = f.module->prepare(request); ASSERT_FALSE(bounded.ok());
  EXPECT_EQ(bounded.status().code(), QueryError::ResourceExhausted);
}

TEST(QueryModuleDomainCache, WarmLowBudgetReturnsStickyFailureInsteadOfThrowingOrExemption) {
  Fixture f; const auto request = f.request(); ASSERT_TRUE(f.module->prepare(request).ok());
  ScopedQueryMetadataBudget budget;
  require_query_metadata("preexisting", QueryMetadataBudget::maximum_bytes - 5);
  std::optional<QueryResult<PreparedQuery>> failed;
  EXPECT_NO_THROW(failed.emplace(f.module->prepare(request)));
  ASSERT_TRUE(failed); ASSERT_FALSE(failed->ok());
  EXPECT_EQ(failed->status().code(), QueryError::ResourceExhausted);
  EXPECT_FALSE(budget.status().ok());
  auto again = f.module->prepare(request); ASSERT_FALSE(again.ok());
  EXPECT_EQ(again.status().code(), QueryError::ResourceExhausted);
}

TEST(QueryModuleDomainCache, EmptyIndexedMatchKeepsProjectedSchemaWithoutExecutableSources) {
  Fixture f; auto request = f.request(); ASSERT_TRUE(f.module->prepare(request).ok());
  request.subset->predicate = make_comparison({"m.tag"}, ComparisonOperator::Equal, LogicalValue::int64(0));
  auto empty = f.module->prepare(request); ASSERT_TRUE(empty.ok());
  EXPECT_TRUE(empty.value().sources().empty()); ASSERT_TRUE(empty.value().result_schema());
  ASSERT_EQ(empty.value().result_schema()->fields().size(), 1U);
  EXPECT_EQ(empty.value().result_schema()->fields().front().field_id, "m.value");
  EXPECT_EQ(f.adapter->preparations, 4U); // one discarded schema probe
}

TEST(QueryModuleDomainCache, WarmUnknownProjectionRetainsOriginalValidationFailure) {
  Fixture f; auto request = f.request(); ASSERT_TRUE(f.module->prepare(request).ok());
  request.subset->projection = {{"m.unknown"}};
  auto invalid = f.module->prepare(request); ASSERT_FALSE(invalid.ok());
  Fixture uncached;
  auto reference = uncached.module->prepare(request); ASSERT_FALSE(reference.ok());
  EXPECT_EQ(invalid.status().code(), reference.status().code());
  EXPECT_EQ(invalid.status().code(), QueryError::UnsupportedCapability);
}

TEST(QueryModuleDomainCache, FailedPreparedQueryCannotPublishAnOptionalDomainArtifact) {
  Fixture f; f.catalog->duplicate = true;
  auto invalid = f.module->prepare(f.request()); ASSERT_FALSE(invalid.ok());
  EXPECT_EQ(invalid.status().code(), QueryError::Internal); EXPECT_EQ(f.adapter->builds, 0U);
  auto claim = f.cache->claim(); EXPECT_TRUE(claim->building()); EXPECT_FALSE(claim->value);
  claim.reset(); EXPECT_EQ(*f.resident, 0U);
  f.catalog->duplicate = false; ASSERT_TRUE(f.module->prepare(f.request()).ok());
  EXPECT_EQ(f.adapter->builds, 1U);
}

TEST(QueryModuleDomainCache, SameAdapterIdReplacementCannotReuseAnotherInstanceDomain) {
  Fixture f; ASSERT_TRUE(f.module->prepare(f.request()).ok());
  auto replacement = std::make_shared<Adapter>();
  auto module = QueryModule::create(f.catalog, f.storage, {replacement}); ASSERT_TRUE(module.ok());
  auto value = module.value()->prepare(f.request()); ASSERT_TRUE(value.ok());
  EXPECT_EQ(value.value().sources().size(), 3U);
  EXPECT_EQ(replacement->preparations, 3U); EXPECT_EQ(replacement->builds, 0U);
}

TEST(QueryModuleDomainCache, PublishedBorrowedBatchArmsSummaryAtTheActualWarmReplayCallsite) {
  Fixture f; auto context = bridge_for(f); const auto request = f.request();
  std::uint64_t reference_bytes = 0;
  { ScopedQueryMetadataBudget budget;
    auto cold = f.module->prepare(request); ASSERT_TRUE(cold.ok());
    reference_bytes = budget.budget()->consumed_bytes(); }
  ASSERT_EQ(context->bindings, 1U); ASSERT_TRUE(context->fees);
  EXPECT_EQ(context->arms, 0U); ASSERT_TRUE(context->compile_summary(f.resident));
  const auto* batch_identity = context->fees.get();
  { ScopedQueryMetadataBudget budget;
    auto warm = f.module->prepare(request); ASSERT_TRUE(warm.ok());
    EXPECT_EQ(warm.value().sources().size(), 2U);
    EXPECT_EQ(budget.budget()->consumed_bytes(), reference_bytes);
    const auto stats = budget.budget()->stage_summary_stats();
    EXPECT_EQ(stats.full_fee_entries_visited, 0U);
    EXPECT_EQ(stats.borrowed_fee_entries_skipped, context->fees->fees().size());
    EXPECT_EQ(stats.successful_stages, 1U); }
  EXPECT_EQ(context->arms, 1U); EXPECT_EQ(context->bindings, 1U);
  EXPECT_EQ(context->fees.get(), batch_identity);
}

TEST(QueryModuleDomainCache, IndexCertificationMissAndLimitErrorDoNotArmOrBindAnotherBatch) {
  Fixture f; auto context = bridge_for(f); auto request = f.request();
  ASSERT_TRUE(f.module->prepare(request).ok()); ASSERT_TRUE(context->compile_summary(f.resident));
  const auto* batch = context->fees.get();
  request.limits.max_ranges = 1;
  auto fallback = f.module->prepare(request); ASSERT_TRUE(fallback.ok());
  EXPECT_EQ(fallback.value().sources().size(), 3U);
  EXPECT_EQ(context->arms, 0U); EXPECT_EQ(context->bindings, 1U);
  request.limits.max_ranges = 2;
  auto failed = f.module->prepare(request); ASSERT_FALSE(failed.ok());
  EXPECT_EQ(failed.status().message(), "index limit failure");
  EXPECT_EQ(context->arms, 0U); EXPECT_EQ(context->bindings, 1U);
  EXPECT_EQ(context->fees.get(), batch);
}

TEST(QueryModuleDomainCache, ActiveCollectorStillReceivesEveryOriginalBorrowedFee) {
  Fixture f; auto context = bridge_for(f); const auto request = f.request();
  ASSERT_TRUE(f.module->prepare(request).ok()); ASSERT_TRUE(context->compile_summary(f.resident));
  ScopedQueryMetadataBudget budget;
  ScopedQueryMetadataTrace collector("");
  auto warm = f.module->prepare(request); ASSERT_TRUE(warm.ok());
  const auto captured = collector.release();
  for (const auto& fee : context->fees->fees()) {
    const auto found = captured.find(fee.first); ASSERT_NE(found, captured.end());
    EXPECT_GE(found->second, fee.second);
  }
  EXPECT_EQ(context->arms, 1U);
  EXPECT_EQ(budget.budget()->stage_summary_stats().successful_stages, 0U);
  EXPECT_EQ(budget.budget()->stage_summary_stats().borrowed_fee_entries_skipped, 0U);
}

TEST(QueryModuleDomainCache, LowBudgetSummaryRefusalRetainsOriginalErrorAndSuccessfulPrefix) {
  Fixture f; auto context = bridge_for(f); const auto request = f.request();
  ASSERT_TRUE(f.module->prepare(request).ok()); ASSERT_TRUE(context->compile_summary(f.resident));
  QueryStatus reference_status; std::uint64_t reference_consumed = 0;
  const auto saved = context->summary; context->summary.reset();
  { ScopedQueryMetadataBudget budget;
    require_query_metadata("preexisting", QueryMetadataBudget::maximum_bytes - 5);
    auto original = f.module->prepare(request); ASSERT_FALSE(original.ok());
    reference_status = original.status(); reference_consumed = budget.budget()->consumed_bytes(); }
  context->summary = saved;
  { ScopedQueryMetadataBudget budget;
    require_query_metadata("preexisting", QueryMetadataBudget::maximum_bytes - 5);
    auto summarized = f.module->prepare(request); ASSERT_FALSE(summarized.ok());
    EXPECT_EQ(summarized.status().code(), reference_status.code());
    EXPECT_EQ(summarized.status().message(), reference_status.message());
    EXPECT_EQ(budget.budget()->consumed_bytes(), reference_consumed);
    EXPECT_FALSE(budget.status().ok());
    EXPECT_GT(budget.budget()->stage_summary_stats().full_fee_entries_visited, 0U); }
}

TEST(QueryModuleDomainCache, BoundBatchKeepsItsOriginalReservationAfterDomainRetirement) {
  std::shared_ptr<MetadataBridgeContext> context;
  std::shared_ptr<std::uint64_t> resident;
  std::weak_ptr<QueryDomainCache> weak_cache;
  {
    Fixture f; context = bridge_for(f); resident = f.resident; weak_cache = f.cache;
    ASSERT_TRUE(f.module->prepare(f.request()).ok()); ASSERT_TRUE(context->fees);
    EXPECT_GT(*resident, 0U);
  }
  EXPECT_TRUE(weak_cache.expired());
  EXPECT_GT(*resident, 0U); // Only the borrowed batch's existing allocation pin remains.
  context->fees.reset(); EXPECT_EQ(*resident, 0U);
}
