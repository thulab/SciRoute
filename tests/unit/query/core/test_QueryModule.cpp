#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/frontend/TypedQueryService.h"
#include "iotdbfs/query/frontend/FuseTypedQueryBridge.h"
#include "iotdbfs/formats/grib/GribFormatQueryAdapter.h"
#include "iotdbfs/formats/grib/GribLayout.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>

#include <gtest/gtest.h>

#include <map>
#include <cerrno>
#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <thread>
#include <memory>
#include <utility>

using namespace iotdbfs;

namespace {

LogicalType type(LogicalTypeId id, bool nullable = false) {
  return {id, nullable, std::nullopt};
}

LogicalSchema adapter_schema() {
  auto schema = LogicalSchema::create({
    {"domain.time", "Time",
      {LogicalTypeId::TimestampNs, false, "UTC"},
      FieldRole::Dimension, "ns"},
    {"data.value", "Value", type(LogicalTypeId::Float64, true),
      FieldRole::Measure, "K"},
    {"data.quality", "Quality", type(LogicalTypeId::Int32),
      FieldRole::Attribute, std::nullopt},
  });
  EXPECT_TRUE(schema.ok()) << schema.status().message();
  return schema.value();
}

StructuredBlobManifest manifest(std::string path, std::int64_t version,
    std::optional<std::string> format_id = std::string("test-format"),
    std::string station = "A") {
  StructuredBlobManifest result;
  result.key = {std::move(path), version};
  result.format_id = std::move(format_id);
  if (result.format_id)
    result.metadata = {{"station", MetadataValue(std::move(station))}};
  result.payload_size = 4;
  result.layout_size = result.format_id ? 2 : 0;
  result.phase = StructuredBlobPhase::Published;
  result.published = true;
  result.revision = 5;
  return result;
}

class ModuleBackend final : public StructuredBlobStorageBackend {
public:
  mutable std::size_t manifest_reads{0};
  mutable std::size_t payload_reads{0};
  mutable std::size_t layout_reads{0};
  bool fail_reads{false};

  explicit ModuleBackend(std::vector<StructuredBlobManifest> manifests,
      StructuredBlobBytes payload = {1, 2, 3, 4},
      StructuredBlobBytes layout = {8, 9}) {
    for (auto& item : manifests) {
      auto key = item.key;
      records_.emplace(std::move(key),
        Record{std::move(item), payload, layout});
    }
  }

  StructuredBlobStatus create_manifest(
      const StructuredBlobManifest&) override {
    return unsupported();
  }
  StructuredBlobResult<StructuredBlobManifest> load_manifest(
      const StructuredBlobVersionKey& key) const override {
    ++manifest_reads;
    const auto found = records_.find(key);
    if (found == records_.end())
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::NotFound, "not found");
    return StructuredBlobResult<StructuredBlobManifest>::success(
      found->second.manifest);
  }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&,
      std::uint64_t, const StructuredBlobManifest&) override {
    return unsupported();
  }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&,
      std::uint64_t, const StructuredBlobBytes&) override {
    return unsupported();
  }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,
      const StructuredBlobBytes&) override {
    return unsupported();
  }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,
      const MetadataMap&) override {
    return unsupported();
  }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
      const StructuredBlobVersionKey& key, std::uint64_t offset,
      std::uint64_t length) const override {
    ++payload_reads;
    if (fail_reads)
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::BackendFailure, "injected source failure");
    const auto found = records_.find(key);
    if (found == records_.end())
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    const auto begin = found->second.payload.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      StructuredBlobBytes(begin, begin + static_cast<std::size_t>(length)));
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      const StructuredBlobVersionKey& key, std::uint64_t offset,
      std::uint64_t length) const override {
    ++layout_reads;
    if (fail_reads)
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::BackendFailure, "injected source failure");
    const auto found = records_.find(key);
    if (found == records_.end())
      return StructuredBlobResult<StructuredBlobBytes>::failure(
        StructuredBlobError::NotFound, "not found");
    const auto begin = found->second.layout.begin() +
      static_cast<std::size_t>(offset);
    return StructuredBlobResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&,
      std::uint64_t) override {
    return unsupported();
  }

private:
  struct Record {
    StructuredBlobManifest manifest;
    StructuredBlobBytes payload;
    StructuredBlobBytes layout;
  };

  static StructuredBlobStatus unsupported() {
    return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,
      "read-only test backend");
  }

  std::map<StructuredBlobVersionKey, Record> records_;
};

class ModuleExecutionPlan final : public AdapterExecutionPlan {};

class ModuleCursor final : public QueryBatchCursor {
public:
  ModuleCursor(const QuerySource& source, bool read_payload)
    : source_(source), schema_(adapter_schema()),
      read_payload_(read_payload) {}

  const LogicalSchema& schema() const override { return schema_; }

  QueryResult<std::optional<QueryBatch>> next() override {
    if (cancelled_)
      return QueryResult<std::optional<QueryBatch>>::failure(
        QueryStatus::failure(QueryError::Cancelled, QueryPhase::Execution,
          "adapter cursor cancelled"));
    if (done_)
      return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
    done_ = true;
    if (read_payload_) {
      auto bytes = source_.read_payload(0, 4);
      if (!bytes.ok())
        return QueryResult<std::optional<QueryBatch>>::failure(bytes.status());
    }
    std::vector<LogicalArray> columns{
      {{LogicalTypeId::TimestampNs, false, "UTC"},
        std::vector<TimestampNs>{{100}, {150}, {250}, {175}}, {}},
      {type(LogicalTypeId::Float64, true),
        std::vector<double>{10.0, 20.0, 30.0, 40.0}, {}},
      {type(LogicalTypeId::Int32),
        std::vector<std::int32_t>{1, 0, 1, 1}, {}},
    };
    DomainDescriptor domain{DomainKind::TimeSeries,
      {{{"domain.time"}, {LogicalTypeId::TimestampNs, false, "UTC"}, "ns"}},
      {4}, {0}};
    auto batch = QueryBatch::create(schema_, 4, std::move(columns), domain);
    if (!batch.ok())
      return QueryResult<std::optional<QueryBatch>>::failure(batch.status());
    return QueryResult<std::optional<QueryBatch>>::success(
      std::move(batch.value()));
  }

  void cancel() override { cancelled_ = true; }

private:
  const QuerySource& source_;
  LogicalSchema schema_;
  bool read_payload_{false};
  bool done_{false};
  bool cancelled_{false};
};

// Each pair is (layout, byte count); reads always begin at offset zero.
using PlanningReads = std::vector<std::pair<bool, std::uint64_t>>;

QueryStatus perform_reads(const QuerySource& source, const PlanningReads& reads) {
  for (const auto& read : reads) {
    auto result = read.first ? source.read_layout(0, read.second)
                             : source.read_payload(0, read.second);
    if (!result.ok()) return result.status();
  }
  return QueryStatus::success();
}

class ModulePlanningState final : public AdapterPlanningState {};

struct PlanningObservation {
  const QueryPlanningContext* active_context{nullptr};
  std::vector<QueryLimits> capability_limits;
  std::vector<QueryLimits> prepare_limits;
  std::vector<std::weak_ptr<const AdapterPlanningState>> states;
};

class ModuleAdapter final : public FormatQueryAdapter {
public:
  explicit ModuleAdapter(bool read_payload = false,
      std::shared_ptr<std::optional<PhysicalArraySubject>> observed = nullptr,
      PlanningReads capability_reads = {}, PlanningReads prepare_reads = {},
      std::shared_ptr<PlanningObservation> planning_observed = nullptr)
    : read_payload_(read_payload), observed_(std::move(observed)),
      capability_reads_(std::move(capability_reads)),
      prepare_reads_(std::move(prepare_reads)),
      planning_observed_(std::move(planning_observed)) {}

  const std::string& id() const override { return id_; }

  QueryResult<QueryCapabilities> capabilities(
      const QueryPlanningContext& context, const QueryFragment& fragment) const override {
    const auto& source = context.source();
    if (planning_observed_) {
      EXPECT_FALSE(context.state());
      planning_observed_->active_context = &context;
      planning_observed_->capability_limits.push_back(context.limits());
      context.set_state(std::make_shared<const ModulePlanningState>());
      planning_observed_->states.emplace_back(context.state());
    }
    auto read = perform_reads(source, capability_reads_);
    if (!read.ok()) return QueryResult<QueryCapabilities>::failure(read);
    if (observed_) *observed_ = fragment.physical_array;
    std::vector<FieldQueryCapability> fields{
      {{"domain.time"}, true, {}, PushdownLevel::None,
        true, PushdownLevel::Candidate, {}, false, false, {}},
      {{"data.value"}, true, {}, PushdownLevel::None,
        false, PushdownLevel::None, {}, false, false, {}},
      {{"data.quality"}, true, {ComparisonOperator::Equal},
        PushdownLevel::None, false, PushdownLevel::None,
        {}, false, false, {}},
    };
    return QueryResult<QueryCapabilities>::success(QueryCapabilities(
      1, id_, "test-1", adapter_schema(), std::move(fields),
      {DomainKind::TimeSeries}, true, false,
      OrderingGuarantee::Domain));
  }

  QueryResult<AdapterPlan> prepare(const QueryPlanningContext& context,
      const QueryFragment& fragment) const override {
    const auto& source = context.source();
    if (planning_observed_) {
      EXPECT_EQ(planning_observed_->active_context, &context);
      EXPECT_EQ(planning_observed_->states.back().lock(), context.state());
      EXPECT_NE(std::dynamic_pointer_cast<const ModulePlanningState>(
        context.state()), nullptr);
      planning_observed_->prepare_limits.push_back(context.limits());
      planning_observed_->active_context = nullptr;
    }
    auto read = perform_reads(source, prepare_reads_);
    if (!read.ok()) return QueryResult<AdapterPlan>::failure(read);
    return QueryResult<AdapterPlan>::success(AdapterPlan(
      std::make_shared<const ModuleExecutionPlan>(), adapter_schema(),
      fragment.predicate, fragment.slices, fragment.projection));
  }

  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(
      const QueryExecutionContext& context,
      std::shared_ptr<const AdapterExecutionPlan>,
      const QueryExecutionOptions&) const override {
    EXPECT_NE(context.source.get(), context.shared_source.get());
    EXPECT_EQ(context.source->storage_namespace(),
      context.shared_source->storage_namespace());
    std::unique_ptr<QueryBatchCursor> cursor =
      std::make_unique<ModuleCursor>(*context.source, read_payload_);
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(
      std::move(cursor));
  }

private:
  std::string id_{"test-format"};
  bool read_payload_{false};
  std::shared_ptr<std::optional<PhysicalArraySubject>> observed_;
  PlanningReads capability_reads_;
  PlanningReads prepare_reads_;
  std::shared_ptr<PlanningObservation> planning_observed_;
};

struct PassThroughObservation {
  std::atomic<const double*> values_data{nullptr};
  std::atomic<int> decoded_batches{0};
};

LogicalSchema pass_through_schema() {
  auto schema = LogicalSchema::create({
    {"data.value", "Value", type(LogicalTypeId::Float64),
      FieldRole::Measure, "K"},
  });
  EXPECT_TRUE(schema.ok()) << schema.status().message();
  return schema.value();
}

class PassThroughCursor final : public QueryBatchCursor {
public:
  PassThroughCursor(const QuerySource& source,
      std::shared_ptr<PassThroughObservation> observation)
    : source_(source), observation_(std::move(observation)),
      schema_(pass_through_schema()) {}

  const LogicalSchema& schema() const override { return schema_; }

  QueryResult<std::optional<QueryBatch>> next() override {
    if (done_)
      return QueryResult<std::optional<QueryBatch>>::success(std::nullopt);
    done_ = true;
    ++observation_->decoded_batches;
    std::vector<double> values{10.0, 20.0, 30.0};
    std::vector<LogicalArray> columns{
      {type(LogicalTypeId::Float64), std::move(values), {}},
    };
    const auto& manifest = source_.manifest();
    auto batch = QueryBatch::create(schema_, 3, std::move(columns),
      std::nullopt, {{manifest.key.path, manifest.key.version,
        manifest.format_id.value_or("")}});
    if (!batch.ok())
      return QueryResult<std::optional<QueryBatch>>::failure(batch.status());
    observation_->values_data = std::get<std::vector<double>>(
      batch.value().columns()[0].values).data();
    return QueryResult<std::optional<QueryBatch>>::success(
      std::move(batch.value()));
  }

  void cancel() override {}

private:
  const QuerySource& source_;
  std::shared_ptr<PassThroughObservation> observation_;
  LogicalSchema schema_;
  bool done_{false};
};

class PassThroughAdapter final : public FormatQueryAdapter {
public:
  explicit PassThroughAdapter(
      std::shared_ptr<PassThroughObservation> observation)
    : observation_(std::move(observation)) {}

  const std::string& id() const override { return id_; }

  QueryResult<QueryCapabilities> capabilities(
      const QueryPlanningContext&, const QueryFragment&) const override {
    return QueryResult<QueryCapabilities>::success(QueryCapabilities(
      1, id_, "test-1", pass_through_schema(),
      {{{"data.value"}, true, {}, PushdownLevel::None,
        false, PushdownLevel::None, {}, false, false, {}}},
      {DomainKind::Rows}, true, false, OrderingGuarantee::None));
  }

  QueryResult<AdapterPlan> prepare(const QueryPlanningContext&,
      const QueryFragment& fragment) const override {
    return QueryResult<AdapterPlan>::success(AdapterPlan(
      std::make_shared<const ModuleExecutionPlan>(), pass_through_schema(),
      nullptr, {}, fragment.projection));
  }

  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(
      const QueryExecutionContext& context,
      std::shared_ptr<const AdapterExecutionPlan>,
      const QueryExecutionOptions&) const override {
    EXPECT_NE(context.source.get(), context.shared_source.get());
    EXPECT_EQ(context.source->storage_namespace(),
      context.shared_source->storage_namespace());
    std::unique_ptr<QueryBatchCursor> cursor =
      std::make_unique<PassThroughCursor>(*context.source, observation_);
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(
      std::move(cursor));
  }

private:
  std::string id_{"pass-through-format"};
  std::shared_ptr<PassThroughObservation> observation_;
};

QueryRequest file_request(QueryExpressionPtr predicate = nullptr) {
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Subtree, "/science"}};
  request.sources.versions = VersionSelection::latest();
  request.sources.formats.adapters = AdapterSelectionKind::Listed;
  request.sources.formats.adapter_ids = {"test-format"};
  request.sources.formats.include_byte_passthrough = false;
  request.sources.file_predicate = std::move(predicate);
  return request;
}

QueryRequest semantic_request() {
  auto request = file_request();
  request.subset = QueryFragment{
    {{"data.value"}},
    make_comparison({"data.quality"}, ComparisonOperator::Equal,
      LogicalValue::int32(1)),
    {{{"domain.time"},
      DomainBound{LogicalValue::timestamp_ns(100), BoundKind::Inclusive},
      DomainBound{LogicalValue::timestamp_ns(200), BoundKind::Exclusive}}},
    {}};
  return request;
}

struct ModuleFixture {
  StructuredBlobManifest a = manifest("/science/a.tsfile", 7);
  StructuredBlobManifest old_a = manifest("/science/a.tsfile", 5);
  StructuredBlobManifest b = manifest("/science/b.tsfile", 6,
    std::string("test-format"), "B");
  std::shared_ptr<ModuleBackend> backend =
    std::make_shared<ModuleBackend>(std::vector<StructuredBlobManifest>{a,
      old_a, b});
  std::shared_ptr<StructuredBlobStorage> storage =
    std::make_shared<StructuredBlobStorage>(backend);
  std::shared_ptr<const PublishedVersionCatalog> catalog =
    make_in_memory_published_version_catalog({a, old_a, b});
};

class EmptySelectionCatalog final : public PublishedVersionCatalog {
public:
  QueryResult<CatalogSelection> scan(const SourceSelector&,
      std::size_t) const override {
    return QueryResult<CatalogSelection>::success(CatalogSelection{{}, nullptr});
  }
};

} // namespace

TEST(QueryModule, PrepareFixesLatestAndExecuteStreamsFileSelection) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>();
  auto module = QueryModule::create(fixture.catalog, fixture.storage,
    {adapter});
  ASSERT_TRUE(module.ok()) << module.status().message();
  auto predicate = make_comparison({"sigma.station"},
    ComparisonOperator::Equal, LogicalValue::utf8("A"));

  auto prepared = module.value()->prepare(file_request(predicate));
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_EQ(prepared.value().sources().size(), 1u);
  EXPECT_EQ(prepared.value().sources()[0].source->manifest().key.version, 7);
  ASSERT_TRUE(prepared.value().result_schema());

  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  EXPECT_EQ(batch.value()->row_count(), 1u);
  EXPECT_EQ(std::get<std::vector<std::string>>(
    batch.value()->columns()[0].values)[0], "/science/a.tsfile");
  EXPECT_EQ(std::get<std::vector<std::int64_t>>(
    batch.value()->columns()[1].values)[0], 7);
  auto end = cursor.value()->next();
  ASSERT_TRUE(end.ok());
  EXPECT_FALSE(end.value());
}

TEST(QueryModule, PassesPhysicalArraySubjectBeforeCapabilities) {
  ModuleFixture fixture;
  auto observed =
    std::make_shared<std::optional<PhysicalArraySubject>>();
  auto adapter = std::make_shared<ModuleAdapter>(false, observed);
  auto module = QueryModule::create(fixture.catalog, fixture.storage,
    {adapter});
  ASSERT_TRUE(module.ok());

  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.sources.versions = VersionSelection::exact_version(7);
  request.subset->physical_array = PhysicalArraySubject{
    "target", 1, 3, {{{2}}, {{0}}}};
  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_TRUE(observed->has_value());
  EXPECT_EQ(observed->value().field_id, "target");
  EXPECT_EQ(observed->value().record_batch_index, 1u);
  EXPECT_EQ(observed->value().row_index, 3u);
  ASSERT_EQ(observed->value().values.size(), 2u);
  EXPECT_EQ(observed->value().values[0].child_indices,
    (std::vector<std::uint64_t>{2}));

  request.sources.versions = VersionSelection::exact_version(99);
  EXPECT_EQ(module.value()->prepare(request).status().code(),
    QueryError::SourceNotFound);
}

TEST(QueryModule, PhysicalArrayExactSourceFilteredByMetadataIsEmpty) {
  ModuleFixture fixture;
  auto module = QueryModule::create(fixture.catalog, fixture.storage,
    {std::make_shared<ModuleAdapter>()});
  ASSERT_TRUE(module.ok());

  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.sources.versions = VersionSelection::exact_version(7);
  request.sources.file_predicate = make_comparison({"sigma.station"},
    ComparisonOperator::Equal, LogicalValue::utf8("B"));
  request.subset->physical_array = PhysicalArraySubject{
    "target", 1, 3, {{{2}}}};

  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_TRUE(prepared.value().sources().empty());
  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  EXPECT_FALSE(batch.value());

  auto file_only = request;
  file_only.subset.reset();
  auto ordinary = module.value()->prepare(file_only);
  ASSERT_TRUE(ordinary.ok()) << ordinary.status().message();
  EXPECT_TRUE(ordinary.value().sources().empty());

  request.sources.file_predicate = nullptr;
  request.sources.formats.adapter_ids = {"other-format"};
  auto format_excluded = module.value()->prepare(request);
  ASSERT_TRUE(format_excluded.ok()) << format_excluded.status().message();
  EXPECT_TRUE(format_excluded.value().sources().empty());

  request.sources.file_predicate = make_comparison({"sigma.station"},
    ComparisonOperator::Equal, LogicalValue::utf8("B"));

  // A catalog may have pushed down the entire predicate and returned no
  // version at all. Existence must still be checked independently.
  auto filtered_catalog = std::make_shared<EmptySelectionCatalog>();
  auto pushed_down = QueryModule::create(filtered_catalog, fixture.storage,
    {std::make_shared<ModuleAdapter>()});
  ASSERT_TRUE(pushed_down.ok());
  auto filtered = pushed_down.value()->prepare(request);
  ASSERT_TRUE(filtered.ok()) << filtered.status().message();
  EXPECT_TRUE(filtered.value().sources().empty());

  request.sources.versions = VersionSelection::exact_version(99);
  EXPECT_EQ(pushed_down.value()->prepare(request).status().code(),
    QueryError::SourceNotFound);
}

TEST(QueryModule, NewPrepareRefreshesLatestWhilePreparedQueryKeepsSnapshot) {
  auto version_7 = manifest("/science/a.tsfile", 7);
  auto version_8 = manifest("/science/a.tsfile", 8);
  auto visible = std::make_shared<std::vector<StructuredBlobManifest>>(
    std::initializer_list<StructuredBlobManifest>{version_7});
  auto backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{version_7, version_8});
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto catalog = make_refreshing_published_version_catalog(
    [visible](const SourceSelector&) {
      return QueryResult<PublishedVersionSnapshot>::success(
        PublishedVersionSnapshot{*visible, {}});
    });
  auto module = QueryModule::create(catalog, storage,
    {std::make_shared<ModuleAdapter>()});
  ASSERT_TRUE(module.ok()) << module.status().message();

  auto old_query = module.value()->prepare(file_request());
  ASSERT_TRUE(old_query.ok()) << old_query.status().message();
  ASSERT_EQ(old_query.value().sources().size(), 1u);
  EXPECT_EQ(old_query.value().sources()[0].source->manifest().key.version, 7);

  visible->push_back(version_8);
  auto new_query = module.value()->prepare(file_request());
  ASSERT_TRUE(new_query.ok()) << new_query.status().message();
  ASSERT_EQ(new_query.value().sources().size(), 1u);
  EXPECT_EQ(new_query.value().sources()[0].source->manifest().key.version, 8);

  auto old_cursor = module.value()->execute(old_query.value());
  ASSERT_TRUE(old_cursor.ok()) << old_cursor.status().message();
  auto old_batch = old_cursor.value()->next();
  ASSERT_TRUE(old_batch.ok()) << old_batch.status().message();
  ASSERT_TRUE(old_batch.value());
  ASSERT_EQ(old_batch.value()->sources().size(), 1u);
  EXPECT_EQ(old_batch.value()->sources()[0].version, 7);
}

TEST(QueryModule, CollectionSummariesStayOutsideScalarPredicateSchema) {
  auto grib = manifest("/science/a.grib2", 7);
  grib.metadata = {
    {"shortName", MetadataValue(MetadataStringSet{"t", "u"})},
    {"levelMin", MetadataValue(std::int64_t(500))},
  };
  auto backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{grib});
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto catalog = make_in_memory_published_version_catalog({grib});
  auto module = QueryModule::create(catalog, storage,
    {std::make_shared<ModuleAdapter>()});
  ASSERT_TRUE(module.ok());

  auto scalar = file_request(make_comparison({"sigma.levelMin"},
    ComparisonOperator::Equal, LogicalValue::int64(500)));
  auto prepared = module.value()->prepare(scalar);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_EQ(prepared.value().sources().size(), 1u);

  auto collection = file_request(make_comparison({"sigma.shortName"},
    ComparisonOperator::Equal, LogicalValue::utf8("t")));
  EXPECT_EQ(module.value()->prepare(collection).status().code(),
    QueryError::UnknownField);
}

TEST(QueryModule, ExecuteAppliesResidualSlicePredicateAndProjection) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>();
  auto module = QueryModule::create(fixture.catalog, fixture.storage,
    {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};

  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_EQ(prepared.value().sources().size(), 1u);
  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  ASSERT_EQ(cursor.value()->schema().fields().size(), 1u);
  EXPECT_EQ(cursor.value()->schema().fields()[0].field_id, "data.value");

  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  EXPECT_EQ(batch.value()->row_count(), 2u);
  EXPECT_EQ(std::get<std::vector<double>>(
    batch.value()->columns()[0].values), (std::vector<double>{10.0, 40.0}));
  EXPECT_FALSE(batch.value()->domain());
  ASSERT_EQ(batch.value()->sources().size(), 1u);
  EXPECT_EQ(batch.value()->sources()[0].path, "/science/a.tsfile");
}

TEST(QueryModule, ExactAdapterBatchPassesThroughWithoutCopyingColumns) {
  auto source_manifest = manifest("/science/exact.bin", 9,
    std::string("pass-through-format"));
  auto backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{source_manifest});
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto catalog = make_in_memory_published_version_catalog({source_manifest});
  auto observation = std::make_shared<PassThroughObservation>();
  auto module = QueryModule::create(catalog, storage,
    {std::make_shared<PassThroughAdapter>(observation)});
  ASSERT_TRUE(module.ok()) << module.status().message();

  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, "/science/exact.bin"}};
  request.sources.versions = VersionSelection::latest();
  request.sources.formats.adapters = AdapterSelectionKind::Listed;
  request.sources.formats.adapter_ids = {"pass-through-format"};
  request.sources.formats.include_byte_passthrough = false;
  request.subset = QueryFragment{{{"data.value"}}, nullptr, {}, {}};

  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  const auto& values = std::get<std::vector<double>>(
    batch.value()->columns()[0].values);
  ASSERT_NE(observation->values_data.load(), nullptr);
  EXPECT_EQ(values.data(), observation->values_data.load());
}

TEST(QueryModule, RegisteredTypedRoutePublishesCompleteIpc) {
  auto source_manifest = manifest("/science/exact.bin", 9,
    std::string("pass-through-format"));
  auto backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{source_manifest});
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto catalog = make_in_memory_published_version_catalog({source_manifest});
  auto observation = std::make_shared<PassThroughObservation>();
  auto query = QueryModule::create(catalog, storage,
    {std::make_shared<PassThroughAdapter>(observation)});
  ASSERT_TRUE(query.ok()) << query.status().message();
  auto frontend = std::make_shared<TypedQueryFrontend>();
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, "/science/exact.bin"}};
  request.sources.versions = VersionSelection::exact_version(9);
  request.sources.formats.adapters = AdapterSelectionKind::Listed;
  request.sources.formats.adapter_ids = {"pass-through-format"};
  request.sources.formats.include_byte_passthrough = false;
  request.subset = QueryFragment{{{"data.value"}}, nullptr, {}, {}};
  auto route = frontend->register_typed(std::move(request));
  ASSERT_TRUE(route.ok()) << route.status().message();
  TypedResultStore::Config constrained;
  constrained.max_result_bytes = 256;
  auto small_store = std::make_shared<TypedResultStore>(constrained);
  TypedQueryService limited(query.value(), frontend, small_store, 1 << 20);
  auto oversized = limited.materialize(route.value());
  ASSERT_FALSE(oversized.ok());
  EXPECT_EQ(oversized.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(oversized.status().phase(), QueryPhase::Encoding);
  EXPECT_EQ(small_store->stats().registered_entries, 0u);
  TypedQueryService service(query.value(), frontend, results, 1 << 20);
  auto result_id = service.materialize(route.value());
  ASSERT_TRUE(result_id.ok()) << result_id.status().message();
  auto handle = service.open_result(result_id.value());
  ASSERT_TRUE(handle.ok()) << handle.status().message();
  auto bytes = handle.value().read(0, handle.value().size());
  ASSERT_TRUE(bytes.ok()) << bytes.status().message();
  auto buffer = std::make_shared<arrow::Buffer>(bytes.value().data(),
    bytes.value().size());
  auto stream = arrow::ipc::RecordBatchStreamReader::Open(
    std::make_shared<arrow::io::BufferReader>(buffer));
  ASSERT_TRUE(stream.ok()) << stream.status().ToString();
  EXPECT_EQ(stream.ValueOrDie()->schema()->metadata()->Get(
    "iotdbfs.protocol").ValueOr(""), "iotdbfs-query-batch-ipc/1");
  auto batch = stream.ValueOrDie()->ReadNext();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_NE(batch->batch, nullptr);
  auto values = std::static_pointer_cast<arrow::DoubleArray>(batch->batch->column(0));
  ASSERT_EQ(values->length(), 3);
  EXPECT_DOUBLE_EQ(values->Value(0), 10.0);
  EXPECT_DOUBLE_EQ(values->Value(1), 20.0);
  EXPECT_DOUBLE_EQ(values->Value(2), 30.0);
  ASSERT_NE(batch->custom_metadata, nullptr);
  EXPECT_EQ(batch->custom_metadata->Get("iotdbfs.source.0.path").ValueOr(""),
    "/science/exact.bin");
  EXPECT_EQ(batch->custom_metadata->Get("iotdbfs.source.0.version").ValueOr(""),
    "9");
}

TEST(QueryModule, FuseBridgePinsStatBytesAcrossOpensAndReauthorizes) {
  auto source_manifest = manifest("/science/exact.bin", 9,
    std::string("pass-through-format"));
  auto backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{source_manifest});
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto catalog = make_in_memory_published_version_catalog({source_manifest});
  auto observation = std::make_shared<PassThroughObservation>();
  auto query = QueryModule::create(catalog, storage,
    {std::make_shared<PassThroughAdapter>(observation)});
  ASSERT_TRUE(query.ok()) << query.status().message();
  auto frontend = std::make_shared<TypedQueryFrontend>();
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto service = std::make_shared<TypedQueryService>(query.value(), frontend,
    results, 1 << 20);
  FuseTypedQueryBridge bridge(frontend, service);
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, "/science/exact.bin"}};
  request.sources.versions = VersionSelection::exact_version(9);
  request.sources.formats.adapters = AdapterSelectionKind::Listed;
  request.sources.formats.adapter_ids = {"pass-through-format"};
  request.sources.formats.include_byte_passthrough = false;
  request.subset = QueryFragment{{{"data.value"}}, nullptr, {}, {}};
  auto route = frontend->register_typed(std::move(request));
  ASSERT_TRUE(route.ok()) << route.status().message();

  std::atomic<int> concurrent_checks{0};
  auto concurrent_allow = [&](const std::vector<QuerySourceReference>&) {
    ++concurrent_checks;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    return QueryStatus::success();
  };
  auto first_open = std::async(std::launch::async, [&] {
    return bridge.acquire(route.value(), concurrent_allow);
  });
  auto second_open = std::async(std::launch::async, [&] {
    return bridge.acquire(route.value(), concurrent_allow);
  });
  auto first_result = first_open.get();
  auto second_result = second_open.get();
  ASSERT_TRUE(first_result.ok()) << first_result.status().message();
  ASSERT_TRUE(second_result.ok()) << second_result.status().message();
  EXPECT_EQ(first_result.value(), second_result.value());
  EXPECT_EQ(concurrent_checks.load(), 2);
  EXPECT_EQ(results->stats().registered_entries, 1u);

  int authorized = 0;
  auto allow = [&](const std::vector<QuerySourceReference>& sources) {
    ++authorized;
    EXPECT_EQ(sources.size(), 1u);
    EXPECT_EQ(sources.front().path, "/science/exact.bin");
    EXPECT_EQ(sources.front().version, 9);
    return QueryStatus::success();
  };
  auto stat = bridge.acquire(route.value(), allow);
  ASSERT_TRUE(stat.ok()) << stat.status().message();
  auto open = bridge.acquire(route.value(), allow);
  ASSERT_TRUE(open.ok()) << open.status().message();
  EXPECT_EQ(stat.value(), open.value());
  EXPECT_EQ(authorized, 2);
  EXPECT_EQ(bridge.cached_routes(), 1u);
  const auto without_version = route.value().substr(0,
    route.value().find("&version="));
  auto equivalent = bridge.acquire(without_version, allow);
  ASSERT_TRUE(equivalent.ok()) << equivalent.status().message();
  EXPECT_EQ(equivalent.value(), open.value());
  EXPECT_EQ(bridge.cached_routes(), 1u);
  EXPECT_EQ(observation->decoded_batches.load(), 1);
  const auto result_id = open.value()->id();
  ASSERT_EQ(result_id.size(), 64u);
  auto exact = bridge.acquire_exact(route.value(), result_id, allow);
  ASSERT_TRUE(exact.ok()) << exact.status().message();
  EXPECT_EQ(exact.value(), open.value());
  EXPECT_EQ(bridge.acquire_exact(route.value(), std::string(64, '0'), allow)
      .status().code(), QueryError::SourceNotFound);
  auto tail = open.value()->read(open.value()->size() - 1, 8);
  ASSERT_TRUE(tail.ok());
  EXPECT_EQ(tail.value().size(), 1u);
  EXPECT_TRUE(open.value()->read(open.value()->size(), 8).value().empty());

  auto deny = [](const std::vector<QuerySourceReference>&) {
    return QueryStatus::failure(QueryError::PermissionDenied,
      QueryPhase::Validation, "denied");
  };
  auto rejected = bridge.acquire(route.value(), deny);
  ASSERT_FALSE(rejected.ok());
  EXPECT_EQ(query_status_errno(rejected.status()), EACCES);
  bridge.drop(route.value());
  EXPECT_EQ(bridge.acquire_exact(route.value(), result_id, allow)
      .status().code(), QueryError::SourceNotFound);
  const auto id = route.value().substr(route.value().find("id=") + 3, 48);
  EXPECT_TRUE(frontend->erase(id));
  EXPECT_EQ(bridge.acquire(route.value(), allow).status().code(),
    QueryError::SourceNotFound);
  EXPECT_TRUE(open.value()->read(0, 8).ok());
}

TEST(QueryModule, FuseBridgeWaitersTimeOutWithoutCancellingProducer) {
  auto source_manifest = manifest("/science/exact.bin", 9,
    std::string("pass-through-format"));
  auto backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{source_manifest});
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto catalog = make_in_memory_published_version_catalog({source_manifest});
  auto query = QueryModule::create(catalog, storage,
    {std::make_shared<PassThroughAdapter>(
      std::make_shared<PassThroughObservation>())});
  ASSERT_TRUE(query.ok()) << query.status().message();
  auto frontend = std::make_shared<TypedQueryFrontend>();
  auto results = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  auto service = std::make_shared<TypedQueryService>(query.value(), frontend,
    results, 1 << 20);
  FuseTypedQueryBridge bridge(frontend, service, 64, std::chrono::minutes(5),
    FuseTypedQueryBridge::Clock::now, std::chrono::milliseconds(30));
  QueryRequest request;
  request.sources.paths = {{PathScopeKind::Exact, "/science/exact.bin"}};
  request.sources.versions = VersionSelection::exact_version(9);
  request.sources.formats.adapters = AdapterSelectionKind::Listed;
  request.sources.formats.adapter_ids = {"pass-through-format"};
  request.sources.formats.include_byte_passthrough = false;
  request.subset = QueryFragment{{{"data.value"}}, nullptr, {}, {}};
  auto route = frontend->register_typed(std::move(request));
  ASSERT_TRUE(route.ok()) << route.status().message();

  std::promise<void> entered;
  auto entered_future = entered.get_future();
  std::promise<void> release;
  auto release_future = release.get_future();
  auto producer = std::async(std::launch::async, [&] {
    return bridge.acquire(route.value(), [&](const std::vector<QuerySourceReference>&) {
      entered.set_value();
      release_future.wait_for(std::chrono::seconds(2));
      return QueryStatus::success();
    });
  });
  ASSERT_EQ(entered_future.wait_for(std::chrono::seconds(2)),
    std::future_status::ready);
  auto timed_out = bridge.acquire(route.value(), {});
  ASSERT_FALSE(timed_out.ok());
  EXPECT_EQ(timed_out.status().code(), QueryError::DeadlineExceeded);
  EXPECT_EQ(query_status_errno(timed_out.status()), ETIMEDOUT);
  release.set_value();
  auto produced = producer.get();
  ASSERT_TRUE(produced.ok()) << produced.status().message();
  auto later = bridge.acquire(route.value(), {});
  ASSERT_TRUE(later.ok()) << later.status().message();
  EXPECT_EQ(later.value(), produced.value());
}

TEST(QueryModule, FuseBridgeReclaimsIdleCachedResultsBeforeNewRoute) {
  std::vector<StructuredBlobManifest> manifests;
  for (char suffix = 'a'; suffix <= 'f'; ++suffix)
    manifests.push_back(manifest("/science/exact-" + std::string(1, suffix) + ".bin",
      9, std::string("pass-through-format")));
  auto backend = std::make_shared<ModuleBackend>(
    manifests);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto catalog = make_in_memory_published_version_catalog(manifests);
  auto observation = std::make_shared<PassThroughObservation>();
  auto query = QueryModule::create(catalog, storage,
    {std::make_shared<PassThroughAdapter>(observation)});
  ASSERT_TRUE(query.ok()) << query.status().message();
  auto frontend = std::make_shared<TypedQueryFrontend>();
  auto register_route = [&](char suffix) {
    QueryRequest request;
    request.sources.paths = {{PathScopeKind::Exact,
      "/science/exact-" + std::string(1, suffix) + ".bin"}};
    request.sources.versions = VersionSelection::exact_version(9);
    request.sources.formats.adapters = AdapterSelectionKind::Listed;
    request.sources.formats.adapter_ids = {"pass-through-format"};
    request.sources.formats.include_byte_passthrough = false;
    request.subset = QueryFragment{{{"data.value"}}, nullptr, {}, {}};
    return frontend->register_typed(std::move(request));
  };
  auto first_route = register_route('a');
  auto second_route = register_route('b');
  auto third_route = register_route('c');
  ASSERT_TRUE(first_route.ok());
  ASSERT_TRUE(second_route.ok());
  ASSERT_TRUE(third_route.ok());

  auto probe_store = std::make_shared<TypedResultStore>(TypedResultStore::Config{});
  TypedQueryService probe(query.value(), frontend, probe_store, 1 << 20);
  auto probe_id = probe.materialize(first_route.value());
  ASSERT_TRUE(probe_id.ok()) << probe_id.status().message();
  auto probe_handle = probe.open_result(probe_id.value());
  ASSERT_TRUE(probe_handle.ok());
  const auto result_size = probe_handle.value().size();
  ASSERT_GT(result_size, 0u);

  TypedResultStore::Config limits;
  limits.max_result_bytes = result_size;
  limits.max_resident_bytes = 2 * result_size;
  auto results = std::make_shared<TypedResultStore>(limits);
  auto service = std::make_shared<TypedQueryService>(query.value(), frontend,
    results, result_size);
  FuseTypedQueryBridge bridge(frontend, service);
  std::weak_ptr<FuseTypedQueryBridge::Handle> first_cached;
  std::weak_ptr<FuseTypedQueryBridge::Handle> second_cached;
  {
    auto first = bridge.acquire(first_route.value(), {});
    ASSERT_TRUE(first.ok()) << first.status().message();
    first_cached = first.value();
  }
  {
    auto second = bridge.acquire(second_route.value(), {});
    ASSERT_TRUE(second.ok()) << second.status().message();
    second_cached = second.value();
  }
  {
    auto recently_used = bridge.acquire(first_route.value(), {});
    ASSERT_TRUE(recently_used.ok());
  }
  EXPECT_EQ(results->stats().resident_bytes, 2 * result_size);
  EXPECT_EQ(results->stats().open_handles, 2u);
  const auto decoded_before_third = observation->decoded_batches.load();
  {
    auto third = bridge.acquire(third_route.value(), {});
    ASSERT_TRUE(third.ok()) << third.status().message();
    EXPECT_EQ(third.value()->size(), result_size);
  }
  EXPECT_EQ(observation->decoded_batches.load(), decoded_before_third + 1);
  EXPECT_FALSE(first_cached.expired());
  EXPECT_TRUE(second_cached.expired());
  EXPECT_EQ(bridge.cached_routes(), 2u);
  EXPECT_EQ(results->stats().open_handles, 2u);

  auto fourth_route = register_route('d');
  ASSERT_TRUE(fourth_route.ok());
  auto fifth_route = register_route('e');
  ASSERT_TRUE(fifth_route.ok());
  std::mutex barrier_mutex;
  std::condition_variable barrier_ready;
  int started = 0;
  auto synchronize = [&](const std::vector<QuerySourceReference>&) {
    std::unique_lock<std::mutex> lock(barrier_mutex);
    ++started;
    barrier_ready.notify_all();
    if (!barrier_ready.wait_for(lock, std::chrono::seconds(2),
        [&] { return started == 2; }))
      return QueryStatus::failure(QueryError::Internal, QueryPhase::Execution,
        "concurrent materializations did not overlap");
    return QueryStatus::success();
  };
  const auto decoded_before_parallel = observation->decoded_batches.load();
  auto fourth = std::async(std::launch::async, [&] {
    return bridge.acquire(fourth_route.value(), synchronize);
  });
  auto fifth = std::async(std::launch::async, [&] {
    return bridge.acquire(fifth_route.value(), synchronize);
  });
  auto fourth_result = fourth.get();
  auto fifth_result = fifth.get();
  ASSERT_TRUE(fourth_result.ok()) << fourth_result.status().message();
  ASSERT_TRUE(fifth_result.ok()) << fifth_result.status().message();
  EXPECT_EQ(observation->decoded_batches.load(), decoded_before_parallel + 2);
  fourth_result.value().reset();
  fifth_result.value().reset();
  EXPECT_EQ(bridge.cached_routes(), 2u);
  EXPECT_EQ(results->stats().open_handles, 2u);

  auto held_fourth = bridge.acquire(fourth_route.value(), {});
  auto held_fifth = bridge.acquire(fifth_route.value(), {});
  ASSERT_TRUE(held_fourth.ok());
  ASSERT_TRUE(held_fifth.ok());
  auto sixth_route = register_route('f');
  ASSERT_TRUE(sixth_route.ok());
  auto blocked_by_real_handles = bridge.acquire(sixth_route.value(), {});
  EXPECT_EQ(blocked_by_real_handles.status().code(), QueryError::ResourceExhausted);
  EXPECT_TRUE(held_fourth.value()->read(0, 8).ok());
  EXPECT_TRUE(held_fifth.value()->read(0, 8).ok());
}

TEST(QueryModule, SemanticSubsetRequiresPersistedMatchingAdapter) {
  ModuleFixture fixture;
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  EXPECT_EQ(module.value()->prepare(request).status().code(),
    QueryError::AdapterUnavailable);

  auto pass = manifest("/science/raw.bin", 8, std::nullopt);
  auto pass_backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{pass});
  auto pass_storage = std::make_shared<StructuredBlobStorage>(pass_backend);
  auto pass_catalog = make_in_memory_published_version_catalog({pass});
  auto pass_module = QueryModule::create(pass_catalog, pass_storage, {});
  ASSERT_TRUE(pass_module.ok());
  request.sources.paths = {{PathScopeKind::Exact, "/science/raw.bin"}};
  request.sources.formats.adapters = AdapterSelectionKind::Listed;
  request.sources.formats.adapter_ids.clear();
  request.sources.formats.include_byte_passthrough = true;
  EXPECT_EQ(pass_module.value()->prepare(request).status().code(),
    QueryError::AdapterUnavailable);
}

TEST(QueryModule, PrepareEnforcesDeclaredAdapterCapabilities) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>();
  auto module = QueryModule::create(fixture.catalog, fixture.storage,
    {adapter});
  ASSERT_TRUE(module.ok());

  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.subset->predicate = make_comparison({"data.quality"},
    ComparisonOperator::Greater, LogicalValue::int32(0));
  EXPECT_EQ(module.value()->prepare(request).status().code(),
    QueryError::UnsupportedCapability);

  request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.subset->output.type_overrides = {
    {{"data.value"}, LogicalTypeId::Float32}};
  EXPECT_EQ(module.value()->prepare(request).status().code(),
    QueryError::UnsupportedCapability);

  request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.subset->output.interpolation = QueryFragment::GridInterpolation{
    QueryFragment::InterpolationMethod::Bilinear,
    {{{"domain.time"}, 4, std::nullopt},
     {{"data.value"}, 4, std::nullopt}},
    QueryFragment::BorderMode::Clamp};
  EXPECT_EQ(module.value()->prepare(request).status().code(),
    QueryError::UnsupportedCapability);
}

TEST(QueryModule, PlanningContextSharesStateAndLimitsOnlyWithinOneSourceSession) {
  ModuleFixture fixture;
  auto observed = std::make_shared<PlanningObservation>();
  auto adapter = std::make_shared<ModuleAdapter>(false, nullptr,
    PlanningReads{}, PlanningReads{}, observed);
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.limits.max_sources = 9;
  request.limits.max_ranges = 17;
  request.limits.max_layout_bytes = 127;
  request.limits.max_payload_bytes = 251;
  request.limits.max_result_rows = 503;
  request.limits.target_batch_rows = 11;
  request.limits.target_batch_bytes = 97;
  for (int attempt = 0; attempt < 2; ++attempt) {
    auto prepared = module.value()->prepare(request);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    ASSERT_EQ(prepared.value().sources().size(), 2u);
    EXPECT_EQ(observed->active_context, nullptr);
    for (const auto& state : observed->states) EXPECT_TRUE(state.expired());
  }
  ASSERT_EQ(observed->capability_limits.size(), 4u);
  ASSERT_EQ(observed->prepare_limits.size(), 4u);
  for (const auto* calls : {&observed->capability_limits,
                           &observed->prepare_limits}) {
    for (const auto& limits : *calls) {
      EXPECT_EQ(limits.max_sources, request.limits.max_sources);
      EXPECT_EQ(limits.max_ranges, request.limits.max_ranges);
      EXPECT_EQ(limits.max_layout_bytes, request.limits.max_layout_bytes);
      EXPECT_EQ(limits.max_payload_bytes, request.limits.max_payload_bytes);
      EXPECT_EQ(limits.max_result_rows, request.limits.max_result_rows);
      EXPECT_EQ(limits.target_batch_rows, request.limits.target_batch_rows);
      EXPECT_EQ(limits.target_batch_bytes, request.limits.target_batch_bytes);
    }
  }
}

TEST(QueryModule, ExecuteEnforcesReadBudgetAndCancellation) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>(true);
  auto module = QueryModule::create(fixture.catalog, fixture.storage,
    {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.limits.max_payload_bytes = 2;
  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();

  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto exhausted = cursor.value()->next();
  EXPECT_EQ(exhausted.status().code(), QueryError::ResourceExhausted);

  request.limits.max_payload_bytes = 4;
  prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok());
  cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok());
  cursor.value()->cancel();
  EXPECT_EQ(cursor.value()->next().status().code(), QueryError::Cancelled);
}

TEST(QueryModule, ExecuteEnforcesFileResultBudget) {
  ModuleFixture fixture;
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {});
  ASSERT_TRUE(module.ok());
  auto request = file_request();
  request.limits.max_result_rows = 1;

  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_EQ(prepared.value().sources().size(), 2u);
  EXPECT_EQ(module.value()->execute(prepared.value()).status().code(),
    QueryError::ResourceExhausted);
}

TEST(QueryModule, PlanningChargesRepeatedActualLayoutReadsAcrossAdapterCalls) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>(false, nullptr,
    PlanningReads{{true, 2}}, PlanningReads{{true, 2}});
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.limits.max_layout_bytes = 3;
  auto prepared = module.value()->prepare(request);
  EXPECT_EQ(prepared.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(prepared.status().phase(), QueryPhase::Planning);
  EXPECT_EQ(fixture.backend->layout_reads, 1u);

  request.limits.max_layout_bytes = 4;
  prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(fixture.backend->layout_reads, 3u);
}

TEST(QueryModule, PlanningChargesPayloadReadsAndPreservesSourceErrors) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>(false, nullptr,
    PlanningReads{{false, 4}});
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.limits.max_payload_bytes = 3;
  auto prepared = module.value()->prepare(request);
  EXPECT_EQ(prepared.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(prepared.status().phase(), QueryPhase::Planning);
  EXPECT_EQ(fixture.backend->payload_reads, 0u);

  request.limits.max_payload_bytes = 4;
  fixture.backend->fail_reads = true;
  prepared = module.value()->prepare(request);
  EXPECT_EQ(prepared.status().code(), QueryError::ReadFailure);
  EXPECT_EQ(prepared.status().phase(), QueryPhase::Storage);
  EXPECT_NE(prepared.status().message().find("injected source failure"),
    std::string::npos);
  EXPECT_EQ(fixture.backend->payload_reads, 1u);
}

TEST(QueryModule, PlanningRangeBudgetIsSharedAcrossSources) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>(false, nullptr,
    PlanningReads{{true, 1}}, PlanningReads{{false, 1}});
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.limits.max_ranges = 3;
  auto prepared = module.value()->prepare(request);
  EXPECT_EQ(prepared.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(prepared.status().phase(), QueryPhase::Planning);
  EXPECT_EQ(fixture.backend->layout_reads, 2u);
  EXPECT_EQ(fixture.backend->payload_reads, 1u);
}

TEST(QueryModule, PlanningAndExecutionUseIndependentActualReadBudgets) {
  ModuleFixture fixture;
  auto adapter = std::make_shared<ModuleAdapter>(true, nullptr,
    PlanningReads{{false, 4}});
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.sources.paths = {{PathScopeKind::Exact, "/science/a.tsfile"}};
  request.limits.max_payload_bytes = 4;
  request.limits.max_ranges = 1;
  // The two-byte manifest does not consume a layout budget without a read.
  request.limits.max_layout_bytes = 1;
  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  EXPECT_EQ(fixture.backend->payload_reads, 2u);
  EXPECT_EQ(fixture.backend->layout_reads, 0u);
  EXPECT_EQ(batch.value()->row_count(), 2u); // Residual-only predicate applied.
}

TEST(QueryModule, CachedGribBinaryDemandSharesPayloadAndRangeBudgetsAcrossSources) {
  // Exercise the real BinaryView adapter through QueryModule's budgeted
  // request source and independent producer capability, including warm hits.
  auto* handle = codes_grib_handle_new_from_samples(nullptr, "GRIB2");
  ASSERT_NE(handle, nullptr);
  const void* raw = nullptr;
  std::size_t length = 0;
  ASSERT_EQ(codes_get_message(handle, &raw, &length), CODES_SUCCESS);
  const auto* begin = static_cast<const std::uint8_t*>(raw);
  StructuredBlobBytes payload(begin, begin + length);
  const auto text = grib_layout_header() +
    grib_utils::build_index_line(handle, 0, length, 2);
  codes_handle_delete(handle);
  StructuredBlobBytes layout(text.begin(), text.end());
  auto a = manifest("/science/a.grib2", 7, std::string("grib"));
  auto b = manifest("/science/b.grib2", 7, std::string("grib"));
  for (auto* source : {&a, &b}) {
    source->payload_size = payload.size();
    source->layout_size = layout.size();
  }
  auto backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{a, b}, payload, layout);
  auto storage = std::make_shared<StructuredBlobStorage>(backend);
  auto runtime = std::make_shared<QueryRuntime>();
  auto adapter = std::make_shared<GribFormatQueryAdapter>(
    GribParsedIndexCacheConfig{}, GribDecodedFieldCacheConfig{}, runtime);
  auto module = QueryModule::create(make_in_memory_published_version_catalog({a, b}),
    storage, {adapter});
  ASSERT_TRUE(module.ok()) << module.status().message();
  auto request = file_request();
  request.sources.formats.adapter_ids = {"grib"};
  request.subset = QueryFragment{};
  request.subset->projection = {{grib_query_fields::kMessageBytes}};
  request.subset->output.mode = QueryFragment::ResultMode::BinaryView;
  request.limits.max_payload_bytes = 2 * length;
  auto warm = module.value()->prepare(request);
  ASSERT_TRUE(warm.ok()) << warm.status().message();
  auto warm_cursor = module.value()->execute(warm.value());
  ASSERT_TRUE(warm_cursor.ok());
  for (int i = 0; i < 2; ++i) {
    auto batch = warm_cursor.value()->next();
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    ASSERT_TRUE(batch.value());
  }
  EXPECT_EQ(backend->payload_reads, 2u);

  for (const bool limit_ranges : {false, true}) {
    request.limits.max_payload_bytes = limit_ranges ? 2 * length : length;
    request.limits.max_ranges = limit_ranges ? 1 : 65536;
    auto prepared = module.value()->prepare(request);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    auto cursor = module.value()->execute(prepared.value());
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    auto first = cursor.value()->next();
    ASSERT_TRUE(first.ok()) << first.status().message();
    ASSERT_TRUE(first.value());
    auto second = cursor.value()->next();
    EXPECT_EQ(second.status().code(), QueryError::ResourceExhausted);
    EXPECT_EQ(second.status().phase(), QueryPhase::Execution);
    // The first source was a hit; the second source's demand is rejected
    // despite also being cached. Neither query issues actual payload reads.
    EXPECT_EQ(backend->payload_reads, 2u);
  }
}

TEST(QueryModule, FreshCatalogSelectionReusesManifestAndPreservesResidualPredicate) {
  ModuleFixture fixture;
  unsigned scans = 0;
  auto catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
    ++scans;
    return QueryResult<PublishedVersionSnapshot>::success(
      {{fixture.a, fixture.old_a, fixture.b}, {}});
  }, fixture.storage->storage_namespace());
  auto module = QueryModule::create(catalog, fixture.storage, {});
  ASSERT_TRUE(module.ok());
  auto request = file_request(make_comparison({"sigma.station"},
    ComparisonOperator::Equal, LogicalValue::utf8("A")));
  auto selected = catalog->scan(request.sources, request.limits.max_sources);
  ASSERT_TRUE(selected.ok());
  auto prepared = module.value()->prepare(request, &selected.value());
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_EQ(prepared.value().sources().size(), 1u);
  EXPECT_EQ(prepared.value().sources()[0].source->manifest().key.version, 7);
  EXPECT_EQ(scans, 1u);
  EXPECT_EQ(fixture.backend->manifest_reads, 0u);
  EXPECT_EQ(prepared.value().sources()[0].source->storage_namespace(),
    fixture.storage->storage_namespace());
}

TEST(QueryModule, CatalogSkeletonMaterializesBeforeMetadataPredicateAndAdapterPlanning) {
  ModuleFixture fixture;
  auto a = fixture.a; auto b = fixture.b;
  a.metadata.clear(); b.metadata.clear();
  a.metadata_materialized = b.metadata_materialized = false;
  auto catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
    return QueryResult<PublishedVersionSnapshot>::success({{a, b}, {}});
  }, fixture.storage->storage_namespace());
  auto observation = std::make_shared<PlanningObservation>();
  auto adapter = std::make_shared<ModuleAdapter>(false, nullptr,
    PlanningReads{}, PlanningReads{}, observation);
  auto module = QueryModule::create(catalog, fixture.storage, {adapter});
  ASSERT_TRUE(module.ok());
  auto request = semantic_request();
  request.sources.file_predicate = make_comparison({"sigma.station"},
    ComparisonOperator::Equal, LogicalValue::utf8("A"));
  auto prepared = module.value()->prepare(request);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  ASSERT_EQ(prepared.value().sources().size(), 1u);
  const auto& materialized = prepared.value().sources().front().source->manifest();
  EXPECT_EQ(materialized.key, fixture.a.key);
  EXPECT_TRUE(materialized.metadata_materialized);
  EXPECT_EQ(std::get<std::string>(materialized.metadata.at("station").value()), "A");
  EXPECT_EQ(fixture.backend->manifest_reads, 2u);
  EXPECT_EQ(observation->prepare_limits.size(), 1u);
  EXPECT_EQ(fixture.backend->payload_reads, 0u);
  EXPECT_EQ(fixture.backend->layout_reads, 0u);
  auto cursor = module.value()->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto next = cursor.value()->next();
  ASSERT_TRUE(next.ok()) << next.status().message();
  ASSERT_TRUE(next.value());
  EXPECT_EQ(next.value()->row_count(), 2u);
}

TEST(QueryModule, CatalogNamespaceMismatchFailsBeforeUsingManifest) {
  ModuleFixture fixture;
  auto foreign_backend = std::make_shared<ModuleBackend>(
    std::vector<StructuredBlobManifest>{fixture.a});
  auto catalog = make_refreshing_published_version_catalog([&](const SourceSelector&) {
    return QueryResult<PublishedVersionSnapshot>::success({{fixture.a}, {}});
  }, foreign_backend->storage_namespace());
  auto module = QueryModule::create(catalog, fixture.storage, {});
  ASSERT_TRUE(module.ok());
  auto prepared = module.value()->prepare(file_request());
  EXPECT_FALSE(prepared.ok());
  EXPECT_EQ(fixture.backend->manifest_reads, 0u);
  EXPECT_EQ(fixture.backend->payload_reads, 0u);
}

TEST(QueryModule, CatalogWithoutNamespaceStillLoadsExactManifest) {
  ModuleFixture fixture;
  auto module = QueryModule::create(fixture.catalog, fixture.storage, {});
  ASSERT_TRUE(module.ok());
  auto prepared = module.value()->prepare(file_request());
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(fixture.backend->manifest_reads, prepared.value().sources().size());
}
