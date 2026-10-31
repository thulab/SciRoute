#include "../../../support/InMemoryDatasetCatalog.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/encoding/ArrowResultEncoder.h"

#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <map>
#include <memory>
#include <utility>
#include <vector>

using namespace iotdbfs;

namespace {

template<class T> QueryResult<T> failure(QueryError code, const char* message) {
  return QueryResult<T>::failure(QueryStatus::failure(
    code, QueryPhase::Planning, message));
}

// H01 exercises the format contract through the real core. No libhdf5,
// ingestion, serialized layout, payload decoder or production adapter is used.
class DatasetProvider final : public Hdf5DatasetProvider {
public:
  std::map<std::string, Hdf5DatasetDescriptor> datasets{
    {"/a", {"/a", LogicalTypeId::Float32, {5, 7, 9}, true, {}}},
    {"/b", {"/b", LogicalTypeId::Float64, {3, 4}, true, {}}},
    {"/empty", {"/empty", LogicalTypeId::Float32, {5, 0, 9}, true, {}}},
    {"/unsupported", {"/unsupported", LogicalTypeId::Float32, {2}, false,
      "fixture datatype is outside the supported profile"}},
  };
  mutable std::size_t descriptions{0};
  mutable std::size_t value_reads{0};
  bool special_native_values{false};

  QueryResult<Hdf5DatasetDescriptor> describe(const std::string& path) const override {
    ++descriptions;
    const auto item = datasets.find(path);
    if (item == datasets.end())
      return failure<Hdf5DatasetDescriptor>(QueryError::UnknownField,
        "dataset does not exist");
    return QueryResult<Hdf5DatasetDescriptor>::success(item->second);
  }
};

class Backend final : public StructuredBlobStorageBackend {
public:
  iotdbfs::test::InMemoryDatasetCatalog dataset_catalogs;
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& value) override { return dataset_catalogs.write(key, value); }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
      const StructuredBlobVersionKey& key) const override { return dataset_catalogs.read(key); }

  Backend() {
    manifest.key = {"/science/data.h5", 7};
    manifest.format_id = "hdf5";
    manifest.payload_size = 16;
    manifest.layout_size = 4;
    manifest.metadata = {{"tag", MetadataValue(std::string("keep"))}};
    manifest.phase = StructuredBlobPhase::Published;
    manifest.published = true;
    manifest.revision = 2;
  }

  StructuredBlobManifest manifest;
  mutable std::size_t reads{0};

  StructuredBlobResult<StructuredBlobManifest> load_manifest(
      const StructuredBlobVersionKey& key) const override {
    if (!(key == manifest.key))
      return StructuredBlobResult<StructuredBlobManifest>::failure(
        StructuredBlobError::NotFound, "missing fixture version");
    return StructuredBlobResult<StructuredBlobManifest>::success(manifest);
  }
  StructuredBlobStatus create_manifest(const StructuredBlobManifest&) override { return denied(); }
  StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey&,
      std::uint64_t, const StructuredBlobManifest&) override { return denied(); }
  StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&,
      std::uint64_t, const StructuredBlobBytes&) override { return denied(); }
  StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,
      const StructuredBlobBytes&) override { return denied(); }
  StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,
      const MetadataMap&) override { return denied(); }
  StructuredBlobStatus erase(const StructuredBlobVersionKey&, std::uint64_t) override { return denied(); }
  StructuredBlobResult<StructuredBlobBytes> read_payload_range(
      const StructuredBlobVersionKey&, std::uint64_t, std::uint64_t) const override {
    ++reads;
    return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::BackendFailure, "H01 must not use payload I/O");
  }
  StructuredBlobResult<StructuredBlobBytes> read_layout_range(
      const StructuredBlobVersionKey&, std::uint64_t, std::uint64_t) const override {
    ++reads;
    return StructuredBlobResult<StructuredBlobBytes>::failure(
      StructuredBlobError::BackendFailure, "H01 must not use layout I/O");
  }
private:
  static StructuredBlobStatus denied() {
    return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,
      "read-only H01 fixture");
  }
};

enum class CursorFault { None, Schema, ColumnType, DomainShape };

struct BindingState final : AdapterPlanningState {
  explicit BindingState(Hdf5QueryBinding value) : binding(std::move(value)) {}
  Hdf5QueryBinding binding;
};

struct Plan final : AdapterExecutionPlan {
  Plan(Hdf5QueryBinding value, LogicalSchema output, QueryLimits bounds)
    : binding(std::move(value)), schema(std::move(output)), limits(bounds) {}
  Hdf5QueryBinding binding;
  LogicalSchema schema;
  QueryLimits limits;
};

LogicalSchema projected_schema(const Hdf5QueryBinding& binding) {
  std::vector<LogicalField> fields;
  for (const auto& field : binding.projection())
    fields.push_back(*binding.schema().find(field.field_id));
  return LogicalSchema::create(std::move(fields)).value();
}

class Cursor final : public QueryBatchCursor {
public:
  Cursor(std::shared_ptr<const Plan> plan, Hdf5SlabIterator slabs,
      std::shared_ptr<DatasetProvider> provider, CursorFault fault)
    : plan_(std::move(plan)), slabs_(std::move(slabs)),
      provider_(std::move(provider)), schema_(plan_->schema), fault_(fault) {
    if (fault_ == CursorFault::Schema) {
      auto fields = schema_.fields();
      fields[0].display_name = "wrong cursor schema";
      schema_ = LogicalSchema::create(std::move(fields)).value();
    }
  }
  const LogicalSchema& schema() const override { return schema_; }
  void cancel() override { cancelled_ = true; }

  QueryResult<std::optional<QueryBatch>> next() override {
    using Result = QueryResult<std::optional<QueryBatch>>;
    if (cancelled_)
      return failure<std::optional<QueryBatch>>(QueryError::Cancelled, "cancelled fixture");
    auto slab = slabs_.next();
    if (!slab.ok()) return Result::failure(slab.status());
    if (!slab.value()) return Result::success(std::nullopt);
    ++provider_->value_reads;
    const auto& block = *slab.value();
    std::vector<std::vector<std::uint64_t>> coordinates;
    for (std::uint64_t row = 0; row < block.row_count; ++row) {
      auto remainder = row;
      auto coordinate = block.domain.origin;
      for (std::size_t axis = coordinate.size(); axis-- > 0;) {
        coordinate[axis] += remainder % block.domain.shape[axis];
        remainder /= block.domain.shape[axis];
      }
      coordinates.push_back(std::move(coordinate));
    }
    std::vector<LogicalArray> columns;
    for (const auto& field : schema_.fields()) {
      if (field.role == FieldRole::Dimension) {
        std::size_t axis = 0;
        while (hdf5_axis_field(axis).field_id != field.field_id) ++axis;
        std::vector<std::uint64_t> values;
        for (const auto& coordinate : coordinates) values.push_back(coordinate[axis]);
        columns.push_back({field.type, std::move(values), {}});
      } else {
        // Coordinates have an independent, easily checked value oracle.
        std::vector<double> values;
        for (const auto& coordinate : coordinates) {
          double value = 0;
          for (auto position : coordinate) value = 10 * value + position;
          if (provider_->special_native_values) {
            const double native_values[]{-0.0,
              std::numeric_limits<double>::quiet_NaN(), -9999.0};
            value = native_values[coordinate.back() % 3];
          }
          values.push_back(value);
        }
        if (field.type.id == LogicalTypeId::Float32 && fault_ != CursorFault::ColumnType)
          columns.push_back({field.type, std::vector<float>(values.begin(), values.end()), {}});
        else columns.push_back({field.type, std::move(values), {}});
      }
    }
    auto domain = block.domain;
    if (fault_ == CursorFault::DomainShape) ++domain.shape[0];
    auto batch = QueryBatch::create(schema_, block.row_count,
      std::move(columns), std::move(domain), {{"/science/data.h5", 7, "hdf5"}});
    if (!batch.ok()) return Result::failure(batch.status());
    return Result::success(std::move(batch.value()));
  }
private:
  std::shared_ptr<const Plan> plan_;
  Hdf5SlabIterator slabs_;
  std::shared_ptr<DatasetProvider> provider_;
  LogicalSchema schema_;
  CursorFault fault_;
  bool cancelled_{false};
};

class Adapter final : public FormatQueryAdapter {
public:
  explicit Adapter(std::shared_ptr<DatasetProvider> provider)
    : provider_(std::move(provider)) {}
  CursorFault fault{CursorFault::None};
  mutable std::size_t capability_calls{0};
  mutable std::size_t prepare_calls{0};

  const std::string& id() const override {
    static const std::string value = "hdf5";
    return value;
  }
  QueryResult<QueryCapabilities> capabilities(const QueryPlanningContext& context,
      const QueryFragment& fragment) const override {
    ++capability_calls;
    auto binding = bind_hdf5_query(*provider_, fragment, context.limits());
    if (!binding.ok()) return QueryResult<QueryCapabilities>::failure(binding.status());
    auto state = std::make_shared<const BindingState>(std::move(binding.value()));
    context.set_state(state);
    return make_hdf5_query_capabilities(state->binding);
  }
  QueryResult<AdapterPlan> prepare(const QueryPlanningContext& context,
      const QueryFragment& fragment) const override {
    ++prepare_calls;
    const auto state = std::dynamic_pointer_cast<const BindingState>(context.state());
    if (!state) return failure<AdapterPlan>(QueryError::Internal,
      "capabilities must bind dataset before prepare");
    auto schema = projected_schema(state->binding);
    auto plan = std::make_shared<const Plan>(state->binding, schema, context.limits());
    return QueryResult<AdapterPlan>::success(AdapterPlan(plan, std::move(schema),
      nullptr, {}, fragment.projection));
  }
  QueryResult<std::unique_ptr<QueryBatchCursor>> execute(const QueryExecutionContext&,
      std::shared_ptr<const AdapterExecutionPlan> implementation,
      const QueryExecutionOptions&) const override {
    auto plan = std::dynamic_pointer_cast<const Plan>(implementation);
    if (!plan) return failure<std::unique_ptr<QueryBatchCursor>>(
      QueryError::Internal, "invalid fixture plan");
    auto slabs = Hdf5SlabIterator::create(plan->binding, plan->limits);
    if (!slabs.ok())
      return QueryResult<std::unique_ptr<QueryBatchCursor>>::failure(slabs.status());
    std::unique_ptr<QueryBatchCursor> cursor = std::make_unique<Cursor>(
      plan, std::move(slabs.value()), provider_, fault);
    return QueryResult<std::unique_ptr<QueryBatchCursor>>::success(std::move(cursor));
  }
private:
  std::shared_ptr<DatasetProvider> provider_;
};

class Hdf5QueryBindingTest : public ::testing::Test {
protected:
  std::shared_ptr<Backend> backend = std::make_shared<Backend>();
  std::shared_ptr<DatasetProvider> provider = std::make_shared<DatasetProvider>();
  std::shared_ptr<Adapter> adapter = std::make_shared<Adapter>(provider);
  std::shared_ptr<const QueryModule> module;

  void SetUp() override {
    auto created = QueryModule::create(
      make_in_memory_published_version_catalog({backend->manifest}),
      std::make_shared<StructuredBlobStorage>(backend), {adapter});
    ASSERT_TRUE(created.ok()) << created.status().message();
    module = std::move(created.value());
  }

  QueryRequest request(const std::string& dataset = "/a") const {
    QueryRequest result;
    result.sources.paths = {{PathScopeKind::Exact, backend->manifest.key.path}};
    result.sources.versions = VersionSelection::exact_version(7);
    result.sources.formats.adapters = AdapterSelectionKind::Listed;
    result.sources.formats.adapter_ids = {"hdf5"};
    result.sources.formats.include_byte_passthrough = false;
    result.subset = QueryFragment{};
    result.subset->projection = {make_hdf5_value_field(dataset).value()};
    return result;
  }

  static DomainSlice slice(std::size_t axis, std::uint64_t begin, std::uint64_t end) {
    return {hdf5_axis_field(axis),
      DomainBound{LogicalValue::uint64(begin), BoundKind::Inclusive},
      DomainBound{LogicalValue::uint64(end), BoundKind::Exclusive}};
  }
};

TEST_F(Hdf5QueryBindingTest, BindsDatasetBeforeCapabilitiesAndStreamsBoundedCOrderSlabs) {
  auto query = request();
  query.subset->slices = {slice(0, 1, 5), slice(1, 2, 7), slice(2, 3, 9)};
  query.limits.target_batch_rows = 13;
  query.limits.target_batch_bytes = 28; // At most seven Float32 values.
  auto prepared = module->prepare(query);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  EXPECT_EQ(provider->descriptions, 1u);
  EXPECT_EQ(adapter->capability_calls, 1u);
  EXPECT_EQ(adapter->prepare_calls, 1u);
  ASSERT_TRUE(prepared.value().result_schema());
  ASSERT_EQ(prepared.value().result_schema()->fields().size(), 1u);
  auto cursor = module->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  std::vector<float> actual;
  std::vector<std::vector<std::uint64_t>> visited;
  for (;;) {
    auto batch = cursor.value()->next();
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    if (!batch.value()) break;
    const auto& value = *batch.value();
    EXPECT_LE(value.row_count(), 7u);
    ASSERT_TRUE(value.domain());
    EXPECT_EQ(value.domain()->kind, DomainKind::DenseGrid);
    ASSERT_EQ(value.domain()->dimensions.size(), 3u);
    EXPECT_EQ(value.sources().front().version, 7);
    const auto& values = std::get<std::vector<float>>(value.columns()[0].values);
    actual.insert(actual.end(), values.begin(), values.end());
    for (std::uint64_t i = 0; i < value.row_count(); ++i) {
      auto coordinate = value.domain()->origin;
      auto remainder = i;
      for (std::size_t axis = coordinate.size(); axis-- > 0;) {
        coordinate[axis] += remainder % value.domain()->shape[axis];
        remainder /= value.domain()->shape[axis];
      }
      visited.push_back(std::move(coordinate));
    }
  }
  std::vector<float> expected;
  std::vector<std::vector<std::uint64_t>> expected_coordinates;
  for (std::uint64_t x = 1; x < 5; ++x)
    for (std::uint64_t y = 2; y < 7; ++y)
      for (std::uint64_t z = 3; z < 9; ++z) {
        expected.push_back(static_cast<float>(100*x + 10*y + z));
        expected_coordinates.push_back({x, y, z});
      }
  EXPECT_EQ(actual, expected);
  EXPECT_EQ(visited, expected_coordinates);
  EXPECT_EQ(backend->reads, 0u);
}

TEST_F(Hdf5QueryBindingTest, RetainsProjectedAxisOrderAndFloat64Values) {
  auto query = request("/b");
  const auto value = query.subset->projection.front();
  query.subset->projection = {hdf5_axis_field(1), value, hdf5_axis_field(0)};
  query.subset->slices = {slice(0, 1, 3), slice(1, 1, 4)};
  query.limits.target_batch_rows = 2;
  query.limits.target_batch_bytes = 48;
  auto prepared = module->prepare(query);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto cursor = module->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  std::vector<double> values;
  std::vector<std::uint64_t> first, last;
  for (;;) {
    auto batch = cursor.value()->next();
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    if (!batch.value()) break;
    EXPECT_LE(batch.value()->row_count(), 2u);
    const auto& columns = batch.value()->columns();
    const auto& a = std::get<std::vector<std::uint64_t>>(columns[0].values);
    const auto& b = std::get<std::vector<double>>(columns[1].values);
    const auto& c = std::get<std::vector<std::uint64_t>>(columns[2].values);
    first.insert(first.end(), a.begin(), a.end());
    values.insert(values.end(), b.begin(), b.end());
    last.insert(last.end(), c.begin(), c.end());
  }
  EXPECT_EQ(values, (std::vector<double>{11, 12, 13, 21, 22, 23}));
  EXPECT_EQ(first, (std::vector<std::uint64_t>{1, 2, 3, 1, 2, 3}));
  EXPECT_EQ(last, (std::vector<std::uint64_t>{1, 1, 1, 2, 2, 2}));
}

TEST_F(Hdf5QueryBindingTest, EmptySelectionsKeepSchemaWithoutValueReads) {
  for (const auto& dataset : {std::string("/a"), std::string("/empty")}) {
    auto query = request(dataset);
    if (dataset == "/a") query.subset->slices = {slice(1, 4, 4)};
    auto prepared = module->prepare(query);
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    ASSERT_TRUE(prepared.value().result_schema());
    EXPECT_EQ(prepared.value().result_schema()->fields().size(), 1u);
    auto cursor = module->execute(prepared.value());
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    EXPECT_EQ(cursor.value()->schema().fields().size(), 1u);
    auto batch = cursor.value()->next();
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    EXPECT_FALSE(batch.value());
  }
  EXPECT_EQ(provider->value_reads, 0u);
  EXPECT_EQ(backend->reads, 0u);
}

TEST_F(Hdf5QueryBindingTest, DistinguishesMissingDatasetAndUnsupportedDataset) {
  auto absent = module->prepare(request("/missing"));
  ASSERT_FALSE(absent.ok());
  EXPECT_EQ(absent.status().code(), QueryError::UnknownField);
  auto unsupported = module->prepare(request("/unsupported"));
  ASSERT_FALSE(unsupported.ok());
  EXPECT_EQ(unsupported.status().code(), QueryError::UnsupportedCapability);
  EXPECT_EQ(adapter->prepare_calls, 0u);
  EXPECT_EQ(provider->value_reads, 0u);
}

TEST_F(Hdf5QueryBindingTest, MissingVersionFailsButFilteredExistingVersionIsEmpty) {
  auto missing = request();
  missing.sources.versions = VersionSelection::exact_version(99);
  auto absent = module->prepare(missing);
  ASSERT_FALSE(absent.ok());
  EXPECT_EQ(absent.status().code(), QueryError::SourceNotFound);

  auto filtered = request();
  filtered.sources.file_predicate = make_comparison({"sigma.tag"},
    ComparisonOperator::Equal, LogicalValue::utf8("exclude"));
  auto empty = module->prepare(filtered);
  ASSERT_TRUE(empty.ok()) << empty.status().message();
  EXPECT_TRUE(empty.value().sources().empty());
  EXPECT_FALSE(empty.value().result_schema());

  filtered.sources.file_predicate.reset();
  filtered.sources.formats.adapter_ids = {"arrow"};
  auto excluded_format = module->prepare(filtered);
  ASSERT_TRUE(excluded_format.ok()) << excluded_format.status().message();
  EXPECT_TRUE(excluded_format.value().sources().empty());
  EXPECT_EQ(adapter->capability_calls, 0u);
}

TEST_F(Hdf5QueryBindingTest, RejectsNonExactAndMixedAddressRequestsBeforeBinding) {
  std::vector<QueryRequest> bad;
  auto latest = request(); latest.sources.versions = VersionSelection::latest(); bad.push_back(latest);
  auto subtree = request(); subtree.sources.paths = {{PathScopeKind::Subtree, subtree.sources.paths.front().path}}; bad.push_back(subtree);
  auto several = request(); several.sources.paths = {{PathScopeKind::Exact, several.sources.paths.front().path}, {PathScopeKind::Exact, "/other.h5"}}; bad.push_back(several);
  auto mixed = request(); mixed.subset->physical_array = PhysicalArraySubject{"target", 0, 0, {{{}}}}; bad.push_back(mixed);
  for (const auto& query : bad) {
    auto prepared = module->prepare(query);
    ASSERT_FALSE(prepared.ok());
    EXPECT_EQ(prepared.status().code(), QueryError::InvalidQuery);
  }
  EXPECT_EQ(provider->descriptions, 0u);
}

TEST_F(Hdf5QueryBindingTest, RejectsWrongAxisTypeAndUnknownProjection) {
  auto wrong_type = request();
  wrong_type.subset->slices = {{hdf5_axis_field(0),
    DomainBound{LogicalValue::int64(1), BoundKind::Inclusive},
    DomainBound{LogicalValue::int64(2), BoundKind::Exclusive}}};
  auto result = module->prepare(wrong_type);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(result.status().code(), QueryError::TypeMismatch);
  auto unknown = request(); unknown.subset->projection.push_back({"hdf5.axis.9"});
  result = module->prepare(unknown);
  ASSERT_FALSE(result.ok());
  EXPECT_EQ(provider->value_reads, 0u);
}

TEST_F(Hdf5QueryBindingTest, EnforcesResultAndSingleRowByteBudgetsBeforeReadingValues) {
  auto row_limited = request(); row_limited.limits.max_result_rows = 1;
  auto prepared = module->prepare(row_limited);
  ASSERT_FALSE(prepared.ok());
  EXPECT_EQ(prepared.status().code(), QueryError::ResourceExhausted);

  auto byte_limited = request(); byte_limited.limits.target_batch_bytes = 3;
  prepared = module->prepare(byte_limited);
  ASSERT_FALSE(prepared.ok());
  EXPECT_EQ(prepared.status().code(), QueryError::ResourceExhausted);
  EXPECT_EQ(provider->value_reads, 0u);
}

TEST_F(Hdf5QueryBindingTest, CoreRejectsCursorSchemaDifferentFromPreparedContract) {
  adapter->fault = CursorFault::Schema;
  auto prepared = module->prepare(request());
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto cursor = module->execute(prepared.value());
  ASSERT_FALSE(cursor.ok());
  EXPECT_EQ(cursor.status().code(), QueryError::SchemaMismatch);
  EXPECT_EQ(provider->value_reads, 0u);
}

TEST_F(Hdf5QueryBindingTest, TypedBatchRejectsWrongColumnStorageAndDomainSize) {
  for (const auto fault : {CursorFault::ColumnType, CursorFault::DomainShape}) {
    adapter->fault = fault;
    auto prepared = module->prepare(request());
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    auto cursor = module->execute(prepared.value());
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    auto batch = cursor.value()->next();
    ASSERT_FALSE(batch.ok());
    EXPECT_EQ(batch.status().code(), fault == CursorFault::ColumnType
      ? QueryError::TypeMismatch : QueryError::SchemaMismatch);
  }
}

TEST_F(Hdf5QueryBindingTest, NativeNaNNegativeZeroAndFillSentinelStayValidValues) {
  provider->special_native_values = true;
  for (const auto dtype : {LogicalTypeId::Float32, LogicalTypeId::Float64}) {
    provider->datasets["/native"] = {"/native", dtype, {3}, true, {}};
    auto prepared = module->prepare(request("/native"));
    ASSERT_TRUE(prepared.ok()) << prepared.status().message();
    auto cursor = module->execute(prepared.value());
    ASSERT_TRUE(cursor.ok()) << cursor.status().message();
    auto batch = cursor.value()->next();
    ASSERT_TRUE(batch.ok()) << batch.status().message();
    ASSERT_TRUE(batch.value());
    ASSERT_EQ(batch.value()->row_count(), 3u);
    const auto& column = batch.value()->columns().front();
    EXPECT_FALSE(column.type.nullable);
    EXPECT_TRUE(column.validity.empty());
    const auto check_values = [](const auto& values) {
      ASSERT_EQ(values.size(), 3u);
      EXPECT_EQ(values[0], 0.0);
      EXPECT_TRUE(std::signbit(values[0]));
      EXPECT_TRUE(std::isnan(values[1]));
      EXPECT_EQ(values[2], -9999.0);
    };
    if (dtype == LogicalTypeId::Float32)
      check_values(std::get<std::vector<float>>(column.values));
    else check_values(std::get<std::vector<double>>(column.values));
    auto end = cursor.value()->next();
    ASSERT_TRUE(end.ok());
    EXPECT_FALSE(end.value());
  }
  EXPECT_EQ(provider->value_reads, 2u);
  EXPECT_EQ(backend->reads, 0u);
}

TEST_F(Hdf5QueryBindingTest, ValueOnlySlabIpcRetainsDatasetAndUnprojectedAxisMetadata) {
  auto query = request();
  query.subset->slices = {slice(0, 1, 2), slice(1, 2, 4), slice(2, 3, 6)};
  query.limits.target_batch_rows = 4;
  query.limits.target_batch_bytes = 16;
  auto prepared = module->prepare(query);
  ASSERT_TRUE(prepared.ok()) << prepared.status().message();
  auto cursor = module->execute(prepared.value());
  ASSERT_TRUE(cursor.ok()) << cursor.status().message();
  auto batch = cursor.value()->next();
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value());
  auto encoded = ArrowResultEncoder::encode_query_batch(*batch.value(), 1 << 20);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto buffer = std::make_shared<arrow::Buffer>(encoded.value().data(), encoded.value().size());
  auto input = std::make_shared<arrow::io::BufferReader>(buffer);
  auto opened = arrow::ipc::RecordBatchStreamReader::Open(input);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto reader = opened.ValueOrDie();
  ASSERT_EQ(reader->schema()->num_fields(), 1);
  const auto metadata_value = [](const std::shared_ptr<const arrow::KeyValueMetadata>& metadata,
      const std::string& key) -> std::string {
    EXPECT_TRUE(metadata);
    if (!metadata) return {};
    auto value = metadata->Get(key);
    EXPECT_TRUE(value.ok()) << key;
    return value.ok() ? value.ValueOrDie() : std::string{};
  };
  EXPECT_EQ(metadata_value(reader->schema()->field(0)->metadata(),
    "iotdbfs.logical_field_id"), query.subset->projection.front().field_id);
  auto next = reader->ReadNext();
  ASSERT_TRUE(next.ok()) << next.status().ToString();
  ASSERT_TRUE(next.ValueOrDie().batch);
  const auto& record = next.ValueOrDie();
  EXPECT_EQ(record.batch->num_rows(), 3);
  EXPECT_EQ(record.batch->num_columns(), 1); // Axes need no repeated data columns.
  EXPECT_EQ(metadata_value(record.custom_metadata, "iotdbfs.domain.kind"), "dense_grid");
  EXPECT_EQ(metadata_value(record.custom_metadata, "iotdbfs.domain.dimensions"), "3");
  const std::vector<std::string> origins{"1", "2", "3"};
  const std::vector<std::string> shapes{"1", "1", "3"};
  for (std::size_t axis = 0; axis < 3; ++axis) {
    const auto prefix = "iotdbfs.domain.dimension." + std::to_string(axis) + ".";
    EXPECT_EQ(metadata_value(record.custom_metadata, prefix + "field_id"),
      hdf5_axis_field(axis).field_id);
    EXPECT_EQ(metadata_value(record.custom_metadata, prefix + "origin"), origins[axis]);
    EXPECT_EQ(metadata_value(record.custom_metadata, prefix + "shape"), shapes[axis]);
  }
  EXPECT_EQ(metadata_value(record.custom_metadata, "iotdbfs.source.0.path"), "/science/data.h5");
  EXPECT_EQ(metadata_value(record.custom_metadata, "iotdbfs.source.0.version"), "7");
  EXPECT_EQ(metadata_value(record.custom_metadata, "iotdbfs.source.0.format_id"), "hdf5");
  const auto values = std::static_pointer_cast<arrow::FloatArray>(record.batch->column(0));
  EXPECT_FLOAT_EQ(values->Value(0), 123.0f);
  EXPECT_FLOAT_EQ(values->Value(2), 125.0f);
  EXPECT_EQ(values->null_count(), 0);
}

} // namespace
