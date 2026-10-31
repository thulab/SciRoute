#include "iotdbfs/query/core/QueryModel.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>

using namespace iotdbfs;

namespace {

LogicalType type(LogicalTypeId id, bool nullable = false) {
  return LogicalType{ id, nullable, std::nullopt };
}

LogicalSchema scientific_schema() {
  auto result = LogicalSchema::create({
    {"source.path", "Source path", type(LogicalTypeId::Utf8),
      FieldRole::SourceIdentity, std::nullopt},
    {"domain.latitude", "Latitude", type(LogicalTypeId::Float64),
      FieldRole::Dimension, "degree_north"},
    {"domain.time", "Time",
      {LogicalTypeId::TimestampNs, false, "UTC"},
      FieldRole::Dimension, "ns"},
    {"data.temperature", "Temperature",
      type(LogicalTypeId::Float32, true), FieldRole::Measure, "K"},
    {"data.quality", "Quality", type(LogicalTypeId::Int32),
      FieldRole::Attribute, std::nullopt},
  });
  EXPECT_TRUE(result.ok()) << result.status().message();
  return result.value();
}

} // namespace

TEST(QueryErrorModel, PreservesPhaseContextAndRetryability) {
  QueryErrorDetail detail;
  detail.source = QuerySourceReference{"/data/a.arrow", 42, "arrow-ipc"};
  detail.field_id = "data.temperature";
  detail.operation = "scan";
  detail.retriable = true;

  const auto status = QueryStatus::failure(QueryError::ReadFailure,
    QueryPhase::Storage, "range read failed", detail);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), QueryError::ReadFailure);
  EXPECT_EQ(status.phase(), QueryPhase::Storage);
  EXPECT_EQ(status.message(), "range read failed");
  ASSERT_TRUE(status.detail().source);
  EXPECT_EQ(status.detail().source->path, "/data/a.arrow");
  EXPECT_EQ(status.detail().source->version, 42);
  EXPECT_TRUE(status.detail().retriable);

  const auto invalid_failure = QueryStatus::failure(QueryError::None,
    QueryPhase::Planning, "not an error");
  EXPECT_EQ(invalid_failure.code(), QueryError::Internal);

  const auto invalid_result = QueryResult<int>::failure(QueryStatus::success());
  EXPECT_FALSE(invalid_result.ok());
  EXPECT_EQ(invalid_result.status().code(), QueryError::Internal);
}

TEST(LogicalSchema, DefinesStableTypedFields) {
  const auto schema = scientific_schema();
  ASSERT_EQ(schema.fields().size(), 5u);
  ASSERT_NE(schema.find("domain.time"), nullptr);
  EXPECT_EQ(schema.find("domain.time")->type.id, LogicalTypeId::TimestampNs);
  EXPECT_EQ(schema.find("domain.time")->type.timezone, "UTC");
  EXPECT_EQ(schema.index_of("data.temperature"), 3u);
  EXPECT_EQ(schema.find("temperature"), nullptr);
}

TEST(LogicalSchema, RejectsDuplicateFieldsAndInvalidTypeAnnotations) {
  auto duplicate = LogicalSchema::create({
    {"data.value", "", type(LogicalTypeId::Float64), FieldRole::Measure, {}},
    {"data.value", "", type(LogicalTypeId::Float64), FieldRole::Measure, {}},
  });
  ASSERT_FALSE(duplicate.ok());
  EXPECT_EQ(duplicate.status().code(), QueryError::SchemaMismatch);

  auto wrong_timezone = LogicalSchema::create({
    {"data.value", "", {LogicalTypeId::Float64, false, "UTC"},
      FieldRole::Measure, {}},
  });
  EXPECT_FALSE(wrong_timezone.ok());

  auto empty_timezone = LogicalSchema::create({
    {"domain.time", "", {LogicalTypeId::TimestampNs, false, ""},
      FieldRole::Dimension, {}},
  });
  EXPECT_FALSE(empty_timezone.ok());
}

TEST(TypedExpression, AcceptsTypedComparisonsSetsNullChecksAndBooleanTrees) {
  const auto schema = scientific_schema();
  auto temperature = make_comparison({"data.temperature"},
    ComparisonOperator::GreaterEqual, LogicalValue::float32(273.15F));
  auto quality = make_in({"data.quality"},
    {LogicalValue::int32(0), LogicalValue::int32(1)});
  auto present = make_is_null({"data.temperature"}, true);
  auto expression = make_logical(LogicalOperator::And,
    {temperature, quality, present});

  const auto status = validate_expression(expression, schema);
  EXPECT_TRUE(status.ok()) << status.message();
}

TEST(TypedExpression, RejectsUnknownFieldsTypeCoercionNullLiteralsAndBadArity) {
  const auto schema = scientific_schema();

  auto unknown = validate_expression(make_comparison({"data.missing"},
    ComparisonOperator::Equal, LogicalValue::float32(1.0F)), schema);
  EXPECT_EQ(unknown.code(), QueryError::UnknownField);

  auto coerced = validate_expression(make_comparison({"data.quality"},
    ComparisonOperator::Equal, LogicalValue::int64(1)), schema);
  EXPECT_EQ(coerced.code(), QueryError::TypeMismatch);

  auto null_literal = validate_expression(make_comparison(
    {"data.temperature"}, ComparisonOperator::Equal,
    LogicalValue::null(LogicalTypeId::Float32)), schema);
  EXPECT_EQ(null_literal.code(), QueryError::InvalidQuery);

  auto bad_not = validate_expression(make_logical(LogicalOperator::Not,
    {make_is_null({"data.temperature"}),
     make_is_null({"data.temperature"}, true)}), schema);
  EXPECT_EQ(bad_not.code(), QueryError::InvalidQuery);

  auto empty_in = validate_expression(make_in({"data.quality"}, {}), schema);
  EXPECT_EQ(empty_in.code(), QueryError::InvalidQuery);
}

TEST(TypedExpression, RejectsUnorderedAndNonFiniteComparisons) {
  auto schema_result = LogicalSchema::create({
    {"data.flag", "", type(LogicalTypeId::Boolean), FieldRole::Attribute, {}},
    {"data.value", "", type(LogicalTypeId::Float64), FieldRole::Measure, {}},
  });
  ASSERT_TRUE(schema_result.ok());

  auto unordered = validate_expression(make_comparison({"data.flag"},
    ComparisonOperator::Less, LogicalValue::boolean(true)),
    schema_result.value());
  EXPECT_EQ(unordered.code(), QueryError::UnsupportedCapability);

  auto not_finite = validate_expression(make_comparison({"data.value"},
    ComparisonOperator::Equal,
    LogicalValue::float64(std::numeric_limits<double>::quiet_NaN())),
    schema_result.value());
  EXPECT_EQ(not_finite.code(), QueryError::InvalidQuery);
}

TEST(DomainSlice, UsesNamedTypedDimensionsWithoutAssumingTwoDimensions) {
  const auto schema = scientific_schema();
  DomainSlice latitude{{"domain.latitude"},
    DomainBound{LogicalValue::float64(20.0), BoundKind::Inclusive},
    DomainBound{LogicalValue::float64(40.0), BoundKind::Exclusive}};
  DomainSlice time_window{{"domain.time"},
    DomainBound{LogicalValue::timestamp_ns(100), BoundKind::Inclusive},
    DomainBound{LogicalValue::timestamp_ns(200), BoundKind::Exclusive}};

  EXPECT_TRUE(validate_domain_slice(latitude, schema).ok());
  EXPECT_TRUE(validate_domain_slice(time_window, schema).ok());
}

TEST(DomainSlice, RejectsMeasuresWrongTypesAndReversedBounds) {
  const auto schema = scientific_schema();
  DomainSlice measure{{"data.temperature"},
    DomainBound{LogicalValue::float32(1.0F), BoundKind::Inclusive}, {}};
  EXPECT_EQ(validate_domain_slice(measure, schema).code(),
    QueryError::InvalidDomainSlice);

  DomainSlice wrong_type{{"domain.latitude"},
    DomainBound{LogicalValue::int64(20), BoundKind::Inclusive}, {}};
  EXPECT_EQ(validate_domain_slice(wrong_type, schema).code(),
    QueryError::TypeMismatch);

  DomainSlice reversed{{"domain.latitude"},
    DomainBound{LogicalValue::float64(40.0), BoundKind::Inclusive},
    DomainBound{LogicalValue::float64(20.0), BoundKind::Inclusive}};
  EXPECT_EQ(validate_domain_slice(reversed, schema).code(),
    QueryError::InvalidDomainSlice);
}

TEST(LogicalArray, ValidatesStorageTypeAndNullBitmap) {
  LogicalArray nullable{{LogicalTypeId::Float32, true, {}},
    std::vector<float>{1.0F, 2.0F, 3.0F}, {0b00000101}};
  EXPECT_TRUE(validate_logical_array(nullable).ok());
  EXPECT_EQ(logical_array_length(nullable), 3u);

  LogicalArray wrong_storage{{LogicalTypeId::Float64, false, {}},
    std::vector<float>{1.0F}, {}};
  EXPECT_EQ(validate_logical_array(wrong_storage).code(),
    QueryError::TypeMismatch);

  LogicalArray null_in_required{{LogicalTypeId::Int32, false, {}},
    std::vector<std::int32_t>{1, 2}, {0b00000001}};
  EXPECT_EQ(validate_logical_array(null_in_required).code(),
    QueryError::SchemaMismatch);

  LogicalArray wrong_bitmap{{LogicalTypeId::Int32, true, {}},
    std::vector<std::int32_t>(9), {0xff}};
  EXPECT_EQ(validate_logical_array(wrong_bitmap).code(),
    QueryError::SchemaMismatch);
}

TEST(QueryBatch, CouplesSchemaColumnsShapeAndProvenance) {
  auto schema_result = LogicalSchema::create({
    {"domain.latitude", "Latitude", type(LogicalTypeId::Float64),
      FieldRole::Dimension, "degree_north"},
    {"data.temperature", "Temperature",
      type(LogicalTypeId::Float32, true), FieldRole::Measure, "K"},
  });
  ASSERT_TRUE(schema_result.ok());

  std::vector<LogicalArray> columns{
    {type(LogicalTypeId::Float64),
      std::vector<double>{20.0, 20.0, 21.0, 21.0}, {}},
    {type(LogicalTypeId::Float32, true),
      std::vector<float>{280.0F, 281.0F, 282.0F, 283.0F}, {0b00001111}},
  };
  DomainDescriptor grid{DomainKind::DenseGrid,
    {{{"domain.latitude"}, type(LogicalTypeId::Float64), "degree_north"},
     {{"domain.longitude"}, type(LogicalTypeId::Float64), "degree_east"}},
    {2, 2}, {0, 0}};
  auto batch = QueryBatch::create(schema_result.value(), 4,
    std::move(columns), grid,
    {{"/forecast/a.grib2", 7, "grib"}});

  ASSERT_TRUE(batch.ok()) << batch.status().message();
  EXPECT_EQ(batch.value().row_count(), 4u);
  EXPECT_EQ(batch.value().columns().size(), 2u);
  ASSERT_TRUE(batch.value().domain());
  EXPECT_EQ(batch.value().domain()->kind, DomainKind::DenseGrid);
  ASSERT_EQ(batch.value().sources().size(), 1u);
  EXPECT_EQ(batch.value().sources()[0].format_id, "grib");
}

TEST(QueryBatch, RejectsColumnAndDomainShapeMismatches) {
  auto schema_result = LogicalSchema::create({
    {"data.value", "Value", type(LogicalTypeId::Float64),
      FieldRole::Measure, {}},
  });
  ASSERT_TRUE(schema_result.ok());

  auto short_column = QueryBatch::create(schema_result.value(), 2,
    {{type(LogicalTypeId::Float64), std::vector<double>{1.0}, {}}});
  ASSERT_FALSE(short_column.ok());
  EXPECT_EQ(short_column.status().code(), QueryError::SchemaMismatch);

  DomainDescriptor wrong_shape{DomainKind::Rows,
    {{{"domain.row"}, type(LogicalTypeId::UInt64), {}}}, {3}, {0}};
  auto wrong_domain = QueryBatch::create(schema_result.value(), 2,
    {{type(LogicalTypeId::Float64), std::vector<double>{1.0, 2.0}, {}}},
    wrong_shape);
  ASSERT_FALSE(wrong_domain.ok());
  EXPECT_EQ(wrong_domain.status().code(), QueryError::SchemaMismatch);
}

TEST(QueryBatch, EmptyDomainDoesNotOverflowBeforeItsTrailingZeroExtent) {
  auto schema = LogicalSchema::create({
    {"data.value", "Value", type(LogicalTypeId::Float32), FieldRole::Measure, {}},
  });
  ASSERT_TRUE(schema.ok());
  const auto huge = std::numeric_limits<std::uint64_t>::max();
  DomainDescriptor empty{DomainKind::DenseGrid,
    {{{"axis.0"}, type(LogicalTypeId::UInt64), {}},
     {{"axis.1"}, type(LogicalTypeId::UInt64), {}},
     {{"axis.2"}, type(LogicalTypeId::UInt64), {}}},
    {huge, huge, 0}, {0, 0, 0}};
  auto batch = QueryBatch::create(schema.value(), 0,
    {{type(LogicalTypeId::Float32), std::vector<float>{}, {}}}, empty);
  ASSERT_TRUE(batch.ok()) << batch.status().message();
  ASSERT_TRUE(batch.value().domain());
  EXPECT_EQ(batch.value().domain()->shape, empty.shape);

  empty.dimensions[2].type.nullable = true;
  auto malformed = QueryBatch::create(schema.value(), 0,
    {{type(LogicalTypeId::Float32), std::vector<float>{}, {}}}, empty);
  EXPECT_EQ(malformed.status().code(), QueryError::SchemaMismatch);

  empty.dimensions[2].type.nullable = false;
  empty.shape[2] = 1;
  auto overflow = QueryBatch::create(schema.value(), 0,
    {{type(LogicalTypeId::Float32), std::vector<float>{}, {}}}, empty);
  EXPECT_EQ(overflow.status().code(), QueryError::SchemaMismatch);
}
