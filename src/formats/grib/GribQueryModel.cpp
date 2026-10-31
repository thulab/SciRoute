#include "iotdbfs/formats/grib/GribQueryModel.h"

namespace iotdbfs {
namespace {

LogicalType logical_type(LogicalTypeId id, bool nullable = false) {
  return {id, nullable, std::nullopt};
}

std::vector<ComparisonOperator> equality() {
  return {ComparisonOperator::Equal};
}

} // namespace

QueryResult<LogicalSchema> make_grib_logical_schema() {
  using namespace grib_query_fields;
  return LogicalSchema::create({
    {kMessageBytes, "Encoded GRIB message", logical_type(LogicalTypeId::Binary),
      FieldRole::Measure, "byte"},
    {kShortName, "GRIB short name", logical_type(LogicalTypeId::Utf8, true),
      FieldRole::Attribute, std::nullopt},
    {kTypeOfLevel, "GRIB level type", logical_type(LogicalTypeId::Utf8, true),
      FieldRole::Attribute, std::nullopt},
    {kLevel, "GRIB level", logical_type(LogicalTypeId::Int64, true),
      FieldRole::Attribute, std::nullopt},
    {kStep, "GRIB forecast step", logical_type(LogicalTypeId::Int64, true),
      FieldRole::Attribute, std::nullopt},
    {kStepRange, "GRIB forecast step range",
      logical_type(LogicalTypeId::Utf8, true), FieldRole::Attribute,
      std::nullopt},
    {kEdition, "GRIB edition", logical_type(LogicalTypeId::Int32),
      FieldRole::Attribute, std::nullopt},
    {kUnit, "GRIB value unit", logical_type(LogicalTypeId::Utf8, true),
      FieldRole::Attribute, std::nullopt},
    {kParamId, "GRIB parameter ID", logical_type(LogicalTypeId::Int64, true),
      FieldRole::Attribute, std::nullopt},
    {kValidityDate, "GRIB validity date (YYYYMMDD)",
      logical_type(LogicalTypeId::Int64, true), FieldRole::Attribute,
      std::nullopt},
    {kValidityTime, "GRIB validity time (HHMM)",
      logical_type(LogicalTypeId::Int64, true), FieldRole::Attribute,
      std::nullopt},
    {kOffset, "GRIB message byte offset", logical_type(LogicalTypeId::UInt64),
      FieldRole::Attribute, "byte"},
    {kLatitude, "Latitude", logical_type(LogicalTypeId::Float64),
      FieldRole::Dimension, "degree_north"},
    {kLongitude, "Longitude", logical_type(LogicalTypeId::Float64),
      FieldRole::Dimension, "degree_east"},
    {kValue, "GRIB value", logical_type(LogicalTypeId::Float32),
      FieldRole::Measure,
      std::nullopt},
  });
}

QueryResult<QueryCapabilities> make_grib_query_capabilities() {
  auto schema = make_grib_logical_schema();
  if (!schema.ok())
    return QueryResult<QueryCapabilities>::failure(schema.status());
  using namespace grib_query_fields;
  std::vector<FieldQueryCapability> fields{
    {{kMessageBytes}, true, {}, PushdownLevel::None, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::BinaryView}},
    {{kShortName}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kTypeOfLevel}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kLevel}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kStep}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kStepRange}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kEdition}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kUnit}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kParamId}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kValidityDate}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kValidityTime}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kOffset}, true, equality(), PushdownLevel::Exact, false,
      PushdownLevel::None, {}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kLatitude}, true, {}, PushdownLevel::None, true,
      PushdownLevel::Exact, {}, true, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kLongitude}, true, {}, PushdownLevel::None, true,
      PushdownLevel::Exact, {}, true, false,
      {QueryFragment::ResultMode::LogicalValues}},
    {{kValue}, true, {}, PushdownLevel::None, false,
      PushdownLevel::None,
      {LogicalTypeId::Float32, LogicalTypeId::Float64}, false, false,
      {QueryFragment::ResultMode::LogicalValues}},
  };
  return QueryResult<QueryCapabilities>::success(QueryCapabilities(
    1, "grib", "typed-query-1", std::move(schema.value()),
    std::move(fields), {DomainKind::DenseGrid}, false, false,
    OrderingGuarantee::Domain,
    {QueryFragment::ResultMode::LogicalValues,
     QueryFragment::ResultMode::BinaryView},
    {QueryFragment::InterpolationMethod::Bilinear}));
}

} // namespace iotdbfs
