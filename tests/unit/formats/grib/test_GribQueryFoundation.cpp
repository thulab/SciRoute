#include "iotdbfs/formats/grib/GribLayout.h"
#include "iotdbfs/formats/grib/GribQueryKernel.h"

#include <gtest/gtest.h>

#include <eccodes.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

using namespace iotdbfs;

namespace {

class MemoryQuerySource final : public QuerySource {
public:
  explicit MemoryQuerySource(StructuredBlobBytes payload,
      StructuredBlobBytes layout = {})
    : payload_(std::move(payload)), layout_(std::move(layout)) {
    manifest_.key = {"/typed/grid.grib2", 11};
    manifest_.format_id = "grib";
    manifest_.payload_size = payload_.size();
    manifest_.layout_size = layout_.size();
    manifest_.phase = StructuredBlobPhase::Published;
    manifest_.published = true;
  }

  const StructuredBlobManifest& manifest() const override {
    return manifest_;
  }

  QueryResult<StructuredBlobBytes> read_payload(std::uint64_t offset,
      std::uint64_t length) const override {
    if (offset > payload_.size() || length > payload_.size() - offset)
      return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage, "test read failed"));
    const auto begin = payload_.begin() + static_cast<std::size_t>(offset);
    return QueryResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }

  QueryResult<StructuredBlobBytes> read_layout(std::uint64_t offset,
      std::uint64_t length) const override {
    if (offset > layout_.size() || length > layout_.size() - offset)
      return QueryResult<StructuredBlobBytes>::failure(QueryStatus::failure(
        QueryError::ReadFailure, QueryPhase::Storage, "test read failed"));
    const auto begin = layout_.begin() + static_cast<std::size_t>(offset);
    return QueryResult<StructuredBlobBytes>::success(
      {begin, begin + static_cast<std::size_t>(length)});
  }

private:
  StructuredBlobManifest manifest_;
  StructuredBlobBytes payload_;
  StructuredBlobBytes layout_;
};

StructuredBlobBytes regular_grid_message() {
  std::unique_ptr<codes_handle, decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr, "regular_ll_sfc_grib2"),
    codes_handle_delete);
  EXPECT_NE(handle, nullptr);
  if (!handle) return {};
  EXPECT_EQ(codes_set_long(handle.get(), "Ni", 2), CODES_SUCCESS);
  EXPECT_EQ(codes_set_long(handle.get(), "Nj", 2), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "latitudeOfFirstGridPointInDegrees", 1), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "latitudeOfLastGridPointInDegrees", 0), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "longitudeOfFirstGridPointInDegrees", 0), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "longitudeOfLastGridPointInDegrees", 1), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "iDirectionIncrementInDegrees", 1), CODES_SUCCESS);
  EXPECT_EQ(codes_set_double(handle.get(),
    "jDirectionIncrementInDegrees", 1), CODES_SUCCESS);
  const double values[]{1, 3, 5, 7};
  EXPECT_EQ(codes_set_double_array(handle.get(), "values", values, 4),
    CODES_SUCCESS);
  const void* data = nullptr;
  std::size_t size = 0;
  EXPECT_EQ(codes_get_message(handle.get(), &data, &size), CODES_SUCCESS);
  const auto* begin = static_cast<const std::uint8_t*>(data);
  return {begin, begin + size};
}

StructuredBlobBytes bytes(std::string value) {
  return {value.begin(), value.end()};
}

} // namespace

TEST(GribQueryModel, FreezesSchemaAndFirstCapabilities) {
  auto schema = make_grib_logical_schema();
  ASSERT_TRUE(schema.ok()) << schema.status().message();
  std::vector<std::string> field_ids;
  for (const auto& field : schema.value().fields())
    field_ids.push_back(field.field_id);
  EXPECT_EQ(field_ids, (std::vector<std::string>{
    grib_query_fields::kMessageBytes,
    grib_query_fields::kShortName,
    grib_query_fields::kTypeOfLevel,
    grib_query_fields::kLevel,
    grib_query_fields::kStep,
    grib_query_fields::kStepRange,
    grib_query_fields::kEdition,
    grib_query_fields::kUnit,
    grib_query_fields::kParamId,
    grib_query_fields::kValidityDate,
    grib_query_fields::kValidityTime,
    grib_query_fields::kOffset,
    grib_query_fields::kLatitude,
    grib_query_fields::kLongitude,
    grib_query_fields::kValue,
  }));
  ASSERT_NE(schema.value().find(grib_query_fields::kValue), nullptr);
  EXPECT_EQ(schema.value().find(grib_query_fields::kValue)->type.id,
    LogicalTypeId::Float32);
  EXPECT_EQ(schema.value().find(grib_query_fields::kLatitude)->role,
    FieldRole::Dimension);

  auto capabilities = make_grib_query_capabilities();
  ASSERT_TRUE(capabilities.ok()) << capabilities.status().message();
  EXPECT_TRUE(validate_query_capabilities(capabilities.value()).ok());
  EXPECT_EQ(capabilities.value().adapter_id(), "grib");
  EXPECT_FALSE(capabilities.value().sequential_scan_available());
  EXPECT_FALSE(capabilities.value().layout_optional());
  EXPECT_EQ(capabilities.value().result_modes().size(), 2u);
  EXPECT_EQ(capabilities.value().interpolation_methods().size(), 1u);
  EXPECT_EQ(capabilities.value().domains(),
    (std::vector<DomainKind>{DomainKind::DenseGrid}));
  const auto capability_for = [&](const char* field_id)
      -> const FieldQueryCapability* {
    const auto& fields = capabilities.value().fields();
    const auto found = std::find_if(fields.begin(), fields.end(),
      [&](const auto& capability) {
        return capability.field.field_id == field_id;
      });
    return found == fields.end() ? nullptr : &*found;
  };
  ASSERT_NE(capability_for(grib_query_fields::kShortName), nullptr);
  EXPECT_EQ(capability_for(grib_query_fields::kShortName)
      ->predicate_operators,
    (std::vector<ComparisonOperator>{ComparisonOperator::Equal}));
  EXPECT_EQ(capability_for(grib_query_fields::kShortName)->result_modes,
    (std::vector<QueryFragment::ResultMode>{
      QueryFragment::ResultMode::LogicalValues}));
  for (const char* field : {grib_query_fields::kParamId,
      grib_query_fields::kValidityDate,
      grib_query_fields::kValidityTime,
      grib_query_fields::kOffset}) {
    ASSERT_NE(capability_for(field), nullptr);
    EXPECT_EQ(capability_for(field)->predicate_operators,
      (std::vector<ComparisonOperator>{ComparisonOperator::Equal}));
  }
  ASSERT_NE(capability_for(grib_query_fields::kMessageBytes), nullptr);
  EXPECT_EQ(capability_for(grib_query_fields::kMessageBytes)->result_modes,
    (std::vector<QueryFragment::ResultMode>{
      QueryFragment::ResultMode::BinaryView}));
  ASSERT_NE(capability_for(grib_query_fields::kLatitude), nullptr);
  EXPECT_TRUE(capability_for(grib_query_fields::kLatitude)->sliceable);
  EXPECT_TRUE(capability_for(grib_query_fields::kLatitude)->interpolatable);
  ASSERT_NE(capability_for(grib_query_fields::kValue), nullptr);
  EXPECT_EQ(capability_for(grib_query_fields::kValue)->output_types,
    (std::vector<LogicalTypeId>{LogicalTypeId::Float32,
      LogicalTypeId::Float64}));
}

TEST(GribTypedQuery, MapsBinaryDtypeAndInterpolationWithoutRawStrings) {
  QueryFragment binary;
  binary.projection = {{grib_query_fields::kMessageBytes}};
  binary.output.mode = QueryFragment::ResultMode::BinaryView;
  auto binary_plan = plan_grib_typed_output(binary);
  ASSERT_TRUE(binary_plan.ok()) << binary_plan.status().message();
  EXPECT_EQ(binary_plan.value().kind, GribResultKind::SourceMessage);

  QueryFragment grid;
  grid.projection = {{grib_query_fields::kValue}};
  grid.output.type_overrides = {
    {{grib_query_fields::kValue}, LogicalTypeId::Float64}};
  QueryFragment::GridInterpolation interpolation;
  interpolation.axes = {
    {{grib_query_fields::kLatitude}, 3, std::nullopt},
    {{grib_query_fields::kLongitude}, 4, std::nullopt},
  };
  interpolation.border = QueryFragment::BorderMode::NaN;
  grid.output.interpolation = interpolation;
  auto grid_plan = plan_grib_typed_output(grid);
  ASSERT_TRUE(grid_plan.ok()) << grid_plan.status().message();
  EXPECT_EQ(grid_plan.value().value_type, grib_utils::DataDType::F64);
  EXPECT_EQ(grid_plan.value().interpolation.outNj, 3);
  EXPECT_EQ(grid_plan.value().interpolation.outNi, 4);
  EXPECT_EQ(grid_plan.value().interpolation.border,
    grib_utils::InterpParams::Border::AS_NAN);
}

TEST(GribLayout, RequiresExplicitSupportedVersion) {
  auto parsed = parse_grib_layout(bytes(grib_layout_header() +
    "{\"of\":0,\"ln\":12}\n"));
  ASSERT_TRUE(parsed.ok()) << parsed.status().message();
  EXPECT_EQ(parsed.value().version, kGribLayoutVersion);
  EXPECT_EQ(parsed.value().records, "{\"of\":0,\"ln\":12}\n");

  EXPECT_EQ(parse_grib_layout(bytes("{\"of\":0}\n")).status().code(),
    QueryError::CorruptLayout);
  EXPECT_EQ(parse_grib_layout(bytes("IOTDBFS-GRIB-LAYOUT/99\n"))
    .status().code(), QueryError::UnsupportedLayoutVersion);

  MemoryQuerySource source({}, bytes(grib_layout_header() +
    "{\"of\":0,\"ln\":12}\n"));
  auto loaded = GribQueryKernel::read_layout(source);
  ASSERT_TRUE(loaded.ok()) << loaded.status().message();
  EXPECT_EQ(loaded.value().version, kGribLayoutVersion);
}

TEST(GribQueryKernel, ReturnsTypedGridShapeCoordinatesAndMessageBytes) {
  auto message = regular_grid_message();
  ASSERT_FALSE(message.empty());
  MemoryQuerySource source(message);
  grib_utils::MsgInfo info{0, message.size(), true, 2, 2, 4};

  auto binary = GribQueryKernel::read_source_message(source, info);
  ASSERT_TRUE(binary.ok()) << binary.status().message();
  EXPECT_EQ(binary.value(), message);

  GribGridPlan plan;
  plan.message = info;
  plan.value_type = grib_utils::DataDType::F32;
  plan.cache_token = "typed-grid-foundation";
  auto grid = GribQueryKernel::decode_grid(source, plan);
  ASSERT_TRUE(grid.ok()) << grid.status().message();
  EXPECT_EQ(grid.value().values.type.id, LogicalTypeId::Float32);
  EXPECT_EQ(std::get<std::vector<float>>(grid.value().values.values),
    (std::vector<float>{1, 3, 5, 7}));
  EXPECT_EQ(grid.value().domain.shape,
    (std::vector<std::uint64_t>{2, 2}));
  EXPECT_EQ(grid.value().latitude_axis,
    (std::vector<double>{1, 0}));
  EXPECT_EQ(grid.value().longitude_axis,
    (std::vector<double>{0, 1}));
}

TEST(GribQueryKernel, ReturnsStructuredFailuresInsteadOfEmptyByteResults) {
  MemoryQuerySource source({1, 2, 3});
  grib_utils::MsgInfo invalid;
  EXPECT_EQ(GribQueryKernel::read_source_message(source, invalid)
      .status().code(),
    QueryError::CorruptLayout);

  grib_utils::MsgInfo unreadable{0, 4, true, 0, 0, 0};
  EXPECT_EQ(GribQueryKernel::read_source_message(source, unreadable)
      .status().code(),
    QueryError::ReadFailure);

  GribGridPlan plan;
  plan.message = {0, 3, true, 0, 0, 0};
  plan.cache_token = "typed-grid-corrupt-payload";
  EXPECT_EQ(GribQueryKernel::decode_grid(source, plan).status().code(),
    QueryError::CorruptPayload);
}
