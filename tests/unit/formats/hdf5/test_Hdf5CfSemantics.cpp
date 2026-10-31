#include "Hdf5CfTestFixture.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5SemanticBinding.h"
#include <gtest/gtest.h>
using hdf5_cf_test::fixture;
#include <cmath>
#include <cstring>
#include <fstream>
#include <hdf5.h>
#include <unistd.h>

using namespace iotdbfs;
namespace {
Hdf5Attribute txt(std::string name, std::string value) {
  Hdf5Attribute a;
  a.name = std::move(name);
  a.type = LogicalTypeId::Utf8;
  a.state = Hdf5AttributeState::Complete;
  a.native_type_class = 3;
  a.native_element_bytes = value.size() + 1;
  a.strings = {std::move(value)};
  return a;
}
Hdf5Attribute num(std::string name, double value) {
  Hdf5Attribute a;
  a.name = std::move(name);
  a.type = LogicalTypeId::Float64;
  a.state = Hdf5AttributeState::Complete;
  a.native_type_class = 1;
  a.native_element_bytes = 8;
  std::uint64_t bits;
  std::memcpy(&bits, &value, 8);
  for (unsigned i = 0; i < 8; ++i)
    a.values.push_back(bits >> (i * 8));
  return a;
}
Hdf5LayoutDocument document() {
  Hdf5LayoutDocument d;
  d.source_size = 4096;
  d.root_object_id = "root";
  Hdf5LayoutObject root;
  root.id = "root";
  root.kind = Hdf5ObjectKind::Group;
  root.canonical_path = "/";
  root.attributes = {txt("Conventions", "CF-1.6, ACDD-1.3")};
  root.attribute_count = 1;
  d.objects.push_back(root);
  auto dataset = [&](std::string id, std::vector<std::uint64_t> shape,
                     LogicalTypeId type) {
    Hdf5LayoutObject o;
    o.id = id;
    o.kind = Hdf5ObjectKind::Dataset;
    o.canonical_path = "/" + id;
    Hdf5DatasetLayout x;
    x.descriptor = {o.canonical_path, type, shape, true, {}};
    x.element_bytes = type == LogicalTypeId::Int32 ? 2 : 8;
    x.native_type_class = type == LogicalTypeId::Int32 ? 0 : 1;
    x.storage = Hdf5StorageLayout::Contiguous;
    x.byte_order = Hdf5ByteOrder::Little;
    x.fill_status = Hdf5FillStatus::Default;
    x.fill_value.resize(x.element_bytes);
    x.index_complete = true;
    x.contiguous_offset = 64;
    x.contiguous_size = x.element_bytes;
    for (auto n : shape)
      x.contiguous_size *= n;
    o.dataset = x;
    o.axis_associations_complete = true;
    o.axis_associations_reason.clear();
    return o;
  };
  auto data = dataset("data", {1, 1, 3, 4}, LogicalTypeId::Int32);
  data.attributes = {txt("units", "Celsius"), num("scale_factor", .01),
                     num("add_offset", 2),    num("_FillValue", -999),
                     num("valid_min", -300),  num("valid_max", 4500)};
  data.attributes[1].shape = {1};
  data.attribute_count = data.attributes.size();
  data.axis_associations = {{2, "latitude", "forward_dimension_list/1"},
                            {3, "longitude", "forward_dimension_list/1"}};
  d.objects.push_back(data);
  auto lat = dataset("latitude", {3}, LogicalTypeId::Float64);
  lat.attributes = {txt("CLASS", "DIMENSION_SCALE"),
                    txt("units", "degrees_north")};
  lat.attribute_count = 2;
  lat.coordinate_index =
      Hdf5CoordinateIndex{"latitude", "degrees_north", "", {2, 1, 0}, false};
  lat.coordinate_index_reason.clear();
  d.objects.push_back(lat);
  auto lon = dataset("longitude", {4}, LogicalTypeId::Float64);
  lon.attributes = {txt("CLASS", "DIMENSION_SCALE"),
                    txt("units", "degrees_east")};
  lon.attribute_count = 2;
  lon.coordinate_index = Hdf5CoordinateIndex{
      "longitude", "degrees_east", "", {0, 90, 180, 270}, true};
  lon.coordinate_index_reason.clear();
  d.objects.push_back(lon);
  for (unsigned i = 1; i < d.objects.size(); ++i)
    d.links.push_back(
        {"root", d.objects[i].id, Hdf5LinkKind::Hard, d.objects[i].id, {}});
  return d;
}
QueryResult<Hdf5LayoutDocument> inventory(const BinaryValue &bytes,
                                          Hdf5LayoutLimits limits = {}) {
  FormatSource source{
      bytes.size(),
      [&](std::uint64_t o, std::uint64_t n) -> std::optional<BinaryValue> {
        if (o > bytes.size() || n > bytes.size() - o)
          return {};
        return BinaryValue(bytes.begin() + o, bytes.begin() + o + n);
      }};
  auto result = QueryResult<Hdf5LayoutDocument>::failure(QueryStatus::failure(
      QueryError::Internal, QueryPhase::Storage, "not called"));
  auto status = with_hdf5_ingestion_source(source, {}, [&](const auto &driver) {
    result = driver.inventory(limits);
    return result.ok() ? QueryStatus::success() : result.status();
  });
  if (!status.ok())
    return QueryResult<Hdf5LayoutDocument>::failure(status);
  return result;
}
} // namespace
TEST(Hdf5CfSemantics,
     CompilesDescendingCoordinateAndScalarOrSingletonTransform) {
  auto d = document();
  auto b = compile_hdf5_cf_binding(d, "data");
  ASSERT_TRUE(b.ok()) << b.status().message();
  ASSERT_EQ(b.value().axes.size(), 2);
  EXPECT_FALSE(b.value().axes[0].ascending);
  EXPECT_DOUBLE_EQ(b.value().scale_factor, .01);
  EXPECT_DOUBLE_EQ(b.value().add_offset, 2);
  EXPECT_EQ(b.value().missing_values, (std::vector<double>{-999}));
}
TEST(Hdf5CfSemantics, RequiresConventionAndForwardEvidence) {
  auto d = document();
  d.objects[0].attributes.clear();
  EXPECT_FALSE(compile_hdf5_cf_binding(d, "data").ok());
  d = document();
  d.objects[1].axis_associations_complete = false;
  d.objects[1].axis_associations_reason = "unavailable";
  EXPECT_FALSE(compile_hdf5_cf_binding(d, "data").ok());
}
TEST(Hdf5CfSemantics, GeosPressureProfileRequiresVerifiedFourAxisEvidence) {
  auto d = document();
  d.objects[0].attributes = {txt("Conventions", "CF-1"),
                             txt("Source", "GEOSadas-5_7_2"),
                             txt("VersionID", "5.7.2")};
  d.objects[0].attribute_count = d.objects[0].attributes.size();
  auto &data = d.objects[1];
  data.dataset->descriptor.value_type = LogicalTypeId::Float32;
  data.dataset->descriptor.shape = {1, 3, 3, 4};
  data.dataset->native_type_class = 1;
  data.dataset->element_bytes = 4;
  data.dataset->fill_value.resize(4);
  data.dataset->contiguous_size = 1 * 3 * 3 * 4 * 4;
  data.attributes = {txt("standard_name", "air_temperature"),
                     txt("units", "K")};
  data.attribute_count = data.attributes.size();
  data.axis_associations = {{0, "time", "forward_dimension_list/1"},
                            {1, "pressure", "forward_dimension_list/1"},
                            {2, "latitude", "forward_dimension_list/1"},
                            {3, "longitude", "forward_dimension_list/1"}};
  auto time = d.objects[2];
  time.id = "time";
  time.canonical_path = "/time";
  time.dataset->descriptor.path = "/time";
  time.dataset->descriptor.shape = {1};
  time.dataset->contiguous_size = 8;
  time.attributes = {txt("CLASS", "DIMENSION_SCALE"),
                     txt("units", "minutes since 2011-07-02 09:00:00")};
  time.attribute_count = time.attributes.size();
  time.coordinate_index = Hdf5CoordinateIndex{
      "time", "minutes since 2011-07-02 09:00:00", "standard", {0}, true};
  auto pressure = d.objects[2];
  pressure.id = "pressure";
  pressure.canonical_path = "/pressure";
  pressure.dataset->descriptor.path = "/pressure";
  pressure.dataset->descriptor.value_type = LogicalTypeId::Float32;
  pressure.dataset->descriptor.shape = {3};
  pressure.dataset->element_bytes = 4;
  pressure.dataset->fill_value.resize(4);
  pressure.dataset->contiguous_size = 3 * 4;
  pressure.attributes = {txt("CLASS", "DIMENSION_SCALE"),
                         txt("units", "hPa"), txt("positive", "down"),
                         txt("standard_name", "PLE_level")};
  pressure.attribute_count = pressure.attributes.size();
  pressure.coordinate_index =
      Hdf5CoordinateIndex{"pressure", "hPa", "", {1000, 750, 500}, false};
  d.objects.push_back(std::move(time));
  d.objects.push_back(std::move(pressure));
  d.links.push_back({"root", "time", Hdf5LinkKind::Hard, "time", {}});
  d.links.push_back({"root", "pressure", Hdf5LinkKind::Hard, "pressure", {}});
  EXPECT_TRUE(hdf5_declares_geos_pressure(d));
  auto bound = compile_hdf5_geos_pressure_binding(d, "data");
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  ASSERT_EQ(bound.value().axes.size(), 4U);
  EXPECT_EQ(bound.value().axes[1].role, "pressure");
  EXPECT_EQ(bound.value().axes[1].dimension, 1U);
  EXPECT_EQ(bound.value().axes[1].values[1], 750);
  auto profiles = default_hdf5_semantic_profiles();
  ASSERT_EQ(profiles.size(), 2U);
  EXPECT_TRUE(profiles[0]->declares(d));
  EXPECT_EQ(profiles[0]->id(), "geos572-pressure-grid/1");
  auto encoded = serialize_hdf5_layout(d);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto restored = read_hdf5_layout_directory(
      [&](auto offset, auto length) {
        return QueryResult<BinaryValue>::success({
            encoded.value().begin() + offset,
            encoded.value().begin() + offset + length});
      }, encoded.value().size(), d.source_size);
  ASSERT_TRUE(restored.ok()) << restored.status().message();
  EXPECT_TRUE(compile_hdf5_geos_pressure_binding(
      restored.value().document(), "data").ok());
  EXPECT_EQ(restored.value().protocol_version(), 5U);
  auto forged_previous = encoded.value();
  forged_previous[sizeof(kHdf5LayoutMagic) - 3] = '4';
  auto rejected = read_hdf5_layout_directory(
      [&](auto offset, auto length) {
        return QueryResult<BinaryValue>::success({
            forged_previous.begin() + offset,
            forged_previous.begin() + offset + length});
      }, forged_previous.size(), d.source_size);
  EXPECT_FALSE(rejected.ok());
  d.objects.back().coordinate_index.reset();
  EXPECT_FALSE(compile_hdf5_geos_pressure_binding(d, "data").ok());
  d.objects.back().coordinate_index =
      Hdf5CoordinateIndex{"pressure", "hPa", "", {1000, 750, 500}, false};
  d.objects[1].axis_associations[1].evidence = "reverse_only";
  EXPECT_FALSE(compile_hdf5_geos_pressure_binding(d, "data").ok());
  d.objects[1].axis_associations[1].evidence = "forward_dimension_list/1";
  d.objects[1].canonical_path = "/T";
  d.objects[1].dataset->descriptor.path = "/T";
  ASSERT_TRUE(verified_hdf5_geos_utc_second(d));
  EXPECT_EQ(*verified_hdf5_geos_utc_second(d), 1309597200);
  d.objects[4].coordinate_index->units = "hours since 2011-07-02 09:00:00";
  EXPECT_FALSE(verified_hdf5_geos_utc_second(d));
}
TEST(Hdf5CfSemantics, GeosFileKeepsCfProfileForOtherDatasets) {
  auto d = document();
  d.objects.front().attributes.push_back(txt("Source", "GEOSadas-5_7_2"));
  d.objects.front().attributes.push_back(txt("VersionID", "5.7.2"));
  d.objects.front().attribute_count = d.objects.front().attributes.size();
  auto profiles = default_hdf5_semantic_profiles();
  ASSERT_EQ(profiles.size(), 2);
  ASSERT_TRUE(profiles[0]->declares(d));
  ASSERT_FALSE(profiles[0]->compile(d, "data").ok());
  ASSERT_TRUE(profiles[1]->declares(d));
  auto cf = profiles[1]->compile(d, "data");
  ASSERT_TRUE(cf.ok()) << cf.status().message();
  EXPECT_EQ(cf.value().profile, "cf-rectilinear/1");
}
TEST(Hdf5CfSemantics, RejectsConflictingAndDuplicateAxes) {
  auto d = document();
  d.objects[2].attributes.push_back(txt("axis", "X"));
  EXPECT_FALSE(compile_hdf5_cf_binding(d, "data").ok());
  d = document();
  d.objects[1].axis_associations.push_back(d.objects[1].axis_associations[0]);
  EXPECT_FALSE(compile_hdf5_cf_binding(d, "data").ok());
}
TEST(Hdf5CfSemantics, RejectsCoordinateExtentOrMissingIndex) {
  auto d = document();
  d.objects[2].coordinate_index->values.pop_back();
  EXPECT_FALSE(compile_hdf5_cf_binding(d, "data").ok());
  d = document();
  d.objects[2].coordinate_index.reset();
  EXPECT_FALSE(compile_hdf5_cf_binding(d, "data").ok());
}
TEST(Hdf5CfSemantics, NativeTimeUnitsCalendarAndNoImplicitUTC) {
  auto d = document();
  auto &o = d.objects[2];
  o.attributes = {txt("units", "days since 2000-01-01"),
                  txt("calendar", "360_day")};
  auto c = inspect_hdf5_cf_coordinate(o);
  ASSERT_TRUE(c.ok());
  EXPECT_EQ(c.value().calendar, "360_day");
  o.attributes.pop_back();
  c = inspect_hdf5_cf_coordinate(o);
  ASSERT_TRUE(c.ok());
  EXPECT_EQ(c.value().calendar, "standard");
  o.attributes[0] = txt("units", "months since 2000-01-01");
  EXPECT_FALSE(inspect_hdf5_cf_coordinate(o).ok());
}
TEST(Hdf5CfSemantics, LayoutFourRoundtripAndRejectsNonMonotoneValues) {
  auto d = document();
  auto encoded = serialize_hdf5_layout(d);
  ASSERT_TRUE(encoded.ok()) << encoded.status().message();
  auto read = read_hdf5_layout_directory(
      [&](auto o, auto n) {
        return QueryResult<BinaryValue>::success(
            {encoded.value().begin() + o, encoded.value().begin() + o + n});
      },
      encoded.value().size(), d.source_size);
  ASSERT_TRUE(read.ok()) << read.status().message();
  EXPECT_TRUE(compile_hdf5_cf_binding(read.value().document(), "data").ok());
  d.objects[2].coordinate_index->values = {2, 2, 0};
  EXPECT_FALSE(serialize_hdf5_layout(d).ok());
}
TEST(Hdf5CfSemantics, LayoutCoordinateBudgetPreservesIndependentValidation) {
  auto d = document();
  Hdf5LayoutLimits limits;
  limits.max_coordinate_bytes = 8;
  EXPECT_FALSE(serialize_hdf5_layout(d, limits).ok());
}
TEST(Hdf5CfSemantics, CatalogPreservesSingletonShapeAndUnsupportedState) {
  auto d = document();
  auto record = hdf5_dataset_catalog_record(d.objects[1], "/data");
  ASSERT_EQ(record.attributes.size(), 6);
  EXPECT_EQ(record.attributes[1].shape, (std::vector<std::uint64_t>{1}));
  ASSERT_TRUE(record.attributes[1].value);
  EXPECT_DOUBLE_EQ(std::get<double>(*record.attributes[1].value), .01);
}
TEST(Hdf5CfSemantics, IsolatedNativeForwardReferencesAndCoordinateValues) {
#ifdef __APPLE__
  GTEST_SKIP()
      << "macOS cannot enforce the configured RLIMIT_AS; tested on Linux";
#endif
  auto result = inventory(fixture());
  ASSERT_TRUE(result.ok()) << result.status().message();
  const Hdf5LayoutObject *data = nullptr;
  for (const auto &o : result.value().objects)
    if (o.canonical_path == "/data")
      data = &o;
  ASSERT_NE(data, nullptr);
  EXPECT_EQ(data->dataset->descriptor.value_type, LogicalTypeId::Int32);
  ASSERT_TRUE(data->axis_associations_complete)
      << data->axis_associations_reason;
  auto b = compile_hdf5_cf_binding(result.value(), data->id);
  ASSERT_TRUE(b.ok()) << b.status().message();
  EXPECT_EQ(b.value().axes.size(), 3);
  EXPECT_EQ(b.value().axes[0].calendar, "standard");
  EXPECT_DOUBLE_EQ(b.value().axes[0].values[0], 16801);
  EXPECT_FALSE(b.value().axes[1].ascending);
}
TEST(Hdf5CfSemantics, Int32TimeIsBoundedCoordinateEvidenceOnly) {
#ifdef __APPLE__
  GTEST_SKIP()
      << "macOS cannot enforce the configured RLIMIT_AS; tested on Linux";
#endif
  auto result = inventory(fixture(false, true));
  ASSERT_TRUE(result.ok()) << result.status().message();
  const Hdf5LayoutObject *time = nullptr, *data = nullptr;
  for (const auto &object : result.value().objects) {
    if (object.canonical_path == "/time")
      time = &object;
    if (object.canonical_path == "/data")
      data = &object;
  }
  ASSERT_NE(time, nullptr);
  ASSERT_NE(data, nullptr);
  ASSERT_TRUE(time->coordinate_index) << time->coordinate_index_reason;
  ASSERT_EQ(time->coordinate_index->values.size(), 1);
  EXPECT_DOUBLE_EQ(time->coordinate_index->values[0], 16801);
  EXPECT_FALSE(time->dataset->descriptor.supported);
  auto binding = compile_hdf5_cf_binding(result.value(), data->id);
  ASSERT_TRUE(binding.ok()) << binding.status().message();
  EXPECT_EQ(binding.value().axes.front().role, "time");
}
TEST(Hdf5CfSemantics,
     NonMonotoneCoordinateDisablesScienceButPreservesPhysicalIndex) {
#ifdef __APPLE__
  GTEST_SKIP()
      << "macOS cannot enforce the configured RLIMIT_AS; tested on Linux";
#endif
  auto result = inventory(fixture(true));
  ASSERT_TRUE(result.ok());
  for (const auto &o : result.value().objects)
    if (o.canonical_path == "/data") {
      EXPECT_TRUE(o.dataset->index_complete);
      EXPECT_FALSE(compile_hdf5_cf_binding(result.value(), o.id).ok());
    }
}
TEST(Hdf5CfSemantics, CoordinateLimitPreservesPhysicalIndex) {
#ifdef __APPLE__
  GTEST_SKIP()
      << "macOS cannot enforce the configured RLIMIT_AS; tested on Linux";
#endif
  Hdf5LayoutLimits limits;
  limits.max_coordinate_elements = 1;
  auto result = inventory(fixture(), limits);
  ASSERT_TRUE(result.ok());
  for (const auto &o : result.value().objects)
    if (o.canonical_path == "/data") {
      EXPECT_TRUE(o.dataset->index_complete);
      EXPECT_FALSE(compile_hdf5_cf_binding(result.value(), o.id).ok());
    }
}
