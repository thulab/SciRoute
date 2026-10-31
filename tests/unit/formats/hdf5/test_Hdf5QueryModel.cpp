#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <limits>
#include <set>

using namespace iotdbfs;
namespace {
class Dataset final : public Hdf5DatasetProvider {
public:
  Hdf5DatasetDescriptor data{"/a", LogicalTypeId::Float32, {5, 7, 9}, true, {}};
  mutable unsigned calls{0};
  QueryResult<Hdf5DatasetDescriptor> describe(const std::string& path) const override {
    ++calls;
    if (path != data.path)
      return QueryResult<Hdf5DatasetDescriptor>::failure(QueryStatus::failure(
        QueryError::UnknownField, QueryPhase::Planning, "dataset not found"));
    return QueryResult<Hdf5DatasetDescriptor>::success(data);
  }
};
FieldRef value(const std::string& path = "/a") {
  auto f = make_hdf5_value_field(path);
  EXPECT_TRUE(f.ok());
  return f.ok() ? f.value() : FieldRef{};
}
QueryFragment fragment() {
  QueryFragment f;
  f.projection = {value()};
  return f;
}
DomainSlice interval(unsigned axis, std::uint64_t begin, std::uint64_t end) {
  return {hdf5_axis_field(axis), DomainBound{LogicalValue::uint64(begin), BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(end), BoundKind::Exclusive}};
}
QueryRequest request(QueryFragment f = fragment()) {
  QueryRequest r;
  r.sources.paths = {{PathScopeKind::Exact, "/source.h5"}};
  r.sources.versions = VersionSelection::exact_version(9);
  r.subset = std::move(f);
  return r;
}
}

TEST(Hdf5QueryModel, CanonicalCodecRoundtripsUtf8AndReservedCharactersOnce) {
  for (const std::string path : {"/strain/Strain", "/温度/é", "/a %2F?#:&=/value", "/a.b_~-"}) {
    auto encoded = make_hdf5_value_field(path);
    ASSERT_TRUE(encoded.ok()) << encoded.status().message();
    auto decoded = parse_hdf5_value_field(encoded.value());
    ASSERT_TRUE(decoded.ok());
    EXPECT_EQ(decoded.value(), path);
    for (const unsigned char ch : encoded.value().field_id) EXPECT_LT(ch, 128);
  }
  EXPECT_EQ(value("/strain/Strain").field_id, "hdf5.value:%2Fstrain%2FStrain");
  EXPECT_EQ(value("/%2F").field_id, "hdf5.value:%2F%252F");
  EXPECT_NE(value("/é").field_id, value("/e\xCC\x81").field_id); // no NFC guessing
  EXPECT_NE(value("/hdf5.axis.0").field_id, hdf5_axis_field(0).field_id);
}

TEST(Hdf5QueryModel, RejectsMalformedAndNoncanonicalPathsAndFields) {
  const std::vector<std::string> paths = {"", "/", "relative", "/a/", "/a//b", "/./a",
    "/a/../b", std::string("/a\0b", 4), "/\xC0\xAF", "/\xED\xA0\x80",
    "/\xF4\x90\x80\x80", "/\xE2\x82", "/" + std::string(4096, 'a')};
  for (const auto& path : paths) EXPECT_FALSE(make_hdf5_value_field(path).ok());
  for (const std::string field : {"value", "hdf5.value:/a", "hdf5.value:%2fa",
      "hdf5.value:%2F%61", "hdf5.value:%2F", "hdf5.value:%2F%", "hdf5.value:%2F%GG",
      "hdf5.value:%2F%00", "hdf5.value:%2F%FF", "hdf5.value:%2Fa%2F", "hdf5.axis.0"})
    EXPECT_FALSE(parse_hdf5_value_field({field}).ok()) << field;
}

TEST(Hdf5QueryModel, RequiresOneValueBindingBeforeProviderAccess) {
  Dataset provider;
  auto f = fragment(); f.projection = {hdf5_axis_field(0)};
  EXPECT_FALSE(bind_hdf5_query(provider, f).ok());
  f.projection = {value(), value("/b")};
  EXPECT_FALSE(bind_hdf5_query(provider, f).ok());
  f.projection = {value(), value()};
  EXPECT_FALSE(bind_hdf5_query(provider, f).ok());
  f.projection = {value(), {"grib.data.value"}};
  EXPECT_FALSE(bind_hdf5_query(provider, f).ok());
  f = fragment(); f.physical_array = PhysicalArraySubject{"data", 0, 0, {{{}}}};
  EXPECT_FALSE(bind_hdf5_query(provider, f).ok());
  EXPECT_EQ(provider.calls, 0u);
}

TEST(Hdf5QueryModel, NativeSchemaHasOrdinalAxesAndNoImplicitNullOrTransform) {
  Dataset provider;
  for (auto dtype : {LogicalTypeId::Float32, LogicalTypeId::Float64}) {
    provider.data.value_type = dtype;
    auto bound = bind_hdf5_query(provider, fragment());
    ASSERT_TRUE(bound.ok()) << bound.status().message();
    EXPECT_EQ(bound.value().row_count(), 315u);
    EXPECT_EQ(bound.value().projected_row_bytes(), dtype == LogicalTypeId::Float32 ? 4u : 8u);
    const auto& schema = bound.value().schema();
    ASSERT_NE(schema.find(value().field_id), nullptr);
    EXPECT_EQ(schema.find(value().field_id)->type.id, dtype);
    EXPECT_FALSE(schema.find(value().field_id)->type.nullable);
    EXPECT_FALSE(schema.find(value().field_id)->unit);
    for (unsigned i=0; i<3; ++i) {
      auto field = schema.find(hdf5_axis_field(i).field_id);
      ASSERT_NE(field, nullptr);
      EXPECT_EQ(field->type.id, LogicalTypeId::UInt64);
      EXPECT_EQ(field->role, FieldRole::Dimension);
      EXPECT_FALSE(field->type.timezone);
    }
    auto caps = make_hdf5_query_capabilities(bound.value());
    ASSERT_TRUE(caps.ok()); EXPECT_TRUE(validate_query_capabilities(caps.value()).ok());
    EXPECT_EQ(caps.value().adapter_id(), "hdf5");
    EXPECT_EQ(caps.value().ordering(), OrderingGuarantee::Domain);
    for (const auto& field : caps.value().fields()) {
      EXPECT_TRUE(field.predicate_operators.empty());
      EXPECT_FALSE(field.null_predicate);
      EXPECT_FALSE(field.interpolatable);
      if (field.field.field_id != value().field_id) {
        EXPECT_TRUE(field.sliceable); EXPECT_EQ(field.slice_pushdown, PushdownLevel::Exact);
      }
    }
  }
}

TEST(Hdf5QueryModel, RankDatatypeAndProfileAreExplicitlyUnsupported) {
  Dataset p;
  for (auto shape : std::vector<std::vector<std::uint64_t>>{{}, {1,2,3,4,5}}) {
    p.data.shape=shape;
    EXPECT_EQ(bind_hdf5_query(p, fragment()).status().code(), QueryError::UnsupportedCapability);
  }
  p.data.shape={4}; p.data.value_type=LogicalTypeId::Int64;
  EXPECT_EQ(bind_hdf5_query(p, fragment()).status().code(), QueryError::UnsupportedCapability);
  p.data.value_type=LogicalTypeId::Float32; p.data.supported=false; p.data.unsupported_reason="virtual dataset";
  EXPECT_EQ(bind_hdf5_query(p, fragment()).status().code(), QueryError::UnsupportedCapability);
}

TEST(Hdf5QueryModel, StrictHalfOpenSlicesDefaultOmittedAxes) {
  Dataset p; auto f=fragment(); f.slices={interval(1,2,7)};
  auto b=bind_hdf5_query(p,f); ASSERT_TRUE(b.ok());
  EXPECT_EQ(b.value().origin(), (std::vector<std::uint64_t>{0,2,0}));
  EXPECT_EQ(b.value().shape(), (std::vector<std::uint64_t>{5,5,9}));
  f.slices[0].lower.reset(); b=bind_hdf5_query(p,f); ASSERT_TRUE(b.ok());
  EXPECT_EQ(b.value().row_count(),315u);
  f.slices={interval(2,3,9)}; f.slices[0].upper.reset(); b=bind_hdf5_query(p,f); ASSERT_TRUE(b.ok());
  EXPECT_EQ(b.value().shape().back(),6u);
}

TEST(Hdf5QueryModel, RejectsBadBoundsDuplicateAndUnknownAxesWithoutClipping) {
  Dataset p; auto f=fragment();
  for (auto ends : std::vector<std::pair<std::uint64_t,std::uint64_t>>{{4,3},{0,6},{6,6}}) {
    f.slices={interval(0,ends.first,ends.second)}; EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  }
  f.slices={interval(0,0,2),interval(0,1,3)}; EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f.slices={interval(0,0,2)}; f.slices[0].dimension={"hdf5.axis.00"}; EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f.slices[0].dimension={"hdf5.axis.3"}; EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f.slices={interval(0,0,2)}; f.slices[0].upper->kind=BoundKind::Inclusive;
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f.slices={interval(0,0,2)}; f.slices[0].lower->kind=BoundKind::Exclusive;
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f.slices={interval(0,0,2)}; f.slices[0].lower->value=LogicalValue::int64(0);
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f.slices={interval(0,0,2)}; f.slices[0].lower->value=LogicalValue::null(LogicalTypeId::UInt64);
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
}

TEST(Hdf5QueryModel, RejectsPredicatesOverridesBinaryAndInterpolation) {
  Dataset p; auto f=fragment(); f.predicate=make_is_null(value());
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f=fragment(); f.output.type_overrides={ {value(),LogicalTypeId::Float64} };
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f=fragment(); f.output.mode=QueryFragment::ResultMode::BinaryView;
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  f=fragment(); f.output.interpolation=QueryFragment::GridInterpolation{};
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
}

TEST(Hdf5QueryModel, ZeroExtentAndEmptySelectionRetainShapeWithoutProductOverflow) {
  Dataset p; const auto max=std::numeric_limits<std::uint64_t>::max();
  for (auto shape : std::vector<std::vector<std::uint64_t>>{{0,7},{max,max,0},{0,max,max}}) {
    p.data.shape=shape; auto b=bind_hdf5_query(p,fragment()); ASSERT_TRUE(b.ok()) << b.status().message();
    EXPECT_EQ(b.value().row_count(),0u); EXPECT_EQ(b.value().shape(),shape);
  }
  p.data.shape={5,7,9}; auto f=fragment(); f.slices={interval(2,9,9)};
  auto b=bind_hdf5_query(p,f); ASSERT_TRUE(b.ok()); EXPECT_EQ(b.value().row_count(),0u);
  EXPECT_EQ(b.value().origin().back(),9u);
}

TEST(Hdf5QueryModel, RejectsResultProductAndByteOverflowBeforeAllocation) {
  Dataset p; auto max=std::numeric_limits<std::uint64_t>::max();
  QueryLimits limits; limits.max_result_rows=max;
  for (auto shape : std::vector<std::vector<std::uint64_t>>{{max,max},{max}}) {
    p.data.shape=shape;
    EXPECT_EQ(bind_hdf5_query(p,fragment(),limits).status().code(),QueryError::ResourceExhausted);
  }
  p.data.shape={5,7,9};limits.max_result_rows=314;
  EXPECT_EQ(bind_hdf5_query(p,fragment(),limits).status().code(),QueryError::ResourceExhausted);
}

TEST(Hdf5QueryModel, SlabsCoverRectanglesInCOrderWithUnprojectedAxes) {
  Dataset p; auto f=fragment(); f.slices={interval(0,1,5),interval(1,2,7),interval(2,3,9)};
  for (std::size_t rows : {1u,4u,6u,7u,30u,31u,120u}) {
    QueryLimits limits; limits.target_batch_rows=rows; limits.target_batch_bytes=rows*4;
    auto b=bind_hdf5_query(p,f,limits);ASSERT_TRUE(b.ok());
    auto iterator=Hdf5SlabIterator::create(b.value(),limits);ASSERT_TRUE(iterator.ok());
    std::vector<std::uint64_t> actual;
    for (;;) {
      auto next=iterator.value().next();ASSERT_TRUE(next.ok());if(!next.value())break;
      const auto& slab=*next.value();EXPECT_LE(slab.row_count,rows);
      const auto& d=slab.domain; ASSERT_EQ(d.dimensions.size(),3u);EXPECT_EQ(d.kind,DomainKind::DenseGrid);
      EXPECT_EQ(slab.row_count,d.shape[0]*d.shape[1]*d.shape[2]);
      for(std::uint64_t i=0;i<d.shape[0];++i) for(std::uint64_t j=0;j<d.shape[1];++j) for(std::uint64_t k=0;k<d.shape[2];++k)
        actual.push_back(((d.origin[0]+i)*7+d.origin[1]+j)*9+d.origin[2]+k);
    }
    std::vector<std::uint64_t> expected;
    for(unsigned i=1;i<5;++i)for(unsigned j=2;j<7;++j)for(unsigned k=3;k<9;++k)expected.push_back((i*7+j)*9+k);
    EXPECT_EQ(actual,expected) << rows;
  }
}

TEST(Hdf5QueryModel, ProjectedAxesAreChargedToBatchByteBudget) {
  Dataset p;auto f=fragment();f.projection={hdf5_axis_field(2),value(),hdf5_axis_field(0)};
  QueryLimits limits;limits.target_batch_rows=100;limits.target_batch_bytes=40;
  auto b=bind_hdf5_query(p,f,limits);ASSERT_TRUE(b.ok());EXPECT_EQ(b.value().projected_row_bytes(),20u);
  auto it=Hdf5SlabIterator::create(b.value(),limits);ASSERT_TRUE(it.ok());
  auto first=it.value().next();ASSERT_TRUE(first.ok());ASSERT_TRUE(first.value());EXPECT_LE(first.value()->row_count,2u);
  limits.target_batch_bytes=19;EXPECT_FALSE(Hdf5SlabIterator::create(b.value(),limits).ok());
}

TEST(Hdf5QueryModel, AlignedSlabsCoverUnalignedRoiAcrossPivotsWithoutIncreasingCaps) {
  Dataset p; p.data.shape = {3,11,13};
  auto f = fragment(); f.slices = {interval(0,1,3),interval(1,2,10),interval(2,3,12)};
  for (std::uint64_t cap : {1u,4u,9u,27u,45u,63u,72u,100u,300u}) {
    QueryLimits limits; limits.target_batch_rows = cap; limits.target_batch_bytes = cap * 4;
    auto bound = bind_hdf5_query(p,f,limits); ASSERT_TRUE(bound.ok());
    auto iterator = Hdf5SlabIterator::create(bound.value(),limits,{2,4,5}); ASSERT_TRUE(iterator.ok());
    std::vector<std::uint64_t> actual, expected;
    for (;;) {
      auto next = iterator.value().next(); ASSERT_TRUE(next.ok()); if (!next.value()) break;
      const auto& slab = *next.value(); const auto& d = slab.domain;
      EXPECT_GT(slab.row_count,0u); EXPECT_LE(slab.row_count,cap);
      EXPECT_LE(slab.row_count * bound.value().projected_row_bytes(),limits.target_batch_bytes);
      for (std::uint64_t i=0;i<d.shape[0];++i) for (std::uint64_t j=0;j<d.shape[1];++j)
        for (std::uint64_t k=0;k<d.shape[2];++k)
          actual.push_back(((d.origin[0]+i)*11+d.origin[1]+j)*13+d.origin[2]+k);
    }
    for (std::uint64_t i=1;i<3;++i) for (std::uint64_t j=2;j<10;++j)
      for (std::uint64_t k=3;k<12;++k) expected.push_back((i*11+j)*13+k);
    EXPECT_EQ(actual,expected) << cap;
  }
}

TEST(Hdf5QueryModel, NativeAlignmentAvoidsRepeatedLargeChunkRowsAndHandlesRoiHead) {
  Dataset p; p.data.shape = {1,1,768,768};
  QueryLimits limits; limits.target_batch_rows = 65536; limits.target_batch_bytes = 65536 * 4;
  auto bound = bind_hdf5_query(p,fragment(),limits); ASSERT_TRUE(bound.ok());
  auto iterator = Hdf5SlabIterator::create(bound.value(),limits,{1,1,32,32}); ASSERT_TRUE(iterator.ok());
  unsigned slabs = 0; std::uint64_t chunk_visits = 0;
  for (;;) {
    auto next = iterator.value().next(); ASSERT_TRUE(next.ok()); if (!next.value()) break;
    const auto& d = next.value()->domain;
    EXPECT_EQ(d.origin[2],slabs * 64u); EXPECT_EQ(d.shape[2],64u);
    EXPECT_LE(next.value()->row_count,limits.target_batch_rows);
    chunk_visits += ((d.origin[2]+d.shape[2]-1)/32-d.origin[2]/32+1)*24;
    ++slabs;
  }
  EXPECT_EQ(slabs,12u); EXPECT_EQ(chunk_visits,576u);
  p.data.shape = {100,64}; auto f = fragment(); f.slices = {interval(0,5,96)};
  limits.target_batch_rows = 48*64; limits.target_batch_bytes = 48*64*4;
  bound = bind_hdf5_query(p,f,limits); ASSERT_TRUE(bound.ok());
  iterator = Hdf5SlabIterator::create(bound.value(),limits,{32,32}); ASSERT_TRUE(iterator.ok());
  for (auto rows : {27u,32u,32u}) {
    auto next = iterator.value().next(); ASSERT_TRUE(next.ok()); ASSERT_TRUE(next.value());
    EXPECT_EQ(next.value()->domain.shape[0],rows);
  }
  EXPECT_FALSE(iterator.value().next().value());
}

TEST(Hdf5QueryModel, CapacitySmallerThanAlignmentStaysBoundedAndMalformedAlignmentFails) {
  Dataset p; p.data.shape = {20};
  QueryLimits limits; limits.target_batch_rows = 3; limits.target_batch_bytes = 12;
  auto bound = bind_hdf5_query(p,fragment(),limits); ASSERT_TRUE(bound.ok());
  auto iterator = Hdf5SlabIterator::create(bound.value(),limits,{8}); ASSERT_TRUE(iterator.ok());
  std::uint64_t rows = 0;
  for (;;) {
    auto next = iterator.value().next(); ASSERT_TRUE(next.ok()); if (!next.value()) break;
    EXPECT_LE(next.value()->row_count,3u); EXPECT_GT(next.value()->row_count,0u);
    EXPECT_EQ(next.value()->domain.origin[0],rows); rows += next.value()->row_count;
  }
  EXPECT_EQ(rows,20u);
  EXPECT_EQ(Hdf5SlabIterator::create(bound.value(),limits,{0}).status().code(),QueryError::CorruptLayout);
  EXPECT_EQ(Hdf5SlabIterator::create(bound.value(),limits,{8,8}).status().code(),QueryError::CorruptLayout);
}

TEST(Hdf5QueryModel, UInt64CoordinatesRemainExactAboveDoubleIntegerPrecision) {
  Dataset p; p.data.shape={9007199254741000ULL}; auto f=fragment();
  f.slices={interval(0,9007199254740993ULL,9007199254740995ULL)};
  auto b=bind_hdf5_query(p,f);ASSERT_TRUE(b.ok()) << b.status().message();
  EXPECT_EQ(b.value().origin().front(),9007199254740993ULL);EXPECT_EQ(b.value().row_count(),2u);
  auto it=Hdf5SlabIterator::create(b.value());ASSERT_TRUE(it.ok());
  auto slab=it.value().next();ASSERT_TRUE(slab.ok());ASSERT_TRUE(slab.value());
  EXPECT_EQ(slab.value()->domain.origin.front(),9007199254740993ULL);
}

TEST(Hdf5QueryModel, EmptySlabsDoNotRequireCapacityForAValue) {
  Dataset p;p.data.shape={4,0};QueryLimits limits;limits.target_batch_bytes=1;
  auto b=bind_hdf5_query(p,fragment(),limits);ASSERT_TRUE(b.ok()) << b.status().message();
  auto it=Hdf5SlabIterator::create(b.value(),limits);ASSERT_TRUE(it.ok());
  auto slab=it.value().next();ASSERT_TRUE(slab.ok());EXPECT_FALSE(slab.value());
}

TEST(Hdf5QueryModel, PreservesProviderReadFailureRatherThanReportingMissingDataset) {
  class Broken final : public Hdf5DatasetProvider {
    QueryResult<Hdf5DatasetDescriptor> describe(const std::string&) const override {
      return QueryResult<Hdf5DatasetDescriptor>::failure(QueryStatus::failure(
        QueryError::ReadFailure,QueryPhase::Storage,"metadata read failed"));
    }
  } provider;
  auto b=bind_hdf5_query(provider,fragment());EXPECT_EQ(b.status().code(),QueryError::ReadFailure);
  EXPECT_EQ(b.status().phase(),QueryPhase::Storage);
}

TEST(Hdf5QueryModel, ExplicitUnboundedSliceIsRejectedLikeCoreRequestValidation) {
  Dataset p;auto f=fragment();f.slices={{hdf5_axis_field(0),std::nullopt,std::nullopt}};
  EXPECT_FALSE(bind_hdf5_query(p,f).ok());
  EXPECT_FALSE(validate_query_request(request(f)).ok());
}

TEST(Hdf5QueryModel, RankFourInt16PhysicalViewRetainsAllAxes) {
  Dataset p; p.data.shape = {1, 2, 3, 4}; p.data.value_type = LogicalTypeId::Int32;
  auto f = fragment(); f.projection.push_back(hdf5_axis_field(3));
  f.slices = {interval(3, 1, 3)};
  auto b = bind_hdf5_query(p, f); ASSERT_TRUE(b.ok()) << b.status().message();
  EXPECT_EQ(b.value().shape(), (std::vector<std::uint64_t>{1,2,3,2}));
  EXPECT_EQ(b.value().row_count(), 12u);
  EXPECT_EQ(b.value().projected_row_bytes(), 12u);
  EXPECT_EQ(b.value().schema().fields().front().type.id, LogicalTypeId::Int32);
}

TEST(Hdf5QueryModel, ScientificViewRequiresEvidenceAndRetainsNativeTimeUnits) {
  Dataset p; p.data.shape = {3}; p.data.value_type = LogicalTypeId::Int32;
  QueryFragment f; f.projection = {make_hdf5_science_value_field("/a").value(), {kHdf5TimeField}};
  EXPECT_FALSE(bind_hdf5_query(p, f).ok());
  Hdf5CfBinding cf;
  cf.axes.push_back({0,"time","t","days since 1978-01-01 12:00:00","standard","verified",{0,1,2},true});
  cf.scale_factor = .1; cf.add_offset = 2; cf.units = "degree_C";
  auto b = bind_hdf5_query(p, f, {}, {}, cf); ASSERT_TRUE(b.ok()) << b.status().message();
  EXPECT_TRUE(b.value().scientific_values());
  EXPECT_EQ(b.value().schema().fields().front().type, (LogicalType{LogicalTypeId::Float64,true,{}}));
  EXPECT_EQ(b.value().schema().find(kHdf5TimeField)->unit, cf.axes[0].units);
  EXPECT_EQ(b.value().cf_binding()->axes[0].calendar,"standard");
  EXPECT_EQ(b.value().descriptor().value_type,LogicalTypeId::Int32);
}

TEST(Hdf5QueryModel, DescendingCfSlicesKeepStoredOrderAndHalfOpenBounds) {
  Dataset p; p.data.shape = {5};
  QueryFragment f; f.projection = {make_hdf5_science_value_field("/a").value(), {kHdf5LatitudeField}};
  f.slices = {{{kHdf5LatitudeField}, DomainBound{LogicalValue::float64(-20),BoundKind::Inclusive}, DomainBound{LogicalValue::float64(40),BoundKind::Exclusive}}};
  Hdf5CfBinding cf; cf.axes.push_back({0,"latitude","lat","degrees_north","","verified",{80,40,0,-20,-80},false});
  auto b = bind_hdf5_query(p,f,{},{},cf); ASSERT_TRUE(b.ok()) << b.status().message();
  EXPECT_EQ(b.value().origin(),(std::vector<std::uint64_t>{2}));
  EXPECT_EQ(b.value().shape(),(std::vector<std::uint64_t>{2}));
  f.slices[0].lower.reset(); b = bind_hdf5_query(p,f,{},{},cf); ASSERT_TRUE(b.ok());
  EXPECT_EQ(b.value().origin()[0],2u); EXPECT_EQ(b.value().shape()[0],3u);
  f.slices.push_back(interval(0,0,1)); EXPECT_FALSE(bind_hdf5_query(p,f,{},{},cf).ok());
}

TEST(Hdf5QueryModel, CfEvidenceRejectsNonmonotonicAxesAndCyclicLongitude) {
  Dataset p; p.data.shape = {3};
  QueryFragment f; f.projection = {make_hdf5_science_value_field("/a").value(), {kHdf5LongitudeField}};
  Hdf5CfBinding cf; cf.axes.push_back({0,"longitude","lon","degrees_east","","verified",{0,2,1},true});
  EXPECT_FALSE(bind_hdf5_query(p,f,{},{},cf).ok());
  cf.axes[0].values={0,1,2};
  f.slices = {{{kHdf5LongitudeField}, DomainBound{LogicalValue::float64(350),BoundKind::Inclusive}, DomainBound{LogicalValue::float64(10),BoundKind::Exclusive}}};
  EXPECT_FALSE(bind_hdf5_query(p,f,{},{},cf).ok());
  f.slices.clear(); f.projection[0]=value();
  auto physical=bind_hdf5_query(p,f,{},{},cf);
  ASSERT_TRUE(physical.ok()) << physical.status().message();
  EXPECT_FALSE(physical.value().scientific_values());
  EXPECT_EQ(physical.value().schema().fields().front().type.id,LogicalTypeId::Float32);
}
TEST(Hdf5QueryModel, PressureSliceOnPhysicalValuesPreservesDescendingAxis) {
  Dataset p; p.data.shape={3};
  QueryFragment f; f.projection={value(),{kHdf5PressureField}};
  f.slices={{{kHdf5PressureField},
    DomainBound{LogicalValue::float64(749),BoundKind::Inclusive},
    DomainBound{LogicalValue::float64(751),BoundKind::Exclusive}}};
  Hdf5CfBinding evidence;
  evidence.profile="geos572-pressure-grid/1";
  evidence.axes.push_back({0,"pressure","lev","hPa","","forward_dimension_list/1",
                           {1000,750,500},false});
  auto bound=bind_hdf5_query(p,f,{},{},evidence);
  ASSERT_TRUE(bound.ok()) << bound.status().message();
  EXPECT_EQ(bound.value().origin(),(std::vector<std::uint64_t>{1}));
  EXPECT_EQ(bound.value().shape(),(std::vector<std::uint64_t>{1}));
  EXPECT_EQ(bound.value().schema().find(kHdf5PressureField)->unit,"hPa");
  EXPECT_FALSE(bound.value().scientific_values());
}

TEST(Hdf5QueryModel, RejectedDatasetPredicateRetainsZeroRowTypedSchema) {
  Dataset p; auto f=fragment(); auto attribute=make_hdf5_attribute_field("standard_name").value();
  f.predicate=make_comparison(attribute,ComparisonOperator::Equal,LogicalValue::utf8("temperature"));
  std::vector<LogicalField> attrs{{attribute.field_id,"standard_name",{LogicalTypeId::Utf8,true,{}},FieldRole::Attribute,{}}};
  auto b=bind_hdf5_query(p,f,{},{},{},attrs,false); ASSERT_TRUE(b.ok()) << b.status().message();
  EXPECT_EQ(b.value().row_count(),0u); EXPECT_EQ(b.value().shape()[0],0u);
  auto caps=make_hdf5_query_capabilities(b.value()); ASSERT_TRUE(caps.ok());
  auto found=std::find_if(caps.value().fields().begin(),caps.value().fields().end(),[&](const auto& field){return field.field.field_id==attribute.field_id;});
  ASSERT_NE(found,caps.value().fields().end()); EXPECT_EQ(found->predicate_pushdown,PushdownLevel::Exact); EXPECT_FALSE(found->projectable);
}

TEST(Hdf5QueryModel, ScientificAndAttributeFieldEncodingIsCanonical) {
  auto science=make_hdf5_science_value_field("/group/海温"); ASSERT_TRUE(science.ok());
  EXPECT_TRUE(is_hdf5_science_value_field(science.value()));
  EXPECT_EQ(parse_hdf5_value_field(science.value()).value(),"/group/海温");
  EXPECT_EQ(make_hdf5_attribute_field("a/b name").value().field_id,"hdf5.attr:a%2Fb%20name");
  EXPECT_FALSE(make_hdf5_attribute_field("").ok());
}
