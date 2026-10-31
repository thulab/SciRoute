#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5SemanticBinding.h"
#include <gtest/gtest.h>
#include "iotdbfs/query/encoding/ArrowResultEncoder.h"
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <cmath>
#include <cstring>
#include <limits>
#include <map>

using namespace iotdbfs;
namespace {
constexpr double start = 1126259446., spacing = 1./4096;
Hdf5Attribute number(const std::string& name, LogicalTypeId type, std::uint64_t bits) {
  Hdf5Attribute a; a.name=name; a.type=type; a.state=Hdf5AttributeState::Complete;
  a.native_type_class=type==LogicalTypeId::Float64?1:0; a.native_element_bytes=8;
  for (unsigned i=0;i<8;++i) a.values.push_back(bits>>(8*i));
  return a;
}
Hdf5Attribute real(const std::string& name, double value) {
  std::uint64_t bits; std::memcpy(&bits,&value,8); return number(name,LogicalTypeId::Float64,bits);
}
Hdf5Attribute text(const std::string& name, const std::string& value) {
  Hdf5Attribute a; a.name=name; a.type=LogicalTypeId::Utf8; a.state=Hdf5AttributeState::Complete;
  a.native_type_class=3; a.native_element_bytes=value.size()+1; a.strings={value}; return a;
}
class Source final : public QuerySource {
public:
  Hdf5LayoutDocument doc; BinaryValue payload, layout;
  StructuredBlobManifest manifest_; mutable unsigned payload_calls=0,layout_calls=0;
  Source() {
    payload.resize(64+128*8);
    for (unsigned i=0;i<128;++i) {
      double v=i+.25; std::uint64_t bits;std::memcpy(&bits,&v,8);
      for(unsigned b=0;b<8;++b)payload[64+i*8+b]=bits>>(8*b);
    }
    Hdf5DatasetLayout data; data.descriptor={"/strain",LogicalTypeId::Float64,{128},true,{}};
    data.storage=Hdf5StorageLayout::Contiguous;data.byte_order=Hdf5ByteOrder::Little;
    data.element_bytes=8;data.native_type_class=1;data.contiguous_offset=64;data.contiguous_size=1024;
    data.fill_status=Hdf5FillStatus::Default;data.fill_value=BinaryValue(8,0);
    data.fill_time=2;data.allocation_status=2;data.index_complete=true;
    doc.source_size=payload.size();doc.root_object_id="root";
    doc.objects={{"root",Hdf5ObjectKind::Group,{}},{"data",Hdf5ObjectKind::Dataset,data}};
    doc.links={{"root","strain",Hdf5LinkKind::Hard,"data",{}},
      {"root","alias",Hdf5LinkKind::Hard,"data",{}}};
    doc.objects[1].attributes={number("Xstart",LogicalTypeId::Int64,1126259446),
      real("Xspacing",spacing),number("Npoints",LogicalTypeId::Int64,128),
      text("Xunits","second"),text("Xlabel","GPS time")};
    manifest_.key={"/scientific.h5",17};manifest_.format_id="hdf5";manifest_.payload_size=payload.size();
    manifest_.revision=3;manifest_.published=true;manifest_.phase=StructuredBlobPhase::Published;encode();
  }
  void encode() {
    doc.objects[1].attribute_count=doc.objects[1].attributes.size();
    auto bytes=serialize_hdf5_layout(doc);if(!bytes.ok())throw std::runtime_error(bytes.status().message());
    layout=std::move(bytes.value());manifest_.layout_size=layout.size();
  }
  const StructuredBlobManifest& manifest()const override{return manifest_;}
  static QueryResult<BinaryValue> read(const BinaryValue& b,std::uint64_t o,std::uint64_t n){
    if(o>b.size()||n>b.size()-o)return QueryResult<BinaryValue>::failure(QueryStatus::failure(
      QueryError::ReadFailure,QueryPhase::Storage,"fixture range"));
    return QueryResult<BinaryValue>::success({b.begin()+o,b.begin()+o+n});
  }
  QueryResult<BinaryValue> read_payload(std::uint64_t o,std::uint64_t n)const override{++payload_calls;return read(payload,o,n);}
  QueryResult<BinaryValue> read_layout(std::uint64_t o,std::uint64_t n)const override{++layout_calls;return read(layout,o,n);}
  Hdf5SemanticConfig config()const{return {{{manifest_.key,"/strain",storage_namespace()}}};}
};
QueryFragment query(double lo=start+3*spacing,double hi=start+17*spacing,const std::string& path="/strain") {
  QueryFragment f;f.projection={{kHdf5GpsSecondsField},make_hdf5_value_field(path).value(),hdf5_axis_field(0)};
  f.slices={{{kHdf5GpsSecondsField},DomainBound{LogicalValue::float64(lo),BoundKind::Inclusive},
    DomainBound{LogicalValue::float64(hi),BoundKind::Exclusive}}};return f;
}
std::vector<QueryBatch> run(const Hdf5FormatQueryAdapter& adapter,const std::shared_ptr<Source>& source,
    const QueryFragment& fragment) {
  auto p=adapter.prepare(QueryPlanningContext(*source),fragment);
  if(!p.ok())throw std::runtime_error(p.status().message());
  QueryExecutionOptions opts;opts.target_batch_rows=3;
  auto c=adapter.execute(QueryExecutionContext(source),p.value().implementation(),opts);
  if(!c.ok())throw std::runtime_error(c.status().message());
  std::vector<QueryBatch> out;
  for(;;){auto b=c.value()->next();if(!b.ok())throw std::runtime_error(b.status().message());
    if(!b.value())break;out.push_back(std::move(*b.value()));}return out;
}
}
TEST(Hdf5SemanticBinding, ExactGridHasCorrectBinarySearchAroundEveryRepresentableBoundary) {
  auto grid=Hdf5GpsTimeAxis::create(start,spacing,128);ASSERT_TRUE(grid.ok());
  for(unsigned i=0;i<128;++i){const auto x=start+i*spacing;EXPECT_DOUBLE_EQ(grid.value().coordinate(i),x);
    EXPECT_EQ(grid.value().lower_bound(x),i);
    EXPECT_EQ(grid.value().lower_bound(std::nextafter(x,-INFINITY)),i);
    EXPECT_EQ(grid.value().lower_bound(std::nextafter(x,INFINITY)),i+1);}
  EXPECT_EQ(grid.value().lower_bound(start-1),0);EXPECT_EQ(grid.value().lower_bound(start+1),128);
  EXPECT_THROW(grid.value().coordinate(128),std::out_of_range);
}
TEST(Hdf5SemanticBinding, RejectsInexactCollapsedOverflowingAndInvalidGrids) {
  for(double dt:{0.,-spacing,.1, std::numeric_limits<double>::quiet_NaN(),INFINITY*1.,std::ldexp(1.,-60)})
    EXPECT_FALSE(Hdf5GpsTimeAxis::create(start,dt,128).ok());
  EXPECT_FALSE(Hdf5GpsTimeAxis::create(INFINITY,spacing,128).ok());
  EXPECT_FALSE(Hdf5GpsTimeAxis::create(.1,.25,4).ok());
  EXPECT_FALSE(Hdf5GpsTimeAxis::create(std::ldexp(1.,53),1,2).ok());
  EXPECT_FALSE(Hdf5GpsTimeAxis::create(0,std::ldexp(1.,1023),3).ok());
  EXPECT_TRUE(Hdf5GpsTimeAxis::create(-2,.5,8).ok());
  EXPECT_TRUE(Hdf5GpsTimeAxis::create(0,std::numeric_limits<double>::denorm_min(),8).ok());
  EXPECT_TRUE(Hdf5GpsTimeAxis::create(start,spacing,0).ok());
}
TEST(Hdf5SemanticBinding, TypedMetadataAndDeclarationEvidenceRemainObservable) {
  Source source; auto grid=compile_hdf5_gwosc_axis(source.doc.objects[1]);ASSERT_TRUE(grid.ok());
  EXPECT_TRUE(grid.value().units_verified());EXPECT_TRUE(grid.value().label_verified());
  for(unsigned i:{3,4}) {auto& a=source.doc.objects[1].attributes[i];a.state=Hdf5AttributeState::UnsupportedVariableLength;
    a.strings.clear();a.reason="unbounded native variable string";}
  auto declared=compile_hdf5_gwosc_axis(source.doc.objects[1]);ASSERT_TRUE(declared.ok());
  EXPECT_FALSE(declared.value().units_verified());EXPECT_FALSE(declared.value().label_verified());
  source.doc.objects[1].attributes[3].native_type_class=9;
  EXPECT_FALSE(compile_hdf5_gwosc_axis(source.doc.objects[1]).ok());
}
TEST(Hdf5SemanticBinding, MissingIncompleteAndUnavailableUnitsNeverBecomeDeclarations) {
  for(unsigned mode=0;mode<7;++mode){Source s;auto& o=s.doc.objects[1];
    if(mode==0)o.attributes.pop_back();
    if(mode==1)o.attributes_complete=false;
    if(mode==2)o.attributes[3].strings={"nanosecond"};
    if(mode==3)o.attributes[3].state=Hdf5AttributeState::TooLarge;
    if(mode==4)o.attributes[3].state=Hdf5AttributeState::UnsupportedType;
    if(mode==5)o.attributes[3].null_dataspace=true;
    if(mode==6)o.attributes[3].shape={1};
    EXPECT_FALSE(compile_hdf5_gwosc_axis(o).ok())<<mode;}
}
TEST(Hdf5SemanticBinding, NumericAttrsRequireScalarTypesMatchingCountAndExactFloat64) {
  for(unsigned mode=0;mode<8;++mode){Source s;auto& a=s.doc.objects[1].attributes;
    if(mode==0)a[0]=number("Xstart",LogicalTypeId::UInt64,UINT64_MAX);
    if(mode==1)a[2]=number("Npoints",LogicalTypeId::Int64,127);
    if(mode==2)a[2]=number("Npoints",LogicalTypeId::Int64,UINT64_MAX);
    if(mode==3)a[0].shape={1};
    if(mode==4)a[0].null_dataspace=true;
    if(mode==5)a[1]=number("Xspacing",LogicalTypeId::Int64,1);
    if(mode==6)a[2]=real("Npoints",128);
    if(mode==7)a[0].values.resize(4);
    EXPECT_FALSE(compile_hdf5_gwosc_axis(s.doc.objects[1]).ok())<<mode;}
}
TEST(Hdf5SemanticBinding, OptInScopesToExactSourceVersionNamespaceAndDataset) {
  auto source=std::make_shared<Source>();
  EXPECT_FALSE(Hdf5FormatQueryAdapter().prepare(QueryPlanningContext(*source),query()).ok());
  for(unsigned mode=0;mode<4;++mode){auto cfg=source->config();auto& spec=cfg.gwosc_bindings[0];
    if(mode==0)++spec.source.version;if(mode==1)spec.source.path="/other.h5";
    if(mode==2)++spec.storage_namespace;if(mode==3)spec.dataset_path="/absent";
    Hdf5FormatQueryAdapter adapter({}, {},cfg);EXPECT_FALSE(adapter.prepare(QueryPlanningContext(*source),query()).ok());}
  EXPECT_EQ(source->payload_calls,0u);
  auto cfg=source->config();cfg.gwosc_bindings[0].storage_namespace=0;
  EXPECT_THROW((Hdf5FormatQueryAdapter({}, {},cfg)),std::invalid_argument);
}
TEST(Hdf5SemanticBinding, SemanticResultsKeepOrdinalDomainAndReusePhysicalCacheAcrossAliases) {
  auto source=std::make_shared<Source>();auto runtime=std::make_shared<QueryRuntime>();
  Hdf5FormatQueryAdapter adapter({},runtime,source->config());
  auto plan=adapter.prepare(QueryPlanningContext(*source),query());ASSERT_TRUE(plan.ok())<<plan.status().message();
  auto identity=hdf5_query_identity(plan.value().implementation());ASSERT_TRUE(identity.ok());
  EXPECT_EQ(identity.value().view,kHdf5GwoscProfile);ASSERT_TRUE(identity.value().gps_axis);
  EXPECT_EQ(identity.value().gps_axis->count(),128);EXPECT_EQ(source->payload_calls,0);
  auto batches=run(adapter,source,query(start+3*spacing,start+17*spacing,"/alias"));unsigned ordinal=3;
  for(const auto& b:batches){ASSERT_TRUE(b.domain());EXPECT_EQ(b.domain()->origin[0],ordinal);
    EXPECT_EQ(b.domain()->dimensions[0].field.field_id,"hdf5.axis.0");
    EXPECT_EQ(b.schema().fields()[0].unit,std::optional<std::string>("second"));
    for(unsigned i=0;i<b.row_count();++i,++ordinal){
      EXPECT_EQ(std::get<std::vector<double>>(b.columns()[0].values)[i],start+ordinal*spacing);
      EXPECT_EQ(std::get<std::vector<double>>(b.columns()[1].values)[i],ordinal+.25);
      EXPECT_EQ(std::get<std::vector<std::uint64_t>>(b.columns()[2].values)[i],ordinal);}}
  EXPECT_EQ(ordinal,17);auto calls=source->payload_calls;
  QueryFragment physical;physical.projection={make_hdf5_value_field("/strain").value()};
  physical.slices={{hdf5_axis_field(0),DomainBound{LogicalValue::uint64(3),BoundKind::Inclusive},
    DomainBound{LogicalValue::uint64(17),BoundKind::Exclusive}}};
  auto physical_batches=run(adapter,source,physical);EXPECT_FALSE(physical_batches.empty());
  EXPECT_EQ(source->payload_calls,calls);
}
TEST(Hdf5SemanticBinding, PhysicalQueriesDoNotRequireOrGuessScientificAttributes) {
  auto source=std::make_shared<Source>();source->doc.objects[1].attributes.clear();source->encode();
  QueryFragment physical;physical.projection={make_hdf5_value_field("/strain").value()};
  Hdf5FormatQueryAdapter adapter({}, {},source->config());EXPECT_FALSE(run(adapter,source,physical).empty());
  EXPECT_FALSE(adapter.prepare(QueryPlanningContext(*source),query()).ok());
}
TEST(Hdf5SemanticBinding, SemanticSlicesClipOutsideCoverageAndHandleOpenBounds) {
  auto s=std::make_shared<Source>();Hdf5FormatQueryAdapter adapter({}, {},s->config());
  for(auto bounds:std::vector<std::pair<double,double>>{{start-2,start-1},{start+1,start+2},{start,start}})
    EXPECT_TRUE(run(adapter,s,query(bounds.first,bounds.second)).empty());
  auto f=query();f.slices[0].lower.reset();auto b=run(adapter,s,f);std::size_t n=0;for(auto& x:b)n+=x.row_count();EXPECT_EQ(n,17);
  f=query();f.slices[0].upper.reset();b=run(adapter,s,f);n=0;for(auto& x:b)n+=x.row_count();EXPECT_EQ(n,125);
  f=query();f.projection={make_hdf5_value_field("/strain").value()};b=run(adapter,s,f);
  ASSERT_FALSE(b.empty());EXPECT_EQ(b[0].domain()->origin[0],3);
}
TEST(Hdf5SemanticBinding, GrammarRejectsAmbiguousAndMistypedCoordinatesBeforePayload) {
  auto s=std::make_shared<Source>();Hdf5FormatQueryAdapter adapter({}, {},s->config());
  for(unsigned mode=0;mode<7;++mode){auto f=query();
    if(mode==0)f.slices.push_back({hdf5_axis_field(0),DomainBound{LogicalValue::uint64(0),BoundKind::Inclusive},{}});
    if(mode==1)f.slices[0].lower->value=LogicalValue::uint64(0);
    if(mode==2)f.slices[0].lower->value=LogicalValue::float64(NAN);
    if(mode==3)f.slices[0].upper->value=LogicalValue::float64(INFINITY);
    if(mode==4)f.slices[0].lower->kind=BoundKind::Exclusive;
    if(mode==5)f.slices[0].upper->value=LogicalValue::float64(start);
    if(mode==6)f.projection.push_back({kHdf5GpsSecondsField});
    EXPECT_FALSE(adapter.prepare(QueryPlanningContext(*s),f).ok())<<mode;}
  EXPECT_EQ(s->payload_calls,0);
}
TEST(Hdf5SemanticBinding, PlanningContextsCannotTransferOptInAcrossAdapters) {
  auto s=std::make_shared<Source>();QueryPlanningContext context(*s);
  Hdf5FormatQueryAdapter enabled({}, {},s->config()),disabled;
  ASSERT_TRUE(enabled.capabilities(context,query()).ok());
  EXPECT_FALSE(disabled.prepare(context,query()).ok());
}
TEST(Hdf5SemanticBinding, PlanningKeyPreservesAdjacentFloat64Bounds) {
  auto s=std::make_shared<Source>();Hdf5FormatQueryAdapter adapter({}, {},s->config());QueryPlanningContext context(*s);
  auto f=query();ASSERT_TRUE(adapter.capabilities(context,f).ok());
  f.slices[0].lower->value=LogicalValue::float64(std::nextafter(start+3*spacing,INFINITY));
  auto reused=adapter.prepare(context,f);ASSERT_FALSE(reused.ok());
  EXPECT_EQ(reused.status().code(),QueryError::StalePlan);
  auto p=adapter.prepare(QueryPlanningContext(*s),f);ASSERT_TRUE(p.ok())<<p.status().message();
  auto c=adapter.execute(QueryExecutionContext(s),p.value().implementation(),{});ASSERT_TRUE(c.ok());
  auto b=c.value()->next();ASSERT_TRUE(b.ok());ASSERT_TRUE(b.value());EXPECT_EQ(b.value()->domain()->origin[0],4);
}
TEST(Hdf5SemanticBinding, ExecutionRejectsPlanFromAnotherSemanticDeclaration) {
  auto source=std::make_shared<Source>();Hdf5FormatQueryAdapter enabled({}, {},source->config()),disabled;
  auto plan=enabled.prepare(QueryPlanningContext(*source),query());ASSERT_TRUE(plan.ok());
  auto cursor=disabled.execute(QueryExecutionContext(source),plan.value().implementation(),{});
  ASSERT_FALSE(cursor.ok());EXPECT_EQ(cursor.status().code(),QueryError::StalePlan);
  EXPECT_EQ(source->payload_calls,0);
}
TEST(Hdf5SemanticBinding, GridPublicApiRejectsNanSearchAndSupportsInfiniteSentinels) {
  auto grid=Hdf5GpsTimeAxis::create(start,spacing,128);ASSERT_TRUE(grid.ok());
  EXPECT_THROW(grid.value().lower_bound(NAN),std::invalid_argument);
  EXPECT_EQ(grid.value().lower_bound(-INFINITY),0);
  EXPECT_EQ(grid.value().lower_bound(INFINITY),128);
}

TEST(Hdf5SemanticBinding, IpcDescriptorPreservesEmptySelectionAndExactGpsEvidence) {
  for (bool declared : {false, true}) {
    auto source = std::make_shared<Source>();
    if (declared) {
      for (unsigned i : {3, 4}) {
        auto& attribute = source->doc.objects[1].attributes[i];
        attribute.state = Hdf5AttributeState::UnsupportedVariableLength;
        attribute.strings.clear(); attribute.reason = "variable_length_attribute";
      }
      source->encode();
    }
    Hdf5FormatQueryAdapter adapter({}, {}, source->config());
    for (bool empty : {false, true}) {
      auto fragment = empty ? query(start + 1, start + 2, "/alias") : query();
      auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
      ASSERT_TRUE(plan.ok()) << plan.status().message();
      const auto& fields = plan.value().implementation()->result_metadata();
      QueryResultMetadata metadata{{"iotdbfs.result_descriptors", "1"}};
      for (const auto& entry : fields)
        metadata.emplace_back("iotdbfs.result_descriptor.0." + entry.first, entry.second);
      ASSERT_TRUE(ArrowResultEncoder::validate_metadata(metadata, 1 << 20).ok());
      auto cursor = adapter.execute(QueryExecutionContext(source), plan.value().implementation(), {});
      ASSERT_TRUE(cursor.ok()) << cursor.status().message();
      auto encoded = ArrowResultEncoder::encode_query_cursor(cursor.value()->schema(), *cursor.value(), 1 << 20, metadata);
      ASSERT_TRUE(encoded.ok()) << encoded.status().message();
      auto input = std::make_shared<arrow::io::BufferReader>(
        std::make_shared<arrow::Buffer>(encoded.value().data(), encoded.value().size()));
      auto reader = arrow::ipc::RecordBatchStreamReader::Open(input);
      ASSERT_TRUE(reader.ok());
      auto field = [&](const char* key) {
        return reader.ValueOrDie()->schema()->metadata()->Get(std::string("iotdbfs.result_descriptor.0.") + key).ValueOr("missing");
      };
      EXPECT_EQ(field("protocol"), "iotdbfs-hdf5-subarray/1");
      EXPECT_EQ(field("source.path"), "/scientific.h5");
      EXPECT_EQ(field("source.version"), "17");
      EXPECT_EQ(field("source.revision"), "3");
      EXPECT_EQ(field("dataset.path"), empty ? "/alias" : "/strain");
      EXPECT_EQ(field("dataset.object_id"), "data");
      EXPECT_EQ(field("dataset.rank"), "1");
      EXPECT_EQ(field("dataset.value_type"), "float64");
      EXPECT_EQ(field("axis.0.full_extent"), "128");
      EXPECT_EQ(field("axis.0.selection_origin"), empty ? "128" : "3");
      EXPECT_EQ(field("axis.0.selection_extent"), empty ? "0" : "14");
      EXPECT_EQ(field("selection.row_count"), empty ? "0" : "14");
      EXPECT_EQ(field("order"), "C");
      EXPECT_EQ(field("layout.directory_sha256").size(), 64);
      EXPECT_EQ(field("view"), kHdf5GwoscProfile);
      EXPECT_EQ(field("coordinate.type"), "float64");
      EXPECT_EQ(field("coordinate.time_system"), "GPS");
      EXPECT_EQ(field("coordinate.unit"), "second");
      EXPECT_EQ(std::stod(field("coordinate.start")), start);
      EXPECT_EQ(std::stod(field("coordinate.spacing")), spacing);
      EXPECT_EQ(field("coordinate.units_provenance"), declared ? "application_declared" : "verified_attribute");
      EXPECT_EQ(field("coordinate.label_provenance"), declared ? "application_declared" : "verified_attribute");
      auto record = reader.ValueOrDie()->ReadNext(); ASSERT_TRUE(record.ok());
      EXPECT_EQ(bool(record.ValueOrDie().batch), !empty);
      if (!empty) {
        EXPECT_EQ(record.ValueOrDie().custom_metadata->Get("iotdbfs.domain.dimension.0.origin").ValueOr(""), "3");
        EXPECT_EQ(record.ValueOrDie().custom_metadata->Get("iotdbfs.domain.dimension.0.shape").ValueOr(""), "14");
      }
    }
  }
}
TEST(Hdf5SemanticBinding, PhysicalIpcDescriptorContainsNoInferredCoordinates) {
  auto source = std::make_shared<Source>();
  Hdf5FormatQueryAdapter adapter({}, {}, source->config());
  QueryFragment fragment; fragment.projection = {make_hdf5_value_field("/strain").value()};
  auto plan = adapter.prepare(QueryPlanningContext(*source), fragment);
  ASSERT_TRUE(plan.ok()) << plan.status().message();
  std::map<std::string, std::string> fields;
  for (const auto& entry : plan.value().implementation()->result_metadata()) fields.insert(entry);
  EXPECT_EQ(fields.at("view"), "physical-ordinal-c-order/1");
  EXPECT_EQ(fields.at("axis.0.selection_origin"), "0");
  EXPECT_EQ(fields.at("axis.0.selection_extent"), "128");
  EXPECT_EQ(fields.count("coordinate.time_system"), 0);
}
