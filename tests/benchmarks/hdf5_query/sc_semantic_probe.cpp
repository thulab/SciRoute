// Correctness oracle only, never a timing baseline: compare native HDF5 raw
// integers + independently read packing attributes with production query values.
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include <hdf5.h>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <memory>
#include <stdexcept>
using namespace iotdbfs;
namespace {
void need(bool value,const std::string& text){if(!value)throw std::runtime_error(text);}
struct Id {hid_t id; herr_t(*close)(hid_t); Id(hid_t id,herr_t(*f)(hid_t)):id(id),close(f){need(id>=0,"native handle failed");}~Id(){close(id);}operator hid_t()const{return id;}};
class Source final:public QuerySource {
public:
  explicit Source(const std::string& path) {
    std::ifstream input(path,std::ios::binary); need(bool(input),"cannot open fixture");
    payload.assign(std::istreambuf_iterator<char>(input),{});
    FormatSource source{payload.size(),[&](std::uint64_t o,std::uint64_t n)->std::optional<BinaryValue>{auto r=range(payload,o,n);return r.ok()?std::optional<BinaryValue>(std::move(r.value())):std::nullopt;}};
    Hdf5FormatAdapter adapter; auto ingestion=adapter.begin_ingestion(source);
    auto consumed=ingestion->consume_chunk(0,payload.data(),payload.size()); need(consumed.ok(),consumed.error());
    auto result=ingestion->finish(); need(result.ok(),result.error()); layout=std::move(result.value().layout);
    m.key={"/oracle/oisst.nc",17};m.format_id="hdf5";m.published=true;m.phase=StructuredBlobPhase::Published;
    m.payload_size=payload.size();m.layout_size=layout.size();m.revision=1;
  }
  const StructuredBlobManifest& manifest()const override{return m;}
  QueryResult<BinaryValue> read_payload(std::uint64_t o,std::uint64_t n)const override{++payload_calls;return range(payload,o,n);}
  QueryResult<BinaryValue> read_layout(std::uint64_t o,std::uint64_t n)const override{return range(layout,o,n);}
  static QueryResult<BinaryValue> range(const BinaryValue& b,std::uint64_t o,std::uint64_t n){
    if(o>b.size()||n>b.size()-o)return QueryResult<BinaryValue>::failure(QueryStatus::failure(QueryError::ReadFailure,QueryPhase::Storage,"oracle source range exceeds EOF"));
    return QueryResult<BinaryValue>::success({b.begin()+o,b.begin()+o+n});
  }
  BinaryValue payload,layout;StructuredBlobManifest m;mutable std::uint64_t payload_calls{0};
};
double attr(hid_t d,const char* name,double fallback){
  if(H5Aexists(d,name)<=0)return fallback;
  Id a(H5Aopen(d,name,H5P_DEFAULT),H5Aclose);Id s(H5Aget_space(a),H5Sclose);
  need(H5Sget_simple_extent_npoints(s)==1,"oracle packing attribute must have one value");
  double value;need(H5Aread(a,H5T_NATIVE_DOUBLE,&value)>=0,"native attr read failed");return value;
}
std::vector<double> coordinate(hid_t f,const char* name){
  Id d(H5Dopen2(f,name,H5P_DEFAULT),H5Dclose);Id s(H5Dget_space(d),H5Sclose);
  auto count=H5Sget_simple_extent_npoints(s);need(count>=0&&count<1000000,"oracle coordinate bound");
  std::vector<double> result(count);need(H5Dread(d,H5T_NATIVE_DOUBLE,H5S_ALL,H5S_ALL,H5P_DEFAULT,result.data())>=0,"native coordinate read");return result;
}
DomainSlice slice(const char* field,double lo,double hi){return {{field},DomainBound{LogicalValue::float64(lo),BoundKind::Inclusive},DomainBound{LogicalValue::float64(hi),BoundKind::Exclusive}};}
struct Counts{std::uint64_t queries{0},rows{0},masked{0},pruned{0};};
}
int main(int argc,char**argv){try{
  need(argc==3,"usage: sc_semantic_probe OISST.nc REPORT.json");
  auto source=std::make_shared<Source>(argv[1]);
  Id access(H5Pcreate(H5P_FILE_ACCESS),H5Pclose);need(H5Pset_fapl_sec2(access)>=0,"native sec2");
  Id file(H5Fopen(argv[1],H5F_ACC_RDONLY,access),H5Fclose);
  const auto time=coordinate(file,"/time"),lat=coordinate(file,"/lat"),lon=coordinate(file,"/lon");
  Counts counts;auto runtime=std::make_shared<QueryRuntime>();Hdf5FormatQueryAdapter adapter({},runtime);
  for(const char* path:{"/sst","/anom","/err","/ice"}){
    Id d(H5Dopen2(file,path,H5P_DEFAULT),H5Dclose);Id space(H5Dget_space(d),H5Sclose);
    hsize_t dims[4];need(H5Sget_simple_extent_ndims(space)==4,"oracle rank4 required");H5Sget_simple_extent_dims(space,dims,nullptr);
    need(dims[0]==time.size()&&dims[1]==1&&dims[2]==lat.size()&&dims[3]==lon.size(),"oracle OISST shape mismatch");
    const auto size=H5Sget_simple_extent_npoints(space);need(size>=0&&size<10000000,"oracle payload bound");
    std::vector<std::int16_t> raw(size);need(H5Dread(d,H5T_NATIVE_SHORT,H5S_ALL,H5S_ALL,H5P_DEFAULT,raw.data())>=0,"native raw read failed");
    const auto scale=attr(d,"scale_factor",1),offset=attr(d,"add_offset",0),fill=attr(d,"_FillValue",NAN),missing=attr(d,"missing_value",NAN),minimum=attr(d,"valid_min",-INFINITY),maximum=attr(d,"valid_max",INFINITY);
    for(unsigned mode=0;mode<5;++mode){
      QueryFragment q; q.projection={mode==0?make_hdf5_value_field(path).value():make_hdf5_science_value_field(path).value()};
      if(mode>0)q.projection.insert(q.projection.end(),{{kHdf5TimeField},{kHdf5LatitudeField},{kHdf5LongitudeField}});
      if(mode==2)q.slices={slice(kHdf5LatitudeField,-1,1),slice(kHdf5LongitudeField,100,102),slice(kHdf5TimeField,time.front(),time.front()+1)};
      if(mode==3)q.slices={slice(kHdf5LatitudeField,91,92)};
      if(mode==4)q.predicate=make_comparison(make_hdf5_attribute_field("absent_oracle_attribute").value(),ComparisonOperator::Equal,LogicalValue::utf8("does not exist"));
      const auto calls=source->payload_calls;
      auto plan=adapter.prepare(QueryPlanningContext(*source),q);
      if(!plan.ok()) {
        auto dir=read_hdf5_layout_directory([&](auto o,auto n){return Source::range(source->layout,o,n);},source->layout.size(),source->payload.size());
        if(dir.ok())for(const auto& object:dir.value().document().objects)if(object.dataset)
          std::cerr<<object.canonical_path<<" associations="<<object.axis_associations_reason<<" coordinate="<<object.coordinate_index_reason<<'\n';
      }
      need(plan.ok(),plan.status().message());
      if(mode>0){
        const auto& metadata=plan.value().implementation()->result_metadata();
        auto has=[&](const std::string& key,const std::string& value){return std::find(metadata.begin(),metadata.end(),std::make_pair(key,value))!=metadata.end();};
        need(has("transform.mask_domain","raw") && has("output.value_type","float64") && has("dataset.native_element_bytes","2"),"science result descriptor lost packing evidence");
        need(has("coordinate.time.calendar","standard") && has("coordinate.time.calendar_source","CF_default_standard") && has("coordinate.time.units","days since 1978-01-01 12:00:00"),"science descriptor lost original time convention");
      }
      QueryExecutionOptions options;options.target_batch_rows=4093;
      auto cursor=adapter.execute(QueryExecutionContext(source),plan.value().implementation(),options);need(cursor.ok(),cursor.status().message());
      std::uint64_t actual=0;
      for(;;){auto next=cursor.value()->next();need(next.ok(),next.status().message());if(!next.value())break;
        const auto& b=*next.value();need(bool(b.domain()),"oracle expected domain");
        auto pos=b.domain()->origin;const auto& ext=b.domain()->shape;
        for(std::uint64_t row=0;row<b.row_count();++row){
          const auto linear=((pos[0]*dims[1]+pos[1])*dims[2]+pos[2])*dims[3]+pos[3];
          const auto v=raw.at(linear);
          if(!mode)need(std::get<std::vector<std::int32_t>>(b.columns()[0].values)[row]==v,"raw Int16 mismatch");
          else {
            const bool expected=std::isfinite(v)&&v!=fill&&v!=missing&&v>=minimum&&v<=maximum;
            const auto& out=b.columns()[0];const bool valid=out.validity.empty()||bool(out.validity[row/8]&(1U<<(row%8)));
            need(valid==expected,"scientific mask mismatch");
            if(valid){volatile double multiplied=double(v)*scale;const double value=multiplied+offset;need(std::get<std::vector<double>>(out.values)[row]==value,"scientific scaled value mismatch");}else ++counts.masked;
            need(std::get<std::vector<double>>(b.columns()[1].values)[row]==time.at(pos[0]),"time coordinate mismatch");
            need(std::get<std::vector<double>>(b.columns()[2].values)[row]==lat.at(pos[2]),"latitude mismatch");
            need(std::get<std::vector<double>>(b.columns()[3].values)[row]==lon.at(pos[3]),"longitude mismatch");
            if(mode==2)need(lat[pos[2]]>=-1&&lat[pos[2]]<1&&lon[pos[3]]>=100&&lon[pos[3]]<102,"slice mismatch");
          }
          ++actual;++counts.rows;
          for(std::size_t axis=4;axis-->0;){if(++pos[axis]<b.domain()->origin[axis]+ext[axis])break;pos[axis]=b.domain()->origin[axis];}
        }
      }
      std::uint64_t expected_rows=raw.size();
      if(mode==2)expected_rows=std::count_if(lat.begin(),lat.end(),[](double x){return x>=-1&&x<1;})*std::count_if(lon.begin(),lon.end(),[](double x){return x>=100&&x<102;})*time.size();
      if(mode>=3)expected_rows=0;
      need(actual==expected_rows,"oracle row count mismatch");
      if(mode>=3){need(source->payload_calls==calls,"empty/rejected query read payload");++counts.pruned;}
      ++counts.queries;
    }
  }
  std::ofstream report(argv[2]);report<<"{\"protocol\":\"iotdbfs-hdf5-sc-oracle/1\",\"ok\":true,\"datasets\":4,\"queries\":"<<counts.queries<<",\"rows_checked\":"<<counts.rows<<",\"masked_values_checked\":"<<counts.masked<<",\"zero_payload_queries\":"<<counts.pruned<<",\"native_oracle\":\"H5Dread raw Int16 + independent raw-domain mask and float64 scale\",\"timing_baseline\":false}";need(bool(report),"cannot save oracle report");
  return 0;
}catch(const std::exception&e){std::cerr<<e.what()<<'\n';return 1;}}
