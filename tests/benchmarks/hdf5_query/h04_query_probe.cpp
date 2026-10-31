#include "../../support/InMemoryDatasetCatalog.h"
// H04 typed query oracle through production ingestion, storage and QueryModule.
// The adapter sees only immutable stored bytes, never the fixture pathname.
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatQueryAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5QueryModel.h"
#include "iotdbfs/query/core/QueryModule.h"
#include <cstring>
#include <sstream>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <cerrno>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <stdexcept>
#include <sys/stat.h>
#include <unistd.h>
using namespace iotdbfs;
namespace {
std::string q(const std::string& s) {
  std::string out="\""; constexpr char h[]="0123456789abcdef";
  for(unsigned char c:s) { if(c=='"'||c=='\\'){out+='\\';out+=c;}
    else if(c<32){out+="\\u00";out+=h[c>>4];out+=h[c&15];}else out+=c; }
  return out+'"';
}
template<class T> void numbers(std::ostream& out,const std::vector<T>& a) {
  out<<'['; for(std::size_t i=0;i<a.size();++i){if(i)out<<',';out<<a[i];}out<<']';
}
void need(bool yes,const std::string& why){if(!yes)throw std::runtime_error(why);}
class Backend final : public StructuredBlobStorageBackend {
public:
  iotdbfs::test::InMemoryDatasetCatalog dataset_catalogs;
  StructuredBlobStatus write_dataset_catalog(const StructuredBlobVersionKey& key,
      const DatasetCatalogArtifact& value) override { return dataset_catalogs.write(key, value); }
  StructuredBlobResult<DatasetCatalogArtifact> read_dataset_catalog(
      const StructuredBlobVersionKey& key) const override { return dataset_catalogs.read(key); }

 std::optional<StructuredBlobManifest> manifest;
 BinaryValue payload,layout;
 std::uint64_t written{0};
 struct Read {std::string phase,kind;std::uint64_t offset,length;};
 mutable std::vector<Read> reads;
 mutable std::mutex read_mutex;
 mutable std::atomic<std::uint64_t> calls{0}, active{0}, peak{0}, batch_calls{0};
 bool multi_range{false};
 std::uint64_t latency_us{0};
 struct CallGuard {
  const Backend& backend;
  explicit CallGuard(const Backend& b):backend(b){
   ++backend.calls;auto n=++backend.active;auto high=backend.peak.load();
   while(high<n&&!backend.peak.compare_exchange_weak(high,n)){}
   if(backend.latency_us)std::this_thread::sleep_for(std::chrono::microseconds(backend.latency_us));
  }
  ~CallGuard(){--backend.active;}
 };
 void record(const char* kind,std::uint64_t o,std::uint64_t n)const{
  std::lock_guard<std::mutex> lock(read_mutex);reads.push_back({phase,kind,o,n});
 }
 std::string phase="ingestion";
 StructuredBlobStatus create_manifest(const StructuredBlobManifest& m) override {
  if(manifest)return bad();manifest=m;return StructuredBlobStatus::success();
 }
 StructuredBlobResult<StructuredBlobManifest> load_manifest(const StructuredBlobVersionKey& k) const override {
  if(!manifest||!(manifest->key==k))return StructuredBlobResult<StructuredBlobManifest>::failure(StructuredBlobError::NotFound,"absent");
  return StructuredBlobResult<StructuredBlobManifest>::success(*manifest);
 }
 StructuredBlobStatus replace_manifest(const StructuredBlobVersionKey& k,std::uint64_t revision,const StructuredBlobManifest& m) override {
  if(!manifest||!(manifest->key==k)||manifest->revision!=revision)return bad();manifest=m;return StructuredBlobStatus::success();
 }
 StructuredBlobStatus write_payload_range(const StructuredBlobVersionKey&,std::uint64_t offset,const BinaryValue& b) override {
  if(offset!=payload.size())return bad();payload.insert(payload.end(),b.begin(),b.end());written+=b.size();return StructuredBlobStatus::success();
 }
 StructuredBlobStatus write_layout(const StructuredBlobVersionKey&,const BinaryValue& b) override {layout=b;return StructuredBlobStatus::success();}
 StructuredBlobStatus write_metadata(const StructuredBlobVersionKey&,const MetadataMap&) override {return StructuredBlobStatus::success();}
 StructuredBlobResult<BinaryValue> read_payload_range(const StructuredBlobVersionKey&,std::uint64_t o,std::uint64_t n) const override {CallGuard call(*this);record("payload",o,n);return range(payload,o,n);}
 bool supports_payload_ranges()const override{return multi_range;}
 StructuredBlobResult<StructuredBlobPayloadBatch> read_payload_ranges(const StructuredBlobVersionKey&,const StructuredBlobPayloadRanges& ranges)const override{
  CallGuard call(*this);++batch_calls;StructuredBlobPayloadBatch out;
  for(const auto& r:ranges){record("payload",r.offset,r.length);auto b=range(payload,r.offset,r.length);
   if(!b.ok())return StructuredBlobResult<StructuredBlobPayloadBatch>::failure(b.status().code(),b.status().message());
   out.push_back(std::move(b.value()));}
  return StructuredBlobResult<StructuredBlobPayloadBatch>::success(std::move(out));
 }
 StructuredBlobResult<BinaryValue> read_layout_range(const StructuredBlobVersionKey&,std::uint64_t o,std::uint64_t n) const override {record("layout",o,n);return range(layout,o,n);}
 StructuredBlobStatus erase(const StructuredBlobVersionKey&,std::uint64_t revision) override {
  if(!manifest||manifest->revision!=revision)return bad();manifest.reset();payload.clear();layout.clear();return StructuredBlobStatus::success();
 }
 static StructuredBlobStatus bad(){return StructuredBlobStatus::failure(StructuredBlobError::BackendFailure,"probe lifecycle violation");}
 static StructuredBlobResult<BinaryValue> range(const BinaryValue& b,std::uint64_t o,std::uint64_t n){
  if(o>b.size()||n>b.size()-o)return StructuredBlobResult<BinaryValue>::failure(StructuredBlobError::RangeError,"invalid range");
  return StructuredBlobResult<BinaryValue>::success({b.begin()+o,b.begin()+o+n});
 }
};
void save(const std::filesystem::path& p,const BinaryValue& b){std::ofstream f(p,std::ios::binary|std::ios::trunc);f.write(reinterpret_cast<const char*>(b.data()),b.size());f.close();need(bool(f),"artifact write failed");}
struct Fd{int value{-1};~Fd(){if(value>=0)::close(value);}};
}
namespace {
std::vector<std::uint64_t> dimensions(const std::string& text) {
 std::vector<std::uint64_t> result;std::istringstream input(text);std::string item;
 while(std::getline(input,item,',')) {need(!item.empty()&&item.find_first_not_of("0123456789")==std::string::npos,"bad dimensions");result.push_back(std::stoull(item));}
 need(!result.empty()&&result.size()<=3,"rank must be 1..3");return result;
}
const char* error_name(QueryError error) {
 switch(error) {
  case QueryError::None:return "None";
  case QueryError::UnsupportedCapability:return "UnsupportedCapability";
  case QueryError::CorruptLayout:return "CorruptLayout";
  case QueryError::CorruptPayload:return "CorruptPayload";
  case QueryError::InvalidDomainSlice:return "InvalidDomainSlice";
  case QueryError::ResourceExhausted:return "ResourceExhausted";
  case QueryError::ReadFailure:return "ReadFailure";
  default:return "Other";
 }
}
template<class T> void append_canonical(BinaryValue& out,const std::vector<T>& values) {
 for(T value:values){std::conditional_t<sizeof(T)==4,std::uint32_t,std::uint64_t> bits;std::memcpy(&bits,&value,sizeof(T));
  for(std::size_t i=0;i<sizeof(T);++i)out.push_back(static_cast<std::uint8_t>(bits>>(8*i)));}
}
}
int main(int argc,char** argv){try{
 need(argc==8,"usage: h04_query_probe FILE OUTPUT_DIRECTORY DATASET ORIGIN_CSV SHAPE_CSV TARGET_ROWS AXES_0_OR_1");
 const std::filesystem::path out=argv[2];std::filesystem::create_directories(out);
 const auto origin=dimensions(argv[4]),shape=dimensions(argv[5]);need(origin.size()==shape.size(),"shape rank mismatch");
 const auto rows=std::stoull(argv[6]);need(rows>0&&rows<=1000000,"invalid batch row target");
 const bool axes=std::string(argv[7])=="1";need(axes||std::string(argv[7])=="0","invalid axes flag");
 Fd file;file.value=::open(argv[1],O_RDONLY|O_CLOEXEC);struct stat stat{};
 need(file.value>=0&&::fstat(file.value,&stat)==0&&S_ISREG(stat.st_mode)&&stat.st_size>=0&&stat.st_size<=64*1024*1024,"invalid or oversized probe file");
 auto backend=std::make_shared<Backend>();auto storage=std::make_shared<StructuredBlobStorage>(backend);
 FormatSource source{static_cast<std::uint64_t>(stat.st_size),{}};
 source.read=[&](std::uint64_t o,std::uint64_t n)->std::optional<BinaryValue>{
  if(o>source.size||n>source.size-o)return std::nullopt;
  BinaryValue b(n);std::size_t done=0;
  while(done<b.size()){auto got=::pread(file.value,b.data()+done,b.size()-done,static_cast<off_t>(o+done));if(got<0&&errno==EINTR)continue;if(got<=0)return std::nullopt;done+=got;}return b;
 };
 FormatRegistry registry;need(registry.register_adapter(std::make_shared<Hdf5FormatAdapter>()),"adapter registration failed");
 IngestionModule ingest(*storage,registry,{65536});StructuredBlobVersionKey key{"/baseline/typed-source.bin",17};
 auto ingested=ingest.ingest(key,source,[](const IngestionOutcome&){return true;});
 need(ingested.ok(),ingested.message());need(backend->written==source.size,"payload not copied exactly once");
 Hdf5FragmentMetrics metrics; Hdf5QueryKernelConfig config;
 config.metrics_observer=[&](const Hdf5FragmentMetrics& current){metrics=current;};
 auto module=QueryModule::create(make_in_memory_published_version_catalog({*backend->manifest}),storage,
  {std::make_shared<Hdf5FormatQueryAdapter>(config)});need(module.ok(),module.status().message());
 QueryRequest request;request.sources.paths={{PathScopeKind::Exact,key.path}};
 request.sources.versions=VersionSelection::exact_version(key.version);
 request.sources.formats={AdapterSelectionKind::Listed,{"hdf5"},false};
 request.subset=QueryFragment{};auto value_field=make_hdf5_value_field(argv[3]);need(value_field.ok(),value_field.status().message());
 // Value last when axes are requested, to catch assumptions about projection positions.
 if(axes)for(std::size_t i=0;i<origin.size();++i)request.subset->projection.push_back(hdf5_axis_field(i));
 request.subset->projection.push_back(value_field.value());
 for(std::size_t i=0;i<origin.size();++i){need(shape[i]<=std::numeric_limits<std::uint64_t>::max()-origin[i],"slice overflow");
  request.subset->slices.push_back({hdf5_axis_field(i),DomainBound{LogicalValue::uint64(origin[i]),BoundKind::Inclusive},
   DomainBound{LogicalValue::uint64(origin[i]+shape[i]),BoundKind::Exclusive}});}
 request.limits.target_batch_rows=rows;
 backend->phase="prepare";auto prepared=module.value()->prepare(request);
 QueryStatus status=prepared.status();std::vector<QueryBatch> batches;BinaryValue values;
 std::optional<LogicalSchema> schema;
 if(prepared.ok()) {
  schema=prepared.value().result_schema();backend->phase="execute";
  auto cursor=module.value()->execute(prepared.value());status=cursor.status();
  if(cursor.ok()) {schema=cursor.value()->schema();
   for(;;){auto next=cursor.value()->next();if(!next.ok()){status=next.status();break;}if(!next.value())break;
    const auto& b=*next.value();auto index=b.schema().index_of(value_field.value().field_id);need(index.has_value(),"missing projected values");
    const auto& c=b.columns()[*index];need(c.validity.empty(),"unexpected H04 nullable output");
    if(c.type.id==LogicalTypeId::Float32)append_canonical(values,std::get<std::vector<float>>(c.values));
    else if(c.type.id==LogicalTypeId::Float64)append_canonical(values,std::get<std::vector<double>>(c.values));
    else throw std::runtime_error("non-floating typed values");
    batches.push_back(std::move(*next.value()));
   }
  }
 }
 save(out/"values.bin",values);
 std::ofstream json(out/"query.json",std::ios::trunc);
 json<<"{\"schema_version\":\"iotdbfs-hdf5-h04-probe/1\",\"ok\":"<<(status.ok()?"true":"false")
     <<",\"error\":"<<q(error_name(status.code()))<<",\"message\":"<<q(status.message())
     <<",\"source_bytes\":"<<source.size<<",\"layout_bytes\":"<<backend->layout.size()<<",\"value_bytes\":"<<values.size()<<",\"schema\":[";
 if(schema)for(std::size_t i=0;i<schema->fields().size();++i){if(i)json<<',';const auto& field=schema->fields()[i];
  json<<"{\"id\":"<<q(field.field_id)<<",\"type\":"<<q(field.type.id==LogicalTypeId::Float32?"Float32":field.type.id==LogicalTypeId::Float64?"Float64":field.type.id==LogicalTypeId::UInt64?"UInt64":"Other")<<'}';}
 json<<"],\"batches\":[";
 for(std::size_t i=0;i<batches.size();++i){if(i)json<<',';const auto& batch=batches[i];need(batch.domain().has_value(),"missing DenseGrid domain");
  const auto& domain=*batch.domain();need(domain.kind==DomainKind::DenseGrid,"wrong domain kind");
  json<<"{\"rows\":"<<batch.row_count()<<",\"origin\":";numbers(json,domain.origin);json<<",\"shape\":";numbers(json,domain.shape);
  json<<",\"dimensions\":[";for(std::size_t a=0;a<domain.dimensions.size();++a){if(a)json<<',';json<<q(domain.dimensions[a].field.field_id);}json<<"],\"axes\":[";
  if(axes)for(std::size_t a=0;a<origin.size();++a){if(a)json<<',';auto index=batch.schema().index_of(hdf5_axis_field(a).field_id);need(index.has_value(),"missing axis projection");numbers(json,std::get<std::vector<std::uint64_t>>(batch.columns()[*index].values));}
  json<<"],\"sources\":[";for(std::size_t s=0;s<batch.sources().size();++s){if(s)json<<',';const auto& ref=batch.sources()[s];json<<"{\"path\":"<<q(ref.path)<<",\"version\":"<<ref.version<<",\"format\":"<<q(ref.format_id)<<'}';}json<<"]}";
 }
 json<<"],\"metrics\":{\"payload_bytes\":"<<metrics.payload_bytes<<",\"payload_ranges\":"<<metrics.payload_ranges
     <<",\"decoded_bytes\":"<<metrics.decoded_bytes<<",\"inflated_bytes\":"<<metrics.inflated_bytes
     <<",\"fill_bytes\":"<<metrics.fill_bytes<<",\"useful_bytes\":"<<metrics.useful_bytes
     <<",\"cache_hits\":"<<metrics.cache_hits<<",\"decode_count\":"<<metrics.decode_count<<"},\"reads\":[";
 for(std::size_t i=0;i<backend->reads.size();++i){if(i)json<<',';const auto& r=backend->reads[i];json<<"{\"phase\":"<<q(r.phase)<<",\"kind\":"<<q(r.kind)<<",\"offset\":"<<r.offset<<",\"length\":"<<r.length<<'}';}
 json<<"]}\n";json.close();need(bool(json),"report write failed");return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
