#include "../../support/InMemoryDatasetCatalog.h"
// H03 artifact/lifecycle validation harness. Native source reads use pread;
// the in-memory backend observes the same production B/L/Sigma publication API.
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5Layout.h"
#include <algorithm>
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
std::string hex(const BinaryValue& v) {std::string s;constexpr char h[]="0123456789abcdef";for(auto b:v){s+=h[b>>4];s+=h[b&15];}return s;}
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
 StructuredBlobResult<BinaryValue> read_payload_range(const StructuredBlobVersionKey&,std::uint64_t o,std::uint64_t n) const override {return range(payload,o,n);}
 StructuredBlobResult<BinaryValue> read_layout_range(const StructuredBlobVersionKey&,std::uint64_t o,std::uint64_t n) const override {return range(layout,o,n);}
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
int main(int argc,char** argv){try{
 need(argc==3,"usage: h03_ingestion_probe FILE OUTPUT_DIRECTORY");
 const std::filesystem::path out=argv[2];std::filesystem::create_directories(out);
 Fd file;file.value=::open(argv[1],O_RDONLY|O_CLOEXEC);struct stat stat{};
 need(file.value>=0&&::fstat(file.value,&stat)==0&&S_ISREG(stat.st_mode)&&stat.st_size>=0&&stat.st_size<=64*1024*1024,"invalid or oversized probe file");
 auto backend=std::make_shared<Backend>();auto storage=std::make_shared<StructuredBlobStorage>(backend);
 struct Read{std::uint64_t o,n;bool after_copy;};std::vector<Read> reads;
 FormatSource source{static_cast<std::uint64_t>(stat.st_size),{}};
 source.read=[&](std::uint64_t o,std::uint64_t n)->std::optional<BinaryValue>{
  if(o>source.size||n>source.size-o)return std::nullopt;
  reads.push_back({o,n,backend->written==source.size});BinaryValue b(n);std::size_t done=0;
  while(done<b.size()){auto got=::pread(file.value,b.data()+done,b.size()-done,static_cast<off_t>(o+done));if(got<0&&errno==EINTR)continue;if(got<=0)return std::nullopt;done+=got;}return b;
 };
 FormatRegistry registry;need(registry.register_adapter(std::make_shared<Hdf5FormatAdapter>()),"adapter registration failed");
 IngestionModule ingest(*storage,registry,{65536});StructuredBlobVersionKey key{"/baseline/wrong-extension.bin",17};
 bool publication_checked=false;
 auto result=ingest.ingest(key,source,[&](const IngestionOutcome&){
  need(!storage->open_published_version(key).ok(),"unpublished version was visible");publication_checked=true;return true;
 });
 need(result.ok(),result.message());need(publication_checked,"publication callback missing");
 need(result.outcome().format_id==std::optional<std::string>("hdf5"),"HDF5 identity was not detected");
 need(backend->written==source.size,"payload not copied exactly once");
 auto opened=open_structured_blob_query_source(storage,key);need(opened.ok(),opened.status().message());
 std::uint64_t layout_reads=0;
 Hdf5LayoutRead reader=[&](std::uint64_t o,std::uint64_t n){layout_reads+=n;return opened.value()->read_layout(o,n);};
 auto parsed=read_hdf5_layout_directory(reader,backend->layout.size(),source.size);need(parsed.ok(),parsed.status().message());
 const auto& directory=parsed.value();
 save(out/"payload.bin",backend->payload);save(out/"layout.bin",backend->layout);
 std::ofstream json(out/"inventory.json",std::ios::trunc);
 // Directory accessors keep validated metadata immutable across page reads.
 const auto& doc=directory.document();
 json<<"{\"schema_version\":\"iotdbfs-hdf5-h03-probe/1\",\"format_id\":\"hdf5\",\"source_bytes\":"<<source.size
     <<",\"payload_written\":"<<backend->written<<",\"layout_bytes\":"<<backend->layout.size()
     <<",\"directory_complete\":"<<(doc.directory_complete?"true":"false")<<",\"root\":"<<q(doc.root_object_id)<<",\"datasets\":[";
 Hdf5LayoutReadBudget charged;
 bool first=true;
 for(const auto& object:doc.objects){if(!object.dataset)continue;if(!first)json<<',';first=false;const auto& d=*object.dataset;
  json<<"{\"id\":"<<q(object.id)<<",\"path\":"<<q(d.descriptor.path)<<",\"supported\":"<<(d.descriptor.supported?"true":"false")
      <<",\"reason\":"<<q(d.descriptor.unsupported_reason)<<",\"index_complete\":"<<(d.index_complete?"true":"false")
      <<",\"index_reason\":"<<q(d.index_unavailable_reason)<<",\"shape\":";numbers(json,d.descriptor.shape);
  json<<",\"chunk_shape\":";numbers(json,d.chunk_shape);
  json<<",\"element_bytes\":"<<d.element_bytes<<",\"storage\":"<<static_cast<int>(d.storage)<<",\"byte_order\":"<<static_cast<int>(d.byte_order)
      <<",\"allocated_chunks\":"<<d.allocated_chunk_count<<",\"fill\":"<<q(hex(d.fill_value))<<",\"fill_status\":"<<static_cast<int>(d.fill_status)
      <<",\"contiguous_offset\":"<<(d.contiguous_offset?std::to_string(*d.contiguous_offset):"null")<<",\"contiguous_size\":"<<d.contiguous_size<<",\"filters\":[";
  for(std::size_t i=0;i<d.filters.size();++i){if(i)json<<',';json<<"{\"id\":"<<d.filters[i].id<<",\"parameters\":";numbers(json,d.filters[i].parameters);json<<'}';}
  json<<"],\"chunks\":[";
  if(d.index_complete&&d.descriptor.supported&&d.storage==Hdf5StorageLayout::Chunked){
   auto chunks=read_hdf5_layout_chunks(reader,directory,object.id,std::vector<std::uint64_t>(d.descriptor.shape.size(),0),d.descriptor.shape,charged);
   need(chunks.ok(),chunks.status().message());
   for(std::size_t i=0;i<chunks.value().size();++i){if(i)json<<',';const auto& c=chunks.value()[i];json<<"{\"origin\":";numbers(json,c.origin);json<<",\"offset\":"<<c.offset<<",\"size\":"<<c.encoded_size<<",\"filter_mask\":"<<c.filter_mask<<'}';}
  }
  json<<"]}";
 }
 json<<"],\"links\":[";
 for(std::size_t i=0;i<doc.links.size();++i){if(i)json<<',';const auto& l=doc.links[i];json<<"{\"parent\":"<<q(l.parent_id)<<",\"name\":"<<q(l.name)<<",\"target\":"<<q(l.target_id)<<",\"kind\":"<<static_cast<int>(l.kind)<<'}';}
 json<<"],\"source_reads\":[";
 for(std::size_t i=0;i<reads.size();++i){if(i)json<<',';const auto& r=reads[i];json<<"{\"offset\":"<<r.o<<",\"length\":"<<r.n<<",\"after_copy\":"<<(r.after_copy?"true":"false")<<'}';}
 json<<"],\"layout_read_bytes\":"<<layout_reads<<"}\n";json.close();need(bool(json),"report write failed");return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
