#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"

#include <openssl/evp.h>
#include <algorithm>
#include <array>
#include <limits>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <type_traits>

namespace iotdbfs::versioning {
namespace {
using MetadataTrace = std::map<std::string,std::uint64_t>;
struct MetadataTraceCapture;
thread_local MetadataTraceCapture* active_trace = nullptr;
thread_local std::string metadata_source;
struct MetadataTraceCapture {
  MetadataTrace trace;
  MetadataTraceCapture* previous{active_trace};
  MetadataTraceCapture() { active_trace=this; }
  ~MetadataTraceCapture() { active_trace=previous; }
};
struct MetadataSourceScope {
  std::string previous;
  MetadataSourceScope(const NamespaceIdentity& identity,const StructuredBlobVersionKey& key)
    : previous(std::move(metadata_source)) {
    metadata_source=identity.namespace_id+identity.incarnation+":"+
      std::to_string(key.path.size())+":"+key.path+":"+std::to_string(key.version)+":";
  }
  ~MetadataSourceScope() { metadata_source=std::move(previous); }
};
void observe_artifact_metadata(const std::string& identity,std::uint64_t bytes) {
  // Cache traces are source-neutral, then charged separately for each exact
  // logical source. Repeated control checks of that source reuse evidence.
  require_query_metadata("artifact:"+metadata_source+identity,bytes);
  for(auto* capture=active_trace;capture;capture=capture->previous) {
    auto& existing=capture->trace[identity];existing=std::max(existing,bytes);
  }
}
void check(bool value, const char* message) {
  if (!value) throw Error(ErrorCode::Corrupt, message);
}
void charge(std::uint64_t& used, std::uint64_t bytes) {
  if (used>kMetadataBytes || bytes>kMetadataBytes-used)
    throw Error(ErrorCode::Limit,"content metadata budget exhausted");
  used+=bytes;
}
struct Out {
  Bytes bytes;
  void number(std::uint64_t n, unsigned width=8) {
    for (unsigned i=width;i;--i) bytes.push_back(n>>((i-1)*8));
  }
  void text(const std::string& value, std::size_t maximum=8192) {
    check(value.size()<=maximum,"content string exceeds limit");
    number(value.size(),4); bytes.insert(bytes.end(),value.begin(),value.end());
  }
  void id(const ContentId& id, bool optional=false) {
    check((optional&&id.empty())||valid_content_id(id),"invalid content reference"); text(id,64);
  }
};
struct In {
  const Bytes& bytes; std::size_t at{0};
  explicit In(const Bytes& b):bytes(b) { check(b.size()<=kPageBytes,"content control object exceeds page limit"); }
  std::uint64_t number(unsigned width=8) {
    check(width<=bytes.size()-at,"truncated content integer"); std::uint64_t n=0;
    while (width--) n=(n<<8)|bytes[at++]; return n;
  }
  std::string text(std::size_t maximum=8192) {
    const auto n=number(4); check(n<=maximum&&n<=bytes.size()-at,"invalid content string");
    std::string value(bytes.begin()+at,bytes.begin()+at+n); at+=n; return value;
  }
  ContentId id(bool optional=false) {
    auto value=text(64); check((optional&&value.empty())||valid_content_id(value),"invalid content reference"); return value;
  }
  bool boolean() { const auto value=number(1); check(value<=1,"invalid content boolean"); return value; }
  void end() { check(at==bytes.size(),"trailing content bytes"); }
};
std::string metadata_wire(const MetadataMap& values) {
  auto result=serialize_metadata(values,65536); check(result.ok(),"invalid content metadata"); return result.value();
}
MetadataMap metadata_parse(const std::string& wire) {
  auto result=deserialize_metadata(wire); check(result.ok(),"invalid content metadata encoding");
  check(metadata_wire(result.value())==wire,"noncanonical content metadata"); return result.value();
}
std::string digest(const Bytes& bytes) {
  unsigned char out[EVP_MAX_MD_SIZE]; unsigned size=0;
  check(EVP_Digest(bytes.data(),bytes.size(),out,&size,EVP_sha256(),nullptr)==1&&size==32,"cannot hash artifact");
  constexpr char hex[]="0123456789abcdef"; std::string result;
  for (unsigned i=0;i<size;++i) { result+=hex[out[i]>>4]; result+=hex[out[i]&15]; }
  return result;
}
std::string protocol(const Bytes& layout) {
  if (layout.empty()) return {};
  auto end=std::find(layout.begin(),layout.begin()+std::min<std::size_t>(layout.size(),128),'\n');
  check(end!=layout.begin()+std::min<std::size_t>(layout.size(),128),"layout has no bounded protocol header");
  return {layout.begin(),end+1};
}
constexpr const char* legacy_prefix="__iotdbfs.dataset_index.";
bool legacy_field(const std::string& key) {
  for (const auto* name:{"protocol","sha256","bytes","records","attributes"})
    if (key==std::string(legacy_prefix)+name) return true;
  return false;
}
void check_legacy(const MetadataMap& metadata) {
  if (metadata.empty()) return;
  check(metadata.size()==5,"partial legacy version envelope");
  for (const auto& item:metadata) check(legacy_field(item.first),"unknown legacy version envelope field");
  StructuredBlobManifest m; m.metadata=metadata;
  check(read_dataset_substructure_seal(m).ok(),"invalid legacy version envelope");
}
void check_descriptor(const FileDescriptor& d) {
  check(d.format.size()<=128&&d.format.find('\0')==std::string::npos,"invalid descriptor format");
  check(d.payload_size<=INT64_MAX&&d.layout_size<=kMetadataBytes,"invalid descriptor lengths");
  check(valid_content_id(d.metadata_id)&&valid_content_id(d.layout_id)&&valid_content_id(d.proof_id)&&valid_content_id(d.payload_root),"descriptor references missing");
}
void check_proof(const FormatProof& p) {
  check(p.rules==kContentRules&&p.format.size()<=128&&p.format.find('\0')==std::string::npos,"unsupported proof rules or format");
  check(valid_content_id(p.layout_id)&&valid_content_id(p.layout_digest),"invalid proof layout identity");
  check(p.catalog_id.empty()||valid_content_id(p.catalog_id),"invalid proof catalog identity");
  check(p.payload_size<=INT64_MAX&&p.layout_size<=kMetadataBytes&&p.catalog_size<=kMaxDatasetCatalogBytes,"invalid proof sizes");
  check(p.layout_protocol.size()<=128&&p.layout_protocol.find('\0')==std::string::npos,"invalid proof layout protocol");
  check((p.layout_size==0)==p.layout_protocol.empty(),"layout length/protocol mismatch");
  if (p.catalog_id.empty()) check(!p.catalog_size&&!p.records&&!p.attributes&&!p.directory_complete&&!p.attributes_complete,"absent catalog has evidence");
  else check(p.catalog_size&&p.records<=p.catalog_size&&p.attributes<=p.catalog_size,"catalog proof counts exceed bytes");
}
void check_seal(const VersionSeal& s) {
  check(valid_uuid(s.identity.namespace_id)&&valid_uuid(s.identity.incarnation),"invalid version seal namespace");
  (void)exact_key(s.key.path,s.key.version);
  check(s.format.size()<=128&&s.payload_size<=INT64_MAX&&valid_content_id(s.descriptor_id)&&valid_content_id(s.proof_id),"invalid version seal");
  check_legacy(s.legacy_envelope);
}
struct Link { std::uint64_t chunks{0}, bytes{0}; ContentId id; };
struct IndexPage { unsigned level{0}; std::vector<Link> children; };
struct Image { std::uint64_t bytes{0},chunks{0}; unsigned level{0}; ContentId index; std::string sha; };
std::uint64_t chunks_for(std::uint64_t bytes) { return bytes/kContentChunkBytes+(bytes%kContentChunkBytes!=0); }
void check_page(const IndexPage& page) {
  check(page.level+2<=kTreeHeight&&!page.children.empty()&&page.children.size()<=kPageEntries,"invalid content index height/count");
  std::uint64_t bytes=0,chunks=0;
  for (std::size_t i=0;i<page.children.size();++i) {
    const auto& child=page.children[i];
    check(valid_content_id(child.id)&&child.bytes&&child.bytes<=kMetadataBytes&&child.chunks==chunks_for(child.bytes),"invalid content child");
    check(page.level||child.chunks==1,"content leaf spans multiple chunks");
    check(i+1==page.children.size()||child.bytes==child.chunks*kContentChunkBytes,"short interior content child");
    check(child.bytes<=kMetadataBytes-bytes&&child.chunks<=kScanEntries-chunks,"content index sum overflow");
    bytes+=child.bytes; chunks+=child.chunks;
  }
}
Bytes encode_index(const IndexPage& page) {
  check_page(page); Out out;out.text("iotdbfs-artifact-index/1");out.number(page.level,1);out.number(page.children.size(),4);
  for (const auto& child:page.children) { out.number(child.chunks);out.number(child.bytes);out.id(child.id); }
  check(out.bytes.size()<=kPageBytes,"content index page exceeds byte limit");return out.bytes;
}
IndexPage decode_index(const Bytes& bytes) {
  In in(bytes);check(in.text()=="iotdbfs-artifact-index/1","unsupported content index");IndexPage page;page.level=in.number(1);
  auto count=in.number(4);check(count&&count<=kPageEntries,"invalid content index count");
  while (count--) page.children.push_back({in.number(),in.number(),in.id()});
  in.end();check_page(page);return page;
}
Bytes encode_image(const Image& image) {
  check(image.bytes<=kMetadataBytes&&image.chunks==chunks_for(image.bytes)&&valid_content_id(image.sha),"invalid content image");
  check((!image.bytes&&image.index.empty()&&!image.level)||(image.bytes&&valid_content_id(image.index)&&image.level+2<=kTreeHeight),"invalid content image root");
  Out out;out.text("iotdbfs-artifact-image/1");out.number(image.bytes);out.number(image.chunks);out.number(image.level,1);out.id(image.index,true);out.id(image.sha);return out.bytes;
}
Image decode_image(const Bytes& bytes) {
  In in(bytes);check(in.text()=="iotdbfs-artifact-image/1","unsupported content image");
  Image image;image.bytes=in.number();image.chunks=in.number();image.level=in.number(1);image.index=in.id(true);image.sha=in.id();in.end();
  check(encode_image(image)==bytes,"noncanonical content image");return image;
}
std::size_t metadata_memory(const MetadataMap& metadata) {
  std::size_t bytes=sizeof(metadata)+256;
  for (const auto& item:metadata) {
    bytes+=sizeof(item)+128+item.first.capacity();
    std::visit([&](const auto& v) {
      using T=std::decay_t<decltype(v)>;
      if constexpr (std::is_same_v<T,std::string>) bytes+=v.capacity()+32;
      else if constexpr (std::is_same_v<T,MetadataStringSet>) {
        for (const auto& text:v) bytes+=128+text.capacity();
      } else if constexpr (!std::is_arithmetic_v<T>) bytes+=v.size()*(sizeof(typename T::value_type)+128);
    },item.second.value());
  }
  return bytes;
}
std::size_t catalog_memory(const DatasetCatalogArtifact& artifact) {
  std::size_t bytes=sizeof(artifact)+256+artifact.protocol.capacity()+artifact.format_id.capacity()+artifact.layout_protocol.capacity()+artifact.layout_digest.capacity();
  bytes+=artifact.records.capacity()*sizeof(DatasetCatalogRecord);
  for (const auto& r:artifact.records) {
    bytes+=r.object_id.capacity()+r.canonical_path.capacity()+128+r.attributes.capacity()*sizeof(DatasetCatalogAttribute);
    for (const auto& a:r.attributes) {
      bytes+=a.name.capacity()+a.reason.capacity()+128+a.shape.capacity()*sizeof(std::uint64_t);
      if (a.value) if (auto text=std::get_if<std::string>(&*a.value)) bytes+=text->capacity()+32;
    }
  }
  return bytes;
}
} // namespace

std::string content_profile_key() { return std::string(1,'\0')+"iotdbfs-namespace-profile"; }
Bytes content_profile() {
  const std::string text="iotdbfs-namespace-record/3\n"+std::string(kContentRules)+"\nidentity-free-wire-bytes\n60KiB-pages\n1MiB-payload-blocks\ncomplete-extent-map/1\n";
  return {text.begin(),text.end()};
}
Bytes encode_file_descriptor(const FileDescriptor& d) {
  check_descriptor(d);Out out;out.text("iotdbfs-file-descriptor/2");out.text(d.format,128);out.number(d.payload_size);out.number(d.layout_size);out.id(d.metadata_id);out.id(d.layout_id);out.id(d.proof_id);out.id(d.payload_root);return out.bytes;
}
FileDescriptor decode_file_descriptor(const Bytes& bytes) {
  In in(bytes);check(in.text()=="iotdbfs-file-descriptor/2","unsupported file descriptor");FileDescriptor d;d.format=in.text(128);d.payload_size=in.number();d.layout_size=in.number();d.metadata_id=in.id();d.layout_id=in.id();d.proof_id=in.id();d.payload_root=in.id();in.end();check_descriptor(d);return d;
}
Bytes encode_format_proof(const FormatProof& p) {
  check_proof(p);Out out;out.text("iotdbfs-format-proof/1");out.text(p.rules,128);out.text(p.format,128);out.text(p.layout_protocol,128);out.id(p.layout_digest);out.id(p.layout_id);out.id(p.catalog_id,true);
  for (auto n:{p.payload_size,p.layout_size,p.catalog_size,p.records,p.attributes}) out.number(n);
  out.number(p.directory_complete,1);out.number(p.attributes_complete,1);return out.bytes;
}
FormatProof decode_format_proof(const Bytes& bytes) {
  In in(bytes);check(in.text()=="iotdbfs-format-proof/1","unsupported format proof");FormatProof p;p.rules=in.text(128);p.format=in.text(128);p.layout_protocol=in.text(128);p.layout_digest=in.id();p.layout_id=in.id();p.catalog_id=in.id(true);
  p.payload_size=in.number();p.layout_size=in.number();p.catalog_size=in.number();p.records=in.number();p.attributes=in.number();p.directory_complete=in.boolean();p.attributes_complete=in.boolean();in.end();check_proof(p);return p;
}
Bytes encode_version_seal(const VersionSeal& s) {
  check_seal(s);Out out;out.text("iotdbfs-version-seal/1");out.text(s.identity.namespace_id,36);out.text(s.identity.incarnation,36);out.text(s.key.path,4096);out.number(s.key.version);out.text(s.format,128);out.number(s.payload_size);out.id(s.descriptor_id);out.id(s.proof_id);out.text(metadata_wire(s.legacy_envelope),65536);
  check(out.bytes.size()<=kPageBytes,"version seal exceeds page size");return out.bytes;
}
VersionSeal decode_version_seal(const Bytes& bytes) {
  In in(bytes);check(in.text()=="iotdbfs-version-seal/1","unsupported version seal");VersionSeal s;s.identity={in.text(36),in.text(36)};s.key.path=in.text(4096);auto v=in.number();check(v<=INT64_MAX,"version seal version overflow");s.key.version=v;s.format=in.text(128);s.payload_size=in.number();s.descriptor_id=in.id();s.proof_id=in.id();s.legacy_envelope=metadata_parse(in.text(65536));in.end();check_seal(s);return s;
}

struct ContentArtifacts::Impl {
  std::shared_ptr<Owner> owner; LayoutValidator validator; std::size_t maximum;
  NamespaceIdentity identity;
  // Protect only LRU/accounting. Never hold this lock while calling Owner,
  // Transaction, format validators, or remote I/O. Concurrent cold misses may
  // duplicate bounded work; no waiter can deadlock a writer holding Owner.
  mutable std::recursive_mutex mutex;
  mutable ContentArtifactStats counters;
  struct Entry { std::shared_ptr<const void> value; std::size_t bytes; std::uint64_t logical; MetadataTrace trace; std::list<std::string>::iterator position; };
  mutable std::map<std::string,Entry> entries;
  mutable std::list<std::string> lru;
  Impl(std::shared_ptr<Owner> o,LayoutValidator v,std::size_t bytes):owner(std::move(o)),validator(std::move(v)),maximum(bytes) {
    check(bool(owner),"content artifacts require owner");
    identity=owner->view().head.identity;
  }
  View view() const {
    auto value=owner->view();check(value.head.identity==identity,"content cache namespace changed");return value;
  }
  std::string key(const std::string& category,const ContentId& id) const {
    return identity.namespace_id+identity.incarnation+category+":"+id;
  }
  template<class T> std::shared_ptr<const T> get(const std::string& k,std::uint64_t& used,bool force=false) const {
    if (force) return {};
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it=entries.find(k);if(it==entries.end()) return {};
    charge(used,it->second.logical);
    for(const auto& item:it->second.trace) observe_artifact_metadata(item.first,item.second);
    lru.splice(lru.begin(),lru,it->second.position);
    return std::static_pointer_cast<const T>(it->second.value);
  }
  template<class T> void put(const std::string& k,std::shared_ptr<const T> value,std::size_t bytes,std::uint64_t logical,MetadataTrace trace={}) const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    if (!maximum||bytes>maximum) return;
    if(trace.empty() && logical) trace.emplace(k,logical);
    for(const auto& item:trace) bytes+=item.first.capacity()+sizeof(MetadataTrace::value_type)+64;
    bytes+=sizeof(Entry)+2*k.capacity()+256;
    if(bytes>maximum) return;
    auto found=entries.find(k);
    if(found!=entries.end()) { counters.cache_bytes-=found->second.bytes;lru.erase(found->second.position);entries.erase(found); }
    while(!lru.empty()&&bytes>maximum-counters.cache_bytes) {
      auto old=entries.find(lru.back());counters.cache_bytes-=old->second.bytes;entries.erase(old);lru.pop_back();++counters.evictions;
    }
    lru.push_front(k);
    try { entries.emplace(k,Entry{std::move(value),bytes,logical,std::move(trace),lru.begin()}); }
    catch(...) { lru.pop_front();throw; }
    counters.cache_bytes+=bytes;counters.cache_entries=entries.size();
  }
  void count(std::uint64_t ContentArtifactStats::* member, std::uint64_t n=1) const {
    std::lock_guard<std::recursive_mutex> lock(mutex);counters.*member+=n;
  }
  Bytes read(const std::string& type,const ContentId& id,std::uint64_t& used,bool force=false) const {
    const auto k=key("raw/"+type,id);
    if (auto cached=get<Bytes>(k,used,force)) { count(&ContentArtifactStats::object_cache_hits);query_profile_count("content.object.cache_hits");return *cached; }
    auto v=view();auto result=owner->read_artifact(v,{type,"iotdbfs-"+type+(type=="file-descriptor"?"/2":"/1"),id},used);
    observe_artifact_metadata(k,result.size());
    count(&ContentArtifactStats::object_reads);count(&ContentArtifactStats::hash_bytes,result.size());query_profile_count("content.object.reads");query_profile_count("content.object.hash_bytes",result.size());
    auto bytes=std::make_shared<const Bytes>(result);put(k,bytes,sizeof(Bytes)+bytes->capacity(),bytes->size());return result;
  }
  ContentId write(Transaction& tx,const std::string& type,const Bytes& bytes) {
    const auto id=tx.put_artifact(type,bytes).id;
    put(key("raw/"+type,id),std::make_shared<const Bytes>(bytes),sizeof(Bytes)+bytes.size(),bytes.size());return id;
  }
  ContentId image(Transaction& tx,const std::string& kind,const Bytes& bytes) {
    check(bytes.size()<=kMetadataBytes,"artifact image exceeds metadata limit");
    std::vector<Link> nodes;
    for(std::size_t at=0;at<bytes.size();at+=kContentChunkBytes) {
      Bytes chunk(bytes.begin()+at,bytes.begin()+std::min(bytes.size(),at+kContentChunkBytes));
      nodes.push_back({1,chunk.size(),write(tx,kind+"-chunk",chunk)});
    }
    Image image;image.bytes=bytes.size();image.chunks=nodes.size();image.sha=digest(bytes);
    if(!nodes.empty()) {
      unsigned level=0;
      for(;;) {
        check(level+2<=kTreeHeight,"artifact image exceeds tree height");std::vector<Link> next;
        for(std::size_t at=0;at<nodes.size();at+=kPageEntries) {
          IndexPage page{level,{nodes.begin()+at,nodes.begin()+std::min(nodes.size(),at+kPageEntries)}};
          Link link;for(const auto& child:page.children) {link.bytes+=child.bytes;link.chunks+=child.chunks;}
          link.id=write(tx,kind+"-index",encode_index(page));next.push_back(std::move(link));
        }
        if(next.size()==1) {image.index=next.front().id;image.level=level;break;}
        nodes=std::move(next);++level;
      }
    }
    return write(tx,kind+"-object",encode_image(image));
  }
  Image header(const std::string& kind,const ContentId& id,std::uint64_t& used,bool force=false) const {
    return decode_image(read(kind+"-object",id,used,force));
  }
  void range_page(const std::string& kind,const Link& link,unsigned level,std::uint64_t offset,std::uint64_t length,
      Bytes& out,std::uint64_t& used,bool force,std::set<ContentId>& ancestors) const {
    check(level+2<=kTreeHeight&&ancestors.insert(link.id).second,"content index cycle or height");
    const auto page=decode_index(read(kind+"-index",link.id,used,force));check(page.level==level,"content child level mismatch");
    std::uint64_t bytes=0,chunks=0;
    for(const auto& child:page.children) {bytes+=child.bytes;chunks+=child.chunks;}
    check(bytes==link.bytes&&chunks==link.chunks,"content child bounds mismatch");
    std::uint64_t begin=0;
    for(const auto& child:page.children) {
      if(offset<begin+child.bytes&&length) {
        const auto within=offset>begin?offset-begin:0;
        const auto wanted=std::min(length,child.bytes-within);
        if(level) range_page(kind,child,level-1,within,wanted,out,used,force,ancestors);
        else {
          auto chunk=read(kind+"-chunk",child.id,used,force);check(chunk.size()==child.bytes,"content chunk length mismatch");
          out.insert(out.end(),chunk.begin()+within,chunk.begin()+within+wanted);
        }
        length-=wanted;offset+=wanted;
      }
      begin+=child.bytes;
    }
    ancestors.erase(link.id);check(!length,"short artifact image read");
  }
  Bytes image_range(const std::string& kind,const ContentId& id,std::uint64_t offset,std::uint64_t length,
      std::uint64_t& used,bool force=false) const {
    const auto image=header(kind,id,used,force);
    check(offset<=image.bytes&&length<=image.bytes-offset,"artifact range outside image");
    Bytes result;if(!length) return result;
    result.reserve(length);std::set<ContentId> ancestors;
    range_page(kind,{image.chunks,image.bytes,image.index},image.level,offset,length,result,used,force,ancestors);
    check(result.size()==length,"short content image read");
    if(offset==0&&length==image.bytes) check(digest(result)==image.sha,"artifact image digest mismatch");
    return result;
  }
  Bytes image_all(const std::string& kind,const ContentId& id,std::uint64_t& used,bool force=false) const {
    const auto image=header(kind,id,used,force);
    if(!image.bytes) {check(image.sha==digest({}),"empty content image digest mismatch");return {};}
    return image_range(kind,id,0,image.bytes,used,force);
  }
  std::shared_ptr<const MetadataMap> metadata(const ContentId& id,std::uint64_t& used,bool force) const {
    const auto k=key("metadata/parsed",id);
    if(auto cached=get<MetadataMap>(k,used,force)) return cached;
    MetadataTraceCapture captured;
    const auto before=used;auto bytes=image_all("metadata",id,used,force);
    check(bytes.size()<=65536,"metadata image exceeds document limit");
    auto parsed=std::make_shared<const MetadataMap>(metadata_parse({bytes.begin(),bytes.end()}));
    for(const auto& item:*parsed) check(item.first.compare(0,std::char_traits<char>::length(legacy_prefix),legacy_prefix)!=0,"version envelope in shared metadata");
    count(&ContentArtifactStats::metadata_parses);query_profile_count("content.metadata.parses");put(k,parsed,metadata_memory(*parsed),used-before,std::move(captured.trace));return parsed;
  }
  std::shared_ptr<const DatasetCatalogArtifact> catalog(const FormatProof& proof,std::uint64_t& used,bool force=false) const {
    if(proof.catalog_id.empty()) return {};
    const auto image=header("catalog",proof.catalog_id,used,force);
    check(image.bytes==proof.catalog_size,"catalog byte count differs from proof");
    const auto k=key("catalog/parsed",proof.catalog_id);std::shared_ptr<const DatasetCatalogArtifact> parsed;
    if(!(parsed=get<DatasetCatalogArtifact>(k,used,force))) {
      MetadataTraceCapture captured;
      const auto before=used;auto bytes=image_all("catalog",proof.catalog_id,used,force);
      check(bytes.size()==proof.catalog_size,"catalog byte count differs from proof");
      auto result=deserialize_dataset_catalog(bytes);check(result.ok(),"invalid shared catalog");
      parsed=std::make_shared<const DatasetCatalogArtifact>(std::move(result.value()));
      count(&ContentArtifactStats::catalog_parses);query_profile_count("content.catalog.parses");put(k,parsed,catalog_memory(*parsed),used-before,std::move(captured.trace));
    }
    std::uint64_t attributes=0;bool complete=true;
    for(const auto& row:parsed->records) {
      attributes+=row.attributes.size();complete=complete&&row.attributes_complete;
      for(const auto& attribute:row.attributes) complete=complete&&attribute.state==DatasetAttributeState::Complete;
    }
    check(parsed->format_id==proof.format&&parsed->layout_protocol==proof.layout_protocol&&parsed->layout_digest==proof.layout_digest&&
      parsed->protocol=="iotdbfs-dataset-catalog/1"&&parsed->directory_complete==proof.directory_complete&&parsed->records.size()==proof.records&&attributes==proof.attributes&&complete==proof.attributes_complete,
      "catalog differs from immutable proof");
    return parsed;
  }
  void validate_layout(const std::string& format,const Bytes& layout,std::uint64_t payload_size,std::uint64_t& used,bool force=false) const {
    const auto sha=digest(layout);const auto k=key("validated-layout/"+format+"/"+std::to_string(payload_size)+"/"+kContentRules,sha);
    if(get<bool>(k,used,force)) return;
    if(format.empty()) check(layout.empty(),"byte pass-through has nonempty layout");
    else {check(bool(validator),"missing layout validator");auto status=validator(format,layout,payload_size);check(status.ok(),"format layout validation failed");}
    count(&ContentArtifactStats::layout_validations);query_profile_count("content.layout.validations");put(k,std::make_shared<const bool>(true),sizeof(bool),0);
  }
};

ContentArtifacts::ContentArtifacts(std::shared_ptr<Owner> owner,LayoutValidator validator,std::size_t bytes)
  :impl_(std::make_unique<Impl>(std::move(owner),std::move(validator),bytes)) {}
ContentArtifacts::~ContentArtifacts()=default;
ContentBinding ContentArtifacts::build(Transaction& tx,const StructuredBlobManifest& manifest,const Bytes& layout,
    const std::optional<DatasetCatalogArtifact>& catalog,const ContentId& payload_root) {
  auto& p=*impl_;const auto view=p.view();
  check(manifest.payload_size<=INT64_MAX&&manifest.layout_size==layout.size(),"manifest layout length mismatch");
  std::uint64_t used=0;p.validate_layout(manifest.format_id.value_or(""),layout,manifest.payload_size,used);
  MetadataMap metadata,legacy;
  for(const auto& item:manifest.metadata) {
    if(item.first.compare(0,std::char_traits<char>::length(legacy_prefix),legacy_prefix)==0) {
      check(legacy_field(item.first),"unknown version-bound metadata field");legacy.insert(item);
    } else metadata.insert(item);
  }
  check_legacy(legacy);
  if(!manifest.format_id) check(metadata.empty()&&legacy.empty()&&!catalog,"byte pass-through has format evidence");
  FormatProof proof;proof.format=manifest.format_id.value_or("");proof.payload_size=manifest.payload_size;proof.layout_size=layout.size();proof.layout_protocol=protocol(layout);proof.layout_digest=digest(layout);
  proof.layout_id=p.image(tx,"layout",layout);
  if(catalog) {
    auto sealed=validate_dataset_substructure_index(manifest,*catalog);check(sealed.ok(),"catalog differs from outer source envelope");
    check(catalog->format_id==proof.format&&catalog->layout_protocol==proof.layout_protocol&&catalog->layout_digest==proof.layout_digest,"catalog/layout identity mismatch");
    auto bytes=serialize_dataset_catalog(*catalog);check(bytes.ok(),"invalid catalog encoding");
    proof.catalog_id=p.image(tx,"catalog",bytes.value());proof.catalog_size=bytes.value().size();proof.directory_complete=catalog->directory_complete;proof.attributes_complete=true;proof.records=catalog->records.size();
    for(const auto& row:catalog->records) {proof.attributes+=row.attributes.size();proof.attributes_complete=proof.attributes_complete&&row.attributes_complete;for(const auto& a:row.attributes) proof.attributes_complete=proof.attributes_complete&&a.state==DatasetAttributeState::Complete;}
  } else check(legacy.empty(),"outer catalog envelope without catalog");
  const auto wire=metadata_wire(metadata);Bytes metadata_bytes(wire.begin(),wire.end());
  FileDescriptor descriptor{proof.format,manifest.payload_size,layout.size(),p.image(tx,"metadata",metadata_bytes),proof.layout_id,{}};
  descriptor.payload_root=payload_root;
  descriptor.proof_id=p.write(tx,"format-proof",encode_format_proof(proof));
  const auto descriptor_id=p.write(tx,"file-descriptor",encode_file_descriptor(descriptor));
  VersionSeal seal{view.head.identity,manifest.key,proof.format,manifest.payload_size,descriptor_id,descriptor.proof_id,std::move(legacy)};
  const auto seal_id=p.write(tx,"version-seal",encode_version_seal(seal));
  return {descriptor_id,seal_id};
}
std::shared_ptr<const VerifiedFileContent> ContentArtifacts::verify(const ContentBinding& binding,
    const StructuredBlobVersionKey& key,const std::optional<std::string>& format,std::uint64_t payload_size,
    std::uint64_t layout_size,bool deep,std::uint64_t* shared_bytes) const {
  auto& p=*impl_;const auto view=p.view();
  MetadataSourceScope source(view.head.identity,key);
  std::uint64_t local=0;auto& used=shared_bytes?*shared_bytes:local;const auto before=used;
  const auto k=p.key("verified-file/"+binding.descriptor_id,binding.seal_id);
  auto result=p.get<VerifiedFileContent>(k,used,deep);
  if(!result) {
    MetadataTraceCapture captured;
    auto file=std::make_shared<VerifiedFileContent>();file->binding=binding;
    file->descriptor=decode_file_descriptor(p.read("file-descriptor",binding.descriptor_id,used,deep));
    file->seal=decode_version_seal(p.read("version-seal",binding.seal_id,used,deep));
    file->proof=decode_format_proof(p.read("format-proof",file->descriptor.proof_id,used,deep));
    const auto& d=file->descriptor;const auto& proof=file->proof;const auto& seal=file->seal;
    check(seal.identity==view.head.identity&&seal.descriptor_id==binding.descriptor_id&&seal.proof_id==d.proof_id&&
      seal.format==d.format&&seal.payload_size==d.payload_size&&proof.format==d.format&&proof.layout_id==d.layout_id&&proof.payload_size==d.payload_size&&proof.layout_size==d.layout_size,"file proof/seal graph mismatch");
    const auto image=p.header("layout",d.layout_id,used,deep);check(image.bytes==d.layout_size&&image.sha==proof.layout_digest,"layout image differs from proof");
    file->metadata=*p.metadata(d.metadata_id,used,deep);
    for(const auto& item:seal.legacy_envelope) check(file->metadata.emplace(item).second,"duplicate outer metadata field");
    auto catalog=p.catalog(proof,used,deep);
    if(catalog) {
      StructuredBlobManifest manifest;manifest.key=seal.key;manifest.format_id=seal.format;manifest.payload_size=d.payload_size;manifest.layout_size=d.layout_size;manifest.metadata=file->metadata;
      check(validate_dataset_substructure_index(manifest,*catalog).ok(),"legacy outer envelope differs from content");
    } else check(seal.legacy_envelope.empty(),"catalog envelope without shared catalog");
    if(d.format.empty()) check(d.layout_size==0&&file->metadata.empty()&&proof.catalog_id.empty(),"byte pass-through has format evidence");
    if(deep) {auto bytes=p.image_all("layout",d.layout_id,used,true);check(protocol(bytes)==proof.layout_protocol,"layout protocol differs from proof");p.validate_layout(d.format,bytes,d.payload_size,used,true);}
    check(file->seal.key==key&&file->descriptor.format==format.value_or("")&&file->descriptor.payload_size==payload_size&&file->descriptor.layout_size==layout_size,"namespace binding differs from verified content");
    result=file;
    p.put(k,result,sizeof(VerifiedFileContent)+metadata_memory(file->metadata)+metadata_memory(file->seal.legacy_envelope)+2048,used-before,std::move(captured.trace));
  }
  check(result->seal.identity==view.head.identity&&result->seal.key==key&&result->descriptor.format==format.value_or("")&&
    result->descriptor.payload_size==payload_size&&result->descriptor.layout_size==layout_size,"namespace binding differs from verified content");
  return result;
}
Bytes ContentArtifacts::read_layout(const VerifiedFileContent& file,std::uint64_t offset,std::uint64_t length) const {
  auto& p=*impl_;check(file.seal.identity==p.view().head.identity,"layout belongs to another namespace");
  MetadataSourceScope source(file.seal.identity,file.seal.key);
  std::uint64_t used=0;return p.image_range("layout",file.descriptor.layout_id,offset,length,used);
}
std::shared_ptr<const DatasetCatalogArtifact> ContentArtifacts::catalog(const VerifiedFileContent& file) const {
  auto& p=*impl_;check(file.seal.identity==p.view().head.identity,"catalog belongs to another namespace");
  MetadataSourceScope source(file.seal.identity,file.seal.key);
  std::uint64_t used=0;return p.catalog(file.proof,used);
}
std::string ContentArtifacts::layout_identity(const VerifiedFileContent& file) const {
  auto& p=*impl_;check(file.seal.identity==p.view().head.identity,"layout identity belongs to another namespace");
  Out out;out.text(file.seal.identity.namespace_id);out.text(file.seal.identity.incarnation);out.text(file.descriptor.format);out.text(file.proof.rules);out.text(file.proof.layout_protocol);out.id(file.descriptor.layout_id);out.number(file.descriptor.payload_size);
  return content_id("verified-layout-identity","iotdbfs-verified-layout-identity/1",out.bytes);
}
ContentArtifactStats ContentArtifacts::stats() const {std::lock_guard<std::recursive_mutex> lock(impl_->mutex);return impl_->counters;}
void ContentArtifacts::clear_cache() {std::lock_guard<std::recursive_mutex> lock(impl_->mutex);impl_->entries.clear();impl_->lru.clear();impl_->counters.cache_bytes=0;impl_->counters.cache_entries=0;}
} // namespace iotdbfs::versioning
