// Read-only, independent graph inspector for namespace-record/2 experiments.
// Never creates schema, adopts a journal, changes configuration or writes rows.
#include "iotdbfs/storage/versioning/ContentArtifacts.h"
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include "iotdbfs/storage/versioning/VersionNamespaceRecord.h"
#include "iotdbfs/DatasetSubstructureIndex.h"
#include "iotdbfs/MetadataSerialization.h"
#include <openssl/evp.h>
#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <tuple>

using namespace iotdbfs;
using namespace iotdbfs::versioning;
namespace {
void check(bool value, const char* message) {
  if (!value) throw Error(ErrorCode::Corrupt, message);
}
std::string env(const char* name) {
  const auto* value=std::getenv(name);
  if (!value || !*value) throw Error(ErrorCode::Invalid, std::string("missing environment variable: ")+name);
  return value;
}
std::string quoted(const std::string& value) {
  std::ostringstream out;out << '"';constexpr char hex[]="0123456789abcdef";
  for(unsigned char c:value) {
    if(c=='"'||c=='\\') out << '\\' << c;
    else if(c<32) out << "\\u00" << hex[c>>4] << hex[c&15];
    else out << c;
  }
  out << '"';return out.str();
}
std::string sha256(const Bytes& value) {
  unsigned char raw[EVP_MAX_MD_SIZE];unsigned size=0;
  check(EVP_Digest(value.data(),value.size(),raw,&size,EVP_sha256(),nullptr)==1&&size==32,"cannot hash content image");
  constexpr char hex[]="0123456789abcdef";std::string out;
  for(unsigned i=0;i<size;++i) {out+=hex[raw[i]>>4];out+=hex[raw[i]&15];}
  return out;
}
struct Wire {
  const Bytes& bytes;std::size_t at{0};
  std::uint64_t number(unsigned width=8) {
    check(at<=bytes.size()&&width<=bytes.size()-at,"truncated graph control integer");
    std::uint64_t result=0;while(width--) result=(result<<8)|bytes[at++];return result;
  }
  std::string text(std::size_t maximum=8192) {
    const auto length=number(4);
    check(length<=maximum&&length<=bytes.size()-at,"invalid graph control text");
    std::string result(bytes.begin()+at,bytes.begin()+at+length);at+=length;return result;
  }
  std::string id(bool optional=false) {
    auto value=text(64);check((optional&&value.empty())||valid_content_id(value),"invalid graph control content id");return value;
  }
  void end() {check(at==bytes.size(),"trailing graph control bytes");}
};
struct Image {std::uint64_t size,chunks;unsigned level;ContentId index,sha;};
std::uint64_t chunk_count(std::uint64_t size) {return size/kContentChunkBytes+(size%kContentChunkBytes!=0);}
struct Inspector {
  IoTDBVersionStore store;
  Head head;
  std::map<std::pair<std::string,ContentId>,Bytes> objects;
  std::map<std::string,std::uint64_t> object_counts;
  std::uint64_t metadata_bytes{0}, image_bytes{0};
  explicit Inspector(const IoTDBClientConfig& config,const std::string& prefix):store(config,prefix) {
    auto raw=store.read_head();check(raw.has_value(),"namespace head is absent");
    head=decode_head(*raw);metadata_bytes=raw->size();
  }
  const Bytes& get(const std::string& type,const ContentId& id) {
    const auto key=std::make_pair(type,id);auto found=objects.find(key);
    if(found!=objects.end()) return found->second;
    check(objects.size()<kScanEntries,"inspect object count budget exceeded");
    auto value=store.get_object(head.identity,{type,"iotdbfs-"+type+(type=="file-descriptor"?"/2":"/1"),id});
    check(metadata_bytes<=kMetadataBytes&&value.size()<=kMetadataBytes-metadata_bytes,"inspect metadata byte budget exceeded");
    metadata_bytes+=value.size();++object_counts[type];
    return objects.emplace(key,std::move(value)).first->second;
  }
  Image image_header(const std::string& kind,const ContentId& id) {
    const auto& bytes=get(kind+"-object",id);check(bytes.size()<=kPageBytes,"image control object exceeds page limit");
    Wire in{bytes};check(in.text()=="iotdbfs-artifact-image/1","unsupported artifact image");
    Image image{in.number(),in.number(),static_cast<unsigned>(in.number(1)),in.id(true),in.id()};in.end();
    check(image.size<=kMetadataBytes&&image.chunks==chunk_count(image.size),"invalid artifact image bounds");
    check((!image.size&&image.index.empty()&&!image.level)||(image.size&&valid_content_id(image.index)&&image.level+2<=kTreeHeight),"invalid artifact image root");
    return image;
  }
  void image_page(const std::string& kind,const ContentId& id,unsigned level,
      std::uint64_t size,std::uint64_t chunks,Bytes& result,std::set<ContentId>& ancestors) {
    check(level+2<=kTreeHeight&&ancestors.insert(id).second,"artifact image cycle or height exceeded");
    const auto& bytes=get(kind+"-index",id);check(bytes.size()<=kPageBytes,"image index exceeds page limit");
    Wire in{bytes};check(in.text()=="iotdbfs-artifact-index/1","unsupported artifact index");
    check(in.number(1)==level,"artifact index level mismatch");auto count=in.number(4);
    check(count&&count<=kPageEntries,"invalid artifact index entry count");
    std::uint64_t total_bytes=0,total_chunks=0;
    for(std::uint64_t i=0;i<count;++i) {
      const auto child_chunks=in.number(),child_bytes=in.number();const auto child_id=in.id();
      check(child_bytes&&child_bytes<=size-total_bytes&&child_chunks<=chunks-total_chunks&&child_chunks==chunk_count(child_bytes),"artifact index child bounds mismatch");
      check(i+1==count||child_bytes==child_chunks*kContentChunkBytes,"short interior artifact child");
      if(level) image_page(kind,child_id,level-1,child_bytes,child_chunks,result,ancestors);
      else {
        check(child_chunks==1,"artifact leaf spans multiple chunks");
        const auto& child=get(kind+"-chunk",child_id);check(child.size()==child_bytes,"artifact chunk size mismatch");
        result.insert(result.end(),child.begin(),child.end());
      }
      total_bytes+=child_bytes;total_chunks+=child_chunks;
    }
    in.end();ancestors.erase(id);check(total_bytes==size&&total_chunks==chunks,"artifact index aggregate mismatch");
  }
  Bytes image(const std::string& kind,const ContentId& id) {
    const auto header=image_header(kind,id);Bytes result;result.reserve(header.size);
    std::set<ContentId> ancestors;
    if(header.size) image_page(kind,header.index,header.level,header.size,header.chunks,result,ancestors);
    check(result.size()==header.size&&sha256(result)==header.sha,"artifact image raw digest mismatch");
    check(image_bytes<=UINT64_MAX-result.size(),"inspect image accounting overflow");image_bytes+=result.size();return result;
  }
};
std::string describe_file(Inspector& scan,const NamespaceRecord& record,const ContentId& record_id) {
  const auto& r=record;
  std::ostringstream out;
  out << "{\"path\":" << quoted(r.inode.path) << ",\"version\":" << r.inode.row_time
    << ",\"format\":" << quoted(r.inode.format_id) << ",\"size\":" << r.inode.size
    << ",\"is_dir\":" << (r.inode.is_dir?"true":"false") << ",\"record_id\":" << quoted(record_id)
    << ",\"descriptor_id\":" << quoted(r.descriptor_id) << ",\"seal_id\":" << quoted(r.seal_id);
  if(!r.inode.is_dir) {
    const auto descriptor=decode_file_descriptor(scan.get("file-descriptor",r.descriptor_id));
    const auto proof=decode_format_proof(scan.get("format-proof",descriptor.proof_id));
    const auto seal=decode_version_seal(scan.get("version-seal",r.seal_id));
    check(seal.identity==scan.head.identity&&seal.key==r.manifest.key&&seal.descriptor_id==r.descriptor_id&&
      seal.proof_id==descriptor.proof_id&&seal.format==r.inode.format_id&&seal.payload_size==r.manifest.payload_size,
      "exact version seal binding mismatch");
    check(descriptor.format==r.inode.format_id&&descriptor.payload_size==r.manifest.payload_size&&
      descriptor.layout_size==r.manifest.layout_size&&proof.format==descriptor.format&&
      proof.payload_size==descriptor.payload_size&&proof.layout_size==descriptor.layout_size&&proof.layout_id==descriptor.layout_id,
      "descriptor/proof/namespace graph mismatch");
    const auto metadata_bytes=scan.image("metadata",descriptor.metadata_id);
    const std::string metadata_text(metadata_bytes.begin(),metadata_bytes.end());
    auto metadata=deserialize_metadata(metadata_text);check(metadata.ok(),"invalid shared metadata");
    auto canonical=serialize_metadata(metadata.value(),65536);
    check(canonical.ok()&&canonical.value()==metadata_text,"noncanonical shared metadata");
    for(const auto& item:metadata.value()) check(item.first.compare(0,23,"__iotdbfs.dataset_index.")!=0,"version envelope leaked into shared metadata");
    const auto layout=scan.image("layout",descriptor.layout_id);
    check(layout.size()==descriptor.layout_size&&sha256(layout)==proof.layout_digest,"layout differs from proof");
    std::string layout_protocol;
    if(!layout.empty()) {
      const auto end=std::find(layout.begin(),layout.begin()+std::min<std::size_t>(128,layout.size()),'\n');
      check(end!=layout.begin()+std::min<std::size_t>(128,layout.size()),"layout omits bounded protocol header");
      layout_protocol.assign(layout.begin(),end+1);
    }
    check(layout_protocol==proof.layout_protocol,"layout protocol differs from proof");
    if(descriptor.format.empty()) check(metadata.value().empty()&&layout.empty()&&proof.catalog_id.empty()&&
      seal.legacy_envelope.empty(),"byte pass-through descriptor contains format evidence");
    if(proof.catalog_id.empty()) check(seal.legacy_envelope.empty(),"outer envelope exists without catalog");
    else {
      const auto bytes=scan.image("catalog",proof.catalog_id);
      check(bytes.size()==proof.catalog_size,"catalog size differs from proof");
      auto catalog=deserialize_dataset_catalog(bytes);check(catalog.ok(),"invalid shared catalog");
      auto canonical_catalog=serialize_dataset_catalog(catalog.value());
      check(canonical_catalog.ok()&&canonical_catalog.value()==bytes,"noncanonical shared catalog");
      std::uint64_t attributes=0;bool complete=true;
      for(const auto& row:catalog.value().records) {
        attributes+=row.attributes.size();complete=complete&&row.attributes_complete;
        for(const auto& attribute:row.attributes) complete=complete&&attribute.state==DatasetAttributeState::Complete;
      }
      check(catalog.value().protocol=="iotdbfs-dataset-catalog/1"&&catalog.value().format_id==proof.format&&
        catalog.value().layout_protocol==proof.layout_protocol&&catalog.value().layout_digest==proof.layout_digest&&
        catalog.value().directory_complete==proof.directory_complete&&catalog.value().records.size()==proof.records&&
        attributes==proof.attributes&&complete==proof.attributes_complete,"catalog facts differ from proof");
      auto manifest=r.manifest;manifest.metadata=metadata.value();
      for(const auto& item:seal.legacy_envelope) check(manifest.metadata.emplace(item).second,"duplicate version envelope metadata");
      check(validate_dataset_substructure_index(manifest,catalog.value()).ok(),"catalog outer exact-version envelope mismatch");
    }
    out << ",\"proof_id\":" << quoted(descriptor.proof_id) << ",\"metadata_id\":" << quoted(descriptor.metadata_id)
      << ",\"layout_id\":" << quoted(descriptor.layout_id) << ",\"catalog_id\":" << quoted(proof.catalog_id)
      << ",\"rules\":" << quoted(proof.rules) << ",\"layout_protocol\":" << quoted(proof.layout_protocol)
      << ",\"layout_digest\":" << quoted(proof.layout_digest)
      << ",\"layout_raw_sha256\":" << quoted(proof.layout_digest) << ",\"layout_bytes\":" << proof.layout_size
      << ",\"catalog_bytes\":" << proof.catalog_size << ",\"catalog_records\":" << proof.records
      << ",\"catalog_attributes\":" << proof.attributes << ",\"directory_complete\":" << (proof.directory_complete?"true":"false")
      << ",\"attributes_complete\":" << (proof.attributes_complete?"true":"false");
  }
  out << '}';return out.str();
}
void inspect(const IoTDBClientConfig& config,const std::string& prefix) {
  Inspector scan(config,prefix);
  const auto commit=decode_commit(scan.get("commit",scan.head.commit));
  check(commit.identity==scan.head.identity&&commit.sequence==scan.head.sequence&&commit.root==scan.head.root,"head/commit mismatch");
  const auto roots=decode_roots(scan.get("catalog-root",scan.head.root));
  auto lookup=[&](Table table,const std::string& key)->std::optional<ContentId> {
    ContentId next=roots[static_cast<unsigned>(table)];std::optional<unsigned> level;
    for(unsigned depth=0;!next.empty();++depth) {
      check(depth<kTreeHeight,"lookup tree height exceeded");const auto page=decode_page(scan.get("catalog-page",next));
      check(!level||page.level==*level,"lookup child level mismatch");
      const auto found=std::lower_bound(page.entries.begin(),page.entries.end(),key,
        [](const PageEntry& entry,const std::string& target){return entry.key<target;});
      if(found==page.entries.end()) return {};
      if(!page.level) return found->key==key?std::optional<ContentId>(found->value):std::nullopt;
      next=found->value;level=page.level-1;
    }
    return {};
  };
  const auto profile=lookup(Table::Current,content_profile_key());
  check(profile&&scan.get("catalog-record",*profile)==content_profile(),"namespace does not have the V02 content profile");
  std::vector<std::pair<ContentId,NamespaceRecord>> records;std::set<ContentId> pages;std::string previous_key;
  std::function<std::string(const ContentId&,std::optional<unsigned>)> visit;
  visit=[&](const ContentId& id,std::optional<unsigned> level)->std::string {
    if(id.empty()) return {};
    check(pages.insert(id).second,"exact tree page cycle or duplicate reference");
    const auto page=decode_page(scan.get("catalog-page",id));check(!level||page.level==*level,"exact child level mismatch");
    for(const auto& entry:page.entries) {
      if(page.level) check(visit(entry.value,page.level-1)==entry.key,"exact child maximum key mismatch");
      else {
        check(records.size()<kScanEntries,"exact record count budget exceeded");
        check(previous_key.empty()||previous_key<entry.key,"exact leaves are out of order");previous_key=entry.key;
        auto record=decode_namespace_record(scan.get("catalog-record",entry.value));
        check(exact_key(record.inode.path,record.inode.row_time)==entry.key,"exact record binding mismatch");
        records.emplace_back(entry.value,std::move(record));
      }
    }
    check(!page.entries.empty(),"empty exact tree page");return page.entries.back().key;
  };
  visit(roots[static_cast<unsigned>(Table::Exact)],{});
  std::vector<std::string> descriptions;
  for(const auto& record:records) descriptions.push_back(describe_file(scan,record.second,record.first));
  const auto final=scan.store.read_head();check(final&&decode_head(*final)==scan.head,"head changed during readonly inspection");
  std::cout << "{\"completed\":true,\"command\":\"inspect\",\"prefix\":" << quoted(prefix)
    << ",\"head\":{\"namespace\":" << quoted(scan.head.identity.namespace_id) << ",\"incarnation\":" << quoted(scan.head.identity.incarnation)
    << ",\"seq\":" << scan.head.sequence << ",\"commit\":" << quoted(scan.head.commit) << ",\"root\":" << quoted(scan.head.root)
    << "},\"namespace_record_protocol\":\"iotdbfs-namespace-record/2\",\"graph_verified\":true,\"exact_pages\":" << pages.size()
    << ",\"record_count\":" << records.size() << ",\"unique_objects\":" << scan.objects.size() << ",\"metadata_bytes\":" << scan.metadata_bytes
    << ",\"reconstituted_image_bytes\":" << scan.image_bytes << ",\"object_counts\":{";
  bool first=true;for(const auto& item:scan.object_counts) {std::cout << (first?"":",") << quoted(item.first) << ':' << item.second;first=false;}
  std::cout << "},\"records\":[";
  for(std::size_t i=0;i<descriptions.size();++i) std::cout << (i?",":"") << descriptions[i];
  std::cout << "]}\n";
}
} // namespace
int main(int argc,char** argv) {
  std::string stage="arguments";
  try {
    std::string prefix,command;
    for(int i=1;i<argc;++i) {
      const std::string arg=argv[i];
      if(arg=="inspect"&&command.empty()) command=arg;
      else if(arg=="--command"&&i+1<argc) command=argv[++i];
      else if(arg=="--prefix"&&i+1<argc) prefix=argv[++i];
      else throw Error(ErrorCode::Invalid,"usage: v02_content_probe inspect --prefix vd_NAME");
    }
    check(command=="inspect"&&!prefix.empty(),"inspect command and prefix are required");stage="configuration";
    IoTDBClientConfig config;config.host=env("IOTDB_HOST");config.user=env("IOTDB_USER");config.pass=env("IOTDB_PASS");config.db=env("IOTDB_DB");
    const auto port=env("IOTDB_PORT");
    check(!port.empty()&&port.size()<=5&&port.find_first_not_of("0123456789")==std::string::npos,"invalid port");
    config.port=std::stoi(port);check(config.port>0&&config.port<=65535,"invalid port");
    check(config.host=="127.0.0.1"||config.host=="localhost","controlled loopback endpoint required");config.host="127.0.0.1";
    check(std::none_of(config.db.begin(),config.db.end(),[](char c){return c>='A'&&c<='Z';}),"database name must be canonical lowercase");
    config.auto_create_db=false;stage="inspect";inspect(config,prefix);return 0;
  } catch(const Error& error) {
    std::cout << "{\"completed\":false,\"stage\":" << quoted(stage) << ",\"error\":" << quoted(error.what()) << "}\n";return 1;
  } catch(const std::exception&) {
    std::cout << "{\"completed\":false,\"stage\":" << quoted(stage)
      << ",\"error\":\"probe rejected or assertion failed; no automatic retry\"}\n";return 1;
  }
}
