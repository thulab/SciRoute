#include "iotdbfs/storage/versioning/PayloadArtifacts.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/IoTDBProfile.h"
#include <algorithm>
#include <limits>
#include <map>
#include <set>

namespace iotdbfs::versioning {
namespace {
void check(bool value, const char* message, ErrorCode code = ErrorCode::Corrupt) {
  if (!value) throw Error(code, message);
}
std::uint64_t add(std::uint64_t a, std::uint64_t b) {
  check(b <= std::numeric_limits<std::uint64_t>::max() - a, "payload extent arithmetic overflow");
  return a+b;
}
std::uint64_t blocks(std::uint64_t size) {
  return size/kPayloadBlockBytes+(size%kPayloadBlockBytes!=0);
}
struct Out {
  Bytes b;
  void number(std::uint64_t n, unsigned count=8) {
    for (unsigned i=count; i; --i) b.push_back(static_cast<std::uint8_t>(n >> ((i-1)*8)));
  }
  void magic(const char* p) { b.insert(b.end(),p,p+4); }
  void id(const ContentId& id) {
    check(valid_content_id(id), "invalid payload content identity");
    b.insert(b.end(),id.begin(),id.end());
  }
};
struct In {
  const Bytes& b; std::size_t at{0};
  void need(std::size_t n) { check(n<=b.size()-at,"truncated payload mapping"); }
  std::uint64_t number(unsigned n=8) { need(n); std::uint64_t v=0; while(n--)v=(v<<8)|b[at++]; return v; }
  void magic(const char* p) { need(4); check(std::equal(b.begin()+at,b.begin()+at+4,p),"unsupported payload mapping codec");at+=4; }
  ContentId id() { need(64);ContentId v(b.begin()+at,b.begin()+at+64);at+=64;check(valid_content_id(v),"invalid payload mapping identity");return v; }
  void end() { check(at==b.size(),"trailing payload mapping bytes"); }
};
void validate(const PayloadMap& m) {
  check(m.extent_count==blocks(m.payload_size),
    "payload map extent count exceeds bounds");
  if (!m.payload_size) {
    check(!m.extent_count&&!m.height&&!m.page_bytes&&m.page.empty(),"nonempty mapping for empty payload");
    return;
  }
  check(m.height>0&&m.height<=kTreeHeight&&valid_content_id(m.page)&&
    m.page_bytes>=33&&m.page_bytes<=kPageBytes,"invalid payload map root");
}
void validate(const ExtentPage& p) {
  check(p.level<kTreeHeight&&p.begin<p.end&&p.begin%kPayloadBlockBytes==0&&
    p.extent_count>0,"invalid extent page bounds");
  const auto count=p.level?p.children.size():p.extents.size();
  check(count>0&&count<=kPageEntries&&(p.level?p.extents.empty():p.children.empty()),"invalid extent page entries");
  std::uint64_t cursor=p.begin,total=0;
  if (!p.level) {
    for (const auto& e:p.extents) {
      check(e.logical_offset==cursor&&e.length>0&&e.length<=kPayloadBlockBytes&&
        e.logical_offset%kPayloadBlockBytes==0&&e.content_offset==0&&e.content_size==e.length&&
        valid_content_id(e.content_id),"invalid fixed payload extent");
      cursor=add(cursor,e.length);++total;
      check(e.length==kPayloadBlockBytes||cursor==p.end,"short payload block before page end");
    }
  } else {
    for (const auto& c:p.children) {
      check(c.begin==cursor&&c.begin<c.end&&c.begin%kPayloadBlockBytes==0&&
        c.extent_count>0&&c.extent_count<=(1ULL<<(8*p.level))&&
        c.encoded_size>=33&&c.encoded_size<=kPageBytes&&valid_content_id(c.page),"invalid extent child");
      cursor=c.end;total=add(total,c.extent_count);
    }
  }
  check(cursor==p.end&&total==p.extent_count&&total==blocks(p.end-p.begin),"extent page has a hole, overlap or wrong count");
}
ObjectReadRequest root_request(const ContentId& root) {
  check(valid_content_id(root),"invalid payload map identity");
  return {{"payload-map","iotdbfs-payload-map/1",root},kPayloadMapEncodedBytes};
}
std::string scope(const View& view,const ContentId& root,const std::string& source) {
  return "payload-map/"+view.head.identity.namespace_id+"/"+view.head.identity.incarnation+"/"+std::to_string(source.size())+":"+source+"/"+root+"/";
}
void charge_map(const View& view,const ContentId& root,const ObjectReadRequest& r,const std::string& source) {
  require_query_metadata(scope(view,root,source)+r.ref.type+"/"+r.ref.id,r.size);
}
PayloadMap load_map(const std::shared_ptr<Owner>& owner,const View& view,
    const ContentId& root,std::uint64_t size,std::uint64_t& metadata,bool force,const std::string& source) {
  storage_read_checkpoint();
  auto r=root_request(root);charge_map(view,root,r,source);
  auto result=owner->read_artifacts(view,{r},metadata,false,force);
  auto map=decode_payload_map(result.at(0));
  check(map.payload_size==size,"payload map and descriptor lengths differ");
  query_profile_count("payload_map.root_requests");return map;
}
using Wanted = std::function<bool(std::uint64_t,std::uint64_t)>;
// Each level's frontier is batched. We validate every fetched child's bounds,
// level and count against its authenticated parent before using its extents.
std::vector<PayloadExtent> walk(const std::shared_ptr<Owner>& owner,const View& view,
    const ContentId& root,const PayloadMap& m,const Wanted& wanted,
    std::uint64_t& metadata,bool force,const std::string& source,
    const std::function<void(const std::vector<PayloadExtent>&)>& consume = {}) {
  std::vector<PayloadExtent> out;std::uint64_t visited=0;
  if(!m.payload_size)return out;
  std::vector<PayloadChild> frontier{{0,m.payload_size,m.extent_count,m.page_bytes,m.page}};
  std::set<ContentId> seen;
  for(unsigned level=m.height;level;--level) {
    std::vector<PayloadChild> next;
    for(std::size_t first=0;first<frontier.size();) {
      std::vector<ObjectReadRequest> requests;std::uint64_t bytes=0;
      const auto begin=first;
      while(first<frontier.size()&&requests.size()<kObjectBatchEntries&&frontier[first].encoded_size<=kObjectBatchBytes-bytes) {
        const auto& c=frontier[first++];
        check(seen.insert(c.page).second,"repeated or cyclic extent page");
        ObjectReadRequest r{{"extent-page","iotdbfs-extent-page/1",c.page},c.encoded_size};
        bytes+=r.size;charge_map(view,root,r,source);requests.push_back(std::move(r));
      }
      check(!requests.empty(),"extent page batch exceeds budget",ErrorCode::Limit);
      storage_read_checkpoint();
      auto encoded=owner->read_artifacts(view,requests,metadata,false,force);
      storage_read_checkpoint();
      query_profile_count("payload_map.page_requests",requests.size());
      for(std::size_t i=0;i<encoded.size();++i) {
        const auto& c=frontier[begin+i];auto p=decode_extent_page(encoded[i]);
        check(p.level==level-1&&p.begin==c.begin&&p.end==c.end&&p.extent_count==c.extent_count,
          "extent child does not match parent bounds or level");
        check(p.end<=m.payload_size,"extent page exceeds payload length");
        if(!p.level) {
          for(const auto& e:p.extents) {
            check(e.length==std::min<std::uint64_t>(kPayloadBlockBytes,m.payload_size-e.logical_offset),"extent is not the canonical fixed block");
            if(!consume&&wanted(e.logical_offset,add(e.logical_offset,e.length)))out.push_back(e);
          }
          if(consume){visited=add(visited,p.extents.size());consume(p.extents);}
        } else for(const auto& child:p.children)if(wanted(child.begin,child.end))next.push_back(child);
      }
    }
    frontier=std::move(next);
  }
  check(frontier.empty(),"extent map height is inconsistent");
  if(consume)check(visited==m.extent_count,"payload map closure is incomplete");
  return out;
}
void visit_blocks(const std::shared_ptr<Owner>& owner,const View& view,
    const std::vector<PayloadExtent>& extents,bool force,
    const std::function<void(const ContentId&,const Bytes&)>& visitor) {
  std::map<ContentId,std::uint64_t> unique;
  for(const auto& e:extents) {
    auto result=unique.emplace(e.content_id,e.content_size);
    check(result.second||result.first->second==e.content_size,"one payload identity has conflicting lengths");
  }
  auto it=unique.begin();
  while(it!=unique.end()) {
    std::vector<ObjectReadRequest> requests;std::uint64_t bytes=0;
    while(it!=unique.end()&&requests.size()<kObjectBatchEntries&&it->second<=kObjectBatchBytes-bytes) {
      requests.push_back({{"payload-block","iotdbfs-payload-block/1",it->first},it->second});bytes+=it->second;++it;
    }
    check(!requests.empty(),"payload block exceeds batch budget",ErrorCode::Limit);
    // Account full blocks, SDK/result copies and digest framing in the
    // shared runtime, beyond the adapter's logical-range reservation.
    auto temporary=reserve_storage_read_temporary(4*bytes);
    record_storage_read_io(bytes,requests.size());
    IoTDBReadObservation observation(true,requests.size(),bytes);
    std::uint64_t unused=0;auto values=owner->read_artifacts(view,requests,unused,true,force,temporary);
    observation.success(bytes);
    storage_read_checkpoint();
    query_profile_count("payload_map.data_batches");query_profile_count("payload_map.block_reads",values.size());
    query_profile_count("payload_map.block_bytes",bytes);query_profile_count("payload_map.hash_bytes",bytes);
    for(std::size_t i=0;i<values.size();++i)visitor(requests[i].ref.id,values[i]);
  }
}
} // namespace

Bytes encode_payload_map(const PayloadMap& m) {
  validate(m);Out o;o.magic("VPM1");o.number(m.payload_size);o.number(m.extent_count);o.number(m.height,1);o.number(m.page_bytes);o.id(m.page.empty()?std::string(64,'0'):m.page);return o.b;
}
PayloadMap decode_payload_map(const Bytes& b) {
  check(b.size()==kPayloadMapEncodedBytes,"invalid payload map encoded length");In in{b};in.magic("VPM1");PayloadMap m;
  m.payload_size=in.number();m.extent_count=in.number();m.height=static_cast<std::uint8_t>(in.number(1));m.page_bytes=in.number();m.page=in.id();in.end();if(!m.height&&m.page==std::string(64,'0'))m.page.clear();validate(m);return m;
}
Bytes encode_extent_page(const ExtentPage& p) {
  validate(p);Out o;o.magic("VEP1");o.number(p.level,1);o.number(p.begin);o.number(p.end);o.number(p.extent_count);
  o.number(p.level?p.children.size():p.extents.size(),4);
  if(!p.level)for(const auto& e:p.extents){o.number(e.logical_offset);o.number(e.length);o.number(e.content_offset);o.number(e.content_size);o.id(e.content_id);}
  else for(const auto& c:p.children){o.number(c.begin);o.number(c.end);o.number(c.extent_count);o.number(c.encoded_size);o.id(c.page);}
  check(o.b.size()<=kPageBytes,"extent page encoded bytes exceed limit",ErrorCode::Limit);return o.b;
}
ExtentPage decode_extent_page(const Bytes& b) {
  check(b.size()<=kPageBytes,"extent page encoded bytes exceed limit",ErrorCode::Limit);In in{b};in.magic("VEP1");ExtentPage p;
  p.level=static_cast<std::uint8_t>(in.number(1));p.begin=in.number();p.end=in.number();p.extent_count=in.number();auto count=in.number(4);
  check(count>0&&count<=kPageEntries,"extent page entry count exceeds limit",ErrorCode::Limit);
  if(!p.level)for(std::uint64_t i=0;i<count;++i){PayloadExtent e;e.logical_offset=in.number();e.length=in.number();e.content_offset=in.number();e.content_size=in.number();e.content_id=in.id();p.extents.push_back(std::move(e));}
  else for(std::uint64_t i=0;i<count;++i){PayloadChild c;c.begin=in.number();c.end=in.number();c.extent_count=in.number();c.encoded_size=in.number();c.page=in.id();p.children.push_back(std::move(c));}
  in.end();validate(p);return p;
}

PayloadArtifacts::PayloadArtifacts(std::shared_ptr<Owner> owner):owner_(std::move(owner)) {
  check(bool(owner_),"payload artifacts require an owner",ErrorCode::Invalid);
}
ContentId PayloadArtifacts::build(Transaction& tx,std::uint64_t size,
    const std::function<Bytes(std::uint64_t,std::uint64_t)>& reader) {
  check(bool(reader)||!size,"payload build requires staging reader",ErrorCode::Invalid);
  std::vector<PayloadChild> children;ExtentPage leaf;
  auto flush=[&]{if(leaf.extents.empty())return;leaf.extent_count=leaf.extents.size();auto bytes=encode_extent_page(leaf);auto ref=tx.put_artifact("extent-page",bytes);children.push_back({leaf.begin,leaf.end,leaf.extent_count,bytes.size(),ref.id});leaf=ExtentPage{};};
  for(std::uint64_t offset=0;offset<size;) {
    const auto length=std::min<std::uint64_t>(kPayloadBlockBytes,size-offset);auto bytes=reader(offset,length);
    check(bytes.size()==length,"staging payload read returned short data");
    auto ref=tx.put_payload_block(bytes);query_profile_count("payload_map.build_blocks");query_profile_count("payload_map.build_logical_bytes",length);
    if(leaf.extents.empty())leaf.begin=offset;
    leaf.extents.push_back({offset,length,0,length,ref.id});offset+=length;leaf.end=offset;
    if(leaf.extents.size()==kPageEntries)flush();
  }
  flush();unsigned height=children.empty()?0:1;
  while(children.size()>1) {
    check(height<kTreeHeight,"payload map tree height exhausted",ErrorCode::Limit);
    std::vector<PayloadChild> parents;
    for(std::size_t begin=0;begin<children.size();begin+=kPageEntries) {
      ExtentPage p;p.level=static_cast<std::uint8_t>(height);p.begin=children[begin].begin;
      const auto end=std::min(children.size(),begin+kPageEntries);
      p.children.assign(children.begin()+begin,children.begin()+end);p.end=p.children.back().end;
      for(const auto& c:p.children)p.extent_count=add(p.extent_count,c.extent_count);
      auto bytes=encode_extent_page(p);auto ref=tx.put_artifact("extent-page",bytes);parents.push_back({p.begin,p.end,p.extent_count,bytes.size(),ref.id});
    }
    children=std::move(parents);++height;
  }
  PayloadMap m;m.payload_size=size;m.extent_count=blocks(size);m.height=static_cast<std::uint8_t>(height);
  if(!children.empty()){m.page=children.front().page;m.page_bytes=children.front().encoded_size;}
  return tx.put_artifact("payload-map",encode_payload_map(m)).id;
}
void PayloadArtifacts::verify(const ContentId& root,std::uint64_t size,bool deep,std::uint64_t* shared,const std::string& source) const {
  auto view=owner_->view();std::uint64_t local=0;auto& metadata=shared?*shared:local;
  auto m=load_map(owner_,view,root,size,metadata,deep,source);
  if(!deep)return;
  walk(owner_,view,root,m,[](auto,auto){return true;},metadata,true,source,
    [&](const std::vector<PayloadExtent>& extents){
      visit_blocks(owner_,view,extents,true,[](const auto&,const auto&){});
    });
}
StructuredBlobPayloadBatch PayloadArtifacts::read_ranges(const ContentId& root,std::uint64_t size,
    const StructuredBlobPayloadRanges& ranges,const std::string& source) const {
  check(!ranges.empty()&&ranges.size()<=kMaxPayloadRangesPerBatch,"invalid payload range count",ErrorCode::Invalid);
  std::uint64_t output=0,demand=0,extent_count=0;
  for(const auto& r:ranges) {
    check(r.length&&r.offset<=size&&r.length<=size-r.offset,"payload range outside file",ErrorCode::Invalid);
    check(r.length<=kMaxPayloadBytesPerBatch-output,"payload batch output exceeds limit",ErrorCode::Limit);output+=r.length;
    const auto first=r.offset/kPayloadBlockBytes,last=(r.offset+r.length-1)/kPayloadBlockBytes;
    const auto n=last-first+1;extent_count=add(extent_count,n);
    auto bytes=n*kPayloadBlockBytes;
    if(last==size/kPayloadBlockBytes&&size%kPayloadBlockBytes)bytes-=kPayloadBlockBytes-size%kPayloadBlockBytes;
    demand=add(demand,bytes);
  }
  check(extent_count<=kPayloadRequestExtents&&demand<=kPayloadRequestBytes,"payload request demand exceeds limit",ErrorCode::Limit);
  std::unique_ptr<ScopedQueryMetadataBudget> standalone;
  if(!current_query_metadata_budget()&&!active_query_metadata_trace)standalone=std::make_unique<ScopedQueryMetadataBudget>();
  storage_read_checkpoint();
  require_query_payload_demand(demand,extent_count);
  auto view=owner_->view();std::uint64_t metadata=0;auto m=load_map(owner_,view,root,size,metadata,false,source);
  auto wanted=[&](std::uint64_t begin,std::uint64_t end){return std::any_of(ranges.begin(),ranges.end(),[&](const auto& r){return begin<r.offset+r.length&&end>r.offset;});};
  auto extents=walk(owner_,view,root,m,wanted,metadata,false,source);
  auto output_memory=reserve_storage_read_temporary(output);
  StructuredBlobPayloadBatch result;result.reserve(ranges.size());
  for(const auto& r:ranges)result.emplace_back(static_cast<std::size_t>(r.length));
  struct Copy {std::size_t output;std::uint64_t destination,source,length;};
  std::map<ContentId,std::vector<Copy>> copies;
  for(std::size_t i=0;i<ranges.size();++i) {
    const auto& r=ranges[i];std::uint64_t cursor=r.offset;
    for(const auto& e:extents) {
      if(e.logical_offset>=r.offset+r.length||e.logical_offset+e.length<=cursor)continue;
      check(e.logical_offset<=cursor,"payload mapping does not cover requested bytes");
      const auto count=std::min(e.logical_offset+e.length,r.offset+r.length)-cursor;
      copies[e.content_id].push_back({i,cursor-r.offset,e.content_offset+cursor-e.logical_offset,count});
      cursor+=count;
    }
    check(cursor==r.offset+r.length,"payload mapping returned a short range");
  }
  visit_blocks(owner_,view,extents,false,[&](const ContentId& id,const Bytes& block){
    for(const auto& copy:copies.at(id)) {
      check(copy.source<=block.size()&&copy.length<=block.size()-copy.source,"payload extent exceeds content block");
      std::copy_n(block.begin()+copy.source,copy.length,result[copy.output].begin()+copy.destination);
    }
  });
  storage_read_checkpoint();
  query_profile_count("payload_map.logical_extents",extent_count);query_profile_count("payload_map.output_bytes",output);return result;
}
} // namespace iotdbfs::versioning
