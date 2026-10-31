#include "iotdbfs/storage/versioning/VersionDirectory.h"

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <algorithm>
#include <limits>
#include <set>
#include <utility>
#include <type_traits>
#include <atomic>
#include <deque>
#include <list>
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryStorageReadControl.h"
#include "iotdbfs/storage/versioning/detail/PayloadBlockFlights.h"

namespace iotdbfs::versioning {
namespace {
void demand(bool condition, ErrorCode code, const char* message) {
  if (!condition) throw Error(code, message);
}
std::uint8_t digit(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  throw Error(ErrorCode::Corrupt, "noncanonical hexadecimal value");
}
std::string hex(const Bytes& bytes) {
  static const char* digits = "0123456789abcdef";
  std::string result;
  result.reserve(bytes.size() * 2);
  for (auto c : bytes) { result += digits[c >> 4]; result += digits[c & 15]; }
  return result;
}
Bytes unhex(const std::string& text) {
  demand(text.size() % 2 == 0, ErrorCode::Corrupt, "odd hexadecimal length");
  Bytes bytes;
  bytes.reserve(text.size() / 2);
  for (std::size_t i = 0; i < text.size(); i += 2)
    bytes.push_back((digit(text[i]) << 4) | digit(text[i + 1]));
  return bytes;
}
struct Encoder {
  Bytes bytes;
  std::size_t limit;
  explicit Encoder(std::size_t cap) : limit(cap) {}
  void raw(const Bytes& value) {
    demand(value.size() <= limit - bytes.size(), ErrorCode::Limit, "encoded object exceeds byte limit");
    bytes.insert(bytes.end(), value.begin(), value.end());
  }
  void raw(const std::string& value) { raw(Bytes(value.begin(), value.end())); }
  void number(std::uint64_t value, unsigned width) {
    demand(width <= limit - bytes.size(), ErrorCode::Limit, "encoded object exceeds byte limit");
    for (unsigned i = width; i; --i) bytes.push_back(static_cast<std::uint8_t>(value >> ((i-1)*8)));
  }
  void text(const std::string& value, std::size_t cap = 4096) {
    demand(value.size() <= cap && value.size() <= UINT32_MAX,
      ErrorCode::Limit, "encoded string exceeds byte limit");
    number(value.size(), 4); raw(value);
  }
  void id(const ContentId& value) {
    demand(valid_content_id(value), ErrorCode::Invalid, "invalid content ID"); raw(unhex(value));
  }
  void optional_id(const ContentId& value) {
    number(!value.empty(), 1); if (!value.empty()) id(value);
  }
  void uuid(const std::string& value) {
    demand(valid_uuid(value), ErrorCode::Invalid, "invalid namespace UUID");
    std::string plain;
    for (char c : value) if (c != '-') plain += c;
    raw(unhex(plain));
  }
  void identity(const NamespaceIdentity& value) { uuid(value.namespace_id); uuid(value.incarnation); }
};
struct Decoder {
  const Bytes& bytes;
  std::size_t offset{0};
  Decoder(const Bytes& value, std::size_t cap) : bytes(value) {
    demand(bytes.size() <= cap, ErrorCode::Limit, "encoded object exceeds byte limit");
  }
  Bytes raw(std::size_t count) {
    demand(count <= bytes.size() - offset, ErrorCode::Corrupt, "truncated object");
    Bytes result(bytes.begin() + offset, bytes.begin() + offset + count);
    offset += count; return result;
  }
  std::uint64_t number(unsigned width) {
    demand(width <= bytes.size() - offset, ErrorCode::Corrupt, "truncated number");
    std::uint64_t result = 0;
    while (width--) result = (result << 8) | bytes[offset++];
    return result;
  }
  void magic(const std::string& value) {
    demand(raw(value.size()) == Bytes(value.begin(), value.end()), ErrorCode::Corrupt, "unsupported object protocol");
  }
  std::string text(std::size_t cap = 4096) {
    auto count = number(4);
    demand(count <= cap, ErrorCode::Limit, "string exceeds byte limit");
    auto value = raw(static_cast<std::size_t>(count));
    return {value.begin(), value.end()};
  }
  ContentId id() { return hex(raw(32)); }
  ContentId optional_id() {
    auto present = number(1);
    demand(present <= 1, ErrorCode::Corrupt, "noncanonical optional ID");
    return present ? id() : ContentId{};
  }
  std::string uuid() {
    auto value = hex(raw(16));
    return value.substr(0,8) + '-' + value.substr(8,4) + '-' + value.substr(12,4) + '-' +
      value.substr(16,4) + '-' + value.substr(20);
  }
  NamespaceIdentity identity() { return {uuid(), uuid()}; }
  void end() { demand(offset == bytes.size(), ErrorCode::Corrupt, "trailing object bytes"); }
};
void charge(std::uint64_t& total, std::uint64_t size) {
  demand(total<=kMetadataBytes&&size <= kMetadataBytes - total, ErrorCode::Limit, "namespace metadata byte budget exhausted");
  total += size;
}
void check_page(const Page& page) {
  demand(page.level < kTreeHeight && !page.entries.empty() && page.entries.size() <= kPageEntries,
    ErrorCode::Limit, "catalog page height or entry limit exceeded");
  std::string previous;
  bool first = true;
  for (const auto& item : page.entries) {
    demand(item.key.size() <= 8192 && (first || previous < item.key) && valid_content_id(item.value),
      ErrorCode::Corrupt, "catalog page has invalid or unordered entries");
    previous = item.key; first = false;
  }
}
void canonical_dependencies(const std::vector<std::string>& values) {
  demand(values.size() <= kScanEntries && std::is_sorted(values.begin(), values.end()) &&
    std::adjacent_find(values.begin(), values.end()) == values.end(),
    ErrorCode::Invalid, "commit dependencies must be bounded, sorted and unique");
}
} // namespace

bool valid_content_id(const ContentId& id) {
  return id.size() == 64 && std::all_of(id.begin(), id.end(), [](char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  });
}
bool valid_uuid(const std::string& id) {
  if (id.size() != 36) return false;
  for (std::size_t i=0; i<id.size(); ++i) {
    if (i==8 || i==13 || i==18 || i==23) { if (id[i]!='-') return false; }
    else if (!((id[i]>='0' && id[i]<='9') || (id[i]>='a' && id[i]<='f'))) return false;
  }
  return true;
}
std::string random_uuid() {
  Bytes bytes(16);
  demand(RAND_bytes(bytes.data(), bytes.size()) == 1, ErrorCode::Unavailable, "UUID entropy unavailable");
  bytes[6] = (bytes[6] & 15) | 64; bytes[8] = (bytes[8] & 63) | 128;
  Decoder decoder(bytes, 16); return decoder.uuid();
}
ContentId content_id(const std::string& type, const std::string& protocol, const Bytes& bytes) {
  Encoder framing(kMetadataBytes + 1024);
  framing.raw(std::string("iotdbfs-content-id/1\0", sizeof("iotdbfs-content-id/1\0")-1));
  framing.text(type, 128); framing.text(protocol, 128); framing.number(bytes.size(), 8); framing.raw(bytes);
  Bytes result(32); unsigned size = 0;
  demand(EVP_Digest(framing.bytes.data(), framing.bytes.size(), result.data(), &size,
    EVP_sha256(), nullptr) == 1 && size == 32, ErrorCode::Unavailable, "SHA-256 failed");
  return hex(result);
}
ObjectRef object_ref(const std::string& type, const Bytes& bytes) {
  const auto protocol = "iotdbfs-" + type + (type=="file-descriptor"?"/2":"/1");
  return {type, protocol, content_id(type, protocol, bytes)};
}
Bytes encode_head(const Head& head) {
  Encoder out(kHeadBytes); out.raw("VDH1"); out.identity(head.identity); out.number(head.sequence,8);
  out.id(head.commit); out.id(head.root);
  out.raw(unhex(content_id("head", "iotdbfs-head/1", out.bytes)));
  return out.bytes;
}
Head decode_head(const Bytes& bytes) {
  Decoder in(bytes,kHeadBytes); in.magic("VDH1");
  Head head; head.identity=in.identity(); head.sequence=in.number(8); head.commit=in.id(); head.root=in.id();
  const Bytes body(bytes.begin(), bytes.begin()+in.offset);
  demand(in.id() == content_id("head", "iotdbfs-head/1", body), ErrorCode::Corrupt, "head digest mismatch");
  in.end(); return head;
}
Bytes encode_roots(const Roots& roots) {
  Encoder out(kPageBytes); out.raw("VDR1"); for (const auto& root : roots) out.optional_id(root); return out.bytes;
}
Roots decode_roots(const Bytes& bytes) {
  Decoder in(bytes,kPageBytes); in.magic("VDR1"); Roots roots;
  for (auto& root : roots) root=in.optional_id(); in.end(); return roots;
}
Bytes encode_page(const Page& page) {
  check_page(page); Encoder out(kPageBytes); out.raw("VDP1"); out.number(page.level,1); out.number(page.entries.size(),4);
  for (const auto& item : page.entries) { out.text(item.key,8192); out.id(item.value); }
  return out.bytes;
}
Page decode_page(const Bytes& bytes) {
  Decoder in(bytes,kPageBytes); in.magic("VDP1"); Page page; page.level=in.number(1); auto count=in.number(4);
  demand(count > 0 && count <= kPageEntries,ErrorCode::Limit,"catalog page entry limit exceeded");
  for (std::size_t i=0;i<count;++i) page.entries.push_back({in.text(8192),in.id()});
  in.end(); check_page(page); return page;
}
Bytes encode_commit(const Commit& commit) {
  canonical_dependencies(commit.dependencies); Encoder out(kMetadataBytes); out.raw("VDC1"); out.identity(commit.identity);
  out.number(commit.sequence,8); out.optional_id(commit.previous_commit); out.id(commit.root);
  out.uuid(commit.nonce); out.text(commit.operation,128); out.number(commit.dependencies.size(),4);
  for (const auto& item : commit.dependencies) out.text(item,8193);
  return out.bytes;
}
Commit decode_commit(const Bytes& bytes) {
  Decoder in(bytes,kMetadataBytes); in.magic("VDC1"); Commit commit; commit.identity=in.identity();
  commit.sequence=in.number(8); commit.previous_commit=in.optional_id(); commit.root=in.id();
  commit.nonce=in.uuid(); commit.operation=in.text(128); auto count=in.number(4);
  demand(count <= kScanEntries,ErrorCode::Limit,"commit dependency limit exceeded");
  for (std::size_t i=0;i<count;++i) commit.dependencies.push_back(in.text(8193));
  in.end(); canonical_dependencies(commit.dependencies); return commit;
}

std::string exact_key(const std::string& path, std::int64_t version) {
  demand(!path.empty() && path.front()=='/' && path.size()<=4096 && path.find('\0')==std::string::npos && version>=0,
    ErrorCode::Invalid,"invalid exact version key");
  demand((path=="/" || path.back()!='/') && path.find("//")==std::string::npos,
    ErrorCode::Invalid,"exact path must be normalized");
  for (std::size_t begin=1;begin<path.size();) {
    const auto end=path.find('/',begin);
    const auto part=path.substr(begin,end==std::string::npos?std::string::npos:end-begin);
    demand(part!="." && part!="..",ErrorCode::Invalid,"exact path must be normalized");
    if (end==std::string::npos) break;
    begin=end+1;
  }
  std::string result=path; result+='\0';
  for (unsigned i=8;i;--i) result+=static_cast<char>(static_cast<std::uint64_t>(version)>>((i-1)*8));
  return result;
}
std::pair<std::string,std::int64_t> decode_exact_key(const std::string& key) {
  demand(key.size()>=10 && key[key.size()-9]=='\0',ErrorCode::Corrupt,"invalid exact key encoding");
  const auto path=key.substr(0,key.size()-9); std::uint64_t version=0;
  for (std::size_t i=key.size()-8;i<key.size();++i) version=(version<<8)|static_cast<unsigned char>(key[i]);
  demand(version<=INT64_MAX,ErrorCode::Corrupt,"version exceeds INT64");
  demand(exact_key(path,static_cast<std::int64_t>(version))==key,ErrorCode::Corrupt,"noncanonical exact key");
  return {path,static_cast<std::int64_t>(version)};
}

namespace {
// COW B+tree. Each edit replaces only its search path. Content addressing
// reuses every unchanged page; reads never follow a previous version/root.
class Tree {
public:
  using Read=std::function<Bytes(const ObjectRef&)>;
  using Write=std::function<ContentId(const Bytes&)>;
  using ReadPage=std::function<std::shared_ptr<const Page>(const ContentId&)>;
  Tree(Read read, Write write={}, ReadPage pages={}) : read_(std::move(read)),write_(std::move(write)),pages_(std::move(pages)) {}
  std::optional<ContentId> get(const ContentId& root,const std::string& key) {
    ContentId id=root;
    std::optional<unsigned> expected;
    std::set<ContentId> visited;
    while (!id.empty()) {
      demand(visited.insert(id).second,ErrorCode::Corrupt,"catalog page cycle");
      const auto borrowed=borrow(id,expected); const auto& page=*borrowed;
      auto it=std::lower_bound(page.entries.begin(),page.entries.end(),key,[](const auto& entry,const auto& k){return entry.key<k;});
      if (it==page.entries.end()) return std::nullopt;
      if (!page.level) return it->key==key ? std::optional<ContentId>(it->value) : std::nullopt;
      id=it->value; expected=page.level-1;
    }
    return std::nullopt;
  }
  ContentId set(const ContentId& root,const std::string& key,const std::optional<ContentId>& value) {
    if (root.empty()) {
      if (!value) return {};
      return save(Page{0,{{key,*value}}});
    }
    auto old=load(root,{});
    auto pieces=edit(root,key,value,old.level);
    if (pieces.empty()) return {};
    if (pieces.size()==1) {
      auto id=pieces.front().value;
      auto page=load(id,{});
      while (page.level && page.entries.size()==1) { id=page.entries.front().value; page=load(id,page.level-1); }
      return id;
    }
    demand(old.level+1<kTreeHeight,ErrorCode::Limit,"catalog tree height exhausted");
    return save(Page{static_cast<std::uint8_t>(old.level+1),std::move(pieces)});
  }
  std::vector<PageEntry> scan(const ContentId& root,const std::string& prefix,std::size_t limit) {
    demand(limit<=kScanEntries,ErrorCode::Limit,"catalog scan entry limit exceeded");
    std::vector<PageEntry> result; std::set<ContentId> active;
    std::function<void(const ContentId&,std::optional<unsigned>,std::optional<std::string>,std::optional<std::string>)> visit;
    visit=[&](const auto& id,auto level,auto lower,auto upper) {
      demand(active.insert(id).second,ErrorCode::Corrupt,"catalog page cycle");
      const auto borrowed=borrow(id,level); const auto& page=*borrowed;
      demand(!upper || page.entries.back().key==*upper,ErrorCode::Corrupt,"catalog child maximum mismatch");
      demand(!lower || page.entries.front().key>*lower,ErrorCode::Corrupt,"catalog child boundary overlap");
      if (!page.level) {
        for (const auto& item:page.entries) if (item.key.compare(0,prefix.size(),prefix)==0) {
          demand(result.size()<limit,ErrorCode::Limit,"catalog scan entry limit exhausted"); result.push_back(item);
        }
      } else {
        auto previous=lower;
        for (const auto& item:page.entries) {
          // A child can be skipped only when its entire ordered key interval
          // is below the requested prefix. No unsafe upper-prefix arithmetic.
          if (prefix.empty() || item.key>=prefix) visit(item.value,page.level-1,previous,item.key);
          previous=item.key;
        }
      }
      active.erase(id);
    };
    if (!root.empty()) visit(root,{}, {}, {});
    return result;
  }
private:
  std::shared_ptr<const Page> borrow(const ContentId& id,std::optional<unsigned> expected) {
    auto page=pages_ ? pages_(id) : std::make_shared<const Page>(decode_page(read_({"catalog-page","iotdbfs-catalog-page/1",id})));
    demand(!expected || page->level==*expected,ErrorCode::Corrupt,"catalog page level mismatch");
    return page;
  }
  Page load(const ContentId& id,std::optional<unsigned> expected) { return *borrow(id,expected); }
  ContentId save(const Page& page) {
    demand(bool(write_),ErrorCode::Invalid,"read-only catalog tree"); return write_(encode_page(page));
  }
  std::vector<PageEntry> pack(Page page) {
    if (page.entries.empty()) return {};
    bool fits=page.entries.size()<=kPageEntries;
    if (fits) try { return {{page.entries.back().key,save(page)}}; }
    catch (const Error& error) { if (error.code()!=ErrorCode::Limit) throw; }
    demand(page.entries.size()>1,ErrorCode::Limit,"catalog entry cannot fit a page");
    const auto middle=page.entries.size()/2;
    Page right{page.level,{page.entries.begin()+middle,page.entries.end()}};
    page.entries.erase(page.entries.begin()+middle,page.entries.end());
    auto first=pack(std::move(page)); auto second=pack(std::move(right));
    first.insert(first.end(),second.begin(),second.end()); return first;
  }
  std::vector<PageEntry> edit(const ContentId& id,const std::string& key,
    const std::optional<ContentId>& value,unsigned level) {
    auto page=load(id,level);
    auto it=std::lower_bound(page.entries.begin(),page.entries.end(),key,[](const auto& entry,const auto& k){return entry.key<k;});
    if (!page.level) {
      if (it!=page.entries.end() && it->key==key) {
        if (!value) page.entries.erase(it);
        else if (it->value==*value) return {{page.entries.back().key,id}};
        else it->value=*value;
      } else if (value) page.entries.insert(it,{key,*value});
      else return {{page.entries.back().key,id}};
    } else {
      if (it==page.entries.end()) it=page.entries.end()-1;
      const auto index=it-page.entries.begin();
      auto child=edit(it->value,key,value,level-1);
      page.entries.erase(page.entries.begin()+index);
      page.entries.insert(page.entries.begin()+index,child.begin(),child.end());
    }
    return pack(std::move(page));
  }
  Read read_;
  Write write_;
  ReadPage pages_;
};
} // namespace

struct Owner::Runtime {
  explicit Runtime(OwnerConfig c) : config(c), payload_flights(c.coalesce_payload_reads ? 64 : 0,
    c.coalesce_payload_reads ? 8ULL * 1024 * 1024 : 0,
    c.coalesce_payload_reads ? c.payload_cache_bytes : 0) {}
  OwnerConfig config;
  detail::PayloadBlockFlights payload_flights;
  mutable std::mutex mutex;
  OwnerStats counters;
  struct Entry {
    std::shared_ptr<const Bytes> bytes;
    std::shared_ptr<const Page> page;
    std::size_t cost;
    std::list<std::string>::iterator position;
  };
  std::map<std::string,Entry> cache;
  std::list<std::string> lru;
  struct Change { std::uint64_t sequence; std::vector<std::string> keys; std::size_t bytes; };
  std::deque<Change> changes; // protected by Owner::mutex_, never the cache mutex
  std::size_t change_keys{0},change_bytes{0};
  std::shared_ptr<std::atomic<std::uint64_t>> leases=std::make_shared<std::atomic<std::uint64_t>>(0);
  static std::string key(const NamespaceIdentity& ns,const ObjectRef& ref) {
    return ns.namespace_id+ns.incarnation+ref.type+"/"+ref.protocol+"/"+ref.id;
  }
  static bool eligible(const ObjectRef& ref) {
    return ref.type=="catalog-page"||ref.type=="catalog-record"||ref.type=="catalog-root"||ref.type=="commit"||
      ref.type=="extent-page"||ref.type=="payload-map";
  }
  static bool payload_mapping(const ObjectRef& ref) {
    return ref.type=="extent-page"||ref.type=="payload-map";
  }
  void insert(const std::string& key,std::shared_ptr<const Bytes> bytes,std::shared_ptr<const Page> page={}) {
    std::size_t cost=sizeof(Entry)+512+2*key.capacity()+sizeof(Bytes)+bytes->capacity();
    if(page) { cost+=sizeof(Page)+page->entries.capacity()*sizeof(PageEntry);for(const auto& e:page->entries)cost+=e.key.capacity()+e.value.capacity()+64; }
    if(!config.cache_bytes||cost>config.cache_bytes)return;
    std::lock_guard<std::mutex> lock(mutex);
    auto old=cache.find(key);
    if(old!=cache.end()){counters.cache_bytes-=old->second.cost;lru.erase(old->second.position);cache.erase(old);}
    while(!lru.empty()&&cost>config.cache_bytes-counters.cache_bytes) {
      auto found=cache.find(lru.back());counters.cache_bytes-=found->second.cost;cache.erase(found);lru.pop_back();++counters.evictions;
    }
    lru.push_front(key);
    try {cache.emplace(key,Entry{std::move(bytes),std::move(page),cost,lru.begin()});}
    catch(...){lru.pop_front();throw;}
    counters.cache_bytes+=cost;counters.cache_entries=cache.size();
  }
  void changed(std::uint64_t sequence,const std::vector<std::string>& keys) {
    std::size_t bytes=sizeof(Change);
    for(const auto& key:keys)bytes+=sizeof(std::string)+key.size()+64;
    if(!config.change_commits||keys.size()>config.change_keys||bytes>config.change_bytes) {
      changes.clear();change_keys=change_bytes=0;return;
    }
    while(!changes.empty()&&(changes.size()>=config.change_commits||keys.size()>config.change_keys-change_keys||bytes>config.change_bytes-change_bytes)) {
      change_keys-=changes.front().keys.size();change_bytes-=changes.front().bytes;changes.pop_front();
    }
    changes.push_back({sequence,keys,bytes});change_keys+=keys.size();change_bytes+=bytes;
  }
};
Owner::Owner(std::shared_ptr<Store> store,std::shared_ptr<Journal> journal,OwnerConfig config)
  :runtime_(std::make_unique<Runtime>(config)),store_(std::move(store)),journal_(std::move(journal)) {
  demand(store_ && journal_,ErrorCode::Invalid,"owner requires a store and durable journal");
  demand(config.cache_bytes<=kMetadataBytes&&config.change_commits<=256&&config.change_keys<=kScanEntries&&config.change_bytes<=8ULL*1024*1024&&config.payload_cache_bytes<=8ULL*1024*1024,ErrorCode::Limit,"owner runtime limits exceed protocol bounds");
}
Owner::~Owner(){
  // Resident pins may refund/reenter runtime accounting. Retire them while
  // every Owner/Runtime member (including mutexes and lease stats) is alive.
  clear_read_cache();
}
Bytes Owner::checked_object(const NamespaceIdentity& identity,const ObjectRef& ref,std::uint64_t& used,bool force) const {
  demand(valid_content_id(ref.id),ErrorCode::Corrupt,"invalid object reference");
  const auto key=Runtime::key(identity,ref);std::shared_ptr<const Bytes> cached;
  if(!force&&Runtime::eligible(ref)) {
    std::lock_guard<std::mutex> lock(runtime_->mutex);auto found=runtime_->cache.find(key);
    if(found!=runtime_->cache.end()){cached=found->second.bytes;runtime_->lru.splice(runtime_->lru.begin(),runtime_->lru,found->second.position);++runtime_->counters.object_cache_hits;}
  }
  if(cached){charge(used,cached->size());if(Runtime::eligible(ref)&&!Runtime::payload_mapping(ref))require_query_metadata("directory/"+key,cached->size());query_profile_count("version_directory.object_cache_hits");return *cached;}
  auto bytes=store_->get_object(identity,ref);charge(used,bytes.size());
  demand(content_id(ref.type,ref.protocol,bytes)==ref.id,ErrorCode::Corrupt,"immutable object digest mismatch");
  if(Runtime::eligible(ref)&&!Runtime::payload_mapping(ref))require_query_metadata("directory/"+key,bytes.size());
  {std::lock_guard<std::mutex> lock(runtime_->mutex);++runtime_->counters.object_reads;runtime_->counters.hash_bytes+=bytes.size();}
  query_profile_count("version_directory.object_reads");query_profile_count("version_directory.hash_bytes",bytes.size());
  if(Runtime::eligible(ref))runtime_->insert(key,std::make_shared<const Bytes>(bytes));
  return bytes;
}
std::shared_ptr<const Page> Owner::checked_page(const NamespaceIdentity& ns,const ContentId& id,std::uint64_t& used,bool force) const {
  const ObjectRef ref{"catalog-page","iotdbfs-catalog-page/1",id};const auto key=Runtime::key(ns,ref);
  if(!force){std::lock_guard<std::mutex> lock(runtime_->mutex);auto it=runtime_->cache.find(key);
    if(it!=runtime_->cache.end()&&it->second.page){charge(used,it->second.bytes->size());require_query_metadata("directory/"+key,it->second.bytes->size());runtime_->lru.splice(runtime_->lru.begin(),runtime_->lru,it->second.position);++runtime_->counters.page_cache_hits;query_profile_count("version_directory.page_cache_hits");return it->second.page;}}
  auto bytes=std::make_shared<const Bytes>(checked_object(ns,ref,used,force));
  auto page=std::make_shared<const Page>(decode_page(*bytes));runtime_->insert(key,bytes,page);return page;
}
void Owner::clear_read_cache(){
  {std::lock_guard<std::mutex> lock(runtime_->mutex);runtime_->cache.clear();runtime_->lru.clear();runtime_->counters.cache_bytes=runtime_->counters.cache_entries=0;}
  runtime_->payload_flights.clear();
}
OwnerStats Owner::stats() const {
  OwnerStats s;
  {std::lock_guard<std::mutex> lock(runtime_->mutex);s=runtime_->counters;}
  s.active_execution_leases=runtime_->leases->load();
  const auto flights=runtime_->payload_flights.stats();
  s.payload_coalesce_leaders=flights.leaders;s.payload_coalesce_joins=flights.joins;
  s.payload_coalesce_bypasses=flights.bypasses;s.payload_coalesce_fallbacks=flights.fallbacks;
  s.payload_coalesce_active_flights=flights.active_flights;
  s.payload_coalesce_retained_entries=flights.retained_entries;
  s.payload_coalesce_retained_bytes=flights.retained_bytes;
  s.payload_cache_hits=flights.cache_hits;s.payload_cache_misses=flights.cache_misses;
  s.payload_cache_insertions=flights.cache_insertions;s.payload_cache_evictions=flights.cache_evictions;
  s.payload_cache_pressure_reclaims=flights.cache_pressure_reclaims;
  s.payload_cache_resident_entries=flights.cache_resident_entries;
  s.payload_cache_resident_bytes=flights.cache_resident_bytes;
  return s;
}
std::unique_ptr<Owner::Admission> Owner::admit(const Head& expected) const {
  auto gate=std::unique_ptr<Admission>(new Admission(mutex_));require_ready();
  demand(current_->head==expected,ErrorCode::Conflict,"namespace changed before final admission");
  query_profile_count("read_context.admissions");return gate;
}
DependencyCheck Owner::check_dependencies(const View& from,const std::vector<std::string>& dependencies) const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();canonical_dependencies(dependencies);
  require_query_metadata("head/"+current_->head.identity.namespace_id+current_->head.identity.incarnation+current_->head.commit,encode_head(current_->head).size());
  {std::lock_guard<std::mutex> count(runtime_->mutex);++runtime_->counters.dependency_checks;}
  query_profile_count("read_context.dependency_checks");
  auto unknown=[&] {std::lock_guard<std::mutex> count(runtime_->mutex);++runtime_->counters.history_gaps;query_profile_count("read_context.history_gaps");return DependencyCheck{DependencyState::Unknown,*current_,0};};
  if(!(from.head.identity==current_->head.identity)||from.head.sequence>current_->head.sequence)return unknown();
  if(from.head==current_->head)return {DependencyState::Unchanged,*current_,0};
  if(from.head.sequence==current_->head.sequence||runtime_->changes.empty()||runtime_->changes.front().sequence>from.head.sequence+1)return unknown();
  std::set<std::string> changed;
  for(const auto& c:runtime_->changes)if(c.sequence>from.head.sequence) {
    for(const auto& key:c.keys)if(std::binary_search(dependencies.begin(),dependencies.end(),key))changed.insert(key);
  }
  {std::lock_guard<std::mutex> count(runtime_->mutex);runtime_->counters.changed_dependencies+=changed.size();}
  query_profile_count("read_context.changed_dependencies",changed.size());
  return {changed.empty()?DependencyState::Unchanged:DependencyState::Changed,*current_,changed.size()};
}
std::shared_ptr<void> Owner::acquire_execution_lease() const {
  std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();
  struct Pin {std::shared_ptr<std::atomic<std::uint64_t>> n;explicit Pin(std::shared_ptr<std::atomic<std::uint64_t>> count):n(std::move(count)){++*n;}~Pin(){--*n;}};
  return std::make_shared<Pin>(runtime_->leases);
}
ContentId Owner::put_object(const std::string& type,const Bytes& bytes,std::uint64_t& used) {
  charge(used,bytes.size()); auto ref=object_ref(type,bytes);
  NamespaceIdentity identity;
  {std::lock_guard<std::recursive_mutex> lock(mutex_);
    demand(state_.has_value(),ErrorCode::Frozen,"owner has no persistence identity");identity=state_->identity;}
  store_->put_object(identity,ref,bytes);
  // This read is the pre-head closure/persistence check, not a proof of WAL
  // durability by itself. The Store's capability gate supplies SYNC semantics.
  auto actual=store_->get_object(identity,ref);
  demand(actual==bytes,ErrorCode::Corrupt,"immutable object write did not read back exactly");
  return ref.id;
}
void Owner::require_ready() const {
  demand(!frozen_ && current_,ErrorCode::Frozen,"namespace owner is not admitted; recovery required");
}
void Owner::initialize(const NamespaceIdentity& identity, const std::vector<Seed>& seeds) {
  std::lock_guard<std::recursive_mutex> writer(writer_mutex_);
  {std::lock_guard<std::recursive_mutex> lock(mutex_);
    demand(!transaction_active_,ErrorCode::Conflict,"cannot initialize during a transaction");}
  demand(valid_uuid(identity.namespace_id) && valid_uuid(identity.incarnation),ErrorCode::Invalid,"invalid namespace identity");
  demand(!journal_->load() && !store_->read_head(),ErrorCode::Conflict,"initialization requires an empty head and absent owner journal");
  {std::lock_guard<std::recursive_mutex> lock(mutex_);
    frozen_=true;state_=JournalState{identity,{}, {}};
    current_=View{Head{identity,0,{},{}},Roots{}};}
  demand(seeds.size()<=kScanEntries,ErrorCode::Limit,"too many namespace seed records");
  Roots roots{};
  std::uint64_t used=0;
  Tree tree([&](const ObjectRef& ref){return checked_object(identity,ref,used);},
    [&](const Bytes& page){return put_object("catalog-page",page,used);});
  std::set<std::pair<std::size_t,std::string>> seen;
  for (const auto& seed:seeds) {
    const auto index=static_cast<std::size_t>(std::get<0>(seed));
    demand(index<roots.size() && seen.emplace(index,std::get<1>(seed)).second,
      ErrorCode::Invalid,"invalid or duplicate namespace seed");
    const auto id=put_object("catalog-record",std::get<2>(seed),used);
    roots[index]=tree.set(roots[index],std::get<1>(seed),id);
  }
  publish(roots,"initialize");
}
void Owner::recover() {
  std::lock_guard<std::recursive_mutex> writer(writer_mutex_);
  {std::lock_guard<std::recursive_mutex> lock(mutex_);
    demand(!transaction_active_,ErrorCode::Conflict,"cannot recover during a transaction");
    frozen_=true;current_.reset();
    runtime_->changes.clear();runtime_->change_keys=runtime_->change_bytes=0;}
  clear_read_cache();
  auto recovered=journal_->load();
  demand(recovered.has_value(),ErrorCode::Frozen,"external owner journal missing; automatic adoption is forbidden");
  auto encoded=store_->read_head();
  demand(encoded.has_value(),ErrorCode::Frozen,"persistent head missing; automatic recreation is forbidden");
  const auto head=decode_head(*encoded);
  demand(head.identity==recovered->identity,ErrorCode::Frozen,"namespace incarnation differs from owner journal");
  if (recovered->pending) {
    demand(*encoded==*recovered->pending,ErrorCode::Frozen,"pending head outcome remains unknown; old RPC may still execute");
    store_->accept_reconciled_head();
    recovered->committed=encoded; recovered->pending.reset(); journal_->save(*recovered);
  } else {
    demand(recovered->committed && *recovered->committed==*encoded,ErrorCode::Frozen,
      "head differs from external committed high-water mark; rollback or foreign writer");
  }
  std::uint64_t used=0;
  const auto commit=decode_commit(checked_object(head.identity,{"commit","iotdbfs-commit/1",head.commit},used,true));
  demand(commit.identity==head.identity && commit.sequence==head.sequence && commit.root==head.root,
    ErrorCode::Corrupt,"head/commit identity mismatch");
  const auto roots=decode_roots(checked_object(head.identity,{"catalog-root","iotdbfs-catalog-root/1",head.root},used,true));
  // The owner remains Frozen while remote closure verification runs without
  // the admission mutex. No reader can mistake a partial recovery for health.
  Tree tree([&](const ObjectRef& ref){return checked_object(head.identity,ref,used,true);});
  for (const auto& root:roots) for (const auto& item:tree.scan(root,{},kScanEntries))
    checked_object(head.identity,{"catalog-record","iotdbfs-catalog-record/1",item.value},used,true);
  {std::lock_guard<std::recursive_mutex> lock(mutex_);
    state_=std::move(recovered);current_=View{head,roots};frozen_=false;}
}
void Owner::publish(const Roots& roots,const std::string& operation,
  const std::vector<std::string>& dependencies) {
  // The writer lock serializes the whole transaction. Prepare immutable root
  // and commit objects while readers may still admit the committed old head.
  View base;
  NamespaceIdentity identity;
  bool genesis;
  {std::lock_guard<std::recursive_mutex> lock(mutex_);
    demand(state_ && current_,ErrorCode::Frozen,"owner has no initialization state");
    base=*current_;identity=state_->identity;genesis=!state_->committed;}
  std::uint64_t used=0;
  const auto root=put_object("catalog-root",encode_roots(roots),used);
  demand(genesis || base.head.sequence<UINT64_MAX,ErrorCode::Limit,"commit sequence overflow");
  Commit commit{identity,genesis?0:base.head.sequence+1,
    genesis?ContentId{}:base.head.commit,root,random_uuid(),operation,dependencies};
  const auto commit_id=put_object("commit",encode_commit(commit),used);
  Head target{identity,commit.sequence,commit_id,root}; auto bytes=encode_head(target);
  // Only this bounded durable-head confirmation window excludes admission.
  // A normal commit never exposes its transient Frozen flag; an uncertain
  // outcome releases the gate with Frozen set for explicit reconciliation.
  std::lock_guard<std::recursive_mutex> gate(mutex_);
  demand(current_ && current_->head==base.head,ErrorCode::Conflict,"writer base changed before head publication");
  state_->pending=bytes; frozen_=true; journal_->save(*state_);
  bool acknowledged=false;
  try { acknowledged=store_->write_head(bytes)==HeadWriteResult::Acknowledged; }
  catch (...) { acknowledged=false; }
  if (!acknowledged) {
    auto observed=store_->read_head();
    demand(observed && *observed==bytes,ErrorCode::Frozen,"head write outcome unknown; namespace admission stopped");
    store_->accept_reconciled_head();
  }
  state_->committed=bytes; state_->pending.reset(); journal_->save(*state_);
  current_=View{std::move(target),roots};
  try {runtime_->changed(current_->head.sequence,dependencies);}
  catch(const std::bad_alloc&) {runtime_->changes.clear();runtime_->change_keys=runtime_->change_bytes=0;}
  frozen_=false;
}
View Owner::view() const {
  std::lock_guard<std::recursive_mutex> lock(mutex_); require_ready();
  require_query_metadata("head/"+current_->head.identity.namespace_id+current_->head.identity.incarnation+current_->head.commit,encode_head(current_->head).size());
  {std::lock_guard<std::mutex> count(runtime_->mutex);++runtime_->counters.head_control_calls;}
  query_profile_count("version_directory.head_control_calls");return *current_;
}
bool Owner::frozen() const { std::lock_guard<std::recursive_mutex> lock(mutex_); return frozen_; }
Bytes Owner::read_artifact(const View& view,const ObjectRef& ref,std::uint64_t& used) const {
  demand(ref.type!="payload-block",ErrorCode::Invalid,"payload blocks require the dedicated batch read channel");
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();demand(view.head.identity==current_->head.identity,ErrorCode::Conflict,"artifact view belongs to another namespace");}
  auto result=checked_object(view.head.identity,ref,used);
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();}
  return result;
}
std::vector<Bytes> Owner::read_artifacts(const View& view,
    const std::vector<ObjectReadRequest>& requests,std::uint64_t& used,bool payload,bool force,
    std::shared_ptr<void> temporary_pin) const {
  demand(!requests.empty()&&requests.size()<=kObjectBatchEntries,ErrorCode::Limit,"artifact batch count exceeds bounds");
  std::uint64_t total=0;
  std::set<std::tuple<std::string,std::string,ContentId>> unique;
  for(const auto& request:requests) {
    const auto& ref=request.ref;
    demand(!ref.type.empty()&&ref.type.size()<=128&&!ref.protocol.empty()&&ref.protocol.size()<=128&&
      ref.type.find('\0')==std::string::npos&&ref.protocol.find('\0')==std::string::npos&&
      valid_content_id(ref.id),ErrorCode::Invalid,"invalid batch artifact reference");
    demand(request.size>0&&request.size<=kObjectBatchBytes-total,ErrorCode::Limit,"artifact batch byte limit exceeded");
    total+=request.size;
    demand(unique.emplace(ref.type,ref.protocol,ref.id).second,ErrorCode::Invalid,"duplicate artifact request");
    if(payload) demand(ref.type=="payload-block"&&ref.protocol=="iotdbfs-payload-block/1"&&
      request.size<=kPayloadBlockBytes,ErrorCode::Invalid,"invalid payload block request");
    else demand(ref.type!="payload-block"&&request.size<=kPageBytes,ErrorCode::Limit,"metadata artifact exceeds page bound");
    if(Runtime::payload_mapping(ref)) demand(ref.protocol=="iotdbfs-"+ref.type+"/1",
      ErrorCode::Invalid,"unsupported payload mapping protocol");
  }
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();
    demand(view.head.identity==current_->head.identity,ErrorCode::Conflict,"artifact view belongs to another namespace");}
  // Charge logical metadata on both hits and misses. Payload bytes belong to
  // the caller's independent payload ledger, never the directory ledger.
  if(!payload) charge(used,total);
  std::vector<Bytes> result(requests.size());
  std::vector<ObjectReadRequest> missing;
  std::vector<std::size_t> positions;
  std::string single_payload_key;
  for(std::size_t i=0;i<requests.size();++i) {
    const auto& request=requests[i];auto key=Runtime::key(view.head.identity,request.ref);
    std::shared_ptr<const Bytes> cached;
    if(!payload&&!force&&Runtime::eligible(request.ref)) {
      std::lock_guard<std::mutex> lock(runtime_->mutex);
      auto it=runtime_->cache.find(key);
      if(it!=runtime_->cache.end()) {
        cached=it->second.bytes;runtime_->lru.splice(runtime_->lru.begin(),runtime_->lru,it->second.position);
        ++runtime_->counters.object_cache_hits;
      }
    }
    if(cached) {
      demand(cached->size()==request.size,ErrorCode::Corrupt,"cached artifact length mismatch");
      result[i]=*cached;query_profile_count("version_directory.object_cache_hits");
    } else {missing.push_back(request);positions.push_back(i);}
    // PayloadArtifacts owns source-qualified logical fees for mapping pages;
    // caching shared pages here must not deduplicate different sources' demand.
    if(!payload&&!Runtime::payload_mapping(request.ref))
      require_query_metadata("directory/"+key,request.size);
    if(payload&&requests.size()==1)single_payload_key=std::move(key);
  }
  if(!missing.empty()) {
    const auto load_validated = [&](const std::vector<ObjectReadRequest>& physical) {
      auto values=store_->get_objects(view.head.identity,physical);
      demand(values.size()==physical.size(),ErrorCode::Corrupt,"artifact batch result count mismatch");
      // Never publish unverified bytes into an in-flight read or metadata cache.
      for(std::size_t i=0;i<physical.size();++i) {
        const auto& request=physical[i];
        demand(values[i].size()==request.size&&content_id(request.ref.type,request.ref.protocol,values[i])==request.ref.id,
          ErrorCode::Corrupt,"artifact batch length or digest mismatch");
      }
      return values;
    };
    std::vector<Bytes> loaded;
    if(payload&&!force&&missing.size()==1&&runtime_->config.coalesce_payload_reads) {
      const auto& request=missing.front();
      std::vector<Bytes> direct_loaded;
      auto shared=runtime_->payload_flights.read(single_payload_key,request.size,
        [&] {direct_loaded=load_validated(missing);return std::move(direct_loaded.front());},
        [] {storage_read_checkpoint();},std::move(temporary_pin));
      // Each caller owns its output. The shared verified bytes and the original
      // producer's temporary reservation live until the last consumer releases.
      if(shared.bytes)loaded.emplace_back(*shared.bytes);
      else {
        // A private fallback must retain the Store's existing vector shell:
        // optional coalescer admission failure adds no post-read allocation.
        direct_loaded.front()=std::move(shared.owned);
        loaded=std::move(direct_loaded);
      }
    } else if(payload&&!force&&runtime_->config.coalesce_payload_reads) {
      std::vector<std::pair<std::string,std::size_t>> keys;
      std::vector<ObjectReadRequest> subsets[2];
      bool prepared=false;
      try {
        // Both the owned subset and a possible failed-producer fallback are
        // prepared before flight admission. Optional metadata allocation must
        // not add a failure after another producer has already been joined.
        subsets[0]=missing;subsets[1]=missing;
        keys.reserve(missing.size());
        for(const auto& request:missing)
          keys.emplace_back(Runtime::key(view.head.identity,request.ref),request.size);
        prepared=true;
      } catch(const std::bad_alloc&) {} // optional admission, before physical I/O
      if(prepared) {
        static_assert(std::is_nothrow_move_assignable<ObjectReadRequest>::value,
          "physical subset selection must not allocate after flight admission");
        std::size_t next_subset=0;
        loaded=runtime_->payload_flights.read_batch(keys,
          [&](const std::vector<std::size_t>* indices) {
            if(!indices)return load_validated(missing);
            auto& physical=subsets[next_subset++];
            // Flight indices are unique and increasing. Moving forward into
            // earlier slots never overwrites a later source; resize only shrinks.
            for(std::size_t j=0;j<indices->size();++j)
              if(j!=(*indices)[j])physical[j]=std::move(physical[(*indices)[j]]);
            physical.resize(indices->size());
            return load_validated(physical);
          },[] {storage_read_checkpoint();},std::move(temporary_pin));
      } else {
        std::vector<ObjectReadRequest>{}.swap(subsets[0]);
        std::vector<ObjectReadRequest>{}.swap(subsets[1]);
        decltype(keys){}.swap(keys);
        loaded=load_validated(missing);
      }
    } else loaded=load_validated(missing);
    std::uint64_t bytes=0;
    for(std::size_t i=0;i<missing.size();++i) {
      bytes+=loaded[i].size();
    }
    {std::lock_guard<std::mutex> lock(runtime_->mutex);runtime_->counters.object_reads+=loaded.size();runtime_->counters.hash_bytes+=bytes;}
    query_profile_count("version_directory.object_reads",loaded.size());query_profile_count("version_directory.hash_bytes",bytes);
    for(std::size_t i=0;i<missing.size();++i) {
      const auto& request=missing[i];
      if(!payload&&Runtime::eligible(request.ref))
        runtime_->insert(Runtime::key(view.head.identity,request.ref),std::make_shared<const Bytes>(loaded[i]));
      result[positions[i]]=std::move(loaded[i]);
    }
  }
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();}
  return result;
}
std::unique_ptr<Transaction> Owner::begin(const std::string& operation) {
  return std::unique_ptr<Transaction>(new Transaction(*this,operation));
}
std::optional<Bytes> Owner::get(const View& view,Table table,const std::string& key) const {
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();demand(view.head.identity==current_->head.identity,ErrorCode::Conflict,"view belongs to another namespace");}
  std::uint64_t used=0;Tree tree([&](const ObjectRef& ref){return checked_object(view.head.identity,ref,used);},{},[&](const ContentId& id){return checked_page(view.head.identity,id,used);});
  const auto id=tree.get(view.roots.at(static_cast<std::size_t>(table)),key);
  std::optional<Bytes> result;if(id)result=checked_object(view.head.identity,{"catalog-record","iotdbfs-catalog-record/1",*id},used);
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();}
  return result;
}
std::vector<std::pair<std::string,Bytes>> Owner::scan(const View& view,Table table,const std::string& prefix,std::size_t limit,
    const std::function<bool(const std::string&)>& key_predicate) const {
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();demand(view.head.identity==current_->head.identity,ErrorCode::Conflict,"view belongs to another namespace");}
  std::uint64_t used=0;Tree tree([&](const ObjectRef& ref){return checked_object(view.head.identity,ref,used);},{},[&](const ContentId& id){return checked_page(view.head.identity,id,used);});
  std::vector<std::pair<std::string,Bytes>> result;
  // Tree::scan still checks the prefix's full reference count against limit
  // and authenticates/charges every page it visits, before any key filtering.
  for(const auto& item:tree.scan(view.roots.at(static_cast<std::size_t>(table)),prefix,limit)) {
    if(key_predicate && !key_predicate(item.key)) continue;
    result.emplace_back(item.key,checked_object(view.head.identity,{"catalog-record","iotdbfs-catalog-record/1",item.value},used));
  }
  {std::lock_guard<std::recursive_mutex> lock(mutex_);require_ready();}
  return result;
}
Transaction::Transaction(Owner& owner,std::string operation)
  :owner_(owner),writer_lock_(owner.writer_mutex_),operation_(std::move(operation)) {
  std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);
  owner_.require_ready();
  demand(!owner_.transaction_active_,ErrorCode::Conflict,"nested owner transaction is forbidden");
  demand(!operation_.empty() && operation_.size()<=128,ErrorCode::Invalid,"invalid transaction operation");
  base_=*owner_.current_;roots_=base_.roots;owner_.transaction_active_=true;
}
Transaction::~Transaction() { abort(); }
ObjectRef Transaction::put_artifact(const std::string& type,const Bytes& bytes) {
  demand(!done_,ErrorCode::Invalid,"transaction already finished");
  demand(type!="payload-block",ErrorCode::Invalid,"payload blocks require the dedicated write channel");
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.require_ready();}
  auto ref=object_ref(type,bytes);
  demand(owner_.put_object(type,bytes,bytes_)==ref.id,ErrorCode::Corrupt,"artifact identity changed during persistence");
  return ref;
}
ObjectRef Transaction::put_payload_block(const Bytes& bytes) {
  demand(!done_,ErrorCode::Invalid,"transaction already finished");
  demand(!bytes.empty()&&bytes.size()<=kPayloadBlockBytes,ErrorCode::Limit,"payload block size exceeds bounds");
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.require_ready();}
  const auto ref=object_ref("payload-block",bytes);
  owner_.store_->put_object(base_.head.identity,ref,bytes);
  const auto actual=owner_.store_->get_object(base_.head.identity,ref);
  demand(actual==bytes&&content_id(ref.type,ref.protocol,actual)==ref.id,ErrorCode::Corrupt,"payload block persistence mismatch");
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.require_ready();}
  return ref;
}
void Transaction::put(Table table,const std::string& key,const Bytes& record) {
  demand(!done_,ErrorCode::Invalid,"transaction already finished");
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.require_ready();}
  demand(dependencies_.size()<kScanEntries,ErrorCode::Limit,"transaction dependency limit exhausted");
  const auto id=owner_.put_object("catalog-record",record,bytes_);
  Tree tree([&](const ObjectRef& ref){return owner_.checked_object(base_.head.identity,ref,bytes_);},
    [&](const Bytes& bytes){return owner_.put_object("catalog-page",bytes,bytes_);});
  auto& root=roots_.at(static_cast<std::size_t>(table)); root=tree.set(root,key,id);
  dependencies_.push_back(std::string(1,static_cast<char>(table))+key);
}
void Transaction::erase(Table table,const std::string& key) {
  demand(!done_,ErrorCode::Invalid,"transaction already finished");
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.require_ready();}
  demand(dependencies_.size()<kScanEntries,ErrorCode::Limit,"transaction dependency limit exhausted");
  Tree tree([&](const ObjectRef& ref){return owner_.checked_object(base_.head.identity,ref,bytes_);},
    [&](const Bytes& bytes){return owner_.put_object("catalog-page",bytes,bytes_);});
  auto& root=roots_.at(static_cast<std::size_t>(table)); root=tree.set(root,key,{});
  dependencies_.push_back(std::string(1,static_cast<char>(table))+key);
}
std::optional<Bytes> Transaction::get(Table table,const std::string& key) {
  demand(!done_,ErrorCode::Invalid,"transaction already finished");
  return owner_.get(View{base_.head,roots_},table,key);
}
void Transaction::commit() {
  demand(!done_,ErrorCode::Invalid,"transaction already finished");
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.require_ready();}
  std::sort(dependencies_.begin(),dependencies_.end());
  dependencies_.erase(std::unique(dependencies_.begin(),dependencies_.end()),dependencies_.end());
  canonical_dependencies(dependencies_);
  if (roots_!=base_.roots) owner_.publish(roots_,operation_,dependencies_);
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.transaction_active_=false;}
  done_=true;writer_lock_.unlock();
}
void Transaction::abort() {
  if (done_) return;
  {std::lock_guard<std::recursive_mutex> gate(owner_.mutex_);owner_.transaction_active_=false;}
  done_=true;
  if (writer_lock_.owns_lock()) writer_lock_.unlock();
}
} // namespace iotdbfs::versioning
