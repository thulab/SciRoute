// Requires the mount to have released its cooperative owner lock. Recovers an
// existing V02 namespace with full validation; never initializes a namespace.
// Recovery may retry committed staging cleanup; this is not a pure read-only tool.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/FormatRegistry.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"

#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <sys/resource.h>
#include <unistd.h>

using namespace iotdbfs;
using namespace iotdbfs::versioning;
namespace {
void check(bool okay, const char* reason) { if (!okay) throw std::runtime_error(reason); }
std::string env(const char* key) {
  const auto* value=std::getenv(key); check(value && *value,"required environment missing"); return value;
}
std::string quote(const std::string& value) {
  std::ostringstream out;out << '"';const char* hex="0123456789abcdef";
  for (unsigned char c:value) {
    if (c=='"'||c=='\\') out << '\\' << c;
    else if (c<32) out << "\\u00" << hex[c>>4] << hex[c&15];
    else out << c;
  }
  out << '"';return out.str();
}
std::string stats_json(const ContentArtifactStats& s) {
  std::ostringstream out;out << '{';
#define FIELD(name) out << "\"" #name "\":" << s.name << ','
  FIELD(object_reads); FIELD(object_cache_hits); FIELD(hash_bytes);
  FIELD(metadata_parses); FIELD(catalog_parses); FIELD(layout_validations);
  FIELD(cache_bytes); FIELD(cache_entries);
#undef FIELD
  out << "\"evictions\":" << s.evictions << '}';return out.str();
}
std::uint64_t peak_rss() {
  struct rusage usage{};check(getrusage(RUSAGE_SELF,&usage)==0,"cannot read peak RSS");
#ifdef __APPLE__
  return usage.ru_maxrss;
#else
  return static_cast<std::uint64_t>(usage.ru_maxrss)*1024;
#endif
}
std::string current_rss() {
  std::ifstream in("/proc/self/status");std::string line;
  while(std::getline(in,line)) if(line.rfind("VmRSS:",0)==0) {
    std::istringstream field(line.substr(6));std::uint64_t kb=0;field>>kb;return std::to_string(kb*1024);
  }
  return "null";
}
}
int main(int argc,char** argv) {
  std::string stage="arguments";
  set_logging_level(LogLevel::OFF);
  try {
    std::string prefix,journal,path;std::optional<long long> version;unsigned repeats=2;
    for(int i=1;i<argc;++i) {
      const std::string arg=argv[i];check(i+1<argc,"missing argument value");
      const std::string value=argv[++i];
      if(arg=="--prefix") prefix=value;
      else if(arg=="--journal") journal=value;
      else if(arg=="--path") path=value;
      else if(arg=="--version") {std::size_t n=0;version=std::stoll(value,&n);check(n==value.size()&&*version>=0,"bad version");}
      else if(arg=="--repeats") {std::size_t n=0;auto count=std::stoul(value,&n);check(n==value.size()&&count>=2&&count<=16,"bad repeats");repeats=count;}
      else throw std::runtime_error("unknown option");
    }
    check(!prefix.empty()&&!journal.empty()&&journal.front()=='/'&&(!version||!path.empty()),"prefix and absolute journal required");
    stage="configuration";IoTDBClientConfig client;
    client.host=env("IOTDB_HOST");client.user=env("IOTDB_USER");client.pass=env("IOTDB_PASS");client.db=env("IOTDB_DB");
    const auto port=env("IOTDB_PORT");std::size_t consumed=0;client.port=std::stoi(port,&consumed);
    check(consumed==port.size()&&client.port>0&&client.port<=65535,"invalid port");
    check(client.host=="localhost"||client.host=="127.0.0.1","loopback required");client.host="127.0.0.1";client.auto_create_db=false;
    IoTDBVersionStore store(client,prefix);store.verify_capabilities();
    // This independent no-create client successfully USEs the existing DB and
    // reads its namespace before the legacy Connector initialization.
    const auto head_before=store.read_head();check(bool(head_before),"existing head required");
    IoTDBConfig connection{client.host,client.port,client.user,client.pass,client.db};
    IoTDBConnector::instance().initialize(connection,2);
    FormatRegistry registry;
    check(registry.register_adapter(std::make_shared<ArrowFormatAdapter>()),"Arrow registration failed");
    check(registry.register_adapter(std::make_shared<Hdf5FormatAdapter>()),"HDF5 registration failed");
    check(registry.register_adapter(std::make_shared<GribFormatAdapter>(GribFormatAdapter::Config{})),"GRIB registration failed");
    IoTDBStructuredBlobBackend backend;VersionDirectoryConfig config;
    config.table_prefix=prefix;config.owner_directory=journal;config.initialize=false;
    config.artifact_validator=[&](const std::string& format,const Bytes& layout,std::uint64_t size) {
      const auto adapter=registry.resolve_by_id(format);
      if(!adapter) return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,"format unavailable");
      const auto result=adapter->validate_layout_artifact(layout,size);
      return result.ok()?StructuredBlobStatus::success():StructuredBlobStatus::failure(StructuredBlobError::InvalidState,result.error());
    };
    stage="recover";const auto start=std::chrono::steady_clock::now();
    const auto before=backend.content_artifact_stats();
    check(backend.configure_version_directory(config),"owner recovery failed");
    const auto recover_ms=std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-start).count();
    const auto recovered=backend.content_artifact_stats();
    stage="inventory";const auto inventory=backend.enumerate_namespace_subtree("/");check(inventory.ok(),"inventory failed");
    std::vector<Inode> files;
    for(const auto& inode:inventory.value()) if(!inode.is_dir&&(path.empty()||inode.path==path)&&(!version||inode.row_time==*version)) files.push_back(inode);
    check(!files.empty(),"no matching published file versions");check(files.size()<=65536,"inventory limit");
    const auto inventoried=backend.content_artifact_stats();
    std::map<std::string,std::vector<StructuredBlobVersionKey>> identities;
    std::map<StructuredBlobVersionKey,std::string> layout_hashes;
    std::vector<ContentArtifactStats> passes;std::vector<double> pass_ms;
    for(unsigned pass=0;pass<repeats;++pass) {
      stage="read_pass";const auto read_start=std::chrono::steady_clock::now();std::uint64_t bytes=0;
      for(const auto& inode:files) {
        const StructuredBlobVersionKey key{inode.path,inode.row_time};
        const auto manifest=backend.load_manifest(key);check(manifest.ok(),"manifest read failed");
        const auto identity=backend.verified_layout_identity(key);check(identity.ok()&&identity.value().has_value(),"layout identity missing");
        check(manifest.value().layout_size<=256ULL*1024*1024-bytes,"per-pass byte limit");bytes+=manifest.value().layout_size;
        const auto layout=backend.read_layout_range(key,0,manifest.value().layout_size);check(layout.ok()&&layout.value().size()==manifest.value().layout_size,"layout read failed");
        const auto hash=content_id("probe-layout","iotdbfs-v02-probe/1",layout.value());
        if(pass==0) {identities[*identity.value()].push_back(key);layout_hashes[key]=hash;}
        else check(layout_hashes.at(key)==hash,"layout changed across passes");
        if(manifest.value().metadata.count("__iotdbfs.dataset_catalog.protocol")) check(backend.read_dataset_catalog(key).ok(),"catalog read failed");
      }
      pass_ms.push_back(std::chrono::duration<double,std::milli>(std::chrono::steady_clock::now()-read_start).count());
      passes.push_back(backend.content_artifact_stats());
    }
    stage="final_head";check(store.read_head()==head_before,"read probe changed namespace head");
    const auto head=decode_head(*head_before);
    const auto& a=passes[passes.size()-2];const auto& b=passes.back();
    const bool hot_reuse=a.metadata_parses==b.metadata_parses&&a.catalog_parses==b.catalog_parses&&a.layout_validations==b.layout_validations;
    std::cout << "{\"completed\":true,\"command\":\"v02_cache\",\"prefix\":" << quote(prefix)
      << ",\"head_sequence\":" << head.sequence << ",\"head_root\":" << quote(head.root)
      << ",\"pid\":" << getpid() << ",\"rss_bytes\":" << current_rss() << ",\"peak_rss_bytes\":" << peak_rss()
      << ",\"recover_ms\":" << recover_ms << ",\"versions\":" << files.size() << ",\"layout_identities\":" << identities.size()
      << ",\"last_pass_reused_parses\":" << (hot_reuse?"true":"false") << ",\"cleanup_failures\":" << backend.staging_cleanup_failures()
      << ",\"before\":" << stats_json(before) << ",\"after_recovery\":" << stats_json(recovered)
      << ",\"after_inventory\":" << stats_json(inventoried) << ",\"passes\":[";
    for(std::size_t i=0;i<passes.size();++i) std::cout << (i?",":"") << "{\"elapsed_ms\":" << pass_ms[i] << ",\"stats\":" << stats_json(passes[i]) << '}';
    std::cout << "],\"groups\":[";bool first=true;
    for(const auto& item:identities) {
      std::cout << (first?"":",") << "{\"layout_identity\":" << quote(item.first) << ",\"versions\":[";first=false;
      for(std::size_t i=0;i<item.second.size();++i) {const auto& key=item.second[i];std::cout << (i?",":"") << "{\"path\":" << quote(key.path) << ",\"version\":" << key.version << ",\"layout_hash\":" << quote(layout_hashes.at(key)) << '}';}
      std::cout << "]}";
    }
    std::cout << "],\"mutates_permissions\":false,\"retries_committed_staging_cleanup\":true,\"pure_read_only\":false,\"head_unchanged\":true}\n";return 0;
  } catch(const std::exception&) {
    // SDK diagnostics may contain credentials; report only the bounded stage.
    std::cout << "{\"completed\":false,\"stage\":" << quote(stage) << ",\"error\":\"probe failed; no automatic retry\"}\n";return 1;
  }
}
