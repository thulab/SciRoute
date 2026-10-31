// Run only after the mount releases its namespace/FileJournal locks. Reopens
// an existing namespace, including full scientific-format recovery validation.
// Recovery may retry committed staging cleanup; this is not a pure SQL reader.
// Never initializes a namespace, creates schema, or changes a namespace head.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/FormatRegistry.h"
#include "iotdbfs/formats/arrow/ArrowFormatAdapter.h"
#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include <openssl/evp.h>
#include <algorithm>
#include <cstdlib>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

using namespace iotdbfs;
using namespace iotdbfs::versioning;
namespace {
class ProbeFailure final : public std::runtime_error {
public: explicit ProbeFailure(const char* message):std::runtime_error(message) {}
};
void check(bool value,const char* message) {if(!value)throw ProbeFailure(message);}
std::string env(const char* name) {auto* value=std::getenv(name);check(value&&*value,"required environment missing");return value;}
std::string quote(const std::string& value) {
  std::ostringstream out;out<<'"';const char* hex="0123456789abcdef";
  for(unsigned char c:value) {
    if(c=='"'||c=='\\')out<<'\\'<<c;
    else if(c<32)out<<"\\u00"<<hex[c>>4]<<hex[c&15];else out<<c;
  }
  out<<'"';return out.str();
}
std::uint64_t number(const std::string& value) {
  check(!value.empty()&&std::all_of(value.begin(),value.end(),[](char c){return c>='0'&&c<='9';}),"invalid numeric argument");
  std::size_t used=0;auto result=std::stoull(value,&used);check(used==value.size(),"invalid numeric argument");return result;
}
std::string sha(const Bytes& bytes) {
  unsigned char digest[32];unsigned size=0;
  check(EVP_Digest(bytes.data(),bytes.size(),digest,&size,EVP_sha256(),nullptr)==1&&size==32,"SHA256 failed");
  constexpr char digits[]="0123456789abcdef";std::string result;result.reserve(64);
  for(auto byte:digest){result+=digits[byte>>4];result+=digits[byte&15];}return result;
}
Bytes journal_state(const std::string& directory) {
  const int fd=::open((directory+"/state").c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
  check(fd>=0,"existing journal state required");
  struct Close {int fd;~Close(){::close(fd);}} close{fd};
  struct stat info{};check(::fstat(fd,&info)==0&&S_ISREG(info.st_mode)&&info.st_size>0&&
    info.st_size<=2*kHeadBytes+256,"journal state is not a bounded regular file");
  Bytes result(static_cast<std::size_t>(info.st_size));std::size_t offset=0;
  while(offset<result.size()){const auto count=::read(fd,result.data()+offset,result.size()-offset);check(count>0,"journal read failed");offset+=count;}
  return result;
}
std::uint64_t count(const QueryProfileRecord& profile,const char* key) {
  const auto found=profile.counters.find(key);return found==profile.counters.end()?0:found->second;
}
struct Observation {
  std::string name;
  StructuredBlobPayloadRanges ranges;
  StructuredBlobPayloadBatch outputs;
  QueryProfileRecord profile;
  std::uint64_t sdk_queries{},store_queries{},batch_queries{},returned_blob_bytes{};
};
Observation observe(const std::string& name,const StructuredBlobReadHandle& reader,
    StructuredBlobPayloadRanges ranges) {
  Observation result;result.name=name;result.ranges=std::move(ranges);
  const auto before=iotdb_profile_snapshot();
  {
    ScopedQueryMetadataBudget budget;
    ScopedQueryProfile profile(&result.profile);
    auto loaded=reader.read_payload_ranges(result.ranges);
    check(loaded.ok(),"production ReadHandle range read failed");
    check(budget.status().ok(),"production range request budget failed");
    result.outputs=std::move(loaded.value());
  }
  const auto after=iotdb_profile_snapshot();
  result.sdk_queries=after.query_calls-before.query_calls;
  result.store_queries=after.version_store_query_calls-before.version_store_query_calls;
  result.batch_queries=after.version_store_batch_query_calls-before.version_store_batch_query_calls;
  result.returned_blob_bytes=after.version_store_blob_bytes-before.version_store_blob_bytes;
  check(result.outputs.size()==result.ranges.size(),"production range count mismatch");
  for(std::size_t i=0;i<result.ranges.size();++i)
    check(result.outputs[i].size()==result.ranges[i].length,"production range length mismatch");
  return result;
}
std::string json(const Observation& value) {
  std::ostringstream out;out<<"{\"name\":"<<quote(value.name)
    <<",\"sdk_query_calls\":"<<value.sdk_queries<<",\"version_store_query_calls\":"<<value.store_queries
    <<",\"actual_query_sum\":"<<value.sdk_queries+value.store_queries
    <<",\"version_store_batch_query_calls\":"<<value.batch_queries
    <<",\"store_returned_blob_bytes\":"<<value.returned_blob_bytes
    <<",\"map_root_requests\":"<<count(value.profile,"payload_map.root_requests")
    <<",\"map_page_requests\":"<<count(value.profile,"payload_map.page_requests")
    <<",\"data_batches\":"<<count(value.profile,"payload_map.data_batches")
    <<",\"actual_block_bytes\":"<<count(value.profile,"payload_map.block_bytes")
    <<",\"output_bytes\":"<<count(value.profile,"payload_map.output_bytes")
    <<",\"logical_extents\":"<<count(value.profile,"payload_map.logical_extents")<<",\"outputs\":[";
  for(std::size_t i=0;i<value.outputs.size();++i) {
    if(i)out<<',';
    out<<"{\"offset\":"<<value.ranges[i].offset<<",\"length\":"<<value.ranges[i].length
      <<",\"sha256\":"<<quote(sha(value.outputs[i]))<<'}';
  }
  out<<"],\"profile_counters\":{";bool first=true;
  for(const auto& item:value.profile.counters){out<<(first?"":",")<<quote(item.first)<<':'<<item.second;first=false;}
  out<<"}}";return out.str();
}
}

int main(int argc,char** argv) {
  set_logging_level(LogLevel::OFF);std::string stage="arguments";
  try {
    std::string prefix,journal,path;std::optional<std::uint64_t> version,size;
    for(int i=1;i<argc;++i) {
      const std::string option=argv[i];check(i+1<argc,"missing option value");const std::string value=argv[++i];
      if(option=="--prefix")prefix=value;else if(option=="--journal")journal=value;
      else if(option=="--path")path=value;else if(option=="--version")version=number(value);
      else if(option=="--size")size=number(value);else throw ProbeFailure("unknown option");
    }
    check(prefix.rfind("vd_v04_",0)==0&&!journal.empty()&&journal.front()=='/'&&!path.empty()&&path.front()=='/'&&
      version&&*version<=static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())&&size,
      "existing V04 prefix, absolute journal/path, version and size required");
    check(*size==3*kPayloadBlockBytes+137,"fixture must contain 3 MiB plus 137 bytes");
    const auto journal_before=journal_state(journal);
    stage="existing_database";IoTDBClientConfig client;
    client.host=env("IOTDB_HOST");client.user=env("IOTDB_USER");client.pass=env("IOTDB_PASS");client.db=env("IOTDB_DB");
    const auto port=number(env("IOTDB_PORT"));check(port>0&&port<=65535,"invalid RPC port");client.port=static_cast<int>(port);
    check(client.host=="127.0.0.1"||client.host=="localhost","controlled loopback required");client.host="127.0.0.1";client.auto_create_db=false;
    IoTDBVersionStore store(client,prefix);store.verify_capabilities();
    const auto head_before=store.read_head();check(bool(head_before),"existing namespace head required");
    IoTDBConnector::instance().initialize({client.host,client.port,client.user,client.pass,client.db},2);
    FormatRegistry formats;
    check(formats.register_adapter(std::make_shared<ArrowFormatAdapter>()),"Arrow registration failed");
    check(formats.register_adapter(std::make_shared<Hdf5FormatAdapter>()),"HDF5 registration failed");
    check(formats.register_adapter(std::make_shared<GribFormatAdapter>(GribFormatAdapter::Config{})),"GRIB registration failed");
    auto backend=std::make_shared<IoTDBStructuredBlobBackend>();VersionDirectoryConfig settings;
    settings.table_prefix=prefix;settings.owner_directory=journal;settings.initialize=false;
    settings.artifact_validator=[&](const std::string& format,const Bytes& layout,std::uint64_t payload_size) {
      auto adapter=formats.resolve_by_id(format);
      if(!adapter)return StructuredBlobStatus::failure(StructuredBlobError::InvalidState,"format unavailable");
      const auto valid=adapter->validate_layout_artifact(layout,payload_size);
      return valid.ok()?StructuredBlobStatus::success():StructuredBlobStatus::failure(StructuredBlobError::InvalidState,"layout validation failed");
    };
    stage="owner_recovery";check(backend->configure_version_directory(settings),"owner recovery failed");
    check(journal_state(journal)==journal_before,"owner recovery changed journal state");
    stage="open_published_exact";StructuredBlobStorage storage(backend);
    auto opened=storage.open_published_version({path,static_cast<std::int64_t>(*version)});
    check(opened.ok()&&opened.value().manifest().payload_size==*size,"published exact file missing or size differs");
    auto& reader=opened.value();check(reader.supports_payload_ranges(),"production batch capability absent");
    stage="full_warmup";auto first=observe("full_warmup",reader,{{0,*size}});
    stage="unordered_cross_block";auto mixed=observe("unordered_cross_block",reader,
      {{2*kPayloadBlockBytes-3,13},{7,19},{kPayloadBlockBytes-2,7},{*size-17,17},{7,19},{0,1},{9,8}});
    for(std::size_t i=0;i<mixed.ranges.size();++i) {
      const auto& range=mixed.ranges[i];
      check(std::equal(mixed.outputs[i].begin(),mixed.outputs[i].end(),first.outputs[0].begin()+range.offset),
        "cross-block, unordered or duplicate range bytes differ from full read");
    }
    stage="warm_full_single_batch";auto warm=observe("warm_full_single_batch",reader,{{0,*size}});
    check(warm.outputs==first.outputs,"warm full read differs");
    check(count(warm.profile,"payload_map.data_batches")==1&&count(warm.profile,"payload_map.logical_extents")==4&&
      count(warm.profile,"payload_map.output_bytes")==*size,"production four-extent batch instrumentation differs");
    check(warm.sdk_queries+warm.store_queries==1&&warm.batch_queries==1,"warm production read did not use exactly one native batch SQL");
    check(warm.returned_blob_bytes==count(warm.profile,"payload_map.block_bytes"),"warm read includes unaccounted Store bytes");
    stage="unchanged_persistence";const auto head_after=store.read_head();const auto journal_after=journal_state(journal);
    check(head_after==head_before&&journal_after==journal_before,"probe changed namespace head or journal");
    const auto head=decode_head(*head_after);
    std::cout<<"{\"completed\":true,\"command\":\"v04_range\",\"prefix\":"<<quote(prefix)
      <<",\"path\":"<<quote(path)<<",\"version\":"<<*version<<",\"size\":"<<*size
      <<",\"head_unchanged\":true,\"head_sha256_before\":"<<quote(sha(*head_before))
      <<",\"head_sha256_after\":"<<quote(sha(*head_after))<<",\"head_sequence\":"<<head.sequence
      <<",\"namespace_id\":"<<quote(head.identity.namespace_id)<<",\"incarnation\":"<<quote(head.identity.incarnation)
      <<",\"head_commit\":"<<quote(head.commit)<<",\"head_root\":"<<quote(head.root)
      <<",\"journal_unchanged\":true,\"journal_sha256_before\":"<<quote(sha(journal_before))
      <<",\"journal_sha256_after\":"<<quote(sha(journal_after))
      <<",\"full_sha256\":"<<quote(sha(first.outputs[0]))<<",\"production_read_handle\":true"
      <<",\"existing_namespace_only\":true,\"recovery_may_retry_committed_staging_cleanup\":true,\"observations\":["
      <<json(first)<<','<<json(mixed)<<','<<json(warm)<<"]}\n";return 0;
  } catch(const ProbeFailure& error) {
    std::cout<<"{\"completed\":false,\"stage\":"<<quote(stage)<<",\"error\":"<<quote(error.what())<<"}\n";return 1;
  } catch(const std::exception&) {
    // SDK exception text may contain connection details; never serialize it.
    std::cout<<"{\"completed\":false,\"stage\":"<<quote(stage)<<",\"error\":\"probe operation failed\"}\n";return 1;
  }
}
