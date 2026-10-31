// Real owner-backend hot-domain/proof integration. Explicitly new vd_hot_*
// prefix only, existing database only; no service control or cleanup.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include "iotdbfs/storage/versioning/IoTDBVersionStore.h"
#include <hdf5.h>
#include <algorithm>
#include <array>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <unistd.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <tuple>

using namespace iotdbfs;
using namespace iotdbfs::versioning;
namespace {
class ProbeFailure final : public std::runtime_error {
public: explicit ProbeFailure(const char* reason):std::runtime_error(reason){}
};
void check(bool okay,const char* reason) {if(!okay)throw ProbeFailure(reason);}
std::string env(const char* name) {const auto* s=std::getenv(name);check(s&&*s,"required environment missing");return s;}
std::string quoted(const std::string& s) {
  std::ostringstream out;out<<'"';const char* hex="0123456789abcdef";
  for(unsigned char c:s) {if(c=='"'||c=='\\')out<<'\\'<<c;else if(c<32)out<<"\\u00"<<hex[c>>4]<<hex[c&15];else out<<c;}
  out<<'"';return out.str();
}
struct H5 {
  hid_t id;herr_t(*close)(hid_t);
  H5(hid_t value,herr_t(*fn)(hid_t)):id(value),close(fn){check(id>=0,"HDF5 fixture allocation failed");}
  ~H5(){close(id);} operator hid_t()const{return id;}
};
void h5check(herr_t status) {check(status>=0,"HDF5 fixture operation failed");}

// Every ordinary dataset has the exact six scalar types used by route3.
// special=1 omits level; special=2 supplies two level values (unsupported).
Bytes hdf_file(int count,std::int64_t level,int special=0) {
  H5 access(H5Pcreate(H5P_FILE_ACCESS),H5Pclose);h5check(H5Pset_fapl_core(access,4096,0));
  H5 file(H5Fcreate("global-hot-cache-memory",H5F_ACC_TRUNC,H5P_DEFAULT,access),H5Fclose);
  const hsize_t extent[]{4};H5 space(H5Screate_simple(1,extent,nullptr),H5Sclose);
  H5 scalar(H5Screate(H5S_SCALAR),H5Sclose);
  for(int n=0;n<count;++n) {
    const auto name="/values_"+std::to_string(n);
    H5 dataset(H5Dcreate2(file,name.c_str(),H5T_IEEE_F32LE,space,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT),H5Dclose);
    const float values[]{1,2,3,4};h5check(H5Dwrite(dataset,H5T_NATIVE_FLOAT,H5S_ALL,H5S_ALL,H5P_DEFAULT,values));
    for(const auto& item:std::vector<std::pair<std::string,std::string>>{{"shortName","t"},{"typeOfLevel","isobaricInhPa"}}) {
      H5 text(H5Tcopy(H5T_C_S1),H5Tclose);h5check(H5Tset_size(text,item.second.size()));
      H5 attr(H5Acreate2(dataset,item.first.c_str(),text,scalar,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
      h5check(H5Awrite(attr,text,item.second.data()));
    }
    for(const auto& item:std::vector<std::pair<std::string,std::int64_t>>{{"level",level},{"step",0},{"validityDate",20260101},{"validityTime",0}}) {
      if(item.first=="level"&&special==1)continue;
      const hsize_t two[]{2};H5 array(H5Screate_simple(1,two,nullptr),H5Sclose);
      const hid_t shape=item.first=="level"&&special==2?array.id:scalar.id;
      H5 attr(H5Acreate2(dataset,item.first.c_str(),H5T_STD_I64LE,shape,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
      const std::int64_t values[]{item.second,item.second};
      h5check(H5Awrite(attr,H5T_NATIVE_INT64,values));
    }
  }
  h5check(H5Fflush(file,H5F_SCOPE_GLOBAL));const auto size=H5Fget_file_image(file,nullptr,0);
  check(size>0,"HDF5 image empty");Bytes result(size);
  check(H5Fget_file_image(file,result.data(),result.size())==size,"HDF5 image differs");return result;
}
Inode inode(const std::string& path,long long version,bool directory) {
  Inode i;i.path=path;i.parent=path_utils::parent_of(path);i.name=path_utils::base(path);
  i.row_time=version;i.is_dir=directory;i.mode=directory?0755:0640;i.uid=1001;i.gid=40077;i.metadata="{}";
  i.ctime=i.mtime=i.atime=1700000000;return i;
}
QueryExpressionPtr full_predicate(std::int64_t level=850) {
  std::vector<QueryExpressionPtr> fields;
  for(const auto& pair:std::vector<std::pair<std::string,std::string>>{{"shortName","t"},{"typeOfLevel","isobaricInhPa"}})
    fields.push_back(make_comparison({"hdf5.attr:"+pair.first},ComparisonOperator::Equal,LogicalValue::utf8(pair.second)));
  for(const auto& pair:std::vector<std::pair<std::string,std::int64_t>>{{"level",level},{"step",0},{"validityDate",20260101},{"validityTime",0}})
    fields.push_back(make_comparison({"hdf5.attr:"+pair.first},ComparisonOperator::Equal,LogicalValue::int64(pair.second)));
  return make_logical(LogicalOperator::And,std::move(fields));
}
std::uint64_t counter(const QueryProfileRecord& p,const std::string& name) {
  const auto found=p.counters.find(name);return found==p.counters.end()?0:found->second;
}
struct Run {
  QueryStatus status,ledger;std::uint64_t bytes{0},sdk_queries{0};QueryProfileRecord profile;
  std::shared_ptr<const QueryReadContext> context;
  std::vector<std::tuple<std::string,long long,std::string>> matches;
  std::uint64_t selected{0},candidates{0},catalog_bytes{0};
  QueryMetadataTrace trace;
};
void pool_bounds(const HotQueryCacheStats& s) {
  check(s.resident_bytes<=HotQueryCache::maximum_bytes&&s.domains<=HotQueryCache::maximum_domains&&
    s.proofs<=HotQueryCache::maximum_proofs&&s.handles<=HotQueryCache::maximum_handles&&
    s.handle_bytes<=HotQueryCache::maximum_handle_bytes,"hot pool exceeded fixed bound including pins");
}
std::string stats_json(const HotQueryCacheStats& s) {
  std::ostringstream o;o<<"{\"resident_bytes\":"<<s.resident_bytes<<",\"domains\":"<<s.domains<<",\"proofs\":"<<s.proofs
    <<",\"handles\":"<<s.handles<<",\"handle_bytes\":"<<s.handle_bytes
    <<",\"domain_hits\":"<<s.domain_hits<<",\"domain_builds\":"<<s.domain_builds<<",\"domain_waits\":"<<s.domain_waits
    <<",\"proof_hits\":"<<s.proof_hits<<",\"proof_builds\":"<<s.proof_builds<<",\"opt_outs\":"<<s.opt_outs<<",\"evictions\":"<<s.evictions<<'}';return o.str();
}
}
int main(int argc,char** argv) {
  set_logging_level(LogLevel::OFF);std::string stage="arguments";std::vector<std::string> cases;
  std::map<std::string,std::uint64_t> observations;
  try {
    std::string prefix,journal;
    for(int i=1;i<argc;++i){const std::string arg=argv[i];check(i+1<argc,"missing argument value");
      if(arg=="--prefix")prefix=argv[++i];else if(arg=="--journal")journal=argv[++i];else throw ProbeFailure("unknown argument");}
    check(prefix.rfind("vd_hot_",0)==0&&!journal.empty()&&journal.front()=='/',"fresh vd_hot prefix and absolute journal required");
    check(::access(journal.c_str(),F_OK)!=0,"journal exists; no resume");
    stage="configuration";IoTDBClientConfig config;
    config.host=env("IOTDB_HOST");config.user=env("IOTDB_USER");config.pass=env("IOTDB_PASS");config.db=env("IOTDB_DB");
    const auto port=env("IOTDB_PORT");std::size_t used=0;config.port=std::stoi(port,&used);
    check(used==port.size()&&config.port==33667,"controlled original RPC port required");
    check(config.host=="127.0.0.1"||config.host=="localhost","controlled loopback required");config.host="127.0.0.1";
    check(config.db=="h09probe1790939889dbfc1f","existing experiment database required");config.auto_create_db=false;
    IoTDBClient existing_database(config,1);
    IoTDBConnector::instance().initialize({config.host,config.port,config.user,config.pass,config.db},2);
    auto backend=std::make_shared<IoTDBStructuredBlobBackend>();
    FormatRegistry formats;auto adapter=std::make_shared<Hdf5FormatAdapter>();check(formats.register_adapter(adapter),"format registration failed");
    VersionDirectoryConfig settings;settings.table_prefix=prefix;settings.owner_directory=journal;settings.initialize=true;
    settings.artifact_validator=[adapter](const std::string& format,const Bytes& layout,std::uint64_t size) {
      if(format!="hdf5")return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,"unexpected fixture format");
      const auto status=adapter->validate_layout_artifact(layout,size);
      return status.ok()?StructuredBlobStatus::success():StructuredBlobStatus::failure(StructuredBlobError::InvalidState,status.error());
    };
    stage="initialize";check(backend->configure_version_directory(settings),"owner initialization failed");check(backend->ensure_schema(),"schema failed");
    IoTDBVersionStore store(config,prefix);store.verify_capabilities();
    for(const auto& path:{"/","/data","/generic","/unrelated"})check(backend->upsert_inode(inode(path,1,true),true),"directory creation failed");
    StructuredBlobStorage storage(backend);IngestionModule ingestion(storage,formats,{});
    auto publish=[&](const std::string& path,long long version,int count,std::int64_t level,int special=0) {
      auto bytes=hdf_file(count,level,special);FormatSource source{bytes.size(),[&](std::uint64_t offset,std::uint64_t size)->std::optional<Bytes>{
        if(offset>bytes.size()||size>bytes.size()-offset)return {};return Bytes(bytes.begin()+offset,bytes.begin()+offset+size);}};
      const auto result=ingestion.ingest({path,version},source,[&](const IngestionOutcome& out){
        auto i=inode(path,version,false);i.size=out.payload_size;i.format_id=out.format_id.value_or("");
        const auto metadata=serialize_metadata(out.metadata,65536);check(metadata.ok(),"metadata encoding failed");i.metadata=metadata.value();
        return backend->upsert_inode(i,false);
      });check(result.ok(),"real HDF5 ingestion failed");
    };
    auto mutate=[&](const std::string& path,long long version,const std::function<void(Inode&)>& edit) {
      auto i=backend->load_inode(path,version);check(bool(i),"mutation inode absent");
      if(!i->is_dir){auto m=backend->load_manifest({path,version});check(m.ok(),"mutation manifest absent");
        auto text=serialize_metadata(m.value().metadata,65536);check(text.ok(),"mutation metadata failed");i->metadata=text.value();}
      edit(*i);check(backend->upsert_inode(*i,i->is_dir),"POSIX mutation failed");
    };
    stage="fixture";publish("/data/a.h5",10,2,850);publish("/data/b.h5",11,1,851);
    const QueryPrincipal owner{1001,40077,{}},denied{1003,5000,{}};
    DatasetCatalogRequest request;request.sources.paths={{PathScopeKind::Subtree,"/data"}};
    request.sources.versions=VersionSelection::latest();request.sources.formats.adapters=AdapterSelectionKind::Listed;
    request.sources.formats.adapter_ids={"hdf5"};request.sources.formats.include_byte_passthrough=false;request.predicate=full_predicate();
    auto run=[&](const DatasetCatalogRequest& req,const QueryPrincipal& principal,std::uint64_t precharge=0,bool poison=false,
        const std::shared_ptr<HotQueryCache::Binding>& binding={},bool collect=false) {
      Run out;auto ledger=std::make_shared<QueryMetadataBudget>();ScopedQueryMetadataBudget budget(ledger);
      if(precharge)ledger->observe_unique(precharge);
      if(poison)try{ledger->observe_unique(QueryMetadataBudget::maximum_bytes+1);}catch(const QueryMetadataBudgetExceeded&){}
      const auto sdk_before=iotdb_profile_snapshot();
      {ScopedQueryProfile profile(&out.profile);
        std::unique_ptr<ScopedQueryMetadataTrace> collector;
        if(collect)collector=std::make_unique<ScopedQueryMetadataTrace>("");
        auto result=backend->discover_datasets(req,[&](const auto&,const CatalogSelection& selection){
          check(bool(selection.read_context),"missing owner context");auto gate=selection.read_context->admit(principal);
          return gate.ok()?QueryStatus::success():gate.status();},binding);
        out.status=result.status();
        if(result.ok()) {
          out.context=result.value().read_context;check(bool(out.context),"successful result lacks context");
          auto final=out.context->admit(principal);out.status=final.ok()?QueryStatus::success():final.status();
          out.selected=result.value().selected_sources;out.candidates=result.value().candidate_datasets;out.catalog_bytes=result.value().catalog_read_bytes;
          for(const auto& m:result.value().datasets)out.matches.emplace_back(m.source.path,m.source.version,m.dataset_path);
        }
        if(collector)out.trace=collector->release();
      }
      out.sdk_queries=iotdb_profile_snapshot().query_calls-sdk_before.query_calls;
      out.bytes=ledger->consumed_bytes();out.ledger=ledger->status();return out;
    };
    auto observe_bounds=[&] {
      const auto s=backend->hot_query_cache_stats();pool_bounds(s);
      observations["peak_observed_resident_bytes"]=std::max(observations["peak_observed_resident_bytes"],s.resident_bytes);
      observations["peak_observed_domains"]=std::max(observations["peak_observed_domains"],s.domains);
      observations["peak_observed_proofs"]=std::max(observations["peak_observed_proofs"],s.proofs);
    };
    auto okay=[&](const Run& r,std::size_t matches,std::size_t sources) {
      check(r.status.ok()&&r.ledger.ok()&&r.matches.size()==matches&&r.selected==sources,"discovery result differs");
      check(r.context&&r.context->sources().size()==sources,"predicate miss lost from context");observe_bounds();
    };
    auto failure=[&](const Run& r,QueryError code){check(!r.status.ok()&&r.status.code()==code,"error code/priority differs");};
    stage="cold_warm";const auto cold=run(request,owner);okay(cold,2,2);const auto after_cold=backend->hot_query_cache_stats();
    const auto warm=run(request,owner);okay(warm,2,2);const auto after_warm=backend->hot_query_cache_stats();
    check(cold.matches==warm.matches&&cold.bytes==warm.bytes,"same-head cold/warm logical request fees or identities differ");
    check(after_warm.domain_hits>after_cold.domain_hits&&after_warm.proof_hits>after_cold.proof_hits,"hot domain/proof was not used");
    check(counter(cold.profile,"version_context.source_permission_checks")>0&&counter(warm.profile,"version_context.source_permission_checks")==0,
      "warm proof did not avoid repeated source permission checks");
    check(counter(warm.profile,"version_context.admissions")>=2,"warm operation skipped admission boundary");
    check(warm.sdk_queries==0,"warm complete discovery used actual SDK SQL");
    observations["cold_bytes"]=cold.bytes;observations["warm_bytes"]=warm.bytes;
    observations["cold_source_checks"]=counter(cold.profile,"version_context.source_permission_checks");
    observations["warm_source_checks"]=counter(warm.profile,"version_context.source_permission_checks");
    observations["warm_admissions"]=counter(warm.profile,"version_context.admissions");
    observations["warm_sdk_queries"]=warm.sdk_queries;
    observations["cold_matches"]=cold.matches.size();observations["warm_matches"]=warm.matches.size();
    observations["cold_selected_sources"]=cold.selected;observations["warm_selected_sources"]=warm.selected;
    observations["cold_catalog_bytes"]=cold.catalog_bytes;observations["warm_catalog_bytes"]=warm.catalog_bytes;
    observations["warm_domain_hit_delta"]=after_warm.domain_hits-after_cold.domain_hits;
    observations["warm_proof_hit_delta"]=after_warm.proof_hits-after_cold.proof_hits;
    cases.push_back("six_attribute_cold_warm_results_fees_and_admissions");
    stage="selector_binding_warm";
    const auto prepared_binding=backend->prepare_selector_binding();
    check(prepared_binding.ok()&&prepared_binding.value(),"selector prepare binding failed");
    const auto selector_binding=prepared_binding.value();
    const auto bound_first=run(request,owner,0,false,selector_binding);okay(bound_first,2,2);
    const auto bound_warm=run(request,owner,0,false,selector_binding);okay(bound_warm,2,2);
    check(bound_warm.matches==warm.matches&&bound_warm.bytes==warm.bytes,"bound warm results or fees differ");
    check(counter(bound_first.profile,"hot_query.selector_long_key_bytes")>0,"first binding did not resolve domain key");
    check(counter(bound_warm.profile,"hot_query.selector_short_ref_hits")==1&&
      counter(bound_warm.profile,"hot_query.selector_long_key_bytes")==0,"bound warm did not bypass long domain key");
    check(counter(bound_warm.profile,"version_context.admissions")>=2&&bound_warm.sdk_queries==0,
      "bound warm skipped admissions or made SDK query");
    auto alternate=request;alternate.predicate=full_predicate(851);
    const auto bound_alternate=run(alternate,owner,0,false,selector_binding);okay(bound_alternate,1,2);
    const auto unbound_alternate=run(alternate,owner);okay(unbound_alternate,1,2);
    check(bound_alternate.matches==unbound_alternate.matches&&bound_alternate.bytes==unbound_alternate.bytes,
      "binding froze old predicate or changed fees");
    alternate.predicate=full_predicate(852);const auto bound_empty=run(alternate,owner,0,false,selector_binding);okay(bound_empty,0,2);
    check(counter(bound_empty.profile,"hot_query.selector_short_ref_hits")==1,"empty match bypassed bound hot domain");
    observations["selector_warm_short_ref_hits"]=counter(bound_warm.profile,"hot_query.selector_short_ref_hits");
    observations["selector_warm_long_key_bytes"]=counter(bound_warm.profile,"hot_query.selector_long_key_bytes");
    observations["selector_warm_admissions"]=counter(bound_warm.profile,"version_context.admissions");
    observations["selector_warm_bytes"]=bound_warm.bytes;
    cases.push_back("selector_binding_same_head_short_reference_multiple_predicates_and_empty_matches");
    stage="selector_binding_immutable_template";
    auto changed_template=request;changed_template.sources.paths={{PathScopeKind::Exact,"/data/a.h5"}};
    failure(run(changed_template,owner,0,false,selector_binding),QueryError::InvalidQuery);
    changed_template=request;changed_template.dataset_path="/values_0";
    failure(run(changed_template,owner,0,false,selector_binding),QueryError::InvalidQuery);
    changed_template=request;changed_template.max_sources=1;
    failure(run(changed_template,owner,0,false,selector_binding),QueryError::InvalidQuery);
    changed_template=request;changed_template.sources.versions=VersionSelection::exact_version(10);
    failure(run(changed_template,owner,0,false,selector_binding),QueryError::InvalidQuery);
    okay(run(request,owner,0,false,selector_binding),2,2);
    cases.push_back("selector_binding_rejects_replaced_static_paths_dataset_scope_limit_and_versions");
    stage="selector_binding_budgets";
    for(int limit=0;limit<2;++limit) {
      auto limited=request;if(limit==0)limited.max_catalog_bytes=1;else limited.max_datasets=1;
      const auto normal=run(limited,owner);const auto bound=run(limited,owner,0,false,selector_binding);
      failure(normal,QueryError::ResourceExhausted);failure(bound,QueryError::ResourceExhausted);
      check(normal.bytes==bound.bytes&&normal.ledger.code()==bound.ledger.code(),"bound per-request limit changed fee/error semantics");
      failure(run(limited,denied,0,false,selector_binding),QueryError::PermissionDenied);
    }
    for(const auto remaining:std::array<std::uint64_t,3>{1,warm.bytes/2,warm.bytes}) {
      const auto precharge=QueryMetadataBudget::maximum_bytes-remaining;
      const auto normal=run(request,owner,precharge);const auto bound=run(request,owner,precharge,false,selector_binding);
      check(normal.status.code()==bound.status.code()&&normal.ledger.code()==bound.ledger.code()&&normal.bytes==bound.bytes&&
        normal.matches==bound.matches,"bound summary budget threshold differs from full selector");
    }
    failure(run(request,owner,0,true,selector_binding),QueryError::ResourceExhausted);
    const auto normal_collector=run(request,owner,0,false,{},true);
    const auto bound_collector=run(request,owner,0,false,selector_binding,true);
    okay(normal_collector,2,2);okay(bound_collector,2,2);
    check(!bound_collector.trace.empty()&&normal_collector.trace==bound_collector.trace&&normal_collector.bytes==bound_collector.bytes,
      "bound Required collector omitted fee identities");
    check(counter(bound_collector.profile,"hot_query.fee_summary_stages")==0,
      "bound collector improperly retained stage summary fast path");
    observations["selector_collector_identities"]=bound_collector.trace.size();
    cases.push_back("selector_binding_per_request_limits_summary_budget_thresholds_and_required_collector");
    stage="principals";const QueryPrincipal group1{1002,5000,{40077,40077,5000}},group2{1002,5000,{40077}},primary{1002,40077,{}};
    const auto p0=backend->hot_query_cache_stats();okay(run(request,group1),2,2);const auto p1=backend->hot_query_cache_stats();
    okay(run(request,group2),2,2);const auto p2=backend->hot_query_cache_stats();okay(run(request,primary),2,2);
    check(backend->hot_query_cache_stats().proof_builds>p2.proof_builds,"primary gid change reused another principal proof");
    check(p1.proof_builds>p0.proof_builds&&p2.proof_builds==p1.proof_builds&&p2.proof_hits>p1.proof_hits,"principal normalization did not share exact credential set");
    const auto denied_run=run(request,denied);failure(denied_run,QueryError::PermissionDenied);
    check(counter(denied_run.profile,"version_context.source_permission_checks")>0,"denied principal reused successful caller proof");
    observations["denied_bytes"]=denied_run.bytes;cases.push_back("uid_gid_and_canonical_supplementary_groups_isolation");
    stage="budget_priority";auto bounded=request;bounded.max_sources=1;failure(run(bounded,owner),QueryError::ResourceExhausted);
    bounded=request;bounded.max_datasets=1;failure(run(bounded,owner),QueryError::ResourceExhausted);
    bounded=request;bounded.max_catalog_bytes=1;failure(run(bounded,owner),QueryError::ResourceExhausted);
    failure(run(bounded,denied),QueryError::PermissionDenied);
    check(cold.bytes>denied_run.bytes+1,"fixture cannot separate authorization and catalog fees");
    const auto remaining=denied_run.bytes+(cold.bytes-denied_run.bytes)/2;
    auto threshold=run(request,denied,QueryMetadataBudget::maximum_bytes-remaining);
    failure(threshold,QueryError::PermissionDenied);check(threshold.ledger.ok(),"warm catalog fees charged before denied authorization");
    failure(run(request,owner,0,true),QueryError::ResourceExhausted);
    auto absent=request;absent.sources.paths={{PathScopeKind::Exact,"/data/absent.h5"}};failure(run(absent,owner,0,true),QueryError::ResourceExhausted);
    observations["authorization_catalog_threshold_remaining"]=remaining;
    cases.push_back("warm_limits_sticky_ledger_and_authorization_error_priority");
    stage="fallback";auto fallback=request;fallback.predicate=make_comparison({"hdf5.attr:level"},ComparisonOperator::Equal,LogicalValue::int64(850));
    auto generic=run(fallback,owner);okay(generic,2,2);check(generic.matches==cold.matches,"generic predicate fallback differs");
    auto missing=request;missing.predicate=make_comparison({"hdf5.attr:absent"},ComparisonOperator::Equal,LogicalValue::int64(0));okay(run(missing,owner),0,2);
    cases.push_back("unsupported_grammar_and_absent_attribute_null_fallback");
    stage="singleflight";mutate("/unrelated",1,[](Inode& i){++i.mtime;});
    const auto before_parallel=backend->hot_query_cache_stats();std::vector<Run> parallel(9);std::vector<std::thread> threads;
    std::vector<std::exception_ptr> parallel_errors(9);
    std::mutex start_mu;std::condition_variable ready_cv;int ready=0;bool start=false;
    for(int i=0;i<9;++i)threads.emplace_back([&,i]{
      {std::unique_lock<std::mutex> lock(start_mu);++ready;ready_cv.notify_all();ready_cv.wait(lock,[&]{return start;});}
      try {parallel[i]=run(request,owner,0,i==8);}catch(...){parallel_errors[i]=std::current_exception();}
    });
    {std::unique_lock<std::mutex> lock(start_mu);ready_cv.wait(lock,[&]{return ready==9;});start=true;}ready_cv.notify_all();
    for(auto& t:threads)t.join();for(const auto& e:parallel_errors)if(e)std::rethrow_exception(e);
    const auto after_parallel=backend->hot_query_cache_stats();
    for(std::size_t i=0;i<8;++i){const auto& r=parallel[i];okay(r,2,2);check(r.matches==cold.matches&&r.bytes==parallel.front().bytes,"concurrent callers shared ledger or changed result");}
    failure(parallel[8],QueryError::ResourceExhausted);
    check(parallel[8].ledger.code()==QueryError::ResourceExhausted,"poisoned concurrent request lost sticky failure");
    check(after_parallel.domain_builds-before_parallel.domain_builds==1,"same-key concurrent domain build was duplicated");
    observations["parallel_calls"]=8;observations["parallel_poisoned_calls"]=1;observations["parallel_domain_builds"]=after_parallel.domain_builds-before_parallel.domain_builds;
    observations["parallel_waits_observed"]=after_parallel.domain_waits-before_parallel.domain_waits;
    cases.push_back("concurrent_same_key_single_build_independent_request_ledgers");
    stage="proof_bound";std::vector<std::shared_ptr<const QueryReadContext>> pins;
    for(unsigned i=0;i<65;++i){const auto r=run(request,QueryPrincipal{2000+i,40077,{}});okay(r,2,2);pins.push_back(r.context);pool_bounds(backend->hot_query_cache_stats());}
    observations["proof_principals_exercised"]=65;cases.push_back("proof_reservations_bounded_with_pinned_contexts_and_fallback");
    stage="domain_bound";const auto before_domains=backend->hot_query_cache_stats();
    for(int i=0;i<18;++i){auto scoped=request;scoped.dataset_path="/absent_"+std::to_string(i);auto r=run(scoped,owner);okay(r,0,2);pins.push_back(r.context);}
    const auto after_domains=backend->hot_query_cache_stats();pool_bounds(after_domains);
    check(after_domains.opt_outs>before_domains.opt_outs||after_domains.evictions>before_domains.evictions,"fixed domain capacity not exercised");
    observations["domain_scopes_exercised"]=18;cases.push_back("domain_reservations_bounded_including_retired_pins");pins.clear();
    stage="source_revoke";mutate("/data/b.h5",11,[](Inode& i){i.mode=0;});
    auto revoked=cold.context->admit(owner);check(!revoked.ok()&&revoked.status().code()==QueryError::PermissionDenied,"predicate miss revocation bypassed");
    failure(run(request,owner),QueryError::PermissionDenied);
    failure(run(request,owner,0,false,selector_binding),QueryError::PermissionDenied);mutate("/data/b.h5",11,[](Inode& i){i.mode=0640;});
    stage="ancestor_revoke";mutate("/data",1,[](Inode& i){i.mode=0600;});
    auto ancestor=cold.context->admit(owner);check(!ancestor.ok()&&ancestor.status().code()==QueryError::PermissionDenied,"ancestor revocation bypassed");
    failure(run(request,owner),QueryError::PermissionDenied);
    failure(run(request,owner,0,false,selector_binding),QueryError::PermissionDenied);mutate("/data",1,[](Inode& i){i.mode=0755;});
    cases.push_back("head_change_rechecks_predicate_miss_and_current_ancestor_permissions");
    stage="new_latest";publish("/data/a.h5",20,2,850);
    check(cold.context->admit(owner).ok()&&cold.context->manifest({"/data/a.h5",10}).ok(),"old exact context lost stable source");
    auto other=cold.context->manifest({"/data/a.h5",20});check(!other.ok()&&other.status().code()==QueryError::StalePlan,"old context rebound Latest");
    auto latest=run(request,owner);okay(latest,2,2);for(const auto& m:latest.matches)check(std::get<1>(m)==20,"new discovery retained old Latest");
    const auto rebound_latest=run(request,owner,0,false,selector_binding);okay(rebound_latest,2,2);
    check(rebound_latest.matches==latest.matches&&rebound_latest.bytes==latest.bytes,"binding froze prepared Latest version");
    check(counter(rebound_latest.profile,"hot_query.selector_short_ref_miss_head")==1&&
      counter(rebound_latest.profile,"hot_query.selector_long_key_bytes")>0,"head change did not resolve a fresh resident domain");
    check(bound_warm.context->sources().size()==2&&bound_warm.context->manifest({"/data/a.h5",10}).ok()&&
      !bound_warm.context->manifest({"/data/a.h5",20}).ok(),"binding rewrote frozen execution context");
    cases.push_back("selector_binding_latest_rebind_preserves_prior_frozen_membership");
    publish("/data/c.h5",21,1,852);auto phantom=run(request,owner);okay(phantom,2,3);
    check(latest.context->sources().size()==2,"old context acquired phantom source");
    const auto bound_phantom=run(request,owner,0,false,selector_binding);okay(bound_phantom,2,3);
    check(bound_phantom.matches==phantom.matches,"binding missed newly published logical source");
    mutate("/data/c.h5",21,[](Inode& i){i.mode=0;});failure(run(request,owner),QueryError::PermissionDenied);
    failure(run(request,owner,0,false,selector_binding),QueryError::PermissionDenied);
    auto phantom_revoke=phantom.context->admit(owner);check(!phantom_revoke.ok()&&phantom_revoke.status().code()==QueryError::PermissionDenied,"new predicate-miss phantom escaped authorization");
    mutate("/data/c.h5",21,[](Inode& i){i.mode=0640;});cases.push_back("latest_new_version_and_new_logical_source_preserve_frozen_membership");
    stage="delete";check(backend->remove_path("/data/b.h5",11),"exact deletion failed");
    auto deleted=cold.context->admit(owner);check(!deleted.ok()&&deleted.status().code()==QueryError::StalePlan,"deleted predicate miss admitted");
    auto bound_deleted=bound_warm.context->admit(owner);
    check(!bound_deleted.ok()&&bound_deleted.status().code()==QueryError::StalePlan,"bound frozen deleted source admitted");
    const auto after_delete=run(request,owner,0,false,selector_binding);okay(after_delete,2,2);
    cases.push_back("deleted_frozen_predicate_miss_rejects_admission");
    cases.push_back("selector_binding_source_and_ancestor_revocation_phantoms_and_deletion");
    stage="attribute_state";publish("/generic/missing.h5",30,1,850,1);publish("/generic/unsupported.h5",31,1,850,2);
    auto schema=request;schema.sources.paths={{PathScopeKind::Exact,"/generic/missing.h5"}};okay(run(schema,owner),0,1);
    schema.sources.paths={{PathScopeKind::Exact,"/generic/unsupported.h5"}};failure(run(schema,owner),QueryError::UnsupportedCapability);
    cases.push_back("missing_required_attribute_null_and_present_unsupported_attribute_error");
    stage="selector_binding_other_owner";
    auto peer_backend=std::make_shared<IoTDBStructuredBlobBackend>();
    auto peer_settings=settings;peer_settings.table_prefix=prefix+"_peer";peer_settings.owner_directory=journal+"-peer";
    check(::access(peer_settings.owner_directory.c_str(),F_OK)!=0,"peer journal exists; no resume");
    check(peer_backend->configure_version_directory(peer_settings),"peer owner initialization failed");
    check(peer_backend->hot_query_cache()!=backend->hot_query_cache(),"peer backend unexpectedly shares hot cache");
    auto cross_owner=peer_backend->discover_datasets(request,[](const auto&,const CatalogSelection&){return QueryStatus::success();},selector_binding);
    check(!cross_owner.ok()&&cross_owner.status().code()==QueryError::StalePlan,"binding crossed owner/cache boundary");
    auto no_hot=request;no_hot.predicate=make_comparison({"hdf5.attr:level"},ComparisonOperator::Equal,LogicalValue::int64(850));
    auto cross_fallback=peer_backend->discover_datasets(no_hot,[](const auto&,const CatalogSelection&){return QueryStatus::success();},selector_binding);
    check(!cross_fallback.ok()&&cross_fallback.status().code()==QueryError::StalePlan,"generic fallback bypassed binding incarnation check");
    cases.push_back("selector_binding_cross_backend_cache_and_namespace_rejects_stale_plan");
    stage="final_head";const auto bytes=store.read_head();check(bool(bytes),"final head absent");const auto head=decode_head(*bytes);
    const auto stats=backend->hot_query_cache_stats();pool_bounds(stats);
    std::cout<<"{\"completed\":true,\"command\":\"global_hot_cache\",\"prefix\":"<<quoted(prefix)<<",\"journal\":"<<quoted(journal)
      <<",\"actual_backend\":true,\"real_hdf5_fixture\":true,\"existing_database\":true,\"six_attribute_hot_grammar\":true"
      <<",\"case_count\":"<<cases.size()<<",\"cases\":[";
    for(std::size_t i=0;i<cases.size();++i)std::cout<<(i?",":"")<<quoted(cases[i]);std::cout<<"],\"observations\":{";bool first=true;
    for(const auto& p:observations){std::cout<<(first?"":",")<<quoted(p.first)<<':'<<p.second;first=false;}
    std::cout<<"},\"final_stats\":"<<stats_json(stats)<<",\"final_head_sequence\":"<<head.sequence
      <<",\"history_gap_commits\":0,\"history_gap_and_owner_frozen_scope\":\"not injected by this short real-backend probe; separate owner/unit and preserved v03 probe evidence required\""
      <<",\"singleflight_scope\":\"eight concurrent actual backend calls, exactly one build; no guarantee that scheduler creates a waiter\""
      <<",\"fixtures_preserved\":true,\"service_restarted\":false}\n";return 0;
  }catch(const ProbeFailure& e){std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()<<",\"error\":"<<quoted(e.what())<<"}\n";return 1;}
  catch(const std::exception&){std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()<<",\"error\":\"probe failed; inspect private native log; no automatic retry\"}\n";return 1;}
}
