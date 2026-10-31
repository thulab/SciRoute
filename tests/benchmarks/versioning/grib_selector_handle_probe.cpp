// Real GRIB selector-handle integration. Fresh vd_grib_handle_* prefixes only;
// existing controlled database, preserved fixtures, no mounts/service control.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include "iotdbfs/formats/grib/GribFormatQueryAdapter.h"
#include "iotdbfs/formats/grib/GribQueryModel.h"
#include "iotdbfs/query/core/QueryModule.h"
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/core/QueryMetadataBudget.h"
#include <eccodes.h>
#include <algorithm>
#include <array>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <tuple>
#include <unistd.h>
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
Inode inode(const std::string& path,long long version,bool directory) {
  Inode i;i.path=path;i.parent=path_utils::parent_of(path);i.name=path_utils::base(path);
  i.row_time=version;i.is_dir=directory;i.mode=directory?0755:0640;i.uid=1001;i.gid=40077;i.metadata="{}";
  i.ctime=i.mtime=i.atime=1700000000;return i;
}
std::uint64_t counter(const QueryProfileRecord& p,const std::string& name) {
  const auto found=p.counters.find(name);return found==p.counters.end()?0:found->second;
}
Bytes grib_file(long level) {
  std::unique_ptr<codes_handle,decltype(&codes_handle_delete)> handle(
    codes_grib_handle_new_from_samples(nullptr,"regular_ll_sfc_grib2"),codes_handle_delete);
  check(bool(handle),"ecCodes sample unavailable");
  auto number=[&](const char* key,long value){check(codes_set_long(handle.get(),key,value)==0,"GRIB fixture integer failed");};
  auto real=[&](const char* key,double value){check(codes_set_double(handle.get(),key,value)==0,"GRIB fixture coordinate failed");};
  number("Ni",2);number("Nj",2);number("paramId",130);
  const std::string kind="isobaricInhPa";std::size_t length=kind.size();
  check(codes_set_string(handle.get(),"typeOfLevel",kind.c_str(),&length)==0,"GRIB fixture level type failed");
  number("level",level);number("step",0);number("dataDate",20260101);number("dataTime",0);
  real("latitudeOfFirstGridPointInDegrees",1);real("latitudeOfLastGridPointInDegrees",0);
  real("longitudeOfFirstGridPointInDegrees",0);real("longitudeOfLastGridPointInDegrees",1);
  real("iDirectionIncrementInDegrees",1);real("jDirectionIncrementInDegrees",1);
  const double values[]{1,2,3,4};check(codes_set_double_array(handle.get(),"values",values,4)==0,"GRIB fixture values failed");
  const void* data=nullptr;std::size_t size=0;check(codes_get_message(handle.get(),&data,&size)==0&&data&&size,"GRIB fixture encoding failed");
  const auto* bytes=static_cast<const std::uint8_t*>(data);return Bytes(bytes,bytes+size);
}
QueryFragment fragment(std::int64_t level=850) {
  using namespace grib_query_fields;
  QueryFragment q;q.projection={{kOffset}};
  q.predicate=make_logical(LogicalOperator::And,{
    make_comparison({kShortName},ComparisonOperator::Equal,LogicalValue::utf8("t")),
    make_comparison({kTypeOfLevel},ComparisonOperator::Equal,LogicalValue::utf8("isobaricInhPa")),
    make_comparison({kLevel},ComparisonOperator::Equal,LogicalValue::int64(level)),
    make_comparison({kStep},ComparisonOperator::Equal,LogicalValue::int64(0)),
    make_comparison({kValidityDate},ComparisonOperator::Equal,LogicalValue::int64(20260101)),
    make_comparison({kValidityTime},ComparisonOperator::Equal,LogicalValue::int64(0))});
  return q;
}
struct Run {
  QueryStatus status,ledger;
  CatalogSelection selection;
  QueryProfileRecord profile;
  QueryMetadataTrace trace;
  std::uint64_t bytes{0};
  QueryMetadataStageSummaryStats summary;
  std::vector<std::tuple<std::string,long long,std::uint64_t,std::string,std::uint64_t,std::uint64_t>> units;
};
void bounds(const HotQueryCacheStats& s) {
  check(s.resident_bytes<=HotQueryCache::maximum_bytes&&s.domains<=HotQueryCache::maximum_domains&&
    s.proofs<=HotQueryCache::maximum_proofs&&s.handles<=HotQueryCache::maximum_handles&&
    s.handle_bytes<=HotQueryCache::maximum_handle_bytes,"hot pool exceeded fixed bounds");
}
}
int main(int argc,char** argv) {
  set_logging_level(LogLevel::OFF);std::string stage="arguments";std::vector<std::string> cases;
  std::map<std::string,std::uint64_t> observations;
  try {
    std::string prefix,journal;
    for(int i=1;i<argc;++i){const std::string arg=argv[i];check(i+1<argc,"missing argument value");
      if(arg=="--prefix")prefix=argv[++i];else if(arg=="--journal")journal=argv[++i];else throw ProbeFailure("unknown argument");}
    check(prefix.rfind("vd_grib_handle_",0)==0&&!journal.empty()&&journal.front()=='/',"fresh vd_grib_handle prefix and absolute journal required");
    check(::access(journal.c_str(),F_OK)!=0&&::access((journal+"-peer").c_str(),F_OK)!=0,"journal exists; no resume");
    stage="configuration";IoTDBClientConfig config;
    config.host=env("IOTDB_HOST");config.user=env("IOTDB_USER");config.pass=env("IOTDB_PASS");config.db=env("IOTDB_DB");
    const auto port=env("IOTDB_PORT");std::size_t used=0;config.port=std::stoi(port,&used);
    check(used==port.size()&&config.port==33667,"controlled original RPC port required");
    check(config.host=="127.0.0.1"||config.host=="localhost","controlled loopback required");config.host="127.0.0.1";
    check(config.db=="h09probe1790939889dbfc1f","existing experiment database required");config.auto_create_db=false;
    IoTDBClient existing_database(config,1);
    IoTDBConnector::instance().initialize({config.host,config.port,config.user,config.pass,config.db},2);
    auto backend=std::make_shared<IoTDBStructuredBlobBackend>();
    GribFormatAdapter::Config grib_config;grib_config.ingestion.detect_threshold_bytes=16;
    auto grib=std::make_shared<GribFormatAdapter>(grib_config);
    FormatRegistry formats;check(formats.register_adapter(grib),"GRIB registration failed");
    VersionDirectoryConfig settings;settings.table_prefix=prefix;settings.owner_directory=journal;settings.initialize=true;
    settings.artifact_validator=[grib](const std::string& format,const auto& layout,std::uint64_t size) {
      if(format!="grib")return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,"unexpected fixture format");
      const auto status=grib->validate_layout_artifact(layout,size);
      return status.ok()?StructuredBlobStatus::success():StructuredBlobStatus::failure(StructuredBlobError::InvalidState,status.error());
    };
    stage="initialize";check(backend->configure_version_directory(settings),"owner initialization failed");check(backend->ensure_schema(),"schema failed");
    for(const auto& path:{"/","/data","/unrelated"})check(backend->upsert_inode(inode(path,1,true),true),"directory creation failed");
    auto storage=std::make_shared<StructuredBlobStorage>(backend);IngestionModule ingestion(*storage,formats,{});
    auto publish=[&](const std::string& path,long long version,const Bytes& bytes) {
      FormatSource source{bytes.size(),[&](std::uint64_t offset,std::uint64_t size)->std::optional<Bytes>{
        if(offset>bytes.size()||size>bytes.size()-offset)return {};return Bytes(bytes.begin()+offset,bytes.begin()+offset+size);}};
      const auto ingested=ingestion.ingest({path,version},source,[&](const IngestionOutcome& outcome){
        check(outcome.format_id==std::optional<std::string>("grib"),"ingestion selected wrong adapter");
        auto node=inode(path,version,false);node.size=outcome.payload_size;node.format_id="grib";
        const auto metadata=serialize_metadata(outcome.metadata,65536);check(metadata.ok(),"metadata encoding failed");node.metadata=metadata.value();
        return backend->upsert_inode(node,false);
      });check(ingested.ok(),"actual GRIB ingestion failed");
    };
    auto mutate=[&](const std::string& path,long long version,const std::function<void(Inode&)>& edit) {
      auto node=backend->load_inode(path,version);check(bool(node),"mutation inode absent");
      if(!node->is_dir){auto manifest=backend->load_manifest({path,version});check(manifest.ok(),"mutation manifest absent");
        const auto text=serialize_metadata(manifest.value().metadata,65536);check(text.ok(),"mutation metadata failed");node->metadata=text.value();}
      edit(*node);check(backend->upsert_inode(*node,node->is_dir),"POSIX mutation failed");
    };
    stage="fixtures";auto two=grib_file(850);const auto one=two;two.insert(two.end(),one.begin(),one.end());
    publish("/data/a.grib",10,two);publish("/data/b.grib",11,grib_file(851));
    auto module=QueryModule::create(make_iotdb_published_version_catalog(backend),storage,{std::make_shared<GribFormatQueryAdapter>()});
    check(module.ok(),"query module creation failed");
    QueryRequest request;request.sources.paths={{PathScopeKind::Subtree,"/data"}};
    request.sources.versions=VersionSelection::latest();request.sources.formats={AdapterSelectionKind::Listed,{"grib"},false};
    request.limits.max_sources=8;request.limits.max_result_rows=32;request.subset=fragment();
    const QueryPrincipal owner{1001,40077,{}},denied{1003,5000,{}};
    auto run=[&](const QueryRequest& req,const QueryPrincipal& principal,const std::shared_ptr<HotQueryCache::Binding>& binding={},
                 std::uint64_t precharge=0,bool collect=false,bool poison=false,std::uint64_t response_bytes=1024*1024,bool late_collect=false) {
      Run out;auto ledger=std::make_shared<QueryMetadataBudget>();ScopedQueryMetadataBudget metadata(ledger);
      if(precharge)ledger->observe_unique(precharge);
      if(poison)try{ledger->observe_unique(QueryMetadataBudget::maximum_bytes+1);}catch(const QueryMetadataBudgetExceeded&){}
      ScopedQueryProfile profile(&out.profile);std::unique_ptr<ScopedQueryMetadataTrace> collector;
      if(collect&&!late_collect)collector=std::make_unique<ScopedQueryMetadataTrace>("");
      auto perform=[&]()->QueryStatus {
        auto valid=module.value()->validate_format_catalog_request(req);if(!valid.ok())return valid;
        auto selected=backend->scan_published_versions(req.sources,req.limits.max_sources,binding);
        if(!selected.ok())return selected.status();out.selection=std::move(selected.value());
        check(bool(out.selection.read_context),"selected owner context missing");
        if(collect&&late_collect)collector=std::make_unique<ScopedQueryMetadataTrace>("");
        auto authorized=out.selection.read_context->admit(principal);if(!authorized.ok())return authorized.status();
        auto admitted=out.selection.read_context->admit(principal);if(!admitted.ok())return admitted.status();
        ScopedQueryExecutionLease execution(admitted.value()->execution_lease());
        auto prepared=module.value()->prepare(req,&out.selection);if(!prepared.ok())return prepared.status();
        auto found=module.value()->format_catalog(prepared.value(),response_bytes);if(!found.ok())return found.status();
        for(const auto& unit:found.value().units)out.units.emplace_back(unit.source.path,unit.source.version,unit.revision,
          unit.unit.unit_id,unit.unit.offset,unit.unit.length);
        auto final=out.selection.read_context->admit(principal);return final.ok()?QueryStatus::success():final.status();
      };
      try {out.status=perform();}catch(const QueryMetadataBudgetExceeded&){out.status=ledger->status();}
      if(collector)out.trace=collector->release();out.bytes=ledger->consumed_bytes();out.ledger=ledger->status();
      out.summary=ledger->stage_summary_stats();
      bounds(backend->hot_query_cache_stats());return out;
    };
    auto okay=[&](const Run& result,std::size_t units,std::size_t sources) {
      const auto context_sources=result.selection.read_context?result.selection.read_context->sources().size():0;
      const bool matches=result.status.ok()&&result.ledger.ok()&&result.units.size()==units&&
        result.selection.versions().size()==sources&&result.selection.read_context&&context_sources==sources;
      if(!matches) {
        // Only fixed numeric diagnostics. Never emit status.message(), paths,
        // connector settings, payload values or other variable source data.
        observations["failure_status_code"]=static_cast<std::uint64_t>(result.status.code());
        observations["failure_ledger_code"]=static_cast<std::uint64_t>(result.ledger.code());
        observations["failure_units"]=result.units.size();
        observations["failure_expected_units"]=units;
        observations["failure_sources"]=result.selection.versions().size();
        observations["failure_expected_sources"]=sources;
        observations["failure_context_sources"]=context_sources;
        observations["failure_context_present"]=result.selection.read_context?1:0;
        observations["failure_metadata_bytes"]=result.bytes;
      }
      check(matches,"query result/frozen scope differs");
    };
    auto failure=[](const Run& result,QueryError error){check(!result.status.ok()&&result.status.code()==error,"error code differs");};
    stage="cold_warm";const auto cold=run(request,owner);okay(cold,2,2);const auto normal=run(request,owner);okay(normal,2,2);
    check(cold.units==normal.units,"cold/warm exact unit locators differ");
    const auto prepared_binding=backend->prepare_selector_binding();check(prepared_binding.ok(),"binding prepare failed");
    const auto binding=prepared_binding.value();const auto first=run(request,owner,binding);okay(first,2,2);
    const auto warm=run(request,owner,binding);okay(warm,2,2);
    check(warm.units==normal.units&&warm.bytes==normal.bytes,"bound warm results or logical fees differ");
    check(counter(first.profile,"hot_query.selector_long_key_bytes")>0&&counter(warm.profile,"hot_query.selector_short_ref_hits")==1&&
      counter(warm.profile,"hot_query.selector_long_key_bytes")==0,"warm candidate binding did not bypass long key");
    check(counter(warm.profile,"hot_query.candidate_domain_hits")==1&&counter(warm.profile,"hot_query.adapter_domain_hits")==1&&
      counter(warm.profile,"version_context.admissions")>=3,"bound query skipped domain/index/admission boundaries");
    observations["warm_bytes"]=warm.bytes;observations["warm_short_ref_hits"]=counter(warm.profile,"hot_query.selector_short_ref_hits");
    observations["warm_long_key_bytes"]=counter(warm.profile,"hot_query.selector_long_key_bytes");
    observations["warm_candidate_fee_logical_entries"]=counter(warm.profile,"hot_query.candidate_fee_logical_entries");
    observations["warm_candidate_fee_entries_visited"]=counter(warm.profile,"hot_query.candidate_fee_entries_visited");
    check(observations["warm_candidate_fee_logical_entries"]>0&&observations["warm_candidate_fee_entries_visited"]==0&&
      counter(warm.profile,"hot_query.candidate_fee_entries_skipped")==observations["warm_candidate_fee_logical_entries"]&&
      counter(warm.profile,"hot_query.candidate_fee_summary_stages")==1,"candidate selection did not use exact stage summary");
    check(counter(warm.profile,"hot_query.adapter_fee_entries_visited")==0&&
      counter(warm.profile,"hot_query.adapter_fee_summary_stages")==1&&warm.summary.successful_stages==3&&
      warm.summary.full_fee_entries_visited==0&&warm.summary.memo_hits>=2,"GRIB combined summary retained a full immutable replay");
    observations["warm_total_summary_stages"]=warm.summary.successful_stages;
    observations["warm_total_fee_entries_visited"]=warm.summary.full_fee_entries_visited;
    observations["warm_total_fee_entries_skipped"]=warm.summary.borrowed_fee_entries_skipped;
    observations["warm_total_fee_memo_hits"]=warm.summary.memo_hits;
    cases.push_back("same_head_candidate_short_reference_exact_units_fees_and_three_admissions");
    stage="predicates";auto changed=request;changed.subset=fragment(851);const auto alternate=run(changed,owner,binding);okay(alternate,1,2);
    const auto alternate_normal=run(changed,owner);okay(alternate_normal,1,2);
    check(alternate.units==alternate_normal.units&&alternate.bytes==alternate_normal.bytes,"binding froze predicate or changed alternate fees");
    changed.subset=fragment(852);okay(run(changed,owner,binding),0,2);
    cases.push_back("one_binding_multiple_predicates_and_empty_match_keeps_full_authorization_scope");
    stage="principal_summaries";const QueryPrincipal group{1002,5000,{40077}};
    okay(run(request,group,binding),2,2);const auto group_warm=run(request,group,binding);okay(group_warm,2,2);
    const auto group_reference=run(request,group,{},0,true);okay(group_reference,2,2);
    check(group_warm.units==group_reference.units&&group_warm.bytes==group_reference.bytes&&
      group_warm.summary.successful_stages==3&&group_warm.summary.full_fee_entries_visited==0,
      "summary borrowed another principal proof or changed logical fees");
    observations["group_summary_stages"]=group_warm.summary.successful_stages;
    cases.push_back("summary_rearms_actual_principal_after_representative_selection_plan");

    stage="template_identity";changed=request;changed.sources.paths={{PathScopeKind::Exact,"/data/a.grib"}};
    failure(run(changed,owner,binding),QueryError::InvalidQuery);changed=request;changed.limits.max_sources=7;
    failure(run(changed,owner,binding),QueryError::InvalidQuery);changed=request;changed.sources.versions=VersionSelection::exact_version(10);
    failure(run(changed,owner,binding),QueryError::InvalidQuery);
    DatasetCatalogRequest hdf;hdf.sources=request.sources;
    const auto wrong_kind=backend->discover_datasets(hdf,[](const auto&,const CatalogSelection&){return QueryStatus::success();},binding);
    check(!wrong_kind.ok()&&wrong_kind.status().code()==QueryError::InvalidQuery,"GRIB binding crossed HDF5 payload type");
    cases.push_back("static_template_and_cross_payload_kind_misuse_fail_closed");
    stage="limits";changed=request;changed.limits.max_result_rows=1;
    const auto limited_normal=run(changed,owner),limited_bound=run(changed,owner,binding);
    failure(limited_normal,QueryError::ResourceExhausted);failure(limited_bound,QueryError::ResourceExhausted);
    check(limited_normal.bytes==limited_bound.bytes,"bound result limit altered fees");
    failure(run(request,owner,binding,0,false,false,64),QueryError::ResourceExhausted);
    failure(run(request,denied,binding),QueryError::PermissionDenied);
    for(const auto remaining:std::array<std::uint64_t,3>{1,normal.bytes/2,normal.bytes}) {
      const auto precharge=QueryMetadataBudget::maximum_bytes-remaining;
      const auto a=run(request,owner,{},precharge),b=run(request,owner,binding,precharge);
      const auto reference=run(request,owner,{},precharge,true);
      check(a.status.code()==b.status.code()&&a.ledger.code()==b.ledger.code()&&a.bytes==b.bytes&&
        b.status.code()==reference.status.code()&&b.status.phase()==reference.status.phase()&&
        b.status.message()==reference.status.message()&&b.ledger.code()==reference.ledger.code()&&b.bytes==reference.bytes,
        "summary threshold changed replay prefix or first error relative to full collector replay");
    }
    failure(run(request,owner,binding,0,false,true),QueryError::ResourceExhausted);
    cases.push_back("per_request_result_response_and_metadata_budgets_preserve_failure_order");
    stage="collector";const auto collected_normal=run(request,owner,{},0,true),collected_bound=run(request,owner,binding,0,true);
    okay(collected_normal,2,2);okay(collected_bound,2,2);
    auto canonical_trace=[](const QueryMetadataTrace& trace) {
      QueryMetadataTrace stable;std::vector<std::uint64_t> explicit_demands;
      for(const auto& fee:trace) {
        if(fee.first.rfind("unique-demand/",0)==0)explicit_demands.push_back(fee.second);
        else stable.emplace(fee);
      }
      std::sort(explicit_demands.begin(),explicit_demands.end());
      return std::make_pair(std::move(stable),std::move(explicit_demands));
    };
    // Required collectors assign fresh per-operation identities to explicit
    // demand. Preserve their exact count/amounts while comparing every stable
    // control-object identity byte-for-byte.
    check(!collected_bound.trace.empty()&&canonical_trace(collected_normal.trace)==canonical_trace(collected_bound.trace)&&
      collected_normal.bytes==collected_bound.bytes,"bound collector omitted or renamed stable fee identities or explicit demand");
    check(counter(collected_bound.profile,"hot_query.candidate_fee_collector_untracked")==1,"collector replay instrumentation misreported tracking");
    observations["collector_fee_identities"]=collected_bound.trace.size();
    cases.push_back("required_collector_observes_original_fee_identities");
    check(collected_bound.summary.successful_stages==0&&
      counter(collected_bound.profile,"hot_query.candidate_fee_collector_untracked")==1,
      "Required collector retained the stage summary fast path");
    stage="late_collector";
    const auto late_normal=run(request,owner,{},0,true,false,1024*1024,true);
    const auto late_bound=run(request,owner,binding,0,true,false,1024*1024,true);
    okay(late_normal,2,2);okay(late_bound,2,2);
    check(late_bound.summary.successful_stages==1&&late_bound.summary.collector_exits==1&&
      late_bound.bytes==normal.bytes&&late_normal.bytes==late_bound.bytes&&
      canonical_trace(late_normal.trace)==canonical_trace(late_bound.trace),
      "late collector failed to retain exact charged prefix and complete later trace");
    observations["late_collector_completed_summary_stages"]=late_bound.summary.successful_stages;
    observations["late_collector_exits"]=late_bound.summary.collector_exits;
    cases.push_back("collector_installed_after_selection_exits_summary_without_losing_charged_prefix");
    stage="summary_quota_refusal";const QueryPrincipal quota_principal{1004,40077,{}};
    {
      ScopedQueryMetadataBudget metadata;
      auto admission=warm.selection.read_context->admit(quota_principal);
      check(admission.ok(),"quota-test principal proof creation failed");
    }
    auto pool=backend->hot_query_cache();const auto free_bytes=HotQueryCache::maximum_bytes-pool->stats().resident_bytes;
    auto filler=pool->reserve(HotQueryCache::Kind::Auxiliary,free_bytes);check(bool(filler),"quota-test filler reservation failed");
    const auto refused=run(request,quota_principal,binding);okay(refused,2,2);
    check(counter(refused.profile,"hot_query.fee_summary_opt_outs")>0&&refused.summary.full_fee_entries_visited>0,
      "pool exhaustion did not decline optional summary compilation");
    filler.reset();const auto refused_reference=run(request,quota_principal,{},0,true);okay(refused_reference,2,2);
    check(refused.units==refused_reference.units&&refused.bytes==refused_reference.bytes,
      "summary refusal changed result or logical fee total");
    const auto refused_again=run(request,quota_principal,binding);okay(refused_again,2,2);
    check(counter(refused_again.profile,"hot_query.fee_summary_build_attempts")==0,
      "one refused immutable proof summary retried without a new domain");
    observations["summary_quota_refusal_visited_entries"]=refused.summary.full_fee_entries_visited;
    cases.push_back("shared_pool_summary_refusal_preserves_replay_and_does_not_retry");
    stage="collector_deferred_build";mutate("/unrelated",1,[](Inode& node){++node.mtime;});
    const auto collector_first=run(request,owner,binding,0,true);okay(collector_first,2,2);
    check(collector_first.summary.successful_stages==0,"first collector request unexpectedly used summary");
    okay(run(request,owner,binding),2,2);const auto after_defer=run(request,owner,binding);okay(after_defer,2,2);
    check(after_defer.summary.successful_stages==3&&after_defer.summary.full_fee_entries_visited==0,
      "collector-specific deferral permanently disabled later ordinary summaries");
    observations["collector_deferred_then_warm_stages"]=after_defer.summary.successful_stages;
    cases.push_back("collector_first_publication_defers_summary_and_later_request_can_compile");

    stage="source_revoke";mutate("/data/b.grib",11,[](Inode& node){node.mode=0;});
    failure(run(request,owner,binding),QueryError::PermissionDenied);
    const auto old_gate=warm.selection.read_context->admit(owner);check(!old_gate.ok()&&old_gate.status().code()==QueryError::PermissionDenied,"old full-candidate proof ignored predicate-miss revocation");
    mutate("/data/b.grib",11,[](Inode& node){node.mode=0640;});
    mutate("/data",1,[](Inode& node){node.mode=0600;});failure(run(request,owner,binding),QueryError::PermissionDenied);
    mutate("/data",1,[](Inode& node){node.mode=0755;});
    cases.push_back("source_predicate_miss_and_current_ancestor_revocation_are_rechecked");
    stage="latest";publish("/data/a.grib",20,two);const auto latest=run(request,owner,binding);okay(latest,2,2);
    for(const auto& unit:latest.units)check(std::get<1>(unit)==20,"bound Latest retained old version");
    check(counter(latest.profile,"hot_query.selector_short_ref_miss_head")==1&&counter(latest.profile,"hot_query.selector_long_key_bytes")>0,
      "Latest did not rebind current head");
    check(warm.selection.read_context->manifest({"/data/a.grib",10}).ok()&&!warm.selection.read_context->manifest({"/data/a.grib",20}).ok(),
      "old frozen context rebound Latest");
    publish("/data/c.grib",21,grib_file(852));const auto phantom=run(request,owner,binding);okay(phantom,2,3);
    check(latest.selection.read_context->sources().size()==2,"new file entered old frozen selection");
    mutate("/data/c.grib",21,[](Inode& node){node.mode=0;});failure(run(request,owner,binding),QueryError::PermissionDenied);
    mutate("/data/c.grib",21,[](Inode& node){node.mode=0640;});
    cases.push_back("latest_and_phantom_rebind_preserve_old_execution_membership");
    stage="deletion";check(backend->remove_path("/data/b.grib",11),"exact deletion failed");
    auto removed=warm.selection.read_context->admit(owner);check(!removed.ok()&&removed.status().code()==QueryError::StalePlan,"deleted frozen predicate miss admitted");
    okay(run(request,owner,binding),2,2);
    cases.push_back("deleted_frozen_source_rejects_old_admission_new_binding_selects_current_scope");
    stage="other_backend";auto peer=std::make_shared<IoTDBStructuredBlobBackend>();auto peer_settings=settings;
    peer_settings.table_prefix=prefix+"_peer";peer_settings.owner_directory=journal+"-peer";
    check(peer->configure_version_directory(peer_settings),"peer owner initialization failed");
    auto cross=peer->scan_published_versions(request.sources,request.limits.max_sources,binding);
    check(!cross.ok()&&cross.status().code()==QueryError::StalePlan,"binding crossed backend/cache/namespace");
    auto unsupported=request.sources;unsupported.formats.include_byte_passthrough=true;
    cross=peer->scan_published_versions(unsupported,request.limits.max_sources,binding);
    check(!cross.ok()&&cross.status().code()==QueryError::StalePlan,"fallback bypassed stale binding check");
    cases.push_back("cross_backend_cache_namespace_and_fallback_reject_stale_binding");
    stage="final";const auto stats=backend->hot_query_cache_stats();bounds(stats);
    std::cout<<"{\"complete\":true,\"completed\":true,\"command\":\"grib_selector_handle\",\"prefix\":"<<quoted(prefix)<<",\"journal\":"<<quoted(journal)
      <<",\"actual_backend\":true,\"real_grib_fixture\":true,\"existing_database\":true,\"case_count\":"<<cases.size()<<",\"cases\":[";
    for(std::size_t i=0;i<cases.size();++i)std::cout<<(i?",":"")<<quoted(cases[i]);std::cout<<"],\"observations\":{";bool first_observation=true;
    for(const auto& entry:observations){std::cout<<(first_observation?"":",")<<quoted(entry.first)<<':'<<entry.second;first_observation=false;}
    std::cout<<"},\"final_resident_bytes\":"<<stats.resident_bytes<<",\"fixtures_preserved\":true,\"service_restarted\":false}\n";return 0;
  }catch(const ProbeFailure& error){
    std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()
      <<",\"error\":"<<quoted(error.what())<<",\"observations\":{";bool first_diagnostic=true;
    for(const auto& entry:observations){std::cout<<(first_diagnostic?"":",")<<quoted(entry.first)<<':'<<entry.second;first_diagnostic=false;}
    std::cout<<"}}\n";return 1;
  }
  catch(const std::exception&){std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()<<",\"error\":\"probe failed; inspect private native log; no retry\"}\n";return 1;}
}
