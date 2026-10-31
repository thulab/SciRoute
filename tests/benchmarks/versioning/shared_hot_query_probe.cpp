// Real owner-backend cross-format candidate domains. Fresh vd_shared_hot_*
// prefix in the existing database only. No service control or cleanup.
// Actual small GRIB/HDF5 ingestion; this checks domain/proof semantics, not decoders.
#include "iotdbfs/IoTDBConnector.h"
#include "iotdbfs/IoTDBStructuredBlobBackend.h"
#include "iotdbfs/IoTDBProfile.h"
#include "iotdbfs/Ingestion.h"
#include "iotdbfs/MetadataSerialization.h"
#include "iotdbfs/PathUtils.h"
#include "iotdbfs/formats/hdf5/Hdf5FormatAdapter.h"
#include "iotdbfs/formats/grib/GribFormatAdapter.h"
#include <eccodes.h>
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
QueryExpressionPtr full_predicate() {
  std::vector<QueryExpressionPtr> fields;
  for(const auto& pair:std::vector<std::pair<std::string,std::string>>{{"shortName","t"},{"typeOfLevel","isobaricInhPa"}})
    fields.push_back(make_comparison({"hdf5.attr:"+pair.first},ComparisonOperator::Equal,LogicalValue::utf8(pair.second)));
  for(const auto& pair:std::vector<std::pair<std::string,std::int64_t>>{{"level",850},{"step",0},{"validityDate",20260101},{"validityTime",0}})
    fields.push_back(make_comparison({"hdf5.attr:"+pair.first},ComparisonOperator::Equal,LogicalValue::int64(pair.second)));
  return make_logical(LogicalOperator::And,std::move(fields));
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
struct Scan {
  QueryStatus status,ledger;
  CatalogSelection selection;
  QueryProfileRecord profile;
  std::uint64_t scan_bytes{0},bytes{0},sdk{0},store_sdk{0};
};
using Keys=std::vector<std::pair<std::string,long long>>;
Keys keys(const CatalogSelection& selection) {
  Keys result;for(const auto& m:selection.versions())result.emplace_back(m.key.path,m.key.version);return result;
}
void pool_bounds(const HotQueryCacheStats& stats) {
  check(stats.resident_bytes<=HotQueryCache::maximum_bytes&&stats.domains<=HotQueryCache::maximum_domains&&
    stats.proofs<=HotQueryCache::maximum_proofs,"shared pool exceeded fixed residency bounds");
}
}

int main(int argc,char** argv) {
  set_logging_level(LogLevel::OFF);std::string stage="arguments";std::vector<std::string> cases;
  std::map<std::string,std::uint64_t> observations;
  try {
    std::string prefix,journal;
    for(int i=1;i<argc;++i){const std::string arg=argv[i];check(i+1<argc,"missing argument value");
      if(arg=="--prefix")prefix=argv[++i];else if(arg=="--journal")journal=argv[++i];else throw ProbeFailure("unknown argument");}
    check(prefix.rfind("vd_shared_hot_",0)==0&&!journal.empty()&&journal.front()=='/',"fresh vd_shared_hot prefix and absolute journal required");
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
    auto hdf=std::make_shared<Hdf5FormatAdapter>();GribFormatAdapter::Config grib_config;
    grib_config.ingestion.detect_threshold_bytes=16;
    auto grib=std::make_shared<GribFormatAdapter>(grib_config);
    FormatRegistry formats;check(formats.register_adapter(hdf)&&formats.register_adapter(grib),"format registration failed");
    VersionDirectoryConfig settings;settings.table_prefix=prefix;settings.owner_directory=journal;settings.initialize=true;
    settings.artifact_validator=[hdf,grib](const std::string& format,const Bytes& layout,std::uint64_t size) {
      auto status=format=="hdf5"?hdf->validate_layout_artifact(layout,size):
        format=="grib"?grib->validate_layout_artifact(layout,size):FormatStatus::failure("unexpected fixture format");
      return status.ok()?StructuredBlobStatus::success():StructuredBlobStatus::failure(StructuredBlobError::InvalidState,status.error());
    };
    stage="initialize";check(backend->configure_version_directory(settings),"owner initialization failed");check(backend->ensure_schema(),"schema failed");
    IoTDBVersionStore store(config,prefix);store.verify_capabilities();
    for(const auto& path:{"/","/data","/data/sub","/unrelated"})check(backend->upsert_inode(inode(path,1,true),true),"directory creation failed");
    StructuredBlobStorage storage(backend);IngestionModule ingestion(storage,formats,{});
    auto publish=[&](const std::string& path,long long version,const Bytes& bytes,const std::string& format) {
      FormatSource source{bytes.size(),[&](std::uint64_t offset,std::uint64_t size)->std::optional<Bytes>{
        if(offset>bytes.size()||size>bytes.size()-offset)return {};return Bytes(bytes.begin()+offset,bytes.begin()+offset+size);}};
      auto result=ingestion.ingest({path,version},source,[&](const IngestionOutcome& outcome){
        check(outcome.format_id.value_or("")==format,"actual ingestion selected wrong adapter");
        auto node=inode(path,version,false);node.size=outcome.payload_size;node.format_id=format;
        auto metadata=serialize_metadata(outcome.metadata,65536);check(metadata.ok(),"metadata encoding failed");node.metadata=metadata.value();
        return backend->upsert_inode(node,false);
      });check(result.ok(),"actual format ingestion failed");
    };
    auto mutate=[&](const std::string& path,long long version,const std::function<void(Inode&)>& edit) {
      auto node=backend->load_inode(path,version);check(bool(node),"mutation inode absent");
      if(!node->is_dir){auto manifest=backend->load_manifest({path,version});check(manifest.ok(),"mutation manifest absent");
        auto text=serialize_metadata(manifest.value().metadata,65536);check(text.ok(),"mutation metadata failed");node->metadata=text.value();}
      edit(*node);check(backend->upsert_inode(*node,node->is_dir),"POSIX mutation failed");
    };
    stage="fixtures";const auto g850=grib_file(850),g851=grib_file(851);
    const auto h850=hdf_file(1,850),h851=hdf_file(1,851);
    publish("/data/a.grib",10,g850,"grib");publish("/data/b.grib",11,g851,"grib");
    publish("/data/a.h5",12,h850,"hdf5");publish("/data/b.h5",13,h851,"hdf5");
    const QueryPrincipal owner{1001,40077,{}},denied{1003,5000,{}};
    auto selector=[](const std::string& format) {
      SourceSelector s;s.paths={{PathScopeKind::Subtree,"/data"}};s.versions=VersionSelection::latest();
      s.formats={AdapterSelectionKind::Listed,{format},false};return s;
    };
    const auto gs=selector("grib"),hs=selector("hdf5");
    auto run=[&](const SourceSelector& source,const QueryPrincipal& principal,std::size_t maximum=8,bool poison=false) {
      Scan out;auto ledger=std::make_shared<QueryMetadataBudget>();ScopedQueryMetadataBudget metadata(ledger);
      if(poison)try{ledger->observe_unique(QueryMetadataBudget::maximum_bytes+1);}catch(const QueryMetadataBudgetExceeded&){}
      const auto before=iotdb_profile_snapshot();
      {ScopedQueryProfile profile(&out.profile);auto result=backend->scan_published_versions(source,maximum);out.status=result.status();
        out.scan_bytes=ledger->consumed_bytes();
        if(result.ok()) {out.selection=std::move(result.value());check(bool(out.selection.read_context),"selected owner context missing");
          auto admission=out.selection.read_context->admit(principal);out.status=admission.ok()?QueryStatus::success():admission.status();}}
      const auto after=iotdb_profile_snapshot();out.sdk=after.query_calls-before.query_calls;
      out.store_sdk=after.version_store_query_calls-before.version_store_query_calls;
      out.bytes=ledger->consumed_bytes();out.ledger=ledger->status();pool_bounds(backend->hot_query_cache_stats());return out;
    };
    auto okay=[](const Scan& r,std::size_t count,bool cached=true) {
      check(r.status.ok()&&r.ledger.ok()&&r.selection.versions().size()==count&&r.selection.read_context->sources().size()==count,"candidate membership differs");
      check(bool(r.selection.hot_state)==cached,"optional candidate caching eligibility differs");
    };
    auto failure=[](const Scan& r,QueryError code){check(!r.status.ok()&&r.status.code()==code,"failure code differs");};
    stage="shared_pool";const auto gcold=run(gs,owner),hcold=run(hs,owner);
    okay(gcold,2);okay(hcold,2);check(gcold.selection.hot_state!=hcold.selection.hot_state,"cross-format hot_state collided");
    const auto initial=backend->hot_query_cache_stats();check(initial.domains==2,"candidate artifact introduced a second domain reservation");
    const auto gwarm=run(gs,owner),hwarm=run(hs,owner);okay(gwarm,2);okay(hwarm,2);
    for(const auto& pair:{std::pair<const Scan*,const Scan*>{&gcold,&gwarm},{&hcold,&hwarm}}) {
      check(pair.first->selection.hot_state==pair.second->selection.hot_state&&pair.first->selection.read_context==pair.second->selection.read_context,"same-head domain not shared");
      check(keys(pair.first->selection)==keys(pair.second->selection)&&pair.first->scan_bytes==pair.second->scan_bytes&&pair.first->bytes==pair.second->bytes,"cold/warm candidate fees differ");
      check(pair.second->sdk==0&&pair.second->store_sdk==0,"warm candidate scan/admission issued SDK SQL");
      check(counter(pair.second->profile,"hot_query.candidate_domain_hits")>0&&counter(pair.second->profile,"hot_query.proof_hits")>0,"candidate/proof hit missing");
      check(counter(pair.second->profile,"version_context.source_permission_checks")==0,"proof did not reuse full-candidate permissions");
    }
    observations["grib_cold_scan_bytes"]=gcold.scan_bytes;observations["grib_warm_scan_bytes"]=gwarm.scan_bytes;
    observations["grib_cold_total_bytes"]=gcold.bytes;observations["grib_warm_total_bytes"]=gwarm.bytes;
    observations["hdf5_cold_scan_bytes"]=hcold.scan_bytes;observations["hdf5_warm_scan_bytes"]=hwarm.scan_bytes;
    observations["hdf5_cold_total_bytes"]=hcold.bytes;observations["hdf5_warm_total_bytes"]=hwarm.bytes;
    observations["initial_domains"]=initial.domains;cases.push_back("cross_format_shared_pool_identity_fees_proofs_and_actual_warm_sql_zero");
    stage="keys";auto normalized=gs;normalized.paths={{PathScopeKind::Subtree,"data"}};
    auto norm=run(normalized,owner);okay(norm,2);check(norm.selection.hot_state==gcold.selection.hot_state,"normalized equivalent selectors did not share");
    auto exact=gs;exact.paths={{PathScopeKind::Exact,"/data/a.grib"}};const auto e=run(exact,owner);okay(e,1);
    auto subtree=exact;subtree.paths={{PathScopeKind::Subtree,subtree.paths.front().path}};const auto t=run(subtree,owner);okay(t,1);
    check(e.selection.hot_state!=t.selection.hot_state,"Exact and Subtree cache keys collided");
    const auto capped=run(gs,owner,2);okay(capped,2);check(capped.selection.hot_state!=gcold.selection.hot_state,"max_sources key was ignored");
    cases.push_back("normalized_paths_scope_kinds_and_source_limit_keys");
    stage="fallback_and_limits";auto optional=gs;optional.formats.include_byte_passthrough=true;okay(run(optional,owner),2,false);
    optional=gs;optional.formats.adapter_ids={"grib","hdf5"};okay(run(optional,owner),4,false);
    optional=gs;optional.file_predicate=make_comparison({"sigma.nonexistent"},ComparisonOperator::Equal,LogicalValue::utf8("miss"));
    const auto residual=run(optional,owner);okay(residual,2,false);check(residual.selection.residual_file_predicate==optional.file_predicate,"file residual replaced by another request");
    failure(run(gs,owner,1),QueryError::ResourceExhausted);failure(run(optional,owner,1),QueryError::ResourceExhausted);
    failure(run(gs,owner,8,true),QueryError::ResourceExhausted);auto absent=gs;absent.paths={{PathScopeKind::Exact,"/absent"}};
    failure(run(absent,owner,8,true),QueryError::ResourceExhausted);failure(run(gs,denied),QueryError::PermissionDenied);
    cases.push_back("optional_flags_residual_fallback_prepruning_limit_and_sticky_failure");
    stage="hdf_endpoint_isolation";DatasetCatalogRequest request;request.sources=hs;request.predicate=full_predicate();
    auto found=backend->discover_datasets(request,[&](const auto&,const CatalogSelection& selected){
      auto gate=selected.read_context->admit(owner);return gate.ok()?QueryStatus::success():gate.status();});
    check(found.ok()&&found.value().selected_sources==2&&found.value().datasets.size()==1&&found.value().datasets.front().source.path=="/data/a.h5","HDF catalog endpoint collided with candidate payload");
    const auto again=run(hs,owner);okay(again,2);check(again.selection.hot_state==hcold.selection.hot_state,"HDF endpoint replaced candidate state");
    pool_bounds(backend->hot_query_cache_stats());cases.push_back("hdf_dataset_endpoint_and_common_candidate_payload_isolation");
    stage="revoke";mutate("/data/b.grib",11,[](Inode& i){i.mode=0;});
    const auto old=gcold.selection.read_context->admit(owner);check(!old.ok()&&old.status().code()==QueryError::PermissionDenied,"old full-candidate proof ignored source revocation");
    const auto revoked=run(gs,owner);failure(revoked,QueryError::PermissionDenied);
    check(revoked.selection.hot_state!=gcold.selection.hot_state,"new head reused old candidate domain");
    mutate("/data/b.grib",11,[](Inode& i){i.mode=0640;});mutate("/data",1,[](Inode& i){i.mode=0600;});
    const auto ancestor=hcold.selection.read_context->admit(owner);check(!ancestor.ok()&&ancestor.status().code()==QueryError::PermissionDenied,"shared HDF context ignored ancestor revocation");
    failure(run(hs,owner),QueryError::PermissionDenied);mutate("/data",1,[](Inode& i){i.mode=0755;});
    cases.push_back("head_invalidation_all_candidate_source_and_ancestor_permissions");
    stage="latest_history";publish("/data/a.grib",20,g850,"grib");publish("/data/a.h5",22,h850,"hdf5");
    check(gcold.selection.read_context->admit(owner).ok()&&hcold.selection.read_context->admit(owner).ok(),"new Latest invalidated live historical exact context");
    check(gcold.selection.read_context->manifest({"/data/a.grib",10}).ok()&&hcold.selection.read_context->manifest({"/data/a.h5",12}).ok(),"historical exact manifest unavailable");
    auto outside=gcold.selection.read_context->manifest({"/data/a.grib",20});check(!outside.ok()&&outside.status().code()==QueryError::StalePlan,"old domain rebound Latest");
    const auto latest=run(gs,owner);okay(latest,2);check(keys(latest.selection)==Keys{{"/data/a.grib",20},{"/data/b.grib",11}},"new scan failed to observe Latest");
    auto historical=exact;historical.versions=VersionSelection::exact_version(10);auto history=run(historical,owner);okay(history,1);
    check(keys(history.selection)==Keys{{"/data/a.grib",10}},"Exact selector retargeted Latest");
    publish("/data/sub/c.grib",30,g851,"grib");const auto phantom=run(gs,owner);okay(phantom,3);
    check(latest.selection.read_context->sources().size()==2&&gcold.selection.read_context->sources().size()==2,"new logical source entered old frozen context");
    check(backend->remove_path("/data/b.grib",11),"exact deletion failed");auto removed=gcold.selection.read_context->admit(owner);
    check(!removed.ok()&&removed.status().code()==QueryError::StalePlan,"deleted frozen candidate admitted");
    cases.push_back("cross_format_history_exact_latest_phantom_and_deleted_candidate");
    stage="final";auto head_bytes=store.read_head();check(bool(head_bytes),"final head absent");const auto head=decode_head(*head_bytes);
    auto stats=backend->hot_query_cache_stats();pool_bounds(stats);
    std::cout<<"{\"completed\":true,\"command\":\"shared_hot_query\",\"prefix\":"<<quoted(prefix)<<",\"journal\":"<<quoted(journal)
      <<",\"actual_backend\":true,\"real_grib_fixture\":true,\"real_hdf5_fixture\":true,\"existing_database\":true"
      <<",\"fixture_versions\":7,\"maximum_candidate_sources\":8,\"case_count\":"<<cases.size()<<",\"cases\":[";
    for(std::size_t i=0;i<cases.size();++i)std::cout<<(i?",":"")<<quoted(cases[i]);std::cout<<"],\"observations\":{";bool first=true;
    for(const auto& item:observations){std::cout<<(first?"":",")<<quoted(item.first)<<':'<<item.second;first=false;}
    std::cout<<"},\"final_stats\":{\"resident_bytes\":"<<stats.resident_bytes<<",\"domains\":"<<stats.domains<<",\"proofs\":"<<stats.proofs
      <<",\"domain_hits\":"<<stats.domain_hits<<",\"domain_builds\":"<<stats.domain_builds<<",\"proof_hits\":"<<stats.proof_hits
      <<",\"proof_builds\":"<<stats.proof_builds<<"},\"final_head_sequence\":"<<head.sequence
      <<",\"scope\":\"candidate domain/proof and logical metadata fees only; no decoder, ROI, throughput or service recovery claim\""
      <<",\"fixtures_preserved\":true,\"service_restarted\":false}\n";return 0;
  }catch(const ProbeFailure& error){std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()<<",\"error\":"<<quoted(error.what())<<"}\n";return 1;}
  catch(const std::exception&){std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()<<",\"error\":\"probe failed; inspect private native log; no retry\"}\n";return 1;}
}
