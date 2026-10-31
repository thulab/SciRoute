// Real backend/Store ReadContext integration. Writes only an explicitly new
// vd_v03_* prefix in the existing database. No service control or cleanup.
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
Bytes hdf_file(const std::string& units) {
  H5 access(H5Pcreate(H5P_FILE_ACCESS),H5Pclose);h5check(H5Pset_fapl_core(access,4096,0));
  H5 file(H5Fcreate("v03-context-memory",H5F_ACC_TRUNC,H5P_DEFAULT,access),H5Fclose);
  const hsize_t extent[]{4};H5 space(H5Screate_simple(1,extent,nullptr),H5Sclose);
  H5 dataset(H5Dcreate2(file,"/values",H5T_IEEE_F32LE,space,H5P_DEFAULT,H5P_DEFAULT,H5P_DEFAULT),H5Dclose);
  const float values[]{1,2,3,4};h5check(H5Dwrite(dataset,H5T_NATIVE_FLOAT,H5S_ALL,H5S_ALL,H5P_DEFAULT,values));
  H5 scalar(H5Screate(H5S_SCALAR),H5Sclose);H5 text(H5Tcopy(H5T_C_S1),H5Tclose);
  h5check(H5Tset_size(text,units.size()));
  H5 attribute(H5Acreate2(dataset,"units",text,scalar,H5P_DEFAULT,H5P_DEFAULT),H5Aclose);
  h5check(H5Awrite(attribute,text,units.data()));h5check(H5Fflush(file,H5F_SCOPE_GLOBAL));
  const auto size=H5Fget_file_image(file,nullptr,0);check(size>0,"HDF5 image is empty");Bytes result(size);
  check(H5Fget_file_image(file,result.data(),result.size())==size,"HDF5 image read differs");return result;
}
Inode inode(const std::string& path,long long version,bool directory) {
  Inode i;i.path=path;i.parent=path_utils::parent_of(path);i.name=path_utils::base(path);
  i.row_time=version;i.is_dir=directory;i.mode=directory?0755:0640;i.uid=1001;i.gid=40077;i.metadata="{}";
  i.ctime=i.mtime=i.atime=1700000000;return i;
}
struct Observation {
  std::string label;std::uint64_t tested_calls{0},sdk_queries{0};QueryProfileRecord profile;
  std::array<std::uint64_t,4> store_counts{};
};
std::uint64_t counter(const QueryProfileRecord& p,const std::string& name) {
  const auto found=p.counters.find(name);return found==p.counters.end()?0:found->second;
}
Observation observe(const std::string& label,const std::function<void(std::uint64_t&)>& action) {
  Observation result;result.label=label;const auto before=iotdb_profile_snapshot();
  {ScopedQueryProfile scope(&result.profile);action(result.tested_calls);}
  const auto after=iotdb_profile_snapshot();result.sdk_queries=after.query_calls-before.query_calls;
  result.store_counts={after.version_store_query_calls-before.version_store_query_calls,
    after.version_store_head_reads-before.version_store_head_reads,
    after.version_store_object_reads-before.version_store_object_reads,
    after.version_store_blob_bytes-before.version_store_blob_bytes};
  check(result.tested_calls>0,"zero-SQL observation did not exercise any call");return result;
}
void no_queries(const Observation& o) {
  check(o.tested_calls>0,"missing measured calls");
  check(counter(o.profile,"read_context.admissions")>0,"measured admission instrumentation missing");
  check(o.sdk_queries==0&&o.store_counts[0]==0,
    "warm operation issued actual SDK/Store SQL");
}
std::string observation_json(const Observation& o) {
  std::ostringstream out;out<<"{\"label\":"<<quoted(o.label)<<",\"tested_calls\":"<<o.tested_calls
    <<",\"sdk_query_calls\":"<<o.sdk_queries<<",\"version_store_counters\":{";
  bool first=true;std::size_t index=0;
  // A synchronous scope surrounded actual calls above; absent increments mean
  // zero only within that exercised scope, never an unavailable whole report.
  for(const auto* key:{"version_store.sql_queries","version_store.head_reads","version_store.object_reads","version_store.blob_bytes"}) {
    out<<(first?"":",")<<quoted(key)<<':'<<o.store_counts[index++];first=false;
  }
  out<<"},\"profile_counters\":{";first=true;
  for(const auto& item:o.profile.counters){out<<(first?"":",")<<quoted(item.first)<<':'<<item.second;first=false;}
  out<<"}}";return out.str();
}
}
int main(int argc,char** argv) {
  set_logging_level(LogLevel::OFF);std::string stage="arguments";
  std::vector<std::string> cases;std::vector<Observation> observations;
  std::map<std::string,std::uint64_t> membership_checks,metadata_budget_observations;
  try {
    std::string prefix,journal;
    for(int i=1;i<argc;++i){const std::string arg=argv[i];check(i+1<argc,"missing argument value");
      if(arg=="--prefix")prefix=argv[++i];else if(arg=="--journal")journal=argv[++i];else throw std::runtime_error("unknown argument");}
    check(prefix.rfind("vd_v03_",0)==0&&!journal.empty()&&journal.front()=='/',"new vd_v03 prefix and absolute new journal required");
    check(::access(journal.c_str(),F_OK)!=0,"journal already exists; probe does not resume");
    stage="configuration";IoTDBClientConfig config;
    config.host=env("IOTDB_HOST");config.user=env("IOTDB_USER");config.pass=env("IOTDB_PASS");config.db=env("IOTDB_DB");
    const auto port=env("IOTDB_PORT");std::size_t used=0;config.port=std::stoi(port,&used);
    check(used==port.size()&&config.port==33667,"controlled original RPC port required");
    check(config.host=="127.0.0.1"||config.host=="localhost","controlled loopback required");config.host="127.0.0.1";
    check(config.db=="h09probe1790939889dbfc1f","existing experiment database required");config.auto_create_db=false;
    // USE the existing DB without enabling automatic database creation. Fresh
    // schema creation belongs to configure_version_directory's owner lock.
    IoTDBClient existing_database(config,1);
    IoTDBConnector::instance().initialize({config.host,config.port,config.user,config.pass,config.db},2);
    auto backend=std::make_shared<IoTDBStructuredBlobBackend>();
    FormatRegistry formats;auto adapter=std::make_shared<Hdf5FormatAdapter>();
    check(formats.register_adapter(adapter),"format registration failed");
    VersionDirectoryConfig settings;settings.table_prefix=prefix;settings.owner_directory=journal;settings.initialize=true;
    settings.artifact_validator=[adapter](const std::string& format,const Bytes& layout,std::uint64_t size) {
      if(format!="hdf5")return StructuredBlobStatus::failure(StructuredBlobError::InvalidArgument,"unexpected fixture format");
      const auto status=adapter->validate_layout_artifact(layout,size);
      return status.ok()?StructuredBlobStatus::success():StructuredBlobStatus::failure(StructuredBlobError::InvalidState,status.error());
    };
    stage="initialize";check(backend->configure_version_directory(settings),"owner initialization failed");check(backend->ensure_schema(),"staging schema failed");
    IoTDBVersionStore store(config,prefix);store.verify_capabilities();
    for(const auto& path:{"/","/data","/unrelated"})check(backend->upsert_inode(inode(path,1,true),true),"directory creation failed");
    StructuredBlobStorage storage(backend);IngestionModule ingestion(storage,formats,{});
    auto publish=[&](const std::string& path,long long version,const std::string& units) {
      auto bytes=hdf_file(units);FormatSource source{bytes.size(),[&](std::uint64_t offset,std::uint64_t size)->std::optional<Bytes>{
        if(offset>bytes.size()||size>bytes.size()-offset)return {};return Bytes(bytes.begin()+offset,bytes.begin()+offset+size);}};
      const auto result=ingestion.ingest({path,version},source,[&](const IngestionOutcome& out){
        auto i=inode(path,version,false);i.size=out.payload_size;i.format_id=out.format_id.value_or("");
        const auto metadata=serialize_metadata(out.metadata,65536);check(metadata.ok(),"fixture metadata encoding failed");i.metadata=metadata.value();
        return backend->upsert_inode(i,false);
      });check(result.ok(),"real HDF5 ingestion failed");
    };
    auto mutate=[&](const std::string& path,long long version,const std::function<void(Inode&)>& edit) {
      auto i=backend->load_inode(path,version);check(bool(i),"mutation inode absent");
      if(!i->is_dir){auto m=backend->load_manifest({path,version});check(m.ok(),"mutation manifest absent");
        auto text=serialize_metadata(m.value().metadata,65536);check(text.ok(),"mutation metadata encoding failed");i->metadata=text.value();}
      edit(*i);check(backend->upsert_inode(*i,i->is_dir),"POSIX mutation failed");
    };
    stage="fixture";publish("/data/a.h5",10,"K");publish("/data/b.h5",11,"C");
    observations.push_back(observe("actual_store_profile_calibration",[&](std::uint64_t& calls){
      check(bool(store.read_head()),"calibration head missing");++calls;
    }));
    check(counter(observations.back().profile,"version_store.sql_queries")>0&&
      counter(observations.back().profile,"version_store.head_reads")>0&&
      counter(observations.back().profile,"version_store.blob_bytes")>0,"actual Store profiling is not instrumented");
    cases.push_back("store_counters_calibrated_against_actual_head_query");
    const QueryPrincipal principal{1001,40077,{}};
    DatasetCatalogRequest request;request.sources.paths={{PathScopeKind::Subtree,"/data"}};
    request.sources.versions=VersionSelection::latest();request.sources.formats.adapters=AdapterSelectionKind::Listed;
    request.sources.formats.adapter_ids={"hdf5"};request.sources.formats.include_byte_passthrough=false;
    request.predicate=make_comparison({"hdf5.attr:units"},ComparisonOperator::Equal,LogicalValue::utf8("K"));
    auto authorize=[&](const auto&,const CatalogSelection& selected) {
      check(bool(selected.read_context),"selected owner context missing");
      auto admission=selected.read_context->admit(principal);return admission.ok()?QueryStatus::success():admission.status();
    };
    stage="all_candidates";auto discovery=backend->discover_datasets(request,authorize);check(discovery.ok(),"initial discovery failed");
    check(discovery.value().datasets.size()==1&&discovery.value().datasets[0].source.path=="/data/a.h5","predicate selection differs");
    const auto context=discovery.value().read_context;check(context&&context->sources().size()==2,"predicate miss disappeared from context");
    cases.push_back("predicate_miss_retained_in_all_candidate_context");
    observations.push_back(observe("warm_admit_manifest",[&](std::uint64_t& calls){
      for(int n=0;n<3;++n){auto gate=context->admit(principal);++calls;check(gate.ok(),"warm admission failed");gate.value().reset();
        auto m=context->manifest({"/data/a.h5",10});++calls;check(m.ok()&&m.value().metadata_materialized,"warm manifest failed");}
    }));no_queries(observations.back());cases.push_back("stable_admit_and_manifest_actual_sql_zero");
    observations.push_back(observe("warm_discovery",[&](std::uint64_t& calls){
      for(int n=0;n<2;++n){auto d=backend->discover_datasets(request,authorize);++calls;check(d.ok()&&d.value().datasets.size()==1,"warm discovery failed");
        check(d.value().read_context->admit(principal).ok(),"final discovery admission failed");++calls;}
    }));no_queries(observations.back());cases.push_back("stable_discovery_actual_sql_zero");
    // Public ReadContext seams: no private backend test interface, extra files,
    // or extra commits. Run the same probe source against each linked core.
    stage="membership";
    {
      ScopedQueryMetadataBudget metadata;
      auto selector=request.sources;
      // This predicate is residual. It must not hide structural candidates or
      // confer membership to a result that is absent from the selected context.
      selector.file_predicate=make_comparison({"sigma.nonexistent"},
        ComparisonOperator::Equal,LogicalValue::utf8("excluded"));
      auto captured=backend->load_published_snapshot(selector,true);
      check(captured.ok()&&captured.value().read_context&&captured.value().manifests.size()==2,
        "file predicate prematurely removed namespace candidates");
      auto raw=captured.value().read_context;
      auto unselected=raw->manifest({"/data/a.h5",10});
      check(!unselected.ok()&&unselected.status().code()==QueryError::StalePlan,
        "unselected context granted manifest membership");
      auto ordered=captured.value().manifests;
      std::sort(ordered.begin(),ordered.end(),[](const auto& a,const auto& b){return a.key.path>b.key.path;});
      check(ordered[0].key.path=="/data/b.h5"&&ordered[1].key.path=="/data/a.h5",
        "fixture did not provide reverse candidate order");
      auto reversed=raw->select(ordered);check(reversed.ok(),"reverse candidate selection failed");
      const auto& refs=reversed.value()->sources();check(refs.size()==ordered.size(),"reverse context cardinality differs");
      for(std::size_t n=0;n<ordered.size();++n)
        check(refs[n].path==ordered[n].key.path&&refs[n].version==ordered[n].key.version,
          "source order changed when dependency keys were sorted");
      auto gate=reversed.value()->admit(principal);check(gate.ok(),"reverse context admission failed");gate.value().reset();
      for(const auto& m:ordered)check(reversed.value()->manifest(m.key).ok(),"reverse context lost valid source");
      auto narrowed=raw->select({ordered[1]});check(narrowed.ok(),"subset selection failed");
      check(narrowed.value()->sources().size()==1&&narrowed.value()->manifest(ordered[1].key).ok(),
        "subset lost chosen source");
      auto omitted=narrowed.value()->manifest(ordered[0].key);
      check(!omitted.ok()&&omitted.status().code()==QueryError::StalePlan,
        "snapshot record bypassed selected-source membership");
      auto reduced=reversed.value()->select({ordered[1]});
      check(!reduced.ok()&&reduced.status().code()==QueryError::InvalidQuery,
        "already frozen context dropped candidates");
      auto duplicate=raw->select({ordered[1],ordered[1]});
      check(!duplicate.ok()&&duplicate.status().code()==QueryError::StalePlan,
        "duplicate candidate accepted");
      auto wrong=ordered[1];++wrong.revision;auto mismatch=raw->select({wrong});
      check(!mismatch.ok()&&mismatch.status().code()==QueryError::StalePlan,
        "candidate revision mismatch accepted");
      std::vector<StructuredBlobVersionKey> rejected{{"/data/unknown.h5",10},{"/data/a.h5",99},
        {"/data",1},{"/",1},{"/data/a.h5",-1},{"data/a.h5",10},{"/data//a.h5",10},
        {"/data/../a.h5",10},{std::string("/data/a.h5\0suffix",17),10},{"/"+std::string(4096,'x'),10}};
      for(const auto& key:rejected){auto denied=reversed.value()->manifest(key);
        check(!denied.ok()&&denied.status().code()==QueryError::StalePlan,
          "unknown, ancestor or malformed key changed manifest rejection");}
      auto admitted=narrowed.value()->admit(principal);check(admitted.ok(),"subset ancestor admission failed");admitted.value().reset();
      membership_checks["reverse_source_order_preserved"]=ordered.size();
      membership_checks["valid_source_manifest_reads"]=ordered.size()+1;
      membership_checks["unknown_ancestor_malformed_keys_rejected"]=rejected.size();
      membership_checks["omitted_snapshot_source_rejected"]=1;
      membership_checks["unselected_context_rejected"]=1;
      membership_checks["duplicate_revision_and_reselection_rejected"]=3;
      membership_checks["residual_file_predicate_candidates_retained"]=ordered.size();
      check(metadata.status().ok(),"membership fixture exhausted metadata budget");
      metadata_budget_observations["membership_fixture_with_rejections_bytes"]=metadata.budget()->consumed_bytes();
    }
    stage="warm_budget";auto bounded=request;bounded.max_sources=1;
    auto too_many=backend->discover_datasets(bounded,authorize);check(!too_many.ok()&&too_many.status().code()==QueryError::ResourceExhausted,"warm source budget bypassed");
    bounded=request;bounded.max_catalog_bytes=1;
    auto too_big=backend->discover_datasets(bounded,authorize);check(!too_big.ok()&&too_big.status().code()==QueryError::ResourceExhausted,"warm catalog byte budget bypassed");
    {
      ScopedQueryMetadataBudget shared_budget;
      require_query_metadata_unique(QueryMetadataBudget::maximum_bytes-1);
      auto exhausted=backend->discover_datasets(request,authorize);
      check(!exhausted.ok()&&exhausted.status().code()==QueryError::ResourceExhausted,
        "warm cached discovery bypassed cumulative metadata demand");
      check(shared_budget.status().code()==QueryError::ResourceExhausted,"shared metadata failure lost stickiness");
      auto empty=request;empty.sources.paths={{PathScopeKind::Exact,"/data/absent.h5"}};
      auto still_exhausted=backend->discover_datasets(empty,authorize);
      check(!still_exhausted.ok()&&still_exhausted.status().code()==QueryError::ResourceExhausted,
        "zero-source discovery bypassed sticky request failure");
    }
    cases.push_back("warm_source_and_catalog_budgets_still_enforced");
    stage="unrelated";mutate("/unrelated",1,[](Inode& i){++i.mtime;});
    observations.push_back(observe("unrelated_commit_admit",[&](std::uint64_t& calls){auto gate=context->admit(principal);++calls;check(gate.ok(),"unrelated commit broke binding");}));
    no_queries(observations.back());cases.push_back("unrelated_commit_preserves_frozen_binding");
    stage="new_latest";publish("/data/a.h5",20,"K");
    check(context->admit(principal).ok(),"new latest invalidated existing exact version");
    check(context->manifest({"/data/a.h5",10}).ok()&&!context->manifest({"/data/a.h5",20}).ok(),"frozen context rebound latest");
    auto latest=backend->discover_datasets(request,authorize);check(latest.ok()&&latest.value().datasets.size()==1&&latest.value().datasets[0].source.version==20,"new selection did not resolve latest");
    check(latest.value().read_context&&latest.value().read_context->manifest({"/data/a.h5",20}).ok(),
      "new latest context lost its exact source");
    auto old_in_new=latest.value().read_context->manifest({"/data/a.h5",10});
    check(!old_in_new.ok()&&old_in_new.status().code()==QueryError::StalePlan,
      "same-path old version bypassed latest context membership");
    membership_checks["same_path_other_published_versions_rejected"]=2;
    cases.push_back("latest_resolved_once_existing_context_stays_exact");
    stage="history_gap";
    for(int n=0;n<257;++n)mutate("/unrelated",1,[](Inode& i){++i.mtime;});
    observations.push_back(observe("history_gap_admit",[&](std::uint64_t& calls){auto gate=context->admit(principal);++calls;check(gate.ok(),"history gap failed exact revalidation");}));
    check(counter(observations.back().profile,"read_context.history_gaps")>0&&
      counter(observations.back().profile,"version_context.dependency_rereads")>0,"history gap skipped full dependency reread");
    cases.push_back("ring_gap_forces_source_and_ancestor_revalidation");
    stage="source_revoke";mutate("/data/b.h5",11,[](Inode& i){i.mode=0;});
    auto revoked=context->admit(principal);check(!revoked.ok()&&revoked.status().code()==QueryError::PermissionDenied,"predicate-miss source revocation bypassed");
    mutate("/data/b.h5",11,[](Inode& i){i.mode=0640;});check(context->admit(principal).ok(),"permission restoration failed");
    cases.push_back("predicate_miss_source_permission_revocation_enforced");
    stage="ancestor_revoke";mutate("/data",1,[](Inode& i){i.mode=0600;});
    auto parent=context->admit(principal);check(!parent.ok()&&parent.status().code()==QueryError::PermissionDenied,"ancestor search revocation bypassed");
    mutate("/data",1,[](Inode& i){i.mode=0755;});check(context->admit(principal).ok(),"ancestor permission restoration failed");
    cases.push_back("current_ancestor_search_revocation_enforced");
    stage="delete";check(backend->remove_path("/data/b.h5",11),"exact deletion failed");
    auto deleted=context->admit(principal);check(!deleted.ok()&&deleted.status().code()==QueryError::StalePlan,"deleted predicate miss still admitted");
    cases.push_back("exact_deleted_predicate_miss_rejects_new_admission");
    stage="final_head";const auto encoded=store.read_head();check(bool(encoded),"final head absent");const auto head=decode_head(*encoded);
    std::cout<<"{\"completed\":true,\"command\":\"v03_context\",\"prefix\":"<<quoted(prefix)<<",\"journal\":"<<quoted(journal)
      <<",\"actual_backend\":true,\"real_hdf5_fixture\":true,\"existing_database\":true,\"principal_uid\":1001,\"principal_gid\":40077"
      <<",\"history_gap_commits\":257,\"final_head_sequence\":"<<head.sequence<<",\"case_count\":"<<cases.size()<<",\"cases\":[";
    for(std::size_t i=0;i<cases.size();++i)std::cout<<(i?",":"")<<quoted(cases[i]);std::cout<<"],\"observations\":[";
    for(std::size_t i=0;i<observations.size();++i)std::cout<<(i?",":"")<<observation_json(observations[i]);
    std::cout<<"],\"membership_checks\":{";bool first=true;
    for(const auto& item:membership_checks){std::cout<<(first?"":",")<<quoted(item.first)<<':'<<item.second;first=false;}
    std::cout<<"},\"metadata_budget_observations\":{";first=true;
    for(const auto& item:metadata_budget_observations){std::cout<<(first?"":",")<<quoted(item.first)<<':'<<item.second;first=false;}
    std::cout<<"},\"metadata_budget_observation_scope\":\"same fixture operations and public API; compare separately linked baseline/candidate, no timing claim\""
      <<",\"fixtures_preserved\":true,\"service_restarted\":false}\n";return 0;
  } catch(const ProbeFailure& error) {
    // All ProbeFailure strings are controlled literals above; SDK exception
    // text is handled separately and never serialized.
    std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()
      <<",\"error\":"<<quoted(error.what())<<"}\n";return 1;
  } catch(const std::exception&) {
    std::cout<<"{\"completed\":false,\"stage\":"<<quoted(stage)<<",\"completed_cases\":"<<cases.size()
      <<",\"error\":\"probe failed; inspect private native log; no automatic retry\"}\n";return 1;
  }
}
