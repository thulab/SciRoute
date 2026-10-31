// H10 measurement harness. B/L are resident in a recording backend: no claim
// about filesystem or physical-disk speed. Production adapters remain unchanged.
#define main h04_probe_entry_point
#include "h04_query_probe.cpp"
#undef main
#include "iotdbfs/query/core/QueryProfile.h"
#include "iotdbfs/query/encoding/ArrowResultEncoder.h"
#include "iotdbfs/migration/MigrationPolicy.h"
#include <set>
#include <sys/resource.h>

namespace {
using Clock = std::chrono::steady_clock;
std::uint64_t elapsed(Clock::time_point t) {
 return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-t).count();
}
void object(std::ostream& out,const std::map<std::string,std::uint64_t>& values) {
 out<<'{';bool comma=false;for(const auto& [k,v]:values){if(comma)out<<',';comma=true;out<<q(k)<<':'<<v;}out<<'}';
}
struct Sample {std::string id,dataset,action;std::vector<std::uint64_t> origin,shape;};
std::vector<Sample> trace(const char* path) {
 std::ifstream file(path);need(bool(file),"cannot open trace");std::string line;std::vector<Sample> out;std::set<std::string> ids;
 while(std::getline(file,line)){
  need(line.size()<4096 && out.size()<256,"trace exceeds diagnostic bound");
  std::istringstream in(line);std::vector<std::string> p;std::string v;
  while(std::getline(in,v,'\t'))p.push_back(v);
  need(p.size()==5,"trace requires five TSV fields");
  need(!p[0].empty()&&p[0].size()<=128&&p[0].find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")==std::string::npos,"unsafe sample ID");
  need(ids.insert(p[0]).second,"duplicate sample ID");
  Sample s{p[0],p[1],p[4],dimensions(p[2]),dimensions(p[3])};
  need(s.origin.size()==s.shape.size()&&(s.action=="retain"||s.action=="evict_before_request"),"invalid trace row");out.push_back(std::move(s));
 }need(!out.empty(),"empty trace");return out;
}
std::map<std::string,std::uint64_t> delta(const QueryRuntimeStats& a,const QueryRuntimeStats& b) {
 std::map<std::string,std::uint64_t> m;
#define D(n) m[#n]=b.n-a.n
 D(hits);D(misses);D(waits);D(producer_count);D(decode_count);D(decoded_bytes);D(decompressions);
 D(decode_nanoseconds);D(decode_cpu_nanoseconds);D(decode_cpu_samples);D(read_bytes);D(read_ranges);D(copy_bytes);
 D(payload_read_calls);D(payload_read_nanoseconds);D(fragment_load_nanoseconds);D(evictions);D(redecodes);
 D(payload_read_bytes);D(payload_read_ranges);D(fragment_queue_nanoseconds);D(fragment_admission_nanoseconds);D(fragment_memory_wait_nanoseconds);
 D(decode_filter_nanoseconds);D(decode_inflate_nanoseconds);D(decode_shuffle_nanoseconds);D(decode_checksum_nanoseconds);D(decode_conversion_nanoseconds);
#undef D
#define G(n) m[#n]=b.n
 G(cache_owned_bytes);G(pinned_bytes);G(inflight_bytes);G(temporary_bytes);G(live_bytes);G(peak_bytes);G(payload_reads_peak);G(payload_reads_active);
#undef G
 return m;
}
}
int main(int argc,char** argv){try{
 need(argc==7,"usage: h10_profile_probe FILE OUTPUT TRACE.tsv PARALLELISM CACHE_BYTES MAX_CALL_RANGES");
 const auto samples=trace(argv[3]);const auto parallel=std::stoull(argv[4]),budget=std::stoull(argv[5]),ranges=std::stoull(argv[6]);
 need((parallel==1||parallel==2||parallel==4)&&(ranges==1||ranges==32),"invalid transport settings");
 const std::filesystem::path out=argv[2];std::filesystem::create_directories(out);
 Fd file;file.value=::open(argv[1],O_RDONLY|O_CLOEXEC);struct stat st{};
 need(file.value>=0&&::fstat(file.value,&st)==0&&S_ISREG(st.st_mode)&&st.st_size>0&&st.st_size<=64*1024*1024,"invalid probe file");
 auto backend=std::make_shared<Backend>();auto storage=std::make_shared<StructuredBlobStorage>(backend);
 FormatSource source{static_cast<std::uint64_t>(st.st_size),{}};
 std::uint64_t ingestion_read_calls=0,ingestion_read_bytes=0;
 source.read=[&](std::uint64_t o,std::uint64_t n)->std::optional<BinaryValue>{
  if(o>source.size||n>source.size-o)return std::nullopt; ++ingestion_read_calls;ingestion_read_bytes+=n;
  BinaryValue b(n);std::size_t done=0;while(done<b.size()){auto got=::pread(file.value,b.data()+done,b.size()-done,o+done);
   if(got<0&&errno==EINTR)continue;if(got<=0)return std::nullopt;done+=got;}return b;
 };
 FormatRegistry registry;need(registry.register_adapter(std::make_shared<Hdf5FormatAdapter>()),"adapter registration failed");
 IngestionModule ingest(*storage,registry,{65536});StructuredBlobVersionKey key{"/baseline/h10.hdf5",17};
 QueryProfileRecord ingestion_profile;auto start=Clock::now();
 {ScopedQueryProfile scope(&ingestion_profile);auto r=ingest.ingest(key,source,[](const IngestionOutcome&){return true;});need(r.ok(),r.message());}
 const auto ingestion_ns=elapsed(start);const auto source_hash=migration_sha256(backend->payload);
 backend->multi_range=ranges>1;
 QueryRuntimeConfig runtime_config;runtime_config.memory_budget_bytes=budget;runtime_config.max_entries=65536;
 runtime_config.wait_timeout_ms=30000;runtime_config.enable_detailed_profile=true;
 auto runtime=std::make_shared<QueryRuntime>(runtime_config);
 struct DecodeTrace {std::mutex mutex;std::set<std::string> keys;std::uint64_t count{0};bool complete{true};};
 auto decode_trace=std::make_shared<DecodeTrace>();
 Hdf5QueryKernelConfig config;config.read_parallelism=parallel;
 config.decoded_fragment_observer=[decode_trace](const std::string& key,std::uint64_t){std::lock_guard<std::mutex> lock(decode_trace->mutex);++decode_trace->count;
  if(decode_trace->keys.size()<65536)decode_trace->keys.insert(key);else if(!decode_trace->keys.count(key))decode_trace->complete=false;};
 auto module=QueryModule::create(make_in_memory_published_version_catalog({*backend->manifest}),storage,
  {std::make_shared<Hdf5FormatQueryAdapter>(config,runtime)});need(module.ok(),module.status().message());
 std::ofstream json(out/"raw.json");json<<"{\"protocol\":\"iotdbfs-hdf5-h10-probe/1\",\"source_sha256\":"<<q(source_hash)
  <<",\"config\":{\"source_path\":"<<q(key.path)<<",\"source_version\":"<<key.version<<",\"backend\":\"recording-memory\",\"parallelism\":"<<parallel<<",\"cache_bytes\":"<<budget<<",\"max_call_ranges\":"<<ranges
  <<"},\"setup\":{\"ingestion_ns\":"<<ingestion_ns<<",\"ingestion_read_calls\":"<<ingestion_read_calls<<",\"ingestion_read_bytes\":"<<ingestion_read_bytes
  <<",\"source_bytes\":"<<source.size<<",\"layout_bytes\":"<<backend->layout.size()<<",\"ingestion_profile\":";object(json,ingestion_profile.nanoseconds);
 json<<",\"ingestion_counters\":";object(json,ingestion_profile.counters);json<<"},\"requests\":[";bool comma=false;
 for(const auto& s:samples){
  if(s.action=="evict_before_request")runtime->cache().evict_all();
  backend->phase=s.id;backend->peak=0;const auto calls=backend->calls.load(),batch_calls=backend->batch_calls.load(),read_start=backend->reads.size();
  const auto before=runtime->stats();const auto decodes_before=decode_trace->count;QueryProfileRecord profile;std::map<std::string,std::uint64_t> timings;
  QueryRequest request;request.sources.paths={{PathScopeKind::Exact,key.path}};request.sources.versions=VersionSelection::exact_version(key.version);
  request.sources.formats={AdapterSelectionKind::Listed,{"hdf5"},false};request.subset=QueryFragment{};
  const auto field=make_hdf5_value_field(s.dataset);need(field.ok(),field.status().message());request.subset->projection={field.value()};
  for(std::size_t a=0;a<s.shape.size();++a){need(s.shape[a]<=UINT64_MAX-s.origin[a],"slice overflow");request.subset->slices.push_back({hdf5_axis_field(a),
   DomainBound{LogicalValue::uint64(s.origin[a]),BoundKind::Inclusive},DomainBound{LogicalValue::uint64(s.origin[a]+s.shape[a]),BoundKind::Exclusive}});}
  request.limits.target_batch_rows=4096;std::vector<QueryBatch> batches;BinaryValue ipc;
  {ScopedQueryProfile scope(&profile);const auto total=Clock::now();start=Clock::now();auto prepared=module.value()->prepare(request);timings["prepare"]=elapsed(start);need(prepared.ok(),prepared.status().message());
   start=Clock::now();auto cursor=module.value()->execute(prepared.value());timings["execute_setup"]=elapsed(start);need(cursor.ok(),cursor.status().message());
   std::uint64_t rows=0;start=Clock::now();for(;;){auto b=cursor.value()->next();need(b.ok(),b.status().message());if(!b.value())break;rows+=b.value()->row_count();need(rows<=2000000,"probe output exceeds diagnostic bound");batches.push_back(std::move(*b.value()));}
   timings["cursor_consume"]=elapsed(start);
   QueryResultMetadata metadata;std::size_t count=0;
   for(const auto& selected:prepared.value().sources())if(selected.plan){const auto& entries=selected.plan->implementation()->result_metadata();if(entries.empty())continue;
    const auto prefix="iotdbfs.result_descriptor."+std::to_string(count++)+".";for(const auto& e:entries)metadata.emplace_back(prefix+e.first,e.second);}
   if(count)metadata.emplace_back("iotdbfs.result_descriptors",std::to_string(count));
   start=Clock::now();auto encoded=ArrowResultEncoder::encode_query_batches(cursor.value()->schema(),batches,64ULL*1024*1024,metadata);
   timings["ipc_encode"]=elapsed(start);need(encoded.ok(),encoded.status().message());ipc=std::move(encoded.value());timings["total"]=elapsed(total);
  }
  const auto after=runtime->stats();auto metrics=delta(before,after);BinaryValue values;
  for(const auto& b:batches){auto index=b.schema().index_of(field.value().field_id);need(index.has_value(),"missing values");const auto& c=b.columns()[*index];need(c.validity.empty(),"unexpected null values");
   if(c.type.id==LogicalTypeId::Float32)append_canonical(values,std::get<std::vector<float>>(c.values));else append_canonical(values,std::get<std::vector<double>>(c.values));}
  struct rusage usage{};if(::getrusage(RUSAGE_SELF,&usage)==0){
#ifdef __APPLE__
   metrics["process_peak_rss_bytes"]=usage.ru_maxrss;
#else
   metrics["process_peak_rss_bytes"]=static_cast<std::uint64_t>(usage.ru_maxrss)*1024;
#endif
  }
  metrics["useful_bytes"]=values.size();metrics["ipc_bytes"]=ipc.size();metrics["recorded_payload_calls"]=backend->calls-calls;
  metrics["recorded_batch_calls"]=backend->batch_calls-batch_calls;metrics["recorded_payload_peak"]=backend->peak;
  metrics["chunk_decode_count"]=decode_trace->count-decodes_before;metrics["unique_decoded_fragments_cumulative"]=decode_trace->keys.size();metrics["unique_decode_trace_complete"]=decode_trace->complete?1:0;
  std::set<std::pair<std::uint64_t,std::uint64_t>> spans;
  for(std::size_t i=read_start;i<backend->reads.size();++i){const auto& r=backend->reads[i];if(r.kind=="payload"){
    metrics["recorded_payload_bytes"]+=r.length;++metrics["recorded_payload_ranges"];spans.insert({r.offset,r.length});}
   else {metrics["layout_bytes"]+=r.length;++metrics["layout_calls"];}}
  metrics["unique_payload_spans"]=spans.size();
  std::uint64_t union_bytes=0,end=0;for(const auto& span:spans){const auto stop=span.first+span.second;if(stop>end){union_bytes+=stop-std::max(end,span.first);end=stop;}}
  metrics["unique_payload_union_bytes"]=union_bytes;
  metrics["repeated_payload_bytes"]=metrics["recorded_payload_bytes"]-union_bytes;
  // Current range planner merges adjacent/overlapping spans only, never gaps.
  metrics["merge_gap_overread_bytes"]=0;
  // Emit explicit zeroes for warm phases; absence never means measured zero.
  for(const char* k:{"recorded_payload_bytes","recorded_payload_ranges","layout_bytes","layout_calls"})metrics.try_emplace(k,0);
  need(metrics["recorded_payload_bytes"]==metrics["payload_read_bytes"]&&metrics["recorded_payload_ranges"]==metrics["payload_read_ranges"]&&metrics["recorded_payload_calls"]==metrics["payload_read_calls"],"recording source/runtime I/O mismatch");
  save(out/(s.id+".values.bin"),values);save(out/(s.id+".ipc"),ipc);
  if(comma)json<<',';comma=true;json<<"{\"sample_id\":"<<q(s.id)<<",\"dataset\":"<<q(s.dataset)<<",\"origin\":";numbers(json,s.origin);json<<",\"shape\":";numbers(json,s.shape);
  json<<",\"cache_action\":"<<q(s.action)<<",\"values_sha256\":"<<q(migration_sha256(values))<<",\"timings_ns\":";object(json,timings);
  json<<",\"stages_ns\":";object(json,profile.nanoseconds);json<<",\"counters\":";object(json,profile.counters);json<<",\"metrics\":";object(json,metrics);
  json<<",\"reads\":[";for(std::size_t i=read_start;i<backend->reads.size();++i){if(i>read_start)json<<',';const auto& r=backend->reads[i];json<<"{\"kind\":"<<q(r.kind)<<",\"offset\":"<<r.offset<<",\"length\":"<<r.length<<'}';}json<<"]}";
 }
 json<<"]}\n";json.close();need(bool(json),"cannot save profile");return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
