// Synthetic bounded-latency transport probe; not an IoTDB performance result.
#define main h04_probe_entry_point
#include "h04_query_probe.cpp"
#undef main
#include <future>

namespace {
BinaryValue consume(const QueryModule& module,const PreparedQuery& prepared,const FieldRef& value) {
 auto cursor=module.execute(prepared);need(cursor.ok(),cursor.status().message());BinaryValue values;
 for(;;){auto batch=cursor.value()->next();need(batch.ok(),batch.status().message());if(!batch.value())break;
  auto index=batch.value()->schema().index_of(value.field_id);need(index.has_value(),"missing value field");
  const auto& column=batch.value()->columns()[*index];need(column.validity.empty(),"unexpected null output");
  if(column.type.id==LogicalTypeId::Float32)append_canonical(values,std::get<std::vector<float>>(column.values));
  else append_canonical(values,std::get<std::vector<double>>(column.values));
 }return values;
}
}
int main(int argc,char** argv){try{
 need(argc==8,"usage: h06_query_probe FILE OUTPUT ORIGIN SHAPE PARALLELISM MULTI_RANGE LATENCY_US");
 auto origin=dimensions(argv[3]),shape=dimensions(argv[4]);need(origin.size()==shape.size(),"rank mismatch");
 const auto parallel=std::stoull(argv[5]);need(parallel==1||parallel==2||parallel==4,"parallelism must be 1/2/4");
 std::filesystem::path output=argv[2];std::filesystem::create_directories(output);
 Fd file;file.value=::open(argv[1],O_RDONLY|O_CLOEXEC);struct stat info{};
 need(file.value>=0&&::fstat(file.value,&info)==0&&info.st_size>=0&&info.st_size<64*1024*1024,"invalid fixture");
 auto backend=std::make_shared<Backend>();auto storage=std::make_shared<StructuredBlobStorage>(backend);
 FormatSource source{static_cast<std::uint64_t>(info.st_size),{}};
 source.read=[&](std::uint64_t offset,std::uint64_t count)->std::optional<BinaryValue>{
  if(offset>source.size||count>source.size-offset)return std::nullopt;BinaryValue bytes(count);std::size_t n=0;
  while(n<bytes.size()){auto got=::pread(file.value,bytes.data()+n,bytes.size()-n,offset+n);if(got<0&&errno==EINTR)continue;if(got<=0)return std::nullopt;n+=got;}return bytes;
 };
 FormatRegistry registry;need(registry.register_adapter(std::make_shared<Hdf5FormatAdapter>()),"registration failed");
 IngestionModule ingestion(*storage,registry,{65536});StructuredBlobVersionKey key{"/baseline/shared.h5",17};
 auto ingested=ingestion.ingest(key,source,[](const IngestionOutcome&){return true;});need(ingested.ok(),ingested.message());
 backend->multi_range=std::string(argv[6])=="1";backend->latency_us=std::stoull(argv[7]);
 QueryRuntimeConfig runtime_config;runtime_config.memory_budget_bytes=256ULL*1024*1024;runtime_config.max_entries=8192;
 runtime_config.wait_timeout_ms=30000;auto runtime=std::make_shared<QueryRuntime>(runtime_config);
 Hdf5QueryKernelConfig config;config.read_parallelism=parallel;config.max_buffer_bytes=192ULL*1024*1024;
 auto module=QueryModule::create(make_in_memory_published_version_catalog({*backend->manifest}),storage,
  {std::make_shared<Hdf5FormatQueryAdapter>(config,runtime)});need(module.ok(),module.status().message());
 std::ofstream json(output/"query.json");json<<"{\"schema_version\":\"iotdbfs-hdf5-h06-probe/1\",\"synthetic_latency_us\":"<<backend->latency_us
  <<",\"parallelism\":"<<parallel<<",\"multi_range\":"<<(backend->multi_range?"true":"false")<<",\"runs\":[";
 bool comma=false;
 auto run=[&](const std::string& label,const std::string& path,const std::vector<std::uint64_t>& start,
              const std::vector<std::uint64_t>& count,std::size_t clients){
  auto field=make_hdf5_value_field(path);need(field.ok(),field.status().message());QueryRequest request;
  request.sources.paths={{PathScopeKind::Exact,key.path}};request.sources.versions=VersionSelection::exact_version(key.version);
  request.sources.formats={AdapterSelectionKind::Listed,{"hdf5"},false};request.subset=QueryFragment{};
  request.subset->projection={field.value()};request.limits.target_batch_rows=1000000;
  for(std::size_t a=0;a<count.size();++a)request.subset->slices.push_back({hdf5_axis_field(a),
   DomainBound{LogicalValue::uint64(start[a]),BoundKind::Inclusive},DomainBound{LogicalValue::uint64(start[a]+count[a]),BoundKind::Exclusive}});
  backend->phase="prepare";auto prepared=module.value()->prepare(request);need(prepared.ok(),prepared.status().message());
  backend->phase=label;backend->peak=0;auto calls=backend->calls.load(),batches=backend->batch_calls.load();
  const auto before=runtime->stats();const auto started=std::chrono::steady_clock::now();
  std::vector<std::future<BinaryValue>> pending;
  for(std::size_t i=0;i<clients;++i)pending.push_back(std::async(std::launch::async,[&]{return consume(*module.value(),prepared.value(),field.value());}));
  BinaryValue values=pending.front().get();for(std::size_t i=1;i<pending.size();++i)need(pending[i].get()==values,"concurrent results differ");
  const auto elapsed=std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-started).count();
  auto after=runtime->stats();save(output/(label+".bin"),values);
  if(comma)json<<',';comma=true;json<<"{\"label\":"<<q(label)<<",\"dataset\":"<<q(path)<<",\"origin\":";numbers(json,start);json<<",\"shape\":";numbers(json,count);
  json<<",\"clients\":"<<clients<<",\"wall_us\":"<<elapsed<<",\"calls\":"<<backend->calls-calls<<",\"batch_calls\":"<<backend->batch_calls-batches
   <<",\"active_calls_peak\":"<<backend->peak<<",\"active_calls_after\":"<<backend->active<<",\"decode_count\":"<<after.decode_count-before.decode_count
   <<",\"decoded_bytes\":"<<after.decoded_bytes-before.decoded_bytes<<",\"read_bytes\":"<<after.read_bytes-before.read_bytes
   <<",\"read_ranges\":"<<after.read_ranges-before.read_ranges<<",\"payload_calls\":"<<after.payload_read_calls-before.payload_read_calls
   <<",\"payload_read_nanoseconds\":"<<after.payload_read_nanoseconds-before.payload_read_nanoseconds
   <<",\"fragment_load_nanoseconds\":"<<after.fragment_load_nanoseconds-before.fragment_load_nanoseconds
   <<",\"decode_nanoseconds\":"<<after.decode_nanoseconds-before.decode_nanoseconds
   <<",\"decode_cpu_nanoseconds\":"<<after.decode_cpu_nanoseconds-before.decode_cpu_nanoseconds
   <<",\"decode_cpu_samples\":"<<after.decode_cpu_samples-before.decode_cpu_samples
   <<",\"copy_bytes\":"<<after.copy_bytes-before.copy_bytes<<",\"hits\":"<<after.hits-before.hits<<",\"waits\":"<<after.waits-before.waits
   <<",\"evictions\":"<<after.evictions-before.evictions<<",\"live_bytes\":"<<after.live_bytes<<",\"memory_peak_bytes\":"<<after.peak_bytes<<'}';
 };
 run("cold","/data",origin,shape,1);run("warm","/data",origin,shape,1);run("alias","/alias",origin,shape,1);
 auto overlap_origin=origin,overlap_shape=shape;overlap_origin[0]+=shape[0]/2;overlap_shape[0]-=shape[0]/2;
 run("overlap","/data",overlap_origin,overlap_shape,1);runtime->cache().evict_all();run("concurrent","/data",origin,shape,4);
 json<<"],\"reads\":[";for(std::size_t i=0;i<backend->reads.size();++i){if(i)json<<',';auto& r=backend->reads[i];json<<"{\"phase\":"<<q(r.phase)<<",\"kind\":"<<q(r.kind)<<",\"offset\":"<<r.offset<<",\"length\":"<<r.length<<'}';}
 json<<"]}\n";json.close();need(bool(json),"report write failed");return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
