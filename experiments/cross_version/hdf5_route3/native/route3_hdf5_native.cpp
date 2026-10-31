// Experimental C ABI: discovery is JSON; selected values are written directly
// into caller-owned arrays. No result hashing or transport is hidden in this ABI.
#include <hdf5.h>
#include <sqlite3.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>

namespace {
namespace fs=std::filesystem;
using Tree=boost::property_tree::ptree;
using Clock=std::chrono::steady_clock;
thread_local std::string error;
void check(bool ok,const std::string& message){if(!ok)throw std::runtime_error(message);}
std::uint64_t ns(Clock::time_point a){return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now()-a).count();}
Tree parse(const std::string& s){std::istringstream in(s);Tree t;boost::property_tree::read_json(in,t);return t;}
std::string quote(const std::string& s){std::ostringstream o;o<<'"';for(unsigned char c:s){if(c=='"'||c=='\\')o<<'\\'<<c;else if(c<32){const char*h="0123456789abcdef";o<<"\\u00"<<h[c>>4]<<h[c&15];}else o<<c;}return o.str()+'"';}
char* copied(const std::string& s){auto*p=static_cast<char*>(std::malloc(s.size()+1));check(p,"allocation failed");std::memcpy(p,s.c_str(),s.size()+1);return p;}
struct Hid{hid_t id=-1;herr_t(*close)(hid_t)=nullptr;Hid()=default;Hid(hid_t i,herr_t(*c)(hid_t)):id(i),close(c){check(i>=0,"HDF5 call failed");}~Hid(){if(id>=0)close(id);}Hid(const Hid&)=delete;Hid&operator=(const Hid&)=delete;operator hid_t()const{return id;}};
struct Source{std::string logical,path;std::int64_t ordinal=0,version=0;std::uint64_t size=0,mtime=0;};
struct Attrs{std::string name,type;std::int64_t level,step,date,time;};
Attrs predicate(const Tree&t){check(t.size()==6,"predicate requires exactly six equality attributes");return{t.get<std::string>("shortName"),t.get<std::string>("typeOfLevel"),t.get<std::int64_t>("level"),t.get<std::int64_t>("step"),t.get<std::int64_t>("validityDate"),t.get<std::int64_t>("validityTime")};}
bool matches(const Attrs&a,const Attrs&b){return std::tie(a.name,a.type,a.level,a.step,a.date,a.time)==std::tie(b.name,b.type,b.level,b.step,b.date,b.time);}
struct Stats{std::uint64_t stats=0,opens=0,dopens=0,attrs=0,candidates=0,scan_source_visits=0,reads=0,bytes=0,filehits=0,dataset_hits=0,select_ns=0,read_ns=0,build_ns=0,build_files=0,build_units=0,build_bytes=0;};
struct Statement{sqlite3_stmt*p=nullptr;Statement(sqlite3*d,const char*s){check(sqlite3_prepare_v2(d,s,-1,&p,nullptr)==SQLITE_OK,sqlite3_errmsg(d));}~Statement(){if(p)sqlite3_finalize(p);}void text(int i,const std::string&s){check(sqlite3_bind_text(p,i,s.c_str(),-1,SQLITE_TRANSIENT)==SQLITE_OK,"SQLite text bind failed");}void number(int i,std::int64_t n){check(sqlite3_bind_int64(p,i,n)==SQLITE_OK,"SQLite integer bind failed");}};
struct FileEntry{std::unique_ptr<Hid>id;std::uint64_t size,mtime,tick;};
struct DatasetEntry{std::unique_ptr<Hid>id;std::string file;std::uint64_t tick;};
struct Context{
 std::vector<Source>sources;std::map<std::string,std::size_t>path_source;std::map<std::string,std::int64_t>latest;
 std::vector<std::size_t>latest_sources;
 std::vector<std::pair<std::int64_t,std::size_t>>sorted_versions,sorted_ordinals;
 std::map<std::string,FileEntry>files;std::map<std::string,DatasetEntry>datasets;
 sqlite3*db=nullptr;std::map<std::string,std::unique_ptr<Statement>>queries;Stats st;std::uint64_t tick=0,metadata_bytes=2097152,raw_bytes=262144;
 std::size_t max_files=8,max_datasets=8;std::string index,catalog_text;
 ~Context(){datasets.clear();files.clear();queries.clear();if(db)sqlite3_close(db);}
 void sql(const char*s){check(sqlite3_exec(db,s,nullptr,nullptr,nullptr)==SQLITE_OK,sqlite3_errmsg(db));}
 std::pair<std::uint64_t,std::uint64_t> identity(const std::string&path){
  ++st.stats;struct stat s{};check(::lstat(path.c_str(),&s)==0&&S_ISREG(s.st_mode),"source must be a readable regular file: "+path);
  check(::access(path.c_str(),R_OK)==0,"source unreadable: "+path);
  return{std::uint64_t(s.st_size),std::uint64_t(s.st_mtim.tv_sec)*1000000000ULL+s.st_mtim.tv_nsec};
 }
 void validate(const Source&s){auto i=identity(s.path);check(i.first==s.size&&i.second==s.mtime,"stale indexed/catalog source: "+s.path);}
 hid_t file(const std::string&path){
  auto identity_now=identity(path);auto old=files.find(path);
  if(old!=files.end()){
   if(old->second.size!=identity_now.first||old->second.mtime!=identity_now.second){
    for(auto it=datasets.begin();it!=datasets.end();)if(it->second.file==path)it=datasets.erase(it);else ++it;
    files.erase(old);throw std::runtime_error("source changed while native handle was cached: "+path);
   }++st.filehits;old->second.tick=++tick;return *old->second.id;
  }
  if(files.size()>=max_files){auto victim=std::min_element(files.begin(),files.end(),[](const auto&a,const auto&b){return a.second.tick<b.second.tick;});
   for(auto it=datasets.begin();it!=datasets.end();)if(it->second.file==victim->first)it=datasets.erase(it);else ++it;files.erase(victim);}
  Hid access(H5Pcreate(H5P_FILE_ACCESS),H5Pclose);
  H5AC_cache_config_t mdc{};mdc.version=H5AC__CURR_CACHE_CONFIG_VERSION;check(H5Pget_mdc_config(access,&mdc)>=0,"metadata cache config read failed");
  mdc.set_initial_size=1;mdc.initial_size=std::min<std::uint64_t>(262144,metadata_bytes);mdc.min_size=65536;mdc.max_size=metadata_bytes;
  check(H5Pset_mdc_config(access,&mdc)>=0,"metadata cache config failed");
  check(H5Pset_cache(access,0,521,raw_bytes,0.75)>=0,"file raw cache config failed");
  auto id=std::make_unique<Hid>(H5Fopen(path.c_str(),H5F_ACC_RDONLY,access),H5Fclose);++st.opens;
  auto result=id->id;files.emplace(path,FileEntry{std::move(id),identity_now.first,identity_now.second,++tick});return result;
 }
 hid_t dataset(const std::string&path,const std::string&name){
  auto f=file(path);const auto key=path+'\n'+name;auto found=datasets.find(key);
  if(found!=datasets.end()){++st.dataset_hits;found->second.tick=++tick;return *found->second.id;}
  if(datasets.size()>=max_datasets){auto victim=std::min_element(datasets.begin(),datasets.end(),[](const auto&a,const auto&b){return a.second.tick<b.second.tick;});datasets.erase(victim);}
  Hid access(H5Pcreate(H5P_DATASET_ACCESS),H5Pclose);check(H5Pset_chunk_cache(access,521,raw_bytes,0.75)>=0,"dataset raw cache config failed");
  auto id=std::make_unique<Hid>(H5Dopen2(f,name.c_str(),access),H5Dclose);++st.dopens;auto result=id->id;
  datasets.emplace(key,DatasetEntry{std::move(id),path,++tick});return result;
 }
 std::string string_attr(hid_t d,const char*name){++st.attrs;Hid a(H5Aopen(d,name,H5P_DEFAULT),H5Aclose),t(H5Aget_type(a),H5Tclose),s(H5Aget_space(a),H5Sclose);
  auto n=H5Tget_size(t);check(H5Tget_class(t)==H5T_STRING&&!H5Tis_variable_str(t)&&n>0&&n<=1024&&H5Sget_simple_extent_npoints(s)==1,"invalid scalar string attribute");
  std::vector<char>b(n+1,0);check(H5Aread(a,t,b.data())>=0,"attribute read failed");return std::string(b.data(),strnlen(b.data(),n));}
 std::int64_t integer_attr(hid_t d,const char*name){++st.attrs;Hid a(H5Aopen(d,name,H5P_DEFAULT),H5Aclose),t(H5Aget_type(a),H5Tclose),s(H5Aget_space(a),H5Sclose);
  check(H5Tget_class(t)==H5T_INTEGER&&H5Tget_sign(t)==H5T_SGN_2&&H5Tget_size(t)<=8&&H5Sget_simple_extent_npoints(s)==1,"invalid signed scalar attribute");
  long long n;check(H5Aread(a,H5T_NATIVE_LLONG,&n)>=0,"attribute read failed");return n;}
 Attrs attrs(hid_t d){return{string_attr(d,"shortName"),string_attr(d,"typeOfLevel"),integer_attr(d,"level"),integer_attr(d,"step"),integer_attr(d,"validityDate"),integer_attr(d,"validityTime")};}
 std::string stats_json()const{std::ostringstream o;o<<"{";
 #define FIELD(k,v) o<<quote(k)<<":"<<(v)<<",";
 FIELD("stat_calls",st.stats) FIELD("file_opens",st.opens) FIELD("dataset_opens",st.dopens) FIELD("attribute_reads",st.attrs)
 FIELD("candidate_datasets",st.candidates) FIELD("scan_source_visits",st.scan_source_visits) FIELD("read_calls",st.reads) FIELD("value_bytes",st.bytes) FIELD("file_cache_hits",st.filehits) FIELD("dataset_cache_hits",st.dataset_hits)
 FIELD("select_ns",st.select_ns) FIELD("read_ns",st.read_ns) FIELD("index_build_ns",st.build_ns) FIELD("index_build_files",st.build_files) FIELD("index_build_units",st.build_units) FIELD("index_bytes",st.build_bytes)
 FIELD("resident_files",files.size()) FIELD("resident_datasets",datasets.size()) FIELD("metadata_cache_bytes_per_file",metadata_bytes)
 #undef FIELD
 o<<"\"raw_chunk_cache_bytes_per_dataset\":"<<raw_bytes<<"}";return o.str();}
};
struct Visitation{Context*c;const Source*s;std::vector<std::pair<std::string,Attrs>>*units;std::exception_ptr exception;};
herr_t visit(hid_t,const char*name,const H5O_info2_t*info,void*p){auto&v=*static_cast<Visitation*>(p);if(info->type!=H5O_TYPE_DATASET)return 0;
 try{check(v.units->size()<262144,"dataset count exceeds bound");++v.c->st.candidates;std::string path="/"+std::string(name);auto d=v.c->dataset(v.s->path,path);v.units->emplace_back(path,v.c->attrs(d));return 0;}catch(...){v.exception=std::current_exception();return-1;}}
std::vector<std::pair<std::string,Attrs>> inventory(Context&c,const Source&s){c.validate(s);std::vector<std::pair<std::string,Attrs>>u;Visitation v{&c,&s,&u,{}};auto f=c.file(s.path);auto rc=H5Ovisit3(f,H5_INDEX_NAME,H5_ITER_INC,visit,&v,H5O_INFO_BASIC);if(v.exception)std::rethrow_exception(v.exception);check(rc>=0,"dataset scan failed");return u;}
void open_index(Context&c){if(c.db)return;check(sqlite3_open_v2(c.index.c_str(),&c.db,SQLITE_OPEN_READONLY,nullptr)==SQLITE_OK,"cannot open index");
 Statement s(c.db,"SELECT catalog FROM identity");check(sqlite3_step(s.p)==SQLITE_ROW,"missing index identity");auto*t=sqlite3_column_text(s.p,0);check(t&&std::string(reinterpret_cast<const char*>(t))==c.catalog_text,"index source catalog identity mismatch");check(sqlite3_step(s.p)==SQLITE_DONE,"duplicate index identity");}
void build_index(Context&c){check(!fs::exists(c.index),"index exists");auto start=Clock::now();check(sqlite3_open_v2(c.index.c_str(),&c.db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,nullptr)==SQLITE_OK,"index create failed");
 c.sql("PRAGMA synchronous=FULL; BEGIN IMMEDIATE; CREATE TABLE identity(catalog TEXT NOT NULL); CREATE TABLE units(source INTEGER NOT NULL,dataset TEXT NOT NULL,name TEXT,type TEXT,level INTEGER,step INTEGER,date INTEGER,time INTEGER,ordinal INTEGER,version INTEGER,logical TEXT); CREATE TABLE sources(source INTEGER PRIMARY KEY,logical TEXT,ordinal INTEGER,version INTEGER);");
 {Statement s(c.db,"INSERT INTO identity VALUES(?)");s.text(1,c.catalog_text);check(sqlite3_step(s.p)==SQLITE_DONE,"identity insert failed");}
 Statement insert(c.db,"INSERT INTO units VALUES(?,?,?,?,?,?,?,?,?,?,?)"),si(c.db,"INSERT INTO sources VALUES(?,?,?,?)");
 for(std::size_t i=0;i<c.sources.size();++i){const auto&s=c.sources[i];si.number(1,i);si.text(2,s.logical);si.number(3,s.ordinal);si.number(4,s.version);check(sqlite3_step(si.p)==SQLITE_DONE,"source insert failed");sqlite3_reset(si.p);
  auto units=inventory(c,s);++c.st.build_files;for(const auto&u:units){insert.number(1,i);insert.text(2,u.first);insert.text(3,u.second.name);insert.text(4,u.second.type);insert.number(5,u.second.level);insert.number(6,u.second.step);insert.number(7,u.second.date);insert.number(8,u.second.time);insert.number(9,s.ordinal);insert.number(10,s.version);insert.text(11,s.logical);check(sqlite3_step(insert.p)==SQLITE_DONE,"unit insert failed");sqlite3_reset(insert.p);++c.st.build_units;}}
 c.sql("CREATE INDEX predicate_version ON units(name,type,level,step,date,time,version,logical,dataset); CREATE INDEX predicate_ordinal ON units(name,type,level,step,date,time,ordinal,logical,dataset); CREATE INDEX version_predicate ON units(version,name,type,level,step,date,time); CREATE INDEX latest_source ON sources(logical,version DESC); COMMIT;");
 // Prepared insert objects still live: close_v2 defers destruction safely.
 sqlite3_close_v2(c.db);c.db=nullptr;c.st.build_ns=ns(start);c.st.build_bytes=fs::file_size(c.index);
 c.datasets.clear();c.files.clear();open_index(c);
}
struct Selection{std::string kind;std::int64_t lower=0,upper=0;const std::map<std::string,std::int64_t>*latest=nullptr;};
Selection selection(Context&c,const Tree&t){Selection s;s.kind=t.get<std::string>("kind");check(s.kind=="latest"||s.kind=="ordinal"||s.kind=="between","unknown selection kind");
 if(s.kind=="latest")s.latest=&c.latest;
 else{s.lower=t.get<std::int64_t>("lower");s.upper=t.get<std::int64_t>("upper");check(s.lower>=0&&s.upper>s.lower,"invalid half-open selection");}return s;}
bool selected(const Source&f,const Selection&s){if(s.kind=="latest")return s.latest->at(f.logical)==f.version;auto n=s.kind=="ordinal"?f.ordinal:f.version;return s.lower<=n&&n<s.upper;}
std::string unit_json(const Source&s,const std::string&d){std::ostringstream o;o<<"{\"logical_path\":"<<quote(s.logical)<<",\"ordinal\":"<<s.ordinal<<",\"version\":"<<s.version<<",\"path\":"<<quote(s.path)<<",\"dataset\":"<<quote(d)<<",\"size\":"<<s.size<<",\"mtime_ns\":"<<s.mtime<<"}";return o.str();}
std::string select(Context&c,const Tree&t){auto start=Clock::now();auto s=selection(c,t.get_child("selection"));auto p=predicate(t.get_child("predicate"));auto mode=t.get<std::string>("mode");check(mode=="scan"||mode=="index","invalid selection mode");std::vector<std::pair<std::size_t,std::string>>matches_out;std::size_t sources=0;std::uint64_t candidates=0;
 std::vector<std::size_t>scan_ids;
 if(s.kind=="latest"){sources=c.latest_sources.size();if(mode=="scan")scan_ids=c.latest_sources;}
 else{const auto&v=s.kind=="ordinal"?c.sorted_ordinals:c.sorted_versions;
  const auto begin=std::lower_bound(v.begin(),v.end(),std::make_pair(s.lower,std::size_t(0)));
  const auto end=std::lower_bound(v.begin(),v.end(),std::make_pair(s.upper,std::size_t(0)));
  sources=end-begin;if(mode=="scan"){scan_ids.reserve(sources);for(auto it=begin;it!=end;++it)scan_ids.push_back(it->second);}}
 if(mode=="scan"){
  for(auto i:scan_ids){++c.st.scan_source_visits;const auto&f=c.sources[i];auto units=inventory(c,f);candidates+=units.size();for(const auto&u:units)if(matches(u.second,p))matches_out.emplace_back(i,u.first);}
 }else{open_index(c);std::string sql="SELECT source,dataset FROM units WHERE name=? AND type=? AND level=? AND step=? AND date=? AND time=?";
  if(s.kind=="latest")sql+=" AND version=(SELECT MAX(s.version) FROM sources s WHERE s.logical=units.logical)";else sql+=s.kind=="ordinal"?" AND ordinal>=? AND ordinal<?":" AND version>=? AND version<?";
  sql+=" ORDER BY logical,version,dataset";auto&cached=c.queries[s.kind];if(!cached)cached=std::make_unique<Statement>(c.db,sql.c_str());auto&q=*cached;sqlite3_reset(q.p);sqlite3_clear_bindings(q.p);q.text(1,p.name);q.text(2,p.type);q.number(3,p.level);q.number(4,p.step);q.number(5,p.date);q.number(6,p.time);if(s.kind!="latest"){q.number(7,s.lower);q.number(8,s.upper);}
  while(true){auto rc=sqlite3_step(q.p);if(rc==SQLITE_DONE)break;check(rc==SQLITE_ROW,"index query failed");auto n=sqlite3_column_int64(q.p,0);check(n>=0&&std::uint64_t(n)<c.sources.size(),"invalid indexed source");const auto&f=c.sources[n];check(selected(f,s),"index selected incorrect version");c.validate(f);auto*d=sqlite3_column_text(q.p,1);check(d,"null indexed dataset");matches_out.emplace_back(n,reinterpret_cast<const char*>(d));++candidates;}sqlite3_reset(q.p);
 }
 std::sort(matches_out.begin(),matches_out.end(),[&](const auto&a,const auto&b){const auto&x=c.sources[a.first];const auto&y=c.sources[b.first];return std::tie(x.logical,x.version,a.second)<std::tie(y.logical,y.version,b.second);});
 c.st.select_ns+=ns(start);std::ostringstream o;o<<"{\"selected_sources\":"<<sources<<",\"candidate_datasets\":"<<candidates<<",\"units\":[";bool first=true;for(const auto&u:matches_out){if(!first)o<<',';first=false;o<<unit_json(c.sources[u.first],u.second);}o<<"],\"stats\":"<<c.stats_json()<<"}";return o.str();}
}

extern "C" {
const char* route3_last_error(){return error.c_str();}
void route3_result_free(void*p){std::free(p);}
void route3_context_free(void*p){delete static_cast<Context*>(p);}
void* route3_context_create(const char*json){try{error.clear();check(json,"missing config");auto t=parse(json);check(t.get<std::string>("protocol")=="iotdbfs-route3-native/1","config protocol mismatch");
 unsigned a,b,c;hbool_t safe;check(H5get_libversion(&a,&b,&c)>=0&&a==1&&b==14&&c==4,"HDF5 1.14.4 required");check(H5is_library_threadsafe(&safe)>=0&&safe,"threadsafe HDF5 required");check(H5PLset_loading_state(0)>=0,"plugin disable failed");H5Eset_auto2(H5E_DEFAULT,nullptr,nullptr);
 for(const auto*k:{"HDF5_DRIVER","HDF5_VOL_CONNECTOR"})check(!std::getenv(k)||!std::getenv(k)[0],"HDF5 driver/VOL overrides forbidden");
 auto out=std::make_unique<Context>();out->max_files=t.get<std::size_t>("cache_files",8);out->max_datasets=t.get<std::size_t>("cache_datasets",8);out->metadata_bytes=t.get<std::uint64_t>("metadata_cache_bytes",2097152);out->raw_bytes=t.get<std::uint64_t>("raw_chunk_cache_bytes",262144);
 check(out->max_files>0&&out->max_files<=8&&out->max_datasets>0&&out->max_datasets<=8&&out->metadata_bytes>=262144&&out->metadata_bytes<=2097152&&out->raw_bytes>=1&&out->raw_bytes<=262144,"native cache budget exceeds protocol");
 auto cp=t.get<std::string>("catalog_path");check(fs::path(cp).is_absolute(),"catalog path must be absolute");std::ifstream in(cp);check(bool(in),"catalog open failed");std::ostringstream data;data<<in.rdbuf();out->catalog_text=data.str();check(out->catalog_text.size()<=64ULL*1024*1024,"catalog exceeds 64 MiB");auto cat=parse(out->catalog_text);check(cat.get<std::string>("protocol")=="iotdbfs-route3-publication/1","catalog protocol mismatch");
 std::map<std::pair<std::string,std::int64_t>,bool>unique;
 for(const auto&x:cat.get_child("sources")){const auto&v=x.second;Source s{v.get<std::string>("logical_path"),v.get<std::string>("path"),v.get<std::int64_t>("ordinal"),v.get<std::int64_t>("version"),v.get<std::uint64_t>("size"),v.get<std::uint64_t>("mtime_ns")};check(!s.logical.empty()&&fs::path(s.path).is_absolute()&&fs::path(s.path).lexically_normal()==fs::path(s.path)&&s.ordinal>=1&&s.version>=1,"invalid catalog source");check(unique.emplace(std::make_pair(s.logical,s.version),true).second&&out->path_source.emplace(s.path,out->sources.size()).second,"duplicate catalog source");check(out->sources.size()<131072,"catalog source count exceeds bound");out->sources.push_back(std::move(s));}
 std::map<std::string,std::size_t>latest_ids;
 for(std::size_t i=0;i<out->sources.size();++i){const auto&s=out->sources[i];auto old=out->latest.find(s.logical);if(old==out->latest.end()||old->second<s.version){out->latest[s.logical]=s.version;latest_ids[s.logical]=i;}out->sorted_versions.emplace_back(s.version,i);out->sorted_ordinals.emplace_back(s.ordinal,i);}
 for(const auto&entry:latest_ids)out->latest_sources.push_back(entry.second);
 std::sort(out->sorted_versions.begin(),out->sorted_versions.end());std::sort(out->sorted_ordinals.begin(),out->sorted_ordinals.end());
 out->index=t.get<std::string>("index_path","");if(!out->index.empty())check(fs::path(out->index).is_absolute(),"index path must be absolute");if(t.get<bool>("build_index",false)){check(!out->index.empty(),"index path required");build_index(*out);}return out.release();
 }catch(const std::exception&e){error=e.what();return nullptr;}catch(...){error="unexpected native create error";return nullptr;}}
void* route3_select_json(void*p,const char*request){try{error.clear();check(p&&request,"missing selection context/request");return copied(select(*static_cast<Context*>(p),parse(request)));}catch(const std::exception&e){error=e.what();return nullptr;}catch(...){error="unexpected native select error";return nullptr;}}
void* route3_stats_json(void*p){try{error.clear();check(p,"missing context");return copied(static_cast<Context*>(p)->stats_json());}catch(const std::exception&e){error=e.what();return nullptr;}}
int route3_read_into(void*p,const char*path,const char*dataset,const std::uint64_t*origin,const std::uint64_t*shape,float*values,std::uint8_t*mask){try{error.clear();check(p&&path&&dataset&&origin&&shape&&values&&mask,"missing read argument");auto&c=*static_cast<Context*>(p);auto start=Clock::now();
 check(fs::path(path).is_absolute()&&fs::path(path).lexically_normal()==fs::path(path)&&dataset[0]=='/',"read paths must be absolute normalized");auto binding=c.path_source.find(path);if(binding!=c.path_source.end())c.validate(c.sources[binding->second]);
 auto d=c.dataset(path,dataset);Hid type(H5Dget_type(d),H5Tclose),space(H5Dget_space(d),H5Sclose);check(H5Tget_class(type)==H5T_FLOAT&&H5Tget_size(type)==4,"reader requires Float32");check(H5Sget_simple_extent_ndims(space)==4,"reader requires rank four");std::array<hsize_t,4>extent{},start4{},count{};check(H5Sget_simple_extent_dims(space,extent.data(),nullptr)>=0,"extent read failed");std::uint64_t n=1;
 for(int i=0;i<4;++i){check(shape[i]>0&&origin[i]<=extent[i]&&shape[i]<=extent[i]-origin[i],"ROI outside dataset");check(shape[i]<=67108864ULL/5/n,"ROI exceeds 64 MiB owned value+mask bound");n*=shape[i];start4[i]=origin[i];count[i]=shape[i];}
 check(H5Sselect_hyperslab(space,H5S_SELECT_SET,start4.data(),nullptr,count.data(),nullptr)>=0,"hyperslab failed");Hid memory(H5Screate_simple(4,count.data(),nullptr),H5Sclose);check(H5Dread(d,H5T_NATIVE_FLOAT,memory,space,H5P_DEFAULT,values)>=0,"H5Dread failed");for(std::uint64_t i=0;i<n;++i){check(std::isfinite(values[i]),"controlled value must be finite");mask[i]=1;}++c.st.reads;c.st.bytes+=n*5;c.st.read_ns+=ns(start);return 0;
 }catch(const std::exception&e){error=e.what();return-1;}catch(...){error="unexpected native read error";return-1;}}
}
