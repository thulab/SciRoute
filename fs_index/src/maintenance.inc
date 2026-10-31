// Included once by native.cpp: reuse the native scientific profile, not a second reader.
#include "../include/fs_index.h"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/syscall.h>
#include <linux/fs.h>
#include <signal.h>
#include <cerrno>

namespace {
constexpr int maintenance_app = 0x46534932;
constexpr const char* maintenance_protocol = "iotdbfs-fs-index/2";
#ifdef FS_INDEX_TEST_FAULTS
constexpr bool test_faults_enabled = true;
#else
constexpr bool test_faults_enabled = false;
#endif
struct Fd {
 int id=-1;
 explicit Fd(int value):id(value){check(id>=0,"file open: "+std::string(std::strerror(errno)));}
 ~Fd(){if(id>=0)::close(id);}
 Fd(const Fd&)=delete;
};
struct Transaction {
 Context& c; bool active=true;
 explicit Transaction(Context& value,bool write=false):c(value){c.sql(write?"BEGIN IMMEDIATE":"BEGIN");}
 ~Transaction(){if(active)sqlite3_exec(c.db,"ROLLBACK",nullptr,nullptr,nullptr);}
 void commit(){c.sql("COMMIT");active=false;}
};
std::string column_text(sqlite3_stmt*p,int column){auto*s=sqlite3_column_text(p,column);check(s,"unexpected SQL NULL");return reinterpret_cast<const char*>(s);}
void done(Statement&q){auto rc=sqlite3_step(q.p);check(rc==SQLITE_DONE,sqlite3_errmsg(sqlite3_db_handle(q.p)));}
std::int64_t scalar(Context&c,const std::string&sql){Statement q(c.db,sql);check(sqlite3_step(q.p)==SQLITE_ROW,"scalar query failed");return sqlite3_column_int64(q.p,0);}
void sync_directory(const fs::path&p){Fd d(::open(p.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW));check(::fsync(d.id)==0,"directory fsync failed");}
std::uint64_t disk_size(const fs::path&p){std::error_code ec;auto n=fs::file_size(p,ec);return ec?0:n;}
std::string hex_bytes(const unsigned char*p,std::size_t n){static const char*h="0123456789abcdef";std::string out;out.reserve(n*2);for(std::size_t i=0;i<n;++i){out+=h[p[i]>>4];out+=h[p[i]&15];}return out;}
struct Sha {
 std::unique_ptr<EVP_MD_CTX,decltype(&EVP_MD_CTX_free)> p{EVP_MD_CTX_new(),EVP_MD_CTX_free};
 Sha(){check(p&&EVP_DigestInit_ex(p.get(),EVP_sha256(),nullptr)==1,"SHA init");}
 void add(const void*b,std::size_t n){check(EVP_DigestUpdate(p.get(),b,n)==1,"SHA update");}
 std::string finish(){unsigned char b[EVP_MAX_MD_SIZE];unsigned n=0;check(EVP_DigestFinal_ex(p.get(),b,&n)==1&&n==32,"SHA final");return hex_bytes(b,n);}
};
std::string sha_text(const std::string&s){Sha h;h.add(s.data(),s.size());return h.finish();}
std::string copy_hash(const fs::path&source,int destination=-1){
 Fd in(::open(source.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));struct stat before{},after{};
 check(fstat(in.id,&before)==0&&S_ISREG(before.st_mode),"input must be a regular file");
 Sha hash;std::array<char,65536>buffer{};
 while(true){auto n=::read(in.id,buffer.data(),buffer.size());if(n<0&&errno==EINTR)continue;check(n>=0,"source read failed");if(!n)break;hash.add(buffer.data(),n);
  for(ssize_t pos=0;destination>=0&&pos<n;){auto k=::write(destination,buffer.data()+pos,n-pos);if(k<0&&errno==EINTR)continue;check(k>0,"staging write failed");pos+=k;}}
 check(fstat(in.id,&after)==0&&before.st_size==after.st_size&&before.st_mtim.tv_sec==after.st_mtim.tv_sec&&before.st_mtim.tv_nsec==after.st_mtim.tv_nsec&&before.st_ctim.tv_sec==after.st_ctim.tv_sec&&before.st_ctim.tv_nsec==after.st_ctim.tv_nsec,"input changed during copy/hash");
 return hash.finish();
}
void fault(const char*point){
#ifdef FS_INDEX_TEST_FAULTS
 const char*configured=std::getenv("FS_INDEX_TEST_KILL_AT");if(configured&&std::string(configured)==point)::kill(::getpid(),SIGKILL);
#else
 (void)point;
#endif
}
void native_environment(){
 unsigned a,b,d;hbool_t safe;
 check(H5get_libversion(&a,&b,&d)>=0&&a==1&&b==14&&d==4,"HDF5 1.14.4 required");
 check(H5is_library_threadsafe(&safe)>=0&&safe,"threadsafe HDF5 required");
 for(const auto*k:{"HDF5_DRIVER","HDF5_VOL_CONNECTOR"})check(!std::getenv(k)||!std::getenv(k)[0],"HDF5 overrides forbidden");
 check(H5PLset_loading_state(0)>=0,"plugin disable failed");H5Eset_auto2(H5E_DEFAULT,nullptr,nullptr);
}
bool absolute_clean(const std::string&p){return !p.empty()&&p.find('\0')==std::string::npos&&fs::path(p).is_absolute()&&fs::path(p).lexically_normal()==fs::path(p);}
struct Maintenance {
 // Declared before core, so the database closes before the writer lock releases.
 std::unique_ptr<Fd> lock;
 Context core;
 fs::path root,objects;
 bool writer=false,indexed=true;
 std::string store_id;
 std::uint64_t generation=0,refresh_ns=0,refresh_sources=0,wal_peak=0,recovered_staging=0;
 std::vector<std::uint64_t>source_generations{0};
 void reset_queries(){for(auto&q:core.queries)if(q.second)q.second->reset();}
 void refresh(){
  auto start=Clock::now();auto head=scalar(core,"SELECT generation FROM head WHERE singleton=1");
  check(head>=0&&std::uint64_t(head)>=generation,"catalog generation moved backwards");
  if(std::uint64_t(head)!=generation){
   Statement q(core.db,"SELECT source,logical,ordinal,version,path,format,size,mtime_ns,generation FROM sources WHERE source>? ORDER BY source");
   q.integer(1,core.sources.size()-1);
   std::vector<std::pair<Source,std::uint64_t>>added;
   while(true){auto rc=sqlite3_step(q.p);if(rc==SQLITE_DONE)break;check(rc==SQLITE_ROW,"source refresh failed");
    check(sqlite3_column_int64(q.p,0)==std::int64_t(core.sources.size()+added.size()),"noncontiguous source ID");
    Source s{column_text(q.p,1),column_text(q.p,4),column_text(q.p,5),sqlite3_column_int64(q.p,2),sqlite3_column_int64(q.p,3),std::uint64_t(sqlite3_column_int64(q.p,6)),std::uint64_t(sqlite3_column_int64(q.p,7))};
    auto born=sqlite3_column_int64(q.p,8);check(born>0&&born<=head&&absolute_clean(s.path)&&fs::path(s.path).parent_path()==objects,"invalid stored source");
    check(s.version>0&&s.ordinal>0&&(s.format=="grib"||s.format=="hdf5")&&absolute_clean(s.logical),"invalid source metadata");
    added.emplace_back(std::move(s),born);
   }
   for(auto&entry:added){auto&s=entry.first;auto sid=core.sources.size();check(core.native_source.emplace(s.path,sid).second,"duplicate stored path");core.logical_sources[s.logical].push_back(sid);core.latest[s.logical]=std::max(core.latest[s.logical],s.version);core.sources.push_back(std::move(s));source_generations.push_back(entry.second);}
   refresh_sources+=added.size();generation=head;
  }
  refresh_ns+=ns(start);
 }
};
void create_schema(Maintenance&m){
 auto&c=m.core;unsigned char random[16];check(RAND_bytes(random,sizeof(random))==1,"store identity generation failed");m.store_id=hex_bytes(random,sizeof(random));
 Transaction tx(c,true);
 c.sql("PRAGMA application_id=1179863346; PRAGMA user_version=2;"
  "CREATE TABLE head(singleton INTEGER PRIMARY KEY CHECK(singleton=1),generation INTEGER NOT NULL,store_id TEXT NOT NULL,indexed INTEGER NOT NULL);"
  "CREATE TABLE sources(source INTEGER PRIMARY KEY AUTOINCREMENT,logical TEXT NOT NULL,ordinal INTEGER NOT NULL,version INTEGER NOT NULL,path TEXT NOT NULL UNIQUE,format TEXT NOT NULL,size INTEGER NOT NULL,mtime_ns INTEGER NOT NULL,sha256 TEXT NOT NULL,generation INTEGER NOT NULL UNIQUE,UNIQUE(logical,version),UNIQUE(logical,ordinal));"
  "CREATE TABLE units(source INTEGER NOT NULL REFERENCES sources(source),dataset TEXT,offset INTEGER,length INTEGER,shape TEXT,name TEXT,type TEXT,level INTEGER,step INTEGER,date INTEGER,time INTEGER,ordinal INTEGER,version INTEGER,logical TEXT);"
  "CREATE TABLE latest(logical TEXT PRIMARY KEY,version INTEGER NOT NULL) WITHOUT ROWID;"
  "CREATE TABLE operations(op_id TEXT PRIMARY KEY,request_hash TEXT NOT NULL,result TEXT NOT NULL) WITHOUT ROWID;"
  "CREATE INDEX predicate_ordinal ON units(name,type,level,step,date,time,ordinal,logical,version);"
  "CREATE INDEX predicate_version ON units(name,type,level,step,date,time,logical,version);"
  "CREATE INDEX source_logical ON sources(logical,version);"
  "CREATE INDEX source_generation ON sources(generation);");
 Statement q(c.db,"INSERT INTO head VALUES(1,0,?,?)");q.text(1,m.store_id);q.integer(2,m.indexed);done(q);tx.commit();
 c.sql("ANALYZE");sync_directory(m.root);
}
std::unique_ptr<Maintenance> maintenance_open(const Tree&t){
 check(t.get<std::string>("protocol")==maintenance_protocol,"maintenance protocol mismatch");native_environment();
 auto m=std::make_unique<Maintenance>();m->writer=t.get<bool>("writer",false);bool create=t.get<bool>("create",false);
 auto root=t.get<std::string>("root");check(absolute_clean(root),"absolute normalized root required");m->root=root;m->objects=m->root/"objects";
 if(create){check(m->writer,"creation requires writer");check(fs::create_directory(m->root),"store root must be new");check(::chmod(m->root.c_str(),0700)==0,"root chmod failed");sync_directory(m->root.parent_path());}
 check(fs::is_directory(m->root)&&!fs::is_symlink(m->root)&&fs::canonical(m->root)==m->root,"store root is not a canonical directory");
 if(m->writer){m->lock=std::make_unique<Fd>(::open((m->root/"writer.lock").c_str(),O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW,0600));check(::flock(m->lock->id,LOCK_EX|LOCK_NB)==0,"another writer owns this store");}
 if(create){check(fs::create_directory(m->objects),"objects must be new");sync_directory(m->root);}
 check(fs::is_directory(m->objects)&&!fs::is_symlink(m->objects),"objects directory missing");
 m->core.index=(m->root/"index.sqlite").string();
 check(create||fs::is_regular_file(m->core.index),"catalog does not exist");check(!fs::is_symlink(m->core.index),"symlink catalog forbidden");
 int flags=m->writer?SQLITE_OPEN_READWRITE:SQLITE_OPEN_READONLY;if(create)flags|=SQLITE_OPEN_CREATE;
 check(sqlite3_open_v2(m->core.index.c_str(),&m->core.db,flags,nullptr)==SQLITE_OK,"catalog open failed");
 sqlite3_busy_timeout(m->core.db,5000);m->core.sql("PRAGMA foreign_keys=ON");
 if(!create)check(scalar(m->core,"PRAGMA application_id")==maintenance_app&&scalar(m->core,"PRAGMA user_version")==2,"unsupported catalog schema");
 if(m->writer){
  {Statement q(m->core.db,"PRAGMA journal_mode=WAL");check(sqlite3_step(q.p)==SQLITE_ROW&&column_text(q.p,0)=="wal","WAL required");}
  m->core.sql("PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=1000");check(scalar(m->core,"PRAGMA synchronous")==2,"FULL sync required");
 }
 m->indexed=t.get<bool>("indexed",true);
 if(create)create_schema(*m);
 {Statement q(m->core.db,"SELECT store_id,indexed FROM head WHERE singleton=1");check(sqlite3_step(q.p)==SQLITE_ROW,"head missing");m->store_id=column_text(q.p,0);m->indexed=sqlite3_column_int(q.p,1)!=0;}
 if(auto requested=t.get_optional<bool>("indexed"))check(*requested==m->indexed,"store index mode mismatch");
 m->core.sources.emplace_back(); // ID 0 is never a committed source.
 m->core.sql("CREATE TEMP TABLE scope(logical TEXT PRIMARY KEY) WITHOUT ROWID; CREATE TEMP TABLE requested_ordinals(ordinal INTEGER PRIMARY KEY)");
 {Transaction tx(m->core);m->refresh();tx.commit();}
 if(m->writer){bool changed=false;for(const auto&entry:fs::directory_iterator(m->objects)){
   auto name=entry.path().filename().string();if(name.rfind(".stage-",0)==0&&name.size()==71){check(entry.is_regular_file()&&!entry.is_symlink(),"invalid staging object");check(fs::remove(entry.path()),"staging recovery failed");++m->recovered_staging;changed=true;}}
  if(changed)sync_directory(m->objects);
 }
 return m;
}
std::string request_identity(const Tree&t,const std::string&digest){
 return sha_text(std::string(maintenance_protocol)+"\n"+quote(t.get<std::string>("logical_path"))+"\n"+quote(t.get<std::string>("format"))+"\n"+std::to_string(t.get<std::int64_t>("version"))+"\n"+std::to_string(t.get<std::int64_t>("ordinal"))+"\n"+std::to_string(t.get<std::int64_t>("expected_version"))+"\n"+digest);
}
std::pair<std::string,std::string> operation(Context&c,const std::string&id){Statement q(c.db,"SELECT request_hash,result FROM operations WHERE op_id=?");q.text(1,id);auto rc=sqlite3_step(q.p);if(rc==SQLITE_DONE)return{};check(rc==SQLITE_ROW,"operation lookup failed");return{column_text(q.p,0),column_text(q.p,1)};}
void check_previous(Context&c,const Tree&t){Statement q(c.db,"SELECT version,ordinal FROM sources WHERE logical=? ORDER BY version DESC LIMIT 1");q.text(1,t.get<std::string>("logical_path"));auto rc=sqlite3_step(q.p);check(rc==SQLITE_ROW||rc==SQLITE_DONE,"previous version lookup failed");auto version=rc==SQLITE_ROW?sqlite3_column_int64(q.p,0):0;auto ordinal=rc==SQLITE_ROW?sqlite3_column_int64(q.p,1):0;check(version==t.get<std::int64_t>("expected_version"),"expected version conflict");check(t.get<std::int64_t>("version")>version&&t.get<std::int64_t>("ordinal")>ordinal,"version/ordinal must increase");}
struct StageCleanup {fs::path path;~StageCleanup(){std::error_code ec;fs::remove(path,ec);}};
herr_t physical_link(hid_t,const char*,const H5L_info2_t*info,void*){
 return info->type==H5L_TYPE_HARD?0:-1;
}
void physical_hdf(Context&c,const Source&s){
 auto&f=c.file(s);
 check(H5Lvisit2(*f.hdf,H5_INDEX_NAME,H5_ITER_INC,physical_link,nullptr)>=0,"maintenance requires file-local hard links");
}
void physical_dataset(Context&c,const Source&s,const std::string&name){
 Hid properties(H5Dget_create_plist(c.dataset(s,name)),H5Pclose);
 check(H5Pget_layout(properties)!=H5D_VIRTUAL&&H5Pget_external_count(properties)==0,"maintenance requires file-local physical datasets");
}
std::string publish(Maintenance&m,const Tree&t){
 check(m.writer,"publish requires writer");auto started=Clock::now();auto&c=m.core;
 std::string input=t.get<std::string>("input"),logical=t.get<std::string>("logical_path"),fmt=t.get<std::string>("format"),op=t.get<std::string>("op_id");
 auto version=t.get<std::int64_t>("version"),ordinal=t.get<std::int64_t>("ordinal");
 check(absolute_clean(input)&&absolute_clean(logical)&&version>0&&ordinal>0&&t.get<std::int64_t>("expected_version")>=0,"invalid publication identity");
 check((fmt=="grib"||fmt=="hdf5")&&!op.empty()&&op.size()<=1024&&op.find('\0')==std::string::npos,"invalid format/operation ID");
 auto prior=operation(c,op);
 if(!prior.first.empty()){auto digest=copy_hash(input);check(request_identity(t,digest)==prior.first,"operation request conflict");return "{\"replayed\":true,\"result\":"+prior.second+",\"metrics\":{\"total_ns\":"+std::to_string(ns(started))+",\"extracted_files\":0,\"extracted_units\":0}}";}
 check_previous(c,t);
 auto stage=m.objects/(".stage-"+sha_text(op));StageCleanup cleanup{stage};
 std::uint64_t copy_ns=0,file_sync_ns=0,finalize_ns=0,dir_sync_ns=0,extract_ns=0,commit_ns=0;
 std::string digest;{
  Fd out(::open(stage.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600));auto start=Clock::now();digest=copy_hash(input,out.id);copy_ns=ns(start);
  check(::fchmod(out.id,0444)==0,"object chmod failed");start=Clock::now();check(::fsync(out.id)==0,"file fsync failed");file_sync_ns=ns(start);fault("file_sync");
 }
 auto fingerprint=request_identity(t,digest);auto final=m.objects/(sha_text(op)+"-"+fingerprint+"."+fmt);
 auto start=Clock::now();
 auto rc=::syscall(SYS_renameat2,AT_FDCWD,stage.c_str(),AT_FDCWD,final.c_str(),RENAME_NOREPLACE);
 if(rc!=0){check(errno==EEXIST,"object rename failed");check(copy_hash(final)==digest,"orphan object differs");check(fs::remove(stage),"duplicate staging cleanup failed");}
 finalize_ns=ns(start);fault("rename");start=Clock::now();sync_directory(m.objects);dir_sync_ns=ns(start);fault("directory_sync");
 Context extraction;Source source;source.path=final.string();source.logical=logical;source.format=fmt;source.version=version;source.ordinal=ordinal;
 auto identity=extraction.identity(source.path);source.size=identity.first;source.mtime=identity.second;extraction.sources.push_back(source);extraction.native_source.emplace(source.path,0);
 std::vector<Unit>units;start=Clock::now();
 if(m.indexed){if(fmt=="hdf5")physical_hdf(extraction,source);units=inventory(extraction,0);check(!units.empty(),"source has no scientific units");std::array<float,4096>values;std::array<std::uint8_t,4096>mask;
  for(const auto&u:units){if(fmt=="hdf5")physical_dataset(extraction,source,u.dataset);read(extraction,parse(unit_json(extraction,u)),values.data(),mask.data(),values.size());}}
 extraction.validate(source);extract_ns=ns(start);
 std::string result;start=Clock::now();{
  Transaction tx(c,true);check(operation(c,op).first.empty(),"operation appeared under exclusive writer lock");check_previous(c,t);
  auto generation=scalar(c,"SELECT generation FROM head WHERE singleton=1")+1;
  Statement q(c.db,"INSERT INTO sources(logical,ordinal,version,path,format,size,mtime_ns,sha256,generation) VALUES(?,?,?,?,?,?,?,?,?)");
  q.text(1,logical);q.integer(2,ordinal);q.integer(3,version);q.text(4,source.path);q.text(5,fmt);q.integer(6,source.size);q.integer(7,source.mtime);q.text(8,digest);q.integer(9,generation);done(q);
  auto sid=sqlite3_last_insert_rowid(c.db);
  Statement unit(c.db,"INSERT INTO units VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
  for(const auto&u:units){unit.integer(1,sid);unit.text(2,u.dataset);unit.integer(3,u.offset);unit.integer(4,u.length);unit.text(5,shape_json(u.shape));unit.text(6,u.attrs.name);unit.text(7,u.attrs.type);unit.integer(8,u.attrs.level);unit.integer(9,u.attrs.step);unit.integer(10,u.attrs.date);unit.integer(11,u.attrs.time);unit.integer(12,ordinal);unit.integer(13,version);unit.text(14,logical);done(unit);unit.reset();}
  Statement latest(c.db,"INSERT INTO latest VALUES(?,?) ON CONFLICT(logical) DO UPDATE SET version=excluded.version");latest.text(1,logical);latest.integer(2,version);done(latest);
  Statement head(c.db,"UPDATE head SET generation=? WHERE singleton=1");head.integer(1,generation);done(head);
  result="{\"generation\":"+std::to_string(generation)+",\"source_id\":"+std::to_string(sid)+",\"version\":"+std::to_string(version)+",\"ordinal\":"+std::to_string(ordinal)+",\"units\":"+std::to_string(units.size())+",\"sha256\":"+quote(digest)+",\"native_path\":"+quote(source.path)+",\"store_id\":"+quote(m.store_id)+"}";
  Statement receipt(c.db,"INSERT INTO operations VALUES(?,?,?)");receipt.text(1,op);receipt.text(2,fingerprint);receipt.text(3,result);done(receipt);
  fault("before_commit");tx.commit();fault("after_commit");
 }commit_ns=ns(start);m.wal_peak=std::max(m.wal_peak,disk_size(c.index+"-wal"));
 std::ostringstream out;out<<"{\"replayed\":false,\"result\":"<<result<<",\"metrics\":{\"copy_hash_ns\":"<<copy_ns<<",\"file_sync_ns\":"<<file_sync_ns<<",\"finalize_ns\":"<<finalize_ns<<",\"directory_sync_ns\":"<<dir_sync_ns<<",\"extract_validate_ns\":"<<extract_ns<<",\"commit_ns\":"<<commit_ns<<",\"total_ns\":"<<ns(started)<<",\"extracted_files\":"<<(m.indexed?1:0)<<",\"extracted_units\":"<<units.size()<<",\"validation_reads\":"<<extraction.st.reads<<",\"extraction_file_opens\":"<<extraction.st.opens<<",\"copied_bytes\":"<<source.size<<"}}";return out.str();
}
std::string maintenance_select(Maintenance&m,const Tree&t,bool explain_only=false){
 check(!m.writer,"select requires reader");auto&c=m.core;auto mode=t.get<std::string>("mode");check(mode=="scan"||mode=="index","mode must be scan/index");check(m.indexed||mode=="scan","source-only catalog has no index");
 auto sel=selection(t.get_child("selection"));(void)predicate(t.get_child("predicate"));
 // Temp tables are populated before the main-database snapshot; no nested BEGIN.
 std::vector<std::string>scope;for(const auto&e:t.get_child("logical_scope")){auto p=e.second.get_value<std::string>();check(absolute_clean(p)&&(scope.empty()||scope.back()<p),"scope must be sorted unique absolute paths");scope.push_back(p);}check(!scope.empty(),"scope cannot be empty");
 try{
  if(mode=="index")index_scope(c,scope,sel);
  Transaction tx(c);m.refresh();
  if(mode=="scan")tx.commit(); // Native discovery must not pin a WAL read transaction.
  if(mode=="scan")for(const auto&p:scope){auto found=c.logical_sources.find(p);check(found!=c.logical_sources.end(),"unknown logical scope path");for(auto sid:found->second){const auto&s=c.sources[sid];if(s.format=="hdf5"&&selected(c,s,sel))physical_hdf(c,s);}}
  if(explain_only){auto result=explain(c,t);tx.commit();return result;}
  auto raw=select(c,t);if(mode=="index")tx.commit();
  auto result=parse(raw);std::ostringstream out;out<<"{\"generation\":"<<m.generation<<",\"selected_sources\":"<<result.get<std::uint64_t>("selected_sources")<<",\"units\":[";bool first=true;
  for(const auto&e:result.get_child("units")){const auto&v=e.second;auto sid=c.native_source.at(v.get<std::string>("native_path"));Unit u;u.source=sid;u.dataset=v.get<std::string>("dataset");u.offset=v.get<std::uint64_t>("offset");u.length=v.get<std::uint64_t>("length");u.shape=shape_tree(v.get_child("shape"));auto row=unit_json(c,u);row.pop_back();if(!first)out<<',';first=false;out<<row<<",\"source_id\":"<<sid<<",\"generation\":"<<m.generation<<",\"store_id\":"<<quote(m.store_id)<<'}';}out<<"]}";return out.str();
 }catch(...){m.reset_queries();sqlite3_exec(c.db,"ROLLBACK",nullptr,nullptr,nullptr);c.scope_initialized=false;c.ordinals_initialized=false;throw;}
}
void maintenance_read(Maintenance&m,const Tree&t,float*values,std::uint8_t*mask,std::uint64_t count){
 check(!m.writer,"read requires reader");auto sid=t.get<std::size_t>("source_id");auto generation=t.get<std::uint64_t>("generation");
 check(t.get<std::string>("store_id")==m.store_id&&sid>0&&sid<m.core.sources.size()&&m.core.sources[sid].path==t.get<std::string>("native_path")&&generation>=m.source_generations.at(sid)&&generation<=m.generation,"locator catalog binding mismatch");
 if(m.core.sources[sid].format=="hdf5")physical_dataset(m.core,m.core.sources[sid],t.get<std::string>("dataset"));
 read(m.core,t,values,mask,count);
}
std::string maintenance_status(Maintenance&m){
 auto&c=m.core;std::set<std::string>paths;std::int64_t generation,sources,units;
 {Transaction tx(c);generation=scalar(c,"SELECT generation FROM head WHERE singleton=1");sources=scalar(c,"SELECT count(*) FROM sources");units=scalar(c,"SELECT count(*) FROM units");Statement q(c.db,"SELECT path FROM sources");while(true){auto rc=sqlite3_step(q.p);if(rc==SQLITE_DONE)break;check(rc==SQLITE_ROW,"status sources failed");paths.insert(column_text(q.p,0));}tx.commit();}
 std::uint64_t orphan_files=0,orphan_bytes=0,staging_files=0,staging_bytes=0,data_bytes=0;
 for(const auto&e:fs::directory_iterator(m.objects)){
  std::error_code ec;auto type=e.symlink_status(ec);
  if(ec==std::errc::no_such_file_or_directory)continue;
  check(!ec,"object stat failed: "+ec.message());if(!fs::is_regular_file(type))continue;
  auto n=e.file_size(ec);if(ec==std::errc::no_such_file_or_directory)continue;
  check(!ec,"object size failed: "+ec.message());
  auto name=e.path().filename().string();if(name.rfind(".stage-",0)==0){++staging_files;staging_bytes+=n;}else if(!paths.count(e.path().string())){++orphan_files;orphan_bytes+=n;}else data_bytes+=n;
 }
 auto wal=disk_size(c.index+"-wal");m.wal_peak=std::max(m.wal_peak,wal);
 std::ostringstream o;o<<"{\"space_snapshot_atomic\":"<<(m.writer?"true":"false")<<",\"protocol\":"<<quote(maintenance_protocol)<<",\"test_faults_enabled\":"<<(test_faults_enabled?"true":"false")<<",\"generation\":"<<generation<<",\"sources\":"<<sources<<",\"units\":"<<units<<",\"indexed\":"<<(m.indexed?"true":"false")<<",\"store_id\":"<<quote(m.store_id)<<",\"database_bytes\":"<<disk_size(c.index)<<",\"wal_bytes\":"<<wal<<",\"shm_bytes\":"<<disk_size(c.index+"-shm")<<",\"observed_wal_peak_bytes\":"<<m.wal_peak<<",\"data_bytes\":"<<data_bytes<<",\"orphan_files\":"<<orphan_files<<",\"orphan_bytes\":"<<orphan_bytes<<",\"staging_files\":"<<staging_files<<",\"staging_bytes\":"<<staging_bytes<<",\"recovered_staging_files\":"<<m.recovered_staging<<",\"refresh_ns\":"<<m.refresh_ns<<",\"refreshed_sources\":"<<m.refresh_sources<<",\"native\":"<<c.stats_json()<<'}';return o.str();
}
std::string maintenance_checkpoint(Maintenance&m){check(m.writer,"checkpoint requires writer");auto started=Clock::now();int log=-1,done=-1;auto rc=sqlite3_wal_checkpoint_v2(m.core.db,nullptr,SQLITE_CHECKPOINT_TRUNCATE,&log,&done);check(rc==SQLITE_OK||rc==SQLITE_BUSY,"checkpoint failed");return "{\"busy\":"+std::string(rc==SQLITE_BUSY?"true":"false")+",\"log_frames\":"+std::to_string(log)+",\"checkpointed_frames\":"+std::to_string(done)+",\"elapsed_ns\":"+std::to_string(ns(started))+",\"wal_bytes\":"+std::to_string(disk_size(m.core.index+"-wal"))+"}";}
template<class F>void*maintenance_json(F function){try{error.clear();return copied(function());}catch(const std::exception&e){error=e.what();return nullptr;}catch(...){error="maintenance error";return nullptr;}}
Maintenance& maintenance_handle(void*p){check(p,"maintenance handle missing");return*static_cast<Maintenance*>(p);}
Tree maintenance_request(const char*p){check(p,"maintenance request missing");return parse(p);}
}
extern "C" {
void*fs_index_open_json(const char*config){try{error.clear();return maintenance_open(maintenance_request(config)).release();}catch(const std::exception&e){error=e.what();return nullptr;}catch(...){error="maintenance open failed";return nullptr;}}
void fs_index_close(void*p){delete static_cast<Maintenance*>(p);}
void*fs_index_publish_json(void*p,const char*r){return maintenance_json([&]{return publish(maintenance_handle(p),maintenance_request(r));});}
void*fs_index_select_json(void*p,const char*r){return maintenance_json([&]{return maintenance_select(maintenance_handle(p),maintenance_request(r));});}
void*fs_index_explain_json(void*p,const char*r){return maintenance_json([&]{return maintenance_select(maintenance_handle(p),maintenance_request(r),true);});}
void*fs_index_status_json(void*p){return maintenance_json([&]{return maintenance_status(maintenance_handle(p));});}
void*fs_index_checkpoint_json(void*p){return maintenance_json([&]{return maintenance_checkpoint(maintenance_handle(p));});}
int fs_index_read_into(void*p,const char*r,float*v,std::uint8_t*m,std::uint64_t n){try{error.clear();check(v&&m,"read buffers missing");maintenance_read(maintenance_handle(p),maintenance_request(r),v,m,n);return 0;}catch(const std::exception&e){error=e.what();return-1;}catch(...){error="maintenance read failed";return-1;}}
}
