// Persistent JSON-lines worker for the N=8 version-directory matrix.
// Reuse precisely the established native discovery/read/owned-buffer/oracle
// implementation. The original entry point and baseline remain untouched.
#define main multifile_native_original_main
#include "../../../multi_file/native/multifile_native.cpp"
#undef main

namespace matrix {
constexpr unsigned FileCount = 8;
struct Config { fs::path root; std::string format; unsigned versions; };
struct VersionUnit { Unit unit; unsigned ordinal; std::string logical_file; };
struct Result {
  std::vector<VersionUnit> units;
  std::uint64_t files=0,candidates=0,discovery=0,reads=0,total=0;
};
Config config(const Tree& input) {
  const auto versions=input.get<unsigned>("version_count");
  check(versions==1||versions==2||versions==4||versions==8,"version_count must be 1, 2, 4 or 8");
  const auto format=input.get<std::string>("format");
  check(format=="hdf5"||format=="grib","unknown format");
  const fs::path root=input.get<std::string>("root");
  check(root.is_absolute()&&root==root.lexically_normal(),"root must be an absolute normalized path");
  return {root,format,versions};
}
std::string version_name(unsigned ordinal) {
  std::ostringstream out;out<<'v'<<std::setw(3)<<std::setfill('0')<<ordinal;return out.str();
}
void check_root(const Config& c) {
  check(fs::is_directory(c.root)&&fs::canonical(c.root)==c.root,"root must be a canonical directory");
}
fs::path version_root(const Config& c,unsigned ordinal) {
  const auto root=c.root/version_name(ordinal);
  check(!fs::is_symlink(root)&&fs::is_directory(root),"selected version directory is absent or symlinked");return root;
}
std::pair<unsigned,unsigned> selected(const Tree& input,const Config& c) {
  const auto selection=input.get<std::string>("selection");
  if(selection=="latest") return {c.versions,c.versions};
  if(selection=="all") return {1,c.versions};
  check(selection=="exact","selection must be latest, exact or all");
  const auto ordinal=input.get<unsigned>("exact_ordinal",1);
  check(ordinal>=1&&ordinal<=c.versions,"exact ordinal is outside history");return {ordinal,ordinal};
}
bool safe_logical_file(const std::string& file) {
  const fs::path path(file);
  if(file.empty()||file.find('\0')!=std::string::npos||path.is_absolute()||path!=path.lexically_normal())return false;
  for(const auto& component:path)if(component=="."||component=="..")return false;
  return true;
}
Result scan_versions(const Config& c,const Attrs& wanted,bool values,
    unsigned first,unsigned last,bool all_predicates=false) {
  const auto started=Clock::now();check_root(c);Result result;
  for(unsigned ordinal=first;ordinal<=last;++ordinal) {
    auto rows=::scan(version_root(c,ordinal),c.format,wanted,values,all_predicates);
    check(rows.files==FileCount,"each selected version must contain exactly eight files");
    result.files+=rows.files;result.candidates+=rows.candidates;result.reads+=rows.reads;
    for(auto& unit:rows.units) {
      check(result.units.size()<MaxUnits,"selected unit bound exceeded");
      const auto logical_file=unit.path;check(safe_logical_file(logical_file),"unsafe discovered logical file");
      unit.path=version_name(ordinal)+"/"+logical_file;
      check(::access((c.root/unit.path).c_str(),R_OK)==0,"discovered source unreadable");
      result.units.push_back({std::move(unit),ordinal,logical_file});
    }
  }
  result.total=elapsed(started,Clock::now());result.discovery=result.total-result.reads;return result;
}
std::unique_ptr<Database> database;
std::string database_path;
std::string build(const Tree& input) {
  const auto started=Clock::now();const auto c=config(input);
  const std::string path=input.get<std::string>("index");
  check(fs::path(path).is_absolute()&&!fs::exists(path),"index must be a new absolute file");
  Attrs unused{};auto rows=scan_versions(c,unused,false,1,c.versions,true);
  Database db(path,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
  db.sql("PRAGMA synchronous=FULL; BEGIN IMMEDIATE; CREATE TABLE identity(protocol TEXT,root TEXT,format TEXT,version_count INTEGER,file_count INTEGER); CREATE TABLE units(ordinal INTEGER,logical_file TEXT,path TEXT,dataset TEXT,offset INTEGER,length INTEGER,size INTEGER,mtime INTEGER,name TEXT,type TEXT,level INTEGER,step INTEGER,date INTEGER,time INTEGER);");
  {
    Statement identity(db.db,"INSERT INTO identity VALUES(?,?,?,?,?)");
    identity.text(1,"iotdbfs-version-matrix-native/1");identity.text(2,c.root.string());identity.text(3,c.format);
    identity.number(4,c.versions);identity.number(5,FileCount);
    check(sqlite3_step(identity.statement)==SQLITE_DONE,"index identity insert failed");
  }
  {
    Statement insert(db.db,"INSERT INTO units VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
    for(const auto& entry:rows.units) {
      const auto& u=entry.unit;
      insert.number(1,entry.ordinal);insert.text(2,entry.logical_file);insert.text(3,u.path);insert.text(4,u.dataset);
      insert.number(5,u.offset);insert.number(6,u.length);insert.number(7,u.size);insert.number(8,u.mtime);
      insert.text(9,u.attrs.name);insert.text(10,u.attrs.type);insert.number(11,u.attrs.level);insert.number(12,u.attrs.step);
      insert.number(13,u.attrs.date);insert.number(14,u.attrs.time);
      check(sqlite3_step(insert.statement)==SQLITE_DONE,"index insert failed");
      check(sqlite3_reset(insert.statement)==SQLITE_OK,"index statement reset failed");
    }
  }
  // Version is the first lookup dimension. A single database covers all V
  // directories; no latest-only or per-predicate materialized index is built.
  db.sql("CREATE INDEX version_predicate ON units(ordinal,name,type,level,step,date,time); COMMIT;");
  const auto ended=Clock::now();
  std::ostringstream out;
  out<<"{\"ok\":true,\"op\":\"build\",\"protocol\":\"iotdbfs-version-matrix-native/1\",\"root\":"<<escape(c.root.string())
    <<",\"format\":"<<escape(c.format)<<",\"version_count\":"<<c.versions<<",\"files_per_version\":"<<FileCount
    <<",\"total_ns\":"<<elapsed(started,ended)<<",\"native_scan_ns\":"<<rows.total<<",\"files\":"<<rows.files
    <<",\"units\":"<<rows.units.size()<<",\"index_bytes\":"<<fs::file_size(path)<<'}';return out.str();
}
Result indexed(const Tree& input,const Config& c,const Attrs& wanted,bool values,
    unsigned first,unsigned last) {
  const auto started=Clock::now();check_root(c);Result result;
  const std::string path=input.get<std::string>("index");check(fs::path(path).is_absolute(),"index must be absolute");
  if(!database||database_path!=path) {database=std::make_unique<Database>(path,SQLITE_OPEN_READONLY);database_path=path;}
  {
    Statement identity(database->db,"SELECT protocol,root,format,version_count,file_count FROM identity");
    check(sqlite3_step(identity.statement)==SQLITE_ROW,"index identity missing");
    auto text=[&](int column){const auto* value=sqlite3_column_text(identity.statement,column);check(value,"null index identity");return std::string(reinterpret_cast<const char*>(value));};
    check(text(0)=="iotdbfs-version-matrix-native/1"&&text(1)==c.root.string()&&text(2)==c.format&&
      sqlite3_column_int64(identity.statement,3)==c.versions&&sqlite3_column_int64(identity.statement,4)==FileCount,"index root/format/version_count identity mismatch");
    check(sqlite3_step(identity.statement)==SQLITE_DONE,"duplicate index identity");
  }
  const char* exact_sql="SELECT ordinal,logical_file,path,dataset,offset,length,size,mtime FROM units WHERE ordinal=? AND name=? AND type=? AND level=? AND step=? AND date=? AND time=? ORDER BY ordinal,logical_file,offset,dataset";
  const char* all_sql="SELECT ordinal,logical_file,path,dataset,offset,length,size,mtime FROM units WHERE ordinal BETWEEN ? AND ? AND name=? AND type=? AND level=? AND step=? AND date=? AND time=? ORDER BY ordinal,logical_file,offset,dataset";
  const bool one=first==last;Statement query(database->db,one?exact_sql:all_sql);int parameter=1;
  query.number(parameter++,first);if(!one)query.number(parameter++,last);
  query.text(parameter++,wanted.name);query.text(parameter++,wanted.type);query.number(parameter++,wanted.level);
  query.number(parameter++,wanted.step);query.number(parameter++,wanted.date);query.number(parameter++,wanted.time);
  while(true) {
    const auto status=sqlite3_step(query.statement);if(status==SQLITE_DONE)break;check(status==SQLITE_ROW,"version-filtered index query failed");
    check(result.units.size()<MaxUnits,"selected unit bound exceeded");
    auto text=[&](int column){const auto* value=sqlite3_column_text(query.statement,column);check(value,"null indexed source text");return std::string(reinterpret_cast<const char*>(value));};
    const auto ordinal=sqlite3_column_int64(query.statement,0);check(ordinal>=first&&ordinal<=last,"index returned an unselected ordinal");
    VersionUnit entry;entry.ordinal=ordinal;entry.logical_file=text(1);auto& u=entry.unit;
    u.path=text(2);u.dataset=text(3);
    check(safe_logical_file(entry.logical_file)&&u.path==version_name(entry.ordinal)+"/"+entry.logical_file,"unsafe indexed source binding");
    for(int column=4;column<8;++column)check(sqlite3_column_int64(query.statement,column)>=0,"negative indexed source integer");
    u.offset=sqlite3_column_int64(query.statement,4);u.length=sqlite3_column_int64(query.statement,5);
    u.size=sqlite3_column_int64(query.statement,6);u.mtime=sqlite3_column_int64(query.statement,7);u.attrs=wanted;
    (void)version_root(c,entry.ordinal);
    const auto file=c.root/u.path;check(fs::canonical(file)==file,"indexed source path is symlinked");
    Unit actual;file_identity(file,actual);
    check(actual.size==u.size&&actual.mtime==u.mtime&&::access(file.c_str(),R_OK)==0,"indexed source stale or unreadable");
    if(values) {
      const auto reading=Clock::now();
      if(c.format=="hdf5") {Id opened(H5Fopen(file.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT),H5Fclose);hdf_values(opened,u);}
      else read_grib_values(c.root,u);
      result.reads+=elapsed(reading,Clock::now());
    }
    result.units.push_back(std::move(entry));
  }
  result.total=elapsed(started,Clock::now());result.discovery=result.total-result.reads;return result;
}
std::string observation(const Result& result,bool values,const Config& c,const std::string& selection) {
  std::ostringstream out;
  out<<"{\"ok\":true,\"op\":\"query\",\"selection\":"<<escape(selection)<<",\"version_count\":"<<c.versions
    <<",\"total_ns\":"<<result.total<<",\"discovery_ns\":"<<result.discovery<<",\"value_read_ns\":"<<result.reads
    <<",\"enumerated_files\":"<<result.files<<",\"candidate_units\":"<<result.candidates
    <<",\"selected_units\":"<<result.units.size()<<",\"units\":[";
  bool first=true;
  for(const auto& entry:result.units) {
    const auto& u=entry.unit;if(!first)out<<',';first=false;
    out<<"{\"logical_file\":"<<escape(entry.logical_file)<<",\"ordinal\":"<<entry.ordinal<<",\"path\":"<<escape(u.path)
      <<",\"dataset\":"<<escape(u.dataset)<<",\"offset\":"<<u.offset<<",\"length\":"<<u.length;
    if(values)out<<",\"rows\":"<<u.values.size()<<",\"value_sha256\":"<<escape(digest(u.values.data(),u.values.size()*4))
      <<",\"mask_sha256\":"<<escape(digest(u.mask.data(),u.mask.size()));
    out<<'}';
  }
  out<<"]}";return out.str();
}
std::string query(const Tree& input) {
  const auto started=Clock::now();const auto c=config(input);const auto versions=selected(input,c);
  const auto mode=input.get<std::string>("mode");check(mode=="scan"||mode=="index","unknown query mode");
  const bool values=input.get<bool>("values");const auto wanted=::predicate(input.get_child("predicate"));
  auto result=mode=="scan"?scan_versions(c,wanted,values,versions.first,versions.second):indexed(input,c,wanted,values,versions.first,versions.second);
  // Include version selection, arm dispatch and formation of every owned array.
  result.total=elapsed(started,Clock::now());result.discovery=result.total-result.reads;
  return observation(result,values,c,input.get<std::string>("selection"));
}
} // namespace matrix
int main(int argc,char** argv) {
  try {
    for(const auto* key:{"HDF5_DRIVER","HDF5_VOL_CONNECTOR"})check(!std::getenv(key)||!std::getenv(key)[0],"driver/VOL overrides forbidden");
    check(H5PLset_loading_state(0)>=0,"cannot disable plugins");H5Eset_auto2(H5E_DEFAULT,nullptr,nullptr);
    unsigned a,b,c;hbool_t threadsafe;
    check(H5get_libversion(&a,&b,&c)>=0&&a==1&&b==14&&c==4,"HDF5 runtime mismatch");
    check(H5is_library_threadsafe(&threadsafe)>=0&&threadsafe,"threadsafe HDF5 required");
    if(argc==2&&std::string(argv[1])=="--versions") {
      std::cout<<"{\"protocol\":\"iotdbfs-version-matrix-native/1\",\"files_per_version\":8,\"hdf5\":\"1.14.4\",\"threadsafe\":true,\"plugins\":false,\"eccodes\":"<<codes_get_api_version()<<",\"sqlite\":"<<escape(sqlite3_libversion())<<"}\n";return 0;
    }
    check(argc==1,"usage: version_matrix_native [--versions]");
    std::string line;
    while(std::getline(std::cin,line)) {
      try {
        check(line.size()<=1048576,"request exceeds bound");std::istringstream text(line);Tree input;boost::property_tree::read_json(text,input);
        const auto op=input.get<std::string>("op");
        if(op=="build")std::cout<<matrix::build(input)<<'\n';
        else {check(op=="query","unknown operation");std::cout<<matrix::query(input)<<'\n';}
      } catch(const std::exception& error) {std::cout<<"{\"ok\":false,\"error\":"<<escape(error.what())<<"}\n";}
      std::cout.flush();
    }
  } catch(const std::exception& error) {std::cerr<<error.what()<<'\n';return 1;}
}
