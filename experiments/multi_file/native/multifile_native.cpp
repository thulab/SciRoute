// Persistent RPC worker; clocks cover native discovery/read/owned buffers only.
// SHA-256, JSON formatting and process/pipe costs are reported outside that clock.
#include <hdf5.h>
#include <eccodes.h>
#include <openssl/evp.h>
#include <sqlite3.h>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using Tree = boost::property_tree::ptree;
using Clock = std::chrono::steady_clock;
constexpr std::size_t MaxBytes = 64 * 1024 * 1024, MaxUnits = 262144;
void check(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
std::uint64_t elapsed(Clock::time_point a, Clock::time_point b) {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(b-a).count();
}
std::string escape(const std::string& value) {
  std::ostringstream out; out << '"';
  for (unsigned char ch : value) {
    if (ch == '"' || ch == '\\') out << '\\' << ch;
    else if (ch < 32) out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << int(ch) << std::dec;
    else out << ch;
  }
  return out.str() + '"';
}
std::string digest(const void* data, std::size_t bytes) {
  unsigned char result[EVP_MAX_MD_SIZE]; unsigned size = 0;
  check(EVP_Digest(data, bytes, result, &size, EVP_sha256(), nullptr)==1, "hash failed");
  std::ostringstream out; out << std::hex << std::setfill('0');
  for (unsigned i=0;i<size;++i) out << std::setw(2) << unsigned(result[i]);
  return out.str();
}
struct Id {
  hid_t id; herr_t(*closer)(hid_t);
  Id(hid_t i, herr_t(*c)(hid_t)):id(i),closer(c) { check(i>=0,"HDF5 open failed"); }
  ~Id(){ if(id>=0) closer(id); }
  operator hid_t() const{return id;}
  Id(const Id&)=delete;
};
struct Attrs { std::string name,type; long long level,step,date,time; };
Attrs predicate(const Tree& p) {
  check(p.size()==6,"native control requires six equality predicates");
  return {p.get<std::string>("shortName"),p.get<std::string>("typeOfLevel"),p.get<long long>("level"),
          p.get<long long>("step"),p.get<long long>("validityDate"),p.get<long long>("validityTime")};
}
bool match(const Attrs& a,const Attrs& b) {
  return a.name==b.name && a.type==b.type && a.level==b.level && a.step==b.step && a.date==b.date && a.time==b.time;
}
struct Unit {
  std::string path,dataset; std::uint64_t offset=0,length=0,size=0,mtime=0;
  Attrs attrs; std::vector<std::uint64_t> shape;
  std::vector<float> values; std::vector<std::uint8_t> mask;
};
struct Observation {
  std::vector<Unit> units; std::uint64_t files=0,candidates=0,discovery=0,reads=0,total=0;
};
void file_identity(const fs::path& file, Unit& unit) {
  struct stat value{}; check(::stat(file.c_str(),&value)==0 && S_ISREG(value.st_mode),"source stat failed");
  unit.size=value.st_size; unit.mtime=std::uint64_t(value.st_mtim.tv_sec)*1000000000+value.st_mtim.tv_nsec;
}
std::string string_attr(hid_t dataset,const char* name) {
  Id attr(H5Aopen(dataset,name,H5P_DEFAULT),H5Aclose), type(H5Aget_type(attr),H5Tclose), space(H5Aget_space(attr),H5Sclose);
  const auto size=H5Tget_size(type);
  check(H5Tget_class(type)==H5T_STRING && H5Tis_variable_str(type)==0 && size<=1024 && H5Sget_simple_extent_npoints(space)==1,"unbounded string attribute");
  std::vector<char> bytes(size+1,0); check(H5Aread(attr,type,bytes.data())>=0,"attribute read failed");
  return std::string(bytes.data(),strnlen(bytes.data(),size));
}
long long integer_attr(hid_t dataset,const char* name) {
  Id attr(H5Aopen(dataset,name,H5P_DEFAULT),H5Aclose), type(H5Aget_type(attr),H5Tclose), space(H5Aget_space(attr),H5Sclose);
  check(H5Tget_class(type)==H5T_INTEGER && H5Tget_sign(type)==H5T_SGN_2 && H5Tget_size(type)<=8 && H5Sget_simple_extent_npoints(space)==1,"attribute is not bounded signed integer");
  long long value; check(H5Aread(attr,H5T_NATIVE_LLONG,&value)>=0,"attribute read failed"); return value;
}
void hdf_values(hid_t file, Unit& unit) {
  Id dataset(H5Dopen2(file,unit.dataset.c_str(),H5P_DEFAULT),H5Dclose), space(H5Dget_space(dataset),H5Sclose), type(H5Dget_type(dataset),H5Tclose);
  check(H5Tget_class(type)==H5T_FLOAT && H5Tget_size(type)==4,"native control requires Float32 data");
  const auto count=H5Sget_simple_extent_npoints(space); check(count>0 && std::uint64_t(count)<=MaxBytes/5,"result exceeds bound");
  const int rank=H5Sget_simple_extent_ndims(space); check(rank==4,"native control requires rank four");
  std::vector<hsize_t> shape(rank); check(H5Sget_simple_extent_dims(space,shape.data(),nullptr)>=0,"shape failed");
  unit.shape.assign(shape.begin(),shape.end()); unit.values.resize(count);
  check(H5Dread(dataset,H5T_NATIVE_FLOAT,H5S_ALL,H5S_ALL,H5P_DEFAULT,unit.values.data())>=0,"H5Dread failed");
  unit.mask.assign(count,1); for(float value:unit.values) check(std::isfinite(value),"controlled values must be finite");
}
long codes_long(codes_handle* handle,const char* name) {
  long value=0; check(codes_get_long(handle,name,&value)==0,"ecCodes integer key failed"); return value;
}
std::string codes_string(codes_handle* handle,const char* name) {
  char data[1024]; std::size_t size=sizeof(data); check(codes_get_string(handle,name,data,&size)==0,"ecCodes string key failed"); return data;
}
void decode_grib_values(codes_handle* handle,Unit& unit) {
  std::size_t count=0;
  check(codes_get_size(handle,"values",&count)==0 && count>0 && count<=MaxBytes/16,"decode exceeds bound");
  std::vector<double> decoded(count); check(codes_get_double_array(handle,"values",decoded.data(),&count)==0,"decode failed");
  unit.values.resize(count); unit.mask.assign(count,1);
  double missing=0; const bool bitmap=codes_long(handle,"bitmapPresent")!=0;
  if(bitmap) check(codes_get_double(handle,"missingValue",&missing)==0,"missing value key failed");
  for(std::size_t i=0;i<count;++i) {
    unit.mask[i]=std::isfinite(decoded[i]) && (!bitmap || decoded[i]!=missing);
    unit.values[i]=unit.mask[i]?float(decoded[i]):0; check(std::isfinite(unit.values[i]),"Float32 overflow");
  }
  unit.shape={std::uint64_t(codes_long(handle,"Nj")),std::uint64_t(codes_long(handle,"Ni"))};
}
void read_grib_values(const fs::path& root,Unit& unit) {
  check(unit.length>0 && unit.length<=MaxBytes,"encoded message exceeds bound");
  std::ifstream in(root/unit.path,std::ios::binary); check(bool(in),"GRIB source open failed");
  in.seekg(unit.offset); std::vector<char> encoded(unit.length); in.read(encoded.data(),encoded.size());
  check(std::size_t(in.gcount())==encoded.size(),"short message read");
  std::unique_ptr<codes_handle,decltype(&codes_handle_delete)> handle(codes_handle_new_from_message_copy(nullptr,encoded.data(),encoded.size()),codes_handle_delete);
  check(bool(handle),"ecCodes rejected message");decode_grib_values(handle.get(),unit);
}
struct Visit {
  hid_t file; Unit prototype; const Attrs& wanted; bool all,values; Observation& result;
  std::exception_ptr error;
};
herr_t visit_dataset(hid_t,const char* name,const H5O_info2_t* info,void* opaque) {
  auto& context=*static_cast<Visit*>(opaque);
  if(info->type!=H5O_TYPE_DATASET) return 0;
  try {
    check(context.result.candidates<MaxUnits,"candidate unit bound exceeded"); ++context.result.candidates;
    Unit unit=context.prototype; unit.dataset=std::string("/")+name;
    Id dataset(H5Dopen2(context.file,unit.dataset.c_str(),H5P_DEFAULT),H5Dclose);
    unit.attrs={string_attr(dataset,"shortName"),string_attr(dataset,"typeOfLevel"),integer_attr(dataset,"level"),
                integer_attr(dataset,"step"),integer_attr(dataset,"validityDate"),integer_attr(dataset,"validityTime")};
    if(context.all || match(unit.attrs,context.wanted)) {
      if(context.values) { const auto begin=Clock::now(); hdf_values(context.file,unit); context.result.reads+=elapsed(begin,Clock::now()); }
      context.result.units.push_back(std::move(unit));
    }
    return 0;
  } catch(...) {context.error=std::current_exception();return -1;}
}
Observation scan(const fs::path& root,const std::string& format,const Attrs& wanted,bool values,bool all=false) {
  const auto begin=Clock::now(); Observation result; std::vector<fs::path> files;
  for(const auto& entry:fs::recursive_directory_iterator(root)) {
    check(!entry.is_symlink(),"native tree must not contain symlinks");
    if(entry.is_regular_file()) files.push_back(entry.path());
    check(files.size()<=4096,"candidate file bound exceeded");
  }
  std::sort(files.begin(),files.end()); result.files=files.size();
  for(const auto& file:files) {
    Unit prototype; prototype.path=file.lexically_relative(root).generic_string(); file_identity(file,prototype);
    if(format=="hdf5") {
      Id opened(H5Fopen(file.c_str(),H5F_ACC_RDONLY,H5P_DEFAULT),H5Fclose);
      Visit context{opened,prototype,wanted,all,values,result,nullptr};
      const auto status=H5Ovisit3(opened,H5_INDEX_NAME,H5_ITER_INC,visit_dataset,&context,H5O_INFO_BASIC);
      if(context.error) std::rethrow_exception(context.error); check(status>=0,"dataset discovery failed");
    } else {
      std::unique_ptr<FILE,decltype(&fclose)> opened(fopen(file.c_str(),"rb"),fclose); check(bool(opened),"GRIB source open failed");
      while(true) {
        const auto offset=ftello(opened.get()); int error=0;
        // ecCodes 2.34.1 exposes its fast metadata reader in grib_api.h.
        // Value requests read only the matched original byte range afterwards.
        std::unique_ptr<codes_handle,decltype(&codes_handle_delete)> handle(grib_new_from_file(nullptr,opened.get(),1,&error),codes_handle_delete);
        if(!handle) {check((error==0 || error==CODES_END_OF_FILE) && offset>=0 && std::uint64_t(offset)==prototype.size,"GRIB scan failed before EOF");break;}
        check(result.candidates<MaxUnits,"candidate unit bound exceeded"); ++result.candidates;
        Unit unit=prototype; unit.offset=offset; unit.length=codes_long(handle.get(),"totalLength");
        unit.attrs={codes_string(handle.get(),"shortName"),codes_string(handle.get(),"typeOfLevel"),codes_long(handle.get(),"level"),codes_long(handle.get(),"step"),codes_long(handle.get(),"validityDate"),codes_long(handle.get(),"validityTime")};
        if(all || match(unit.attrs,wanted)) {
          if(values) {const auto start=Clock::now();read_grib_values(root,unit);result.reads+=elapsed(start,Clock::now());}
          result.units.push_back(std::move(unit));
        }
      }
    }
  }
  result.total=elapsed(begin,Clock::now()); result.discovery=result.total-result.reads; return result;
}
struct Database {
  sqlite3* db=nullptr;
  Database(const std::string& path,int flags) { check(sqlite3_open_v2(path.c_str(),&db,flags,nullptr)==SQLITE_OK,"SQLite open failed"); }
  ~Database(){if(db)sqlite3_close(db);}
  void sql(const char* text) {check(sqlite3_exec(db,text,nullptr,nullptr,nullptr)==SQLITE_OK,"SQLite execution failed");}
};
struct Statement {
  sqlite3_stmt* statement=nullptr;
  Statement(sqlite3* db,const char* sql){check(sqlite3_prepare_v2(db,sql,-1,&statement,nullptr)==SQLITE_OK,"SQLite prepare failed");}
  ~Statement(){if(statement)sqlite3_finalize(statement);}
  void text(int column,const std::string& value){check(sqlite3_bind_text(statement,column,value.c_str(),-1,SQLITE_TRANSIENT)==SQLITE_OK,"SQLite bind failed");}
  void number(int column,std::int64_t value){check(sqlite3_bind_int64(statement,column,value)==SQLITE_OK,"SQLite bind failed");}
};
std::string build_index(const Tree& input) {
  const auto started=Clock::now(); const fs::path root=input.get<std::string>("root");
  const std::string format=input.get<std::string>("format"),path=input.get<std::string>("index");
  check(!fs::exists(path),"index exists"); auto rows=scan(root,format,predicate(input.get_child("predicate")),false,true);
  Database db(path,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE); db.sql("PRAGMA synchronous=FULL; BEGIN IMMEDIATE; CREATE TABLE units(path TEXT,dataset TEXT,offset INTEGER,length INTEGER,size INTEGER,mtime INTEGER,name TEXT,type TEXT,level INTEGER,step INTEGER,date INTEGER,time INTEGER); CREATE TABLE identity(root TEXT,format TEXT);");
  {Statement identity(db.db,"INSERT INTO identity VALUES(?,?)"); identity.text(1,root.string());identity.text(2,format);check(sqlite3_step(identity.statement)==SQLITE_DONE,"identity insert failed");}
  {Statement insert(db.db,"INSERT INTO units VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
    for(const auto& u:rows.units) {insert.text(1,u.path);insert.text(2,u.dataset);insert.number(3,u.offset);insert.number(4,u.length);insert.number(5,u.size);insert.number(6,u.mtime);insert.text(7,u.attrs.name);insert.text(8,u.attrs.type);insert.number(9,u.attrs.level);insert.number(10,u.attrs.step);insert.number(11,u.attrs.date);insert.number(12,u.attrs.time);check(sqlite3_step(insert.statement)==SQLITE_DONE,"index insert failed");sqlite3_reset(insert.statement);}
  }
  db.sql("CREATE INDEX predicate ON units(name,type,level,step,date,time); COMMIT;");
  const auto ended=Clock::now();
  return "{\"ok\":true,\"total_ns\":"+std::to_string(elapsed(started,ended))+",\"native_scan_ns\":"+std::to_string(rows.total)+",\"files\":"+std::to_string(rows.files)+",\"units\":"+std::to_string(rows.units.size())+",\"index_bytes\":"+std::to_string(fs::file_size(path))+"}";
}
std::unique_ptr<Database> index_db; std::string opened_index;
Observation indexed(const Tree& input,const Attrs& wanted,bool values) {
  const auto begin=Clock::now(); Observation result;
  const auto root=fs::path(input.get<std::string>("root"));const auto format=input.get<std::string>("format"),path=input.get<std::string>("index");
  if(!index_db || opened_index!=path) {index_db=std::make_unique<Database>(path,SQLITE_OPEN_READONLY);opened_index=path;}
  {Statement identity(index_db->db,"SELECT root,format FROM identity"); check(sqlite3_step(identity.statement)==SQLITE_ROW,"missing index identity");
   check(root.string()==reinterpret_cast<const char*>(sqlite3_column_text(identity.statement,0)) && format==reinterpret_cast<const char*>(sqlite3_column_text(identity.statement,1)),"index root/format mismatch");}
  Statement query(index_db->db,"SELECT path,dataset,offset,length,size,mtime FROM units WHERE name=? AND type=? AND level=? AND step=? AND date=? AND time=? ORDER BY path,offset,dataset");
  query.text(1,wanted.name);query.text(2,wanted.type);query.number(3,wanted.level);query.number(4,wanted.step);query.number(5,wanted.date);query.number(6,wanted.time);
  while(true) {const auto status=sqlite3_step(query.statement);if(status==SQLITE_DONE)break;check(status==SQLITE_ROW,"index query failed");
    check(result.units.size()<MaxUnits,"selected unit bound exceeded"); Unit u;
    u.path=reinterpret_cast<const char*>(sqlite3_column_text(query.statement,0));u.dataset=reinterpret_cast<const char*>(sqlite3_column_text(query.statement,1));
    u.offset=sqlite3_column_int64(query.statement,2);u.length=sqlite3_column_int64(query.statement,3);u.size=sqlite3_column_int64(query.statement,4);u.mtime=sqlite3_column_int64(query.statement,5);u.attrs=wanted;
    check(!fs::path(u.path).is_absolute() && u.path.find("..") == std::string::npos,"unsafe indexed source path");
    Unit actual;file_identity(root/u.path,actual);check(actual.size==u.size && actual.mtime==u.mtime && ::access((root/u.path).c_str(),R_OK)==0,"indexed source stale or unreadable");
    if(values) {const auto started=Clock::now();if(format=="hdf5"){Id file(H5Fopen((root/u.path).c_str(),H5F_ACC_RDONLY,H5P_DEFAULT),H5Fclose);hdf_values(file,u);}else read_grib_values(root,u);result.reads+=elapsed(started,Clock::now());}
    result.units.push_back(std::move(u));
  }
  result.total=elapsed(begin,Clock::now());result.discovery=result.total-result.reads;return result;
}
std::string observation(const Observation& value,bool values) {
  std::ostringstream out;
  out<<"{\"ok\":true,\"total_ns\":"<<value.total<<",\"discovery_ns\":"<<value.discovery<<",\"value_read_ns\":"<<value.reads<<",\"enumerated_files\":"<<value.files<<",\"candidate_units\":"<<value.candidates<<",\"selected_units\":"<<value.units.size()<<",\"units\":[";
  bool first=true;
  for(const auto& u:value.units) {
    if(!first)out<<',';first=false;
    out<<"{\"path\":"<<escape(u.path)<<",\"dataset\":"<<escape(u.dataset)<<",\"offset\":"<<u.offset<<",\"length\":"<<u.length;
    if(values) {out<<",\"rows\":"<<u.values.size()<<",\"value_sha256\":"<<escape(digest(u.values.data(),u.values.size()*4))<<",\"mask_sha256\":"<<escape(digest(u.mask.data(),u.mask.size()));}
    out<<'}';
  }
  return out.str()+"]}";
}
int main(int argc,char** argv) {
  try {
    for(const auto* key:{"HDF5_DRIVER","HDF5_VOL_CONNECTOR"})check(!std::getenv(key)||!std::getenv(key)[0],"driver/VOL overrides forbidden");
    check(H5PLset_loading_state(0)>=0,"cannot disable plugins");H5Eset_auto2(H5E_DEFAULT,nullptr,nullptr);
    unsigned a,b,c;hbool_t threadsafe;check(H5get_libversion(&a,&b,&c)>=0&&a==1&&b==14&&c==4,"HDF5 runtime mismatch");check(H5is_library_threadsafe(&threadsafe)>=0&&threadsafe,"threadsafe HDF5 required");
    if(argc==2 && std::string(argv[1])=="--versions") {std::cout<<"{\"hdf5\":\"1.14.4\",\"threadsafe\":true,\"plugins\":false,\"eccodes\":"<<codes_get_api_version()<<",\"sqlite\":"<<escape(sqlite3_libversion())<<"}\n";return 0;}
    std::string line;
    while(std::getline(std::cin,line)) {
      try {
        check(line.size()<=1048576,"request exceeds bound"); std::istringstream text(line);Tree input;boost::property_tree::read_json(text,input);
        const auto op=input.get<std::string>("op"),format=input.get<std::string>("format");check(format=="hdf5"||format=="grib","unknown format");
        if(op=="build") std::cout<<build_index(input)<<'\n';
        else {check(op=="query","unknown operation");const auto mode=input.get<std::string>("mode");check(mode=="scan"||mode=="index","unknown query mode");const bool values=input.get<bool>("values");const auto attrs=predicate(input.get_child("predicate"));auto result=mode=="scan"?scan(input.get<std::string>("root"),format,attrs,values):indexed(input,attrs,values);std::cout<<observation(result,values)<<'\n';}
      } catch(const std::exception& error){std::cout<<"{\"ok\":false,\"error\":"<<escape(error.what())<<"}\n";}
      std::cout.flush();
    }
  } catch(const std::exception& error){std::cerr<<error.what()<<'\n';return 1;}
}
